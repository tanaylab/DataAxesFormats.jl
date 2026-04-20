"""
A `Daf` storage format in a [Zarr](https://zarr.readthedocs.io/) directory tree. Like
[`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf), the data lives in a directory of files on the filesystem (so
standard filesystem tools work, and deleting a property immediately frees its storage), and offers a different
trade-off compared to [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) and
[`H5df`](@ref DataAxesFormats.H5dfFormat.H5df).

`FilesDaf` uses its own `Daf`-specific layout, but the individual files are in deliberately simple formats (`JSON` for
metadata, one-line-per-entry text for axis entries, raw little-endian binary for numeric data), so they are easy to
inspect or produce with standard command-line tools even without any `Daf`-aware library. `ZarrDaf` instead lays the
files out according to the Zarr specification: the per-array `.zarray` metadata and the chunk files are more opaque
than `FilesDaf`'s plain text/JSON, but in exchange the directory can be read directly by any Zarr library (e.g. the
Python `zarr` package) without that library having to know anything about `Daf`.

A Zarr directory is still a directory rather than a single file, so you need to create a `zip` or `tar` archive to
publish it, and accessing it consumes multiple file descriptors. In addition, not every Zarr feature is supported here:
we rely on the local `DirectoryStore` backend (no ZIP stores or remote object stores), and require every array to be
stored in a single uncompressed chunk, so we can memory-map it directly for efficient access.

We use the following internal structure under some root Zarr group (which is **not** compatible with any specific
existing Zarr-based convention such as [`OME-NGFF`](https://ngff.openmicroscopy.org/)):

  - The directory will contain 4 sub-groups: `scalars`, `axes`, `vectors`, and `matrices`, and a `daf` array.

  - The `daf` array signifies that the group contains `Daf` data. It contains two `UInt8` integers, the first being the
    major version number and the second the minor version number, using [semantic versioning](https://semver.org/).
    This makes it easy to test whether some Zarr group does/n't contain `Daf` data, and which version of the internal
    structure it is using. Currently the only defined version is `[1,0]`.

  - The `scalars` group contains scalar properties, each as a single-element Zarr array. The only supported scalar data
    types are these included in [`StorageScalar`](@ref). If you **really** need something else, serialize it to JSON
    and store the result as a string scalar. This should be **extremely** rare.

  - The `axes` group contains a Zarr array per axis, which contains a vector of strings (the names of the axis
    entries).

  - The `vectors` group contains a sub-group for each axis. Each such sub-group contains vector properties. If the
    vector is dense, it is stored directly as a Zarr array. Otherwise, it is stored as a sub-group containing two child
    Zarr arrays: `nzind` containing the indices of the non-zero values, and `nzval` containing the actual values. See
    Julia's `SparseVector` implementation for details. The only supported vector element types are these included in
    [`StorageScalar`](@ref), same as [`StorageVector`](@ref).

    If the data type is `Bool` then the data vector is typically all-`true` values; in this case we simply skip storing
    the `nzval` child array.

  - The `matrices` group contains a sub-group for each rows axis, which contains a sub-group for each columns axis.
    Each such sub-sub-group contains matrix properties. If the matrix is dense, it is stored directly as a Zarr array
    (in column-major layout). Otherwise, it is stored as a sub-group containing three child Zarr arrays: `colptr`
    containing the indices of the rows of each column in `rowval`, `rowval` containing the indices of the non-zero rows
    of the columns, and `nzval` containing the non-zero matrix entry values. See Julia's `SparseMatrixCSC`
    implementation for details. The only supported matrix element types are these included in [`StorageReal`](@ref) -
    this explicitly excludes matrices of strings, same as [`StorageMatrix`](@ref).

    If the data type is `Bool` then the data matrix is typically all-`true` values; in this case we simply skip storing
    the `nzval` child array.

  - Every Zarr array is created without compression, using a single chunk covering the full array, so the chunk file on
    disk is a raw binary image that we can memory-map for direct access.

Example Zarr directory structure:

    example-daf-dataset-root-directory.zarr/
    ├─ .zgroup
    ├─ daf/
    │  ├─ .zarray
    │  └─ 0
    ├─ scalars/
    │  ├─ .zgroup
    │  └─ version/
    │     ├─ .zarray
    │     └─ 0
    ├─ axes/
    │  ├─ .zgroup
    │  ├─ cell/
    │  └─ gene/
    ├─ vectors/
    │  ├─ .zgroup
    │  ├─ cell/
    │  │  ├─ .zgroup
    │  │  └─ batch/
    │  └─ gene/
    │     ├─ .zgroup
    │     └─ is_marker/
    └─ matrices/
       ├─ .zgroup
       ├─ cell/
       │  ├─ .zgroup
       │  └─ gene/
       │     ├─ .zgroup
       │     └─ UMIs/
       │        ├─ .zgroup
       │        ├─ colptr/
       │        ├─ rowval/
       │        └─ nzval/
       └─ gene/
          ├─ .zgroup
          ├─ cell/
          └─ gene/

!!! note

    Only the local `DirectoryStore` backend (a `.zarr` directory tree on a local filesystem) is supported. Other Zarr
    stores (ZIP archives, cloud object stores, …) are not.

!!! note

    The code here assumes the Zarr data obeys all the above conventions and restrictions. As long as you only create
    and access `Daf` data in Zarr directories using [`ZarrDaf`](@ref), then the code will work as expected (assuming no
    bugs). However, if you do this in some other way (e.g., a Zarr library in another language producing compressed or
    multi-chunk arrays), and the result is invalid, then the code here may fail with "less than friendly" error
    messages.
"""
module ZarrFormat

export ZarrDaf

using ..Formats
using ..ReadOnly
using ..Readers
using ..StorageTypes
using ..Writers
using Base.Filesystem
using Mmap
using ProgressMeter
using SparseArrays
using Zarr

import ..Formats
import ..Formats.Internal
import ..Readers.base_array
import ..Reorder
using TanayLabUtilities

"""
The major version of the [`ZarrDaf`](@ref) on-disk format supported by this code.
"""
MAJOR_VERSION::UInt8 = 1

"""
The highest minor version of the [`ZarrDaf`](@ref) on-disk format supported by this code.
"""
MINOR_VERSION::UInt8 = 0

const DAF_KEY = "daf"
const SCALARS = "scalars"
const AXES = "axes"
const VECTORS = "vectors"
const MATRICES = "matrices"

"""
    ZarrDaf(
        path::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing]
    )

Storage in a Zarr directory tree.

The `path` is a filesystem path to a Zarr directory (by convention, using a `.zarr` suffix).

When opening an existing data set, if `name` is not specified, and there exists a "name" scalar property, it is used as
the name. Otherwise, the `path` will be used as the name.

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type         |
|:---- |:-------------------- |:------------------------- |:------------------- |:--------------------- |
| `r`  | No                   | No                        | No                  | [`DafReadOnly`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`ZarrDaf`](@ref)     |
| `w+` | Yes                  | Yes                       | No                  | [`ZarrDaf`](@ref)     |
| `w`  | Yes                  | Yes                       | Yes                 | [`ZarrDaf`](@ref)     |
"""
struct ZarrDaf <: DafWriter
    name::AbstractString
    internal::Internal
    root::ZGroup
    mode::AbstractString
    path::AbstractString
end

function ZarrDaf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{ZarrDaf, DafReadOnly}
    (is_read_only, create_if_missing, truncate_if_exists) = Formats.parse_mode(mode)
    full_path = abspath(path)

    if isdir(full_path)
        if truncate_if_exists
            rm(full_path; recursive = true)  # NOJET
            mkpath(full_path)
            root = zgroup(full_path)  # NOJET
            create_daf(root)
        else
            root = zopen(full_path, is_read_only ? "r" : "w")  # NOJET
            if !(root isa ZGroup)
                error("not a daf zarr group: $(full_path)")  # UNTESTED
            end
            verify_daf(root, full_path)
        end
    else
        if !create_if_missing
            error("not a zarr directory: $(full_path)")
        end
        mkpath(full_path)
        root = zgroup(full_path)  # NOJET
        create_daf(root)
    end

    if name === nothing && haskey(root.groups, SCALARS)
        scalars_group = root.groups[SCALARS]
        if haskey(scalars_group.arrays, "name")
            name = string(read_scalar_value(scalars_group.arrays["name"]))
        end
    end

    if name === nothing
        name = full_path
    end
    name = unique_name(name)

    daf = ZarrDaf(name, Internal(; is_frozen = is_read_only), root, mode, full_path)
    @debug "Daf: $(brief(daf)) root: $(root)" _group = :daf_repos
    if is_read_only
        return read_only(daf)
    else
        return daf
    end
end

function create_daf(root::ZGroup)::Nothing
    daf_array = zcreate(UInt8, root, DAF_KEY, 2; compressor = Zarr.NoCompressor())
    daf_array[:] = UInt8[MAJOR_VERSION, MINOR_VERSION]  # NOJET

    zgroup(root, SCALARS)
    zgroup(root, AXES)
    zgroup(root, VECTORS)
    zgroup(root, MATRICES)

    return nothing
end

function verify_daf(root::ZGroup, full_path::AbstractString)::Nothing
    if !haskey(root.arrays, DAF_KEY)
        error("not a daf zarr group: $(full_path)")
    end
    version = root.arrays[DAF_KEY][:]
    @assert length(version) == 2
    if version[1] != MAJOR_VERSION || version[2] > MINOR_VERSION
        error(chomp("""
              incompatible format version: $(Int(version[1])).$(Int(version[2]))
              for the daf zarr group: $(full_path)
              the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
              """))
    end
    return nothing
end

function Readers.is_leaf(::ZarrDaf)::Bool
    return true
end

function Readers.is_leaf(::Type{ZarrDaf})::Bool  # FLAKY TESTED
    return true
end

function Readers.complete_path(daf::ZarrDaf)::Maybe{AbstractString}
    return daf.path
end

function Formats.format_description_header(daf::ZarrDaf, indent::AbstractString, lines::Vector{String}, ::Bool)::Nothing
    @assert Formats.has_data_read_lock(daf)
    push!(lines, "$(indent)type: ZarrDaf")
    push!(lines, "$(indent)path: $(daf.path)")
    push!(lines, "$(indent)mode: $(daf.mode)")
    return nothing
end

function scalars_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[SCALARS]
end

function axes_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[AXES]
end

function vectors_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[VECTORS]
end

function matrices_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[MATRICES]
end

function array_path(array::ZArray)::String  # FLAKY TESTED
    return joinpath(array.storage.folder, lstrip(array.path, '/'))
end

function is_writable(daf::ZarrDaf)::Bool
    return daf.mode != "r"
end

function mmap_chunk(  # FLAKY TESTED
    daf::ZarrDaf,
    chunk_path::AbstractString,
    ::Type{Array{T, N}},
    dims::NTuple{N, Int},
)::Array{T, N} where {T, N}
    open(chunk_path, is_writable(daf) ? "r+" : "r") do io
        return Mmap.mmap(io, Array{T, N}, dims)
    end
end

function array_as_vector(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageVector, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        chunk_path = joinpath(array_path(array), "0")
        if isfile(chunk_path)
            vector = mmap_chunk(daf, chunk_path, Vector{T}, (length(array),))
            return (vector, Formats.MappedData)
        end
    end
    return (array[:], Formats.MemoryData)
end

function array_as_matrix(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageMatrix, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        chunk_path = joinpath(array_path(array), "0.0")
        if isfile(chunk_path)
            matrix = mmap_chunk(daf, chunk_path, Matrix{T}, size(array))
            return (matrix, Formats.MappedData)
        end
    end
    return (array[:, :], Formats.MemoryData)  # NOJET
end

function can_mmap(array::ZArray{T})::Bool where {T}  # FLAKY TESTED
    return isbitstype(T) &&
           array.metadata.compressor isa Zarr.NoCompressor &&
           array.metadata.filters === nothing &&
           array.metadata.chunks == size(array) &&
           array.storage isa Zarr.DirectoryStore
end

function Formats.format_has_scalar(daf::ZarrDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(daf)
    return haskey(scalars_group(daf).arrays, name)
end

function read_scalar_value(array::ZArray{T})::StorageScalar where {T}
    return array[1]
end

function Formats.format_get_scalar(daf::ZarrDaf, name::AbstractString)::Tuple{StorageScalar, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    array = scalars_group(daf).arrays[name]
    return (read_scalar_value(array), Formats.MemoryData)
end

function Formats.format_set_scalar!(daf::ZarrDaf, name::AbstractString, value::StorageScalar)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(daf)
    array = zcreate(typeof(value), scalars_group(daf), name, 1; compressor = Zarr.NoCompressor())
    array[1] = value  # NOJET
    return Formats.MemoryData
end

function Formats.format_delete_scalar!(daf::ZarrDaf, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(daf)
    delete_child(scalars_group(daf), name)
    return nothing
end

function Formats.format_scalars_set(daf::ZarrDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    return Set(keys(scalars_group(daf).arrays))
end

function Formats.format_has_axis(daf::ZarrDaf, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(daf)
    return haskey(axes_group(daf).arrays, axis)
end

function Formats.format_add_axis!(
    daf::ZarrDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    axis_array = zcreate(String, axes_group(daf), axis, length(entries); compressor = Zarr.NoCompressor())
    axis_array[:] = String.(entries)  # NOJET

    zgroup(vectors_group(daf), axis)

    axes = keys(axes_group(daf).arrays)
    @assert axis in axes

    axis_matrices = zgroup(matrices_group(daf), axis)
    for other_axis in axes
        if other_axis != axis
            zgroup(axis_matrices, other_axis)
        end
    end

    for other_axis in axes
        zgroup(matrices_group(daf).groups[other_axis], axis)
    end

    return nothing
end

function Formats.format_delete_axis!(daf::ZarrDaf, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(daf)
    delete_child(axes_group(daf), axis)
    delete_child(vectors_group(daf), axis)
    delete_child(matrices_group(daf), axis)

    for (_, other_group) in matrices_group(daf).groups
        if haskey(other_group.groups, axis)
            delete_child(other_group, axis)
        end
    end

    return nothing
end

function Formats.format_axes_set(daf::ZarrDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    return Set(keys(axes_group(daf).arrays))
end

function Formats.format_axis_vector(
    daf::ZarrDaf,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Maybe{Formats.CacheGroup}}
    @assert Formats.has_data_read_lock(daf)
    return (axes_group(daf).arrays[axis][:], Formats.MemoryData)
end

function Formats.format_axis_length(daf::ZarrDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(daf)
    return length(axes_group(daf).arrays[axis])
end

function axis_vectors_group(daf::ZarrDaf, axis::AbstractString)::ZGroup
    return vectors_group(daf).groups[axis]
end

function Formats.format_has_vector(daf::ZarrDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    return haskey(group.arrays, name) || haskey(group.groups, name)
end

function Formats.format_set_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)

    if vector isa StorageScalar
        array = zcreate(typeof(vector), group, name, nelements; compressor = Zarr.NoCompressor())
        array[:] = fill(vector, nelements)  # NOJET
    else
        @assert vector isa AbstractVector
        vector = base_array(vector)
        if issparse(vector)
            write_sparse_vector(group, name, vector)
        elseif eltype(vector) <: AbstractString
            array = zcreate(String, group, name, nelements; compressor = Zarr.NoCompressor())
            array[:] = String.(vector)  # NOJET
        else
            array = zcreate(eltype(vector), group, name, nelements; compressor = Zarr.NoCompressor())
            array[:] = Vector(vector)  # NOJET
        end
    end
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)
    array = zcreate(T, group, name, nelements; compressor = Zarr.NoCompressor())
    array[:] = zeros(T, nelements)
    return array_as_vector(daf, array)
end

function write_sparse_vector(parent::ZGroup, name::AbstractString, vector::AbstractVector)::Nothing
    vector_group = zgroup(parent, name)

    nzind_vector = nzind(vector)
    nzind_array =
        zcreate(eltype(nzind_vector), vector_group, "nzind", length(nzind_vector); compressor = Zarr.NoCompressor())
    nzind_array[:] = nzind_vector

    if eltype(vector) != Bool || !all(nzval(vector))
        nzval_vector = nzval(vector)
        nzval_array =
            zcreate(eltype(nzval_vector), vector_group, "nzval", length(nzval_vector); compressor = Zarr.NoCompressor())
        nzval_array[:] = nzval_vector
    end
    return nothing
end

function Formats.format_get_empty_sparse_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    vector_group = zgroup(group, name)

    nnz_int = Int(nnz)
    nzind_array = zcreate(I, vector_group, "nzind", nnz_int; compressor = Zarr.NoCompressor())
    nzval_array = zcreate(T, vector_group, "nzval", nnz_int; compressor = Zarr.NoCompressor())
    nzind_array[:] = zeros(I, nnz_int)
    nzval_array[:] = zeros(T, nnz_int)

    nzind_vec, _ = array_as_vector(daf, nzind_array)
    nzval_vec, _ = array_as_vector(daf, nzval_array)
    return (nzind_vec, nzval_vec)
end

function Formats.format_filled_empty_sparse_vector!(
    ::ZarrDaf,
    ::AbstractString,
    ::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
)::Maybe{Formats.CacheGroup}
    return Formats.MappedData
end

function Formats.format_delete_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    delete_child(axis_vectors_group(daf, axis), name)
    return nothing
end

function Formats.format_vectors_set(daf::ZarrDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    return union(Set(keys(group.arrays)), Set(keys(group.groups)))
end

function Formats.format_get_vector(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    if haskey(group.arrays, name)
        return array_as_vector(daf, group.arrays[name])
    end

    @assert haskey(group.groups, name)
    vector_group = group.groups[name]
    nelements = Formats.format_axis_length(daf, axis)

    nzind_vector, nzind_cache_group = array_as_vector(daf, vector_group.arrays["nzind"])
    if haskey(vector_group.arrays, "nzval")
        nzval_vector, nzval_cache_group = array_as_vector(daf, vector_group.arrays["nzval"])
    else
        nzval_vector = fill(true, length(nzind_vector))
        nzval_cache_group = Formats.MemoryData
    end

    vector = SparseVector(nelements, nzind_vector, nzval_vector)
    cache_group = if nzind_cache_group == Formats.MappedData && nzval_cache_group == Formats.MappedData
        Formats.MappedData
    else
        Formats.MemoryData
    end
    return (vector, cache_group)
end

function columns_axis_group(daf::ZarrDaf, rows_axis::AbstractString, columns_axis::AbstractString)::ZGroup
    return matrices_group(daf).groups[rows_axis].groups[columns_axis]
end

function Formats.format_has_matrix(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    return haskey(group.arrays, name) || haskey(group.groups, name)
end

function Formats.format_set_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalarBase, StorageMatrix},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    if matrix isa StorageReal
        array = zcreate(typeof(matrix), group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = fill(matrix, nrows, ncols)  # NOJET
    elseif matrix isa AbstractString
        array = zcreate(String, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = fill(String(matrix), nrows, ncols)  # NOJET
    elseif eltype(matrix) <: AbstractString
        array = zcreate(String, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = String.(matrix)  # NOJET
    else
        @assert matrix isa AbstractMatrix
        @assert major_axis(matrix) != Rows
        matrix = base_array(matrix)
        if issparse(matrix)
            write_sparse_matrix(group, name, matrix)
            return nothing
        else
            array = zcreate(eltype(matrix), group, name, nrows, ncols; compressor = Zarr.NoCompressor())
            array[:, :] = Matrix(matrix)  # NOJET
        end
    end
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)
    array = zcreate(T, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
    array[:, :] = zeros(T, nrows, ncols)
    return array_as_matrix(daf, array)
end

function write_sparse_matrix(parent::ZGroup, name::AbstractString, matrix::AbstractMatrix)::Nothing
    matrix_group = zgroup(parent, name)

    colptr_vector = colptr(matrix)
    colptr_array =
        zcreate(eltype(colptr_vector), matrix_group, "colptr", length(colptr_vector); compressor = Zarr.NoCompressor())
    colptr_array[:] = colptr_vector

    rowval_vector = rowval(matrix)
    rowval_array =
        zcreate(eltype(rowval_vector), matrix_group, "rowval", length(rowval_vector); compressor = Zarr.NoCompressor())
    rowval_array[:] = rowval_vector

    if eltype(matrix) != Bool || !all(nzval(matrix))
        nzval_vector = nzval(matrix)
        nzval_array =
            zcreate(eltype(nzval_vector), matrix_group, "nzval", length(nzval_vector); compressor = Zarr.NoCompressor())
        nzval_array[:] = nzval_vector
    end
    return nothing
end

function Formats.format_get_empty_sparse_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)
    matrix_group = zgroup(group, name)

    nnz_int = Int(nnz)
    colptr_array = zcreate(I, matrix_group, "colptr", ncols + 1; compressor = Zarr.NoCompressor())
    rowval_array = zcreate(I, matrix_group, "rowval", nnz_int; compressor = Zarr.NoCompressor())
    nzval_array = zcreate(T, matrix_group, "nzval", nnz_int; compressor = Zarr.NoCompressor())

    colptr_init = fill(I(nnz_int + 1), ncols + 1)
    colptr_init[1] = I(1)
    colptr_array[:] = colptr_init
    rowval_array[:] = ones(I, nnz_int)
    nzval_array[:] = zeros(T, nnz_int)

    colptr_vec, _ = array_as_vector(daf, colptr_array)
    rowval_vec, _ = array_as_vector(daf, rowval_array)
    nzval_vec, _ = array_as_vector(daf, nzval_array)
    return (colptr_vec, rowval_vec, nzval_vec)
end

function Formats.format_filled_empty_sparse_matrix!(
    ::ZarrDaf,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Maybe{Formats.CacheGroup}
    return Formats.MappedData
end

function Formats.format_relayout_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
)::StorageMatrix
    @assert Formats.has_data_write_lock(daf)
    if eltype(matrix) <: AbstractString
        group = columns_axis_group(daf, columns_axis, rows_axis)
        nrows = axis_length(daf, columns_axis)
        ncols = axis_length(daf, rows_axis)
        relayout_matrix = flipped(matrix)
        array = zcreate(String, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = String.(relayout_matrix)
        return relayout_matrix
    end
    if issparse(matrix)
        sparse_colptr, sparse_rowval, sparse_nzval = Formats.format_get_empty_sparse_matrix!(
            daf,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(colptr(matrix)),
        )
        sparse_colptr .= length(sparse_nzval) + 1
        sparse_colptr[1] = 1
        relayout_matrix = SparseMatrixCSC(
            axis_length(daf, columns_axis),
            axis_length(daf, rows_axis),
            sparse_colptr,
            sparse_rowval,
            sparse_nzval,
        )
        relayout!(flip(relayout_matrix), matrix)
        return relayout_matrix
    end
    relayout_matrix, _ = Formats.format_get_empty_dense_matrix!(daf, columns_axis, rows_axis, name, eltype(matrix))
    relayout!(flip(relayout_matrix), matrix)
    return relayout_matrix
end

function Formats.format_delete_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    delete_child(columns_axis_group(daf, rows_axis, columns_axis), name)
    return nothing
end

function Formats.format_matrices_set(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    return union(Set(keys(group.arrays)), Set(keys(group.groups)))
end

function Formats.format_get_matrix(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    if haskey(group.arrays, name)
        return array_as_matrix(daf, group.arrays[name])
    end

    @assert haskey(group.groups, name)
    matrix_group = group.groups[name]
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    colptr_vector, colptr_cache_group = array_as_vector(daf, matrix_group.arrays["colptr"])
    rowval_vector, rowval_cache_group = array_as_vector(daf, matrix_group.arrays["rowval"])
    if haskey(matrix_group.arrays, "nzval")
        nzval_vector, nzval_cache_group = array_as_vector(daf, matrix_group.arrays["nzval"])
    else
        nzval_vector = fill(true, length(rowval_vector))
        nzval_cache_group = Formats.MemoryData
    end

    matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
    cache_group =
        if colptr_cache_group == Formats.MappedData &&
           rowval_cache_group == Formats.MappedData &&
           nzval_cache_group == Formats.MappedData
            Formats.MappedData
        else
            Formats.MemoryData
        end
    return (matrix, cache_group)
end

function delete_child(parent::ZGroup, name::AbstractString)::Nothing
    if haskey(parent.arrays, name)
        delete!(parent.arrays, name)
    elseif haskey(parent.groups, name)
        delete!(parent.groups, name)
    end
    if parent.storage isa Zarr.DirectoryStore
        child_path = joinpath(parent.storage.folder, lstrip(parent.path, '/'), name)
        if ispath(child_path)
            rm(child_path; recursive = true)
        end
    end
    return nothing
end

const REORDER_BACKUP_DIR = ".reorder.backup"

function reorder_backup_root(daf::ZarrDaf)::String
    return "$(daf.path)/$(REORDER_BACKUP_DIR)"
end

function recursive_hardlink(src::AbstractString, dst::AbstractString)::Nothing
    mkpath(dst)
    for (root, _, files) in walkdir(src)
        rel = relpath(root, src)
        out_dir = rel == "." ? dst : joinpath(dst, rel)
        mkpath(out_dir)
        for f in files
            hardlink(joinpath(root, f), joinpath(out_dir, f))
        end
    end
    return nothing
end

function child_zarr_path(group::ZGroup, name::AbstractString)::String  # FLAKY TESTED
    return isempty(group.path) ? String(name) : rstrip(group.path, '/') * '/' * String(name)
end

function reopen_zgroup_child!(group::ZGroup, name::AbstractString)::Nothing
    child = Zarr.zopen_noerr(group.storage, "w"; path = child_zarr_path(group, name), fill_as_missing = false)  # NOJET
    if child isa ZArray
        group.arrays[name] = child
    elseif child isa ZGroup
        group.groups[name] = child
    end
    return nothing
end

function Reorder.format_lock_reorder!(daf::ZarrDaf, ::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    @assert !isdir(backup_root)
    mkdir(backup_root)
    return nothing
end

function Reorder.format_has_reorder_lock(daf::ZarrDaf)::Bool
    @assert Formats.has_data_write_lock(daf)
    return isdir(reorder_backup_root(daf))
end

function Reorder.format_backup_reorder!(daf::ZarrDaf, plan::Reorder.FormatReorderPlan)::Nothing
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    @assert isdir(backup_root)

    for (axis, _) in plan.planned_axes
        src = "$(daf.path)/$(AXES)/$(axis)"
        if isdir(src)
            recursive_hardlink(src, "$(backup_root)/$(AXES)/$(axis)")
        end
    end

    for planned in plan.planned_vectors
        src = "$(daf.path)/$(VECTORS)/$(planned.axis)/$(planned.name)"
        if isdir(src)
            recursive_hardlink(src, "$(backup_root)/$(VECTORS)/$(planned.axis)/$(planned.name)")
        end
    end

    for planned in plan.planned_matrices
        src = "$(daf.path)/$(MATRICES)/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)"
        if isdir(src)
            recursive_hardlink(
                src,
                "$(backup_root)/$(MATRICES)/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)",
            )
        end
    end

    return nothing
end

function Reorder.format_replace_reorder!(
    daf::ZarrDaf,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    crash_counter::Maybe{Ref{Int}},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    @assert isdir(reorder_backup_root(daf))

    for (axis, planned_axis) in plan.planned_axes
        if Formats.format_has_axis(daf, axis; for_change = false)
            delete_child(axes_group(daf), axis)
            axis_array = zcreate(
                String,
                axes_group(daf),
                axis,
                length(planned_axis.new_entries);
                compressor = Zarr.NoCompressor(),
            )
            axis_array[:] = String.(planned_axis.new_entries)  # NOJET
        end
    end

    for planned in plan.planned_vectors
        replace_reorder_vector(daf, planned, plan, replacement_progress)
        Reorder.tick_crash_counter!(crash_counter)
    end

    for planned in plan.planned_matrices
        replace_reorder_matrix(daf, planned, plan, replacement_progress)
        Reorder.tick_crash_counter!(crash_counter)
    end

    return nothing
end

function replace_reorder_vector(
    daf::ZarrDaf,
    planned::Reorder.PlannedVector,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_vector, _ = Formats.format_get_vector(daf, planned.axis, planned.name)
    planned_axis = plan.planned_axes[planned.axis]
    group = axis_vectors_group(daf, planned.axis)

    if eltype(source_vector) <: AbstractString
        permuted = Vector{String}(undef, length(source_vector))
        permute_vector!(;
            destination = permuted,
            source = source_vector,
            permutation = planned_axis.permutation,
            progress = replacement_progress,
        )
        delete_child(group, planned.name)
        array = zcreate(String, group, planned.name, length(permuted); compressor = Zarr.NoCompressor())
        array[:] = permuted  # NOJET
    elseif source_vector isa SparseVector
        T = eltype(source_vector)
        I = eltype(SparseArrays.nonzeroinds(source_vector))
        source_length = length(source_vector)
        source_nnz = nnz(source_vector)
        source_nzind = copy(SparseArrays.nonzeroinds(source_vector))
        source_nzval = copy(nonzeros(source_vector))

        delete_child(group, planned.name)
        destination_nzind, destination_nzval =
            Formats.format_get_empty_sparse_vector!(daf, planned.axis, planned.name, T, source_nnz, I)
        permute_sparse_vector_buffers!(;
            destination_nzind,
            destination_nzval,
            source_length,
            source_nzind,
            source_nzval,
            inverse_permutation = planned_axis.inverse_permutation,
            progress = replacement_progress,
        )
    else
        T = eltype(source_vector)
        materialized = Vector{T}(source_vector)
        delete_child(group, planned.name)
        destination, _ = Formats.format_get_empty_dense_vector!(daf, planned.axis, planned.name, T)
        permute_vector!(;
            destination,
            source = materialized,
            permutation = planned_axis.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

function replace_reorder_matrix(
    daf::ZarrDaf,
    planned::Reorder.PlannedMatrix,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_matrix, _ = Formats.format_get_matrix(daf, planned.rows_axis, planned.columns_axis, planned.name)
    planned_rows = get(plan.planned_axes, planned.rows_axis, nothing)
    planned_columns = get(plan.planned_axes, planned.columns_axis, nothing)
    @assert planned_rows !== nothing || planned_columns !== nothing

    group = columns_axis_group(daf, planned.rows_axis, planned.columns_axis)
    nrows, ncols = size(source_matrix)

    if eltype(source_matrix) <: AbstractString
        permuted = Matrix{String}(undef, nrows, ncols)
        permute_matrix_into!(permuted, source_matrix, planned_rows, planned_columns, replacement_progress)
        delete_child(group, planned.name)
        array = zcreate(String, group, planned.name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = permuted  # NOJET
    elseif source_matrix isa SparseMatrixCSC
        T = eltype(source_matrix)
        I = eltype(source_matrix.colptr)
        source_nnz_val = nnz(source_matrix)
        src_colptr = copy(source_matrix.colptr)
        src_rowval = copy(source_matrix.rowval)
        src_nzval = copy(source_matrix.nzval)

        delete_child(group, planned.name)
        destination_colptr, destination_rowval, destination_nzval = Formats.format_get_empty_sparse_matrix!(
            daf,
            planned.rows_axis,
            planned.columns_axis,
            planned.name,
            T,
            source_nnz_val,
            I,
        )
        if planned_rows !== nothing && planned_columns !== nothing
            permute_sparse_matrix_both_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = src_colptr,
                source_rowval = src_rowval,
                source_nzval = src_nzval,
                inverse_rows_permutation = planned_rows.inverse_permutation,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        elseif planned_rows !== nothing
            permute_sparse_matrix_rows_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = src_colptr,
                source_rowval = src_rowval,
                source_nzval = src_nzval,
                inverse_rows_permutation = planned_rows.inverse_permutation,
                progress = replacement_progress,
            )
        else
            permute_sparse_matrix_columns_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = src_colptr,
                source_rowval = src_rowval,
                source_nzval = src_nzval,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        end
    else
        T = eltype(source_matrix)
        materialized = Matrix{T}(source_matrix)
        delete_child(group, planned.name)
        destination, _ =
            Formats.format_get_empty_dense_matrix!(daf, planned.rows_axis, planned.columns_axis, planned.name, T)
        permute_matrix_into!(destination, materialized, planned_rows, planned_columns, replacement_progress)
    end
    return nothing
end

function permute_matrix_into!(
    destination::AbstractMatrix,
    source::AbstractMatrix,
    planned_rows::Maybe{Reorder.PlannedAxis},
    planned_columns::Maybe{Reorder.PlannedAxis},
    replacement_progress::Maybe{Progress},
)::Nothing
    if planned_rows !== nothing && planned_columns !== nothing
        permute_dense_matrix_both!(;
            destination,
            source,
            rows_permutation = planned_rows.permutation,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    elseif planned_rows !== nothing
        permute_dense_matrix_rows!(;
            destination,
            source,
            rows_permutation = planned_rows.permutation,
            progress = replacement_progress,
        )
    else
        permute_dense_matrix_columns!(;
            destination,
            source,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

function Reorder.format_cleanup_reorder!(daf::ZarrDaf)::Nothing
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    @assert isdir(backup_root)
    rm(backup_root; force = true, recursive = true)
    return nothing
end

function Reorder.format_reset_reorder!(daf::ZarrDaf)::Bool
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    if !isdir(backup_root)
        return false
    end

    axes_backup = "$(backup_root)/$(AXES)"
    if isdir(axes_backup)
        for axis in readdir(axes_backup)
            delete_child(axes_group(daf), axis)
            recursive_hardlink("$(axes_backup)/$(axis)", "$(daf.path)/$(AXES)/$(axis)")
            reopen_zgroup_child!(axes_group(daf), axis)
        end
    end

    vectors_backup = "$(backup_root)/$(VECTORS)"
    if isdir(vectors_backup)
        for axis in readdir(vectors_backup)
            parent = axis_vectors_group(daf, axis)
            axis_backup = "$(vectors_backup)/$(axis)"
            for name in readdir(axis_backup)
                delete_child(parent, name)
                recursive_hardlink("$(axis_backup)/$(name)", "$(daf.path)/$(VECTORS)/$(axis)/$(name)")
                reopen_zgroup_child!(parent, name)
            end
        end
    end

    matrices_backup = "$(backup_root)/$(MATRICES)"
    if isdir(matrices_backup)
        for rows_axis in readdir(matrices_backup)
            rows_backup = "$(matrices_backup)/$(rows_axis)"
            for columns_axis in readdir(rows_backup)
                parent = columns_axis_group(daf, rows_axis, columns_axis)
                cols_backup = "$(rows_backup)/$(columns_axis)"
                for name in readdir(cols_backup)
                    delete_child(parent, name)
                    recursive_hardlink(
                        "$(cols_backup)/$(name)",
                        "$(daf.path)/$(MATRICES)/$(rows_axis)/$(columns_axis)/$(name)",
                    )
                    reopen_zgroup_child!(parent, name)
                end
            end
        end
    end

    rm(backup_root; force = true, recursive = true)
    return true
end

function TanayLabUtilities.Brief.brief(value::ZarrDaf; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ZarrDaf $(name)"
end

end  # module
