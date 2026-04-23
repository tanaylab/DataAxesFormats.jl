"""
A `Daf` storage format in an HDF5 disk file. This is the "native" way to store `Daf` data in HDF5 files, which can be
used to contain "anything", as HDF5 is essentially "a filesystem inside a file", with "groups" instead of directories
and "datasets" instead of files. Therefore HDF5 is very generic, and there are various specific formats which use
specific internal structure to hold some data in it - for example, `h5ad` files have a specific internal structure for
representing [AnnData](https://pypi.org/project/anndata/) objects. To represent `Daf` data in HDF5 storage, we use the
following internal structure (which

is **not** compatible with `h5ad`):

  - The HDF5 file may contain `Daf` data directly in the root group, in which case, it is restricted to holding just a
    single `Daf` data set. When using such a file, you automatically access the single `Daf` data set contained in it.
    By convention such files are given a `.h5df` suffix.

  - Alternatively, the HDF5 file may contain `Daf` data inside some arbitrary group, in which case, there's no
    restriction on the content of other groups in the file. Such groups may contain other `Daf` data (allowing for
    multiple `Daf` data sets in a single file), and/or non-`Daf` data. When using such a file, you need to specify the
    name of the group that contains the `Daf` data set you are interested it. By convention, at least if such files
    contain "mostly" (or only) `Daf` data sets, they are given a `.h5dfs` suffix, and are accompanied by some
    documentation describing the top-level groups in the file.

  - Under the `Daf` data group, there are 4 sub-groups: `scalars`, `axes`, `vectors` and `matrices` and a `daf` dataset.

  - To future-proof the format, the `daf` dataset will contain a vector of two integers, the first acting as the major
    version number and the second as the minor version number, using [semantic versioning](https://semver.org/). This
    makes it easy to test whether some group in an HDF5 file does/n't contain `Daf` data, and which version of the
    internal structure it is using. Currently the only defined version is `[1,0]`.

  - The `scalars` group contains scalar properties, each as its own "dataset". The only supported scalar data types
    are these included in [`StorageScalar`](@ref). If you **really** need something else, serialize it to JSON and store
    the result as a string scalar. This should be **extremely** rare.

  - The `axes` group contains a "dataset" per axis, which contains a vector of strings (the names of the axis entries).

  - The `vectors` group contains a sub-group for each axis. Each such sub-group contains vector properties. If the
    vector is dense, it is stored directly as a "dataset". Otherwise, it is stored as a group containing two vector
    "datasets": `nzind` is containing the indices of the non-zero values, and `nzval` containing the actual values. See
    Julia's `SparseVector` implementation for details. The only supported vector element types are these included in
    [`StorageScalar`](@ref), same as [`StorageVector`](@ref).

    If the data type is `Bool` then the data vector is typically all-`true` values; in this case we simply skip storing
    it.

    We switch to using this sparse format for sufficiently sparse string data (where the zero value is the empty
    string). This isn't supported by `SparseVector` because "reasons" so we load it into a dense vector. In this case we
    name the values vector `nztxt`.

  - The `matrices` group contains a sub-group for each rows axis, which contains a sub-group for each columns axis. Each
    such sub-sub group contains matrix properties. If the matrix is dense, it is stored directly as a "dataset" (in
    column-major layout). Otherwise, it is stored as a group containing three vector "datasets": `colptr` containing the
    indices of the rows of each column in `rowval`, `rowval` containing the indices of the non-zero rows of the columns,
    and `nzval` containing the non-zero matrix entry values. See Julia's `SparseMatrixCSC` implementation for details.
    The only supported matrix element types are these included in [`StorageReal`](@ref) - this explicitly excludes
    matrices of strings, same as [`StorageMatrix`](@ref).

    If the data type is `Bool` then the data vector is typically all-`true` values; in this case we simply skip storing
    it.

    We switch to using this sparse format for sufficiently sparse string data (where the zero value is the empty
    string). This isn't supported by `SparseMatrixCSC` because "reasons" so we load it into a dense matrix. In this case
    we name the values vector `nztxt`.

  - All vectors and matrices are stored in a contiguous way in the file, which allows us to efficiently memory-map
    them.

That's all there is to it. Due to the above restrictions on types and layout, the metadata provided by HDF5 for each
"dataset" is sufficient to fully describe the data, and one should be able to directly access it using any HDF5 API in
any programming language, if needed. Typically, however, it is easiest to simply use the Julia `Daf` package to access
the data.

Example HDF5 structure:

    example-daf-dataset-root-group/
    ├─ daf
    ├─ scalars/
    │  └─ version
    ├─ axes/
    │  ├─ cell
    │  └─ gene
    ├─ vectors/
    │  ├─ cell/
    │  │  └─ batch
    │  └─ gene/
    │     └─ is_marker
    └─ matrices/
       ├─ cell/
       │   ├─ cell/
       │   └─ gene/
       │      └─ UMIs/
       │         ├─ colptr
       │         ├─ rowval
       │         └─ nzval
       └─ gene/
          ├─ cell/
          └─ gene/

!!! note

    When creating an HDF5 file to contain `Daf` data, you should specify
    `;fapl=HDF5.FileAccessProperties(;alignment=(1,8))`. This ensures all the memory buffers are properly aligned for
    efficient access. Otherwise, memory mapping will be **much** less efficient. A warning is therefore generated
    whenever you try to access `Daf` data stored in an HDF5 file which does not enforce proper alignment.

!!! note

    Deleting data from an HDF5 file does not reuse the abandoned storage. In general if you want to reclaim that
    storage, you will need to repack the file, which will invalidate any memory-mapped buffers created for it.
    Therefore, if you delete data (e.g. using [`delete_vector!`](@ref)), you should eventually abandon the `H5df`
    object, repack the HDF5 file, then create a new `H5df` object to access the repacked data.

!!! note

    `HDF5.jl` writes dataset dimensions in the reverse of the Julia matrix shape so that the raw bytes match Julia's
    native column-major layout. A `Daf` matrix whose `(rows_axis, columns_axis)` are `(cell, gene)` (a Julia
    `(n_cells, n_genes)` matrix) is therefore written with an HDF5 dataspace of shape `[n_genes, n_cells]`. A client
    using a different HDF5 binding — most notably Python's `h5py` — will see `dset.shape == (n_genes, n_cells)` and
    load it into a C-contiguous NumPy array of that shape, which is the **transpose** of the Julia view. The bytes
    on disk are identical; only the shape labels are swapped. To obtain the `Daf`-canonical `(cell, gene)`
    orientation in Python, apply `.T` (a zero-copy view) to the loaded array. This affects only dense matrices
    (and the `colptr`/`rowval`/`nzval` of sparse matrices are 1D vectors, unaffected); 1D axis-entry arrays and
    vector properties have the same shape in both languages.

!!! note

    The code here assumes the HDF5 data obeys all the above conventions and restrictions (that said, code will be able
    to access vectors and matrices stored in unaligned, chunked and/or compressed formats, but this will be much less
    efficient). As long as you only create and access `Daf` data in HDF5 files using [`H5df`](@ref), then the code will
    work as expected (assuming no bugs). However, if you do this in some other way (e.g., directly using some HDF5 API
    in some arbitrary programming language), and the result is invalid, then the code here may fails with "less than
    friendly" error messages.
"""
module H5dfFormat

export H5df

using ..Formats
using ..ReadOnly
using ..Readers
using ..StorageTypes
using ..Writers
using HDF5
using SparseArrays

import ..Formats
import ..Formats.Internal
import ..Readers.base_array
import ..Reorder
using ProgressMeter
using TanayLabUtilities

"""
The specific major version of the [`H5df`](@ref) format that is supported by this code (`1`). The code will refuse to
access data that is stored in a different major format.
"""
MAJOR_VERSION::UInt8 = 1

"""
The maximal minor version of the [`H5df`](@ref) format that is supported by this code (`0`). The code will refuse to
access data that is stored with the expected major version (`1`), but that uses a higher minor version.

!!! note

    Modifying data that is stored with a lower minor version number **may** increase its minor version number.
"""
MINOR_VERSION::UInt8 = 0

"""
    H5df(
        root::Union{AbstractString, HDF5.File, HDF5.Group},
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing]
    )

Storage in a HDF5 file.

The `root` can be the path of an HDF5 file, which will be opened with the specified `mode`, or an opened HDF5 file, in
which cases the `Daf` data set will be stored directly in the root of the file (by convention, using a `.h5df` file name
suffix). Alternatively, the `root` can be a group inside an HDF5 file, which allows to store multiple `Daf` data sets
inside the same HDF5 file (by convention, using a `.h5dfs` file name suffix).

As a shorthand, you can also specify a `root` which is the path of a HDF5 file with a `.h5dfs` suffix, followed by `#`
and the path of the group in the file.

!!! note

    If you create a directory whose name is `something.h5dfs#` and place `Daf` HDF5 files in it, this scheme will fail.
    So don't.

When opening an existing data set, if `name` is not specified, and there exists a "name" scalar property, it is used as
the name. Otherwise, the path of the HDF5 file will be used as the name, followed by `#` and the internal path of the
group (if any).

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type         |
|:---- |:-------------------- |:------------------------- |:------------------- |:--------------------- |
| `r`  | No                   | No                        | No                  | [`DafReadOnly`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`H5df`](@ref)        |
| `w+` | Yes                  | Yes                       | No                  | [`H5df`](@ref)        |
| `w`  | Yes                  | Yes                       | Yes                 | [`H5df`](@ref)        |

If the `root` is a path followed by `#` and a group, then `w` mode will *not* truncate the whole file if it exists;
instead, it will only truncate the group.

!!! note

    If specifying a path (string) `root`, when calling `h5open`, the file alignment of created files is set to `(1, 8)`
    to maximize efficiency of mapped vectors and matrices, and the `w+` mode is converted to `cw`.

!!! note

    When several [`H5df`](@ref) instances in the same process share a path (typically different `#/group` sub-dafs of
    the same `.h5dfs` file), they also share a single underlying `HDF5.File` handle and a single `data_lock`, so that
    concurrent calls serialize correctly (`libhdf5` is not thread-safe by default). The first such open determines the
    handle's writability: later opens of the same path that request write access will raise an error if the first open
    was read-only. Release the read-only handle first, or open the writable sub-daf first.
"""
struct H5df <: DafWriter
    name::AbstractString
    internal::Internal
    root::Union{HDF5.File, HDF5.Group}
    mode::AbstractString
    path::Maybe{AbstractString}
end

# Weak-cache entry pairing an open HDF5 file with the data_lock shared by every
# sub-H5df of that file. libhdf5 is not thread-safe by default, so concurrent
# calls on the same file handle across sub-dafs must serialize on this lock.
# `is_writable` records the mode the file was opened in, so that a later sub-H5df
# opening the same path in an incompatible mode can be rejected cleanly.
mutable struct SharedH5dfHandle
    file::HDF5.File
    data_lock::ExtendedReadWriteLock
    is_writable::Bool
    function SharedH5dfHandle(file::HDF5.File, data_lock::ExtendedReadWriteLock, is_writable::Bool)  # FLAKY TESTED
        return new(file, data_lock, is_writable)
    end
end

function H5df(
    root::Union{AbstractString, HDF5.File, HDF5.Group},
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{H5df, DafReadOnly}
    (is_read_only, create_if_missing, truncate_if_exists) = Formats.parse_mode(mode)

    full_path = nothing
    shared_handle::Maybe{SharedH5dfHandle} = nothing
    if root isa AbstractString
        parts = split(root, ".h5dfs#/")
        if length(parts) == 1
            group = nothing
            full_path = abspath(root)
        else
            @assert length(parts) == 2 "can't parse as <file-path>.h5dfs#/<group-path>: $(root)"
            root, group = parts
            if isempty(group)
                error("empty group name after '#/' in H5df path: $(parts[1]).h5dfs#/")
            end
            root *= ".h5dfs"
            full_path = abspath(root) * "#/" * group
        end

        key = (:daf, :hdf5)
        if !truncate_if_exists
            purge = false
        elseif group === nothing
            purge = true
        else
            mode = "w+"
            purge = false
        end

        shared_handle = get_through_global_weak_cache(abspath(root), key; purge) do _
            file = h5open(root, mode == "w+" ? "cw" : mode; fapl = HDF5.FileAccessProperties(; alignment = (1, 8)))  # NOJET
            return SharedH5dfHandle(file, ExtendedReadWriteLock(), !is_read_only)
        end
        if !is_read_only && !shared_handle.is_writable
            error(  # UNTESTED
                "HDF5 file is already open read-only in this process; " *
                "release the existing handle before opening it for write: $(abspath(root))",
            )
        end
        root = shared_handle.file

        if group !== nothing
            if haskey(root, group)
                if truncate_if_exists
                    delete_object(root, group)
                    create_group(root, group)  # NOJET
                end
            else
                if create_if_missing  # UNTESTED
                    create_group(root, group)  # UNTESTED
                end
            end
            root = root[group]
        end
    end

    verify_alignment(root)

    if haskey(root, "daf")  # NOJET
        if truncate_if_exists
            delete_content(root)
            create_daf(root)
        else
            verify_daf(root)
        end
    else
        if create_if_missing
            delete_content(root)
            create_daf(root)
        else
            error("not a daf data set: $(root)")
        end
    end

    if name === nothing && haskey(root, "scalars")
        scalars_group = root["scalars"]
        @assert scalars_group isa HDF5.Group
        if haskey(scalars_group, "name")
            name_dataset = scalars_group["name"]
            @assert name_dataset isa HDF5.Dataset
            name = string(read(name_dataset))
        end
    end

    if root isa HDF5.Group
        if name === nothing
            name = "$(root.file.filename)#$(HDF5.name(root))"
        end
        if full_path === nothing
            full_path = "$(abspath(root.file.filename))#$(HDF5.name(root))"
        end
    else
        @assert root isa HDF5.File
        if name === nothing
            name = root.filename
        end
        if full_path === nothing
            full_path = abspath(root.filename)
        end
    end
    name = unique_name(name)
    @assert full_path !== nothing

    internal = if shared_handle === nothing
        Internal(; is_frozen = is_read_only)
    else
        Internal(; is_frozen = is_read_only, data_lock = shared_handle.data_lock, shared_resource = shared_handle)
    end
    h5df = H5df(name, internal, root, mode, full_path)
    @debug "Daf: $(brief(h5df)) root: $(root)" _group = :daf_repos
    if is_read_only
        return read_only(h5df)
    else
        return h5df
    end
end

function verify_alignment(root::HDF5.Group)::Nothing # UNTESTED
    return verify_alignment(root.file)
end

function verify_alignment(root::HDF5.File)::Nothing
    file_access_properties = HDF5.get_access_properties(root)  # NOJET
    alignment = file_access_properties.alignment
    if alignment[1] > 1 || alignment[2] != 8
        @warn """
              unsafe HDF5 file alignment for Daf: ($(Int(alignment[1])), $(Int(alignment[2])))
              the safe HDF5 file alignment is: (1, 8)
              note that unaligned data is inefficient,
              and will break the empty_* functions;
              to force the alignment, create the file using:
              h5open(...;fapl=HDF5.FileAccessProperties(;alignment=(1,8))
              """
    end
end

function create_daf(root::Union{HDF5.File, HDF5.Group})::Nothing
    root["daf"] = [MAJOR_VERSION, MINOR_VERSION]  # NOJET
    scalars_group = create_group(root, "scalars")
    axes_group = create_group(root, "axes")
    vectors_group = create_group(root, "vectors")
    matrices_group = create_group(root, "matrices")

    close(scalars_group)
    close(axes_group)
    close(vectors_group)
    close(matrices_group)

    return nothing
end

function verify_daf(root::Union{HDF5.File, HDF5.Group})::Nothing
    format_dataset = root["daf"]
    @assert format_dataset isa HDF5.Dataset
    format_version = read(format_dataset)  # NOJET
    @assert length(format_version) == 2
    @assert eltype(format_version) <: Unsigned
    if format_version[1] != MAJOR_VERSION || format_version[2] > MINOR_VERSION
        error(chomp("""
              incompatible format version: $(format_version[1]).$(format_version[2])
              for the daf data: $(root)
              the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
              """))
    end
end

function delete_content(root::Union{HDF5.File, HDF5.Group})::Nothing
    attribute_names = collect(keys(HDF5.attributes(root)))  # NOJET
    for attribute_name in attribute_names
        HDF5.delete_attribute(root, attribute_name)
    end
    object_names = collect(keys(root))
    for object_name in object_names
        HDF5.delete_object(root, object_name)
    end
end

function Readers.is_leaf(::H5df)::Bool  # FLAKY TESTED
    return true
end

function Readers.is_leaf(::Type{H5df})::Bool  # FLAKY TESTED
    return true
end

function Formats.format_has_scalar(h5df::H5df, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    return haskey(scalars_group, name)
end

function Formats.format_set_scalar!(h5df::H5df, name::AbstractString, value::StorageScalar)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    scalars_group[name] = value
    return Formats.MemoryData
end

function Formats.format_delete_scalar!(h5df::H5df, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    delete_object(scalars_group, name)
    return nothing
end

function Formats.format_get_scalar(h5df::H5df, name::AbstractString)::Tuple{StorageScalar, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    scalars_dataset = scalars_group[name]
    @assert scalars_dataset isa HDF5.Dataset
    return (read(scalars_dataset), Formats.MemoryData)
end

function Formats.format_scalars_set(h5df::H5df)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(h5df)

    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group

    return flame_timed("H5df.keys_as_set") do
        return Set(keys(scalars_group))
    end
end

function Formats.format_has_axis(h5df::H5df, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(h5df)
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group
    return haskey(axes_group, axis)
end

function Formats.format_add_axis!(h5df::H5df, axis::AbstractString, entries::AbstractVector{<:AbstractString})::Nothing
    @assert Formats.has_data_write_lock(h5df)
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    flame_timed("H5df.create_axis_vector") do
        axis_dataset = create_dataset(axes_group, axis, String, (length(entries)))
        axis_dataset[:] = entries
        return close(axis_dataset)
    end

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group
    axis_vectors_group = create_group(vectors_group, axis)
    close(axis_vectors_group)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    axes = flame_timed("H5df.keys_as_set") do
        return Set(keys(axes_group))
    end
    @assert axis in axes

    axis_matrices_group = create_group(matrices_group, axis)

    for other_axis in axes
        if other_axis != axis
            other_axis_matrices_group = create_group(axis_matrices_group, other_axis)
            close(other_axis_matrices_group)
        end
    end

    close(axis_matrices_group)

    for other_axis in axes
        matrix_axis_group = matrices_group[other_axis]
        @assert matrix_axis_group isa HDF5.Group
        axis_matrices_group = create_group(matrix_axis_group, axis)
        close(axis_matrices_group)
    end

    return nothing
end

function Formats.format_delete_axis!(h5df::H5df, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group
    delete_object(axes_group, axis)

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group
    delete_object(vectors_group, axis)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group
    delete_object(matrices_group, axis)

    for other_axis in keys(matrices_group)
        other_axis_group = matrices_group[other_axis]
        @assert other_axis_group isa HDF5.Group
        delete_object(other_axis_group, axis)
    end

    return nothing
end

function Formats.format_axes_set(h5df::H5df)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(h5df)

    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    return flame_timed("H5df.keys_as_set") do
        return Set(keys(axes_group))
    end
end

function Formats.format_axis_vector(
    h5df::H5df,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(h5df)

    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    axis_dataset = axes_group[axis]
    @assert axis_dataset isa HDF5.Dataset
    return dataset_as_vector(axis_dataset)
end

function Formats.format_axis_length(h5df::H5df, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(h5df)
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    axis_dataset = axes_group[axis]
    @assert axis_dataset isa HDF5.Dataset
    return length(axis_dataset)
end

function Formats.format_has_vector(h5df::H5df, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(h5df)
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    if !haskey(vectors_group, axis)
        return false  # UNTESTED
    end

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    return haskey(axis_vectors_group, name)
end

function Formats.format_set_vector!(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    if vector isa StorageScalar
        flame_timed("H5df.fill_real_vector") do
            vector_dataset =
                create_dataset(axis_vectors_group, name, typeof(vector), (Formats.format_axis_length(h5df, axis),))
            vector_dataset[:] = vector  # NOJET
            return close(vector_dataset)
        end

    else
        @assert vector isa AbstractVector

        vector = base_array(vector)

        if issparse(vector)
            flame_timed("H5df.write_sparse_vector") do
                @assert vector isa AbstractVector
                vector_group = create_group(axis_vectors_group, name)
                vector_group["nzind"] = nzind(vector)  # NOJET
                if eltype(vector) != Bool || !all(nzval(vector))
                    if eltype(vector) <: AbstractString
                        vector_group["nzval"] = String.(nzval(vector))  # NOJET # UNTESTED
                    else
                        vector_group["nzval"] = nzval(vector)
                    end
                end
                return close(vector_group)
            end

        elseif eltype(vector) <: AbstractString
            write_string_vector(axis_vectors_group, name, vector)

        else
            flame_timed("H5df.write_dense_vector") do
                nice_vector = nothing
                try
                    base = pointer(vector)
                    nice_vector = Base.unsafe_wrap(Array, base, size(vector))
                catch
                    nice_vector = Vector(vector)  # NOJET # UNTESTED
                end
                return axis_vectors_group[name] = nice_vector  # NOJET
            end
        end
    end

    return nothing
end

function write_string_vector( # UNTESTED
    axis_vectors_group::HDF5.Group,
    name::AbstractString,
    vector::AbstractVector{<:AbstractString},
)::Nothing
    flame_timed("H5df.write_string_vector") do
        n_empty = 0
        nonempty_size = 0
        for value in vector
            value_size = length(value)
            if value_size > 0
                nonempty_size += value_size
            else
                n_empty += 1
            end
        end

        n_values = length(vector)
        n_nonempty = n_values - n_empty
        indtype = indtype_for_size(n_values)

        dense_size = nonempty_size + length(vector)
        sparse_size = nonempty_size + n_nonempty * (1 + sizeof(indtype))

        if sparse_size <= dense_size * 0.75
            nzind_vector = Vector{indtype}(undef, n_nonempty)
            nztxt_vector = Vector{String}(undef, n_nonempty)
            position = 1
            for (index, value) in enumerate(vector)
                if length(value) > 0
                    nzind_vector[position] = index
                    nztxt_vector[position] = String(value)
                    position += 1
                end
            end
            @assert position == n_nonempty + 1

            vector_group = create_group(axis_vectors_group, name)
            vector_group["nzind"] = nzind_vector  # NOJET
            vector_group["nztxt"] = nztxt_vector
            close(vector_group)

        else
            nice_vector = String.(vector)
            axis_vectors_group[name] = nice_vector  # NOJET
        end
    end

    return nothing
end

function Formats.format_get_empty_dense_vector!(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(h5df)
    size = Formats.format_axis_length(h5df, axis)
    return create_empty_dense_vector_in(h5df.root, axis, name, eltype, size)
end

function create_empty_dense_vector_in(
    daf_root::Union{HDF5.File, HDF5.Group},
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    size::Integer,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    vectors_group = daf_root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    local vector_dataset
    flame_timed("H5df.create_empty_dense_vector") do
        vector_dataset = create_dataset(axis_vectors_group, name, T, (size,))
        @assert vector_dataset isa HDF5.Dataset
        return vector_dataset[:] = 0
    end

    vector, cache_group = dataset_as_vector(vector_dataset)
    close(vector_dataset)
    return (vector, cache_group)
end

function Formats.format_get_empty_sparse_vector!(  # FLAKY TESTED
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(h5df)
    return create_empty_sparse_vector_in(h5df.root, axis, name, eltype, nnz, indtype)
end

function create_empty_sparse_vector_in(
    daf_root::Union{HDF5.File, HDF5.Group},
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    vectors_group = daf_root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group
    vector_group = create_group(axis_vectors_group, name)

    local nzind_dataset
    local nzval_dataset
    flame_timed("H5df.create_empty_sparse_vector") do
        nzind_dataset = create_dataset(vector_group, "nzind", I, (nnz,))
        nzval_dataset = create_dataset(vector_group, "nzval", T, (nnz,))

        @assert nzind_dataset isa HDF5.Dataset
        @assert nzval_dataset isa HDF5.Dataset

        nzind_dataset[:] = 0
        return nzval_dataset[:] = 0
    end

    nzind_vector, _ = dataset_as_vector(nzind_dataset)
    nzval_vector, _ = dataset_as_vector(nzval_dataset)

    close(nzind_dataset)
    close(nzval_dataset)
    close(vector_group)

    return (nzind_vector, nzval_vector)
end

function Formats.format_filled_empty_sparse_vector!(
    ::H5df,
    ::AbstractString,
    ::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
)::Maybe{Formats.CacheGroup}
    return Formats.MappedData
end

function Formats.format_delete_vector!(h5df::H5df, axis::AbstractString, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(h5df)
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    delete_object(axis_vectors_group, name)
    return nothing
end

function Formats.format_vectors_set(h5df::H5df, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(h5df)

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    return flame_timed("H5df.keys_as_set") do
        return Set(keys(axis_vectors_group))
    end
end

function Formats.format_get_vector(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(h5df)

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    vector_object = axis_vectors_group[name]
    if vector_object isa HDF5.Dataset
        vector, cache_group = dataset_as_vector(vector_object)

    else
        @assert vector_object isa HDF5.Group
        nelements = Formats.format_axis_length(h5df, axis)

        nzind_dataset = vector_object["nzind"]
        @assert nzind_dataset isa HDF5.Dataset
        nzind_vector, nzind_cache_group = dataset_as_vector(nzind_dataset)

        if haskey(vector_object, "nztxt")
            nztxt_dataset = vector_object["nztxt"]
            @assert nztxt_dataset isa HDF5.Dataset
            nztxt_vector, _ = dataset_as_vector(nztxt_dataset)
            vector = Vector{AbstractString}(undef, nelements)
            fill!(vector, "")
            vector[nzind_vector] .= nztxt_vector
            cache_group = Formats.MemoryData

        else
            if haskey(vector_object, "nzval")
                nzval_dataset = vector_object["nzval"]
                @assert nzval_dataset isa HDF5.Dataset
                nzval_vector, nzval_cache_group = dataset_as_vector(nzval_dataset)
            else
                nzval_vector = fill(true, length(nzind_vector))
                nzval_cache_group = Formats.MemoryData
            end

            vector = SparseVector(nelements, nzind_vector, nzval_vector)
            cache_group = if (nzind_cache_group == Formats.MappedData && nzval_cache_group == Formats.MappedData)
                Formats.MappedData
            else
                Formats.MemoryData
            end
        end
    end

    return (vector, nothing, cache_group)
end

function Formats.format_has_matrix(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(h5df)
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    if !haskey(matrices_group, rows_axis)
        return false  # UNTESTED
    end

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    if !haskey(rows_axis_group, columns_axis)
        return false  # UNTESTED
    end

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    return haskey(columns_axis_group, name)
end

function Formats.format_set_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalarBase, StorageMatrix},
)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    nrows = Formats.format_axis_length(h5df, rows_axis)
    ncols = Formats.format_axis_length(h5df, columns_axis)

    if matrix isa StorageReal
        flame_timed("H5df.fill_real_matrix") do
            matrix_dataset = create_dataset(columns_axis_group, name, typeof(matrix), (nrows, ncols))
            matrix_dataset[:, :] = matrix
            return close(matrix_dataset)
        end

    elseif matrix isa AbstractString
        flame_timed("H5df.fill_string_matrix") do
            matrix_dataset = create_dataset(columns_axis_group, name, String, (nrows, ncols))
            matrix_dataset[:, :] = matrix
            return close(matrix_dataset)
        end

    elseif eltype(matrix) <: AbstractString
        write_string_matrix(columns_axis_group, name, matrix)  # NOJET

    else
        @assert matrix isa AbstractMatrix
        @assert major_axis(matrix) != Rows

        matrix = base_array(matrix)

        if issparse(matrix)
            flame_timed("H5df.write_sparse_matrix") do
                @assert matrix isa AbstractMatrix
                matrix_group = create_group(columns_axis_group, name)
                matrix_group["colptr"] = colptr(matrix)
                matrix_group["rowval"] = rowval(matrix)
                if eltype(matrix) != Bool || !all(nzval(matrix))
                    if eltype(matrix) <: AbstractString
                        matrix_group["nzval"] = String.(nzval(matrix))  # UNTESTED
                    else
                        matrix_group["nzval"] = nzval(matrix)
                    end
                end
                return close(matrix_group)
            end

        else
            flame_timed("H5df.write_dense_matrix") do
                nice_matrix = nothing
                try
                    base = pointer(matrix)
                    nice_matrix = Base.unsafe_wrap(Array, base, size(matrix))
                catch
                    nice_matrix = Matrix(matrix) # UNTESTED
                end
                return columns_axis_group[name] = nice_matrix  # NOJET
            end
        end
    end

    return nothing
end

function write_string_matrix( # UNTESTED
    columns_axis_group::HDF5.Group,
    name::AbstractString,
    matrix::AbstractMatrix{<:AbstractString},
)::Nothing
    flame_timed("H5df.write_string_matrix") do
        nrows, ncols = size(matrix)

        n_empty = 0
        nonempty_size = 0
        for value in matrix
            value_size = length(value)
            if value_size > 0
                nonempty_size += value_size
            else
                n_empty += 1
            end
        end

        n_values = nrows * ncols
        n_nonempty = n_values - n_empty
        indtype = indtype_for_size(n_values)

        dense_size = nonempty_size + length(matrix)
        sparse_size = nonempty_size + n_nonempty + (ncols + 1 + n_nonempty) * sizeof(indtype)

        if sparse_size <= dense_size * 0.75
            colptr_vector = Vector{indtype}(undef, ncols + 1)
            rowval_vector = Vector{indtype}(undef, n_nonempty)
            nztxt_vector = Vector{String}(undef, n_nonempty)

            position = 1
            for column_index in 1:ncols
                colptr_vector[column_index] = position
                for row_index in 1:nrows
                    value = matrix[row_index, column_index]
                    if length(value) > 0
                        @assert !(contains(value, '\n'))
                        rowval_vector[position] = row_index
                        nztxt_vector[position] = String(value)
                        position += 1
                    end
                end
            end
            @assert position == n_nonempty + 1
            colptr_vector[ncols + 1] = n_nonempty + 1

            matrix_group = create_group(columns_axis_group, name)
            matrix_group["colptr"] = colptr_vector
            matrix_group["rowval"] = rowval_vector
            matrix_group["nztxt"] = nztxt_vector
            close(matrix_group)

        else
            nice_matrix = String.(matrix)
            columns_axis_group[name] = nice_matrix  # NOJET
        end
    end
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(h5df)
    nrows = Formats.format_axis_length(h5df, rows_axis)
    ncols = Formats.format_axis_length(h5df, columns_axis)
    return create_empty_dense_matrix_in(h5df.root, rows_axis, columns_axis, name, eltype, nrows, ncols)
end

function create_empty_dense_matrix_in(
    daf_root::Union{HDF5.File, HDF5.Group},
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    matrices_group = daf_root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    local matrix_dataset
    flame_timed("H5df.create_empty_dense_matrix") do
        matrix_dataset = create_dataset(columns_axis_group, name, T, (nrows, ncols))
        return matrix_dataset[:, :] = 0
    end

    matrix, cache_group = dataset_as_matrix(matrix_dataset)
    close(matrix_dataset)

    return (matrix, cache_group)
end

function Formats.format_get_empty_sparse_matrix!(  # FLAKY TESTED
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(h5df)
    ncols = Formats.format_axis_length(h5df, columns_axis)
    return create_empty_sparse_matrix_in(h5df.root, rows_axis, columns_axis, name, eltype, nnz, indtype, ncols)
end

function create_empty_sparse_matrix_in(
    daf_root::Union{HDF5.File, HDF5.Group},
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
    ncols::Integer,
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    matrices_group = daf_root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group
    matrix_group = create_group(columns_axis_group, name)

    local colptr_dataset
    local rowval_dataset
    local nzval_dataset

    flame_timed("H5df.create_empty_sparse_matrix") do
        colptr_dataset = create_dataset(matrix_group, "colptr", I, ncols + 1)
        rowval_dataset = create_dataset(matrix_group, "rowval", I, Int(nnz))
        nzval_dataset = create_dataset(matrix_group, "nzval", T, Int(nnz))

        colptr_dataset[:] = nnz + 1
        colptr_dataset[1] = 1
        rowval_dataset[:] = 1
        return nzval_dataset[:] = 0
    end

    colptr_vector, _ = dataset_as_vector(colptr_dataset)
    rowval_vector, _ = dataset_as_vector(rowval_dataset)
    nzval_vector, _ = dataset_as_vector(nzval_dataset)

    close(colptr_dataset)
    close(rowval_dataset)
    close(nzval_dataset)
    close(matrix_group)

    return (colptr_vector, rowval_vector, nzval_vector)
end

function Formats.format_filled_empty_sparse_matrix!(
    ::H5df,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Maybe{Formats.CacheGroup}
    return Formats.MappedData
end

function Formats.format_relayout_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
)::StorageMatrix
    @assert Formats.has_data_write_lock(h5df)

    if issparse(matrix)
        sparse_colptr, sparse_rowval, sparse_nzval = Formats.format_get_empty_sparse_matrix!(
            h5df,
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
            axis_length(h5df, columns_axis),
            axis_length(h5df, rows_axis),
            sparse_colptr,
            sparse_rowval,
            sparse_nzval,
        )
        relayout!(flip(relayout_matrix), matrix)

    elseif eltype(matrix) <: AbstractString
        matrices_group = h5df.root["matrices"]
        @assert matrices_group isa HDF5.Group

        columns_axis_group = matrices_group[columns_axis]
        @assert columns_axis_group isa HDF5.Group

        rows_axis_group = columns_axis_group[rows_axis]
        @assert rows_axis_group isa HDF5.Group

        relayout_matrix = flipped(matrix)
        write_string_matrix(rows_axis_group, name, relayout_matrix)

    else
        relayout_matrix, _ = Formats.format_get_empty_dense_matrix!(h5df, columns_axis, rows_axis, name, eltype(matrix))
        relayout!(flip(relayout_matrix), matrix)
    end

    return relayout_matrix
end

function Formats.format_delete_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    delete_object(columns_axis_group, name)
    return nothing
end

function Formats.format_matrices_set(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(h5df)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    return flame_timed("H5df.keys_as_set") do
        return Set(keys(columns_axis_group))
    end
end

function Formats.format_get_matrix(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(h5df)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    matrix_object = columns_axis_group[name]
    if matrix_object isa HDF5.Dataset
        matrix, cache_group = dataset_as_matrix(matrix_object)
        return (matrix, nothing, cache_group)

    else
        @assert matrix_object isa HDF5.Group

        colptr_dataset = matrix_object["colptr"]
        @assert colptr_dataset isa HDF5.Dataset
        colptr_vector, colptr_cache_group = dataset_as_vector(colptr_dataset)

        rowval_dataset = matrix_object["rowval"]
        @assert rowval_dataset isa HDF5.Dataset
        rowval_vector, rowval_cache_group = dataset_as_vector(rowval_dataset)

        nrows = Formats.format_axis_length(h5df, rows_axis)
        ncols = Formats.format_axis_length(h5df, columns_axis)

        if haskey(matrix_object, "nztxt")
            nztxt_dataset = matrix_object["nztxt"]
            @assert nztxt_dataset isa HDF5.Dataset
            nztxt_vector, _ = dataset_as_vector(nztxt_dataset)

            matrix = Matrix{AbstractString}(undef, nrows, ncols)
            fill!(matrix, "")

            index = 0
            for column in 1:ncols
                first_row_position = colptr_vector[column]
                last_row_position = colptr_vector[column + 1] - 1
                for row in rowval_vector[first_row_position:last_row_position]
                    index += 1
                    matrix[row, column] = nztxt_vector[index]
                end
            end

            return (matrix, nothing, Formats.MemoryData)

        else
            if haskey(matrix_object, "nzval")
                nzval_dataset = matrix_object["nzval"]
                @assert nzval_dataset isa HDF5.Dataset
                nzval_vector, nzval_cache_group = dataset_as_vector(nzval_dataset)
            else
                nzval_vector = fill(true, length(rowval_vector))
                nzval_cache_group = Formats.MemoryData
            end

            matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
            cache_group =
                if (
                    colptr_cache_group == Formats.MappedData &&
                    rowval_cache_group == Formats.MappedData &&
                    nzval_cache_group == Formats.MappedData
                )
                    Formats.MappedData
                else
                    Formats.MemoryData
                end
            return (matrix, nothing, cache_group)
        end
    end
end

function dataset_as_vector(dataset::HDF5.Dataset)::Tuple{StorageVector, Formats.CacheGroup}
    return flame_timed("H5df.dataset_as_vector") do
        if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset) && !isempty(dataset)
            return (HDF5.readmmap(dataset), Formats.MappedData)  # NOJET
        else
            return (read(dataset), Formats.MemoryData)
        end
    end
end

function dataset_as_matrix(dataset::HDF5.Dataset)::Tuple{StorageMatrix, Formats.CacheGroup}
    return flame_timed("H5df.dataset_as_matrix") do
        if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset) && !isempty(dataset)
            return (HDF5.readmmap(dataset), Formats.MappedData)  # NOJET
        else
            return (read(dataset), Formats.MemoryData)
        end
    end
end

function Formats.format_description_header(h5df::H5df, indent::AbstractString, lines::Vector{String}, ::Bool)::Nothing
    @assert Formats.has_data_read_lock(h5df)
    push!(lines, "$(indent)type: H5df")
    push!(lines, "$(indent)root: $(h5df.root)")
    push!(lines, "$(indent)mode: $(h5df.mode)")
    return nothing
end

function Readers.complete_path(h5df::H5df)::Maybe{AbstractString}
    return h5df.path
end

const REORDER_LOCK_DATASET = "daf_reorder_lock"
const REORDER_REPOS_GROUP = "daf_reorder_repos"

function h5df_file(h5df::H5df)::HDF5.File  # FLAKY TESTED
    root = h5df.root
    return root isa HDF5.File ? root : root.file
end

function h5df_group_path(h5df::H5df)::String  # FLAKY TESTED
    root = h5df.root
    return root isa HDF5.File ? "/" : String(HDF5.name(root))
end

function h5df_backup_path(h5df::H5df)::String  # FLAKY TESTED
    return abspath(h5df_file(h5df).filename) * ".reorder.backup"
end

function lock_entry_name(group_path::String)::String  # FLAKY TESTED
    return replace(group_path, "/" => "__")
end

function Reorder.format_lock_reorder!(h5df::H5df, operation_id::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    file = h5df_file(h5df)
    op_id = String(operation_id)

    while true
        try
            write_dataset(file, REORDER_LOCK_DATASET, op_id)  # NOJET
            break
        catch  # UNTESTED
        end

        if !haskey(file, REORDER_LOCK_DATASET)  # NOJET  # UNTESTED
            continue  # UNTESTED
        end

        existing_id = try  # UNTESTED
            read(file[REORDER_LOCK_DATASET])  # NOJET  # UNTESTED
        catch
            continue  # UNTESTED
        end

        if existing_id == op_id  # UNTESTED
            break  # UNTESTED
        end

        error(chomp("""  # UNTESTED
                    cannot reorder: $(h5df_group_path(h5df))
                    in: $(file.filename)
                    another reorder operation: $(existing_id)
                    is already in progress
                    """))
    end

    if !haskey(file, REORDER_REPOS_GROUP)  # NOJET
        create_group(file, REORDER_REPOS_GROUP)  # NOJET
    end
    repos_group = file[REORDER_REPOS_GROUP]  # NOJET
    entry = lock_entry_name(h5df_group_path(h5df))
    @assert !haskey(repos_group, entry)
    repos_group[entry] = UInt8[1]  # NOJET
    return nothing
end

function Reorder.format_has_reorder_lock(h5df::H5df)::Bool
    @assert Formats.has_data_write_lock(h5df)
    file = h5df_file(h5df)
    if !haskey(file, REORDER_REPOS_GROUP)  # NOJET
        return false
    end
    repos_group = file[REORDER_REPOS_GROUP]  # NOJET  # UNTESTED
    entry = lock_entry_name(h5df_group_path(h5df))  # UNTESTED
    return haskey(repos_group, entry)  # NOJET  # UNTESTED
end

function Reorder.format_backup_reorder!(h5df::H5df, ::Reorder.FormatReorderPlan)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    backup_path = h5df_backup_path(h5df)
    if !isfile(backup_path)
        flush(h5df_file(h5df))
        cp(abspath(h5df_file(h5df).filename), backup_path)
    end
    return nothing
end

function Reorder.format_replace_reorder!(
    h5df::H5df,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    crash_counter::Maybe{Ref{Int}},
)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    backup_path = h5df_backup_path(h5df)
    @assert isfile(backup_path)

    needs_compaction = Ref(false)
    backup_file = h5open(backup_path, "r")  # NOJET
    try
        group_path = h5df_group_path(h5df)
        backup_root = group_path == "/" ? backup_file : backup_file[group_path]  # NOJET

        for (axis, planned_axis) in plan.planned_axes
            if Formats.format_has_axis(h5df, axis; for_change = false)
                axes_group = h5df.root["axes"]  # NOJET
                delete_object(axes_group, axis)  # NOJET
                axis_dataset = create_dataset(axes_group, axis, String, (length(planned_axis.new_entries),))  # NOJET
                axis_dataset[:] = planned_axis.new_entries
                close(axis_dataset)
            end
        end

        for planned in plan.planned_vectors
            replace_reorder_vector(h5df, backup_root, planned, plan, replacement_progress, needs_compaction)  # NOJET
            Reorder.tick_crash_counter!(crash_counter)
        end

        for planned in plan.planned_matrices
            replace_reorder_matrix(h5df, backup_root, planned, plan, replacement_progress, needs_compaction)  # NOJET
            Reorder.tick_crash_counter!(crash_counter)
        end
    finally
        close(backup_file)
    end

    if needs_compaction[]  # FLAKY TESTED
        @warn """
              reorder had to delete and recreate non-mmappable datasets in: $(h5df_file(h5df).filename)
              consider repacking the file to reclaim wasted space
              """
    end

    return nothing
end

function is_overwritable_dataset(dataset::HDF5.Dataset)::Bool  # FLAKY TESTED
    return HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset) && !isempty(dataset)
end

function replace_reorder_vector(
    h5df::H5df,
    backup_root::Union{HDF5.File, HDF5.Group},
    planned::Reorder.PlannedVector,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    needs_compaction::Ref{Bool},
)::Nothing
    planned_axis = plan.planned_axes[planned.axis]
    backup_object = backup_root["vectors"][planned.axis][planned.name]  # NOJET
    live_axis_vectors = h5df.root["vectors"][planned.axis]  # NOJET

    if backup_object isa HDF5.Dataset
        source = read(backup_object)
        if eltype(source) <: AbstractString
            permuted = Vector{String}(undef, length(source))
            permute_vector!(;
                destination = permuted,
                source,
                permutation = planned_axis.permutation,
                progress = replacement_progress,
            )
            delete_object(live_axis_vectors, planned.name)  # NOJET
            write_string_vector(live_axis_vectors, planned.name, permuted)
            needs_compaction[] = true
        else
            live_object = live_axis_vectors[planned.name]  # NOJET
            if live_object isa HDF5.Dataset && is_overwritable_dataset(live_object)
                destination, _ = dataset_as_vector(live_object)
                permute_vector!(;
                    destination,
                    source,
                    permutation = planned_axis.permutation,
                    progress = replacement_progress,
                )
            else
                delete_object(live_axis_vectors, planned.name)  # NOJET  # UNTESTED
                destination =  # UNTESTED
                    create_empty_dense_vector_in(h5df.root, planned.axis, planned.name, eltype(source), length(source))
                permute_vector!(;  # UNTESTED
                    destination,
                    source,
                    permutation = planned_axis.permutation,
                    progress = replacement_progress,
                )
                needs_compaction[] = true  # UNTESTED
            end
        end
    else
        @assert backup_object isa HDF5.Group
        source_nzind = read(backup_object["nzind"])  # NOJET
        has_nzval = haskey(backup_object, "nzval")
        source_nzval = has_nzval ? read(backup_object["nzval"]) : nothing  # NOJET

        if haskey(backup_object, "nztxt")
            nztxt_vector = read(backup_object["nztxt"])  # NOJET
            source_length = Formats.format_axis_length(h5df, planned.axis)
            full_vector = Vector{String}(undef, source_length)
            fill!(full_vector, "")
            full_vector[source_nzind] .= nztxt_vector
            permuted = Vector{String}(undef, source_length)
            permute_vector!(;
                destination = permuted,
                source = full_vector,
                permutation = planned_axis.permutation,
                progress = replacement_progress,
            )
            delete_object(live_axis_vectors, planned.name)  # NOJET
            write_string_vector(live_axis_vectors, planned.name, permuted)
            needs_compaction[] = true
        else
            source_length = Formats.format_axis_length(h5df, planned.axis)
            live_object = live_axis_vectors[planned.name]  # NOJET

            if live_object isa HDF5.Group &&
               is_overwritable_dataset(live_object["nzind"]) &&  # NOJET
               (!has_nzval || is_overwritable_dataset(live_object["nzval"]))
                dest_nzind, _ = dataset_as_vector(live_object["nzind"])  # NOJET
                dest_nzval = has_nzval ? dataset_as_vector(live_object["nzval"])[1] : nothing  # NOJET
                permute_sparse_vector_buffers!(;
                    destination_nzind = dest_nzind,
                    destination_nzval = dest_nzval,
                    source_length,
                    source_nzind,
                    source_nzval,
                    inverse_permutation = planned_axis.inverse_permutation,
                    progress = replacement_progress,
                )
            else
                delete_object(live_axis_vectors, planned.name)  # NOJET  # UNTESTED
                T = has_nzval ? eltype(source_nzval) : Bool  # UNTESTED
                I = eltype(source_nzind)  # UNTESTED
                dest_nzind, dest_nzval =  # UNTESTED
                    create_empty_sparse_vector_in(h5df.root, planned.axis, planned.name, T, length(source_nzind), I)
                permute_sparse_vector_buffers!(;  # UNTESTED
                    destination_nzind = dest_nzind,
                    destination_nzval = dest_nzval,
                    source_length,
                    source_nzind,
                    source_nzval,
                    inverse_permutation = planned_axis.inverse_permutation,
                    progress = replacement_progress,
                )
                needs_compaction[] = true  # UNTESTED
            end
        end
    end
    return nothing
end

function replace_reorder_matrix(
    h5df::H5df,
    backup_root::Union{HDF5.File, HDF5.Group},
    planned::Reorder.PlannedMatrix,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    needs_compaction::Ref{Bool},
)::Nothing
    planned_rows = get(plan.planned_axes, planned.rows_axis, nothing)
    planned_columns = get(plan.planned_axes, planned.columns_axis, nothing)
    @assert planned_rows !== nothing || planned_columns !== nothing

    backup_object = backup_root["matrices"][planned.rows_axis][planned.columns_axis][planned.name]  # NOJET
    live_columns_group = h5df.root["matrices"][planned.rows_axis][planned.columns_axis]  # NOJET

    if backup_object isa HDF5.Dataset
        source = read(backup_object)
        if eltype(source) <: AbstractString
            nrows, ncols = size(source)
            permuted = Matrix{String}(undef, nrows, ncols)
            if planned_rows !== nothing && planned_columns !== nothing
                permute_dense_matrix_both!(;
                    destination = permuted,
                    source,
                    rows_permutation = planned_rows.permutation,
                    columns_permutation = planned_columns.permutation,
                    progress = replacement_progress,
                )
            elseif planned_rows !== nothing
                permute_dense_matrix_rows!(;
                    destination = permuted,
                    source,
                    rows_permutation = planned_rows.permutation,
                    progress = replacement_progress,
                )
            else
                permute_dense_matrix_columns!(;
                    destination = permuted,
                    source,
                    columns_permutation = planned_columns.permutation,
                    progress = replacement_progress,
                )
            end
            delete_object(live_columns_group, planned.name)  # NOJET
            write_string_matrix(live_columns_group, planned.name, permuted)
            needs_compaction[] = true
        else
            nrows, ncols = size(source)
            live_object = live_columns_group[planned.name]  # NOJET
            if live_object isa HDF5.Dataset && is_overwritable_dataset(live_object)
                destination, _ = dataset_as_matrix(live_object)
            else
                delete_object(live_columns_group, planned.name)  # NOJET  # UNTESTED
                destination = create_empty_dense_matrix_in(  # UNTESTED
                    h5df.root,
                    planned.rows_axis,
                    planned.columns_axis,
                    planned.name,
                    eltype(source),
                    nrows,
                    ncols,
                )
                needs_compaction[] = true  # UNTESTED
            end

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
        end
    else
        @assert backup_object isa HDF5.Group
        source_colptr = read(backup_object["colptr"])  # NOJET
        source_rowval = read(backup_object["rowval"])  # NOJET
        has_nzval = haskey(backup_object, "nzval")
        source_nzval = has_nzval ? read(backup_object["nzval"]) : nothing  # NOJET

        if haskey(backup_object, "nztxt")
            nztxt_vector = read(backup_object["nztxt"])  # NOJET
            nrows = Formats.format_axis_length(h5df, planned.rows_axis)
            ncols = Formats.format_axis_length(h5df, planned.columns_axis)
            full_matrix = Matrix{String}(undef, nrows, ncols)
            fill!(full_matrix, "")
            for col in 1:ncols
                for idx in source_colptr[col]:(source_colptr[col + 1] - 1)
                    full_matrix[source_rowval[idx], col] = nztxt_vector[idx]
                end
            end
            permuted = Matrix{String}(undef, nrows, ncols)
            if planned_rows !== nothing && planned_columns !== nothing
                permute_dense_matrix_both!(;  # UNTESTED
                    destination = permuted,
                    source = full_matrix,
                    rows_permutation = planned_rows.permutation,
                    columns_permutation = planned_columns.permutation,
                    progress = replacement_progress,
                )
            elseif planned_rows !== nothing
                permute_dense_matrix_rows!(;
                    destination = permuted,
                    source = full_matrix,
                    rows_permutation = planned_rows.permutation,
                    progress = replacement_progress,
                )
            else
                permute_dense_matrix_columns!(;  # UNTESTED
                    destination = permuted,
                    source = full_matrix,
                    columns_permutation = planned_columns.permutation,
                    progress = replacement_progress,
                )
            end
            delete_object(live_columns_group, planned.name)  # NOJET
            write_string_matrix(live_columns_group, planned.name, permuted)
            needs_compaction[] = true
        else
            nrows = Formats.format_axis_length(h5df, planned.rows_axis)
            ncols = Formats.format_axis_length(h5df, planned.columns_axis)
            source_nnz = length(source_rowval)
            live_object = live_columns_group[planned.name]  # NOJET

            if live_object isa HDF5.Group &&
               is_overwritable_dataset(live_object["colptr"]) &&  # NOJET
               is_overwritable_dataset(live_object["rowval"]) &&  # NOJET
               (!has_nzval || is_overwritable_dataset(live_object["nzval"]))  # NOJET
                dest_colptr, _ = dataset_as_vector(live_object["colptr"])  # NOJET
                dest_rowval, _ = dataset_as_vector(live_object["rowval"])  # NOJET
                dest_nzval = has_nzval ? dataset_as_vector(live_object["nzval"])[1] : nothing  # NOJET
            else
                delete_object(live_columns_group, planned.name)  # NOJET  # UNTESTED
                T = has_nzval ? eltype(source_nzval) : Bool  # UNTESTED
                I = eltype(source_colptr)  # UNTESTED
                dest_colptr, dest_rowval, dest_nzval = create_empty_sparse_matrix_in(  # UNTESTED
                    h5df.root,
                    planned.rows_axis,
                    planned.columns_axis,
                    planned.name,
                    T,
                    source_nnz,
                    I,
                    ncols,
                )
                needs_compaction[] = true  # UNTESTED
            end

            if planned_rows !== nothing && planned_columns !== nothing
                permute_sparse_matrix_both_buffers!(;
                    destination_colptr = dest_colptr,
                    destination_rowval = dest_rowval,
                    destination_nzval = dest_nzval,
                    source_n_rows = nrows,
                    source_colptr,
                    source_rowval,
                    source_nzval,
                    inverse_rows_permutation = planned_rows.inverse_permutation,
                    columns_permutation = planned_columns.permutation,
                    progress = replacement_progress,
                )
            elseif planned_rows !== nothing
                permute_sparse_matrix_rows_buffers!(;
                    destination_colptr = dest_colptr,
                    destination_rowval = dest_rowval,
                    destination_nzval = dest_nzval,
                    source_n_rows = nrows,
                    source_colptr,
                    source_rowval,
                    source_nzval,
                    inverse_rows_permutation = planned_rows.inverse_permutation,
                    progress = replacement_progress,
                )
            else
                permute_sparse_matrix_columns_buffers!(;
                    destination_colptr = dest_colptr,
                    destination_rowval = dest_rowval,
                    destination_nzval = dest_nzval,
                    source_n_rows = nrows,
                    source_colptr,
                    source_rowval,
                    source_nzval,
                    columns_permutation = planned_columns.permutation,
                    progress = replacement_progress,
                )
            end
        end
    end
    return nothing
end

function Reorder.format_cleanup_reorder!(h5df::H5df)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    file = h5df_file(h5df)
    @assert haskey(file, REORDER_REPOS_GROUP)
    repos_group = file[REORDER_REPOS_GROUP]
    entry = lock_entry_name(h5df_group_path(h5df))
    @assert haskey(repos_group, entry)
    delete_object(repos_group, entry)  # NOJET
    if isempty(keys(repos_group))  # NOJET
        delete_object(file, REORDER_REPOS_GROUP)
        delete_object(file, REORDER_LOCK_DATASET)
        rm(h5df_backup_path(h5df); force = true)
    end
    return nothing
end

function Reorder.format_reset_reorder!(h5df::H5df)::Bool
    @assert Formats.has_data_write_lock(h5df)
    file = h5df_file(h5df)
    if !haskey(file, REORDER_REPOS_GROUP)
        return false
    end
    repos_group = file[REORDER_REPOS_GROUP]
    entry = lock_entry_name(h5df_group_path(h5df))
    if !haskey(repos_group, entry)
        return false  # UNTESTED
    end

    backup_path = h5df_backup_path(h5df)
    if isfile(backup_path)
        group_path = h5df_group_path(h5df)
        backup_file = h5open(backup_path, "r")
        try
            backup_root = group_path == "/" ? backup_file : backup_file[group_path]
            restore_group_from_backup(h5df.root, backup_root)  # NOJET
        finally
            close(backup_file)
        end
    end

    delete_object(repos_group, entry)  # NOJET
    if isempty(keys(repos_group))  # NOJET
        delete_object(file, REORDER_REPOS_GROUP)
        delete_object(file, REORDER_LOCK_DATASET)
        rm(backup_path; force = true)
    end
    return true
end

function restore_group_from_backup(
    live_root::Union{HDF5.File, HDF5.Group},
    backup_root::Union{HDF5.File, HDF5.Group},
)::Nothing
    axes_group = live_root["axes"]
    backup_axes = backup_root["axes"]
    for axis in keys(backup_axes)  # NOJET
        if haskey(axes_group, axis)
            delete_object(axes_group, axis)  # NOJET
        end
        backup_ds = backup_axes[axis]  # NOJET
        axis_dataset = create_dataset(axes_group, axis, String, size(backup_ds))  # NOJET
        axis_dataset[:] = read(backup_ds)  # NOJET
        close(axis_dataset)
    end

    backup_vectors = backup_root["vectors"]
    live_vectors = live_root["vectors"]
    for axis in keys(backup_vectors)  # NOJET
        if !haskey(live_vectors, axis)
            continue  # UNTESTED
        end
        backup_axis_vectors = backup_vectors[axis]
        live_axis_vectors = live_vectors[axis]  # NOJET
        for name in keys(backup_axis_vectors)  # NOJET
            if haskey(live_axis_vectors, name)  # NOJET
                delete_object(live_axis_vectors, name)
            end
            restore_object(live_axis_vectors, backup_axis_vectors, name)
        end
    end

    backup_matrices = backup_root["matrices"]
    live_matrices = live_root["matrices"]
    for rows_axis in keys(backup_matrices)
        if !haskey(live_matrices, rows_axis)
            continue  # UNTESTED
        end
        for columns_axis in keys(backup_matrices[rows_axis])  # NOJET
            if !haskey(live_matrices[rows_axis], columns_axis)  # NOJET
                continue  # UNTESTED
            end
            backup_properties = backup_matrices[rows_axis][columns_axis]  # NOJET
            live_properties = live_matrices[rows_axis][columns_axis]  # NOJET
            for name in keys(backup_properties)  # NOJET
                if haskey(live_properties, name)  # NOJET
                    delete_object(live_properties, name)  # NOJET
                end
                restore_object(live_properties, backup_properties, name)
            end
        end
    end
    return nothing
end

function restore_object(live_group::HDF5.Group, backup_group::HDF5.Group, name::AbstractString)::Nothing
    obj = backup_group[name]
    if obj isa HDF5.Dataset
        data = read(obj)
        live_group[name] = data
    else
        @assert obj isa HDF5.Group
        new_group = create_group(live_group, name)
        for sub_name in keys(obj)
            restore_object(new_group, obj, sub_name)
        end
        close(new_group)
    end
    return nothing
end

end  # module
