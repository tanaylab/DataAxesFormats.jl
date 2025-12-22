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
"""
struct H5df <: DafWriter
    name::AbstractString
    internal::Internal
    root::Union{HDF5.File, HDF5.Group}
    mode::AbstractString
end

function H5df(
    root::Union{AbstractString, HDF5.File, HDF5.Group},
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{H5df, DafReadOnly}
    (is_read_only, create_if_missing, truncate_if_exists) = Formats.parse_mode(mode)

    if root isa AbstractString
        parts = split(root, ".h5dfs#/")
        if length(parts) == 1
            group = nothing
        else
            @assert length(parts) == 2 "can't parse as <file-path>.h5dfs#/<group-path>: $(root)"
            root, group = parts
            root *= ".h5dfs"
        end

        key = (:daf, :hdf5, is_read_only ? "r" : "r+")
        if !truncate_if_exists
            purge = false
        elseif group === nothing
            purge = true  # UNTESTED
        else
            mode = "w+"
            purge = false
        end

        root = get_through_global_weak_cache(abspath(root), key; purge) do _
            return h5open(root, mode == "w+" ? "cw" : mode; fapl = HDF5.FileAccessProperties(; alignment = (1, 8)))  # NOJET
        end

        if group !== nothing
            if haskey(root, group)
                if truncate_if_exists
                    delete_object(root, group)
                    create_group(root, group)
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

    if name === nothing
        if root isa HDF5.Group
            name = "$(root.file.filename)#$(HDF5.name(root))"
        else
            @assert root isa HDF5.File
            name = root.filename
        end
    end
    name = unique_name(name)

    h5df = H5df(name, Internal(; cache_group = MappedData, is_frozen = is_read_only), root, mode)
    @debug "Daf: $(brief(h5df)) root: $(root)"
    if is_read_only
        return read_only(h5df)
    else
        return h5df
    end
end

function verify_alignment(root::HDF5.Group)::Nothing
    return verify_alignment(root.file)
end

function verify_alignment(root::HDF5.File)::Nothing
    file_access_properties = HDF5.get_access_properties(root)
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

function Formats.format_has_scalar(h5df::H5df, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    return haskey(scalars_group, name)
end

function Formats.format_set_scalar!(h5df::H5df, name::AbstractString, value::StorageScalar)::Nothing
    @assert Formats.has_data_write_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    scalars_group[name] = value
    return nothing
end

function Formats.format_delete_scalar!(h5df::H5df, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    delete_object(scalars_group, name)
    return nothing
end

function Formats.format_get_scalar(h5df::H5df, name::AbstractString)::StorageScalar
    @assert Formats.has_data_read_lock(h5df)
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    scalars_dataset = scalars_group[name]
    @assert scalars_dataset isa HDF5.Dataset
    return read(scalars_dataset)
end

function Formats.format_scalars_set(h5df::H5df)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(h5df)

    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group

    return Set(keys(scalars_group))
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
    axis_dataset = create_dataset(axes_group, axis, String, (length(entries)))
    axis_dataset[:] = entries

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group
    axis_vectors_group = create_group(vectors_group, axis)
    close(axis_vectors_group)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    axes = Set(keys(axes_group))
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

    return Set(keys(axes_group))
end

function Formats.format_axis_vector(h5df::H5df, axis::AbstractString)::AbstractVector{<:AbstractString}
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
        vector_dataset =
            create_dataset(axis_vectors_group, name, typeof(vector), (Formats.format_axis_length(h5df, axis),))
        vector_dataset[:] = vector  # NOJET
        close(vector_dataset)

    else
        @assert vector isa AbstractVector

        vector = base_array(vector)

        if issparse(vector)
            @assert vector isa AbstractVector
            vector_group = create_group(axis_vectors_group, name)
            vector_group["nzind"] = nzind(vector)  # NOJET
            if eltype(vector) != Bool || !all(nzval(vector))
                if eltype(vector) <: AbstractString
                    vector_group["nzval"] = String.(nzval(vector))  # NOJET
                else
                    vector_group["nzval"] = nzval(vector)
                end
            end
            close(vector_group)

        elseif eltype(vector) <: AbstractString
            write_string_vector(axis_vectors_group, name, vector)

        else
            nice_vector = nothing
            try
                base = pointer(vector)
                nice_vector = Base.unsafe_wrap(Array, base, size(vector))
            catch
                nice_vector = Vector(vector)  # NOJET # UNTESTED
            end
            axis_vectors_group[name] = nice_vector  # NOJET
        end
    end

    return nothing
end

function write_string_vector(
    axis_vectors_group::HDF5.Group,
    name::AbstractString,
    vector::AbstractVector{<:AbstractString},
)::Nothing
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
                nztxt_vector[position] = String.(value)
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

    return nothing
end

function Formats.format_get_empty_dense_vector!(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::AbstractVector{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(h5df)
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    vector_dataset = create_dataset(axis_vectors_group, name, eltype, (Formats.format_axis_length(h5df, axis),))
    @assert vector_dataset isa HDF5.Dataset
    vector_dataset[:] = 0

    vector = dataset_as_vector(vector_dataset)
    close(vector_dataset)
    return vector
end

function Formats.format_get_empty_sparse_vector!(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(h5df)
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group
    vector_group = create_group(axis_vectors_group, name)

    nzind_dataset = create_dataset(vector_group, "nzind", indtype, (nnz,))
    nzval_dataset = create_dataset(vector_group, "nzval", eltype, (nnz,))

    @assert nzind_dataset isa HDF5.Dataset
    @assert nzval_dataset isa HDF5.Dataset

    nzind_dataset[:] = 0
    nzval_dataset[:] = 0

    nzind_vector = dataset_as_vector(nzind_dataset)
    nzval_vector = dataset_as_vector(nzval_dataset)

    close(nzind_dataset)
    close(nzval_dataset)
    close(vector_group)

    return (nzind_vector, nzval_vector)
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

    return Set(keys(axis_vectors_group))
end

function Formats.format_get_vector(h5df::H5df, axis::AbstractString, name::AbstractString)::StorageVector
    @assert Formats.has_data_read_lock(h5df)

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    vector_object = axis_vectors_group[name]
    if vector_object isa HDF5.Dataset
        vector = dataset_as_vector(vector_object)

    else
        @assert vector_object isa HDF5.Group
        nelements = Formats.format_axis_length(h5df, axis)

        nzind_dataset = vector_object["nzind"]
        @assert nzind_dataset isa HDF5.Dataset
        nzind_vector = dataset_as_vector(nzind_dataset)

        if haskey(vector_object, "nztxt")
            nztxt_dataset = vector_object["nztxt"]
            @assert nztxt_dataset isa HDF5.Dataset
            nztxt_vector = dataset_as_vector(nztxt_dataset)
            vector = Vector{AbstractString}(undef, nelements)
            fill!(vector, "")
            vector[nzind_vector] .= nztxt_vector

        else
            if haskey(vector_object, "nzval")
                nzval_dataset = vector_object["nzval"]
                @assert nzval_dataset isa HDF5.Dataset
                nzval_vector = dataset_as_vector(nzval_dataset)
            else
                nzval_vector = fill(true, length(nzind_vector))
            end

            vector = SparseVector(nelements, nzind_vector, nzval_vector)
        end
    end

    return vector
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

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

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
        matrix_dataset = create_dataset(columns_axis_group, name, typeof(matrix), (nrows, ncols))
        matrix_dataset[:, :] = matrix
        close(matrix_dataset)

    elseif matrix isa AbstractString
        matrix_dataset = create_dataset(columns_axis_group, name, String, (nrows, ncols))
        matrix_dataset[:, :] = matrix
        close(matrix_dataset)

    elseif eltype(matrix) <: AbstractString
        write_string_matrix(columns_axis_group, name, matrix)  # NOJET

    else
        @assert matrix isa AbstractMatrix
        @assert major_axis(matrix) != Rows

        matrix = base_array(matrix)

        if issparse(matrix)
            @assert matrix isa AbstractMatrix
            matrix_group = create_group(columns_axis_group, name)
            matrix_group["colptr"] = colptr(matrix)
            matrix_group["rowval"] = rowval(matrix)
            if eltype(matrix) != Bool || !all(nzval(matrix))
                if eltype(matrix) <: AbstractString
                    matrix_group["nzval"] = String.(nzval(matrix))
                else
                    matrix_group["nzval"] = nzval(matrix)
                end
            end
            close(matrix_group)

        else
            nice_matrix = nothing
            try
                base = pointer(matrix)
                nice_matrix = Base.unsafe_wrap(Array, base, size(matrix))
            catch
                nice_matrix = Matrix(matrix) # UNTESTED
            end
            columns_axis_group[name] = nice_matrix  # NOJET
        end
    end

    return nothing
end

function write_string_matrix(
    columns_axis_group::HDF5.Group,
    name::AbstractString,
    matrix::AbstractMatrix{<:AbstractString},
)::Nothing
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
                    nztxt_vector[position] = String.(value)
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

    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::AbstractMatrix{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(h5df)
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    nrows = Formats.format_axis_length(h5df, rows_axis)
    ncols = Formats.format_axis_length(h5df, columns_axis)
    matrix_dataset = create_dataset(columns_axis_group, name, eltype, (nrows, ncols))
    matrix_dataset[:, :] = 0
    matrix = dataset_as_matrix(matrix_dataset)
    close(matrix_dataset)

    return matrix
end

function Formats.format_get_empty_sparse_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(h5df)
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group
    matrix_group = create_group(columns_axis_group, name)

    ncols = Formats.format_axis_length(h5df, columns_axis)

    colptr_dataset = create_dataset(matrix_group, "colptr", indtype, ncols + 1)
    rowval_dataset = create_dataset(matrix_group, "rowval", indtype, Int(nnz))
    nzval_dataset = create_dataset(matrix_group, "nzval", eltype, Int(nnz))

    colptr_dataset[:] = nnz + 1
    colptr_dataset[1] = 1
    rowval_dataset[:] = 1
    nzval_dataset[:] = 0

    colptr_vector = dataset_as_vector(colptr_dataset)
    rowval_vector = dataset_as_vector(rowval_dataset)
    nzval_vector = dataset_as_vector(nzval_dataset)

    close(colptr_dataset)
    close(rowval_dataset)
    close(nzval_dataset)
    close(matrix_group)

    return (colptr_vector, rowval_vector, nzval_vector)
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
        relayout_matrix = Formats.format_get_empty_dense_matrix!(h5df, columns_axis, rows_axis, name, eltype(matrix))
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

    return Set(keys(columns_axis_group))
end

function Formats.format_get_matrix(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    @assert Formats.has_data_read_lock(h5df)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    matrix_object = columns_axis_group[name]
    if matrix_object isa HDF5.Dataset
        return dataset_as_matrix(matrix_object)

    else
        @assert matrix_object isa HDF5.Group

        colptr_dataset = matrix_object["colptr"]
        @assert colptr_dataset isa HDF5.Dataset
        colptr_vector = dataset_as_vector(colptr_dataset)

        rowval_dataset = matrix_object["rowval"]
        @assert rowval_dataset isa HDF5.Dataset
        rowval_vector = dataset_as_vector(rowval_dataset)

        nrows = Formats.format_axis_length(h5df, rows_axis)
        ncols = Formats.format_axis_length(h5df, columns_axis)

        if haskey(matrix_object, "nztxt")
            nztxt_dataset = matrix_object["nztxt"]
            @assert nztxt_dataset isa HDF5.Dataset
            nztxt_vector = dataset_as_vector(nztxt_dataset)

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

        else
            if haskey(matrix_object, "nzval")
                nzval_dataset = matrix_object["nzval"]
                @assert nzval_dataset isa HDF5.Dataset
                nzval_vector = dataset_as_vector(nzval_dataset)
            else
                nzval_vector = fill(true, length(rowval_vector))
            end

            matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
        end
    end

    return matrix
end

function dataset_as_vector(dataset::HDF5.Dataset)::StorageVector
    if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset) && !isempty(dataset)
        return HDF5.readmmap(dataset)
    else
        return read(dataset)
    end
end

function dataset_as_matrix(dataset::HDF5.Dataset)::StorageMatrix
    if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset) && !isempty(dataset)
        return HDF5.readmmap(dataset)
    else
        return read(dataset)
    end
end

function Formats.format_description_header(h5df::H5df, indent::AbstractString, lines::Vector{String}, ::Bool)::Nothing
    @assert Formats.has_data_read_lock(h5df)
    push!(lines, "$(indent)type: H5df")
    push!(lines, "$(indent)root: $(h5df.root)")
    push!(lines, "$(indent)mode: $(h5df.mode)")
    return nothing
end

end  # module
