"""
HDF5 storage `Daf` storage format. This is the "native" way to store `Daf` data in HDF5 files. HDF5 files are
essentially "a filesystem inside a file", with "groups" instead of directories and "datasets" instead of files. This is
a generic format and there are various specific formats which use specific internal structure to hold some data - for
example, `h5ad` files have a specific internal structure for representing `AnnData` objects. To represent `Daf` data in
HDF5 storage, we use the following internal structure (which is **not** compatible with `h5ad`):

  - An HDF5 file may contain `Daf` data directly in the root group, in which case, it is restricted to holding just a
    single `Daf` data set. When using such a file, you automatically access the single `Daf` data set contained in it.
    By convention such files are given a `.h5df` suffix.
  - Alternatively, an HDF5 file may contain `Daf` data inside some arbitrary group, in which case, there's no restriction
    on the content of other groups in the file. Such groups may contain other `Daf` data (allowing for multiple `Daf` data
    sets in a single file), and/or non-`Daf` data. When using such a file, you need to specify the name of the group that
    contains the `Daf` data set you are interested it. By convention, at least if such files contain "mostly" (or only)
    `Daf` data sets, they are given a `.h5dfs` suffix, and are accompanied by some documentation describing the top-level
    groups in the file.
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
  - The `matrices` group contains a sub-group for each rows axis, which contains a sub-group for each columns axis. Each
    such sub-sub group contains matrix properties. If the matrix is dense, it is stored directly as a "dataset" (in
    column-major layout). Otherwise, it is stored as a group containing three vector "datasets": `colptr` containing the
    indices of the rows of each column in `rowval`, `rowval` containing the indices of the non-zero rows of the columns,
    and `nzval` containing the non-zero matrix entry values. See Julia's `SparseMatrixCSC` implementation for details.
    The only supported matrix element types are these included in [`StorageNumber`](@ref) - this explicitly excludes
    matrices of strings, same as [`StorageMatrix`](@ref).
  - All vectors and matrices are stored in a contiguous way in the file, which allows us to efficiently memory-map
    them.

That's all there is to it. Due to the above restrictions on types and layout, the metadata provided by HDF5 for each
"dataset" is sufficient to fully describe the data, and one should be able to directly access it using any HDF5 API in
any programming language, if needed. Typically, however, it is easiest to simply use the Julia `Daf` package to access
the data.

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

using Daf.Data
using Daf.Unions
using Daf.Formats
using Daf.MatrixLayouts
using Daf.ReadOnly
using Daf.StorageTypes
using HDF5
using SparseArrays

import Daf.Data.base_array
import Daf.Formats
import Daf.Formats.Internal

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

If the `name` is not specified, uses the path of the HDF5 file, followed by the internal path of the group (if any).

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type          |
|:---- |:-------------------- |:------------------------- |:------------------- |:---------------------- |
| `r`  | No                   | No                        | No                  | [`ReadOnlyView`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`H5df`](@ref)         |
| `w+` | Yes                  | Yes                       | No                  | [`H5df`](@ref)         |
| `w`  | Yes                  | Yes                       | Yes                 | [`H5df`](@ref)         |

!!! note

    If specifying a path (string) `root`, when calling `h5open`, the file alignment of created files is set to `(1, 8)`
    to maximize efficiency of mapped vectors and matrices, and the `w+` mode is converted to `cw`.
"""
struct H5df <: DafWriter
    internal::Internal
    root::Union{HDF5.File, HDF5.Group}
end

function H5df(
    root::Union{AbstractString, HDF5.File, HDF5.Group},
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{H5df, ReadOnlyView}
    (is_read_only, create_if_missing, truncate_if_exists) = Formats.parse_mode(mode)

    if root isa AbstractString
        if mode == "w+"
            mode = "cw"
        end
        root = h5open(root, mode; fapl = HDF5.FileAccessProperties(; alignment = (1, 8)))  # NOJET
    end
    verify_alignment(root)

    if name == nothing
        if root isa HDF5.File
            name = root.filename
        else
            @assert root isa HDF5.Group
            name = "$(root.file.filename):$(HDF5.name(root))"
        end
    end

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

    h5df = H5df(Internal(name), root)
    if is_read_only
        return read_only(h5df)  # untested
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
        @warn (
            "unsafe HDF5 file alignment for Daf: ($(Int(alignment[1])), $(Int(alignment[2])))\n" *
            "the safe HDF5 file alignment is: (1, 8)\n" *
            "note that unaligned data is inefficient,\n" *
            "and will break the empty_* functions;\n" *
            "to force the alignment, create the file using:\n" *
            "h5open(...;fapl=HDF5.FileAccessProperties(;alignment=(1,8))"
        )
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
    return close(matrices_group)
end

function verify_daf(root::Union{HDF5.File, HDF5.Group})::Nothing
    format_dataset = root["daf"]
    @assert format_dataset isa HDF5.Dataset
    format_version = read(format_dataset)  # NOJET
    @assert length(format_version) == 2
    @assert eltype(format_version) <: Unsigned
    if format_version[1] != MAJOR_VERSION || format_version[2] > MINOR_VERSION
        error(
            "incompatible format version: $(format_version[1]).$(format_version[2])\n" *
            "for the daf data: $(root)\n" *
            "the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)",
        )
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
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    return haskey(scalars_group, name)
end

function Formats.format_set_scalar!(h5df::H5df, name::AbstractString, value::StorageScalar)::Nothing
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    scalars_group[name] = value
    return nothing
end

function Formats.format_delete_scalar!(h5df::H5df, name::AbstractString; for_set::Bool)::Nothing
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    delete_object(scalars_group, name)
    return nothing
end

function Formats.format_get_scalar(h5df::H5df, name::AbstractString)::StorageScalar
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group
    scalars_dataset = scalars_group[name]
    @assert scalars_dataset isa HDF5.Dataset
    return read(scalars_dataset)
end

function Formats.format_scalar_names(h5df::H5df)::AbstractStringSet
    scalars_group = h5df.root["scalars"]
    @assert scalars_group isa HDF5.Group

    names = Set(keys(scalars_group))
    Formats.cache_scalar_names!(h5df, names, MemoryData)
    return names
end

function Formats.format_has_axis(h5df::H5df, axis::AbstractString)::Bool
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group
    return haskey(axes_group, axis)
end

function Formats.format_add_axis!(h5df::H5df, axis::AbstractString, entries::AbstractStringVector)::Nothing
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group
    axes_group[axis] = entries

    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group
    axis_vectors_group = create_group(vectors_group, axis)
    close(axis_vectors_group)

    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    axes = Formats.get_axis_names_through_cache(h5df)

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

function Formats.format_axis_names(h5df::H5df)::AbstractStringSet
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    names = Set(keys(axes_group))
    Formats.cache_axis_names!(h5df, names, MemoryData)
    return names
end

function Formats.format_get_axis(h5df::H5df, axis::AbstractString)::AbstractStringVector
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    axis_dataset = axes_group[axis]
    @assert axis_dataset isa HDF5.Dataset
    entries, cache_type = dataset_as_vector(axis_dataset)
    Formats.cache_axis!(h5df, axis, entries, cache_type)
    return entries
end

function Formats.format_axis_length(h5df::H5df, axis::AbstractString)::Int64
    axes_group = h5df.root["axes"]
    @assert axes_group isa HDF5.Group

    axis_dataset = axes_group[axis]
    @assert axis_dataset isa HDF5.Dataset
    return length(axis_dataset)
end

function Formats.format_has_vector(h5df::H5df, axis::AbstractString, name::AbstractString)::Bool
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
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    if vector isa StorageScalar
        vector_dataset =
            create_dataset(axis_vectors_group, name, typeof(vector), (Formats.format_axis_length(h5df, axis),))

        vector_dataset[:] = vector
        close(vector_dataset)

    elseif vector isa SparseVector
        vector_group = create_group(axis_vectors_group, name)
        vector_group["nzind"] = vector.nzind
        vector_group["nzval"] = vector.nzval
        close(vector_group)

    else
        @assert vector isa AbstractVector
        axis_vectors_group[name] = base_array(vector)
    end

    return nothing
end

function Formats.format_empty_dense_vector!(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::AbstractVector{T} where {T <: StorageNumber}
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    vector_dataset = create_dataset(axis_vectors_group, name, eltype, (Formats.format_axis_length(h5df, axis),))
    @assert vector_dataset isa HDF5.Dataset
    vector_dataset[1] = 0

    vector, cache_type = dataset_as_vector(vector_dataset)
    close(vector_dataset)

    Formats.cache_vector!(h5df, axis, name, vector, cache_type)
    return vector
end

function Formats.format_empty_sparse_vector!(
    h5df::H5df,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: StorageNumber, I <: StorageInteger}
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group
    vector_group = create_group(axis_vectors_group, name)

    nzind_dataset = create_dataset(vector_group, "nzind", indtype, (nnz,))
    nzval_dataset = create_dataset(vector_group, "nzval", eltype, (nnz,))

    @assert nzind_dataset isa HDF5.Dataset
    @assert nzval_dataset isa HDF5.Dataset

    nzind_dataset[1] = 0
    nzval_dataset[1] = 0

    nzind_vector, nzind_cache_type = dataset_as_vector(nzind_dataset)
    nzval_vector, nzval_cache_type = dataset_as_vector(nzval_dataset)

    close(nzind_dataset)
    close(nzval_dataset)
    close(vector_group)

    nelements = Formats.format_axis_length(h5df, axis)
    sparse_vector = SparseVector(nelements, nzind_vector, nzval_vector)

    Formats.cache_vector!(
        h5df,
        axis,
        name,
        sparse_vector,
        Formats.combined_cache_type(nzind_cache_type, nzval_cache_type),
    )
    return sparse_vector
end

function Formats.format_delete_vector!(h5df::H5df, axis::AbstractString, name::AbstractString; for_set::Bool)::Nothing
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    delete_object(axis_vectors_group, name)
    return nothing
end

function Formats.format_vector_names(h5df::H5df, axis::AbstractString)::AbstractStringSet
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    names = Set(keys(axis_vectors_group))
    Formats.cache_vector_names!(h5df, axis, names, MemoryData)
    return names
end

function Formats.format_get_vector(h5df::H5df, axis::AbstractString, name::AbstractString)::StorageVector
    vectors_group = h5df.root["vectors"]
    @assert vectors_group isa HDF5.Group

    axis_vectors_group = vectors_group[axis]
    @assert axis_vectors_group isa HDF5.Group

    vector_object = axis_vectors_group[name]
    if vector_object isa HDF5.Dataset
        vector, cache_type = dataset_as_vector(vector_object)
        Formats.cache_vector!(h5df, axis, name, vector, cache_type)
        return vector

    else
        @assert vector_object isa HDF5.Group

        nzind_dataset = vector_object["nzind"]
        @assert nzind_dataset isa HDF5.Dataset
        nzind_vector, nzind_ismapped = dataset_as_vector(nzind_dataset)

        nzval_dataset = vector_object["nzval"]
        @assert nzval_dataset isa HDF5.Dataset
        nzval_vector, nzval_ismapped = dataset_as_vector(nzval_dataset)

        nelements = Formats.format_axis_length(h5df, axis)
        return SparseVector(nelements, nzind_vector, nzval_vector)
    end
end

function Formats.format_has_matrix(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
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
    matrix::Union{StorageNumber, StorageMatrix},
)::Nothing
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    if matrix isa StorageNumber
        nrows = Formats.format_axis_length(h5df, rows_axis)
        ncols = Formats.format_axis_length(h5df, columns_axis)
        matrix_dataset = create_dataset(columns_axis_group, name, typeof(matrix), (nrows, ncols))
        matrix_dataset[:, :] = matrix
        close(matrix_dataset)

    elseif matrix isa SparseMatrixCSC
        matrix_group = create_group(columns_axis_group, name)
        matrix_group["colptr"] = matrix.colptr
        matrix_group["rowval"] = matrix.rowval
        matrix_group["nzval"] = matrix.nzval
        close(matrix_group)

    else
        columns_axis_group[name] = matrix
    end

    return nothing
end

function Formats.format_empty_dense_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::AbstractMatrix{T} where {T <: StorageNumber}
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    nrows = Formats.format_axis_length(h5df, rows_axis)
    ncols = Formats.format_axis_length(h5df, columns_axis)
    matrix_dataset = create_dataset(columns_axis_group, name, eltype, (nrows, ncols))
    matrix_dataset[1, 1] = 0
    matrix, cache_type = dataset_as_matrix(matrix_dataset)
    close(matrix_dataset)

    Formats.cache_matrix!(h5df, rows_axis, columns_axis, name, matrix, cache_type)
    return matrix
end

function Formats.format_empty_sparse_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: StorageNumber, I <: StorageInteger}
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group
    matrix_group = create_group(columns_axis_group, name)

    nrows = Formats.format_axis_length(h5df, rows_axis)
    ncols = Formats.format_axis_length(h5df, columns_axis)

    colptr_dataset = create_dataset(matrix_group, "colptr", indtype, ncols + 1)
    rowval_dataset = create_dataset(matrix_group, "rowval", indtype, Int(nnz))
    nzval_dataset = create_dataset(matrix_group, "nzval", eltype, Int(nnz))

    colptr_dataset[:] = nnz + 1
    colptr_dataset[1] = 1
    rowval_dataset[1] = 0
    nzval_dataset[1] = 0

    colptr_vector, colptr_cache_type = dataset_as_vector(colptr_dataset)
    rowval_vector, rowval_cache_type = dataset_as_vector(rowval_dataset)
    nzval_vector, nzval_cache_type = dataset_as_vector(nzval_dataset)

    close(colptr_dataset)
    close(rowval_dataset)
    close(nzval_dataset)
    close(matrix_group)

    sparse_matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
    Formats.cache_matrix!(
        h5df,
        rows_axis,
        columns_axis,
        name,
        sparse_matrix,
        Formats.combined_cache_type(colptr_cache_type, rowval_cache_type, nzval_cache_type),
    )
    return sparse_matrix
end

function Formats.format_relayout_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    matrix = Formats.get_matrix_through_cache(h5df, rows_axis, columns_axis, name)

    if matrix isa SparseMatrixCSC
        relayout_matrix = Formats.format_empty_sparse_matrix!(
            h5df,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(matrix.colptr),
        )
    else
        relayout_matrix = Formats.format_empty_dense_matrix!(h5df, columns_axis, rows_axis, name, eltype(matrix))
    end

    relayout!(transpose(relayout_matrix), matrix)
    return nothing
end

function Formats.format_delete_matrix!(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,
)::Nothing
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    delete_object(columns_axis_group, name)
    return nothing
end

function Formats.format_matrix_names(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractStringSet
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    names = Set(keys(columns_axis_group))
    Formats.cache_matrix_names!(h5df, rows_axis, columns_axis, names, MemoryData)
    return names
end

function Formats.format_get_matrix(
    h5df::H5df,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    matrices_group = h5df.root["matrices"]
    @assert matrices_group isa HDF5.Group

    rows_axis_group = matrices_group[rows_axis]
    @assert rows_axis_group isa HDF5.Group

    columns_axis_group = rows_axis_group[columns_axis]
    @assert columns_axis_group isa HDF5.Group

    matrix_object = columns_axis_group[name]
    if matrix_object isa HDF5.Dataset
        matrix, cache_type = dataset_as_matrix(matrix_object)
        Formats.cache_matrix!(h5df, rows_axis, columns_axis, name, matrix, cache_type)
        return matrix

    else
        @assert matrix_object isa HDF5.Group

        colptr_dataset = matrix_object["colptr"]
        @assert colptr_dataset isa HDF5.Dataset
        colptr_vector, colptr_cache_type = dataset_as_vector(colptr_dataset)

        rowval_dataset = matrix_object["rowval"]
        @assert rowval_dataset isa HDF5.Dataset
        rowval_vector, rowval_cache_type = dataset_as_vector(rowval_dataset)

        nzval_dataset = matrix_object["nzval"]
        @assert nzval_dataset isa HDF5.Dataset
        nzval_vector, nzval_cache_type = dataset_as_vector(nzval_dataset)

        nrows = Formats.format_axis_length(h5df, rows_axis)
        ncols = Formats.format_axis_length(h5df, columns_axis)
        sparse_matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
        Formats.cache_matrix!(
            h5df,
            rows_axis,
            columns_axis,
            name,
            sparse_matrix,
            Formats.combined_cache_type(colptr_cache_type, rowval_cache_type, nzval_cache_type),
        )
        return sparse_matrix
    end
end

function dataset_as_vector(dataset::HDF5.Dataset)::Tuple{StorageVector, CacheType}
    if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset)
        return (HDF5.readmmap(dataset), MappedData)
    else
        return (read(dataset), MemoryData)
    end
end

function dataset_as_matrix(dataset::HDF5.Dataset)::Tuple{StorageMatrix, CacheType}
    if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset)
        return (HDF5.readmmap(dataset), MappedData)
    else
        return (read(dataset), MemoryData)  # untested
    end
end

# function dump_tree(name::AbstractString, root::Union{HDF5.File, HDF5.Group}, indent = "")::Nothing
#     println("TODO X $(indent)$(name):")
#     indent *= "  "
#     for key in keys(root)
#         value = root[key]
#         if value isa HDF5.Dataset
#             value = read(value)
#             println("TODO X $(indent)$(key): $(typeof(value)) $(value)")
#         else
#             dump_tree(key, value, indent)
#         end
#    end
# end

end
