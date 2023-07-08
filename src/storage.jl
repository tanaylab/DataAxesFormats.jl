"""
Storage objects provide low-level API for storing data in specific formats. To extend `Daf` to support an additional
format, create a new concrete implementation of [`AbstractStorage`](@ref) for that format.

A storage object contains some named scalar data, a set of axes (each with a unique name for each entry), and named
vector and matrix data based on these axes.

Data properties are identified by a unique name given the axes it is based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each specific pair of axes.

For matrices, we keep careful track of their [`MatrixLayouts`](@ref). A storage will only list matrix data under the
axis order that describes its layout (that is, storage formats only deal with column-major matrices). In theory a
storage may hold two copies of the same matrix in both possible memory layouts, in which case it will be listed twice,
under both axes orders.
"""
module Storage

export AbstractStorage
export add_axis!
export axis_length
export axis_names
export delete_axis!
export delete_matrix!
export delete_scalar!
export delete_vector!
export empty_dense_matrix!
export empty_dense_vector!
export empty_sparse_matrix!
export empty_sparse_vector!
export get_axis
export get_matrix
export get_scalar
export get_vector
export has_axis
export has_matrix
export has_scalar
export has_vector
export matrix_names
export scalar_names
export set_matrix!
export set_scalar!
export set_vector!
export unsafe_add_axis!
export unsafe_axis_length
export unsafe_delete_axis!
export unsafe_delete_matrix!
export unsafe_delete_scalar!
export unsafe_delete_vector!
export unsafe_empty_dense_vector!
export unsafe_empty_sparse_vector!
export unsafe_empty_dense_matrix!
export unsafe_empty_sparse_matrix!
export unsafe_get_axis
export unsafe_get_matrix
export unsafe_get_scalar
export unsafe_get_vector
export unsafe_has_matrix
export unsafe_has_vector
export unsafe_matrix_names
export unsafe_set_matrix!
export unsafe_set_scalar!
export unsafe_set_vector!
export unsafe_vector_names
export vector_names

using Daf.DataTypes
using Daf.MatrixLayouts
using Daf.Messages
using SparseArrays

import Daf.DataTypes.require_storage_matrix
import Daf.DataTypes.require_storage_vector

"""
An abstract interface for all `Daf` storage formats.

We require each storage to have a human-readable `.name::String` property for error messages and the like. This name
should be unique, using [`unique_name`](@ref).

To implement a new concrete storage format adapter, you will need to provide a `.name::String` property, and the
[`has_scalar`](@ref) and [`has_axis`](@ref) functions listed below. In addition, you will need to implement the "unsafe"
variant of the rest of the functions, which are listed in [`Concrete storage`] below. This implementation can ignore
most error conditions because the "safe" version of the functions performs most validations first, before calling the
"unsafe" variant.

In general, storage functionality is as "dumb" as possible, to make it easier to create new storage format adapters.
That is, the required functions implement a glorified key-value repositories, with the absolutely minimal necessary
logic to deal with the separate namespaces listed above. Most of this logic is common to all storage formats, and is
contained in the base functions so the concrete storage format adapters are even simpler.
"""
abstract type AbstractStorage end

"""
    has_scalar(storage::AbstractStorage, name::String)::Bool

Check whether a scalar property with some `name` exists in the `storage`.
"""
function has_scalar(storage::AbstractStorage, name::String)::Bool  # untested
    return error("missing method: has_scalar\nfor storage type: $(typeof(storage))")
end

"""
    set_scalar!(
        storage::AbstractStorage,
        name::String,
        value::StorageScalar
        [; overwrite::Bool]
    )::Nothing

Set the `value` of a scalar property with some `name` in the `storage`.

If `overwrite` is `false` (the default), this first verifies the `name` scalar property does not exist.
"""
function set_scalar!(storage::AbstractStorage, name::String, value::StorageScalar; overwrite::Bool = false)::Nothing
    if !overwrite
        require_no_scalar(storage, name)
    end

    unsafe_set_scalar!(storage, name, value)
    return nothing
end

"""
    unsafe_set_scalar!(
        storage::AbstractStorage,
        name::String,
        value::StorageScalar,
    )::Nothing

Implement setting the `value` of a scalar property with some `name` in the `storage`.

This will silently overwrite an existing `value` for the same `name` scalar property.
"""
function unsafe_set_scalar!(storage::AbstractStorage, name::String, value::StorageScalar)::Nothing  # untested
    return error("missing method: unsafe_set_scalar!\nfor storage type: $(typeof(storage))")
end

"""
    delete_scalar!(
        storage::AbstractStorage,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete a scalar property with some `name` from the `storage`.

If `must_exist` is `true` (the default), this first verifies the `name` scalar property exists in the `storage`.
"""
function delete_scalar!(storage::AbstractStorage, name::String; must_exist::Bool = true)::Nothing
    if must_exist
        require_scalar(storage, name)
    elseif !has_scalar(storage, name)
        return nothing
    end

    unsafe_delete_scalar!(storage, name)
    return nothing
end

"""
    unsafe_delete_scalar!(storage::AbstractStorage, name::String)::Nothing

Implement deleting a scalar property with some `name` from `storage`.

This trusts that the `name` scalar property exists in the `storage`.
"""
function unsafe_delete_scalar!(storage::AbstractStorage, name::String)::Nothing  # untested
    return error("missing method: unsafe_delete_scalar!\nfor storage type: $(typeof(storage))")
end

"""
    scalar_names(storage::AbstractStorage)::Set{String}

The names of the scalar properties in the `storage`.
"""
function scalar_names(storage::AbstractStorage)::AbstractSet{String}  # untested
    return error("missing method: scalar_names\nfor storage type: $(typeof(storage))")
end

"""
    get_scalar(storage::AbstractStorage, name::String[; default::StorageScalar])::StorageScalar

Get the value of a scalar property with some `name` in the `storage`.

If `default` is not specified, this first verifies the `name` scalar property exists in the `storage`.
"""
function get_scalar(
    storage::AbstractStorage,
    name::String;
    default::Union{StorageScalar, Nothing} = nothing,
)::StorageScalar
    if default == nothing
        require_scalar(storage, name)
    elseif !has_scalar(storage, name)
        return default
    end

    return unsafe_get_scalar(storage, name)
end

"""
    unsafe_get_scalar(storage::AbstractStorage, name::String)::StorageScalar

Implement fetching the value of a scalar property with some `name` in the `storage`.

This trusts the `name` scalar property exists in the `storage`.
"""
function unsafe_get_scalar(storage::AbstractStorage, name::String)::StorageScalar  # untested
    return error("missing method: unsafe_get_scalar\nfor storage type: $(typeof(storage))")
end

function require_scalar(storage::AbstractStorage, name::String)::Nothing
    if !has_scalar(storage, name)
        error("missing scalar: $(name)\nin the storage: $(storage.name)")
    end
    return nothing
end

function require_no_scalar(storage::AbstractStorage, name::String)::Nothing
    if has_scalar(storage, name)
        error("existing scalar: $(name)\nin the storage: $(storage.name)")
    end
    return nothing
end

"""
    has_axis(storage::AbstractStorage, axis::String)::Bool

Check whether some `axis` exists in the `storage`.
"""
function has_axis(storage::AbstractStorage, axis::String)::Bool  # untested
    return error("missing method: has_axis\nfor storage type: $(typeof(storage))")
end

"""
    add_axis!(
        storage::AbstractStorage,
        axis::String,
        entries::DenseVector{String}
    )::Nothing

Add a new `axis` to the `storage`.

This first verifies the `axis` does not exist and that the `entries` are unique.
"""
function add_axis!(storage::AbstractStorage, axis::String, entries::DenseVector{String})::Nothing
    require_no_axis(storage, axis)

    if !allunique(entries)
        error("non-unique entries for new axis: $(axis)\nin the storage: $(storage.name)")
    end

    unsafe_add_axis!(storage, axis, entries)
    return nothing
end

"""
    unsafe_add_axis!(
        storage::AbstractStorage,
        axis::String,
        entries::DenseVector{String}
    )::Nothing

Implement adding a new `axis` to `storage`.

This trusts that the `axis` does not already exist in the `storage`, and that the names of the `entries` are unique.
"""
function unsafe_add_axis!(storage::AbstractStorage, axis::String, entries::DenseVector{String})::Nothing  # untested
    return error("missing method: unsafe_add_axis!\nfor storage type: $(typeof(storage))")
end

"""
    delete_axis!(
        storage::AbstractStorage,
        axis::String;
        must_exist::Bool = true,
    )::Nothing

Delete an `axis` from the `storage`. This will also delete any vector or matrix properties that are based on this axis.

If `must_exist` is `true` (the default), this first verifies the `axis` exists in the `storage`.
"""
function delete_axis!(storage::AbstractStorage, axis::String; must_exist::Bool = true)::Nothing
    if must_exist
        require_axis(storage, axis)
    elseif !has_axis(storage, axis)
        return nothing
    end

    for name in vector_names(storage, axis)
        unsafe_delete_vector!(storage, axis, name)
    end

    for other_axis in axis_names(storage)
        for name in matrix_names(storage, axis, other_axis)
            unsafe_delete_matrix!(storage, axis, other_axis, name)
        end
    end

    unsafe_delete_axis!(storage, axis)
    return nothing
end

"""
    unsafe_delete_axis!(
        storage::AbstractStorage,
        axis::String,
    )::Nothing

Implement deleting some `axis` from `storage`.

This trusts that the `axis` exists in the `storage`, and that all properties that are based on this axis have already
been deleted.
"""
function unsafe_delete_axis!(storage::AbstractStorage, axis::String)::Nothing  # untested
    return error("missing method: unsafe_delete_axis!\nfor storage type: $(typeof(storage))")
end

"""
    axis_names(storage::AbstractStorage)::AbstractSet{String}

The names of the axes of the `storage`.
"""
function axis_names(storage::AbstractStorage)::AbstractSet{String}  # untested
    return error("missing method: axis_names\nfor storage type: $(typeof(storage))")
end

"""
    get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}

The unique names of the entries of some `axis` of the `storage`. This is identical to doing [`get_vector`](@ref) for the
special `name` property.

This first verifies the `axis` exists in the `storage`.
"""
function get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}
    require_axis(storage, axis)
    return unsafe_get_axis(storage, axis)
end

"""
    unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}

Implement fetching the unique names of the entries of some `axis` of the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}  # untested
    return error("missing method: unsafe_get_axis\nfor storage type: $(typeof(storage))")
end

"""
    axis_length(storage::AbstractStorage, axis::String)::Int64

The number of entries along the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function axis_length(storage::AbstractStorage, axis::String)::Int64
    require_axis(storage, axis)
    return unsafe_axis_length(storage, axis)
end

"""
    unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64  # untested
    return error("missing method: unsafe_axis_length\nfor storage type: $(typeof(storage))")
end

function require_axis(storage::AbstractStorage, axis::String)::Nothing
    if !has_axis(storage, axis)
        error("missing axis: $(axis)\nin the storage: $(storage.name)")
    end
    return nothing
end

function require_no_axis(storage::AbstractStorage, axis::String)::Nothing
    if has_axis(storage, axis)
        error("existing axis: $(axis)\nin the storage: $(storage.name)")
    end
    return nothing
end

"""
    has_vector(storage::AbstractStorage, axis::String, name::String)::Bool

Check whether a vector property with some `name` exists for the `axis` in the `storage`. This is always true for the
special `name` property.

This first verifies the `axis` exists in the `storage`.
"""
function has_vector(storage::AbstractStorage, axis::String, name::String)::Bool
    require_axis(storage, axis)
    return name == "name" || unsafe_has_vector(storage, axis, name)
end

"""
    has_vector(storage::AbstractStorage, axis::String, name::String)::Bool

Implement checking whether a vector property with some `name` exists for the `axis` in the `storage`.

This trusts the `axis` exists in the `storage` and that the property name isn't `name`.
"""
function unsafe_has_vector(storage::AbstractStorage, axis::String, name::String)::Bool  # untested
    return error("missing method: unsafe_has_vector\nfor storage type: $(typeof(storage))")
end

"""
    set_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        vector::Union{StorageScalar, StorageVector}
        [; overwrite::Bool]
    )::Nothing

Set a vector property with some `name` for some `axis` in the `storage`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This first verifies the `axis` exists in the `storage`, that the property name isn't `name`, and that the `vector` has
the appropriate length. If `overwrite` is `false` (the default), this also verifies the `name` vector does not exist for
the `axis`.
"""
function set_vector!(
    storage::AbstractStorage,
    axis::String,
    name::String,
    vector::Union{StorageScalar, StorageVector};
    overwrite::Bool = false,
)::Nothing
    require_not_name(storage, axis, name)
    require_axis(storage, axis)

    if vector isa AbstractVector
        require_storage_vector(vector)
        require_axis_length(storage, "vector length", length(vector), axis)
    end

    if !overwrite
        require_no_vector(storage, axis, name)
    end

    unsafe_set_vector!(storage, axis, name, vector)
    return nothing
end

"""
    unsafe_set_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        vector::Union{StorageScalar, StorageVector},
    )::Nothing

Implement setting a vector property with some `name` for some `axis` in the `storage`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This trusts the `axis` exists in the `storage`, that the property name isn't `name`, and that the `vector` has the
appropriate length. It will silently overwrite an existing vector for the same `name` for the `axis`.
"""
function unsafe_set_vector!(  # untested
    storage::AbstractStorage,
    axis::String,
    name::String,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    return error("missing method: unsafe_set_vector!\nfor storage type: $(typeof(storage))")
end

"""
    empty_dense_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        eltype::Type{T}
        [; overwrite::Bool]
    )::DenseVector{T} where {T <: Number}

Create an empty dense vector property with some `name` for some `axis` in the `storage`.

The returned vector will be uninitialized; the caller is expected to fill it with values. This saves creating a copy of
the vector before setting it in the storage, which makes a huge difference when creating vectors on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `axis` exists in the `storage` and that the property name isn't `name`. If `overwrite` is
`false` (the default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_dense_vector!(
    storage::AbstractStorage,
    axis::String,
    name::String,
    eltype::Type{T};
    overwrite::Bool = false,
)::DenseVector{T} where {T <: Number}
    require_not_name(storage, axis, name)
    require_axis(storage, axis)

    if !overwrite
        require_no_vector(storage, axis, name)
    end

    return unsafe_empty_dense_vector!(storage, axis, name, eltype)
end

"""
    unsafe_empty_dense_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        eltype::Type{T},
    )::DenseVector where {T <: Number}

Implement setting a vector property with some `name` for some `axis` in the `storage`.

Implement creating an empty dense `matrix` with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This trusts the `axis` exists in the `storage` and that the property name isn't `name`. It will silently overwrite an
existing vector for the same `name` for the `axis`.
"""
function unsafe_empty_dense_vector!(  # untested
    storage::AbstractStorage,
    axis::String,
    name::String,
    eltype::Type{T},
)::DenseVector{T} where {T <: Number}
    return error("missing method: unsafe_empty_dense_vector!\nfor storage type: $(typeof(storage))")
end

"""
    empty_sparse_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        eltype::Type{T},
        nnz::Integer,
        indtype::Type{I}
        [; overwrite::Bool]
    )::DenseVector{T} where {T <: Number, I <: Integer}

Create an empty dense vector property with some `name` for some `axis` in the `storage`.

The returned vector will be uninitialized; the caller is expected to fill it with values. This means manually filling
the `nzind` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow pre-allocating disk
storage. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse vector is created by concatenating several smaller ones; this function
allows doing so directly into the storage vector, avoiding a copy in case of memory-mapped disk storage formats.

!!! warning

    It is the caller's responsibility to fill the three vectors with valid data. **There's no safety net if you mess
    this up**. Specifically, you must ensure:

      - `nzind[1] == 1`
      - `nzind[i] <= nzind[i + 1]`
      - `nzind[end] == nnz`

This first verifies the `axis` exists in the `storage` and that the property name isn't `name`. If `overwrite` is
`false` (the default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_sparse_vector!(
    storage::AbstractStorage,
    axis::String,
    name::String,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I};
    overwrite::Bool = false,
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    require_not_name(storage, axis, name)
    require_axis(storage, axis)

    if !overwrite
        require_no_vector(storage, axis, name)
    end

    return unsafe_empty_sparse_vector!(storage, axis, name, eltype, nnz, indtype)
end

"""
    unsafe_empty_dense_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        eltype::Type{T},
        nnz::Integer,
        indtype::Type{I},
    )::DenseVector where {T <: Number, I <: Integer}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in the
`storage`.

This trusts the `axis` exists in the `storage` and that the property name isn't `name`. It will silently overwrite an
existing vector for the same `name` for the `axis`.
"""
function unsafe_empty_sparse_vector!(  # untested
    storage::AbstractStorage,
    axis::String,
    name::String,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    return error("missing method: unsafe_empty_sparse_vector!\nfor storage type: $(typeof(storage))")
end

"""
    delete_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete a vector property with some `name` for some `axis` from the `storage`.

This first verifies the `axis` exists in the `storage` and that the property name isn't `name`. If `must_exist` is
`true` (the default), this also verifies the `name` vector exists for the `axis`.
"""
function delete_vector!(storage::AbstractStorage, axis::String, name::String; must_exist::Bool = true)::Nothing
    require_not_name(storage, axis, name)
    require_axis(storage, axis)

    if must_exist
        require_vector(storage, axis, name)
    elseif !has_vector(storage, axis, name)
        return nothing
    end

    unsafe_delete_vector!(storage, axis, name)
    return nothing
end

"""
    unsafe_delete_vector!(storage::AbstractStorage, axis::String, name::String)::Nothing

Implement deleting a vector property with some `name` for some `axis` from `storage`.

This trusts the `axis` exists in the `storage`, that the property name isn't `name`, and that the `name` vector exists
for the `axis`.
"""
function unsafe_delete_vector!(storage::AbstractStorage, axis::String, name::String)::Nothing  # untested
    return error("missing method: unsafe_delete_vector! for storage type: $(typeof(storage))")
end

"""
    vector_names(storage::AbstractStorage, axis::String)::Set{String}

The names of the vector properties for the `axis` in the `storage`, **not** including the special `name` property.

This first verifies the `axis` exists in the `storage`.
"""
function vector_names(storage::AbstractStorage, axis::String)::AbstractSet{String}
    require_axis(storage, axis)
    return unsafe_vector_names(storage, axis)
end

"""
    unsafe_vector_names(storage::AbstractStorage, axis::String)::Set{String}

Implement fetching the names of the vectors for the `axis` in the `storage`, **not** including the special `name`
property.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_vector_names(storage::AbstractStorage, axis::String)::AbstractSet{String}  # untested
    return error("missing method: unsafe_vector_names\nfor storage type: $(typeof(storage))")
end

"""
    get_vector(
        storage::AbstractStorage,
        axis::String,
        name::String
        [; default::Union{StorageScalar, StorageVector}]
    )::StorageVector

Get the vector property with some `name` for some `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`. If `default` is not specified, this first verifies the `name`
vector exists in the `storage`. Otherwise, if `default` is a `StorageVector`, it has to be of the same size as the
`axis`, and is returned. Otherwise, a new `Vector` is created of the correct size containing the `default`, and is
returned.
"""
function get_vector(
    storage::AbstractStorage,
    axis::String,
    name::String;
    default::Union{StorageScalar, StorageVector, Nothing} = nothing,
)::StorageVector
    require_axis(storage, axis)

    if name == "name"
        return unsafe_get_axis(storage, axis)
    end

    if default isa AbstractVector
        require_storage_vector(default)
        require_axis_length(storage, "default length", length(default), axis)
    end

    if default == nothing
        require_vector(storage, axis, name)
    elseif !has_vector(storage, axis, name)
        if default isa StorageVector
            return default
        else
            return fill(default, axis_length(storage, axis))
        end
    end

    vector = unsafe_get_vector(storage, axis, name)
    if !(vector isa StorageVector)
        error(  # untested
            "unsafe_get_vector for: $(typeof(storage))\n" * "returned invalid Daf storage vector: $(typeof(vector))",
        )
    end

    if length(vector) != axis_length(storage, axis)
        error( # untested
            "unsafe_get_vector for: $(typeof(storage))\n" *
            "returned vector length: $(length(vector))\n" *
            "instead of axis: $(axis)\n" *
            "length: $(axis_length(storage, axis))\n" *
            "in the storage: $(storage.name)",
        )
    end

    return vector
end

"""
    unsafe_get_vector(storage::AbstractStorage, axis::String, name::String)::StorageVector

Implement fetching the vector property with some `name` for some `axis` in the `storage`.

This trusts the `axis` exists in the `storage`, and the `name` vector exists for the `axis`.
"""
function unsafe_get_vector(storage::AbstractStorage, name::String)::StorageVector  # untested
    return error("missing method: unsafe_get_vector\nfor storage type: $(typeof(storage))")
end

function require_vector(storage::AbstractStorage, axis::String, name::String)::Nothing
    if !has_vector(storage, axis, name)
        error("missing vector: $(name)\nfor the axis: $(axis)\nin the storage: $(storage.name)")
    end
    return nothing
end

function require_no_vector(storage::AbstractStorage, axis::String, name::String)::Nothing
    if has_vector(storage, axis, name)
        error("existing vector: $(name)\nfor the axis: $(axis)\nin the storage: $(storage.name)")
    end
    return nothing
end

"""
    has_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
    )::Bool

Check whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in the `storage`.
Since this is Julia, this means a column-major matrix. A storage may contain two copies of the same data, in which case
it would report the matrix under both axis orders.

This first verifies the `rows_axis` and `columns_axis` exists in the `storage`.
"""
function has_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Bool
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    return unsafe_has_matrix(storage, rows_axis, columns_axis, name)
end

"""
    has_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
    )::Bool

Implement checking whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in the
`storage`.

This trusts the `rows_axis` and the `columns_axis` exist in the `storage`.
"""
function unsafe_has_matrix(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
)::Bool
    return error("missing method: unsafe_has_matrix\nfor storage type: $(typeof(storage))")
end

"""
    set_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        matrix::StorageMatrix
        [; overwrite::Bool]
    )::Nothing

Set the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`. Since this is Julia,
this should be a column-major `matrix`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`, that the `matrix` is column-major of the
appropriate size. If `overwrite` is `false` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function set_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    matrix::Union{StorageScalar, StorageMatrix};
    overwrite::Bool = false,
)::Nothing
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)

    if matrix isa StorageMatrix
        require_storage_matrix(matrix)
        require_column_major(matrix)
        require_axis_length(storage, "matrix rows", size(matrix, Rows), rows_axis)
        require_axis_length(storage, "matrix columns", size(matrix, Columns), columns_axis)
    end

    if !overwrite
        require_no_matrix(storage, rows_axis, columns_axis, name)
    end

    unsafe_set_matrix!(storage, rows_axis, columns_axis, name, matrix)
    return nothing
end

"""
    unsafe_set_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        matrix::StorageMatrix,
    )::Nothing

Implement setting the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and that the `matrix` is column-major of the
appropriate size. It will silently overwrite an existing matrix for the same `name` for the `rows_axis` and
`columns_axis`.
"""
function unsafe_set_matrix!(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    matrix::Union{StorageScalar, StorageMatrix},
)::Nothing
    return error("missing method: unsafe_set_matrix!\nfor storage type: $(typeof(storage))")
end

"""
    empty_dense_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        eltype::Type{T}
        [; overwrite::Bool]
    )::DenseMatrix{T} where {T <: Number}

Create an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`. Since
this is Julia, this will be a column-major `matrix`.

The returned matrix will be uninitialized; the caller is expected to fill it with values. This saves creating a copy of
the matrix before setting it in the storage, which makes a huge difference when creating matrices on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`, that the `matrix` is column-major of the
appropriate size. If `overwrite` is `false` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function empty_dense_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    eltype::Type{T};
    overwrite::Bool = false,
)::DenseMatrix{T} where {T <: Number}
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)

    if !overwrite
        require_no_matrix(storage, rows_axis, columns_axis, name)
    end

    return unsafe_empty_dense_matrix!(storage, rows_axis, columns_axis, name, eltype)
end

"""
    unsafe_empty_dense_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        eltype::Type{T},
    )::DenseMatrix{T} where {T <: Number}

Implement creating an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in the
`storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`. It will silently overwrite an existing matrix for
the same `name` for the `rows_axis` and `columns_axis`.
"""
function unsafe_empty_dense_matrix!(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    eltype::Type{T},
)::DenseMatrix{T} where {T <: Number}
    return error("missing method: unsafe_empty_dense_matrix!\nfor storage type: $(typeof(storage))")
end

"""
    empty_sparse_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        eltype::Type{T},
        nnz::Integer,
        intdype::Type{I}
        [; overwrite::Bool]
    )::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}

Create an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

The returned matrix will be uninitialized; the caller is expected to fill it with values. This means manually filling
the `colptr`, `rowval` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow
pre-allocating disk storage. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse matrix is created by concatenating several smaller ones; this function
allows doing so directly into the storage matrix, avoiding a copy in case of memory-mapped disk storage formats.

!!! warning

    It is the caller's responsibility to fill the three vectors with valid data. **There's no safety net if you mess
    this up**. Specifically, you must ensure:

      - `colptr[1] == 1`
      - `colptr[end] == nnz + 1`
      - `colptr[i] <= colptr[i + 1]`
      - for all `j`, for all `i` such that `colptr[j] <= i` and `i + 1 < colptr[j + 1]`, `1 <= rowptr[i] < rowptr[i + 1] <= nrows`

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`. If `overwrite` is `false` (the default),
this also verifies the `name` matrix does not exist for the `rows_axis` and `columns_axis`.
"""
function empty_sparse_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I};
    overwrite::Bool = false,
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)

    if !overwrite
        require_no_matrix(storage, rows_axis, columns_axis, name)
    end

    return unsafe_empty_sparse_matrix!(storage, rows_axis, columns_axis, name, eltype, nnz, indtype)
end

"""
    unsafe_empty_dense_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        eltype::Type{T},
        intdype::Type{I},
        nnz::Integer,
    )::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in the
`storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`. It will silently overwrite an existing matrix for
the same `name` for the `rows_axis` and `columns_axis`.
"""
function unsafe_empty_sparse_matrix!(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    return error("missing method: unsafe_empty_sparse_matrix!\nfor storage type: $(typeof(storage))")
end

"""
    delete_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete a matrix property with some `name` for some `rows_axis` and `columns_axis` from the `storage`.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`. If `must_exist` is `true` (the default),
this also verifies the `name` matrix exists for the `rows_axis` and `columns_axis`.
"""
function delete_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String;
    must_exist::Bool = true,
)::Nothing
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)

    if must_exist
        require_matrix(storage, rows_axis, columns_axis, name)
    elseif !has_matrix(storage, rows_axis, columns_axis, name)
        return nothing
    end

    unsafe_delete_matrix!(storage, rows_axis, columns_axis, name)
    return nothing
end

"""
    unsafe_delete_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String
    )::Nothing

Implement deleting a matrix property with some `name` for some `rows_axis` and `columns_axis` from `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and that the `name` matrix exists for the
`rows_axis` and `columns_axis`.
"""
function unsafe_delete_matrix!(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
)::Nothing
    return error("missing method: unsafe_delete_matrix!\nfor storage type: $(typeof(storage))")
end

"""
    matrix_names(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
    )::Set{String}

The names of the matrix properties for the `rows_axis` and `columns_axis` in the `storage`.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`.
"""
function matrix_names(storage::AbstractStorage, rows_axis::String, columns_axis::String)::AbstractSet{String}
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    return unsafe_matrix_names(storage, rows_axis, columns_axis)
end

"""
    unsafe_matrix_names(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
    )::Set{String}

Implement fetching the names of the matrix properties for the `rows_axis` and `columns_axis` in the `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`.
"""
function unsafe_matrix_names(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
)::AbstractSet{String}
    return error("missing method: unsafe_matrix_names\nfor storage type: $(typeof(storage))")
end

function require_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Nothing
    if !has_matrix(storage, rows_axis, columns_axis, name)
        error(
            "missing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the storage: $(storage.name)",
        )
    end
    return nothing
end

function require_no_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Nothing
    if has_matrix(storage, rows_axis, columns_axis, name)
        error(
            "existing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the storage: $(storage.name)",
        )
    end
    return nothing
end

"""
    get_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String
        [; default::Union{StorageScalar, StorageMatrix}]
    )::StorageMatrix

get the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`. If `default` is not specified, this first
verifies the `name` matrix exists in the `storage`. Otherwise, if `default` is a `StorageMatrix`, it has to be of the
same size as the `rows_axis` and `columns_axis`, and is returned. Otherwise, a new `Matrix` is created of the correct
size containing the `default`, and is returned.
"""
function get_matrix(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String;
    default::Union{StorageScalar, StorageMatrix, Nothing} = nothing,
)::StorageMatrix
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)

    if default isa StorageMatrix
        require_storage_matrix(default)
        require_column_major(default)
        require_axis_length(storage, "default rows", size(default, Rows), rows_axis)
        require_axis_length(storage, "default columns", size(default, Columns), columns_axis)
    end

    if default == nothing
        require_matrix(storage, rows_axis, columns_axis, name)
    elseif !has_matrix(storage, rows_axis, columns_axis, name)
        if default isa StorageMatrix
            return default
        else
            return fill(default, axis_length(storage, rows_axis), axis_length(storage, columns_axis))
        end
    end

    matrix = unsafe_get_matrix(storage, rows_axis, columns_axis, name)
    if !(matrix isa StorageMatrix)
        error( # untested
            "unsafe_get_matrix for: $(typeof(storage))\n" * "returned invalid Daf storage matrix: $(typeof(matrix))",
        )
    end

    if size(matrix, Rows) != axis_length(storage, rows_axis)
        error( # untested
            "unsafe_get_matrix for: $(typeof(storage))\n" *
            "returned matrix rows: $(size(matrix, Rows))\n" *
            "instead of axis: $(axis)\n" *
            "length: $(axis_length(storage, rows_axis))\n" *
            "in the storage: $(storage.name)",
        )
    end

    if size(matrix, Columns) != axis_length(storage, columns_axis)
        error( # untested
            "unsafe_get_matrix for: $(typeof(storage))\n" *
            "returned matrix columns: $(size(matrix, Columns))\n" *
            "instead of axis: $(axis)\n" *
            "length: $(axis_length(storage, columns_axis))\n" *
            "in the storage: $(storage.name)",
        )
    end

    if major_axis(matrix) != Columns
        error( # untested
            "unsafe_get_matrix for: $(typeof(storage))\n" * "returned non column-major matrix: $(typeof(matrix))",
        )
    end

    return matrix
end

"""
    unsafe_get_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String
    )::StorageMatrix

Implement fetching the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and the `name` matrix exists for the `rows_axis`
and `columns_axis`.
"""
function unsafe_get_matrix(  # untested
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
)::StorageMatrix
    return error("missing method: unsafe_get_matrix\nfor storage type: $(typeof(storage))")
end

function require_column_major(matrix::StorageMatrix)::Nothing
    if major_axis(matrix) != Columns
        error("type: $(typeof(matrix)) is not in column-major layout")
    end
end

function require_axis_length(storage::AbstractStorage, what_name::String, what_length::Integer, axis::String)::Nothing
    if what_length != axis_length(storage, axis)
        error(
            "$(what_name): $(what_length)\n" *
            "is different from the length: $(axis_length(storage, axis))\n" *
            "of the axis: $(axis)\n" *
            "in the storage: $(storage.name)",
        )
    end
    return nothing
end

function require_not_name(storage::AbstractStorage, axis::String, name::String)::Nothing
    if name == "name"
        error("setting the reserved property: name\n" * "for the axis: $(axis)\n" * "in the storage: $(storage.name)")
    end
    return nothing
end

include("storage/memory.jl")

end # module
