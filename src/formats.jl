"""
The storage format is a low-level API for storing `Daf` data. To extend `Daf` to support an additional format, create a
new implementation of this API.

!!! note

    When implementing a new `Daf` storage format, you should write `struct MyContainer <: Container`. This will also
    provide all the high-level API functions on objects of `MyContainer`, so they will be directly usable by application
    code. If you only say `struct MyFormat <: Format` then the high-level API functions will *not* be available. We only
    created the [`Format`](@ref) abstract type to clarify the minimal set of functions required to implement a new
    format.

A storage format object contains some named scalar data, a set of axes (each with a unique name for each entry), and
named vector and matrix data based on these axes.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each (ordered) pair of axes.

For matrices, we keep careful track of their [`MatrixLayouts`](@ref). Specifically, a storage format only deals with
column-major matrices, listed under the rows axis first and the columns axis second. A storage format object may hold
two copies of the same matrix, in both possible memory layouts, in which case it will be listed twice, under both axes
orders.

The storage format API is intentionally low-level and would only be used outside the `Daf` package if defining an
additional storage format. We therefore do not reexport anything from this module under the top-level `Daf` namespace.
"""
module Formats

export Internal
export Format
export format_add_axis!
export format_axis_length
export format_axis_names
export format_delete_axis!
export format_delete_matrix!
export format_delete_scalar!
export format_delete_vector!
export format_empty_dense_matrix!
export format_empty_dense_vector!
export format_empty_sparse_matrix!
export format_empty_sparse_vector!
export format_get_axis
export format_get_matrix
export format_get_scalar
export format_get_vector
export format_has_axis
export format_has_matrix
export format_has_matrix
export format_has_scalar
export format_has_vector
export format_has_vector
export format_matrix_names
export format_scalar_names
export format_set_matrix!
export format_set_scalar!
export format_set_vector!
export format_vector_names

using Daf.DataTypes
using Daf.MatrixLayouts
using Daf.Messages
using SparseArrays

"""
    Internal(name::AbstractString)

Internal data we need to keep in any concrete [`Format`](@ref). This has to be available as a `.internal` data member of the
concrete format. This enables all the high-level `Container` functions.

The constructor will automatically call [`unique_name`](@ref) to try and make the names unique for improved error
messages.
"""
struct Internal
    name::String

    function Internal(name::AbstractString)::Internal
        return new(unique_name(name))
    end
end

"""
An abstract interface for all `Daf` storage formats.

We require each storage format to have a `.internal::`[`Internal`](@ref) property. This enables all the high-level
`Container` functions.

We also require each storage format to have a `.cache::Dict{String,Any}` property which we'll use to cache
[`relayout!`](@ref) and query results.

Finally, each storage format must implement the functions listed below.

In general, storage format objects are as "dumb" as possible, to make it easier to support new storage formats. The
required functions implement a glorified key-value repository, with the absolutely minimal necessary logic to deal with
the separate namespaces listed above. Most of this logic is common to all storage formats, and is provided in the
`Container` functions, so the storage format functions are as simple as possible.
"""
abstract type Format end

"""
    format_has_scalar(storage::Format, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in the `storage`.
"""
function format_has_scalar(storage::Format, name::AbstractString)::Bool  # untested
    return error("missing method: format_has_scalar\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_set_scalar!(
        storage::Format,
        name::AbstractString,
        value::StorageScalar,
    )::Nothing

Implement setting the `value` of a scalar property with some `name` in the `storage`.

This will silently overwrite an existing `value` for the same `name` scalar property.
"""
function format_set_scalar!(storage::Format, name::AbstractString, value::StorageScalar)::Nothing  # untested
    return error("missing method: format_set_scalar!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_delete_scalar!(storage::Format, name::AbstractString)::Nothing

Implement deleting a scalar property with some `name` from `storage`.

This trusts that the `name` scalar property exists in the `storage`.
"""
function format_delete_scalar!(storage::Format, name::AbstractString)::Nothing  # untested
    return error("missing method: format_delete_scalar!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_scalar_names(storage::Format)::Set{String}

The names of the scalar properties in the `storage`.
"""
function format_scalar_names(storage::Format)::AbstractSet{String}  # untested
    return error("missing method: format_scalar_names\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_get_scalar(storage::Format, name::AbstractString)::StorageScalar

Implement fetching the value of a scalar property with some `name` in the `storage`.

This trusts the `name` scalar property exists in the `storage`.
"""
function format_get_scalar(storage::Format, name::AbstractString)::StorageScalar  # untested
    return error("missing method: format_get_scalar\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_has_axis(storage::Format, axis::AbstractString)::Bool

Check whether some `axis` exists in the `storage`.
"""
function format_has_axis(storage::Format, axis::AbstractString)::Bool  # untested
    return error("missing method: format_has_axis\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_add_axis!(
        storage::Format,
        axis::AbstractString,
        entries::DenseVector{String}
    )::Nothing

Implement adding a new `axis` to `storage`.

This trusts that the `axis` does not already exist in the `storage`, and that the names of the `entries` are unique.
"""
function format_add_axis!(  # untested
    storage::Format,
    axis::AbstractString,
    entries::DenseVector{String},
)::Nothing
    return error("missing method: format_add_axis!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_delete_axis!(
        storage::Format,
        axis::AbstractString,
    )::Nothing

Implement deleting some `axis` from `storage`.

This trusts that the `axis` exists in the `storage`, and that all properties that are based on this axis have already
been deleted.
"""
function format_delete_axis!(storage::Format, axis::AbstractString)::Nothing  # untested
    return error("missing method: format_delete_axis!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_axis_names(storage::Format)::AbstractSet{String}

The names of the axes of the `storage`.
"""
function format_axis_names(storage::Format)::AbstractSet{String}  # untested
    return error("missing method: format_axis_names\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_get_axis(storage::Format, axis::AbstractString)::DenseVector{String}

Implement fetching the unique names of the entries of some `axis` of the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function format_get_axis(storage::Format, axis::AbstractString)::DenseVector{String}  # untested
    return error("missing method: format_get_axis\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_axis_length(storage::Format, axis::AbstractString)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` exists in the `storage`.
"""
function format_axis_length(storage::Format, axis::AbstractString)::Int64  # untested
    return error("missing method: format_axis_length\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_has_vector(storage::Format, axis::AbstractString, name::AbstractString)::Bool

Implement checking whether a vector property with some `name` exists for the `axis` in the `storage`.

This trusts the `axis` exists in the `storage` and that the property name isn't `name`.
"""
function format_has_vector(storage::Format, axis::AbstractString, name::AbstractString)::Bool  # untested
    return error("missing method: format_has_vector\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_set_vector!(
        storage::Format,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector},
    )::Nothing

Implement setting a vector property with some `name` for some `axis` in the `storage`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This trusts the `axis` exists in the `storage`, that the property name isn't `name`, and that the `vector` has the
appropriate length. It will silently overwrite an existing vector for the same `name` for the `axis`.
"""
function format_set_vector!(  # untested
    storage::Format,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    return error("missing method: format_set_vector!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_empty_dense_vector!(
        storage::Format,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::DenseVector where {T <: Number}

Implement setting a vector property with some `name` for some `axis` in the `storage`.

Implement creating an empty dense `matrix` with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This trusts the `axis` exists in the `storage` and that the property name isn't `name`. It will silently overwrite an
existing vector for the same `name` for the `axis`.
"""
function format_empty_dense_vector!(  # untested
    storage::Format,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::DenseVector{T} where {T <: Number}
    return error("missing method: format_empty_dense_vector!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_empty_dense_vector!(
        storage::Format,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::Integer,
        indtype::Type{I},
    )::DenseVector where {T <: Number, I <: Integer}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in the
`storage`.

This trusts the `axis` exists in the `storage` and that the property name isn't `name`. It will silently overwrite an
existing vector for the same `name` for the `axis`.
"""
function format_empty_sparse_vector!(  # untested
    storage::Format,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    return error("missing method: format_empty_sparse_vector!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_delete_vector!(storage::Format, axis::AbstractString, name::AbstractString)::Nothing

Implement deleting a vector property with some `name` for some `axis` from `storage`.

This trusts the `axis` exists in the `storage`, that the property name isn't `name`, and that the `name` vector exists
for the `axis`.
"""
function format_delete_vector!(storage::Format, axis::AbstractString, name::AbstractString)::Nothing  # untested
    return error("missing method: format_delete_vector! for Daf.Format: $(typeof(storage))")
end

"""
    format_vector_names(storage::Format, axis::AbstractString)::Set{String}

Implement fetching the names of the vectors for the `axis` in the `storage`, **not** including the special `name`
property.

This trusts the `axis` exists in the `storage`.
"""
function format_vector_names(storage::Format, axis::AbstractString)::AbstractSet{String}  # untested
    return error("missing method: format_vector_names\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_get_vector(storage::Format, axis::AbstractString, name::AbstractString)::StorageVector

Implement fetching the vector property with some `name` for some `axis` in the `storage`.

This trusts the `axis` exists in the `storage`, and the `name` vector exists for the `axis`.
"""
function format_get_vector(storage::Format, name::AbstractString)::StorageVector  # untested
    return error("missing method: format_get_vector\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_has_matrix(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
    )::Bool

Implement checking whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in the
`storage`.

This trusts the `rows_axis` and the `columns_axis` exist in the `storage`.
"""
function format_has_matrix(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return error("missing method: format_has_matrix\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_set_matrix!(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::StorageMatrix,
    )::Nothing

Implement setting the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and that the `matrix` is column-major of the
appropriate size. It will silently overwrite an existing matrix for the same `name` for the `rows_axis` and
`columns_axis`.
"""
function format_set_matrix!(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalar, StorageMatrix},
)::Nothing
    return error("missing method: format_set_matrix!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_empty_dense_matrix!(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::DenseMatrix{T} where {T <: Number}

Implement creating an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in the
`storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`. It will silently overwrite an existing matrix for
the same `name` for the `rows_axis` and `columns_axis`.
"""
function format_empty_dense_matrix!(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    eltype::Type{T},
)::DenseMatrix{T} where {T <: Number}
    return error("missing method: format_empty_dense_matrix!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_empty_dense_matrix!(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        intdype::Type{I},
        nnz::Integer,
    )::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in the
`storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`. It will silently overwrite an existing matrix for
the same `name` for the `rows_axis` and `columns_axis`.
"""
function format_empty_sparse_matrix!(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    return error("missing method: format_empty_sparse_matrix!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_delete_matrix!(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::Nothing

Implement deleting a matrix property with some `name` for some `rows_axis` and `columns_axis` from `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and that the `name` matrix exists for the
`rows_axis` and `columns_axis`.
"""
function format_delete_matrix!(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    return error("missing method: format_delete_matrix!\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_matrix_names(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
    )::Set{String}

Implement fetching the names of the matrix properties for the `rows_axis` and `columns_axis` in the `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`.
"""
function format_matrix_names(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    return error("missing method: format_matrix_names\nfor Daf.Format: $(typeof(storage))")
end

"""
    format_get_matrix(
        storage::Format,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::StorageMatrix

Implement fetching the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and the `name` matrix exists for the `rows_axis`
and `columns_axis`.
"""
function format_get_matrix(  # untested
    storage::Format,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    return error("missing method: format_get_matrix\nfor Daf.Format: $(typeof(storage))")
end

end # module
