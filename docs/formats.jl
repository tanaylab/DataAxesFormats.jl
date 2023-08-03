"""
The [`ReadFormat`](@ref) and [`WriteFormat`](@ref) interfaces specify a low-level API for storing `Daf` data. To extend
`Daf` to support an additional format, create a new implementation of this API.

A storage format object contains some named scalar data, a set of axes (each with a unique name for each entry), and
named vector and matrix data based on these axes.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each (ordered) pair of axes.

For matrices, we keep careful track of their [`MatrixLayouts`](@ref). Specifically, a storage format only deals with
column-major matrices, listed under the rows axis first and the columns axis second. A storage format object may hold
two copies of the same matrix, in both possible memory layouts, in which case it will be listed twice, under both axes
orders.

In general, storage format objects are as "dumb" as possible, to make it easier to support new storage formats. The
required functions implement a glorified key-value repository, with the absolutely minimal necessary logic to deal with
the separate property namespaces listed above.

For clarity of documentation, we split the type hierarchy to `WriteDaf <: WriteFormat <: ReadDaf <: ReadFormat`.

The functions listed here use the [`ReadFormat`](@ref) for read-only operations and [`WriteFormat`](@ref) for write
operations into a `Daf` storage. This is a low-level API, not meant to be used from outside the package, and therefore
is not re-exported from the top-level `Daf` namespace.

In contrast, the functions using [`ReadDaf`](@ref) and [`WriteDaf`](@ref) describe the high-level API meant to be used
from outside the package, and are re-exported. These functions are listed in the `Daf.Data` module. These functions
provide all the logic common to any storage format, allowing us to keep the format-specific functions as simple as
possible.

That is, when implementing a new `Daf` storage format, you should write `struct MyFormat <: WriteDaf`, and implement the
functions listed here for both [`ReadFormat`](@ref) and [`WriteFormat`](@ref).
"""
module Formats

export ReadDaf
export WriteDaf

using Daf.MatrixLayouts
using Daf.Messages
using Daf.StorageTypes
using OrderedCollections
using SparseArrays

"""
    Internal(name::AbstractString)

Internal data we need to keep in any concrete [`ReadFormat`](@ref). This has to be available as a `.internal` data
member of the concrete format. This enables all the high-level [`ReadDaf`](@ref) and [`WriteDaf`](@ref) functions.

The constructor will automatically call [`unique_name`](@ref) to try and make the names unique for improved error
messages.
"""
struct Internal
    name::String
    is_read_only::Vector{Bool}
    axes::Dict{String, OrderedDict{String, Int64}}
end

function Internal(name::AbstractString; is_read_only::Bool = false)::Internal
    return Internal(unique_name(name), [is_read_only], Dict{String, OrderedDict{String, Int64}}())
end

"""
An low-level abstract interface for reading from `Daf` storage formats.

We require each storage format to have a `.internal::`[`Internal`](@ref) property. This enables all the high-level
`ReadDaf` functions.

Each storage format must implement the functions listed below for reading from the storage.
"""
abstract type ReadFormat end

"""
A  high-level abstract interface for read-only access to `Daf` data.

All the functions for this type are provided based on the functions required for [`ReadFormat`](@ref). See the
`Daf.Data` module for their description.
"""
abstract type ReadDaf <: ReadFormat end

"""
An abstract interface for writing into `Daf` storage formats.

Each storage format must implement the functions listed below for writing into the storage.
"""
abstract type WriteFormat <: ReadDaf end

"""
A  high-level abstract interface for write access to `Daf` data.

All the functions for this type are provided based on the functions required for [`WriteFormat`](@ref). See the
`Daf.Data` module for their description.
"""
abstract type WriteDaf <: WriteFormat end

"""
    format_has_scalar(daf::ReadFormat, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in `daf`.
"""
function format_has_scalar(daf::ReadFormat, name::AbstractString)::Bool  # untested
    return error("missing method: format_has_scalar\nfor the daf format: $(typeof(daf))")
end

"""
    format_set_scalar!(
        daf::WriteFormat,
        name::AbstractString,
        value::StorageScalar,
    )::Nothing

Implement setting the `value` of a scalar property with some `name` in `daf`.

This will silently overwrite an existing `value` for the same `name` scalar property.
"""
function format_set_scalar!(daf::WriteFormat, name::AbstractString, value::StorageScalar)::Nothing  # untested
    return error("missing method: format_set_scalar!\nfor the daf format: $(typeof(daf))")
end

"""
    format_delete_scalar!(daf::WriteFormat, name::AbstractString)::Nothing

Implement deleting a scalar property with some `name` from `daf`.

This trusts that the `name` scalar property exists in `daf`.
"""
function format_delete_scalar!(daf::WriteFormat, name::AbstractString)::Nothing  # untested
    return error("missing method: format_delete_scalar!\nfor the daf format: $(typeof(daf))")
end

"""
    format_scalar_names(daf::ReadFormat)::Set{String}

The names of the scalar properties in `daf`.
"""
function format_scalar_names(daf::ReadFormat)::AbstractSet{String}  # untested
    return error("missing method: format_scalar_names\nfor the daf format: $(typeof(daf))")
end

"""
    format_get_scalar(daf::ReadFormat, name::AbstractString)::StorageScalar

Implement fetching the value of a scalar property with some `name` in `daf`.

This trusts the `name` scalar property exists in `daf`.
"""
function format_get_scalar(daf::ReadFormat, name::AbstractString)::StorageScalar  # untested
    return error("missing method: format_get_scalar\nfor the daf format: $(typeof(daf))")
end

"""
    format_has_axis(daf::ReadFormat, axis::AbstractString)::Bool

Check whether some `axis` exists in `daf`.
"""
function format_has_axis(daf::ReadFormat, axis::AbstractString)::Bool  # untested
    return error("missing method: format_has_axis\nfor the daf format: $(typeof(daf))")
end

"""
    format_add_axis!(
        daf::WriteFormat,
        axis::AbstractString,
        entries::DenseVector{String}
    )::Nothing

Implement adding a new `axis` to `daf`.

This trusts that the `axis` does not already exist in `daf`, and that the names of the `entries` are unique.
"""
function format_add_axis!(  # untested
    daf::WriteFormat,
    axis::AbstractString,
    entries::DenseVector{String},
)::Nothing
    return error("missing method: format_add_axis!\nfor the daf format: $(typeof(daf))")
end

"""
    format_delete_axis!(
        daf::WriteFormat,
        axis::AbstractString,
    )::Nothing

Implement deleting some `axis` from `daf`.

This trusts that the `axis` exists in `daf`, and that all properties that are based on this axis have already been
deleted.
"""
function format_delete_axis!(daf::WriteFormat, axis::AbstractString)::Nothing  # untested
    return error("missing method: format_delete_axis!\nfor the daf format: $(typeof(daf))")
end

"""
    format_axis_names(daf::ReadFormat)::AbstractSet{String}

The names of the axes of `daf`.
"""
function format_axis_names(daf::ReadFormat)::AbstractSet{String}  # untested
    return error("missing method: format_axis_names\nfor the daf format: $(typeof(daf))")
end

"""
    format_get_axis(daf::ReadFormat, axis::AbstractString)::DenseVector{String}

Implement fetching the unique names of the entries of some `axis` of `daf`.

This trusts the `axis` exists in `daf`.
"""
function format_get_axis(daf::ReadFormat, axis::AbstractString)::DenseVector{String}  # untested
    return error("missing method: format_get_axis\nfor the daf format: $(typeof(daf))")
end

"""
    format_axis_length(daf::ReadFormat, axis::AbstractString)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` exists in `daf`.
"""
function format_axis_length(daf::ReadFormat, axis::AbstractString)::Int64  # untested
    return error("missing method: format_axis_length\nfor the daf format: $(typeof(daf))")
end

"""
    format_has_vector(daf::ReadFormat, axis::AbstractString, name::AbstractString)::Bool

Implement checking whether a vector property with some `name` exists for the `axis` in `daf`.

This trusts the `axis` exists in `daf` and that the property name isn't `name`.
"""
function format_has_vector(daf::ReadFormat, axis::AbstractString, name::AbstractString)::Bool  # untested
    return error("missing method: format_has_vector\nfor the daf format: $(typeof(daf))")
end

"""
    format_set_vector!(
        daf::WriteFormat,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector},
    )::Nothing

Implement setting a vector property with some `name` for some `axis` in `daf`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This trusts the `axis` exists in `daf`, that the property name isn't `name`, and that the `vector` has the appropriate
length. It will silently overwrite an existing vector for the same `name` for the `axis`.
"""
function format_set_vector!(  # untested
    daf::WriteFormat,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    return error("missing method: format_set_vector!\nfor the daf format: $(typeof(daf))")
end

"""
    format_empty_dense_vector!(
        daf::WriteFormat,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::DenseVector where {T <: Number}

Implement setting a vector property with some `name` for some `axis` in `daf`.

Implement creating an empty dense `matrix` with some `name` for some `rows_axis` and `columns_axis` in `daf`.

This trusts the `axis` exists in `daf` and that the property name isn't `name`. It will silently overwrite an existing
vector for the same `name` for the `axis`.
"""
function format_empty_dense_vector!(  # untested
    daf::WriteFormat,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::DenseVector{T} where {T <: Number}
    return error("missing method: format_empty_dense_vector!\nfor the daf format: $(typeof(daf))")
end

"""
    format_empty_dense_vector!(
        daf::WriteFormat,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::Integer,
        indtype::Type{I},
    )::DenseVector where {T <: Number, I <: Integer}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in
`daf`.

This trusts the `axis` exists in `daf` and that the property name isn't `name`. It will silently overwrite an existing
vector for the same `name` for the `axis`.
"""
function format_empty_sparse_vector!(  # untested
    daf::WriteFormat,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    return error("missing method: format_empty_sparse_vector!\nfor the daf format: $(typeof(daf))")
end

"""
    format_delete_vector!(daf::WriteFormat, axis::AbstractString, name::AbstractString)::Nothing

Implement deleting a vector property with some `name` for some `axis` from `daf`.

This trusts the `axis` exists in `daf`, that the property name isn't `name`, and that the `name` vector exists for the
`axis`.
"""
function format_delete_vector!(daf::WriteFormat, axis::AbstractString, name::AbstractString)::Nothing  # untested
    return error("missing method: format_delete_vector! for the daf format: $(typeof(daf))")
end

"""
    format_vector_names(daf::ReadFormat, axis::AbstractString)::Set{String}

Implement fetching the names of the vectors for the `axis` in `daf`, **not** including the special `name`
property.

This trusts the `axis` exists in `daf`.
"""
function format_vector_names(daf::ReadFormat, axis::AbstractString)::AbstractSet{String}  # untested
    return error("missing method: format_vector_names\nfor the daf format: $(typeof(daf))")
end

"""
    format_get_vector(daf::ReadFormat, axis::AbstractString, name::AbstractString)::StorageVector

Implement fetching the vector property with some `name` for some `axis` in `daf`.

This trusts the `axis` exists in `daf`, and the `name` vector exists for the `axis`.
"""
function format_get_vector(daf::ReadFormat, name::AbstractString)::StorageVector  # untested
    return error("missing method: format_get_vector\nfor the daf format: $(typeof(daf))")
end

"""
    format_has_matrix(
        daf::ReadFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
    )::Bool

Implement checking whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in
`daf`.

This trusts the `rows_axis` and the `columns_axis` exist in `daf`.
"""
function format_has_matrix(  # untested
    daf::ReadFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return error("missing method: format_has_matrix\nfor the daf format: $(typeof(daf))")
end

"""
    format_set_matrix!(
        daf::WriteFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::StorageMatrix,
    )::Nothing

Implement setting the matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

This trusts the `rows_axis` and `columns_axis` exist in `daf`, and that the `matrix` is column-major of the appropriate
size. It will silently overwrite an existing matrix for the same `name` for the `rows_axis` and `columns_axis`.
"""
function format_set_matrix!(  # untested
    daf::WriteFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalar, StorageMatrix},
)::Nothing
    return error("missing method: format_set_matrix!\nfor the daf format: $(typeof(daf))")
end

"""
    format_empty_dense_matrix!(
        daf::WriteFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::DenseMatrix{T} where {T <: Number}

Implement creating an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`.

This trusts the `rows_axis` and `columns_axis` exist in `daf`. It will silently overwrite an existing matrix for the
same `name` for the `rows_axis` and `columns_axis`.
"""
function format_empty_dense_matrix!(  # untested
    daf::WriteFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    eltype::Type{T},
)::DenseMatrix{T} where {T <: Number}
    return error("missing method: format_empty_dense_matrix!\nfor the daf format: $(typeof(daf))")
end

"""
    format_empty_dense_matrix!(
        daf::WriteFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        intdype::Type{I},
        nnz::Integer,
    )::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in
`daf`.

This trusts the `rows_axis` and `columns_axis` exist in `daf`. It will silently overwrite an existing matrix for
the same `name` for the `rows_axis` and `columns_axis`.
"""
function format_empty_sparse_matrix!(  # untested
    daf::WriteFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    return error("missing method: format_empty_sparse_matrix!\nfor the daf format: $(typeof(daf))")
end

"""
    format_delete_matrix!(
        daf::WriteFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::Nothing

Implement deleting a matrix property with some `name` for some `rows_axis` and `columns_axis` from `daf`.

This trusts the `rows_axis` and `columns_axis` exist in `daf`, and that the `name` matrix exists for the `rows_axis` and
`columns_axis`.
"""
function format_delete_matrix!(  # untested
    daf::WriteFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    return error("missing method: format_delete_matrix!\nfor the daf format: $(typeof(daf))")
end

"""
    format_matrix_names(
        daf::ReadFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
    )::Set{String}

Implement fetching the names of the matrix properties for the `rows_axis` and `columns_axis` in `daf`.

This trusts the `rows_axis` and `columns_axis` exist in `daf`.
"""
function format_matrix_names(  # untested
    daf::ReadFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    return error("missing method: format_matrix_names\nfor the daf format: $(typeof(daf))")
end

"""
    format_get_matrix(
        daf::ReadFormat,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::StorageMatrix

Implement fetching the matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`.

This trusts the `rows_axis` and `columns_axis` exist in `daf`, and the `name` matrix exists for the `rows_axis` and
`columns_axis`.
"""
function format_get_matrix(  # untested
    daf::ReadFormat,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    return error("missing method: format_get_matrix\nfor the daf format: $(typeof(daf))")
end

"""
    function format_description_header(format::ReadFormat, lines::Array{String})::Nothing

Allow a `format` to amit additional description header lines.
"""
function format_description_header(daf::ReadFormat, lines::Array{String})::Nothing
    return nothing
end

"""
    function format_description_footer(format::ReadFormat, lines::Array{String})::Nothing

Allow a `format` to amit additional description footer lines.
"""
function format_description_footer(daf::ReadFormat, lines::Array{String})::Nothing
    return nothing
end

end # module
