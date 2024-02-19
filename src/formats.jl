"""
The [`FormatReader`](@ref) and [`FormatWriter`](@ref) interfaces specify a low-level API for storing `Daf` data. To extend
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

For clarity of documentation, we split the type hierarchy to `DafWriter <: FormatWriter <: DafReader <: FormatReader`.

The functions listed here use the [`FormatReader`](@ref) for read-only operations and [`FormatWriter`](@ref) for write
operations into a `Daf` storage. This is a low-level API, not meant to be used from outside the package, and therefore
is not re-exported from the top-level `Daf` namespace.

In contrast, the functions using [`DafReader`](@ref) and [`DafWriter`](@ref) describe the high-level API meant to be used
from outside the package, and are re-exported. These functions are listed in the `Daf.Data` module. These functions
provide all the logic common to any storage format, allowing us to keep the format-specific functions as simple as
possible.

That is, when implementing a new `Daf` storage format, you should write `struct MyFormat <: DafWriter`, and implement the
functions listed here for both [`FormatReader`](@ref) and [`FormatWriter`](@ref).
"""
module Formats

export DafReader
export DafWriter

using Daf.MatrixLayouts
using Daf.Messages
using Daf.StorageTypes
using OrderedCollections
using SparseArrays

"""
    Internal(name::AbstractString)

Internal data we need to keep in any concrete [`FormatReader`](@ref). This has to be available as a `.internal` data
member of the concrete format. This enables all the high-level [`DafReader`](@ref) and [`DafWriter`](@ref) functions.

The constructor will automatically call [`unique_name`](@ref) to try and make the names unique for improved error
messages.
"""
struct Internal
    name::AbstractString
    axes::Dict{String, OrderedDict{String, Int64}}
    cache::Dict{String, Any}
    dependency_cache_keys::Dict{String, Set{String}}
end

function Internal(name::AbstractString)::Internal
    return Internal(
        unique_name(name),
        Dict{String, OrderedDict{String, Int64}}(),
        Dict{String, Any}(),
        Dict{String, Set{String}}(),
    )
end

"""
An low-level abstract interface for reading from `Daf` storage formats.

We require each storage format to have a `.internal::`[`Internal`](@ref) property. This enables all the high-level
`DafReader` functions.

Each storage format must implement the functions listed below for reading from the storage.
"""
abstract type FormatReader end

"""
A  high-level abstract interface for read-only access to `Daf` data.

All the functions for this type are provided based on the functions required for [`FormatReader`](@ref). See the
`Daf.Data` module for their description.
"""
abstract type DafReader <: FormatReader end

"""
An abstract interface for writing into `Daf` storage formats.

Each storage format must implement the functions listed below for writing into the storage.
"""
abstract type FormatWriter <: DafReader end

"""
A  high-level abstract interface for write access to `Daf` data.

All the functions for this type are provided based on the functions required for [`FormatWriter`](@ref). See the
`Daf.Data` module for their description.
"""
abstract type DafWriter <: FormatWriter end

"""
    format_has_scalar(format::FormatReader, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in `format`.
"""
function format_has_scalar end

"""
    format_set_scalar!(
        format::FormatWriter,
        name::AbstractString,
        value::StorageScalar,
    )::Nothing

Implement setting the `value` of a scalar property with some `name` in `format`.

This trusts that the `name` scalar property does not exist in `format`.
"""
function format_set_scalar! end

"""
    format_delete_scalar!(
        format::FormatWriter,
        name::AbstractString;
        for_set::Bool
    )::Nothing

Implement deleting a scalar property with some `name` from `format`. If `for_set`, this is done just prior to setting
the scalar with a different value.

This trusts that the `name` scalar property exists in `format`.
"""
function format_delete_scalar! end

"""
    format_scalar_names(format::FormatReader)::Set{String}

The names of the scalar properties in `format`.
"""
function format_scalar_names end

"""
    format_get_scalar(format::FormatReader, name::AbstractString)::StorageScalar

Implement fetching the value of a scalar property with some `name` in `format`.

This trusts the `name` scalar property exists in `format`.
"""
function format_get_scalar end

"""
    format_has_axis(format::FormatReader, axis::AbstractString)::Bool

Check whether some `axis` exists in `format`.
"""
function format_has_axis end

"""
    format_add_axis!(
        format::FormatWriter,
        axis::AbstractString,
        entries::AbstractVector{String}
    )::Nothing

Implement adding a new `axis` to `format`.

This trusts that the `axis` does not already exist in `format`, and that the names of the `entries` are unique.
"""
function format_add_axis! end

"""
    format_delete_axis!(format::FormatWriter, axis::AbstractString)::Nothing

Implement deleting some `axis` from `format`.

This trusts that the `axis` exists in `format`, and that all properties that are based on this axis have already been
deleted.
"""
function format_delete_axis! end

"""
    format_axis_names(format::FormatReader)::AbstractSet{String}

The names of the axes of `format`.
"""
function format_axis_names end

"""
    format_get_axis(format::FormatReader, axis::AbstractString)::AbstractVector{String}

Implement fetching the unique names of the entries of some `axis` of `format`.

This trusts the `axis` exists in `format`.
"""
function format_get_axis end

"""
    format_axis_length(format::FormatReader, axis::AbstractString)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` exists in `format`.
"""
function format_axis_length end

"""
    format_has_vector(format::FormatReader, axis::AbstractString, name::AbstractString)::Bool

Implement checking whether a vector property with some `name` exists for the `axis` in `format`.

This trusts the `axis` exists in `format` and that the property name isn't `name`.
"""
function format_has_vector end

"""
    format_set_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector},
    )::Nothing

Implement setting a vector property with some `name` for some `axis` in `format`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This trusts the `axis` exists in `format`, that the vector property `name` isn't `"name"`, that it does not exist for
the `axis`, and that the `vector` has the appropriate length for it.
"""
function format_set_vector! end

"""
    format_empty_dense_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::VectorVector where {T <: StorageNumber}

Implement setting a vector property with some `name` for some `axis` in `format`.

Implement creating an empty dense `matrix` with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts the `axis` exists in `format` and that the vector property `name` isn't `"name"`, and that it does not exist
for the `axis`.

!!! note

    The return type of this function is always a *functionally* dense vector, that is, it will have `strides` of `(1,)`,
    so that elements are consecutive in memory. However it need not be an actual `DenseVector` because of Julia's type
    system's limitations.
"""
function format_empty_dense_vector! end

"""
    format_empty_sparse_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        indtype::Type{I},
    )::SparseVector{T, I} where {T <: StorageNumber, I <: StorageInteger}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in
`format`.

This trusts the `axis` exists in `format` and that the vector property `name` isn't `"name"`, and that it does not exist
for the `axis`.
"""
function format_empty_sparse_vector! end

"""
    format_delete_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString;
        for_set::Bool
    )::Nothing

Implement deleting a vector property with some `name` for some `axis` from `format`. If `for_set`, this is done just
prior to setting the vector with a different value.

This trusts the `axis` exists in `format`, that the vector property name isn't `name`, and that the `name` vector exists
for the `axis`.
"""
function format_delete_vector! end

"""
    format_vector_names(format::FormatReader, axis::AbstractString)::Set{String}

Implement fetching the names of the vectors for the `axis` in `format`, **not** including the special `name` property.

This trusts the `axis` exists in `format`.
"""
function format_vector_names end

"""
    format_get_vector(format::FormatReader, axis::AbstractString, name::AbstractString)::StorageVector

Implement fetching the vector property with some `name` for some `axis` in `format`.

This trusts the `axis` exists in `format`, and the `name` vector property exists for the `axis`.
"""
function format_get_vector end

"""
    format_has_matrix(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
    )::Bool

Implement checking whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in
`format`.

This trusts the `rows_axis` and the `columns_axis` exist in `format`.
"""
function format_has_matrix end

"""
    format_set_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::StorageMatrix,
    )::Nothing

Implement setting the matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

This trusts the `rows_axis` and `columns_axis` exist in `format`, that the `name` matrix property does not exist for
them, and that the `matrix` is column-major of the appropriate size for it.
"""
function format_set_matrix! end

"""
    format_empty_dense_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::AbstractMatrix{T} where {T <: StorageNumber}

Implement creating an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts the `rows_axis` and `columns_axis` exist in `format` and that the `name` matrix property does not exist for
them.

!!! note

    The return type of this function is always a *functionally* dense vector, that is, it will have `strides` of
    `(1,nrows)`, so that elements are consecutive in memory. However it need not be an actual `DenseMatrix` because of
    Julia's type system's limitations.
"""
function format_empty_dense_matrix! end

"""
    format_empty_dense_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        intdype::Type{I},
        nnz::StorageInteger,
    )::AbstractMatrix{T}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts the `rows_axis` and `columns_axis` exist in `format` and that the `name` matrix property does not exist for
them.
"""
function format_empty_sparse_matrix! end

"""
    format_relayout_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::Nothing

[`relayout!`](@ref) the existing `name` column-major matrix property for the `rows_axis` and the `columns_axis` and
store the results as a row-major matrix property (that is, with flipped axes).

This trusts the `rows_axis` and `columns_axis` exist in `format`, that the `name` matrix property exists for them, and
that it does not exist for the flipped axes.
"""
function format_relayout_matrix! end

"""
    format_delete_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        for_set::Bool
    )::Nothing

Implement deleting a matrix property with some `name` for some `rows_axis` and `columns_axis` from `format`. If
`for_set`, this is done just prior to setting the matrix with a different value.

This trusts the `rows_axis` and `columns_axis` exist in `format`, and that the `name` matrix property exists for them.
"""
function format_delete_matrix! end

"""
    format_matrix_names(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
    )::Set{String}

Implement fetching the names of the matrix properties for the `rows_axis` and `columns_axis` in `format`.

This trusts the `rows_axis` and `columns_axis` exist in `format`.
"""
function format_matrix_names end

"""
    format_get_matrix(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::StorageMatrix

Implement fetching the matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts the `rows_axis` and `columns_axis` exist in `format`, and the `name` matrix property exists for them.
"""
function format_get_matrix end

"""
    function format_description_header(format::FormatReader, lines::Array{String})::Nothing

Allow a `format` to amit additional description header lines.
"""
function format_description_header(format::FormatReader, indent::AbstractString, lines::Array{String})::Nothing
    push!(lines, "$(indent)type: $(typeof(format))")
    return nothing
end

"""
    function format_description_footer(format::FormatReader, lines::Array{String})::Nothing

Allow a `format` to amit additional description footer lines. If `deep`, this also emit the description of any data sets
nested in this one, if any.
"""
function format_description_footer(
    format::FormatReader,
    indent::AbstractString,
    lines::Array{String},
    deep::Bool,
)::Nothing
    return nothing
end

end # module
