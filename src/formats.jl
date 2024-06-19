"""
The [`FormatReader`](@ref) and [`FormatWriter`](@ref) interfaces specify a low-level API for storing `Daf` data. To
extend `Daf` to support an additional format, create a new implementation of this API.

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

For clarity of documentation, we split the type hierarchy to [`DafWriter`](@ref) `<:` [`FormatWriter`](@ref) `<:`
[`DafReader`](@ref) `<:` [`FormatReader`](@ref).

The functions listed here use the [`FormatReader`](@ref) for read-only operations and [`FormatWriter`](@ref) for write
operations into a `Daf` storage. This is a low-level API, not meant to be used from outside the package, and therefore
is not re-exported from the top-level `Daf` namespace.

In contrast, the functions using [`DafReader`](@ref) and [`DafWriter`](@ref) describe the high-level API meant to be
used from outside the package, and are re-exported. These functions are listed in the `Daf.Readers` and `Daf.Writers`
modules. These functions provide all the logic common to any storage format, allowing us to keep the format-specific
functions as simple as possible.

That is, when implementing a new `Daf` storage format, you should write `struct MyFormat <: DafWriter`, and implement
the functions listed here for both [`FormatReader`](@ref) and [`FormatWriter`](@ref).
"""
module Formats

export CacheType
export DafReader
export DafWriter
export DataKey
export MappedData
export MemoryData
export QueryData
export empty_cache!
export end_data_write_lock

using ..GenericLocks
using ..GenericTypes
using ..MatrixLayouts
using ..Messages
using ..StorageTypes
using ..Tokens
using Base.Threads
using LinearAlgebra
using NamedArrays
using OrderedCollections
using SparseArrays

"""
A key specifying some data property in `Daf`.

**Scalars** are identified by their name.

**Vectors** are specified as a tuple of the axis name and the property name.

**Matrices** are specified as a tuple or the rows axis, the columns axis, and the property name.

The [`DafReader`](@ref) and [`DafWriter`](@ref) interfaces do not use this type, as each function knows exactly the type
of data property it works on. However, higher-level APIs do use this as keys for dictionaries etc.
"""
DataKey =
    Union{AbstractString, Tuple{AbstractString, AbstractString}, Tuple{AbstractString, AbstractString, AbstractString}}

"""
Types of cached data inside `Daf`.

  - `MappedData` - memory-mapped disk data. This is the cheapest data, as it doesn't put pressure on the garbage
    collector. It requires some OS resources to maintain the mapping, and physical memory for the subset of the data
    that is actually being accessed. That is, one can memory map larger data than the physical memory, and performance
    will be good, as long as the subset of the data that is actually accessed is small enough to fit in memory. If it
    isn't, the performance will drop (a lot!) because the OS will be continuously reading data pages from disk - but it
    will not crash due to an out of memory error. It is very important not to re-map the same data twice because that
    causes all sort of inefficiencies and edge cases in the hardware and low-level software.
  - `MemoryData` - a copy of data (from disk, or computed). This does pressure the garbage collector and can cause out
    of memory errors. However, recomputing or re-fetching the data from disk is slow, so caching this data is crucial
    for performance.
  - `QueryData` - data that is computed by queries based on stored data (e.g., masked data, or results of a reduction
    or an element-wise operation). This again takes up application memory and may cause out of memory errors, but it is
    very useful to cache the results when the same query is executed multiple times (e.g., when using views). Manually
    executing queries therefore allows to explicitly disable the caching of the query results, since some queries will
    not be repeated.

If too much data has been cached, call [`empty_cache!`](@ref) to release it.
"""
@enum CacheType MappedData MemoryData QueryData

struct CacheEntry
    cache_type::CacheType
    data::Union{
        AbstractSet{<:AbstractString},
        AbstractVector{<:AbstractString},
        StorageScalar,
        NamedArray,
        AbstractDict{<:AbstractString, <:Integer},
    }
end

mutable struct WriterThread
    thread_id::Int
    depth::Int
end

"""
    struct Internal ... end

Internal data we need to keep in any concrete [`FormatReader`](@ref). This has to be available as a `.internal` data
member of the concrete format. This enables all the high-level [`DafReader`](@ref) and [`DafWriter`](@ref) functions.

The constructor will automatically call [`unique_name`](@ref) to try and make the names unique for improved error
messages.
"""
struct Internal
    cache::Dict{AbstractString, CacheEntry}
    dependency_cache_keys::Dict{AbstractString, Set{AbstractString}}
    version_counters::Dict{DataKey, UInt32}
    cache_lock::QueryReadWriteLock
    data_lock::QueryReadWriteLock
    is_frozen::Bool
end

function Internal(; is_frozen::Bool)::Internal
    return Internal(
        Dict{AbstractString, CacheEntry}(),
        Dict{AbstractString, Set{AbstractString}}(),
        Dict{DataKey, UInt32}(),
        QueryReadWriteLock(),
        QueryReadWriteLock(),
        is_frozen,
    )
end

"""
An low-level abstract interface for reading from `Daf` storage formats.

We require each storage format to have a `.name` and an `.internal::`[`Internal`](@ref) property. This enables all the
high-level `DafReader` functions.

Each storage format must implement the functions listed below for reading from the storage.
"""
abstract type FormatReader end

"""
A high-level abstract interface for read-only access to `Daf` data.

All the functions for this type are provided based on the functions required for [`FormatReader`](@ref). See the
`Daf.Readers` module for their description.
"""
abstract type DafReader <: FormatReader end

"""
An abstract interface for writing into `Daf` storage formats.

Each storage format must implement the functions listed below for writing into the storage.
"""
abstract type FormatWriter <: DafReader end

"""
A high-level abstract interface for write access to `Daf` data.

All the functions for this type are provided based on the functions required for [`FormatWriter`](@ref). See the
`Daf.Writers` module for their description.
"""
abstract type DafWriter <: FormatWriter end

function Base.show(io::IO, format_reader::FormatReader)::Nothing
    print(io, depict(format_reader))
    return nothing
end

"""
    format_has_scalar(format::FormatReader, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in `format`.

This trusts that we have a read lock on the data set.
"""
function format_has_scalar end

"""
    format_set_scalar!(
        format::FormatWriter,
        name::AbstractString,
        value::StorageScalar,
    )::Nothing

Implement setting the `value` of a scalar property with some `name` in `format`.

This trusts that we have a write lock on the data set, and that the `name` scalar property does not exist in `format`.
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

This trusts that we have a write lock on the data set, and that the `name` scalar property exists in `format`.
"""
function format_delete_scalar! end

"""
    format_scalars_set(format::FormatReader)::AbstractSet{<:AbstractString}

The names of the scalar properties in `format`.

This trusts that we have a read lock on the data set.
"""
function format_scalars_set end

"""
    format_get_scalar(format::FormatReader, name::AbstractString)::StorageScalar

Implement fetching the value of a scalar property with some `name` in `format`.

This trusts that we have a read lock on the data set, and that the `name` scalar property exists in `format`.
"""
function format_get_scalar end

"""
    format_has_axis(format::FormatReader, axis::AbstractString; for_change::Bool)::Bool

Check whether some `axis` exists in `format`. If `for_change`, this is done just prior to adding or deleting the axis.

This trusts that we have a read lock on the data set.
"""
function format_has_axis end

"""
    format_add_axis!(
        format::FormatWriter,
        axis::AbstractString,
        entries::AbstractVector{<:AbstractString}
    )::Nothing

Implement adding a new `axis` to `format`.

This trusts we have a write lock on the data set, that the `axis` does not already exist in `format`, and that the names
of the `entries` are unique.
"""
function format_add_axis! end

"""
    format_delete_axis!(format::FormatWriter, axis::AbstractString)::Nothing

Implement deleting some `axis` from `format`.

This trusts This trusts we have a write lock on the data set, that the `axis` exists in `format`, and that all
properties that are based on this axis have already been deleted.
"""
function format_delete_axis! end

"""
    format_axes_set(format::FormatReader)::AbstractSet{<:AbstractString}

The names of the axes of `format`.

This trusts that we have a read lock on the data set.
"""
function format_axes_set end

"""
    format_axis_array(format::FormatReader, axis::AbstractString)::AbstractVector{<:AbstractString}

Implement fetching the unique names of the entries of some `axis` of `format`.

This trusts that we have a read lock on the data set, and that the `axis` exists in `format`.
"""
function format_axis_array end

"""
    format_axis_length(format::FormatReader, axis::AbstractString)::Int64

Implement fetching the number of entries along the `axis`.

This trusts that we have a read lock on the data set, and that the `axis` exists in `format`.
"""
function format_axis_length end

"""
    format_has_vector(format::FormatReader, axis::AbstractString, name::AbstractString)::Bool

Implement checking whether a vector property with some `name` exists for the `axis` in `format`.

This trusts that we have a read lock on the data set, that the `axis` exists in `format` and that the property name
isn't `name`.
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

This trusts we have a write lock on the data set, that the `axis` exists in `format`, that the vector property `name`
isn't `"name"`, that it does not exist for the `axis`, and that the `vector` has the appropriate length for it.
"""
function format_set_vector! end

"""
    format_get_empty_dense_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::Vector{T} where {T <: StorageNumber}

Implement setting a vector property with some `name` for some `axis` in `format`.

Implement creating an empty dense `matrix` with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts we have a write lock on the data set, that the `axis` exists in `format` and that the vector property `name`
isn't `"name"`, and that it does not exist for the `axis`.

!!! note

    The return type of this function is always a **functionally** dense vector, that is, it will have `strides` of
    `(1,)`, so that elements are consecutive in memory. However it need not be an actual `DenseVector` because of
    Julia's type system's limitations.
"""
function format_get_empty_dense_vector! end

"""
    format_filled_empty_dense_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        filled_vector::AbstractVector{<:StorageNumber},
    )::Nothing

Allow the `format` to perform caching once the empty dense vector has been `filled`. By default this does nothing.
"""
function format_filled_empty_dense_vector!(
    ::DafWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractVector{<:StorageNumber},
)::Nothing
    return nothing
end

"""
    format_get_empty_sparse_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        indtype::Type{I},
    )::Tuple{AbstractVector{I}, AbstractVector{T}, Any}
    where {T <: StorageNumber, I <: StorageInteger}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in
`format`. The final tuple element is passed to [`format_filled_empty_sparse_vector!`](@ref).

This trusts we have a write lock on the data set, that the `axis` exists in `format` and that the vector property `name`
isn't `"name"`, and that it does not exist for the `axis`.
"""
function format_get_empty_sparse_vector! end

"""
    format_filled_empty_sparse_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        extra::Any,
        filled::SparseVector{<:StorageNumber, <:StorageInteger},
    )::Nothing

Allow the `format` to perform caching once the empty sparse vector has been `filled`. By default this does nothing.
"""
function format_filled_empty_sparse_vector!(  # untested
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::Any,
    ::SparseVector{<:StorageNumber, <:StorageInteger},
)::Nothing
    return nothing
end

"""
    format_delete_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString;
        for_set::Bool
    )::Nothing

Implement deleting a vector property with some `name` for some `axis` from `format`. If `for_set`, this is done just
prior to setting the vector with a different value.

This trusts we have a write lock on the data set, that the `axis` exists in `format`, that the vector property name
isn't `name`, and that the `name` vector exists for the `axis`.
"""
function format_delete_vector! end

"""
    format_vectors_set(format::FormatReader, axis::AbstractString)::AbstractSet{<:AbstractString}

Implement fetching the names of the vectors for the `axis` in `format`, **not** including the special `name` property.

This trusts that we have a read lock on the data set, and that the `axis` exists in `format`.
"""
function format_vectors_set end

"""
    format_get_vector(format::FormatReader, axis::AbstractString, name::AbstractString)::StorageVector

Implement fetching the vector property with some `name` for some `axis` in `format`.

This trusts that we have a read lock on the data set, that the `axis` exists in `format`, and the `name` vector property
exists for the `axis`.
"""
function format_get_vector end

"""
    format_has_matrix(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [for_relayout::Bool = false]
    )::Bool

Implement checking whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in
`format`.

This trusts that we have a read lock on the data set, and that the `rows_axis` and the `columns_axis` exist in `format`.
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

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format`, that the
`name` matrix property does not exist for them, and that the `matrix` is column-major of the appropriate size for it.
"""
function format_set_matrix! end

"""
    format_get_empty_dense_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::AbstractMatrix{T} where {T <: StorageNumber}

Implement creating an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format` and that the
`name` matrix property does not exist for them.

!!! note

    The return type of this function is always a **functionally** dense vector, that is, it will have `strides` of
    `(1,nrows)`, so that elements are consecutive in memory. However it need not be an actual `DenseMatrix` because of
    Julia's type system's limitations.
"""
function format_get_empty_dense_matrix! end

"""
    format_filled_empty_dense_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        filled_matrix::AbstractVector{<:StorageNumber},
    )::Nothing

Allow the `format` to perform caching once the empty dense matrix has been `filled`. By default this does nothing.
"""
function format_filled_empty_dense_matrix!(
    ::DafWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::AbstractMatrix{<:StorageNumber},
)::Nothing
    return nothing
end

"""
    format_get_empty_sparse_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        intdype::Type{I},
        nnz::StorageInteger,
    )::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}, Any}
    where {T <: StorageNumber, I <: StorageInteger}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.
The final tuple element is passed to [`format_filled_empty_sparse_matrix!`](@ref).

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format` and that the
`name` matrix property does not exist for them.
"""
function format_get_empty_sparse_matrix! end

"""
    format_filled_empty_dense_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        extra::Any,
        filled::SparseMatrixCSC{<:StorageNumber, <:StorageInteger},
    )::Nothing

Allow the `format` to perform caching once the empty sparse matrix has been `filled`. By default this does nothing.
"""
function format_filled_empty_sparse_matrix!(  # untested
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::Any,
    ::SparseMatrixCSC{<:StorageNumber, <:StorageInteger},
)::Nothing
    return nothing
end

"""
    format_relayout_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::Nothing

[`relayout!`](@ref) the existing `name` column-major matrix property for the `rows_axis` and the `columns_axis` and
store the results as a row-major matrix property (that is, with flipped axes).

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` are different from each other,
exist in `format`, that the `name` matrix property exists for them, and that it does not exist for the flipped axes.
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

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format`, and that
the `name` matrix property exists for them.
"""
function format_delete_matrix! end

"""
    format_matrices_set(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
    )::AbstractSet{<:AbstractString}

Implement fetching the names of the matrix properties for the `rows_axis` and `columns_axis` in `format`.

This trusts that we have a read lock on the data set, and that the `rows_axis` and `columns_axis` exist in `format`.
"""
function format_matrices_set end

"""
    format_get_matrix(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::StorageMatrix

Implement fetching the matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts that we have a read lock on the data set, and that the `rows_axis` and `columns_axis` exist in `format`, and
the `name` matrix property exists for them.
"""
function format_get_matrix end

"""
    format_description_header(format::FormatReader, lines::Vector{String}, deep::Bool)::Nothing

Allow a `format` to amit additional description header lines.

This trusts that we have a read lock on the data set.
"""
function format_description_header(format::FormatReader, indent::AbstractString, lines::Vector{String}, ::Bool)::Nothing
    push!(lines, "$(indent)type: $(typeof(format))")
    return nothing
end

"""
    format_description_footer(format::FormatReader, lines::Vector{String}, cache::Bool, deep::Bool)::Nothing

Allow a `format` to amit additional description footer lines. If `deep`, this also emit the description of any data sets
nested in this one, if any.

This trusts that we have a read lock on the data set.
"""
function format_description_footer(::FormatReader, ::AbstractString, ::Vector{String}, ::Bool, ::Bool)::Nothing
    return nothing
end

function get_from_cache(format::FormatReader, cache_key::AbstractString, ::Type{T})::Maybe{T} where {T}
    @assert has_data_read_lock(format)
    with_read_lock(format.internal.cache_lock, format.name, "cache for:", cache_key) do  # NOJET
        result = get(format.internal.cache, cache_key, nothing)
        if result === nothing
            return nothing
        else
            return result.data
        end
    end
end

function get_through_cache(getter::Function, format::FormatReader, cache_key::AbstractString, ::Type{T})::T where {T}
    cached = get_from_cache(format, cache_key, T)
    if cached === nothing
        return getter()
    else
        return cached
    end
end

function get_scalars_set_through_cache(format::FormatReader)::AbstractSet{<:AbstractString}
    return get_through_cache(format, scalars_set_cache_key(), AbstractSet{<:AbstractString}) do
        return format_scalars_set(format)
    end
end

function get_axes_set_through_cache(format::FormatReader)::AbstractSet{<:AbstractString}
    return get_through_cache(format, axes_set_cache_key(), AbstractSet{<:AbstractString}) do
        return format_axes_set(format)
    end
end

function get_vectors_set_through_cache(format::FormatReader, axis::AbstractString)::AbstractSet{<:AbstractString}
    return get_through_cache(format, vectors_set_cache_key(axis), AbstractSet{<:AbstractString}) do
        return format_vectors_set(format, axis)
    end
end

function get_matrices_set_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    return get_through_cache(format, matrices_set_cache_key(rows_axis, columns_axis), AbstractSet{<:AbstractString}) do
        return format_matrices_set(format, rows_axis, columns_axis)
    end
end

function get_scalar_through_cache(format::FormatReader, name::AbstractString)::StorageScalar
    return get_through_cache(format, scalar_cache_key(name), StorageScalar) do
        return format_get_scalar(format, name)
    end
end

function axis_array_through_cache(format::FormatReader, axis::AbstractString)::AbstractVector{<:AbstractString}
    return get_through_cache(format, axis_array_cache_key(axis), AbstractVector{<:AbstractString}) do
        return format_axis_array(format, axis)
    end
end

function axis_dict_with_cache(format::FormatReader, axis::AbstractString)::AbstractDict{<:AbstractString, <:Integer}
    cache_key = axis_dict_cache_key(axis)

    axis_dict = with_read_lock(format.internal.cache_lock, format.name, "cache for:", cache_key) do
        cache_entry = get(format.internal.cache, cache_key, nothing)
        if cache_entry !== nothing
            return cache_entry.data
        end
    end

    if axis_dict === nothing
        axis_dict = with_write_lock(format.internal.cache_lock, format.name, "cache for:", cache_key) do
            cache_entry = get(format.internal.cache, cache_key, nothing)
            if cache_entry !== nothing
                return cache_entry.data  # untested
            end

            names = axis_array_through_cache(format, axis)
            if eltype(names) != AbstractString
                names = Vector{AbstractString}(names)  # NOJET
            end
            names = read_only_array(names)
            named_array = NamedArray(spzeros(length(names)); names = (names,), dimnames = (axis,))
            axis_dict = named_array.dicts[1]
            format.internal.cache[cache_key] = CacheEntry(MemoryData, axis_dict)
            return axis_dict
        end
    end

    return axis_dict
end

function get_vector_through_cache(format::FormatReader, axis::AbstractString, name::AbstractString)::NamedArray
    return get_through_cache(format, vector_cache_key(axis, name), StorageVector) do
        vector = format_get_vector(format, axis, name)
        return as_named_vector(format, axis, vector)
    end
end

function get_matrix_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::NamedArray
    return get_through_cache(format, matrix_cache_key(rows_axis, columns_axis, name), StorageMatrix) do
        matrix = format_get_matrix(format, rows_axis, columns_axis, name)
        return as_named_matrix(format, rows_axis, columns_axis, matrix)
    end
end

function cache_scalars_set!(format::FormatReader, names::AbstractSet{<:AbstractString}, cache_type::CacheType)::Nothing
    cache_key = scalars_set_cache_key()
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_axes_set!(format::FormatReader, names::AbstractSet{<:AbstractString}, cache_type::CacheType)::Nothing
    cache_key = axes_set_cache_key()
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_axis!(
    format::FormatReader,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
    cache_type::CacheType,
)::Nothing
    cache_key = axis_array_cache_key(axis)
    cache_data!(format, cache_key, entries, cache_type)
    return nothing
end

function cache_vectors_set!(
    format::FormatReader,
    axis::AbstractString,
    names::AbstractSet{<:AbstractString},
    cache_type::CacheType,
)::Nothing
    cache_key = vectors_set_cache_key(axis)
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_matrices_set!(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    names::AbstractSet{<:AbstractString},
    cache_type::CacheType,
)::Nothing
    cache_key = matrices_set_cache_key(rows_axis, columns_axis)
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_scalar!(format::FormatReader, name::AbstractString, value::StorageScalar, cache_type::CacheType)::Nothing
    cache_key = scalar_cache_key(name)
    cache_data!(format, cache_key, value, cache_type)
    return nothing
end

function cache_vector!(
    format::FormatReader,
    axis::AbstractString,
    name::AbstractString,
    vector::StorageVector,
    cache_type::CacheType,
)::Nothing
    cache_key = vector_cache_key(axis, name)
    named_vector = as_named_vector(format, axis, vector)
    with_write_lock(format.internal.cache_lock, format.name, "cache for:", cache_key) do
        cache_data!(format, cache_key, named_vector, cache_type)
        return store_cached_dependency_key!(format, cache_key, axis_array_cache_key(axis))
    end
    return nothing
end

function cache_matrix!(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
    cache_type::CacheType,
)::Nothing
    cache_key = matrix_cache_key(rows_axis, columns_axis, name)
    named_matrix = as_named_matrix(format, rows_axis, columns_axis, matrix)
    with_write_lock(format.internal.cache_lock, format.name, "cache for:", cache_key) do
        cache_data!(format, cache_key, named_matrix, cache_type)
        store_cached_dependency_key!(format, cache_key, axis_array_cache_key(rows_axis))
        return store_cached_dependency_key!(format, cache_key, axis_array_cache_key(columns_axis))
    end
    return nothing
end

function as_named_vector(::FormatReader, ::AbstractString, vector::NamedVector)::NamedArray
    return vector
end

function as_named_vector(format::FormatReader, axis::AbstractString, vector::AbstractVector)::NamedArray
    axis_dict = axis_dict_with_cache(format, axis)
    return NamedArray(vector, (axis_dict,), (axis,))
end

function as_named_matrix(::FormatReader, ::AbstractString, ::AbstractString, matrix::NamedMatrix)::NamedArray
    return matrix
end

function as_named_matrix(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::AbstractMatrix,
)::NamedArray
    rows_axis_dict = axis_dict_with_cache(format, rows_axis)
    columns_axis_dict = axis_dict_with_cache(format, columns_axis)
    return NamedArray(matrix, (rows_axis_dict, columns_axis_dict), (rows_axis, columns_axis))
end

function read_only_array(array::AbstractArray)::AbstractArray
    return SparseArrays.ReadOnly(array)
end

function read_only_array(array::Transpose)::Transpose
    parent_array = parent(array)
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return Transpose(read_only_parent_array)
    end
end

function read_only_array(array::Adjoint)::Adjoint
    parent_array = parent(array)
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return Adjoint(read_only_parent_array)
    end
end

function read_only_array(array::SparseArrays.ReadOnly)::SparseArrays.ReadOnly
    return array
end

function read_only_array(array::NamedArray)::NamedArray
    parent_array = array.array
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return NamedArray(read_only_parent_array, array.dicts, array.dimnames)
    end
end

function scalars_set_cache_key()::String
    return "? scalars"
end

function axes_set_cache_key()::String
    return "? axes"
end

function vectors_set_cache_key(axis::AbstractString)::String
    return "/ $(axis) ?"
end

function matrices_set_cache_key(rows_axis::AbstractString, columns_axis::AbstractString)::String
    return "? / $(rows_axis) / $(columns_axis)" # TRICKY: NOT the query key, which returns the union.
end

function matrix_relayout_names_cache_key(rows_axis::AbstractString, columns_axis::AbstractString)::String
    return "/ $(rows_axis) / $(columns_axis) ?"  # TRICKY: The query key, which returns the union.
end

function scalar_cache_key(name::AbstractString)::String
    return ": $(escape_value(name))"
end

function axis_array_cache_key(axis::AbstractString)::String
    return "/ $(escape_value(axis))"
end

function axis_dict_cache_key(axis::AbstractString)::String
    return "# $(escape_value(axis))"
end

function vector_cache_key(axis::AbstractString, name::AbstractString)::String
    return "/ $(escape_value(axis)) : $(escape_value(name))"
end

function matrix_cache_key(rows_axis::AbstractString, columns_axis::AbstractString, name::AbstractString)::String
    return "/ $(escape_value(rows_axis)) / $(escape_value(columns_axis)) : $(escape_value(name))"
end

function store_cached_dependency_keys!(
    format::FormatReader,
    cache_key::AbstractString,
    dependency_keys::Set{AbstractString},
)::Nothing
    with_write_lock(format.internal.cache_lock, format.name, "cache for dependency keys of:", cache_key) do
        for dependency_key in dependency_keys
            if cache_key != dependency_key
                store_cached_dependency_key!(format, cache_key, dependency_key)
            end
        end
    end
    return nothing
end

function store_cached_dependency_key!(
    format::FormatReader,
    cache_key::AbstractString,
    dependency_key::AbstractString,
)::Nothing
    with_write_lock(  # NOJET
        format.internal.cache_lock,
        format.name,
        "cache for dependency key:",
        dependency_key,
        "of:",
        cache_key,
    ) do
        keys_set = get!(format.internal.dependency_cache_keys, dependency_key) do
            return Set{AbstractString}()
        end
        @assert cache_key != dependency_key
        return push!(keys_set, cache_key)
    end
    return nothing
end

function invalidate_cached!(format::FormatReader, cache_key::AbstractString)::Nothing
    @debug "invalidate_cached! daf: $(depict(format)) cache_key: $(cache_key)"
    @debug "- delete cache_key: $(cache_key)"
    with_write_lock(format.internal.cache_lock, format.name, "cache for invalidate key:", cache_key) do
        delete!(format.internal.cache, cache_key)

        dependent_keys = pop!(format.internal.dependency_cache_keys, cache_key, nothing)
        if dependent_keys !== nothing
            for dependent_key in dependent_keys
                @debug "- delete dependent_key: $(dependent_key)"
                delete!(format.internal.cache, dependent_key)
            end
        end
    end

    return nothing
end

function combined_cache_type(first_cache_type::CacheType, second_cache_type::CacheType)::CacheType
    if first_cache_type == QueryData || second_cache_type == QueryData
        return QueryData  # untested
    elseif first_cache_type == MemoryData || second_cache_type == MemoryData
        return MemoryData  # untested
    else
        return MappedData
    end
end

function combined_cache_type(
    first_cache_type::CacheType,
    second_cache_type::CacheType,
    third_cache_type::CacheType,
)::CacheType
    return combined_cache_type(first_cache_type, combined_cache_type(second_cache_type, third_cache_type))
end

function parse_mode(mode::AbstractString)::Tuple{Bool, Bool, Bool}
    if mode == "r"
        (true, false, false)
    elseif mode == "r+"
        (false, false, false)
    elseif mode == "w+"
        (false, true, false)
    elseif mode == "w"
        (false, true, true)
    else
        error("invalid mode: $(mode)")
    end
end

function begin_data_read_lock(format::FormatReader, what::AbstractString...)::Bool
    return read_lock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
end

function end_data_read_lock(format::FormatReader, what::AbstractString...)::Nothing
    read_unlock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
    return nothing
end

struct UpgradeToWriteLockException <: Exception end

function with_data_read_lock(action::Function, format::FormatReader, what::AbstractString...)::Any
    is_top_level = begin_data_read_lock(format, what...)
    try
        return action()
    catch exception
        if !is_top_level || !(exception isa UpgradeToWriteLockException)
            rethrow()
        end
    finally
        end_data_read_lock(format, what...)
    end
    return with_data_write_lock(action, format, what...)
end

function has_data_read_lock(format::FormatReader; read_only::Bool = false)::Bool
    return has_read_lock(format.internal.data_lock; read_only = read_only)
end

function begin_data_write_lock(format::FormatReader, what::AbstractString...)::Nothing
    write_lock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
    return nothing
end

function end_data_write_lock(format::FormatReader, what::AbstractString...)::Nothing
    write_unlock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
    return nothing
end

function with_data_write_lock(action::Function, format::FormatReader, what::AbstractString...)::Any
    begin_data_write_lock(format, what...)
    try
        return action()
    finally
        end_data_write_lock(format, what...)
    end
end

function has_data_write_lock(format::FormatReader)::Bool
    return has_write_lock(format.internal.data_lock)
end

function upgrade_to_data_write_lock(format::FormatReader)::Nothing
    if !has_data_write_lock(format)
        throw(UpgradeToWriteLockException())
    end
    return nothing
end

function format_get_version_counter(format::FormatReader, version_key::DataKey)::UInt32
    return with_read_lock(
        format.internal.cache_lock,
        format.name,
        "cache for version counter of:",
        string(version_key),
    ) do
        return get(format.internal.version_counters, version_key, UInt32(0))
    end
end

function format_increment_version_counter(format::FormatWriter, version_key::DataKey)::Nothing
    with_write_lock(format.internal.cache_lock, format.name, "cache for version counter of:", string(version_key)) do
        previous_version_counter = format_get_version_counter(format, version_key)
        return format.internal.version_counters[version_key] = previous_version_counter + 1
    end
    return nothing
end

function cache_data!(
    format::FormatReader,
    cache_key::AbstractString,
    data::Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray},
    cache_type::CacheType,
)::Nothing
    if data isa AbstractArray
        data = read_only_array(data)
    end
    @debug "cache_data! daf: $(depict(format)) cache_key: $(cache_key) data: $(depict(data)) cache_type: $(cache_type)"
    with_write_lock(format.internal.cache_lock, format.name, "cache for:", cache_key) do  # NOJET
        @assert !haskey(format.internal.cache, cache_key)
        return format.internal.cache[cache_key] = CacheEntry(cache_type, data)
    end
    return nothing
end

"""
    empty_cache!(
        daf::DafReader;
        [clear::Maybe{CacheType} = nothing,
        keep::Maybe{CacheType} = nothing]
    )::Nothing

Clear some cached data. By default, completely empties the caches. You can specify either `clear`, to only forget a
specific [`CacheType`](@ref) (e.g., for clearing only `QueryData`), or `keep`, to forget everything except a specific
[`CacheType`](@ref) (e.g., for keeping only `MappedData`). You can't specify both `clear` and `keep`.
"""
function empty_cache!(daf::DafReader; clear::Maybe{CacheType} = nothing, keep::Maybe{CacheType} = nothing)::Nothing
    @assert clear === nothing || keep === nothing
    with_write_lock(daf.internal.cache_lock, daf.name, "empty_cache!") do
        @debug "empty_cache! daf: $(depict(daf)) clear: $(clear) keep: $(keep)"
        if clear === nothing && keep === nothing
            empty!(daf.internal.cache)
        else
            filter!(daf.internal.cache) do key_value
                cache_type = key_value[2].cache_type
                return cache_type == keep || (cache_type != clear && clear !== nothing)
            end
        end

        if isempty(daf.internal.cache)
            empty!(daf.internal.dependency_cache_keys)
        else
            for (_, dependent_keys) in daf.internal.dependency_cache_keys
                filter(dependent_keys) do dependent_key
                    return haskey(daf.internal.cache, dependent_key)
                end
            end
            filter(daf.internal.dependency_cache_keys) do entry
                dependent_keys = entry[2]
                return !isempty(dependent_keys)
            end
        end
    end
    return nothing
end

end # module
