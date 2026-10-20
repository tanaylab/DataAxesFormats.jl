"""
The [`FormatReader`](@ref) and [`FormatWriter`](@ref) interfaces specify a low-level API for storing `Daf` data. To
extend `Daf` to support an additional format, create a new implementation of this API.

A storage format object contains some named scalar data, a set of axes (each with a unique name for each entry), and
named vector and matrix data based on these axes.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each (ordered) pair of axes.

For matrices, we keep careful track of their layout(@ref). Specifically, a storage format only deals with column-major
matrices, listed under the rows axis first and the columns axis second. A storage format object may hold two copies of
the same matrix, in both possible memory layouts, in which case it will be listed twice, under both axes orders.

In general, storage format objects are as "dumb" as possible, to make it easier to support new storage formats. The
required functions implement a glorified key-value repository, with the absolutely minimal necessary logic to deal with
the separate property namespaces listed above.

For clarity of documentation, we split the type hierarchy to [`DafWriter`](@ref) `<:` [`FormatWriter`](@ref) `<:`
[`DafReader`](@ref) `<:` [`FormatReader`](@ref).

The functions listed here use the [`FormatReader`](@ref) for read-only operations and [`FormatWriter`](@ref) for write
operations into a `Daf` storage. This is a low-level API, not meant to be used from outside the package, and therefore
is not re-exported from the top-level `DataAxesFormats` namespace.

In contrast, the functions using [`DafReader`](@ref) and [`DafWriter`](@ref) describe the high-level API meant to be
used from outside the package, and are re-exported. These functions are listed in the `DataAxesFormats.Readers` and
`DataAxesFormats.Writers` modules. They provide all the logic common to any storage format, allowing us to keep the
format-specific functions as simple as possible.

That is, when implementing a new `Daf` storage format, you should write `struct MyFormat <: DafWriter`, and implement
the functions listed here for both [`FormatReader`](@ref) and [`FormatWriter`](@ref).
"""
module Formats

export CacheGroup
export DafReader
export DafWriter
export empty_cache!
export end_data_write_lock
export MappedData
export MemoryData
export PropertyMetadata
export QueryData

using ..Keys
using ..StorageTypes
using ..Tokens
using Base.Threads
using ConcurrentUtils
using LinearAlgebra
using NamedArrays
using OrderedCollections
using SparseArrays
using TanayLabUtilities

import ..Registry.QueryOperation

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
@enum CacheGroup MappedData MemoryData QueryData

"""
    struct PropertyMetadata
        eltype::Type
        is_sparse::Bool
        nnz::Maybe{Int}
        indtype::Maybe{Type}
        is_packed::Bool
    end

How a vector or matrix property is stored. This is obtained without reading the data, using
[`vector_metadata`](@ref DataAxesFormats.Readers.vector_metadata) or
[`matrix_metadata`](@ref DataAxesFormats.Readers.matrix_metadata).

  - `eltype` - the type of the elements. This is `String` for any string property.
  - `is_sparse` - whether the property is stored as a sparse array.
  - `nnz` - the number of stored (non-zero) elements of a sparse property, or `nothing` for a dense one.
  - `indtype` - the type of the indices of a sparse property, or `nothing` for a dense one.
  - `is_packed` - whether the property is stored using the packed (chunked and compressed) encoding.

!!! note

    A sparse string property stores only its non-empty values. Reading it still returns a dense array.
"""
struct PropertyMetadata
    eltype::Type
    is_sparse::Bool
    nnz::Maybe{Int}
    indtype::Maybe{Type}
    is_packed::Bool
end

function TanayLabUtilities.Brief.brief(metadata::PropertyMetadata)::String
    eltype_name = metadata.eltype <: AbstractString ? "Str" : string(metadata.eltype)
    if metadata.is_sparse
        nnz = metadata.nnz
        @assert nnz !== nothing
        storage = "Sparse $(delimited_number(nnz)) [$(metadata.indtype)]"
    else
        storage = "Dense"
    end
    if metadata.is_packed
        storage *= ", Packed"
    end
    return "$(eltype_name) ($(storage))"
end

# The [`PropertyMetadata`](@ref) of an in-memory (not packed) `array`.
function array_metadata(array::AbstractArray)::PropertyMetadata
    array = base_array(array)
    element_type = eltype(array) <: AbstractString ? String : eltype(array)
    if issparse(array)
        return PropertyMetadata(element_type, true, nnz(array), SparseArrays.indtype(array), false)
    else
        return PropertyMetadata(element_type, false, nothing, nothing, false)
    end
end

const CacheData = Union{
    AbstractSet{<:AbstractString},  # Names
    AbstractVector{<:AbstractString},  # Axis entries
    StorageScalar,  # Scalar
    NamedArray,  # Vector / Matrix
    AbstractDict{<:AbstractString, <:Integer}, # Axis dictionary
    QueryOperation,  # View query
    Missing,  # View hidden
    AbstractDict{String, Any},  # Parsed JSON object (format metadata)
}

# The data of a cache entry while its slow computation runs. The computing task sets the entry's data to the result (or
# deletes the entry if the computation fails), then sets `is_done`, then notifies `done_event`.
struct PendingResult
    is_done::Threads.Atomic{Bool}
    done_event::Threads.Event
end

function PendingResult()
    return PendingResult(Threads.Atomic{Bool}(false), Threads.Event())
end

mutable struct CacheEntry  # NOLINT
    cache_group::CacheGroup
    data::Union{PendingResult, CacheData}
end

@enum CacheType CachedAxis CachedData CachedQuery CachedNames

const AxisCacheKey = Tuple{AxisKey, Bool}

const NamesKey = Union{AbstractString, Tuple{AbstractString}, Tuple{AbstractString, AbstractString, Bool}}

const CacheKey = Tuple{CacheType, Union{AxisCacheKey, PropertyKey, AbstractString, NamesKey}, Symbol}  # NOLINT

function Base.show(io::IO, cache_key::CacheKey)::Nothing
    type, key, qualifier = cache_key
    if type == CachedAxis
        @assert key isa AxisCacheKey
        if key[2]
            print(io, "axis_dict[axis: $(key[1])]:$(qualifier)")
        else
            print(io, "axis_vector[axis: $(key[1])]:$(qualifier)")
        end
    elseif type == CachedQuery
        @assert key isa AbstractString  # UNTESTED
        print(io, "query[$(key)]:$(qualifier)")  # UNTESTED
    elseif type == CachedData
        if key isa AbstractString
            print(io, "scalar[$(key)]:$(qualifier)")
        elseif key isa Tuple{AbstractString, AbstractString}
            print(io, "vector[axis: $(key[1]) name: $(key[2])]:$(qualifier)")
        elseif key isa Tuple{AbstractString, AbstractString, AbstractString}
            print(io, "matrix[rows_axis: $(key[1]) columns_axis: $(key[2]) name: $(key[3])]:$(qualifier)")
        else
            @assert false
        end
    elseif type == CachedNames
        if key isa AbstractString
            print(io, "names[$(key)]:$(qualifier)")
        elseif key isa Tuple{AbstractString}
            print(io, "vectors[axis: $(key[1])]:$(qualifier)")
        elseif key isa Tuple{AbstractString, AbstractString, Bool}
            if key[3]
                print(io, "matrices[relayout rows_axis: $(key[1]) columns_axis: $(key[2])]:$(qualifier)")  # UNTESTED
            else
                print(io, "matrices[rows_axis: $(key[1]) columns_axis: $(key[2])]:$(qualifier)")
            end
        end
    else
        @assert false
    end

    return nothing
end

function scalars_set_cache_key(qualifier::Symbol = :value)::CacheKey
    return (CachedNames, "scalars", qualifier)
end

function axes_set_cache_key(qualifier::Symbol = :value)::CacheKey
    return (CachedNames, "axes", qualifier)
end

function vectors_set_cache_key(axis::AbstractString, qualifier::Symbol = :value)::CacheKey
    return (CachedNames, (axis,), qualifier)
end

function matrices_set_cache_key(
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    qualifier::Symbol = :value;
    relayout::Bool,
)::CacheKey
    if relayout && columns_axis < rows_axis
        rows_axis, columns_axis = columns_axis, rows_axis
    end
    return (CachedNames, (rows_axis, columns_axis, relayout), qualifier)
end

function scalar_cache_key(name::AbstractString, qualifier::Symbol = :value)::CacheKey
    return (CachedData, name, qualifier)
end

function axis_vector_cache_key(axis::AxisKey, qualifier::Symbol = :value)::CacheKey
    return (CachedAxis, (axis, false), qualifier)
end

function axis_dict_cache_key(axis::AbstractString, qualifier::Symbol = :value)::CacheKey
    return (CachedAxis, (axis, true), qualifier)
end

function vector_cache_key(axis::AbstractString, name::AbstractString, qualifier::Symbol = :value)::CacheKey
    return (CachedData, (axis, name), qualifier)
end

function matrix_cache_key(
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    qualifier::Symbol = :value,
)::CacheKey
    return (CachedData, (rows_axis, columns_axis, name), qualifier)
end

"""
    struct Internal ... end

Internal data we need to keep in any concrete [`FormatReader`](@ref). This has to be available as a `.internal` data
member of the concrete format. This enables all the high-level [`DafReader`](@ref) and [`DafWriter`](@ref) functions.

The constructor will automatically call `unique_name` to try and make the names unique for improved error messages.
"""
struct Internal  # NOLINT
    cache::Dict{CacheKey, CacheEntry}
    dependents_of_cache_keys::Dict{CacheKey, Set{CacheKey}}
    dependency_versions_of_cache_keys::Dict{CacheKey, Dict{CacheKey, UInt32}}
    lazy_arrays_of_cache_keys::Dict{CacheKey, Vector{WeakRef}}
    version_counters::Dict{CacheKey, UInt32}
    cache_lock::ExtendedReadWriteLock
    data_lock::ExtendedReadWriteLock
    is_frozen::Bool
    pending_condition::Threads.Condition
    pending_count::Vector{UInt32}
    packed_default::Bool
    shared_resource::Any
end

function Internal(;
    is_frozen::Bool,
    data_lock::Maybe{ExtendedReadWriteLock} = nothing,
    packed_default::Bool = false,
    shared_resource::Any = nothing,
)::Internal
    return Internal(
        Dict{CacheKey, CacheEntry}(),
        Dict{CacheKey, Set{CacheKey}}(),
        Dict{CacheKey, Dict{CacheKey, UInt32}}(),
        Dict{CacheKey, Vector{WeakRef}}(),
        Dict{CacheKey, UInt32}(),
        ExtendedReadWriteLock(),
        data_lock === nothing ? ExtendedReadWriteLock() : data_lock,
        is_frozen,
        Threads.Condition(),
        UInt32[0],
        packed_default,
        shared_resource,
    )
end

"""
An low-level abstract interface for reading from `Daf` storage formats.

We require each storage format to have a `.name` and an `.internal::`[`Internal`](@ref) property. This enables all the
high-level [`DafReader`](@ref) functions.

Each storage format must implement the functions listed below for reading from the storage.
"""
abstract type FormatReader end  # NOLINT

"""
A high-level abstract interface for read-only access to `Daf` data.

All the functions for this type are provided based on the functions required for [`FormatReader`](@ref). See the
[`Readers`](@ref DataAxesFormats.Readers) module for their description.
"""
abstract type DafReader <: FormatReader end

"""
An abstract interface for writing into `Daf` storage formats.

Each storage format must implement the functions listed below for writing into the storage.
"""
abstract type FormatWriter <: DafReader end  # NOLINT

"""
A high-level abstract interface for write access to `Daf` data.

All the functions for this type are provided based on the functions required for [`FormatWriter`](@ref). See the
[`Writers`](@ref DataAxesFormats.Writers) module for their description.
"""
abstract type DafWriter <: FormatWriter end

function Base.show(io::IO, format_reader::FormatReader)::Nothing
    print(io, brief(format_reader))
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
    )::Maybe{CacheGroup}

Implement setting the `value` of a scalar property with some `name` in `format`. Return the [`CacheGroup`](@ref) to use
when caching the just-written value (as would be returned by [`format_get_scalar`](@ref)), or `nothing` to skip caching.

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
    format_get_scalar(
        format::FormatReader,
        name::AbstractString,
    )::Tuple{StorageScalar, CacheGroup}

Implement fetching the value of a scalar property with some `name` in `format`, together with a per-item
[`CacheGroup`](@ref) describing how the returned value should be cached. Formats that can classify a returned value's
mmappability (e.g. [`H5df`](@ref DataAxesFormats.H5dfFormat.H5df)) should return `MappedData` when handing back an
mmapped view and `MemoryData` when handing back an eagerly-read copy.

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
    format_axis_vector(
        format::FormatReader,
        axis::AbstractString,
    )::Tuple{AbstractVector{<:AbstractString}, CacheGroup}

Implement fetching the unique names of the entries of some `axis` of `format`, together with a per-item
[`CacheGroup`](@ref) describing how the returned vector should be cached.

This trusts that we have a read lock on the data set, and that the `axis` exists in `format`.
"""
function format_axis_vector end

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
        is_packed::Bool,
    )::Nothing

Implement setting a vector property with some `name` for some `axis` in `format`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

The resolved `is_packed` flag indicates the requested encoding for this write (`true` means the packed / chunked +
compressed on-disk encoding, `false` means today's flat encoding). Backends that do not support packed encoding ignore
the flag.

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
        is_packed::Bool,
    )::Tuple{AbstractVector{T}, Maybe{CacheGroup}} where {T <: StorageReal}

Implement creating an empty dense vector property with some `name` for some `axis` in `format`. Return the buffer to be
filled by the caller, together with the [`CacheGroup`](@ref) to use when caching the filled buffer (as would be returned
by [`format_get_vector`](@ref)), or `nothing` to skip caching.

The resolved `is_packed` flag indicates the requested encoding for this write. Backends that do not support packed
encoding ignore the flag.

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
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        filled::AbstractVector{<:StorageReal},
    )::Nothing

Allow the `format` to perform finalization once the empty dense vector has been `filled` (e.g. flushing metadata,
patching chunk checksums). The default does nothing.

This trusts we have a write lock on the data set, that the `axis` exists in `format`, that the `name` vector property
exists for the `axis`, and that `filled` is the same buffer that was returned by
[`format_get_empty_dense_vector!`](@ref).
"""
function format_filled_empty_dense_vector!(
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractVector{<:StorageReal},
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
        is_packed::Bool,
    )::Tuple{AbstractVector{I}, AbstractVector{T}, Maybe{CacheGroup}}
    where {T <: StorageReal, I <: StorageInteger}

Implement creating an empty sparse vector property with some `name` for some `axis` in `format`. Return the `nzind`
and `nzval` buffers to be filled by the caller, together with the [`CacheGroup`](@ref) to use when caching the
assembled sparse vector (as would be returned by [`format_get_vector`](@ref)), or `nothing` to skip caching.

The resolved `is_packed` flag indicates the requested encoding for this write. Backends that do not support packed
encoding ignore the flag.

This trusts we have a write lock on the data set, that the `axis` exists in `format` and that the vector property `name`
isn't `"name"`, and that it does not exist for the `axis`.
"""
function format_get_empty_sparse_vector! end

"""
    format_filled_empty_sparse_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        filled::SparseVector{<:StorageReal, <:StorageInteger},
    )::Nothing

Allow the `format` to perform finalization once the empty sparse vector has been `filled` (e.g. flushing metadata,
patching chunk checksums). The default does nothing.
"""
function format_filled_empty_sparse_vector!( # UNTESTED
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
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
    format_get_vector(
        format::FormatReader,
        axis::AbstractString,
        name::AbstractString,
    )::Tuple{StorageVector, CacheGroup}

Implement fetching the vector property with some `name` for some `axis` in `format`, together with a per-item
[`CacheGroup`](@ref) describing how the returned vector should be cached. The returned vector must keep alive any memory
it reads from, since the caller may keep it after it was removed from the cache.

This trusts that we have a read lock on the data set, that the `axis` exists in `format`, and the `name` vector property
exists for the `axis`.
"""
function format_get_vector end

"""
    format_vector_metadata(format::FormatReader, axis::AbstractString, name::AbstractString)::PropertyMetadata

Implement fetching the [`PropertyMetadata`](@ref) of the vector property with some `name` for some `axis` in `format`,
without reading its data.

This trusts that we have a read lock on the data set, that the `axis` exists in `format`, and the `name` vector property
exists for the `axis`.
"""
function format_vector_metadata end

"""
    format_has_matrix(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
    )::Bool

Implement checking whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in
`format`. If `cache` also checks whether the matrix exists in the cache.

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
        is_packed::Bool,
    )::Nothing

Implement setting the matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

The resolved `is_packed` flag indicates the requested encoding for this write. Backends that do not support packed
encoding ignore the flag.

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
        is_packed::Bool,
    )::Tuple{AbstractMatrix{T}, Maybe{CacheGroup}} where {T <: StorageReal}

Implement creating an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.
Return the buffer to be filled by the caller, together with the [`CacheGroup`](@ref) to use when caching the filled
buffer (as would be returned by [`format_get_matrix`](@ref)), or `nothing` to skip caching.

The resolved `is_packed` flag indicates the requested encoding for this write. Backends that do not support packed
encoding ignore the flag.

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
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        filled::AbstractMatrix{<:StorageReal},
    )::Nothing

Allow the `format` to perform finalization once the empty dense matrix has been `filled` (e.g. flushing metadata,
patching chunk checksums). The default does nothing.

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format`, that the
`name` matrix property exists for them, and that `filled` is the same buffer that was returned by
[`format_get_empty_dense_matrix!`](@ref).
"""
function format_filled_empty_dense_matrix!(
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::AbstractMatrix{<:StorageReal},
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
        nnz::StorageInteger,
        indtype::Type{I},
        is_packed::Bool,
    )::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}, Maybe{CacheGroup}}
    where {T <: StorageReal, I <: StorageInteger}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.
Return the `colptr`, `rowval` and `nzval` buffers to be filled by the caller, together with the [`CacheGroup`](@ref) to
use when caching the assembled sparse matrix (as would be returned by [`format_get_matrix`](@ref)), or `nothing` to skip
caching.

The resolved `is_packed` flag indicates the requested encoding for this write. Backends that do not support packed
encoding ignore the flag.

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format` and that the
`name` matrix property does not exist for them.
"""
function format_get_empty_sparse_matrix! end

"""
    format_filled_empty_sparse_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        filled::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
    )::Nothing

Allow the `format` to perform finalization once the empty sparse matrix has been `filled` (e.g. flushing metadata,
patching chunk checksums). The default does nothing.
"""
function format_filled_empty_sparse_matrix!( # UNTESTED
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Nothing
    return nothing
end

"""
    format_relayout_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::StorageMatrix,
        is_packed::Bool,
    )::StorageMatrix

`relayout!` the existing `name` column-major `matrix` property for the `rows_axis` and the `columns_axis` and
store the results as a row-major matrix property (that is, with flipped axes).

The resolved `is_packed` flag indicates the requested encoding for the flipped matrix. This is the same flag the caller
would have passed to a write of a fresh matrix, so the flipped representation produced by `relayout!` matches the
encoding the caller intended.

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
    )::StorageMatrix

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
    )::Tuple{StorageMatrix, CacheGroup}

Implement fetching the matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`, together
with a per-item [`CacheGroup`](@ref) describing how the returned matrix should be cached. As in
[`format_get_vector`](@ref), the returned matrix must keep alive any memory it reads from.

This trusts that we have a read lock on the data set, and that the `rows_axis` and `columns_axis` exist in `format`, and
the `name` matrix property exists for them.
"""
function format_get_matrix end

"""
    format_matrix_metadata(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
    )::PropertyMetadata

Implement fetching the [`PropertyMetadata`](@ref) of the matrix property with some `name` for some `rows_axis` and
`columns_axis` in `format`, without reading its data.

This trusts that we have a read lock on the data set, that the `rows_axis` and `columns_axis` exist in `format`, and the
`name` matrix property exists for them.
"""
function format_matrix_metadata end

"""
    format_description_header(format::FormatReader, lines::Vector{String}, deep::Bool)::Nothing

Allow a `format` to amit additional description header lines.

This trusts that we have a read lock on the data set.
"""
function format_description_header(format::FormatReader, indent::AbstractString, lines::Vector{String}, ::Bool)::Nothing
    push!(lines, "$(indent)type: $(nameof(typeof(format)))")
    return nothing
end

"""
    format_description_footer(
        format::FormatReader,
        lines::Vector{String};
        cache::Bool,
        deep::Bool,
        tensors::Bool,
    )::Nothing

Allow a `format` to amit additional description footer lines. If `deep`, this also emit the description of any data sets
nested in this one, if any.

This trusts that we have a read lock on the data set.
"""
function format_description_footer(
    ::FormatReader,
    ::AbstractString,
    ::AbstractVector{<:AbstractString};
    cache::Bool,  # NOLINT
    deep::Bool,  # NOLINT
    tensors::Bool,  # NOLINT
)::Nothing
    return nothing
end

function put_in_cache!(format::FormatReader, cache_key::CacheKey, data::CacheData, cache_group::CacheGroup)::Nothing
    @assert has_data_read_lock(format)
    @assert has_write_lock(format.internal.cache_lock)
    if data isa AbstractArray
        data = read_only_array(data)
    end
    @debug (
        "put_in_cache! daf: $(brief(format)) cache_key: $(cache_key) data: $(brief(data)) " *
        "cache_group: $(cache_group)"
    )
    format.internal.cache[cache_key] = CacheEntry(cache_group, data)
    return nothing
end

function set_in_cache!(format::FormatReader, cache_key::CacheKey, data::CacheData, cache_group::CacheGroup)::Nothing
    return with_cache_write_lock(format, "for set_in_cache!:", cache_key) do
        return put_in_cache!(format, cache_key, data, cache_group)
    end
end

function format_has_cached_matrix(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return format_has_matrix(format, rows_axis, columns_axis, name) ||
           with_cache_read_lock(format, "for has_cached_matrix:", rows_axis, columns_axis, name) do
        return haskey(format.internal.cache, matrix_cache_key(rows_axis, columns_axis, name))
    end
end

"""
    format_matrix_cache_format(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
    )::FormatReader

The format whose cache should hold whatever is derived from a matrix. By default this is the format itself, which is
the one holding the matrix.

A wrapper holding no data of its own answers with the format the matrix comes from, so that what is derived from it is
cached beside it. Otherwise the wrapper caches something and then, asking what it wraps whether it has it, answers no -
and each wrapper of the same data derives it again rather than sharing the one copy.
"""
function format_matrix_cache_format(
    format::FormatReader,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
)::FormatReader
    return format
end

function get_through_cache(getter::Function, format::FormatReader, cache_key::CacheKey, ::Type{T})::T where {T}
    @assert has_data_read_lock(format)
    cached = nothing
    while cached === nothing
        cache_entry = with_cache_read_lock(format, "for get_from_cache:", cache_key) do
            return get(format.internal.cache, cache_key, nothing)
        end
        cached = result_from_cache(cache_entry)
        # If another computation became pending meanwhile, this returns `nothing`, and the loop waits for it.
        if cached === nothing
            cached = write_through_cache(getter, format, cache_key, T)
        end
    end
    return cached::T
end

function get_slow_through_cache(
    getter::Function,
    format::FormatReader,
    cache_key::CacheKey,
    ::Type{T},
    cache_group::CacheGroup,
)::T where {T}
    @assert has_data_read_lock(format)
    cached = nothing
    while cached === nothing
        cached = valid_result_from_cache(format, cache_key)
        # If another computation became pending meanwhile, this returns `nothing`, and the loop waits for it.
        if cached === nothing
            cached = write_slow_through_cache(getter, format, cache_key, T, cache_group)
        end
    end
    return cached::T
end

# The cached result, or `nothing` if it needs to be computed. A query result is valid only while the versions of its
# dependencies are the same as when it was computed. The versions are checked without the cache lock, since they may
# come from other dafs (e.g., the bases of a chain). A stale entry is deleted unless another task replaced it meanwhile.
function valid_result_from_cache(format::FormatReader, cache_key::CacheKey)::Any
    cache_entry, dependency_versions = with_cache_read_lock(format, "for get_from_cache:", cache_key) do
        return (
            get(format.internal.cache, cache_key, nothing),
            get(format.internal.dependency_versions_of_cache_keys, cache_key, nothing),
        )
    end
    result = result_from_cache(cache_entry)
    if result !== nothing && dependency_versions !== nothing && !are_current_versions(format, dependency_versions)
        with_cache_write_lock(format, "for stale:", cache_key) do
            if get(format.internal.cache, cache_key, nothing) === cache_entry
                increment_version_counter!(format, cache_key)
                delete!(format.internal.cache, cache_key)
                delete!(format.internal.dependency_versions_of_cache_keys, cache_key)
            end
            return nothing
        end
        result = nothing
    end
    return result
end

function are_current_versions(format::FormatReader, dependency_versions::AbstractDict{CacheKey, UInt32})::Bool
    for (dependency_key, dependency_version) in dependency_versions
        if format_get_version_counter(format, dependency_key) != dependency_version
            return false
        end
    end
    return true
end

function result_from_cache(::Nothing)::Nothing
    return nothing
end

# The cached result, or `nothing` if it needs to be computed. A pending entry is waited for. If its computation failed,
# the entry was deleted without a result, and the caller computes it again.
function result_from_cache(cache_entry::CacheEntry)::Any
    pending_result = cache_entry.data
    if pending_result isa PendingResult
        wait_for_pending_result(pending_result)
        result = cache_entry.data
        return result isa PendingResult ? nothing : result
    end
    return pending_result
end

# How many times a waiter checks a pending result before it blocks. This avoids a context switch when the computation
# is about to finish.
const PENDING_RESULT_SPIN_COUNT = 1000

function wait_for_pending_result(pending_result::PendingResult)::Nothing
    for _ in 1:PENDING_RESULT_SPIN_COUNT
        if pending_result.is_done[]
            return nothing
        end
        ccall(:jl_cpu_pause, Cvoid, ())
    end
    wait(pending_result.done_event)
    return nothing
end

function write_through_cache(getter::Function, format::FormatReader, cache_key::CacheKey, ::Type{T})::Maybe{T} where {T}
    return with_cache_write_lock(format, "for get_through_cache:", cache_key) do  # NOJET
        cache_entry = get(format.internal.cache, cache_key, nothing)
        if cache_entry !== nothing
            if cache_entry.data isa PendingResult  # UNTESTED
                return nothing  # UNTESTED
            else
                return cache_entry.data::T  # UNTESTED
            end
        else
            result, cache_group = getter()
            if cache_group !== nothing
                put_in_cache!(format, cache_key, result, cache_group)
            end
            return result
        end
    end
end

function write_slow_through_cache(
    getter::Function,
    format::FormatReader,
    cache_key::CacheKey,
    ::Type{T},
    cache_group::CacheGroup,
)::Maybe{T} where {T}
    cached = with_cache_write_lock(format, "for get_through_cache:", cache_key) do
        existing_entry = get(format.internal.cache, cache_key, nothing)
        if existing_entry !== nothing
            if existing_entry.data isa PendingResult  # UNTESTED
                return nothing  # UNTESTED
            else
                return existing_entry.data::T  # UNTESTED
            end
        else
            pending_entry = CacheEntry(cache_group, PendingResult())
            format.internal.cache[cache_key] = pending_entry
            lock(format.internal.pending_condition) do
                format.internal.pending_count[1] += 1
                return nothing
            end
            return pending_entry
        end
    end
    if cached isa CacheEntry
        return fill_pending_cache_entry!(getter, format, cache_key, cached)::T
    end
    return cached
end

# Compute the data of the pending `cache_entry` of the `cache_key` using the `getter`, store it in the entry, and return
# it. If the `getter` fails, the entry is deleted. Either way, any tasks waiting for the entry are released.
function fill_pending_cache_entry!(
    getter::Function,
    format::FormatReader,
    cache_key::CacheKey,
    cache_entry::CacheEntry,
)::Any
    pending_result = cache_entry.data
    @assert pending_result isa PendingResult
    try
        result, dependency_keys = getter()
        # The versions may come from other dafs (e.g., the bases of a chain), so they are collected before taking the
        # cache lock.
        dependency_versions = if dependency_keys === nothing
            nothing
        else
            Dict{CacheKey, UInt32}(
                dependency_key => format_get_version_counter(format, dependency_key) for
                dependency_key in dependency_keys
            )
        end
        with_cache_write_lock(format, "for slow:", cache_key) do
            if dependency_versions !== nothing
                for dependency_key in keys(dependency_versions)
                    put_cached_dependency_key!(format, cache_key, dependency_key)
                end
                format.internal.dependency_versions_of_cache_keys[cache_key] = dependency_versions
            end
            cache_entry.data = result
            lock(format.internal.pending_condition) do
                format.internal.pending_count[1] -= 1
                if format.internal.pending_count[1] == 0
                    notify(format.internal.pending_condition)
                end
            end
            return nothing
        end
        return result
    catch
        with_cache_write_lock(format, "for slow error:", cache_key) do  # FLAKY TESTED
            delete!(format.internal.cache, cache_key)
            lock(format.internal.pending_condition) do
                format.internal.pending_count[1] -= 1
                if format.internal.pending_count[1] == 0
                    notify(format.internal.pending_condition)
                end
            end
            return nothing
        end
        rethrow()
    finally
        pending_result.is_done[] = true
        notify(pending_result.done_event)
    end
end

function get_scalars_set_through_cache(format::FormatReader)::AbstractSet{<:AbstractString}
    return get_through_cache(format, scalars_set_cache_key(), AbstractSet{<:AbstractString}) do
        return (format_scalars_set(format), MemoryData)
    end
end

function get_axes_set_through_cache(format::FormatReader)::AbstractSet{<:AbstractString}
    return get_through_cache(format, axes_set_cache_key(), AbstractSet{<:AbstractString}) do
        return (format_axes_set(format), MemoryData)
    end
end

function get_vectors_set_through_cache(format::FormatReader, axis::AbstractString)::AbstractSet{<:AbstractString}
    return get_through_cache(format, vectors_set_cache_key(axis), AbstractSet{<:AbstractString}) do
        return (format_vectors_set(format, axis), MemoryData)
    end
end

function get_matrices_set_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    return get_through_cache(
        format,
        matrices_set_cache_key(rows_axis, columns_axis; relayout = false),
        AbstractSet{<:AbstractString},
    ) do
        return (format_matrices_set(format, rows_axis, columns_axis), MemoryData)
    end
end

# The names of the matrices stored in either layout.
function get_relayout_matrices_set_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    return get_through_cache(
        format,
        matrices_set_cache_key(rows_axis, columns_axis; relayout = true),
        AbstractSet{<:AbstractString},
    ) do
        first_names = get_matrices_set_through_cache(format, rows_axis, columns_axis)
        second_names = get_matrices_set_through_cache(format, columns_axis, rows_axis)
        return (union(first_names, second_names), MemoryData)
    end
end

function get_scalar_through_cache(format::FormatReader, name::AbstractString)::StorageScalar
    return get_through_cache(format, scalar_cache_key(name), StorageScalar) do
        return format_get_scalar(format, name)
    end
end

function get_axis_vector_through_cache(format::FormatReader, axis::AbstractString)::AbstractVector{<:AbstractString}
    return get_through_cache(format, axis_vector_cache_key(axis), AbstractVector{<:AbstractString}) do
        vector, cache_group = format_axis_vector(format, axis)
        return (read_only_array(vector), cache_group)
    end
end

# Build the name-to-index lookup dictionary that a `NamedArray` uses for an axis. The key type is forced to
# `AbstractString` so that lookups accept any string type; if it were the concrete type of `names` (for memory-mapped
# data, a zero-copy `SubString{StringView{...}}`), `NamedArray` would only accept that exact type as a lookup key.
function names_dict(names::AbstractVector{<:AbstractString})::OrderedDict{AbstractString, Int}
    dict = OrderedDict{AbstractString, Int}()
    for (index, name) in enumerate(names)
        dict[name] = index
    end
    length(dict) == length(names) || error("Cannot have duplicated names for indices")
    return dict
end

function get_axis_dict_through_cache(
    format::FormatReader,
    axis::AbstractString,
)::AbstractDict{<:AbstractString, <:Integer}
    return get_through_cache(format, axis_dict_cache_key(axis), AbstractDict{<:AbstractString, <:Integer}) do
        names = get_axis_vector_through_cache(format, axis)
        return (names_dict(names), MemoryData)
    end
end

function get_vector_through_cache(format::FormatReader, axis::AbstractString, name::AbstractString)::NamedArray
    cache_key = vector_cache_key(axis, name)
    return get_through_cache(format, cache_key, NamedArray) do
        vector, cache_group = format_get_vector(format, axis, name)
        register_lazy_array!(format, cache_key, vector)
        return (as_named_vector(format, axis, read_only_array(vector)), cache_group)
    end
end

function get_matrix_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::NamedArray
    cache_key = matrix_cache_key(rows_axis, columns_axis, name)
    return get_through_cache(format, cache_key, NamedArray) do
        matrix, cache_group = format_get_matrix(format, rows_axis, columns_axis, name)
        register_lazy_array!(format, cache_key, matrix)
        return (as_named_matrix(format, rows_axis, columns_axis, read_only_array(matrix)), cache_group)
    end
end

function get_relayout_matrix_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::NamedArray
    # The transposed copy is cached beside the matrix it is of, rather than in whatever was read through to reach it.
    # The stored layout says which format that is, since that is the one holding the data being transposed.
    format = format_matrix_cache_format(format, columns_axis, rows_axis, name)
    @assert !format_has_matrix(format, rows_axis, columns_axis, name)
    stored_matrix = get_matrix_through_cache(format, columns_axis, rows_axis, name).array
    return get_slow_through_cache(format, matrix_cache_key(rows_axis, columns_axis, name), NamedArray, MemoryData) do
        relayout_matrix = as_named_matrix(format, rows_axis, columns_axis, read_only_array(flipped(stored_matrix)))
        return (relayout_matrix, Set{CacheKey}([matrix_cache_key(columns_axis, rows_axis, name)]))
    end
end

function cache_scalar!(
    format::FormatReader,
    name::AbstractString,
    value::StorageScalar,
    cache_group::Maybe{CacheGroup},
)::Nothing
    if cache_group !== nothing
        set_in_cache!(format, scalar_cache_key(name), value, cache_group)
    end
    return nothing
end

function cache_vector!(
    format::FormatReader,
    axis::AbstractString,
    name::AbstractString,
    vector::NamedVector,
    cache_group::Maybe{CacheGroup},
)::Nothing
    if cache_group !== nothing
        set_in_cache!(format, vector_cache_key(axis, name), vector, cache_group)
    end
    return nothing
end

function cache_matrix!(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::NamedMatrix,
    cache_group::Maybe{CacheGroup},
)::Nothing
    if cache_group !== nothing
        set_in_cache!(format, matrix_cache_key(rows_axis, columns_axis, name), matrix, cache_group)
    end
    return nothing
end

function as_named_vector(::FormatReader, axis::AbstractString, vector::NamedVector)::NamedArray
    if dimnames(vector) != axis
        vector = NamedArray(vector.array, vector.dicts, (axis,))
    end
    return vector
end

function as_named_vector(format::FormatReader, axis::AbstractString, vector::AbstractVector)::NamedArray
    axis_dict = get_axis_dict_through_cache(format, axis)
    return NamedArray(vector, (axis_dict,), (axis,))
end

function as_named_matrix(
    ::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::NamedMatrix,
)::NamedArray
    if dimnames(matrix) != (rows_axis, columns_axis)
        matrix = NamedArray(matrix.array, matrix.dicts, (rows_axis, columns_axis))
    end
    return matrix
end

function as_named_matrix(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::AbstractMatrix,
)::NamedArray
    rows_axis_dict = get_axis_dict_through_cache(format, rows_axis)
    columns_axis_dict = get_axis_dict_through_cache(format, columns_axis)
    @assert size(matrix) == (length(rows_axis_dict), length(columns_axis_dict))
    return NamedArray(matrix, (rows_axis_dict, columns_axis_dict), (rows_axis, columns_axis))
end

function put_cached_dependency_key!(format::FormatReader, cache_key::CacheKey, dependency_key::CacheKey)::Nothing
    @assert has_data_read_lock(format)
    @assert has_write_lock(format.internal.cache_lock)
    if dependency_key == cache_key
        return nothing
    end
    keys_set = get!(format.internal.dependents_of_cache_keys, dependency_key) do
        return Set{CacheKey}()
    end
    size_before = length(keys_set)
    push!(keys_set, cache_key)
    size_after = length(keys_set)
    if size_after > size_before
        @debug (
            "put_cached_dependency_key! daf: $(brief(format)) cache_key: $(cache_key) " *
            "dependency_key: $(dependency_key)"
        )
    end
    return nothing
end

# Whether a storage array read from a format is lazy: it reads its data from storage on demand. A lazy array must be
# told when its property is modified, so it won't mix old cached data with the new storage. The formats that return lazy
# arrays add methods to this, to `invalidate_lazy_array!` and to `purge_lazy_array!`.
function is_lazy_array(::AbstractArray)::Bool
    return false
end

# Mark a lazy array as reading a property that was modified, so any later read of it throws, and release its memory.
function invalidate_lazy_array! end

# Release the memory a lazy array holds (decoded chunks and its materialization). It stays readable.
function purge_lazy_array! end

# Remember a lazy storage array read for a cache key, so `invalidate_cached!` of that key invalidates it, even after
# `empty_cache!` dropped it from the cache. Must be called under the cache write lock.
function register_lazy_array!(format::FormatReader, cache_key::CacheKey, array::AbstractArray)::Nothing
    if is_lazy_array(array)
        weak_references = get!(Vector{WeakRef}, format.internal.lazy_arrays_of_cache_keys, cache_key)
        filter!(weak_reference -> weak_reference.value !== nothing, weak_references)
        push!(weak_references, WeakRef(array))
    end
    return nothing
end

function invalidate_cached!(format::FormatReader, cache_key::CacheKey)::Nothing
    @debug "invalidate_cached! daf: $(brief(format)) cache_key: $(cache_key)"
    @debug "- delete cache_key: $(cache_key)"
    with_cache_write_lock(format, "for invalidate key:", cache_key) do
        increment_version_counter!(format, cache_key)
        delete!(format.internal.cache, cache_key)

        weak_references = pop!(format.internal.lazy_arrays_of_cache_keys, cache_key, nothing)
        if weak_references !== nothing
            for weak_reference in weak_references
                lazy_array = weak_reference.value
                if lazy_array !== nothing
                    invalidate_lazy_array!(lazy_array)
                end
            end
        end

        dependents_keys = pop!(format.internal.dependents_of_cache_keys, cache_key, nothing)
        if dependents_keys !== nothing
            for dependent_key in dependents_keys
                @debug "- delete dependent_key: $(dependent_key)"
                increment_version_counter!(format, dependent_key)
                delete!(format.internal.cache, dependent_key)
                delete!(format.internal.dependency_versions_of_cache_keys, dependent_key)
            end
        end
        return delete!(format.internal.dependency_versions_of_cache_keys, cache_key)
    end

    return nothing
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

function begin_data_read_lock(format::FormatReader, what::Any...)::Nothing
    if isempty(what)
        lazy_what = () -> "$(format.name) data"  # UNTESTED
    else
        lazy_what = () -> join([format.name, "data for", what...], " ")
    end
    lock_read(format.internal.data_lock; what = lazy_what)
    return nothing
end

function end_data_read_lock(format::FormatReader, what::Any...)::Nothing
    if isempty(what)
        lazy_what = () -> "$(format.name) data"
    else
        lazy_what = () -> join([format.name, "data for", what...], " ")
    end
    unlock_read(format.internal.data_lock; what = lazy_what)
    return nothing
end

function with_cache_read_lock(action::Function, format::FormatReader, what::Any...)::Any
    if isempty(what)
        lazy_what = () -> "$(format.name) cache"  # UNTESTED
    else
        lazy_what = () -> join([format.name, "cache for", what...], " ")
    end
    return lock_read(action, format.internal.cache_lock; what = lazy_what)
end

function with_cache_write_lock(action::Function, format::FormatReader, what::Any...)::Any
    if isempty(what)
        lazy_what = () -> "$(format.name) cache"  # UNTESTED
    else
        lazy_what = () -> join([format.name, "cache for", what...], " ")
    end
    return lock_write(action, format.internal.cache_lock; what = lazy_what)
end

function with_data_read_lock(action::Function, format::FormatReader, what::Any...)::Any
    if isempty(what)
        lazy_what = () -> "$(format.name) data"  # UNTESTED
    else
        lazy_what = () -> join([format.name, "data for", what...], " ")
    end
    begin_data_read_lock(format, lazy_what)
    try
        return action()
    finally
        end_data_read_lock(format, lazy_what)
    end
end

function has_data_read_lock(format::FormatReader; read_only::Bool = false)::Bool
    return has_read_lock(format.internal.data_lock; read_only)
end

function begin_data_write_lock(format::FormatReader, what::Any...)::Nothing
    if isempty(what)
        lazy_what = () -> "$(format.name) data"
    else
        lazy_what = () -> join([format.name, "data for", what...], " ")
    end
    lock_write(format.internal.data_lock; what = lazy_what)
    return nothing
end

function end_data_write_lock(format::FormatReader, what::Any...)::Nothing
    if isempty(what)
        lazy_what = () -> "$(format.name) data"
    else
        lazy_what = () -> join([format.name, "data for", what...], " ")
    end
    unlock_write(format.internal.data_lock; what = lazy_what)
    return nothing
end

function with_data_write_lock(action::Function, format::FormatReader, what::Any...)::Any
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

# The version counter of a cache key: how many times it was invalidated. Every write invalidates every key it affects,
# whether it is cached or not. The counter only ever increases, and `empty_cache!` never resets it. So a value computed
# from the data of some keys is valid as long as their counters are unchanged.
function format_get_version_counter(format::FormatReader, cache_key::CacheKey)::UInt32
    return with_cache_read_lock(format, "for version counter of:", cache_key) do
        return get(format.internal.version_counters, cache_key, UInt32(0))
    end
end

# Increment the version counter of a cache key. Must be called under the cache write lock.
function increment_version_counter!(format::FormatReader, cache_key::CacheKey)::Nothing
    format.internal.version_counters[cache_key] = get(format.internal.version_counters, cache_key, UInt32(0)) + 1
    return nothing
end

"""
    invalidate_axis_data!(writer::FormatWriter, axis::AbstractString)::Nothing

Invalidate all cached data that depends on the content of `axis`: the axis entries, the axis-to-index dict, every vector
on the axis, and every matrix where `axis` is one of the two axes. This increments the version counter of each of
these. Must be called inside a write lock.
"""
function invalidate_axis_data!(writer::FormatWriter, axis::AbstractString)::Nothing
    @assert has_data_write_lock(writer)

    invalidate_cached!(writer, axis_vector_cache_key(axis))
    invalidate_cached!(writer, axis_dict_cache_key(axis))

    for name in get_vectors_set_through_cache(writer, axis)
        invalidate_cached!(writer, vector_cache_key(axis, name))
    end

    for other_axis in get_axes_set_through_cache(writer)
        for name in get_matrices_set_through_cache(writer, axis, other_axis)
            invalidate_cached!(writer, matrix_cache_key(axis, other_axis, name))
            if axis != other_axis
                invalidate_cached!(writer, matrix_cache_key(other_axis, axis, name))
            end
        end
        if axis != other_axis
            for name in get_matrices_set_through_cache(writer, other_axis, axis)
                invalidate_cached!(writer, matrix_cache_key(other_axis, axis, name))
                invalidate_cached!(writer, matrix_cache_key(axis, other_axis, name))
            end
        end
    end

    return nothing
end

"""
    empty_cache!(
        daf::DafReader;
        [clear::Maybe{CacheGroup} = nothing,
        keep::Maybe{CacheGroup} = nothing]
    )::Nothing

Clear some cached data. By default, completely empties the caches. You can specify either `clear`, to only forget a
specific [`CacheGroup`](@ref) (e.g., for clearing only `QueryData`), or `keep`, to forget everything except a specific
[`CacheGroup`](@ref) (e.g., for keeping only `MappedData`). You can't specify both `clear` and `keep`.

!!! note

    If there are any slow cache update operations in flight (matrix relayout, queries) then this will wait until they
    are done to ensure that the cache is in a consistent state.

!!! note

    Packed properties may be returned as lazy arrays, which read their data on demand. Emptying the cache also releases
    the decoded chunks and the materialized data these arrays keep. They remain readable, and read their data again
    when needed.

!!! warning

    Some backends return vectors and matrices that alias buffers held alive by the cache entry (for zero-copy access).
    Emptying the cache releases those buffers, so any array reference previously returned by such a backend becomes
    dangling. Currently this affects [`HttpDaf`](@ref DataAxesFormats.HttpFormat.HttpDaf); drop all such references
    before calling `empty_cache!` on one of these backends.
"""
function empty_cache!(daf::DafReader; clear::Maybe{CacheGroup} = nothing, keep::Maybe{CacheGroup} = nothing)::Nothing
    @assert clear === nothing || keep === nothing
    lock(daf.internal.pending_condition) do
        while daf.internal.pending_count[1] > 0
            wait(daf.internal.pending_condition)  # UNTESTED
        end
    end
    with_cache_write_lock(daf, "empty_cache!") do
        @debug "empty_cache! daf: $(brief(daf)) clear: $(clear) keep: $(keep)"
        if clear === nothing && keep === nothing
            empty!(daf.internal.cache)
            empty!(daf.internal.dependents_of_cache_keys)
            empty!(daf.internal.dependency_versions_of_cache_keys)
        else
            filter!(daf.internal.cache) do key_value
                cache_group = key_value[2].cache_group
                return cache_group == keep || (cache_group != clear && clear !== nothing)
            end

            if isempty(daf.internal.cache)
                empty!(daf.internal.dependents_of_cache_keys)  # UNTESTED
                empty!(daf.internal.dependency_versions_of_cache_keys)  # UNTESTED
            else
                for (_, dependents_keys) in daf.internal.dependents_of_cache_keys
                    filter!(dependents_keys) do dependent_key
                        return haskey(daf.internal.cache, dependent_key)
                    end
                end
                filter!(daf.internal.dependents_of_cache_keys) do entry
                    dependents_keys = entry[2]
                    return !isempty(dependents_keys)
                end
                filter!(daf.internal.dependency_versions_of_cache_keys) do entry
                    return haskey(daf.internal.cache, entry[1])
                end
            end
        end

        # The lazy arrays stay registered, so a later modification still invalidates them.
        for (cache_key, weak_references) in daf.internal.lazy_arrays_of_cache_keys
            filter!(weak_reference -> weak_reference.value !== nothing, weak_references)
            if !haskey(daf.internal.cache, cache_key)
                for weak_reference in weak_references
                    lazy_array = weak_reference.value
                    if lazy_array !== nothing
                        purge_lazy_array!(lazy_array)
                    end
                end
            end
        end
        filter!(daf.internal.lazy_arrays_of_cache_keys) do entry
            weak_references = entry[2]
            return !isempty(weak_references)
        end
    end
    return nothing
end

function assert_valid_cache(format::FormatReader)::Nothing
    for cache_key in keys(format.internal.cache)
        type, key, _ = cache_key
        if type == CachedAxis
            println("$(cache_key) => has_axis?")
            @assert format_has_axis(format, key[1]; for_change = false)
        elseif type == CachedQuery
            println("$(cache_key) => query")
        elseif type == CachedData
            if key isa AbstractString
                println("$(cache_key) => has_scalar?")
                @assert format_has_scalar(format, key)
            elseif key isa Tuple{AbstractString, AbstractString}
                println("$(cache_key) => has_vector?")
                @assert format_has_axis(format, key[1]; for_change = false)
                @assert format_has_vector(format, key...)
            elseif key isa Tuple{AbstractString, AbstractString, AbstractString}
                println("$(cache_key) => has_axes?")
                @assert format_has_axis(format, key[1]; for_change = false)
                @assert format_has_axis(format, key[2]; for_change = false)
            else
                @assert false
            end
        elseif type == CachedNames
            if key isa AbstractString
                println("$(cache_key) => names")
            elseif key isa Tuple{AbstractString}
                println("$(cache_key) => has_axis?")
                @assert format_has_axis(format, key[1]; for_change = false)
            elseif key isa Tuple{AbstractString, AbstractString, Bool}
                println("$(cache_key) => has_axes?")
                @assert format_has_axis(format, key[1]; for_change = false)
                @assert format_has_axis(format, key[2]; for_change = false)
            end
        else
            @assert false
        end
    end
end

end # module
