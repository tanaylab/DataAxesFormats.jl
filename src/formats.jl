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
export MappedData
export MemoryData
export QueryData
export empty_cache!
export end_data_write_lock

using ..GenericLocks
using ..GenericTypes
using ..Keys
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

CacheData = Union{
    AbstractSet{<:AbstractString},
    AbstractVector{<:AbstractString},
    StorageScalar,
    NamedArray,
    AbstractDict{<:AbstractString, <:Integer},
}

mutable struct CacheEntry
    cache_group::CacheGroup
    data::Union{ReentrantLock, CacheData}
end

@enum CacheType CachedAxis CachedData CachedQuery CachedNames

AxisCacheKey = Tuple{AxisKey, Bool}

NamesKey = Union{AbstractString, Tuple{AbstractString}, Tuple{AbstractString, AbstractString, Bool}}

CacheKey = Tuple{CacheType, Union{AxisCacheKey, PropertyKey, AbstractString, NamesKey}}

function Base.show(io::IO, cache_key::CacheKey)::Nothing
    type, key = cache_key
    if type == CachedAxis
        @assert key isa AxisCacheKey
        if key[2]
            print(io, "axis_dict[axis: $(key[1])]")
        else
            print(io, "axis_vector[axis: $(key[1])]")  # UNTESTED
        end
    elseif type == CachedQuery
        @assert key isa AbstractString
        print(io, "query[$(key)]")
    elseif type == CachedData
        if key isa AbstractString  # UNTESTED
            print(io, "scalar[$(key)]")  # UNTESTED
        elseif key isa Tuple{AbstractString, AbstractString}  # UNTESTED
            print(io, "vector[axis: $(key[1]) name: $(key[2])]")  # UNTESTED
        elseif key isa Tuple{AbstractString, AbstractString, AbstractString}  # UNTESTED
            print(  # UNTESTED
                io,
                "matrix[rows_axis: $(key[1]) columns_axis: $(key[2]) name: $(key[3])]",
            )
        else
            @assert false
        end
    elseif type == CachedNames
        if key isa AbstractString
            print(io, "names[$(key)]")
        elseif key isa Tuple{AbstractString}
            print(io, "vectors[axis: $(key[1])]")
        elseif key isa Tuple{AbstractString, AbstractString, Bool}  # UNTESTED
            if key[3]  # UNTESTED
                print(io, "matrices[relayout rows_axis: $(key[1]) columns_axis: $(key[2])]")  # UNTESTED
            else
                print(io, "matrices[rows_axis: $(key[1]) columns_axis: $(key[2])]")  # UNTESTED
            end
        end
    else
        @assert false
    end

    return nothing
end

function scalars_set_cache_key()::CacheKey
    return (CachedNames, "scalars")
end

function axes_set_cache_key()::CacheKey
    return (CachedNames, "axes")
end

function vectors_set_cache_key(axis::AbstractString)::CacheKey
    return (CachedNames, (axis,))
end

function matrices_set_cache_key(rows_axis::AbstractString, columns_axis::AbstractString; relayout::Bool)::CacheKey
    if relayout && columns_axis < rows_axis
        rows_axis, columns_axis = columns_axis, rows_axis
    end
    return (CachedNames, (rows_axis, columns_axis, relayout))
end

function scalar_cache_key(name::AbstractString)::CacheKey
    return (CachedData, name)
end

function axis_vector_cache_key(axis::AxisKey)::CacheKey
    return (CachedAxis, (axis, false))
end

function axis_dict_cache_key(axis::AbstractString)::CacheKey
    return (CachedAxis, (axis, true))
end

function vector_cache_key(axis::AbstractString, name::AbstractString)::CacheKey
    return (CachedData, (axis, name))
end

function matrix_cache_key(rows_axis::AbstractString, columns_axis::AbstractString, name::AbstractString)::CacheKey
    return (CachedData, (rows_axis, columns_axis, name))
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
    cache::Dict{CacheKey, CacheEntry}
    cache_group::Maybe{CacheGroup}
    dependents_of_cache_keys::Dict{CacheKey, Set{CacheKey}}
    dependencies_of_query_keys::Dict{CacheKey, Set{CacheKey}}
    version_counters::Dict{PropertyKey, UInt32}
    cache_lock::QueryReadWriteLock
    data_lock::QueryReadWriteLock
    is_frozen::Bool
    pending_condition::Threads.Condition
    pending_count::Vector{UInt32}
end

function Internal(; cache_group::Maybe{CacheGroup}, is_frozen::Bool)::Internal
    return Internal(
        Dict{CacheKey, CacheEntry}(),
        cache_group,
        Dict{CacheKey, Set{CacheKey}}(),
        Dict{CacheKey, Set{CacheKey}}(),
        Dict{PropertyKey, UInt32}(),
        QueryReadWriteLock(),
        QueryReadWriteLock(),
        is_frozen,
        Threads.Condition(),
        UInt32[0],
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
`DataAxesFormats.Readers` module for their description.
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
`DataAxesFormats.Writers` module for their description.
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
    format_axis_vector(format::FormatReader, axis::AbstractString)::AbstractVector{<:AbstractString}

Implement fetching the unique names of the entries of some `axis` of `format`.

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
    )::Vector{T} where {T <: StorageReal}

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
    format_get_empty_sparse_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        indtype::Type{I},
    )::Tuple{AbstractVector{I}, AbstractVector{T}, Any}
    where {T <: StorageReal, I <: StorageInteger}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in
`format`.

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

Allow the `format` to perform caching once the empty sparse vector has been `filled`. By default this does nothing.
"""
function format_filled_empty_sparse_vector!(
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
    )::AbstractMatrix{T} where {T <: StorageReal}

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
    format_get_empty_sparse_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        intdype::Type{I},
        nnz::StorageInteger,
    )::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}, Any}
    where {T <: StorageReal, I <: StorageInteger}

Implement creating an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `format`.

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

Allow the `format` to perform caching once the empty sparse matrix has been `filled`. By default this does nothing.
"""
function format_filled_empty_sparse_matrix!(
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
    )::StorageMatrix

[`relayout!`](@ref) the existing `name` column-major `matrix` property for the `rows_axis` and the `columns_axis` and
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
    push!(lines, "$(indent)type: $(nameof(typeof(format)))")
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

function put_in_cache!(format::FormatReader, cache_key::CacheKey, data::CacheData, cache_group::CacheGroup)::Nothing
    @assert has_data_read_lock(format)
    @assert has_write_lock(format.internal.cache_lock)
    if data isa AbstractArray
        data = read_only_array(data)
    end
    @debug "put_in_cache! daf: $(depict(format)) cache_key: $(cache_key) data: $(depict(data)) cache_group: $(cache_group)"
    format.internal.cache[cache_key] = CacheEntry(cache_group, data)
    return nothing
end

function set_in_cache!(format::FormatReader, cache_key::CacheKey, data::CacheData, cache_group::CacheGroup)::Nothing
    return with_cache_write_lock(format, "for set_in_cache!:", cache_key) do  # NOJET
        return put_in_cache!(format, cache_key, data, cache_group)
    end
end

function set_in_cache!(::FormatReader, ::CacheKey, ::CacheData, ::Nothing)::Nothing
    return nothing
end

function format_has_cached_matrix(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return format_has_matrix(format, rows_axis, columns_axis, name) ||
           haskey(format.internal.cache, matrix_cache_key(rows_axis, columns_axis, name))
end

function get_through_cache(
    getter::Function,
    format::FormatReader,
    cache_key::CacheKey,
    ::Type{T},
    cache_group::Maybe{CacheGroup};
    is_slow::Bool = false,
)::T where {T}
    @assert has_data_read_lock(format)
    cached = nothing
    while cached === nothing
        cache_entry = with_cache_read_lock(format, "for get_from_cache:", cache_key) do  # NOJET
            return get(format.internal.cache, cache_key, nothing)
        end
        cached = result_from_cache(cache_entry, T)
        while cached === nothing
            if cache_group === nothing
                cached = getter()
            else
                cached = write_throgh_cache(getter, format, cache_key, T, cache_group; is_slow)
            end
        end
    end
    return cached
end

function result_from_cache(::Nothing, ::Type{T})::Nothing where {T}
    return nothing
end

function result_from_cache(cache_entry::CacheEntry, ::Type{T})::T where {T}
    entry_lock = cache_entry.data
    if entry_lock isa ReentrantLock
        cache_entry = lock(entry_lock) do  # UNTESTED
            return cache_entry  # UNTESTED
        end
    end
    return cache_entry.data
end

function write_throgh_cache(
    getter::Function,
    format::FormatReader,
    cache_key::CacheKey,
    ::Type{T},
    cache_group::Maybe{CacheGroup};
    is_slow::Bool = false,
)::Maybe{T} where {T}
    result = with_cache_write_lock(format, "for get_through_cache:", cache_key) do  # NOJET
        cache_entry = get(format.internal.cache, cache_key, nothing)
        if cache_entry !== nothing
            if cache_entry.data isa ReentrantLock  # UNTESTED
                return nothing  # UNTESTED
            else
                return cache_entry.data  # UNTESTED
            end
        else
            if is_slow
                entry_lock = ReentrantLock()
                cache_entry = CacheEntry(cache_group, entry_lock)
                format.internal.cache[cache_key] = cache_entry
                lock(entry_lock)
                lock(format.internal.pending_condition) do
                    format.internal.pending_count[1] += 1
                    return nothing
                end
                return cache_entry
            else
                result = getter()
                put_in_cache!(format, cache_key, result, cache_group)
                return result
            end
        end
    end
    if result isa CacheEntry
        cache_entry = result
        entry_lock = cache_entry.data
        @assert is_slow
        @assert entry_lock isa ReentrantLock
        try
            result, dependency_keys = getter()
            with_cache_write_lock(format, "for slow:", cache_key) do  # NOJET
                if dependency_keys !== nothing
                    for dependency_key in dependency_keys
                        put_cached_dependency_key!(format, cache_key, dependency_key)
                    end
                    if cache_key[1] == CachedQuery
                        format.internal.dependencies_of_query_keys[cache_key] = dependency_keys
                    end
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
        finally
            unlock(entry_lock)
        end
    end
    return result
end

function get_scalars_set_through_cache(format::FormatReader)::AbstractSet{<:AbstractString}
    return get_through_cache(format, scalars_set_cache_key(), AbstractSet{<:AbstractString}, MemoryData) do
        return format_scalars_set(format)
    end
end

function get_axes_set_through_cache(format::FormatReader)::AbstractSet{<:AbstractString}
    return get_through_cache(format, axes_set_cache_key(), AbstractSet{<:AbstractString}, MemoryData) do
        return format_axes_set(format)
    end
end

function get_vectors_set_through_cache(format::FormatReader, axis::AbstractString)::AbstractSet{<:AbstractString}
    return get_through_cache(format, vectors_set_cache_key(axis), AbstractSet{<:AbstractString}, MemoryData) do
        return format_vectors_set(format, axis)
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
        format.internal.cache_group,
    ) do
        return format_matrices_set(format, rows_axis, columns_axis)
    end
end

function get_scalar_through_cache(format::FormatReader, name::AbstractString)::StorageScalar
    return get_through_cache(format, scalar_cache_key(name), StorageScalar, format.internal.cache_group) do
        return format_get_scalar(format, name)
    end
end

function get_axis_vector_through_cache(format::FormatReader, axis::AbstractString)::AbstractVector{<:AbstractString}
    return get_through_cache(
        format,
        axis_vector_cache_key(axis),
        AbstractVector{<:AbstractString},
        format.internal.cache_group,
    ) do
        return read_only_array(format_axis_vector(format, axis))
    end
end

function get_axis_dict_through_cache(
    format::FormatReader,
    axis::AbstractString,
)::AbstractDict{<:AbstractString, <:Integer}
    return get_through_cache(
        format,
        axis_dict_cache_key(axis),
        AbstractDict{<:AbstractString, <:Integer},
        MemoryData,
    ) do
        names = get_axis_vector_through_cache(format, axis)
        if eltype(names) != AbstractString
            names = Vector{AbstractString}(names)  # NOJET
        end
        names = read_only_array(names)
        named_array = NamedArray(spzeros(length(names)); names = (names,), dimnames = (axis,))
        return named_array.dicts[1]
    end
end

function get_vector_through_cache(format::FormatReader, axis::AbstractString, name::AbstractString)::NamedArray
    return get_through_cache(format, vector_cache_key(axis, name), StorageVector, format.internal.cache_group) do
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
    return get_through_cache(
        format,
        matrix_cache_key(rows_axis, columns_axis, name),
        StorageMatrix,
        format.internal.cache_group,
    ) do
        matrix = format_get_matrix(format, rows_axis, columns_axis, name)
        matrix = as_named_matrix(format, rows_axis, columns_axis, matrix)
        return matrix
    end
end

function get_relayout_matrix_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::NamedArray
    @assert !format_has_matrix(format, rows_axis, columns_axis, name)
    matrix = get_matrix_through_cache(format, columns_axis, rows_axis, name).array
    return get_through_cache(
        format,
        matrix_cache_key(rows_axis, columns_axis, name),
        StorageMatrix,
        MemoryData;
        is_slow = true,
    ) do
        matrix = transposer(matrix)
        matrix = as_named_matrix(format, rows_axis, columns_axis, matrix)
        return (matrix, nothing)
    end
end

function cache_scalar!(format::FormatReader, name::AbstractString, value::StorageScalar)::Nothing
    set_in_cache!(format, scalar_cache_key(name), value, format.internal.cache_group)
    return nothing
end

function cache_vector!(format::FormatReader, axis::AbstractString, name::AbstractString, vector::NamedVector)::Nothing
    set_in_cache!(format, vector_cache_key(axis, name), vector, format.internal.cache_group)
    return nothing
end

function cache_matrix!(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::NamedMatrix,
)::Nothing
    set_in_cache!(format, matrix_cache_key(rows_axis, columns_axis, name), matrix, format.internal.cache_group)
    return nothing
end

function as_named_vector(::FormatReader, ::AbstractString, vector::NamedVector)::NamedArray
    return vector
end

function as_named_vector(format::FormatReader, axis::AbstractString, vector::AbstractVector)::NamedArray
    axis_dict = get_axis_dict_through_cache(format, axis)
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
    rows_axis_dict = get_axis_dict_through_cache(format, rows_axis)
    columns_axis_dict = get_axis_dict_through_cache(format, columns_axis)
    @assert size(matrix) == (length(rows_axis_dict), length(columns_axis_dict))
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

function put_cached_dependency_key!(format::FormatReader, cache_key::CacheKey, dependency_key::CacheKey)::Nothing
    @assert has_data_read_lock(format)
    @assert has_write_lock(format.internal.cache_lock)
    if dependency_key == cache_key
        return nothing
    end
    keys_set = get!(format.internal.dependents_of_cache_keys, dependency_key) do
        return Set{AbstractString}()
    end
    size_before = length(keys_set)
    push!(keys_set, cache_key)
    size_after = length(keys_set)
    if size_after > size_before
        @debug "put_cached_dependency_key! daf: $(depict(format)) cache_key: $(cache_key) dependency_key: $(dependency_key)"
    end
    return nothing
end

function invalidate_cached!(format::FormatReader, cache_key::CacheKey)::Nothing
    @debug "invalidate_cached! daf: $(depict(format)) cache_key: $(cache_key)"
    @debug "- delete cache_key: $(cache_key)"
    with_cache_write_lock(format, "for invalidate key:", cache_key) do
        delete!(format.internal.cache, cache_key)

        dependents_keys = pop!(format.internal.dependents_of_cache_keys, cache_key, nothing)
        if dependents_keys !== nothing
            for dependent_key in dependents_keys
                @debug "- delete dependent_key: $(dependent_key)"
                delete!(format.internal.cache, dependent_key)
            end
        end
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
    read_lock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
    return nothing
end

function end_data_read_lock(format::FormatReader, what::Any...)::Nothing
    read_unlock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
    return nothing
end

function with_cache_read_lock(action::Function, format::FormatReader, what::Any...)::Any
    return with_read_lock(action, format.internal.cache_lock, format.name, what...)
end

function with_cache_write_lock(action::Function, format::FormatReader, what::Any...)::Any
    return with_write_lock(action, format.internal.cache_lock, format.name, what...)
end

function with_data_read_lock(action::Function, format::FormatReader, what::Any...)::Any
    begin_data_read_lock(format, what...)
    try
        return action()
    finally
        end_data_read_lock(format, what...)
    end
end

function has_data_read_lock(format::FormatReader; read_only::Bool = false)::Bool
    return has_read_lock(format.internal.data_lock; read_only)
end

function begin_data_write_lock(format::FormatReader, what::Any...)::Nothing
    write_lock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
    return nothing
end

function end_data_write_lock(format::FormatReader, what::Any...)::Nothing
    write_unlock(format.internal.data_lock, format.name, "data for", what...)  # NOJET
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

function format_get_version_counter(format::FormatReader, version_key::PropertyKey)::UInt32
    return with_cache_read_lock(format, "for version counter of:", string(version_key)) do
        return get(format.internal.version_counters, version_key, UInt32(0))
    end
end

function format_increment_version_counter(format::FormatWriter, version_key::PropertyKey)::Nothing
    with_cache_write_lock(format, "for version counter of:", string(version_key)) do
        previous_version_counter = format_get_version_counter(format, version_key)
        return format.internal.version_counters[version_key] = previous_version_counter + 1
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
"""
function empty_cache!(daf::DafReader; clear::Maybe{CacheGroup} = nothing, keep::Maybe{CacheGroup} = nothing)::Nothing
    @assert clear === nothing || keep === nothing
    lock(daf.internal.pending_condition) do
        while daf.internal.pending_count[1] > 0
            wait(daf.internal.pending_condition)  # UNTESTED
        end
        with_cache_write_lock(daf, "empty_cache!") do
            @debug "empty_cache! daf: $(depict(daf)) clear: $(clear) keep: $(keep)"
            if clear === nothing && keep === nothing
                empty!(daf.internal.cache)
            else
                filter!(daf.internal.cache) do key_value
                    cache_group = key_value[2].cache_group
                    return cache_group == keep || (cache_group != clear && clear !== nothing)
                end
            end

            if isempty(daf.internal.cache)
                empty!(daf.internal.dependents_of_cache_keys)
            else
                for (_, dependents_keys) in daf.internal.dependents_of_cache_keys
                    filter(dependents_keys) do dependent_key
                        return haskey(daf.internal.cache, dependent_key)
                    end
                end
                filter(daf.internal.dependents_of_cache_keys) do entry
                    dependents_keys = entry[2]
                    return !isempty(dependents_keys)
                end
            end
        end
    end
    return nothing
end

function assert_valid_cache(format::FormatReader)::Nothing  # UNTESTED
    for cache_key in keys(format.internal.cache)
        type, key = cache_key
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
