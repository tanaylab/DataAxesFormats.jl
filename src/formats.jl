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

For clarity of documentation, we split the type hierarchy to `DafWriter <: FormatWriter <: DafReader <: FormatReader`.

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
export end_write_lock

using Base.Threads
using ConcurrentUtils
using Daf.GenericTypes
using Daf.MatrixLayouts
using Daf.Messages
using Daf.StorageTypes
using Daf.Tokens
using NamedArrays
using OrderedCollections
using SparseArrays

struct UpgradToWriteLockException <: Exception end

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
  - `MemoryData` - disk data copied to application memory, or alternative layout of data matrices. This does pressure
    the garbage collector and can cause out of memory errors. However, re-fetching the data from disk is very slow,
    so caching this data is crucial for performance.
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
    data::Union{AbstractStringSet, AbstractStringVector, StorageScalar, NamedArray}
end

"""
    Internal(name::AbstractString)

Internal data we need to keep in any concrete [`FormatReader`](@ref). This has to be available as a `.internal` data
member of the concrete format. This enables all the high-level [`DafReader`](@ref) and [`DafWriter`](@ref) functions.

The constructor will automatically call [`unique_name`](@ref) to try and make the names unique for improved error
messages.
"""
struct Internal
    name::AbstractString
    axes::Dict{AbstractString, OrderedDict{AbstractString, Int64}}
    cache::Dict{AbstractString, CacheEntry}
    dependency_cache_keys::Dict{AbstractString, Set{AbstractString}}
    version_counters::Dict{DataKey, UInt32}
    lock::ReadWriteLock
    writer_thread::Vector{Int}
    thread_has_read_lock::Vector{Bool}
end

function Internal(name::AbstractString)::Internal
    return Internal(
        unique_name(name),
        Dict{AbstractString, OrderedDict{AbstractString, Int64}}(),
        Dict{AbstractString, CacheEntry}(),
        Dict{AbstractString, Set{AbstractString}}(),
        Dict{DataKey, UInt32}(),
        ReadWriteLock(),
        [0],
        fill(false, nthreads()),
    )
end

function renamed_internal(internal::Internal, name::AbstractString)::Internal
    return Internal(
        name,
        internal.axes,
        internal.cache,
        internal.dependency_cache_keys,
        internal.version_counters,
        internal.lock,
        internal.writer_thread,
        internal.thread_has_read_lock,
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
    format_scalar_names(format::FormatReader)::AbstractStringSet

The names of the scalar properties in `format`.

This trusts that we have a read lock on the data set.
"""
function format_scalar_names end

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
        entries::AbstractStringVector
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
    format_axis_names(format::FormatReader)::AbstractStringSet

The names of the axes of `format`.

This trusts that we have a read lock on the data set.
"""
function format_axis_names end

"""
    format_get_axis(format::FormatReader, axis::AbstractString)::AbstractStringVector

Implement fetching the unique names of the entries of some `axis` of `format`.

This trusts that we have a read lock on the data set, and that the `axis` exists in `format`.
"""
function format_get_axis end

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
    format_empty_dense_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
    )::VectorVector where {T <: StorageNumber}

Implement setting a vector property with some `name` for some `axis` in `format`.

Implement creating an empty dense `matrix` with some `name` for some `rows_axis` and `columns_axis` in `format`.

This trusts we have a write lock on the data set, that the `axis` exists in `format` and that the vector property `name`
isn't `"name"`, and that it does not exist for the `axis`.

!!! note

    The return type of this function is always a **functionally** dense vector, that is, it will have `strides` of
    `(1,)`, so that elements are consecutive in memory. However it need not be an actual `DenseVector` because of
    Julia's type system's limitations.
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
    )::Tuple{AbstractVector{I}, AbstractVector{T}, Any}
    where {T <: StorageNumber, I <: StorageInteger}

Implement creating an empty dense vector property with some `name` for some `rows_axis` and `columns_axis` in
`format`. The final tuple element is passed to [`format_filled_sparse_vector!`](@ref).

This trusts we have a write lock on the data set, that the `axis` exists in `format` and that the vector property `name`
isn't `"name"`, and that it does not exist for the `axis`.
"""
function format_empty_sparse_vector! end

"""
    format_filled_sparse_vector!(
        format::FormatWriter,
        axis::AbstractString,
        name::AbstractString,
        extra::Any,
        filled::SparseVector{T, I},
    )::Nothing where {T <: StorageNumber, I <: StorageInteger}

Allow the `format` to perform caching once the empty sparse vector has been `filled`. By default this does nothing.
"""
function format_filled_sparse_vector!(  # untested
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::Any,
    ::SparseVector{T, I},
)::Nothing where {T <: StorageNumber, I <: StorageInteger}
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
    format_vector_names(format::FormatReader, axis::AbstractString)::AbstractStringSet

Implement fetching the names of the vectors for the `axis` in `format`, **not** including the special `name` property.

This trusts that we have a read lock on the data set, and that the `axis` exists in `format`.
"""
function format_vector_names end

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
    format_empty_dense_matrix!(
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
function format_empty_dense_matrix! end

"""
    format_empty_sparse_matrix!(
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
The final tuple element is passed to [`format_filled_sparse_matrix!`](@ref).

This trusts we have a write lock on the data set, that the `rows_axis` and `columns_axis` exist in `format` and that the
`name` matrix property does not exist for them.
"""
function format_empty_sparse_matrix! end

"""
    format_filled_dense_matrix!(
        format::FormatWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        extra::Any,
        filled::SparseMatrixCSC{T, I},
    )::Nothing where {T <: StorageNumber, I <: StorageInteger}

Allow the `format` to perform caching once the empty sparse matrix has been `filled`. By default this does nothing.
"""
function format_filled_sparse_matrix!(  # untested
    ::FormatWriter,
    ::AbstractString,
    ::AbstractString,
    ::AbstractString,
    ::Any,
    ::SparseMatrixCSC{T, I},
)::Nothing where {T <: StorageNumber, I <: StorageInteger}
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
    format_matrix_names(
        format::FormatReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
    )::AbstractStringSet

Implement fetching the names of the matrix properties for the `rows_axis` and `columns_axis` in `format`.

This trusts that we have a read lock on the data set, and that the `rows_axis` and `columns_axis` exist in `format`.
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

This trusts that we have a read lock on the data set, and that the `rows_axis` and `columns_axis` exist in `format`, and
the `name` matrix property exists for them.
"""
function format_get_matrix end

"""
    format_description_header(format::FormatReader, lines::Vector{String})::Nothing

Allow a `format` to amit additional description header lines.

This trusts that we have a read lock on the data set.
"""
function format_description_header(format::FormatReader, indent::AbstractString, lines::Vector{String})::Nothing
    push!(lines, "$(indent)type: $(typeof(format))")
    return nothing
end

"""
    format_description_footer(format::FormatReader, lines::Vector{String})::Nothing

Allow a `format` to amit additional description footer lines. If `deep`, this also emit the description of any data sets
nested in this one, if any.

This trusts that we have a read lock on the data set.
"""
function format_description_footer(::FormatReader, ::AbstractString, ::Vector{String}, ::Bool, ::Bool)::Nothing
    return nothing
end

function get_from_cache(format::FormatReader, cache_key::AbstractString, ::Type{T})::Maybe{T} where {T}
    result = get(format.internal.cache, cache_key, nothing)
    if result === nothing
        return nothing
    else
        return result.data
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

function get_scalar_names_through_cache(format::FormatReader)::AbstractStringSet
    return get_through_cache(format, scalar_names_cache_key(), AbstractStringSet) do
        return format_scalar_names(format)
    end
end

function get_axis_names_through_cache(format::FormatReader)::AbstractStringSet
    return get_through_cache(format, axis_names_cache_key(), AbstractStringSet) do
        return format_axis_names(format)
    end
end

function get_vector_names_through_cache(format::FormatReader, axis::AbstractString)::AbstractStringSet
    return get_through_cache(format, vector_names_cache_key(axis), AbstractStringSet) do
        return format_vector_names(format, axis)
    end
end

function get_matrix_names_through_cache(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractStringSet
    return get_through_cache(format, matrix_names_cache_key(rows_axis, columns_axis), AbstractStringSet) do
        return format_matrix_names(format, rows_axis, columns_axis)
    end
end

function get_scalar_through_cache(format::FormatReader, name::AbstractString)::StorageScalar
    return get_through_cache(format, scalar_cache_key(name), StorageScalar) do
        return format_get_scalar(format, name)
    end
end

function get_axis_through_cache(format::FormatReader, axis::AbstractString)::AbstractStringVector
    return get_through_cache(format, axis_cache_key(axis), AbstractStringVector) do
        return format_get_axis(format, axis)
    end
end

function get_vector_through_cache(format::FormatReader, axis::AbstractString, name::AbstractString)::StorageVector
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

function cache_data!(
    format::FormatReader,
    cache_key::AbstractString,
    data::Union{AbstractStringSet, AbstractStringVector, StorageScalar, NamedArray},
    cache_type::CacheType,
)::Nothing
    @debug "cache_data! daf: $(depict(format)) cache_key: $(cache_key) data: $(depict(data)) cache_type: $(cache_type)"
    @assert format.internal.writer_thread[1] == threadid()
    @assert !haskey(format.internal.cache, cache_key)
    format.internal.cache[cache_key] = CacheEntry(cache_type, data)
    return nothing
end

function cache_scalar_names!(format::FormatReader, names::AbstractStringSet, cache_type::CacheType)::Nothing
    cache_key = scalar_names_cache_key()
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_axis_names!(format::FormatReader, names::AbstractStringSet, cache_type::CacheType)::Nothing
    cache_key = axis_names_cache_key()
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_axis!(
    format::FormatReader,
    axis::AbstractString,
    entries::AbstractStringVector,
    cache_type::CacheType,
)::Nothing
    cache_key = axis_cache_key(axis)
    cache_data!(format, cache_key, entries, cache_type)
    return nothing
end

function cache_vector_names!(
    format::FormatReader,
    axis::AbstractString,
    names::AbstractStringSet,
    cache_type::CacheType,
)::Nothing
    cache_key = vector_names_cache_key(axis)
    cache_data!(format, cache_key, names, cache_type)
    return nothing
end

function cache_matrix_names!(
    format::FormatReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    names::AbstractStringSet,
    cache_type::CacheType,
)::Nothing
    cache_key = matrix_names_cache_key(rows_axis, columns_axis)
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
    cache_data!(format, cache_key, named_vector, cache_type)
    store_cached_dependency_key!(format, cache_key, axis_cache_key(axis))
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
    cache_data!(format, cache_key, named_matrix, cache_type)
    store_cached_dependency_key!(format, cache_key, axis_cache_key(rows_axis))
    store_cached_dependency_key!(format, cache_key, axis_cache_key(columns_axis))
    return nothing
end

function as_named_vector(::FormatReader, ::AbstractString, vector::NamedVector)::NamedArray
    return vector
end

function as_named_vector(format::FormatReader, axis::AbstractString, vector::AbstractVector)::NamedArray
    axis_names_dict = get(format.internal.axes, axis, nothing)
    if axis_names_dict === nothing
        names = as_read_only_array(Formats.get_axis_through_cache(format, axis))
        named_array = NamedArray(vector; names = (names,), dimnames = (axis,))
        format.internal.axes[axis] = named_array.dicts[1]
        return named_array

    else
        return NamedArray(vector, (axis_names_dict,), (axis,))
    end
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
    rows_axis_names_dict = get(format.internal.axes, rows_axis, nothing)
    columns_axis_names_dict = get(format.internal.axes, columns_axis, nothing)
    if rows_axis_names_dict === nothing || columns_axis_names_dict === nothing
        rows_names = as_read_only_array(Formats.get_axis_through_cache(format, rows_axis))
        columns_names = as_read_only_array(Formats.get_axis_through_cache(format, columns_axis))
        named_array = NamedArray(matrix; names = (rows_names, columns_names), dimnames = (rows_axis, columns_axis))
        format.internal.axes[rows_axis] = named_array.dicts[1]
        format.internal.axes[columns_axis] = named_array.dicts[2]
        return named_array

    else
        return NamedArray(matrix, (rows_axis_names_dict, columns_axis_names_dict), (rows_axis, columns_axis))
    end
end

function as_read_only_array(array::SparseArrays.ReadOnly)::SparseArrays.ReadOnly
    return array
end

function as_read_only_array(array::NamedArray)::NamedArray
    if array.array isa SparseArrays.ReadOnly
        return array  # untested
    else
        return NamedArray(as_read_only_array(array.array), array.dicts, array.dimnames)
    end
end

function as_read_only_array(array::AbstractArray)::SparseArrays.ReadOnly
    return SparseArrays.ReadOnly(array)
end

function scalar_names_cache_key()::String
    return "? scalars"
end

function axis_names_cache_key()::String
    return "? axes"
end

function vector_names_cache_key(axis::AbstractString)::String
    return "/ $(axis) ?"
end

function matrix_names_cache_key(rows_axis::AbstractString, columns_axis::AbstractString)::String
    return "? / $(rows_axis) / $(columns_axis)" # TRICKY: NOT the query key, which returns the union.
end

function matrix_relayout_names_cache_key(rows_axis::AbstractString, columns_axis::AbstractString)::String
    return "/ $(rows_axis) / $(columns_axis) ?"  # TRICKY: The query key, which returns the union.
end

function scalar_cache_key(name::AbstractString)::String
    return ": $(escape_value(name))"
end

function axis_cache_key(axis::AbstractString)::String
    return "/ $(escape_value(axis))"
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
    for dependency_key in dependency_keys
        if cache_key != dependency_key
            store_cached_dependency_key!(format, cache_key, dependency_key)
        end
    end
    return nothing
end

function store_cached_dependency_key!(
    format::FormatReader,
    cache_key::AbstractString,
    dependency_key::AbstractString,
)::Nothing
    @assert format.internal.writer_thread[1] == threadid()
    keys_set = get!(format.internal.dependency_cache_keys, dependency_key) do
        return Set{AbstractString}()
    end
    @assert cache_key != dependency_key
    push!(keys_set, cache_key)
    return nothing
end

function invalidate_cached!(format::FormatReader, cache_key::AbstractString)::Nothing
    delete!(format.internal.cache, cache_key)

    dependent_keys = pop!(format.internal.dependency_cache_keys, cache_key, nothing)
    if dependent_keys !== nothing
        for dependent_key in dependent_keys
            delete!(format.internal.cache, dependent_key)
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

function with_write_lock(action::Function, format::FormatReader)::Any
    thread_id = threadid()
    if format.internal.writer_thread[1] == thread_id
        return action()  # untested
    end

    lock(format.internal.lock)
    try
        format.internal.writer_thread[1] = thread_id
        return action()
    finally
        end_write_lock(format)
    end
end

# For avoiding callbacks when calling Julia from another language.
function begin_write_lock(action::Function, format::FormatReader)::Any
    thread_id = threadid()
    @assert format.internal.writer_thread[1] != thread_id

    lock(format.internal.lock)
    try
        format.internal.writer_thread[1] = thread_id
        return action()
    catch
        end_write_lock(format)
        rethrow()
    end
end

# For avoiding callbacks when calling Julia from another language.
function end_write_lock(format::FormatReader)::Any
    thread_id = threadid()
    @assert !format.internal.thread_has_read_lock[thread_id]
    @assert format.internal.writer_thread[1] == thread_id
    format.internal.writer_thread[1] = 0
    return unlock(format.internal.lock)
end

function with_read_lock(action::Function, format::FormatReader)::Any
    thread_id = threadid()
    if format.internal.writer_thread[1] == thread_id || format.internal.thread_has_read_lock[thread_id]
        return action()
    end

    lock_read(format.internal.lock)
    try
        format.internal.thread_has_read_lock[thread_id] = true
        @assert format.internal.writer_thread[1] == 0
        return action()

    catch exception
        if exception isa UpgradToWriteLockException
            @assert format.internal.thread_has_read_lock[thread_id]
            @assert format.internal.writer_thread[1] == 0
            format.internal.thread_has_read_lock[thread_id] = false
            unlock_read(format.internal.lock)
            lock(format.internal.lock)
            format.internal.writer_thread[1] = thread_id
            return action()
        else
            rethrow(exception)
        end

    finally
        if format.internal.writer_thread[1] == thread_id
            @assert !format.internal.thread_has_read_lock[thread_id]
            format.internal.writer_thread[1] = 0
            unlock(format.internal.lock)
        else
            @assert format.internal.thread_has_read_lock[thread_id]
            @assert format.internal.writer_thread[1] == 0
            format.internal.thread_has_read_lock[thread_id] = false
            unlock_read(format.internal.lock)
        end
    end
end

function upgrade_to_write_lock(format::FormatReader)::Nothing
    thread_id = threadid()
    writer_thread = format.internal.writer_thread[1]
    if writer_thread == 0
        @assert format.internal.thread_has_read_lock[thread_id]
        throw(UpgradToWriteLockException())
    else
        @assert !format.internal.thread_has_read_lock[thread_id]
        @assert writer_thread == thread_id
        return nothing
    end
end

function format_get_version_counter(format::FormatReader, version_key::DataKey)::UInt32
    return get(format.internal.version_counters, version_key, UInt32(0))
end

function format_increment_version_counter(format::FormatWriter, version_key::DataKey)::Nothing
    previous_version_counter = format_get_version_counter(format, version_key)
    format.internal.version_counters[version_key] = previous_version_counter + 1
    return nothing
end

"""
    empty_cache!(
        daf::FormatReader;
        [clear::Maybe{CacheType} = nothing,
        keep::Maybe{CacheType} = nothing]
    )::Nothing

Clear some cached data. By default, completely empties the caches. You can specify either `clear`, to only forget a
specific [`CacheType`](@ref) (e.g., for clearing only `QueryData`), or `keep`, to forget everything except a specific
[`CacheType`](@ref) (e.g., for keeping only `MappedData`). You can't specify both `clear` and `keep`.
"""
function empty_cache!(daf::DafReader; clear::Maybe{CacheType} = nothing, keep::Maybe{CacheType} = nothing)::Nothing
    return with_write_lock(daf) do
        @debug "empty_cache! daf: $(depict(daf)) clear: $(clear) keep: $(keep)"
        @assert clear === nothing || keep === nothing
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

        return nothing
    end
end

end # module
