"""
The [`DafReader`](@ref) interface specifies a high-level API for reading `Daf` data. This API is implemented here, on
top of the low-level [`FormatReader`](@ref) API. The high-level API provides thread safety so the low-level API can
(mostly) ignore this issue.

Each data set is given a name to use in error messages etc. You can explicitly set this name when creating a `Daf`
object. Otherwise, when opening an existing data set, if it contains a scalar "name" property, it is used. Otherwise
some reasonable default is used. In all cases, object names are passed through [`unique_name`](@ref) to avoid ambiguity.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each **unordered** pair of
axes.

For matrices, we keep careful track of their [`MatrixLayouts`](@ref). Returned matrices are always in column-major
layout, using [`relayout!`](@ref) if necessary. As this is an expensive operation, we'll cache the result in memory.
Similarly, we cache the results of applying a query to the data. We allow clearing the cache to reduce memory usage, if
necessary.

The data API is the high-level API intended to be used from outside the package, and is therefore re-exported from the
top-level `Daf` namespace. It provides additional functionality on top of the low-level [`FormatReader`](@ref)
implementation, accepting more general data types, automatically dealing with [`relayout!`](@ref) when needed. In
particular, it enforces single-writer multiple-readers for each data set, so the format code can ignore multi-threading
and still be thread-safe.

!!! note

    In the APIs below, when getting a value, specifying a `default` of `undef` means that it is an `error` for the value
    not to exist. In contrast, specifying a `default` of `nothing` means it is OK for the value not to exist, returning
    `nothing`. Specifying an actual value for `default` means it is OK for the value not to exist, returning the
    `default` instead. This is in spirit with, but not identical to, `undef` being used as a flag for array construction
    saying "there is no initializer". If you feel this is an abuse of the `undef` value, take some comfort in that it is
    the default value for the `default`, so you almost never have to write it explicitly in your code.
"""
module Readers

export axes_set
export axis_array
export axis_dict
export axis_length
export axis_version_counter
export description
export empty_cache!
export get_matrix
export get_scalar
export get_vector
export has_axis
export has_matrix
export has_scalar
export has_vector
export matrices_set
export matrix_version_counter
export scalars_set
export vector_version_counter
export vectors_set

using ..Formats
using ..GenericLocks
using ..GenericTypes
using ..MatrixLayouts
using ..Messages
using ..StorageTypes
using ConcurrentUtils
using NamedArrays
using SparseArrays

import ..Formats
import ..Formats.CacheEntry
import ..Formats.FormatReader  # For documentation.
import ..Messages

function Base.getproperty(daf::DafReader, property::Symbol)::Any
    if property == :name
        return daf.internal.name
    else
        return getfield(daf, property)
    end
end

"""
    has_scalar(daf::DafReader, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in `daf`.
"""
function has_scalar(daf::DafReader, name::AbstractString)::Bool
    return Formats.with_data_read_lock(daf, "has_scalar of:", name) do
        result = Formats.format_has_scalar(daf, name)
        @debug "has_scalar daf: $(depict(daf)) name: $(name) result: $(result)"
        return result
    end
end

"""
    scalars_set(daf::DafReader)::AbstractStringSet

The names of the scalar properties in `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.
"""
function scalars_set(daf::DafReader)::AbstractStringSet
    return Formats.with_data_read_lock(daf, "scalars_set") do
        result = Formats.get_scalars_set_through_cache(daf)
        @debug "scalars_set daf: $(depict(daf)) result: $(depict(result))"
        return result
    end
end

"""
    get_scalar(
        daf::DafReader,
        name::AbstractString;
        [default::Union{StorageScalar, Nothing, UndefInitializer} = undef]
    )::Maybe{StorageScalar}

Get the value of a scalar property with some `name` in `daf`.

If `default` is `undef` (the default), this first verifies the `name` scalar property exists in `daf`. Otherwise
`default` will be returned if the property does not exist.
"""
function get_scalar(
    daf::DafReader,
    name::AbstractString;
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
)::Maybe{StorageScalar}
    return Formats.with_data_read_lock(daf, "get_scalar of:", name) do
        if default == undef
            require_scalar(daf, name)
        elseif !has_scalar(daf, name)
            @debug "get_scalar daf: $(depict(daf)) name: $(name) default result: $(depict(default))"
            return default
        end

        result = Formats.get_scalar_through_cache(daf, name)
        @debug "get_scalar daf: $(depict(daf)) name: $(name) default: $(depict(default)) result: $(depict(result))"
        return result
    end
end

function require_scalar(daf::DafReader, name::AbstractString)::Nothing
    if !Formats.format_has_scalar(daf, name)
        error("missing scalar: $(name)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_axis(daf::DafReader, axis::AbstractString)::Bool

Check whether some `axis` exists in `daf`.
"""
function has_axis(daf::DafReader, axis::AbstractString)::Bool
    return Formats.with_data_read_lock(daf, "has_axis of:", axis) do
        result = Formats.format_has_axis(daf, axis; for_change = false)
        @debug "has_axis daf: $(depict(daf)) axis: $(axis) result: $(result)"
        return result
    end
end

"""
    axis_version_counter(daf::DafReader, axis::AbstractString)::UInt32

Return the version number of the axis. This is incremented every time [`delete_axis!`](@ref Daf.Writers.delete_axis!) is
called. It is used by interfaces to other programming languages to minimize copying data.

!!! note

    This is purely in-memory per-instance, and **not** a global persistent version counter. That is, the version counter
    starts at zero even if opening a persistent disk `daf` data set.
"""
function axis_version_counter(daf::DafReader, axis::AbstractString)::UInt32
    # TRICKY: We don't track versions for scalars so we can use the string keys for the axes.
    return Formats.format_get_version_counter(daf, axis)
end

"""
    axes_set(daf::DafReader)::AbstractStringSet

The names of the axes of `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.
"""
function axes_set(daf::DafReader)::AbstractStringSet
    return Formats.with_data_read_lock(daf, "axes_set") do
        result = Formats.get_axes_set_through_cache(daf)
        @debug "axes_set daf: $(depict(daf)) result: $(depict(result))"
        return result
    end
end

"""
    axis_array(
        daf::DafReader,
        axis::AbstractString;
        [default::Union{Nothing, UndefInitializer} = undef]
    )::Maybe{AbstractStringVector}

The array of unique names of the entries of some `axis` of `daf`. This is similar to doing [`get_vector`](@ref) for the
special `name` property, except that it returns a simple vector (array) of strings instead of a `NamedVector`.

If `default` is `undef` (the default), this verifies the `axis` exists in `daf`. Otherwise, the `default` is `nothing`,
which is returned if the `axis` does not exist.
"""
function axis_array(
    daf::DafReader,
    axis::AbstractString;
    default::Union{Nothing, UndefInitializer} = undef,
)::Maybe{AbstractStringVector}
    return Formats.with_data_read_lock(daf, "axis_array of:", axis) do
        result_prefix = ""
        if !Formats.format_has_axis(daf, axis; for_change = false)
            if default === nothing
                @debug "axis_array daf: $(depict(daf)) axis: $(axis) default: nothing result: nothing"
                return nothing
            else
                result_prefix = "default "
                @assert default == undef
                require_axis(daf, axis)
            end
        end

        result = Formats.read_only_array(Formats.axis_array_through_cache(daf, axis))
        @debug "axis_array daf: $(depict(daf)) axis: $(axis) default: $(depict(default)) $(result_prefix)result: $(depict(result))"
        return result
    end
end

"""
    function axis_dict(daf::DafReader, axis::AbstractString)::AbstractDict{<:AbstractString, <:Integer}

Return a dictionary converting axis entry names to their integer index.
"""
function axis_dict(daf::DafReader, axis::AbstractString)::AbstractDict{<:AbstractString, <:Integer}
    return Formats.with_data_read_lock(daf, "axis_dict of:", axis) do
        require_axis(daf, axis)
        result = Formats.axis_dict_with_cache(daf, axis)
        @debug "axis_dict daf: $(depict(daf)) result: $(depict(result))"
        return result
    end
end

"""
    axis_length(daf::DafReader, axis::AbstractString)::Int64

The number of entries along the `axis` in `daf`.

This first verifies the `axis` exists in `daf`.
"""
function axis_length(daf::DafReader, axis::AbstractString)::Int64
    return Formats.with_data_read_lock(daf, "axis_length of:", axis) do
        require_axis(daf, axis)
        result = Formats.format_axis_length(daf, axis)
        @debug "axis_length daf: $(depict(daf)) axis: $(axis) result: $(result)"
        return result
    end
end

function require_axis(daf::DafReader, axis::AbstractString; for_change::Bool = false)::Nothing
    if !Formats.format_has_axis(daf, axis; for_change = for_change)
        error("missing axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Bool

Check whether a vector property with some `name` exists for the `axis` in `daf`. This is always true for the special
`name` property.

This first verifies the `axis` exists in `daf`.
"""
function has_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Bool
    return Formats.with_data_read_lock(daf, "has_vector of:", name, "of:", axis) do
        require_axis(daf, axis)
        result = name == "name" || Formats.format_has_vector(daf, axis, name)
        @debug "has_vector daf: $(depict(daf)) axis: $(axis) name: $(name) result: $(result)"
        return result
    end
end

"""
    vector_version_counter(daf::DafReader, axis::AbstractString, name::AbstractString)::UInt32

Return the version number of the vector. This is incremented every time [`set_vector!`](@ref Daf.Writers.set_vector!),
[`empty_dense_vector!`](@ref Daf.Writers.empty_dense_vector!) or
[`empty_sparse_vector!`](@ref Daf.Writers.empty_sparse_vector!) are called. It is used by interfaces to other
programming languages to minimize copying data.

!!! note

    This is purely in-memory per-instance, and **not** a global persistent version counter. That is, the version counter
    starts at zero even if opening a persistent disk `daf` data set.
"""
function vector_version_counter(daf::DafReader, axis::AbstractString, name::AbstractString)::UInt32
    result = Formats.format_get_version_counter(daf, (axis, name))
    @debug "vector_version_counter daf: $(depict(daf)) axis: $(axis) name: $(name) result: $(result)"
    return result
end

"""
    vectors_set(daf::DafReader, axis::AbstractString)::AbstractStringSet

The names of the vector properties for the `axis` in `daf`, **not** including the special `name` property.

This first verifies the `axis` exists in `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.
"""
function vectors_set(daf::DafReader, axis::AbstractString)::AbstractStringSet
    return Formats.with_data_read_lock(daf, "vectors_set of:", axis) do
        require_axis(daf, axis)
        result = Formats.get_vectors_set_through_cache(daf, axis)
        @debug "vectors_set daf: $(depict(daf)) axis: $(axis) result: $(depict(result))"
        return result
    end
end

"""
    get_vector(
        daf::DafReader,
        axis::AbstractString,
        name::AbstractString;
        [default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef]
    )::Maybe{NamedVector}

Get the vector property with some `name` for some `axis` in `daf`. The names of the result are the names of the vector
entries (same as returned by [`axis_array`](@ref)). The special property `name` returns an array whose values are also the
(read-only) names of the entries of the axis.

This first verifies the `axis` exists in `daf`. If `default` is `undef` (the default), this first verifies the `name`
vector exists in `daf`. Otherwise, if `default` is `nothing`, it will be returned. If it is a [`StorageVector`](@ref),
it has to be of the same size as the `axis`, and is returned. If it is a [`StorageScalar`](@ref). Otherwise, a new
`Vector` is created of the correct size containing the `default`, and is returned.
"""
function get_vector(
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
)::Maybe{NamedArray}
    return Formats.with_data_read_lock(daf, "get_vector of:", name, "of:", axis) do
        require_axis(daf, axis)

        if default isa StorageVector
            require_axis_length(daf, "default length", length(default), axis)
            if default isa NamedVector
                require_dim_name(daf, axis, "default dim name", dimnames(default, 1))
                require_axis_names(daf, axis, "entry names of the: default", names(default, 1))
            end
        end

        cached_vector = Formats.get_from_cache(daf, Formats.vector_cache_key(axis, name), StorageVector)
        if cached_vector !== nothing
            if eltype(cached_vector) <: AbstractString
                cached_vector = Formats.read_only_array(cached_vector)
            end
            result = Formats.as_named_vector(daf, axis, cached_vector)
            @debug "get_vector daf: $(depict(daf)) axis: $(axis) name: $(name) default: $(depict(default)) cached result: $(depict(result))"
            return result
        end

        if name == "name"
            result = Formats.as_named_vector(daf, axis, Formats.axis_array_through_cache(daf, axis))
            @debug "get_vector daf: $(depict(daf)) axis: $(axis) name: $(name) default: $(depict(default)) name result: $(depict(result))"
            return result
        end

        vector = nothing
        result_prefix = ""
        if !Formats.format_has_vector(daf, axis, name)
            if default === nothing
                return nothing
            end
            result_prefix = "default "
            if default == undef
                require_vector(daf, axis, name)
            elseif default isa StorageVector
                vector = default
            elseif default == 0
                vector = spzeros(typeof(default), Formats.format_axis_length(daf, axis))
            else
                @assert default isa StorageScalar
                vector = fill(default, Formats.format_axis_length(daf, axis))
            end
        end

        if vector === nothing
            vector = Formats.get_vector_through_cache(daf, axis, name)
            if !(vector isa StorageVector)
                error(  # untested
                    "format_get_vector for daf format: $(typeof(daf))\n" *
                    "returned invalid Daf.StorageVector: $(depict(vector))",
                )
            end
            if length(vector) != Formats.format_axis_length(daf, axis)
                error( # untested
                    "format_get_vector for daf format: $(typeof(daf))\n" *
                    "returned vector length: $(length(vector))\n" *
                    "instead of axis: $(axis)\n" *
                    "length: $(axis_length(daf, axis))\n" *
                    "in the daf data: $(daf.name)",
                )
            end
        end

        result = Formats.as_named_vector(daf, axis, vector)
        @debug "get_vector daf: $(depict(daf)) axis: $(axis) name: $(name) default: $(depict(default)) $(result_prefix)result: $(depict(result))"
        return result
    end
end

function require_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if !Formats.format_has_vector(daf, axis, name)
        error("missing vector: $(name)\nfor the axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_matrix(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [relayout::Bool = true]
    )::Bool

Check whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in `daf`. Since this
is Julia, this means a column-major matrix. A daf may contain two copies of the same data, in which case it would report
the matrix under both axis orders.

If `relayout` (the default), this will also check whether the data exists in the other layout (that is, with flipped
axes).

This first verifies the `rows_axis` and `columns_axis` exists in `daf`.
"""
function has_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool = true,
)::Bool
    return Formats.with_data_read_lock(daf, "has_matrix of:", name, "of:", rows_axis, "and:", columns_axis) do
        Formats.relayout = relayout && rows_axis != columns_axis

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        result = with_read_lock(daf.internal.cache_lock) do
            return haskey(daf.internal.cache, Formats.matrix_cache_key(rows_axis, columns_axis, name)) ||
                   Formats.format_has_matrix(daf, rows_axis, columns_axis, name) ||
                   (relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name))
        end
        @debug "has_matrix daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) relayout: $(relayout) result: $(depict(result))"
        return result
    end
end

"""
    matrices_set(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString;
        [relayout::Bool = true]
    )::AbstractStringSet

The names of the matrix properties for the `rows_axis` and `columns_axis` in `daf`.

If `relayout` (default), then this will include the names of matrices that exist in the other layout (that is, with
flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.
"""
function matrices_set(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString;
    relayout::Bool = true,
)::AbstractStringSet
    return Formats.with_data_read_lock(daf, "matrices_set of:", rows_axis, "and:", columns_axis) do
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if !relayout
            names = Formats.get_matrices_set_through_cache(daf, rows_axis, columns_axis)
        else
            first_relayout_cache_key = Formats.matrix_relayout_names_cache_key(rows_axis, columns_axis)
            names = Formats.get_from_cache(daf, first_relayout_cache_key, AbstractStringSet)
            if names === nothing
                names = with_write_lock(
                    daf.internal.cache_lock,
                    daf.name,
                    "cache for matrices_set of:",
                    rows_axis,
                    "and:",
                    columns_axis,
                ) do
                    second_relayout_cache_key = Formats.matrix_relayout_names_cache_key(columns_axis, rows_axis)
                    first_names = Formats.get_matrices_set_through_cache(daf, rows_axis, columns_axis)
                    second_names = Formats.get_matrices_set_through_cache(daf, columns_axis, rows_axis)
                    names = union(first_names, second_names)
                    Formats.cache_data!(daf, first_relayout_cache_key, names, MemoryData)
                    Formats.cache_data!(daf, second_relayout_cache_key, names, MemoryData)
                    return names
                end
            end
        end

        @debug "matrices_set daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) relayout: $(relayout) result: $(depict(names))"
        return names
    end
end

function require_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(daf)
    if !has_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
        if relayout
            extra = "(and the other way around)\n"
        else
            extra = ""
        end
        error(
            "missing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            extra *
            "in the daf data: $(daf.name)",
        )
    end
    return nothing
end

struct UpgradeToWriteLockException <: Exception end

"""
    get_matrix(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [default::Union{StorageNumber, StorageMatrix, Nothing, UndefInitializer} = undef,
        relayout::Bool = true]
    )::Maybe{NamedMatrix}

Get the column-major matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. The names of the
result axes are the names of the relevant axes entries (same as returned by [`axis_array`](@ref)).

If `relayout` (the default), then if the matrix is only stored in the other memory layout (that is, with flipped axes),
then automatically call [`relayout!`](@ref) to compute the result. If `daf` isa [`DafWriter`](@ref), then store the
result for future use; otherwise, just cache it as [`MemoryData`](@ref CacheType). This may lock up very large amounts
of memory; you can call [`empty_cache!`](@ref) to release it.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If `default` is `undef` (the default), this first
verifies the `name` matrix exists in `daf`. Otherwise, if `default` is `nothing`, it is returned. If `default` is a
`StorageMatrix`, it has to be of the same size as the `rows_axis` and `columns_axis`, and is returned. Otherwise, a new
`Matrix` is created of the correct size containing the `default`, and is returned.
"""
function get_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    default::Union{StorageNumber, StorageMatrix, Nothing, UndefInitializer} = undef,
    relayout::Bool = true,
)::Maybe{NamedArray}
    try
        return Formats.with_data_read_lock(daf, "get_matrix of:", name, "of:", rows_axis, "and:", columns_axis) do
            return do_get_matrix(daf, rows_axis, columns_axis, name; default = default, relayout = relayout)
        end
    catch exception
        if exception isa UpgradeToWriteLockException
            return Formats.with_data_write_lock(daf, "get_matrix of:", name, "of:", rows_axis, "and:", columns_axis) do
                return do_get_matrix(daf, rows_axis, columns_axis, name; default = default, relayout = relayout)
            end
        else
            rethrow()
        end
    end
end

function do_get_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    default::Union{StorageNumber, StorageMatrix, Nothing, UndefInitializer} = undef,
    relayout::Bool = true,
)::Maybe{NamedArray}
    relayout = relayout && rows_axis != columns_axis

    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    if default isa StorageMatrix
        require_column_major(default)
        require_axis_length(daf, "default rows", size(default, Rows), rows_axis)
        require_axis_length(daf, "default columns", size(default, Columns), columns_axis)
        if default isa NamedMatrix
            require_dim_name(daf, rows_axis, "default rows dim name", dimnames(default, 1); prefix = "rows")
            require_dim_name(daf, columns_axis, "default columns dim name", dimnames(default, 2); prefix = "columns")
            require_axis_names(daf, rows_axis, "row names of the: default", names(default, 1))
            require_axis_names(daf, columns_axis, "column names of the: default", names(default, 2))
        end
    end

    cached_matrix = Formats.get_from_cache(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name), StorageMatrix)
    if cached_matrix !== nothing
        result = Formats.as_named_matrix(daf, rows_axis, columns_axis, cached_matrix)
        @debug "get_matrix daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) default: $(depict(default)) cached result: $(depict(result))"
        return result
    end

    matrix = nothing
    result_prefix = ""
    if !Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        if relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            result_prefix = "relayout "
            if daf isa DafWriter &&
               !daf.internal.is_frozen &&
               Formats.format_has_matrix(daf, columns_axis, rows_axis, name; for_relayout = true)
                if !Formats.has_data_write_lock(daf)
                    throw(UpgradeToWriteLockException())
                end
                Formats.format_relayout_matrix!(daf, columns_axis, rows_axis, name)
            else
                cache_key = Formats.matrix_cache_key(rows_axis, columns_axis, name)
                cache_entry = with_read_lock(daf.internal.cache_lock, daf.name, "cache for:", cache_key) do
                    return get(daf.internal.cache, cache_key, nothing)
                end
                if cache_entry === nothing
                    with_write_lock(daf.internal.cache_lock, daf.name, "cache for:", cache_key) do
                        cache_entry = get(daf.internal.cache, cache_key, nothing)
                        if cache_entry === nothing
                            Formats.store_cached_dependency_key!(
                                daf,
                                cache_key,
                                Formats.axis_array_cache_key(rows_axis),
                            )
                            Formats.store_cached_dependency_key!(
                                daf,
                                cache_key,
                                Formats.axis_array_cache_key(columns_axis),
                            )
                            flipped_matrix = Formats.get_matrix_through_cache(daf, columns_axis, rows_axis, name).array
                            relayout_matrix = relayout!(flipped_matrix)
                            transposed_matrix = transpose(relayout_matrix)
                            cache_entry = CacheEntry(
                                MemoryData,
                                Formats.as_named_matrix(daf, rows_axis, columns_axis, transposed_matrix),
                            )
                            daf.internal.cache[cache_key] = cache_entry
                        end
                    end
                end
                matrix = cache_entry.data
            end
        else
            if default === nothing
                @debug "get_matrix daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) default: nothing default result: nothing"
                return nothing
            end
            result_prefix = "default "
            if default == undef
                require_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
            elseif default isa StorageMatrix
                matrix = default
            elseif default == 0
                matrix = spzeros(
                    typeof(default),
                    Formats.format_axis_length(daf, rows_axis),
                    Formats.format_axis_length(daf, columns_axis),
                )
            else
                @assert default isa StorageScalar
                matrix = fill(  # NOJET
                    default,
                    Formats.format_axis_length(daf, rows_axis),
                    Formats.format_axis_length(daf, columns_axis),
                )
            end
        end
    end

    if matrix === nothing
        matrix = Formats.get_matrix_through_cache(daf, rows_axis, columns_axis, name)
        if !(matrix isa StorageMatrix)
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned invalid Daf.StorageMatrix: $(depict(matrix))",
            )
        end

        if size(matrix, Rows) != Formats.format_axis_length(daf, rows_axis)
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned matrix rows: $(size(matrix, Rows))\n" *
                "instead of axis: $(rows_axis)\n" *
                "length: $(axis_length(daf, rows_axis))\n" *
                "in the daf data: $(daf.name)",
            )
        end

        if size(matrix, Columns) != Formats.format_axis_length(daf, columns_axis)
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned matrix columns: $(size(matrix, Columns))\n" *
                "instead of axis: $(columns_axis)\n" *
                "length: $(axis_length(daf, columns_axis))\n" *
                "in the daf data: $(daf.name)",
            )
        end

        if major_axis(matrix) != Columns
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned non column-major matrix: $(depict(matrix))",
            )
        end
    end

    result = Formats.as_named_matrix(daf, rows_axis, columns_axis, matrix)
    @debug "get_matrix daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) default: $(depict(default)) $(result_prefix)result: $(depict(result))"
    return result
end

"""
    matrix_version_counter(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::UInt32

Return the version number of the matrix. The order of the axes does not matter. This is incremented every time
[`set_matrix!`](@ref Daf.Writers.set_matrix!), [`empty_dense_matrix!`](@ref Daf.Writers.empty_dense_matrix!) or
[`empty_sparse_matrix!`](@ref Daf.Writers.empty_sparse_matrix!) are called. It is used by interfaces to other
programming languages to minimize copying data.

!!! note

    This is purely in-memory per-instance, and **not** a global persistent version counter. That is, the version counter
    starts at zero even if opening a persistent disk `daf` data set.
"""
function matrix_version_counter(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::UInt32
    if columns_axis < rows_axis
        rows_axis, columns_axis = columns_axis, rows_axis
    end
    result = Formats.format_get_version_counter(daf, (rows_axis, columns_axis, name))
    @debug "matrix_version_counter daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) result: $(result)"
    return result
end

function require_column_major(matrix::StorageMatrix)::Nothing
    if major_axis(matrix) != Columns
        error("type not in column-major layout: $(depict(matrix))")
    end
end

function require_axis_length(
    daf::DafReader,
    what_name::AbstractString,
    what_length::StorageInteger,
    axis::AbstractString,
)::Nothing
    if what_length != Formats.format_axis_length(daf, axis)
        error(
            "$(what_name): $(what_length)\n" *
            "is different from the length: $(Formats.format_axis_length(daf, axis))\n" *
            "of the axis: $(axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    return nothing
end

function require_dim_name(
    daf::DafReader,
    axis::AbstractString,
    what::AbstractString,
    name::Union{Symbol, AbstractString};
    prefix::AbstractString = "",
)::Nothing
    if prefix != ""
        prefix = prefix * " "
    end
    string_name = String(name)
    if string_name != axis
        error("$(what): $(string_name)\nis different from the $(prefix)axis: $(axis)\nin the daf data: $(daf.name)")
    end
end

function require_axis_names(
    daf::DafReader,
    axis::AbstractString,
    what::AbstractString,
    names::AbstractStringVector,
)::Nothing
    @assert Formats.has_data_read_lock(daf)
    expected_names = axis_array(daf, axis)
    if names != expected_names
        error("$(what)\nmismatch the entry names of the axis: $(axis)\nin the daf data: $(daf.name)")
    end
end

"""
    description(daf::DafReader[; deep::Bool = false])::AbstractString

Return a (multi-line) description of the contents of `daf`. This tries to hit a sweet spot between usefulness and
terseness. If `cache`, also describes the content of the cache. If `deep`, also describes any data set nested inside
this one (if any).
"""
function description(daf::DafReader; cache::Bool = false, deep::Bool = false)::String
    return Formats.with_data_read_lock(daf, "description") do
        lines = String[]
        description(daf, "", lines, cache, deep)
        push!(lines, "")
        return join(lines, "\n")
    end
end

function description(daf::DafReader, indent::AbstractString, lines::Vector{String}, cache::Bool, deep::Bool)::Nothing
    if indent == ""
        push!(lines, "$(indent)name: $(daf.name)")
    else
        push!(lines, "- $(indent[3:end])name: $(daf.name)")
    end

    Formats.format_description_header(daf, indent, lines)

    scalars_description(daf, indent, lines)

    axes = collect(axes_set(daf))
    sort!(axes)
    if !isempty(axes)
        axes_description(daf, axes, indent, lines)
        vectors_description(daf, axes, indent, lines)
        matrices_description(daf, axes, indent, lines)
        if cache
            cache_description(daf, indent, lines)
        end
    end

    Formats.format_description_footer(daf, indent, lines, cache, deep)
    return nothing
end

function scalars_description(daf::DafReader, indent::AbstractString, lines::Vector{String})::Nothing
    scalars = collect(Formats.get_scalars_set_through_cache(daf))
    if !isempty(scalars)
        sort!(scalars)
        push!(lines, "$(indent)scalars:")
        for scalar in scalars
            push!(lines, "$(indent)  $(scalar): $(depict(get_scalar(daf, scalar)))")
        end
    end
    return nothing
end

function axes_description(
    daf::DafReader,
    axes::AbstractStringVector,
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    push!(lines, "$(indent)axes:")
    for axis in axes
        push!(lines, "$(indent)  $(axis): $(axis_length(daf, axis)) entries")
    end
    return nothing
end

function vectors_description(
    daf::DafReader,
    axes::AbstractStringVector,
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    is_first = true
    for axis in axes
        vectors = collect(Formats.get_vectors_set_through_cache(daf, axis))
        if !isempty(vectors)
            if is_first
                push!(lines, "$(indent)vectors:")
                is_first = false
            end
            sort!(vectors)
            push!(lines, "$(indent)  $(axis):")
            for vector in vectors
                push!(lines, "$(indent)    $(vector): $(depict(base_array(get_vector(daf, axis, vector))))")
            end
        end
    end
    return nothing
end

function matrices_description(
    daf::DafReader,
    axes::AbstractStringVector,
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    is_first = true
    for rows_axis in axes
        for columns_axis in axes
            matrices = collect(Formats.get_matrices_set_through_cache(daf, rows_axis, columns_axis))
            if !isempty(matrices)
                if is_first
                    push!(lines, "$(indent)matrices:")
                    is_first = false
                end
                sort!(matrices)
                push!(lines, "$(indent)  $(rows_axis),$(columns_axis):")
                for matrix in matrices
                    push!(
                        lines,
                        "$(indent)    $(matrix): " *
                        depict(base_array(get_matrix(daf, rows_axis, columns_axis, matrix; relayout = false))),
                    )
                end
            end
        end
    end
    return nothing
end

function cache_description(daf::DafReader, indent::AbstractString, lines::Vector{String})::Nothing
    is_first = true
    with_read_lock(daf.internal.cache_lock) do
        cache_keys = collect(keys(daf.internal.cache))
        sort!(cache_keys)
        for key in cache_keys
            if is_first
                push!(lines, "$(indent)cache:")
                is_first = false
            end
            cache_entry = daf.internal.cache[key]
            value = cache_entry.data
            if value isa AbstractArray
                value = base_array(value)
            end
            key = replace(key, "'" => "''")
            push!(lines, "$(indent)  '$(key)': ($(cache_entry.cache_type)) $(depict(value))")
        end
    end
    return nothing
end

function base_array(array::AbstractArray)::AbstractArray
    return array
end

function base_array(array::SparseArrays.ReadOnly)::AbstractArray
    return base_array(parent(array))
end

function base_array(array::NamedArray)::AbstractArray
    return base_array(array.array)
end

function Messages.depict(daf::DafReader; name::Maybe{AbstractString} = nothing)::AbstractString
    if name === nothing
        name = daf.name
    end
    return "$(typeof(daf)) $(name)"
end

end # module
