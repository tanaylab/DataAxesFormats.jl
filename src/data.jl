"""
The [`DafReader`](@ref) and [`DafWriter`](@ref) interfaces specify a high-level API for accessing `Daf` data. This API
is implemented here, on top of the low-level [`FormatReader`](@ref) and [`FormatWriter`](@ref) API. The high-level API
provides thread safety so the low-level API can (mostly) ignore this issue.

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
top-level `Daf` namespace. It provides additional functionality on top of the low-level [`FormatReader`](@ref) and
[`FormatWriter`](@ref) implementations, accepting more general data types, automatically dealing with
[`relayout!`](@ref) when needed. In particular, it enforces single-writer multiple-readers for each data set, so the
format code can ignore multi-threading and still be thread-safe.

!!! note

    In the APIs below, when getting a value, specifying a `default` of `undef` means that it is an `error` for the value
    not to exist. In contrast, specifying a `default` of `nothing` means it is OK for the value not to exist, returning
    `nothing`. Specifying an actual value for `default` means it is OK for the value not to exist, returning the
    `default` instead. This is in spirit with, but not identical to, `undef` being used as a flag for array construction
    saying "there is no initializer". If you feel this is an abuse of the `undef` value, take some comfort in that it is
    the default value for the `default`, so you almost never have to write it explicitly in your code.
"""
module Data

export add_axis!
export axis_length
export axis_names
export DataKey
export delete_axis!
export delete_matrix!
export delete_scalar!
export delete_vector!
export description
export empty_cache!
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
export relayout_matrix!
export scalar_names
export set_matrix!
export set_scalar!
export set_vector!
export vector_names

using ConcurrentUtils
using Daf.Formats
using Daf.Generic
using Daf.MatrixLayouts
using Daf.Messages
using Daf.StorageTypes
using NamedArrays
using SparseArrays

import Daf.Formats
import Daf.Formats.CacheEntry
import Daf.Formats.FormatReader
import Daf.Formats.FormatWriter
import Daf.Formats.upgrade_to_write_lock
import Daf.Formats.with_read_lock
import Daf.Formats.with_write_lock
import Daf.Messages

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
    return with_read_lock(daf) do
        result = Formats.format_has_scalar(daf, name)
        # @debug "has_scalar $(daf.name) : $(name) -> $(present(result))"
        return result
    end
end

"""
    set_scalar!(
        daf::DafWriter,
        name::AbstractString,
        value::StorageScalar;
        [overwrite::Bool = false]
    )::Nothing

Set the `value` of a scalar property with some `name` in `daf`.

If not `overwrite` (the default), this first verifies the `name` scalar property does not exist.
"""
function set_scalar!(daf::DafWriter, name::AbstractString, value::StorageScalar; overwrite::Bool = false)::Nothing
    return with_write_lock(daf) do
        @debug "set_scalar! $(daf.name) : $(name) <$(overwrite ? "=" : "-") $(present(value))"

        if !overwrite
            require_no_scalar(daf, name)
        elseif Formats.format_has_scalar(daf, name)
            Formats.format_delete_scalar!(daf, name; for_set = true)
        end

        Formats.format_set_scalar!(daf, name, value)

        Formats.invalidate_cached!(daf, Formats.scalar_cache_key(name))
        Formats.invalidate_cached!(daf, Formats.scalar_names_cache_key())
        return nothing
    end
end

"""
    delete_scalar!(
        daf::DafWriter,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a scalar property with some `name` from `daf`.

If `must_exist` (the default), this first verifies the `name` scalar property exists in `daf`.
"""
function delete_scalar!(daf::DafWriter, name::AbstractString; must_exist::Bool = true)::Nothing
    return with_write_lock(daf) do
        @debug "delete_scalar! $(daf.name) : $(name)$(must_exist ? "" : " ?")"

        if must_exist
            require_scalar(daf, name)
        end

        if Formats.format_has_scalar(daf, name)
            Formats.format_delete_scalar!(daf, name; for_set = false)
        end

        Formats.invalidate_cached!(daf, Formats.scalar_cache_key(name))
        Formats.invalidate_cached!(daf, Formats.scalar_names_cache_key())
        return nothing
    end
end

"""
    scalar_names(daf::DafReader)::AbstractStringSet

The names of the scalar properties in `daf`.
"""
function scalar_names(daf::DafReader)::AbstractStringSet
    return with_read_lock(daf) do
        result = Formats.get_scalar_names_through_cache(daf)
        # @debug "scalar_names $(daf.name) -> $(present(result))"
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
    return with_read_lock(daf) do
        if default == undef
            require_scalar(daf, name)
        elseif !has_scalar(daf, name)
            @debug "get_scalar $(daf.name) : $(name) -> $(present(default)) ?"
            return default
        end

        result = Formats.format_get_scalar(daf, name)
        @debug "get_scalar $(daf.name) : $(name) -> $(present(result))"
        return result
    end
end

function require_scalar(daf::DafReader, name::AbstractString)::Nothing
    if !Formats.format_has_scalar(daf, name)
        error("missing scalar: $(name)\nin the daf data: $(daf.name)")
    end
    return nothing
end

function require_no_scalar(daf::DafReader, name::AbstractString)::Nothing
    if Formats.format_has_scalar(daf, name)
        error("existing scalar: $(name)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_axis(daf::DafReader, axis::AbstractString)::Bool

Check whether some `axis` exists in `daf`.
"""
function has_axis(daf::DafReader, axis::AbstractString)::Bool
    return with_read_lock(daf) do
        result = Formats.format_has_axis(daf, axis; for_change = false)
        # @debug "has_axis $(daf.name) / $(axis) -> $(present(result))"
        return result
    end
end

"""
    add_axis!(
        daf::DafWriter,
        axis::AbstractString,
        entries::AbstractStringVector
    )::Nothing

Add a new `axis` `daf`.

This first verifies the `axis` does not exist and that the `entries` are unique.
"""
function add_axis!(daf::DafWriter, axis::AbstractString, entries::AbstractStringVector)::Nothing
    return with_write_lock(daf) do
        @debug "add_axis $(daf.name) / $(axis) <- $(present(entries))"

        require_no_axis(daf, axis; for_change = true)

        if !allunique(entries)
            error("non-unique entries for new axis: $(axis)\nin the daf data: $(daf.name)")
        end

        Formats.invalidate_cached!(daf, Formats.axis_names_cache_key())

        Formats.format_add_axis!(daf, axis, entries)
        return nothing
    end
end

"""
    delete_axis!(
        daf::DafWriter,
        axis::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete an `axis` from the `daf`. This will also delete any vector or matrix properties that are based on this axis.

If `must_exist` (the default), this first verifies the `axis` exists in the `daf`.
"""
function delete_axis!(daf::DafWriter, axis::AbstractString; must_exist::Bool = true)::Nothing
    return with_write_lock(daf) do
        @debug "delete_axis! $(daf.name) / $(axis)$(must_exist ? "" : " ?")"

        if must_exist
            require_axis(daf, axis; for_change = true)
        end

        if !Formats.format_has_axis(daf, axis; for_change = true)
            return nothing
        end

        vector_names = Formats.get_vector_names_through_cache(daf, axis)
        for name in vector_names
            Formats.format_delete_vector!(daf, axis, name; for_set = false)
        end

        axis_names = Formats.get_axis_names_through_cache(daf)
        for other_axis in axis_names
            matrix_names = Formats.get_matrix_names_through_cache(daf, axis, other_axis)
            for name in matrix_names
                Formats.format_delete_matrix!(daf, axis, other_axis, name; for_set = false)
            end
            matrix_names = Formats.get_matrix_names_through_cache(daf, other_axis, axis)
            for name in matrix_names
                Formats.format_delete_matrix!(daf, other_axis, axis, name; for_set = false)
            end
        end

        Formats.invalidate_cached!(daf, Formats.axis_cache_key(axis))
        Formats.invalidate_cached!(daf, Formats.axis_names_cache_key())

        Formats.format_delete_axis!(daf, axis)
        return nothing
    end
end

"""
    axis_names(daf::DafReader)::AbstractStringSet

The names of the axes of `daf`.
"""
function axis_names(daf::DafReader)::AbstractStringSet
    return with_read_lock(daf) do
        result = Formats.get_axis_names_through_cache(daf)
        # @debug "axis_names $(daf.name) -> $(present(result))"
        return result
    end
end

"""
    get_axis(
        daf::DafReader,
        axis::AbstractString;
        [default::Union{Nothing, UndefInitializer} = undef]
    )::Maybe{AbstractStringVector}

The unique names of the entries of some `axis` of `daf`. This is similar to doing [`get_vector`](@ref) for the special
`name` property, except that it returns a simple vector of strings instead of a `NamedVector`.

If `default` is `undef` (the default), this verifies the `axis` exists in `daf`. Otherwise, the `default` is `nothing`,
which is returned if the `axis` does not exist.
"""
function get_axis(
    daf::DafReader,
    axis::AbstractString;
    default::Union{Nothing, UndefInitializer} = undef,
)::Maybe{AbstractStringVector}
    return with_read_lock(daf) do
        if !has_axis(daf, axis)
            if default == nothing
                @debug "get_axis! $(daf.name) / $(axis) -> $(present(missing))"
                return nothing
            else
                @assert default == undef
                require_axis(daf, axis)
            end
        end

        result = as_read_only_array(Formats.get_axis_through_cache(daf, axis))
        @debug "get_axis! $(daf.name) / $(axis) -> $(present(result))"
        return result
    end
end

"""
    axis_length(daf::DafReader, axis::AbstractString)::Int64

The number of entries along the `axis` in `daf`.

This first verifies the `axis` exists in `daf`.
"""
function axis_length(daf::DafReader, axis::AbstractString)::Int64
    return with_read_lock(daf) do
        require_axis(daf, axis)
        result = Formats.format_axis_length(daf, axis)
        # @debug "axis_length! $(daf.name) / $(axis) -> $(present(result))"
        return result
    end
end

function require_axis(daf::DafReader, axis::AbstractString; for_change::Bool = false)::Nothing
    if !Formats.format_has_axis(daf, axis; for_change = for_change)
        error("missing axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

function require_no_axis(daf::DafReader, axis::AbstractString; for_change::Bool = false)::Nothing
    if Formats.format_has_axis(daf, axis; for_change = for_change)
        error("existing axis: $(axis)\nin the daf data: $(daf.name)")
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
    return with_read_lock(daf) do
        require_axis(daf, axis)
        result = name == "name" || Formats.format_has_vector(daf, axis, name)
        # @debug "has_vector $(daf.name) / $(axis) : $(name) -> $(present(result))"
        return result
    end
end

"""
    set_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector};
        [overwrite::Bool = false]
    )::Nothing

Set a vector property with some `name` for some `axis` in `daf`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This first verifies the `axis` exists in `daf`, that the property name isn't `name`, and that the `vector` has the
appropriate length. If not `overwrite` (the default), this also verifies the `name` vector does not exist for the
`axis`.
"""
function set_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector};
    overwrite::Bool = false,
)::Nothing
    return with_write_lock(daf) do
        @debug "set_vector! $(daf.name) / $(axis) : $(name) <$(overwrite ? "=" : "-") $(present(vector))"

        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        if vector isa StorageVector
            require_axis_length(daf, "vector length", length(vector), axis)
            if vector isa NamedVector
                require_dim_name(daf, axis, "vector dim name", dimnames(vector, 1))
                require_axis_names(daf, axis, "entry names of the: vector", names(vector, 1))
            end
        end

        if !overwrite
            require_no_vector(daf, axis, name)
        elseif Formats.format_has_vector(daf, axis, name)
            Formats.format_delete_vector!(daf, axis, name; for_set = true)
        end

        Formats.format_set_vector!(daf, axis, name, vector)

        Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))
        return nothing
    end
end

"""
    empty_dense_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T};
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber}

Create an empty dense vector property with some `name` for some `axis` in `daf`, pass it to `fill`, and return the
result.

The returned vector will be uninitialized; the caller is expected to `fill` it with values. This saves creating a copy
of the vector before setting it in the data, which makes a huge difference when creating vectors on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If not `overwrite` (the
default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_dense_vector!(
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber}
    return with_write_lock(daf) do
        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        if !overwrite
            require_no_vector(daf, axis, name)
        elseif Formats.format_has_vector(daf, axis, name)
            Formats.format_delete_vector!(daf, axis, name; for_set = true)
        end

        empty_vector = Formats.format_empty_dense_vector!(daf, axis, name, eltype)
        result = as_named_vector(daf, axis, empty_vector)

        Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))

        @debug "empty_dense_vector! $(daf.name) / $(axis) : $(name) <$(overwrite ? "=" : "-") $(present(result))"
        return fill(result)
    end
end

"""
    empty_sparse_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        indtype::Type{I};
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber, I <: StorageInteger}

Create an empty sparse vector property with some `name` for some `axis` in `daf`, pass it to `fill` and return the
result.

The returned vector will be uninitialized; the caller is expected to `fill` it with values. This means manually filling
the `nzind` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow pre-allocating disk
data. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse vector is created by concatenating several smaller ones; this function
allows doing so directly into the data vector, avoiding a copy in case of memory-mapped disk formats.

!!! warning

    It is the caller's responsibility to fill the three vectors with valid data. Specifically, you must ensure:

      - `nzind[1] == 1`
      - `nzind[i] <= nzind[i + 1]`
      - `nzind[end] == nnz`

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If not `overwrite` (the
default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_sparse_vector!(
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber, I <: StorageInteger}
    return with_write_lock(daf) do
        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        if !overwrite
            require_no_vector(daf, axis, name)
        elseif Formats.format_has_vector(daf, axis, name)
            Formats.format_delete_vector!(daf, axis, name; for_set = true)
        end

        empty_vector = Formats.format_empty_sparse_vector!(daf, axis, name, eltype, nnz, indtype)
        result = fill(as_named_vector(daf, axis, empty_vector))
        verified = SparseVector(length(empty_vector), empty_vector.nzind, empty_vector.nzval)

        Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))

        @debug "empty_dense_vector! $(daf.name) / $(axis) : $(name) <$(overwrite ? "=" : "-") $(present(verified))"
        return result
    end
end

"""
    delete_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a vector property with some `name` for some `axis` from `daf`.

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If `must_exist` (the default),
this also verifies the `name` vector exists for the `axis`.
"""
function delete_vector!(daf::DafWriter, axis::AbstractString, name::AbstractString; must_exist::Bool = true)::Nothing
    return with_write_lock(daf) do
        @debug "delete_vector! $(daf.name) / $(axis) : $(name) $(must_exist ? "" : " ?")"

        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        if must_exist
            require_vector(daf, axis, name)
        end

        if !Formats.format_has_vector(daf, axis, name)
            return nothing
        end

        Formats.format_delete_vector!(daf, axis, name; for_set = false)

        Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))
        return nothing
    end
end

"""
    vector_names(daf::DafReader, axis::AbstractString)::AbstractStringSet

The names of the vector properties for the `axis` in `daf`, **not** including the special `name` property.

This first verifies the `axis` exists in `daf`.
"""
function vector_names(daf::DafReader, axis::AbstractString)::AbstractStringSet
    return with_read_lock(daf) do
        require_axis(daf, axis)
        result = Formats.format_vector_names(daf, axis)
        # @debug "vector_names $(daf.name) / $(axis) -> $(present(result))"
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
entries (same as returned by [`get_axis`](@ref)). The special property `name` returns an array whose values are also the
(read-only) names of the entries of the axis.

This first verifies the `axis` exists in `daf`. If `default` is `undef` (the default), this first verifies the `name`
vector exists in `daf`. Otherwise, if `default` is `nothing`, it will be returned. If it is a `StorageVector`, it has to
be of the same size as the `axis`, and is returned. If it is a [`StorageScalar`](@ref). Otherwise, a new `Vector` is
created of the correct size containing the `default`, and is returned.
"""
function get_vector(
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
)::Maybe{NamedArray}
    return with_read_lock(daf) do
        require_axis(daf, axis)

        if default isa StorageVector
            require_axis_length(daf, "default length", length(default), axis)
            if default isa NamedVector
                require_dim_name(daf, axis, "default dim name", dimnames(default, 1))
                require_axis_names(daf, axis, "entry names of the: default", names(default, 1))
            end
        end

        cached_vector = Formats.get_from_cache(daf, Formats.vector_cache_key(axis, name), StorageVector)
        if cached_vector != nothing
            if eltype(cached_vector) <: AbstractString
                cached_vector = as_read_only_array(cached_vector)
            end
            return as_named_vector(daf, axis, cached_vector)
        end

        if name == "name"
            result = as_named_vector(daf, axis, Formats.get_axis_through_cache(daf, axis))
            @debug "get_vector $(daf.name) / $(axis) : $(name) -> $(present(result))"
            return result
        end

        default_suffix = ""
        vector = nothing
        if !Formats.format_has_vector(daf, axis, name)
            if default == nothing
                @debug "get_vector $(daf.name) / $(axis) : $(name) -> $(present(nothing))"
                return nothing
            end
            default_suffix = " ?"
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

        if vector == nothing
            vector = Formats.get_vector_through_cache(daf, axis, name)
            if !(vector isa StorageVector)
                error(  # untested
                    "format_get_vector for daf format: $(typeof(daf))\n" *
                    "returned invalid Daf.StorageVector: $(present(vector))",
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

        result = as_named_vector(daf, axis, vector)
        @debug "get_vector $(daf.name) / $(axis) : $(name) -> $(present(result))$(default_suffix)"
        return result
    end
end

function require_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if !Formats.format_has_vector(daf, axis, name)
        error("missing vector: $(name)\nfor the axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

function require_no_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if Formats.format_has_vector(daf, axis, name)
        error("existing vector: $(name)\nfor the axis: $(axis)\nin the daf data: $(daf.name)")
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
    return with_read_lock(daf) do
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        result =
            haskey(daf.internal.cache, Formats.matrix_cache_key(rows_axis, columns_axis, name)) ||
            Formats.format_has_matrix(daf, rows_axis, columns_axis, name) ||
            (relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name))
        # @debug "has_matrix $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) $(relayout ? "%" : "#")> $(result)"
        return result
    end
end

"""
    set_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::Union{StorageNumber, StorageMatrix};
        [overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing

Set the matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. Since this is Julia, this
should be a column-major `matrix`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

If `relayout` (the default), this will also automatically [`relayout!`](@ref) the matrix and store the result, so the
data would also be stored in row-major layout (that is, with the axes flipped), similarly to calling
[`relayout_matrix!`](@ref).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function set_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageNumber, StorageMatrix};
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    return with_write_lock(daf) do
        relayout = relayout && rows_axis != columns_axis

        @debug "set_matrix! $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) <$(relayout ? "%" : "#")$(overwrite ? "=" : "-") $(matrix)"

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if matrix isa StorageMatrix
            require_column_major(matrix)
            require_axis_length(daf, "matrix rows", size(matrix, Rows), rows_axis)
            require_axis_length(daf, "matrix columns", size(matrix, Columns), columns_axis)
            if matrix isa NamedMatrix
                require_dim_name(daf, rows_axis, "matrix rows dim name", dimnames(matrix, 1); prefix = "rows")
                require_dim_name(daf, columns_axis, "matrix columns dim name", dimnames(matrix, 2); prefix = "columns")
                require_axis_names(daf, rows_axis, "row names of the: matrix", names(matrix, 1))
                require_axis_names(daf, columns_axis, "column names of the: matrix", names(matrix, 2))
            end
        end

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
        end

        if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
        end

        if relayout && overwrite && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            Formats.format_delete_matrix!(daf, columns_axis, rows_axis, name; for_set = true)
        end

        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(columns_axis, rows_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(rows_axis, columns_axis))
        for cache_key in Formats.matrix_relayout_names_cache_keys(rows_axis, columns_axis)
            Formats.invalidate_cached!(daf, cache_key)
        end

        Formats.format_set_matrix!(daf, rows_axis, columns_axis, name, matrix)
        if relayout && rows_axis != columns_axis
            Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name)
        end
        return nothing
    end
end

"""
    empty_dense_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T};
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber}

Create an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`, pass it to
`fill`, and return the result. Since this is Julia, this will be a column-major `matrix`.

The returned matrix will be uninitialized; the caller is expected to `fill` it with values. This saves creating a copy
of the matrix before setting it in `daf`, which makes a huge difference when creating matrices on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function empty_dense_matrix!(
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber}
    return with_write_lock(daf) do
        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        elseif Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
        end

        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(columns_axis, rows_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(rows_axis, columns_axis))
        for cache_key in Formats.matrix_relayout_names_cache_keys(rows_axis, columns_axis)
            Formats.invalidate_cached!(daf, cache_key)
        end

        named = as_named_matrix(
            daf,
            rows_axis,
            columns_axis,
            Formats.format_empty_dense_matrix!(daf, rows_axis, columns_axis, name, eltype),
        )
        result = fill(named)

        @debug "empty_dense_matrix! $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) <$(overwrite ? "=" : "-") $(named)"
        return result
    end
end

"""
    empty_sparse_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        intdype::Type{I};
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber, I <: StorageInteger}

Create an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`, pass it to
`fill`, and return the result.

The returned matrix will be uninitialized; the caller is expected to `fill` it with values. This means manually filling
the `colptr`, `rowval` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow
pre-allocating disk space. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse matrix is created by concatenating several smaller ones; this function
allows doing so directly into the data, avoiding a copy in case of memory-mapped disk formats.

!!! warning


It is the caller's responsibility to fill the three vectors with valid data. Specifically, you must ensure:

  - `colptr[1] == 1`
  - `colptr[end] == nnz + 1`
  - `colptr[i] <= colptr[i + 1]`
  - for all `j`, for all `i` such that `colptr[j] <= i` and `i + 1 < colptr[j + 1]`, `1 <= rowptr[i] < rowptr[i + 1] <= nrows`

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If not `overwrite` (the default), this also
verifies the `name` matrix does not exist for the `rows_axis` and `columns_axis`.
"""
function empty_sparse_matrix!(
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber, I <: StorageInteger}
    return with_write_lock(daf) do
        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        elseif Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
        end

        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(columns_axis, rows_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(rows_axis, columns_axis))
        for cache_key in Formats.matrix_relayout_names_cache_keys(rows_axis, columns_axis)
            Formats.invalidate_cached!(daf, cache_key)
        end

        empty_matrix = Formats.format_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, eltype, nnz, indtype)
        result = fill(as_named_matrix(daf, rows_axis, columns_axis, empty_matrix))
        verified = SparseMatrixCSC(size(empty_matrix)..., empty_matrix.colptr, empty_matrix.rowval, empty_matrix.nzval)

        @debug "empty_sparse_matrix! $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) <$(overwrite ? "=" : "-") $(verified)"
        return result
    end
end

"""
    relayout_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [overwrite::Bool = false]
    )::Nothing

Given a matrix property with some `name` exists (in column-major layout) in `daf` for the `rows_axis` and the
`columns_axis`, then [`relayout!`](@ref) it and store the row-major result as well (that is, with flipped axes).

This is useful following calling [`empty_dense_matrix!`](@ref) or [`empty_sparse_matrix!`](@ref) to ensure both layouts
of the matrix are stored in `def`. When calling [`set_matrix!`](@ref), it is simpler to just specify (the default)
`relayout = true`.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, and that there is a `name` (column-major) matrix
property for them. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
*flipped* `rows_axis` and `columns_axis`.

!!! note

    A restriction of the way `Daf` stores data is that square data is only stored in one (column-major) layout (e.g., to
    store a weighted directed graph between cells, you may store an outgoing_weights matrix where each cell's column
    holds the outgoing weights from the cell to the other cells. In this case you **can't** ask `Daf` to relayout the
    matrix to row-major order so that each cell's row would be the incoming weights from the other cells. Instead you
    would need to explicitly store a separate incoming_weights matrix where each cell's column holds the incoming
    weights).
"""
function relayout_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    overwrite::Bool = false,
)::Nothing
    return with_write_lock(daf) do
        @debug "relayout_matrix! $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) <$(overwrite ? "=" : "-")>"

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if rows_axis == columns_axis
            error(
                "can't relayout square matrix: $(name)\n" *
                "of the axis: $(rows_axis)\n" *
                "due to daf representation limitations\n" *
                "in the daf data: $(daf.name)",
            )
        end

        require_matrix(daf, rows_axis, columns_axis, name; relayout = false)

        if !overwrite
            require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
        elseif Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            Formats.format_delete_matrix!(daf, columns_axis, rows_axis, name; for_set = true)
        end

        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(columns_axis, rows_axis, name))

        Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name)
        return nothing
    end
end

"""
    delete_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [must_exist::Bool = true,
        relayout::Bool = true]
    )::Nothing

Delete a matrix property with some `name` for some `rows_axis` and `columns_axis` from `daf`.

If `relayout` (the default), this will also delete the matrix in the other layout (that is, with flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If `must_exist` (the default), this also verifies
the `name` matrix exists for the `rows_axis` and `columns_axis`.
"""
function delete_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
    relayout::Bool = true,
)::Nothing
    return with_write_lock(daf) do
        relayout = relayout && rows_axis != columns_axis

        @debug "delete_matrix! $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) $(must_exist ? "" : " ?")"

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if must_exist
            require_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
        end

        if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = false)
        end

        if relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            Formats.format_delete_matrix!(daf, columns_axis, rows_axis, name; for_set = false)
        end

        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(columns_axis, rows_axis, name))
        Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(rows_axis, columns_axis))
        for cache_key in Formats.matrix_relayout_names_cache_keys(rows_axis, columns_axis)
            Formats.invalidate_cached!(daf, cache_key)
        end
        return nothing
    end
end

"""
    matrix_names(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString;
        [relayout::Bool = true]
    )::AbstractStringSet

The names of the matrix properties for the `rows_axis` and `columns_axis` in `daf`.

If `relayout` (default), then this will include the names of matrices that exist in the other layout (that is, with
flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`.
"""
function matrix_names(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString;
    relayout::Bool = true,
)::AbstractStringSet
    return with_read_lock(daf) do
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if !relayout
            names = Formats.get_matrix_names_through_cache(daf, rows_axis, columns_axis)
        else
            cache_keys = Formats.matrix_relayout_names_cache_keys(rows_axis, columns_axis)
            names = Formats.get_from_cache(daf, cache_keys[1], AbstractStringSet)
            if names == nothing
                upgrade_to_write_lock(daf)
                names = Formats.get_matrix_names_through_cache(daf, rows_axis, columns_axis)
                names = union(names, Formats.get_matrix_names_through_cache(daf, columns_axis, rows_axis))
                for cache_key in cache_keys
                    Formats.cache_data!(daf, cache_key, names, MemoryData)
                end
            end
        end

        # @debug "matrix_names $(daf.name) / $(rows_axis) / $(columns_axis) $(relayout ? "%" : "#")> $(present(names))"
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

function require_no_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        error(
            "existing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    if relayout
        require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
    end
    return nothing
end

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
result axes are the names of the relevant axes entries (same as returned by [`get_axis`](@ref)).

If `relayout` (the default), then if the matrix is only stored in the other memory layout (that is, with flipped axes),
then automatically call [`relayout!`](@ref) to compute the result. If `daf isa DafWriter`, then store the result for
future use; otherwise, just cache it as [`MemoryData`](@ref CacheType). This may lock up very large amounts of memory;
you can call `empty_cache!` to release it.

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
    return with_read_lock(daf) do
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if default isa StorageMatrix
            require_column_major(default)
            require_axis_length(daf, "default rows", size(default, Rows), rows_axis)
            require_axis_length(daf, "default columns", size(default, Columns), columns_axis)
            if default isa NamedMatrix
                require_dim_name(daf, rows_axis, "default rows dim name", dimnames(default, 1); prefix = "rows")
                require_dim_name(
                    daf,
                    columns_axis,
                    "default columns dim name",
                    dimnames(default, 2);
                    prefix = "columns",
                )
                require_axis_names(daf, rows_axis, "row names of the: default", names(default, 1))
                require_axis_names(daf, columns_axis, "column names of the: default", names(default, 2))
            end
        end

        cached_matrix =
            Formats.get_from_cache(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name), StorageMatrix)
        if cached_matrix != nothing
            return as_named_matrix(daf, rows_axis, columns_axis, cached_matrix)
        end

        default_suffix = ""
        matrix = nothing
        if !Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            if relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
                upgrade_to_write_lock(daf)
                if daf isa DafWriter
                    Formats.format_relayout_matrix!(daf, columns_axis, rows_axis, name)
                else
                    cache_key = Formats.matrix_cache_key(rows_axis, columns_axis, name)
                    cache_entry = get!(daf.internal.cache, cache_key) do
                        Formats.store_cached_dependency_key!(daf, cache_key, Formats.axis_cache_key(rows_axis))
                        Formats.store_cached_dependency_key!(daf, cache_key, Formats.axis_cache_key(columns_axis))
                        flipped_matrix = Formats.get_matrix_through_cache(daf, columns_axis, rows_axis, name)
                        return CacheEntry(MemoryData, transpose(relayout!(flipped_matrix)))
                    end
                    matrix = cache_entry.data
                end
            else
                if default == nothing
                    @debug "get_matrix $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) -> $(present(nothing)) ?"
                    return nothing
                end
                default_suffix = " ?"
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
                    matrix = fill(
                        default,
                        Formats.format_axis_length(daf, rows_axis),
                        Formats.format_axis_length(daf, columns_axis),
                    )
                end
            end
        end

        if matrix == nothing
            matrix = Formats.get_matrix_through_cache(daf, rows_axis, columns_axis, name)
            if !(matrix isa StorageMatrix)
                error( # untested
                    "format_get_matrix for daf format: $(typeof(daf))\n" *
                    "returned invalid Daf.StorageMatrix: $(present(matrix))",
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
                    "returned non column-major matrix: $(present(matrix))",
                )
            end
        end

        result = as_named_matrix(daf, rows_axis, columns_axis, matrix)
        @debug "get_matrix $(daf.name) / $(rows_axis) / $(columns_axis) : $(name) -> $(present(result))$(default_suffix)"
        return result
    end
end

function require_column_major(matrix::StorageMatrix)::Nothing
    if major_axis(matrix) != Columns
        error("type not in column-major layout: $(present(matrix))")
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

function require_not_name(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if name == "name"
        error("setting the reserved vector: name\n" * "for the axis: $(axis)\n" * "in the daf data: $(daf.name)")
    end
    return nothing
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
    expected_names = get_axis(daf, axis)
    if names != expected_names
        error("$(what)\nmismatch the entry names of the axis: $(axis)\nin the daf data: $(daf.name)")
    end
end

function as_named_vector(daf::DafReader, axis::AbstractString, vector::NamedVector)::NamedArray
    return vector
end

function as_named_vector(daf::DafReader, axis::AbstractString, vector::AbstractVector)::NamedArray
    axis_names_dict = get(daf.internal.axes, axis, nothing)
    if axis_names_dict == nothing
        named_array = NamedArray(vector; names = (get_axis(daf, axis),), dimnames = (axis,))
        daf.internal.axes[axis] = named_array.dicts[1]
        return named_array

    else
        return NamedArray(vector, (axis_names_dict,), (axis,))
    end
end

function as_named_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::NamedMatrix,
)::NamedArray
    return matrix
end

function as_named_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::AbstractMatrix,
)::NamedArray
    rows_axis_names_dict = get(daf.internal.axes, rows_axis, nothing)
    columns_axis_names_dict = get(daf.internal.axes, columns_axis, nothing)
    if rows_axis_names_dict == nothing || columns_axis_names_dict == nothing
        named_array = NamedArray(
            matrix;
            names = (get_axis(daf, rows_axis), get_axis(daf, columns_axis)),
            dimnames = (rows_axis, columns_axis),
        )
        daf.internal.axes[rows_axis] = named_array.dicts[1]
        daf.internal.axes[columns_axis] = named_array.dicts[2]
        return named_array

    else
        return NamedArray(matrix, (rows_axis_names_dict, columns_axis_names_dict), (rows_axis, columns_axis))
    end
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

"""
    description(daf::DafReader[; deep::Bool = false])::AbstractString

Return a (multi-line) description of the contents of `daf`. This tries to hit a sweet spot between usefulness and
terseness. If `deep`, also describes any data set nested inside this one (if any).
"""
function description(daf::DafReader; deep::Bool = false)::String
    return with_read_lock(daf) do
        lines = String[]
        description(daf, "", lines, deep)
        push!(lines, "")
        return join(lines, "\n")
    end
end

function description(daf::DafReader, indent::AbstractString, lines::Vector{String}, deep::Bool)::Nothing
    if indent == ""
        push!(lines, "$(indent)name: $(daf.name)")
    else
        push!(lines, "- $(indent[3:end])name: $(daf.name)")
    end

    Formats.format_description_header(daf, indent, lines)

    scalars_description(daf, indent, lines)

    axes = collect(Formats.format_axis_names(daf))
    sort!(axes)
    if !isempty(axes)
        axes_description(daf, axes, indent, lines)
        vectors_description(daf, axes, indent, lines)
        matrices_description(daf, axes, indent, lines)
        cache_description(daf, axes, indent, lines)
    end

    Formats.format_description_footer(daf, indent, lines, deep)
    return nothing
end

function scalars_description(daf::DafReader, indent::AbstractString, lines::Vector{String})::Nothing
    scalars = collect(Formats.get_scalar_names_through_cache(daf))
    if !isempty(scalars)
        sort!(scalars)
        push!(lines, "$(indent)scalars:")
        for scalar in scalars
            push!(lines, "$(indent)  $(scalar): $(present(Formats.format_get_scalar(daf, scalar)))")
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
        push!(lines, "$(indent)  $(axis): $(Formats.format_axis_length(daf, axis)) entries")
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
        vectors = collect(Formats.get_vector_names_through_cache(daf, axis))
        if !isempty(vectors)
            if is_first
                push!(lines, "$(indent)vectors:")
                is_first = false
            end
            sort!(vectors)
            push!(lines, "$(indent)  $(axis):")
            for vector in vectors
                push!(
                    lines,
                    "$(indent)    $(vector): $(present(base_array(Formats.format_get_vector(daf, axis, vector))))",
                )
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
            matrices = collect(Formats.get_matrix_names_through_cache(daf, rows_axis, columns_axis))
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
                        present(base_array(Formats.format_get_matrix(daf, rows_axis, columns_axis, matrix))),
                    )
                end
            end
        end
    end
    return nothing
end

function cache_description(
    daf::DafReader,
    axes::AbstractStringVector,
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    is_first = true
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
        push!(lines, "$(indent)  '$(key)': ($(cache_entry.cache_type)) $(present(value))")
    end
    return nothing
end

function Messages.present(daf::DafReader; name::Maybe{AbstractString} = nothing)::AbstractString
    if name == nothing
        name = daf.name
    end
    return "$(typeof(daf)) $(name)"
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
    return with_write_lock(daf) do
        @assert clear == nothing || keep == nothing
        if clear == nothing && keep == nothing
            empty!(daf.internal.cache)
        else
            filter!(daf.internal.cache) do key_value
                cache_type = key_value[2].cache_type
                return cache_type == keep || (cache_type != clear && clear != nothing)
            end
        end

        if isempty(daf.internal.cache)
            empty!(daf.internal.dependency_cache_keys)
        else
            for (cache_key, dependent_keys) in daf.internal.dependency_cache_keys
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
