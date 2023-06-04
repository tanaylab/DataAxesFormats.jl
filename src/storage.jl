"""
Storage objects provide low-level API for storing data in specific formats. To extend `Daf` to support an additional
format, create a new concrete implementation of `AbstractStorage` for that format.

A storage object contains some named scalar data, a set of axes (each with a unique name for each entry), and named
vector and matrix data based on these axes.

Data is identified by its unique name given the axes it is based on. That is, there is a separate namespace for scalar,
vectors for each specific axis, and matrices for each specific pair of axes.

For matrices, we keep careful track of their layout, that is, which axis is "major" and which is "minor". A storage will
only list matrix data under the axis order that describes its layout. In theory a storage may hold two copies of the
same matrix in both possible memory layouts, in which case it will be listed twice, under both axes orders.

In general, storage objects are as "dumb" as possible, to make it easier to create new storage format adapters. That is,
storage objects are just glorified key-value repositories, with the absolutely minimal necessary logic to deal with the
separate namespaces listed above. Most of this logic is common to all storage formats, and is contained in the base
functions so the concrete storage format adapters are even simpler.

The storage API is therefore very low-level, and is not suitable for use from outside the package, except for
constructing storage objects to be used by the higher-level `DafContainer` API.
"""
module Storage

using Reexport

export AbstractStorage
export add_axis!
export axis_length
export delete_axis!
export delete_scalar!
export get_axis
export get_scalar
export has_axis
export has_scalar
export set_scalar!
export storage_name
export unsafe_add_axis!
export unsafe_axis_length
export unsafe_delete_axis!
export unsafe_delete_scalar!
export unsafe_get_axis
export unsafe_get_scalar
export unsafe_set_scalar!

"""
An abstract interface for all `Daf` storage formats.
"""
abstract type AbstractStorage end

"""
    storage_name(storage::AbstractStorage)::String

Return a human-readable name identifying the `storage`.
"""
function storage_name(storage::AbstractStorage)::String
    return error("missing method: storage_name for storage type: $(typeof(storage))")
end

"""
    has_scalar(storage::AbstractStorage, name::String)::Bool

Check whether a scalar with some `name` exists in the `storage`.
"""
function has_scalar(storage::AbstractStorage, name::String)::Bool
    return error("missing method: has_scalar for storage type: $(typeof(storage))")
end

"""
    set_scalar!(
        storage::AbstractStorage,
        name::String,
        value::Any;
        overwrite::Bool = false,
    )::Nothing

Set the `value` of a scalar with some `name` in the `storage`.

If `overwrite` is `false` (the default), this first verifies the `name` does not exist.
"""
function set_scalar!(storage::AbstractStorage, name::String, value::Any; overwrite::Bool = false)::Nothing
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
        value::Any,
    )::Nothing

Implement setting the `value` of a scalar with some `name` in the `storage`.

This will silently overwrite an existing `value` for the same `name` scalar.
"""
function unsafe_set_scalar!(storage::AbstractStorage, name::String, value::Any)::Nothing
    return error("missing method: unsafe_set_scalar! for storage type: $(typeof(storage))")
end

"""
    delete_scalar!(
        storage::AbstractStorage,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing scalar with some `name` from the `storage`.

If `must_exist` is `true` (the default), this first verifies the `name` scalar does exist.
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

Implement deleting a scalar with some `name` from `storage`.

This trusts that the `name` scalar does exist in the `storage`.
"""
function unsafe_delete_scalar!(storage::AbstractStorage, axis::String)::Nothing
    return error("missing method: unsafe_delete_scalar! for storage type: $(typeof(storage))")
end

struct NoDefault end
NO_DEFAULT = NoDefault()

"""
    get_scalar(storage::AbstractStorage, name::String[; default::Any])::Any

The value of a scalar with some `name` in the `storage`.

If `default` is not specified, this first verifies the `name` scalar exists in the `storage`.
"""
function get_scalar(storage::AbstractStorage, name::String; default::Any = NO_DEFAULT)::Any
    if default === NO_DEFAULT
        require_scalar(storage, name)
    elseif !has_scalar(storage, name)
        return default
    end
    return unsafe_get_scalar(storage, name)
end

"""
    unsafe_get_scalar(storage::AbstractStorage, name::String)::Any

Implement fetching the value of a scalar with some `name` in the `storage`.

This trusts the `name` scalar does exist in the `storage`.
"""
function unsafe_get_scalar(storage::AbstractStorage, name::String)::Any
    return error("missing method: unsafe_get_scalar for storage type: $(typeof(storage))")
end

function require_scalar(storage::AbstractStorage, name::String)::Nothing
    if !has_scalar(storage, name)
        error("missing scalar: $(name) in storage: $(storage_name(storage))")
    end
    return nothing
end

function require_no_scalar(storage::AbstractStorage, name::String)::Nothing
    if has_scalar(storage, name)
        error("existing scalar: $(name) in storage: $(storage_name(storage))")
    end
    return nothing
end

"""
    has_axis(storage::AbstractStorage, axis::String)::Bool

Check whether an `axis` exists in the `storage`.
"""
function has_axis(storage::AbstractStorage, axis::String)::Bool
    return error("missing method: has_axis for storage type: $(typeof(storage))")
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
        error("non-unique entries for new axis: $(axis) in storage: $(storage_name(storage))")
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
function unsafe_add_axis!(storage::AbstractStorage, axis::String, entries::DenseVector{String})::Nothing
    return error("missing method: unsafe_add_axis! for storage type: $(typeof(storage))")
end

"""
    delete_axis!(
        storage::AbstractStorage,
        axis::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing `axis` from the `storage`. This will also delete any vector or matrix data that is based on this
axis.

If `must_exist` is `true` (the default), this first verifies the `axis` does exist.
"""
function delete_axis!(storage::AbstractStorage, axis::String; must_exist::Bool = true)::Nothing
    if must_exist
        require_axis(storage, axis)
    elseif !has_axis(storage, axis)
        return nothing
    end
    unsafe_delete_axis!(storage, axis)
    return nothing
end

"""
    unsafe_delete_axis!(
        storage::AbstractStorage,
        axis::String,
    )::Nothing

Implement deleting an `axis` from `storage`.

This trusts that the `axis` does exist in the `storage`, and that all data that is based on this axis has already been
deleted.
"""
function unsafe_delete_axis!(storage::AbstractStorage, axis::String)::Nothing
    return error("missing method: unsafe_delete_axis! for storage type: $(typeof(storage))")
end

"""
    get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}

The unique names of the entries of an `axis` of the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}
    require_axis(storage, axis)
    return unsafe_get_axis(storage, axis)
end

"""
    unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}

Implement fetching the unique names of the entries of an `axis` of the `storage`.

This trusts the `axis` does exist in the `storage`.
"""
function unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}
    return error("missing method: unsafe_get_axis for storage type: $(typeof(storage))")
end

"""
    axis_length(storage::AbstractStorage, axis::String)::Int64

The number of entries along the `axis` in the `storage`.

This first verifies the `axis` exists.
"""
function axis_length(storage::AbstractStorage, axis::String)::Int64
    require_axis(storage, axis)
    return unsafe_axis_length(storage, axis)
end

"""
    unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` does exist in the `storage`.
"""
function unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64
    return error("missing method: unsafe_axis_length for storage type: $(typeof(storage))")
end

function require_axis(storage::AbstractStorage, axis::String)::Nothing
    if !has_axis(storage, axis)
        error("missing axis: $(axis) in storage: $(storage_name(storage))")
    end
    return nothing
end

function require_no_axis(storage::AbstractStorage, axis::String)::Nothing
    if has_axis(storage, axis)
        error("existing axis: $(axis) in storage: $(storage_name(storage))")
    end
    return nothing
end

include("storage/memory.jl")

end # module
