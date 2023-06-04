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
export axis_names
export delete_axis!
export delete_scalar!
export delete_vector!
export get_axis
export get_scalar
export get_vector
export has_axis
export has_scalar
export has_vector
export scalar_names
export set_scalar!
export set_vector!
export storage_name
export unsafe_add_axis!
export unsafe_axis_length
export unsafe_delete_axis!
export unsafe_delete_scalar!
export unsafe_delete_vector!
export unsafe_get_axis
export unsafe_get_scalar
export unsafe_get_vector
export unsafe_set_scalar!
export unsafe_set_vector!
export unsafe_vector_names
export vector_names

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

If `must_exist` is `true` (the default), this first verifies the `name` scalar exists in the `storage`.
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

This trusts that the `name` scalar exists in the `storage`.
"""
function unsafe_delete_scalar!(storage::AbstractStorage, name::String)::Nothing
    return error("missing method: unsafe_delete_scalar! for storage type: $(typeof(storage))")
end

"""
    scalar_names(storage::AbstractStorage)::Set{String}

The names of the scalars in the `storage`.
"""
function scalar_names(storage::AbstractStorage)::AbstractSet{String}
    return error("missing method: scalar_names for storage type: $(typeof(storage))")
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

This trusts the `name` scalar exists in the `storage`.
"""
function unsafe_get_scalar(storage::AbstractStorage, name::String)::Any
    return error("missing method: unsafe_get_scalar for storage type: $(typeof(storage))")
end

function require_scalar(storage::AbstractStorage, name::String)::Nothing
    if !has_scalar(storage, name)
        error("missing scalar: $(name) in the storage: $(storage_name(storage))")
    end
    return nothing
end

function require_no_scalar(storage::AbstractStorage, name::String)::Nothing
    if has_scalar(storage, name)
        error("existing scalar: $(name) in the storage: $(storage_name(storage))")
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
        error("non-unique entries for new axis: $(axis) in the storage: $(storage_name(storage))")
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

If `must_exist` is `true` (the default), this first verifies the `axis` exists in the `storage`.
"""
function delete_axis!(storage::AbstractStorage, axis::String; must_exist::Bool = true)::Nothing
    if must_exist
        require_axis(storage, axis)
    elseif !has_axis(storage, axis)
        return nothing
    end
    for name in vector_names(storage, axis)
        unsafe_delete_vector!(storage, axis, name)
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

This trusts that the `axis` exists in the `storage`, and that all data that is based on this axis has already been
deleted.
"""
function unsafe_delete_axis!(storage::AbstractStorage, axis::String)::Nothing
    return error("missing method: unsafe_delete_axis! for storage type: $(typeof(storage))")
end

"""
    axis_names(storage::AbstractStorage)::AbstractSet{String}

The names of the axes of the `storage`.
"""
function axis_names(storage::AbstractStorage)::AbstractSet{String}
    return error("missing method: axis_names for storage type: $(typeof(storage))")
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

This trusts the `axis` exists in the `storage`.
"""
function unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}
    return error("missing method: unsafe_get_axis for storage type: $(typeof(storage))")
end

"""
    axis_length(storage::AbstractStorage, axis::String)::Int64

The number of entries along the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function axis_length(storage::AbstractStorage, axis::String)::Int64
    require_axis(storage, axis)
    return unsafe_axis_length(storage, axis)
end

"""
    unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64
    return error("missing method: unsafe_axis_length for storage type: $(typeof(storage))")
end

function require_axis(storage::AbstractStorage, axis::String)::Nothing
    if !has_axis(storage, axis)
        error("missing axis: $(axis) in the storage: $(storage_name(storage))")
    end
    return nothing
end

function require_no_axis(storage::AbstractStorage, axis::String)::Nothing
    if has_axis(storage, axis)
        error("existing axis: $(axis) in the storage: $(storage_name(storage))")
    end
    return nothing
end

"""
    has_vector(storage::AbstractStorage, axis::String, name::String)::Bool

Check whether a vector with some `name` exists for the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function has_vector(storage::AbstractStorage, axis::String, name::String)::Bool
    require_axis(storage, axis)
    return unsafe_has_vector(storage, axis, name)
end

"""
    has_vector(storage::AbstractStorage, axis::String, name::String)::Bool

Implement checking whether a vector with some `name` exists for the `axis` in the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_has_vector(storage::AbstractStorage, axis::String, name::String)::Bool
    return error("missing method: unsafe_has_vector for storage type: $(typeof(storage))")
end

"""
    set_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        value::Any;
        overwrite::Bool = false,
    )::Nothing

Set the `value` of a vector with some `name` for some `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`. If `overwrite` is `false` (the default), this also verifies the
`name` vector does not exist for the `axis`.
"""
function set_vector!(storage::AbstractStorage, axis::String, name::String, value::Any; overwrite::Bool = false)::Nothing
    require_axis(storage, axis)
    if length(value) != axis_length(storage, axis)
        error(
            "value length: $(length(value)) " *  # only seems untested
            "is different from axis: $(axis) " *  # only seems untested
            "length: $(axis_length(storage, axis)) " *  # only seems untested
            "in the storage: $(storage_name)",  # only seems untested
        )
    end
    if !overwrite
        require_no_vector(storage, axis, name)
    end
    unsafe_set_vector!(storage, axis, name, value)
    return nothing
end

"""
    unsafe_set_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        value::Any,
    )::Nothing

Implement setting the `value` of a vector with some `name` for some `axis` in the `storage`.

This trusts the `axis` exists in the `storage`. It will silently overwrite an existing `value` for the same `name`
vector for the `axis`.
"""
function unsafe_set_vector!(storage::AbstractStorage, axis::String, name::String, value::Any)::Nothing
    return error("missing method: unsafe_set_vector! for storage type: $(typeof(storage))")  # untested
end

"""
    delete_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing vector with some `name` for some `axis` from the `storage`.

This first verifies the `axis` exists in the `storage`. If `must_exist` is `true` (the default), this also verifies the
`name` vector exists for the `axis`.
"""
function delete_vector!(storage::AbstractStorage, axis::String, name::String; must_exist::Bool = true)::Nothing
    require_axis(storage, axis)
    if must_exist
        require_vector(storage, axis, name)
    elseif !has_vector(storage, axis, name)
        return nothing
    end
    unsafe_delete_vector!(storage, axis, name)
    return nothing
end

"""
    unsafe_delete_vector!(storage::AbstractStorage, axis::String, name::String)::Nothing

Implement deleting a vector with some `name` for some `axis` from `storage`.

This trusts the `axis` exists in the `storage`, and that the `name` vector exists for the `axis`.
"""
function unsafe_delete_vector!(storage::AbstractStorage, axis::String, name::String)::Nothing
    return error("missing method: unsafe_delete_vector! for storage type: $(typeof(storage))")  # untested
end

"""
    vector_names(storage::AbstractStorage, axis::String)::Set{String}

The names of the vectors for the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function vector_names(storage::AbstractStorage, axis::String)::AbstractSet{String}
    require_axis(storage, axis)
    return unsafe_vector_names(storage, axis)
end

"""
    unsafe_vector_names(storage::AbstractStorage, axis::String)::Set{String}

Implement fetching the names of the vectors for the `axis` in the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_vector_names(storage::AbstractStorage, axis::String)::AbstractSet{String}
    return error("missing method: unsafe_vector_names for storage type: $(typeof(storage))")  # untested
end

"""
    get_vector(storage::AbstractStorage, axis::String, name::String[; default::Any])::Any

The value of a vector with some `name` for some `axis` in the `storage`.

This first verifies the `axis`
If `default` is not specified, this first verifies the `name` vector exists in the `storage`. Otherwise, if `default` is
an `AbstractVector`, it has to be of the same size of the `axis`, and is returned. Otherwise, a new `Vector` is created
of the correct size containing the `default`, and is returned.
"""
function get_vector(storage::AbstractStorage, axis::String, name::String; default::Any = NO_DEFAULT)::AbstractVector
    require_axis(storage, axis)
    if default isa AbstractVector && length(default) != axis_length(storage, axis)
        error(
            "default length: $(length(default)) " *  # only seems untested
            "is different from axis: $(axis) " *  # only seems untested
            "length: $(axis_length(storage)) " *  # only seems untested
            "in the storage: $(storage_name)",  # only seems untested
        )
    end
    if default === NO_DEFAULT
        require_vector(storage, axis, name)
    elseif !has_vector(storage, axis, name)
        if default isa AbstractVector
            return default
        else
            return fill(default, axis_length(storage, axis))
        end
    end
    return unsafe_get_vector(storage, axis, name)
end

"""
    unsafe_get_vector(storage::AbstractStorage, axis::String, name::String)::AbstractVector

Implement fetching the value of a vector with some `name` in the `storage`.

This trusts the `axis` exists in the `storage` and the `name` vector exists for the `axis`.
"""
function unsafe_get_vector(storage::AbstractStorage, name::String)::Any
    return error("missing method: unsafe_get_vector for storage type: $(typeof(storage))")  # untested
end

function require_vector(storage::AbstractStorage, axis::String, name::String)::Nothing
    if !has_vector(storage, axis, name)
        error("missing vector: $(name) for the axis: $(axis) in the storage: $(storage_name(storage))")
    end
    return nothing
end

function require_no_vector(storage::AbstractStorage, axis::String, name::String)::Nothing
    if has_vector(storage, axis, name)
        error("existing vector: $(name) for the axis: $(axis) in the storage: $(storage_name(storage))")
    end
    return nothing
end

include("storage/memory.jl")

end # module
