export MemoryStorage

"""
    MemoryStorage(name::String)

Simple in-memory storage.

This just keeps everything in-memory, similarly to the way an `AnnData` object works; that is, this is a lightweight
object that just keeps references to the data it is given.

This is the "default" storage type you should use, unless you need to persist the data on the disk.

If the `name` ends with `#`, then we append an object identifier to it, to make it unique in the current process.
"""
struct MemoryStorage <: AbstractStorage
    name::String

    scalars::Dict{String, Any}

    axes::Dict{String, DenseVector{String}}

    vectors::Dict{String, Dict{String, AbstractVector}}

    function MemoryStorage(name::String)
        scalars = Dict{String, Any}()
        axes = Dict{String, DenseVector{String}}()
        vectors = Dict{String, Dict{String, AbstractVector{String}}}()

        if endswith(name, "#")
            name = name * string(objectid(axes); base = 16)
        end

        return new(name, scalars, axes, vectors)
    end
end

function Storage.storage_name(storage::MemoryStorage)::String
    return storage.name
end

function Storage.has_scalar(storage::MemoryStorage, name::String)::Bool
    return haskey(storage.scalars, name)
end

function Storage.unsafe_set_scalar!(storage::MemoryStorage, name::String, value::Any)::Nothing
    storage.scalars[name] = value
    return nothing
end

function Storage.unsafe_delete_scalar!(storage::MemoryStorage, name::String)::Nothing
    delete!(storage.scalars, name)
    return nothing
end

function Storage.unsafe_get_scalar(storage::MemoryStorage, name::String)::Any
    return storage.scalars[name]
end

function Storage.scalar_names(storage::MemoryStorage)::AbstractSet{String}
    return keys(storage.scalars)
end

function Storage.has_axis(storage::MemoryStorage, axis::String)::Bool
    return haskey(storage.axes, axis)
end

function Storage.unsafe_add_axis!(storage::MemoryStorage, axis::String, entries::DenseVector{String})::Nothing
    storage.axes[axis] = entries
    storage.vectors[axis] = Dict{String, AbstractVector}()
    return nothing
end

function Storage.unsafe_delete_axis!(storage::MemoryStorage, axis::String)::Nothing
    delete!(storage.axes, axis)
    delete!(storage.vectors, axis)
    return nothing
end

function Storage.axis_names(storage::MemoryStorage)::AbstractSet{String}
    return keys(storage.axes)
end

function Storage.unsafe_get_axis(storage::MemoryStorage, axis::String)::DenseVector{String}
    return storage.axes[axis]
end

function Storage.unsafe_axis_length(storage::MemoryStorage, axis::String)::Int64
    return length(storage.axes[axis])
end

function Storage.unsafe_has_vector(storage::MemoryStorage, axis::String, name::String)::Bool
    return haskey(storage.vectors[axis], name)
end

function Storage.unsafe_set_vector!(storage::MemoryStorage, axis::String, name::String, value::Any)::Nothing
    storage.vectors[axis][name] = value
    return nothing
end

function Storage.unsafe_delete_vector!(storage::MemoryStorage, axis::String, name::String)::Nothing
    delete!(storage.vectors[axis], name)
    return nothing
end

function Storage.unsafe_vector_names(storage::MemoryStorage, axis::String)::AbstractSet{String}
    return keys(storage.vectors[axis])
end

function Storage.unsafe_get_vector(storage::MemoryStorage, axis::String, name::String)::Any
    return storage.vectors[axis][name]
end
