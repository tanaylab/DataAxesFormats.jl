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

    axes::Dict{String, DenseVector{String}}

    function MemoryStorage(name::String)
        axes = Dict{String, DenseVector{String}}()

        if endswith(name, "#")
            name = name * string(objectid(axes); base = 16)
        end

        return new(name, axes)
    end
end

function Storage.name(storage::MemoryStorage)::String
    return storage.name
end

function Storage.unsafe_add_axis!(storage::MemoryStorage, axis::String, entries::DenseVector{String})::Nothing
    storage.axes[axis] = entries
    return nothing
end

function Storage.unsafe_delete_axis!(storage::MemoryStorage, axis::String)::Nothing
    delete!(storage.axes, axis)
    return nothing
end

function Storage.has_axis(storage::MemoryStorage, axis::String)::Bool
    return haskey(storage.axes, axis)
end

function Storage.unsafe_axis_length(storage::MemoryStorage, axis::String)::Int64
    return length(storage.axes[axis])
end

function Storage.unsafe_axis_entries(storage::MemoryStorage, axis::String)::DenseVector{String}
    return storage.axes[axis]
end
