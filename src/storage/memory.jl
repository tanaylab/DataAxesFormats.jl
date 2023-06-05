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

    matrices::Dict{String, Dict{String, Dict{String, AbstractMatrix}}}

    function MemoryStorage(name::String)
        scalars = Dict{String, Any}()
        axes = Dict{String, DenseVector{String}}()
        vectors = Dict{String, Dict{String, AbstractVector{String}}}()
        matrices = Dict{String, Dict{String, Dict{String, AbstractVector{String}}}}()

        if endswith(name, "#")
            name = name * string(objectid(axes); base = 16)
        end

        return new(name, scalars, axes, vectors, matrices)
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
    storage.matrices[axis] = Dict{String, Dict{String, AbstractMatrix}}()
    for other_axis in keys(storage.axes)
        storage.matrices[axis][other_axis] = Dict{String, AbstractMatrix}()
        storage.matrices[other_axis][axis] = Dict{String, AbstractMatrix}()
    end
    return nothing
end

function Storage.unsafe_delete_axis!(storage::MemoryStorage, axis::String)::Nothing
    delete!(storage.axes, axis)
    delete!(storage.vectors, axis)
    delete!(storage.matrices, axis)
    for other_axis in keys(storage.matrices)
        delete!(storage.matrices[other_axis], axis)
    end
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

function Storage.unsafe_set_vector!(
    storage::MemoryStorage,
    axis::String,
    name::String,
    vector::Union{Number, String, AbstractVector},
)::Nothing
    if vector isa AbstractVector
        storage.vectors[axis][name] = vector
    else
        storage.vectors[axis][name] = fill(vector, unsafe_axis_length(storage, axis))
    end
    return nothing
end

function Storage.unsafe_delete_vector!(storage::MemoryStorage, axis::String, name::String)::Nothing
    delete!(storage.vectors[axis], name)
    return nothing
end

function Storage.unsafe_vector_names(storage::MemoryStorage, axis::String)::AbstractSet{String}
    return keys(storage.vectors[axis])
end

function Storage.unsafe_get_vector(storage::MemoryStorage, axis::String, name::String)::AbstractVector
    return storage.vectors[axis][name]
end

function Storage.unsafe_has_matrix(storage::MemoryStorage, rows_axis::String, columns_axis::String, name::String)::Bool
    return haskey(storage.matrices[rows_axis][columns_axis], name)
end

function Storage.unsafe_set_matrix!(
    storage::MemoryStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    matrix::Union{Number, String, AbstractMatrix},
)::Nothing
    if matrix isa AbstractMatrix
        storage.matrices[rows_axis][columns_axis][name] = matrix
    else
        storage.matrices[rows_axis][columns_axis][name] =
            fill(matrix, unsafe_axis_length(storage, rows_axis), unsafe_axis_length(storage, columns_axis))  # only seems untested
    end
    return nothing
end

function Storage.unsafe_delete_matrix!(
    storage::MemoryStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
)::Nothing
    delete!(storage.matrices[rows_axis][columns_axis], name)
    return nothing
end

function Storage.unsafe_matrix_names(
    storage::MemoryStorage,
    rows_axis::String,
    columns_axis::String,
)::AbstractSet{String}
    return keys(storage.matrices[rows_axis][columns_axis])
end

function Storage.unsafe_get_matrix(
    storage::MemoryStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
)::AbstractMatrix
    return storage.matrices[rows_axis][columns_axis][name]
end
