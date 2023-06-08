export MemoryStorage

"""
    MemoryStorage(name::String)

Simple in-memory storage.

This just keeps everything in-memory, similarly to the way an `AnnData` object works; that is, this is a lightweight
object that just keeps references to the data it is given.

This is the "default" storage type you should use, unless you need to persist the data on the disk.
"""
struct MemoryStorage <: AbstractStorage
    name::String

    is_frozen::Array{Bool, 1}

    scalars::Dict{String, Any}

    axes::Dict{String, DenseVector{String}}

    vectors::Dict{String, Dict{String, StorageVector}}

    matrices::Dict{String, Dict{String, Dict{String, StorageMatrix}}}

    function MemoryStorage(name::String)
        scalars = Dict{String, Any}()
        axes = Dict{String, DenseVector{String}}()
        vectors = Dict{String, Dict{String, StorageVector{String}}}()
        matrices = Dict{String, Dict{String, Dict{String, StorageVector{String}}}}()
        return new(unique_name(name), [false], scalars, axes, vectors, matrices)
    end
end

function Storage.is_frozen(storage::MemoryStorage)::Bool
    return storage.is_frozen[1]
end

function Storage.freeze(storage::MemoryStorage)::Nothing
    storage.is_frozen[1] = true
    return nothing
end

function Storage.unfreeze(storage::MemoryStorage)::Nothing
    storage.is_frozen[1] = false
    return nothing
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
    storage.vectors[axis] = Dict{String, StorageVector}()
    storage.matrices[axis] = Dict{String, Dict{String, StorageMatrix}}()

    for other_axis in keys(storage.axes)
        storage.matrices[axis][other_axis] = Dict{String, StorageMatrix}()
        storage.matrices[other_axis][axis] = Dict{String, StorageMatrix}()
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
    vector::Union{Number, String, StorageVector},
)::Nothing
    if vector isa StorageVector
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

function Storage.unsafe_get_vector(storage::MemoryStorage, axis::String, name::String)::StorageVector
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
    matrix::Union{Number, String, StorageMatrix},
)::Nothing
    if matrix isa StorageMatrix
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
)::StorageMatrix
    return storage.matrices[rows_axis][columns_axis][name]
end
