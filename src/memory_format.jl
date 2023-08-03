"""
In-memory storage `Daf` storage format.
"""
module MemoryFormat

export MemoryDaf

using Daf.Data
using Daf.Formats
using Daf.Messages
using Daf.StorageTypes
using SparseArrays

import Daf.Formats
import Daf.Formats.Internal

"""
    MemoryDaf(name::String)

Simple in-memory storage.

This just keeps everything in-memory, similarly to the way an `AnnData` object works; that is, this is a lightweight
object that just keeps references to the data it is given.

This is the "default" storage type you should use, unless you need to persist the data on the disk.
"""
struct MemoryDaf <: WriteDaf
    internal::Internal

    scalars::Dict{String, StorageScalar}

    axes::Dict{String, DenseVector{String}}

    vectors::Dict{String, Dict{String, StorageVector}}

    matrices::Dict{String, Dict{String, Dict{String, StorageMatrix}}}
end

function MemoryDaf(name::AbstractString)::MemoryDaf
    scalars = Dict{String, StorageScalar}()
    axes = Dict{String, DenseVector{String}}()
    vectors = Dict{String, Dict{String, StorageVector{String}}}()
    matrices = Dict{String, Dict{String, Dict{String, StorageVector{String}}}}()
    return MemoryDaf(Internal(name), scalars, axes, vectors, matrices)
end

function Formats.format_has_scalar(storage::MemoryDaf, name::AbstractString)::Bool
    return haskey(storage.scalars, name)
end

function Formats.format_set_scalar!(storage::MemoryDaf, name::AbstractString, value::StorageScalar)::Nothing
    storage.scalars[name] = value
    return nothing
end

function Formats.format_delete_scalar!(storage::MemoryDaf, name::AbstractString)::Nothing
    delete!(storage.scalars, name)
    return nothing
end

function Formats.format_get_scalar(storage::MemoryDaf, name::AbstractString)::StorageScalar
    return storage.scalars[name]
end

function Formats.format_scalar_names(storage::MemoryDaf)::AbstractSet{String}
    return keys(storage.scalars)
end

function Formats.format_has_axis(storage::MemoryDaf, axis::AbstractString)::Bool
    return haskey(storage.axes, axis)
end

function Formats.format_add_axis!(storage::MemoryDaf, axis::AbstractString, entries::DenseVector{String})::Nothing
    storage.axes[axis] = entries
    storage.vectors[axis] = Dict{String, StorageVector}()
    storage.matrices[axis] = Dict{String, Dict{String, StorageMatrix}}()

    for other_axis in keys(storage.axes)
        storage.matrices[axis][other_axis] = Dict{String, StorageMatrix}()
        storage.matrices[other_axis][axis] = Dict{String, StorageMatrix}()
    end

    return nothing
end

function Formats.format_delete_axis!(storage::MemoryDaf, axis::AbstractString)::Nothing
    delete!(storage.axes, axis)
    delete!(storage.vectors, axis)
    delete!(storage.matrices, axis)

    for other_axis in keys(storage.matrices)
        delete!(storage.matrices[other_axis], axis)
    end

    return nothing
end

function Formats.format_axis_names(storage::MemoryDaf)::AbstractSet{String}
    return keys(storage.axes)
end

function Formats.format_get_axis(storage::MemoryDaf, axis::AbstractString)::DenseVector{String}
    return storage.axes[axis]
end

function Formats.format_axis_length(storage::MemoryDaf, axis::AbstractString)::Int64
    return length(storage.axes[axis])
end

function Formats.format_has_vector(storage::MemoryDaf, axis::AbstractString, name::AbstractString)::Bool
    return haskey(storage.vectors[axis], name)
end

function Formats.format_set_vector!(
    storage::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{Number, String, StorageVector},
)::Nothing
    if vector isa StorageVector
        storage.vectors[axis][name] = vector
    else
        storage.vectors[axis][name] = fill(vector, Formats.format_axis_length(storage, axis))
    end

    return nothing
end

function Formats.format_empty_dense_vector!(
    storage::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::DenseVector{T} where {T <: Number}
    nelements = Formats.format_axis_length(storage, axis)
    vector = Vector{T}(undef, nelements)
    storage.vectors[axis][name] = vector
    return vector
end

function Formats.format_empty_sparse_vector!(
    storage::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    nelements = Formats.format_axis_length(storage, axis)
    nzind = Vector{I}(undef, nnz)
    nzval = Vector{T}(undef, nnz)
    vector = SparseVector(nelements, nzind, nzval)
    storage.vectors[axis][name] = vector
    return vector
end

function Formats.format_delete_vector!(storage::MemoryDaf, axis::AbstractString, name::AbstractString)::Nothing
    delete!(storage.vectors[axis], name)
    return nothing
end

function Formats.format_vector_names(storage::MemoryDaf, axis::AbstractString)::AbstractSet{String}
    return keys(storage.vectors[axis])
end

function Formats.format_get_vector(storage::MemoryDaf, axis::AbstractString, name::AbstractString)::StorageVector
    return storage.vectors[axis][name]
end

function Formats.format_has_matrix(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return haskey(storage.matrices[rows_axis][columns_axis], name)
end

function Formats.format_set_matrix!(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{Number, String, StorageMatrix},
)::Nothing
    if matrix isa StorageMatrix
        storage.matrices[rows_axis][columns_axis][name] = matrix
    else
        storage.matrices[rows_axis][columns_axis][name] = fill(
            matrix,
            Formats.format_axis_length(storage, rows_axis),
            Formats.format_axis_length(storage, columns_axis),
        )
    end

    return nothing
end

function Formats.format_empty_dense_matrix!(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::DenseMatrix{T} where {T <: Number}
    nrows = Formats.format_axis_length(storage, rows_axis)
    ncols = Formats.format_axis_length(storage, columns_axis)
    matrix = Matrix{T}(undef, nrows, ncols)
    storage.matrices[rows_axis][columns_axis][name] = matrix
    return matrix
end

function Formats.format_empty_sparse_matrix!(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    nrows = Formats.format_axis_length(storage, rows_axis)
    ncols = Formats.format_axis_length(storage, columns_axis)
    colptr = fill(I(1), ncols + 1)
    colptr[end] = nnz + 1
    rowval = Vector{I}(undef, nnz)
    nzval = Vector{T}(undef, nnz)
    matrix = SparseMatrixCSC(nrows, ncols, colptr, rowval, nzval)
    storage.matrices[rows_axis][columns_axis][name] = matrix
    return matrix
end

function Formats.format_delete_matrix!(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    delete!(storage.matrices[rows_axis][columns_axis], name)
    return nothing
end

function Formats.format_matrix_names(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    return keys(storage.matrices[rows_axis][columns_axis])
end

function Formats.format_get_matrix(
    storage::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    return storage.matrices[rows_axis][columns_axis][name]
end

end
