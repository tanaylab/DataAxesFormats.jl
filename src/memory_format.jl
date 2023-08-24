"""
In-memory storage `Daf` storage format.
"""
module MemoryFormat

export MemoryDaf

using Daf.Data
using Daf.Formats
using Daf.MatrixLayouts
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
struct MemoryDaf <: DafWriter
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

function Formats.format_has_scalar(memory::MemoryDaf, name::AbstractString)::Bool
    return haskey(memory.scalars, name)
end

function Formats.format_set_scalar!(memory::MemoryDaf, name::AbstractString, value::StorageScalar)::Nothing
    memory.scalars[name] = value
    return nothing
end

function Formats.format_delete_scalar!(memory::MemoryDaf, name::AbstractString; for_set::Bool)::Nothing
    delete!(memory.scalars, name)
    return nothing
end

function Formats.format_get_scalar(memory::MemoryDaf, name::AbstractString)::StorageScalar
    return memory.scalars[name]
end

function Formats.format_scalar_names(memory::MemoryDaf)::AbstractSet{String}
    return keys(memory.scalars)
end

function Formats.format_has_axis(memory::MemoryDaf, axis::AbstractString)::Bool
    return haskey(memory.axes, axis)
end

function Formats.format_add_axis!(memory::MemoryDaf, axis::AbstractString, entries::DenseVector{String})::Nothing
    memory.axes[axis] = entries
    memory.vectors[axis] = Dict{String, StorageVector}()
    memory.matrices[axis] = Dict{String, Dict{String, StorageMatrix}}()

    for other_axis in keys(memory.axes)
        memory.matrices[axis][other_axis] = Dict{String, StorageMatrix}()
        memory.matrices[other_axis][axis] = Dict{String, StorageMatrix}()
    end

    return nothing
end

function Formats.format_delete_axis!(memory::MemoryDaf, axis::AbstractString)::Nothing
    delete!(memory.axes, axis)
    delete!(memory.vectors, axis)
    delete!(memory.matrices, axis)

    for other_axis in keys(memory.matrices)
        delete!(memory.matrices[other_axis], axis)
    end

    return nothing
end

function Formats.format_axis_names(memory::MemoryDaf)::AbstractSet{String}
    return keys(memory.axes)
end

function Formats.format_get_axis(memory::MemoryDaf, axis::AbstractString)::DenseVector{String}
    return memory.axes[axis]
end

function Formats.format_axis_length(memory::MemoryDaf, axis::AbstractString)::Int64
    return length(memory.axes[axis])
end

function Formats.format_has_vector(memory::MemoryDaf, axis::AbstractString, name::AbstractString)::Bool
    return haskey(memory.vectors[axis], name)
end

function Formats.format_set_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{Number, String, StorageVector},
)::Nothing
    if vector isa StorageVector
        memory.vectors[axis][name] = vector
    else
        memory.vectors[axis][name] = fill(vector, Formats.format_axis_length(memory, axis))
    end

    return nothing
end

function Formats.format_empty_dense_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::DenseVector{T} where {T <: Number}
    nelements = Formats.format_axis_length(memory, axis)
    vector = Vector{T}(undef, nelements)
    memory.vectors[axis][name] = vector
    return vector
end

function Formats.format_empty_sparse_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    nelements = Formats.format_axis_length(memory, axis)
    nzind = Vector{I}(undef, nnz)
    nzval = Vector{T}(undef, nnz)
    vector = SparseVector(nelements, nzind, nzval)
    memory.vectors[axis][name] = vector
    return vector
end

function Formats.format_delete_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,
)::Nothing
    delete!(memory.vectors[axis], name)
    return nothing
end

function Formats.format_vector_names(memory::MemoryDaf, axis::AbstractString)::AbstractSet{String}
    return keys(memory.vectors[axis])
end

function Formats.format_get_vector(memory::MemoryDaf, axis::AbstractString, name::AbstractString)::StorageVector
    return memory.vectors[axis][name]
end

function Formats.format_has_matrix(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return haskey(memory.matrices[rows_axis][columns_axis], name)
end

function Formats.format_set_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{Number, String, StorageMatrix},
)::Nothing
    if matrix isa StorageMatrix
        memory.matrices[rows_axis][columns_axis][name] = matrix
    else
        memory.matrices[rows_axis][columns_axis][name] = fill(
            matrix,
            Formats.format_axis_length(memory, rows_axis),
            Formats.format_axis_length(memory, columns_axis),
        )
    end

    return nothing
end

function Formats.format_empty_dense_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::DenseMatrix{T} where {T <: Number}
    nrows = Formats.format_axis_length(memory, rows_axis)
    ncols = Formats.format_axis_length(memory, columns_axis)
    matrix = Matrix{T}(undef, nrows, ncols)
    memory.matrices[rows_axis][columns_axis][name] = matrix
    return matrix
end

function Formats.format_empty_sparse_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    nrows = Formats.format_axis_length(memory, rows_axis)
    ncols = Formats.format_axis_length(memory, columns_axis)
    colptr = fill(I(1), ncols + 1)
    colptr[end] = nnz + 1
    rowval = Vector{I}(undef, nnz)
    nzval = Vector{T}(undef, nnz)
    matrix = SparseMatrixCSC(nrows, ncols, colptr, rowval, nzval)
    memory.matrices[rows_axis][columns_axis][name] = matrix
    return matrix
end

function Formats.format_relayout_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    matrix = Formats.format_get_matrix(memory, rows_axis, columns_axis, name)
    relayout = relayout!(matrix)
    Formats.format_set_matrix!(memory, columns_axis, rows_axis, name, transpose(relayout))
    return nothing
end

function Formats.format_delete_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,
)::Nothing
    delete!(memory.matrices[rows_axis][columns_axis], name)
    return nothing
end

function Formats.format_matrix_names(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    return keys(memory.matrices[rows_axis][columns_axis])
end

function Formats.format_get_matrix(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    return memory.matrices[rows_axis][columns_axis][name]
end

end
