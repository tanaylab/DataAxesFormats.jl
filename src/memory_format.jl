"""
In-memory `Daf` storage format.
"""
module MemoryFormat

export MemoryDaf

using Daf.Data
using Daf.Formats
using Daf.Generic
using Daf.MatrixLayouts
using Daf.Messages
using Daf.StorageTypes
using SparseArrays

import Daf.Formats
import Daf.Formats.Internal

"""
    struct MemoryDaf <: DafWriter ... end

    MemoryDaf(; name = "memory")

Simple in-memory storage.

This just keeps everything in-memory, similarly to the way an `AnnData` object works; that is, this is a lightweight
object that just keeps references to the data it is given.

This is the "default" storage type you should use, unless you need to persist the data on the disk.
"""
struct MemoryDaf <: DafWriter
    internal::Internal

    scalars::Dict{AbstractString, StorageScalar}

    axes::Dict{AbstractString, AbstractStringVector}

    vectors::Dict{AbstractString, Dict{AbstractString, StorageVector}}

    matrices::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, StorageMatrix}}}
end

function MemoryDaf(; name::AbstractString = "memory")::MemoryDaf
    scalars = Dict{AbstractString, StorageScalar}()
    axes = Dict{AbstractString, AbstractStringVector}()
    vectors = Dict{AbstractString, Dict{AbstractString, StorageVector}}()
    matrices = Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, StorageMatrix}}}()
    memory = MemoryDaf(Internal(name), scalars, axes, vectors, matrices)
    return memory
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

function Formats.format_scalar_names(memory::MemoryDaf)::AbstractStringSet
    return keys(memory.scalars)
end

function Formats.format_has_axis(memory::MemoryDaf, axis::AbstractString; for_change::Bool)::Bool
    return haskey(memory.axes, axis)
end

function Formats.format_add_axis!(memory::MemoryDaf, axis::AbstractString, entries::AbstractStringVector)::Nothing
    memory.axes[axis] = entries
    memory.vectors[axis] = Dict{AbstractString, StorageVector}()
    memory.matrices[axis] = Dict{AbstractString, Dict{AbstractString, StorageMatrix}}()

    for other_axis in keys(memory.axes)
        memory.matrices[axis][other_axis] = Dict{AbstractString, StorageMatrix}()
        memory.matrices[other_axis][axis] = Dict{AbstractString, StorageMatrix}()
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

function Formats.format_axis_names(memory::MemoryDaf)::AbstractStringSet
    return keys(memory.axes)
end

function Formats.format_get_axis(memory::MemoryDaf, axis::AbstractString)::AbstractStringVector
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
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    if vector isa StorageVector
        memory.vectors[axis][name] = vector
    elseif vector == 0
        memory.vectors[axis][name] = spzeros(typeof(vector), Formats.format_axis_length(memory, axis))
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
)::AbstractVector{T} where {T <: StorageNumber}
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
    nnz::StorageInteger,
    indtype::Type{I},
)::SparseVector{T, I} where {T <: StorageNumber, I <: StorageInteger}
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

function Formats.format_vector_names(memory::MemoryDaf, axis::AbstractString)::AbstractStringSet
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
    matrix::Union{StorageNumber, StorageMatrix},
)::Nothing
    if matrix isa StorageMatrix
        memory.matrices[rows_axis][columns_axis][name] = matrix
    elseif matrix == 0
        memory.matrices[rows_axis][columns_axis][name] = spzeros(
            typeof(matrix),
            Formats.format_axis_length(memory, rows_axis),
            Formats.format_axis_length(memory, columns_axis),
        )
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
)::AbstractMatrix{T} where {T <: StorageNumber}
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
    nnz::StorageInteger,
    indtype::Type{I},
)::SparseMatrixCSC{T, I} where {T <: StorageNumber, I <: StorageInteger}
    nrows = Formats.format_axis_length(memory, rows_axis)
    ncols = Formats.format_axis_length(memory, columns_axis)
    colptr = fill(I(nnz + 1), ncols + 1)
    colptr[1] = 1
    rowval = fill(I(1), nnz)
    nzval = Vector{T}(undef, nnz)
    println("TODOX JULIA format_empty_sparse_matrix")
    println("TODOX JULIA colptr $(colptr)")
    println("TODOX JULIA rowval $(rowval)")
    println("TODOX JULIA nzval $(nzval)")
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
)::AbstractStringSet
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

end  # module
