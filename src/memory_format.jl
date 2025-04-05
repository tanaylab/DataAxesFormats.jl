"""
In-memory `Daf` storage format.
"""
module MemoryFormat

export MemoryDaf

using ..Formats
using ..Readers
using ..StorageTypes
using ..Writers
using NamedArrays
using SparseArrays
using TanayLabUtilities

import ..Formats
import ..Formats.Internal

"""
    struct MemoryDaf <: DafWriter ... end

    MemoryDaf(; name = "memory")

Simple in-memory storage.

This just keeps everything in-memory, similarly to the way an `AnnData` object works; that is, this is a lightweight
object that just keeps references to the data it is given.

This is the "default" storage type you should use, unless you need to persist the data on the disk.
"""
struct MemoryDaf <: DafWriter
    name::AbstractString
    internal::Internal
    scalars::Dict{AbstractString, StorageScalar}
    axes::Dict{AbstractString, AbstractVector{<:AbstractString}}
    vectors::Dict{AbstractString, Dict{AbstractString, StorageVector}}
    matrices::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, StorageMatrix}}}
end

function MemoryDaf(; name::AbstractString = "memory")::MemoryDaf
    scalars = Dict{AbstractString, StorageScalar}()
    axes = Dict{AbstractString, AbstractVector{<:AbstractString}}()
    vectors = Dict{AbstractString, Dict{AbstractString, StorageVector}}()
    matrices = Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, StorageMatrix}}}()
    name = unique_name(name)
    memory = MemoryDaf(name, Internal(; cache_group = nothing, is_frozen = false), scalars, axes, vectors, matrices)
    @debug "Daf: $(brief(memory))"
    return memory
end

function Formats.format_has_scalar(memory::MemoryDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(memory)
    return haskey(memory.scalars, name)
end

function Formats.format_set_scalar!(memory::MemoryDaf, name::AbstractString, value::StorageScalar)::Nothing
    @assert Formats.has_data_write_lock(memory)
    memory.scalars[name] = value
    return nothing
end

function Formats.format_delete_scalar!(memory::MemoryDaf, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(memory)
    delete!(memory.scalars, name)  # NOJET
    return nothing
end

function Formats.format_get_scalar(memory::MemoryDaf, name::AbstractString)::StorageScalar
    @assert Formats.has_data_read_lock(memory)
    return memory.scalars[name]
end

function Formats.format_scalars_set(memory::MemoryDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(memory)
    return keys(memory.scalars)
end

function Formats.format_has_axis(memory::MemoryDaf, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(memory)
    return haskey(memory.axes, axis)
end

function Formats.format_add_axis!(
    memory::MemoryDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(memory)
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
    @assert Formats.has_data_write_lock(memory)
    delete!(memory.axes, axis)  # NOJET
    delete!(memory.vectors, axis)
    delete!(memory.matrices, axis)

    for other_axis in keys(memory.matrices)
        delete!(memory.matrices[other_axis], axis)
    end

    return nothing
end

function Formats.format_axes_set(memory::MemoryDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(memory)
    return keys(memory.axes)
end

function Formats.format_axis_vector(memory::MemoryDaf, axis::AbstractString)::AbstractVector{<:AbstractString}
    @assert Formats.has_data_read_lock(memory)
    return memory.axes[axis]
end

function Formats.format_axis_length(memory::MemoryDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(memory)
    return length(memory.axes[axis])
end

function Formats.format_has_vector(memory::MemoryDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(memory)
    return haskey(memory.vectors[axis], name)
end

function Formats.format_set_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    @assert Formats.has_data_write_lock(memory)
    if vector isa StorageVector
        memory.vectors[axis][name] = vector
    elseif vector == 0
        memory.vectors[axis][name] = spzeros(typeof(vector), Formats.format_axis_length(memory, axis))
    else
        memory.vectors[axis][name] = fill(vector, Formats.format_axis_length(memory, axis))
    end

    return nothing
end

function Formats.format_get_empty_dense_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
)::AbstractVector{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(memory)
    nelements = Formats.format_axis_length(memory, axis)
    vector = Vector{T}(undef, nelements)
    memory.vectors[axis][name] = vector
    return vector
end

function Formats.format_get_empty_sparse_vector!(
    memory::MemoryDaf,
    ::AbstractString,
    ::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(memory)
    nzind = Vector{I}(undef, nnz)
    nzval = Vector{T}(undef, nnz)
    return (nzind, nzval)
end

function Formats.format_filled_empty_sparse_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString,
    filled::SparseVector{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(memory)
    memory.vectors[axis][name] = filled
    return nothing
end

function Formats.format_delete_vector!(
    memory::MemoryDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(memory)
    delete!(memory.vectors[axis], name)  # NOJET
    return nothing
end

function Formats.format_vectors_set(memory::MemoryDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(memory)
    return keys(memory.vectors[axis])
end

function Formats.format_get_vector(memory::MemoryDaf, axis::AbstractString, name::AbstractString)::StorageVector
    @assert Formats.has_data_read_lock(memory)
    return memory.vectors[axis][name]
end

function Formats.format_has_matrix(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(memory)
    return haskey(memory.matrices[rows_axis][columns_axis], name)
end

function Formats.format_set_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageReal, StorageMatrix},
)::Nothing
    @assert Formats.has_data_write_lock(memory)
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

function Formats.format_get_empty_dense_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
)::AbstractMatrix{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(memory)
    nrows = Formats.format_axis_length(memory, rows_axis)
    ncols = Formats.format_axis_length(memory, columns_axis)
    matrix = Matrix{T}(undef, nrows, ncols)
    memory.matrices[rows_axis][columns_axis][name] = matrix
    return matrix
end

function Formats.format_get_empty_sparse_matrix!(
    memory::MemoryDaf,
    ::AbstractString,
    columns_axis::AbstractString,
    ::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(memory)
    ncols = Formats.format_axis_length(memory, columns_axis)
    colptr = fill(I(nnz + 1), ncols + 1)
    colptr[1] = 1
    rowval = fill(I(1), nnz)
    nzval = Vector{T}(undef, nnz)
    return (colptr, rowval, nzval)
end

function Formats.format_filled_empty_sparse_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(memory)
    memory.matrices[rows_axis][columns_axis][name] = filled
    return nothing
end

function Formats.format_relayout_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
)::StorageMatrix
    @assert Formats.has_data_write_lock(memory)
    matrix = transposer(matrix)
    Formats.format_set_matrix!(memory, columns_axis, rows_axis, name, matrix)
    return matrix
end

function Formats.format_delete_matrix!(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(memory)
    delete!(memory.matrices[rows_axis][columns_axis], name)  # NOJET
    return nothing
end

function Formats.format_matrices_set(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(memory)
    return keys(memory.matrices[rows_axis][columns_axis])
end

function Formats.format_get_matrix(
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    @assert Formats.has_data_read_lock(memory)
    return memory.matrices[rows_axis][columns_axis][name]
end

end  # module
