"""
In-memory `Daf` storage format.
"""
module MemoryFormat

export MemoryDaf

using ..Formats
using ..Keys
using ..Readers
using ..Reorder
using ..StorageTypes
using ..Writers
using NamedArrays
using ProgressMeter
using SparseArrays
using TanayLabUtilities

import ..Formats
import ..Formats.Internal
import ..Reorder

# Per-operation backup for `reorder_axes!` on a `MemoryDaf`. Saves old data before replacement;
# `nothing` outside a pending reorder.
mutable struct MemoryReorderBackup
    old_axes::Dict{AxisKey, AbstractVector{<:AbstractString}}  # NOJET
    old_vectors::Dict{VectorKey, StorageVector}
    old_matrices::Dict{MatrixKey, StorageMatrix}
end

"""
    struct MemoryDaf <: DafWriter ... end

    MemoryDaf(; name = "memory")

Simple in-memory storage.

This just keeps everything in-memory, similarly to the way an `AnnData` object works; that is, this is a lightweight
object that just keeps references to the data it is given.

This is the "default" storage type you should use, unless you need to persist the data on the disk.
"""
mutable struct MemoryDaf <: DafWriter
    name::AbstractString
    internal::Internal
    scalars::Dict{AbstractString, StorageScalar}
    axes::Dict{AbstractString, AbstractVector{<:AbstractString}}
    vectors::Dict{AbstractString, Dict{AbstractString, StorageVector}}
    matrices::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, StorageMatrix}}}
    reorder_backup::Maybe{MemoryReorderBackup}
end

function MemoryDaf(; name::AbstractString = "memory")::MemoryDaf
    scalars = Dict{AbstractString, StorageScalar}()
    axes = Dict{AbstractString, AbstractVector{<:AbstractString}}()
    vectors = Dict{AbstractString, Dict{AbstractString, StorageVector}}()
    matrices = Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, StorageMatrix}}}()
    name = unique_name(name)
    memory =
        MemoryDaf(name, Internal(; cache_group = nothing, is_frozen = false), scalars, axes, vectors, matrices, nothing)
    @debug "Daf: $(brief(memory))" _group = :daf_repose
    return memory
end

function Readers.is_leaf(::MemoryDaf)::Bool  # FLAKY TESTED
    return true
end

function Readers.is_leaf(::Type{MemoryDaf})::Bool  # FLAKY TESTED
    return true
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
    matrix::Union{StorageScalarBase, StorageMatrix},
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
    matrix = flipped(matrix)
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

# Allocate a permuted copy of a dense or sparse storage vector. The caller passes both the forward and the cached
# inverse permutation so sparse and dense branches can each pick the one their kernel expects.
function stage_permuted_vector(
    source::StorageVector,
    permutation::AbstractVector{<:Integer},
    inverse_permutation::AbstractVector{<:Integer},
    replacement_progress::Maybe{Progress},
)::StorageVector
    if source isa SparseVector
        destination_nzind = similar(SparseArrays.nonzeroinds(source))
        destination_nzval = similar(SparseArrays.nonzeros(source))
        permute_sparse_vector!(;
            destination_nzind,
            destination_nzval,
            source,
            inverse_permutation,
            progress = replacement_progress,
        )
        return SparseVector(length(source), destination_nzind, destination_nzval)
    else
        destination = similar(source)
        permute_vector!(; destination, source, permutation, progress = replacement_progress)
        return destination
    end
end

# Allocate a permuted copy of a dense or sparse storage matrix, dispatching on which sides of the matrix are being
# reordered. `rows_permutation === nothing` means the rows axis is not permuted; likewise for `columns_permutation`.
# At least one of the two must be non-`nothing` — matrices untouched by the reorder are never staged.
function stage_permuted_matrix(
    source::StorageMatrix,
    rows_permutation::Maybe{AbstractVector{<:Integer}},
    inverse_rows_permutation::Maybe{AbstractVector{<:Integer}},
    columns_permutation::Maybe{AbstractVector{<:Integer}},
    replacement_progress::Maybe{Progress},
)::StorageMatrix
    @assert rows_permutation !== nothing || columns_permutation !== nothing
    if source isa SparseMatrixCSC
        destination_colptr = similar(source.colptr)
        destination_rowval = similar(source.rowval)
        destination_nzval = similar(source.nzval)
        if rows_permutation !== nothing && columns_permutation !== nothing
            permute_sparse_matrix_both!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source,
                inverse_rows_permutation,
                columns_permutation,
                progress = replacement_progress,
            )
        elseif rows_permutation !== nothing
            permute_sparse_matrix_rows!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source,
                inverse_rows_permutation,
                progress = replacement_progress,
            )
        else
            permute_sparse_matrix_columns!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source,
                columns_permutation,
                progress = replacement_progress,
            )
        end
        return SparseMatrixCSC(
            size(source, 1),
            size(source, 2),
            destination_colptr,
            destination_rowval,
            destination_nzval,
        )
    else
        destination = similar(source)
        if rows_permutation !== nothing && columns_permutation !== nothing
            permute_dense_matrix_both!(;
                destination,
                source,
                rows_permutation,
                columns_permutation,
                progress = replacement_progress,
            )
        elseif rows_permutation !== nothing
            permute_dense_matrix_rows!(; destination, source, rows_permutation, progress = replacement_progress)
        else
            permute_dense_matrix_columns!(; destination, source, columns_permutation, progress = replacement_progress)
        end
        return destination
    end
end

function Reorder.format_lock_reorder!(memory::MemoryDaf, ::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(memory)
    @assert memory.reorder_backup === nothing
    memory.reorder_backup = MemoryReorderBackup(
        Dict{AxisKey, AbstractVector{<:AbstractString}}(),
        Dict{VectorKey, StorageVector}(),
        Dict{MatrixKey, StorageMatrix}(),
    )
    return nothing
end

function Reorder.format_backup_reorder!(memory::MemoryDaf, plan::Reorder.FormatReorderPlan)::Nothing
    @assert Formats.has_data_write_lock(memory)
    backup = memory.reorder_backup
    @assert backup !== nothing

    for (axis, _) in plan.planned_axes
        if haskey(memory.axes, axis)
            backup.old_axes[axis] = memory.axes[axis]
        end
    end
    for planned in plan.planned_vectors
        backup.old_vectors[(planned.axis, planned.name)] = memory.vectors[planned.axis][planned.name]
    end
    for planned in plan.planned_matrices
        backup.old_matrices[(planned.rows_axis, planned.columns_axis, planned.name)] =
            memory.matrices[planned.rows_axis][planned.columns_axis][planned.name]
    end
    return nothing
end

function Reorder.format_replace_reorder!(
    memory::MemoryDaf,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    crash_counter::Maybe{Ref{Int}},
)::Nothing
    @assert Formats.has_data_write_lock(memory)
    @assert memory.reorder_backup !== nothing

    for (axis, planned_axis) in plan.planned_axes
        if haskey(memory.axes, axis)
            memory.axes[axis] = planned_axis.new_entries
        end
    end

    for planned in plan.planned_vectors
        source_vector = memory.reorder_backup.old_vectors[(planned.axis, planned.name)]
        planned_axis = plan.planned_axes[planned.axis]
        memory.vectors[planned.axis][planned.name] = stage_permuted_vector(
            source_vector,
            planned_axis.permutation,
            planned_axis.inverse_permutation,
            replacement_progress,
        )
        Reorder.tick_crash_counter!(crash_counter)
    end

    for planned in plan.planned_matrices
        source_matrix = memory.reorder_backup.old_matrices[(planned.rows_axis, planned.columns_axis, planned.name)]
        planned_rows_axis = get(plan.planned_axes, planned.rows_axis, nothing)
        planned_columns_axis = get(plan.planned_axes, planned.columns_axis, nothing)
        rows_permutation = planned_rows_axis === nothing ? nothing : planned_rows_axis.permutation
        inverse_rows_permutation = planned_rows_axis === nothing ? nothing : planned_rows_axis.inverse_permutation
        columns_permutation = planned_columns_axis === nothing ? nothing : planned_columns_axis.permutation
        memory.matrices[planned.rows_axis][planned.columns_axis][planned.name] = stage_permuted_matrix(
            source_matrix,
            rows_permutation,
            inverse_rows_permutation,
            columns_permutation,
            replacement_progress,
        )
        Reorder.tick_crash_counter!(crash_counter)
    end

    return nothing
end

function Reorder.format_cleanup_reorder!(memory::MemoryDaf)::Nothing  # FLAKY TESTED
    @assert Formats.has_data_write_lock(memory)
    @assert memory.reorder_backup !== nothing
    memory.reorder_backup = nothing
    return nothing
end

function Reorder.format_has_reorder_lock(memory::MemoryDaf)::Bool  # FLAKY TESTED
    @assert Formats.has_data_write_lock(memory)
    return memory.reorder_backup !== nothing
end

function Reorder.format_reset_reorder!(memory::MemoryDaf)::Bool
    @assert Formats.has_data_write_lock(memory)
    backup = memory.reorder_backup
    if backup === nothing
        return false
    end
    for (axis, old_entries) in backup.old_axes
        memory.axes[axis] = old_entries
    end
    for ((axis, name), old_vector) in backup.old_vectors
        memory.vectors[axis][name] = old_vector
    end
    for ((rows_axis, columns_axis, name), old_matrix) in backup.old_matrices
        memory.matrices[rows_axis][columns_axis][name] = old_matrix
    end
    memory.reorder_backup = nothing
    return true
end

end  # module
