"""
Reordering axis entries across one or more `Daf` writers with on-disk backup for crash recovery.

Why reorder axes? In a word, *performance*. Take "for example" the genes and cell axes of single-cell RNA sequencing.
Most genes are not interesting. A small subset will typically be accessed repeatedly and the rest, possibly not at all.
If you order the gene axis such the "interesting" ones are first, then accessing column-major memory-mapped data where
genes are rows, you will bring into memory less pages than if the interesting genes were scattered randomly in their
axis. As for the cells axis - if the cells in the same metacell are adjacent in this axis, when using cells as rows, the
same logic applies.

Another use case is in training when random subsets of the data is accessed. Randomizing the axes order in advance on
disk, and then accessing contiguous ranges of entries, greatly reduces the amount of pages one brings from this. This
works even better if using a 2D chunked format. TODO: Support such formats well in `Daf`.

For large data sets, this can make a dramatic difference in performance of. This dovetails with choosing the correct
layout of the data used in analysis code. These two considerations can save (or cost) 2-3 orders of magnitude(!) in
performance. Of course, other considerations also apply - parallelism, avoiding memory allocation and garbage collection
in inner loops, vectorization (SIMD), minimizing the number of passes on the same data... But these don't hold a candle
to getting the layout of the data right - on disk and in memory.
"""
module Reorder

export FormatReorderPlan
export PlannedAxis
export PlannedMatrix
export PlannedVector
export build_reorder_plan
export reorder_axes!
export reset_reorder_axes!

using ..Formats
using ..LazySparse
using ..Readers
using ..StorageTypes
using ProgressMeter

import ..PackedFormat.fill_column!
import ProgressMeter.AbstractProgress  # NOLINT
using SparseArrays
using TanayLabUtilities
using UUIDs

struct SimulatedCrash <: Exception end

function tick_crash_counter!(crash_counter::Maybe{Ref{Int}})::Nothing
    if crash_counter === nothing
        return nothing
    end
    crash_counter[] -= 1
    if crash_counter[] <= 0
        throw(SimulatedCrash())
    end
    return nothing
end

"""
    struct PlannedAxis
        permutation::AbstractVector{<:Integer}
        inverse_permutation::AbstractVector{<:Integer}
        new_entries::AbstractVector{<:AbstractString}
    end

Per-axis data carried by a [`FormatReorderPlan`](@ref): the forward and inverse index permutations, and the
already-permuted entry list. The orchestrator computes `new_entries` once per axis and shares it across all writers, so
no format has to re-permute the axis entry names.
"""
struct PlannedAxis
    permutation::AbstractVector{<:Integer}
    inverse_permutation::AbstractVector{<:Integer}
    new_entries::AbstractVector{<:AbstractString}
end

"""
    struct PlannedVector
        axis::AbstractString
        name::AbstractString
        n_replacement_elements::Int
    end

One entry in a [`FormatReorderPlan`](@ref) identifying a vector that will be rewritten when its axis is permuted.
`n_replacement_elements` is the number of progress ticks the replacement phase will produce for this vector (the dense
length for dense and sparse vectors alike).
"""
struct PlannedVector
    axis::AbstractString
    name::AbstractString
    n_replacement_elements::Int
end

"""
    struct PlannedMatrix
        rows_axis::AbstractString
        columns_axis::AbstractString
        name::AbstractString
        n_replacement_elements::Int
    end

One entry in a [`FormatReorderPlan`](@ref) identifying a matrix that will be rewritten when one or both of its axes are
permuted. `n_replacement_elements` is the number of progress ticks the replacement phase will produce for this matrix
(`n_rows * n_columns` for dense matrices, `nnz` for sparse matrices).
"""
struct PlannedMatrix
    rows_axis::AbstractString
    columns_axis::AbstractString
    name::AbstractString
    n_replacement_elements::Int
end

"""
    struct FormatReorderPlan
        planned_axes::AbstractDict{<:AbstractString, PlannedAxis}
        planned_vectors::Vector{PlannedVector}
        planned_matrices::Vector{PlannedMatrix}
    end

Enumerates every property that will be rewritten when the given axes are permuted in a single `FormatWriter`. Produced
by [`build_reorder_plan`](@ref) and consumed by [`format_replace_reorder!`](@ref), [`format_cleanup_reorder!`](@ref),
and [`format_reset_reorder!`](@ref). The orchestrator derives replacement progress totals by summing
`n_replacement_elements` across `planned_vectors` and `planned_matrices`.
"""
struct FormatReorderPlan
    planned_axes::AbstractDict{<:AbstractString, PlannedAxis}
    planned_vectors::Vector{PlannedVector}
    planned_matrices::Vector{PlannedMatrix}
end

"""
    build_reorder_plan(
        writer::FormatWriter,
        planned_axes::AbstractDict{<:AbstractString, PlannedAxis},
    )::FormatReorderPlan

Walk `writer`'s axes, vectors, and matrices and return a [`FormatReorderPlan`](@ref) enumerating every property that
will be rewritten when the given axes are permuted. Must be called inside a write lock. Permuted axes that don't exist
in `writer` are ignored; matrices whose rows and columns axes are both unpermuted are skipped.
"""
function build_reorder_plan(
    writer::Formats.FormatWriter,
    planned_axes::AbstractDict{<:AbstractString, PlannedAxis},
)::FormatReorderPlan
    @assert Formats.has_data_write_lock(writer)

    planned_vectors = PlannedVector[]
    for axis in keys(planned_axes)
        if !Formats.format_has_axis(writer, axis; for_change = false)
            continue
        end
        n_axis_entries = Formats.format_axis_length(writer, axis)
        for name in Formats.format_vectors_set(writer, axis)
            push!(planned_vectors, PlannedVector(axis, name, n_axis_entries))
        end
    end

    planned_matrices = PlannedMatrix[]
    for permuted_axis in keys(planned_axes)
        if !Formats.format_has_axis(writer, permuted_axis; for_change = false)
            continue
        end
        for other_axis in Formats.format_axes_set(writer)
            for name in Formats.format_matrices_set(writer, permuted_axis, other_axis)
                n_elements = n_matrix_replacement_elements(writer, permuted_axis, other_axis, name)
                push!(planned_matrices, PlannedMatrix(permuted_axis, other_axis, name, n_elements))
            end
            if !haskey(planned_axes, other_axis)
                for name in Formats.format_matrices_set(writer, other_axis, permuted_axis)
                    n_elements = n_matrix_replacement_elements(writer, other_axis, permuted_axis, name)
                    push!(planned_matrices, PlannedMatrix(other_axis, permuted_axis, name, n_elements))
                end
            end
        end
    end

    return FormatReorderPlan(planned_axes, planned_vectors, planned_matrices)
end

# The number of progress ticks for replacing a matrix: its nnz if it is sparse, otherwise its number of elements. Sparse
# string matrices are read as dense, so they count all their elements.
function n_matrix_replacement_elements(
    writer::Formats.FormatWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Int
    metadata = Formats.format_matrix_metadata(writer, rows_axis, columns_axis, name)
    if metadata.is_sparse && metadata.eltype != String
        return metadata.nnz
    else
        return Formats.format_axis_length(writer, rows_axis) * Formats.format_axis_length(writer, columns_axis)
    end
end

# The length of an axis before the reorder. `planned_axis === nothing` means the axis is not permuted.
function old_axis_length(writer::Formats.FormatWriter, axis::AbstractString, planned_axis::Maybe{PlannedAxis})::Int
    if planned_axis === nothing
        return Formats.format_axis_length(writer, axis)
    else
        return length(planned_axis.permutation)
    end
end

# Allocate a permuted in-memory copy of a dense or sparse storage vector. Lazy or chunked sources (packed properties)
# are read into memory first. A permutation reads them out of order, which defeats the chunk cache.
function stage_permuted_vector(
    source::StorageVector,
    planned_axis::PlannedAxis,
    replacement_progress::Maybe{AbstractProgress},
)::StorageVector
    if source isa LazySparseVector
        source = convert(SparseVector, source)
    elseif !issparse(source) && !(source isa DenseVector)
        source = Vector(source)
    end

    if source isa SparseVector
        destination_nzind = similar(SparseArrays.nonzeroinds(source))
        destination_nzval = similar(SparseArrays.nonzeros(source))
        permute_sparse_vector!(;
            destination_nzind,
            destination_nzval,
            source,
            inverse_permutation = planned_axis.inverse_permutation,
            progress = replacement_progress,
        )
        return SparseVector(length(source), destination_nzind, destination_nzval)
    else
        destination = similar(source)
        permute_vector!(; destination, source, permutation = planned_axis.permutation, progress = replacement_progress)
        return destination
    end
end

# Allocate a permuted in-memory copy of a dense or sparse storage matrix. `planned_rows === nothing` means the rows axis
# is not permuted; likewise for `planned_columns`. At least one of the two must be non-`nothing`. Lazy or chunked
# sources (packed properties) are read into memory first. A permutation reads them out of order, which defeats the chunk
# cache, and the packed sparse writer needs whole arrays anyway.
function stage_permuted_matrix(
    source::StorageMatrix,
    planned_rows::Maybe{PlannedAxis},
    planned_columns::Maybe{PlannedAxis},
    replacement_progress::Maybe{AbstractProgress},
)::StorageMatrix
    if source isa LazySparseMatrix
        source = convert(SparseMatrixCSC, source)
    elseif !issparse(source) && !(source isa DenseMatrix)
        source = Matrix(source)
    end

    if source isa SparseMatrixCSC
        destination_colptr = similar(source.colptr)
        destination_rowval = similar(source.rowval)
        destination_nzval = similar(source.nzval)
        permute_sparse_matrix_into!(;
            destination_colptr,
            destination_rowval,
            destination_nzval,
            source_n_rows = size(source, 1),
            source_colptr = source.colptr,
            source_rowval = source.rowval,
            source_nzval = source.nzval,
            planned_rows,
            planned_columns,
            replacement_progress,
        )
        return SparseMatrixCSC(
            size(source, 1),
            size(source, 2),
            destination_colptr,
            destination_rowval,
            destination_nzval,
        )
    else
        destination = similar(source)
        permute_dense_matrix_into!(destination, source, planned_rows, planned_columns, replacement_progress)
        return destination
    end
end

# Permute a dense matrix into a destination of the same size. `planned_rows === nothing` means the rows axis is not
# permuted; likewise for `planned_columns`. At least one of the two must be non-`nothing`.
function permute_dense_matrix_into!(
    destination::AbstractMatrix,
    source::AbstractMatrix,
    planned_rows::Maybe{PlannedAxis},
    planned_columns::Maybe{PlannedAxis},
    replacement_progress::Maybe{AbstractProgress},
)::Nothing
    @assert planned_rows !== nothing || planned_columns !== nothing
    if planned_rows !== nothing && planned_columns !== nothing
        permute_dense_matrix_both!(;
            destination,
            source,
            rows_permutation = planned_rows.permutation,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    elseif planned_rows !== nothing
        permute_dense_matrix_rows!(;
            destination,
            source,
            rows_permutation = planned_rows.permutation,
            progress = replacement_progress,
        )
    else
        permute_dense_matrix_columns!(;
            destination,
            source,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

# Permute the buffers of a sparse matrix into destination buffers of the same sizes. The `nzval` buffers are `nothing`
# for a `Bool` matrix that stores no values. `planned_rows === nothing` means the rows axis is not permuted; likewise
# for `planned_columns`. At least one of the two must be non-`nothing`.
function permute_sparse_matrix_into!(;
    destination_colptr::AbstractVector{<:Integer},
    destination_rowval::AbstractVector{<:Integer},
    destination_nzval::Maybe{AbstractVector},
    source_n_rows::Integer,
    source_colptr::AbstractVector{<:Integer},
    source_rowval::AbstractVector{<:Integer},
    source_nzval::Maybe{AbstractVector},
    planned_rows::Maybe{PlannedAxis},
    planned_columns::Maybe{PlannedAxis},
    replacement_progress::Maybe{AbstractProgress},
)::Nothing
    @assert planned_rows !== nothing || planned_columns !== nothing
    if planned_rows !== nothing && planned_columns !== nothing
        permute_sparse_matrix_both_buffers!(;
            destination_colptr,
            destination_rowval,
            destination_nzval,
            source_n_rows,
            source_colptr,
            source_rowval,
            source_nzval,
            inverse_rows_permutation = planned_rows.inverse_permutation,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    elseif planned_rows !== nothing
        permute_sparse_matrix_rows_buffers!(;
            destination_colptr,
            destination_rowval,
            destination_nzval,
            source_n_rows,
            source_colptr,
            source_rowval,
            source_nzval,
            inverse_rows_permutation = planned_rows.inverse_permutation,
            progress = replacement_progress,
        )
    else
        permute_sparse_matrix_columns_buffers!(;
            destination_colptr,
            destination_rowval,
            destination_nzval,
            source_n_rows,
            source_colptr,
            source_rowval,
            source_nzval,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

# Write the permuted copy of a vector property into `writer`. The property must not exist in `writer` (its old storage
# was already removed). A vector is at most one axis long, so it is permuted in memory.
function write_permuted_vector(
    writer::Formats.FormatWriter,
    planned::PlannedVector,
    source::StorageVector,
    planned_axis::PlannedAxis,
    is_packed::Bool,
    replacement_progress::Maybe{AbstractProgress},
)::Nothing
    permuted = stage_permuted_vector(source, planned_axis, replacement_progress)
    Formats.format_set_vector!(writer, planned.axis, planned.name, permuted, is_packed)
    return nothing
end

# Write the permuted copy of a matrix property into `writer`. The property must not exist in `writer` (its old storage
# was already removed). Dense numeric matrices are streamed one column at a time. Flat sparse matrices are permuted
# directly into the new property's storage. Packed sparse matrices and string matrices are permuted in memory.
function write_permuted_matrix(
    writer::Formats.FormatWriter,
    planned::PlannedMatrix,
    source::StorageMatrix,
    planned_rows::Maybe{PlannedAxis},
    planned_columns::Maybe{PlannedAxis},
    is_packed::Bool,
    replacement_progress::Maybe{AbstractProgress},
)::Nothing
    if !issparse(source) && !(eltype(source) <: AbstractString)
        stream_permuted_dense_matrix(
            writer,
            planned,
            source,
            planned_rows,
            planned_columns,
            is_packed,
            replacement_progress,
        )
    elseif source isa SparseMatrixCSC && !is_packed
        fill_permuted_sparse_matrix(writer, planned, source, planned_rows, planned_columns, replacement_progress)
    else
        permuted = stage_permuted_matrix(source, planned_rows, planned_columns, replacement_progress)
        Formats.format_set_matrix!(writer, planned.rows_axis, planned.columns_axis, planned.name, permuted, is_packed)
    end
    return nothing
end

# Write the permuted copy of a dense matrix into `writer` one column at a time, so only one column is held in memory.
function stream_permuted_dense_matrix(
    writer::Formats.FormatWriter,
    planned::PlannedMatrix,
    source::AbstractMatrix{<:StorageReal},
    planned_rows::Maybe{PlannedAxis},
    planned_columns::Maybe{PlannedAxis},
    is_packed::Bool,
    replacement_progress::Maybe{AbstractProgress},
)::Nothing
    n_rows, n_columns = size(source)
    rows_permutation = planned_rows === nothing ? (1:n_rows) : planned_rows.permutation
    destination, _ = Formats.format_get_empty_dense_matrix!(
        writer,
        planned.rows_axis,
        planned.columns_axis,
        planned.name,
        eltype(source),
        is_packed,
    )
    for column in 1:n_columns
        source_column = planned_columns === nothing ? column : planned_columns.permutation[column]
        if source isa DenseMatrix
            source_values = view(source, :, source_column)
        else
            source_values = source[:, source_column]
        end
        fill_column!(destination, column) do destination_column
            permute_vector!(;
                destination = destination_column,
                source = source_values,
                permutation = rows_permutation,
                progress = replacement_progress,
            )
            return nothing
        end
    end
    Formats.format_filled_empty_dense_matrix!(
        writer,
        planned.rows_axis,
        planned.columns_axis,
        planned.name,
        destination,
    )
    return nothing
end

# Write the permuted copy of a sparse matrix directly into the buffers of a new flat sparse matrix property in `writer`.
function fill_permuted_sparse_matrix(
    writer::Formats.FormatWriter,
    planned::PlannedMatrix,
    source::SparseMatrixCSC,
    planned_rows::Maybe{PlannedAxis},
    planned_columns::Maybe{PlannedAxis},
    replacement_progress::Maybe{AbstractProgress},
)::Nothing
    n_rows, n_columns = size(source)
    destination_colptr, destination_rowval, destination_nzval, _ = Formats.format_get_empty_sparse_matrix!(
        writer,
        planned.rows_axis,
        planned.columns_axis,
        planned.name,
        eltype(source),
        nnz(source),
        eltype(source.colptr),
        false,
    )
    permute_sparse_matrix_into!(;
        destination_colptr,
        destination_rowval,
        destination_nzval,
        source_n_rows = n_rows,
        source_colptr = source.colptr,
        source_rowval = source.rowval,
        source_nzval = source.nzval,
        planned_rows,
        planned_columns,
        replacement_progress,
    )
    Formats.format_filled_empty_sparse_matrix!(
        writer,
        planned.rows_axis,
        planned.columns_axis,
        planned.name,
        SparseMatrixCSC(n_rows, n_columns, destination_colptr, destination_rowval, destination_nzval),
    )
    return nothing
end

"""
    format_lock_reorder!(writer::FormatWriter, operation_id::AbstractString)::Nothing

Fast, atomic: claim the reorder lock on `writer` so subsequent [`build_reorder_plan`](@ref),
[`format_replace_reorder!`](@ref), and [`format_cleanup_reorder!`](@ref) calls have exclusive rights to the reorder
backup state. Must be called inside a write lock, and only when [`format_has_reorder_lock`](@ref) returns `false`.

`operation_id` is an opaque token (typically a UUID) generated once per reorder batch by the orchestrator. For formats
where multiple writers can share the same backing store (e.g. H5df), the implementation verifies that any pre-existing
lock entries carry the *same* `operation_id`; if a foreign operation is detected, an error is raised. This prevents two
independent reorders from silently interfering with each other's backup state.
"""
function format_lock_reorder! end

"""
    format_backup_reorder!(writer::FormatWriter, plan::FormatReorderPlan)::Nothing

Fast: save a backup of every property listed in `plan` so that [`format_reset_reorder!`](@ref) can
restore the pre-reorder state. Must be called after [`format_lock_reorder!`](@ref) and
[`build_reorder_plan`](@ref) but before [`format_replace_reorder!`](@ref).
"""
function format_backup_reorder! end

"""
    format_replace_reorder!(
        writer::FormatWriter,
        plan::FormatReorderPlan,
        replacement_progress::Maybe{AbstractProgress},
        crash_counter::Maybe{Ref{Int}},
    )::Nothing

Replace the live data in `writer` with the reordered versions described by `plan`, ticking
`replacement_progress` as work completes. Must be called after [`format_backup_reorder!`](@ref).
If `crash_counter` is not `nothing`, it is decremented after each property replacement and a
`SimulatedCrash` is thrown when it reaches zero (used for testing crash recovery).
"""
function format_replace_reorder! end

"""
    format_cleanup_reorder!(writer::FormatWriter)::Nothing

Remove the backup state created by previous phases. Must be called after
[`format_replace_reorder!`](@ref).
"""
function format_cleanup_reorder! end

"""
    format_has_reorder_lock(writer::FormatWriter)::Bool

Return `true` if and only if a recovery marker from a previously-crashed reorder exists on `writer`.
"""
function format_has_reorder_lock end

"""
    format_reset_reorder!(writer::FormatWriter)::Bool

Roll any pending reorder back to the pre-reorder state. Returns `true` if and only if work was done.
"""
function format_reset_reorder! end

"""
    reorder_axes!(
        daf::DafWriter,
        axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}},
    )::Nothing

    reorder_axes!(
        dafs::AbstractVector{<:DafWriter},
        axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}},
    )::Nothing

Reorder the entries of one or more axes in one or more leaf `DafWriter`s. Each value in `axes_permutations` is a
permutation vector: `new_entries[i] = old_entries[permutation[i]]`. All writers that contain a permuted axis must have
identical entry lists for that axis.

The operation is crash-safe: a backup is created before any data is modified, and a lock marker is written so that a
subsequent call can detect and roll back a partially-applied reorder.

When multiple writers share the same backing store (e.g. several H5df groups in one HDF5 file), pass them all in a
single call so that the backup and lock are coordinated correctly.
"""
function reorder_axes!(
    daf::Formats.DafWriter,
    axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}};
    _simulate_crash::Maybe{Integer} = nothing,
)::Nothing
    return reorder_axes!(Formats.DafWriter[daf], axes_permutations; _simulate_crash)
end

function reorder_axes!(
    dafs::AbstractVector{<:Formats.DafWriter},
    axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}};
    _simulate_crash::Maybe{Integer} = nothing,
)::Nothing
    if isempty(dafs) || isempty(axes_permutations)
        return nothing
    end

    for daf in dafs
        if !Readers.is_leaf(daf)
            error(chomp("""
                        non-leaf type: $(nameof(typeof(daf)))
                        for the daf data: $(daf.name)
                        given to reorder_axes!
                        """))
        end
    end

    sorted_dafs = sort(collect(dafs); by = reorder_sort_key)
    operation_id = string(uuid4())

    n_locked = 0
    try
        for daf in sorted_dafs
            Formats.begin_data_write_lock(daf, "reorder_axes!")
            n_locked += 1
            @assert !format_has_reorder_lock(daf) "stale reorder lock on: $(daf.name)"
            format_lock_reorder!(daf, operation_id)
        end
    catch
        for index in n_locked:-1:1
            Formats.end_data_write_lock(sorted_dafs[index], "reorder_axes!")
        end
        rethrow()
    end

    try
        reorder_axes_locked!(sorted_dafs, axes_permutations, _simulate_crash)
    finally
        for daf in reverse(sorted_dafs)
            Formats.end_data_write_lock(daf, "reorder_axes!")
        end
    end

    return nothing
end

function reorder_sort_key(daf::Formats.DafWriter)
    path = Readers.complete_path(daf)
    if path !== nothing
        return (0, path)
    end
    return (1, string(objectid(daf)))
end

function reorder_axes_locked!(
    sorted_dafs::AbstractVector{<:Formats.DafWriter},
    axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}},
    _simulate_crash::Maybe{Integer},
)::Nothing
    planned_axes = compute_planned_axes(sorted_dafs, axes_permutations)
    plans = FormatReorderPlan[build_reorder_plan(daf, planned_axes) for daf in sorted_dafs]

    for (daf, plan) in zip(sorted_dafs, plans)
        format_backup_reorder!(daf, plan)
    end

    total_elements = 0
    for plan in plans
        for planned in plan.planned_vectors
            total_elements += planned.n_replacement_elements
        end
        for planned in plan.planned_matrices
            total_elements += planned.n_replacement_elements
        end
    end

    crash_counter = _simulate_crash === nothing ? nothing : Ref{Int}(Int(_simulate_crash))

    replacement_progress = DebugProgress(total_elements; group = :daf_loops, desc = "reorder_axes")
    for (daf, plan) in zip(sorted_dafs, plans)
        format_replace_reorder!(daf, plan, replacement_progress, crash_counter)
        invalidate_reorder_caches!(daf, plan)
    end

    for daf in sorted_dafs
        format_cleanup_reorder!(daf)
    end

    return nothing
end

function compute_planned_axes(
    sorted_dafs::AbstractVector{<:Formats.DafWriter},
    axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}},
)::Dict{String, PlannedAxis}
    planned_axes = Dict{String, PlannedAxis}()

    for (axis, permutation) in axes_permutations
        entries = nothing
        for daf in sorted_dafs
            if !Formats.format_has_axis(daf, axis; for_change = false)
                continue
            end
            daf_entries = Formats.get_axis_vector_through_cache(daf, axis)
            if entries === nothing
                entries = daf_entries
                if length(permutation) != length(entries)
                    error(chomp("""
                          permutation length: $(length(permutation))
                          does not match axis: $(axis)
                          length: $(length(entries))
                          in the daf data: $(daf.name)
                          """))
                end
            elseif daf_entries != entries
                error(chomp("""
                            axis: $(axis) entries differ
                            between the daf data: $(sorted_dafs[1].name)
                            and the daf data: $(daf.name)
                            """))
            end
        end
        if entries === nothing
            error("axis: $(axis)\ndoes not exist in any of the writers")
        end

        inverse_permutation = invperm(permutation)
        new_entries = entries[permutation]
        planned_axes[axis] = PlannedAxis(permutation, inverse_permutation, new_entries)
    end

    return planned_axes
end

"""
    reset_reorder_axes!(
        daf::DafWriter,
    )::Bool

    reset_reorder_axes!(
        dafs::AbstractVector{<:DafWriter},
    )::Bool

Roll back a partially-applied reorder on one or more leaf `DafWriter`s. This is the **only** way to recover from a
crash during [`reorder_axes!`](@ref). Returns `true` if any writer had a pending reorder that was rolled back.
"""
function reset_reorder_axes!(daf::Formats.DafWriter)::Bool
    return reset_reorder_axes!(Formats.DafWriter[daf])
end

function reset_reorder_axes!(dafs::AbstractVector{<:Formats.DafWriter})::Bool
    if isempty(dafs)
        return false  # UNTESTED
    end

    for daf in dafs
        if !Readers.is_leaf(daf)
            error(chomp("""  # UNTESTED
                        non-leaf type: $(nameof(typeof(daf)))
                        for the daf data: $(daf.name)
                        given to reset_reorder_axes!
                        """))
        end
    end

    sorted_dafs = sort(collect(dafs); by = reorder_sort_key)

    for daf in sorted_dafs
        Formats.begin_data_write_lock(daf, "reset_reorder_axes!")
    end

    did_work = false
    try
        for daf in sorted_dafs
            if format_reset_reorder!(daf)
                did_work = true
                for axis in Formats.format_axes_set(daf)
                    Formats.invalidate_axis_data!(daf, axis)
                end
            end
        end
    finally
        for daf in reverse(sorted_dafs)
            Formats.end_data_write_lock(daf, "reset_reorder_axes!")
        end
    end

    return did_work
end

function invalidate_reorder_caches!(writer::Formats.FormatWriter, plan::FormatReorderPlan)::Nothing
    for axis in keys(plan.planned_axes)
        if Formats.format_has_axis(writer, axis; for_change = false)
            Formats.invalidate_axis_data!(writer, axis)
        end
    end
    return nothing
end

end  # module
