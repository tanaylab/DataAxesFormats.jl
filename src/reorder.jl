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
using ..Readers
using ..StorageTypes
using SparseArrays
using TanayLabUtilities
using UUIDs

struct SimulatedCrash <: Exception end

function tick_crash_counter!(crash_counter::Maybe{Ref{Int}})::Nothing  # FLAKY TESTED
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
permuted. `n_replacement_elements` is the number of progress ticks the replacement phase will produce for this matrix (`n_rows * n_columns` for dense matrices, `nnz` for sparse matrices).
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

Enumerates every property that will be rewritten when the given axes are permuted in a single `FormatWriter`.
Produced by [`build_reorder_plan`](@ref) and consumed by [`format_replace_reorder!`](@ref),
[`format_cleanup_reorder!`](@ref), and [`format_reset_reorder!`](@ref). The orchestrator derives replacement progress totals by summing `n_replacement_elements` across `planned_vectors` and
`planned_matrices`.
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
        for name in Formats.format_vectors_set(writer, axis)
            vector, _, _ = Formats.format_get_vector(writer, axis, name)
            push!(planned_vectors, PlannedVector(axis, name, length(vector)))
        end
    end

    planned_matrices = PlannedMatrix[]
    for permuted_axis in keys(planned_axes)
        if !Formats.format_has_axis(writer, permuted_axis; for_change = false)
            continue
        end
        for other_axis in Formats.format_axes_set(writer)
            for name in Formats.format_matrices_set(writer, permuted_axis, other_axis)
                matrix, _, _ = Formats.format_get_matrix(writer, permuted_axis, other_axis, name)
                n_elements = matrix isa SparseMatrixCSC ? nnz(matrix) : length(matrix)
                push!(planned_matrices, PlannedMatrix(permuted_axis, other_axis, name, n_elements))
            end
            if !haskey(planned_axes, other_axis)
                for name in Formats.format_matrices_set(writer, other_axis, permuted_axis)
                    matrix, _, _ = Formats.format_get_matrix(writer, other_axis, permuted_axis, name)
                    n_elements = matrix isa SparseMatrixCSC ? nnz(matrix) : length(matrix)
                    push!(planned_matrices, PlannedMatrix(other_axis, permuted_axis, name, n_elements))
                end
            end
        end
    end

    return FormatReorderPlan(planned_axes, planned_vectors, planned_matrices)
end

"""
    format_lock_reorder!(writer::FormatWriter, operation_id::AbstractString)::Nothing

Fast, atomic: claim the reorder lock on `writer` so subsequent [`build_reorder_plan`](@ref),
[`format_replace_reorder!`](@ref), and [`format_cleanup_reorder!`](@ref) calls have exclusive rights to the reorder backup
state. Must be called inside a write lock, and only when [`format_has_reorder_lock`](@ref) returns `false`.

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
        replacement_progress::Maybe{Progress},
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

When multiple writers share the same backing store (e.g. several H5df groups in one HDF5 file), pass them all in a single
call so that the backup and lock are coordinated correctly.
"""
function reorder_axes!(  # FLAKY TESTED
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
    catch  # FLAKY TESTED
        for i in n_locked:-1:1  # UNTESTED
            Formats.end_data_write_lock(sorted_dafs[i], "reorder_axes!")  # UNTESTED
        end
        rethrow()  # UNTESTED
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

function reorder_sort_key(daf::Formats.DafWriter)  # FLAKY TESTED
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
function reset_reorder_axes!(daf::Formats.DafWriter)::Bool  # FLAKY TESTED
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
