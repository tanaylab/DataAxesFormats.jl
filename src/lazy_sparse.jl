"""
Lazy slice-then-materialize wrapper around a packed sparse matrix.

A [`LazySparseMatrix`](@ref) holds the `colptr` of a sparse matrix in memory plus the `rowval` and `nzval` as packed
[`Zarr`](https://github.com/JuliaIO/Zarr.jl) `ZArray` sources, so a packed sparse property opened from disk does not pay
the decompression cost up front. Slicing the wrapper accumulates row / column [`SparseSelection`](@ref)s without
touching the chunked storage; materialisation runs only when downstream code asks for concrete `rowval` / `nzval` data,
and runs only on the selected slice.

This module covers the type and the [`SparseSelection`](@ref) family. Slicing semantics and materialisation land in
later phases of the packed-storage roadmap.
"""
module LazySparse

export AllOf
export IndicesOf
export LazySparseMatrix
export MaskOf
export RangeOf
export SparseSelection

using SparseArrays
using TanayLabUtilities
using Zarr

import SparseArrays.AbstractSparseMatrixCSC  # NOLINT

"""
    abstract type SparseSelection end

The kind of slice exposed through a [`LazySparseMatrix`](@ref) along one of the original sparse matrix axes. Concrete
subtypes:

  - [`AllOf`](@ref) — every entry of the original axis.
  - [`RangeOf`](@ref) — a contiguous range of original indices.
  - [`IndicesOf`](@ref) — an explicit ordered vector of original indices.
  - [`MaskOf`](@ref) — entries selected by a boolean mask.

Each subtype implements [`Base.length`](https://docs.julialang.org/en/v1/base/collections/#Base.length).
"""
abstract type SparseSelection end

"""
    struct AllOf <: SparseSelection
        n_entries::Int
    end

A [`SparseSelection`](@ref) that exposes every entry of the original axis (the axis has `n_entries` entries).
"""
struct AllOf <: SparseSelection
    n_entries::Int
end

function Base.length(selection::AllOf)::Int
    return selection.n_entries
end

"""
    struct RangeOf <: SparseSelection
        range::UnitRange{Int}
    end

A [`SparseSelection`](@ref) that exposes a contiguous range of entries from the original axis.
"""
struct RangeOf <: SparseSelection
    range::UnitRange{Int}
end

function Base.length(selection::RangeOf)::Int
    return length(selection.range)
end

"""
    struct IndicesOf <: SparseSelection
        indices::Vector{Int}
    end

A [`SparseSelection`](@ref) that exposes an explicit ordered vector of original-axis indices.
"""
struct IndicesOf <: SparseSelection
    indices::Vector{Int}
end

function Base.length(selection::IndicesOf)::Int
    return length(selection.indices)
end

"""
    struct MaskOf <: SparseSelection
        mask::BitVector
    end

A [`SparseSelection`](@ref) that exposes the entries selected by a boolean mask over the original axis.
"""
struct MaskOf <: SparseSelection
    mask::BitVector
end

function Base.length(selection::MaskOf)::Int
    return count(selection.mask)
end

"""
    mutable struct LazySparseMatrix{Tv, Ti <: Integer} <: AbstractSparseMatrixCSC{Tv, Ti}
        full_n_rows::Int
        full_n_columns::Int
        full_colptr::Vector{Ti}
        rowval_source::AbstractVector{Ti}
        nzval_source::AbstractVector{Tv}
        row_select::SparseSelection
        column_select::SparseSelection
        materialized::Maybe{SparseMatrixCSC{Tv, Ti}}
    end

Lazy [`AbstractSparseMatrixCSC{Tv, Ti}`](https://docs.julialang.org/en/v1/stdlib/SparseArrays/#SparseArrays.AbstractSparseMatrixCSC)
over packed `rowval` / `nzval` sources.

Holds the original `colptr` in memory (small — `sizeof(Ti) × (full_n_columns + 1)` bytes) and the `rowval` / `nzval`
as one-dimensional indexable sources (typically `Zarr.ZArray{T, 1}` or a `DiskArrays.cache`-wrapped `ZArray`,
with `Vector{T}` accepted for flat mmap'd components in mixed flat/packed properties) that decompress per-chunk on
access. `row_select` and `column_select` describe the slice of the original axes exposed through this wrapper.
Materialisation copies the selected slice into a plain
[`SparseMatrixCSC`](https://docs.julialang.org/en/v1/stdlib/SparseArrays/#SparseArrays.SparseMatrixCSC) the first time
it is required and caches it in `materialized` for subsequent use.

The `full_n_rows` / `full_n_columns` / `full_colptr` fields describe the original (unsliced) matrix. The corresponding
`AbstractSparseMatrixCSC` accessors (`size`, `SparseArrays.getcolptr`) report the *sliced* shape — `getcolptr` triggers
materialisation so it can return the per-slice colptr. Use the `full_*` fields when internal Daf code needs the
original layout without materialising.

# Materialisation triggers

The following operations materialise the current slice into the cached `materialized` matrix on first call (and reuse
the cache on subsequent calls):

  - `SparseArrays.rowvals` / `SparseArrays.nonzeros` / `SparseArrays.getcolptr` / `SparseArrays.nnz`.
  - `SparseMatrixCSC(lazy)` and `convert(SparseMatrixCSC{Tv, Ti}, lazy)`.
  - Any generic `AbstractSparseMatrixCSC` / `AbstractMatrix` algorithm that calls those primitives (e.g.
    `Matrix(lazy)`, multiplication, the `LinearAlgebra` operations).

The following operations do **not** materialise:

  - `Base.size`, `Base.eltype`, `Base.length`.
  - The four slicing forms `lazy[:, range]` / `lazy[range, :]` / `lazy[:, indices]` / `lazy[mask, :]` (each rebinds
    selections and clears the cache, returning a fresh wrapper).
  - `Base.getindex(lazy, ::Int, ::Int)` — the only access form that decompresses on the fly without populating the
    cache: each call reads the column's `rowval` chunk and one element of `nzval`. Repeated calls on the same column
    are amortised by the `DiskArrays.cache` wrapper that the read paths place over the packed sources.

User code obtains a `LazySparseMatrix` only as the result of reading a packed sparse property through
[`DataAxesFormats.Formats.format_get_matrix`](@ref); construction lives behind the read paths and is not part of the
public API.
"""
mutable struct LazySparseMatrix{Tv, Ti <: Integer} <: AbstractSparseMatrixCSC{Tv, Ti}
    full_n_rows::Int
    full_n_columns::Int
    full_colptr::Vector{Ti}
    rowval_source::AbstractVector{Ti}
    nzval_source::AbstractVector{Tv}
    row_select::SparseSelection
    column_select::SparseSelection
    materialized::Maybe{SparseMatrixCSC{Tv, Ti}}
end

# Construct a `LazySparseMatrix` exposing the full original matrix, with both selections set to `AllOf`. The
# `full_colptr` vector is taken by reference; callers that intend to share it should `copy` first. `n_rows` is the
# original row count; `length(full_colptr) - 1` is the original column count.
function LazySparseMatrix(
    n_rows::Integer,
    full_colptr::Vector{Ti},
    rowval_source::AbstractVector{Ti},
    nzval_source::AbstractVector{Tv},
)::LazySparseMatrix{Tv, Ti} where {Tv, Ti <: Integer}
    full_n_rows = Int(n_rows)
    full_n_columns = length(full_colptr) - 1
    return LazySparseMatrix{Tv, Ti}(
        full_n_rows,
        full_n_columns,
        full_colptr,
        rowval_source,
        nzval_source,
        AllOf(full_n_rows),
        AllOf(full_n_columns),
        nothing,
    )
end

function Base.size(matrix::LazySparseMatrix)::Tuple{Int, Int}
    return (length(matrix.row_select), length(matrix.column_select))
end

# Compose `new` (a selection of the slice that `existing` exposes) with `existing` (a selection of the original axis),
# producing a [`SparseSelection`](@ref) over the original axis that picks the same entries `new` picks over the
# existing slice. An `AllOf` on either side collapses; `RangeOf ∘ RangeOf` stays a `RangeOf`; `MaskOf ∘ MaskOf` stays
# a `MaskOf` (the original axis length is `length(existing.mask)`); every other combination falls back to `IndicesOf`.
function compose(::AllOf, existing::SparseSelection)::SparseSelection
    return existing
end

function compose(new::SparseSelection, ::AllOf)::SparseSelection
    return new
end

# Resolve the `compose(::AllOf, ::AllOf)` method ambiguity in favour of returning `new` (semantically equivalent to
# returning `existing` because both are `AllOf` over the same axis).
function compose(new::AllOf, ::AllOf)::SparseSelection
    return new
end

function compose(new::RangeOf, existing::RangeOf)::RangeOf
    @assert checkbounds(Bool, existing.range, new.range)
    return RangeOf(existing.range[new.range])
end

function compose(new::RangeOf, existing::IndicesOf)::IndicesOf
    @assert checkbounds(Bool, existing.indices, new.range)
    return IndicesOf(existing.indices[new.range])
end

function compose(new::RangeOf, existing::MaskOf)::IndicesOf
    selected_originals = findall(existing.mask)
    @assert checkbounds(Bool, selected_originals, new.range)
    return IndicesOf(selected_originals[new.range])
end

function compose(new::IndicesOf, existing::RangeOf)::IndicesOf
    @assert checkbounds(Bool, existing.range, new.indices)
    return IndicesOf(collect(Int, existing.range[new.indices]))
end

function compose(new::IndicesOf, existing::IndicesOf)::IndicesOf
    @assert checkbounds(Bool, existing.indices, new.indices)
    return IndicesOf(existing.indices[new.indices])
end

function compose(new::IndicesOf, existing::MaskOf)::IndicesOf
    selected_originals = findall(existing.mask)
    @assert checkbounds(Bool, selected_originals, new.indices)
    return IndicesOf(selected_originals[new.indices])
end

function compose(new::MaskOf, existing::RangeOf)::IndicesOf
    @assert length(new.mask) == length(existing.range)
    return IndicesOf([existing.range[i] for i in eachindex(new.mask) if new.mask[i]])
end

function compose(new::MaskOf, existing::IndicesOf)::IndicesOf
    @assert length(new.mask) == length(existing.indices)
    return IndicesOf(existing.indices[new.mask])
end

# `MaskOf ∘ MaskOf` stays a `MaskOf` over the original axis: each true bit in `existing.mask` is enabled by the
# corresponding bit of `new.mask` (in 1..count(existing.mask) order), disabled otherwise.
function compose(new::MaskOf, existing::MaskOf)::MaskOf
    @assert length(new.mask) == count(existing.mask)
    composed_mask = falses(length(existing.mask))
    new_position = 0
    for original_position in eachindex(existing.mask)
        if existing.mask[original_position]
            new_position += 1
            if new.mask[new_position]
                composed_mask[original_position] = true
            end
        end
    end
    return MaskOf(composed_mask)
end

# Fresh `LazySparseMatrix` with new selections and the rest of the fields shared by reference. The `materialized` cache
# is reset because the cached `SparseMatrixCSC` is keyed to the prior selections.
function with_selections(  # FLAKY TESTED
    matrix::LazySparseMatrix{Tv, Ti},
    row_select::SparseSelection,
    column_select::SparseSelection,
)::LazySparseMatrix{Tv, Ti} where {Tv, Ti}
    return LazySparseMatrix{Tv, Ti}(
        matrix.full_n_rows,
        matrix.full_n_columns,
        matrix.full_colptr,
        matrix.rowval_source,
        matrix.nzval_source,
        row_select,
        column_select,
        nothing,
    )
end

# Translate one axis indexer into a [`SparseSelection`](@ref) over the parent axis the wrapper currently exposes
# (`length(parent_select)` entries). The `BitVector` / `AbstractVector{Bool}` overloads come before the
# `AbstractVector{<:Integer}` one because `Bool <: Integer`. `Colon` has no overload here — the slicing methods
# below short-circuit on `Colon` and reuse `matrix.row_select` / `matrix.column_select` directly.
function selection_from_indexer(indexer::AbstractUnitRange{<:Integer})::RangeOf
    return RangeOf(Int(first(indexer)):Int(last(indexer)))
end

function selection_from_indexer(indexer::BitVector)::MaskOf
    return MaskOf(indexer)
end

function selection_from_indexer(indexer::AbstractVector{Bool})::MaskOf
    return MaskOf(BitVector(indexer))  # NOJET
end

function selection_from_indexer(indexer::AbstractVector{<:Integer})::IndicesOf
    return IndicesOf(Vector{Int}(indexer))
end

# Slicing through a `LazySparseMatrix` rebinds row / column selections without touching the chunked sources. Each
# axis accepts `Colon`, `AbstractUnitRange{<:Integer}`, `AbstractVector{<:Integer}`, and `AbstractVector{Bool}`
# (including `BitVector`). Four explicit two-argument methods cover the `Colon` / non-`Colon` combinations to
# disambiguate against `SparseArrays`'s `getindex(::AbstractSparseMatrixCSC, ::Colon, i)` /
# `getindex(::AbstractSparseMatrixCSC, i, ::Colon)` / `getindex(::AbstractSparseMatrixCSC, ::Colon, ::Colon)`.
# The same dispatch covers direct slicing (`lazy[:, 2:3]`) and slicing through the
# [`NamedArray`](https://github.com/davidavdav/NamedArrays.jl) wrapper that `get_matrix` returns (which converts
# `Colon` to `collect(1:length)` before delegating to the parent).
const NonColonIndexer = Union{AbstractRange{<:Integer}, AbstractVector{<:Integer}, AbstractVector{Bool}}

function Base.getindex(matrix::LazySparseMatrix, ::Colon, ::Colon)::LazySparseMatrix
    return with_selections(matrix, matrix.row_select, matrix.column_select)
end

function Base.getindex(matrix::LazySparseMatrix, rows::NonColonIndexer, columns::NonColonIndexer)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

# `lazy[:, i]` and `lazy[i, :]` are not given explicit methods: `SparseArrays`'s
# `getindex(::AbstractSparseMatrixCSC, ::Colon, i) = getindex(A, axes(A, 1), i)` (and its symmetric form for the row
# axis) recurses with `axes(A, *)::OneTo`, which then dispatches to the `(NonColonIndexer, NonColonIndexer)` method
# above (or one of the disambiguation overrides below) and preserves the lazy slicing semantics. Adding a direct
# `(::Colon, ::NonColonIndexer)` method would just save one dispatch hop with no semantic change.

# Shared body for the disambiguation overrides below and for the general `(NonColonIndexer, NonColonIndexer)` method
# above.
function slice_with_indexers(matrix::LazySparseMatrix, rows, columns)::LazySparseMatrix
    return with_selections(
        matrix,
        compose(selection_from_indexer(rows), matrix.row_select),
        compose(selection_from_indexer(columns), matrix.column_select),
    )
end

# Explicit overrides matching `SparseArrays`'s parametric and `Bool`-eltype `getindex(::AbstractSparseMatrixCSC, ...)`
# methods on a `LazySparseMatrix` to disambiguate the mixed-specificity cases (where SparseArrays is more specific on
# the index types and the LazySparseMatrix dispatch is more specific on the matrix type). Each delegates to
# `slice_with_indexers` so the lazy slicing semantics carry through. The `(_, ::Colon)` and `(::Colon, _)` slicing
# forms (including those produced by `NamedArray`'s wrapper) recurse via
# `SparseArrays`'s `getindex(A, ::Colon, i) = getindex(A, axes(A, 1), i)` (and its symmetric form), so the row /
# column axis becomes a `Base.OneTo` range and dispatch lands on one of the integer / range overrides below. The
# `AbstractRange{Bool}` overrides exist only to satisfy `Aqua.test_ambiguities`; no natural slicing call constructs a
# `Bool`-eltype range.
function Base.getindex(  # FLAKY TESTED
    matrix::LazySparseMatrix{Tv, Ti},
    rows::AbstractRange{<:Integer},
    columns::AbstractVector{<:Integer},
)::LazySparseMatrix where {Tv, Ti <: Integer}
    return slice_with_indexers(matrix, rows, columns)
end

function Base.getindex(  # FLAKY TESTED
    matrix::LazySparseMatrix,
    rows::AbstractVector{Bool},
    columns::AbstractVector{Bool},
)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

function Base.getindex(  # FLAKY TESTED
    matrix::LazySparseMatrix,
    rows::AbstractVector{Bool},
    columns::AbstractVector{<:Integer},
)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

function Base.getindex(  # FLAKY TESTED
    matrix::LazySparseMatrix,
    rows::AbstractVector{<:Integer},
    columns::AbstractVector{Bool},
)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

function Base.getindex(  # UNTESTED
    matrix::LazySparseMatrix,
    rows::AbstractRange{Bool},
    columns::AbstractVector{<:Integer},
)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

function Base.getindex(  # FLAKY TESTED
    matrix::LazySparseMatrix,
    rows::AbstractRange{<:Integer},
    columns::AbstractVector{Bool},
)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

function Base.getindex(  # UNTESTED
    matrix::LazySparseMatrix,
    rows::AbstractRange{Bool},
    columns::AbstractVector{Bool},
)::LazySparseMatrix
    return slice_with_indexers(matrix, rows, columns)
end

# Iterate `(new_index, original_index)` pairs of a [`SparseSelection`](@ref). Used by the materialisation algorithm
# to walk the column selection in lockstep with output positions. `MaskOf` allocates `findall` once per call —
# materialisation calls this once per `LazySparseMatrix`, so the cost is amortised.
function iter_selection(selection::AllOf)
    return enumerate(1:selection.n_entries)
end

function iter_selection(selection::RangeOf)
    return enumerate(selection.range)
end

function iter_selection(selection::IndicesOf)
    return enumerate(selection.indices)
end

function iter_selection(selection::MaskOf)
    return enumerate(findall(selection.mask))
end

# Build an `original_index::Int -> new_index::Int` callable for a [`SparseSelection`](@ref); a return value of `0`
# means the original index is filtered out of the slice. The callable is `O(1)` per call. Construction is `O(1)` for
# `AllOf` / `RangeOf`, `O(length(indices))` for `IndicesOf` (the per-call cost beats `findfirst`'s `O(n)`), and
# `O(length(mask))` for `MaskOf` (the cumulative-sum precompute beats per-call `count`).
function build_original_to_new(::AllOf)::Function
    return identity
end

function build_original_to_new(selection::RangeOf)::Function
    range_first = first(selection.range)
    range_last = last(selection.range)
    return original_index -> (range_first <= original_index <= range_last) ? original_index - range_first + 1 : 0
end

function build_original_to_new(selection::IndicesOf)::Function
    table = Dict{Int, Int}()
    sizehint!(table, length(selection.indices))
    for (new_index, original_index) in enumerate(selection.indices)
        table[original_index] = new_index
    end
    return original_index -> get(table, original_index, 0)
end

function build_original_to_new(selection::MaskOf)::Function
    cumulative = cumsum(selection.mask)
    mask = selection.mask
    return original_index -> mask[original_index] ? Int(cumulative[original_index]) : 0
end

# Translate a 1-based position in the slice back to its position in the original axis. Used by scalar
# `Base.getindex(lazy, ::Int, ::Int)`. `MaskOf`'s implementation is `O(length(mask))` per call (it walks `findall`);
# tight loops should materialise the slice or call `build_original_to_new` instead.
function original_at(::AllOf, new_index::Int)::Int
    return new_index
end

function original_at(selection::RangeOf, new_index::Int)::Int
    return first(selection.range) + new_index - 1
end

function original_at(selection::IndicesOf, new_index::Int)::Int
    return selection.indices[new_index]
end

function original_at(selection::MaskOf, new_index::Int)::Int
    return findall(selection.mask)[new_index]
end

# Read the contiguous `range` of elements out of `source` for the materialisation algorithm to walk. The result is
# only iterated and indexed inside the inner loop and is not retained, so a zero-copy `view` is preferred for flat
# `Vector` sources (mmap-backed in practice). For `Zarr.ZArray` and other chunked sources, eager decode is the right
# behaviour: the per-chunk decompression cost should be paid once per range, not once per element access through a
# lazy `SubArray`.
function read_chunk_range(source::DenseVector{T}, range::UnitRange{Int}) where {T}  # FLAKY TESTED
    return @view source[range]
end

function read_chunk_range(source::AbstractVector{T}, range::UnitRange{Int})::Vector{T} where {T}
    return Vector{T}(source[range])
end

# Materialise the current slice into a `SparseMatrixCSC{Tv, Ti}` and cache it in `matrix.materialized`. Subsequent
# calls return the cached matrix without re-reading the packed sources. For each original column in `column_select`,
# decompresses the column's `rowval` / `nzval` slabs, filters rows through the `row_select` lookup, and appends the
# surviving entries to the output buffers.
function ensure_materialized!(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    cached = matrix.materialized
    if cached !== nothing
        return cached
    end
    n_new_rows = length(matrix.row_select)
    n_new_columns = length(matrix.column_select)
    output_colptr = Vector{Ti}(undef, n_new_columns + 1)
    output_colptr[1] = 1
    output_rowval = Vector{Ti}()
    output_nzval = Vector{Tv}()
    row_lookup = build_original_to_new(matrix.row_select)
    for (new_column_index, original_column_index) in iter_selection(matrix.column_select)
        range_start = Int(matrix.full_colptr[original_column_index])
        range_stop = Int(matrix.full_colptr[original_column_index + 1]) - 1
        if range_start <= range_stop
            range_in_source = range_start:range_stop
            column_rowvals = read_chunk_range(matrix.rowval_source, range_in_source)
            column_nzvals = read_chunk_range(matrix.nzval_source, range_in_source)
            for k in eachindex(column_rowvals)
                new_row_index = row_lookup(Int(column_rowvals[k]))
                if new_row_index != 0
                    push!(output_rowval, Ti(new_row_index))
                    push!(output_nzval, column_nzvals[k])
                end
            end
        end
        output_colptr[new_column_index + 1] = Ti(length(output_rowval) + 1)
    end
    materialized = SparseMatrixCSC{Tv, Ti}(n_new_rows, n_new_columns, output_colptr, output_rowval, output_nzval)
    matrix.materialized = materialized
    return materialized
end

function SparseArrays.rowvals(matrix::LazySparseMatrix{Tv, Ti})::Vector{Ti} where {Tv, Ti}
    return rowvals(ensure_materialized!(matrix))
end

function SparseArrays.nonzeros(matrix::LazySparseMatrix{Tv, Ti})::Vector{Tv} where {Tv, Ti}
    return nonzeros(ensure_materialized!(matrix))
end

function SparseArrays.getcolptr(matrix::LazySparseMatrix{Tv, Ti})::Vector{Ti} where {Tv, Ti}
    return SparseArrays.getcolptr(ensure_materialized!(matrix))
end

function SparseArrays.nnz(matrix::LazySparseMatrix)::Int
    return nnz(ensure_materialized!(matrix))
end

function SparseArrays.SparseMatrixCSC{Tv, Ti}(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return ensure_materialized!(matrix)
end

function SparseArrays.SparseMatrixCSC(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return ensure_materialized!(matrix)
end

function Base.convert(  # FLAKY TESTED
    ::Type{SparseMatrixCSC{Tv, Ti}},
    matrix::LazySparseMatrix{Tv, Ti},
)::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return ensure_materialized!(matrix)
end

# `TanayLabUtilities.colptr` / `rowval` / `nzval` are the writer-side sparse-component accessors that
# `FilesFormat.write_sparse_numeric_matrix` (and `ZipFormat.write_sparse_numeric_matrix`) call when
# `set_matrix!` reaches the format layer. They are explicit per-type methods (`SparseMatrixCSC`, `NamedArray`,
# `SparseArrays.ReadOnly`) with no generic `AbstractSparseMatrixCSC` fallback, so dispatch on `LazySparseMatrix`
# would error out without these overrides. Each delegates through the materialised cache to a concrete `Vector`,
# which is what the writer expects.
function TanayLabUtilities.colptr(matrix::LazySparseMatrix)::AbstractVector
    return SparseArrays.getcolptr(matrix)
end

function TanayLabUtilities.rowval(matrix::LazySparseMatrix)::AbstractVector
    return SparseArrays.rowvals(matrix)
end

function TanayLabUtilities.nzval(matrix::LazySparseMatrix)::AbstractVector
    return SparseArrays.nonzeros(matrix)
end

# Per-cell scalar lookup that does not populate `matrix.materialized`. Reads the target column's `rowval` slab from
# the packed source, binary-searches it for `original_row` (CSC stores rowvals sorted within a column), and returns
# the matching `nzval` entry (or `zero(Tv)` if the cell is structurally absent). When the matrix is already
# materialised, this delegates to the cached `SparseMatrixCSC`.
function Base.getindex(matrix::LazySparseMatrix{Tv, Ti}, row_index::Int, column_index::Int)::Tv where {Tv, Ti}
    @boundscheck begin
        @assert 1 <= row_index <= length(matrix.row_select)
        @assert 1 <= column_index <= length(matrix.column_select)
    end
    cached = matrix.materialized
    if cached !== nothing
        return cached[row_index, column_index]
    end
    original_row = original_at(matrix.row_select, row_index)
    original_column = original_at(matrix.column_select, column_index)
    range_start = Int(matrix.full_colptr[original_column])
    range_stop = Int(matrix.full_colptr[original_column + 1]) - 1
    if range_start > range_stop
        return zero(Tv)
    end
    range_in_source = range_start:range_stop
    column_rowvals = read_chunk_range(matrix.rowval_source, range_in_source)
    position = searchsortedfirst(column_rowvals, Ti(original_row))
    if position > length(column_rowvals) || Int(column_rowvals[position]) != original_row
        return zero(Tv)
    end
    target_in_source = range_start + position - 1
    return Tv(matrix.nzval_source[target_in_source])
end

end  # module
