"""
A lazy wrapper around a sparse matrix whose components are chunked sources.

A [`LazySparseMatrix`](@ref) holds the `colptr` of a sparse matrix in memory, and the `rowval` and `nzval` as lazy
sources (packed on disk, or chunked over HTTP). Reading a subset of its columns reads only those columns. Any other
access reads the whole matrix once.

Sparse vectors are at most one axis long, so the formats always read them eagerly.
"""
module LazySparse

export LazySparseMatrix

using DiskArrays
using LinearAlgebra
using NamedArrays
using SparseArrays
using TanayLabUtilities

import ..Formats
import ..PackedFormat.clear_materialization!
import ..PackedFormat.existing_materialization
import ..PackedFormat.materialization!
import ..PackedFormat.MaterializationCell
import ..PackedFormat.pinned_materialization!
import ..PackedFormat.require_unmodified
import SparseArrays.AbstractSparseMatrixCSC  # NOLINT
import SparseArrays.AbstractSparseMatrixCSCInclAdjointAndTranspose

"""
    mutable struct LazySparseMatrix{Tv, Ti <: Integer} <: AbstractSparseMatrixCSC{Tv, Ti}
        n_rows::Int
        colptr::Vector{Ti}
        rowval_source::AbstractVector{Ti}
        nzval_source::AbstractVector{Tv}
        materialization_cell::MaterializationCell{SparseMatrixCSC{Tv, Ti}}
        did_report_scalar_access::Threads.Atomic{Bool}
    end

Lazy [`AbstractSparseMatrixCSC{Tv, Ti}`](https://docs.julialang.org/en/v1/stdlib/SparseArrays/#SparseArrays.AbstractSparseMatrixCSC)
over the `rowval` / `nzval` sources of a stored sparse matrix.

It holds the `colptr` in memory (one entry per column). The `rowval` and `nzval` sources decode their data on demand,
one chunk at a time. A source may also be a plain `Vector`, for a flat component of a property whose other component is
packed.

Reading a subset of the columns reads only those columns. This covers `lazy[:, columns]`, `lazy[rows, columns]`,
`lazy[:, column]`, `lazy[rows, column]`, and a `view` of any of these. It returns a new `SparseMatrixCSC` (or
`SparseVector`) owned by the caller, which is not cached.

Any other access reads the whole matrix into a `SparseMatrixCSC`, its materialization. All readers share it while any of
them holds it. Once no reader holds it, its memory is freed, and the next access reads the matrix again. This includes
row access and any generic `AbstractSparseMatrixCSC` operation (arithmetic, broadcasting, `Matrix(lazy)`, ...). The
generic operations reach the data through `rowvals` / `nonzeros`, which return read-only views of the materialization.
Code that loops over many elements should take `plain_array(lazy)` once, and use it. This reads the matrix once, and
holds it in memory while the code runs. Such code should still prefer walking the columns of the `SparseMatrixCSC`.

Scalar access (`lazy[row, column]`) is reported to the TanayLabUtilities `GLOBAL_INEFFICIENT_ACTION_HANDLER`, once per
lazy matrix. It is usually part of a loop over many elements. So the lazy matrix then holds its materialization until it
is modified or purged by `empty_cache!`, so that such a loop reads the matrix only once.

`convert(SparseMatrixCSC, lazy)` and `plain_array(lazy)` return the shared materialization itself.
`SparseMatrixCSC(lazy)` and `copy(lazy)` return a new matrix owned by the caller.

User code obtains a `LazySparseMatrix` only as the result of reading a sparse property whose `rowval` / `nzval`
components are lazy sources, through [`DataAxesFormats.Formats.format_get_matrix`](@ref). Construction lives behind the
read paths and is not part of the public API.
"""
mutable struct LazySparseMatrix{Tv, Ti <: Integer} <: AbstractSparseMatrixCSC{Tv, Ti}
    n_rows::Int
    colptr::Vector{Ti}
    rowval_source::AbstractVector{Ti}
    nzval_source::AbstractVector{Tv}
    materialization_cell::MaterializationCell{SparseMatrixCSC{Tv, Ti}}
    did_report_scalar_access::Threads.Atomic{Bool}
end

# The `colptr` is taken by reference; callers that intend to share it should `copy` first.
function LazySparseMatrix(
    n_rows::Integer,
    colptr::Vector{Ti},
    rowval_source::AbstractVector{Ti},
    nzval_source::AbstractVector{Tv},
)::LazySparseMatrix{Tv, Ti} where {Tv, Ti <: Integer}
    @assert length(rowval_source) == length(nzval_source)
    return LazySparseMatrix{Tv, Ti}(
        Int(n_rows),
        colptr,
        rowval_source,
        nzval_source,
        MaterializationCell{SparseMatrixCSC{Tv, Ti}}(),
        Threads.Atomic{Bool}(false),
    )
end

function Base.size(matrix::LazySparseMatrix)::Tuple{Int, Int}
    return (matrix.n_rows, length(matrix.colptr) - 1)
end

# The whole matrix, shared by all its callers while any of them holds it. Reads it with [`new_materialization`](@ref) if
# no caller holds it.
function shared_materialization!(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    require_unmodified(matrix)
    return materialization!(() -> new_materialization(matrix), matrix.materialization_cell)
end

# A new `SparseMatrixCSC` of the whole matrix, owned by the caller; nothing is shared.
function new_materialization(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return read_columns(matrix, 1:size(matrix, 2))
end

# Whether a caller holds the materialization of the whole matrix.
function is_materialized(matrix::LazySparseMatrix)::Bool
    return existing_materialization(matrix.materialization_cell) !== nothing
end

# A new `SparseMatrixCSC` of all the rows of some columns, owned by the caller; nothing is cached. Reads only these
# columns' ranges of the sources, with one read per run of consecutive columns.
function read_columns(
    matrix::LazySparseMatrix{Tv, Ti},
    column_indices::AbstractVector{<:Integer},
)::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    n_columns = length(column_indices)
    colptr = Vector{Ti}(undef, n_columns + 1)
    colptr[1] = 1
    for (position, column_index) in enumerate(column_indices)
        colptr[position + 1] = colptr[position] + matrix.colptr[column_index + 1] - matrix.colptr[column_index]
    end

    n_stored = Int(colptr[end]) - 1
    rowval = Vector{Ti}(undef, n_stored)
    nzval = Vector{Tv}(undef, n_stored)
    first_position = 1
    while first_position <= n_columns
        last_position = first_position
        while last_position < n_columns && column_indices[last_position + 1] == column_indices[last_position] + 1
            last_position += 1
        end
        source_range =
            Int(
                matrix.colptr[column_indices[first_position]],
            ):(Int(matrix.colptr[column_indices[last_position] + 1]) - 1)
        if !isempty(source_range)
            destination_range = Int(colptr[first_position]):(Int(colptr[last_position + 1]) - 1)
            read_source_range!(view(rowval, destination_range), matrix.rowval_source, source_range)
            read_source_range!(view(nzval, destination_range), matrix.nzval_source, source_range)
        end
        first_position = last_position + 1
    end

    return SparseMatrixCSC{Tv, Ti}(matrix.n_rows, n_columns, colptr, rowval, nzval)
end

# Copy a `range` of a component source into `destination`. A chunked source decodes straight into it.
function read_source_range!(
    destination::AbstractVector,
    source::DiskArrays.AbstractDiskArray,
    range::UnitRange{Int},
)::Nothing
    DiskArrays.readblock!(source, destination, range)
    return nothing
end

function read_source_range!(destination::AbstractVector, source::AbstractVector, range::UnitRange{Int})::Nothing
    copyto!(destination, view(source, range))
    return nothing
end

# Whether a row indexer selects every row, in order. A `NamedArray` passes `:` to its parent as `collect(1:n_rows)`.
function is_all_rows(rows::AbstractVector{Bool}, n_rows::Int)::Bool
    return length(rows) == n_rows && all(rows)
end

function is_all_rows(rows::AbstractVector{<:Integer}, n_rows::Int)::Bool
    return length(rows) == n_rows && all(position -> rows[position] == position, 1:n_rows)
end

# Some rows of some columns, as a new `SparseMatrixCSC` owned by the caller. Reads only these columns.
function read_rows_of_columns(
    matrix::LazySparseMatrix{Tv, Ti},
    rows::Union{Colon, AbstractVector},
    columns::AbstractVector,
)::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    require_unmodified(matrix)
    cached = existing_materialization(matrix.materialization_cell)
    if cached !== nothing
        return cached[rows, columns]
    end
    column_indices = columns isa AbstractVector{Bool} ? findall(columns) : columns
    columns_subset = read_columns(matrix, column_indices)
    if rows isa Colon || is_all_rows(rows, matrix.n_rows)
        return columns_subset
    else
        return columns_subset[rows, :]
    end
end

# Some rows of one column, as a new `SparseVector` owned by the caller. Reads only this column.
function read_rows_of_column(
    matrix::LazySparseMatrix{Tv, Ti},
    rows::Union{Colon, AbstractVector},
    column_index::Integer,
)::SparseVector{Tv, Ti} where {Tv, Ti}
    require_unmodified(matrix)
    cached = existing_materialization(matrix.materialization_cell)
    if cached !== nothing
        return cached[rows, column_index]
    end
    column_subset = read_columns(matrix, column_index:column_index)
    column = SparseVector{Tv, Ti}(matrix.n_rows, rowvals(column_subset), nonzeros(column_subset))
    if rows isa Colon || is_all_rows(rows, matrix.n_rows)
        return column
    else
        return column[rows]
    end
end

# The column subset forms of `getindex`. Each mirrors the signature of one `SparseArrays` method for an
# `AbstractSparseMatrixCSC`, so the call isn't ambiguous between the two. The forms that select some rows of all the
# columns, a single row, or a single element are left to `SparseArrays`, which reads the whole matrix once.
function Base.getindex(matrix::LazySparseMatrix, ::Colon, ::Colon)::SparseMatrixCSC
    return SparseMatrixCSC(matrix)
end

function Base.getindex(matrix::LazySparseMatrix, ::Colon, columns::AbstractVector)::SparseMatrixCSC
    return read_rows_of_columns(matrix, :, columns)
end

function Base.getindex(matrix::LazySparseMatrix, rows::AbstractVector, columns::AbstractVector)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(matrix::LazySparseMatrix, rows::AbstractRange, columns::AbstractVector)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)  # NOJET
end

function Base.getindex(  # UNTESTED
    matrix::LazySparseMatrix,
    rows::AbstractRange{Bool},
    columns::AbstractVector{Bool},
)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(  # UNTESTED
    matrix::LazySparseMatrix,
    rows::AbstractRange{Bool},
    columns::AbstractVector{<:Integer},
)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(
    matrix::LazySparseMatrix,
    rows::AbstractRange{<:Integer},
    columns::AbstractVector{Bool},
)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(
    matrix::LazySparseMatrix,
    rows::AbstractVector{Bool},
    columns::AbstractVector{Bool},
)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(
    matrix::LazySparseMatrix,
    rows::AbstractVector{<:Integer},
    columns::AbstractVector{Bool},
)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(
    matrix::LazySparseMatrix,
    rows::AbstractVector{Bool},
    columns::AbstractVector{<:Integer},
)::SparseMatrixCSC
    return read_rows_of_columns(matrix, rows, columns)
end

function Base.getindex(matrix::LazySparseMatrix, ::Colon, column_index::Integer)::SparseVector
    return read_rows_of_column(matrix, :, column_index)
end

function Base.getindex(matrix::LazySparseMatrix, rows::AbstractVector, column_index::Integer)::SparseVector
    return read_rows_of_column(matrix, rows, column_index)
end

function Base.getindex(matrix::LazySparseMatrix, rows::AbstractUnitRange, column_index::Integer)::SparseVector
    return read_rows_of_column(matrix, rows, column_index)
end

function Base.getindex(matrix::LazySparseMatrix, rows::AbstractVector{Bool}, column_index::Integer)::SparseVector
    return read_rows_of_column(matrix, rows, column_index)
end

function Base.getindex(matrix::LazySparseMatrix{Tv}, row_index::Integer, column_index::Integer)::Tv where {Tv}
    require_unmodified(matrix)
    report_scalar_access(matrix)
    return shared_materialization!(matrix)[row_index, column_index]
end

# Report the first scalar access to the `matrix`, and from then on hold its materialization. Scalar access is usually
# part of a loop over many elements, which would otherwise read the whole matrix again whenever the GC freed it.
function report_scalar_access(matrix::LazySparseMatrix)::Nothing
    if !matrix.did_report_scalar_access[]
        handle_abnormal(TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER) do
            return """
                inefficient scalar access to a lazy sparse matrix (take plain_array first)
                for: $(brief(matrix))
                """
        end
        if !Threads.atomic_xchg!(matrix.did_report_scalar_access, true)
            pinned_materialization!(() -> new_materialization(matrix), matrix.materialization_cell)
        end
    end
    return nothing
end

# Comparing a lazy matrix through the generic methods reads it element by element (scalar access), and so does comparing
# a `NamedArray` of one. These methods compare the plain storage instead, which uses the efficient sparse methods. The
# methods with `AbstractSparseMatrixCSC`, with transposed sparse matrices, and with two lazy matrices resolve
# ambiguities with the `SparseArrays` methods.
NamedLazySparseMatrix = NamedArray{<:Any, 2, <:LazySparseMatrix}

LazyOrNamedLazySparseMatrix = Union{LazySparseMatrix, NamedLazySparseMatrix}

function Base.:(==)(left::LazyOrNamedLazySparseMatrix, right::AbstractArray)::Bool
    return plain_array(left) == plain_array(right)
end

function Base.:(==)(left::AbstractArray, right::LazyOrNamedLazySparseMatrix)::Bool
    return plain_array(left) == plain_array(right)
end

function Base.:(==)(left::LazyOrNamedLazySparseMatrix, right::LazyOrNamedLazySparseMatrix)::Bool
    return plain_array(left) == plain_array(right)
end

function Base.:(==)(left::LazySparseMatrix, right::AbstractSparseMatrixCSC)::Bool
    return plain_array(left) == plain_array(right)
end

function Base.:(==)(left::AbstractSparseMatrixCSC, right::LazySparseMatrix)::Bool
    return plain_array(left) == plain_array(right)
end

function Base.:(==)(left::LazySparseMatrix, right::LazySparseMatrix)::Bool
    return plain_array(left) == plain_array(right)
end

TransposedSparseMatrix = Union{
    Adjoint{<:Any, <:AbstractSparseMatrixCSCInclAdjointAndTranspose},
    Transpose{<:Any, <:AbstractSparseMatrixCSCInclAdjointAndTranspose},
}

function Base.:(==)(left::LazySparseMatrix, right::TransposedSparseMatrix)::Bool
    return plain_array(left) == right
end

function Base.:(==)(
    left::Adjoint{<:Any, <:AbstractSparseMatrixCSCInclAdjointAndTranspose},
    right::LazySparseMatrix,
)::Bool
    return left == plain_array(right)
end

function Base.:(==)(
    left::Transpose{<:Any, <:AbstractSparseMatrixCSCInclAdjointAndTranspose},
    right::LazySparseMatrix,
)::Bool
    return left == plain_array(right)
end

function Base.isequal(left::LazyOrNamedLazySparseMatrix, right::AbstractArray)::Bool
    return isequal(plain_array(left), plain_array(right))
end

function Base.isequal(left::AbstractArray, right::LazyOrNamedLazySparseMatrix)::Bool
    return isequal(plain_array(left), plain_array(right))
end

function Base.isequal(left::LazyOrNamedLazySparseMatrix, right::LazyOrNamedLazySparseMatrix)::Bool
    return isequal(plain_array(left), plain_array(right))
end

function Base.isequal(left::LazySparseMatrix, right::AbstractSparseMatrixCSC)::Bool
    return isequal(plain_array(left), plain_array(right))
end

function Base.isequal(left::AbstractSparseMatrixCSC, right::LazySparseMatrix)::Bool
    return isequal(plain_array(left), plain_array(right))
end

function Base.isequal(left::LazySparseMatrix, right::LazySparseMatrix)::Bool
    return isequal(plain_array(left), plain_array(right))
end

# Showing a lazy matrix (or a `NamedArray` of one) shows its plain storage, rather than reading the displayed elements
# one at a time (scalar access).
function Base.show(io::IO, matrix::LazySparseMatrix)::Nothing
    show(io, plain_array(matrix))
    return nothing
end

function Base.show(io::IO, mime::MIME"text/plain", matrix::LazySparseMatrix)::Nothing
    show(io, mime, plain_array(matrix))
    return nothing
end

function Base.show(io::IO, named::NamedLazySparseMatrix)::Nothing
    show(io, NamedArray(plain_array(named.array), named.dicts, named.dimnames))
    return nothing
end

function Base.show(io::IO, mime::MIME"text/plain", named::NamedLazySparseMatrix)::Nothing
    show(io, mime, NamedArray(plain_array(named.array), named.dicts, named.dimnames))
    return nothing
end

# A lazy matrix never changes, so a view of a column subset is the same as reading it.
function Base.view(
    matrix::LazySparseMatrix,
    rows::Union{Colon, AbstractVector},
    columns::Union{Integer, AbstractVector},
)::AbstractSparseArray
    return matrix[rows, columns]
end

# A lazy matrix is read-only. The generic `SparseArrays` code reads and writes a sparse matrix through these accessors,
# so they return read-only views of the shared materialization. Any write through them (`setindex!`, `dropzeros!`,
# `nonzeros(matrix)[i] = ...`) fails instead of changing the data all later readers see.
function SparseArrays.rowvals(matrix::LazySparseMatrix{Tv, Ti})::AbstractVector{Ti} where {Tv, Ti}
    return read_only_array(rowvals(shared_materialization!(matrix)))
end

function SparseArrays.nonzeros(matrix::LazySparseMatrix{Tv, Ti})::AbstractVector{Tv} where {Tv, Ti}
    return read_only_array(nonzeros(shared_materialization!(matrix)))
end

function SparseArrays.getcolptr(matrix::LazySparseMatrix{Tv, Ti})::AbstractVector{Ti} where {Tv, Ti}
    return read_only_array(matrix.colptr)
end

function SparseArrays.nnz(matrix::LazySparseMatrix)::Int
    return Int(matrix.colptr[end]) - 1
end

# Like `SparseArrays`' own constructors, these return a matrix owned by the caller. If a caller holds the
# materialization, this copies it; otherwise this builds a new one without sharing it. In contrast, `convert` returns
# the shared materialization itself.
function SparseArrays.SparseMatrixCSC{Tv, Ti}(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    require_unmodified(matrix)
    cached = existing_materialization(matrix.materialization_cell)
    if cached === nothing
        return new_materialization(matrix)
    else
        return copy(cached)
    end
end

function SparseArrays.SparseMatrixCSC(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return SparseMatrixCSC{Tv, Ti}(matrix)
end

# The generic sparse `copy` would fail on the read-only accessors, and would copy the shared materialization after
# building it. This builds one new matrix owned by the caller.
function Base.copy(matrix::LazySparseMatrix{Tv, Ti})::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return SparseMatrixCSC{Tv, Ti}(matrix)
end

function Base.convert(::Type{M}, matrix::LazySparseMatrix{Tv, Ti})::M where {Tv, Ti, M <: SparseMatrixCSC}
    return convert(M, shared_materialization!(matrix))
end

# `TanayLabUtilities.colptr` / `rowval` / `nzval` are the writer-side sparse-component accessors that
# `FilesFormat.write_sparse_numeric_matrix` (and `ZipFormat.write_sparse_numeric_matrix`) call when
# `set_matrix!` reaches the format layer. They are explicit per-type methods (`SparseMatrixCSC`, `NamedArray`,
# `SparseArrays.ReadOnly`) with no generic `AbstractSparseMatrixCSC` fallback, so dispatch on `LazySparseMatrix`
# would error out without these overrides. Like the `SparseArrays.ReadOnly` methods, each returns the plain arrays of
# the shared materialization (not read-only views), which is what the writers and fast loops expect.
function TanayLabUtilities.colptr(matrix::LazySparseMatrix)::AbstractVector
    return SparseArrays.getcolptr(shared_materialization!(matrix))
end

function TanayLabUtilities.rowval(matrix::LazySparseMatrix)::AbstractVector
    return SparseArrays.rowvals(shared_materialization!(matrix))
end

function TanayLabUtilities.nzval(matrix::LazySparseMatrix)::AbstractVector
    return SparseArrays.nonzeros(shared_materialization!(matrix))
end

# A lazy matrix was modified exactly when one of its lazy sources was.
function require_unmodified(matrix::LazySparseMatrix)::Nothing
    require_unmodified(matrix.rowval_source)
    require_unmodified(matrix.nzval_source)
    return nothing
end

function Formats.is_lazy_array(::LazySparseMatrix)::Bool
    return true
end

function Formats.invalidate_lazy_array!(matrix::LazySparseMatrix)::Nothing
    for source in (matrix.rowval_source, matrix.nzval_source)
        if Formats.is_lazy_array(source)
            Formats.invalidate_lazy_array!(source)
        end
    end
    clear_materialization!(matrix.materialization_cell)
    return nothing
end

function Formats.purge_lazy_array!(matrix::LazySparseMatrix)::Nothing
    for source in (matrix.rowval_source, matrix.nzval_source)
        if Formats.is_lazy_array(source)
            Formats.purge_lazy_array!(source)
        end
    end
    clear_materialization!(matrix.materialization_cell)
    matrix.did_report_scalar_access[] = false
    return nothing
end

# A lazy matrix rejects writes, so it needs no read-only wrapper. Without one, broadcasting, `view` and `similar` see
# the lazy matrix itself.
function TanayLabUtilities.ReadOnlyArrays.read_only_array(matrix::LazySparseMatrix)::LazySparseMatrix
    return matrix
end

function TanayLabUtilities.ReadOnlyArrays.is_read_only_array(::LazySparseMatrix)::Bool
    return true
end

function TanayLabUtilities.MatrixLayouts.plain_array(
    matrix::LazySparseMatrix{Tv, Ti};
    transposed_as::Maybe{Type} = nothing,  # NOLINT
    contiguous::Bool = false,  # NOLINT
)::SparseMatrixCSC{Tv, Ti} where {Tv, Ti}
    return shared_materialization!(matrix)
end

end  # module
