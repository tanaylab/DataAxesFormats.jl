"""
All stored `Daf` matrix data has a clear matrix layout, that is, a [`major_axis`](@ref), regardless of whether it is
dense or sparse.

That is, for [`Columns`](@ref)-major data, the values of each column are laid out consecutively in memory (each column
is a single contiguous vector), so any operation that works on whole columns will be fast (e.g., summing the value of
each column). In contrast, the values of each row are stored far apart from each other, so any operation that works on
whole rows will be very slow in comparison (e.g., summing the value of each row).

For [`Rows`](@ref)-major data, the values of each row are laid out consecutively in memory (each row is a single
contiguous vector). In contrast, the values of each column are stored far apart from each other. In this case, summing
columns would be slow, and summing rows would be fast.

This is much simpler than the `ArrayLayouts` module which attempts to fully describe the layout of N-dimensional arrays,
a much more ambitious goal which is an overkill for our needs.

!!! note

    The "default" layout in Julia is column-major, which inherits this from matlab, which inherits this from FORTRAN,
    allegedly because this is more efficient for some linear algebra operations. In contrast, Python `numpy` uses
    row-major layout by default. In either case, this is just an arbitrary convention, and all systems work just fine
    with data of either memory layout; the key consideration is to keep track of the layout, and to apply operations
    "with the grain" rather than "against the grain" of the data.
"""
module MatrixLayouts

export axis_name
export check_efficient_action
export Columns
export inefficient_action_handler
export major_axis
export minor_axis
export other_axis
export relayout!
export require_major_axis
export require_minor_axis
export Rows

using Daf.Generic
using Distributed
using LinearAlgebra
using NamedArrays
using SparseArrays

import Distributed.@everywhere

"""
A symbolic name for the rows axis. It is much more readable to write, say, `size(matrix, Rows)`, instead of
`size(matrix, 1)`.
"""
Rows = 1

"""
A symbolic name for the rows axis. It is much more readable to write, say, `size(matrix, Columns)`, instead of
`size(matrix, 2)`.
"""
Columns = 2

"""
    axis_name(axis::Maybe{Integer})::String

Return the name of the axis (for messages).
"""
function axis_name(axis::Maybe{Integer})::String
    if axis == nothing
        return "nothing"
    end

    if axis == Rows
        return "Rows"
    end

    if axis == Columns
        return "Columns"
    end

    return error("invalid matrix axis: $(axis)")
end

"""
    major_axis(matrix::AbstractMatrix)::Maybe{Int8}

Return the index of the major axis of a matrix, that is, the axis one should keep **fixed** for an efficient inner loop
accessing the matrix elements. If the matrix doesn't support any efficient access axis, returns `nothing`.
"""
function major_axis(matrix::NamedMatrix)::Maybe{Int8}
    return major_axis(matrix.array)
end

function major_axis(matrix::SparseArrays.ReadOnly)::Maybe{Int8}
    return major_axis(parent(matrix))
end

function major_axis(matrix::Transpose)::Maybe{Int8}
    return other_axis(major_axis(matrix.parent))
end

function major_axis(matrix::AbstractSparseMatrix)::Maybe{Int8}
    return Columns
end

function major_axis(matrix::AbstractMatrix)::Maybe{Int8}
    try
        matrix_strides = strides(matrix)
        if matrix_strides[1] == 1
            return Columns
        end
        if matrix_strides[2] == 1
            return Rows
        end
        return nothing             # untested

    catch MethodError
        return nothing  # untested
    end
end

"""
    require_major_axis(matrix::AbstractMatrix)::Int8

Similar to [`major_axis`](@ref) but will `error` if the matrix isn't in either row-major or column-major layout.
"""
function require_major_axis(matrix::AbstractMatrix)::Int8
    axis = major_axis(matrix)
    if axis == nothing
        error("type: $(typeof(matrix)) is not in any-major layout")  # untested
    end
    return axis
end

"""
    minor_axis(matrix::AbstractMatrix)::Maybe{Int8}

Return the index of the minor axis of a matrix, that is, the axis one should **vary** for an efficient inner loop
accessing the matrix elements. If the matrix doesn't support any efficient access axis, returns `nothing`.
"""
function minor_axis(matrix::AbstractMatrix)::Maybe{Int8}
    return other_axis(major_axis(matrix))
end

"""
    require_minor_axis(matrix::AbstractMatrix)::Int8

Similar to [`minor_axis`](@ref) but will `error` if the matrix isn't in either row-major or column-major layout.
"""
function require_minor_axis(matrix::AbstractMatrix)::Int8  # untested
    return other_axis(require_major_axis(matrix))
end

"""
    other_axis(axis::Maybe{Integer})::Maybe{Int8}

Return the other `matrix` `axis` (that is, convert between [`Rows`](@ref) and [`Columns`](@ref)). If given `nothing`
returns `nothing`.
"""
function other_axis(axis::Maybe{Integer})::Maybe{Int8}
    if axis == nothing
        return nothing
    end

    if axis == Rows || axis == Columns
        return Int8(3 - axis)
    end

    return error("invalid matrix axis: $(axis)")
end

GLOBAL_INEFFICIENT_ACTION_HANDLER::AbnormalHandler = IgnoreHandler

"""
    inefficient_action_handler(handler::AbnormalHandler)::AbnormalHandler

Specify the [`AbnormalHandler`](@ref) to use when accessing a matrix in an inefficient way ("against the grain").
Returns the previous handler.

!!! note

    This will affect **all** the processes `@everywhere`, not just the current one.
"""
function inefficient_action_handler(handler::AbnormalHandler)::AbnormalHandler
    global GLOBAL_INEFFICIENT_ACTION_HANDLER
    previous_inefficient_action_handler = GLOBAL_INEFFICIENT_ACTION_HANDLER

    @eval @everywhere Daf.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = $handler

    return previous_inefficient_action_handler
end

"""
    check_efficient_action(
        action::AbstractString,
        axis::Integer,
        operand::AbstractString,
        matrix::AbstractMatrix,
    )::Nothing

This will check whether the `action` about to be executed for an `operand` which is `matrix` works "with the grain" of
the data, which requires the `matrix` to be in `axis`-major layout. If it isn't, then apply the
[`inefficient_action_handler`](@ref).

In general, you **really** want operations to go "with the grain" of the data. Unfortunately, Julia (and Python, and R,
and matlab) will silently run operations "against the grain", which would be **painfully** slow. A liberal application
of this function in your code will help in detecting such slowdowns, without having to resort to profiling the code to
isolate the problem.

!!! note

    This will not prevent the code from performing "against the grain" operations such as `selectdim(matrix, Rows, 1)`
    for a column-major matrix, but if you add this check before performing any (series of) operations on a matrix, then
    you will have a clear indication of whether (and where) such operations occur. You can then consider whether to
    invoke [`relayout!`](@ref) on the data, or (for data fetched from `Daf`), simply query for the other memory layout.
"""
function check_efficient_action(
    action::AbstractString,
    axis::Integer,
    operand::AbstractString,
    matrix::AbstractMatrix,
)::Nothing
    if major_axis(matrix) != axis
        global GLOBAL_INEFFICIENT_ACTION_HANDLER
        handle_abnormal(GLOBAL_INEFFICIENT_ACTION_HANDLER) do
            return (
                "the major axis: $(axis_name(axis))\n" *
                "of the action: $(action)\n" *
                "is different from the major axis: $(axis_name(major_axis(matrix)))\n" *
                "of the $(operand) matrix: $(typeof(matrix))"
            )
        end
    end
end

"""
    relayout!(matrix::AbstractMatrix)::AbstractMatrix
    relayout!(matrix::NamedMatrix)::NamedMatrix
    relayout!(into::AbstractMatrix, from::AbstractMatrix)::AbstractMatrix
    relayout!(into::AbstractMatrix, from::NamedMatrix)::NamedMatrix

Return the same `matrix` data, but in the other memory layout.

Suppose you have a column-major UMIs matrix, whose rows are cells, and columns are genes. Therefore, summing the UMIs of
a gene will be fast, but summing the UMIs of a cell will be slow. A `transpose` (no `!`) of a matrix is fast; it creates
a zero-copy wrapper of the matrix with flipped axes, so its rows will be genes and columns will be cells, but in
row-major layout. Therefore, **still**, summing the UMIs of a gene is fast, and summing the UMIs of a cell is slow.

In contrast, `transpose!` (with a `!`) is slow; it creates a rearranged copy of the data, also returning a matrix whose
rows are genes and columns are cells, but this time, in column-major layout. Therefore, in this case summing the UMIs of
a gene will be slow, and summing the UMIs of a cell will be fast.

!!! note

    It is almost always worthwhile to `relayout!` a matrix and then perform operations "with the grain" of the data,
    instead of skipping it and performing operations "against the grain" of the data. This is because (in Julia at
    least) the implementation of `transpose!` is optimized for the task, while the other operations typically don't
    provide any specific optimizations for working "against the grain" of the data. The benefits of a `relayout!` become
    even more significant when performing a series of operations (e.g., summing the gene UMIs in each cell, converting
    gene UMIs to fractions out of these totals, then computing the log base 2 of this fraction).

If you `transpose` (no `!`) the result of `transpose!` (with a `!`), you end up with a matrix that **appears** to be the
same as the original (rows are cells and columns are genes), but behaves **differently** - summing the UMIs of a gene
will be slow, and summing the UMIs of a cell is fast. This `transpose` of `transpose!` is a common idiom and is
basically what `relayout!` does for you. In addition, `relayout!` will work for both sparse and dense matrices, and if
`into` is not specified, a `similar` matrix is allocated automatically for it.

!!! note

    The caller is responsible for providing a sensible `into` matrix (sparse for a sparse `from`, dense for a non-sparse
    `from`). This can be a transposed matrix. If `from` is a `NamedMatrix`, then the result will be a `NamedMatrix` with
    the same axes. If `into` is also a `NamedMatrix`, then its axes must match `from`.
"""
function relayout!(matrix::NamedMatrix)::NamedArray
    return NamedArray(relayout!(matrix.array), matrix.dicts, matrix.dimnames)
end

function relayout!(matrix::SparseArrays.ReadOnly)::AbstractMatrix
    return relayout!(parent(matrix))
end

function relayout!(matrix::Transpose)::AbstractMatrix
    return transpose(relayout!(parent(matrix)))
end

function relayout!(matrix::AbstractSparseMatrix)::AbstractMatrix
    @assert require_major_axis(matrix) == Columns
    return transpose(SparseMatrixCSC(transpose(matrix)))
end

function relayout!(matrix::AbstractMatrix)::AbstractMatrix
    return transpose(transpose!(similar(transpose(matrix)), matrix))
end

function relayout!(into::AbstractMatrix, from::NamedMatrix)::NamedArray  # untested
    return NamedArray(relayout!(into, from.array), from.dicts, from.dimnames)
end

function relayout!(into::Transpose, from::NamedMatrix)::AbstractMatrix
    relayout!(parent(into), transpose(from.array))
    return into
end

function relayout!(into::SparseMatrixCSC, from::NamedMatrix)::AbstractMatrix  # untested
    relayout!(into, from.array)
    return into
end

function relayout!(into::NamedArray, from::NamedMatrix)::NamedArray
    @assert into.dimnames == from.dimnames  # NOJET
    @assert into.dicts == from.dicts
    return NamedArray(relayout!(into.array, from.array), from.dicts, from.dimnames)
end

function relayout!(into::NamedArray, from::AbstractMatrix)::NamedArray
    return NamedArray(relayout!(into.array, from), into.dicts, into.dimnames)
end

function relayout!(into::Transpose, from::AbstractMatrix)::AbstractMatrix
    relayout!(parent(into), transpose(from))
    return into
end

function relayout!(into::SparseMatrixCSC, from::AbstractMatrix)::SparseMatrixCSC
    if size(into) != size(from)
        error("relayout into size: $(size(into))\nis different from size: $(size(from))")
    end
    if !issparse(from)
        error("relayout into sparse: $(typeof(into)) of non-sparse matrix: $(typeof(from))")
    end
    base_from = base_sparse_matrix(from)
    transpose_base_from = transpose(base_from)
    if transpose_base_from isa SparseMatrixCSC && nnz(transpose_base_from) == nnz(into)
        return sparse_transpose_in_memory!(into, transpose_base_from)
    else
        return transpose!(into, transpose_base_from)  # untested
    end
end

function relayout!(into::DenseMatrix, from::AbstractMatrix)::DenseMatrix
    if size(into) != size(from)
        error("relayout into size: $(size(into))\nis different from size: $(size(from))")
    end
    if issparse(from)
        error("relayout into dense: $(typeof(into)) of sparse matrix: $(typeof(from))")
    end
    return transpose!(into, transpose(from))
end

function relayout!(into::AbstractMatrix, from::AbstractMatrix)::AbstractMatrix  # untested
    try
        into_strides = strides(into)
        into_size = size(into)
        if into_strides == (1, into_size[1]) || into_strides == (into_size[2], 1)
            return transpose!(into, transpose(from))
        end
    catch
    end
    return error("unsupported relayout into: $(typeof(into)) from: $(typeof(from))")
end

function base_sparse_matrix(matrix::Transpose)::AbstractMatrix
    return transpose(base_sparse_matrix(matrix.parent))
end

function base_sparse_matrix(matrix::NamedMatrix)::AbstractMatrix  # untested
    return base_sparse_matrix(matrix.array)
end

function base_sparse_matrix(matrix::SparseArrays.ReadOnly)::AbstractMatrix
    return base_sparse_matrix(parent(matrix))
end

function base_sparse_matrix(matrix::AbstractSparseMatrix)::AbstractMatrix
    return matrix
end

function base_sparse_matrix(matrix::AbstractMatrix)::AbstractMatrix  # untested
    return error("unsupported relayout sparse matrix: $(typeof(matrix))")
end

mutable struct FromPosition
    from_row::Int
    from_column::Int
    from_nnz::Int
end

function get_from_position_key(from_position::FromPosition)::Tuple{Int, Int}
    return (from_position.from_row, from_position.from_column)
end

function sparse_transpose_in_memory!(into::SparseMatrixCSC, from::SparseMatrixCSC)::SparseMatrixCSC
    both_nnz = nnz(from)
    from_nrows, from_ncols = size(from)
    into_nrows, into_ncols = size(into)
    @assert nnz(into) == nnz(from)
    @assert into_nrows == from_ncols
    @assert into_ncols == from_nrows
    find_position_step = Int(ceil(from_nrows * (from_ncols / both_nnz)))
    @assert find_position_step > 0

    from_positions = Vector{FromPosition}(undef, from_ncols)
    for from_column in 1:from_ncols
        from_nnz = from.colptr[from_column]
        if from_nnz == from.colptr[from_column + 1]
            from_row = from_nrows + 1
            from_nnz = both_nnz + 1
        else
            from_row = from.rowval[from_nnz]
        end
        from_positions[from_column] = FromPosition(from_row, from_column, from_nnz)
    end

    sort!(from_positions; by = get_from_position_key)

    into_column = 0
    for nnz_index in 1:both_nnz
        from_position = from_positions[1]
        into_column = insert_into_entry!(into, into_column, nnz_index, from, from_position)
        update_from_position!(from, from_position, from_nrows, both_nnz)

        if from_ncols > 1
            resort_from_positions!(from_position, from_positions, find_position_step)
        end
    end

    @assert from_positions[1].from_row == from_nrows + 1

    return into
end

function insert_into_entry!(
    into::SparseMatrixCSC,
    into_column::Int,
    into_nnz::Int,
    from::SparseMatrixCSC,
    from_position::FromPosition,
)::Int
    from_column = from_position.from_column
    from_row = from_position.from_row
    from_nnz = from_position.from_nnz

    @assert into_column <= from_row
    if into_column < from_row
        into.colptr[(into_column + 1):from_row] .= into_nnz  # NOJET
        into_column = from_row
    end
    into.rowval[into_nnz] = from_column
    into.nzval[into_nnz] = from.nzval[from_nnz]

    return into_column
end

function update_from_position!(
    from::SparseMatrixCSC,
    from_position::FromPosition,
    from_nrows::Int,
    both_nnz::Int,
)::Nothing
    from_column = from_position.from_column
    from_nnz = from_position.from_nnz

    from_nnz += 1
    if from_nnz == from.colptr[from_column + 1]
        from_position.from_row = from_nrows + 1
        from_position.from_nnz = both_nnz + 1
    else
        from_position.from_row = from.rowval[from_nnz]
        from_position.from_nnz = from_nnz
    end

    return nothing
end

function resort_from_positions!(
    from_position::FromPosition,
    from_positions::Vector{FromPosition},
    find_position_step::Int,
)::Nothing
    from_position_key = get_from_position_key(from_position)
    low_position_index = 2
    low_position_key = get_from_position_key(from_positions[low_position_index])
    if from_position_key < low_position_key
        return nothing  # untested
    end

    high_position_index =
        find_high_position_index(from_positions, low_position_index, find_position_step, from_position_key)
    if high_position_index == nothing
        from_positions_length = length(from_positions)
        copyto!(from_positions, 1, from_positions, 2, from_positions_length - 1)
        from_positions[from_positions_length] = from_position
    else
        position_range = high_position_index - low_position_index
        while position_range > 1
            mid_position_index = low_position_index + div(position_range, 2)
            mid_position_key = get_from_position_key(from_positions[mid_position_index])
            if mid_position_key < from_position_key
                low_position_index = mid_position_index
            else
                high_position_index = mid_position_index
            end
            position_range = high_position_index - low_position_index
        end
        copyto!(from_positions, 1, from_positions, 2, low_position_index - 1)
        from_positions[low_position_index] = from_position
    end

    return nothing
end

function find_high_position_index(
    from_positions::Vector{FromPosition},
    low_position_index::Int,
    find_position_step::Int,
    from_position_key::Tuple{Int, Int},
)::Maybe{Int}
    from_positions_length = length(from_positions)
    high_position_index = low_position_index
    while true
        high_position_index = min(high_position_index + find_position_step, from_positions_length)
        high_position_key = get_from_position_key(from_positions[high_position_index])

        if high_position_key > from_position_key
            return high_position_index
        end

        if high_position_index == from_positions_length
            return nothing
        end
    end
end

end # module
