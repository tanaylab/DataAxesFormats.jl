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
export ErrorPolicy
export inefficient_action_policy
export InefficientActionPolicy
export major_axis
export minor_axis
export other_axis
export relayout!
export Rows
export WarnPolicy

using Daf.AsDense
using Distributed
using LinearAlgebra
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
    axis_name(axis::Union{Integer, Nothing})::String

Return the name of the axis (for messages).
"""
function axis_name(axis::Union{Integer, Nothing})::String
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
    major_axis(matrix::AbstractMatrix)::Union{Int8,Nothing}

Return the index of the major axis of a matrix, that is, the axis one should keep *fixed* for an efficient loop
accessing the matrix elements. If the matrix doesn't support any efficient access axis, returns `nothing`.
"""
function major_axis(::SparseMatrixCSC)
    return Columns
end

function major_axis(matrix::Transpose)
    return other_axis(major_axis(matrix.parent))
end

function major_axis(matrix::AbstractMatrix)::Union{Int8, Nothing}
    try
        matrix_strides = strides(matrix)
        matrix_sizes = size(matrix)

        if (matrix_strides[1] == 1 && matrix_strides[2] == matrix_sizes[1])
            return Columns
        end
        if (matrix_strides[1] == matrix_sizes[2] && matrix_strides[2] == 1)
            return Rows
        end

        return nothing  # untested

    catch MethodError
        return nothing  # untested
    end
end

"""
    minor_axis(matrix::AbstractMatrix)::Union{Int8,Nothing}

Return the index of the minor axis of a matrix, that is, the axis one should *vary* for an efficient loop accessing the
matrix elements. If the matrix doesn't support any efficient access axis, returns `nothing`.
"""
@inline minor_axis(matrix::AbstractMatrix) = other_axis(major_axis(matrix))

"""
    other_axis(axis::Union{Integer,Nothing})::Union{Int8,Nothing}

Return the other `matrix` `axis` (that is, convert between [`Rows`](@ref) and [`Columns`](@ref)). If given `nothing`
returns `nothing`.
"""
@inline function other_axis(axis::Union{Integer, Nothing})::Union{Int8, Nothing}
    if axis == nothing
        return nothing
    end

    if axis == Rows || axis == Columns
        return Int8(3 - axis)
    end

    return error("invalid matrix axis: $(axis)")
end

"""
The action to take when performing an operation "against the grain" of the memory layout of a matrix.

Valid values are:

`nothing` - do nothing special, just execute the code and hope for the best (the default).

`WarnPolicy` - emit a warning using `@warn`.

`ErrorPolicy` - abort the program with an error message.
"""
@enum InefficientActionPolicy WarnPolicy ErrorPolicy

GLOBAL_INEFFICIENT_ACTION_POLICY = nothing

"""
    inefficient_action_policy(
        policy::Union{InefficientActionPolicy,Nothing}
    )::Union{InefficientActionPolicy,Nothing}

Specify the `policy` to take when accessing a matrix in an inefficient way. Returns the previous policy.

!!! note

    This will affect *all* the processes `@everywhere`, not just the current one.
"""
function inefficient_action_policy(
    policy::Union{InefficientActionPolicy, Nothing},
)::Union{InefficientActionPolicy, Nothing}
    global GLOBAL_INEFFICIENT_ACTION_POLICY
    previous_inefficient_action_policy = GLOBAL_INEFFICIENT_ACTION_POLICY

    @eval @everywhere Daf.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_POLICY = $policy

    return previous_inefficient_action_policy
end

"""
    check_efficient_action(
        action::AbstractString,
        axis::Integer,
        operand::String,
        matrix::AbstractMatrix,
    )::Nothing

This will check whether the `action` about to be executed for an `operand` which is `matrix` works "with the grain" of
the data, which requires the `matrix` to be in `axis`-major layout. If it isn't, then apply the
[`inefficient_action_policy`](@ref).

In general, you *really* want operations to go "with the grain" of the data. Unfortunately, Julia (and Python, and R,
and matlab) will silently run operations "against the grain", which would be painfully slow. A liberal application of
this function will help in detecting such slowdowns, without having to resort to profiling the code to isolate the
problem.

!!! note

    This will not prevent the code from performing "against the grain" operations such as `selectdim(matrix, Rows, 1)`
    for a column-major matrix, but if you add this check before performing any (series of) operations on a matrix, then
    you will have a clear indication of whether (and where) such operations occur. You can then consider whether to
    invoke [`relayout!`](@ref) on the data, or (for data fetched from `Daf`), simply query for the other memory layout.
"""
function check_efficient_action(action::AbstractString, axis::Integer, operand::String, matrix::AbstractMatrix)::Nothing
    global GLOBAL_INEFFICIENT_ACTION_POLICY
    if major_axis(matrix) == axis || GLOBAL_INEFFICIENT_ACTION_POLICY == nothing
        return
    end

    message = (
        "the major axis: $(axis_name(axis))\n" *
        "of the action: $(action)\n" *
        "is different from the major axis: $(axis_name(major_axis(matrix)))\n" *
        "of the $(operand) matrix: $(typeof(matrix))"
    )

    if GLOBAL_INEFFICIENT_ACTION_POLICY == WarnPolicy
        @warn message

    elseif GLOBAL_INEFFICIENT_ACTION_POLICY == ErrorPolicy
        error(message)

    else
        @assert false  # untested
    end
end

"""
    relayout!(matrix::SparseMatrixCSC)::SparseMatrixCSC
    relayout!([into::DenseMatrix], matrix::DenseMatrix)::DenseMatrix

Return the same `matrix` data, but in the other memory layout.

Suppose you have a column-major UMIs matrix, whose rows are cells, and columns are genes. Therefore, summing the UMIs of
a gene will be fast, but summing the UMIs of a cell will be slow. A `transpose` (no `!`) of a matrix is fast; it creates
a zero-copy wrapper of the matrix with flipped axes, so its rows will be genes and columns will be cells, but in
row-major layout. Therefore, *still*, summing the UMIs of a gene is fast, and summing the UMIs of a cell is slow.

In contrast, `transpose!` (with a `!`) is slow; it creates a rearranged copy of the data, also returning a matrix whose
rows are genes and columns are cells, but this time, in column-major layout. Therefore, in this case summing the UMIs of
a gene will be slow, and summing the UMIs of a cell will be fast.

If you `transpose` (no `!`) the result of `transpose!` (with a `!`), you end up with a matrix that appears to be "the
same" as the original (rows are cells and columns are genes), but behaves differently - summing the UMIs of a gene will
be slow, and summing the UMIs of a cell is fast. This `transpose` of `transpose!` is a common idiom and is basically
what `relayout!` does for you. However, `relayout!` will work for both `SparseMatrixCSC` and `DenseMatrix`, and if
`into` is not specified, a `similar` matrix is allocated automatically for it.

!!! note

    It is almost always worthwhile to `relayout!` a matrix and then perform operations "with the grain" of the data,
    instead of skipping it and performing operations "against the grain" of the data. This is because (in Julia at
    least) the implementation of `transpose!` is optimized for the task, while the other operations typically don't
    provide any specific optimizations for working "against the grain" of the data. The benefits of a `relayout!` become
    even more significant when performing a series of operations (e.g., summing the gene UMIs in each cell, converting
    gene UMIs to fractions out of these totals, then computing the log base 2 of this fraction).
"""
function relayout!(matrix::SparseMatrixCSC)::SparseMatrixCSC
    return SparseMatrixCSC(transpose(matrix))
end

function relayout!(to::DenseMatrix, from::DenseMatrix)::DenseMatrix
    return transpose!(to, from)
end

function relayout!(matrix::DenseMatrix)::DenseMatrix
    return transpose!(similar(transpose(matrix)), matrix)
end

end # module
