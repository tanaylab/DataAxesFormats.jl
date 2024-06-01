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

This is much simpler than the [ArrayLayouts](https://github.com/JuliaLinearAlgebra/ArrayLayouts.jl) module which
attempts to fully describe the layout of N-dimensional arrays, a much more ambitious goal which is an overkill for our
needs.

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
export copy_array
export Columns
export inefficient_action_handler
export major_axis
export minor_axis
export other_axis
export relayout!
export require_major_axis
export require_minor_axis
export Rows

using ..GenericFunctions
using ..GenericTypes
using Distributed
using LinearAlgebra
using NamedArrays
using SparseArrays

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
    if axis === nothing
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

function major_axis(matrix::Union{Transpose, Adjoint})::Maybe{Int8}
    return other_axis(major_axis(matrix.parent))
end

function major_axis(::AbstractSparseMatrix)::Maybe{Int8}
    return Columns
end

function major_axis(::BitMatrix)::Maybe{Int8}  # untested
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
        return nothing

    catch MethodError  # NOLINT
        return nothing
    end
end

"""
    require_major_axis(matrix::AbstractMatrix)::Int8

Similar to [`major_axis`](@ref) but will `error` if the matrix isn't in either row-major or column-major layout.
"""
function require_major_axis(matrix::AbstractMatrix)::Int8
    axis = major_axis(matrix)
    if axis === nothing
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
    if axis === nothing
        return nothing
    end

    if axis == Rows || axis == Columns
        return Int8(3 - axis)
    end

    return error("invalid matrix axis: $(axis)")
end

GLOBAL_INEFFICIENT_ACTION_HANDLER::AbnormalHandler = WarnHandler

"""
    inefficient_action_handler(handler::AbnormalHandler)::AbnormalHandler

Specify the [`AbnormalHandler`](@ref) to use when accessing a matrix in an inefficient way ("against the grain").
Returns the previous handler. The default handler is `WarnHandler`.
"""
function inefficient_action_handler(handler::AbnormalHandler)::AbnormalHandler
    global GLOBAL_INEFFICIENT_ACTION_HANDLER
    previous_inefficient_action_handler = GLOBAL_INEFFICIENT_ACTION_HANDLER
    GLOBAL_INEFFICIENT_ACTION_HANDLER = handler  # NOLINT
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
    relayout!(destination::AbstractMatrix, source::AbstractMatrix)::AbstractMatrix
    relayout!(destination::AbstractMatrix, source::NamedMatrix)::NamedMatrix

Return the same `matrix` data, but in the other memory layout.

Suppose you have a column-major UMIs matrix, whose rows are cells, and columns are genes. Therefore, summing the UMIs of
a gene will be fast, but summing the UMIs of a cell will be slow. A `transpose` (no `!`) of a matrix is fast; it creates
a zero-copy wrapper of the matrix with flipped axes, so its rows will be genes and columns will be cells, but in
row-major layout. Therefore, **still**, summing the UMIs of a gene is fast, and summing the UMIs of a cell is slow.

In contrast, [`transpose!`](@ref) (with a `!`) is slow; it creates a rearranged copy of the data, also returning a matrix whose
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
`destination` is not specified, a `similar` matrix is allocated automatically for it.

!!! note

    The caller is responsible for providing a sensible `destination` matrix (sparse for a sparse `source`, dense for a
    non-sparse `source`). This can be a transposed matrix. If `source` is a `NamedMatrix`, then the result will be a
    `NamedMatrix` with the same axes. If `destination` is also a `NamedMatrix`, then its axes must match `source`.
"""
function relayout!(matrix::AbstractMatrix)::AbstractMatrix
    return transpose(LinearAlgebra.transpose!(matrix))
end

function relayout!(destination::AbstractMatrix, source::NamedMatrix)::NamedArray  # untested
    return NamedArray(relayout!(destination, source.array), source.dicts, source.dimnames)
end

function relayout!(destination::DenseMatrix, source::NamedArrays.NamedMatrix)  # untested
    return NamedArray(relayout!(destination, source.array), source.dicts, source.dimnames)
end

function relayout!(destination::Union{Transpose, Adjoint}, source::NamedMatrix)::AbstractMatrix
    relayout!(parent(destination), transpose(source.array))
    return destination
end

function relayout!(destination::SparseMatrixCSC, source::NamedMatrix)::AbstractMatrix  # untested
    relayout!(destination, source.array)
    return destination
end

function relayout!(destination::NamedArray, source::NamedMatrix)::NamedArray
    @assert destination.dimnames == source.dimnames  # NOJET
    @assert destination.dicts == source.dicts
    return NamedArray(relayout!(destination.array, source.array), source.dicts, source.dimnames)
end

function relayout!(destination::NamedArray, source::AbstractMatrix)::NamedArray
    return NamedArray(relayout!(destination.array, source), destination.dicts, destination.dimnames)
end

function relayout!(destination::Union{Transpose, Adjoint}, source::AbstractMatrix)::AbstractMatrix
    relayout!(parent(destination), transpose(source))
    return destination
end

function relayout!(destination::SparseMatrixCSC, source::AbstractMatrix)::SparseMatrixCSC
    @debug "relayout! destination: $(depict(destination)) source: $(depict(source)) {"  # NOLINT
    if size(destination) != size(source)
        error("relayout destination size: $(size(destination))\nis different from source size: $(size(source))")
    end
    if !issparse(source)
        error("relayout sparse destination: $(typeof(destination))\nand non-sparse source: $(typeof(source))")
    end
    base_from = base_sparse_matrix(source)
    transpose_base_from = transpose(base_from)
    result = LinearAlgebra.transpose!(destination, transpose_base_from)
    @debug "relayout! result: $(depict(result)) }"  # NOLINT
    return result
end

function relayout!(destination::DenseMatrix, source::AbstractMatrix)::DenseMatrix
    @debug "relayout! destination: $(depict(destination)) source: $(depict(source)) {"  # NOLINT
    if size(destination) != size(source)
        error("relayout destination size: $(size(destination))\nis different from source size: $(size(source))")
    end
    if issparse(source)
        destination .= source
    else
        LinearAlgebra.transpose!(destination, transpose(source))
    end
    @debug "relayout! result: $(depict(destination)) }"  # NOLINT
    return destination
end

function relayout!(destination::AbstractMatrix, source::AbstractMatrix)::AbstractMatrix  # untested
    @debug "relayout! destination: $(depict(destination)) source: $(depict(source)) {"  # NOLINT
    try
        into_strides = strides(destination)
        into_size = size(destination)
        if into_strides == (1, into_size[1]) || into_strides == (into_size[2], 1)
            result = LinearAlgebra.transpose!(destination, transpose(source))
            @debug "relayout! result: $(depict(result)) }"  # NOLINT
            return result
        end
    catch
    end
    return error("unsupported relayout destination: $(typeof(destination))\nand source: $(typeof(source))")
end

"""
    transpose!(matrix::AbstractMatrix)::AbstractMatrix
    transpose!(matrix::NamedMatrix)::NamedMatrix

This is a shorthand for `LinearAlgebra.transpose!(similar(transpose(m)), m)`. That is, this will return a transpose of a
matrix, but instead of simply using a zero-copy wrapper, it actually rearranges the data. See [`relayout!`](@ref).
"""
function LinearAlgebra.transpose!(matrix::AbstractMatrix)::AbstractMatrix
    @debug "transpose! $(depict(matrix)) {"  # NOLINT
    result = LinearAlgebra.transpose!(similar(transpose(matrix)), matrix)
    @debug "transpose! $(depict(result)) }"  # NOLINT
    return result
end

function LinearAlgebra.transpose!(matrix::AbstractSparseMatrix)::AbstractMatrix
    @assert require_major_axis(matrix) == Columns
    @debug "transpose! $(depict(matrix)) {"  # NOLINT
    result = SparseMatrixCSC(transpose(matrix))
    @debug "transpose! $(depict(result)) }"  # NOLINT
    return result
end

function LinearAlgebra.transpose!(matrix::NamedMatrix)::NamedArray
    return NamedArray(LinearAlgebra.transpose!(matrix.array), flip_tuple(matrix.dicts), flip_tuple(matrix.dimnames))
end

function flip_tuple(tuple::Tuple{T1, T2})::Tuple{T2, T1} where {T1, T2}
    value1, value2 = tuple
    return (value2, value1)
end

function LinearAlgebra.transpose!(matrix::SparseArrays.ReadOnly)::AbstractMatrix
    return LinearAlgebra.transpose!(parent(matrix))
end

function LinearAlgebra.transpose!(matrix::Transpose)::AbstractMatrix
    return transpose(LinearAlgebra.transpose!(parent(matrix)))
end

function LinearAlgebra.transpose!(matrix::Adjoint)::AbstractMatrix
    return adjoint(LinearAlgebra.transpose!(parent(matrix)))
end

"""
    copy_array(array::AbstractArray)::AbstractArray

Create a mutable copy of an array. This differs from `Base.copy` in the following:

  - Copying a read-only array is a mutable array. In contrast, both `Base.copy` and `Base.deepcopy` of a
    read-only array will return a read-only array, which is technically correct, but is rather pointless for
    `Base.copy`.
  - Copying will preserve the layout of the data; for example, copying a `Transpose` array is still a `Transpose` array.
    In contrast, while `Base.deepcopy` will preserve the layout, `Base.copy` will silently [`relayout!`](@ref) the matrix,
    which is both expensive and confusing.
  - Copying a sparse vector or matrix gives the same type of sparse array or matrix. Copying anything else gives a
    simple dense array regardless of the original type. This is done because a `deepcopy` of `PyArray` will still
    share the underlying buffer. Sigh.
"""
function copy_array(array::Union{SparseMatrixCSC, SparseVector})::AbstractArray
    return deepcopy(array)
end

function copy_array(array::AbstractMatrix)::Matrix
    return Matrix(array)
end

function copy_array(array::AbstractVector)::Vector
    return Vector(array)
end

function copy_array(matrix::Transpose)::Transpose
    return Transpose(copy_array(mutable_array(parent(matrix))))
end

function copy_array(matrix::Adjoint)::Adjoint
    return Adjoint(copy_array(mutable_array(parent(matrix))))
end

function copy_array(array::SparseArrays.ReadOnly)::AbstractArray
    return copy_array(mutable_array(parent(array)))
end

function copy_array(array::NamedArray)::NamedArray
    return NamedArray(copy_array(mutable_array(array.array)), array.dicts, array.dimnames)
end

function mutable_array(array::AbstractArray)::AbstractArray
    return array
end

function mutable_array(array::Transpose)::Transpose
    parent_array = parent(array)
    mutable_parent_array = mutable_array(parent_array)
    if mutable_parent_array === parent_array
        return array
    else
        return Transpose(mutable_parent_array)
    end
end

function mutable_array(array::Adjoint)::Adjoint
    parent_array = parent(array)
    mutable_parent_array = mutable_array(parent_array)
    if mutable_parent_array === parent_array
        return array
    else
        return Adjoint(mutable_parent_array)
    end
end

function mutable_array(array::SparseArrays.ReadOnly)::AbstractArray
    return parent(array)
end

function mutable_array(array::NamedArray)::NamedArray
    parent_array = array.array
    mutable_parent_array = mutable_array(parent_array)
    if mutable_parent_array === parent_array
        return array
    else
        return NamedArray(mutable_parent_array, array.dicts, array.dimnames)
    end
end

function base_sparse_matrix(matrix::Union{Transpose, Adjoint})::AbstractMatrix
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

function depict_matrix_size(matrix::AbstractMatrix, kind::AbstractString; transposed::Bool = false)::String
    layout = major_axis(matrix)
    if transposed
        layout = other_axis(layout)
    end

    if layout === nothing
        layout_suffix = "w/o major axis"  # untested
    else
        layout_suffix = "in $(axis_name(layout))"
    end

    if transposed
        return "$(size(matrix, 2)) x $(size(matrix, 1)) x $(eltype(matrix)) $(layout_suffix) ($(kind))"
    else
        return "$(size(matrix, 1)) x $(size(matrix, 2)) x $(eltype(matrix)) $(layout_suffix) ($(kind))"
    end
end

function depict end

end # module
