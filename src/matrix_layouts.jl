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

export @assert_matrix
export @assert_vector
export Columns
export Rows
export axis_name
export bestify
export check_efficient_action
export copy_array
export densify
export inefficient_action_handler
export major_axis
export minor_axis
export other_axis
export relayout
export relayout!
export require_major_axis
export require_minor_axis
export sparsify
export transposer
export read_only_array
export is_read_only_array

using ..Documentation
using ..GenericFunctions
using ..GenericTypes
using ..Messages
using ..ReadOnlyArrays
using ..StorageTypes
using Distributed
using LinearAlgebra
using NamedArrays
using SparseArrays

import ..StorageTypes.indtype_for_size

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

function major_axis(::BitMatrix)::Maybe{Int8}  # UNTESTED
    return Columns
end

function major_axis(matrix::AbstractMatrix)::Maybe{Int8}
    try
        matrix_strides = strides(matrix)
        if matrix_strides[1] == 1  # NOJET
            return Columns
        end
        if matrix_strides[2] == 1
            return Rows
        end
        return nothing  # UNTESTED

    catch MethodError  # NOLINT
        return nothing  # UNTESTED
    end
end

"""
    require_major_axis(matrix::AbstractMatrix)::Int8

Similar to [`major_axis`](@ref) but will `error` if the matrix isn't in either row-major or column-major layout.
"""
function require_major_axis(matrix::AbstractMatrix)::Int8
    axis = major_axis(matrix)
    if axis === nothing
        error("type: $(typeof(matrix)) is not in any-major layout")  # UNTESTED
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
function require_minor_axis(matrix::AbstractMatrix)::Int8  # UNTESTED
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

macro assert_is_vector(source_file, source_line, vector)
    vector_name = string(vector)
    return esc(
        :(@assert $vector isa AbstractVector (
            "non-vector " *
            $vector_name *
            ": " *
            depict($vector) *
            "\nin: " *
            $(string(source_file)) *
            ":" *
            $(string(source_line))
        )),
    )
end

macro assert_vector_size(source_file, source_line, vector, n_elements)
    vector_name = string(vector)
    n_elements_name = string(n_elements)
    return esc(
        :(@assert length($vector) == $n_elements (
            "wrong size: " *
            string(length($vector)) *
            "\nof the vector: " *
            $vector_name *
            "\nis different from " *
            $n_elements_name *
            ": " *
            string($n_elements) *
            "\nin: " *
            $(string(source_file)) *
            ":" *
            $(string(source_line))
        )),
    )
end

"""
    @assert_vector(vector::Any, [n_elements::Integer])

Assert that the `vector` is an `AbstractVector` and optionally that it has `n_elements`, with a friendly error message
if it fails.
"""
macro assert_vector(vector)
    return esc(:(DataAxesFormats.MatrixLayouts.@assert_is_vector($(__source__.file), $(__source__.line), $vector)))
end

macro assert_vector(vector, n_elements)
    return esc(
        :(  #
            DataAxesFormats.MatrixLayouts.@assert_is_vector($(__source__.file), $(__source__.line), $vector);   #
            DataAxesFormats.MatrixLayouts.@assert_vector_size(
                $(__source__.file),
                $(__source__.line),
                $vector,
                $n_elements
            )  #
        ),
    )
end

macro assert_is_matrix(source_file, source_line, matrix)
    matrix_name = string(matrix)
    return esc(
        :(@assert $matrix isa AbstractMatrix (
            "non-matrix " *
            $matrix_name *
            ": " *
            depict($matrix) *
            "\nin: " *
            $(string(source_file)) *
            ":" *
            $(string(source_line))
        )),
    )
end

macro assert_matrix_size(source_file, source_line, matrix, n_rows, n_columns)
    matrix_name = string(matrix)
    n_rows_name = string(n_rows)
    n_columns_name = string(n_columns)
    return esc(
        :(@assert size($matrix) == ($n_rows, $n_columns) (
            "wrong size: " *
            string(size($matrix)) *
            "\nof the matrix: " *
            $matrix_name *
            "\nis different from (" *
            $n_rows_name *
            ", " *
            $n_columns_name *
            "): (" *
            string($n_rows) *
            ", " *
            string($n_columns) *
            ")\nin: " *
            $(string(source_file)) *
            ":" *
            $(string(source_line))
        )),
    )
end

macro check_matrix_layout(source_file, source_line, matrix, major_axis)
    matrix_name = string(matrix)
    return esc(
        :(
            DataAxesFormats.MatrixLayouts.check_efficient_action(
                $source_file,
                $source_line,
                $matrix_name,
                $matrix,
                $major_axis,
            ),
        ),
    )
end

"""
    @assert_matrix(matrix::Any, [n_rows::Integer, n_columns::Integer], [major_axis::Int8])

Assert that the `matrix` is an `AbstractMatrix` and optionally that it has `n_rows` and `n_columns`. If the `major_axis`
is given, also calls `check_efficient_action` to verify that the matrix is in an efficient layout.
"""
macro assert_matrix(matrix)
    return esc(:(DataAxesFormats.MatrixLayouts.@assert_is_matrix($(__source__.file), $(__source__.line), $matrix)))
end

macro assert_matrix(matrix, axis)
    return esc(
        :( #
            DataAxesFormats.MatrixLayouts.@assert_is_matrix($(__source__.file), $(__source__.line), $matrix); #
            DataAxesFormats.MatrixLayouts.@check_matrix_layout(
                $(string(__source__.file)),
                $(__source__.line),
                $matrix,
                $axis
            ) #
        ),
    )
end

macro assert_matrix(matrix, n_rows, n_columns)
    return esc(
        :(  #
            DataAxesFormats.MatrixLayouts.@assert_is_matrix($(__source__.file), $(__source__.line), $matrix);  #
            DataAxesFormats.MatrixLayouts.@assert_matrix_size(
                $(__source__.file),
                $(__source__.line),
                $matrix,
                $n_rows,
                $n_columns
            )  #
        ),
    )
end

macro assert_matrix(matrix, n_rows, n_columns, axis)
    return esc(
        :( #
            DataAxesFormats.MatrixLayouts.@assert_is_matrix($(__source__.file), $(__source__.line), $matrix); #
            DataAxesFormats.MatrixLayouts.@assert_matrix_size(
                $(__source__.file),
                $(__source__.line),
                $matrix,
                $n_rows,
                $n_columns
            ); #
            DataAxesFormats.MatrixLayouts.@check_matrix_layout(
                $(string(__source__.file)),
                $(__source__.line),
                $matrix,
                $axis
            ) #
        ),
    )
end

"""
    check_efficient_action(
        source_file::AbstractString,
        source_line::Integer,
        operand::AbstractString,
        matrix::AbstractMatrix,
        axis::Integer,
    )::Nothing

This will check whether the code about to be executed for an `operand` which is `matrix` works "with the grain" of the
data, which requires the `matrix` to be in `axis`-major layout. If it isn't, then apply the
[`inefficient_action_handler`](@ref). Typically this isn't invoked directly; instead use [`@assert_matrix`](@ref).

In general, you **really** want operations to go "with the grain" of the data. Unfortunately, Julia (and Python, and R,
and matlab) will silently run operations "against the grain", which would be **painfully** slow. A liberal application
of this function in your code will help in detecting such slowdowns, without having to resort to profiling the code to
isolate the problem.

!!! note

    This will not prevent the code from performing "against the grain" operations such as `selectdim(matrix, Rows, 1)`
    for a column-major matrix, but if you add this check before performing any (series of) operations on a matrix, then
    you will have a clear indication of whether such operations occur. You can then consider whether to
    invoke [`relayout!`](@ref) on the data, or (for data fetched from `Daf`), simply query for the other memory layout.
"""
function check_efficient_action(
    source_file::AbstractString,
    source_line::Integer,
    operand::AbstractString,
    matrix::AbstractMatrix,
    axis::Integer,
)::Nothing
    if major_axis(matrix) != axis
        global GLOBAL_INEFFICIENT_ACTION_HANDLER
        handle_abnormal(GLOBAL_INEFFICIENT_ACTION_HANDLER) do
            depicted = depict(matrix)
            return (dedent("""
                inefficient major axis: $(axis_name(major_axis(matrix)))
                for $(operand): $(depicted)
                in: $(source_file):$(source_line)
            """))
        end
    end
end

"""
    relayout!(destination::AbstractMatrix, source::AbstractMatrix)::AbstractMatrix
    relayout!(destination::AbstractMatrix, source::NamedMatrix)::NamedMatrix

Return the same `matrix` data, but in the other memory layout.

Suppose you have a column-major UMIs matrix, whose rows are cells, and columns are genes. Therefore, summing the UMIs of
a gene will be fast, but summing the UMIs of a cell will be slow. A `transpose` (no `!`) of a matrix is fast; it creates
a zero-copy wrapper of the matrix with flipped axes, so its rows will be genes and columns will be cells, but in
row-major layout. Therefore, **still**, summing the UMIs of a gene is fast, and summing the UMIs of a cell is slow.

In contrast, `transpose!` (with a `!`) (or [`transposer`](@ref)) is slow; it creates a rearranged copy of the data, also
returning a matrix whose rows are genes and columns are cells, but this time, in column-major layout. Therefore, in this
case summing the UMIs of a gene will be slow, and summing the UMIs of a cell will be fast.

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
function relayout!(destination::AbstractMatrix, source::NamedMatrix)::NamedArray  # UNTESTED
    return NamedArray(relayout!(destination, source.array), source.dicts, source.dimnames)
end

function relayout!(destination::DenseMatrix, source::NamedArrays.NamedMatrix)  # UNTESTED
    return NamedArray(relayout!(destination, source.array), source.dicts, source.dimnames)
end

function relayout!(destination::Union{Transpose, Adjoint}, source::NamedMatrix)::AbstractMatrix
    relayout!(parent(destination), transpose(source.array))
    return destination
end

function relayout!(destination::SparseMatrixCSC, source::NamedMatrix)::AbstractMatrix  # UNTESTED
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
    @debug "relayout! destination: $(depict(destination)) source: $(depict(source)) {"
    if size(destination) != size(source)
        error("relayout destination size: $(size(destination))\nis different from source size: $(size(source))")
    end
    if !issparse(source)
        error("relayout sparse destination: $(typeof(destination))\nand non-sparse source: $(typeof(source))")
    end
    base_from = base_sparse_matrix(source)
    transpose_base_from = transpose(base_from)
    result = LinearAlgebra.transpose!(destination, transpose_base_from)  # NOJET
    @debug "relayout! result: $(depict(result)) }"
    return result
end

function relayout!(destination::DenseMatrix, source::AbstractMatrix)::DenseMatrix
    @debug "relayout! destination: $(depict(destination)) source: $(depict(source)) {"
    if size(destination) != size(source)
        error("relayout destination size: $(size(destination))\nis different from source size: $(size(source))")
    end
    if issparse(source)
        destination .= source
    else
        LinearAlgebra.transpose!(destination, transpose(source))
    end
    @debug "relayout! result: $(depict(destination)) }"
    return destination
end

function relayout!(destination::AbstractMatrix, source::AbstractMatrix)::AbstractMatrix  # UNTESTED
    @debug "relayout! destination: $(depict(destination)) source: $(depict(source)) {"
    try
        into_strides = strides(destination)
        into_size = size(destination)
        if into_strides == (1, into_size[1]) || into_strides == (into_size[2], 1)
            result = LinearAlgebra.transpose!(destination, transpose(source))
            @debug "relayout! result: $(depict(result)) }"
            return result
        end
    catch
    end
    return error("unsupported relayout destination: $(typeof(destination))\nand source: $(typeof(source))")
end

"""
    relayout(matrix::AbstractMatrix)::AbstractMatrix
    relayout(matrix::NamedMatrix)::NamedMatrix

Same as [`relayout!`](@ref) but allocates the destination matrix for you. Is equivalent to
`transpose(transposer(matrix))`.
"""
function relayout(matrix::AbstractMatrix)::AbstractMatrix
    return transpose(transposer(matrix))
end

"""
    transposer(matrix::AbstractMatrix)::AbstractMatrix
    transposer(matrix::NamedMatrix)::NamedMatrix

Return a transpose of a matrix, but instead of simply using a zero-copy wrapper, it actually rearranges the data. See
[`relayout!`](@ref).
"""
function transposer(matrix::AbstractMatrix)::AbstractMatrix
    @debug "transposer $(depict(matrix)) {"
    axis = major_axis(matrix)
    if axis == Columns
        result = Matrix{eltype(matrix)}(undef, size(matrix, 2), size(matrix, 1))
    elseif axis == Rows
        result = transpose(Matrix{eltype(matrix)}(undef, size(matrix, 1), size(matrix, 2)))
    else
        @assert false "transposer of a matrix w/o clear layout: $(depict(matrix))"  # UNTESTED
    end
    LinearAlgebra.transpose!(result, matrix)
    @assert size(result, 1) == size(matrix, 2)
    @assert size(result, 2) == size(matrix, 1)
    @assert major_axis(result) == major_axis(matrix)
    @debug "transposer $(depict(result)) }"
    return result
end

function transposer(matrix::AbstractSparseMatrix)::AbstractMatrix
    @assert require_major_axis(matrix) == Columns
    @debug "transposer $(depict(matrix)) {"
    result = SparseMatrixCSC(transpose(matrix))
    @debug "transposer $(depict(result)) }"
    @assert size(result, 1) == size(matrix, 2)
    @assert size(result, 2) == size(matrix, 1)
    @assert major_axis(result) == major_axis(matrix)
    return result
end

function transposer(matrix::NamedMatrix)::NamedArray
    return NamedArray(transposer(matrix.array), flip_tuple(matrix.dicts), flip_tuple(matrix.dimnames))
end

function flip_tuple(tuple::Tuple{T1, T2})::Tuple{T2, T1} where {T1, T2}
    value1, value2 = tuple
    return (value2, value1)
end

function transposer(matrix::SparseArrays.ReadOnly)::AbstractMatrix
    return transposer(parent(matrix))
end

function transposer(matrix::Transpose)::AbstractMatrix
    return transpose(transposer(parent(matrix)))
end

function transposer(matrix::Adjoint)::AbstractMatrix
    return adjoint(transposer(parent(matrix)))
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
  - Copying a vector of anything derived from `AbstractString` returns a vector of `AbstractString`.
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

function copy_array(array::AbstractVector{<:AbstractString})::Vector{AbstractString}
    return Vector{AbstractString}(array)  # NOJET
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

function base_sparse_matrix(matrix::Union{Transpose, Adjoint})::AbstractMatrix
    return transpose(base_sparse_matrix(matrix.parent))
end

function base_sparse_matrix(matrix::NamedMatrix)::AbstractMatrix  # UNTESTED
    return base_sparse_matrix(matrix.array)
end

function base_sparse_matrix(matrix::SparseArrays.ReadOnly)::AbstractMatrix
    return base_sparse_matrix(parent(matrix))
end

function base_sparse_matrix(matrix::AbstractSparseMatrix)::AbstractMatrix
    return matrix
end

function base_sparse_matrix(matrix::AbstractMatrix)::AbstractMatrix  # UNTESTED
    return error("unsupported relayout sparse matrix: $(typeof(matrix))")
end

function Messages.depict(vector::AbstractVector)::String
    return Messages.depict_array(vector, depict_vector(vector, ""))
end

function depict_vector(vector::SparseArrays.ReadOnly, prefix::AbstractString)::String
    return depict_vector(parent(vector), Messages.concat_prefixes(prefix, "ReadOnly"))
end

function depict_vector(vector::NamedArray, prefix::AbstractString)::String
    return depict_vector(vector.array, Messages.concat_prefixes(prefix, "Named"))
end

function depict_vector(vector::DenseVector, prefix::AbstractString)::String
    return depict_vector_size(vector, Messages.concat_prefixes(prefix, "Dense"))
end

function depict_vector(vector::SparseVector, prefix::AbstractString)::String
    nnz = depict_percent(length(vector.nzval), length(vector))
    return depict_vector_size(vector, Messages.concat_prefixes(prefix, "Sparse $(SparseArrays.indtype(vector)) $(nnz)"))
end

function depict_vector(vector::AbstractVector, prefix::AbstractString)::String  # UNTESTED
    try
        if strides(vector) == (1,)
            return depict_vector_size(vector, Messages.concat_prefixes(prefix, "$(nameof(typeof(vector))) - Dense"))
        else
            return depict_vector_size(vector, Messages.concat_prefixes(prefix, "$(nameof(typeof(vector))) - Strided"))
        end
    catch
        return depict_vector_size(vector, Messages.concat_prefixes(prefix, "$(nameof(typeof(vector)))"))
    end
end

function depict_vector_size(vector::AbstractVector, kind::AbstractString)::String
    return "$(length(vector)) x $(eltype(vector)) ($(kind))"
end

function Messages.depict(transposed::Transpose)::String
    parent = transposed.parent
    if parent isa AbstractVector
        return depict_vector(parent, "Transpose")  # UNTESTED
    elseif parent isa AbstractMatrix
        return depict_matrix(transposed.parent, "Transpose"; transposed = true)
    else
        @assert false
    end
end

function Messages.depict(matrix::AbstractMatrix)::String
    return Messages.depict_array(matrix, depict_matrix(matrix, ""; transposed = false))
end

function depict_matrix(matrix::SparseArrays.ReadOnly, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(parent(matrix), Messages.concat_prefixes(prefix, "ReadOnly"); transposed)
end

function depict_matrix(matrix::NamedMatrix, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(matrix.array, Messages.concat_prefixes(prefix, "Named"); transposed)
end

function depict_matrix(matrix::Transpose, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(parent(matrix), Messages.concat_prefixes(prefix, "Transpose"); transposed = !transposed)
end

function depict_matrix(matrix::Adjoint, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(parent(matrix), Messages.concat_prefixes(prefix, "Adjoint"); transposed = !transposed)
end

function depict_matrix(matrix::DenseMatrix, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix_size(matrix, Messages.concat_prefixes(prefix, "Dense"); transposed)
end

function depict_matrix(matrix::SparseMatrixCSC, prefix::AbstractString; transposed::Bool = false)::String
    nnz = depict_percent(length(matrix.nzval), length(matrix))
    return depict_matrix_size(
        matrix,
        Messages.concat_prefixes(prefix, "Sparse $(SparseArrays.indtype(matrix)) $(nnz)");
        transposed,
    )
end

function depict_matrix(matrix::AbstractMatrix, ::AbstractString; transposed::Bool = false)::String  # UNTESTED
    try
        matrix_strides = strides(matrix)
        matrix_sizes = size(matrix)
        if matrix_strides == (1, matrix_sizes[1]) || matrix_strides == (matrix_sizes[2], 1)
            return depict_matrix_size(matrix, "$(nameof(typeof(matrix))) - Dense"; transposed)
        else
            return depict_matrix_size(matrix, "$(nameof(typeof(matrix))) - Strided"; transposed)
        end
    catch
        return depict_matrix_size(matrix, "$(nameof(typeof(matrix)))"; transposed)
    end
end

function depict_matrix_size(matrix::AbstractMatrix, kind::AbstractString; transposed::Bool = false)::String
    layout = major_axis(matrix)
    if transposed
        layout = other_axis(layout)
    end

    if layout === nothing
        layout_suffix = "w/o major axis"  # UNTESTED
    else
        layout_suffix = "in $(axis_name(layout))"
    end

    if transposed
        return "$(size(matrix, 2)) x $(size(matrix, 1)) x $(eltype(matrix)) $(layout_suffix) ($(kind))"
    else
        return "$(size(matrix, 1)) x $(size(matrix, 2)) x $(eltype(matrix)) $(layout_suffix) ($(kind))"
    end
end

"""
    sparsify(matrix::AbstractMatrix{T}; copy::Bool = false)::AbstractMatrix{T} where {T <: StorageReal}
    sparsify(vector::AbstractVector{T}; copy::Bool = false)::AbstractVector{T} where {T <: StorageReal}

Return a sparse version of an array. This will preserve the matrix layout. If `copy`, this will create a copy even if it
is already sparse.
"""
function sparsify(matrix::AbstractMatrix{T}; copy::Bool = false)::AbstractMatrix{T} where {T <: StorageReal}  # NOLINT
    return SparseMatrixCSC(matrix)
end

function sparsify(
    matrix::Union{Transpose{T}, Adjoint{T}};
    copy::Bool = false,
)::AbstractMatrix{T} where {T <: StorageReal}
    return typeof(matrix)(sparsify(parent(matrix); copy))
end

function sparsify(matrix::NamedMatrix{T}; copy::Bool = false)::NamedArray{T} where {T <: StorageReal}  # NOLINT
    return NamedArray(sparsify(matrix.array), matrix.dicts, matrix.dimnames)  # NOJET
end

function sparsify(vector::AbstractVector{T}; copy::Bool = false)::AbstractVector{T} where {T <: StorageReal}  # NOLINT
    return SparseVector(vector)
end

function sparsify(vector::NamedVector{T}; copy::Bool = false)::NamedArray{T} where {T <: StorageReal}  # NOLINT
    return NamedArray(sparsify(vector.array), vector.dicts, vector.dimnames)
end

function sparsify(array::AbstractSparseArray{T}; copy::Bool = false)::AbstractSparseArray{T} where {T <: StorageReal}
    if copy
        array = copy_array(array)
    end
    return array
end

"""
    densify(matrix::AbstractMatrix{T}; copy::Bool = false)::AbstractMatrix{T} where {T <: StorageReal}
    densify(vector::AbstractVector{T}; copy::Bool = false)::AbstractVector{T} where {T <: StorageReal}

Return a dense version of an array. This will preserve matrix layout. If `copy`, this will create a copy even if it is
already dense.
"""
function densify(matrix::AbstractMatrix{T}; copy::Bool = false)::AbstractMatrix{T} where {T <: StorageReal}
    if copy || major_axis(matrix) == Nothing
        matrix = Matrix(matrix)
    end
    return matrix
end

function densify(
    matrix::Union{Transpose{T}, Adjoint{T}};
    copy::Bool = false,
)::AbstractMatrix{T} where {T <: StorageReal}
    return typeof(matrix)(densify(parent(matrix); copy))
end

function densify(matrix::NamedMatrix{T}; copy::Bool = false)::AbstractMatrix{T} where {T <: StorageReal}  # NOLINT
    return NamedArray(densify(matrix.array), matrix.dicts, matrix.dimnames)
end

function densify(matrix::AbstractSparseMatrix{T}; copy::Bool = false)::AbstractMatrix{T} where {T <: StorageReal}  # NOLINT
    return Matrix(matrix)
end

function densify(vector::AbstractVector{T}; copy::Bool = false)::AbstractVector{T} where {T <: StorageReal}
    if copy
        vector = Vector(vector)
    end
    return vector
end

function densify(vector::AbstractSparseVector{T}; copy::Bool = false)::AbstractVector{T} where {T <: StorageReal}  # NOLINT
    return Vector(vector)
end

function densify(vector::NamedVector{T}; copy::Bool = false)::AbstractVector{T} where {T <: StorageReal}  # NOLINT
    return NamedArray(densify(vector.array), vector.dicts, vector.dimnames)
end

"""
    bestify(
        matrix::AbstractMatrix{T};
        copy::Bool = false,
        sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
    )::AbstractMatrix{T} where {T <: StorageReal}
    bestify(
        matrix::AbstractVector{T};
        copy::Bool = false,
        sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
    )::AbstractVector{T} where {T <: StorageReal}

Return a "best" (dense or sparse) version of an array. The sparse format is chosen if it saves at least
`sparse_if_saves_storage_fraction` of the storage of the dense format. If `copy`, this will create a copy even if it is
already in the best format.

!!! note

    If not `copy` and the matrix is already sparse, we do not change the integer index type, even though this may save
    space.
"""
@documented function bestify(
    matrix::AbstractMatrix{T};
    copy::Bool = false,
    sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
)::AbstractMatrix{T} where {T <: StorageReal}
    @debug "bestify $(depict(matrix)) {"
    @assert 0 < sparse_if_saves_storage_fraction < 1
    if sparse_matrix_saves_storage_fraction(matrix; copy) >= sparse_if_saves_storage_fraction
        result = sparsify(matrix; copy)
    else
        result = densify(matrix; copy)
    end
    @debug "bestify $(depict(result)) }"
    return result
end

@documented function bestify(
    named::Union{NamedVector{T}, NamedMatrix{T}};
    copy::Bool = false,
    sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
)::AbstractArray{T} where {T <: StorageReal}
    result = bestify(named.array; copy, sparse_if_saves_storage_fraction)
    if result === named.array
        return named
    else
        return NamedArray(result, named.dicts, named.dimnames)
    end
end

function bestify(
    vector::AbstractVector{T};
    copy::Bool = false,
    sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
)::AbstractVector{T} where {T <: StorageReal}
    @debug "bestify $(depict(vector)) {"
    @assert 0 < sparse_if_saves_storage_fraction < 1
    if sparse_vector_saves_storage_fraction(vector; copy) >= sparse_if_saves_storage_fraction
        result = sparsify(vector; copy)
    else
        result = densify(vector; copy)
    end
    @debug "bestify $(depict(result)) }"
    return result
end

function sparse_matrix_saves_storage_fraction(
    matrix::AbstractSparseMatrix{T, I};
    copy::Bool,
)::Float64 where {T <: StorageReal, I <: StorageInteger}
    n_rows, n_columns = size(matrix)
    dense_bytes = n_rows * n_columns * sizeof(T)

    if copy
        indtype = indtype_for_size(n_rows * n_columns)
    else
        indtype = I
    end

    n_nz = nnz(matrix)
    sparse_bytes = n_nz * (sizeof(T) + sizeof(indtype)) + (n_columns + 1) * sizeof(indtype)

    saved_fraction = (dense_bytes - sparse_bytes) / dense_bytes
    @debug "(sparse) dense_bytes: $(dense_bytes) sparse_bytes: $(sparse_bytes) saved_fraction: $(saved_fraction)"
    return saved_fraction
end

function sparse_matrix_saves_storage_fraction(matrix::AbstractMatrix; copy::Bool)::Float64  # NOLINT
    n_rows, n_columns = size(matrix)
    dense_bytes = n_rows * n_columns * sizeof(eltype(matrix))

    n_nz = sum(matrix .!= 0)
    indtype = indtype_for_size(n_rows * n_columns)
    sparse_bytes = n_nz * (sizeof(eltype(matrix)) + sizeof(indtype)) + (n_columns + 1) * sizeof(indtype)

    saved_fraction = (dense_bytes - sparse_bytes) / dense_bytes
    @debug "(dense?) dense_bytes: $(dense_bytes) sparse_bytes: $(sparse_bytes) saved_fraction: $(saved_fraction)"
    return saved_fraction
end

function sparse_vector_saves_storage_fraction(
    vector::AbstractSparseVector{T, I};
    copy::Bool,
)::Float64 where {T <: StorageReal, I <: StorageInteger}
    size = length(vector)
    dense_bytes = size * sizeof(T)

    if copy
        indtype = indtype_for_size(size)
    else
        indtype = I
    end

    n_nz = sum(vector .!= 0)
    sparse_bytes = n_nz * (sizeof(T) + sizeof(indtype))

    saved_fraction = (dense_bytes - sparse_bytes) / dense_bytes
    @debug "(sparse) dense_bytes: $(dense_bytes) sparse_bytes: $(sparse_bytes) saved_fraction: $(saved_fraction)"
    return saved_fraction
end

function sparse_vector_saves_storage_fraction(vector::AbstractVector; copy::Bool)::Float64  # NOLINT
    size = length(vector)
    dense_bytes = size * sizeof(eltype(vector))

    n_nz = sum(vector .!= 0)
    indtype = indtype_for_size(size)
    sparse_bytes = n_nz * (sizeof(eltype(vector)) + sizeof(indtype))

    saved_fraction = (dense_bytes - sparse_bytes) / dense_bytes
    @debug "(dense?) dense_bytes: $(dense_bytes) sparse_bytes: $(sparse_bytes) saved_fraction: $(saved_fraction)"
    return saved_fraction
end

# WHY do we have to define these ourselves... Sigh.

function SparseArrays.indtype(read_only::SparseArrays.ReadOnly)::Type  # UNTESTED
    return SparseArrays.indtype(parent(read_only))
end

function SparseArrays.nnz(read_only::SparseArrays.ReadOnly)::Integer
    return SparseArrays.nnz(parent(read_only))
end

# These we can excuse...

function SparseArrays.indtype(named::NamedArray)::Type  # UNTESTED
    @assert issparse(named.array)
    return SparseArrays.indtype(named.array)
end

function SparseArrays.nnz(named::NamedArray)::Integer
    @assert issparse(named.array)
    return SparseArrays.nnz(named.array)
end

function colptr(read_only::SparseArrays.ReadOnly)::AbstractVector
    return colptr(parent(read_only))
end

function colptr(matrix::SparseMatrixCSC)::AbstractVector
    return matrix.colptr
end

function colptr(named::NamedArray)::AbstractVector
    @assert issparse(named.array)
    return colptr(named.array)
end

function nzind(read_only::SparseArrays.ReadOnly)::AbstractVector  # UNTESTED
    return nzind(parent(read_only))
end

function nzind(vector::SparseVector)::AbstractVector
    return vector.nzind
end

function nzind(named::NamedArray)::AbstractVector  # UNTESTED
    @assert issparse(named.array)
    return nzind(named.array)
end

function nzval(vector::SparseVector)::AbstractVector
    return vector.nzval
end

function nzval(matrix::SparseMatrixCSC)::AbstractVector
    return matrix.nzval
end

function nzval(read_only::SparseArrays.ReadOnly)::AbstractVector  # UNTESTED
    return nzval(parent(read_only))
end

function nzval(named::NamedArray)::AbstractVector
    @assert issparse(named.array)
    return nzval(named.array)
end

function rowval(matrix::SparseMatrixCSC)::AbstractVector
    return matrix.rowval
end

function rowval(read_only::SparseArrays.ReadOnly)::AbstractVector  # UNTESTED
    return rowval(parent(read_only))
end

function rowval(named::NamedArray)::AbstractVector
    @assert issparse(named.array)
    return rowval(named.array)
end

end # module
