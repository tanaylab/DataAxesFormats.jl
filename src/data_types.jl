"""
Only a restricted set of scalar, matrix and vector types is supported by `Daf`.

The set of scalar types is restricted because we need to be able to store them in disk files. This rules out compound
types such as `Dict`. This isn't an issue for vector and matrix elements but is sometimes bothersome for "scalar" data
(not associated with any axis). If you find yourself needed to store such data, you'll have to serialize it to a string.
By convention, we use `JSON` blobs for such data.

Julia supports a potential infinite variety of ways to represent matrices and vectors. `Daf` is intentionally
restricted to specific representations. This has several advantages:

  - `Daf` storage formats need only implement storing these restricted representations, which lend themselves to simple
    storage in consecutive bytes (in memory and/or on disk). These representations also allow for memory-mapping the
    data from disk files, which allows `Daf` to deal with data sets larger than the available memory.

  - Client code need only worry about dealing with these restricted representations, which limits the amount of code
    paths required for efficient algorithm implementations. However, you (mostly) need not worry about this when
    invoking library functions, which have code paths covering all common matrix types. You *do* need to consider the
    layout of the data, though (see below).

This has the downside that `Daf` doesn't support efficient storage of specialized matrices (to pick a random example,
upper triangular matrices). This isn't a great loss, since `Daf` targets storing arbitrary scientific data (especially
biological data), which in general is not of any such special shape.

The upside is that all matrices stored and returned by `Daf` have a clear column-major layout (regardless of whether
they are dense or sparse). That is, the values of each column are laid out consecutively in memory (each column is a
single contiguous vector), so any operation that works on whole columns will be fast (e.g., summing the value of each
column). In contrast, the values of each row are stored very far apart from each other, so any operation that works on
whole rows will be very slow in comparison (e.g., summing the value of each row).

If a matrix has the opposite layout (row-major instead of column-major), then this will be reversed (summing rows would
be efficient, and summing columns would be slow). In general, you would want operations to go "with the grain" instead
of "against the grain" of the data. Unfortunately, Julia (and Python, and R, and matlab) will silently run operations
"against the grain", which would be painfully slow. This will require manual profiling to detect the problem.

We provide a few convenience functions for detecting whether a specific matrix instance is in column-major or row-major
layout, and in general provide symbolic names to row- and column-oriented operations for readability, rather than using
the opaque axis indices `1` and `2`. We also provide functions you can use in your code to (optionally) complain if an
operation is applied "against the grain" of the data, so the caller will get some indication of the problem (instead of
the program silently running much slower than it should). However these will not protect one against directly invoking
low-level Julia operations "against the grain" of the data. Sigh.

The bottom line is that, when storing and fetching data in `Daf`, one has to carefully pick which axis to use for
columns, and which axis to use for rows, to ensure operations will be applied "with the grain" of the data. Internally,
`Daf` typically stores just one variant of the data (with a particular axes order). If asked for the other layout (by
flipping the requested axes), then `Daf` will `transpose!` the data, and cache the results (in memory) for future use.
You can also explicitly store both layouts in `Daf`, in which case the right one will be fetched. In this case, it would
be your responsibility to ensure both versions of the data contain the same values. This is a hassle, but it is
worthwhile when working with large data sets.

Note that `transpose!` is very different from `transpose`. A `transpose` (no `!`) of a matrix is a zero-copy wrapper
which merely flips the axes, so a `transpose` of a column-major matrix is a row-major matrix (and vice-versa). In
contrast, `transpose!` (with a `!`) of a column-major matrix makes a copy of the data (with flipped axes), rearranging
the values so the result is also in column-major format (and similarly for row-major data). For large matrices, this is
an expensive operation (by definition, it goes "against the grain" of the data). We actually use [`relayout!`](@ref),
which also works for `SparseMatrixCSC`, but is just a thin wrapper for `transpose!` for a `DenseMatrix`.

It is typically worthwhile to `relayout!` (or `transpose!`) a matrix and then, for example, sum the values in each
column "with the grain" of the data, rather than to directly sum the values in each row "against the grain" of the
original data. This is even more true if one performs several consecutive operations "against the grain" of the data
(e.g., summing the values in each row, and then normalizing each row by dividing it by its sum). Note that, as of
writing this, Julia's `transpose!` (and therefore, `relayout!`) implementation for dense data is much more efficient
than the equivalent in `numpy`, so you are better off asking `Daf` for the data in the right layout, instead of
converting it yourself in Python after the fact.

Finally, we use column-major layout because that's the default in Julia, which inherits this from matlab, which
inherits this from FORTRAN, allegedly because this is a bit more convenient for linear algebra operations. In contrast,
Python `numpy` uses row-major layout by default. In either case, this is just an arbitrary convention, and all systems
work just fine with data of either memory layout; the key consideration is to keep track of the layout, and to apply
operations "with the grain" rather than "against the grain" of the data.
"""
module DataTypes

export axis_name
export check_efficient_action
export Columns
export ErrorPolicy
export inefficient_action_policy
export InefficientActionPolicy
export is_storage_matrix
export major_axis
export minor_axis
export other_axis
export relayout!
export require_storage_matrix
export require_storage_vector
export Rows
export StorageMatrix
export StorageScalar
export StorageVector
export WarnPolicy

using ArrayLayouts
using Distributed
using LinearAlgebra
using SparseArrays

import Distributed.@everywhere

"""
Types that can be used as scalars, or elements in stored matrices or vectors.

This is restricted to numbers (including Booleans) and strings. It is arguably too restrictive, as in principle we could
support any arbitrary `isbitstype`. However, in practice this would cause much trouble when accessing the data from
other systems (specifically Python and R). Since `Daf` targets storing arbitrary scientific data (especially biological
data), this restriction seems reasonable.
"""
StorageScalar = Union{String, Number}

"""
Matrices that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files.

The storable matrix types are:

  - Any `DenseMatrix` type, that is, any matrix stored as a contiguous memory region, *as long as it is in column-major
    layout*. This isn't a concrete type; while Julia's `Matrix` type `isa DenseMatrix`, there are other types that are
    always, or can sometimes contain, dense matrices. Note that not all `DenseMatrix` need be in column-major format (they
    may be in row-major format instead); in this case we will store the zero-copy `transpose` of the matrix (flip the
    axes).

    The `DenseMatrix` type is important because it naturally lends itself to being stored on disk using memory mapped
    files, which is much more efficient than reading and writing (that is, copying) the data. In addition, operations on
    dense data triggers a host of optimizations in almost any Julia code that deals with arrays.

  - The `SparseMatrixCSC` type, that is, a sparse matrix stored in columns. This is a concrete type, which stores
    three (dense) internal vectors: the row index of each non-zero element, the starting offset of each column, and of
    course the values of the non-zero elements. This is the "standard" sparse matrix format in Julia, and is easy
    to store (and memory map), by storing three separate dense vectors.

Note we require `StorageMatrix` data to be in column-major layout. Therefore, use [`is_storage_matrix`](@ref) instead of
(or in addition to) `isa StorageMatrix` to detect whether a matrix is a valid `StorageMatrix` or not.
"""
StorageMatrix{T} = Union{DenseMatrix{T}, SparseMatrixCSC{T}} where {T <: StorageScalar}

"""
Vectors that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files.

The storable vector types are:

  - Any `DenseVector` type, that is, any vector stored as a contiguous memory region. This isn't a concrete type;
    while Julia's `Vector` type `isa DenseVector`, there are other types that are always, or can sometimes contain, dense
    matrices.

  - The `SparseVector` type. This is a concrete type, which stores two dense vectors - the index of each non-zero element,
    and the values of the non-zero elements (for Boolean data the second vector can be omitted). This is the natural
    choice given it is what we get when we slice a `SparseMatrixCSC`.

Here, mercifully, we can just use `isa StorageVector` to detect whether some data is a valid `StorageVector`.
"""
StorageVector{T} = Union{DenseVector{T}, SparseVector{T}} where {T <: StorageScalar}

"""
    is_storage_matrix(data::SparseMatrixCSC)::Bool

Test whether some `data` is a valid [`StorageMatrix`](@ref).

Alas, just `isa StorageMatrix` does *not* work, because Julia's type system does not distinguish between column-major
and row-major `DenseMatrix` data.
"""
function is_storage_matrix(data::SparseMatrixCSC)::Bool
    return true
end

function is_storage_matrix(data::DenseMatrix)::Bool
    return major_axis(data) == Columns
end

function is_storage_matrix(data::Any)::Bool
    return false
end

"""
    require_storage_matrix(matrix::Any)

Ensure that the `matrix` is a valid [`StorageMatrix`](@ref), or raise an `error`.
"""
function require_storage_matrix(matrix::Any)::Nothing
    return error("type: $(typeof(matrix)) is not a valid Daf.StorageMatrix")
end

function require_storage_matrix(matrix::StorageMatrix)::Nothing
    if !is_storage_matrix(matrix)
        error("matrix: $(typeof(matrix)) is not a column-major Daf.StorageMatrix")  # untested
    end

    return nothing
end

"""
    require_storage_vector(vector::Any)

Ensure that the `vector` is a valid [`StorageVector`](@ref), or raise an `error`.
"""
function require_storage_vector(vector::Any)::Nothing
    if !(vector isa StorageVector)
        error("type: $(typeof(vector)) is not a valid Daf.StorageVector")
    end

    return nothing
end

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

Return the index of the major axis of a matrix, that is, the axis one should use for the *inner* loop for the most
efficient access to the matrix elements. If the matrix doesn't support any efficient access axis, returns `nothing`.
"""
@inline major_axis(::SparseMatrixCSC) = Columns
@inline major_axis(matrix::Transpose) = other_axis(major_axis(matrix.parent))
@inline major_axis(matrix::AbstractMatrix) = axis_of_layout(MemoryLayout(matrix))

@inline axis_of_layout(::AbstractColumnMajor) = Columns
@inline axis_of_layout(::AbstractRowMajor) = Rows
@inline axis_of_layout(::MemoryLayout) = nothing

"""
    minor_axis(matrix::AbstractMatrix)::Union{Int8,Nothing}

Return the index of the minor axis of a matrix, that is, the axis one should use for the *outer* loop for the most
efficient access to the matrix elements. If the matrix doesn't support any efficient access axis, returns `nothing`.
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

Valid values are `nothing` - do nothing special, just execute the code and hope for the best (the default), `WarnPolicy`

  - emit a warning using `@warn`, and `ErrorPolicy` - abort the program with an error message.
"""
@enum InefficientActionPolicy WarnPolicy ErrorPolicy

GLOBAL_INEFFICIENT_ACTION_POLICY = nothing

"""
    inefficient_action_policy(
        policy::Union{InefficientActionPolicy,Nothing}
    )::Union{InefficientActionPolicy,Nothing}

Specify the `policy` to take when accessing a matrix in an inefficient way. Returns the previous policy.

Note this will affect *all* the processes, not just the current one.
"""
function inefficient_action_policy(
    policy::Union{InefficientActionPolicy, Nothing},
)::Union{InefficientActionPolicy, Nothing}
    global GLOBAL_INEFFICIENT_ACTION_POLICY
    previous_inefficient_action_policy = GLOBAL_INEFFICIENT_ACTION_POLICY

    @eval @everywhere Daf.DataTypes.GLOBAL_INEFFICIENT_ACTION_POLICY = $policy

    return previous_inefficient_action_policy
end

"""
    check_efficient_action(
        action::AbstractString,
        axis::Integer,
        operand::String,
        matrix::AbstractMatrix,
    )::Nothing

Check whether the `action` about to be executed for an `operand` which is `matrix` works "with the grain" of the data,
which requires the `matrix` to be in `axis`-major layout. If it isn't, then apply the
[`inefficient_action_policy`](@ref).

This will not protect you against performing "against the grain" operations such as `selectdim(matrix, Rows, 1)` for a
column-major matrix. It is meant to be added in your own code before such actions, to verify you will not apply them
"against the grain" of the data.
"""
function check_efficient_action(action::AbstractString, axis::Integer, operand::String, matrix::AbstractMatrix)::Nothing
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

This differs from `transpose!` in that it works for both `SparseMatrixCSC` and `DenseMatrix`, and that if `into` is not
specified, a `similar` matrix is allocated automatically for it.
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
