"""
Allow Julia to apply optimizations to dense arrays, even though their concrete type is not a `DenseArray`.

Many Julia packages provide optimized code for `DenseArray`. However, due to the restrictions of Julia's simplistic type
system, some operations return a type that is not a `DenseArray` even though it could be. For example, a `transpose` of
a `DenseArray` _should_ logically be a `DenseArray`, but instead it is a `Transpose` which is _not_ a `DenseArray`. This
is because one can transpose any `AbstractMatrix` so there's no guarantee in general that the result would be a
`DenseArray`.

This is unfortunate because Julia is supposed to allow us to achieve high performance, and `DenseArray` allows for
significant performance boots in many cases. While all(most) Julia code which is optimized for `DenseArray` also
recognizes its `Transpose`, so this specific case does not harm efficiency. However, this general type system weakness
also applies to lesser known types, for example `PyArray` which is an efficient zero-copy wrapper for `numpy` arrays
(when invoking Julia from Python code, for example in order to use `Daf`). This means that even for the very common case
where the `numpy` array is actually dense, some (many) optimizations will not kick in for it, resulting in a significant
performance loss.

To deal with such cases, this module implements `DenseView` which is a thin zero-copy wrapper around any array that is
"really" dense. Since `DenseView` _is_ a `DenseArray`, this allows all relevant optimizations to be applied to it
Something like this really should have been in a different package, possibly even part of `Base`.

The `DenseView` tries to be "sticky", in that "simple" operations (transposing, viewing or reshaping) will result with a
dense array if possible. For more complex operations (`map`, `adjoint`, etc.) you are left at the tender mercies of the
underlying array implementation. When in doubt, you can always wrap the result with [`as_dense_if_possible`](@ref).

The internal concrete `DenseView` type is not exposed; the idea is that all external code uses `DenseArray` as usual.
Instead, we provide functions to wrap and/or convert any `AbstractArray` into a `DenseArray`.
"""
module AsDense

using LinearAlgebra
using Base: @propagate_inbounds

export as_dense_if_possible
export as_dense_or_copy
export as_dense_or_fail

# The following is based on the internal `ReadOnly` implementation in the Julia standard library.

struct DenseView{T, N, P <: AbstractArray{T, N}} <: DenseArray{T, N}
    parent::P
end

Base.getproperty(dv::DenseView, symbol::Symbol) = Base.getproperty(parent(dv), symbol)

@inline Base.parent(dv::DenseView) = getfield(dv, :parent)

for method in [:length, :first, :last, :eachindex, :firstindex, :lastindex, :eltype]
    @eval @propagate_inbounds @inline Base.$method(dv::DenseView) = Base.$method(parent(dv))
end

for method in [:iterate, :axes, :getindex, :setindex!, :size, :strides]
    @eval @propagate_inbounds @inline Base.$method(dv::DenseView, args...) = Base.$method(parent(dv), args...)
end

function Base.unsafe_convert(pointer::Type{Ptr{T}}, dv::DenseView) where {T}
    return Base.unsafe_convert(pointer, parent(dv))
end

Base.elsize(::Type{DenseView{T, N, P}}) where {T, N, P} = Base.elsize(P)

for method in [:IteratorSize, :IndexStyle]
    @eval @inline Base.$method(::Type{DenseView{T, N, P}}) where {T, N, P} = Base.$method(P)
end

@inline function Base.resize!(dv::DenseView, new_length)
    return new_length == length(parent(dv)) ? dv : error("can't resize $(typeof(dv))")
end

Base.copy(dv::DenseView) = as_dense_if_possible(copy(parent(dv)))

Base.:(==)(left_dv::DenseView, right_matrix::AbstractMatrix) = parent(left_dv) == right_matrix

Base.:(==)(left_matrix::AbstractMatrix, right_dv::DenseView) = left_matrix == parent(right_dv)

Base.:(==)(left_dv::DenseView, right_dv::DenseView) = parent(left_dv) == parent(right_dv)

Base.dataids(::DenseView) = tuple()

# The rest is ours:

@propagate_inbounds @inline function Base.view(dv::DenseView, indices::Integer...)::Any
    return as_dense_if_possible(view(dv.parent, indices...))  # untested
end

@inline function Base.similar(dv::DenseView, dimensions::Integer...)::Any
    return as_dense_if_possible(similar(dv.parent, dimensions...))  # untested
end

@inline function Base.similar(dv::DenseView, type::Type, dimensions::Integer...)::Any
    return as_dense_if_possible(similar(dv.parent, type, dimensions...))  # untested
end

@inline function Base.reshape(dv::DenseView, dimensions::Integer...)::Any
    return as_dense_if_possible(reshape(dv.parent, dimensions...))  # untested
end

@inline function LinearAlgebra.transpose(dv::DenseView)::Any
    return as_dense_if_possible(transpose(dv.parent))  # untested
end

"""
    function as_dense_if_possible(vector::AbstractVector{T})::AbstractVector{T} where {T}
    function as_dense_if_possible(matrix::AbstractMatrix{T})::AbstractMatrix{T} where {T}

Given a `vector` or `matrix`, return a zero-copy `DenseVector` or `DenseMatrix` wrapper for it, if possible, otherwise
the original data.

This will return any `DenseVector` or `DenseMatrix` as-is without creating a wrapper.
"""
function as_dense_if_possible(vector::AbstractVector{T})::AbstractVector{T} where {T}
    if vector isa DenseVector
        return vector
    end

    try
        vector_strides = strides(vector)
        if vector_strides[1] == 1
            return DenseView(vector)
        end

        return vector

    catch MethodError  # only seems untested
        return vector  # untested
    end
end

function as_dense_if_possible(matrix::AbstractMatrix{T})::AbstractMatrix{T} where {T}
    if matrix isa DenseMatrix
        return matrix
    end

    try
        matrix_strides = strides(matrix)
        matrix_sizes = size(matrix)

        if (matrix_strides[1] == 1 && matrix_strides[2] == matrix_sizes[1]) ||
           (matrix_strides[1] == matrix_sizes[2] && matrix_strides[2] == 1)  # only seems untested
            return DenseView(matrix)
        end

        return matrix

    catch MethodError  # only seems untested
        return matrix
    end
end

"""
    function as_dense_or_copy(vector::AbstractVector{T})::DenseVector{T} where {T}
    function as_dense_or_copy(matrix::AbstractMatrix{T})::DenseMatrix{T} where {T}

Given any `vector` or `matrix`, return a zero-copy `DenseVector` or `DenseMatrix` wrapper
for it if possible, otherwise a dense copy of the original data.

This will return any `DenseVector` or `DenseMatrix` as-is without creating a wrapper or copying.
"""
function as_dense_or_copy(array::AbstractArray{T, N})::DenseArray{T, N} where {T, N}
    as_dense = as_dense_if_possible(array)
    if as_dense isa DenseArray
        return as_dense
    else
        return Array(as_dense)
    end
end

"""
    function as_dense_or_copy(vector::AbstractVector{T})::DenseVector{T} where {T}
    function as_dense_or_copy(matrix::AbstractMatrix{T})::DenseMatrix{T} where {T}

Given any `vector` or `matrix`, return a zero-copy `DenseVector` or `DenseMatrix` wrapper
for it if possible, otherwise fail with an `error`.

This will return any `DenseVector` or `DenseMatrix` as-is without creating a wrapper or copying.
"""
function as_dense_or_fail(array::AbstractArray{T, N})::DenseArray{T, N} where {T, N}
    as_dense = as_dense_if_possible(array)
    if as_dense isa DenseArray
        return as_dense
    else
        error("the array: $(typeof(array)) is not dense")
    end
end

end # module
