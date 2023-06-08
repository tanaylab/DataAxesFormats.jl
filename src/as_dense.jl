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

struct DenseView{T, N} <: DenseArray{T, N}
    parent::AbstractArray{T, N}
end

@inline function Base.IndexStyle(::Type{<:DenseView})::Any
    return Base.IndexLinear()
end

@inline function Base.length(dense_view::DenseView)::Any
    return length(dense_view.parent)
end

@inline function Base.size(dense_view::DenseView)::Any
    return size(dense_view.parent)
end

@inline function Base.strides(dense_view::DenseView)::Any
    return strides(dense_view.parent)
end

@inline function Base.stride(dense_view::DenseView, axis::Integer)::Any
    return stride(dense_view.parent, axis)
end

@inline function Base.elsize(dense_view::Type{<:DenseView{T}})::Any where {T}
    return Base.isbitstype(T) ? sizeof(T) : (Base.isbitsunion(T) ? Base.bitsunionsize(T) : sizeof(Ptr))  # untested
end

@inline function Base.parent(dense_view::DenseView)::Any
    return dense_view.parent  # untested
end

@propagate_inbounds @inline function Base.getindex(dense_view::DenseView, indices::Integer...)::Any
    return getindex(dense_view.parent, indices...)
end

@propagate_inbounds @inline function Base.setindex!(dense_view::DenseView, value::Any, indices::Integer...)::Any
    return setindex!(dense_view.parent, value, indices...)  # untested
end

@propagate_inbounds @inline function Base.view(dense_view::DenseView, indices::Integer...)::Any
    return as_dense_if_possible(view(dense_view.parent, indices...))  # untested
end

@inline function Base.similar(dense_view::DenseView, dimensions::Integer...)::Any
    return as_dense_if_possible(similar(dense_view.parent, dimensions...))  # untested
end

@inline function Base.similar(dense_view::DenseView, type::Type, dimensions::Integer...)::Any
    return as_dense_if_possible(similar(dense_view.parent, type, dimensions...))  # untested
end

@inline function Base.reshape(dense_view::DenseView, dimensions::Integer...)::Any
    return as_dense_if_possible(reshape(dense_view.parent, dimensions...))  # untested
end

@inline function LinearAlgebra.transpose(dense_view::DenseView)::Any
    return DenseView(transpose(dense_view.parent))  # untested
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
        array_strides = strides(matrix)
        array_sizes = size(matrix)

        if (array_strides[1] == 1 && array_strides[2] == array_sizes[1]) ||
           (array_strides[1] == array_sizes[2] && array_strides[2] == 1)  # only seems untested
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
