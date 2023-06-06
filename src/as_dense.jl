"""
Allow Julia to apply optimizations to dense arrays, even though their concrete type is not `DenseArray`.

Many Julia packages provide optimized code for `DenseArray`. However, due to the restrictions of Julia's simplistic type
system, some operations return a type that is not a `DenseArray` even though it could be. For example, a `transpose` of
a `DenseArray` _should_ logically be a `DenseArray`, but instead it is a `Transpose` which is _not_ a `DenseArray`. This
is because one can transpose any `AbstractMatrix` so there's no guarantee in general that the result would be a
`DenseArray`.

In theory Julia could have provided a `DenseTranspose` type and return that when transposing a `DenseArray`. Of course
transposing is just one example; such an approach would have required doubling the number of types created in many
packages, which is not practical.

This is unfortunate because Julia is supposed to allow us to achieve high performance, and `DenseArray` allows for
significant performance boots in many cases. Ideally, Julia would evolve its type system to handle such issues, but
this will not happen any time soon (if ever).

In the meanwhile, this module implements `DenseView` which is a thin zero-copy wrapper around any array that is "really"
dense. Since `DenseView` _is_ a `DenseArray`, this allows all relevant optimizations to be applied to it. A `DenseView`
works by directly accessing the (contiguous) storage of the underlying array; to ensure this memory is not claimed by
the garbage collector, the `DenseView` also holds a reference to the wrapped array object.

The internal concrete `DenseView` type is not exposed; the idea is that all external code uses `DenseArray` as usual.
Instead, we two functions to wrap and/or convert any `AbstractArray` into a `DenseArray`:

The `DenseView` tries to be "sticky", in that "simple" operations (transposing, viewing or reshaping) will result with a
dense array if possible. For more complex operations (`map`, `adjoint`, etc.) you are left at the tender mercies of the
underlying array implementation. When in doubt, you can always wrap the result with [`as_dense_if_possible`](@ref).

This really should have been in a different package, possibly even part of `Base`.
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
    function as_dense_if_possible(array::AbstractArray{T, N})::AbstractArray{T, N} where {T, N}

Given any `array`, return a `DenseArray` sharing the same memory if possible, or the original `array` otherwise.

This will return any `DenseArray` as-is without creating a wrapper.
"""
function as_dense_if_possible(array::AbstractArray{T, N})::AbstractArray{T, N} where {T, N}
    if array isa DenseArray
        return array
    end
    try
        array_strides = strides(array)
        array_sizes = size(array)
        if (array_strides[1] == 1 && array_strides[2] == array_sizes[1]) ||
           (array_strides[1] == array_sizes[2] && array_strides[2] == 1)  # only seems untested
            return DenseView(array)
        else
            return array
        end
    catch MethodError  # only seems untested
        return array
    end
end

"""
    function as_dense_or_copy(array::AbstractArray{T, N})::DenseArray{T, N} where {T, N}

Given any `array`, return a `DenseArray` sharing the same memory if possible, or a dense `Array` copy of the data.

This will return any `DenseArray` as-is without creating a wrapper or doing any copying.
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
    function as_dense_or_fail(array::AbstractArray{T, N})::DenseArray{T, N} where {T, N}

Given any `array`, return a `DenseArray` sharing the same memory if possible, or fail with an error message.

This will return any `DenseArray` as-is without creating a wrapper or doing any copying.
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
