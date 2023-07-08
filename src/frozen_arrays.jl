"""
Prevent an array from modification. This is used when returning vector or matrix data from a frozen (read-only) `Daf`
storage, to protect against accidental modification of the data.

The `FrozenView` tries to be "sticky", in that "simple" operations (transposing, viewing or reshaping) will result
with a read-only array if that makes sense.

The internal concrete `FrozenView` type is not exposed. Instead, we provide functions to wrap and/or convert any
`AbstractArray` into a `FrozenView`.

This probably belongs in a separate package (or `Base`).
"""
module FrozenArrays

export frozen

using LinearAlgebra

import Base.@propagate_inbounds

struct FrozenView{T, N, P <: AbstractArray{T, N}} <: DenseArray{T, N}
    parent::P
end

Base.getproperty(dv::FrozenView, symbol::Symbol) = Base.getproperty(parent(dv), symbol)

@inline Base.parent(dv::FrozenView) = getfield(dv, :parent)

for method in [:length, :first, :last, :eachindex, :firstindex, :lastindex, :eltype]
    @eval @propagate_inbounds @inline Base.$method(dv::FrozenView) = Base.$method(parent(dv))
end

for method in [:iterate, :axes, :getindex, :size, :strides, :similar]
    @eval @propagate_inbounds @inline Base.$method(dv::FrozenView, args...) = Base.$method(parent(dv), args...)
end

function Base.unsafe_convert(pointer::Type{Ptr{T}}, dv::FrozenView)::Ptr{T} where {T}  # untested
    return Base.unsafe_convert(pointer, parent(dv))
end

Base.elsize(::Type{FrozenView{T, N, P}}) where {T, N, P} = Base.elsize(P)

for method in [:IteratorSize, :IndexStyle]
    @eval @inline Base.$method(::Type{FrozenView{T, N, P}}) where {T, N, P} = Base.$method(P)
end

@inline function Base.resize!(dv::FrozenView, new_length)::FrozenView  # untested
    return new_length == length(parent(dv)) ? dv : error("can't resize $(typeof(dv))")
end

Base.copy(dv::FrozenView) = as_dense_if_possible(copy(parent(dv)))

Base.:(==)(left_dv::FrozenView, right_matrix::AbstractMatrix) = parent(left_dv) == right_matrix

Base.:(==)(left_matrix::AbstractMatrix, right_dv::FrozenView) = left_matrix == parent(right_dv)

Base.:(==)(left_dv::FrozenView, right_dv::FrozenView) = parent(left_dv) == parent(right_dv)

Base.dataids(::FrozenView) = tuple()

@propagate_inbounds @inline function Base.view(dv::FrozenView, indices::Integer...)::Any  # untested
    return read_only(view(dv.parent, indices...))
end

@inline function Base.reshape(dv::FrozenView, dimensions::Integer...)::Any  # untested
    return read_only(reshape(dv.parent, dimensions...))
end

@inline function LinearAlgebra.transpose(dv::FrozenView)::Any  # untested
    return read_only(transpose(dv.parent))
end

"""
    function frozen(array::AbstractArray{T})::AbstractArray{T} where {T}

Given an array, return a zero-copy `FrozenView` of it.
"""
function frozen(array::AbstractArray{T, N})::AbstractArray{T, N} where {T, N}
    if array isa FrozenView
        return array
    else
        return FrozenView(array)
    end
end

end # module
