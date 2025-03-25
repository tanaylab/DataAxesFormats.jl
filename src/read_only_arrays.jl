"""
Read-only vectors and matrices, by ab/using `SparseArrays.ReadOnly`. We need this because we want access to `Daf` data
to be copy-free (and cache results) for efficiency (in particular, matrices can be several GBs). However, it would be
very easy for the user code to access vector or matrix data and accidentally modify it in-place, thereby corrupting it
and causing all sort of hard-to-debug hilarity. Julia in its infinite wisdom takes the view that "everything is mutable"
so has no builtin notion of "read-only view of an array", probably due to the general weakness of its type system (which
might be a price paid for efficient multiple dispatch?). However the `SparseArrays` package happens to implement
something along these lines, which we shamelessly ab/use for our purposes.

TODO: Explicitly support the concept of in-place modifications of data in `Daf` (building on the memory-mapped
implementation).

!!! note

    The read-only array functions below are restricted to dealing with normal (dense) arrays, `SparseArrays`,
    `NamedArrays`, `PermutedDimsArray`, and `LinearAlgebra` arrays (specifically, `Transpose` and `Adjoint`), as these
    are the types actually used in `Daf` storage. YMMV if using more exotic matrix types. In theory you could extend the
    implementation to cover such types as well.
"""
module ReadOnlyArrays
## Arrays

export mutable_array
export read_only_array
export is_read_only_array

using LinearAlgebra
using NamedArrays
using SparseArrays

"""
    read_only_array(array::AbstractArray):AbstractArray

Return an immutable view of an `array`. This uses `SparseArrays.ReadOnly`, and properly deals with `NamedArray`. If the
array is already immutable, it is returned as-is.
"""
function read_only_array(array::AbstractArray)::AbstractArray
    return SparseArrays.ReadOnly(array)
end

function read_only_array(array::PermutedDimsArray{T, 2, P, IP, A})::PermutedDimsArray where {T, P, IP, A}
    parent_array = parent(array)
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return PermutedDimsArray(read_only_parent_array, P)
    end
end

function read_only_array(array::Transpose)::Transpose
    parent_array = parent(array)
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return Transpose(read_only_parent_array)
    end
end

function read_only_array(array::Adjoint)::Adjoint
    parent_array = parent(array)
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return Adjoint(read_only_parent_array)
    end
end

function read_only_array(array::SparseArrays.ReadOnly)::SparseArrays.ReadOnly
    return array
end

function read_only_array(array::NamedArray)::NamedArray
    parent_array = array.array
    read_only_parent_array = read_only_array(parent_array)
    if read_only_parent_array === parent_array
        return array
    else
        return NamedArray(read_only_parent_array, array.dicts, array.dimnames)
    end
end

"""
    is_read_only_array(array::AbstractArray)::Bool

Return whether an `array` is immutable.
"""
function is_read_only_array(array::AbstractArray)::Bool
    return mutable_array(array) !== array
end

"""
    mutable_array(array::AbstractArray)::AbstractArray

Return mutable access to an array, even if it is a read-only array. **This should be used with great care** because the
code depends on read-only arrays not changing their values.
"""
function mutable_array(array::AbstractArray)::AbstractArray
    return array
end

function mutable_array(array::PermutedDimsArray{T, 2, P, IP, A})::PermutedDimsArray where {T, P, IP, A}
    parent_array = parent(array)
    mutable_parent_array = mutable_array(parent_array)
    if mutable_parent_array === parent_array
        return array
    else
        return PermutedDimsArray(mutable_parent_array, P)
    end
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

end # module
