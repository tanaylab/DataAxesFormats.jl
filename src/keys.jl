"""
Identify data inside a `Daf` data set using a key. These types are used in various high-level API parameters.

A separate key space is used for axes and data; thus, both axes and scalars use a simple string key.
"""
module Keys

export AxisKey
export DataKey
export MatrixKey
export PropertyKey
export ScalarKey
export TensorKey
export VectorKey
export named_tuple_as_pairs
export pairs_as_dict

"""
A key specifying some axis in `Daf` by its name.
"""
AxisKey = AbstractString

"""
A key specifying some scalar in `Daf` by its name.
"""
ScalarKey = AbstractString

"""
A key specifying some vector in `Daf` by its axis and name.
"""
VectorKey = Tuple{AbstractString, AbstractString}

"""
A key specifying some matrix in `Daf` by its axes and name. The axes order does not matter.
"""
MatrixKey = Tuple{AbstractString, AbstractString, AbstractString}

"""
A key specifying some atomic data property in `Daf`. That is, these keys refer to data we can directly get or set using the APIs.
"""
PropertyKey = Union{ScalarKey, VectorKey, MatrixKey}

"""
A key specifying some tensor in `Daf` by its axes and name. `Daf` is restricted to storing 0D, 1D and 2D data, for good
reasons; higher dimensional data raises sticky issues about layout and there is no support for any sparse representation
(which is even more important for this kind of data). However, sometimes it is necessary to store 3D data in `Daf`. In
this case, we pick the 1st axis as the main one, and store a series of `<main-axis-entry>_<property_name>` matrices
using the other two axes (whose order doesn't matter). Access is only to each specific matrix, not to the whole 3D
tensor. However, it is useful to be able to specify the whole set of matrices for copying, views, contracts, etc.
"""
TensorKey = Tuple{AbstractString, AbstractString, AbstractString, AbstractString}

"""
A key specifying some data property in `Daf`. This includes [`TensorKey`](@ref) which actually refers to a series of
matrix properties instead of a single data property.
"""
DataKey = Union{PropertyKey, TensorKey}

"""
Convert a dictionary, a vector of pairs, or a named tuple to a dictionary. Returns `nothing` for `nothing`.
"""
function pairs_as_dict(pairs_input::AbstractDict)::AbstractDict  # FLAKY TESTED
    return pairs_input
end

function pairs_as_dict(pairs_input::AbstractVector)::Dict{Any, Any}  # FLAKY TESTED
    return Dict{Any, Any}(pairs_input)
end

function pairs_as_dict(pairs_input::NamedTuple)::Dict{AbstractString, Any}  # FLAKY TESTED
    return Dict{AbstractString, Any}(String(key) => value for (key, value) in pairs(pairs_input))
end

function pairs_as_dict(::Nothing)::Nothing  # FLAKY TESTED
    return nothing
end

"""
Convert a vector of pairs or a named tuple to a vector of pairs. Returns `nothing` for `nothing`.
"""
function named_tuple_as_pairs(pairs_input::AbstractVector)::AbstractVector
    return pairs_input
end

function named_tuple_as_pairs(pairs_input::NamedTuple)::Vector{Pair{AbstractString, Any}}
    return Pair{AbstractString, Any}[String(key) => value for (key, value) in pairs(pairs_input)]
end

function named_tuple_as_pairs(::Nothing)::Nothing  # UNTESTED
    return nothing
end

end  # module
