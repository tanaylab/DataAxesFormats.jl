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
tensor. However, it is useful to be able to specify the whole set of matrices for copying, views, cobtracts, etc.
"""
TensorKey = Tuple{AbstractString, AbstractString, AbstractString, AbstractString}

"""
A key specifying some data property in `Daf`. This includes [`TensorKey`](@ref) which actually refers to a series of
matrix properties instead of a single data property.
"""
DataKey = Union{PropertyKey, TensorKey}
DataKey
end  # module
