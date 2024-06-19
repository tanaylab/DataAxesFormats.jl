"""
Identify data inside a `Daf` data set using a key. These types are used in various high-level API parameters.

A separate key space is used for axes and data; thus, both axes and scalars use a simple string key.
"""
module Keys

export AxisKey
export ScalarKey
export VectorKey
export MatrixKey
export DataKey

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
A key specifying some data property in `Daf`.
"""
DataKey = Union{ScalarKey, VectorKey, MatrixKey}

end  # module
