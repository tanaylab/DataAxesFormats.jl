"""
Only a restricted set of scalar, matrix and vector types is stored by `Daf`.

The set of scalar types is restricted because we need to be able to store them in disk files. This rules out compound
types such as `Dict`. This isn't an issue for vector and matrix elements but is sometimes bothersome for "scalar" data
(not associated with any axis). If you find yourself needed to store such data, you'll have to serialize it to a string.
By convention, we use `JSON` blobs for such data to maximize portability between different systems.

Julia supports a potentially infinite variety of ways to represent matrices and vectors. `Daf` is intentionally
restricted to specific representations. This has several advantages:

  - `Daf` storage formats need only implement storing these restricted representations, which lend themselves to simple
    storage in consecutive bytes (in memory and/or on disk). These representations also allow for memory-mapping the
    data from disk files, which allows `Daf` to deal with data sets larger than the available memory.

  - Client code need only worry about dealing with these restricted representations, which limits the amount of code
    paths required for efficient algorithm implementations. However, you (mostly) need not worry about this when
    invoking library functions, which have code paths covering all common matrix types. You **do** need to consider the
    layout of the data, though (see below).

This has the downside that `Daf` doesn't support efficient storage of specialized matrices (to pick a random example,
upper triangular matrices). This isn't a great loss, since `Daf` targets storing arbitrary scientific data (especially
biological data), which in general is not of any such special shape. The upside is that all matrices stored and returned
by `Daf` have a clear [`MatrixLayouts`](@ref) (regardless of whether they are dense or sparse). This allows user code to
ensure it is working "with the grain" of the data, which is **much** more efficient.
"""
module StorageTypes

export AbstractStringSet
export AbstractStringVector
export StorageFloat
export StorageInteger
export StorageMatrix
export StorageNumber
export StorageScalar
export StorageScalarBase
export StorageVector

using Daf.MatrixLayouts
using SparseArrays

"""
    AbstractStringSet = AbstractSet{S} where {S <: AbstractString}

A set of strings, without commitment to the concrete implementation of either the set or the strings contained in it.
"""
AbstractStringSet = AbstractSet{S} where {S <: AbstractString}

"""
    AbstractStringVector = AbstractVector{S} where {S <: AbstractString}

A vector of strings, without commitment to the concrete implementation of either the vector or the strings contained in
it.
"""
AbstractStringVector = AbstractVector{S} where {S <: AbstractString}

"""
    StorageInteger = Union{Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64}

Integer number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageInteger = Union{Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64}

"""
    StorageFloat = Union{Float32, Float64}

Floating point number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageFloat = Union{Float32, Float64}

"""
    StorageNumber = Union{Bool, StorageInteger, StorageFloat}

Number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageNumber = Union{Bool, StorageInteger, StorageFloat}

"""
    StorageScalar = Union{StorageNumber, S} where {S <: AbstractString}

Types that can be used as scalars, or elements in stored matrices or vectors.

This is restricted to [`StorageNumber`](@ref) (including Booleans) and strings. It is arguably too restrictive, as in
principle we could support any arbitrary `isbitstype`. However, in practice this would cause much trouble when accessing
the data from other systems (specifically Python and R). Since `Daf` targets storing scientific data (especially
biological data), as opposed to "anything at all", this restriction seems reasonable.
"""
StorageScalar = Union{StorageNumber, S} where {S <: AbstractString}

"""
    StorageScalarBase = Union{StorageNumber, AbstractString}

For using in `where` clauses when a type needs to be a [`StorageScalar`](@ref). That is, write
`where {T <: StorageScalarBase}` instead of `where {T <: StorageScalar}`, because of the
limitations of Julia's type system.
"""
StorageScalarBase = Union{StorageNumber, AbstractString}

"""
    StorageMatrix{T} = AbstractMatrix{T} where {T <: StorageNumber}

Matrices that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageNumber`](@ref), to allow efficient storage of the data in disk files. That is,
matrices of strings are **not** supported.

!!! note

    All matrices we store must have a clear [`MatrixLayouts`](@ref), that is, must be in either row-major or
    column-major format.
"""
StorageMatrix{T} = AbstractMatrix{T} where {T <: StorageNumber}

"""
    StorageVector{T} = AbstractVector{T} where {T <: StorageScalar}

Vectors that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files. Vectors of strings
are supported but will be less efficient.
"""
StorageVector{T} = AbstractVector{T} where {T <: StorageScalar}

end # module
