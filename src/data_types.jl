"""
Only a restricted set of scalar, matrix and vector types is supported by `Daf`.

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
    invoking library functions, which have code paths covering all common matrix types. You *do* need to consider the
    layout of the data, though (see below).

This has the downside that `Daf` doesn't support efficient storage of specialized matrices (to pick a random example,
upper triangular matrices). This isn't a great loss, since `Daf` targets storing arbitrary scientific data (especially
biological data), which in general is not of any such special shape. The upside is that all matrices stored and returned
by `Daf` have a clear [`MatrixLayouts`](@ref) (regardless of whether they are dense or sparse). This allows user code to
ensure it is working "with the grain" of the data, which is *much* more efficient.
"""
module DataTypes

export StorageMatrix
export StorageScalar
export StorageVector

using Daf.MatrixLayouts
using SparseArrays

"""
    StorageScalar = Union{String, Number}

Types that can be used as scalars, or elements in stored matrices or vectors.

This is restricted to numbers (including Booleans) and strings. It is arguably too restrictive, as in principle we could
support any arbitrary `isbitstype`. However, in practice this would cause much trouble when accessing the data from
other systems (specifically Python and R). Since `Daf` targets storing scientific data (especially biological data), as
opposed to "anything at all", this restriction seems reasonable.
"""
StorageScalar = Union{String, Number}

"""
    StorageMatrix{T} = AbstractMatrix{T} where {T <: StorageScalar}

Matrices that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files.

!!! note

    All matrices we store must have a clear [`MatrixLayouts`](@ref), that is, must be in either row- or column-major
    format.
"""
StorageMatrix{T} = AbstractMatrix{T} where {T <: StorageScalar}

"""
    StorageVector{T} = AbstractVector{T} where {T <: StorageScalar}

Vectors that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files.
"""
StorageVector{T} = AbstractVector{T} where {T <: StorageScalar}

"""
    MappableMatrix{T} = Union{DenseMatrix{T}, SparseMatrixCSC{T}} where {T <: StorageScalar}

Matrices that we can memory-map to disk storage. Storing any other matrix type into disk storage will convert the data
to one of these formats.

The element type must be a [`StorageScalar`](@ref).
"""
MappableMatrix{T} = Union{DenseMatrix{T}, SparseMatrixCSC{T}} where {T <: StorageScalar}

"""
    MappableVector{T} = Union{DenseVector{T}, SparseVector{T}} where {T <: StorageScalar}

vectors that we can memory-map to disk storage. Storing any other vector type into disk storage will convert the data to
one of these formats.

The element type must be a [`StorageScalar`](@ref).
"""
MappableVector{T} = Union{DenseVector{T}, SparseVector{T}} where {T <: StorageScalar}

end # module
