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

using Daf.AsDense
using Daf.MatrixLayouts
using LinearAlgebra
using SparseArrays

"""
Types that can be used as scalars, or elements in stored matrices or vectors.

This is restricted to numbers (including Booleans) and strings. It is arguably too restrictive, as in principle we could
support any arbitrary `isbitstype`. However, in practice this would cause much trouble when accessing the data from
other systems (specifically Python and R). Since `Daf` targets storing scientific data (especially biological data), as
opposed to "anything at all", this restriction seems reasonable.
"""
StorageScalar = Union{String, Number}

"""
Matrices that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files.

The storable matrix types are:

  - Any `DenseMatrix` type, that is, any matrix stored as a contiguous memory region. This is an abstract type, and
    Julia's `Matrix` type `isa DenseMatrix`. However, due to Julia's type system limitations, we can have other concrete
    matrix types which don't (can't) derive from this abstract type, but can and do have specific instances that are in
    fact dense. In such cases, use [`AsDense`](@ref) to expose to type type system the fact the specific instance is
    indeed dense.

  - The `SparseMatrixCSC` type, that is, a sparse matrix stored in columns. This is a concrete type, which stores
    three (dense) internal vectors: the row index of each non-zero element, the starting offset of each column, and of
    course the values of the non-zero elements. This is the "standard" sparse matrix format in Julia, and is easy to
    store (and memory map), by storing three separate dense vectors. There is no standard row-major `SparseMatrixCSR`
    equivalent in Julia, so if needed, `Daf` will store (and provide) the `Transpose` of a `SparseMatrixCSC` matrix
    instead.
"""
StorageMatrix{T} = Union{DenseMatrix{T}, SparseMatrixCSC{T}, Transpose{SparseMatrixCSC{T}}} where {T <: StorageScalar}

"""
Vectors that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files.

The storable vector types are:

  - Any `DenseVector` type, that is, any vector stored as a contiguous memory region. This is an abstract type, and
    Julia's `Vector` `isa DenseVector`. However, due to Julia's type system limitations, we can have other concrete
    vector types which don't (can't) derive from this abstract type, but can and do have specific instances that are in
    fact dense. In such cases, use [`AsDense`](@ref) to expose to type type system the fact the specific instance is
    indeed dense.

  - The `SparseVector` type. This is a concrete type, which stores two dense vectors - the index of each non-zero
    element, and the values of the non-zero elements (for Boolean data the second vector can be omitted). This is the
    natural choice given it is what we get when we slice a `SparseMatrixCSC`.
"""
StorageVector{T} = Union{DenseVector{T}, SparseVector{T}} where {T <: StorageScalar}

function require_storage_matrix(matrix::Any)::Nothing
    return error("type: $(typeof(matrix)) is not a valid Daf.StorageMatrix")
end

function require_storage_matrix(matrix::StorageMatrix)::Nothing
    return nothing
end

function require_storage_vector(vector::Any)::Nothing
    return error("type: $(typeof(vector)) is not a valid Daf.StorageVector")
end

function require_storage_vector(vector::StorageVector)::Nothing
    return nothing
end

end # module
