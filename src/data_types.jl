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

export as_storage_if_possible
export as_storage_or_copy
export as_storage_or_fail
export StorageMatrix
export StorageScalar
export StorageVector

using Daf.AsDense
using Daf.MatrixLayouts
using LinearAlgebra
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
    StorageMatrix{T} = Union{
        DenseMatrix{T},
        SparseMatrixCSC{T},
    } where {T <: StorageScalar}

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
    store (and memory map), by storing three separate dense vectors.
"""
StorageMatrix{T} = Union{DenseMatrix{T}, SparseMatrixCSC{T}} where {T <: StorageScalar}

"""
    StorageVector{T} = Union{
        DenseVector{T},
        SparseVector{T}
    } where {T <: StorageScalar}

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

"""
    function as_storage_if_possible(vector::AbstractVector{T})::StorageVector{T} where {T}
    function as_storage_if_possible(matrix::AbstractMatrix{T})::StorageMatrix{T} where {T}

Given a `vector` or `matrix`, return them as a `StorageMatrix` or `StorageVector` if possible, using a zero-copy
`DenseVector` or `DenseMatrix` wrapper if necessary, otherwise, return the original data.

This will return any `StorageVector` or `StorageMatrix` as-is without creating a wrapper.
"""
function as_storage_if_possible(matrix::StorageMatrix{T})::StorageMatrix{T} where {T}
    return matrix
end

function as_storage_if_possible(matrix::AbstractMatrix{T})::AbstractMatrix{T} where {T}
    return as_dense_if_possible(matrix)
end

function as_storage_if_possible(vector::StorageVector{T})::StorageVector{T} where {T}
    return vector
end

function as_storage_if_possible(vector::AbstractVector{T})::AbstractVector{T} where {T}  # untested
    return as_dense_if_possible(vector)
end

"""
    function as_storage_or_copy(vector::AbstractVector{T})::StorageVector{T} where {T}
    function as_storage_or_copy(matrix::AbstractMatrix{T})::StorageMatrix{T} where {T}

Given a `vector` or `matrix`, return them as a `StorageMatrix` or `StorageVector` if possible, using a zero-copy
`DenseVector` or `DenseMatrix` wrapper if necessary, otherwise, return a dense copy of the the original data.

This will return any `StorageVector` or `StorageMatrix` as-is without creating a wrapper or copying.
"""
function as_storage_or_copy(array::AbstractArray{T})::Union{StorageMatrix{T}, StorageVector{T}} where {T}
    as_storage = as_storage_if_possible(array)
    if as_storage isa StorageMatrix || as_storage isa StorageVector
        return as_storage
    end

    parent = array
    while true
        try
            parent = parent.parent
        catch
            break
        end
    end

    if parent isa SparseMatrixCSC
        return SparseMatrixCSC(as_storage)
    elseif parent isa SparseVector       # untested
        return SparseVector(as_storage)  # untested
    else
        return Array(as_storage)         # untested
    end
end

"""
    function as_storage_or_fail(vector::AbstractVector{T})::StorageVector{T} where {T}
    function as_storage_or_fail(matrix::AbstractMatrix{T})::StorageMatrix{T} where {T}

Given a `vector` or `matrix`, return them as a `StorageMatrix` or `StorageVector` if possible, using a zero-copy
`DenseVector` or `DenseMatrix` wrapper if necessary, otherwise, fail with an `error`.

This will return any `StorageVector` or `StorageMatrix` as-is without creating a wrapper or copying.
"""
function as_storage_or_fail(array::AbstractArray{T})::Union{StorageMatrix{T}, StorageVector{T}} where {T}
    as_storage = as_storage_if_possible(array)
    if as_storage isa StorageMatrix || as_storage isa StorageVector
        return as_storage
    else
        error("the array: $(typeof(array)) is not storage")
    end
end

end # module
