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
by `Daf` have a clear [`MatrixLayouts`](@ref DataAxesFormats.MatrixLayouts) (regardless of whether they are dense or
sparse). This allows user code to ensure it is working "with the grain" of the data, which is **much** more efficient.
"""
module StorageTypes

export StorageFloat
export StorageInteger
export StorageMatrix
export StorageReal
export StorageScalar
export StorageScalarBase
export StorageSigned
export StorageUnsigned
export StorageVector
export sparse_matrix_csc
export sparse_vector

using SparseArrays

"""
    StorageSigned = Union{Int8, Int16, Int32, Int64}

Signed integer number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageSigned = Union{Int8, Int16, Int32, Int64}

"""
    StorageUnsigned = Union{UInt8, UInt16, UInt32, UInt64}

Unsigned integer number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageUnsigned = Union{UInt8, UInt16, UInt32, UInt64}

"""
    StorageInteger = Union{StorageSigned, StorageUnsigned}

Integer number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageInteger = Union{StorageSigned, StorageUnsigned}

"""
    StorageFloat = Union{Float32, Float64}

Floating point number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageFloat = Union{Float32, Float64}

"""
    StorageReal = Union{Bool, StorageInteger, StorageFloat}

Number types that can be used as scalars, or elements in stored matrices or vectors.
"""
StorageReal = Union{Bool, StorageInteger, StorageFloat}

"""
    StorageScalar = Union{StorageReal, <:AbstractString}

Types that can be used as scalars, or elements in stored matrices or vectors.

This is restricted to [`StorageReal`](@ref) (including Booleans) and strings. It is arguably too restrictive, as in
principle we could support any arbitrary `isbitstype`. However, in practice this would cause much trouble when accessing
the data from other systems (specifically Python and R). Since `Daf` targets storing scientific data (especially
biological data), as opposed to "anything at all", this restriction seems reasonable.
"""
StorageScalar = Union{StorageReal, S} where {S <: AbstractString}

"""
    StorageScalarBase = Union{StorageReal, AbstractString}

For using in `where` clauses when a type needs to be a [`StorageScalar`](@ref). That is, write
`where {T <: StorageScalarBase}` instead of `where {T <: StorageScalar}`, because of the
limitations of Julia's type system.
"""
StorageScalarBase = Union{StorageReal, AbstractString}

"""
    StorageMatrix{T} = AbstractMatrix{T} where {T <: StorageReal}

Matrices that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageReal`](@ref), to allow efficient storage of the data in disk files. That is,
matrices of strings are **not** supported.

!!! note

    All matrices we store must have a clear [`MatrixLayouts`](@ref DataAxesFormats.MatrixLayouts), that is, must be in
    either row-major or column-major format.
"""
StorageMatrix{T} = AbstractMatrix{T} where {T <: StorageReal}

"""
    StorageVector{T} = AbstractVector{T} where {T <: StorageScalar}

Vectors that can be directly stored (and fetched) from `Daf` storage.

The element type must be a [`StorageScalar`](@ref), to allow storing the data in disk files. Vectors of strings
are supported but will be less efficient.
"""
StorageVector{T} = AbstractVector{T} where {T <: StorageScalar}

"""
    sparse_vector(dense::StorageMatrix)::SparseVector

Create a sparse vector using the smallest unsigned integer type needed for this size of matrix.
"""
function sparse_vector(dense::StorageVector{T})::SparseVector{T} where {T <: StorageReal}
    return SparseVector{eltype(dense), indtype_for_size(length(dense))}(dense)
end

"""
    sparse_matrix_csc(dense::StorageMatrix)::SparseMatrixCSC

Create a sparse matrix using the smallest unsigned integer type needed for this size of matrix.
"""
function sparse_matrix_csc(dense::StorageMatrix)::SparseMatrixCSC
    return SparseMatrixCSC{eltype(dense), indtype_for_size(length(dense))}(dense)
end

function indtype_for_size(size::Integer)::Type
    for type in (UInt8, UInt16, UInt32)
        if size <= typemax(type)
            return type
        end
    end
    return UInt64  # untested
end

end # module
