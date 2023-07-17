"""
The [`Container`](@ref) provides the high-level API for accessing some `Daf` data in a specific [`Format`](@ref).

A `Daf` container object contains some named scalar data, a set of axes (each with a unique name for each entry), and
named vector and matrix data based on these axes.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each *unordered* pair of
axes.

For matrices, we keep careful track of their [`MatrixLayouts`](@ref). Returned matrices are always in column-major
layout, using [`relayout!`](@ref) if necessary. As this is an expensive operation, we'll cache the result in memory.
Similarly, we cache the results of applying a query to the container. We allow clearing the cache to reduce memory
usage, if necessary.

The container API is the high-level API intended to be used from outside the package. It provides additional
functionality on top of the low-level [`Format`](@ref) implementations, accepting more general data types, automatically
dealing with [`relayout!`](@ref) when needed, and even providing a language for [`Queries`](@ref) for flexible
extraction of data from the container.
"""
module Containers

export add_axis!
export axis_length
export axis_names
export Container
export delete_axis!
export delete_matrix!
export delete_scalar!
export delete_vector!
export description
export empty_dense_matrix!
export empty_dense_vector!
export empty_sparse_matrix!
export empty_sparse_vector!
export get_axis
export get_matrix
export get_scalar
export get_vector
export has_axis
export has_matrix
export has_scalar
export has_vector
export matrix_names
export matrix_query
export scalar_names
export scalar_query
export set_matrix!
export set_scalar!
export set_vector!
export vector_names
export vector_query

using Daf.DataTypes
using Daf.Formats
using Daf.MatrixLayouts
using Daf.Messages
using Daf.Registry
using Daf.Queries
using SparseArrays

import Daf.DataTypes.require_storage_matrix
import Daf.DataTypes.require_storage_vector
import Daf.Queries.CmpEqual
import Daf.Queries.CmpGreaterOrEqual
import Daf.Queries.CmpGreaterThan
import Daf.Queries.CmpLessOrEqual
import Daf.Queries.CmpLessThan
import Daf.Queries.CmpMatch
import Daf.Queries.CmpNotEqual
import Daf.Queries.CmpNotMatch
import Daf.Queries.FilterAnd
import Daf.Queries.FilterOr
import Daf.Queries.FilterXor

"""
An abstract interface for all `Daf` containers.

We require each container to have a human-readable `.name::String` property for error messages and the like. We ensure
this name is unique by using [`unique_name`](@ref) when the container is constructed.

We distinguish between the high-level API of the `Container` which is meant to be used from outside the package, and the
internal low-level API of the [`Format`](@ref) which is needed to implement a new storage format.
"""
abstract type Container <: Format end

"""
    has_scalar(container::Container, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in the `container`.
"""
function has_scalar(container::Container, name::AbstractString)::Bool
    return format_has_scalar(container, name)
end

"""
    set_scalar!(
        container::Container,
        name::AbstractString,
        value::StorageScalar
        [; overwrite::Bool]
    )::Nothing

Set the `value` of a scalar property with some `name` in the `container`.

If `overwrite` is `false` (the default), this first verifies the `name` scalar property does not exist.
"""
function set_scalar!(container::Container, name::AbstractString, value::StorageScalar; overwrite::Bool = false)::Nothing
    if !overwrite
        require_no_scalar(container, name)
    end

    format_set_scalar!(container, name, value)
    return nothing
end

"""
    delete_scalar!(
        container::Container,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a scalar property with some `name` from the `container`.

If `must_exist` is `true` (the default), this first verifies the `name` scalar property exists in the `container`.
"""
function delete_scalar!(container::Container, name::AbstractString; must_exist::Bool = true)::Nothing
    if must_exist
        require_scalar(container, name)
    elseif !has_scalar(container, name)
        return nothing
    end

    format_delete_scalar!(container, name)
    return nothing
end

"""
    scalar_names(container::Container)::Set{String}

The names of the scalar properties in the `container`.
"""
function scalar_names(container::Container)::AbstractSet{String}
    return format_scalar_names(container)
end

"""
    get_scalar(
        container::Container,
        name::AbstractString[; default::StorageScalar]
    )::StorageScalar

Get the value of a scalar property with some `name` in the `container`.

If `default` is not specified, this first verifies the `name` scalar property exists in the `container`.
"""
function get_scalar(
    container::Container,
    name::AbstractString;
    default::Union{StorageScalar, Nothing} = nothing,
)::StorageScalar
    if default == nothing
        require_scalar(container, name)
    elseif !has_scalar(container, name)
        return default
    end

    return format_get_scalar(container, name)
end

function require_scalar(container::Container, name::AbstractString)::Nothing
    if !has_scalar(container, name)
        error("missing scalar property: $(name)\nin the Daf.Container: $(container.name)")
    end
    return nothing
end

function require_no_scalar(container::Container, name::AbstractString)::Nothing
    if has_scalar(container, name)
        error("existing scalar property: $(name)\nin the Daf.Container: $(container.name)")
    end
    return nothing
end

"""
    has_axis(container::Container, axis::AbstractString)::Bool

Check whether some `axis` exists in the `container`.
"""
function has_axis(container::Container, axis::AbstractString)::Bool
    return format_has_axis(container, axis)
end

"""
    add_axis!(
        container::Container,
        axis::AbstractString,
        entries::DenseVector{String}
    )::Nothing

Add a new `axis` to the `container`.

This first verifies the `axis` does not exist and that the `entries` are unique.
"""
function add_axis!(container::Container, axis::AbstractString, entries::DenseVector{String})::Nothing
    require_no_axis(container, axis)

    if !allunique(entries)
        error("non-unique entries for new axis: $(axis)\nin the Daf.Container: $(container.name)")
    end

    format_add_axis!(container, axis, entries)
    return nothing
end

"""
    delete_axis!(
        container::Container,
        axis::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete an `axis` from the `container`. This will also delete any vector or matrix properties that are based on this
axis.

If `must_exist` is `true` (the default), this first verifies the `axis` exists in the `container`.
"""
function delete_axis!(container::Container, axis::AbstractString; must_exist::Bool = true)::Nothing
    if must_exist
        require_axis(container, axis)
    elseif !has_axis(container, axis)
        return nothing
    end

    for name in vector_names(container, axis)
        format_delete_vector!(container, axis, name)
    end

    for other_axis in axis_names(container)
        for name in matrix_names(container, axis, other_axis)
            format_delete_matrix!(container, axis, other_axis, name)
        end
    end

    format_delete_axis!(container, axis)
    return nothing
end

"""
    axis_names(container::Container)::AbstractSet{String}

The names of the axes of the `container`.
"""
function axis_names(container::Container)::AbstractSet{String}
    return format_axis_names(container)
end

"""
    get_axis(container::Container, axis::AbstractString)::DenseVector{String}

The unique names of the entries of some `axis` of the `container`. This is identical to doing [`get_vector`](@ref) for
the special `name` property.

This first verifies the `axis` exists in the `container`.
"""
function get_axis(container::Container, axis::AbstractString)::DenseVector{String}
    require_axis(container, axis)
    return format_get_axis(container, axis)
end

"""
    axis_length(container::Container, axis::AbstractString)::Int64

The number of entries along the `axis` in the `container`.

This first verifies the `axis` exists in the `container`.
"""
function axis_length(container::Container, axis::AbstractString)::Int64
    require_axis(container, axis)
    return format_axis_length(container, axis)
end

function require_axis(container::Container, axis::AbstractString)::Nothing
    if !has_axis(container, axis)
        error("missing axis: $(axis)\nin the Daf.Container: $(container.name)")
    end
    return nothing
end

function require_no_axis(container::Container, axis::AbstractString)::Nothing
    if has_axis(container, axis)
        error("existing axis: $(axis)\nin the Daf.Container: $(container.name)")
    end
    return nothing
end

"""
    has_vector(container::Container, axis::AbstractString, name::AbstractString)::Bool

Check whether a vector property with some `name` exists for the `axis` in the `container`. This is always true for the
special `name` property.

This first verifies the `axis` exists in the `container`.
"""
function has_vector(container::Container, axis::AbstractString, name::AbstractString)::Bool
    require_axis(container, axis)
    return name == "name" || format_has_vector(container, axis, name)
end

"""
    set_vector!(
        container::Container,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector}
        [; overwrite::Bool]
    )::Nothing

Set a vector property with some `name` for some `axis` in the `container`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This first verifies the `axis` exists in the `container`, that the property name isn't `name`, and that the `vector` has
the appropriate length. If `overwrite` is `false` (the default), this also verifies the `name` vector does not exist for
the `axis`.
"""
function set_vector!(
    container::Container,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector};
    overwrite::Bool = false,
)::Nothing
    require_not_name(container, axis, name)
    require_axis(container, axis)

    if vector isa AbstractVector
        require_storage_vector(vector)
        require_axis_length(container, "vector length", length(vector), axis)
    end

    if !overwrite
        require_no_vector(container, axis, name)
    end

    format_set_vector!(container, axis, name, vector)
    return nothing
end

"""
    empty_dense_vector!(
        container::Container,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T}
        [; overwrite::Bool]
    )::DenseVector{T} where {T <: Number}

Create an empty dense vector property with some `name` for some `axis` in the `container`.

The returned vector will be uninitialized; the caller is expected to fill it with values. This saves creating a copy of
the vector before setting it in the container, which makes a huge difference when creating vectors on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `axis` exists in the `container` and that the property name isn't `name`. If `overwrite` is
`false` (the default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_dense_vector!(
    container::Container,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::DenseVector{T} where {T <: Number}
    require_not_name(container, axis, name)
    require_axis(container, axis)

    if !overwrite
        require_no_vector(container, axis, name)
    end

    return format_empty_dense_vector!(container, axis, name, eltype)
end

"""
    empty_sparse_vector!(
        container::Container,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::Integer,
        indtype::Type{I}
        [; overwrite::Bool]
    )::DenseVector{T} where {T <: Number, I <: Integer}

Create an empty dense vector property with some `name` for some `axis` in the `container`.

The returned vector will be uninitialized; the caller is expected to fill it with values. This means manually filling
the `nzind` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow pre-allocating disk
container. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse vector is created by concatenating several smaller ones; this function
allows doing so directly into the container vector, avoiding a copy in case of memory-mapped disk container formats.

!!! warning

    It is the caller's responsibility to fill the three vectors with valid data. **There's no safety net if you mess
    this up**. Specifically, you must ensure:

      - `nzind[1] == 1`
      - `nzind[i] <= nzind[i + 1]`
      - `nzind[end] == nnz`

This first verifies the `axis` exists in the `container` and that the property name isn't `name`. If `overwrite` is
`false` (the default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_sparse_vector!(
    container::Container,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I};
    overwrite::Bool = false,
)::SparseVector{T, I} where {T <: Number, I <: Integer}
    require_not_name(container, axis, name)
    require_axis(container, axis)

    if !overwrite
        require_no_vector(container, axis, name)
    end

    return format_empty_sparse_vector!(container, axis, name, eltype, nnz, indtype)
end

"""
    delete_vector!(
        container::Container,
        axis::AbstractString,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a vector property with some `name` for some `axis` from the `container`.

This first verifies the `axis` exists in the `container` and that the property name isn't `name`. If `must_exist` is
`true` (the default), this also verifies the `name` vector exists for the `axis`.
"""
function delete_vector!(
    container::Container,
    axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
)::Nothing
    require_not_name(container, axis, name)
    require_axis(container, axis)

    if must_exist
        require_vector(container, axis, name)
    elseif !has_vector(container, axis, name)
        return nothing
    end

    format_delete_vector!(container, axis, name)
    return nothing
end

"""
    vector_names(container::Container, axis::AbstractString)::Set{String}

The names of the vector properties for the `axis` in the `container`, **not** including the special `name` property.

This first verifies the `axis` exists in the `container`.
"""
function vector_names(container::Container, axis::AbstractString)::AbstractSet{String}
    require_axis(container, axis)
    return format_vector_names(container, axis)
end

"""
    get_vector(
        container::Container,
        axis::AbstractString,
        name::AbstractString
        [; default::Union{StorageScalar, StorageVector}]
    )::StorageVector

Get the vector property with some `name` for some `axis` in the `container`.

This first verifies the `axis` exists in the `container`. If `default` is not specified, this first verifies the `name`
vector exists in the `container`. Otherwise, if `default` is a `StorageVector`, it has to be of the same size as the
`axis`, and is returned. Otherwise, a new `Vector` is created of the correct size containing the `default`, and is
returned.
"""
function get_vector(
    container::Container,
    axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageVector, Nothing} = nothing,
)::StorageVector
    require_axis(container, axis)

    if name == "name"
        return format_get_axis(container, axis)
    end

    if default isa AbstractVector
        require_storage_vector(default)
        require_axis_length(container, "default length", length(default), axis)
    end

    if default == nothing
        require_vector(container, axis, name)
    elseif !has_vector(container, axis, name)
        if default isa StorageVector
            return default
        else
            return fill(default, axis_length(container, axis))
        end
    end

    vector = format_get_vector(container, axis, name)
    if !(vector isa StorageVector)
        error(  # untested
            "format_get_vector for Daf.Format: $(typeof(container))\n" *
            "returned invalid Daf.StorageVector: $(typeof(vector))",
        )
    end

    if length(vector) != axis_length(container, axis)
        error( # untested
            "format_get_vector for Daf.Format: $(typeof(container))\n" *
            "returned vector length: $(length(vector))\n" *
            "instead of axis: $(axis)\n" *
            "length: $(axis_length(container, axis))\n" *
            "in the Daf.Container: $(container.name)",
        )
    end

    return vector
end

function require_vector(container::Container, axis::AbstractString, name::AbstractString)::Nothing
    if !has_vector(container, axis, name)
        error("missing vector property: $(name)\nfor the axis: $(axis)\nin the Daf.Container: $(container.name)")
    end
    return nothing
end

function require_no_vector(container::Container, axis::AbstractString, name::AbstractString)::Nothing
    if has_vector(container, axis, name)
        error("existing vector property: $(name)\nfor the axis: $(axis)\nin the Daf.Container: $(container.name)")
    end
    return nothing
end

"""
    has_matrix(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
    )::Bool

Check whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in the `container`.
Since this is Julia, this means a column-major matrix. A container may contain two copies of the same data, in which
case it would report the matrix under both axis orders.

This first verifies the `rows_axis` and `columns_axis` exists in the `container`.
"""
function has_matrix(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)
    return format_has_matrix(container, rows_axis, columns_axis, name)
end

"""
    set_matrix!(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::StorageMatrix
        [; overwrite::Bool]
    )::Nothing

Set the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `container`. Since this is
Julia, this should be a column-major `matrix`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

This first verifies the `rows_axis` and `columns_axis` exist in the `container`, that the `matrix` is column-major of
the appropriate size. If `overwrite` is `false` (the default), this also verifies the `name` matrix does not exist for
the `rows_axis` and `columns_axis`.
"""
function set_matrix!(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalar, StorageMatrix};
    overwrite::Bool = false,
)::Nothing
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)

    if matrix isa StorageMatrix
        require_storage_matrix(matrix)
        require_column_major(matrix)
        require_axis_length(container, "matrix rows", size(matrix, Rows), rows_axis)
        require_axis_length(container, "matrix columns", size(matrix, Columns), columns_axis)
    end

    if !overwrite
        require_no_matrix(container, rows_axis, columns_axis, name)
    end

    format_set_matrix!(container, rows_axis, columns_axis, name, matrix)
    return nothing
end

"""
    empty_dense_matrix!(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T}
        [; overwrite::Bool]
    )::DenseMatrix{T} where {T <: Number}

Create an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in the `container`. Since
this is Julia, this will be a column-major `matrix`.

The returned matrix will be uninitialized; the caller is expected to fill it with values. This saves creating a copy of
the matrix before setting it in the `container`, which makes a huge difference when creating matrices on disk (using
memory mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `rows_axis` and `columns_axis` exist in the `container`, that the `matrix` is column-major of
the appropriate size. If `overwrite` is `false` (the default), this also verifies the `name` matrix does not exist for
the `rows_axis` and `columns_axis`.
"""
function empty_dense_matrix!(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::DenseMatrix{T} where {T <: Number}
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)

    if !overwrite
        require_no_matrix(container, rows_axis, columns_axis, name)
    end

    return format_empty_dense_matrix!(container, rows_axis, columns_axis, name, eltype)
end

"""
    empty_sparse_matrix!(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::Integer,
        intdype::Type{I}
        [; overwrite::Bool]
    )::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}

Create an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in the `container`.

The returned matrix will be uninitialized; the caller is expected to fill it with values. This means manually filling
the `colptr`, `rowval` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow
pre-allocating disk container. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse matrix is created by concatenating several smaller ones; this function
allows doing so directly into the container matrix, avoiding a copy in case of memory-mapped disk container formats.

!!! warning

    It is the caller's responsibility to fill the three vectors with valid data. **There's no safety net if you mess
    this up**. Specifically, you must ensure:

      - `colptr[1] == 1`
      - `colptr[end] == nnz + 1`
      - `colptr[i] <= colptr[i + 1]`
      - for all `j`, for all `i` such that `colptr[j] <= i` and `i + 1 < colptr[j + 1]`, `1 <= rowptr[i] < rowptr[i + 1] <= nrows`

This first verifies the `rows_axis` and `columns_axis` exist in the `container`. If `overwrite` is `false` (the
default), this also verifies the `name` matrix does not exist for the `rows_axis` and `columns_axis`.
"""
function empty_sparse_matrix!(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I};
    overwrite::Bool = false,
)::SparseMatrixCSC{T, I} where {T <: Number, I <: Integer}
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)

    if !overwrite
        require_no_matrix(container, rows_axis, columns_axis, name)
    end

    return format_empty_sparse_matrix!(container, rows_axis, columns_axis, name, eltype, nnz, indtype)
end

"""
    delete_matrix!(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a matrix property with some `name` for some `rows_axis` and `columns_axis` from the `container`.

This first verifies the `rows_axis` and `columns_axis` exist in the `container`. If `must_exist` is `true` (the
default), this also verifies the `name` matrix exists for the `rows_axis` and `columns_axis`.
"""
function delete_matrix!(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
)::Nothing
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)

    if must_exist
        require_matrix(container, rows_axis, columns_axis, name)
    elseif !has_matrix(container, rows_axis, columns_axis, name)
        return nothing
    end

    format_delete_matrix!(container, rows_axis, columns_axis, name)
    return nothing
end

"""
    matrix_names(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
    )::Set{String}

The names of the matrix properties for the `rows_axis` and `columns_axis` in the `container`.

This first verifies the `rows_axis` and `columns_axis` exist in the `container`.
"""
function matrix_names(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)
    return format_matrix_names(container, rows_axis, columns_axis)
end

function require_matrix(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    if !has_matrix(container, rows_axis, columns_axis, name)
        error(
            "missing matrix property: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end
    return nothing
end

function require_no_matrix(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    if has_matrix(container, rows_axis, columns_axis, name)
        error(
            "existing matrix property: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end
    return nothing
end

"""
    get_matrix(
        container::Container,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
        [; default::Union{StorageScalar, StorageMatrix}]
    )::StorageMatrix

get the matrix property with some `name` for some `rows_axis` and `columns_axis` in the `container`.

This first verifies the `rows_axis` and `columns_axis` exist in the `container`. If `default` is not specified, this
first verifies the `name` matrix exists in the `container`. Otherwise, if `default` is a `StorageMatrix`, it has to be
of the same size as the `rows_axis` and `columns_axis`, and is returned. Otherwise, a new `Matrix` is created of the
correct size containing the `default`, and is returned.
"""
function get_matrix(
    container::Container,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageMatrix, Nothing} = nothing,
)::StorageMatrix
    require_axis(container, rows_axis)
    require_axis(container, columns_axis)

    if default isa StorageMatrix
        require_storage_matrix(default)
        require_column_major(default)
        require_axis_length(container, "default rows", size(default, Rows), rows_axis)
        require_axis_length(container, "default columns", size(default, Columns), columns_axis)
    end

    if default == nothing
        require_matrix(container, rows_axis, columns_axis, name)
    elseif !has_matrix(container, rows_axis, columns_axis, name)
        if default isa StorageMatrix
            return default
        else
            return fill(default, axis_length(container, rows_axis), axis_length(container, columns_axis))
        end
    end

    matrix = format_get_matrix(container, rows_axis, columns_axis, name)
    if !(matrix isa StorageMatrix)
        error( # untested
            "format_get_matrix for Daf.Format: $(typeof(container))\n" *
            "returned invalid Daf.StorageMatrix: $(typeof(matrix))",
        )
    end

    if size(matrix, Rows) != axis_length(container, rows_axis)
        error( # untested
            "format_get_matrix for Daf.Format: $(typeof(container))\n" *
            "returned matrix rows: $(size(matrix, Rows))\n" *
            "instead of axis: $(axis)\n" *
            "length: $(axis_length(container, rows_axis))\n" *
            "in the Daf.Container: $(container.name)",
        )
    end

    if size(matrix, Columns) != axis_length(container, columns_axis)
        error( # untested
            "format_get_matrix for Daf.Format: $(typeof(container))\n" *
            "returned matrix columns: $(size(matrix, Columns))\n" *
            "instead of axis: $(axis)\n" *
            "length: $(axis_length(container, columns_axis))\n" *
            "in the Daf.Container: $(container.name)",
        )
    end

    if major_axis(matrix) != Columns
        error( # untested
            "format_get_matrix for Daf.Format: $(typeof(container))\n" *
            "returned non column-major matrix: $(typeof(matrix))",
        )
    end

    return matrix
end

function require_column_major(matrix::StorageMatrix)::Nothing
    if major_axis(matrix) != Columns
        error("type: $(typeof(matrix)) is not in column-major layout")
    end
end

function require_axis_length(
    container::Container,
    what_name::AbstractString,
    what_length::Integer,
    axis::AbstractString,
)::Nothing
    if what_length != axis_length(container, axis)
        error(
            "$(what_name): $(what_length)\n" *
            "is different from the length: $(axis_length(container, axis))\n" *
            "of the axis: $(axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end
    return nothing
end

function require_not_name(container::Container, axis::AbstractString, name::AbstractString)::Nothing
    if name == "name"
        error(
            "setting the reserved property: name\n" *
            "for the axis: $(axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end
    return nothing
end

"""
    description(container::Container)::AbstractString

Return a (multi-line) description of the contents of some `container`. This tries to hit a sweet spot between usefulness
and terseness.
"""
function description(container::Container)::AbstractString
    lines = String[]

    push!(lines, "type: $(typeof(container))")
    push!(lines, "name: $(container.name)")

    scalars_description(container, lines)

    axes = collect(axis_names(container))
    sort!(axes)
    if !isempty(axes)
        axes_description(container, axes, lines)
        vectors_description(container, axes, lines)
        matrices_description(container, axes, lines)
    end

    push!(lines, "")
    return join(lines, "\n")
end

function scalars_description(container::Container, lines::Vector{String})::Nothing
    scalars = collect(scalar_names(container))
    if !isempty(scalars)
        sort!(scalars)
        push!(lines, "scalars:")
        for scalar in scalars
            push!(lines, "  $(scalar): $(present(get_scalar(container, scalar)))")
        end
    end
    return nothing
end

function axes_description(container::Container, axes::Vector{String}, lines::Vector{String})::Nothing
    push!(lines, "axes:")
    for axis in axes
        push!(lines, "  $(axis): $(axis_length(container, axis)) entries")
    end
    return nothing
end

function vectors_description(container::Container, axes::Vector{String}, lines::Vector{String})::Nothing
    is_first = true
    for axis in axes
        vectors = collect(vector_names(container, axis))
        if !isempty(vectors)
            if is_first
                push!(lines, "vectors:")
                is_first = false
            end
            sort!(vectors)
            push!(lines, "  $(axis):")
            for vector in vectors
                push!(lines, "    $(vector): $(present(get_vector(container, axis, vector)))")
            end
        end
    end
    return nothing
end

function matrices_description(container::Container, axes::Vector{String}, lines::Vector{String})::Nothing
    is_first = true
    for rows_axis in axes
        for columns_axis in axes
            matrices = collect(matrix_names(container, rows_axis, columns_axis))
            if !isempty(matrices)
                if is_first
                    push!(lines, "matrices:")
                    is_first = false
                end
                sort!(matrices)
                push!(lines, "  $(rows_axis),$(columns_axis):")
                for matrix in matrices
                    push!(lines, "    $(matrix): $(present(get_matrix(container, rows_axis, columns_axis, matrix)))")
                end
            end
        end
    end
    return nothing
end

"""
    matrix_query(container::Container, query::AbstractString)::Union{StorageMatrix, Nothing}

Query the `container` for some matrix results. See [`MatrixQuery`](@ref) for the possible queries that return matrix
results.
"""
function matrix_query(container::Container, query::AbstractString)::Union{StorageMatrix, Nothing}
    return matrix_query(container, parse_matrix_query(query))
end

function matrix_query(container::Container, matrix_query::MatrixQuery)::Union{StorageMatrix, Nothing}
    result = compute_matrix_lookup(container, matrix_query.matrix_property_lookup)
    result = compute_eltwise_result(matrix_query.eltwise_operations, result)
    return result
end

function compute_matrix_lookup(
    container::Container,
    matrix_property_lookup::MatrixPropertyLookup,
)::Union{AbstractMatrix, Nothing}
    result = get_matrix(
        container,
        matrix_property_lookup.matrix_axes.rows_axis.axis_name,
        matrix_property_lookup.matrix_axes.columns_axis.axis_name,
        matrix_property_lookup.property_name,
    )

    rows_mask = compute_filtered_axis(container, matrix_property_lookup.matrix_axes.rows_axis)
    columns_mask = compute_filtered_axis(container, matrix_property_lookup.matrix_axes.columns_axis)

    if (rows_mask != nothing && !any(rows_mask)) || (columns_mask != nothing && !any(columns_mask))
        return nothing
    end

    if rows_mask != nothing && columns_mask != nothing
        result = result[rows_mask, columns_mask]
    elseif rows_mask != nothing
        result = result[rows_mask, :]  # untested
    elseif columns_mask != nothing
        result = result[:, columns_mask]  # untested
    end

    return result
end

function compute_filtered_axis(container::Container, filtered_axis::FilteredAxis)::Union{Vector{Bool}, Nothing}
    if isempty(filtered_axis.axis_filters)
        return nothing
    end

    mask = fill(true, axis_length(container, filtered_axis.axis_name))
    for axis_filter in filtered_axis.axis_filters
        mask = compute_axis_filter(container, mask, filtered_axis.axis_name, axis_filter)
    end

    return mask
end

function compute_axis_filter(
    container::Container,
    mask::Vector{Bool},
    axis::AbstractString,
    axis_filter::AxisFilter,
)::Vector{Bool}
    filter = compute_axis_lookup(container, axis, axis_filter.axis_lookup)
    if eltype(filter) != Bool
        error(
            "non-Bool data type: $(eltype(filter))\n" *
            "for the axis filter: $(canonical(axis_filter))\n" *
            "in the Daf.Container: $(container.name)",
        )
    end

    if axis_filter.axis_lookup.is_inverse
        filter = .!filter
    end

    if axis_filter.filter_operator == FilterAnd
        return .&(mask, filter)
    elseif axis_filter.filter_operator == FilterOr   # untested
        return .|(mask, filter)                      # untested
    elseif axis_filter.filter_operator == FilterXor  # untested
        return @. xor(mask, filter)                  # untested
    else
        @assert false  # untested
    end
end

function compute_axis_lookup(container::Container, axis::AbstractString, axis_lookup::AxisLookup)::Vector
    values = compute_property_lookup(container, axis, axis_lookup.property_lookup)

    if axis_lookup.property_comparison == nothing
        return values

    elseif axis_lookup.property_comparison.comparison_operator == CmpMatch ||
           axis_lookup.property_comparison.comparison_operator == CmpNotMatch
        return compute_axis_lookup_match(container, axis, axis_lookup, values)

    else
        return compute_axis_lookup_compare(container, axis, axis_lookup, values)
    end
end

function compute_axis_lookup_match(
    container::Container,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    values::Vector,
)::Vector
    if eltype(values) != String
        error(
            "non-String data type: $(eltype(values))\n" *
            "for the match axis lookup: $(canonical(axis_lookup))\n" *
            "for the axis: $(axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end

    regex = nothing
    try
        regex = Regex("^(?:" * axis_lookup.property_comparison.property_value * ")\$")
    catch
        error(
            "invalid Regex: \"$(escape_string(axis_lookup.property_comparison.property_value))\"\n" *
            "for the axis lookup: $(canonical(axis_lookup))\n" *
            "for the axis: $(axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end

    if axis_lookup.property_comparison.comparison_operator == CmpMatch
        return [match(regex, value) != nothing for value in values]
    elseif axis_lookup.property_comparison.comparison_operator == CmpNotMatch  # untested
        return [match(regex, value) == nothing for value in values]                        # untested
    else
        @assert false  # untested
    end
end

function compute_axis_lookup_compare(
    container::Container,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    values::Vector,
)::Vector
    value = axis_lookup.property_comparison.property_value
    if eltype(values) != String
        try
            value = parse(eltype(values), value)
        catch
            error(
                "invalid $(eltype) value: \"$(escape_string(axis_lookup.property_comparison.property_value))\"\n" *
                "for the axis lookup: $(canonical(axis_lookup))\n" *
                "for the axis: $(axis)\n" *
                "in the Daf.Container: $(container.name)",
            )
        end
    end

    if axis_lookup.property_comparison.comparison_operator == CmpLessThan
        return values .< value
    elseif axis_lookup.property_comparison.comparison_operator == CmpLessOrEqual
        return values .<= value  # untested
    elseif axis_lookup.property_comparison.comparison_operator == CmpEqual
        return values .== value
    elseif axis_lookup.property_comparison.comparison_operator == CmpNotEqual
        return values .!= value                                                      # untested
    elseif axis_lookup.property_comparison.comparison_operator == CmpGreaterThan
        return values .> value
    elseif axis_lookup.property_comparison.comparison_operator == CmpGreaterOrEqual  # untested
        return values .>= value                                                      # untested
    else
        @assert false  # untested
    end
end

function compute_property_lookup(container::Container, axis::AbstractString, property_lookup::PropertyLookup)::Vector
    last_property_name = property_lookup.property_names[1]
    values = get_vector(container, axis, last_property_name)

    for next_property_name in property_lookup.property_names[2:end]
        if eltype(values) != String
            error(
                "non-String data type: $(eltype(values))\n" *
                "for the chained property: $(last_property_name)\n" *
                "for the axis: $(axis)\n" *
                "in the Daf.Container: $(container.name)",
            )
        end
        values, axis = compute_chained_property(container, axis, last_property_name, values, next_property_name)
        last_property_name = next_property_name
    end

    return values
end

function compute_chained_property(
    container::Container,
    last_axis::AbstractString,
    last_property_name::AbstractString,
    last_property_values::Vector{String},
    next_property_name::AbstractString,
)::Tuple{Vector, String}
    if has_axis(container, last_property_name)
        next_axis = last_property_name
    else
        next_axis = split(last_property_name, "."; limit = 2)[1]
    end

    next_axis_entries = get_axis(container, next_axis)
    next_axis_values = get_vector(container, next_axis, next_property_name)

    return (
        [
            find_axis_value(
                container,
                last_axis,
                last_property_name,
                property_value,
                next_axis,
                next_axis_entries,
                next_axis_values,
            ) for property_value in last_property_values
        ],
        next_axis,
    )
end

function find_axis_value(
    container::Container,
    last_axis::AbstractString,
    last_property_name::AbstractString,
    last_property_value::AbstractString,
    next_axis::AbstractString,
    next_axis_entries::Vector{String},
    next_axis_values::Vector,
)::Any
    index = findfirst(==(last_property_value), next_axis_entries)
    if index == nothing
        error(
            "invalid value: $(last_property_value)\n" *
            "of the chained property: $(last_property_name)\n" *
            "of the axis: $(last_axis)\n" *
            "is missing from the next axis: $(next_axis)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end
    return next_axis_values[index]
end

"""
    vector_query(container::Container, query::AbstractString)::Union{StorageVector, Nothing}

Query the `container` for some vector results. See [`VectorQuery`](@ref) for the possible queries that return vector
results.
"""
function vector_query(container::Container, query::AbstractString)::Union{StorageVector, Nothing}
    return vector_query(container, parse_vector_query(query))
end

function vector_query(container::Container, vector_query::VectorQuery)::Union{StorageVector, Nothing}
    result = compute_vector_data_lookup(container, vector_query.vector_data_lookup)
    result = compute_eltwise_result(vector_query.eltwise_operations, result)
    return result
end

function compute_vector_data_lookup(
    container::Container,
    vector_property_lookup::VectorPropertyLookup,
)::Union{StorageVector, Nothing}
    result = compute_axis_lookup(
        container,
        vector_property_lookup.filtered_axis.axis_name,
        vector_property_lookup.axis_lookup,
    )
    mask = compute_filtered_axis(container, vector_property_lookup.filtered_axis)

    if mask != nothing && !any(mask)
        return nothing
    end

    if mask != nothing
        result = result[mask]
    end

    return result
end

function compute_vector_data_lookup(
    container::Container,
    matrix_slice_lookup::MatrixSliceLookup,
)::Union{StorageVector, Nothing}
    result = get_matrix(
        container,
        matrix_slice_lookup.matrix_slice_axes.filtered_axis.axis_name,
        matrix_slice_lookup.matrix_slice_axes.axis_entry.axis_name,
        matrix_slice_lookup.property_name,
    )

    index = find_axis_entry_index(container, matrix_slice_lookup.matrix_slice_axes.axis_entry)
    result = result[:, index]

    rows_mask = compute_filtered_axis(container, matrix_slice_lookup.matrix_slice_axes.filtered_axis)
    if rows_mask != nothing
        result = result[rows_mask]
    end

    return result
end

function compute_vector_data_lookup(
    container::Container,
    reduce_matrix_query::ReduceMatrixQuery,
)::Union{StorageVector, Nothing}
    result = matrix_query(container, reduce_matrix_query.matrix_query)
    if result == nothing
        return nothing
    end
    return compute_reduction_result(reduce_matrix_query.reduction_operation, result)
end

"""
    scalar_query(container::Container, query::AbstractString)::Union{StorageScalar, Nothing}

Query the `container` for some scalar results. See [`ScalarQuery`](@ref) for the possible queries that return scalar
results.
"""
function scalar_query(container::Container, query::AbstractString)::Union{StorageScalar, Nothing}
    return scalar_query(container, parse_scalar_query(query))
end

function scalar_query(container::Container, scalar_query::ScalarQuery)::Union{StorageScalar, Nothing}
    result = compute_scalar_data_lookup(container, scalar_query.scalar_data_lookup)
    result = compute_eltwise_result(scalar_query.eltwise_operations, result)
    return result
end

function compute_scalar_data_lookup(
    container::Container,
    scalar_property_lookup::ScalarPropertyLookup,
)::Union{StorageScalar, Nothing}
    return get_scalar(container, scalar_property_lookup.property_name)
end

function compute_scalar_data_lookup(
    container::Container,
    reduce_vector_query::ReduceVectorQuery,
)::Union{StorageScalar, Nothing}
    result = vector_query(container, reduce_vector_query.vector_query)
    return compute_reduction_result(reduce_vector_query.reduction_operation, result)
end

function compute_scalar_data_lookup(
    container::Container,
    vector_entry_lookup::VectorEntryLookup,
)::Union{StorageScalar, Nothing}
    result = compute_axis_lookup(container, vector_entry_lookup.axis_entry.axis_name, vector_entry_lookup.axis_lookup)
    index = find_axis_entry_index(container, vector_entry_lookup.axis_entry)
    return result[index]
end

function compute_scalar_data_lookup(
    container::Container,
    matrix_entry_lookup::MatrixEntryLookup,
)::Union{StorageScalar, Nothing}
    result = get_matrix(
        container,
        matrix_entry_lookup.matrix_entry_axes.rows_entry.axis_name,
        matrix_entry_lookup.matrix_entry_axes.columns_entry.axis_name,
        matrix_entry_lookup.property_name,
    )
    row_index = find_axis_entry_index(container, matrix_entry_lookup.matrix_entry_axes.rows_entry)
    column_index = find_axis_entry_index(container, matrix_entry_lookup.matrix_entry_axes.columns_entry)
    return result[row_index, column_index]
end

function find_axis_entry_index(container::Container, axis_entry::AxisEntry)::Int
    axis_entries = get_axis(container, axis_entry.axis_name)
    index = findfirst(==(axis_entry.entry_name), axis_entries)
    if index == nothing
        error(
            "the entry: $(axis_entry.entry_name)\n" *
            "is missing from the axis: $(axis_entry.axis_name)\n" *
            "in the Daf.Container: $(container.name)",
        )
    end
    return index
end

function compute_eltwise_result(
    eltwise_operations::Vector{EltwiseOperation},
    result::Union{StorageMatrix, StorageVector, StorageScalar, Nothing},
)::Union{StorageMatrix, StorageVector, StorageScalar, Nothing}
    if result == nothing
        return nothing
    else
        for eltwise_operation in eltwise_operations
            if !(eltype(result) <: Number)
                error(
                    "non-numeric input: $(typeof(result))\n" *
                    "for the eltwise operation: $(canonical(eltwise_operation))\n",
                )
            end
            result = compute_eltwise(eltwise_operation, result)
        end
        return result
    end
end

function compute_reduction_result(
    reduction_operation::ReductionOperation,
    result::Union{StorageMatrix, StorageVector, Nothing},
)::Union{StorageVector, StorageScalar, Nothing}
    if result == nothing
        return nothing
    else
        if !(eltype(result) <: Number)
            error(
                "non-numeric input: $(typeof(result))\n" *
                "for the reduction operation: $(canonical(reduction_operation))\n",
            )
        end
        return compute_reduction(reduction_operation, result)
    end
end

end # module
