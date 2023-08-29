"""
The [`DafReader`](@ref) and [`DafWriter`](@ref) interfaces specify a high-level API for accessing `Daf` data. This API is
implemented here, on top of the low-level [`FormatReader`](@ref) and [`FormatWriter`](@ref) API.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each *unordered* pair of
axes.

For matrices, we keep careful track of their [`MatrixLayouts`](@ref). Returned matrices are always in column-major
layout, using [`relayout!`](@ref) if necessary. As this is an expensive operation, we'll cache the result in memory.
Similarly, we cache the results of applying a query to the data. We allow clearing the cache to reduce memory usage, if
necessary.

The data API is the high-level API intended to be used from outside the package, and is therefore re-exported from the
top-level `Daf` namespace. It provides additional functionality on top of the low-level [`FormatReader`](@ref) and
[`FormatWriter`](@ref) implementations, accepting more general data types, automatically dealing with [`relayout!`](@ref)
when needed, and even providing a language for [`Queries`](@ref) for flexible extraction of data from the container.
"""
module Data

export add_axis!
export axis_length
export axis_names
export delete_axis!
export delete_matrix!
export delete_scalar!
export delete_vector!
export description
export empty_cache!
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
export relayout_matrix!
export scalar_names
export scalar_query
export set_matrix!
export set_scalar!
export set_vector!
export vector_names
export vector_query

using Daf.Formats
using Daf.MatrixLayouts
using Daf.Messages
using Daf.Oprec
using Daf.Queries
using Daf.Registry
using Daf.StorageTypes
using NamedArrays
using SparseArrays

import Daf.Formats
import Daf.Formats.FormatReader
import Daf.Formats.FormatWriter
import Daf.Messages
import Daf.Queries.CmpDefault
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

function Base.getproperty(daf::DafReader, property::Symbol)::Any
    if property == :name
        return daf.internal.name
    else
        return getfield(daf, property)
    end
end

"""
    has_scalar(daf::DafReader, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in `daf`.
"""
function has_scalar(daf::DafReader, name::AbstractString)::Bool
    result = Formats.format_has_scalar(daf, name)
    # @debug "has_scalar $(daf.name) / $(name) -> $(present(result))"
    return result
end

"""
    set_scalar!(
        daf::DafWriter,
        name::AbstractString,
        value::StorageScalar;
        [overwrite::Bool = false]
    )::Nothing

Set the `value` of a scalar property with some `name` in `daf`.

If not `overwrite` (the default), this first verifies the `name` scalar property does not exist.
"""
function set_scalar!(daf::DafWriter, name::AbstractString, value::StorageScalar; overwrite::Bool = false)::Nothing
    @debug "set_scalar! $(daf.name) / $(name) <$(overwrite ? "=" : "-") $(present(value))"

    if !overwrite
        require_no_scalar(daf, name)
    elseif Formats.format_has_scalar(daf, name)
        Formats.format_delete_scalar!(daf, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, scalar_dependency_key(name))

    Formats.format_set_scalar!(daf, name, value)
    return nothing
end

"""
    delete_scalar!(
        daf::DafWriter,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a scalar property with some `name` from `daf`.

If `must_exist` (the default), this first verifies the `name` scalar property exists in `daf`.
"""
function delete_scalar!(daf::DafWriter, name::AbstractString; must_exist::Bool = true)::Nothing
    @debug "delete_scalar! $(daf.name) / $(name)$(must_exist ? "" : " ?")"

    if must_exist
        require_scalar(daf, name)
    end

    if Formats.format_has_scalar(daf, name)
        Formats.format_delete_scalar!(daf, name; for_set = false)
    end

    invalidate_cached_dependencies!(daf, scalar_dependency_key(name))

    return nothing
end

function scalar_dependency_key(name::AbstractString)::String
    return escape_query(name)
end

"""
    scalar_names(daf::DafReader)::Set{String}

The names of the scalar properties in `daf`.
"""
function scalar_names(daf::DafReader)::AbstractSet{String}
    result = Formats.format_scalar_names(daf)
    # @debug "scalar_names $(daf.name) -> $(present(result))"
    return result
end

"""
    get_scalar(
        daf::DafReader,
        name::AbstractString;
        [default::Union{StorageScalar, Nothing, Missing} = nothing]
    )::Union{StorageScalar, Missing}

Get the value of a scalar property with some `name` in `daf`.

If `default` is `nothing` (the default), this first verifies the `name` scalar property exists in `daf`. Otherwise
`default` will be returned of it does not exist.
"""
function get_scalar(
    daf::DafReader,
    name::AbstractString;
    default::Union{StorageScalar, Nothing, Missing} = nothing,
)::Union{StorageScalar, Missing}
    if default !== missing && default == nothing
        require_scalar(daf, name)
    end

    if has_scalar(daf, name)
        result = Formats.format_get_scalar(daf, name)
        @debug "get_scalar $(daf.name) / $(name) -> $(present(result))"
    else
        result = default
        @debug "get_scalar $(daf.name) / $(name) -> $(present(result)) ?"
    end

    return result
end

function require_scalar(daf::DafReader, name::AbstractString)::Nothing
    if !Formats.format_has_scalar(daf, name)
        error("missing scalar: $(name)\nin the daf data: $(daf.name)")
    end
    return nothing
end

function require_no_scalar(daf::DafReader, name::AbstractString)::Nothing
    if Formats.format_has_scalar(daf, name)
        error("existing scalar: $(name)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_axis(daf::DafReader, axis::AbstractString)::Bool

Check whether some `axis` exists in `daf`.
"""
function has_axis(daf::DafReader, axis::AbstractString)::Bool
    result = Formats.format_has_axis(daf, axis)
    # @debug "has_axis $(daf.name) / $(axis) -> $(present(result))"
    return result
end

"""
    add_axis!(
        daf::DafWriter,
        axis::AbstractString,
        entries::AbstractVector{String}
    )::Nothing

Add a new `axis` `daf`.

This first verifies the `axis` does not exist and that the `entries` are unique.
"""
function add_axis!(daf::DafWriter, axis::AbstractString, entries::AbstractVector{String})::Nothing
    @debug "add_axis $(daf.name) / $(axis) <- $(present(entries))"

    require_no_axis(daf, axis)

    if !allunique(entries)
        error("non-unique entries for new axis: $(axis)\nin the daf data: $(daf.name)")
    end

    Formats.format_add_axis!(daf, axis, entries)
    return nothing
end

"""
    delete_axis!(
        daf::DafWriter,
        axis::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete an `axis` from the `daf`. This will also delete any vector or matrix properties that are based on this axis.

If `must_exist` (the default), this first verifies the `axis` exists in the `daf`.
"""
function delete_axis!(daf::DafWriter, axis::AbstractString; must_exist::Bool = true)::Nothing
    @debug "delete_axis! $(daf.name) / $(axis)$(must_exist ? "" : " ?")"

    if must_exist
        require_axis(daf, axis)
    end

    if !Formats.format_has_axis(daf, axis)
        return nothing
    end

    invalidate_cached_dependencies!(daf, axis_dependency_key(axis))

    for name in Formats.format_vector_names(daf, axis)
        Formats.format_delete_vector!(daf, axis, name; for_set = false)
    end

    for other_axis in Formats.format_axis_names(daf)
        for name in Formats.format_matrix_names(daf, axis, other_axis)
            Formats.format_delete_matrix!(daf, axis, other_axis, name; for_set = false)
        end
        for name in Formats.format_matrix_names(daf, other_axis, axis)
            Formats.format_delete_matrix!(daf, other_axis, axis, name; for_set = false)
        end
    end

    Formats.format_delete_axis!(daf, axis)
    return nothing
end

function axis_dependency_key(axis::AbstractString)::String
    return "$(escape_query(axis)) @"
end

"""
    axis_names(daf::DafReader)::AbstractSet{String}

The names of the axes of `daf`.
"""
function axis_names(daf::DafReader)::AbstractSet{String}
    result = Formats.format_axis_names(daf)
    # @debug "axis_names $(daf.name) -> $(present(result))"
    return result
end

"""
    get_axis(
        daf::DafReader,
        axis::AbstractString
        [; default::Union{Nothing, Missing} = nothing]
    )::Union{AbstractVector{String}, Missing}

The unique names of the entries of some `axis` of `daf`. This is similar to doing [`get_vector`](@ref) for the special
`name` property, except that it returns a simple vector of strings instead of a `NamedVector`.

If `default` is `missing`, it is returned if the `axis` does not exist. Otherwise (the default), this verifies the
`axis` exists in `daf`.
"""
function get_axis(
    daf::DafReader,
    axis::AbstractString;
    default::Union{Nothing, Missing} = nothing,
)::Union{AbstractVector{String}, Missing}
    if !has_axis(daf, axis)
        if default === missing
            @debug "get_axis! $(daf.name) / $(axis) -> $(present(missing))"
            return missing
        else
            @assert default == nothing
            require_axis(daf, axis)
        end
    end

    result = as_read_only(Formats.format_get_axis(daf, axis))
    @debug "get_axis! $(daf.name) / $(axis) -> $(present(result))"
    return result
end

"""
    axis_length(daf::DafReader, axis::AbstractString)::Int64

The number of entries along the `axis` in `daf`.

This first verifies the `axis` exists in `daf`.
"""
function axis_length(daf::DafReader, axis::AbstractString)::Int64
    require_axis(daf, axis)
    result = Formats.format_axis_length(daf, axis)
    # @debug "axis_length! $(daf.name) / $(axis) -> $(present(result))"
    return result
end

function require_axis(daf::DafReader, axis::AbstractString)::Nothing
    if !Formats.format_has_axis(daf, axis)
        error("missing axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

function require_no_axis(daf::DafReader, axis::AbstractString)::Nothing
    if Formats.format_has_axis(daf, axis)
        error("existing axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Bool

Check whether a vector property with some `name` exists for the `axis` in `daf`. This is always true for the special
`name` property.

This first verifies the `axis` exists in `daf`.
"""
function has_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Bool
    require_axis(daf, axis)
    result = name == "name" || Formats.format_has_vector(daf, axis, name)
    # @debug "has_vector $(daf.name) / $(axis) / $(name) -> $(present(result))"
    return result
end

"""
    set_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector};
        [overwrite::Bool = false]
    )::Nothing

Set a vector property with some `name` for some `axis` in `daf`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This first verifies the `axis` exists in `daf`, that the property name isn't `name`, and that the `vector` has the
appropriate length. If not `overwrite` (the default), this also verifies the `name` vector does not exist for the
`axis`.
"""
function set_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector};
    overwrite::Bool = false,
)::Nothing
    @debug "set_vector! $(daf.name) / $(axis) / $(name) <$(overwrite ? "=" : "-") $(present(vector))"

    require_not_name(daf, axis, name)
    require_axis(daf, axis)

    if vector isa StorageVector
        require_axis_length(daf, "vector length", length(vector), axis)
        if vector isa NamedVector
            require_dim_name(daf, axis, "vector dim name", dimnames(vector, 1))
            require_axis_names(daf, axis, "entry names of the: vector", names(vector, 1))
        end
    end

    if !overwrite
        require_no_vector(daf, axis, name)
    elseif Formats.format_has_vector(daf, axis, name)
        Formats.format_delete_vector!(daf, axis, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, vector_dependency_key(axis, name))

    Formats.format_set_vector!(daf, axis, name, vector)
    return nothing
end

"""
    empty_dense_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T};
        [overwrite::Bool = false]
    )::Any where {T <: Number}

Create an empty dense vector property with some `name` for some `axis` in `daf`, pass it to `fill`, and return the result.

The returned vector will be uninitialized; the caller is expected to `fill` it with values. This saves creating a copy of
the vector before setting it in the data, which makes a huge difference when creating vectors on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If not `overwrite` (the
default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_dense_vector!(
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: Number}
    require_not_name(daf, axis, name)
    require_axis(daf, axis)

    if !overwrite
        require_no_vector(daf, axis, name)
    elseif Formats.format_has_vector(daf, axis, name)
        Formats.format_delete_vector!(daf, axis, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, vector_dependency_key(axis, name))

    result = as_named_vector(daf, axis, Formats.format_empty_dense_vector!(daf, axis, name, eltype))
    @debug "empty_dense_vector! $(daf.name) / $(axis) / $(name) <$(overwrite ? "=" : "-") $(present(result))"
    return fill(result)
end

"""
    empty_sparse_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::Integer,
        indtype::Type{I};
        [overwrite::Bool = false]
    )::Any where {T <: Number, I <: Integer}

Create an empty sparse vector property with some `name` for some `axis` in `daf`, pass it to `fill` and return the
result.

The returned vector will be uninitialized; the caller is expected to `fill` it with values. This means manually filling
the `nzind` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow pre-allocating disk
data. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse vector is created by concatenating several smaller ones; this function
allows doing so directly into the data vector, avoiding a copy in case of memory-mapped disk formats.

!!! warning

    It is the caller's responsibility to fill the three vectors with valid data. Specifically, you must ensure:

      - `nzind[1] == 1`
      - `nzind[i] <= nzind[i + 1]`
      - `nzind[end] == nnz`

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If not `overwrite` (the
default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_sparse_vector!(
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I};
    overwrite::Bool = false,
)::Any where {T <: Number, I <: Integer}
    require_not_name(daf, axis, name)
    require_axis(daf, axis)

    if !overwrite
        require_no_vector(daf, axis, name)
    elseif Formats.format_has_vector(daf, axis, name)
        Formats.format_delete_vector!(daf, axis, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, vector_dependency_key(axis, name))

    empty_vector = Formats.format_empty_sparse_vector!(daf, axis, name, eltype, nnz, indtype)
    result = fill(as_named_vector(daf, axis, empty_vector))
    verified = SparseVector(length(empty_vector), empty_vector.nzind, empty_vector.nzval)
    @debug "empty_dense_vector! $(daf.name) / $(axis) / $(name) <$(overwrite ? "=" : "-") $(present(verified))"
    return result
end

"""
    delete_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a vector property with some `name` for some `axis` from `daf`.

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If `must_exist` (the default),
this also verifies the `name` vector exists for the `axis`.
"""
function delete_vector!(daf::DafWriter, axis::AbstractString, name::AbstractString; must_exist::Bool = true)::Nothing
    @debug "delete_vector! $(daf.name) / $(axis) / $(name) $(must_exist ? "" : " ?")"

    require_not_name(daf, axis, name)
    require_axis(daf, axis)

    if must_exist
        require_vector(daf, axis, name)
    end

    if !Formats.format_has_vector(daf, axis, name)
        return nothing
    end

    invalidate_cached_dependencies!(daf, vector_dependency_key(axis, name))

    Formats.format_delete_vector!(daf, axis, name; for_set = false)
    return nothing
end

function vector_dependency_key(axis::AbstractString, name::AbstractString)::String
    return "$(escape_query(axis)) @ $(escape_query(name))"
end

"""
    vector_names(daf::DafReader, axis::AbstractString)::Set{String}

The names of the vector properties for the `axis` in `daf`, **not** including the special `name` property.

This first verifies the `axis` exists in `daf`.
"""
function vector_names(daf::DafReader, axis::AbstractString)::AbstractSet{String}
    require_axis(daf, axis)
    result = Formats.format_vector_names(daf, axis)
    # @debug "vector_names $(daf.name) / $(axis) -> $(present(result))"
    return result
end

"""
    get_vector(
        daf::DafReader,
        axis::AbstractString,
        name::AbstractString;
        [default::Union{StorageScalar, StorageVector, Nothing, Missing} = nothing]
    )::Union{NamedVector, Missing}

Get the vector property with some `name` for some `axis` in `daf`. The names of the result are the names of the vector
entries (same as returned by [`get_axis`](@ref)). The special property `name` returns an array whose values are also the
(read-only) names of the entries of the axis.

This first verifies the `axis` exists in `daf`. If `default` is `nothing` (the default), this first verifies the `name`
vector exists in `daf`. Otherwise, if `default` is `missing`, it will be returned. If it is a `StorageVector`, it has to
be of the same size as the `axis`, and is returned. If it is a [`StorageScalar`](@ref). Otherwise, a new `Vector` is
created of the correct size containing the `default`, and is returned.
"""
function get_vector(
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageVector, Nothing, Missing} = nothing,
)::Union{NamedArray, Missing}
    require_axis(daf, axis)

    if name == "name"
        result = as_named_vector(daf, axis, as_read_only(Formats.format_get_axis(daf, axis)))
        @debug "get_vector $(daf.name) / $(axis) / $(name) -> $(present(result))"
        return result
    end

    if default !== missing && default isa StorageVector
        require_axis_length(daf, "default length", length(default), axis)
        if default isa NamedVector
            require_dim_name(daf, axis, "default dim name", dimnames(default, 1))
            require_axis_names(daf, axis, "entry names of the: default", names(default, 1))
        end
    end

    default_suffix = ""
    vector = nothing
    if !Formats.format_has_vector(daf, axis, name)
        if default === missing
            @debug "get_vector $(daf.name) / $(axis) / $(name) -> $(present(missing))"
            return missing
        end
        default_suffix = " ?"
        if default isa StorageVector
            vector = default
        elseif default isa StorageScalar
            vector = fill(default, Formats.format_axis_length(daf, axis))
        end
    end

    if vector == nothing
        require_vector(daf, axis, name)
        vector = Formats.format_get_vector(daf, axis, name)
        if !(vector isa StorageVector)
            error(  # untested
                "format_get_vector for daf format: $(typeof(daf))\n" *
                "returned invalid Daf.StorageVector: $(typeof(vector))",
            )
        end
        if length(vector) != Formats.format_axis_length(daf, axis)
            error( # untested
                "format_get_vector for daf format: $(typeof(daf))\n" *
                "returned vector length: $(length(vector))\n" *
                "instead of axis: $(axis)\n" *
                "length: $(axis_length(daf, axis))\n" *
                "in the daf data: $(daf.name)",
            )
        end
    end

    result = as_named_vector(daf, axis, vector)
    @debug "get_vector $(daf.name) / $(axis) / $(name) -> $(present(result))$(default_suffix)"
    return result
end

function require_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if !Formats.format_has_vector(daf, axis, name)
        error("missing vector: $(name)\nfor the axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

function require_no_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if Formats.format_has_vector(daf, axis, name)
        error("existing vector: $(name)\nfor the axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    has_matrix(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [relayout::Bool = true]
    )::Bool

Check whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in `daf`. Since this
is Julia, this means a column-major matrix. A daf may contain two copies of the same data, in which case it would report
the matrix under both axis orders.

If `relayout` (the default), this will also check whether the data exists in the other layout (that is, with flipped
axes).

This first verifies the `rows_axis` and `columns_axis` exists in `daf`.
"""
function has_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool = true,
)::Bool
    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)
    result =
        Formats.format_has_matrix(daf, rows_axis, columns_axis, name) ||
        (relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name))
    # @debug "has_matrix $(daf) / $(rows_axis) / $(columns_axis) / $(name) $(relayout ? "%" : "#")> $(result)"
    return result
end

"""
    set_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::StorageMatrix;
        [overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing

Set the matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. Since this is Julia, this
should be a column-major `matrix`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

If `relayout` (the default), this will also automatically [`relayout!`](@ref) the matrix and store the result, so the
data would also be stored in row-major layout (that is, with the axes flipped), similarly to calling
[`relayout_matrix!`](@ref).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function set_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalar, StorageMatrix};
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    @debug "set_matrix! $(daf) / $(rows_axis) / $(columns_axis) / $(name) <$(relayout ? "%" : "#")$(overwrite ? "=" : "-") $(matrix)"

    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    if matrix isa StorageMatrix
        require_column_major(matrix)
        require_axis_length(daf, "matrix rows", size(matrix, Rows), rows_axis)
        require_axis_length(daf, "matrix columns", size(matrix, Columns), columns_axis)
        if matrix isa NamedMatrix
            require_dim_name(daf, rows_axis, "matrix rows dim name", dimnames(matrix, 1); prefix = "rows")
            require_dim_name(daf, columns_axis, "matrix columns dim name", dimnames(matrix, 2); prefix = "columns")
            require_axis_names(daf, rows_axis, "row names of the: matrix", names(matrix, 1))
            require_axis_names(daf, columns_axis, "column names of the: matrix", names(matrix, 2))
        end
    end

    if !overwrite
        require_no_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
    end

    invalidate_cached_dependencies!(daf, matrix_dependency_key(rows_axis, columns_axis, name))

    if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
    end
    Formats.format_set_matrix!(daf, rows_axis, columns_axis, name, matrix)

    if relayout
        if overwrite && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            Formats.format_delete_matrix!(daf, columns_axis, rows_axis, name; for_set = true)
        end
        Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name)
    end

    return nothing
end

"""
    empty_dense_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T};
        [overwrite::Bool = false]
    )::Any where {T <: Number}

Create an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`, pass it to
`fill`, and return the result. Since this is Julia, this will be a column-major `matrix`.

The returned matrix will be uninitialized; the caller is expected to `fill` it with values. This saves creating a copy
of the matrix before setting it in `daf`, which makes a huge difference when creating matrices on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function empty_dense_matrix!(
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: Number}
    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    if !overwrite
        require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
    elseif Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, matrix_dependency_key(rows_axis, columns_axis, name))

    named = as_named_matrix(
        daf,
        rows_axis,
        columns_axis,
        Formats.format_empty_dense_matrix!(daf, rows_axis, columns_axis, name, eltype),
    )
    result = fill(named)
    @debug "empty_dense_matrix! $(daf) / $(rows_axis) / $(columns_axis) / $(name) <$(overwrite ? "=" : "-") $(named)"
    return result
end

"""
    empty_sparse_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::Integer,
        intdype::Type{I};
        [overwrite::Bool = false]
    )::Any where {T <: Number, I <: Integer}

Create an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`, pass it to
`fill`, and return the result.

The returned matrix will be uninitialized; the caller is expected to `fill` it with values. This means manually filling
the `colptr`, `rowval` and `nzval` vectors. Specifying the `nnz` makes their sizes known in advance, to allow
pre-allocating disk space. For this reason, this does not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse matrix is created by concatenating several smaller ones; this function
allows doing so directly into the data, avoiding a copy in case of memory-mapped disk formats.

!!! warning


https://science.slashdot.org/story/23/08/12/1942234/common-alzheimers-disease-gene-may-have-helped-our-ancestors-have-more-kids
It is the caller's responsibility to fill the three vectors with valid data. Specifically, you must ensure:

      - `colptr[1] == 1`
      - `colptr[end] == nnz + 1`
      - `colptr[i] <= colptr[i + 1]`
      - for all `j`, for all `i` such that `colptr[j] <= i` and `i + 1 < colptr[j + 1]`, `1 <= rowptr[i] < rowptr[i + 1] <= nrows`

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If not `overwrite` (the default), this also
verifies the `name` matrix does not exist for the `rows_axis` and `columns_axis`.
"""
function empty_sparse_matrix!(
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::Integer,
    indtype::Type{I};
    overwrite::Bool = false,
)::Any where {T <: Number, I <: Integer}
    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    if !overwrite
        require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
    elseif Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, matrix_dependency_key(rows_axis, columns_axis, name))

    empty_matrix = Formats.format_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, eltype, nnz, indtype)
    result = fill(as_named_matrix(daf, rows_axis, columns_axis, empty_matrix))
    verified = SparseMatrixCSC(size(empty_matrix)..., empty_matrix.colptr, empty_matrix.rowval, empty_matrix.nzval)
    @debug "empty_sparse_matrix! $(daf) / $(rows_axis) / $(columns_axis) / $(name) <$(overwrite ? "=" : "-") $(verified)"
    return result
end

"""
    relayout_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [overwrite::Bool = false]
    )::Nothing

Given a matrix property with some `name` exists (in column-major layout) in `daf` for the `rows_axis` and the
`columns_axis`, then [`relayout!`](@ref) it and store the row-major result as well (that is, with flipped axes).

This is useful following calling [`empty_dense_matrix!`](@ref) or [`empty_sparse_matrix!`](@ref) to ensure both layouts
of the matrix are stored in `def`. When calling [`set_matrix!`](@ref), it is simpler to just specify (the default)
`relayout = true`.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, and that there is a `name` (column-major) matrix
property for them. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
*flipped* `rows_axis` and `columns_axis`.
"""
function relayout_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    overwrite::Bool = false,
)::Nothing
    @debug "relayout_matrix! $(daf) / $(rows_axis) / $(columns_axis) / $(name) <$(overwrite ? "=" : "-")>"

    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    require_matrix(daf, rows_axis, columns_axis, name; relayout = false)

    if !overwrite
        require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
    elseif Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
        Formats.format_delete_matrix!(daf, columns_axis, rows_axis, name; for_set = true)
    end

    invalidate_cached_dependencies!(daf, matrix_dependency_key(rows_axis, columns_axis, name))

    Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name)
    return nothing
end

"""
    delete_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [must_exist::Bool = true,
        relayout::Bool = true]
    )::Nothing

Delete a matrix property with some `name` for some `rows_axis` and `columns_axis` from `daf`.

If `relayout` (the default), this will also delete the matrix in the other layout (that is, with flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If `must_exist` (the default), this also verifies
the `name` matrix exists for the `rows_axis` and `columns_axis`.
"""
function delete_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
    relayout::Bool = true,
)::Nothing
    @debug "delete_matrix! $(daf.name) / $(rows_axis) / $(columns_axis) / $(name) $(must_exist ? "" : " ?")"

    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    if must_exist
        require_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
    end

    invalidate_cached_dependencies!(daf, matrix_dependency_key(rows_axis, columns_axis, name))

    if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = false)
    end

    if relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
        Formats.format_delete_matrix!(daf, columns_axis, rows_axis, name; for_set = false)
    end

    return nothing
end

function matrix_dependency_key(rows_axis::AbstractString, columns_axis::AbstractString, name::AbstractString)::String
    if rows_axis < columns_axis
        return "$(escape_query(rows_axis)), $(escape_query(columns_axis)) @ $(escape_query(name))"
    else
        return "$(escape_query(columns_axis)), $(escape_query(rows_axis)) @ $(escape_query(name))"
    end
end

"""
    matrix_names(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString;
        [relayout::Bool = true]
    )::Set{String}

The names of the matrix properties for the `rows_axis` and `columns_axis` in `daf`.

If `relayout` (default), then this will include the names of matrices that exist in the other layout (that is, with
flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`.
"""
function matrix_names(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString;
    relayout::Bool = true,
)::AbstractSet{String}
    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)
    names = Formats.format_matrix_names(daf, rows_axis, columns_axis)
    if relayout
        names = union(names, Formats.format_matrix_names(daf, columns_axis, rows_axis))
    end
    # @debug "matrix_names $(daf.name) / $(rows_axis) / $(columns_axis) $(relayout ? "%" : "#")> $(present(names))"
    return names
end

function require_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    if !has_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
        if relayout
            extra = "(and the other way around)\n"
        else
            extra = ""
        end
        error(
            "missing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            extra *
            "in the daf data: $(daf.name)",
        )
    end
    return nothing
end

function require_no_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        error(
            "existing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    if relayout
        require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
    end
    return nothing
end

"""
    get_matrix(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [default::Union{StorageScalar, StorageMatrix, Nothing, Missing} = nothing,
        relayout::Bool = true]
    )::Union{NamedMatrix, Missing}

Get the column-major matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. The names of the
result axes are the names of the relevant axes entries (same as returned by [`get_axis`](@ref)).

If `relayout` (the default), then if the matrix is only stored in the other memory layout (that is, with flipped axes),
then automatically call [`relayout!`](@ref) to compute the result. If `daf isa DafWriter`, then store the result for
future use; otherwise, just cache it in-memory (similar to a query result). This may lock up very large amounts of
memory; you can [`empty_cache!`](@ref) to release it.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If `default` is `nothing` (the default), this
first verifies the `name` matrix exists in `daf`. Otherwise, if `default` is `missing`, it is returned. if `default` is
a `StorageMatrix`, it has to be of the same size as the `rows_axis` and `columns_axis`, and is returned. Otherwise, a
new `Matrix` is created of the correct size containing the `default`, and is returned.
"""
function get_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageMatrix, Nothing, Missing} = nothing,
    relayout::Bool = true,
)::Union{NamedArray, Missing}
    require_axis(daf, rows_axis)
    require_axis(daf, columns_axis)

    if default !== missing && default isa StorageMatrix
        require_column_major(default)
        require_axis_length(daf, "default rows", size(default, Rows), rows_axis)
        require_axis_length(daf, "default columns", size(default, Columns), columns_axis)
        if default isa NamedMatrix
            require_dim_name(daf, rows_axis, "default rows dim name", dimnames(default, 1); prefix = "rows")
            require_dim_name(daf, columns_axis, "default columns dim name", dimnames(default, 2); prefix = "columns")
            require_axis_names(daf, rows_axis, "row names of the: default", names(default, 1))
            require_axis_names(daf, columns_axis, "column names of the: default", names(default, 2))
        end
    end

    default_suffix = ""
    matrix = nothing
    if !Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        if relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            if daf isa DafWriter
                Formats.format_relayout_matrix!(daf, columns_axis, rows_axis, name)
            else
                key = "$(escape_query(rows_axis)), $(escape_query(columns_axis)) @ $(escape_query(name))"
                flipped_key = "$(escape_query(columns_axis)), $(escape_query(rows_axis)) @ $(escape_query(name))"

                store_cached_dependency_key!(daf, axis_dependency_key(rows_axis), key)
                store_cached_dependency_key!(daf, axis_dependency_key(columns_axis), key)
                store_cached_dependency_key!(daf, matrix_dependency_key(rows_axis, columns_axis, name), key)

                matrix = get!(daf.internal.cache, key) do
                    return transpose(relayout!(Formats.format_get_matrix(daf, columns_axis, rows_axis, name)))
                end
            end
        else
            if default === missing
                @debug "get_matrix $(daf.name) / $(rows_axis) / $(columns_axis) / $(name) -> $(present(missing))"
                return missing
            end
            default_suffix = " ?"
            if default isa StorageMatrix
                matrix = default
            elseif default isa StorageScalar
                matrix = fill(
                    default,
                    Formats.format_axis_length(daf, rows_axis),
                    Formats.format_axis_length(daf, columns_axis),
                )
            end
        end
    end

    if matrix == nothing
        require_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
        matrix = Formats.format_get_matrix(daf, rows_axis, columns_axis, name)
        if !(matrix isa StorageMatrix)
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned invalid Daf.StorageMatrix: $(typeof(matrix))",
            )
        end

        if size(matrix, Rows) != Formats.format_axis_length(daf, rows_axis)
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned matrix rows: $(size(matrix, Rows))\n" *
                "instead of axis: $(rows_axis)\n" *
                "length: $(axis_length(daf, rows_axis))\n" *
                "in the daf data: $(daf.name)",
            )
        end

        if size(matrix, Columns) != Formats.format_axis_length(daf, columns_axis)
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned matrix columns: $(size(matrix, Columns))\n" *
                "instead of axis: $(columns_axis)\n" *
                "length: $(axis_length(daf, columns_axis))\n" *
                "in the daf data: $(daf.name)",
            )
        end

        if major_axis(matrix) != Columns
            error( # untested
                "format_get_matrix for daf format: $(typeof(daf))\n" *
                "returned non column-major matrix: $(typeof(matrix))",
            )
        end
    end

    result = as_named_matrix(daf, rows_axis, columns_axis, matrix)
    @debug "get_matrix $(daf.name) / $(rows_axis) / $(columns_axis) / $(name) -> $(present(result))$(default_suffix)"
    return result
end

function require_column_major(matrix::StorageMatrix)::Nothing
    if major_axis(matrix) != Columns
        error("type: $(typeof(matrix)) is not in column-major layout")
    end
end

function require_axis_length(
    daf::DafReader,
    what_name::AbstractString,
    what_length::Integer,
    axis::AbstractString,
)::Nothing
    if what_length != Formats.format_axis_length(daf, axis)
        error(
            "$(what_name): $(what_length)\n" *
            "is different from the length: $(Formats.format_axis_length(daf, axis))\n" *
            "of the axis: $(axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    return nothing
end

function require_not_name(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if name == "name"
        error("setting the reserved vector: name\n" * "for the axis: $(axis)\n" * "in the daf data: $(daf.name)")
    end
    return nothing
end

function as_read_only(array::SparseArrays.ReadOnly)::SparseArrays.ReadOnly
    return array
end

function as_read_only(array::NamedArray)::NamedArray
    if array.array isa SparseArrays.ReadOnly
        return array  # untested
    else
        return NamedArray(as_read_only(array.array), array.dicts, array.dimnames)
    end
end

function as_read_only(array::AbstractArray)::SparseArrays.ReadOnly
    return SparseArrays.ReadOnly(array)
end

function require_dim_name(
    daf::DafReader,
    axis::AbstractString,
    what::String,
    name::Union{Symbol, String};
    prefix::String = "",
)::Nothing
    if prefix != ""
        prefix = prefix * " "
    end
    string_name = String(name)
    if string_name != axis
        error("$(what): $(string_name)\nis different from the $(prefix)axis: $(axis)\nin the daf data: $(daf.name)")
    end
end

function require_axis_names(daf::DafReader, axis::AbstractString, what::String, names::Vector{String})::Nothing
    expected_names = get_axis(daf, axis)
    if names != expected_names
        error("$(what)\nmismatch the entry names of the axis: $(axis)\nin the daf data: $(daf.name)")
    end
end

function as_named_vector(daf::DafReader, axis::AbstractString, vector::NamedVector)::NamedArray
    return vector
end

function as_named_vector(daf::DafReader, axis::AbstractString, vector::AbstractVector)::NamedArray
    axis_names_dict = get(daf.internal.axes, axis, nothing)
    if axis_names_dict == nothing
        named_array = NamedArray(vector; names = (get_axis(daf, axis),), dimnames = (axis,))
        daf.internal.axes[axis] = named_array.dicts[1]
        return named_array

    else
        return NamedArray(vector, (axis_names_dict,), (axis,))
    end
end

function as_named_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::NamedMatrix,
)::NamedArray
    return matrix
end

function as_named_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix::AbstractMatrix,
)::NamedArray
    rows_axis_names_dict = get(daf.internal.axes, rows_axis, nothing)
    columns_axis_names_dict = get(daf.internal.axes, columns_axis, nothing)
    if rows_axis_names_dict == nothing || columns_axis_names_dict == nothing
        named_array = NamedArray(
            matrix;
            names = (get_axis(daf, rows_axis), get_axis(daf, columns_axis)),
            dimnames = (rows_axis, columns_axis),
        )
        daf.internal.axes[rows_axis] = named_array.dicts[1]
        daf.internal.axes[columns_axis] = named_array.dicts[2]
        return named_array

    else
        return NamedArray(matrix, (rows_axis_names_dict, columns_axis_names_dict), (rows_axis, columns_axis))
    end
end

function base_array(array::AbstractArray)::AbstractArray
    return array
end

function base_array(array::SparseArrays.ReadOnly)::AbstractArray
    return base_array(parent(array))
end

function base_array(array::NamedArray)::AbstractArray
    return base_array(array.array)
end

"""
    description(daf::DafReader[; deep::Bool = false])::AbstractString

Return a (multi-line) description of the contents of `daf`. This tries to hit a sweet spot between usefulness and
terseness. If `deep`, also describes any data set nested inside this one (if any).
"""
function description(daf::DafReader; deep::Bool = false)::String
    lines = String[]
    description(daf, "", lines, deep)
    push!(lines, "")
    return join(lines, "\n")
end

function description(daf::DafReader, indent::String, lines::Vector{String}, deep::Bool)::Nothing
    if indent == ""
        push!(lines, "$(indent)name: $(daf.name)")
    else
        push!(lines, "- $(indent[3:end])name: $(daf.name)")
    end

    Formats.format_description_header(daf, indent, lines)

    scalars_description(daf, indent, lines)

    axes = collect(Formats.format_axis_names(daf))
    sort!(axes)
    if !isempty(axes)
        axes_description(daf, axes, indent, lines)
        vectors_description(daf, axes, indent, lines)
        matrices_description(daf, axes, indent, lines)
        cache_description(daf, axes, indent, lines)
    end

    Formats.format_description_footer(daf, indent, lines, deep)
    return nothing
end

function scalars_description(daf::DafReader, indent::String, lines::Vector{String})::Nothing
    scalars = collect(Formats.format_scalar_names(daf))
    if !isempty(scalars)
        sort!(scalars)
        push!(lines, "$(indent)scalars:")
        for scalar in scalars
            push!(lines, "$(indent)  $(scalar): $(present(Formats.format_get_scalar(daf, scalar)))")
        end
    end
    return nothing
end

function axes_description(daf::DafReader, axes::Vector{String}, indent::String, lines::Vector{String})::Nothing
    push!(lines, "$(indent)axes:")
    for axis in axes
        push!(lines, "$(indent)  $(axis): $(Formats.format_axis_length(daf, axis)) entries")
    end
    return nothing
end

function vectors_description(daf::DafReader, axes::Vector{String}, indent::String, lines::Vector{String})::Nothing
    is_first = true
    for axis in axes
        vectors = collect(Formats.format_vector_names(daf, axis))
        if !isempty(vectors)
            if is_first
                push!(lines, "$(indent)vectors:")
                is_first = false
            end
            sort!(vectors)
            push!(lines, "$(indent)  $(axis):")
            for vector in vectors
                push!(
                    lines,
                    "$(indent)    $(vector): $(present(base_array(Formats.format_get_vector(daf, axis, vector))))",
                )
            end
        end
    end
    return nothing
end

function matrices_description(daf::DafReader, axes::Vector{String}, indent::String, lines::Vector{String})::Nothing
    is_first = true
    for rows_axis in axes
        for columns_axis in axes
            matrices = collect(Formats.format_matrix_names(daf, rows_axis, columns_axis))
            if !isempty(matrices)
                if is_first
                    push!(lines, "$(indent)matrices:")
                    is_first = false
                end
                sort!(matrices)
                push!(lines, "$(indent)  $(rows_axis),$(columns_axis):")
                for matrix in matrices
                    push!(
                        lines,
                        "$(indent)    $(matrix): " *
                        present(base_array(Formats.format_get_matrix(daf, rows_axis, columns_axis, matrix))),
                    )
                end
            end
        end
    end
    return nothing
end

function cache_description(daf::DafReader, axes::Vector{String}, indent::String, lines::Vector{String})::Nothing
    is_first = true
    cache_keys = collect(keys(daf.internal.cache))
    sort!(cache_keys)
    for key in cache_keys
        if is_first
            push!(lines, "$(indent)cache:")
            is_first = false
        end
        value = daf.internal.cache[key]
        if value isa AbstractArray
            value = base_array(value)
        end
        push!(lines, "$(indent)  $(key): $(present(value))")
    end
    return nothing
end

"""
    matrix_query(daf::DafReader, query::AbstractString)::Union{NamedMatrix, Nothing}

Query `daf` for some matrix results. See [`MatrixQuery`](@ref) for the possible queries that return matrix results. The
names of the axes of the result are the names of the axis entries. This is especially useful when the query applies
masks to the axes. Will return `nothing` if any of the masks is empty.

The query result is cached in memory to speed up repeated queries. For computed queries (e.g., results of element-wise
operations) this may lock up very large amounts of memory; you can [`empty_cache!`](@ref) to release it.
"""
function matrix_query(daf::DafReader, query::AbstractString)::Union{NamedArray, Nothing}
    return matrix_query(daf, parse_matrix_query(query))
end

function matrix_query(
    daf::DafReader,
    matrix_query::MatrixQuery,
    outer_dependency_keys::Union{Set{String}, Nothing} = nothing,
)::Union{NamedArray, Nothing}
    cache_key = canonical(matrix_query)
    return get!(daf.internal.cache, cache_key) do
        matrix_dependency_keys = Set{String}()
        result = compute_matrix_lookup(daf, matrix_query.matrix_property_lookup, matrix_dependency_keys)
        result = compute_eltwise_result(matrix_query.eltwise_operations, result)

        for dependency_key in matrix_dependency_keys
            store_cached_dependency_key!(daf, dependency_key, cache_key)
        end

        if outer_dependency_keys != nothing
            union!(outer_dependency_keys, matrix_dependency_keys)
        end

        return result
    end
end

function compute_matrix_lookup(
    daf::DafReader,
    matrix_property_lookup::MatrixPropertyLookup,
    dependency_keys::Set{String},
)::Union{NamedArray, Nothing}
    rows_axis = matrix_property_lookup.matrix_axes.rows_axis.axis_name
    columns_axis = matrix_property_lookup.matrix_axes.columns_axis.axis_name
    name = matrix_property_lookup.property_name

    push!(dependency_keys, axis_dependency_key(rows_axis))
    push!(dependency_keys, axis_dependency_key(columns_axis))
    push!(dependency_keys, matrix_dependency_key(rows_axis, columns_axis, name))
    result = get_matrix(daf, rows_axis, columns_axis, name)

    rows_mask = compute_filtered_axis_mask(daf, matrix_property_lookup.matrix_axes.rows_axis, dependency_keys)
    columns_mask = compute_filtered_axis_mask(daf, matrix_property_lookup.matrix_axes.columns_axis, dependency_keys)
    if (rows_mask != nothing && !any(rows_mask)) || (columns_mask != nothing && !any(columns_mask))
        return nothing
    end

    if rows_mask != nothing && columns_mask != nothing
        return result[rows_mask, columns_mask]  # NOJET
    elseif rows_mask != nothing
        return result[rows_mask, :]
    elseif columns_mask != nothing
        return result[:, columns_mask]
    else
        return result
    end
end

function compute_filtered_axis_mask(
    daf::DafReader,
    filtered_axis::FilteredAxis,
    dependency_keys::Set{String},
)::Union{Vector{Bool}, Nothing}
    if isempty(filtered_axis.axis_filters)
        return nothing
    end

    mask = fill(true, axis_length(daf, filtered_axis.axis_name))
    for axis_filter in filtered_axis.axis_filters
        mask = compute_axis_filter(daf, mask, filtered_axis.axis_name, axis_filter, dependency_keys)
    end

    return mask
end

function compute_axis_filter(
    daf::DafReader,
    mask::AbstractVector{Bool},
    axis::AbstractString,
    axis_filter::AxisFilter,
    dependency_keys::Set{String},
)::AbstractVector{Bool}
    filter = compute_axis_lookup(daf, axis, axis_filter.axis_lookup, dependency_keys, nothing)
    if eltype(filter) != Bool
        filter = NamedArray(filter .!= zero_of(filter), filter.dicts, filter.dimnames)
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

function compute_axis_lookup(
    daf::DafReader,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    dependency_keys::Set{String},
    mask::Union{Vector{Bool}, Nothing},
)::NamedArray
    allow_missing_entries =
        axis_lookup.property_comparison != nothing && axis_lookup.property_comparison.comparison_operator == CmpDefault
    values, missing_mask =
        compute_property_lookup(daf, axis, axis_lookup.property_lookup, dependency_keys, mask, allow_missing_entries)

    if allow_missing_entries
        @assert missing_mask != nothing
        value = axis_lookup_comparison_value(daf, axis, axis_lookup, values)
        values[missing_mask] .= value  # NOJET
        return values
    end

    @assert missing_mask == nothing
    if axis_lookup.property_comparison == nothing
        return values
    end

    result =
        if axis_lookup.property_comparison.comparison_operator == CmpMatch ||
           axis_lookup.property_comparison.comparison_operator == CmpNotMatch
            compute_axis_lookup_match_mask(daf, axis, axis_lookup, values)
        else
            compute_axis_lookup_compare_mask(daf, axis, axis_lookup, values)
        end

    return NamedArray(result, values.dicts, values.dimnames)
end

function compute_axis_lookup_match_mask(
    daf::DafReader,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    values::AbstractVector,
)::Vector{Bool}
    if eltype(values) != String
        error(
            "non-String data type: $(eltype(values))\n" *
            "for the match axis lookup: $(canonical(axis_lookup))\n" *
            "for the axis: $(axis)\n" *
            "in the daf data: $(daf.name)",
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
            "in the daf data: $(daf.name)",
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

function compute_axis_lookup_compare_mask(
    daf::DafReader,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    values::AbstractVector,
)::Vector{Bool}
    value = axis_lookup_comparison_value(daf, axis, axis_lookup, values)
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

function axis_lookup_comparison_value(
    daf::DafReader,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    values::AbstractVector,
)::StorageScalar
    value = axis_lookup.property_comparison.property_value
    if eltype(values) != String
        try
            value = parse(eltype(values), value)
        catch
            error(
                "invalid $(eltype) value: \"$(escape_string(axis_lookup.property_comparison.property_value))\"\n" *
                "for the axis lookup: $(canonical(axis_lookup))\n" *
                "for the axis: $(axis)\n" *
                "in the daf data: $(daf.name)",
            )
        end
    end
    return value
end

function compute_property_lookup(
    daf::DafReader,
    axis::AbstractString,
    property_lookup::PropertyLookup,
    dependency_keys::Set{String},
    mask::Union{Vector{Bool}, Nothing},
    allow_missing_entries::Bool,
)::Tuple{NamedArray, Union{Vector{Bool}, Nothing}}
    last_property_name = property_lookup.property_names[1]

    push!(dependency_keys, axis_dependency_key(axis))
    push!(dependency_keys, vector_dependency_key(axis, last_property_name))
    values = get_vector(daf, axis, last_property_name)
    if mask != nothing
        values = values[mask]
    end

    if allow_missing_entries
        missing_mask = zeros(Bool, length(values))
    else
        missing_mask = nothing
    end

    for next_property_name in property_lookup.property_names[2:end]
        if eltype(values) != String
            error(
                "non-String data type: $(eltype(values))\n" *
                "for the chained: $(last_property_name)\n" *
                "for the axis: $(axis)\n" *
                "in the daf data: $(daf.name)",
            )
        end
        values, axis = compute_chained_property(
            daf,
            axis,
            last_property_name,
            values,
            next_property_name,
            dependency_keys,
            missing_mask,
        )
        last_property_name = next_property_name
    end

    @assert allow_missing_entries == (missing_mask != nothing)

    return values, missing_mask
end

function compute_chained_property(
    daf::DafReader,
    last_axis::AbstractString,
    last_property_name::AbstractString,
    last_property_values::NamedVector{String},
    next_property_name::AbstractString,
    dependency_keys::Set{String},
    missing_mask::Union{Vector{Bool}, Nothing},
)::Tuple{NamedArray, String}
    if has_axis(daf, last_property_name)
        next_axis = last_property_name
    else
        next_axis = split(last_property_name, "."; limit = 2)[1]
    end

    push!(dependency_keys, axis_dependency_key(next_axis))
    next_axis_entries = get_axis(daf, next_axis)

    push!(dependency_keys, vector_dependency_key(next_axis, next_property_name))
    next_axis_values = get_vector(daf, next_axis, next_property_name)

    next_property_values = [
        find_axis_value(
            daf,
            last_axis,
            last_property_name,
            property_value,
            next_axis,
            next_axis_entries,
            next_axis_values,
            property_index,
            missing_mask,
        ) for (property_index, property_value) in enumerate(last_property_values)
    ]

    return (NamedArray(next_property_values, last_property_values.dicts, last_property_values.dimnames), next_axis)
end

function find_axis_value(
    daf::DafReader,
    last_axis::AbstractString,
    last_property_name::AbstractString,
    last_property_value::AbstractString,
    next_axis::AbstractString,
    next_axis_entries::AbstractVector{String},
    next_axis_values::AbstractVector,
    property_index::Int,
    missing_mask::Union{Vector{Bool}, Nothing},
)::Any
    if missing_mask != nothing && missing_mask[property_index]
        return zero_of(next_axis_values)  # untested
    end
    index = findfirst(==(last_property_value), next_axis_entries)
    if index != nothing
        return next_axis_values[index]
    elseif missing_mask != nothing
        missing_mask[property_index] = true
        return zero_of(next_axis_values)
    else
        error(
            "invalid value: $(last_property_value)\n" *
            "of the chained: $(last_property_name)\n" *
            "of the axis: $(last_axis)\n" *
            "is missing from the next axis: $(next_axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end
end

function zero_of(values::AbstractVector{T})::T where {T <: StorageScalar}
    if T == String
        return ""
    else
        return zero(T)
    end
end

"""
    vector_query(daf::DafReader, query::AbstractString)::Union{NamedVector, Nothing}

Query `daf` for some vector results. See [`VectorQuery`](@ref) for the possible queries that return vector results. The
names of the results are the names of the axis entries. This is especially useful when the query applies a mask to the
axis. Will return `nothing` if any of the masks is empty.

The query result is cached in memory to speed up repeated queries. For computed queries (e.g., results of element-wise
operations) this may lock up very large amounts of memory; you can [`empty_cache!`](@ref) to release it.
"""
function vector_query(daf::DafReader, query::AbstractString)::Union{NamedArray, Nothing}
    return vector_query(daf, parse_vector_query(query))
end

function vector_query(
    daf::DafReader,
    vector_query::VectorQuery,
    outer_dependency_keys::Union{Set{String}, Nothing} = nothing,
)::Union{NamedArray, Nothing}
    cache_key = canonical(vector_query)
    return get!(daf.internal.cache, cache_key) do
        vector_dependency_keys = Set{String}()
        result = compute_vector_data_lookup(daf, vector_query.vector_data_lookup, vector_dependency_keys)
        result = compute_eltwise_result(vector_query.eltwise_operations, result)

        for dependency_key in vector_dependency_keys
            store_cached_dependency_key!(daf, dependency_key, cache_key)
        end

        if outer_dependency_keys != nothing
            union!(outer_dependency_keys, vector_dependency_keys)
        end

        return result
    end
end

function compute_vector_data_lookup(
    daf::DafReader,
    vector_property_lookup::VectorPropertyLookup,
    dependency_keys::Set{String},
)::Union{NamedArray, Nothing}
    mask = compute_filtered_axis_mask(daf, vector_property_lookup.filtered_axis, dependency_keys)
    if mask != nothing && !any(mask)
        return nothing
    end

    return compute_axis_lookup(
        daf,
        vector_property_lookup.filtered_axis.axis_name,
        vector_property_lookup.axis_lookup,
        dependency_keys,
        mask,
    )
end

function compute_vector_data_lookup(
    daf::DafReader,
    matrix_slice_lookup::MatrixSliceLookup,
    dependency_keys::Set{String},
)::Union{NamedArray, Nothing}
    rows_axis = matrix_slice_lookup.matrix_slice_axes.filtered_axis.axis_name
    columns_axis = matrix_slice_lookup.matrix_slice_axes.axis_entry.axis_name
    name = matrix_slice_lookup.property_name

    push!(dependency_keys, axis_dependency_key(rows_axis))
    push!(dependency_keys, axis_dependency_key(columns_axis))
    push!(dependency_keys, matrix_dependency_key(rows_axis, columns_axis, name))
    result = get_matrix(daf, rows_axis, columns_axis, name)

    index = find_axis_entry_index(daf, matrix_slice_lookup.matrix_slice_axes.axis_entry)
    result = result[:, index]  # NOJET

    rows_mask = compute_filtered_axis_mask(daf, matrix_slice_lookup.matrix_slice_axes.filtered_axis, dependency_keys)
    if rows_mask == nothing
        return result
    elseif !any(rows_mask)
        return nothing  # untested
    else
        return result[rows_mask]
    end
end

function compute_vector_data_lookup(
    daf::DafReader,
    reduce_matrix_query::ReduceMatrixQuery,
    dependency_keys::Set{String},
)::Union{NamedArray, Nothing}
    result = matrix_query(daf, reduce_matrix_query.matrix_query, dependency_keys)
    if result == nothing
        return nothing
    end
    return compute_reduction_result(reduce_matrix_query.reduction_operation, result)
end

"""
    scalar_query(daf::DafReader, query::AbstractString)::Union{StorageScalar, Nothing}

Query `daf` for some scalar results. See [`ScalarQuery`](@ref) for the possible queries that return scalar results.

The query result is cached in memory to speed up repeated queries. For computed queries (e.g., results of element-wise
operations) this may lock up very large amounts of memory; you can [`empty_cache!`](@ref) to release it.
"""
function scalar_query(daf::DafReader, query::AbstractString)::Union{StorageScalar, Nothing}
    return scalar_query(daf, parse_scalar_query(query))
end

function scalar_query(
    daf::DafReader,
    scalar_query::ScalarQuery,
    outer_dependency_keys::Union{Set{String}, Nothing} = nothing,
)::Union{StorageScalar, Nothing}
    cache_key = canonical(scalar_query)
    return get!(daf.internal.cache, cache_key) do
        scalar_dependency_keys = Set{String}()
        result = compute_scalar_data_lookup(daf, scalar_query.scalar_data_lookup, scalar_dependency_keys)
        result = compute_eltwise_result(scalar_query.eltwise_operations, result)

        for dependency_key in scalar_dependency_keys
            store_cached_dependency_key!(daf, dependency_key, cache_key)
        end

        if outer_dependency_keys != nothing
            union!(outer_dependency_keys, scalar_dependency_keys)  # untested
        end

        return result
    end
end

function compute_scalar_data_lookup(
    daf::DafReader,
    scalar_property_lookup::ScalarPropertyLookup,
    dependency_keys::Set{String},
)::Union{StorageScalar, Nothing}
    name = scalar_property_lookup.property_name
    push!(dependency_keys, scalar_dependency_key(name))
    return get_scalar(daf, name)
end

function compute_scalar_data_lookup(
    daf::DafReader,
    reduce_vector_query::ReduceVectorQuery,
    dependency_keys::Set{String},
)::Union{StorageScalar, Nothing}
    result = vector_query(daf, reduce_vector_query.vector_query, dependency_keys)
    return compute_reduction_result(reduce_vector_query.reduction_operation, result)
end

function compute_scalar_data_lookup(
    daf::DafReader,
    vector_entry_lookup::VectorEntryLookup,
    dependency_keys::Set{String},
)::Union{StorageScalar, Nothing}
    index = find_axis_entry_index(daf, vector_entry_lookup.axis_entry)
    mask = zeros(Bool, axis_length(daf, vector_entry_lookup.axis_entry.axis_name))
    mask[index] = true

    result = compute_axis_lookup(
        daf,
        vector_entry_lookup.axis_entry.axis_name,
        vector_entry_lookup.axis_lookup,
        dependency_keys,
        mask,
    )

    @assert length(result) == 1
    return result[1]
end

function compute_scalar_data_lookup(
    daf::DafReader,
    matrix_entry_lookup::MatrixEntryLookup,
    dependency_keys::Set{String},
)::Union{StorageScalar, Nothing}
    rows_axis = matrix_entry_lookup.matrix_entry_axes.rows_entry.axis_name
    columns_axis = matrix_entry_lookup.matrix_entry_axes.columns_entry.axis_name
    name = matrix_entry_lookup.property_name

    push!(dependency_keys, axis_dependency_key(rows_axis))
    push!(dependency_keys, axis_dependency_key(columns_axis))
    push!(dependency_keys, matrix_dependency_key(rows_axis, columns_axis, name))
    result = get_matrix(daf, rows_axis, columns_axis, name)

    row_index = find_axis_entry_index(daf, matrix_entry_lookup.matrix_entry_axes.rows_entry)
    column_index = find_axis_entry_index(daf, matrix_entry_lookup.matrix_entry_axes.columns_entry)
    return result[row_index, column_index]
end

function find_axis_entry_index(daf::DafReader, axis_entry::AxisEntry)::Int
    axis_entries = get_axis(daf, axis_entry.axis_name)
    index = findfirst(==(axis_entry.entry_name), axis_entries)  # NOJET
    if index == nothing
        error(
            "the entry: $(axis_entry.entry_name)\n" *
            "is missing from the axis: $(axis_entry.axis_name)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    return index
end

function compute_eltwise_result(
    eltwise_operations::Vector{EltwiseOperation},
    input::Union{NamedArray, StorageScalar, Nothing},
)::Union{NamedArray, StorageScalar, Nothing}
    if input == nothing
        return nothing
    end

    result = input
    for eltwise_operation in eltwise_operations
        named_result = result
        if result isa StorageScalar
            check_type = typeof(result)
            error_type = typeof(result)
        else
            check_type = eltype(result)
            error_type = typeof(base_array(result))  # NOJET
        end

        if !(check_type <: Number)
            error("non-numeric input: $(error_type)\n" * "for the eltwise operation: $(canonical(eltwise_operation))\n")
        end

        if result isa StorageScalar
            result = compute_eltwise(eltwise_operation, result)  # NOJET
        else
            result = NamedArray(compute_eltwise(eltwise_operation, result.array), result.dicts, result.dimnames)  # NOJET
        end
    end
    return result
end

function compute_reduction_result(
    reduction_operation::ReductionOperation,
    input::Union{NamedArray, Nothing},
)::Union{NamedArray, StorageScalar, Nothing}
    if input == nothing
        return nothing
    end

    if !(eltype(input) <: Number)
        error(
            "non-numeric input: $(typeof(base_array(input)))\n" *
            "for the reduction operation: $(canonical(reduction_operation))\n",
        )
    end
    if ndims(input) == 2
        return NamedArray(compute_reduction(reduction_operation, input.array), (input.dicts[2],), (input.dimnames[2],))
    else
        return compute_reduction(reduction_operation, input.array)
    end
end

function store_cached_dependency_key!(daf::DafReader, dependency_key::String, cache_key::String)::Nothing
    keys_set = get!(daf.internal.dependency_cache_keys, dependency_key) do
        return Set{String}()
    end
    push!(keys_set, cache_key)
    return nothing
end

function invalidate_cached_dependencies!(daf::DafReader, dependency_key::String)::Nothing
    cache_keys = pop!(daf.internal.dependency_cache_keys, dependency_key, nothing)
    if cache_keys != nothing
        for cache_key in cache_keys
            delete!(daf.internal.cache, cache_key)
        end
    end
end

"""
    empty_cache!(daf::DafReader)::Nothing

Empty the cached computed results. This includes computed query results, as well as any relayout matrices that couldn't
be stored in the `daf` storage itself.

This might be needed if caching consumes too much memory. To see what (if anything) is cached, look at the results of
[`description`](@ref).
"""
function empty_cache!(daf::DafReader)::Nothing
    empty!(daf.internal.cache)
    empty!(daf.internal.dependency_cache_keys)
    return nothing
end

function Messages.present(value::DafReader)::String
    return "$(typeof(value)) $(value.name)"
end

end # module
