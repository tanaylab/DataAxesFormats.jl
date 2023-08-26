"""
Create a different view of `Daf` data using queries. This is a very flexible mechanism which can be used for a variety
of use cases. A simple way of using this is to view a subset of the data as a `Daf` data set. A variant of this also
renames the data properties to adapt them to the requirements of some computational tool. This makes it simpler to
create such tools (using fixed, generic property names) and apply them to arbitrary data (with arbitrary specific
property names).
"""
module Views

export ALL_AXES
export ALL_MATRICES
export ALL_SCALARS
export ALL_VECTORS
export viewer

using Daf.Data
using Daf.Formats
using Daf.StorageTypes

import Daf.Data.as_read_only
import Daf.Data.base_array
import Daf.Data.parse_matrix_query
import Daf.Data.parse_scalar_query
import Daf.Data.parse_vector_query
import Daf.Data.MatrixQuery
import Daf.Data.ScalarQuery
import Daf.Data.VectorQuery
import Daf.Formats
import Daf.Formats.Internal
import Daf.Oprec.decode_expression
import Daf.Oprec.encode_expression
import Daf.Oprec.escape_query
import Daf.Queries.canonical
import Daf.Queries.vector_query_axis
import Daf.ReadOnly.ReadOnlyView

"""
    struct DafView(daf::F) <: DafReader where {F <: DafReader}

A read-only wrapper for any [`DafReader`](@ref) data, which exposes an arbitrary view of it as another
[`DafReader`](@ref). This isn't typically created manually; instead call [`viewer`](@ref).
"""
struct DafView <: DafReader
    internal::Internal
    daf::DafReader
    scalars::Dict{String, Union{ScalarQuery, StorageScalar}}
    axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}}
    vectors::Dict{String, Dict{String, Union{VectorQuery, StorageVector}}}
    matrices::Dict{String, Dict{String, Dict{String, Union{MatrixQuery, StorageMatrix}}}}
end

"""
A pair to use in the `scalars` parameter of [`viewer`](@ref) to specify the view exposes all the base data scalars.
"""
ALL_SCALARS = "*" => "="

"""
A pair to use in the `axes` parameter of [`viewer`](@ref) to specify the view exposes all the base data axes.
"""
ALL_AXES = "*" => "="

"""
A pair to use in the `vectors` parameter of [`viewer`](@ref) to specify the view exposes all the vectors of the exposed axes.
"""
ALL_VECTORS = ("*", "*") => "="

"""
A pair to use in the `matrices` parameter of [`viewer`](@ref) to specify the view exposes all the matrices of the exposed axes.
"""
ALL_MATRICES = ("*", "*", "*") => "="

"""
    viewer(
        name::AbstractString,
        daf::DafReader;
        [scalars::AbstractVector{Pair{String, Union{String, Nothing}}} = [],
        axes::AbstractVector{Pair{String, Union{String, Nothing}}} = [],
        vectors::AbstractVector{Pair{Tuple{String, String}, Union{String, Nothing}}} = []]
        matrices::AbstractVector{Pair{Tuple{String, String, String}, Union{String, Nothing}}} = []]
    )::DafView

Wrap `daf` data with a read-only [`DafView`](@ref). The exposed view is defined by a set of queries applied to the
original data. These queries are evaluated only when data is actually accessed. Therefore, creating a view is a
relatively cheap operation.

Queries are listed separately for scalars, axes, vector and matrix properties, as follows:

*Scalars* are specified as a list of pairs (similar to initializing a `Dict`). The order of the pairs matter (last one
wins). If the key is `"*"`, then it is replaced by all the names of the scalar properties of the wrapped `daf` data. If
the value is `nothing`, then the scalar will *not* be exposed by the view. If the value is `"="`, then the scalar will
be exposed with the same value as in the original `daf` data. Otherwise the value is any valid [`scalar_query`](@ref).

That is, saying `"*" => "="` will expose all the original `daf` data scalars from the view. Following this by saying
`"version" => nothing` will hide the `version` from the view. Add `"total_umis" => "cell, gene @ UMIs %> Sum %> Sum"` to
expose a `total_umis` scalar containing the total sum of all UMIs of all genes in all cells, etc.

*Axes* are specified similarly, except that the value should be a [`vector_query`](@ref) instead of a
[`scalar_query`](@ref). This query should be for the names of the entries of the axis, so a `@ name` suffix is
automatically added to the end of the value.

That is, writing `"gene" => "gene & marker"` will restrict the exposed `gene` axis to only the `marker` genes. This
ability to restrict a view to a subset of the entries is extremely useful.

*Vectors* are specified similarly, but require a key specifying both an axis and a property name. The axis must be
exposed by the view (based on the `axes` parameter). If the axis is `"*"`, it is replaces by all the exposed axis names
specified by the `axes` parameter. Similarly, if the name is `"*"` (e.g., `("gene", "*")`), then  it is replaced by all
the vector properties of the axis. Therefore if the key is `("*", "*")`, all vector properties of all the exposed axes
will also be exposed; to expose all vector properties of all axes, write `axes=["*" => "="], vectors=[("*", "*") => "="]`.

The value for vectors must be a vector query based on the appropriate axis; a value of `"="` is again used to expose the
property as-is. Since the axis might be specified by a query itself, it is automatically added (including the `@`
operator) as a prefix for the value; for example, specifying that `axes = ["gene" => "gene & marker"]`, and then that
`vectors = [("gene", "forbidden") => "lateral"]`, then the view will expose a `forbidden` vector property for the `gene`
axis, by applying the query `gene & marker @ lateral` to the original `daf` data.

This gets trickier when using a query reducing a matrix to a vector. In these cases, the value should contain a `@` but
only specify the rows axis, and the columns axis will be added automatically. For example, given the same `axes` as
above, and `vectors = [("gene", "total_umis") => "cell @ UMIS %> Sum"]`, then the view will expose a `total_umis` vector
property for the `gene` axis, by applying the query `cell, gene & marker @ UMIs %> Sum` to the original `daf` data.

*Matrices* require a key specifying both axes and a property name. The axes must both be exposed by the view (based on
the `axes` parameter). Again if any or both of the axes are `"*"`, they are replaced by all the exposed axes (based on
the `axes` parameter), and likewise if the name is `"*"`, it replaced by all the matrix properties of the axes.

The value for matrices can again be `"="` to expose the property as is, or the suffix of a matrix query. The full query
will have the axes queries appended automatically, similarly to the above. The order of the axes does not matter, so
`matrices = [("gene", "cell", "UMIs") => "="]` has the same effect as `matrices = [("cell", "gene", "UMIs") => "="]`.
"""
function viewer(
    name::AbstractString,
    daf::DafReader;
    scalars::AbstractVector{Pair{String, S}} = Vector{Pair{String, Union{String, Nothing}}}(),
    axes::AbstractVector{Pair{String, A}} = Vector{Pair{String, Union{String, Nothing}}}(),
    vectors::AbstractVector{Pair{Tuple{String, String}, V}} = Vector{
        Pair{Tuple{String, String}, Union{String, Nothing}},
    }(),
    matrices::AbstractVector{Pair{Tuple{String, String, String}, M}} = Vector{
        Pair{Tuple{String, String, String}, Union{String, Nothing}},
    }(),
)::DafView where {
    S <: Union{String, Nothing},
    A <: Union{String, Nothing},
    V <: Union{String, Nothing},
    M <: Union{String, Nothing},
}
    if daf isa ReadOnlyView
        daf = daf.daf
    end
    collected_scalars = collect_scalars(daf, scalars)
    collected_axes = collect_axes(daf, axes)
    collected_vectors = collect_vectors(name, daf, collected_axes, vectors)
    collected_matrices = collect_matrices(name, daf, collected_axes, matrices)
    return DafView(Internal(name), daf, collected_scalars, collected_axes, collected_vectors, collected_matrices)
end

function collect_scalars(
    daf::DafReader,
    scalars::AbstractVector{Pair{String, S}},
)::Dict{String, Union{ScalarQuery, StorageScalar}} where {S <: Union{String, Nothing}}
    collected_scalars = Dict{String, Union{ScalarQuery, StorageScalar}}()
    for (scalar, query) in scalars
        collect_scalar(daf, collected_scalars, scalar, query)
    end
    return collected_scalars
end

function collect_scalar(
    daf::DafReader,
    collected_scalars::Dict{String, Union{ScalarQuery, StorageScalar}},
    scalar::String,
    query::Union{String, Nothing},
)::Nothing
    if scalar == "*"
        for scalar in scalar_names(daf)
            collect_scalar(daf, collected_scalars, scalar, query)
        end
    elseif query == nothing
        delete!(collected_scalars, scalar)
    else
        if query == "="
            query = escape_query(scalar)
        end
        collected_scalars[scalar] = parse_scalar_query(query)
    end
    return nothing
end

function collect_axes(
    daf::DafReader,
    axes::AbstractVector{Pair{String, A}},
)::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}} where {A <: Union{String, Nothing}}
    collected_axes = Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}}()
    for (axis, query) in axes
        collect_axis(daf, collected_axes, axis, query)
    end
    return collected_axes
end

function collect_axis(
    daf::DafReader,
    collected_axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}},
    axis::String,
    query::Union{String, Nothing},
)::Nothing
    if axis == "*"
        for axis in axis_names(daf)
            collect_axis(daf, collected_axes, axis, query)
        end
    elseif query == nothing
        delete!(collected_axes, axis)
    else
        if query == "="
            query = escape_query(axis)
        end
        collected_axes[axis] = (query, parse_vector_query("$(query) @ name"))
    end
    return nothing
end

function collect_vectors(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}},
    vectors::AbstractVector{Pair{Tuple{String, String}, V}},
)::Dict{String, Dict{String, Union{VectorQuery, StorageVector}}} where {V <: Union{String, Nothing}}
    collected_vectors = Dict{String, Dict{String, Union{VectorQuery, StorageVector}}}()
    for axis in keys(collected_axes)
        collected_vectors[axis] = Dict{String, Union{VectorQuery, StorageVector}}()
    end
    for ((axis, vector_name), query) in vectors
        collect_vector(view_name, daf, collected_axes, collected_vectors, axis, vector_name, query)
    end
    return collected_vectors
end

function collect_vector(
    view_name::String,
    daf::DafReader,
    collected_axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}},
    collected_vectors::Dict{String, Dict{String, Union{VectorQuery, StorageVector}}},
    axis::String,
    vector_name::String,
    query::Union{String, Nothing},
)::Nothing
    if axis == "*"
        for axis in keys(collected_axes)
            collect_vector(view_name, daf, collected_axes, collected_vectors, axis, vector_name, query)
        end
    elseif vector_name == "*"
        axis_query = collected_axes[axis][2]
        @assert axis_query isa VectorQuery
        for vector_name in vector_names(daf, vector_query_axis(axis_query))
            collect_vector(view_name, daf, collected_axes, collected_vectors, axis, vector_name, query)
        end
    elseif query == nothing
        delete!(collected_vectors[axis], vector_name)
    else
        axis_prefix = require_axis_prefix(daf, view_name, collected_axes, axis)
        if query == "="
            query = escape_query(vector_name)
        end
        encoded_query = encode_expression(query)
        if occursin("@", encoded_query)
            encoded_query = replace(encoded_query, "@" => ", $(axis_prefix) @"; count = 1)
            collected_vectors[axis][vector_name] = parse_vector_query(decode_expression(encoded_query))
        else
            axis_prefix = require_axis_prefix(daf, view_name, collected_axes, axis)
            collected_vectors[axis][vector_name] = parse_vector_query("$(axis_prefix) @ $(query)")
        end
    end
    return nothing
end

function collect_matrices(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}},
    matrices::AbstractVector{Pair{Tuple{String, String, String}, M}},
)::Dict{String, Dict{String, Dict{String, Union{MatrixQuery, StorageMatrix}}}} where {M <: Union{String, Nothing}}
    collected_matrices = Dict{String, Dict{String, Dict{String, Union{MatrixQuery, StorageMatrix}}}}()
    for rows_axis in keys(collected_axes)
        collected_matrices[rows_axis] = Dict{String, Dict{String, Union{MatrixQuery, StorageMatrix}}}()
        for columns_axis in keys(collected_axes)
            collected_matrices[rows_axis][columns_axis] = Dict{String, Union{MatrixQuery, StorageMatrix}}()
        end
    end
    for ((rows_axis, columns_axis, matrix_name), query) in matrices
        collect_matrix(view_name, daf, collected_matrices, collected_axes, rows_axis, columns_axis, matrix_name, query)
    end
    return collected_matrices
end

function collect_matrix(
    view_name::AbstractString,
    daf::DafReader,
    collected_matrices::Dict{String, Dict{String, Dict{String, Union{MatrixQuery, StorageMatrix}}}},
    collected_axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}},
    rows_axis::String,
    columns_axis::String,
    matrix_name::String,
    query::Union{String, Nothing},
)::Nothing
    if rows_axis == "*"
        for rows_axis in keys(collected_axes)
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis,
                columns_axis,
                matrix_name,
                query,
            )
        end
    elseif columns_axis == "*"
        for columns_axis in keys(collected_axes)
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis,
                columns_axis,
                matrix_name,
                query,
            )
        end
    elseif matrix_name == "*"
        rows_axis_query = collected_axes[rows_axis][2]
        @assert rows_axis_query isa VectorQuery
        columns_axis_query = collected_axes[columns_axis][2]
        @assert columns_axis_query isa VectorQuery
        for matrix_name in matrix_names(daf, vector_query_axis(rows_axis_query), vector_query_axis(columns_axis_query))
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis,
                columns_axis,
                matrix_name,
                query,
            )
        end
    elseif query == nothing
        delete!(collected_matrices[rows_axis][columns_axis], matrix_name)
        delete!(collected_matrices[columns_axis][rows_axis], matrix_name)
    else
        rows_axis_prefix = require_axis_prefix(daf, view_name, collected_axes, rows_axis)
        columns_axis_prefix = require_axis_prefix(daf, view_name, collected_axes, columns_axis)
        if query == "="
            query = escape_query(matrix_name)
        end
        collected_matrices[rows_axis][columns_axis][matrix_name] =
            parse_matrix_query("$(rows_axis_prefix), $(columns_axis_prefix) @ $(query)")
        collected_matrices[columns_axis][rows_axis][matrix_name] =
            parse_matrix_query("$(columns_axis_prefix), $(rows_axis_prefix) @ $(query)")
    end
    return nothing
end

function require_axis_prefix(
    daf::DafReader,
    name::AbstractString,
    collected_axes::Dict{String, Tuple{String, Union{VectorQuery, AbstractVector{String}}}},
    axis::String,
)::String
    result = get(collected_axes, axis, nothing)
    if result == nothing
        error("missing axis: $(axis)\nfor the view: $(name)\nof the daf data: $(daf.name)")
    end
    return result[1]
end

function Formats.format_has_scalar(view::DafView, name::AbstractString)::Bool
    return haskey(view.scalars, name)
end

function Formats.format_get_scalar(view::DafView, name::AbstractString)::StorageScalar
    value = view.scalars[name]
    if value isa ScalarQuery
        value = scalar_query(view.daf, value)
        view.scalars[name] = value
    end
    return value
end

function Formats.format_scalar_names(view::DafView)::AbstractSet{String}
    return keys(view.scalars)
end

function Formats.format_has_axis(view::DafView, axis::AbstractString)::Bool
    return haskey(view.axes, axis)
end

function Formats.format_axis_names(view::DafView)::AbstractSet{String}
    return keys(view.axes)
end

function Formats.format_get_axis(view::DafView, axis::AbstractString)::AbstractVector{String}
    (prefix, names) = view.axes[axis]
    if names isa VectorQuery
        result = vector_query(view.daf, names)
        if result == nothing
            error(
                "empty result for query: $(canonical(names))\n" *
                "for the axis: $(axis)\n" *
                "for the view: $(view.name)\n" *
                "of the daf data: $(view.daf.name)",
            )
        end
        names = base_array(result)
        view.axes[axis] = (prefix, names)
    end
    return names
end

function Formats.format_axis_length(view::DafView, axis::AbstractString)::Int64
    return length(Formats.format_get_axis(view, axis))
end

function Formats.format_has_vector(view::DafView, axis::AbstractString, name::AbstractString)::Bool
    return haskey(view.vectors[axis], name)
end

function Formats.format_vector_names(view::DafView, axis::AbstractString)::AbstractSet{String}
    return keys(view.vectors[axis])
end

function Formats.format_get_vector(view::DafView, axis::AbstractString, name::AbstractString)::StorageVector
    value = view.vectors[axis][name]
    if value isa VectorQuery
        result = vector_query(view.daf, value)
        if result == nothing
            error(
                "empty result for query: $(canonical(value))\n" *
                "for the vector: $(name)\n" *
                "for the axis: $(axis)\n" *
                "for the view: $(view.name)\n" *
                "of the daf data: $(view.daf.name)",
            )
        end
        value = as_read_only(result.array)
        view.vectors[axis][name] = value
    end
    return value
end

function Formats.format_has_matrix(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return haskey(view.matrices[rows_axis][columns_axis], name)
end

function Formats.format_matrix_names(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    return keys(view.matrices[rows_axis][columns_axis])
end

function Formats.format_get_matrix(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    value = view.matrices[rows_axis][columns_axis][name]
    if value isa MatrixQuery
        result = matrix_query(view.daf, value)
        if result == nothing
            error(
                "empty result for query: $(canonical(value))\n" *
                "for the matrix: $(name)\n" *
                "for the rows_axis: $(rows_axis)\n" *
                "and the columns: $(columns_axis)\n" *
                "for the view: $(view.name)\n" *
                "of the daf data: $(view.daf.name)",
            )
        end
        value = as_read_only(result.array)
        view.matrices[rows_axis][columns_axis][name] = value
    end
    return value  # NOJET
end

function Formats.format_description_header(view::DafView, indent::String, lines::Array{String})::Nothing
    push!(lines, "$(indent)type: View $(typeof(view.daf))")
    return nothing
end

end # module
