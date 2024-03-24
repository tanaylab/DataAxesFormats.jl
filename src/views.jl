"""
Create a different view of `Daf` data using queries. This is a very flexible mechanism which can be used for a variety
of use cases. A simple way of using this is to view a subset of the data as a `Daf` data set. A variant of this also
renames the data properties to adapt them to the requirements of some computation. This makes it simpler to create such
tools (using fixed, generic property names) and apply them to arbitrary data (with arbitrary specific
property names).
"""
module Views

export ALL_AXES
export ALL_MATRICES
export ALL_SCALARS
export ALL_VECTORS
export DafView
export VIEW_ALL_AXES
export VIEW_ALL_DATA
export VIEW_ALL_MATRICES
export VIEW_ALL_SCALARS
export VIEW_ALL_VECTORS
export ViewAxes
export ViewData
export viewer

using Daf.Data
using Daf.Formats
using Daf.Generic
using Daf.Messages
using Daf.Queries
using Daf.ReadOnly
using Daf.StorageTypes
using Daf.Tokens

import Daf.Data.as_read_only_array
import Daf.Data.base_array
import Daf.Formats
import Daf.Formats.Internal
import Daf.Messages
import Daf.Tokens.decode_expression
import Daf.Tokens.encode_expression
import Daf.ReadOnly
import Daf.ReadOnly.DafReadOnlyWrapper

# Something we fetch from the original data.
mutable struct Fetch{T}
    query::Query
    value::Maybe{T}
end

"""
    struct DafView(daf::DafReader) <: DafReader

A read-only wrapper for any [`DafReader`](@ref) data, which exposes an arbitrary view of it as another
[`DafReadOnly`](@ref). This isn't typically created manually; instead call [`viewer`](@ref).
"""
struct DafView <: DafReadOnly
    internal::Internal
    daf::DafReader
    scalars::Dict{AbstractString, Fetch{StorageScalar}}
    axes::Dict{AbstractString, Fetch{AbstractStringVector}}
    vectors::Dict{AbstractString, Dict{AbstractString, Fetch{StorageVector}}}
    matrices::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, Fetch{StorageMatrix}}}}
end

"""
Specify axes to expose from a view.

This is specified as a vector of pairs (similar to initializing a `Dict`). The order of the pairs matter (last one
wins).

If the key is `"*"`, then it is replaced by all the names of the axes of the wrapped `daf` data. Otherwise, the key is
just the name of an axis.

If the value is `nothing`, then the axis will **not** be exposed by the view. If the value is `"="`, then the axis will
be exposed with the same entries as in the original `daf` data. Otherwise the value is any valid query that returns a
vector of (unique!) strings to serve as the vector entries.

That is, saying `"*" => "="` (or, [`VIEW_ALL_AXES`](@ref) will expose all the original `daf` data axes from the view.
Following this by saying `"type" => nothing` will hide the `type` from the view. Saying `"batch" => q"/ batch & age > 1`
will expose the `batch` axis, but only including the batches whose `age` property is greater than 1.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        ViewAxes = AbstractVector{Pair{AbstractString, Maybe{Union{AbstractString, Query}}}}

    But what we are **forced** to say is:

        ViewAxes = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
ViewAxes = AbstractVector{<:Pair}

"""
Specify data to expose from view. This is specified as a vector of pairs (similar to initializing a `Dict`). The order
of the pairs matter (last one wins).

**Scalars** are specified similarly to [`ViewAxes`](@ref), except that the query should return a scalar instead of a
vector. That is, saying `"*" => "="` (or, [`VIEW_ALL_SCALARS`](@ref)) will expose all the original `daf` data scalars
from the view. Following this by saying `"version" => nothing` will hide the `version` from the view. Adding
`"total_umis" => q"/ cell / gene : UMIs %> Sum %> Sum"` will expose a `total_umis` scalar containing the total sum of
all UMIs of all genes in all cells, etc.

**Vectors** are specified similarly to scalars, but require a key specifying both an axis and a property name. The axis
must be exposed by the view (based on the `axes` parameter). If the axis is `"*"`, it is replaces by all the exposed
axis names specified by the `axes` parameter. Similarly, if the property name is `"*"` (e.g., `("gene", "*")`), then  it
is replaced by all the vector properties of the exposed axis in the base data. Therefore if the pair is `("*", "*") => "="` (or [`VIEW_ALL_VECTORS`](@ref))`, all vector properties of all the (exposed) axes will also be exposed.

The value for vectors must be the suffix of a vector query based on the appropriate axis; a value of `"="` is again used
to expose the property as-is. That is, the value for the vector will normally start with the `:` ([`Lookup`](@ref))
query operator.

That is, specifying that `axes = ["gene" => q"/ gene & marker"]`, and then that
`data = [("gene", "forbidden") => q": lateral"]`, then the view will expose a `forbidden` vector property for the `gene`
axis, by applying the combined query `/ gene & marker : lateral` to the original `daf` data.

This gets trickier when using a query reducing a matrix to a vector. In these cases, the value query will start with `/`
([`Axis`](@ref)) query operator to specify the reduced matrix axis, followed by the `:` ([`Lookup`](@ref)) operator.
When constructing the full query for the data, we can't simply concatenate the suffix to the axis query prefix; instead
we need to swap the order of the axes (this is because Julia, in its infinite wisdom, uses column-major matrices, like R
and matlab; so reduction eliminates the rows instead of the columns of the matrix).

That is, specifying `axes = ["cell" => q"/ cell & type = TCell"]`, and then
`data = [("cell", "total_noisy_UMIs") => q"/ gene & noisy : UMIs %> Sum` will expose `total_noisy_UMIs` as a
per-`cell` vector property, using the query `/ gene & noisy / cell & type = TCell : UMIs %> Sum`, which will
compute the sum of the `UMIs` of all the noisy genes for each cell (whose `type` is `TCell`).

**Matrices** require a key specifying both axes and a property name. The axes must both be exposed by the view (based on
the `axes` parameter). Again if any or both of the axes are `"*"`, they are replaced by all the exposed axes (based on
the `axes` parameter), and likewise if the name is `"*"`, it replaced by all the matrix properties of the axes. The
value for matrices can again be `"="` to expose the property as is, or the suffix of a matrix query. Therefore if the
pair is `("*", "*", "*") => "="` (or, `VIEW_ALL_MATRICES`), all matrix properties of all the (exposed) axes will also be
exposed.

The order of the axes does not matter, so
`data = [("gene", "cell", "UMIs") => "="]` has the same effect as `data = [("cell", "gene", "UMIs") => "="]`.

That is, assuming a `gene` and `cell` axes were exposed by the `axes` parameter, then specifying that
`("cell", "gene", "log_UMIs") => q": UMIs % Log base 2 eps"` will expose the matrix `log_UMIs` for each cell and gene.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        ViewData = AbstractVector{Pair{DataKey, Maybe{Union{AbstractString, Query}}}}

    But what we are **forced** to say is:

        ViewData = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
ViewData = AbstractVector{<:Pair}

"""
A pair to use in the `axes` parameter of [`viewer`](@ref) to specify all the base data axes.
"""
ALL_AXES = "*"

"""
A pair to use in the `axes` parameter of [`viewer`](@ref) to specify the view exposes all the base data axes.
"""
VIEW_ALL_AXES = ALL_AXES => "="

"""
A key to use in the `data` parameter of [`viewer`](@ref) to specify all the base data scalars.
"""
ALL_SCALARS = "*"

"""
A pair to use in the `data` parameter of [`viewer`](@ref) to specify the view exposes all the base data scalars.
"""
VIEW_ALL_SCALARS = ALL_SCALARS => "="

"""
A key to use in the `data` parameter of [`viewer`](@ref) to specify all the vectors of the exposed axes.
"""
ALL_VECTORS = ("*", "*")

"""
A pair to use in the `data` parameter of [`viewer`](@ref) to specify the view exposes all the vectors of the exposed
axes.
"""
VIEW_ALL_VECTORS = ALL_VECTORS => "="

"""
A key to use in the `data` parameter of [`viewer`](@ref) to specify all the matrices of the exposed axes.
"""
ALL_MATRICES = ("*", "*", "*")

"""
A pair to use in the `data` parameter of [`viewer`](@ref) to specify the view exposes all the matrices of the exposed
axes.
"""
VIEW_ALL_MATRICES = ALL_MATRICES => "="

"""
A vector of pairs to use in the `data` parameters of [`viewer`](@ref) (using `...`) to specify the view exposes all
the data of the exposed axes.
"""
VIEW_ALL_DATA = [VIEW_ALL_SCALARS, VIEW_ALL_VECTORS, VIEW_ALL_MATRICES]

EMPTY_AXES = Vector{Pair{String, String}}()
EMPTY_DATA = Vector{Pair{String, String}}()

"""
    viewer(
        daf::DafReader;
        [name::Maybe{AbstractString} = nothing,
        axes::Maybe{ViewAxes} = nothing,
        data::Maybe{ViewData} = nothing]
    )::DafReadOnly

Wrap `daf` data with a read-only [`DafView`](@ref). The exposed view is defined by a set of queries applied to the
original data. These queries are evaluated only when data is actually accessed. Therefore, creating a view is a
relatively cheap operation.

If the `name` is not specified, the result name will be based on the name of `daf`, with a `.view` suffix.

Queries are listed separately for axes, and scalars, vector and matrix properties, as follows:

!!! note

    As an optimization, calling `viewer` with all-empty (default) arguments returns a simple
    [`DafReadOnlyWrapper`](@ref), that is, it is equivalent to calling [`read_only`](@ref). Additionally, saying
    `data = VIEW_ALL_DATA` will expose all the data using any of the exposed axes; you can write
    `data = [VIEW_ALL_DATA..., key => nothing]` to hide specific data based on its `key`.
"""
function viewer(
    daf::DafReader;
    name::Maybe{AbstractString} = nothing,
    axes::Maybe{ViewAxes} = nothing,
    data::Maybe{ViewData} = nothing,
)::DafReadOnly
    if axes == nothing
        axes = EMPTY_AXES
    end
    if data == nothing
        data = EMPTY_DATA
    end

    if isempty(axes) && isempty(data)
        return read_only(daf; name = name)
    end

    if daf isa ReadOnly.DafReadOnlyWrapper
        daf = daf.daf
    end

    if name == nothing
        name = daf.name * ".view"
    end

    for (key, query) in data
        @assert key isa DataKey
        @assert query isa Maybe{Union{AbstractString, Query}}
    end

    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}} = collect_axes(name, daf, axes)
    collected_scalars::Dict{AbstractString, Fetch{StorageScalar}} = collect_scalars(name, daf, data)
    collected_vectors::Dict{AbstractString, Dict{AbstractString, Fetch{StorageVector}}} =
        collect_vectors(name, daf, collected_axes, data)
    collected_matrices::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, Fetch{StorageMatrix}}}} =
        collect_matrices(name, daf, collected_axes, data)

    return DafView(Internal(name), daf, collected_scalars, collected_axes, collected_vectors, collected_matrices)
end

function collect_scalars(
    view_name::AbstractString,
    daf::DafReader,
    data::ViewData,
)::Dict{AbstractString, Fetch{StorageScalar}}
    collected_scalars = Dict{AbstractString, Fetch{StorageScalar}}()
    for (key, query) in data
        if key isa AbstractString
            collect_scalar(view_name, daf, collected_scalars, key, prepare_query(query))
        end
    end
    return collected_scalars
end

function prepare_query(maybe_query::Maybe{Union{AbstractString, Query}})::Maybe{Union{AbstractString, Query}}
    if maybe_query isa AbstractString
        maybe_query = strip(maybe_query)
        if maybe_query != "="
            maybe_query = Query(maybe_query)
        end
    end
    return maybe_query
end

QUERY_TYPE_BY_DIMENSIONS = ["scalar", "vector", "matrix"]

function collect_scalar(
    view_name::AbstractString,
    daf::DafReader,
    collected_scalars::Dict{AbstractString, Fetch{StorageScalar}},
    scalar_name::AbstractString,
    scalar_query::Maybe{Union{AbstractString, Query}},
)::Nothing
    if scalar_name == "*"
        for scalar_name in scalar_names(daf)
            collect_scalar(view_name, daf, collected_scalars, scalar_name, scalar_query)
        end
    elseif scalar_query == nothing
        delete!(collected_scalars, scalar_name)
    else
        if scalar_query == "="
            scalar_query = Lookup(scalar_name)
        else
            @assert scalar_query isa Query
        end
        dimensions = query_result_dimensions(scalar_query)
        if dimensions != 0
            error(
                "$(QUERY_TYPE_BY_DIMENSIONS[dimensions + 1]) query: $(scalar_query)\n" *
                "for the scalar: $(scalar_name)\n" *
                "for the view: $(view_name)\n" *
                "of the daf data: $(daf.name)",
            )
        end
        collected_scalars[scalar_name] = Fetch{StorageScalar}(scalar_query, nothing)
    end
    return nothing
end

function collect_axes(
    view_name::AbstractString,
    daf::DafReader,
    axes::ViewAxes,
)::Dict{AbstractString, Fetch{AbstractStringVector}}
    collected_axes = Dict{AbstractString, Fetch{AbstractStringVector}}()
    for (axis, query) in axes
        @assert axis isa AbstractString
        @assert query isa Maybe{Union{AbstractString, Query}}
        collect_axis(view_name, daf, collected_axes, axis, prepare_query(query))
    end
    return collected_axes
end

function collect_axis(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}},
    axis_name::AbstractString,
    axis_query::Maybe{Union{AbstractString, Query}},
)::Nothing
    if axis_name == "*"
        for axis_name in axis_names(daf)
            collect_axis(view_name, daf, collected_axes, axis_name, axis_query)
        end
    elseif axis_query == nothing
        delete!(collected_axes, axis_name)
    else
        if axis_query == "="
            axis_query = Axis(axis_name)
        else
            @assert axis_query isa Query
        end
        dimensions = query_result_dimensions(axis_query)
        if dimensions != 1
            error(
                "$(QUERY_TYPE_BY_DIMENSIONS[dimensions + 1]) query: $(axis_query)\n" *
                "for the axis: $(axis_name)\n" *
                "for the view: $(view_name)\n" *
                "of the daf data: $(daf.name)",
            )
        end
        collected_axes[axis_name] = Fetch{AbstractStringVector}(axis_query, nothing)
    end
    return nothing
end

function collect_vectors(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}},
    data::ViewData,
)::Dict{AbstractString, Dict{AbstractString, Fetch{StorageVector}}}
    collected_vectors = Dict{AbstractString, Dict{AbstractString, Fetch{StorageVector}}}()
    for axis in keys(collected_axes)
        collected_vectors[axis] = Dict{AbstractString, Fetch{StorageVector}}()
    end
    for (key, query) in data
        if key isa Tuple{AbstractString, AbstractString}
            axis_name, vector_name = key
            collect_vector(
                view_name,
                daf,
                collected_axes,
                collected_vectors,
                axis_name,
                vector_name,
                prepare_query(query),
            )
        end
    end
    return collected_vectors
end

function collect_vector(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}},
    collected_vectors::Dict{AbstractString, Dict{AbstractString, Fetch{StorageVector}}},
    axis_name::AbstractString,
    vector_name::AbstractString,
    vector_query::Maybe{Union{AbstractString, Query}},
)::Nothing
    if axis_name == "*"
        for axis_name in keys(collected_axes)
            collect_vector(view_name, daf, collected_axes, collected_vectors, axis_name, vector_name, vector_query)
        end
    elseif vector_name == "*"
        fetch_axis = get_fetch_axis(view_name, daf, collected_axes, axis_name)
        base_axis = base_axis_of_query(fetch_axis.query)
        for vector_name in vector_names(daf, base_axis)
            collect_vector(view_name, daf, collected_axes, collected_vectors, axis_name, vector_name, vector_query)
        end
    elseif vector_query == nothing
        delete!(collected_vectors[axis_name], vector_name)
    else
        fetch_axis = get_fetch_axis(view_name, daf, collected_axes, axis_name)
        if vector_query == "="
            vector_query = Lookup(vector_name)
        else
            @assert vector_query isa Query
        end
        if vector_query isa QuerySequence && vector_query.query_operations[1] isa Axis
            query_prefix, query_suffix = split_vector_query(vector_query)
            vector_query = query_prefix |> fetch_axis.query |> query_suffix
        else
            vector_query = fetch_axis.query |> vector_query
        end
        dimensions = query_result_dimensions(vector_query)
        if dimensions != 1
            error(
                "$(QUERY_TYPE_BY_DIMENSIONS[dimensions + 1]) query: $(vector_query)\n" *
                "for the vector: $(vector_name)\n" *
                "for the axis: $(axis_name)\n" *
                "for the view: $(view_name)\n" *
                "of the daf data: $(daf.name)",
            )
        end
        collected_vectors[axis_name][vector_name] = Fetch{StorageVector}(vector_query, nothing)
    end
    return nothing
end

function split_vector_query(query_sequence::QuerySequence)::Tuple{QuerySequence, QuerySequence}
    index = findfirst(query_sequence.query_operations) do query_operation
        return query_operation isa Lookup
    end
    if index == nothing
        return (query_sequence, QuerySequence(()))  # untested
    else
        return (
            QuerySequence(query_sequence.query_operations[1:(index - 1)]),
            QuerySequence(query_sequence.query_operations[index:end]),
        )
    end
end

function collect_matrices(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}},
    data::ViewData,
)::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, Fetch{StorageMatrix}}}}
    collected_matrices = Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, Fetch{StorageMatrix}}}}()
    for rows_axis_name in keys(collected_axes)
        collected_matrices[rows_axis_name] = Dict{AbstractString, Dict{AbstractString, Fetch{StorageMatrix}}}()
        for columns_axis_name in keys(collected_axes)
            collected_matrices[rows_axis_name][columns_axis_name] = Dict{AbstractString, Fetch{StorageMatrix}}()
        end
    end
    for (key, query) in data
        if key isa Tuple{AbstractString, AbstractString, AbstractString}
            (rows_axis_name, columns_axis_name, matrix_name) = key
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis_name,
                columns_axis_name,
                matrix_name,
                prepare_query(query),
            )
        end
    end
    return collected_matrices
end

function collect_matrix(
    view_name::AbstractString,
    daf::DafReader,
    collected_matrices::Dict{AbstractString, Dict{AbstractString, Dict{AbstractString, Fetch{StorageMatrix}}}},
    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}},
    rows_axis_name::AbstractString,
    columns_axis_name::AbstractString,
    matrix_name::AbstractString,
    matrix_query::Maybe{Union{AbstractString, Query}},
)::Nothing
    if rows_axis_name == "*"
        for rows_axis_name in keys(collected_axes)
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis_name,
                columns_axis_name,
                matrix_name,
                matrix_query,
            )
        end
    elseif columns_axis_name == "*"
        for columns_axis_name in keys(collected_axes)
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis_name,
                columns_axis_name,
                matrix_name,
                matrix_query,
            )
        end
    elseif matrix_name == "*"
        fetch_rows_axis = get_fetch_axis(view_name, daf, collected_axes, rows_axis_name)
        fetch_columns_axis = get_fetch_axis(view_name, daf, collected_axes, columns_axis_name)
        base_rows_axis = base_axis_of_query(fetch_rows_axis.query)
        base_columns_axis = base_axis_of_query(fetch_columns_axis.query)
        for matrix_name in matrix_names(daf, base_rows_axis, base_columns_axis)
            collect_matrix(
                view_name,
                daf,
                collected_matrices,
                collected_axes,
                rows_axis_name,
                columns_axis_name,
                matrix_name,
                matrix_query,
            )
        end
    elseif matrix_query == nothing
        delete!(collected_matrices[rows_axis_name][columns_axis_name], matrix_name)
        delete!(collected_matrices[columns_axis_name][rows_axis_name], matrix_name)
    else
        fetch_rows_axis = get_fetch_axis(view_name, daf, collected_axes, rows_axis_name)
        fetch_columns_axis = get_fetch_axis(view_name, daf, collected_axes, columns_axis_name)
        if matrix_query == "="
            matrix_query = Lookup(matrix_name)
        else
            @assert matrix_query isa Query
        end
        matrix_query = fetch_rows_axis.query |> fetch_columns_axis.query |> matrix_query
        dimensions = query_result_dimensions(matrix_query)
        if dimensions != 2
            error(
                "$(QUERY_TYPE_BY_DIMENSIONS[dimensions + 1]) query: $(matrix_query)\n" *
                "for the matrix: $(matrix_name)\n" *
                "for the rows axis: $(rows_axis_name)\n" *
                "and the columns axis: $(columns_axis_name)\n" *
                "for the view: $(view_name)\n" *
                "of the daf data: $(daf.name)",
            )
        end
        collected_matrices[rows_axis_name][columns_axis_name][matrix_name] = Fetch{StorageMatrix}(matrix_query, nothing)
    end
    return nothing
end

function base_axis_of_query(query_sequence::QuerySequence)::AbstractString
    return base_axis_of_query(query_sequence.query_operations[1])
end

function base_axis_of_query(axis::Axis)::AbstractString
    return axis.axis_name
end

function get_fetch_axis(
    view_name::AbstractString,
    daf::DafReader,
    collected_axes::Dict{AbstractString, Fetch{AbstractStringVector}},
    axis::AbstractString,
)::Fetch{AbstractStringVector}
    fetch_axis = get(collected_axes, axis, nothing)
    if fetch_axis == nothing
        error("the axis: $(axis)\n" * "is not exposed by the view: $(view_name)\n" * "of the daf data: $(daf.name)")
    end
    return fetch_axis
end

function Formats.format_has_scalar(view::DafView, name::AbstractString)::Bool
    return haskey(view.scalars, name)
end

function Formats.format_get_scalar(view::DafView, name::AbstractString)::StorageScalar
    fetch_scalar = view.scalars[name]
    scalar_value = fetch_scalar.value
    if scalar_value == nothing
        scalar_value = get_query(view.daf, fetch_scalar.query)
        fetch_scalar.value = scalar_value
    end
    return scalar_value
end

function Formats.format_scalar_names(view::DafView)::AbstractStringSet
    return keys(view.scalars)
end

function Formats.format_has_axis(view::DafView, axis::AbstractString; for_change::Bool)::Bool
    return haskey(view.axes, axis)
end

function Formats.format_axis_names(view::DafView)::AbstractStringSet
    return keys(view.axes)
end

function Formats.format_get_axis(view::DafView, axis::AbstractString)::AbstractStringVector
    fetch_axis = view.axes[axis]
    axis_names = fetch_axis.value
    if axis_names == nothing
        axis_names = as_read_only_array(get_query(view.daf, fetch_axis.query).array)
        if !(eltype(axis_names) <: AbstractString)
            error(
                "non-String vector of: $(eltype(axis_names))\n" *
                "names vector for the axis: $(axis)\n" *
                "results from the query: $(fetch_axis.query)\n" *
                "for the daf data: $(view.daf.name)",
            )
        end
        fetch_axis.value = axis_names
    end
    return axis_names
end

function Formats.format_axis_length(view::DafView, axis::AbstractString)::Int64
    return length(Formats.format_get_axis(view, axis))
end

function Formats.format_has_vector(view::DafView, axis::AbstractString, name::AbstractString)::Bool
    return haskey(view.vectors[axis], name)
end

function Formats.format_vector_names(view::DafView, axis::AbstractString)::AbstractStringSet
    return keys(view.vectors[axis])
end

function Formats.format_get_vector(view::DafView, axis::AbstractString, name::AbstractString)::StorageVector
    fetch_vector = view.vectors[axis][name]
    vector_value = fetch_vector.value
    if vector_value == nothing
        vector_value = as_read_only_array(get_query(view.daf, fetch_vector.query))
        fetch_vector.value = vector_value
    end
    return vector_value
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
)::AbstractStringSet
    return keys(view.matrices[rows_axis][columns_axis])
end

function Formats.format_get_matrix(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    fetch_matrix = view.matrices[rows_axis][columns_axis][name]
    matrix_value = fetch_matrix.value
    if matrix_value == nothing
        matrix_value = as_read_only_array(get_query(view.daf, fetch_matrix.query))
        fetch_matrix.value = matrix_value
    end
    return matrix_value
end

function Formats.format_description_header(view::DafView, indent::AbstractString, lines::Vector{String})::Nothing
    push!(lines, "$(indent)type: View $(typeof(view.daf))")
    return nothing
end

function Messages.describe(value::DafView; name::Maybe{AbstractString} = nothing)::String
    if name == nothing
        name = value.name
    end
    return "View $(describe(value.daf; name = name))"
end

function ReadOnly.read_only(daf::DafView; name::Maybe{AbstractString} = nothing)::DafView
    if name == nothing
        return daf
    else
        return DafView(
            Formats.renamed_internal(daf.internal, name),
            daf.daf,
            daf.scalars,
            daf.axes,
            daf.vectors,
            daf.matrices,
        )
    end
end

end # module
