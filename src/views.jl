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
export ViewAxis
export ViewData
export ViewDatum
export viewer

using NamedArrays

using ..Formats
using ..Keys
using ..Queries
using ..Readers
using ..ReadOnly
using ..Registry
using ..StorageTypes
using ..Tokens
using TanayLabUtilities

import ..Formats
import ..Formats.axis_vector_cache_key
import ..Formats.get_through_cache
import ..Formats.Internal
import ..Formats.matrix_cache_key
import ..Formats.put_in_cache!
import ..Formats.scalar_cache_key
import ..Formats.vector_cache_key
import ..Formats.with_cache_write_lock
import ..Queries.as_query_sequence
import ..Queries.patch_query
import ..Readers.base_array
import ..ReadOnly
import ..ReadOnly.DafReadOnlyWrapper
import ..Registry.QueryOperation
import ..Tokens.decode_expression
import ..Tokens.encode_expression

"""
    struct DafView(daf::DafReader) <: DafReader

A read-only wrapper for any [`DafReader`](@ref) data, which exposes an arbitrary view of it as another
[`DafReadOnly`](@ref). This isn't typically created manually; instead call [`viewer`](@ref).
"""
struct DafView <: DafReadOnly
    name::AbstractString
    internal::Internal
    daf::DafReader
    reversed_view_axes::Vector{Tuple{AbstractString, Maybe{QueryString}}}
    reversed_view_scalars::Vector{Tuple{ScalarKey, Maybe{QueryString}}}
    reversed_view_vectors::Vector{Tuple{VectorKey, Maybe{QueryString}}}
    reversed_view_matrices::Vector{Tuple{MatrixKey, Maybe{QueryString}}}
    reversed_view_tensors::Vector{Tuple{TensorKey, Maybe{QueryString}}}
end

"""
Specify an axis to expose from a view.

This is a pair (similar to initializing a `Dict`). The key is the name of the axis in the view and the value is the
query describing how to compute it from the base repository. We also allow using a tuple to to make it easy to invoke
the API from other languages such as Python which do not have the concept of a `Pair`.

If the value is `nothing`, then the axis will **not** be exposed by the view. If the value is `"="`, then the axis will
be exposed with the same entries as in the original `daf` data. If the value is a name it is interpreted as if it is an
axis name (that is, `"obs" => "cell"` is the same as `"obs" => q"@ cell"`). Otherwise the query should be a valid axis
query. For example, saying `"batch" => q"@ batch [ age > 1 ]` will expose the `batch` axis, but only including the
batches whose `age` property is greater than 1.

If the key is `"*"`, then it is replaced by all the names of the axes of the wrapped `daf` data. The only valid queries
in this case are `nothing` to hide all the axes or `=` to expose all the axes. The latter is often used as the first
pair, followed by additional ones to hide or override specific axes.
"""
ViewAxis = Union{Tuple{AbstractString, Maybe{QueryString}}, Pair{<:AbstractString, <:Maybe{QueryString}}}

"""
Specify all the axes to expose from a view. The order of the pairs (or tuples) matters - the last one wins. We would
have liked to specify this as `AbstractVector{<:ViewAxis}` but Julia in its infinite wisdom considers does not allow
`Pair{String, String}` to be a subtype of `Pair{AbstractString, AbstractString}`.
"""
ViewAxes = AbstractVector

"""
Specify a single datum to expose from view.

**Scalars** are specified similar to [`ViewAxis`](@ref), except that a `"*"` key expands to all the scalars in the base
repository and a simple name query is interpreted as a scalar name (that is, `"quality" => "score"` is the same as
`"quality" => q". score"`). In general the query should give a scalar result, for example
`"total_umis" => q"@ cell @ gene :: UMIs >> Sum"` will expose a `total_umis` scalar containing the total sum of all UMIs
of all genes in all cells.

**Vectors** are specified similarly to scalars, but require a tuple key specifying both an axis and a property name.
The axis must be exposed by the view (based on the `axes` parameter). If the axis is `"*"`, it is replaces by all the
exposed axis names specified by the `axes` parameter. Similarly, if the property name is `"*"` (e.g., `("gene", "*")`),
then  it is replaced by all the vector properties of the exposed axis in the base data. Therefore specifying `("*", "*")` (or [`ALL_VECTORS`](@ref))`, all vector properties of all the (exposed) axes will also be exposed.

The value for vectors must be the suffix of a vector query based on the appropriate axis. For example, `("cell", "color") => ": type : color"` will expose a vector of color for each exposed cell, which is the color of the type of the
cell, even if the exposed cell axis is a subset of the original cell axis.

However, if the query starts with an axis operator, then it should be a complete query. This may require repeating the
axis query in it; as a convenience, a axis operator with the special name `__axis__` is replaced by the axis query. For
example, suppose the cell axis is defined as `"cell" => "@ cell [ type = TCell ]"`, then we could expose a vector of the
total UMIs for each cell by saying `"cell", "total_UMIs") => "@ gene @ __axis__ :: UMIs >- Sum"`, which would be
expanded to `@ gene @ cell [ type = TCell ] :: UMIs >- Sum"` to compute the total UMIs only for the exposed cells.

**Matrices** require a tuple key specifying both axes and a property name. The axes must both be exposed by the view
(based on the `axes` parameter). Again if any or both of the axes are `"*"`, they are replaced by all the exposed axes
(based on the `axes` parameter), and likewise if the name is `"*"`, it replaced by all the matrix properties of the
axes. Normally the query is prefixed by the rows and columns axes queries, unless the query starts with an axis operator.
To avoid having to repeat the axes queries in this case, saying `@ __rows_axis__` will expand to the query of the rows
axis and `@ __columns_axis__` will expand to the query of the columns axis.

**3D Tensors** require a tuple key specifying the main axis, followed by two axes, and a property name. All the axes
must be exposed by the view (based on the `axes` parameter). In this cases, none of the axes may be `"*"`, and the value
can only be be `"="` to expose all the matrix properties of the tensor as they are or `nothing` to hide all of them;
that is, views can expose or hide existing (possibly masked) 3D tensors, but can't be used to create new ones.

That is, assuming a `gene`, `cell` and `batch` axes were exposed by the `axes` parameters, then specifying that
`("batch", "cell", "gene", "is_measured") => "="` will expose the set of per-cell-per-gene matrices
`batch1_is_measured`, `batch2_is_measured`, etc.
"""
ViewDatum = Union{Tuple{DataKey, Maybe{QueryString}}, Pair{<:DataKey, <:Maybe{QueryString}}}

"""
Specify all the data to expose from a view. The order of the pairs (or tuples) matters - the last one wins. However,
[`TensorKey`](@ref)s are interpreted after interpreting all [`MatrixKey`](@ref)s, so they will override them even if
they appear earlier in the list of keys. For clarity it is best to list them at the very end of the list.

We would have liked to specify this as `AbstractVector{<:ViewDatum}` but Julia in its infinite wisdom considers does not
allow `Pair{String, String}` to be a subtype of `Pair{AbstractString, AbstractString}`.
"""
ViewData = AbstractVector

"""
A key to use in the `axes` parameter of [`viewer`](@ref) to specify all the base data axes.
"""
ALL_AXES = "*"

"""
A pair to use in the `axes` parameter of [`viewer`](@ref) to specify all the base data axes. This is the default, so the
only reason do this is to say `[VIEW_ALL_AXES, ...]` - that is, follow it by some modifications.
"""
VIEW_ALL_AXES = ALL_AXES => "="

"""
A key to use in the `data` parameter of [`viewer`](@ref) to specify all the base data scalars.
"""
ALL_SCALARS = "*"

"""
A pair to use in the `data` parameter of [`viewer`](@ref) to specify all the base data scalars.
"""
VIEW_ALL_SCALARS = ALL_SCALARS => "="

"""
A key to use in the `data` parameter of [`viewer`](@ref) to specify all the vectors of the exposed axes.
"""
ALL_VECTORS = ("*", "*")

"""
A pair to use in the `data` parameter of [`viewer`](@ref) to specify all the vectors of the exposed axes.
"""
VIEW_ALL_VECTORS = ALL_VECTORS => "="

"""
A key to use in the `data` parameter of [`viewer`](@ref) to specify all the matrices of the exposed axes.
"""
ALL_MATRICES = ("*", "*", "*")

"""
A pair to use in the `data` parameter of [`viewer`](@ref) to specify all the matrices of the exposed axes.
"""
VIEW_ALL_MATRICES = ALL_MATRICES => "="

"""
A vector to use in the `data` parameters of [`viewer`](@ref) to specify the view exposes all the data of the exposed
axes. This is the default, so the only reason do this is to say `[VIEW_ALL_DATA..., ...]` - that is, follow it by some
modifications.
"""
VIEW_ALL_DATA = [VIEW_ALL_SCALARS, VIEW_ALL_VECTORS, VIEW_ALL_MATRICES]

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

Queries are listed separately for axes and data.

!!! note

    As an optimization, calling `viewer` with all-empty (default) arguments returns a simple
    [`DafReadOnlyWrapper`](@ref), that is, it is equivalent to calling [`read_only`](@ref).
"""
function viewer(
    daf::DafReader;
    name::Maybe{AbstractString} = nothing,
    axes::Maybe{ViewAxes} = nothing,
    data::Maybe{ViewData} = nothing,
)::DafReadOnly
    if axes === nothing && data === nothing
        return read_only(daf; name)
    end

    if axes === nothing
        axes = [VIEW_ALL_AXES]
    end

    if data === nothing
        data = VIEW_ALL_DATA
    end

    for axis in axes
        @assert (
            axis isa Union{Pair, Tuple} &&
            length(axis) == 2 &&
            axis[1] isa AbstractString &&
            axis[1] isa Maybe{QueryString}
        ) "invalid ViewAxis: $(axis)"
    end

    for datum in data
        @assert (
            datum isa Union{Pair, Tuple} &&
            length(datum) == 2 &&
            datum[1] isa DataKey &&
            datum[2] isa Maybe{QueryString}
        ) "invalid ViewDatum: $(datum)"
    end

    if daf isa ReadOnly.DafReadOnlyWrapper
        daf = daf.daf
    end

    if name === nothing
        name = daf.name * ".view"
    end
    name = unique_name(name)  # NOJET

    reversed_view_axes = Tuple{AxisKey, Maybe{QueryString}}[(key, query) for (key, query) in axes]
    reversed_view_scalars = Tuple{ScalarKey, Maybe{QueryString}}[]
    reversed_view_vectors = Tuple{VectorKey, Maybe{QueryString}}[]
    reversed_view_matrices = Tuple{MatrixKey, Maybe{QueryString}}[]
    reversed_view_tensors = Tuple{TensorKey, Maybe{QueryString}}[]
    for (key, query) in data
        if key isa ScalarKey
            if key == "*" && !(query in ("=", nothing))
                error(chomp("""
                            invalid wildcard scalar query: $(query)
                            query for wildcard must be one of: "=", nothing
                            for the view: $(name)
                            of the daf data: $(daf.name)
                            """))
            end
            push!(reversed_view_scalars, (key, query))
        elseif key isa VectorKey
            if "*" in key && !(query in ("=", nothing))
                key_axis, key_name = key
                error(chomp("""
                            invalid wildcard vector query: $(query)
                            query for wildcard must be one of: "=", nothing
                            for the vector property: $(key_name)
                            for the vector axis: $(key_axis)
                            for the view: $(name)
                            of the daf data: $(daf.name)
                            """))
            end
            push!(reversed_view_vectors, (key, query))
        elseif key isa MatrixKey
            if "*" in key && !(query in ("=", nothing))
                key_rows_axis, key_columns_axis, key_name = key
                error(chomp("""
                            invalid wildcard matrix query: $(query)
                            query for wildcard must be one of: "=", nothing
                            for the matrix property: $(key_name)
                            for the rows axis: $(key_rows_axis)
                            for the columns axis: $(key_columns_axis)
                            for the view: $(name)
                            of the daf data: $(daf.name)
                            """))
            end
            push!(reversed_view_matrices, (key, query))
        elseif key isa TensorKey
            if "*" in key
                main_axis_name, rows_axis_name, columns_axis_name, matrix_name = key
                error(chomp("""
                            unsupported tensor wildcard
                            for the matrix: $(matrix_name)
                            for the main axis: $(main_axis_name)
                            and the rows axis: $(rows_axis_name)
                            and the columns axis: $(columns_axis_name)
                            for the view: $(name)
                            of the daf data: $(daf.name)
                            """))
            end
            if query != "=" && query !== nothing
                main_axis_name, rows_axis_name, columns_axis_name, matrix_name = key
                error(chomp("""
                            unsupported tensor query: $(query)
                            query for tensor must be one of: "=", nothing
                            for the matrix: $(matrix_name)
                            for the main axis: $(main_axis_name)
                            and the rows axis: $(rows_axis_name)
                            and the columns axis: $(columns_axis_name)
                            for the view: $(name)
                            of the daf data: $(daf.name)
                            """))
            end
            push!(reversed_view_tensors, (key, query))
        else
            @assert false
        end
    end

    reverse!(reversed_view_axes)
    reverse!(reversed_view_scalars)
    reverse!(reversed_view_vectors)
    reverse!(reversed_view_matrices)
    reverse!(reversed_view_tensors)

    wrapper = DafView(
        name,
        Internal(; cache_group = MemoryData, is_frozen = true),
        daf,
        reversed_view_axes,
        reversed_view_scalars,
        reversed_view_vectors,
        reversed_view_matrices,
        reversed_view_tensors,
    )
    @debug "Daf: $(brief(wrapper)) base: $(brief(daf))"
    return wrapper
end

function Formats.begin_data_read_lock(view::DafView, what::Any...)::Nothing
    invoke(Formats.begin_data_read_lock, Tuple{DafReader, Vararg{Any}}, view, what...)
    Formats.begin_data_read_lock(view.daf, what...)
    return nothing
end

function Formats.end_data_read_lock(view::DafView, what::Any...)::Nothing
    Formats.end_data_read_lock(view.daf, what...)
    invoke(Formats.end_data_read_lock, Tuple{DafReader, Vararg{Any}}, view, what...)
    return nothing
end

function Formats.has_data_read_lock(view::DafView)::Bool
    return Formats.has_data_read_lock(view.daf)
end

function Formats.begin_data_write_lock(view::DafView, what::Any...)::Nothing  # flaky tested
    invoke(Formats.begin_data_write_lock, Tuple{DafReader, Vararg{Any}}, view, what...)
    return Formats.begin_data_write_lock(view.daf, what...)
end

function Formats.end_data_write_lock(view::DafView, what::Any...)::Nothing  # flaky tested
    Formats.end_data_write_lock(view.daf, what...)
    return invoke(Formats.end_data_write_lock, Tuple{DafReader, Vararg{Any}}, view, what...)
end

function Formats.has_data_write_lock(::DafView)::Bool  # UNTESTED
    return false
end

function Formats.format_has_scalar(view::DafView, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(view)
    return fetch_scalar_query(view, name) !== nothing
end

function Formats.format_get_scalar(view::DafView, name::AbstractString)::StorageScalar
    @assert Formats.has_data_read_lock(view)
    return fetch_scalar_data(view, name)
end

function Formats.format_scalars_set(view::DafView)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(view)
    return collect_view_scalars(view)
end

function Formats.format_has_axis(view::DafView, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(view)
    return fetch_axis_query(view, axis) !== nothing
end

function Formats.format_axes_set(view::DafView)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(view)
    return collect_view_axes(view)
end

function Formats.format_axis_vector(view::DafView, axis::AbstractString)::AbstractVector{<:AbstractString}
    @assert Formats.has_data_read_lock(view)
    return fetch_axis_data(view, axis)  # NOJET
end

function Formats.format_axis_length(view::DafView, axis::AbstractString)::Int64
    return length(Formats.format_axis_vector(view, axis))
end

function Formats.format_has_vector(view::DafView, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(view)
    return fetch_vector_query(view, axis, name) !== nothing
end

function Formats.format_vectors_set(view::DafView, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(view)
    return collect_view_vectors(view, axis)
end

function Formats.format_get_vector(view::DafView, axis::AbstractString, name::AbstractString)::StorageVector
    @assert Formats.has_data_read_lock(view)
    return fetch_vector_data(view, axis, name)
end

function Formats.format_has_matrix(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(view)
    return fetch_matrix_query(view, rows_axis, columns_axis, name) !== nothing
end

function Formats.format_matrices_set(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(view)
    return collect_view_matrices(view, rows_axis, columns_axis)
end

function Formats.format_get_matrix(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    @assert Formats.has_data_read_lock(view)
    return fetch_matrix_data(view, rows_axis, columns_axis, name)
end

function Formats.format_description_header(
    view::DafView,
    indent::AbstractString,
    lines::Vector{String},
    deep::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(view)
    push!(lines, "$(indent)type: View")
    if !deep
        push!(lines, "$(indent)base: $(brief(view.daf))")
    end
    return nothing
end

function Formats.format_description_footer(
    view::DafView,
    indent::AbstractString,
    lines::Vector{String};
    cache::Bool,
    deep::Bool,
    tensors::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(view)
    if deep
        push!(lines, "$(indent)base:")
        description(view.daf, indent * "  ", lines; cache, deep, tensors)  # NOJET
    end
    return nothing
end

function TanayLabUtilities.Brief.brief(value::DafView; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "View $(brief(value.daf; name))"
end

function ReadOnly.read_only(daf::DafView; name::Maybe{AbstractString} = nothing)::Union{DafView, DafReadOnlyWrapper}
    if name === nothing
        return daf
    else
        wrapper = DafReadOnlyWrapper(name, daf.internal, daf)
        @debug "Daf: $(brief(wrapper)) base: $(daf)"
        return wrapper
    end
end

QUERY_TYPE_BY_DIMENSIONS = ["scalar", "vector", "matrix"]

function fetch_scalar_query(view::DafView, name::AbstractString)::Maybe{QueryOperation}
    cache_key = scalar_cache_key(name, :query)

    query = get_through_cache(view, cache_key, Unsure{QueryOperation}, QueryData) do
        for (view_key, view_query) in view.reversed_view_scalars
            if view_key == name || (view_key == "*" && has_scalar(view.daf, name))
                query = prepare_scalar_query(view, view_query, name)
                if query === nothing || !has_query(view.daf, query)
                    return missing
                else
                    return query
                end
            end
        end
        return missing
    end

    if query === missing
        return nothing
    else
        return query
    end
end

function fetch_scalar_data(view::DafView, name::AbstractString)::StorageScalar
    cache_key = scalar_cache_key(name)
    return get_through_cache(view, cache_key, StorageScalar, QueryData) do
        query = fetch_scalar_query(view, name)
        @assert query !== nothing
        return get_query(view.daf, query; cache = false)
    end
end

function fetch_axis_query(view::DafView, axis::AbstractString)::Maybe{QueryOperation}
    cache_key = axis_vector_cache_key(axis, :query)

    query = get_through_cache(view, cache_key, Unsure{QueryOperation}, QueryData) do
        for (view_key, view_query) in view.reversed_view_axes
            if view_key == axis || (view_key == "*" && has_axis(view.daf, axis))
                query = prepare_axis_query(view, view_query, axis)
                if query === nothing || !has_query(view.daf, query)
                    return missing
                else
                    return query
                end
            end
        end
        return missing
    end

    if query === missing
        return nothing
    else
        return query
    end
end

function fetch_axis_data(view::DafView, axis::AbstractString)::AbstractVector{<:AbstractString}
    cache_key = axis_vector_cache_key(axis)
    return get_through_cache(view, cache_key, AbstractVector{<:AbstractString}, QueryData) do
        query = fetch_axis_query(view, axis)
        @assert query !== nothing
        return get_query(view.daf, query; cache = false)
    end
end

function fetch_vector_query(view::DafView, axis::AbstractString, name::AbstractString)::Maybe{QueryOperation}
    cache_key = vector_cache_key(axis, name, :query)

    axis_query = fetch_axis_query(view, axis)
    @assert axis_query !== nothing
    base_axis = query_axis_name(axis_query)

    query = get_through_cache(view, cache_key, Unsure{QueryOperation}, QueryData) do
        for (key, query) in view.reversed_view_vectors
            key_axis, key_name = key
            if (key_axis == axis || (key_axis == "*")) &&
               (key_name == name || (key_name == "*" && has_vector(view.daf, base_axis, name)))
                query = prepare_vector_query(view, axis_query, axis, query, name)
                if query === nothing || !has_query(view.daf, query)
                    return missing
                else
                    return query
                end
            end
        end
        return missing
    end

    if query === missing
        return nothing
    else
        return query
    end
end

function fetch_vector_data(view::DafView, axis::AbstractString, name::AbstractString)::StorageVector
    cache_key = vector_cache_key(axis, name)
    return get_through_cache(view, cache_key, StorageVector, QueryData) do
        query = fetch_vector_query(view, axis, name)
        @assert query !== nothing
        return get_query(view.daf, query; cache = false)
    end
end

function fetch_matrix_query(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Maybe{QueryOperation}
    cache_key = matrix_cache_key(rows_axis, columns_axis, name, :query)

    rows_axis_query = fetch_axis_query(view, rows_axis)
    @assert rows_axis_query !== nothing

    columns_axis_query = fetch_axis_query(view, columns_axis)
    @assert columns_axis_query !== nothing

    query = get_through_cache(view, cache_key, Unsure{QueryOperation}, QueryData) do
        for (key, query) in view.reversed_view_tensors
            key_main_axis, key_rows_axis, key_columns_axis, key_name = key
            for (test_key_rows_axis, test_key_columns_axis) in
                ((key_rows_axis, key_columns_axis), (key_columns_axis, key_rows_axis))
                if test_key_rows_axis == rows_axis &&
                   test_key_columns_axis == columns_axis &&
                   endswith(name, key_name) &&
                   haskey(axis_dict(view.daf, key_main_axis), name[1:(end - length(key_name) - 1)])
                    query = prepare_matrix_query(
                        view,
                        rows_axis_query,
                        rows_axis,
                        columns_axis_query,
                        columns_axis,
                        query,
                        name,
                    )
                    if query === nothing || !has_query(view.daf, query) || query_requires_relayout(view.daf, query)
                        return missing
                    else
                        return query
                    end
                end
            end
        end

        for (key, query) in view.reversed_view_matrices
            key_rows_axis, key_columns_axis, key_name = key
            for (test_key_rows_axis, test_key_columns_axis) in
                ((key_rows_axis, key_columns_axis), (key_columns_axis, key_rows_axis))
                if (test_key_rows_axis == rows_axis || test_key_rows_axis == "*") &&
                   (test_key_columns_axis == columns_axis || test_key_columns_axis == "*") &&
                   (key_name == name || key_name == "*")
                    query = prepare_matrix_query(
                        view,
                        rows_axis_query,
                        rows_axis,
                        columns_axis_query,
                        columns_axis,
                        query,
                        name,
                    )
                    if query === nothing || !has_query(view.daf, query) || query_requires_relayout(view.daf, query)
                        return missing
                    else
                        return query
                    end
                end
            end
        end

        return missing
    end

    if query === missing
        return nothing
    else
        return query
    end
end

function fetch_matrix_data(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    cache_key = matrix_cache_key(rows_axis, columns_axis, name)
    return get_through_cache(view, cache_key, StorageMatrix, QueryData) do
        query = fetch_matrix_query(view, rows_axis, columns_axis, name)
        @assert query !== nothing
        return get_query(view.daf, query; cache = false)
    end
end

function collect_view_scalars(view::DafView)::AbstractSet{<:AbstractString}
    seen_keys = Set{AbstractString}()
    scalars = Set{AbstractString}()
    for (key, query) in view.reversed_view_scalars
        collect_view_scalar(view, seen_keys, scalars, key, query)
    end
    return scalars
end

function collect_view_axes(view::DafView)::AbstractSet{<:AbstractString}
    seen_keys = Set{AbstractString}()
    axes = Set{AbstractString}()
    for (key, query) in view.reversed_view_axes
        collect_view_axis(view, seen_keys, axes, key, query)
    end
    return axes
end

function collect_view_vectors(view::DafView, axis::AbstractString)::AbstractSet{<:AbstractString}
    seen_vectors = Set{AbstractString}()
    vectors = Set{AbstractString}()
    axis_query = fetch_axis_query(view, axis)
    base_axis = query_axis_name(axis_query)
    for (key, query) in view.reversed_view_vectors
        key_axis, key_name = key
        collect_view_vector(view, axis, axis_query, base_axis, seen_vectors, vectors, key_axis, key_name, query)
    end
    return vectors
end

function collect_view_matrices(
    view::DafView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    seen_matrices = Set{AbstractString}()
    matrices = Set{AbstractString}()
    rows_axis_query = fetch_axis_query(view, rows_axis)
    columns_axis_query = fetch_axis_query(view, columns_axis)
    base_rows_axis = query_axis_name(rows_axis_query)
    base_columns_axis = query_axis_name(columns_axis_query)
    for (key, query) in view.reversed_view_matrices
        key_rows_axis, key_columns_axis, key_name = key
        collect_view_matrix(
            view,
            rows_axis,
            rows_axis_query,
            base_rows_axis,
            columns_axis,
            columns_axis_query,
            base_columns_axis,
            seen_matrices,
            matrices,
            key_rows_axis,
            key_columns_axis,
            key_name,
            query,
        )
    end
    for (key, query) in view.reversed_view_tensors
        key_main_axis, key_rows_axis, key_columns_axis, key_name = key
        collect_view_tensor(
            view,
            rows_axis,
            base_rows_axis,
            columns_axis,
            base_columns_axis,
            seen_matrices,
            matrices,
            key_main_axis,
            key_rows_axis,
            key_columns_axis,
            key_name,
            query,
        )
    end
    return matrices
end

function collect_view_scalar(
    view::DafView,
    seen_keys::Set{AbstractString},
    scalars::Set{AbstractString},
    key_name::ScalarKey,
    query::Maybe{QueryString},
)::Nothing
    if key_name == "*"
        for name in scalars_set(view.daf)
            @assert name != "*"
            collect_view_scalar(view, seen_keys, scalars, name, query)
        end
    elseif !(key_name in seen_keys)
        push!(seen_keys, key_name)
        query = prepare_scalar_query(view, query, key_name)
        if query !== nothing && has_query(view.daf, query)
            push!(scalars, key_name)
        end
    end
    return nothing
end

function collect_view_axis(
    view::DafView,
    seen_keys::Set{AbstractString},
    axes::Set{AbstractString},
    key_name::AxisKey,
    query::Maybe{QueryString},
)::Nothing
    if key_name == "*"
        for axis in axes_set(view.daf)
            @assert axis != "*"
            collect_view_axis(view, seen_keys, axes, axis, query)
        end
    elseif !(key_name in seen_keys)
        push!(seen_keys, key_name)
        query = prepare_axis_query(view, query, key_name)
        if query !== nothing && has_query(view.daf, query)
            push!(axes, key_name)
        end
    end
    return nothing
end

function collect_view_vector(
    view::DafView,
    axis::AbstractString,
    axis_query::QueryOperation,
    base_axis::AbstractString,
    seen_vectors::Set{AbstractString},
    vectors::Set{AbstractString},
    key_axis::AbstractString,
    key_name::AbstractString,
    query::Maybe{QueryString},
)::Nothing
    if key_axis == "*" || key_axis == axis
        if key_name == "*"
            for name in vectors_set(view.daf, base_axis)
                @assert name != "*"
                collect_view_vector(view, axis, axis_query, base_axis, seen_vectors, vectors, axis, name, query)
            end

        elseif !(key_name in seen_vectors)
            push!(seen_vectors, key_name)
            query = prepare_vector_query(view, axis_query, axis, query, key_name)
            if query !== nothing && has_query(view.daf, query)
                push!(vectors, key_name)
            end
        end
    end
    return nothing
end

function collect_view_matrix(
    view::DafView,
    rows_axis::AbstractString,
    rows_axis_query::QueryOperation,
    base_rows_axis::AbstractString,
    columns_axis::AbstractString,
    columns_axis_query::QueryOperation,
    base_columns_axis::AbstractString,
    seen_matrices::Set{AbstractString},
    matrices::Set{AbstractString},
    key_rows_axis::AbstractString,
    key_columns_axis::AbstractString,
    key_name::AbstractString,
    query::Maybe{QueryString},
)::Nothing
    for (test_key_rows_axis, test_key_columns_axis) in
        ((key_rows_axis, key_columns_axis), (key_columns_axis, key_rows_axis))
        if (test_key_rows_axis == "*" || test_key_rows_axis == rows_axis) &&
           (test_key_columns_axis == "*" || test_key_columns_axis == columns_axis)
            if key_name == "*"
                for name in matrices_set(view.daf, base_rows_axis, base_columns_axis; relayout = false)
                    @assert name != "*"
                    collect_view_matrix(
                        view,
                        rows_axis,
                        rows_axis_query,
                        base_rows_axis,
                        columns_axis,
                        columns_axis_query,
                        base_columns_axis,
                        seen_matrices,
                        matrices,
                        rows_axis,
                        columns_axis,
                        name,
                        query,
                    )
                end

            elseif !(key_name in seen_matrices)
                push!(seen_matrices, key_name)
                query = prepare_matrix_query(
                    view,
                    rows_axis_query,
                    rows_axis,
                    columns_axis_query,
                    columns_axis,
                    query,
                    key_name,
                )
                if query !== nothing && has_query(view.daf, query) && !query_requires_relayout(view.daf, query)
                    push!(matrices, key_name)
                end
            end
        end
    end
    return nothing
end

function collect_view_tensor(
    view::DafView,
    rows_axis::AbstractString,
    base_rows_axis::AbstractString,
    columns_axis::AbstractString,
    base_columns_axis::AbstractString,
    seen_matrices::Set{AbstractString},
    matrices::Set{AbstractString},
    key_main_axis::AbstractString,
    key_rows_axis::AbstractString,
    key_columns_axis::AbstractString,
    key_name::AbstractString,
    query::Maybe{QueryString},
)::Nothing
    if !Formats.format_has_axis(view, key_main_axis; for_change = false)
        error(chomp("""
                    hidden tensor main axis: $(key_main_axis)
                    for the matrix: $(key_name)
                    for the rows axis: $(key_rows_axis)
                    for the columns axis: $(key_columns_axis)
                    for the view: $(view.name)
                    of the daf data: $(view.daf.name)
                    """))
    end
    if query !== nothing
        main_axis_dict = axis_dict(view, key_main_axis)
        for (test_key_rows_axis, test_key_columns_axis) in
            ((key_rows_axis, key_columns_axis), (key_columns_axis, key_rows_axis))
            if (test_key_rows_axis == rows_axis && test_key_columns_axis == columns_axis) ||
               (test_key_rows_axis == columns_axis && test_key_columns_axis == rows_axis)
                for candidate_name in
                    matrices_set(view.daf, base_rows_axis, base_columns_axis; tensors = false, relayout = false)
                    if endswith(candidate_name, key_name) &&
                       candidate_name[end - length(key_name)] == '_' &&
                       haskey(main_axis_dict, candidate_name[1:(end - length(key_name) - 1)]) &&
                       !(candidate_name in seen_matrices)
                        push!(matrices, candidate_name)
                    end
                end
            end
        end
    end
    return nothing
end

function prepare_scalar_query(
    view::DafView,
    maybe_query::Maybe{QueryString},
    name::AbstractString,
)::Maybe{QueryOperation}
    query = prepare_query(maybe_query, name, LookupScalar)
    if query !== nothing
        dimensions = query_result_dimensions(query)
        if dimensions != 0
            error(chomp("""
                $(QUERY_TYPE_BY_DIMENSIONS[dimensions + 1]) query: $(query)
                for the scalar: $(name)
                for the view: $(view.name)
                of the daf data: $(view.daf.name)
                """))
        end
    end
    return query
end

function prepare_axis_query(view::DafView, maybe_query::Maybe{QueryString}, axis::AbstractString)::Maybe{QueryOperation}
    query = prepare_query(maybe_query, axis, Axis)
    if query !== nothing && !is_axis_query(query)
        error(chomp("""
            not an axis query: $(query)
            for the axis: $(axis)
            for the view: $(view.name)
            of the daf data: $(view.daf.name)
            """))
    end
    return query
end

function prepare_vector_query(
    view::DafView,
    axis_query::QueryOperation,
    axis::AbstractString,
    maybe_query::Maybe{QueryString},
    name::AbstractString,
)::Maybe{QueryOperation}
    query = prepare_query(maybe_query, name, LookupVector)
    if query !== nothing
        axis_query = as_query_sequence(axis_query)
        query = patch_query(query, ["__axis__" => as_query_sequence(axis_query)])
        if !(query.query_operations[1] isa Axis)
            query = axis_query |> query
        end
        dimensions = query_result_dimensions(query)
        if dimensions != 1
            error(chomp("""
                $(QUERY_TYPE_BY_DIMENSIONS[dimensions + 1]) query: $(query)
                for the vector: $(name)
                for the axis: $(axis)
                for the view: $(view.name)
                of the daf data: $(view.daf.name)
                """))
        end
    end
    return query
end

function prepare_matrix_query(
    view::DafView,
    rows_axis_query::QueryOperation,
    rows_axis::AbstractString,
    columns_axis_query::QueryOperation,
    columns_axis::AbstractString,
    maybe_query::Maybe{QueryString},
    name::AbstractString,
)::Maybe{QueryOperation}
    query = prepare_query(maybe_query, name, LookupMatrix)
    if query !== nothing
        rows_axis_query = as_query_sequence(rows_axis_query)
        columns_axis_query = as_query_sequence(columns_axis_query)
        query = patch_query(
            query,
            [
                "__rows_axis__" => as_query_sequence(rows_axis_query),
                "__columns_axis__" => as_query_sequence(columns_axis_query),
            ],
        )
        if !(query.query_operations[1] isa Axis)
            query = rows_axis_query |> columns_axis_query |> query
        end
        dimensions = query_result_dimensions(query)
        if dimensions != 2
            error(chomp("""
                        $(QUERY_TYPE_BY_DIMENSIONS[dimensions + 2]) query: $(query)
                        for the matrix: $(name)
                        for the rows axis: $(rows_axis)
                        and the columns axis: $(columns_axis)
                        for the view: $(view.name)
                        of the daf data: $(view.daf.name)
                        """))
        end
    end
    return query
end

function prepare_query(
    maybe_query::Maybe{QueryString},
    name::AbstractString,
    operand_only::Maybe{Type{<:QueryOperation}},
)::Maybe{QueryOperation}
    if maybe_query === nothing
        return nothing
    elseif maybe_query isa QueryOperation
        return maybe_query  # UNTESTED
    else
        @assert maybe_query isa AbstractString
        query_string = strip(maybe_query)
        if query_string == "="
            query_string = strip(name)
        end
        return parse_query(query_string, operand_only)
    end
end

end  # module
