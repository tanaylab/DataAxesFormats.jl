"""
Compute queries on `daf` data.
"""
module DataQueries

export empty_cache!
export matrix_query
export scalar_query
export vector_query

using Daf.Data
using Daf.Formats
using Daf.Messages
using Daf.Oprec
using Daf.ParseQueries
using Daf.Registry
using Daf.StorageTypes
using NamedArrays

import Daf.Data.axis_dependency_key
import Daf.Data.base_array
import Daf.Data.matrix_dependency_key
import Daf.Data.scalar_dependency_key
import Daf.Data.store_cached_dependency_key!
import Daf.Data.vector_dependency_key
import Daf.Formats
import Daf.Messages
import Daf.ParseQueries.CmpDefault
import Daf.ParseQueries.CmpEqual
import Daf.ParseQueries.CmpGreaterOrEqual
import Daf.ParseQueries.CmpGreaterThan
import Daf.ParseQueries.CmpLessOrEqual
import Daf.ParseQueries.CmpLessThan
import Daf.ParseQueries.CmpMatch
import Daf.ParseQueries.CmpNotEqual
import Daf.ParseQueries.CmpNotMatch
import Daf.ParseQueries.FilterAnd
import Daf.ParseQueries.FilterOr
import Daf.ParseQueries.FilterXor

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
    next_axis_entries = get_vector(daf, next_axis, "name")

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
    next_axis_entries::NamedVector{String},
    next_axis_values::AbstractVector,
    property_index::Int,
    missing_mask::Union{Vector{Bool}, Nothing},
)::Any
    if missing_mask != nothing && missing_mask[property_index]
        return zero_of(next_axis_values)  # untested
    end
    index = get(next_axis_entries.dicts[1], last_property_value, nothing)
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
    axis_entries = get_vector(daf, axis_entry.axis_name, "name")
    index = get(axis_entries.dicts[1], axis_entry.entry_name, nothing)
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
