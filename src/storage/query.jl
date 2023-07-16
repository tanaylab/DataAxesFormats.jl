import Daf.Registry.EltwiseOperation
import Daf.Registry.ReductionOperation
import Daf.Registry.compute_eltwise
import Daf.Registry.compute_reduction
import Daf.Query.CmpEqual
import Daf.Query.CmpGreaterOrEqual
import Daf.Query.CmpGreaterThan
import Daf.Query.CmpLessOrEqual
import Daf.Query.CmpLessThan
import Daf.Query.CmpMatch
import Daf.Query.CmpNotEqual
import Daf.Query.CmpNotMatch
import Daf.Query.FilterAnd
import Daf.Query.FilterOr
import Daf.Query.FilterXor

function query(storage::AbstractStorage, matrix_query::MatrixQuery)::Union{StorageMatrix, Nothing}
    result = compute_matrix_lookup(storage, matrix_query.matrix_property_lookup)
    result = compute_eltwise_result(matrix_query.eltwise_operations, result)
    return result
end

function compute_matrix_lookup(
    storage::AbstractStorage,
    matrix_property_lookup::MatrixPropertyLookup,
)::Union{AbstractMatrix, Nothing}
    result = get_matrix(
        storage,
        matrix_property_lookup.matrix_axes.rows_axis.axis_name,
        matrix_property_lookup.matrix_axes.columns_axis.axis_name,
        matrix_property_lookup.property_name,
    )

    rows_mask = compute_filtered_axis(storage, matrix_property_lookup.matrix_axes.rows_axis)
    columns_mask = compute_filtered_axis(storage, matrix_property_lookup.matrix_axes.columns_axis)

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

function compute_filtered_axis(storage::AbstractStorage, filtered_axis::FilteredAxis)::Union{Vector{Bool}, Nothing}
    if isempty(filtered_axis.axis_filters)
        return nothing
    end

    mask = fill(true, axis_length(storage, filtered_axis.axis_name))
    for axis_filter in filtered_axis.axis_filters
        mask = compute_axis_filter(storage, mask, filtered_axis.axis_name, axis_filter)
    end

    return mask
end

function compute_axis_filter(
    storage::AbstractStorage,
    mask::Vector{Bool},
    axis::AbstractString,
    axis_filter::AxisFilter,
)::Vector{Bool}
    filter = compute_axis_lookup(storage, axis, axis_filter.axis_lookup)
    if eltype(filter) != Bool
        error(
            "non-Bool data type: $(eltype(filter))\n" *
            "for the axis filter: $(canonical(axis_filter))\n" *
            "in the storage: $(storage.name)",
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

function compute_axis_lookup(storage::AbstractStorage, axis::AbstractString, axis_lookup::AxisLookup)::Vector
    values = compute_property_lookup(storage, axis, axis_lookup.property_lookup)

    if axis_lookup.property_comparison == nothing
        return values

    elseif axis_lookup.property_comparison.comparison_operator == CmpMatch ||
           axis_lookup.property_comparison.comparison_operator == CmpNotMatch
        return compute_axis_lookup_match(storage, axis, axis_lookup, values)

    else
        return compute_axis_lookup_compare(storage, axis, axis_lookup, values)
    end
end

function compute_axis_lookup_match(
    storage::AbstractStorage,
    axis::AbstractString,
    axis_lookup::AxisLookup,
    values::Vector,
)::Vector
    if eltype(values) != String
        error(
            "non-String data type: $(eltype(values))\n" *
            "for the match axis lookup: $(canonical(axis_lookup))\n" *
            "for the axis: $(axis)\n" *
            "in the storage: $(storage.name)",
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
            "in the storage: $(storage.name)",
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
    storage::AbstractStorage,
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
                "in the storage: $(storage.name)",
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

function compute_property_lookup(
    storage::AbstractStorage,
    axis::AbstractString,
    property_lookup::PropertyLookup,
)::Vector
    last_property_name = property_lookup.property_names[1]
    values = get_vector(storage, axis, last_property_name)

    for next_property_name in property_lookup.property_names[2:end]
        if eltype(values) != String
            error(
                "non-String data type: $(eltype(values))\n" *
                "for the chained property: $(last_property_name)\n" *
                "for the axis: $(axis)\n" *
                "in the storage: $(storage.name)",
            )
        end
        values, axis = compute_chained_property(storage, axis, last_property_name, values, next_property_name)
        last_property_name = next_property_name
    end

    return values
end

function compute_chained_property(
    storage::AbstractStorage,
    last_axis::AbstractString,
    last_property_name::AbstractString,
    last_property_values::Vector{String},
    next_property_name::AbstractString,
)::Tuple{Vector, String}
    if has_axis(storage, last_property_name)
        next_axis = last_property_name
    else
        next_axis = split(last_property_name, "."; limit = 2)[1]
    end

    next_axis_entries = get_axis(storage, next_axis)
    next_axis_values = get_vector(storage, next_axis, next_property_name)

    return (
        [
            find_axis_value(
                storage,
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
    storage::AbstractStorage,
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
            "in the storage: $(storage.name)",
        )
    end
    return next_axis_values[index]
end

function query(storage::AbstractStorage, vector_query::VectorQuery)::Union{StorageVector, Nothing}
    result = compute_vector_data_lookup(storage, vector_query.vector_data_lookup)
    result = compute_eltwise_result(vector_query.eltwise_operations, result)
    return result
end

function compute_vector_data_lookup(
    storage::AbstractStorage,
    vector_property_lookup::VectorPropertyLookup,
)::Union{StorageVector, Nothing}
    result =
        compute_axis_lookup(storage, vector_property_lookup.filtered_axis.axis_name, vector_property_lookup.axis_lookup)
    mask = compute_filtered_axis(storage, vector_property_lookup.filtered_axis)

    if mask != nothing && !any(mask)
        return nothing
    end

    if mask != nothing
        result = result[mask]
    end

    return result
end

function compute_vector_data_lookup(
    storage::AbstractStorage,
    matrix_slice_lookup::MatrixSliceLookup,
)::Union{StorageVector, Nothing}
    result = get_matrix(
        storage,
        matrix_slice_lookup.matrix_slice_axes.filtered_axis.axis_name,
        matrix_slice_lookup.matrix_slice_axes.axis_entry.axis_name,
        matrix_slice_lookup.property_name,
    )

    index = find_axis_entry_index(storage, matrix_slice_lookup.matrix_slice_axes.axis_entry)
    result = result[:, index]

    rows_mask = compute_filtered_axis(storage, matrix_slice_lookup.matrix_slice_axes.filtered_axis)
    if rows_mask != nothing
        result = result[rows_mask]
    end

    return result
end

function compute_vector_data_lookup(
    storage::AbstractStorage,
    reduce_matrix_query::ReduceMatrixQuery,
)::Union{StorageVector, Nothing}
    result = query(storage, reduce_matrix_query.matrix_query)
    if result == nothing
        return nothing
    end
    return compute_reduction_result(reduce_matrix_query.reduction_operation, result)
end

function query(storage::AbstractStorage, scalar_query::ScalarQuery)::Union{StorageScalar, Nothing}
    result = compute_scalar_data_lookup(storage, scalar_query.scalar_data_lookup)
    result = compute_eltwise_result(scalar_query.eltwise_operations, result)
    return result
end

function compute_scalar_data_lookup(
    storage::AbstractStorage,
    scalar_property_lookup::ScalarPropertyLookup,
)::Union{StorageScalar, Nothing}
    return get_scalar(storage, scalar_property_lookup.property_name)
end

function compute_scalar_data_lookup(
    storage::AbstractStorage,
    reduce_vector_query::ReduceVectorQuery,
)::Union{StorageScalar, Nothing}
    result = query(storage, reduce_vector_query.vector_query)
    return compute_reduction_result(reduce_vector_query.reduction_operation, result)
end

function compute_scalar_data_lookup(
    storage::AbstractStorage,
    vector_entry_lookup::VectorEntryLookup,
)::Union{StorageScalar, Nothing}
    result = compute_axis_lookup(storage, vector_entry_lookup.axis_entry.axis_name, vector_entry_lookup.axis_lookup)
    index = find_axis_entry_index(storage, vector_entry_lookup.axis_entry)
    return result[index]
end

function compute_scalar_data_lookup(
    storage::AbstractStorage,
    matrix_entry_lookup::MatrixEntryLookup,
)::Union{StorageScalar, Nothing}
    result = get_matrix(
        storage,
        matrix_entry_lookup.matrix_entry_axes.rows_entry.axis_name,
        matrix_entry_lookup.matrix_entry_axes.columns_entry.axis_name,
        matrix_entry_lookup.property_name,
    )
    row_index = find_axis_entry_index(storage, matrix_entry_lookup.matrix_entry_axes.rows_entry)
    column_index = find_axis_entry_index(storage, matrix_entry_lookup.matrix_entry_axes.columns_entry)
    return result[row_index, column_index]
end

function find_axis_entry_index(storage::AbstractStorage, axis_entry::AxisEntry)::Int
    axis_entries = get_axis(storage, axis_entry.axis_name)
    index = findfirst(==(axis_entry.entry_name), axis_entries)
    if index == nothing
        error(
            "the entry: $(axis_entry.entry_name)\n" *
            "is missing from the axis: $(axis_entry.axis_name)\n" *
            "in the storage: $(storage.name)",
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
