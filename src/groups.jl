"""
A common data pattern is for entries of one axis to be grouped together. When this happens, we can associate with each
entry a data property of the group, or we can aggregate a data property of the entries into a data property of the
group. For example, if we group cells into types, we can obtain a cell color by looking up the color of the type of each
cell; or if each cell has an age, we can compute the mean cell age of each type.

The following functions implement these lookup and aggregation operations.
"""
module Groups

export aggregate_group_vector
export count_groups_matrix
export get_chained_vector

using Daf.Data
using Daf.DataQueries
using Daf.Formats
using Daf.StorageTypes
using NamedArrays

import Daf.DataQueries.axis_of_property
import Daf.DataQueries.compute_property_lookup

"""
    get_chained_vector(
        daf::DafReader;
        axis::AbstractString,
        names::Vector[S],
        [default::Union{StorageScalar, UndefInitializer} = undef]
    ) -> StorageVector where {S <: AbstractString}

Given an `axis` and a series of `names` properties, expect each property value to be a string, used to lookup its value
in a property axis of the same name, until the last property that is actually returned. For example, if the `axis` is
`cell` and the `names` are `["batch", "donor", "sex"]`, then fetch the sex of the donor of the batch of each cell.

The group axis is assumed to have the same name as the named property (e.g., there would be `batch` and `donor` axes).
It is also possible to have the property name begin with the axis name followed by a `.suffix`, for example, fetching
`["type.manual", "color"]` will fetch the `color` from the `type` axis, based on the value of the `type.manual` of each
cell.

If, at any place along the chain, the group property value is the empty string, then `default` must be specified, and
will be used for the final result.
"""
function get_chained_vector(
    daf::DafReader,
    axis::AbstractString,
    names::Vector{S};
    default::Union{StorageScalar, UndefInitializer} = undef,
)::NamedArray where {S <: AbstractString}
    if isempty(names)
        error("empty names for get_chained_vector")
    end

    values, missing_mask = compute_property_lookup(daf, axis, names, Set{String}(), nothing, default != undef)

    if default != undef
        @assert missing_mask != nothing
        values[missing_mask] .= default
    else
        @assert missing_mask == nothing
    end

    return values
end

"""
    function aggregate_group_vector(
        aggregate::Function,
        daf::DafReader;
        axis::AbstractString,
        name::AbstractString,
        group::AbstractString,
        group_axis::Union{AbstractString, Nothing} = nothing,
        default::Union{StorageScalar, UndefInitializer} = undef,
    )::NamedArray

Given an `axis` of the `daf` data (e.g., cell), a `name` vector property of this axis (e.g., age) and a `group` vector
property of this axis (e.g., type), whose value is the name of an entry of a group axis, then return a vector assigning a
value for each entry of the group axis, which is the `aggregate` of the values of all the original axis entries grouped
into that entry (e.g., the mean age of the cells in each type).

By default, the `group_axis` is assumed to have the same name as the `group` property (e.g., there would be a type
property per cell, and a type axis). It is possible to override this by specifying an explicit `group_axis` if the
actual name is different.

The `group` property must have a string element type. An empty string means that the entry belongs to no group (e.g., we
don't have a type assignment for some cell), so its value will not be aggregated into any group. In addition, a group
may be empty (e.g., no cell is assigned to some type). In this case, `default` must be specified, and is used for the
empty groups.
"""
function aggregate_group_vector(
    aggregate::Function,
    daf::DafReader;
    group::AbstractString,
    axis::AbstractString,
    name::AbstractString,
    group_axis::Union{AbstractString, Nothing} = nothing,
    default::Union{StorageScalar, UndefInitializer} = undef,
)::NamedArray
    value_of_entries = get_vector(daf, axis, name)
    group_of_entries = get_vector(daf, axis, group)
    if eltype(group_of_entries) != String
        error(
            "non-String data type: $(eltype(group_of_entries))\n" *
            "for the group: $(group)\n" *
            "for the axis: $(axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end

    if group_axis == nothing
        group_axis = group
    end
    name_of_groups = get_vector(daf, group_axis, "name")

    value_of_groups = Vector{eltype(value_of_entries)}(undef, length(name_of_groups))
    for (group_index, group_name) in enumerate(name_of_groups.array)
        mask_of_entries_of_groups = group_of_entries .== group_name
        value_of_entries_of_groups = value_of_entries[mask_of_entries_of_groups]
        if isempty(value_of_entries_of_groups)
            if default == undef
                error(
                    "empty group: $(group_name)\n" *
                    "with the index: $(group_index)\n" *
                    "in the group: $(group)\n" *
                    "for the axis: $(axis)\n" *
                    "in the daf data: $(daf.name)",
                )
            end
            value_of_groups[group_index] = default
        else
            value_of_groups[group_index] = aggregate(value_of_entries_of_groups)
        end
    end

    return NamedArray(value_of_groups, name_of_groups.dicts, name_of_groups.dimnames)
end

"""
    function count_groups_matrix(
        daf::DafReader,
        axis::AbstractString,
        rows_names::Vector{R},
        columns_names::Vector{C};
        type::Type = UInt32,
        rows_default::Union{StorageScalar, Nothing},
        columns_default::Union{StorageScalar, Nothing},
    )::NamedMatrix

Given an `axis` of the `daf` data (e.g., cell), fetch two chained vector properties for it using
[`get_chained_vector`](@ref), and generate a matrix where each entry is the number of instances which have each specific
combination of the values. For example, if `axis` is `cell`, `rows_names` is `["batch", "age"]`, and `columns_names` is
`["type", "color"]`, then the matrix will have the different ages as rows, different colors as columns, and each entry
will count the number of cells with a specific age and a specific color.

If there exists an axis with the same name as the final row and/or column name, it is used to determine the set of valid
values and their order. Otherwise, the entries are sorted in ascending order.

By default, the data type of the matrix is `UInt32`, which is a reasonable trade-off between expressiveness (up to 4G)
and size (only 4 bytes per entry). You can override this using the `type` parameter.

!!! note

    The typically the chained value type is a string; in this case, entries with an empty string values (ungrouped
    entries) are not counted. However, the values can also be numeric. In either case, it is expected that the set of
    actually present values will be small, otherwise the resulting matrix will be very large.
"""
function count_groups_matrix(
    daf::DafReader,
    axis::AbstractString,
    rows_names::Vector{R},
    columns_names::Vector{C};
    type::Type = UInt32,
    rows_default::Union{StorageScalar, UndefInitializer} = undef,
    columns_default::Union{StorageScalar, UndefInitializer} = undef,
)::NamedMatrix where {R <: AbstractString, C <: AbstractString}
    rows_axis, row_value_of_entries, all_row_values = collect_count_axis(daf, axis, rows_names, rows_default)
    columns_axis, column_value_of_entries, all_column_values =
        collect_count_axis(daf, axis, columns_names, columns_default)

    counts_matrix = NamedArray(
        zeros(type, length(all_row_values), length(all_column_values));
        names = (all_row_values, all_column_values),
        dimnames = (rows_axis, columns_axis),
    )

    for (row_value, column_value) in zip(row_value_of_entries, column_value_of_entries)
        row_value = string(row_value)
        column_value = string(column_value)
        if row_value != "" && column_value != ""
            counts_matrix[row_value, column_value] += 1  # NOJET
        end
    end

    return counts_matrix
end

function collect_count_axis(
    daf::DafReader,
    axis::AbstractString,
    names::Vector{S},
    default::Union{StorageScalar, UndefInitializer},
)::Tuple{AbstractString, StorageVector, AbstractVector{String}} where {S <: AbstractString}
    value_of_entries = get_chained_vector(daf, axis, names; default = default)

    axis_name = axis_of_property(daf, names[end])
    if has_axis(daf, axis_name)
        all_values = get_axis(daf, axis_name)
    else
        axis_name = names[end]
        all_values = unique!(sort!(copy(value_of_entries.array)))
        if eltype(all_values) != String
            all_values = [string(value) for value in all_values]
        elseif !isempty(all_values) && all_values[1] == ""
            all_values = all_values[2:end]
        end
    end

    return (axis_name, value_of_entries, all_values)
end

end # module
