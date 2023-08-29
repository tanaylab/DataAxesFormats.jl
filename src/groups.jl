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
export get_group_vector

using Daf.Data
using Daf.Formats
using Daf.StorageTypes
using NamedArrays

"""
    get_group_vector(
        daf::DafReader;
        axis::AbstractString,
        group::AbstractString,
        group_axis::Union{AbstractString, Nothing} = nothing,
        name::AbstractString,
        [default::Union{StorageScalar, UndefInitializer} = undef]
    ) -> StorageVector

Given an `axis` of the `daf` data (e.g., cell), and a `group` vector property of this axis (e.g., type), whose value is
the name of an entry of a group axis, and a `name` of a vector property of this group axis (e.g., color), then return a
vector assigning a value for each entry of the original axis (e.g., a color for each cell).

By default, the `group_axis` is assumed to have the same name as the `group` property (e.g., there would be a type
property per cell, and a type axis). It is possible to override this by specifying an explicit `group_axis` if the
actual name is different.

The `group` property must have a string element type. An empty string means that the entry belongs to no group (e.g., we
don't have a type assignment for some cell). In this case, `default` must be specified, and is used for the ungrouped
entries.
"""
function get_group_vector(
    daf::DafReader;
    axis::AbstractString,
    group::AbstractString,
    name::AbstractString,
    group_axis::Union{AbstractString, Nothing} = nothing,
    default::Union{StorageScalar, UndefInitializer} = undef,
)::NamedArray
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
    value_of_groups = get_vector(daf, group_axis, name)

    value_of_entries = Vector{eltype(value_of_groups)}(undef, length(group_of_entries))
    for (entry_index, group_of_entry) in enumerate(group_of_entries.array)
        if group_of_entry == ""
            if default == undef
                error(
                    "ungrouped entry: $(names(group_of_entries, 1)[entry_index])\n" *
                    "with the index: $(entry_index)\n" *
                    "of the axis: $(axis)\n" *
                    "has empty group: $(group)\n" *
                    "in the daf data: $(daf.name)",
                )
            end
            value_of_entries[entry_index] = default
        else
            index_of_value_of_group = get(value_of_groups.dicts[1], group_of_entry, nothing)
            if index_of_value_of_group == nothing
                error(
                    "invalid value: $(group_of_entry)\n" *
                    "of the group: $(group)\n" *
                    "of the entry: $(names(group_of_entries, 1)[entry_index])\n" *
                    "with the index: $(entry_index)\n" *
                    "of the axis: $(axis)\n" *
                    "is missing from the group axis: $(group_axis)\n" *
                    "in the daf data: $(daf.name)",
                )
            end
            value_of_entries[entry_index] = value_of_groups.array[index_of_value_of_group]
        end
    end

    return NamedArray(value_of_entries, group_of_entries.dicts, group_of_entries.dimnames)
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
        daf::DafReader;
        axis::AbstractString,
        rows_name::AbstractString,
        columns_name::AbstractString,
        [rows_axis::Union{AbstractString, Nothing} = nothing,
        columns_axis::Union{AbstractString, Nothing} = nothing,
        type::Type = UInt32]
    )::NamedMatrix

Given an `axis` of the `daf` data (e.g., cell) that has two vector properties, called `rows_name` and `columns_name`
(e.g., type and age), return a matrix whose rows and columns are the unique values of these properties, containing the
number of entries that have that combination of values (e.g., the number of cells with a specific type and a specific
age).

If there exists an axis with the same name as the `rows_name` and/or the `columns_name`, it is used to determine the
order of the entries. Otherwise, the entries are sorted in ascending order. If the name of any of the axes is different,
specify the `rows_axis` and/or `columns_axis` to override the name(s).

By default, the data type of the matrix is `UInt32`, which is a reasonable trade-off between expressiveness (up to 4G)
and size (only 4 bytes per entry). You can override this using the `type` parameter.

!!! note

    The typically value type is a string; in this case, entries with an empty string values (ungrouped entries) are not
    counter. However, the values can also be numeric. In either case, it is expected that the set of actually present
    values will be small, otherwise the resulting matrix will be very large.
"""
function count_groups_matrix(
    daf::DafReader;
    axis::AbstractString,
    rows_name::AbstractString,
    columns_name::AbstractString,
    rows_axis::Union{AbstractString, Nothing} = nothing,
    columns_axis::Union{AbstractString, Nothing} = nothing,
    type::Type = UInt32,
)::NamedMatrix
    rows_axis, row_value_of_entries, all_row_values = collect_count_axis(daf, axis, rows_name, rows_axis)
    columns_axis, column_value_of_entries, all_column_values = collect_count_axis(daf, axis, columns_name, columns_axis)

    counts_matrix = NamedArray(
        zeros(type, length(all_row_values), length(all_column_values));
        names = (all_row_values, all_column_values),
        dimnames = (rows_axis, columns_axis),
    )

    for (row_value, column_value) in zip(row_value_of_entries, column_value_of_entries)
        row_value = string(row_value)
        column_value = string(column_value)
        if row_value != "" && column_value != ""
            counts_matrix[row_value, column_value] += 1
        end
    end

    return counts_matrix
end

function collect_count_axis(
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString,
    axis_name::Union{AbstractString, Nothing},
)::Tuple{AbstractString, StorageVector, AbstractVector{String}}
    value_of_entries = get_vector(daf, axis, name)

    if axis_name == nothing
        axis_name = name
    end

    if has_axis(daf, axis_name)
        all_values = get_axis(daf, axis_name)
    else
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
