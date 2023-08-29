"""
A common data pattern is for entries of one axis to be grouped together. When this happens, we can associate with each
entry a data property of the group, or we can aggregate a data property of the entries into a data property of the
group. For example, if we group cells into types, we can obtain a cell color by looking up the color of the type of each
cell; or if each cell has an age, we can compute the mean cell age of each type.

The following functions implement these lookup and aggregation operations.
"""
module Groups

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

Given an `axis` of the `daf` data (e.g. cell), and a `group` vector property of this axis (e.g. type), whose value is
the name of an entry of a group axis, and a `name` of a vector property of this group axis (e.g., color), then return a
vector assigning a value for each entry of the original axis (e.g., a color for each cell).

By default, the `group_axis` is assumed to have the same name as the `group` property (e.g., there would be a type
property per cell, and a type axis). It is possible to override this by specifying an explicit `group_axis` if the
actual name is different.

The `group` property must have a string element type. An empty string means that the element belongs to no group (e.g.,
we don't have a type assignment for some cell). In this case, `default` must be specified, and is used for the ungrouped
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

end # module
