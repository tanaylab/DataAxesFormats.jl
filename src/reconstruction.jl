"""
Reconstruct implicit axes. Due to `AnnData` two-axes limitations, other axes are often represented by storing their
expanded data (e.g., a type for each cell, and a color for each cell, where the color is actually per type). When
converting such data to `Daf`, it is useful to reconstruct such axes (e.g., create a type axis, assign a color for each
type, and delete the per-cell color property).
"""
module Reconstruction

export reconstruct_axis

using Daf.Data
using Daf.Formats
using Daf.Generic
using Daf.StorageTypes
using Daf.Queries

"""
    function reconstruct_axis(
        daf::DafWriter,
        existing_axis::AbstractString,
        implicit_axis::AbstractString,
        rename_axis::Maybe{AbstractString} = Nothing,
        empty_implicit::Maybe{AbstractString} = Nothing,
        implicit_properties::Maybe{AbstractStringSet} = Nothing,
    )::AbstractStringSet

Given an `existing_axis` in `daf`, which has a property `implicit_axis`, create a new axis with the same name (or, if
specified, call it `rename_axis`). If `empty_implicit` is specified, this value of the property is replaced by the empty
string (indicate there is no value associated with the `existing_axis` entry). For each of the `implicit_properties`, we
collect the mapping between the `implicit_axis` and the property values, and store it as a property of the newly created
axis.

If `implicit_properties` are explicitly specified, then we require the mapping from `implicit_axis` to be consistent.
Otherwise, we look at all the properties of the `existing_axis`, and check for each one whether the mapping is
consistent; if it is, we migrate the property to the new axis. For example, when importing `AnnData` containing per-cell
data, it isn't always clear which property is actually per-batch (e.g., cell age) and which is actually per cell (e.g.,
doublet score). Not specifying the `implicit_properties` allows the function to figure it out on its own.

!!! note

    For each converted property, the value associated with `existing_axis` entries which have no `implicit_axis` value
    (that is, have an empty string or `empty_implicit` value) is lost. For example, if each cell type has a color, but
    some cells do not have a type, then the color of "cells with no type" is lost. We still require this value to be
    consistent, and return a mapping between each migrated property name and the value of such entries (if any exist).
    When reconstructing the original property, specify this value using [`IfNot`](@ref) (e.g., `/ cell : type => color ?? magenta`).
"""
function reconstruct_axis(
    daf::DafWriter;
    existing_axis::AbstractString,
    implicit_axis::AbstractString,
    rename_axis::Maybe{AbstractString} = nothing,
    empty_implicit::Maybe{StorageScalar} = nothing,
    implicit_properties::Maybe{AbstractStringSet} = nothing,
)::AbstractDict{<:AbstractString, Maybe{StorageScalar}}
    if implicit_properties != nothing
        @assert !(implicit_axis in implicit_properties)
    end

    implicit_values = get_vector(daf, existing_axis, implicit_axis)
    overwrite_implicit_values =
        !(eltype(implicit_values) <: AbstractString) || (empty_implicit != nothing && empty_implicit != "")
    if eltype(implicit_values) <: AbstractString && empty_implicit == nothing
        empty_implicit = ""
    end
    unique_values = unique(implicit_values[implicit_values .!= empty_implicit])
    sort!(unique_values)
    if !(eltype(unique_values) <: AbstractString)
        unique_values = [string(unique_value) for unique_value in unique_values]
    end
    implicit_values = [
        if implicit_value == empty_implicit
            ""  # only seems untested
        else
            string(implicit_value)
        end for implicit_value in implicit_values
    ]

    empty_values_of_properties = Dict{AbstractString, Maybe{StorageScalar}}()
    vector_values_of_properties = Dict{AbstractString, StorageVector}()
    for property in vector_names(daf, existing_axis)
        is_explicit = implicit_properties != nothing && property in implicit_properties
        if is_explicit || (implicit_properties == nothing && property != implicit_axis)
            property_data = collect_property_data(
                daf,
                existing_axis,
                implicit_axis,
                property,
                implicit_values,
                unique_values;
                must_be_consistent = is_explicit,
            )
            if property_data != nothing
                empty_value_of_property, vector_value_of_property = property_data
                empty_values_of_properties[property] = empty_value_of_property
                vector_values_of_properties[property] = vector_value_of_property
            end
        end
    end

    if rename_axis == nothing
        rename_axis = implicit_axis
    end
    add_axis!(daf, rename_axis, unique_values)

    if overwrite_implicit_values
        set_vector!(daf, existing_axis, implicit_axis, implicit_values; overwrite = true)
    end

    for (property, vector_value) in vector_values_of_properties
        set_vector!(daf, rename_axis, property, vector_value)
        delete_vector!(daf, existing_axis, property)
    end

    return empty_values_of_properties
end

function collect_property_data(
    daf::DafReader,
    existing_axis::AbstractString,
    implicit_axis::AbstractString,
    property::AbstractString,
    implicit_values::AbstractStringVector,
    unique_values::AbstractStringVector;
    must_be_consistent::Bool,
)::Maybe{Tuple{Maybe{StorageScalar}, <:StorageVector}}
    property_values = get_vector(daf, existing_axis, property)
    property_values_of_implicits = Dict{AbstractString, eltype(property_values)}()
    @assert length(property_values) == length(implicit_values)

    for (property_value, implicit_value) in zip(property_values, implicit_values)
        property_value_of_implicit = get(property_values_of_implicits, implicit_value, nothing)
        if property_value_of_implicit == nothing
            property_values_of_implicits[implicit_value] = property_value
        elseif property_value_of_implicit != property_value
            if must_be_consistent
                error(
                    "inconsistent values of the property: $(property)\n" *
                    "of the axis: $(existing_axis)\n" *
                    "for the reconstructed axis: $(implicit_axis)\n" *
                    "in the daf data: $(daf.name)",
                )
            end
            return nothing
        end
    end

    empty_value_of_property = get(property_values_of_implicits, "", nothing)
    vector_value_of_property = [property_values_of_implicits[unique_value] for unique_value in unique_values]
    return (empty_value_of_property, vector_value_of_property)
end

end  # module
