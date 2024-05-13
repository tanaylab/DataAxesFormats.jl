"""
Reconstruct implicit axes. Due to `AnnData` two-axes limitations, other axes are often represented by storing their
expanded data (e.g., a type for each cell, and a color for each cell, where the color is actually per type). When
converting such data to `Daf`, it is useful to reconstruct such axes (e.g., create a type axis, assign a color for each
type, and delete the per-cell color property).
"""
module Reconstruction

export reconstruct_axis!

using ..Formats
using ..GenericLogging
using ..GenericTypes
using ..Queries
using ..Readers
using ..StorageTypes
using ..Writers

"""
    reconstruct_axis!(
        daf::DafWriter;
        existing_axis::AbstractString,
        implicit_axis::AbstractString,
        [rename_axis::Maybe{AbstractString} = nothing,
        empty_implicit::Maybe{StorageScalar} = nothing,
        implicit_properties::Maybe{AbstractStringSet} = nothing,
        properties_defaults::Maybe{AbstractDict} = nothing]
    )::AbstractDict{<:AbstractString, Maybe{StorageScalar}}

Given an `existing_axis` in `daf`, which has a property `implicit_axis`, create a new axis with the same name (or, if
specified, call it `rename_axis`). If `empty_implicit` is specified, this value of the property is replaced by the empty
string (indicate there is no value associated with the `existing_axis` entry). For each of the `implicit_properties`, we
collect the mapping between the `implicit_axis` and the property values, and store it as a property of the newly created
axis.

If the `implicit_axis` already exists, we verify that all the values provided for it by the `existing_axis` do, in fact,
exist as names of entries in the `implicit_axis`. This allows manually creating the `implicit_axis` with additional
entries that are not currently in use.

!!! note

    If the `implicit_axis` already exists and contains entries that aren't currently in use, you must specify
    `properties_defaults` for the values of these entries of the reconstructed properties.

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        properties_defaults::Maybe{AbstractDict{<:AbstractString, <:StorageScalar}} = nothing

    But what we are **forced** to say is:

        properties_defaults::Maybe{Dict} = nothing

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.

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
    When reconstructing the original property, specify this value using [`IfNot`](@ref) (e.g.,
    `/ cell : type => color ?? magenta`).
"""
@logged function reconstruct_axis!(
    daf::DafWriter;
    existing_axis::AbstractString,
    implicit_axis::AbstractString,
    rename_axis::Maybe{AbstractString} = nothing,
    empty_implicit::Maybe{StorageScalar} = nothing,
    implicit_properties::Maybe{AbstractStringSet} = nothing,
    properties_defaults::Maybe{Dict} = nothing,
)::AbstractDict{<:AbstractString, Maybe{StorageScalar}}
    if rename_axis === nothing
        rename_axis = implicit_axis
    end

    if implicit_properties !== nothing
        @assert !(implicit_axis in implicit_properties)
    end

    implicit_values = get_vector(daf, existing_axis, implicit_axis)
    overwrite_implicit_values =
        !(eltype(implicit_values) <: AbstractString) || (empty_implicit !== nothing && empty_implicit != "")
    if eltype(implicit_values) <: AbstractString && empty_implicit === nothing
        empty_implicit = ""
    end
    unique_values = unique(implicit_values[implicit_values .!= empty_implicit])
    sort!(unique_values)
    if !(eltype(unique_values) <: AbstractString)
        unique_values = [string(unique_value) for unique_value in unique_values]
    end
    if has_axis(daf, rename_axis)
        axis_values = axis_array(daf, rename_axis)
        axis_values_set = Set(axis_values)
        for unique_value in unique_values
            if !(unique_value in axis_values_set)
                error(
                    "missing used entry: $(unique_value)\n" *
                    "from the existing reconstructed axis: $(implicit_axis)\n" *
                    "in the daf data: $(daf.name)",
                )
            end
        end
        unique_values = axis_values
    end

    implicit_values =
        [implicit_value == empty_implicit ? "" : string(implicit_value) for implicit_value in implicit_values]

    value_of_empties_of_properties = Dict{AbstractString, Maybe{StorageScalar}}()
    vector_values_of_properties = Dict{AbstractString, StorageVector}()
    for property in vectors_set(daf, existing_axis)
        is_explicit = implicit_properties !== nothing && property in implicit_properties
        if is_explicit || (implicit_properties === nothing && property != implicit_axis)
            if properties_defaults === nothing
                default_value = nothing
            else
                default_value = get(properties_defaults, property, nothing)
            end
            property_data = collect_property_data(
                daf,
                existing_axis,
                implicit_axis,
                property,
                implicit_values,
                unique_values,
                default_value;
                must_be_consistent = is_explicit,
            )
            if property_data !== nothing
                value_of_empty_of_property, vector_value_of_property = property_data
                value_of_empties_of_properties[property] = value_of_empty_of_property
                vector_values_of_properties[property] = vector_value_of_property
            end
        end
    end

    if !has_axis(daf, rename_axis)
        add_axis!(daf, rename_axis, unique_values)
    end

    if overwrite_implicit_values
        set_vector!(daf, existing_axis, implicit_axis, implicit_values; overwrite = true)
    end

    for (property, vector_value) in vector_values_of_properties
        set_vector!(daf, rename_axis, property, vector_value)
        delete_vector!(daf, existing_axis, property)
    end

    return value_of_empties_of_properties
end

function collect_property_data(
    daf::DafReader,
    existing_axis::AbstractString,
    implicit_axis::AbstractString,
    property::AbstractString,
    implicit_values::AbstractStringVector,
    unique_values::AbstractStringVector,
    default_value::Maybe{StorageScalar};
    must_be_consistent::Bool,
)::Maybe{Tuple{Maybe{StorageScalar}, <:StorageVector}}
    property_values = get_vector(daf, existing_axis, property)
    property_values_of_implicits = Dict{AbstractString, eltype(property_values)}()
    @assert length(property_values) == length(implicit_values)

    for (property_value, implicit_value) in zip(property_values, implicit_values)
        property_value_of_implicit = get(property_values_of_implicits, implicit_value, nothing)
        if property_value_of_implicit === nothing
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

    value_of_empty_of_property = get(property_values_of_implicits, "", nothing)
    vector_value_of_property = [
        value_of_implicit_property(daf, property, property_values_of_implicits, unique_value, default_value) for
        unique_value in unique_values
    ]
    return (value_of_empty_of_property, vector_value_of_property)
end

function value_of_implicit_property(
    daf::DafReader,
    property::AbstractString,
    property_values_of_implicits::Dict{AbstractString, <:StorageScalar},
    unique_value::AbstractString,
    default_value::Maybe{StorageScalar},
)::StorageScalar
    value = get(property_values_of_implicits, unique_value, default_value)
    if value === nothing
        error(
            "no default value specified for the unused entry: $(unique_value)\n" *
            "of the reconstructed property: $(property)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    return value
end

end  # module
