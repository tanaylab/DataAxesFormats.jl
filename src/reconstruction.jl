"""
Reconstruct implicit axes. Due to `AnnData` two-axes limitations, other axes are often represented by storing their
expanded data (e.g., a type for each cell, and a color for each cell, where the color is actually per type). When
converting such data to `Daf`, it is useful to reconstruct such axes (e.g., create a type axis, assign a color for each
type, and delete the per-cell color property).
"""
module Reconstruction

export connect_axes!
export EmptyImplicit
export PropertiesDefaults
export reconstruct_axis!
export unify_empty_vector_values!

using ..Formats
using ..Keys
using ..Queries
using ..Readers
using ..StorageTypes
using ..Writers
using TanayLabUtilities

"""
Map property names to a default value. This can be specified as a dictionary, a vector of pairs, or a named tuple.
"""
PropertiesDefaults = Union{AbstractDict, AbstractVector, NamedTuple}

"""
The value(s) of a property which mean "there is no value". This can be specified as a single value, or as a vector, set
or tuple of them, for data which spells "no value" in more than one way (e.g., both `Outliers` and `Doublet`).
"""
EmptyImplicit = Union{
    StorageScalar,
    AbstractVector{<:StorageScalarBase},
    AbstractSet{<:StorageScalarBase},
    Tuple{Vararg{StorageScalarBase}},
}

# Collect the value(s) meaning "there is no value" into a set, so that one of them and several of them are treated the
# same way. Specifying none of them gives an empty set, which no value is a member of.
function set_of_empty_implicit(empty_implicit::EmptyImplicit)::Set{StorageScalarBase}
    if empty_implicit isa StorageScalar
        return Set{StorageScalarBase}((empty_implicit,))
    else
        return Set{StorageScalarBase}(empty_implicit)
    end
end

"""
    reconstruct_axis!(
        daf::DafWriter;
        existing_axis::AbstractString,
        implicit_axis::AbstractString,
        [rename_axis::Maybe{AbstractString} = nothing,
        implicit_properties::Maybe{AbstractSet{<:AbstractString}} = nothing,
        skipped_properties::Maybe{AbstractSet{<:AbstractString}} = nothing,
        properties_defaults::Maybe{AbstractDict} = nothing]
    )::AbstractDict{<:AbstractString, Maybe{StorageScalar}}

Given an `existing_axis` in `daf`, which has a property `implicit_axis`, create a new axis with the same name as the
property (or, if specified, call it `rename_axis`). An empty string means there is no value associated with that
`existing_axis` entry; data spelling that some other way - `NA`, `Outliers`, a sentinel number - should be passed
through [`unify_empty_vector_values!`](@ref) first, which is where that concept lives. For each of the
`implicit_properties`, we collect the mapping between the `implicit_axis` and the property values, and store it as a
property of the newly created axis.

exist as names of entries in the `implicit_axis`. This allows manually creating the `implicit_axis` with additional
entries that are not currently in use.

If `implicit_properties` are explicitly specified, then we require the mapping from `implicit_axis` to be consistent for
them. Otherwise, we look at all the properties of the `existing_axis`, and check for each one whether the mapping is
consistent; if it is, we migrate the property to the new axis. For example, when importing `AnnData` containing per-cell
data, it isn't always clear which property is actually per-batch (e.g., cell age) and which is actually per cell (e.g.,
doublet score). Not specifying the `implicit_properties` allows the function to figure it out on its own. If
`skipped_properties` are specified, they are skipped, then these properties are skipped even if they happen
(accidentally) to have a consistent mapping with the type.

If the `implicit_axis` already exists, we verify that all the values provided for it by the `existing_axis` do, in fact,

If the reconstructed `implicit_axis` axis already exists, it may contain values that don't exist in the property of the
`existing_axis`. In this case, for each reconstructed property, you should specify an entry in the `properties_defaults`
to use for these values.

!!! note

    For each converted property, the value associated with `existing_axis` entries which have no `implicit_axis` value
    (that is, have an empty string) is lost. For example, if each cell type has a color, but
    some cells do not have a type, then the color of "cells with no type" is lost. We still require this value to be
    consistent, and return a mapping between each migrated property name and the value of such entries (if any exist).
    When reconstructing the original property, specify this value using [`IfNot`](@ref) (e.g.,
    `/ cell : type => color ?? magenta`).
"""
@logged :daf_ops function reconstruct_axis!(
    daf::DafWriter;
    existing_axis::AbstractString,
    implicit_axis::AbstractString,
    rename_axis::Maybe{AbstractString} = nothing,
    implicit_properties::Maybe{AbstractSet{<:AbstractString}} = nothing,
    skipped_properties::Maybe{AbstractSet{<:AbstractString}} = nothing,
    properties_defaults::Maybe{PropertiesDefaults} = nothing,
)::AbstractDict{<:AbstractString, Maybe{StorageScalar}}
    properties_defaults = pairs_as_dict(properties_defaults)

    if rename_axis === nothing
        rename_axis = implicit_axis
    end

    if implicit_properties !== nothing
        @assert !(implicit_axis in implicit_properties)
    end

    if skipped_properties !== nothing
        @assert !(implicit_axis in skipped_properties)  # UNTESTED
    end

    if properties_defaults !== nothing
        for (property, default) in properties_defaults
            @assert property isa AbstractString "invalid property name: $(property)"
            @assert default isa StorageScalar "invalid property default: $(default)"
        end
    end

    implicit_values = get_vector(daf, existing_axis, implicit_axis)
    if !(eltype(implicit_values) <: AbstractString)
        error(chomp("""
            not a property of strings: $(implicit_axis)
            of the axis: $(existing_axis)
            in the daf data: $(daf.name)
            use unify_empty_vector_values! to convert it, saying which of its values mean nothing
            """))
    end

    is_empty_per_value = implicit_values .== ""
    unique_values = unique(implicit_values[.!is_empty_per_value])
    sort!(unique_values)
    if has_axis(daf, rename_axis)
        axis_values = axis_vector(daf, rename_axis)
        axis_values_set = Set(axis_values)
        for unique_value in unique_values
            if !(unique_value in axis_values_set)
                error(chomp("""
                    missing used entry: $(unique_value)
                    from the existing reconstructed axis: $(implicit_axis)
                    in the daf data: $(daf.name)
                    """))
            end
        end
        unique_values = axis_values
    end

    value_of_empties_of_properties = Dict{AbstractString, Maybe{StorageScalar}}()
    vector_values_of_properties = Dict{AbstractString, StorageVector}()
    for property in vectors_set(daf, existing_axis)
        if skipped_properties !== nothing && property in skipped_properties
            continue  # UNTESTED
        end
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

    for (property, vector_value) in vector_values_of_properties
        @debug "reconstruct $(rename_axis) vector: $(property)" _group = :daf_sets
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
    implicit_values::AbstractVector{<:AbstractString},
    unique_values::AbstractVector{<:AbstractString},
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
                error(chomp("""
                    inconsistent values: $(property_value) != $(property_value_of_implicit)
                    of the property: $(property)
                    for the same implicit axis value: $(implicit_value)
                    of the axis: $(existing_axis)
                    for the reconstructed axis: $(implicit_axis)
                    in the daf data: $(daf.name)
                    """))
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
        error(chomp("""
            no default value specified for the unused entry: $(unique_value)
            of the reconstructed property: $(property)
            in the daf data: $(daf.name)
            """))
    end
    return value
end

"""
    unify_empty_vector_values!(
        daf::DafWriter;
        axis::AbstractString,
        property::AbstractString,
        empty_values::EmptyImplicit,
        [dtype::Maybe{Type{<:StorageScalarBase}} = nothing,
        empty_value::Maybe{StorageScalar} = nothing]
    )::Nothing

Replace every one of the `empty_values` of a `property` of an `axis` with a single `empty_value`, so that "there is no
value here" is spelled one way, converting the property to a `dtype` on the way if one is given.

Data arrives spelling it several ways, often several ways in the same property: an empty string in some entries and
`NA` in others, `(Missing)` elsewhere, and for numbers a sentinel such as the smallest integer, which is not obviously
a sentinel at all - it is a number, so a mean or a plot of that property is silently wrong rather than visibly absent.

This matters before [`reconstruct_axis!`](@ref) and [`connect_axes!`](@ref), which decide what to do with an entry by
asking whether its value is empty. A property still saying `NA` would have `NA` reconstructed into an entry of the new
axis, sitting among the real ones.

Numbers often arrive as text for exactly this reason - a column of measurements is a column of strings because a few of
its entries say `NA`. Giving a `dtype` converts the values which are not empty, which is an error unless all of them
are values of that type; the ones which are empty become the `empty_value`, which is why this is one operation and not
two.

By default the `empty_value` is the empty string for strings, `NaN` for floats, and `0` for unsigned integers, which is
the same convention `Daf` uses for module indices - they are 1-based, so `0` is free to mean "none". A signed integer
or a Boolean has no such value, so one must be given, or a `dtype` which has one.

A property none of whose values is empty is left as it is, rather than being an error: which markers a property
carries is a fact about the file, and the same cleanup has to keep working on a file which happens to be clean. What
*is* an error is asking for nothing at all - no `empty_values` and no `dtype` - since that cannot do anything whatever
the data says, and so is a mistake in the call rather than a fact about the file.

The result is `bestify`d, so a property which turns out to be mostly empty is stored sparsely rather than densely.
"""
@logged :daf_ops function unify_empty_vector_values!(
    daf::DafWriter;
    axis::AbstractString,
    property::AbstractString,
    empty_values::EmptyImplicit,
    dtype::Maybe{Type{<:StorageScalarBase}} = nothing,
    empty_value::Maybe{StorageScalar} = nothing,
)::Nothing
    values = get_vector(daf, axis, property).array

    if dtype === nothing
        dtype = eltype(values)
    end

    empty_values_set = set_of_empty_implicit(empty_values)
    if isempty(empty_values_set) && dtype === eltype(values)
        error(chomp("""
            no empty values and no type to convert to
            of the property: $(property)
            of the axis: $(axis)
            in the daf data: $(daf.name)
            """))
    end

    is_empty_per_value = in.(values, Ref(empty_values_set))

    # Only needed if some value actually is empty: a property of a type which has no empty value is fine as long as
    # none of its values is one, which is the common case for a property which merely needs converting.
    if empty_value === nothing && any(is_empty_per_value)
        empty_value = default_empty_value(daf, axis, property, dtype)
    end

    unified_values = [
        is_empty ? dtype(empty_value) : value_as_type(daf, axis, property, value, dtype) for
        (value, is_empty) in zip(values, is_empty_per_value)
    ]
    if eltype(unified_values) <: Real
        unified_values = TanayLabUtilities.MatrixFormats.bestify(unified_values)
    end
    set_vector!(daf, axis, property, unified_values; overwrite = true)

    return nothing
end

# What "there is no value here" is spelled as, for each kind of type which has such a spelling.
function default_empty_value(
    daf::DafWriter,
    axis::AbstractString,
    property::AbstractString,
    dtype::Type{<:StorageScalarBase},
)::StorageScalar
    if dtype <: AbstractString
        return ""
    elseif dtype <: AbstractFloat
        return NaN
    elseif dtype <: Unsigned
        return 0
    else
        error(chomp("""
            no empty value for the type: $(dtype)
            of the property: $(property)
            of the axis: $(axis)
            in the daf data: $(daf.name)
            """))
    end
end

# Convert one value which is not empty, saying which one it was when it is not a value of the type at all.
function value_as_type(
    daf::DafWriter,
    axis::AbstractString,
    property::AbstractString,
    value::StorageScalar,
    dtype::Type{<:StorageScalarBase},
)::StorageScalar
    if value isa dtype
        return value
    end

    if dtype <: AbstractString
        return string(value)
    end

    converted = value isa AbstractString ? tryparse(dtype, value) : dtype(value)
    if converted === nothing
        error(chomp("""
            invalid value: $(value)
            for the type: $(dtype)
            of the property: $(property)
            of the axis: $(axis)
            in the daf data: $(daf.name)
            """))
    end

    return converted
end

"""
    connect_axes!(
        daf::DafWriter;
        base_axis::AbstractString,
        from_axis::AbstractString,
        [from_property::Maybe{AbstractString} = nothing,]
        to_axis::AbstractString,
        [to_property::Maybe{AbstractString} = nothing,
        connect_property::Maybe{AbstractString} = nothing,
        overwrite::Bool = false]
    )::Nothing

Given a `base_axis` with two vector properties, one holding a reference to `from_axis` and one to `to_axis`, create a
property of `from_axis` that references `to_axis`. This is only possible if every entry of `from_axis` is always
associated with a single entry of `to_axis`.

This can happen when one axis (say, "batch") references two other axes (say, "plate" and "tray"). If *every* batch was
placed in one plate and every plate was in a tray, then we'd have batch refers to plate, plate refers to run;
[`reconstruct_axis!`](@ref) would have been enough to deal with it, and batch simply wouldn't have a "tray" property.
This is the more common and more sensible case.

However, if for some reason some batches *do* have a tray reference, but (for whatever reason) do *not* have a plate
reference, we still want to record that "each plate is in a tray", while not giving up on "each batch is in a tray". So
we must duplicate data. We record for each plate which tray it is in using `connect_axes!` - creating a new "tray"
property for the plate axis - while keeping the original tray property per batch.

This is in contrast to `reconstruct_axis!` which does *not* duplicate data - it *moves* the data to its proper place,
removing the original which became redundant.

By default the properties of `base_axis` holding the references are named after the axes they refer to, and the created
`connect_property` of `from_axis` is named after `to_axis`. Specify `from_property`, `to_property` and
`connect_property` when they are not; a base axis may refer to the same axis twice (a "sorted_by" and a "sequenced_by"
run, say), in which case the name of the property is the only thing telling them apart.

An entry of `base_axis` with no `from_axis` reference is skipped, since there is nothing to record it against; its
`to_axis` reference is therefore not examined at all. An entry of `from_axis` which no entry of `base_axis` refers to
is given an empty value.
"""
@logged :daf_ops function connect_axes!(
    daf::DafWriter;
    base_axis::AbstractString,
    from_axis::AbstractString,
    from_property::Maybe{AbstractString} = nothing,
    to_axis::AbstractString,
    to_property::Maybe{AbstractString} = nothing,
    connect_property::Maybe{AbstractString} = nothing,
    overwrite::Bool = false,
)::Nothing
    if from_property === nothing
        from_property = from_axis
    end

    if to_property === nothing
        to_property = to_axis
    end

    if connect_property === nothing
        connect_property = to_axis
    end

    from_per_base = get_vector(daf, base_axis, from_property)
    to_per_base = get_vector(daf, base_axis, to_property)

    from_names_set = Set(axis_vector(daf, from_axis))
    to_names_set = Set(axis_vector(daf, to_axis))

    to_name_per_from_name = Dict{AbstractString, AbstractString}()
    for (from_value, to_value) in zip(from_per_base, to_per_base)
        from_name = string(from_value)
        if from_name == ""
            continue
        end

        # Each message names the property as well as the axis, since the two need not be named the same, and it is the
        # property which has to be looked at to see what is wrong.
        if !(from_name in from_names_set)
            error(chomp("""
                missing entry: $(from_name)
                of the axis: $(from_axis)
                named by the property: $(from_property)
                of the axis: $(base_axis)
                in the daf data: $(daf.name)
                """))
        end

        to_name = string(to_value)
        if to_name != "" && !(to_name in to_names_set)
            error(chomp("""
                missing entry: $(to_name)
                of the axis: $(to_axis)
                named by the property: $(to_property)
                of the axis: $(base_axis)
                in the daf data: $(daf.name)
                """))
        end

        previous_to_name = get(to_name_per_from_name, from_name, nothing)
        if previous_to_name === nothing
            to_name_per_from_name[from_name] = to_name
        elseif previous_to_name != to_name
            # Quoted, unlike the messages above, because here one of the two may be the empty value, and an empty
            # value is a value: "R1 != " reads as though something went missing from the message itself.
            error(chomp("""
                conflicting entries: "$(previous_to_name)" != "$(to_name)"
                of the axis: $(to_axis)
                named by the property: $(to_property)
                of the axis: $(base_axis)
                for the entry: $(from_name)
                of the axis: $(from_axis)
                named by the property: $(from_property)
                in the daf data: $(daf.name)
                """))
        end
    end

    # Nothing is written until everything above has been verified, so rejected data is left as it was.
    to_name_per_from = [get(to_name_per_from_name, from_name, "") for from_name in axis_vector(daf, from_axis)]
    set_vector!(daf, from_axis, connect_property, to_name_per_from; overwrite)

    return nothing
end

end  # module
