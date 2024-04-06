"""
Copy data between `Daf` data sets.

!!! note

    Copying into an in-memory data set does **not** duplicate the data; instead it just shares a reference to it. This
    is fast. In contrast, copying into a disk-based data set (e.g. using HDF5 or simple files) **will** create a
    duplicate of the data on disk. This is slow. However, both directions will not significantly increase the amount of
    memory allocated by the application.
"""
module Copies

export copy_all!
export copy_axis!
export copy_matrix!
export copy_scalar!
export copy_vector!
export EmptyData

using Daf.Formats
using Daf.GenericTypes
using Daf.Messages
using Daf.Readers
using Daf.StorageTypes
using Daf.Writers
using NamedArrays
using SparseArrays

import Daf.Readers.as_named_vector
import Daf.Readers.as_named_matrix

"""
    function copy_scalar(;
        destination::DafWriter,
        source::DafReader,
        name::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
        overwrite::Bool = false]
    )::Nothing

Copy a scalar with some `name` from some `source` `DafReader` into some `destination` `DafWriter`.

The scalar is fetched using the `name` and the `default`. If `rename` is specified, store the scalar using this new
name. If `overwrite` (not the default), overwrite an existing scalar in the target.
"""
function copy_scalar!(;
    destination::DafWriter,
    source::DafReader,
    name::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
    overwrite::Bool = false,
)::Nothing
    @debug "copy_scalar! $(destination.name) <$(overwrite ? "=" : "-") $(source.name) : $(name) || $(describe(default))"
    value = get_scalar(source, name; default = default)
    if value !== nothing
        rename = new_name(rename, name)
        set_scalar!(destination, rename, value; overwrite = overwrite)
    end
end

"""
    function copy_axis(;
        destination::DafWriter,
        source::DafReader,
        axis::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        default::Union{Nothing, UndefInitializer} = undef]
    )::Nothing

Copy an `axis` from some `source` `DafReader` into some `destination` `DafWriter`.

The axis is fetched using the `name` and the `default`. If `rename` is specified, store the axis using this name.
"""
function copy_axis!(;
    destination::DafWriter,
    source::DafReader,
    axis::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    default::Union{Nothing, UndefInitializer} = undef,
)::Nothing
    if rename === nothing
        @debug "copy_axis! $(destination.name) <- $(source.name) : $(axis) || $(describe(default))"
    else
        @debug "copy_axis! $(destination.name) <- $(source.name) : $(rename) <- $(axis) || $(describe(default))"  # untested
    end
    value = get_axis(source, axis; default = default)
    if value !== nothing
        rename = new_name(rename, axis)
        add_axis!(destination, rename, value)
    end
end

"""
    function copy_vector(;
        destination::DafWriter,
        source::DafReader,
        axis::AbstractString,
        name::AbstractString,
        [reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        overwrite::Bool = false]
    )::Nothing

Copy a vector from some `source` `DafReader` into some `destination` `DafWriter`.

The vector is fetched using the `axis`, `name` and the `default`. If `reaxis` is specified, store the vector using this
axis. If `rename` is specified, store the vector using this name. If `overwrite` (not the default), overwrite an
existing vector in the target.

This requires the axis of one data set is the same, or is a superset of, or a subset of, the other. If the target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If the
source axis contains entries that do not exist in the target, they are discarded (not copied).
"""
function copy_vector!(;
    destination::DafWriter,
    source::DafReader,
    axis::AbstractString,
    name::AbstractString,
    reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageScalar} = nothing,
    overwrite::Bool = false,
    relation::Maybe{Symbol} = nothing,
)::Nothing
    if rename === nothing
        @debug "copy_vector! $(destination.name) <$(overwrite ? "-" : "=") $(source.name) / $(axis) : $(name) || $(describe(default))"
    else
        @debug "copy_vector! $(destination.name) <$(overwrite ? "-" : "=") $(source.name) / $(axis) : $(rename) <- $(name) || $(describe(default))"  # untested
    end

    reaxis = new_name(reaxis, axis)
    rename = new_name(rename, name)

    what_for = empty === nothing ? "the vector: $(name)\nto the vector: $(rename)" : nothing
    if relation === nothing
        relation = verify_axis(destination, reaxis, source, axis; allow_missing = false, what_for = what_for)
        @assert relation !== nothing
    end

    value = get_vector(source, axis, name; default = default)
    if value === nothing
        return nothing
    end

    if relation == :same
        set_vector!(destination, reaxis, rename, value; overwrite = overwrite)
        return nothing
    end

    if relation == :destination_is_subset
        value = value[get_axis(destination, reaxis)]
        set_vector!(destination, reaxis, rename, value; overwrite = overwrite)
        return nothing
    end

    @assert relation == :source_is_subset
    verify_subset(source.name, axis, destination.name, reaxis, what_for)

    if issparse(value) || eltype(value) <: AbstractString
        dense = Vector{eltype(value)}(undef, axis_length(destination, reaxis))
        named = NamedArray(dense; names = (get_axis(destination, reaxis),))
        named .= empty
        named[names(value, 1)] .= value  # NOJET
        value = issparse(value) ? sparse_vector(dense) : dense
        set_vector!(destination, reaxis, rename, value; overwrite = overwrite)
    else
        empty_dense_vector!(destination, reaxis, rename, eltype(value); overwrite = overwrite) do empty_vector
            empty_vector .= empty
            named_vector = as_named_vector(destination, axis, empty_vector)
            named_vector[names(value, 1)] .= value
            return nothing
        end
    end

    return nothing
end

"""
    function copy_matrix(;
        destination::DafWriter,
        source::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        [rows_reaxis::Maybe{AbstractString} = nothing,
        columns_reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        relayout::Bool = true,
        overwrite::Bool = false]
    )::Nothing

Copy a matrix from some `source` `DafReader` into some `destination` `DafWriter`.

The matrix is fetched using the `rows_axis`, `columns_axis`, `name`, `relayout` and the `default`. If `rows_reaxis`
and/or `columns_reaxis` are specified, store the vector using these axes. If `rename` is specified, store the matrix
using this name. If `overwrite` (not the default), overwrite an existing matrix in the target. The matrix is stored with
the same `relayout`.

This requires each axis of one data set is the same, or is a superset of, or a subset of, the other. If a target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If a source
axis contains entries that do not exist in the target, they are discarded (not copied).

!!! note

    When copying a matrix from a subset to a superset, if the `empty` value is zero, then we create a sparse matrix in
    the destination. However, currently we create a temporary dense matrix for this; this is inefficient and should be
    replaced by a more efficient method.
"""
function copy_matrix!(;
    destination::DafWriter,
    source::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    rows_reaxis::Maybe{AbstractString} = nothing,
    columns_reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    default::Union{StorageNumber, StorageMatrix, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageNumber} = nothing,
    relayout::Bool = true,
    overwrite::Bool = false,
    rows_relation::Maybe{Symbol} = nothing,
    columns_relation::Maybe{Symbol} = nothing,
)::Nothing
    relayout = relayout && rows_axis != columns_axis
    if rename === nothing
        @debug "copy_matrix! $(destination.name) <$(relayout ? "%" : "#")$(overwrite ? "-" : "=") $(source.name) / $(rows_axis)($(rows_relation)) / $(columns_axis)($(columns_relation)) : $(name) || $(describe(default)) ?? $(describe(empty))"
    else
        @debug "copy_matrix! $(destination.name) <$(relayout ? "%" : "#")$(overwrite ? "-" : "=") $(source.name) / $(rows_axis)($(rows_relation)) / $(columns_axis)($(columns_relation)) : $(rename) <- $(name) || $(describe(default)) ?? $(describe(empty))"
    end

    rows_reaxis = new_name(rows_reaxis, rows_axis)
    columns_reaxis = new_name(columns_reaxis, columns_axis)
    rename = new_name(rename, name)

    what_for = empty === nothing ? "the matrix: $(name)\nto the matrix: $(rename)" : nothing
    if rows_relation === nothing
        rows_relation =
            verify_axis(destination, rows_reaxis, source, rows_axis; allow_missing = false, what_for = what_for)
        @assert rows_relation !== nothing
    end
    if columns_relation === nothing
        columns_relation =
            verify_axis(destination, columns_reaxis, source, columns_axis; allow_missing = false, what_for = what_for)
        @assert columns_relation !== nothing
    end

    value = get_matrix(source, rows_axis, columns_axis, name; default = default, relayout = relayout)
    if value === nothing
        return nothing
    end

    if relayout && has_matrix(source, columns_axis, rows_axis, name; relayout = false)
        copy_matrix!(;
            destination = destination,
            source = source,
            rows_axis = columns_axis,
            columns_axis = rows_axis,
            name = name,
            rows_reaxis = columns_reaxis,
            columns_reaxis = rows_reaxis,
            rename = rename,
            default = nothing,
            relayout = false,
            empty = empty,
            overwrite = overwrite,
            rows_relation = rows_relation,
            columns_relation = columns_relation,
        )
        relayout = false
    end

    if columns_relation == :source_is_subset
        verify_subset(source.name, columns_axis, destination.name, columns_reaxis, what_for)
    end

    if rows_relation == :source_is_subset
        verify_subset(source.name, rows_axis, destination.name, rows_reaxis, what_for)
    end

    if rows_relation == :same && columns_relation == :same
        set_matrix!(destination, rows_reaxis, columns_reaxis, rename, value; overwrite = overwrite, relayout = relayout)
        return nothing
    end

    if (rows_relation == :destination_is_subset || rows_relation == :same) &&
       (columns_relation == :destination_is_subset || columns_relation == :same)
        value = value[get_axis(destination, rows_reaxis), get_axis(destination, columns_reaxis)]
        set_matrix!(destination, rows_reaxis, columns_reaxis, rename, value; overwrite = overwrite, relayout = relayout)
        return nothing
    end

    @assert rows_relation == :source_is_subset || columns_relation == :source_is_subset

    if issparse(value) || empty == 0
        dense = Matrix{eltype(value)}(
            undef,
            axis_length(destination, rows_reaxis),
            axis_length(destination, columns_reaxis),
        )
        named = NamedArray(dense; names = (get_axis(destination, rows_reaxis), get_axis(destination, columns_reaxis)))
        named .= empty
        named[names(value, 1), names(value, 2)] .= value  # NOJET
        sparse = sparse_matrix_csc(dense)
        set_matrix!(
            destination,
            rows_reaxis,
            columns_reaxis,
            rename,
            sparse;
            overwrite = overwrite,
            relayout = relayout,
        )
    else
        empty_dense_matrix!(
            destination,
            rows_reaxis,
            columns_reaxis,
            rename,
            eltype(value);
            overwrite = overwrite,
        ) do empty_matrix
            empty_matrix .= empty
            named_matrix = as_named_matrix(destination, rows_axis, columns_axis, empty_matrix)
            named_matrix[names(value, 1), names(value, 2)] .= value
            return nothing
        end
        if relayout
            relayout_matrix!(destination, rows_reaxis, columns_reaxis, rename; overwrite = overwrite)  # untested
        end
    end

    return nothing
end

"""
Specify the data to use for missing properties in a `Daf` data set. This is a dictionary with an [`DataKey`](@ref)
specifying for which property we specify a value to, and the value to use.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs when
    initializing this dictionary. That is, what we'd **like** to say is:

        EmptyData = AbstractDict{DataKey, StorageScalar}

    But what we are **forced** to say is:

        EmptyData = AbstractDict

    That's **not** a mistake. Even `EmptyData = AbstractDict{Key, StorageScalar} where {Key}` fails to work, as do all
    the (many) possibilities for expressing "this is a dictionary where the key or the value can be one of several
    things" Sigh. Glory to anyone who figures out an incantation that would force the system to perform **any**
    meaningful type inference here.
"""
EmptyData = AbstractDict

"""
    copy_all!(;
        destination::DafWriter,
        source::DafReader
        [empty::Maybe{EmptyData} = nothing,
        overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing

Copy all the content of a `source` `DafReader` into a `destination` `DafWriter`. If `overwrite`, this will overwrite
existing data in the target. If `relayout`, matrices will be stored in the target both layouts, regardless of how they
were stored in the source.

This will create target axes that exist in only in the source, but will **not** overwrite existing target axes,
regardless of the value of `overwrite`. An axis that exists in the target must be identical to, or be a subset of, the
same axis in the source.

If the source has axes which are a subset of the same axes in the target, then you must specify a dictionary of values
for the `empty` entries that will be created in the target when copying any vector and/or matrix properties. This is
specified using a `(axis, property) => value` entry for specifying an empty value for a vector property and a
`(rows_axis, columns_axis, property) => entry` for specifying an empty value for a matrix property. The order of the
axes for matrix properties doesn't matter (the same empty value is automatically used for both axes orders).
"""
function copy_all!(;
    destination::DafWriter,
    source::DafReader,
    empty::Maybe{EmptyData} = nothing,
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    if empty !== nothing
        for (key, value) in empty
            @assert key isa DataKey
            @assert value isa StorageScalar
        end
    end

    what_for = empty === nothing ? ": data" : nothing
    axis_relations = verify_axes(destination, source; what_for = what_for)
    copy_scalars(destination, source, overwrite)
    copy_axes(destination, source)
    copy_vectors(destination, source, axis_relations, empty, overwrite)
    copy_matrices(destination, source, axis_relations, empty, overwrite, relayout)

    return nothing
end

function verify_axes(
    destination::DafWriter,
    source::DafReader;
    what_for::Maybe{AbstractString},
)::Dict{AbstractString, Symbol}
    axis_relations = Dict{AbstractString, Symbol}()
    for axis in axis_names(source)
        axis_relations[axis] = verify_axis(destination, axis, source, axis; allow_missing = true, what_for = what_for)
    end
    return axis_relations
end

function verify_axis(
    destination_daf::DafWriter,
    destination_axis::AbstractString,
    source_daf::DafReader,
    source_axis::AbstractString;
    allow_missing::Bool,
    what_for::Maybe{AbstractString},
)::Maybe{Symbol}
    if allow_missing && !has_axis(destination_daf, destination_axis)
        return :same
    end

    source_entries = Set(get_axis(source_daf, source_axis))
    destination_entries = Set(get_axis(destination_daf, destination_axis))

    if destination_entries == source_entries
        return :same
    end

    if source_entries > destination_entries
        return :destination_is_subset
    end

    if source_entries < destination_entries
        verify_subset(source_daf.name, source_axis, destination_daf.name, destination_axis, what_for)
        return :source_is_subset
    end

    return error(
        "disjoint entries in the axis: $(source_axis)\n" *
        "of the source daf data: $(source_daf.name)\n" *
        "and the axis: $(destination_axis)\n" *
        "of the target daf data: $(destination_daf.name)\n",
    )
end

function verify_subset(
    source_name::AbstractString,
    source_axis::AbstractString,
    destination_name::AbstractString,
    destination_axis::AbstractString,
    what_for::Maybe{AbstractString},
)::Nothing
    if what_for !== nothing
        error(
            "missing entries in the axis: $(source_axis)\n" *
            "of the source daf data: $(source_name)\n" *
            "which are needed for copying $(what_for)\n" *
            "of the axis: $(destination_axis)\n" *
            "of the target daf data: $(destination_name)\n",
        )
    end
end

function copy_scalars(destination::DafWriter, source::DafReader, overwrite::Bool)::Nothing
    for name in scalar_names(source)
        copy_scalar!(; destination = destination, source = source, name = name, overwrite = overwrite)
    end
end

function copy_axes(destination::DafWriter, source::DafReader)::Nothing
    for axis in axis_names(source)
        if !has_axis(destination, axis)
            copy_axis!(; source = source, destination = destination, axis = axis)
        end
    end
end

function copy_vectors(
    destination::DafWriter,
    source::DafReader,
    axis_relations::Dict{AbstractString, Symbol},
    empty::Maybe{EmptyData},
    overwrite::Bool,
)::Nothing
    empty_value = nothing
    for (axis, relation) in axis_relations
        for name in vector_names(source, axis)
            if empty !== nothing
                empty_value = get(empty, (axis, name), nothing)
            end
            copy_vector!(;
                destination = destination,
                source = source,
                axis = axis,
                name = name,
                empty = empty_value,
                overwrite = overwrite,
                relation = relation,
            )
        end
    end
end

function copy_matrices(
    destination::DafWriter,
    source::DafReader,
    axis_relations::Dict{AbstractString, Symbol},
    empty::Maybe{EmptyData},
    overwrite::Bool,
    relayout::Bool,
)::Nothing
    empty_value = nothing
    for (rows_axis, rows_relation) in axis_relations
        for (columns_axis, columns_relation) in axis_relations
            if !relayout || columns_axis >= rows_axis
                for name in matrix_names(source, rows_axis, columns_axis; relayout = relayout)
                    if empty !== nothing
                        empty_value = get(empty, (rows_axis, columns_axis, name), nothing)
                        if empty_value === nothing
                            empty_value = get(empty, (columns_axis, rows_axis, name), nothing)
                        end
                    end
                    copy_matrix!(;
                        destination = destination,
                        source = source,
                        rows_axis = rows_axis,
                        columns_axis = columns_axis,
                        name = name,
                        empty = empty_value,
                        overwrite = overwrite,
                        rows_relation = rows_relation,
                        columns_relation = columns_relation,
                    )
                end
            end
        end
    end
end

function new_name(rename::Maybe{AbstractString}, name::AbstractString)::AbstractString
    return rename === nothing ? name : rename
end

end # module
