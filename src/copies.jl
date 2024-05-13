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

using ..Formats
using ..GenericLogging
using ..GenericTypes
using ..Messages
using ..Readers
using ..StorageTypes
using ..Writers
using NamedArrays
using SparseArrays

import ..Readers.as_named_vector
import ..Readers.as_named_matrix

"""
    copy_scalar(;
        destination::DafWriter,
        source::DafReader,
        name::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        dtype::Maybe{Type{T}} = nothing,
        default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
        overwrite::Bool = false]
    )::Nothing where {T <: StorageScalarBase}

Copy a scalar with some `name` from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The scalar is fetched using the `name` and the `default`. If `rename` is specified, store the scalar using this new
name. If `dtype` is specified, the data is converted to this type. If `overwrite` (not the default), overwrite an
existing scalar in the target.
"""
@logged function copy_scalar!(;
    destination::DafWriter,
    source::DafReader,
    name::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    dtype::Maybe{Type{T}} = nothing,
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
    overwrite::Bool = false,
)::Nothing where {T <: StorageScalarBase}
    value = get_scalar(source, name; default = default)
    if value !== nothing
        rename = new_name(rename, name)
        if dtype !== nothing
            concrete_dtype, abstract_dtype = target_types(dtype)
            if !(typeof(value) <: abstract_dtype)
                if concrete_dtype == String
                    value = string(value)
                elseif typeof(value) <: AbstractString
                    value = parse(concrete_dtype, value)
                else
                    value = concrete_dtype(value)
                end
            end
        end
        set_scalar!(destination, rename, value; overwrite = overwrite)
    end
end

"""
    copy_axis(;
        destination::DafWriter,
        source::DafReader,
        axis::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        default::Union{Nothing, UndefInitializer} = undef]
    )::Nothing

Copy an `axis` from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The axis is fetched using the `name` and the `default`. If `rename` is specified, store the axis using this name.
"""
@logged function copy_axis!(;
    destination::DafWriter,
    source::DafReader,
    axis::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    default::Union{Nothing, UndefInitializer} = undef,
)::Nothing
    value = axis_array(source, axis; default = default)
    if value !== nothing
        rename = new_name(rename, axis)
        add_axis!(destination, rename, value)
    end
end

"""
    copy_vector(;
        destination::DafWriter,
        source::DafReader,
        axis::AbstractString,
        name::AbstractString,
        [reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        dtype::Maybe{Type{T}} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        overwrite::Bool = false]
    )::Nothing where {T <: StorageScalarBase}

Copy a vector from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The vector is fetched using the `axis`, `name` and the `default`. If `reaxis` is specified, store the vector using this
axis. If `rename` is specified, store the vector using this name. If `dtype` is specified, the data is converted to this
type. If `overwrite` (not the default), overwrite an existing vector in the target.

This requires the axis of one data set is the same, or is a superset of, or a subset of, the other. If the target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If the
source axis contains entries that do not exist in the target, they are discarded (not copied).
"""
@logged function copy_vector!(;
    destination::DafWriter,
    source::DafReader,
    axis::AbstractString,
    name::AbstractString,
    reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    dtype::Maybe{Type{T}} = nothing,
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageScalar} = nothing,
    overwrite::Bool = false,
    relation::Maybe{Symbol} = nothing,
)::Nothing where {T <: StorageScalarBase}
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

    if relation == :destination_is_subset
        value = value[axis_array(destination, reaxis)]
        relation = :same
    end

    concrete_dtype, abstract_dtype = target_types(dtype === nothing ? eltype(value) : dtype)
    if concrete_dtype <: AbstractString && (issparse(value) || !(eltype(value) <: AbstractString))
        string_vector = [string(element) for element in value]
        value = NamedArray(string_vector, value.dicts, value.dimnames)
    end

    if relation == :same
        if eltype(value) <: abstract_dtype
            set_vector!(destination, reaxis, rename, value; overwrite = overwrite)
        elseif issparse(value.array)
            @assert isbitstype(concrete_dtype)
            empty_sparse_vector!(
                destination,
                reaxis,
                rename,
                concrete_dtype,
                nnz(value.array);
                overwrite = overwrite,
            ) do nzind, nzval
                nzind .= value.array.nzind
                nzval .= value.array.nzval
                return nothing
            end
        else
            @assert isbitstype(concrete_dtype)
            empty_dense_vector!(destination, reaxis, rename, concrete_dtype; overwrite = overwrite) do empty_vector
                empty_vector .= value
                return nothing
            end
        end
        return nothing
    end

    @assert relation == :source_is_subset
    verify_subset(source.name, axis, destination.name, reaxis, what_for)

    if issparse(value.array) || concrete_dtype <: AbstractString
        dense = Vector{concrete_dtype}(undef, axis_length(destination, reaxis))
        named = NamedArray(dense; names = (axis_array(destination, reaxis),))
        named .= empty
        named[names(value, 1)] .= value  # NOJET
        value = issparse(value.array) && !(concrete_dtype <: AbstractString) ? sparse_vector(dense) : dense
        set_vector!(destination, reaxis, rename, value; overwrite = overwrite)
    else
        empty_dense_vector!(destination, reaxis, rename, concrete_dtype; overwrite = overwrite) do empty_vector
            empty_vector .= empty
            named_vector = as_named_vector(destination, axis, empty_vector)
            named_vector[names(value, 1)] .= value
            return nothing
        end
    end

    return nothing
end

"""
    copy_matrix(;
        destination::DafWriter,
        source::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        [rows_reaxis::Maybe{AbstractString} = nothing,
        columns_reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        dtype::Maybe{Type{T}} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        relayout::Bool = true,
        overwrite::Bool = false]
    )::Nothing where {T <: StorageScalarBase}

Copy a matrix from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The matrix is fetched using the `rows_axis`, `columns_axis`, `name`, `relayout` and the `default`. If `rows_reaxis`
and/or `columns_reaxis` are specified, store the vector using these axes. If `rename` is specified, store the matrix
using this name. If `dtype` is specified, the data is converted to this type. If `overwrite` (not the default),
overwrite an existing matrix in the target. The matrix is stored with the same `relayout`.

This requires each axis of one data set is the same, or is a superset of, or a subset of, the other. If a target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If a source
axis contains entries that do not exist in the target, they are discarded (not copied).

!!! note

    When copying a matrix from a subset to a superset, if the `empty` value is zero, then we create a sparse matrix in
    the destination. However, currently we create a temporary dense matrix for this; this is inefficient and should be
    replaced by a more efficient method.
"""
@logged function copy_matrix!(;
    destination::DafWriter,
    source::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    rows_reaxis::Maybe{AbstractString} = nothing,
    columns_reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    dtype::Maybe{Type{T}} = nothing,
    default::Union{StorageNumber, StorageMatrix, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageNumber} = nothing,
    relayout::Bool = true,
    overwrite::Bool = false,
    rows_relation::Maybe{Symbol} = nothing,
    columns_relation::Maybe{Symbol} = nothing,
)::Nothing where {T <: StorageScalarBase}
    relayout = relayout && rows_axis != columns_axis
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

    if (rows_relation == :destination_is_subset || rows_relation == :same) &&
       (columns_relation == :destination_is_subset || columns_relation == :same)
        value = value[axis_array(destination, rows_reaxis), axis_array(destination, columns_reaxis)]
        rows_relation = :same
        columns_relation = :same
    end

    concrete_dtype, _ = target_types(dtype === nothing ? eltype(value) : dtype)
    @assert isbitstype(concrete_dtype)

    if rows_relation == :same && columns_relation == :same
        if eltype(value) == concrete_dtype
            set_matrix!(
                destination,
                rows_reaxis,
                columns_reaxis,
                rename,
                value;
                overwrite = overwrite,
                relayout = relayout,
            )
        else
            empty_dense_matrix!(
                destination,
                rows_reaxis,
                columns_reaxis,
                rename,
                concrete_dtype;
                overwrite = overwrite,
            ) do empty_matrix
                empty_matrix .= value
                return nothing
            end
        end
        return nothing
    end

    @assert rows_relation == :source_is_subset || columns_relation == :source_is_subset

    if issparse(value) || empty == 0
        dense = Matrix{concrete_dtype}(
            undef,
            axis_length(destination, rows_reaxis),
            axis_length(destination, columns_reaxis),
        )
        named =
            NamedArray(dense; names = (axis_array(destination, rows_reaxis), axis_array(destination, columns_reaxis)))
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
            concrete_dtype;
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
Specify the data type to use for overriding properties types in a `Daf` data set. This is a dictionary with an
[`DataKey`](@ref) specifying for which property we specify a value to, and the data type to use.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs when
    initializing this dictionary. That is, what we'd **like** to say is:

        DataTypes = AbstractDict{DataKey, Type{T}} where {T <: StorageScalarBase}

    But what we are **forced** to say is:

        DataTypes = AbstractDict

    That's **not** a mistake. Even
    `DataTypes = AbstractDict{Key, T <: StorageScalarBase} where {Key, T <: StorageScalarBase}` fails to work, as do all
    the (many) possibilities for expressing "this is a dictionary where the key or the value can be one of several
    things" Sigh. Glory to anyone who figures out an incantation that would force the system to perform **any**
    meaningful type inference here.
"""
DataTypes = AbstractDict

"""
    copy_all!(;
        destination::DafWriter,
        source::DafReader
        [empty::Maybe{EmptyData} = nothing,
        dtypes::Maybe{DataTypes} = nothing,
        overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing

Copy all the content of a `source` [`DafReader`](@ref) into a `destination` [`DafWriter`](@ref). If `overwrite`, this
will overwrite existing data in the target. If `relayout`, matrices will be stored in the target both layouts,
regardless of how they were stored in the source.

This will create target axes that exist in only in the source, but will **not** overwrite existing target axes,
regardless of the value of `overwrite`. An axis that exists in the target must be identical to, or be a subset of, the
same axis in the source.

If the source has axes which are a subset of the same axes in the target, then you must specify a dictionary of values
for the `empty` entries that will be created in the target when copying any vector and/or matrix properties. This is
specified using a `(axis, property) => value` entry for specifying an empty value for a vector property and a
`(rows_axis, columns_axis, property) => entry` for specifying an empty value for a matrix property. The order of the
axes for matrix properties doesn't matter (the same empty value is automatically used for both axes orders).

If `dtype` is specified, the copied data of the matching property is converted to the specified data type.
"""
@logged function copy_all!(;
    destination::DafWriter,
    source::DafReader,
    empty::Maybe{EmptyData} = nothing,
    dtypes::Maybe{DataTypes} = nothing,
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    if empty !== nothing
        for (key, value) in empty
            @assert key isa DataKey
            @assert value isa StorageScalar
        end
    end

    if dtypes !== nothing
        for (key, value) in dtypes
            @assert key isa DataKey
            @assert value isa Type
            @assert value <: StorageScalarBase
            @assert value <: AbstractString || isbitstype(value)
        end
    end

    what_for = empty === nothing ? ": data" : nothing
    axis_relations = verify_axes(destination, source; what_for = what_for)
    copy_scalars(destination, source, dtypes, overwrite)
    copy_axes(destination, source)
    copy_vectors(destination, source, axis_relations, empty, dtypes, overwrite)
    copy_matrices(destination, source, axis_relations, empty, dtypes, overwrite, relayout)

    return nothing
end

function verify_axes(
    destination::DafWriter,
    source::DafReader;
    what_for::Maybe{AbstractString},
)::Dict{AbstractString, Symbol}
    axis_relations = Dict{AbstractString, Symbol}()
    for axis in axes_set(source)
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

    source_entries = Set(axis_array(source_daf, source_axis))
    destination_entries = Set(axis_array(destination_daf, destination_axis))

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

function copy_scalars(destination::DafWriter, source::DafReader, dtypes::Maybe{DataTypes}, overwrite::Bool)::Nothing
    for name in scalars_set(source)
        dtype = nothing
        if dtypes !== nothing
            dtype = get(dtypes, name, nothing)
        end
        copy_scalar!(; destination = destination, source = source, name = name, dtype = dtype, overwrite = overwrite)
    end
end

function copy_axes(destination::DafWriter, source::DafReader)::Nothing
    for axis in axes_set(source)
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
    dtypes::Maybe{DataTypes},
    overwrite::Bool,
)::Nothing
    for (axis, relation) in axis_relations
        for name in vectors_set(source, axis)
            empty_value = nothing
            if empty !== nothing
                empty_value = get(empty, (axis, name), nothing)
            end

            dtype = nothing
            if dtypes !== nothing
                dtype = get(dtypes, (axis, name), nothing)
            end

            copy_vector!(;
                destination = destination,
                source = source,
                axis = axis,
                name = name,
                empty = empty_value,
                dtype = dtype,
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
    dtypes::Maybe{DataTypes},
    overwrite::Bool,
    relayout::Bool,
)::Nothing
    for (rows_axis, rows_relation) in axis_relations
        for (columns_axis, columns_relation) in axis_relations
            if !relayout || columns_axis >= rows_axis
                for name in matrices_set(source, rows_axis, columns_axis; relayout = relayout)
                    empty_value = nothing
                    if empty !== nothing
                        empty_value = get(empty, (rows_axis, columns_axis, name), nothing)
                        if empty_value === nothing
                            empty_value = get(empty, (columns_axis, rows_axis, name), nothing)
                        end
                    end

                    dtype = nothing
                    if dtypes !== nothing
                        dtype = get(dtypes, (rows_axis, columns_axis, name), nothing)
                        if dtype === nothing
                            dtype = get(dtypes, (columns_axis, rows_axis, name), nothing)
                        end
                    end

                    copy_matrix!(;
                        destination = destination,
                        source = source,
                        rows_axis = rows_axis,
                        columns_axis = columns_axis,
                        name = name,
                        empty = empty_value,
                        dtype = dtype,
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

function target_types(dtype::Type)::Tuple{Type, Type}
    if dtype <: AbstractString
        return (String, AbstractString)
    else
        @assert isbitstype(dtype)
        return (dtype, dtype)
    end
end

end # module
