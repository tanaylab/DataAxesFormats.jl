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
export empty_dict
export EmptyKey

using Daf.Data
using Daf.Formats
using Daf.StorageTypes
using Daf.Unions
using NamedArrays
using SparseArrays

"""
    function copy_scalar(;
        into::DafWriter,
        from::DafReader,
        name::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
        overwrite::Bool = false]
    )::Nothing

Copy a scalar `from` some `DafReader` `into` some `DafWriter`.

The scalar is fetched using the `name` and the `default`. If `rename` is specified, store the scalar using this new
name. If `overwrite` (not the default), overwrite an existing scalar in the target.
"""
function copy_scalar!(;
    into::DafWriter,
    from::DafReader,
    name::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
    overwrite::Bool = false,
)::Nothing
    value = get_scalar(from, name; default = default)
    if value != nothing
        rename = new_name(rename, name)
        set_scalar!(into, rename, value; overwrite = overwrite)
    end
end

"""
    function copy_axis(;
        into::DafWriter,
        from::DafReader,
        name::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        default::Union{Nothing, UndefInitializer} = undef]
    )::Nothing

Copy an axis `from` some `DafReader` into some `DafWriter`.

The axis is fetched using the `name` and the `default`. If `rename` is specified, store the axis using this name.
"""
function copy_axis!(;
    into::DafWriter,
    from::DafReader,
    name::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    default::Union{Nothing, UndefInitializer} = undef,
)::Nothing
    value = get_axis(from, name; default = default)
    if value != nothing
        rename = new_name(rename, name)
        add_axis!(into, rename, value)
    end
end

"""
    function copy_vector(;
        into::DafWriter,
        from::DafReader,
        axis::AbstractString,
        name::AbstractString,
        [reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        overwrite::Bool = false]
    )::Nothing

Copy a vector `from` some `DafReader` `into` some `DafWriter`.

The vector is fetched using the `axis`, `name` and the `default`. If `reaxis` is specified, store the vector using this
axis. If `rename` is specified, store the vector using this name. If `overwrite` (not the default), overwrite an
existing vector in the target.

This requires the axis of one data set is the same, or is a superset of, or a subset of, the other. If the target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If the
source axis contains entries that do not exist in the target, they are discarded (not copied).
"""
function copy_vector!(;
    into::DafWriter,
    from::DafReader,
    axis::AbstractString,
    name::AbstractString,
    reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageScalar} = nothing,
    overwrite::Bool = false,
    relation::Maybe{Symbol} = nothing,
)::Nothing
    reaxis = new_name(reaxis, axis)
    rename = new_name(rename, name)

    if relation == nothing
        relation = verify_axis(into, reaxis, from, axis; allow_missing = false, allow_from_subset = empty != nothing)
        @assert relation != nothing
    end

    value = get_vector(from, axis, name; default = default)
    if value == nothing
        return nothing
    end

    if relation == :same
        set_vector!(into, reaxis, rename, value; overwrite = overwrite)
        return nothing
    end

    if relation == :into_is_subset
        value = value[get_axis(into, reaxis)]
        set_vector!(into, reaxis, rename, value; overwrite = overwrite)
        return nothing
    end

    @assert relation == :from_is_subset
    verify_subset(from.name, axis, into.name, reaxis, empty != nothing)

    if issparse(value)
        dense = Vector{eltype(value)}(undef, axis_length(into, reaxis))
        named = NamedArray(dense; names = (get_axis(into, reaxis),))
        named .= empty
        named[names(value, 1)] .= value  # NOJET
        sparse = SparseVector(dense)
        set_vector!(into, reaxis, rename, sparse; overwrite = overwrite)
    else
        empty_dense_vector!(into, reaxis, rename, eltype(value); overwrite = overwrite) do empty_vector
            empty_vector .= empty
            return empty_vector[names(value, 1)] .= value
        end
    end

    return nothing
end

"""
    function copy_matrix(;
        into::DafWriter,
        from::DafReader,
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

Copy a matrix `from` some `DafReader` `into` some `DafWriter`.

The matrix is fetched using the `rows_axis`, `columns_axis`, `name`, `relayout` and the `default`. If `rows_reaxis`
and/or `columns_reaxis` are specified, store the vector using these axes. If `rename` is specified, store the matrix
using this name. If `overwrite` (not the default), overwrite an existing matrix in the target. The matrix is stored with
the same `relayout`.

This requires each axis of one data set is the same, or is a superset of, or a subset of, the other. If a target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If a source
axis contains entries that do not exist in the target, they are discarded (not copied).
"""
function copy_matrix!(;
    into::DafWriter,
    from::DafReader,
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
    rows_reaxis = new_name(rows_reaxis, rows_axis)
    columns_reaxis = new_name(columns_reaxis, columns_axis)
    rename = new_name(rename, name)

    if rows_relation == nothing
        rows_relation =
            verify_axis(into, rows_reaxis, from, rows_axis; allow_missing = false, allow_from_subset = empty != nothing)
        @assert rows_relation != nothing
    end
    if columns_relation == nothing
        columns_relation = verify_axis(
            into,
            columns_reaxis,
            from,
            columns_axis;
            allow_missing = false,
            allow_from_subset = empty != nothing,
        )
        @assert columns_relation != nothing
    end

    value = get_matrix(from, rows_axis, columns_axis, name; default = default, relayout = relayout)
    if value == nothing
        return nothing
    end

    if relayout && has_matrix(from, columns_axis, rows_axis, name; relayout = false)
        copy_matrix!(;
            into = into,
            from = from,
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

    if columns_relation == :from_is_subset
        verify_subset(from.name, columns_axis, into.name, columns_reaxis, empty != nothing)
    end

    if rows_relation == :from_is_subset
        verify_subset(from.name, rows_axis, into.name, rows_reaxis, empty != nothing)
    end

    if rows_relation == :same && columns_relation == :same
        set_matrix!(into, rows_reaxis, columns_reaxis, rename, value; overwrite = overwrite, relayout = relayout)
        return nothing
    end

    if (rows_relation == :into_is_subset || rows_relation == :same) &&
       (columns_relation == :into_is_subset || columns_relation == :same)
        value = value[get_axis(into, rows_reaxis), get_axis(into, columns_reaxis)]
        set_matrix!(into, rows_reaxis, columns_reaxis, rename, value; overwrite = overwrite, relayout = relayout)
        return nothing
    end

    @assert rows_relation == :from_is_subset || columns_relation == :from_is_subset

    if issparse(value)
        dense = Matrix{eltype(value)}(undef, axis_length(into, rows_reaxis), axis_length(into, columns_reaxis))
        named = NamedArray(dense; names = (get_axis(into, rows_reaxis), get_axis(into, columns_reaxis)))
        named .= empty
        named[names(value, 1), names(value, 2)] .= value  # NOJET
        sparse = SparseMatrixCSC(dense)
        set_matrix!(into, rows_reaxis, columns_reaxis, rename, sparse; overwrite = overwrite, relayout = relayout)
    else
        empty_dense_matrix!(
            into,
            rows_reaxis,
            columns_reaxis,
            rename,
            eltype(value);
            overwrite = overwrite,
        ) do empty_matrix
            empty_matrix .= empty
            return empty_matrix[names(value, 1), names(value, 2)] .= value
        end
        if relayout
            relayout_matrix!(into, rows_reaxis, columns_reaxis, rename; overwrite = overwrite)  # untested
        end
    end

    return nothing
end

"""
    copy_all!(;
        into::DafWriter,
        from::DafReader
        [empty::Maybe{AbstractDict{Key, Value}} = nothing,
        overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing where {
        Key <: Union{
            Tuple{AbstractString, AbstractString},                  # Key for empty value for vectors.
            Tuple{AbstractString, AbstractString, AbstractString},  # Key for empty value for matrices.
        },
        Value <: StorageScalarBase
    }

Copy all the content of a `DafReader` into a `DafWriter`. If `overwrite`, this will overwrite existing data in the
target. If `relayout`, matrices will be stored in the target both layouts, regardless of how they were stored in the
source.

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
    into::DafWriter,
    from::DafReader,
    empty::Maybe{AbstractDict} = nothing,
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    axis_relations = verify_axes(into, from; allow_from_subset = (empty != nothing))
    copy_scalars(into, from, overwrite)
    copy_axes(into, from)
    copy_vectors(into, from, axis_relations, empty, overwrite)
    copy_matrices(into, from, axis_relations, empty, overwrite, relayout)
    return nothing
end

function verify_axes(into::DafWriter, from::DafReader; allow_from_subset::Bool)::Dict{String, Symbol}
    axis_relations = Dict{String, Symbol}()
    for axis in axis_names(from)
        axis_relations[axis] =
            verify_axis(into, axis, from, axis; allow_missing = true, allow_from_subset = allow_from_subset)
    end
    return axis_relations
end

function verify_axis(
    into_daf::DafWriter,
    into_axis::AbstractString,
    from_daf::DafReader,
    from_axis::AbstractString;
    allow_missing::Bool,
    allow_from_subset::Bool,
)::Maybe{Symbol}
    if allow_missing && !has_axis(into_daf, into_axis)
        return :same
    end

    from_entries = Set(get_axis(from_daf, from_axis))
    into_entries = Set(get_axis(into_daf, into_axis))

    if into_entries == from_entries
        return :same
    end

    if from_entries > into_entries
        return :into_is_subset
    end

    if from_entries < into_entries
        verify_subset(from_daf.name, from_axis, into_daf.name, into_axis, allow_from_subset)
        return :from_is_subset
    end

    return error(
        "disjoint entries in the axis: $(from_axis)\n" *
        "of the source daf data: $(from_daf.name)\n" *
        "and the axis: $(into_axis)\n" *
        "of the target daf data: $(into_daf.name)\n",
    )
end

function verify_subset(
    from_name::AbstractString,
    from_axis::AbstractString,
    into_name::AbstractString,
    into_axis::AbstractString,
    allow_subset::Bool,
)::Nothing
    if !allow_subset
        error(
            "missing entries from the axis: $(from_axis)\n" *
            "of the source daf data: $(from_name)\n" *
            "which are needed for the axis: $(into_axis)\n" *
            "of the target daf data: $(into_name)\n",
        )
    end
end

function copy_scalars(into::DafWriter, from::DafReader, overwrite::Bool)::Nothing
    for name in scalar_names(from)
        copy_scalar!(; into = into, from = from, name = name, overwrite = overwrite)
    end
end

function copy_axes(into::DafWriter, from::DafReader)::Nothing
    for axis in axis_names(from)
        if !has_axis(into, axis)
            copy_axis!(; from = from, into = into, name = axis)
        end
    end
end

function copy_vectors(
    into::DafWriter,
    from::DafReader,
    axis_relations::Dict{String, Symbol},
    empty::Maybe{AbstractDict},
    overwrite::Bool,
)::Nothing
    empty_value = nothing
    for (axis, relation) in axis_relations
        for name in vector_names(from, axis)
            if empty != nothing
                empty_value = get(empty, (axis, name), nothing)
            end
            copy_vector!(;
                into = into,
                from = from,
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
    into::DafWriter,
    from::DafReader,
    axis_relations::Dict{String, Symbol},
    empty::Maybe{AbstractDict},
    overwrite::Bool,
    relayout::Bool,
)::Nothing
    empty_value = nothing
    for (rows_axis, rows_relation) in axis_relations
        for (columns_axis, columns_relation) in axis_relations
            if !relayout || columns_axis >= rows_axis
                for name in matrix_names(from, rows_axis, columns_axis; relayout = relayout)
                    if empty != nothing
                        empty_value = get(empty, (rows_axis, columns_axis, name), nothing)
                        if empty_value == nothing
                            empty_value = get(empty, (columns_axis, rows_axis, name), nothing)
                        end
                    end
                    copy_matrix!(;
                        into = into,
                        from = from,
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
    return rename == nothing ? name : rename
end

end # module
