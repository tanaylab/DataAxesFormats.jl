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
export copy_tensor!
export copy_vector!
export EmptyData

using ..Documentation
using ..Formats
using ..GenericFunctions
using ..GenericLogging
using ..GenericTypes
using ..Keys
using ..MatrixLayouts
using ..Messages
using ..Readers
using ..StorageTypes
using ..Writers
using NamedArrays
using SparseArrays

import ..Formats.as_named_matrix
import ..Formats.as_named_vector
import ..Formats.with_data_write_lock
import ..MatrixLayouts.colptr
import ..MatrixLayouts.nzind
import ..MatrixLayouts.nzval
import ..MatrixLayouts.rowval

"""
    copy_scalar(;
        destination::DafWriter,
        source::DafReader,
        name::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        type::Maybe{Type{<:StorageScalarBase}} = nothing,
        default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
        overwrite::Bool = false,
        insist::Bool = true]
    )::Nothing

Copy a scalar with some `name` from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The scalar is fetched using the `name` and the `default`. If `rename` is specified, store the scalar using this new
name. If `type` is specified, the data is converted to this type. If the scalar already exists in the target, if
`overwrite`, it will be replaced; otherwise, if not `insist`, skip the copy; otherwise, fail.
"""
@logged function copy_scalar!(;
    destination::DafWriter,
    source::DafReader,
    name::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    type::Maybe{Type{<:StorageScalarBase}} = nothing,
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
    overwrite::Bool = false,
    insist::Bool = true,
)::Nothing
    rename = new_name(rename, name)

    if !overwrite && !insist && has_scalar(destination, rename)
        return nothing  # UNTESTED
    end

    value = get_scalar(source, name; default)
    if value !== nothing
        if type !== nothing
            concrete_type, abstract_type = target_types(type)
            if !(typeof(value) <: abstract_type)
                if concrete_type == String
                    value = string(value)
                elseif typeof(value) <: AbstractString
                    value = parse(concrete_type, value)
                else
                    value = concrete_type(value)
                end
            end
        end
        set_scalar!(destination, rename, value; overwrite)
    end
end

"""
    copy_axis(;
        destination::DafWriter,
        source::DafReader,
        axis::AbstractString,
        [rename::Maybe{AbstractString} = nothing,
        default::Union{Nothing, UndefInitializer} = undef,
        overwrite::Bool = false,
        insist::Bool = true]
    )::Nothing

Copy an `axis` from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The axis is fetched using the `name` and the `default`. If `rename` is specified, store the axis using this name.

If the axis already exists in the target, if `overwrite`, it will be replaced (erasing all data for that axis);
otherwise, if not `insist`, skip the copy; otherwise, fail.
"""
@logged function copy_axis!(;
    destination::DafWriter,
    source::DafReader,
    axis::AbstractString,
    rename::Maybe{AbstractString} = nothing,
    default::Union{Nothing, UndefInitializer} = undef,
    overwrite::Bool = false,
    insist::Bool = true,
)::Nothing
    rename = new_name(rename, axis)

    if has_axis(destination, rename)
        if overwrite  # UNTESTED
            delete_axis!(destination, rename)  # UNTESTED
        elseif !insist  # UNTESTED
            return nothing  # UNTESTED
        end
    end

    value = axis_vector(source, axis; default)
    if value !== nothing
        add_axis!(destination, rename, value)
    end

    return nothing
end

"""
    copy_vector!(;
        destination::DafWriter,
        source::DafReader,
        axis::AbstractString,
        name::AbstractString,
        [reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        type::Maybe{Type{<:StorageScalarBase}} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        bestify::Bool = false,
        sparse_if_saves_storage_fraction::AbstractFloat = $(DEFAULT.sparse_if_saves_storage_fraction),
        overwrite::Bool = false,
        insist::Bool = true]
    )::Nothing

Copy a vector from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The vector is fetched using the `axis`, `name` and the `default`. If `reaxis` is specified, store the vector using this
axis. If `rename` is specified, store the vector using this name. If `type` is specified, the data is converted to this
type. If the vector already exists in the target, if `overwrite`, it will be replaced; otherwise, if not `insist`, skip
the copy; otherwise, fail.

If `bestify` is set, then [`bestify`](@ref) the data before writing it, using `sparse_if_saves_storage_fraction`.

This requires the axis of one data set is the same, or is a superset of, or a subset of, the other. If the target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If the
source axis contains entries that do not exist in the target, they are discarded (not copied).
"""
@logged @documented function copy_vector!(;
    destination::DafWriter,
    source::DafReader,
    axis::AbstractString,
    name::AbstractString,
    reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    eltype::Maybe{Type{<:StorageScalarBase}} = nothing,
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageScalar} = nothing,
    bestify::Bool = false,
    sparse_if_saves_storage_fraction::AbstractFloat = function_default(
        MatrixLayouts.bestify,
        :sparse_if_saves_storage_fraction,
    ),
    overwrite::Bool = false,
    insist::Bool = true,
    relation::Maybe{Symbol} = nothing,
)::Nothing
    reaxis = new_name(reaxis, axis)
    rename = new_name(rename, name)

    if !overwrite && !insist && has_vector(destination, reaxis, rename)
        return nothing  # UNTESTED
    end

    what_for = empty === nothing ? "the vector: $(name)\n    to the vector: $(rename)" : nothing
    if relation === nothing
        relation = verify_axis(;
            destination,
            destination_axis = reaxis,
            source,
            source_axis = axis,
            allow_missing = false,
            what_for,
        )
        @assert relation !== nothing
    end

    value = get_vector(source, axis, name; default)
    if value === nothing
        return nothing
    end

    if relation == :destination_is_subset
        value = value[axis_vector(destination, reaxis)]
        relation = :same
    end

    concrete_eltype, abstract_eltype = target_types(eltype === nothing ? Base.eltype(value) : eltype)
    if concrete_eltype <: AbstractString && (issparse(value) || !(Base.eltype(value) <: AbstractString))
        string_vector = [string(element) for element in value]
        value = NamedArray(string_vector, value.dicts, value.dimnames)
    end

    if !overwrite && !insist && has_vector(destination, reaxis, rename)
        return nothing  # UNTESTED
    end

    if bestify && Base.eltype(value) <: Real
        value = MatrixLayouts.bestify(value; sparse_if_saves_storage_fraction, eltype)  # UNTESTED
    end

    if relation == :same
        if Base.eltype(value) <: abstract_eltype
            set_vector!(destination, reaxis, rename, value; overwrite)
        elseif issparse(value.array)
            @assert isbitstype(concrete_eltype) "not a bits type: $(concrete_eltype)"
            empty_sparse_vector!(
                destination,
                reaxis,
                rename,
                concrete_eltype,
                nnz(value.array);
                overwrite,
            ) do sparse_nzind, sparse_nzval
                sparse_nzind .= nzind(value.array)
                sparse_nzval .= nzval(value.array)
                return nothing
            end
        else
            @assert isbitstype(concrete_eltype) "not a bits type: $(concrete_eltype)"
            empty_dense_vector!(destination, reaxis, rename, concrete_eltype; overwrite) do empty_vector
                empty_vector .= value
                return nothing
            end
        end
        return nothing
    end

    @assert relation == :source_is_subset
    verify_subset(;
        source_name = source.name,
        source_axis = axis,
        destination_name = destination.name,
        destination_axis = reaxis,
        what_for,
    )

    with_data_write_lock(destination) do
        if issparse(value.array) || concrete_eltype <: AbstractString
            dense = Vector{concrete_eltype}(undef, axis_length(destination, reaxis))
            named = as_named_vector(destination, reaxis, dense)
            named .= empty
            named[names(value, 1)] .= value  # NOJET
            value = issparse(value.array) && !(concrete_eltype <: AbstractString) ? sparse_vector(dense) : dense
            set_vector!(destination, reaxis, rename, value; overwrite)
        else
            empty_dense_vector!(destination, reaxis, rename, concrete_eltype; overwrite) do empty_vector
                empty_vector .= empty
                named_vector = Formats.as_named_vector(destination, axis, empty_vector)
                named_vector[names(value, 1)] .= value
                return nothing
            end
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
        eltype::Maybe{Type{<:StorageScalarBase}} = nothing,
        default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
        empty::Maybe{StorageScalar} = nothing,
        bestify::Bool = false,
        sparse_if_saves_storage_fraction::AbstractFloat = $(DEFAULT.sparse_if_saves_storage_fraction),
        relayout::Bool = true,
        overwrite::Bool = false,
        insist::Bool = true]
    )::Nothing

Copy a matrix from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

The matrix is fetched using the `rows_axis`, `columns_axis`, `name`, `relayout` and the `default`. If `rows_reaxis`
and/or `columns_reaxis` are specified, store the vector using these axes. If `rename` is specified, store the matrix
using this name. If `eltype` is specified, the data is converted to this type. If the matrix already exists in the
target, if `overwrite`, it will be replaced; otherwise, if not `insist`, skip the copy; otherwise, fail.

If `bestify` is set, then [`bestify`](@ref) the data before writing it, using `sparse_if_saves_storage_fraction`.

This requires each axis of one data set is the same, or is a superset of, or a subset of, the other. If a target axis
contains entries that do not exist in the source, then `empty` must be specified to fill the missing values. If a source
axis contains entries that do not exist in the target, they are discarded (not copied).

!!! note

    When copying a matrix from a subset to a superset, if the `empty` value is zero, then we create a sparse matrix in
    the destination. However, currently we create a temporary dense matrix for this; this is inefficient and should be
    replaced by a more efficient method.
"""
@logged @documented function copy_matrix!(;
    destination::DafWriter,
    source::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    rows_reaxis::Maybe{AbstractString} = nothing,
    columns_reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    eltype::Maybe{Type{<:StorageScalarBase}} = nothing,
    default::Union{StorageReal, StorageMatrix, Nothing, UndefInitializer} = undef,
    empty::Maybe{StorageReal} = nothing,
    bestify::Bool = false,
    sparse_if_saves_storage_fraction::AbstractFloat = function_default(
        MatrixLayouts.bestify,
        :sparse_if_saves_storage_fraction,
    ),
    relayout::Bool = true,
    overwrite::Bool = false,
    insist::Bool = true,
    rows_relation::Maybe{Symbol} = nothing,
    columns_relation::Maybe{Symbol} = nothing,
)::Nothing
    relayout = relayout && rows_axis != columns_axis
    rows_reaxis = new_name(rows_reaxis, rows_axis)
    columns_reaxis = new_name(columns_reaxis, columns_axis)
    rename = new_name(rename, name)

    if !overwrite && !insist && has_matrix(destination, rows_reaxis, columns_reaxis, rename; relayout)
        return nothing  # UNTESTED
    end

    what_for = empty === nothing ? "the matrix: $(name)\n    to the matrix: $(rename)" : nothing
    if rows_relation === nothing
        rows_relation = verify_axis(;
            destination,
            destination_axis = rows_reaxis,
            source,
            source_axis = rows_axis,
            allow_missing = false,
            what_for,
        )
        @assert rows_relation !== nothing
    end
    if columns_relation === nothing
        columns_relation = verify_axis(;
            destination,
            destination_axis = columns_reaxis,
            source,
            source_axis = columns_axis,
            allow_missing = false,
            what_for,
        )
        @assert columns_relation !== nothing
    end

    value = get_matrix(source, rows_axis, columns_axis, name; default, relayout)
    if value === nothing
        return nothing
    end

    if bestify
        value = MatrixLayouts.bestify(value; sparse_if_saves_storage_fraction, eltype)  # UNTESTED
    end

    if relayout && has_matrix(source, columns_axis, rows_axis, name; relayout = false)
        copy_matrix!(;
            destination,
            source,
            rows_axis = columns_axis,
            columns_axis = rows_axis,
            name,
            rows_reaxis = columns_reaxis,
            columns_reaxis = rows_reaxis,
            rename,
            default = nothing,
            relayout = false,
            empty,
            overwrite,
            rows_relation = columns_relation,
            columns_relation = rows_relation,
        )
        relayout = false
    end

    if columns_relation == :source_is_subset
        verify_subset(;
            source_name = source.name,
            source_axis = columns_axis,
            destination_name = destination.name,
            destination_axis = columns_reaxis,
            what_for,
        )
    end

    if rows_relation == :source_is_subset
        verify_subset(;
            source_name = source.name,
            source_axis = rows_axis,
            destination_name = destination.name,
            destination_axis = rows_reaxis,
            what_for,
        )
    end

    if (rows_relation == :destination_is_subset || rows_relation == :same) &&
       (columns_relation == :destination_is_subset || columns_relation == :same) &&
       (rows_relation == :destination_is_subset || columns_relation == :destination_is_subset)
        value = value[axis_vector(destination, rows_reaxis), axis_vector(destination, columns_reaxis)]
        rows_relation = :same
        columns_relation = :same
    end

    concrete_eltype, _ = target_types(eltype === nothing ? Base.eltype(value) : eltype)
    @assert isbitstype(concrete_eltype) "not a bits type: $(concrete_eltype)"

    if rows_relation == :same && columns_relation == :same
        if Base.eltype(value) == concrete_eltype
            set_matrix!(destination, rows_reaxis, columns_reaxis, rename, value; overwrite, relayout)
        else
            if issparse(value)
                empty_sparse_matrix!(
                    destination,
                    rows_reaxis,
                    columns_reaxis,
                    rename,
                    concrete_eltype,
                    nnz(value);
                    overwrite,
                ) do sparse_colptr, sparse_rowval, sparse_nzval
                    sparse_colptr .= colptr(value)
                    sparse_rowval .= rowval(value)
                    sparse_nzval .= nzval(value)
                    return nothing
                end
            else
                empty_dense_matrix!(
                    destination,
                    rows_reaxis,
                    columns_reaxis,
                    rename,
                    concrete_eltype;
                    overwrite,
                ) do empty_matrix
                    empty_matrix .= value
                    return nothing
                end
            end
            if relayout
                relayout_matrix!(destination, rows_reaxis, columns_reaxis, rename; overwrite)  # UNTESTED
            end
        end
        return nothing
    end

    @assert rows_relation == :source_is_subset || columns_relation == :source_is_subset

    with_data_write_lock(destination) do
        if issparse(value) || empty == 0
            dense = Matrix{concrete_eltype}(
                undef,
                axis_length(destination, rows_reaxis),
                axis_length(destination, columns_reaxis),
            )
            named = as_named_matrix(destination, rows_reaxis, columns_reaxis, dense)
            named .= empty
            named[names(value, 1), names(value, 2)] .= value  # NOJET
            sparse = sparse_matrix_csc(dense)
            set_matrix!(destination, rows_reaxis, columns_reaxis, rename, sparse; overwrite, relayout)
        else
            empty_dense_matrix!(
                destination,
                rows_reaxis,
                columns_reaxis,
                rename,
                concrete_eltype;
                overwrite,
            ) do empty_matrix
                empty_matrix .= empty
                named_matrix = Formats.as_named_matrix(destination, rows_axis, columns_axis, empty_matrix)
                named_matrix[names(value, 1), names(value, 2)] .= value
                return nothing
            end
            if relayout
                relayout_matrix!(destination, rows_reaxis, columns_reaxis, rename; overwrite)  # UNTESTED
            end
        end
    end

    return nothing
end

"""
    copy_tensor(;
        destination::DafWriter,
        source::DafReader,
        main_axis::AbstractString,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        [rows_reaxis::Maybe{AbstractString} = nothing,
        columns_reaxis::Maybe{AbstractString} = nothing,
        rename::Maybe{AbstractString} = nothing,
        eltype::Maybe{Type{<:StorageScalarBase}} = nothing,
        empty::Maybe{StorageScalar} = nothing,
        bestify::Bool = false,
        sparse_if_saves_storage_fraction::AbstractFloat = $(DEFAULT.sparse_if_saves_storage_fraction),
        relayout::Bool = true,
        overwrite::Bool = false,
        insist::Bool = true]
    )::Nothing

Copy a tensor from some `source` [`DafReader`](@ref) into some `destination` [`DafWriter`](@ref).

If `bestify` is set, then [`bestify`](@ref) the data before writing it, using `sparse_if_saves_storage_fraction`.

This is basically a loop that calls [`copy_matrix!`](@ref) for each of the tensor matrices, based on the entries of the
`main_axis` in the `destination`. This will create an matrix full of the `empty` value for any entries of the main axis
which exist in the destination but do not exist in the source. If a tensor matrix already exists in the target, if
`overwrite`, it will be replaced; otherwise, if not `insist`, skip the copy; otherwise, fail.
"""
@logged @documented function copy_tensor!(;
    destination::DafWriter,
    source::DafReader,
    main_axis::AbstractString,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    rows_reaxis::Maybe{AbstractString} = nothing,
    columns_reaxis::Maybe{AbstractString} = nothing,
    rename::Maybe{AbstractString} = nothing,
    eltype::Maybe{Type{<:StorageScalarBase}} = nothing,
    empty::Maybe{StorageReal} = nothing,
    bestify::Bool = false,
    sparse_if_saves_storage_fraction::AbstractFloat = function_default(
        MatrixLayouts.bestify,
        :sparse_if_saves_storage_fraction,
    ),
    relayout::Bool = true,
    overwrite::Bool = false,
    insist::Bool = true,
    rows_relation::Maybe{Symbol} = nothing,
    columns_relation::Maybe{Symbol} = nothing,
)::Nothing
    entries = axis_vector(destination, main_axis)
    for entry in entries
        copy_matrix!(;
            destination,
            source,
            rows_axis,
            columns_axis,
            name = "$(entry)_$(name)",
            rows_reaxis,
            columns_reaxis,
            rename = rename === nothing ? nothing : "$(entry)_$(rename)",
            eltype,
            default = empty,
            empty,
            bestify,
            sparse_if_saves_storage_fraction,
            relayout,
            overwrite,
            insist,
            rows_relation,
            columns_relation,
        )
    end
end

"""
Specify the data to use for missing properties in a `Daf` data set. This is a dictionary with an [`DataKey`](@ref)
specifying for which property we spec,aify a value to, and the value to use. We would have liked to specify this as
`AbstractDict{<:DataKey, <:StorageScalarBase}` but Julia in its infinite wisdom considers
`Dict(["a" => "b", ("c", "d") => 1])` to be a `Dict{Any, Any}`, which would require literals to be annotated with the
type.

!!! note

    A [`TensorKey`](@ref) is interpreted as if it as the set of [`MatrixKey`](@ref)s that are included in the tensor.
    These are expanded in an internal copy of the dictionary and will override any other specified [`MatrixKey`](@ref).
"""
EmptyData = AbstractDict

"""
Specify the data type to use for overriding properties types in a `Daf` data set. This is a dictionary with an
[`DataKey`](@ref) specifying for which property we specify a value to, and the data type to use.
We would have liked to specify this as
`AbstractDict{<:DataKey, Type{<:StorageScalarBase}}` but Julia in its infinite wisdom considers
`Dict(["a" => Bool, ("c", "d") => Int32])` to be a `Dict{Any, DataType}`, which would require literals to be annotated
with the type.

!!! note

    A [`TensorKey`](@ref) is interpreted as if it as the set of [`MatrixKey`](@ref)s that are included in the tensor.
    These are expanded in an internal copy of the dictionary and will override any other specified [`MatrixKey`](@ref).
"""
DataTypes = AbstractDict

"""
    copy_all!(;
        destination::DafWriter,
        source::DafReader
        [empty::Maybe{EmptyData} = nothing,
        types::Maybe{DataTypes} = nothing,
        overwrite::Bool = false,
        insist::Bool = true,
        relayout::Bool = true]
    )::Nothing

Copy all the content of a `source` [`DafReader`](@ref) into a `destination` [`DafWriter`](@ref). If some data already
exists in the target, if `overwrite`, it will be replaced; otherwise, if not `insist`, skip the copy; otherwise, fail.

This will create target axes that exist in only in the source, but will **not** overwrite existing target axes,
regardless of the value of `overwrite`. An axis that exists in the target must be identical to, or be a subset of, the
same axis in the source.

If the source has axes which are a subset of the same axes in the target, then you must specify a dictionary of values
for the `empty` entries that will be created in the target when copying any vector and/or matrix properties. This is
specified using a `(axis, property) => value` entry for specifying an `empty` value for a vector property and a
`(rows_axis, columns_axis, property) => entry` for specifying an `empty` value for a matrix property. The order of the
axes for matrix properties doesn't matter (the same `empty` value is automatically used for both axes orders).

If `types` are specified, the copied data of the matching property is converted to the specified data type.

If a [`TensorKey`](@ref) is specified, this will create an matrix full of the `empty` value for any entries of the main
axis which exist in the destination but do not exist in the source.
"""
@logged function copy_all!(;
    destination::DafWriter,
    source::DafReader,
    empty::Maybe{EmptyData} = nothing,
    types::Maybe{DataTypes} = nothing,
    overwrite::Bool = false,
    insist::Bool = true,
    relayout::Bool = true,
)::Nothing
    tensor_keys = Vector{TensorKey}()
    empty = expand_tensors(; dafs = DafReader[destination, source], data = empty, tensor_keys, what_for = "empty")
    types = expand_tensors(; dafs = DafReader[destination, source], data = types, tensor_keys, what_for = "types")

    if empty !== nothing
        for (key, value) in empty
            @assert key isa DataKey "invalid empty DataKey: $(key)"
            @assert value isa StorageScalar "invalid empty StorageScalar: $(value)"
        end
    end

    if types !== nothing
        for (key, value) in types
            @assert key isa DataKey "invalid types DataKey: $(key)"
            @assert value isa Type "invalid types Type: $(value)"
            @assert value <: StorageScalarBase "not a StorageScalarBase: $(value)"
            @assert value <: AbstractString || isbitstype(value) "not a storable type: $(value)"
        end
    end

    what_for = empty === nothing ? ": data" : nothing
    axis_relations = verify_axes(; destination, source, what_for)
    copy_scalars(; destination, source, types, overwrite, insist)
    copy_axes(; destination, source, overwrite, insist)
    copy_vectors(; destination, source, axis_relations, empty, types, overwrite, insist)
    copy_matrices(; destination, source, axis_relations, empty, types, overwrite, insist, relayout)
    ensure_tensors(; destination, source, axis_relations, empty, types, overwrite, insist, relayout, tensor_keys)

    return nothing
end

function expand_tensors(;
    dafs::AbstractVector{DafReader},
    data::Maybe{AbstractDict},
    tensor_keys::AbstractVector{TensorKey},
    what_for::AbstractString,
)::Maybe{EmptyData}
    if data === nothing
        return nothing
    end

    first_tensor_key_index = length(tensor_keys)
    for (key, value) in data
        @assert key isa DataKey "invalid $(what_for) DataKey: $(key)"
        if what_for == "empty"
            @assert value isa StorageScalar "invalid empty StorageScalar: $(value)"
        end
        if key isa TensorKey
            push!(tensor_keys, key)
        end
    end

    if length(tensor_keys) == first_tensor_key_index
        return data
    end

    new_data = Dict{Any, Any}(data)

    for (key, value) in data
        if key isa TensorKey
            delete!(new_data, key)
            (main_axis, row_axis, column_axis, matrix_name) = key
            for daf in dafs
                if has_axis(daf, main_axis)
                    main_axis_entries = axis_vector(daf, main_axis)
                    for entry in main_axis_entries
                        new_data[(row_axis, column_axis, "$(entry)_$(matrix_name)")] = value
                    end
                end
            end
        end
    end

    return new_data
end

function verify_axes(;
    destination::DafWriter,
    source::DafReader,
    what_for::Maybe{AbstractString},
)::Dict{AbstractString, Symbol}
    axis_relations = Dict{AbstractString, Symbol}()
    for axis in axes_set(source)
        axis_relations[axis] = verify_axis(;
            destination,
            destination_axis = axis,
            source,
            source_axis = axis,
            allow_missing = true,
            what_for,
        )
    end
    return axis_relations
end

function verify_axis(;
    destination::DafWriter,
    destination_axis::AbstractString,
    source::DafReader,
    source_axis::AbstractString,
    allow_missing::Bool,
    what_for::Maybe{AbstractString},
)::Maybe{Symbol}
    if allow_missing && !has_axis(destination, destination_axis)
        return :same
    end

    source_entries = Set(axis_vector(source, source_axis))
    destination_entries = Set(axis_vector(destination, destination_axis))

    if destination_entries == source_entries
        return :same
    end

    if source_entries > destination_entries
        return :destination_is_subset
    end

    if source_entries < destination_entries
        verify_subset(;
            source_name = source.name,
            source_axis,
            destination_name = destination.name,
            destination_axis,
            what_for,
        )
        return :source_is_subset
    end

    return error(dedent("""
        disjoint entries in the axis: $(source_axis)
        of the source daf data: $(source.name)
        and the axis: $(destination_axis)
        of the target daf data: $(destination.name)
    """))
end

function verify_subset(;
    source_name::AbstractString,
    source_axis::AbstractString,
    destination_name::AbstractString,
    destination_axis::AbstractString,
    what_for::Maybe{AbstractString},
)::Nothing
    if what_for !== nothing
        error(dedent("""
            missing entries in the axis: $(source_axis)
            of the source daf data: $(source_name)
            which are needed for copying $(what_for)
            of the axis: $(destination_axis)
            of the target daf data: $(destination_name)
        """))
    end
end

function copy_scalars(;
    destination::DafWriter,
    source::DafReader,
    types::Maybe{DataTypes},
    overwrite::Bool,
    insist::Bool,
)::Nothing
    for name in scalars_set(source)
        type = nothing
        if types !== nothing
            type = get(types, name, nothing)
        end
        copy_scalar!(; destination, source, name, type, overwrite, insist)
    end
end

function copy_axes(; destination::DafWriter, source::DafReader, overwrite::Bool = false, insist::Bool = true)::Nothing
    for axis in axes_set(source)
        if !has_axis(destination, axis)
            copy_axis!(; source, destination, axis, overwrite, insist)
        end
    end
end

function copy_vectors(;
    destination::DafWriter,
    source::DafReader,
    axis_relations::Dict{AbstractString, Symbol},
    empty::Maybe{EmptyData},
    types::Maybe{DataTypes},
    overwrite::Bool,
    insist::Bool,
)::Nothing
    for (axis, relation) in axis_relations
        for name in vectors_set(source, axis)
            empty_value = nothing
            if empty !== nothing
                empty_value = get(empty, (axis, name), nothing)
            end

            type = nothing
            if types !== nothing
                type = get(types, (axis, name), nothing)
            end

            copy_vector!(;
                destination,
                source,
                axis,
                name,
                empty = empty_value,
                eltype = type,
                overwrite,
                insist,
                relation,
            )
        end
    end
end

function copy_matrices(;
    destination::DafWriter,
    source::DafReader,
    axis_relations::Dict{AbstractString, Symbol},
    empty::Maybe{EmptyData},
    types::Maybe{DataTypes},
    overwrite::Bool,
    insist::Bool,
    relayout::Bool,
)::Nothing
    for (rows_axis, rows_relation) in axis_relations
        for (columns_axis, columns_relation) in axis_relations
            if !relayout || columns_axis >= rows_axis
                for name in matrices_set(source, rows_axis, columns_axis; relayout)
                    copy_single_matrix(;
                        destination,
                        source,
                        empty,
                        types,
                        overwrite,
                        insist,
                        rows_axis,
                        rows_relation,
                        columns_axis,
                        columns_relation,
                        name,
                        relayout,
                    )
                end
            end
        end
    end
end

function ensure_tensors(;
    destination::DafWriter,
    source::DafReader,
    axis_relations::Dict{AbstractString, Symbol},
    empty::Maybe{EmptyData},
    types::Maybe{DataTypes},
    overwrite::Bool,
    insist::Bool,
    relayout::Bool,
    tensor_keys::AbstractVector{TensorKey},
)::Nothing
    for (main_axis, rows_axis, columns_axis, matrix_name) in tensor_keys
        main_axis_entries = axis_vector(destination, main_axis)
        for entry in main_axis_entries
            name = "$(entry)_$(matrix_name)"
            if !has_matrix(destination, rows_axis, columns_axis, name; relayout)
                copy_single_matrix(;
                    destination,
                    source,
                    empty,
                    types,
                    overwrite,
                    insist,
                    rows_axis,
                    rows_relation = axis_relations[rows_axis],
                    columns_axis,
                    columns_relation = axis_relations[columns_axis],
                    name,
                    relayout,
                )
            end
        end
    end
end

function copy_single_matrix(;
    destination::DafWriter,
    source::DafReader,
    empty::Maybe{EmptyData},
    types::Maybe{DataTypes},
    overwrite::Bool,
    insist::Bool,
    rows_axis::AbstractString,
    rows_relation::Symbol,
    columns_axis::AbstractString,
    columns_relation::Symbol,
    name::AbstractString,
    relayout::Bool,
)::Nothing
    empty_value = nothing
    if empty !== nothing
        empty_value = get(empty, (rows_axis, columns_axis, name), nothing)
        if empty_value === nothing
            empty_value = get(empty, (columns_axis, rows_axis, name), nothing)
        end
    end

    type = nothing
    if types !== nothing
        type = get(types, (rows_axis, columns_axis, name), nothing)
        if type === nothing
            type = get(types, (columns_axis, rows_axis, name), nothing)
        end
    end

    if empty_value === nothing
        default = undef
    else
        default = empty_value
    end

    return copy_matrix!(;
        destination,
        source,
        rows_axis,
        columns_axis,
        name,
        default,
        empty = empty_value,
        eltype = type,
        overwrite,
        insist,
        rows_relation,
        columns_relation,
        relayout,
    )
end

function new_name(rename::Maybe{AbstractString}, name::AbstractString)::AbstractString
    return rename === nothing ? name : rename
end

function target_types(type::Type)::Tuple{Type, Type}
    if type <: AbstractString
        return (String, AbstractString)
    else
        @assert isbitstype(type) "not a bits type: $(type)"
        return (type, type)
    end
end

end # module
