"""
Concatenate multiple `Daf` data sets along some axis. This copies the data from the concatenated data sets into some
target data set.

The exact behavior of concatenation is surprisingly complex when accounting for sparse vs. dense matrices, different
matrix layouts, and properties which are not along the concatenation axis. The implementation is further complicated by
minimizing the allocation of intermediate memory buffers for the data; that is, in principle, concatenating from and
into memory-mapped data sets should not allocate "any" memory buffers - the data should be copied directly from one
memory-mapped region to another.
"""
module Concat

export CollectAxis
export concatenate!
export LastValue
export MergeAction
export MergeData
export SkipProperty

using ..Copies
using ..Formats
using ..GenericLogging
using ..GenericTypes
using ..Readers
using ..StorageTypes
using ..Views
using ..Writers
using Base.Threads
using NamedArrays
using SparseArrays

import ..Readers.require_axis
import ..StorageTypes.indtype_for_size
import ..Writers.require_no_axis
import ..Writers.require_no_matrix

"""
A vector of pairs where the key is a [`DataKey`](@ref) and the value is [`MergeAction`](@ref). Similarly to
[`ViewData`](@ref), the order of the entries matters (last one wins), and a key containing `"*"` is expanded to all the
relevant properties. For matrices, merge is done separately for each layout. That is, the order of the key
`(rows_axis, columns_axis, matrix_name)` key *does* matter in the `MergeData`, which is different from how
[`ViewData`](@ref) works.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        MergeData = AbstractVector{Pair{DataKey, MergeAction}}

    But what we are **forced** to say is:

        ViewData = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
MergeData = AbstractVector{<:Pair}

"""
The action for merging the values of a property from the concatenated data sets into the result data set. This is used
to properties that do not apply to the concatenation axis (that is, scalar properties, and vector and matrix properties
of other axes). Valid values are:

  - `SkipProperty` - do not create the property in the result. This is the default.
  - `LastValue` - use the value from the last concatenated data set (that has a value for the property). This is useful
    for properties that have the same value for all concatenated data sets.
  - `CollectAxis` - collect the values from all the data sets, adding a dimension to the data (that is, convert a scalar
    property to a vector, and a vector property to a matrix). This can't be applied to matrix properties, because we
    can't directly store 3D data inside `Daf`. In addition, this requires that a dataset axis is created in the target,
    and that an empty value is specified for the property if it is missing from any of the concatenated data sets.
"""
@enum MergeAction SkipProperty LastValue CollectAxis

"""
    concatenate!(
        destination::DafWriter,
        axis::Union{AbstractString, AbstractVector{<:AbstractString}},
        sources::AbstractVector{<:DafReader};
        [names::Maybe{AbstractVector{<:AbstractString}} = nothing,
        dataset_axis::Maybe{AbstractString} = "dataset",
        dataset_property::Bool = true,
        prefix::Union{Bool, AbstractVector{Bool}} = false,
        prefixed::Maybe{Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractSet{<:AbstractString}}}} = nothing,
        empty::Maybe{EmptyData} = nothing,
        sparse_if_saves_storage_fraction = 0.25,
        merge::Maybe{MergeData} = nothing,
        overwrite::Bool = false]
    )::Nothing

Concatenate data from a `sources` sequence of `Daf` data sets into a single `destination` data set along one or more
concatenation `axis`. You can also concatenate along multiple axes by specifying an array of `axis` names.

We need a unique name for each of the concatenated data sets. By default, we use the `DafReader.name`. You can
override this by specifying an explicit `names` vector with one name per data set.

By default, a new axis named by `dataset_axis` is created with one entry per concatenated data set, using these unique
names. You can disable this by setting `dataset_axis` to `nothing`.

If an axis is created, and `dataset_property` is set (the default), a property with the same name is created for the
concatenated `axis`, containing the name of the data set each entry was collected from.

The entries of each concatenated axis must be unique. By default, we require that no entry name is used in more than one
data set. If this isn't the case, then set `prefix` to specify adding the unique data set name (and a `.` separator) to
its entries (either once for all the axes, or using a vector with a setting per axis).

!!! note

    If a prefix is added to the axis entry names, then it must also be added to all the vector properties whose values
    are entries of the axis. By default, we assume that any property name that is identical to the axis name is such a
    property (e.g., given a `cluster` axis, a `cluster` property of each `cell` is assumed to contain the names of
    clusters from that axis). We also allow for property names to just start with the axis name, followed by `.` and
    some suffix (e.g., `cluster.manual` will also be assumed to contain the names of clusters). We'll automatically add
    the unique prefix to all such properties.

    If, however, this heuristic fails, you can specify a vector of properties to be `prefixed` (or a vector of such
    vectors, one per concatenated axis). In this case only the listed properties will be prefixed with the unique data
    set names.

Vector and matrix properties for the `axis` will be concatenated. If some of the concatenated data sets do not contain
some property, then an `empty` value must be specified for it, and will be used for the missing data.

Concatenated matrices are always stored in column-major layout where the concatenation axis is the column axis. There
should not exist any matrices whose both axes are concatenated (e.g., square matrices of the concatenated axis).

The concatenated properties will be sparse if the storage for the sparse data is smaller than naive dense storage by at
`sparse_if_saves_storage_fraction` (by default, if using sparse storage saves at least 25% of the space, that is, takes
at most 75% of the dense storage space). When estimating this fraction, we assume dense data is 100% non-zero; we only
take into account data already stored as sparse, as well as any missing data whose `empty` value is zero.

By default, properties that do not apply to any of the concatenation `axis` will be ignored. If `merge` is specified,
then such properties will be processed according to it. Using `CollectAxis` for a property requires that the
`dataset_axis` will not be `nothing`.

By default, concatenation will fail rather than `overwrite` existing properties in the target.
"""
@logged function concatenate!(
    destination::DafWriter,
    axis::Union{AbstractString, AbstractVector{<:AbstractString}},
    sources::AbstractVector{<:DafReader};
    names::Maybe{AbstractVector{<:AbstractString}} = nothing,
    dataset_axis::Maybe{AbstractString} = "dataset",
    dataset_property::Bool = true,
    prefix::Union{Bool, AbstractVector{Bool}} = false,
    prefixed::Maybe{Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractSet{<:AbstractString}}}} = nothing,
    empty::Maybe{EmptyData} = nothing,
    sparse_if_saves_storage_fraction = 0.1,
    merge::Maybe{MergeData} = nothing,
    overwrite::Bool = false,
)::Nothing
    for source in sources
        Formats.begin_data_read_lock(source, "concatenate! of source:", source.name)
    end
    Formats.begin_data_write_lock(destination, "concatenate! of destination:", destination.name)

    try
        if axis isa AbstractString
            axes = [axis]
        else
            axes = axis
        end

        @assert length(axes) > 0
        @assert allunique(axes)

        for axis in axes
            require_no_axis(destination, axis)
            for source in sources
                require_axis(source, "for: concatenate!", axis)
            end
        end

        for rows_axis in axes
            for columns_axis in axes
                for source in sources
                    invalid_matrices_set = matrices_set(source, rows_axis, columns_axis; relayout = false)
                    for invalid_matrix_name in invalid_matrices_set
                        error(
                            "can't concatenate the matrix: $(invalid_matrix_name)\n" *
                            "for the concatenated rows axis: $(rows_axis)\n" *
                            "and the concatenated columns axis: $(columns_axis)\n" *
                            "in the daf data: $(source.name)\n" *
                            "concatenated into the daf data: $(destination.name)",
                        )
                    end
                end
            end
        end

        @assert length(sources) > 0

        if names === nothing
            names = [source.name for source in sources]
        end
        @assert length(names) == length(sources)
        @assert allunique(names)

        if dataset_axis !== nothing
            @assert !(dataset_axis in axes)
            require_no_axis(destination, dataset_axis)
            for source in sources
                require_no_axis(source, dataset_axis)
            end
        end

        if prefix isa AbstractVector
            prefixes = prefix
        else
            prefixes = fill(prefix, length(axes))
        end
        @assert length(prefixes) == length(axes)

        if prefixed isa AbstractSet{<:AbstractString}
            prefixed = [prefixed]
        end
        @assert prefixed === nothing || length(prefixed) == length(axes)

        @assert empty === nothing || !(CollectAxis in values(empty)) || dataset_axis !== nothing

        axes_names_set = Set(axes)
        other_axes_entry_names = Dict{AbstractString, Tuple{AbstractString, AbstractVector{<:AbstractString}}}()
        for source in sources
            for axis_name in axes_set(source)
                if !(axis_name in axes_names_set)
                    other_axis_entry_names = axis_array(source, axis_name)
                    previous_axis_data = get(other_axes_entry_names, axis_name, nothing)
                    if previous_axis_data === nothing
                        other_axes_entry_names[axis_name] = (source.name, other_axis_entry_names)
                    else
                        (previous_source_name, previous_axis_entry_names) = previous_axis_data
                        if other_axis_entry_names != previous_axis_entry_names
                            error(
                                "different entries for the axis: $(axis_name)\n" *
                                "between the daf data: $(previous_source_name)\n" *
                                "and the daf data: $(source.name)\n" *
                                "concatenated into the daf data: $(destination.name)",
                            )
                        end
                    end
                end
            end
        end
        other_axes_set = keys(other_axes_entry_names)

        if dataset_axis !== nothing
            add_axis!(destination, dataset_axis, names)
        end

        for (other_axis, other_axis_entry_names) in other_axes_entry_names
            add_axis!(destination, other_axis, other_axis_entry_names[2])
        end

        for (axis_index, (axis, prefix)) in enumerate(zip(axes, prefixes))
            if prefixed === nothing
                axis_prefixed = nothing
            else
                axis_prefixed = prefixed[axis_index]
            end
            concatenate_axis(
                destination,
                axis,
                sources;
                axes = axes,
                other_axes_set = other_axes_set,
                names = names,
                dataset_axis = dataset_axis,
                dataset_property = dataset_property,
                prefix,
                prefixes = prefixes,
                prefixed = axis_prefixed,
                empty = empty,
                sparse_if_saves_storage_fraction = sparse_if_saves_storage_fraction,
                overwrite = overwrite,
            )
        end

        if merge !== nothing
            concatenate_merge(
                destination,
                sources;
                dataset_axis = dataset_axis,
                other_axes_set = other_axes_set,
                empty = empty,
                merge = merge,
                sparse_if_saves_storage_fraction = sparse_if_saves_storage_fraction,
                overwrite = overwrite,
            )
        end

        return nothing

    finally
        Formats.end_data_write_lock(destination, "concatenate! of destination:", destination.name)
        for source in reverse(sources)
            Formats.end_data_read_lock(source, "concatenate! of source:", source.name)
        end
    end
end

function concatenate_axis(
    destination::DafWriter,
    axis::AbstractString,
    sources::AbstractVector{<:DafReader};
    axes::AbstractVector{<:AbstractString},
    other_axes_set::AbstractSet{<:AbstractString},
    names::AbstractVector{<:AbstractString},
    dataset_axis::Maybe{AbstractString},
    dataset_property::Bool,
    prefix::Bool,
    prefixes::AbstractArray{Bool},
    prefixed::Maybe{AbstractSet{<:AbstractString}},
    empty::Maybe{EmptyData},
    sparse_if_saves_storage_fraction::AbstractFloat,
    overwrite::Bool,
)::Nothing
    sizes = [axis_length(source, axis) for source in sources]
    concatenated_axis_size = sum(sizes)

    offsets = cumsum(sizes)
    offsets[2:end] = offsets[1:(end - 1)]
    offsets[1] = 0

    concatenate_axis_entry_names(
        destination,
        axis,
        sources;
        names = names,
        prefix = prefix,
        sizes = sizes,
        offsets = offsets,
        concatenated_axis_size = concatenated_axis_size,
    )

    if dataset_axis !== nothing && dataset_property
        concatenate_axis_dataset_property(
            destination,
            axis;
            names = names,
            dataset_axis = dataset_axis,
            offsets = offsets,
            sizes = sizes,
            concatenated_axis_size = concatenated_axis_size,
            overwrite = overwrite,
        )
    end

    vector_properties_set = Set{AbstractString}()
    for source in sources
        union!(vector_properties_set, vectors_set(source, axis))
    end

    for vector_property in vector_properties_set
        if prefixed !== nothing
            prefix_axis = vector_property in prefixed
        else
            prefix_axis = false
            for (other_axis, other_prefix) in zip(axes, prefixes)
                if other_prefix
                    other_axis_prefix = other_axis * "."
                    prefix_axis = vector_property == other_axis || startswith(vector_property, other_axis_prefix)
                    if prefix_axis
                        break
                    end
                end
            end
        end

        empty_value = get_empty_value(empty, (axis, vector_property))

        concatenate_axis_vector(
            destination,
            axis,
            vector_property,
            sources;
            names = names,
            prefix = prefix_axis,
            empty_value = empty_value,
            sparse_if_saves_storage_fraction = sparse_if_saves_storage_fraction,
            offsets = offsets,
            sizes = sizes,
            concatenated_axis_size = concatenated_axis_size,
            overwrite = overwrite,
        )
    end

    for other_axis in other_axes_set
        matrix_properties_set = Set{AbstractString}()
        for source in sources
            union!(matrix_properties_set, matrices_set(source, other_axis, axis; relayout = false))
        end

        for matrix_property in matrix_properties_set
            empty_value =
                get_empty_value(empty, (other_axis, axis, matrix_property), (axis, other_axis, matrix_property))
            concatenate_axis_matrix(
                destination,
                other_axis,
                axis,
                matrix_property,
                sources;
                empty_value = empty_value,
                sparse_if_saves_storage_fraction = sparse_if_saves_storage_fraction,
                offsets = offsets,
                sizes = sizes,
                overwrite = overwrite,
            )
        end
    end

    return nothing
end

function concatenate_axis_entry_names(
    destination::DafWriter,
    axis::AbstractString,
    sources::AbstractVector{<:DafReader};
    names::AbstractVector{<:AbstractString},
    prefix::Bool,
    sizes::AbstractVector{<:Integer},
    offsets::AbstractVector{<:Integer},
    concatenated_axis_size::Integer,
)::Nothing
    axis_entry_names = Vector{AbstractString}(undef, concatenated_axis_size)
    n_sources = length(sources)
    @threads for index in 1:n_sources
        source = sources[index]
        offset = offsets[index]
        size = sizes[index]
        from_axis_entry_names = axis_array(source, axis)
        if prefix
            name = names[index]
            axis_entry_names[(offset + 1):(offset + size)] = (name * ".") .* from_axis_entry_names
        else
            axis_entry_names[(offset + 1):(offset + size)] .= from_axis_entry_names[:]
        end
    end

    add_axis!(destination, axis, axis_entry_names)

    return nothing
end

function concatenate_axis_dataset_property(
    destination::DafWriter,
    axis::AbstractString;
    names::AbstractVector{<:AbstractString},
    dataset_axis::AbstractString,
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    concatenated_axis_size::Integer,
    overwrite::Bool,
)::Nothing
    axis_datasets = Vector{AbstractString}(undef, concatenated_axis_size)

    n_sources = length(offsets)
    @threads for index in 1:n_sources
        offset = offsets[index]
        size = sizes[index]
        name = names[index]
        axis_datasets[(offset + 1):(offset + size)] .= name
    end

    set_vector!(destination, axis, dataset_axis, axis_datasets; overwrite = overwrite)
    return nothing
end

function concatenate_axis_vector(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    names::AbstractVector{<:AbstractString},
    prefix::Bool,
    empty_value::Maybe{StorageScalar},
    sparse_if_saves_storage_fraction::AbstractFloat,
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    concatenated_axis_size::Integer,
    overwrite::Bool,
)::Nothing
    vectors = [get_vector(source, axis, vector_property; default = nothing) for source in sources]
    dtype = reduce(merge_dtypes, vectors; init = typeof(empty_value))

    if dtype == String
        concatenate_axis_string_vectors(
            destination,
            axis,
            vector_property,
            sources;
            names = names,
            prefix = prefix,
            empty_value = empty_value,
            offsets = offsets,
            sizes = sizes,
            vectors = vectors,
            concatenated_axis_size = concatenated_axis_size,
            overwrite = overwrite,
        )

    else
        @assert empty_value isa Maybe{StorageScalar}
        sparse_saves = sparse_storage_fraction(empty_value, dtype, sizes, vectors, 1, 1)  # NOJET
        if sparse_saves >= sparse_if_saves_storage_fraction
            @assert empty_value === nothing || empty_value == 0
            sparse_vectors = sparsify_vectors(vectors, dtype, sizes)
            concatenate_axis_sparse_vectors(
                destination,
                axis,
                vector_property;
                dtype = dtype,
                offsets = offsets,
                vectors = sparse_vectors,
                overwrite = overwrite,
            )

        else
            concatenate_axis_dense_vectors(
                destination,
                axis,
                vector_property,
                sources;
                dtype = dtype,
                empty_value = empty_value,
                offsets = offsets,
                sizes = sizes,
                vectors = vectors,
                overwrite = overwrite,
            )
        end
    end

    return nothing
end

function concatenate_axis_string_vectors(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    names::AbstractVector{<:AbstractString},
    prefix::Bool,
    empty_value::Maybe{StorageScalar},
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    vectors::AbstractVector{<:Maybe{<:NamedVector}},
    concatenated_axis_size::Integer,
    overwrite::Bool,
)::Nothing
    concatenated_vector = Vector{AbstractString}(undef, concatenated_axis_size)

    n_sources = length(sources)
    @threads for index in 1:n_sources
        vector = vectors[index]
        source = sources[index]
        offset = offsets[index]
        size = sizes[index]
        name = names[index]
        if vector === nothing
            concatenated_vector[(offset + 1):(offset + size)] .=
                require_empty_value_for_vector(empty_value, vector_property, axis, source, destination)
        else
            @assert length(vector) == size
            if !(eltype(vector) <: AbstractString)
                vector = string.(vector)
            end
            if prefix
                concatenated_vector[(offset + 1):(offset + size)] = (name * ".") .* vector
            else
                concatenated_vector[(offset + 1):(offset + size)] = vector
            end
        end
    end

    set_vector!(destination, axis, vector_property, concatenated_vector; overwrite = overwrite)
    return nothing
end

function concatenate_axis_sparse_vectors(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString;
    dtype::Type,
    offsets::AbstractVector{<:Integer},
    vectors::AbstractVector{<:SparseVector},
    overwrite::Bool,
)::Nothing
    nnz_offsets, nnz_sizes, total_nnz = nnz_arrays(vectors)

    empty_sparse_vector!(destination, axis, vector_property, dtype, total_nnz; overwrite = overwrite) do nzind, nzval
        n_sources = length(vectors)
        @threads for index in 1:n_sources
            vector = vectors[index]
            offset = offsets[index]
            nnz_offset = nnz_offsets[index]
            nnz_size = nnz_sizes[index]
            nzval[(nnz_offset + 1):(nnz_offset + nnz_size)] = vector.nzval
            nzind[(nnz_offset + 1):(nnz_offset + nnz_size)] = vector.nzind
            nzind[(nnz_offset + 1):(nnz_offset + nnz_size)] .+= offset
        end
    end

    return nothing
end

function concatenate_axis_dense_vectors(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    dtype::Type,
    empty_value::Maybe{StorageNumber},
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    vectors::AbstractVector{<:Maybe{<:StorageVector}},
    overwrite::Bool,
)::Nothing
    empty_dense_vector!(destination, axis, vector_property, dtype; overwrite = overwrite) do concatenated_vector
        n_sources = length(sources)
        @threads for index in 1:n_sources
            source = sources[index]
            vector = vectors[index]
            offset = offsets[index]
            size = sizes[index]
            if vector === nothing
                concatenated_vector[(offset + 1):(offset + size)] .=
                    require_empty_value_for_vector(empty_value, vector_property, axis, source, destination)
            else
                @assert length(vector) == size
                concatenated_vector[(offset + 1):(offset + size)] = vector
            end
        end
        return nothing
    end

    return nothing
end

function concatenate_axis_matrix(
    destination::DafWriter,
    other_axis::AbstractString,
    axis::AbstractString,
    matrix_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    empty_value::Maybe{StorageNumber},
    sparse_if_saves_storage_fraction::AbstractFloat,
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    overwrite::Bool,
)::Nothing
    matrices = [get_matrix(source, other_axis, axis, matrix_property; default = nothing) for source in sources]
    dtype = reduce(merge_dtypes, matrices; init = typeof(empty_value))

    nrows = axis_length(destination, other_axis)
    sparse_saves = sparse_storage_fraction(empty_value, dtype, sizes, matrices, axis_length(destination, other_axis), 2)
    if sparse_saves >= sparse_if_saves_storage_fraction
        @assert empty_value === nothing || empty_value == 0
        sparse_matrices = sparsify_matrices(matrices, dtype, nrows, sizes)
        concatenate_axis_sparse_matrices(
            destination,
            other_axis,
            axis,
            matrix_property;
            dtype = dtype,
            offsets = offsets,
            sizes = sizes,
            matrices = sparse_matrices,
            overwrite = overwrite,
        )

    else
        concatenate_axis_dense_matrices(
            destination,
            other_axis,
            axis,
            matrix_property,
            sources;
            dtype = dtype,
            empty_value = empty_value,
            offsets = offsets,
            sizes = sizes,
            matrices = matrices,
            overwrite = overwrite,
        )
    end

    return nothing
end

function concatenate_axis_sparse_matrices(
    destination::DafWriter,
    other_axis::AbstractString,
    axis::AbstractString,
    matrix_property::AbstractString;
    dtype::Type,
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    matrices::AbstractVector{<:SparseMatrixCSC},
    overwrite::Bool,
)::Nothing
    nnz_offsets, nnz_sizes, total_nnz = nnz_arrays(matrices)

    empty_sparse_matrix!(
        destination,
        other_axis,
        axis,
        matrix_property,
        dtype,
        total_nnz;
        overwrite = overwrite,
    ) do colptr, rowval, nzval
        n_sources = length(matrices)
        @threads for index in 1:n_sources
            matrix = matrices[index]
            column_offset = offsets[index]
            ncols = sizes[index]
            nnz_offset = nnz_offsets[index]
            nnz_size = nnz_sizes[index]
            nzval[(nnz_offset + 1):(nnz_offset + nnz_size)] = matrix.nzval
            rowval[(nnz_offset + 1):(nnz_offset + nnz_size)] = matrix.rowval
            colptr[(column_offset + 1):(column_offset + ncols)] = matrix.colptr[1:ncols]
            colptr[(column_offset + 1):(column_offset + ncols)] .+= nnz_offset
        end
        return colptr[end] = total_nnz + 1
    end

    return nothing
end

function concatenate_axis_dense_matrices(
    destination::DafWriter,
    other_axis::AbstractString,
    axis::AbstractString,
    matrix_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    dtype::Type,
    empty_value::Maybe{StorageNumber},
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    matrices::AbstractVector{<:Maybe{<:StorageMatrix}},
    overwrite::Bool,
)::Nothing
    empty_dense_matrix!(
        destination,
        other_axis,
        axis,
        matrix_property,
        dtype;
        overwrite = overwrite,
    ) do concatenated_matrix
        n_sources = length(sources)
        @threads for index in 1:n_sources
            source = sources[index]
            matrix = matrices[index]
            column_offset = offsets[index]
            ncols = sizes[index]
            if matrix === nothing
                concatenated_matrix[:, (column_offset + 1):(column_offset + ncols)] .=
                    require_empty_value_for_matrix(empty_value, matrix_property, other_axis, axis, source, destination)
            else
                @assert size(matrix)[2] == ncols
                concatenated_matrix[:, (column_offset + 1):(column_offset + ncols)] = matrix
            end
        end
        return nothing
    end

    return nothing
end

function concatenate_merge(
    destination::DafWriter,
    sources::AbstractVector{<:DafReader};
    dataset_axis::Maybe{AbstractString},
    other_axes_set::AbstractSet{<:AbstractString},
    empty::Maybe{EmptyData},
    merge::MergeData,
    sparse_if_saves_storage_fraction::AbstractFloat,
    overwrite::Bool,
)::Nothing
    scalar_properties_set = Set{AbstractString}()
    for source in sources
        union!(scalar_properties_set, scalars_set(source))
    end
    for scalar_property in scalar_properties_set
        merge_action = get_merge_action(merge, scalar_property)
        empty_value = get_empty_value(empty, scalar_property)
        if merge_action != SkipProperty
            concatenate_merge_scalar(
                destination,
                scalar_property,
                sources;
                dataset_axis = dataset_axis,
                empty_value = empty_value,
                merge_action = merge_action,
                overwrite = overwrite,
            )
        end
    end

    for axis in other_axes_set
        vector_properties_set = Set{AbstractString}()
        square_matrix_properties_set = Set{AbstractString}()
        for source in sources
            union!(vector_properties_set, vectors_set(source, axis))
            union!(square_matrix_properties_set, matrices_set(source, axis, axis))
        end

        for vector_property in vector_properties_set
            merge_action = get_merge_action(merge, (axis, vector_property))
            empty_value = get_empty_value(empty, (axis, vector_property))
            if merge_action != SkipProperty
                concatenate_merge_vector(
                    destination,
                    axis,
                    vector_property,
                    sources;
                    dataset_axis = dataset_axis,
                    empty_value = empty_value,
                    merge_action = merge_action,
                    sparse_if_saves_storage_fraction = sparse_if_saves_storage_fraction,
                    overwrite = overwrite,
                )
            end
        end

        for square_matrix_property in square_matrix_properties_set
            merge_action = get_merge_action(merge, (axis, axis, square_matrix_property))
            if merge_action != SkipProperty
                concatenate_merge_matrix(
                    destination,
                    axis,
                    axis,
                    square_matrix_property,
                    sources;
                    merge_action = merge_action,
                    overwrite = overwrite,
                )
            end
        end
    end

    for rows_axis in other_axes_set
        for columns_axis in other_axes_set
            if rows_axis != columns_axis
                matrix_properties_set = Set{AbstractString}()
                for source in sources
                    union!(matrix_properties_set, matrices_set(source, rows_axis, columns_axis; relayout = false))
                end

                for matrix_property in matrix_properties_set
                    merge_action = get_merge_action(merge, (rows_axis, columns_axis, matrix_property))
                    if merge_action != SkipProperty
                        concatenate_merge_matrix(
                            destination,
                            rows_axis,
                            columns_axis,
                            matrix_property,
                            sources;
                            merge_action = merge_action,
                            overwrite = overwrite,
                        )
                    end
                end
            end
        end
    end

    return nothing
end

function concatenate_merge_scalar(
    destination::DafWriter,
    scalar_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    dataset_axis::Maybe{AbstractString},
    empty_value::Maybe{StorageScalar},
    merge_action::MergeAction,
    overwrite::Bool,
)::Nothing
    if merge_action == LastValue
        for source in reverse(sources)
            value = get_scalar(source, scalar_property; default = nothing)
            if value !== nothing
                set_scalar!(destination, scalar_property, value; overwrite = overwrite)
                return nothing
            end
        end
        @assert false

    elseif merge_action == CollectAxis
        if dataset_axis === nothing
            error(
                "can't collect axis for the scalar: $(scalar_property)\n" *
                "of the daf data sets concatenated into the daf data: $(destination.name)\n",
                "because no data set axis was created",
            )
        end

        scalars = [get_scalar(source, scalar_property; default = nothing) for source in sources]
        dtype = reduce(merge_dtypes, scalars; init = typeof(empty_value))
        scalars = [dtype(scalar) for scalar in scalars]
        set_vector!(destination, dataset_axis, scalar_property, scalars; overwrite = overwrite)
        return nothing

    else
        @assert false
    end
end

function concatenate_merge_vector(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    dataset_axis::Maybe{AbstractString},
    empty_value::Maybe{StorageScalar},
    merge_action::MergeAction,
    sparse_if_saves_storage_fraction::AbstractFloat,
    overwrite::Bool,
)::Nothing
    if merge_action == LastValue
        for source in reverse(sources)
            value = get_vector(source, axis, vector_property; default = nothing)
            if value !== nothing
                set_vector!(destination, axis, vector_property, value; overwrite = overwrite)
                return nothing
            end
        end
        @assert false

    elseif merge_action == CollectAxis
        if dataset_axis === nothing
            error(
                "can't collect axis for the vector: $(vector_property)\n" *
                "of the axis: $(axis)\n" *
                "of the daf data sets concatenated into the daf data: $(destination.name)\n",
                "because no data set axis was created",
            )
        end

        vectors = [get_vector(source, axis, vector_property; default = nothing) for source in sources]

        size = nothing
        for vector in vectors
            if vector !== nothing
                size = length(vector)
                break
            end
        end
        @assert size !== nothing
        sizes = repeat([size]; outer = length(vectors))
        dtype = reduce(merge_dtypes, vectors; init = typeof(empty_value))

        if dtype != String
            sparse_saves = sparse_storage_fraction(empty_value, dtype, sizes, vectors, 1, 1)  # NOJET
            if sparse_saves >= sparse_if_saves_storage_fraction
                @assert empty_value === nothing || empty_value == 0
                sparse_vectors = sparsify_vectors(vectors, dtype, sizes)
                concatenate_merge_sparse_vector(
                    destination,
                    axis,
                    dataset_axis,
                    vector_property;
                    dtype = dtype,
                    vectors = sparse_vectors,
                    overwrite = overwrite,
                )
                return nothing
            end
        end

        concatenate_merge_dense_vector(
            destination,
            axis,
            dataset_axis,
            vector_property,
            sources;
            dtype = dtype,
            empty_value = empty_value,
            vectors = vectors,
            overwrite = overwrite,
        )
        return nothing

    else
        @assert false
    end
end

function concatenate_merge_sparse_vector(
    destination::DafWriter,
    axis::AbstractString,
    dataset_axis::AbstractString,
    vector_property::AbstractString;
    dtype::Type,
    vectors::AbstractVector{<:SparseVector},
    overwrite::Bool,
)::Nothing
    nnz_offsets, nnz_sizes, total_nnz = nnz_arrays(vectors)

    empty_sparse_matrix!(
        destination,
        axis,
        dataset_axis,
        vector_property,
        dtype,
        total_nnz;
        overwrite = overwrite,
    ) do colptr, rowval, nzval
        colptr[1] == 1
        n_sources = length(vectors)
        @threads for index in 1:n_sources
            vector = vectors[index]
            nnz_offset = nnz_offsets[index]
            nnz_size = nnz_sizes[index]
            nzval[(nnz_offset + 1):(nnz_offset + nnz_size)] = vector.nzval
            rowval[(nnz_offset + 1):(nnz_offset + nnz_size)] = vector.nzind
            colptr[index + 1] = nnz_offset + nnz_size + 1
        end
    end

    return nothing
end

function concatenate_merge_dense_vector(
    destination::DafWriter,
    axis::AbstractString,
    dataset_axis::AbstractString,
    vector_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    dtype::Type,
    empty_value::Maybe{StorageScalar},
    vectors::AbstractVector{<:Maybe{<:NamedVector}},
    overwrite::Bool,
)::Nothing
    empty_dense_matrix!(
        destination,
        axis,
        dataset_axis,
        vector_property,
        dtype;
        overwrite = overwrite,
    ) do concatenated_matrix
        n_sources = length(sources)
        @threads for index in 1:n_sources
            source = sources[index]
            vector = vectors[index]
            if vector === nothing
                concatenated_matrix[:, index] .=
                    require_empty_value_for_vector(empty_value, vector_property, axis, source, destination)
            else
                concatenated_matrix[:, index] = vector
            end
        end
        return nothing
    end

    return nothing
end

function concatenate_merge_matrix(
    destination::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    matrix_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    merge_action::MergeAction,
    overwrite::Bool,
)::Nothing
    if merge_action == LastValue
        for source in reverse(sources)
            matrix = get_matrix(source, rows_axis, columns_axis, matrix_property; relayout = false, default = nothing)
            if matrix !== nothing
                set_matrix!(
                    destination,
                    rows_axis,
                    columns_axis,
                    matrix_property,
                    matrix;
                    relayout = false,
                    overwrite = overwrite,
                )
                return nothing
            end
        end
        @assert false

    elseif merge_action == CollectAxis
        error(
            "can't collect axis for the matrix: $(matrix_property)\n" *
            "of the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "of the daf data sets concatenated into the daf data: $(destination.name)\n",
            "because that would create a 3D tensor, which is not supported",
        )

    else
        @assert false
    end
end

function require_empty_value_for_vector(
    empty_value::Maybe{StorageScalar},
    vector_property::AbstractString,
    axis::AbstractString,
    daf::DafReader,
    destination::DafWriter,
)::StorageScalar
    if empty_value === nothing
        error(
            "no empty value for the vector: $(vector_property)\n" *
            "of the axis: $(axis)\n" *
            "which is missing from the daf data: $(daf.name)\n" *
            "concatenated into the daf data: $(destination.name)",
        )
    end
    return empty_value
end

function require_empty_value_for_matrix(
    empty_value::Maybe{StorageNumber},
    matrix_property::AbstractString,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    daf::DafReader,
    destination::DafWriter,
)::StorageNumber
    if empty_value === nothing
        error(
            "no empty value for the matrix: $(matrix_property)\n" *
            "of the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "which is missing from the daf data: $(daf.name)\n" *
            "concatenated into the daf data: $(destination.name)",
        )
    end
    return empty_value
end

function get_empty_value(empty::Maybe{EmptyData}, first_key::Any, second_key::Any = nothing)::Maybe{StorageScalar}
    if empty === nothing
        return nothing
    else
        value = get(empty, first_key, nothing)
        if value === nothing && second_key !== nothing
            value = get(empty, second_key, nothing)
        end
        return value
    end
end

function get_merge_action(merge::MergeData, scalar_property::AbstractString)::MergeAction
    for (key, merge_action) in reverse(merge)
        @assert key isa DataKey
        @assert merge_action isa MergeAction
        if key == scalar_property || key == "*"
            return merge_action
        end
    end
    return SkipProperty
end

function get_merge_action(merge::MergeData, vector_property::Tuple{AbstractString, AbstractString})::MergeAction
    for (key, merge_action) in reverse(merge)
        @assert key isa DataKey
        @assert merge_action isa MergeAction
        if key isa Tuple{AbstractString, AbstractString}
            if (key[1] == "*" || key[1] == vector_property[1]) && (key[2] == "*" || key[2] == vector_property[2])
                return merge_action
            end
        end
    end
    return SkipProperty
end

function get_merge_action(
    merge::MergeData,
    matrix_property::Tuple{AbstractString, AbstractString, AbstractString},
)::MergeAction
    for (key, merge_action) in reverse(merge)
        @assert key isa DataKey
        @assert merge_action isa MergeAction
        if key isa Tuple{AbstractString, AbstractString, AbstractString}
            if (key[1] == "*" || key[1] == matrix_property[1]) &&
               (key[2] == "*" || key[2] == matrix_property[2]) &&
               (key[3] == "*" || key[3] == matrix_property[3])
                return merge_action
            end
        end
    end
    return SkipProperty
end

FLOAT_TYPES = (Float32, Float64)
SIGNED_TYPES = (Int8, Int16, Int32, Int64)
UNSIGNED_TYPES = (UInt8, UInt16, UInt32, UInt64)

function merge_dtypes(left_dtype::Type, right_dtype::Any)::Type
    while right_dtype isa AbstractArray
        right_dtype = eltype(right_dtype)
    end
    if !(right_dtype isa Type)
        right_dtype = typeof(right_dtype)
    end

    if left_dtype == Nothing
        return right_dtype
    elseif right_dtype == Nothing
        return left_dtype
    elseif left_dtype <: AbstractString || right_dtype <: AbstractString
        return String
    elseif left_dtype <: AbstractFloat || right_dtype <: AbstractFloat
        return dtype_for_size(max(sizeof(left_dtype), sizeof(right_dtype)), FLOAT_TYPES)
    elseif left_dtype <: Signed || right_dtype <: Signed
        return dtype_for_size(max(signed_sizeof(left_dtype), signed_sizeof(right_dtype)), SIGNED_TYPES)
    else
        @assert left_dtype <: Unsigned && right_dtype <: Unsigned
        return dtype_for_size(max(sizeof(left_dtype), sizeof(right_dtype)), UNSIGNED_TYPES)
    end
end

function signed_sizeof(type::Type{<:Signed})::Integer
    return sizeof(type)
end

function signed_sizeof(type::Type{<:Unsigned})::Integer
    return sizeof(type) + 1
end

function sparse_storage_fraction(
    empty_value::Maybe{StorageNumber},
    dtype::Type,
    sizes::AbstractVector{<:Integer},
    arrays::AbstractVector{<:Maybe{NamedArray}},
    scale::Integer,
    dimensions::Integer,
)::Float64
    sparse_size = 0
    dense_size = 0

    for (size, array) in zip(sizes, arrays)
        dense_size += size * scale
        if array !== nothing
            array = array.array
            if array isa AbstractSparseArray
                sparse_size += nnz(array)
            else
                sparse_size += size * scale
            end
        elseif empty_value != 0
            sparse_size += size * scale
        end
    end

    indtype = indtype_for_size(dense_size)
    dense_bytes = dense_size * sizeof(dtype)
    sparse_bytes = sparse_size * (sizeof(dtype) + dimensions * sizeof(indtype))
    return (dense_bytes - sparse_bytes) / dense_bytes
end

function sparsify_vectors(
    vectors::AbstractArray{<:Maybe{<:NamedVector}},
    dtype::Type,
    sizes::AbstractArray{<:Integer},
)::Vector{SparseVector}
    sparse_vectors = Vector{SparseVector}(undef, length(vectors))

    n_sources = length(vectors)
    @threads for index in 1:n_sources
        vector = vectors[index]
        size = sizes[index]
        if vector === nothing
            sparse_vectors[index] = spzeros(dtype, size)
        else
            @assert length(vector) == size
            vector = vector.array
            if !(vector isa SparseVector)
                vector = sparse_vector(vector)
            end
            sparse_vectors[index] = vector
        end
    end

    return sparse_vectors
end

function sparsify_matrices(
    matrices::AbstractArray{<:Maybe{<:NamedMatrix}},
    dtype::Type,
    nrows::Integer,
    sizes::AbstractArray{<:Integer},
)::Vector{SparseMatrixCSC}
    sparse_matrices = Vector{SparseMatrixCSC}(undef, length(matrices))

    n_sources = length(matrices)
    @threads for index in 1:n_sources
        matrix = matrices[index]
        ncols = sizes[index]
        if matrix === nothing
            sparse_matrices[index] = spzeros(dtype, nrows, ncols)
        else
            @assert size(matrix) == (nrows, ncols)
            sparse_matrices[index] = matrix.array
        end
    end

    return sparse_matrices
end

function nnz_arrays(
    arrays::AbstractVector{<:AbstractSparseArray},
)::Tuple{AbstractVector{<:Integer}, AbstractVector{<:Integer}, Integer}
    nnz_sizes = [nnz(array) for array in arrays]

    total_nnz = sum(nnz_sizes)

    nnz_offsets = cumsum(nnz_sizes)
    nnz_offsets[2:end] = nnz_offsets[1:(end - 1)]
    nnz_offsets[1] = 0

    return nnz_offsets, nnz_sizes, total_nnz
end

function dtype_for_size(size::Integer, types::NTuple{<:Any, Type})::Type
    for type in types[1:(end - 1)]
        if size <= sizeof(type)
            return type
        end
    end
    return types[end]
end

end  # module
