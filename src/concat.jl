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
export MergeDatum
export MergeData
export SkipProperty

using ..Copies
using ..Formats
using ..Keys
using ..Readers
using ..StorageTypes
using ..Views
using ..Writers
using Base.Threads
using NamedArrays
using SparseArrays
using TanayLabUtilities

import ..Copies.expand_tensors
import ..Readers.require_axis
import ..Writers.require_no_axis
import ..Writers.require_no_matrix

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
A pair where the key is a [`PropertyKey`](@ref) and the value is [`MergeAction`](@ref). We also allow specifying
tuples instead of pairs to make it easy to invoke the API from other languages such as Python which do not have the
concept of a `Pair`.

Similarly to [`ViewData`](@ref), the order of the entries matters (last one wins), and a key containing `"*"` is
expanded to all the relevant properties. For matrices, merge is done separately for each layout. That is, the order of
the key `(rows_axis, columns_axis, matrix_name)` key *does* matter in the `MergeData`, which is different from how
[`ViewData`](@ref) works.
"""
MergeDatum = Union{Pair{<:PropertyKey, <:MergeAction}, Tuple{PropertyKey, MergeAction}}

"""
Specify all the data to merge. We would have liked to specify this as `AbstractVector{<:MergeDatum}` but Julia in its
infinite wisdom considers `["a" => "b", ("c", "d") => "e"]` to be a `Vector{Any}`, which would require literals to be
annotated with the type.
"""
MergeData = AbstractVector

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
        sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
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
@logged function concatenate!(  # NOLINT
    destination::DafWriter,
    axis::Union{AbstractString, AbstractVector{<:AbstractString}},
    sources::AbstractVector{<:DafReader};
    names::Maybe{AbstractVector{<:AbstractString}} = nothing,
    dataset_axis::Maybe{AbstractString} = "dataset",
    dataset_property::Bool = true,
    prefix::Union{Bool, AbstractVector{Bool}} = false,
    prefixed::Maybe{Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractSet{<:AbstractString}}}} = nothing,
    empty::Maybe{EmptyData} = nothing,
    sparse_if_saves_storage_fraction::AbstractFloat = 0.25,
    merge::Maybe{MergeData} = nothing,
    overwrite::Bool = false,
)::Nothing
    @assert 0 < sparse_if_saves_storage_fraction < 1

    tensor_keys = Vector{TensorKey}()
    empty = expand_tensors(; dafs = DafReader[destination, sources...], data = empty, tensor_keys, what_for = "empty")

    if merge !== nothing
        for merge_datum in merge
            @assert (
                merge_datum isa Union{Pair, Tuple} &&
                length(merge_datum) == 2 &&
                merge_datum[1] isa PropertyKey &&
                merge_datum[2] isa MergeAction
            ) "invalid MergeDatum: $(merge_datum)"
        end
    end

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

        @assert !isempty(axes)
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
                        error(chomp("""
                              can't concatenate the matrix: $(invalid_matrix_name)
                              for the concatenated rows axis: $(rows_axis)
                              and the concatenated columns axis: $(columns_axis)
                              in the daf data: $(source.name)
                              concatenated into the daf data: $(destination.name)
                              """))
                    end
                end
            end
        end

        @assert !isempty(sources)

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
                    other_axis_entry_names = axis_vector(source, axis_name)
                    previous_axis_data = get(other_axes_entry_names, axis_name, nothing)
                    if previous_axis_data === nothing
                        other_axes_entry_names[axis_name] = (source.name, other_axis_entry_names)
                    else
                        (previous_source_name, previous_axis_entry_names) = previous_axis_data
                        if other_axis_entry_names != previous_axis_entry_names
                            error(chomp("""
                                  different entries for the axis: $(axis_name)
                                  between the daf data: $(previous_source_name)
                                  and the daf data: $(source.name)
                                  concatenated into the daf data: $(destination.name)
                                  """))
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
                axes,
                other_axes_set,
                names,
                dataset_axis,
                dataset_property,
                prefix,
                prefixes,
                prefixed = axis_prefixed,
                empty,
                sparse_if_saves_storage_fraction,
                overwrite,
            )
        end

        if merge !== nothing
            concatenate_merge(
                destination,
                sources;
                dataset_axis,
                other_axes_set,
                empty,
                merge,
                sparse_if_saves_storage_fraction,
                overwrite,
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

    concatenate_axis_entry_names(destination, axis, sources; names, prefix, sizes, offsets, concatenated_axis_size)

    if dataset_axis !== nothing && dataset_property
        concatenate_axis_dataset_property(
            destination,
            axis;
            names,
            dataset_axis,
            offsets,
            sizes,
            concatenated_axis_size,
            overwrite,
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
            names,
            prefix = prefix_axis,
            empty_value,
            sparse_if_saves_storage_fraction,
            offsets,
            sizes,
            concatenated_axis_size,
            overwrite,
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
                empty_value,
                sparse_if_saves_storage_fraction,
                offsets,
                sizes,
                overwrite,
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
    parallel_loop_wo_rng(
        1:n_sources;
        name = "concatenate_axis_entry_names",
        progress = DebugProgress(n_sources; desc = "concatenate_axis_entry_names"),
    ) do source_index
        source = sources[source_index]
        offset = offsets[source_index]
        size = sizes[source_index]
        from_axis_entry_names = axis_vector(source, axis)
        if prefix
            name = names[source_index]
            axis_entry_names[(offset + 1):(offset + size)] = (name * ".") .* from_axis_entry_names
        else
            axis_entry_names[(offset + 1):(offset + size)] .= from_axis_entry_names[:]
        end
        return nothing
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
    parallel_loop_wo_rng(
        1:n_sources;
        name = "concatenate_axis_dataset_property",
        progress = DebugProgress(n_sources; desc = "concatenate_axis_dataset_property"),
    ) do source_index
        offset = offsets[source_index]
        size = sizes[source_index]
        name = names[source_index]
        axis_datasets[(offset + 1):(offset + size)] .= name
        return nothing
    end

    set_vector!(destination, axis, dataset_axis, axis_datasets; overwrite)
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
    eltype = reduce(merge_dtypes, vectors; init = typeof(empty_value))

    if eltype == String
        concatenate_axis_string_vectors(
            destination,
            axis,
            vector_property,
            sources;
            names,
            prefix,
            empty_value,
            offsets,
            sizes,
            vectors,
            concatenated_axis_size,
            overwrite,
        )

    else
        @assert empty_value isa Maybe{StorageScalar}
        sparse_saves = sparse_vectors_storage_fraction(empty_value, eltype, sizes, vectors)  # NOJET
        if sparse_saves >= sparse_if_saves_storage_fraction
            @assert empty_value === nothing || empty_value == 0
            sparse_vectors = sparsify_concatenated_vectors(vectors, eltype, sizes)
            concatenate_axis_sparse_vectors(
                destination,
                axis,
                vector_property;
                eltype,
                offsets,
                vectors = sparse_vectors,
                overwrite,
            )

        else
            concatenate_axis_dense_vectors(
                destination,
                axis,
                vector_property,
                sources;
                eltype,
                empty_value,
                offsets,
                sizes,
                vectors,
                overwrite,
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
    parallel_loop_wo_rng(
        1:n_sources;
        name = "concatenate_axis_string_vectors",
        progress = DebugProgress(n_sources; desc = "concatenate_axis_string_vectors"),
    ) do source_index
        vector = vectors[source_index]
        source = sources[source_index]
        offset = offsets[source_index]
        size = sizes[source_index]
        name = names[source_index]
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
        return nothing
    end

    set_vector!(destination, axis, vector_property, concatenated_vector; overwrite)
    return nothing
end

function concatenate_axis_sparse_vectors(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString;
    eltype::Type,
    offsets::AbstractVector{<:Integer},
    vectors::AbstractVector{<:AbstractVector},
    overwrite::Bool,
)::Nothing
    nnz_offsets, nnz_sizes, total_nnz = nnz_arrays(vectors)

    empty_sparse_vector!(destination, axis, vector_property, eltype, total_nnz; overwrite) do sparse_nzind, sparse_nzval
        n_sources = length(vectors)
        parallel_loop_wo_rng(
            1:n_sources;
            name = "concatenate_axis_sparse_vectors",
            progress = DebugProgress(n_sources; desc = "concatenate_axis_sparse_vectors"),
        ) do source_index
            vector = vectors[source_index]
            @assert issparse(vector)
            offset = offsets[source_index]
            nnz_offset = nnz_offsets[source_index]
            nnz_size = nnz_sizes[source_index]
            sparse_nzval[(nnz_offset + 1):(nnz_offset + nnz_size)] = nzval(vector)  # NOLINT
            sparse_nzind[(nnz_offset + 1):(nnz_offset + nnz_size)] = nzind(vector)  # NOLINT
            sparse_nzind[(nnz_offset + 1):(nnz_offset + nnz_size)] .+= offset
            return nothing
        end
    end

    return nothing
end

function concatenate_axis_dense_vectors(
    destination::DafWriter,
    axis::AbstractString,
    vector_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    eltype::Type,
    empty_value::Maybe{StorageReal},
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    vectors::AbstractVector{<:Maybe{<:StorageVector}},
    overwrite::Bool,
)::Nothing
    empty_dense_vector!(destination, axis, vector_property, eltype; overwrite) do concatenated_vector
        n_sources = length(sources)
        parallel_loop_wo_rng(
            1:n_sources;
            name = "concatenate_axis_dense_vectors",
            progress = DebugProgress(n_sources; desc = "concatenate_axis_dense_vectors"),
        ) do source_index
            source = sources[source_index]
            vector = vectors[source_index]
            offset = offsets[source_index]
            size = sizes[source_index]
            if vector === nothing
                concatenated_vector[(offset + 1):(offset + size)] .=
                    require_empty_value_for_vector(empty_value, vector_property, axis, source, destination)
            else
                @assert length(vector) == size
                concatenated_vector[(offset + 1):(offset + size)] = vector
            end
            return nothing
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
    empty_value::Maybe{StorageReal},
    sparse_if_saves_storage_fraction::AbstractFloat,
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    overwrite::Bool,
)::Nothing
    matrices = [get_matrix(source, other_axis, axis, matrix_property; default = nothing) for source in sources]
    eltype = reduce(merge_dtypes, matrices; init = typeof(empty_value))

    nrows = axis_length(destination, other_axis)
    sparse_saves =
        sparse_matrices_storage_fraction(empty_value, eltype, sizes, matrices, axis_length(destination, other_axis))
    if sparse_saves >= sparse_if_saves_storage_fraction
        @assert empty_value === nothing || empty_value == 0
        sparse_matrices = sparsify_concatenated_matrices(matrices, eltype, nrows, sizes)
        concatenate_axis_sparse_matrices(
            destination,
            other_axis,
            axis,
            matrix_property;
            eltype,
            offsets,
            sizes,
            matrices = sparse_matrices,
            overwrite,
        )

    else
        concatenate_axis_dense_matrices(
            destination,
            other_axis,
            axis,
            matrix_property,
            sources;
            eltype,
            empty_value,
            offsets,
            sizes,
            matrices,
            overwrite,
        )
    end

    return nothing
end

function concatenate_axis_sparse_matrices(
    destination::DafWriter,
    other_axis::AbstractString,
    axis::AbstractString,
    matrix_property::AbstractString;
    eltype::Type,
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    matrices::AbstractVector{<:AbstractMatrix},
    overwrite::Bool,
)::Nothing
    nnz_offsets, nnz_sizes, total_nnz = nnz_arrays(matrices)

    empty_sparse_matrix!(
        destination,
        other_axis,
        axis,
        matrix_property,
        eltype,
        total_nnz;
        overwrite,
    ) do sparse_colptr, sparse_rowval, sparse_nzval
        n_sources = length(matrices)
        parallel_loop_wo_rng(
            1:n_sources;
            name = "concatenate_axis_sparse_matrices",
            progress = DebugProgress(n_sources; desc = "concatenate_axis_sparse_matrices"),
        ) do source_index
            matrix = matrices[source_index]
            @assert issparse(matrix)
            column_offset = offsets[source_index]
            ncols = sizes[source_index]
            nnz_offset = nnz_offsets[source_index]
            nnz_size = nnz_sizes[source_index]
            sparse_nzval[(nnz_offset + 1):(nnz_offset + nnz_size)] = nzval(matrix)
            sparse_rowval[(nnz_offset + 1):(nnz_offset + nnz_size)] = rowval(matrix)
            sparse_colptr[(column_offset + 1):(column_offset + ncols)] = colptr(matrix)[1:ncols]
            sparse_colptr[(column_offset + 1):(column_offset + ncols)] .+= nnz_offset
            return nothing
        end
        sparse_colptr[end] = total_nnz + 1
        return nothing
    end

    return nothing
end

function concatenate_axis_dense_matrices(
    destination::DafWriter,
    other_axis::AbstractString,
    axis::AbstractString,
    matrix_property::AbstractString,
    sources::AbstractVector{<:DafReader};
    eltype::Type,
    empty_value::Maybe{StorageReal},
    offsets::AbstractVector{<:Integer},
    sizes::AbstractVector{<:Integer},
    matrices::AbstractVector{<:Maybe{<:StorageMatrix}},
    overwrite::Bool,
)::Nothing
    empty_dense_matrix!(destination, other_axis, axis, matrix_property, eltype; overwrite) do concatenated_matrix
        n_sources = length(sources)
        parallel_loop_wo_rng(
            1:n_sources;
            name = "concatenate_axis_dense_matrices",
            progress = DebugProgress(n_sources; desc = "concatenate_axis_dense_matrices"),
        ) do source_index
            source = sources[source_index]
            matrix = matrices[source_index]
            column_offset = offsets[source_index]
            ncols = sizes[source_index]
            if matrix === nothing
                concatenated_matrix[:, (column_offset + 1):(column_offset + ncols)] .=
                    require_empty_value_for_matrix(empty_value, matrix_property, other_axis, axis, source, destination)
            else
                @assert size(matrix)[2] == ncols
                concatenated_matrix[:, (column_offset + 1):(column_offset + ncols)] = matrix
            end
            return nothing
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
                dataset_axis,
                empty_value,
                merge_action,
                overwrite,
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
                    dataset_axis,
                    empty_value,
                    merge_action,
                    sparse_if_saves_storage_fraction,
                    overwrite,
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
                    merge_action,
                    overwrite,
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
                            merge_action,
                            overwrite,
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
                set_scalar!(destination, scalar_property, value; overwrite)
                return nothing
            end
        end
        @assert false

    elseif merge_action == CollectAxis
        if dataset_axis === nothing
            error(chomp("""
                  can't collect axis for the scalar: $(scalar_property)
                  of the daf data sets concatenated into the daf data: $(destination.name)
                  because no data set axis was created
                  """))
        end

        scalars = [get_scalar(source, scalar_property; default = nothing) for source in sources]
        eltype = reduce(merge_dtypes, scalars; init = typeof(empty_value))
        scalars = [eltype(scalar) for scalar in scalars]
        set_vector!(destination, dataset_axis, scalar_property, scalars; overwrite)
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
                set_vector!(destination, axis, vector_property, value; overwrite)
                return nothing
            end
        end
        @assert false

    elseif merge_action == CollectAxis
        if dataset_axis === nothing
            error(chomp("""
                  can't collect axis for the vector: $(vector_property)
                  of the axis: $(axis)
                  of the daf data sets concatenated into the daf data: $(destination.name)
                  because no data set axis was created
                  """))
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
        eltype = reduce(merge_dtypes, vectors; init = typeof(empty_value))

        if eltype != String
            sparse_saves = sparse_vectors_storage_fraction(empty_value, eltype, sizes, vectors)  # NOJET
            if sparse_saves >= sparse_if_saves_storage_fraction
                @assert empty_value === nothing || empty_value == 0
                sparse_vectors = sparsify_concatenated_vectors(vectors, eltype, sizes)
                concatenate_merge_sparse_vector(
                    destination,
                    axis,
                    dataset_axis,
                    vector_property;
                    eltype,
                    vectors = sparse_vectors,
                    overwrite,
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
            eltype,
            empty_value,
            vectors,
            overwrite,
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
    eltype::Type,
    vectors::AbstractVector{<:AbstractVector},
    overwrite::Bool,
)::Nothing
    nnz_offsets, nnz_sizes, total_nnz = nnz_arrays(vectors)

    empty_sparse_matrix!(
        destination,
        axis,
        dataset_axis,
        vector_property,
        eltype,
        total_nnz;
        overwrite,
    ) do sparse_colptr, sparse_rowval, sparse_nzval
        sparse_colptr[1] == 1
        n_sources = length(vectors)
        parallel_loop_wo_rng(
            1:n_sources;
            name = "concatenate_merge_sparse_vector",
            progress = DebugProgress(n_sources; desc = "concatenate_merge_sparse_vector"),
        ) do source_index
            vector = vectors[source_index]
            @assert issparse(vector)
            nnz_offset = nnz_offsets[source_index]
            nnz_size = nnz_sizes[source_index]
            sparse_nzval[(nnz_offset + 1):(nnz_offset + nnz_size)] = nzval(vector)
            sparse_rowval[(nnz_offset + 1):(nnz_offset + nnz_size)] = nzind(vector)
            sparse_colptr[source_index + 1] = nnz_offset + nnz_size + 1
            return nothing
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
    eltype::Type,
    empty_value::Maybe{StorageScalar},
    vectors::AbstractVector{<:Maybe{<:NamedVector}},
    overwrite::Bool,
)::Nothing
    empty_dense_matrix!(destination, axis, dataset_axis, vector_property, eltype; overwrite) do concatenated_matrix
        n_sources = length(sources)
        parallel_loop_wo_rng(
            1:n_sources;
            name = "concatenate_merge_dense_vector",
            progress = DebugProgress(n_sources; desc = "concatenate_merge_dense_vector"),
        ) do source_index
            source = sources[source_index]
            vector = vectors[source_index]
            if vector === nothing
                concatenated_matrix[:, source_index] .=
                    require_empty_value_for_vector(empty_value, vector_property, axis, source, destination)
            else
                concatenated_matrix[:, source_index] = vector
            end
            return nothing
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
                set_matrix!(destination, rows_axis, columns_axis, matrix_property, matrix; relayout = false, overwrite)
                return nothing
            end
        end
        @assert false

    elseif merge_action == CollectAxis
        error(chomp("""
              can't collect axis for the matrix: $(matrix_property)
              of the rows axis: $(rows_axis)
              and the columns axis: $(columns_axis)
              of the daf data sets concatenated into the daf data: $(destination.name)
              because that would create a 3D tensor, which is not supported
              """))

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
        error(chomp("""
              no empty value for the vector: $(vector_property)
              of the axis: $(axis)
              which is missing from the daf data: $(daf.name)
              concatenated into the daf data: $(destination.name)
              """))
    end
    return empty_value
end

function require_empty_value_for_matrix(
    empty_value::Maybe{StorageReal},
    matrix_property::AbstractString,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    daf::DafReader,
    destination::DafWriter,
)::StorageReal
    if empty_value === nothing
        error(chomp("""
              no empty value for the matrix: $(matrix_property)
              of the rows axis: $(rows_axis)
              and the columns axis: $(columns_axis)
              which is missing from the daf data: $(daf.name)
              concatenated into the daf data: $(destination.name)
              """))
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
        if key == scalar_property || key == "*"
            return merge_action
        end
    end
    return SkipProperty
end

function get_merge_action(merge::MergeData, vector_property::Tuple{AbstractString, AbstractString})::MergeAction
    for (key, merge_action) in reverse(merge)
        if key isa VectorKey
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
        if key isa MatrixKey
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

function sparse_vectors_storage_fraction(
    empty_value::Maybe{StorageReal},
    eltype::Type,
    sizes::AbstractVector{<:Integer},
    arrays::AbstractVector{<:Maybe{NamedArray}},
)::Float64
    sparse_size = 0
    dense_size = 0

    for (size, array) in zip(sizes, arrays)
        dense_size += size
        if array !== nothing
            array = array.array
            if array isa AbstractSparseArray
                sparse_size += nnz(array)
            else
                sparse_size += size
            end
        elseif empty_value != 0
            sparse_size += size
        end
    end

    indtype = indtype_for_size(dense_size)  # NOLINT
    dense_bytes = dense_size * sizeof(eltype)
    sparse_bytes = sparse_size * (sizeof(eltype) + sizeof(indtype))
    return (dense_bytes - sparse_bytes) / dense_bytes
end

function sparse_matrices_storage_fraction(
    empty_value::Maybe{StorageReal},
    eltype::Type,
    sizes::AbstractVector{<:Integer},
    arrays::AbstractVector{<:Maybe{NamedArray}},
    n_rows::Integer,
)::Float64
    if eltype <: AbstractString
        return -1.0  # UNTESTED
    end

    sparse_size = 0
    dense_size = 0

    total_n_columns = 0
    for (n_columns, array) in zip(sizes, arrays)
        total_n_columns += n_columns
        dense_size += n_rows * n_columns
        if array !== nothing
            array = array.array
            if array isa AbstractSparseArray
                sparse_size += nnz(array)
            else
                sparse_size += n_rows * n_columns
            end
        elseif empty_value != 0
            sparse_size += n_rows * n_columns
        end
    end

    indtype = indtype_for_size(dense_size)  # NOLINT
    dense_bytes = dense_size * sizeof(eltype)
    sparse_bytes = sparse_size * (sizeof(eltype) + sizeof(indtype)) + (total_n_columns + 1) * sizeof(indtype)
    return (dense_bytes - sparse_bytes) / dense_bytes
end

function sparsify_concatenated_vectors(
    vectors::AbstractArray{<:Maybe{<:NamedVector}},
    eltype::Type,
    sizes::AbstractArray{<:Integer},
)::Vector{SparseVector}
    sparse_vectors = Vector{SparseVector}(undef, length(vectors))

    n_sources = length(vectors)
    parallel_loop_wo_rng(
        1:n_sources;
        name = "sparsify_concatenated_vectors",
        progress = DebugProgress(n_sources; desc = "sparsify_concatenated_vectors"),
    ) do source_index
        vector = vectors[source_index]
        size = sizes[source_index]
        if vector === nothing
            sparse_vectors[source_index] = spzeros(eltype, size)
        else
            @assert length(vector) == size
            vector = vector.array
            if !issparse(vector)
                vector = sparse_vector(vector)  # NOLINT
            end
            sparse_vectors[source_index] = vector
        end
        return nothing
    end

    return sparse_vectors
end

function sparsify_concatenated_matrices(
    matrices::AbstractArray{<:Maybe{<:NamedMatrix}},
    eltype::Type,
    nrows::Integer,
    sizes::AbstractArray{<:Integer},
)::Vector{SparseMatrixCSC}
    @assert eltype <: Real
    sparse_matrices = Vector{SparseMatrixCSC}(undef, length(matrices))

    n_sources = length(matrices)
    parallel_loop_wo_rng(
        1:n_sources;
        name = "sparsify_concatenated_matrices",
        progress = DebugProgress(n_sources; desc = "sparsify_concatenated_matrices"),
    ) do source_index
        matrix = matrices[source_index]
        ncols = sizes[source_index]
        if matrix === nothing
            sparse_matrices[source_index] = spzeros(eltype, nrows, ncols)
        else
            @assert size(matrix) == (nrows, ncols)
            sparse_matrices[source_index] = matrix.array
        end
        return nothing
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
