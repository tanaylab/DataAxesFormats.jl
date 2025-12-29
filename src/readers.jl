"""
The [`DafReader`](@ref) interface specifies a high-level API for reading `Daf` data. This API is implemented here, on
top of the low-level [`FormatReader`](@ref) API. The high-level API provides thread safety so the low-level API can
(mostly) ignore this issue.

Each data set is given a name to use in error messages etc. You can explicitly set this name when creating a `Daf`
object. Otherwise, when opening an existing data set, if it contains a scalar "name" property, it is used. Otherwise
some reasonable default is used. In all cases, object names are passed through `unique_name` to avoid ambiguity.

Data properties are identified by a unique name given the axes they are based on. That is, there is a separate namespace
for scalar properties, vector properties for each specific axis, and matrix properties for each **unordered** pair of
axes.

For matrices, we keep careful track of their layout. Returned matrices are always in column-major layout, using
`relayout!` if necessary. As this is an expensive operation, we'll cache the result in memory. Similarly, we
cache the results of applying a query to the data. We allow clearing the cache to reduce memory usage, if necessary.

The data API is the high-level API intended to be used from outside the package, and is therefore re-exported from the
top-level `Daf` namespace. It provides additional functionality on top of the low-level [`FormatReader`](@ref)
implementation, accepting more general data types, automatically dealing with `relayout!` when needed. In
particular, it enforces single-writer multiple-readers for each data set, so the format code can ignore multi-threading
and still be thread-safe.

!!! note

    In the APIs below, when getting a value, specifying a `default` of `undef` means that it is an `error` for the value
    not to exist. In contrast, specifying a `default` of `nothing` means it is OK for the value not to exist, returning
    `nothing`. Specifying an actual value for `default` means it is OK for the value not to exist, returning the
    `default` instead. This is in spirit with, but not identical to, `undef` being used as a flag for array construction
    saying "there is no initializer". If you feel this is an abuse of the `undef` value, take some comfort in that it is
    the default value for the `default`, so you almost never have to write it explicitly in your code.
"""
module Readers

export axes_set
export axis_dict
export axis_entries
export axis_indices
export axis_length
export axis_vector
export axis_version_counter
export description
export empty_cache!
export get_matrix
export get_scalar
export get_vector
export has_axis
export has_matrix
export has_scalar
export has_vector
export matrices_set
export matrix_version_counter
export scalars_set
export vector_version_counter
export vectors_set

using ..Formats
using ..StorageTypes
using ConcurrentUtils
using NamedArrays
using SparseArrays
using TanayLabUtilities

import ..Formats
import ..Formats.CacheEntry
import ..Formats.CacheKey
import ..Formats.FormatReader  # For documentation.

"""
    has_scalar(daf::DafReader, name::AbstractString)::Bool

Check whether a scalar property with some `name` exists in `daf`.

```jldoctest
has_scalar(example_cells_daf(), "organism")

# output

true
```

```jldoctest
has_scalar(example_metacells_daf(), "organism")

# output

false
```
"""
function has_scalar(daf::DafReader, name::AbstractString)::Bool
    return Formats.with_data_read_lock(daf, "has_scalar of:", name) do
        result = Formats.format_has_scalar(daf, name)
        @debug "has_scalar daf: $(brief(daf)) name: $(name) result: $(result)"
        return result
    end
end

"""
    scalars_set(daf::DafReader)::AbstractSet{<:AbstractString}

The names of the scalar properties in `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.

```jldoctest
String.(scalars_set(example_cells_daf()))

# output

1-element Vector{String}:
 "organism"
```
"""
function scalars_set(daf::DafReader)::AbstractSet{<:AbstractString}
    return Formats.with_data_read_lock(daf, "scalars_set") do
        # Formats.assert_valid_cache(daf)
        result = Formats.get_scalars_set_through_cache(daf)
        @debug "scalars_set daf: $(brief(daf)) result: $(brief(result))"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

"""
    get_scalar(
        daf::DafReader,
        name::AbstractString;
        [default::Union{StorageScalar, Nothing, UndefInitializer} = undef]
    )::Maybe{StorageScalar}

Get the value of a scalar property with some `name` in `daf`.

If `default` is `undef` (the default), this first verifies the `name` scalar property exists in `daf`. Otherwise
`default` will be returned if the property does not exist.

```jldoctest
get_scalar(example_cells_daf(), "organism")

# output

"human"
```

```jldoctest
println(get_scalar(example_metacells_daf(), "organism"; default = nothing))

# output

nothing
```
"""
function get_scalar(
    daf::DafReader,
    name::AbstractString;
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
)::Maybe{StorageScalar}
    return Formats.with_data_read_lock(daf, "get_scalar of:", name) do
        # Formats.assert_valid_cache(daf)
        if default == undef
            require_scalar(daf, name)
        elseif !has_scalar(daf, name)
            @debug "get_scalar daf: $(brief(daf)) name: $(name) default result: $(brief(default))"
            return default
        end

        result = Formats.get_scalar_through_cache(daf, name)
        @debug "get_scalar daf: $(brief(daf)) name: $(name) default: $(brief(default)) result: $(brief(result))"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

function require_scalar(daf::DafReader, name::AbstractString)::Nothing
    if !Formats.format_has_scalar(daf, name)
        error(chomp("""
            missing scalar: $(name)
            in the daf data: $(daf.name)
            """))
    end
    return nothing
end

"""
    has_axis(daf::DafReader, axis::AbstractString)::Bool

Check whether some `axis` exists in `daf`.

```jldoctest
has_axis(example_cells_daf(), "metacell")

# output

false
```

```jldoctest
has_axis(example_metacells_daf(), "metacell")

# output

true
```
"""
function has_axis(daf::DafReader, axis::AbstractString)::Bool
    return Formats.with_data_read_lock(daf, "has_axis of:", axis) do
        result = Formats.format_has_axis(daf, axis; for_change = false)
        @debug "has_axis daf: $(brief(daf)) axis: $(axis) result: $(result)"
        return result
    end
end

"""
    axis_version_counter(daf::DafReader, axis::AbstractString)::UInt32

Return the version number of the axis. This is incremented every time [`delete_axis!`](@ref
DataAxesFormats.Writers.delete_axis!) is called. It is used by interfaces to other programming languages to safely cache
per-axis data.

!!! note

    This is purely in-memory per-instance, and **not** a global persistent version counter. That is, the version counter
    starts at zero even if opening a persistent disk `daf` data set.

```jldoctest
metacells = example_metacells_daf()
println(axis_version_counter(metacells, "type"))
delete_axis!(metacells, "type")
add_axis!(metacells, "type", ["Foo", "Bar", "Baz"])
println(axis_version_counter(metacells, "type"))

# output

0
1
```
"""
function axis_version_counter(daf::DafReader, axis::AbstractString)::UInt32
    # TRICKY: We don't track versions for scalars so we can use the string keys for the axes.
    return Formats.format_get_version_counter(daf, axis)
end

"""
    axes_set(daf::DafReader)::AbstractSet{<:AbstractString}

The names of the axes of `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.

```jldoctest
sort!(String.(axes_set(example_cells_daf())))

# output

4-element Vector{String}:
 "cell"
 "donor"
 "experiment"
 "gene"
```
"""
function axes_set(daf::DafReader)::AbstractSet{<:AbstractString}
    return Formats.with_data_read_lock(daf, "axes_set") do
        # Formats.assert_valid_cache(daf)
        result = Formats.get_axes_set_through_cache(daf)
        @debug "axes_set daf: $(brief(daf)) result: $(brief(result))"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

"""
    axis_vector(
        daf::DafReader,
        axis::AbstractString;
        [default::Union{Nothing, UndefInitializer} = undef]
    )::Maybe{AbstractVector{<:AbstractString}}

The array of unique names of the entries of some `axis` of `daf`. This is similar to doing [`get_vector`](@ref) for the
special `name` property, except that it returns a simple vector (array) of strings instead of a `NamedVector`.

If `default` is `undef` (the default), this verifies the `axis` exists in `daf`. Otherwise, the `default` is `nothing`,
which is returned if the `axis` does not exist.

```jldoctest
String.(axis_vector(example_metacells_daf(), "type"))

# output

4-element Vector{String}:
 "MEBEMP-E"
 "MEBEMP-L"
 "MPP"
 "memory-B"
```
"""
function axis_vector(
    daf::DafReader,
    axis::AbstractString;
    default::Union{Nothing, UndefInitializer} = undef,
)::Maybe{AbstractVector{<:AbstractString}}
    return Formats.with_data_read_lock(daf, "axis_vector of:", axis) do
        # Formats.assert_valid_cache(daf)
        result_prefix = ""
        if !Formats.format_has_axis(daf, axis; for_change = false)
            if default === nothing
                @debug "axis_vector daf: $(brief(daf)) axis: $(axis) default: nothing result: nothing"
                return nothing
            else
                result_prefix = "default "
                @assert default == undef
                require_axis(daf, "for: axis_vector", axis)
            end
        end

        result = Formats.get_axis_vector_through_cache(daf, axis)
        @debug "axis_vector daf: $(brief(daf)) axis: $(axis) default: $(brief(default)) $(result_prefix)result: $(brief(result))"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

"""
    axis_dict(daf::DafReader, axis::AbstractString)::AbstractDict{<:AbstractString, <:Integer}

Return a dictionary converting axis entry names to their integer index.

```jldoctest
axis_dict(example_metacells_daf(), "type")

# output

OrderedCollections.OrderedDict{AbstractString, Int64} with 4 entries:
  "MEBEMP-E" => 1
  "MEBEMP-L" => 2
  "MPP"      => 3
  "memory-B" => 4
```
"""
function axis_dict(daf::DafReader, axis::AbstractString)::AbstractDict{<:AbstractString, <:Integer}
    return Formats.with_data_read_lock(daf, "axis_dict of:", axis) do
        # Formats.assert_valid_cache(daf)
        require_axis(daf, "for: axis_dict", axis)
        result = Formats.get_axis_dict_through_cache(daf, axis)
        @debug "axis_dict daf: $(brief(daf)) result: $(brief(result))"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

"""
    axis_indices(
        daf::DafReader,
        axis::AbstractString,
        entries::AbstractVector{<:AbstractString};
        allow_empty::Bool = false,
    )::AbstractVector{<:Integer}

Return a vector of the indices of the `entries` in the `axis`. If `allow_empty`, the empty string is converted to a zero
index. Otherwise, all `entries` must exist in the `axis`.

```jldoctest
axis_indices(example_metacells_daf(), "type", ["MPP", ""]; allow_empty = true)

# output

2-element Vector{Int64}:
 3
 0
```
"""
function axis_indices(
    daf::DafReader,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString};
    allow_empty::Bool = false,
)::AbstractVector{<:Integer}
    dictionary = axis_dict(daf, axis)

    if allow_empty
        result = [entry == "" ? 0 : dictionary[entry] for entry in entries]
    else
        result = [dictionary[entry] for entry in entries]
    end

    @debug "axis_indices daf: $(brief(daf)) allow_empty: $(allow_empty) result: $(brief(result))"
    return result
end

"""
    axis_entries(
        daf::DafReader,
        axis::AbstractString,
        indices::Maybe{AbstractVector{<:Integer}} = nothing;
        allow_empty::Bool = false,
    )::AbstractVector{<:AbstractString}

Return a vector of the names of the entries with `indices` in the `axis`. If `allow_empty`, the zero (or negative) index
is converted to the empty string. Otherwise, all `indices` must be valid. If `indices` are no specified, returns the
vector of entry names. That is, `axis_entries(daf, axis)` is the same as `axis_vector(daf, axis)`.

```jldoctest
axis_entries(example_metacells_daf(), "type", [3, 0]; allow_empty = true)

# output

2-element Vector{AbstractString}:
 "MPP"
 ""
```
"""
function axis_entries(
    daf::DafReader,
    axis::AbstractString,
    indices::Maybe{AbstractVector{<:Integer}} = nothing;
    allow_empty::Bool = false,
)::AbstractVector{<:AbstractString}
    entries = axis_vector(daf, axis)

    if indices === nothing
        result = entries

    elseif allow_empty
        result = AbstractString[index <= 0 ? "" : entries[index] for index in indices]

    else
        result = entries[indices]
    end

    @debug "axis_entries daf: $(brief(daf)) allow_empty: $(allow_empty) result: $(brief(result))"
    return result
end

"""
    axis_length(daf::DafReader, axis::AbstractString)::Int64

The number of entries along the `axis` in `daf`.

This first verifies the `axis` exists in `daf`.

```jldoctest
axis_length(example_metacells_daf(), "type")

# output

4
```
"""
function axis_length(daf::DafReader, axis::AbstractString)::Int64
    return Formats.with_data_read_lock(daf, "axis_length of:", axis) do
        # Formats.assert_valid_cache(daf)
        require_axis(daf, "for: axis_length", axis)
        result = Formats.format_axis_length(daf, axis)
        @debug "axis_length daf: $(brief(daf)) axis: $(axis) result: $(result)"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

function require_axis(daf::DafReader, what_for::AbstractString, axis::AbstractString; for_change::Bool = false)::Nothing
    if !Formats.format_has_axis(daf, axis; for_change)
        error(chomp("""
            missing axis: $(axis)
            $(what_for)
            of the daf data: $(daf.name)
            """))
    end
    return nothing
end

"""
    has_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Bool

Check whether a vector property with some `name` exists for the `axis` in `daf`. This is always true for the special
`name` property.

This first verifies the `axis` exists in `daf`.

```jldoctest
has_vector(example_cells_daf(), "cell", "type")

# output

false
```

```jldoctest
has_vector(example_metacells_daf(), "metacell", "type")

# output

true
```
"""
function has_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Bool
    return Formats.with_data_read_lock(daf, "has_vector of:", name, "of:", axis) do
        # Formats.assert_valid_cache(daf)
        require_axis(daf, "for has_vector: $(name)", axis)
        result = name == "name" || name == "index" || Formats.format_has_vector(daf, axis, name)
        @debug "has_vector daf: $(brief(daf)) axis: $(axis) name: $(name) result: $(result)"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

"""
    vector_version_counter(daf::DafReader, axis::AbstractString, name::AbstractString)::UInt32

Return the version number of the vector. This is incremented every time
[`set_vector!`](@ref DataAxesFormats.Writers.set_vector!),
[`empty_dense_vector!`](@ref DataAxesFormats.Writers.empty_dense_vector!) or
[`empty_sparse_vector!`](@ref DataAxesFormats.Writers.empty_sparse_vector!) are called. It is used by interfaces to
safely cache per-vector data.

!!! note

    This is purely in-memory per-instance, and **not** a global persistent version counter. That is, the version counter
    starts at zero even if opening a persistent disk `daf` data set.

```jldoctest
metacells = example_metacells_daf()
println(vector_version_counter(metacells, "type", "color"))
set_vector!(metacells, "type", "color", string.(collect(1:4)); overwrite = true)
println(vector_version_counter(metacells, "type", "color"))

# output

1
2
```
"""
function vector_version_counter(daf::DafReader, axis::AbstractString, name::AbstractString)::UInt32
    result = Formats.format_get_version_counter(daf, (axis, name))
    @debug "vector_version_counter daf: $(brief(daf)) axis: $(axis) name: $(name) result: $(result)"
    return result
end

"""
    vectors_set(daf::DafReader, axis::AbstractString)::AbstractSet{<:AbstractString}

The names of the vector properties for the `axis` in `daf`, **not** including the special `name` property.

This first verifies the `axis` exists in `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.

```jldoctest
sort!(String.(vectors_set(example_cells_daf(), "cell")))

# output

2-element Vector{String}:
 "donor"
 "experiment"
```
"""
function vectors_set(daf::DafReader, axis::AbstractString)::AbstractSet{<:AbstractString}
    return Formats.with_data_read_lock(daf, "vectors_set of:", axis) do
        # Formats.assert_valid_cache(daf)
        require_axis(daf, "for: vectors_set", axis)
        result = Formats.get_vectors_set_through_cache(daf, axis)
        @debug "vectors_set daf: $(brief(daf)) axis: $(axis) result: $(brief(result))"
        # Formats.assert_valid_cache(daf)
        return result
    end
end

"""
    get_vector(
        daf::DafReader,
        axis::AbstractString,
        name::AbstractString;
        [default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef]
    )::Maybe{NamedVector}

Get the vector property with some `name` for some `axis` in `daf`. The names of the result are the names of the vector
entries (same as returned by [`axis_vector`](@ref)). The special property `name` returns an array whose values are also the
(read-only) names of the entries of the axis.

This first verifies the `axis` exists in `daf`. If `default` is `undef` (the default), this first verifies the `name`
vector exists in `daf`. Otherwise, if `default` is `nothing`, it will be returned. If it is a [`StorageVector`](@ref),
it has to be of the same size as the `axis`, and is returned. If it is a [`StorageScalar`](@ref). Otherwise, a new
`Vector` is created of the correct size containing the `default`, and is returned.

```jldoctest
get_vector(example_metacells_daf(), "type", "color")

# output

4-element Named SparseArrays.ReadOnly{String, 1, Vector{String}}
type     │
─────────┼────────────
MEBEMP-E │   "#eebb6e"
MEBEMP-L │      "plum"
MPP      │      "gold"
memory-B │ "steelblue"
```
"""
function get_vector(
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
)::Maybe{NamedArray}
    return Formats.with_data_read_lock(daf, "get_vector of:", name, "of:", axis) do
        # Formats.assert_valid_cache(daf)
        require_axis(daf, "for the vector: $(name)", axis)

        if default isa StorageVector
            require_axis_length(daf, length(default), "default for the vector: $(name)", axis)
            if default isa NamedVector
                require_dim_name(daf, axis, "default dim name", dimnames(default, 1))
                require_axis_names(daf, axis, "entry names of the: default", names(default, 1))
            end
        end

        if name == "name" || name == "index"
            values = Formats.get_axis_vector_through_cache(daf, axis)
            if name == "index"
                dictionary = Formats.get_axis_dict_through_cache(daf, axis)
                values = getindex.(Ref(dictionary), values)
            end
            vector = Formats.as_named_vector(daf, axis, values)
            @debug "get_vector daf: $(brief(daf)) axis: $(axis) name: $(name) default: $(brief(default)) result: $(brief(vector))"
            # Formats.assert_valid_cache(daf)
            return vector
        end

        if Formats.format_has_vector(daf, axis, name)
            result_prefix = ""
            vector = Formats.get_vector_through_cache(daf, axis, name)
            @assert length(vector) == Formats.format_axis_length(daf, axis) """
                format_get_vector for daf format: $(nameof(typeof(daf)))
                returned vector length: $(length(vector))
                instead of axis: $(axis)
                length: $(axis_length(daf, axis))
                in the daf data: $(daf.name)
                """
        else
            result_prefix = "default "
            if default === nothing
                vector = nothing
            elseif default == undef
                require_vector(daf, axis, name)
                @assert false
            elseif default isa StorageVector
                vector = default
            elseif default == 0
                vector = spzeros(typeof(default), Formats.format_axis_length(daf, axis))
            else
                @assert default isa StorageScalar
                vector = fill(default, Formats.format_axis_length(daf, axis))
            end
        end

        if vector !== nothing
            if eltype(vector) <: AbstractString
                vector = Formats.read_only_array(vector)
            end

            vector = Formats.as_named_vector(daf, axis, vector)
        end

        @debug "get_vector daf: $(brief(daf)) axis: $(axis) name: $(name) default: $(brief(default)) $(result_prefix)result: $(brief(vector))"
        # Formats.assert_valid_cache(daf)
        return vector
    end
end

function require_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if !Formats.format_has_vector(daf, axis, name)
        error(chomp("""
                    missing vector: $(name)
                    for the axis: $(axis)
                    in the daf data: $(daf.name)
                    """))
    end
    return nothing
end

"""
    has_matrix(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [relayout::Bool = true]
    )::Bool

Check whether a matrix property with some `name` exists for the `rows_axis` and the `columns_axis` in `daf`. Since this
is Julia, this means a column-major matrix. A daf may contain two copies of the same data, in which case it would report
the matrix under both axis orders.

If `relayout` (the default), this will also check whether the data exists in the other layout (that is, with flipped
axes).

This first verifies the `rows_axis` and `columns_axis` exists in `daf`.

```jldoctest
has_matrix(example_cells_daf(), "gene", "cell", "UMIs")

# output

true
```
"""
function has_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool = true,
)::Bool
    return Formats.with_data_read_lock(daf, "has_matrix of:", name, "of:", rows_axis, "and:", columns_axis) do
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        result = Formats.with_cache_read_lock(
            daf,
            "cache for has_matrix of:",
            name,
            "of:",
            rows_axis,
            "and:",
            columns_axis,
        ) do
            return Formats.format_has_matrix(daf, rows_axis, columns_axis, name) ||
                   (relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name))
        end
        @debug "has_matrix daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) relayout: $(relayout) result: $(brief(result))"
        return result
    end
end

"""
    matrices_set(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString;
        [relayout::Bool = true]
    )::AbstractSet{<:AbstractString}

The names of the matrix properties for the `rows_axis` and `columns_axis` in `daf`.

If `tensors`, this will condense the list of tensor matrices (`<`_tensor_axis_entry_`>_name`) into a single entry called
`/<`_tensor_axis_name_`>/_matrix`. This makes the reasonable assumption that names do not contain the `/` character
(which wouldn't work in files and H5DF storage formats, anyway).

If `relayout` (default), then this will include the names of matrices that exist in the other layout (that is, with
flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`.

!!! note

    There's no immutable set type in Julia for us to return. If you do modify the result set, bad things *will* happen.

```jldoctest
String.(matrices_set(example_cells_daf(), "gene", "cell"))

# output

1-element Vector{String}:
 "UMIs"
```
"""
function matrices_set(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString;
    tensors::Bool = true,
    relayout::Bool = true,
)::AbstractSet{<:AbstractString}
    return Formats.with_data_read_lock(daf, "matrices_set of:", rows_axis, "and:", columns_axis) do
        # Formats.assert_valid_cache(daf)
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, "for the rows of: matrices_set", rows_axis)
        require_axis(daf, "for the columns of: matrices_set", columns_axis)

        if !relayout
            names = Formats.get_matrices_set_through_cache(daf, rows_axis, columns_axis)
            can_modify_names = false
            candidate_cached_names = Formats.get_matrices_set_through_cache(daf, columns_axis, rows_axis)
            Formats.with_cache_read_lock(daf, "cache for matrices_set of:", rows_axis, "and:", columns_axis) do
                for candidate_cached_name in candidate_cached_names
                    if !(candidate_cached_name in names)
                        candidate_cache_key = Formats.matrix_cache_key(rows_axis, columns_axis, candidate_cached_name)
                        if haskey(daf.internal.cache, candidate_cache_key)
                            if !can_modify_names
                                names = Set{AbstractString}(names)
                                can_modify_names = true
                            end
                            push!(names, candidate_cached_name)
                        end
                    end
                end
            end
        else
            can_modify_names = false
            names = Formats.get_through_cache(
                daf,
                Formats.matrices_set_cache_key(rows_axis, columns_axis; relayout = true),
                AbstractSet{<:AbstractString},
                daf.internal.cache_group,
            ) do
                first_names = Formats.get_matrices_set_through_cache(daf, rows_axis, columns_axis)
                second_names = Formats.get_matrices_set_through_cache(daf, columns_axis, rows_axis)
                names = union(first_names, second_names)
                return names
            end
        end

        if tensors
            matrices_per_tensor_per_axes_per_axis = Dict{
                AbstractString,
                Dict{Tuple{AbstractString, AbstractString}, Dict{AbstractString, Set{AbstractString}}},
            }()
            all_tensor_matrices = Set{AbstractString}()
            collect_tensors(daf, rows_axis, columns_axis; matrices_per_tensor_per_axes_per_axis, all_tensor_matrices)
            if !can_modify_names
                names = Set{AbstractString}(names)
            end
            filter!(names) do name
                return !(name in all_tensor_matrices)
            end
            for (tensor, matrices_per_tensor) in matrices_per_tensor_per_axes_per_axis
                matrices = get(matrices_per_tensor, (rows_axis, columns_axis), nothing)
                if matrices !== nothing
                    for matrix in keys(matrices)
                        push!(names, "/$(tensor)/_$(matrix)")
                    end
                end
            end
        end

        @debug "matrices_set daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) relayout: $(relayout) result: $(brief(names))"
        # Formats.assert_valid_cache(daf)
        return names
    end
end

function require_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(daf)
    if !has_matrix(daf, rows_axis, columns_axis, name; relayout)
        if relayout
            extra = "\n(and the other way around)"
        else
            extra = ""
        end
        error(chomp("""
              missing matrix: $(name)
              for the rows axis: $(rows_axis)
              and the columns axis: $(columns_axis)$(extra)
              in the daf data: $(daf.name)
              """))
    end
    return nothing
end

"""
    get_matrix(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [default::Union{StorageScalarBase, StorageMatrix, Nothing, UndefInitializer} = undef,
        relayout::Bool = true]
    )::Maybe{NamedMatrix}

Get the column-major matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. The names of the
result axes are the names of the relevant axes entries (same as returned by [`axis_vector`](@ref)).

If `relayout` (the default), then if the matrix is only stored in the other memory layout (that is, with flipped axes),
then automatically call `relayout!` to compute the result. If `daf` isa [`DafWriter`](@ref), then store the
result for future use; otherwise, just cache it as [`MemoryData`](@ref CacheGroup). This may lock up very large amounts
of memory; you can call [`empty_cache!`](@ref) to release it.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If `default` is `undef` (the default), this first
verifies the `name` matrix exists in `daf`. Otherwise, if `default` is `nothing`, it is returned. If `default` is a
`StorageMatrix`, it has to be of the same size as the `rows_axis` and `columns_axis`, and is returned. Otherwise, a new
`Matrix` is created of the correct size containing the `default`, and is returned.

```jldoctest
get_matrix(example_metacells_daf(), "gene", "metacell", "fraction")

# output

683×7 Named Matrix{Float32}
gene ╲ metacell │    M1671.28     M2357.20  …      M756.63      M412.08
────────────────┼──────────────────────────────────────────────────────
RPL22           │  0.00447666    0.0041286  …   0.00434327   0.00373581
PARK7           │  8.52301f-5  0.000154199     0.000108019   6.50531f-5
ENO1            │ 0.000464448  0.000482609     0.000248241   4.22228f-5
PRDM2           │    2.053f-5   2.85439f-5      2.46575f-5  0.000151486
HP1BP3          │ 0.000107137  0.000110915       8.9043f-5   0.00012099
CDC42           │ 0.000153017  0.000207847     0.000152447  0.000176377
HNRNPR          │ 0.000122974    6.7171f-5      7.09771f-5    6.7083f-5
RPL11           │    0.010306    0.0110606       0.0109086    0.0124251
⋮                           ⋮            ⋮  ⋱            ⋮            ⋮
NRIP1           │ 0.000155974  0.000361428     0.000197766   2.79487f-5
ATP5PF          │  8.62855f-5  0.000125912     0.000121949   8.22312f-5
CCT8            │ 0.000104152   7.55233f-5      0.00011572   4.13243f-5
SOD1            │ 0.000177344  0.000147838     0.000104723  0.000103708
SON             │ 0.000280491  0.000262015     0.000170829   0.00032361
ATP5PO          │ 0.000134007  0.000123143      0.00018833   9.73498f-5
TTC3            │ 0.000111978   0.00011131     0.000100166  0.000122469
HMGN1           │ 0.000345676  0.000287754  …  0.000264526  0.000160654
```
"""
function get_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalarBase, StorageMatrix, Nothing, UndefInitializer} = undef,
    relayout::Bool = true,
)::Maybe{NamedArray}
    return Formats.with_data_read_lock(daf, "get_matrix of:", name, "of:", rows_axis, "and:", columns_axis) do
        # Formats.assert_valid_cache(daf)
        relayout = relayout && rows_axis != columns_axis

        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        if default isa StorageMatrix
            require_column_major(default)
            require_axis_length(daf, size(default, Rows), "rows of the default for the matrix: $(name)", rows_axis)
            require_axis_length(
                daf,
                size(default, Columns),
                "columns of the default for the matrix: $(name)",
                columns_axis,
            )
            if default isa NamedMatrix
                require_dim_name(daf, rows_axis, "default rows dim name", dimnames(default, 1); prefix = "rows")
                require_dim_name(
                    daf,
                    columns_axis,
                    "default columns dim name",
                    dimnames(default, 2);
                    prefix = "columns",
                )
                require_axis_names(daf, rows_axis, "row names of the: default", names(default, 1))
                require_axis_names(daf, columns_axis, "column names of the: default", names(default, 2))
            end
        end

        if Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
            result_prefix = ""
            matrix = Formats.get_matrix_through_cache(daf, rows_axis, columns_axis, name)
            assert_valid_matrix(daf, rows_axis, columns_axis, name, matrix)
        elseif relayout && Formats.format_has_cached_matrix(daf, columns_axis, rows_axis, name)
            result_prefix = "relayout "
            matrix = Formats.get_relayout_matrix_through_cache(daf, rows_axis, columns_axis, name)
            assert_valid_matrix(daf, rows_axis, columns_axis, name, matrix)
        else
            result_prefix = "default "
            if default === nothing
                matrix = nothing
            elseif default == undef
                require_matrix(daf, rows_axis, columns_axis, name; relayout)
                @assert false
            elseif default isa StorageMatrix
                matrix = default
            elseif default == 0
                matrix = spzeros(
                    typeof(default),
                    Formats.format_axis_length(daf, rows_axis),
                    Formats.format_axis_length(daf, columns_axis),
                )
            else
                @assert default isa StorageScalar
                matrix = fill(  # NOJET
                    default,
                    Formats.format_axis_length(daf, rows_axis),
                    Formats.format_axis_length(daf, columns_axis),
                )
            end
        end

        if matrix !== nothing
            matrix = Formats.as_named_matrix(daf, rows_axis, columns_axis, matrix)
        end

        @debug "get_matrix daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) default: $(brief(default)) $(result_prefix)result: $(brief(matrix))"
        # # Formats.assert_valid_cache(daf)
        return matrix
    end
end

function assert_valid_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::AbstractMatrix,
)::Nothing
    @assert size(matrix, Rows) == Formats.format_axis_length(daf, rows_axis) """
        format_get_matrix: $(name)
        for the daf format: $(nameof(typeof(daf)))
        returned matrix rows: $(size(matrix, Rows))
        instead of axis: $(rows_axis)
        length: $(axis_length(daf, rows_axis))
        in the daf data: $(daf.name)
        """

    @assert size(matrix, Columns) == Formats.format_axis_length(daf, columns_axis) """
        format_get_matrix: $(name)
        for the daf format: $(nameof(typeof(daf)))
        returned matrix columns: $(size(matrix, Columns))
        instead of axis: $(columns_axis)
        length: $(axis_length(daf, columns_axis))
        in the daf data: $(daf.name)
        """

    @assert major_axis(matrix) == Columns """
        format_get_matrix for daf format: $(nameof(typeof(daf)))
        returned non column-major matrix: $(brief(matrix))
        """

    return nothing
end

"""
    matrix_version_counter(
        daf::DafReader,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString
    )::UInt32

Return the version number of the matrix. The order of the axes does not matter. This is incremented every time
[`set_matrix!`](@ref DataAxesFormats.Writers.set_matrix!),
[`empty_dense_matrix!`](@ref DataAxesFormats.Writers.empty_dense_matrix!) or
[`empty_sparse_matrix!`](@ref DataAxesFormats.Writers.empty_sparse_matrix!) are called. It is used by interfaces to other
programming languages to safely cache per-matrix data.

!!! note

    This is purely in-memory per-instance, and **not** a global persistent version counter. That is, the version counter
    starts at zero even if opening a persistent disk `daf` data set.

```jldoctest
metacells = example_metacells_daf()
println(matrix_version_counter(metacells, "gene", "metacell", "fraction"))
set_matrix!(metacells, "gene", "metacell", "fraction", rand(Float32, 683, 7); overwrite = true)
println(matrix_version_counter(metacells, "gene", "metacell", "fraction"))

# output

1
2
```
"""
function matrix_version_counter(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::UInt32
    if columns_axis < rows_axis
        rows_axis, columns_axis = columns_axis, rows_axis
    end
    result = Formats.format_get_version_counter(daf, (rows_axis, columns_axis, name))
    @debug "matrix_version_counter daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) result: $(result)"
    return result
end

function require_column_major(matrix::StorageMatrix)::Nothing
    if major_axis(matrix) != Columns
        error("type not in column-major layout: $(brief(matrix))")
    end
end

function require_axis_length(
    daf::DafReader,
    what_length::StorageInteger,
    vector_name::AbstractString,
    axis::AbstractString,
)::Nothing
    if what_length != Formats.format_axis_length(daf, axis)
        error(chomp("""
            the length: $(what_length)
            of the $(vector_name)
            is different from the length: $(Formats.format_axis_length(daf, axis))
            of the axis: $(axis)
            in the daf data: $(daf.name)
            """))
    end
    return nothing
end

function require_dim_name(
    daf::DafReader,
    axis::AbstractString,
    what::AbstractString,
    name::Union{Symbol, AbstractString};
    prefix::AbstractString = "",
)::Nothing
    if prefix != ""
        prefix = prefix * " "
    end
    string_name = String(name)
    if string_name != axis
        error(chomp("""
                    $(what): $(string_name)
                    is different from the $(prefix)axis: $(axis)
                    in the daf data: $(daf.name)
                    """))
    end
end

function require_axis_names(
    daf::DafReader,
    axis::AbstractString,
    what::AbstractString,
    names::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_read_lock(daf)
    expected_names = axis_vector(daf, axis)
    if names != expected_names
        error(chomp("""
                    $(what)
                    mismatch the entry names of the axis: $(axis)
                    in the daf data: $(daf.name)
                    """))
    end
end

"""
    description(
        daf::DafReader[;
        deep::Bool = false,
        cache::Bool = false,
        tensors::Bool = true,
    )::AbstractString

Return a (multi-line) description of the contents of `daf`. This tries to hit a sweet spot between usefulness and
terseness. If `cache`, also describes the content of the cache. If `deep`, also describes any data set nested inside
this one (if any).

If `tensors` is set, this will include a `tensors` section which will condense the long list of tensor matrices.

```jldoctest
print(description(example_chain_daf(); deep = true))

# output

name: chain!
type: Write Chain
scalars:
  organism: "human"
axes:
  cell: 856 entries
  donor: 95 entries
  experiment: 23 entries
  gene: 683 entries
  metacell: 7 entries
  type: 4 entries
vectors:
  cell:
    donor: 856 x Str (Dense)
    experiment: 856 x Str (Dense)
    metacell: 856 x Str (Dense)
  donor:
    age: 95 x UInt32 (Dense)
    sex: 95 x Str (Dense)
  gene:
    is_lateral: 683 x Bool (Dense; 438 (64%) true)
    is_marker: 683 x Bool (Dense; 650 (95%) true)
  metacell:
    type: 7 x Str (Dense)
  type:
    color: 4 x Str (Dense)
matrices:
  cell,gene:
    UMIs: 856 x 683 x UInt8 in Columns (Dense)
  gene,cell:
    UMIs: 683 x 856 x UInt8 in Columns (Dense)
  gene,metacell:
    fraction: 683 x 7 x Float32 in Columns (Dense)
  metacell,metacell:
    edge_weight: 7 x 7 x Float32 in Columns (Dense)
chain:
- name: cells!
  type: MemoryDaf
  scalars:
    organism: "human"
  axes:
    cell: 856 entries
    donor: 95 entries
    experiment: 23 entries
    gene: 683 entries
  vectors:
    cell:
      donor: 856 x Str (Dense)
      experiment: 856 x Str (Dense)
    donor:
      age: 95 x UInt32 (Dense)
      sex: 95 x Str (Dense)
    gene:
      is_lateral: 683 x Bool (Dense; 438 (64%) true)
  matrices:
    cell,gene:
      UMIs: 856 x 683 x UInt8 in Columns (Dense)
    gene,cell:
      UMIs: 683 x 856 x UInt8 in Columns (Dense)
- name: metacells!
  type: MemoryDaf
  axes:
    cell: 856 entries
    gene: 683 entries
    metacell: 7 entries
    type: 4 entries
  vectors:
    cell:
      metacell: 856 x Str (Dense)
    gene:
      is_marker: 683 x Bool (Dense; 650 (95%) true)
    metacell:
      type: 7 x Str (Dense)
    type:
      color: 4 x Str (Dense)
  matrices:
    gene,metacell:
      fraction: 683 x 7 x Float32 in Columns (Dense)
    metacell,metacell:
      edge_weight: 7 x 7 x Float32 in Columns (Dense)
```
"""
function description(daf::DafReader; cache::Bool = false, deep::Bool = false, tensors::Bool = true)::String
    return Formats.with_data_read_lock(daf, "description") do
        lines = String[]
        description(daf, "", lines; cache, deep, tensors)
        push!(lines, "")
        return join(lines, "\n")
    end
end

function description(
    daf::DafReader,
    indent::AbstractString,
    lines::Vector{String};
    cache::Bool,
    deep::Bool,
    tensors::Bool,
)::Nothing
    push!(lines, "$(indent)name: $(daf.name)")
    if startswith(indent, "-")
        indent = " " * indent[2:end]
        @assert indent isa AbstractString
    end

    Formats.format_description_header(daf, indent, lines, deep)

    scalars_description(daf, indent, lines) # NOJET

    axes = collect(axes_set(daf))

    if !isempty(axes)
        sort!(axes)

        if tensors
            matrices_per_tensor_per_axes_per_axis = Dict{
                AbstractString,
                Dict{Tuple{AbstractString, AbstractString}, Dict{AbstractString, Set{AbstractString}}},
            }()
            for rows_axis in axes
                for columns_axis in axes
                    collect_tensors(daf, rows_axis, columns_axis; matrices_per_tensor_per_axes_per_axis)
                end
            end
        end

        axes_description(daf, axes, indent, lines) # NOJET
        vectors_description(daf, axes, indent, lines) # NOJET
        matrices_description(daf, axes, indent, lines; tensors) # NOJET
        if cache
            cache_description(daf, indent, lines)  # NOJET # UNTESTED
        end

        if tensors && !isempty(matrices_per_tensor_per_axes_per_axis)
            tensors_description(daf, indent, lines, matrices_per_tensor_per_axes_per_axis)  # NOJET
        end
    end

    Formats.format_description_footer(daf, indent, lines; cache, deep, tensors)  # NOJET
    return nothing
end

function scalars_description(daf::DafReader, indent::AbstractString, lines::Vector{String})::Nothing
    scalars = collect(Formats.get_scalars_set_through_cache(daf))
    if !isempty(scalars)
        sort!(scalars)
        push!(lines, "$(indent)scalars:")
        for scalar in scalars
            push!(lines, "$(indent)  $(scalar): $(brief(get_scalar(daf, scalar)))")
        end
    end
    return nothing
end

function axes_description(
    daf::DafReader,
    axes::AbstractVector{<:AbstractString},
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    push!(lines, "$(indent)axes:")
    for axis in axes
        push!(lines, "$(indent)  $(axis): $(axis_length(daf, axis)) entries")
    end
    return nothing
end

function vectors_description(
    daf::DafReader,
    axes::AbstractVector{<:AbstractString},
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    is_first = true
    for axis in axes
        vectors = collect(Formats.get_vectors_set_through_cache(daf, axis))
        if !isempty(vectors)
            if is_first
                push!(lines, "$(indent)vectors:")
                is_first = false
            end
            sort!(vectors)
            push!(lines, "$(indent)  $(axis):")
            for vector in vectors
                push!(lines, "$(indent)    $(vector): $(brief(base_array(get_vector(daf, axis, vector))))")
            end
        end
    end
    return nothing
end

function matrices_description(
    daf::DafReader,
    axes::AbstractVector{<:AbstractString},
    indent::AbstractString,
    lines::Vector{String};
    tensors::Bool,
)::Nothing
    is_first_matrix = true
    for rows_axis in axes
        for columns_axis in axes
            matrices = collect(matrices_set(daf, rows_axis, columns_axis; relayout = false, tensors))
            if !isempty(matrices)
                sort!(matrices)
                is_first_axes = true
                for matrix in matrices
                    if !tensors || matrix[1] != '/'
                        data = base_array(get_matrix(daf, rows_axis, columns_axis, matrix; relayout = false))
                        if is_first_matrix
                            push!(lines, "$(indent)matrices:")
                            is_first_matrix = false
                        end
                        if is_first_axes
                            push!(lines, "$(indent)  $(rows_axis),$(columns_axis):")
                            is_first_axes = false
                        end
                        push!(lines, "$(indent)    $(matrix): " * brief(data))
                    end
                end
            end
        end
    end
    return nothing
end

function tensors_description(
    daf::DafReader,
    indent::AbstractString,
    lines::Vector{String},
    matrices_per_tensor_per_axes_per_axis::Dict{
        AbstractString,
        Dict{Tuple{AbstractString, AbstractString}, Dict{AbstractString, Set{AbstractString}}},
    },
)::Nothing
    tensor_axes = collect(keys(matrices_per_tensor_per_axes_per_axis))
    sort!(tensor_axes)
    push!(lines, "$(indent)tensors:")
    for tensor_axis in tensor_axes
        tensor_axis_entries_set = keys(axis_dict(daf, tensor_axis))
        n_tensor_axis_entries = length(tensor_axis_entries_set)

        push!(lines, "$(indent)  $(tensor_axis):")

        matrices_per_tensor_per_axes = matrices_per_tensor_per_axes_per_axis[tensor_axis]
        axes = collect(keys(matrices_per_tensor_per_axes))
        sort!(axes)

        for (rows_axis, columns_axis) in axes
            push!(lines, "$(indent)    $(rows_axis),$(columns_axis):")

            matrices_per_tensor = matrices_per_tensor_per_axes[(rows_axis, columns_axis)]
            tensors = collect(keys(matrices_per_tensor))
            sort!(tensors)
            for tensor_name in tensors
                matrices = matrices_per_tensor[tensor_name]
                n_tensor_matrices = length(matrices)

                if n_tensor_matrices == n_tensor_axis_entries
                    suffix = ""
                else
                    suffix = " ($(n_tensor_matrices) out of $(n_tensor_axis_entries))"  # UNTESTED
                end

                counters_per_text = Dict{AbstractString, Union{Int, Tuple{Int, Int, Int64, Int64}}}()
                for matrix in values(matrices)
                    data = base_array(get_matrix(daf, rows_axis, columns_axis, matrix; relayout = false))
                    text = brief(data)
                    parts = split(text, " ")
                    sparse_index = findfirst((parts .== "Sparse") .| (parts .== "(Sparse"))
                    counted = nothing
                    counted_index = nothing
                    if sparse_index !== nothing
                        counted = nnz(data)
                        counted_index = sparse_index + 1
                        parts[counted_index] = "."
                        parts[counted_index + 1] = "."
                    else
                        true_index = findfirst(parts .== "true)")
                        if true_index !== nothing
                            counted = sum(data)
                            counted_index = true_index - 2
                            parts[true_index - 1] = "."
                            parts[true_index - 2] = "."
                        end
                    end

                    if counted !== nothing
                        @assert counted_index !== nothing
                        parts[counted_index] = "."
                        parts[counted_index + 1] = "."
                        text = join(parts, " ")
                        counters = get(counters_per_text, text, (0, counted_index, Int64(0), Int64(0)))
                        counters_per_text[text] =
                            (counters[1] + 1, counters[2], counters[3] + counted, counters[4] + length(data))
                    else
                        @assert counted_index === nothing  # UNTESTED
                        counters_per_text[text] = get(counters_per_text, text, 0) + 1  # NOJET # UNTESTED
                    end
                end

                if length(counters_per_text) == 1
                    for (text, counters) in counters_per_text
                        push!(lines, "$(indent)      $(tensor_name)$(suffix): $(format_counters(counters, text))")
                    end
                else
                    push!(lines, "$(indent)      $(tensor_name)$(suffix):")

                    texts = collect(keys(counters_per_text))
                    sort!(texts)
                    for text in texts
                        counters = counters_per_text[text]
                        push!(lines, "$(indent)      - $(format_counters(counters, text))")
                    end
                end
            end
        end
    end

    return nothing
end

function format_counters(counters::Int, text::AbstractString)::AbstractString  # UNTESTED
    return "$(counters) X $(text)"
end

function format_counters(counters::Tuple{Int, Int, Int64, Int64}, text::AbstractString)::AbstractString
    count, counted_index, counted, out_of = counters
    parts = split(text, " ")
    @assert parts[counted_index] == "."
    @assert parts[counted_index + 1] == "."
    parts[counted_index] = "$(counted)"
    parts[counted_index + 1] = "($(percent(counted, out_of)))"
    return "$(count) X $(join(parts, " "))"
end

function cache_description(daf::DafReader, indent::AbstractString, lines::Vector{String})::Nothing  # UNTESTED
    is_first = true
    Formats.with_cache_read_lock(daf, "cache for: description") do
        cache_keys = collect(keys(daf.internal.cache))
        sort!(cache_keys; lt = cache_key_is_less)
        for key in cache_keys
            if is_first
                push!(lines, "$(indent)cache:")
                is_first = false
            end
            cache_entry = daf.internal.cache[key]
            value = cache_entry.data
            if value isa AbstractArray
                value = base_array(value)
            end
            key = replace("$(key)", "'" => "''")
            push!(lines, "$(indent)  '$(key)': ($(cache_entry.cache_group)) $(brief(value))")
        end
    end
    return nothing
end

function cache_key_is_less(left::CacheKey, right::CacheKey)::Bool  # UNTESTED
    left_type, left_key = left
    right_type, right_key = right
    if !(left_key isa Tuple)
        left_key = (left_key,)
    end
    if !(right_key isa Tuple)
        right_key = (right_key,)
    end
    return (left_type, left_key) < (right_type, right_key)
end

function base_array(array::AbstractArray)::AbstractArray
    return array
end

function base_array(array::SparseArrays.ReadOnly)::AbstractArray
    return base_array(parent(array))
end

function base_array(array::NamedArray)::AbstractArray
    return base_array(array.array)
end

function TanayLabUtilities.Brief.brief(daf::DafReader; name::Maybe{AbstractString} = nothing)::AbstractString
    if name === nothing
        name = daf.name
    end
    return "$(nameof(typeof(daf))) $(name)"
end

function collect_tensors(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString;
    matrices_per_tensor_per_axes_per_axis::Maybe{
        Dict{AbstractString, Dict{Tuple{AbstractString, AbstractString}, Dict{AbstractString, Set{AbstractString}}}},
    } = nothing,
    all_tensor_matrices::Maybe{AbstractSet{<:AbstractString}} = nothing,
)::Nothing
    @assert Formats.has_data_read_lock(daf)

    all_axes = axes_set(daf)
    all_matrices = matrices_set(daf, rows_axis, columns_axis; relayout = false, tensors = false)

    for tensor_axis in all_axes
        tensor_axis_entries_set = keys(axis_dict(daf, tensor_axis))
        matrices_per_tensor = Dict{AbstractString, Set{AbstractString}}()

        for matrix_name in all_matrices
            @assert matrix_name isa AbstractString
            separators_positions = findall("_", matrix_name)
            if separators_positions !== nothing
                for position in separators_positions
                    @views entry_name = matrix_name[1:(position[1] - 1)]
                    if entry_name in tensor_axis_entries_set
                        @views tensor_name = matrix_name[(position[1] + 1):end]
                        matrices = get(matrices_per_tensor, tensor_name, nothing)
                        if matrices === nothing
                            matrices = Set{AbstractString}()
                            matrices_per_tensor[tensor_name] = matrices
                        end
                        push!(matrices, matrix_name)
                        if all_tensor_matrices !== nothing
                            push!(all_tensor_matrices, matrix_name)
                        end
                    end
                end
            end
        end

        if !isempty(matrices_per_tensor) && matrices_per_tensor_per_axes_per_axis !== nothing
            matrices_per_tensor_per_axes = get(matrices_per_tensor_per_axes_per_axis, tensor_axis, nothing)
            if matrices_per_tensor_per_axes === nothing
                matrices_per_tensor_per_axes =
                    Dict{Tuple{AbstractString, AbstractString}, Dict{AbstractString, Set{AbstractString}}}()
                matrices_per_tensor_per_axes_per_axis[tensor_axis] = matrices_per_tensor_per_axes
            end
            matrices_per_tensor_per_axes[(rows_axis, columns_axis)] = matrices_per_tensor
        end
    end

    return nothing
end

end # module
