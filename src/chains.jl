"""
View a chain of `Daf` data as a single data set. This allows creating a small `Daf` data set that contains extra (or
overriding) data on top of a larger read-only data set. In particular this allows creating several such incompatible
extra data sets (e.g., different groupings of cells to metacells), without having to duplicate the common (read only)
data.
"""
module Chains

export BaseDaf
export chain_reader
export chain_writer
export complete_chain!

using JSON
using NamedArrays
using SparseArrays
using TanayLabUtilities

using ..Formats
using ..Keys
using ..ReadOnly
using ..Readers
using ..StorageTypes
using ..Views
using ..Writers

import ..Formats.CacheKey
import ..Formats.FormatReader
import ..Formats.Internal
import ..ReadOnly.DafReadOnlyWrapper

"""
    struct ReadOnlyChain <: DafReadOnly ... end

A wrapper for a chain of [`DafReader`](@ref) data, presenting them as a single `DafReadOnly`. When accessing the
content, the exposed value is that provided by the last data set that contains the data, that is, later data sets can
override earlier data sets. However, if an axis exists in more than one data set in the chain, then its entries must be
identical. This isn't typically created manually; instead call [`chain_reader`](@ref).
"""
struct ReadOnlyChain <: DafReadOnly
    name::AbstractString
    internal::Internal
    dafs::Vector{DafReader}
end

"""
    struct WriteChain <: DafWriter ... end

A wrapper for a chain of [`DafReader`](@ref) data, with a final [`DafWriter`](@ref), presenting them as a single
[`DafWriter`](@ref). When accessing the content, the exposed value is that provided by the last data set that contains
the data, that is, later data sets can override earlier data sets (where the writer has the final word). However, if an
axis exists in more than one data set in the chain, then its entries must be identical. This isn't typically created
manually; instead call [`chain_reader`](@ref).

Any modifications or additions to the chain are directed at the final writer. Deletions are only allowed for data that
exists only in this writer. That is, it is impossible to delete from a chain something that exists in any of the
readers; it is only possible to override it.
"""
struct WriteChain <: DafWriter
    name::AbstractString
    internal::Internal
    dafs::Vector{DafReader}
    daf::DafWriter
end

"""
    chain_reader(dafs::AbstractVector{<:DafReader}; name::Maybe{AbstractString} = nothing)::DafReader

Create a read-only chain wrapper of [`DafReader`](@ref)s, presenting them as a single [`DafReader`](@ref). When
accessing the content, the exposed value is that provided by the last data set that contains the data, that is, later
data sets can override earlier data sets. However, if an axis exists in more than one data set in the chain, then its
entries must be identical. This isn't typically created manually; instead call [`chain_reader`](@ref).

!!! note

    While this verifies the axes are consistent at the time of creating the chain, it's no defense against modifying the
    chained data after the fact, creating inconsistent axes. *Don't do that*.
"""
function chain_reader(dafs::AbstractVector{<:DafReader}; name::Maybe{AbstractString} = nothing)::DafReadOnly
    if isempty(dafs)
        error("empty chain$(name_suffix(name))")
    end

    if length(dafs) == 1
        return read_only(dafs[1]; name)
    end

    if name === nothing
        name = join([daf.name for daf in dafs], ";")
        @assert name !== nothing
    end
    name = unique_name(name, ";#")

    internal_dafs = reader_internal_dafs(dafs, name)
    chain = ReadOnlyChain(name, Internal(; is_frozen = true), internal_dafs)
    @debug "Daf: $(brief(chain)) chain: $(join([daf.name for daf in dafs], ";"))" _group = :daf_repos
    return chain
end

"""
    chain_writer(dafs::AbstractVector{<:DafReader}; name::Maybe{AbstractString} = nothing)::DafWriter

Create a chain wrapper for a chain of [`DafReader`](@ref) data, presenting them as a single [`DafWriter`](@ref). This
acts similarly to [`chain_reader`](@ref), but requires the final entry in the chain to be a [`DafWriter`](@ref). Any
modifications or additions to the chain are directed only at this final writer.

!!! note

    Deletions are only allowed for data that exists only in the final writer. That is, it is impossible to delete from a
    chain something that exists in any of the readers; it is only possible to override it.
"""
function chain_writer(dafs::AbstractVector{<:DafReader}; name::Maybe{AbstractString} = nothing)::DafWriter
    if isempty(dafs)
        error("empty chain$(name_suffix(name))")
    end

    if length(dafs) == 1 && name === nothing
        return dafs[1]
    end

    if !(dafs[end] isa DafWriter) || dafs[end].internal.is_frozen
        error(chomp("""
              read-only final data: $(dafs[end].name)
              in write chain$(name_suffix(name))
              """))
    end

    if name === nothing
        if length(dafs) == 1
            return dafs[1]  # UNTESTED
        end
        name = join([daf.name for daf in dafs], ";")
        @assert name !== nothing
    else
        name = unique_name(name)
    end

    internal_dafs = reader_internal_dafs(dafs, name)
    reader = ReadOnlyChain(name, Internal(; is_frozen = false), internal_dafs)
    chain = WriteChain(name, reader.internal, reader.dafs, dafs[end])
    @debug "Daf: $(brief(chain)) chain: $(join([daf.name for daf in dafs], ";"))"
    return chain
end

"""
    @kwdef struct BaseDaf
        daf::DafReader
        axes::Maybe{ViewAxes} = nothing
        data::Maybe{ViewData} = nothing
    end

One base repository of a [`complete_chain!`](@ref), and the [`viewer`](@ref) parameters to apply to it, which restrict
it to a subset of its data and/or rename that data. Pass a plain `DafReader` instead wherever the whole of it is used,
which is the common case.
"""
@kwdef struct BaseDaf
    daf::DafReader
    axes::Maybe{ViewAxes} = nothing
    data::Maybe{ViewData} = nothing
end

# One immediate base of a repository, as it is written into and read back from its `base_daf_repository`. The path is
# as recorded, that is, relative to the repository naming it unless it was stored as an absolute one.
struct RecordedBase  # NOLINT
    path::AbstractString
    axes::Maybe{ViewAxes}
    data::Maybe{ViewData}
end

# Two views of one repository are two different bases, so which repository it is of is not enough to tell them apart.
function Base.:(==)(left::RecordedBase, right::RecordedBase)::Bool
    return left.path == right.path && left.axes == right.axes && left.data == right.data
end

"""
    complete_chain!(;
        base_daf::Union{DafReader, BaseDaf, AbstractVector{<:Union{BaseDaf, DafReader}}},
        new_daf::DafWriter,
        name::Maybe{AbstractString} = nothing,
        absolute::Bool = false
    )::DafWriter

Immediately after creating an empty disk based `new_daf`, chain it on top of one or more disk based base repositories,
and return the new chain. Each base is a `DafReader`, or a [`BaseDaf`](@ref) when only a view of it is used. Give
several of them when the `new_daf` rests on more than one repository - say, a repository of shared computed results and
a repository of the parameters this variant of the analysis uses, both resting in turn on the same raw data. Later bases
override earlier ones, as in any chain, and a repository reached more than once is used once, at its earliest position.

This will set the `base_daf_repository` scalar property of the `new_daf` to describe the bases, so that the chain can be
recreated by calling [`complete_daf`](@ref DataAxesFormats.CompleteDaf.complete_daf) in the future.

By default, the stored paths will be relative to the `new_daf`, for the common case where a group of repositories is
stored under a common root. This allows this root to be renamed or moved somewhere else and still allow `complete_daf`
to work. If `absolute` is set, then the stored paths will be absolute.
"""
function complete_chain!(;
    base_daf::Union{DafReader, BaseDaf, AbstractVector{<:Union{BaseDaf, DafReader}}},
    new_daf::DafWriter,
    name::Maybe{AbstractString} = nothing,
    absolute::Bool = false,
)::DafWriter
    new_path = complete_path(new_daf)
    @assert new_path !== nothing

    bases = immediate_bases(base_daf, dirname(new_path), absolute)
    set_scalar!(new_daf, "base_daf_repository", base_specification(bases))

    base_dafs = DafReader[base.second for base in bases]
    push!(base_dafs, new_daf)
    return chain_writer(base_dafs; name)
end

# The immediate bases of a repository: how each is recorded, and the reader it is reached through. Only the immediate
# ones - what each of them in turn rests on is recorded in it, and is found by following the records.
function immediate_bases(
    base_daf::AbstractVector,
    new_directory::AbstractString,
    absolute::Bool,
)::Vector{Pair{RecordedBase, DafReader}}
    return unique_bases([immediate_base(base, new_directory, absolute) for base in base_daf])
end

function immediate_bases(
    base_daf::Union{DafReader, BaseDaf},
    new_directory::AbstractString,
    absolute::Bool,
)::Vector{Pair{RecordedBase, DafReader}}
    return [immediate_base(base_daf, new_directory, absolute)]
end

function immediate_base(
    base_daf::DafReader,
    new_directory::AbstractString,
    absolute::Bool,
)::Pair{RecordedBase, DafReader}
    return RecordedBase(base_path(base_daf, new_directory, absolute), nothing, nothing) => base_daf
end

function immediate_base(base_daf::BaseDaf, new_directory::AbstractString, absolute::Bool)::Pair{RecordedBase, DafReader}
    recorded = RecordedBase(base_path(base_daf.daf, new_directory, absolute), base_daf.axes, base_daf.data)
    return recorded => viewer(base_daf.daf; base_daf.axes, base_daf.data)
end

# The same base twice is the same data twice, so it is kept once, at its earliest position: a chain resolves later-wins,
# and a base must not override what rests on it.
function unique_bases(bases::AbstractVector{<:Pair{RecordedBase, <:DafReader}})::Vector{Pair{RecordedBase, DafReader}}
    kept = Pair{RecordedBase, DafReader}[]
    for base in bases
        if !any(kept_base.first == base.first for kept_base in kept)
            push!(kept, base)
        end
    end
    return kept
end

# How the immediate bases are described in the `base_daf_repository` scalar. A lone unviewed base is stored as its path
# rather than as JSON, both because that is what almost every repository has, and because it is what someone looking at
# the property expects to see.
function base_specification(bases::AbstractVector{Pair{RecordedBase, DafReader}})::AbstractString
    if length(bases) == 1 && bases[1].first.axes === nothing && bases[1].first.data === nothing
        return bases[1].first.path
    else
        return JSON.json([base_json(base.first) for base in bases])  # NOJET
    end
end

function base_json(base::RecordedBase)::Any
    if base.axes === nothing && base.data === nothing
        return base.path
    end
    json = Dict{String, Any}("path" => base.path)
    if base.axes !== nothing
        json["axes"] = view_json(base.axes)
    end
    if base.data !== nothing
        json["data"] = view_json(base.data)
    end
    return json
end

# A view's axes and data are pairs, and the order of them matters - a pattern is overridden by a later one. JSON has
# neither pairs nor ordered objects, so they are written as an array of single-entry objects rather than as one object.
function view_json(parameters::Union{AbstractVector, NamedTuple})::Vector{Dict{String, Any}}
    return [Dict{String, Any}(view_json_key(key) => value) for (key, value) in named_tuple_as_pairs(parameters)]
end

function view_json_key(key::AbstractString)::AbstractString
    return key
end

# A matrix names both of its axes, which is written as it is spelled in Julia and read back by `view_parameters`.
function view_json_key(key::Tuple)::AbstractString
    return string(key)
end

# The immediate bases a repository records - only its own, never theirs. A repository which rests on several is a
# JSON array, one which rests on a view of a single one is a JSON object, and the common case of resting on the whole
# of a single one is the path itself.
function recorded_bases(specification::AbstractString)::Vector{RecordedBase}
    specification = lstrip(specification)
    if !startswith(specification, "[") && !startswith(specification, "{")
        return [RecordedBase(specification, nothing, nothing)]
    end

    json = JSON.parse(specification)
    if json isa AbstractVector
        return [recorded_base(base) for base in json]
    else
        return [recorded_base(json)]
    end
end

function recorded_base(json::AbstractString)::RecordedBase
    return RecordedBase(json, nothing, nothing)
end

function recorded_base(json::AbstractDict)::RecordedBase
    return RecordedBase(
        json["path"],
        view_parameters(get(json, "axes", nothing)),
        view_parameters(get(json, "data", nothing)),
    )
end

function view_parameters(::Nothing)::Nothing
    return nothing
end

# A view's axes and data are pairs, and JSON has no pairs, so they are stored as a list of single-entry objects. A
# matrix key is a pair of axes, which is spelled with parentheses in Julia and stored as a JSON array.
function view_parameters(json::AbstractVector)::Vector{Pair}
    pairs = Pair[]
    for entry in json
        for (pattern, value) in entry
            if contains(pattern, "(")
                pattern = Tuple(JSON.parse(replace(pattern, "(" => "[", ")" => "]")))
            end
            push!(pairs, pattern => value)
        end
    end
    return pairs
end

function base_path(base_daf::DafReader, new_directory::AbstractString, absolute::Bool)::AbstractString
    path = complete_path(base_daf)
    @assert path !== nothing
    if absolute
        return path
    else
        return relpath(path, new_directory)
    end
end

# The repositories of a chain: whatever the caller gave us, flattened, and with each repository appearing once.
#
# A chain of chains is the same data as one long chain, and repositories form a tree rather than a list - two bases of
# the same repository typically rest on a common ancestor, which is therefore reached through both of them. It is the
# same data either way, so it is kept at its earliest position: a chain resolves later-wins, and an ancestor must not
# override what is based on it.
function flatten_dafs(dafs::AbstractVector, name::AbstractString)::Vector{DafReader}
    expanded_dafs = Vector{DafReader}()
    for daf in dafs
        if daf isa DafReadOnlyWrapper
            daf = daf.daf
        end
        if daf isa AnyChain
            append!(expanded_dafs, flatten_dafs(daf.dafs, name))  # NOJET
        else
            push!(expanded_dafs, daf)
        end
    end

    # The last repository is the one a `chain_writer` writes to. Reaching it again is a repository based on itself
    # rather than a diamond, and there is no order which makes sense of that. This is asked before dropping repeats,
    # since dropping one would remove the very repository that is written to. A view of it counts as reaching it:
    # writing through the chain would change what an earlier link of the same chain reads.
    last_path = complete_path(expanded_dafs[end])
    if last_path !== nothing && any(complete_path(daf) == last_path for daf in expanded_dafs[1:(end - 1)])
        error(chomp("""
              cyclic repository: $(last_path)
              is also a base of itself
              in the chain: $(name)
              """))
    end

    # Only a whole repository is the same data as another copy of itself. A view is a subset of one, and two views of
    # the same repository - or a view of it and the repository itself - report the same path while exposing different
    # data, so a view is never dropped. Neither is a repository which is not persistent, having no path to be
    # recognized by.
    flat_dafs = Vector{DafReader}()
    whole_paths = Set{AbstractString}()
    for daf in expanded_dafs
        path = daf isa DafView ? nothing : complete_path(daf)
        if path === nothing
            push!(flat_dafs, daf)
        elseif !(path in whole_paths)
            push!(whole_paths, path)
            push!(flat_dafs, daf)
        end
    end

    return flat_dafs
end

function reader_internal_dafs(dafs::AbstractVector, name::AbstractString)::Vector{DafReader}
    axes_entries = Dict{AbstractString, Tuple{AbstractString, AbstractVector{<:AbstractString}}}()
    internal_dafs = Vector{DafReader}()
    for daf in flatten_dafs(dafs, name)
        push!(internal_dafs, daf)
        for axis in axes_set(daf)
            new_axis_entries = axis_vector(daf, axis)
            old_axis_entries = get(axes_entries, axis, nothing)
            if old_axis_entries === nothing
                axes_entries[axis] = (daf.name, new_axis_entries)
            elseif length(new_axis_entries) != length(old_axis_entries[2])
                error(chomp("""
                      different number of entries: $(length(new_axis_entries))
                      for the axis: $(axis)
                      in the daf data: $(daf.name)
                      from the number of entries: $(length(old_axis_entries[2]))
                      for the axis: $(axis)
                      in the daf data: $(old_axis_entries[1])
                      in the chain: $(name)
                      """))
            else
                for (index, (new_entry, old_entry)) in enumerate(zip(new_axis_entries, old_axis_entries[2]))
                    if new_entry != old_entry
                        error(chomp("""
                              different entry#$(index): $(new_entry)
                              for the axis: $(axis)
                              in the daf data: $(daf.name)
                              from the entry#$(index): $(old_entry)
                              for the axis: $(axis)
                              in the daf data: $(old_axis_entries[1])
                              in the chain: $(name)
                              """))
                    end
                end
            end
        end
    end
    return internal_dafs
end

function name_suffix(name::Maybe{AbstractString})::String
    if name === nothing
        return ""
    else
        return ": $(name)"
    end
end

AnyChain = Union{ReadOnlyChain, WriteChain}

function Formats.begin_data_read_lock(chain::AnyChain, what::Any...)::Nothing
    invoke(Formats.begin_data_read_lock, Tuple{DafReader, Vararg{Any}}, chain, what...)
    for daf in chain.dafs
        Formats.begin_data_read_lock(daf, what...)
    end
    return nothing
end

function Formats.end_data_read_lock(chain::AnyChain, what::Any...)::Nothing
    for daf in reverse(chain.dafs)
        Formats.end_data_read_lock(daf, what...)
    end
    invoke(Formats.end_data_read_lock, Tuple{DafReader, Vararg{Any}}, chain, what...)
    return nothing
end

function Formats.begin_data_write_lock(::ReadOnlyChain, ::Any...)::Nothing
    @assert false
end

function Formats.end_data_write_lock(::ReadOnlyChain, ::Any...)::Nothing
    @assert false
end

function Formats.begin_data_write_lock(chain::WriteChain, what::Any...)::Nothing
    invoke(Formats.begin_data_write_lock, Tuple{DafReader, Vararg{Any}}, chain, what...)
    @assert chain.daf === chain.dafs[end]
    Formats.begin_data_write_lock(chain.daf, what...)
    for daf in reverse(chain.dafs[1:(end - 1)])
        Formats.begin_data_read_lock(daf, what...)
    end
    return nothing
end

function Formats.end_data_write_lock(chain::WriteChain, what::Any...)::Nothing
    for daf in chain.dafs[1:(end - 1)]
        Formats.end_data_read_lock(daf, what...)
    end
    @assert chain.daf === chain.dafs[end]
    Formats.end_data_write_lock(chain.daf, what...)
    invoke(Formats.end_data_write_lock, Tuple{DafReader, Vararg{Any}}, chain, what...)
    return nothing
end

function Formats.format_has_scalar(chain::AnyChain, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_scalar(daf, name)
            return true
        end
    end
    return false
end

function Formats.format_set_scalar!(
    chain::WriteChain,
    name::AbstractString,
    value::StorageScalar,
)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(chain)
    return Formats.format_set_scalar!(chain.daf, name, value)
end

function Formats.format_delete_scalar!(chain::WriteChain, name::AbstractString; for_set::Bool)::Nothing
    @assert Formats.has_data_write_lock(chain)
    if !for_set
        for daf in reverse(chain.dafs[1:(end - 1)])
            if Formats.format_has_scalar(daf, name)
                error(chomp("""
                      failed to delete the scalar: $(name)
                      from the daf data: $(chain.daf.name)
                      of the chain: $(chain.name)
                      because it exists in the earlier: $(daf.name)
                      """))
            end
        end
    end
    Formats.format_delete_scalar!(chain.daf, name; for_set)
    return nothing
end

function Formats.format_get_scalar(
    chain::AnyChain,
    name::AbstractString,
)::Tuple{StorageScalar, Maybe{Formats.CacheGroup}}
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_scalar(daf, name)
            return (Formats.get_scalar_through_cache(daf, name), Formats.MemoryData)
        end
    end
    @assert false
end

function Formats.format_scalars_set(chain::AnyChain)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(chain)
    return reduce(
        union,
        [Formats.get_scalars_set_through_cache(daf) for daf in chain.dafs];
        init = Set{AbstractString}(),
    )
end

function Formats.format_has_axis(chain::AnyChain, axis::AbstractString; for_change::Bool)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change)
            return true
        end
        for_change = false
    end
    return false
end

function Formats.format_add_axis!(
    chain::WriteChain,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    Formats.format_add_axis!(chain.daf, axis, entries)
    return nothing
end

function Formats.format_delete_axis!(chain::WriteChain, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(chain)
    for daf in reverse(chain.dafs[1:(end - 1)])
        if Formats.format_has_axis(daf, axis; for_change = false)
            error(chomp("""
                  failed to delete the axis: $(axis)
                  from the daf data: $(chain.daf.name)
                  of the chain: $(chain.name)
                  because it exists in the earlier: $(daf.name)
                  """))
        end
    end
    Formats.format_delete_axis!(chain.daf, axis)
    return nothing
end

function Formats.format_axes_set(chain::AnyChain)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(chain)
    return reduce(union, [Formats.get_axes_set_through_cache(daf) for daf in chain.dafs]; init = Set{AbstractString}())
end

function Formats.format_axis_vector(
    chain::AnyChain,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Maybe{Formats.CacheGroup}}
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false)
            return (Formats.get_axis_vector_through_cache(daf, axis), Formats.MemoryData)
        end
    end
    @assert false
end

function Formats.format_axis_length(chain::AnyChain, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false)
            return Formats.format_axis_length(daf, axis)
        end
    end
    @assert false
end

function Formats.format_has_vector(chain::AnyChain, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false) && Formats.format_has_vector(daf, axis, name)
            return true
        end
    end
    return false
end

function Formats.format_set_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
    is_packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    if !Formats.format_has_axis(chain.daf, axis; for_change = false)
        add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
    end
    Formats.format_set_vector!(chain.daf, axis, name, vector, is_packed)
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    is_packed::Bool,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(chain)
    if !Formats.format_has_axis(chain.daf, axis; for_change = false)
        add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
    end
    return Formats.format_get_empty_dense_vector!(chain.daf, axis, name, eltype, is_packed)
end

function Formats.format_get_empty_sparse_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
    is_packed::Bool,
)::Tuple{AbstractVector{I}, AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(chain)
    if !Formats.format_has_axis(chain.daf, axis; for_change = false)
        add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
    end
    return Formats.format_get_empty_sparse_vector!(chain.daf, axis, name, eltype, nnz, indtype, is_packed)
end

function Formats.format_filled_empty_dense_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    filled::AbstractVector{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    Formats.format_filled_empty_dense_vector!(chain.daf, axis, name, filled)
    return nothing
end

function Formats.format_filled_empty_sparse_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    filled::SparseVector{T, I},
)::Nothing where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(chain)
    Formats.format_filled_empty_sparse_vector!(chain.daf, axis, name, filled)
    return nothing
end

function Formats.format_delete_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    if !for_set
        for daf in reverse(chain.dafs[1:(end - 1)])
            if Formats.format_has_axis(daf, axis; for_change = false) && Formats.format_has_vector(daf, axis, name)
                error(chomp("""
                      failed to delete the vector: $(name)
                      of the axis: $(axis)
                      from the daf data: $(chain.daf.name)
                      of the chain: $(chain.name)
                      because it exists in the earlier: $(daf.name)
                      """))
            end
        end
    end
    if Formats.format_has_axis(chain.daf, axis; for_change = false) && Formats.format_has_vector(chain.daf, axis, name)
        Formats.format_delete_vector!(chain.daf, axis, name; for_set)
    end
    return nothing
end

function Formats.format_vectors_set(chain::AnyChain, axis::AbstractString)::AbstractSet{<:AbstractString}
    return reduce(
        union,
        [
            Formats.get_vectors_set_through_cache(daf, axis) for
            daf in chain.dafs if Formats.format_has_axis(daf, axis; for_change = false)
        ];
        init = Set{AbstractString}(),
    )
end

function Formats.format_get_vector(
    chain::AnyChain,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Any, Maybe{Formats.CacheGroup}}
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false) && Formats.format_has_vector(daf, axis, name)
            return (
                Formats.read_only_array(Formats.get_vector_through_cache(daf, axis, name)),
                nothing,
                Formats.MemoryData,
            )
        end
    end
    @assert false
end

function Formats.format_is_packed_vector(chain::AnyChain, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false) && Formats.format_has_vector(daf, axis, name)
            return Formats.format_is_packed_vector(daf, axis, name)
        end
    end
    @assert false
end

function Formats.format_has_matrix(
    chain::AnyChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            return true
        end
    end
    return false
end

function Formats.format_has_cached_matrix(
    chain::AnyChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
            return true
        end
    end
    return false
end

function Formats.format_set_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageReal, StorageMatrix},
    is_packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    for axis in (rows_axis, columns_axis)
        if !Formats.format_has_axis(chain.daf, axis; for_change = false)
            add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
        end
    end
    Formats.format_set_matrix!(chain.daf, rows_axis, columns_axis, name, matrix, is_packed)
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    is_packed::Bool,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(chain)
    for axis in (rows_axis, columns_axis)
        if !Formats.format_has_axis(chain.daf, axis; for_change = false)
            add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
        end
    end
    return Formats.format_get_empty_dense_matrix!(chain.daf, rows_axis, columns_axis, name, eltype, is_packed)
end

function Formats.format_get_empty_sparse_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
    is_packed::Bool,
)::Tuple{
    AbstractVector{I},
    AbstractVector{I},
    AbstractVector{T},
    Maybe{Formats.CacheGroup},
} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(chain)
    for axis in (rows_axis, columns_axis)
        if !Formats.format_has_axis(chain.daf, axis; for_change = false)
            add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
        end
    end
    return Formats.format_get_empty_sparse_matrix!(
        chain.daf,
        rows_axis,
        columns_axis,
        name,
        eltype,
        nnz,
        indtype,
        is_packed,
    )
end

function Formats.format_filled_empty_dense_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::AbstractMatrix{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    Formats.format_filled_empty_dense_matrix!(chain.daf, rows_axis, columns_axis, name, filled)
    return nothing
end

function Formats.format_filled_empty_sparse_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::SparseMatrixCSC{T, I},
)::Nothing where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(chain)
    Formats.format_filled_empty_sparse_matrix!(chain.daf, rows_axis, columns_axis, name, filled)
    return nothing
end

function Formats.format_relayout_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
    is_packed::Bool,
)::StorageMatrix
    @assert Formats.has_data_write_lock(chain)

    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
            if daf isa DafWriter && !daf.internal.is_frozen
                return Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name, matrix, is_packed)
            else
                entry = Formats.get_slow_through_cache(  # UNTESTED
                    daf,
                    Formats.matrix_cache_key(columns_axis, rows_axis, name),
                    Tuple{NamedArray, Any},
                    MemoryData,
                ) do
                    return ((Formats.as_named_matrix(daf, columns_axis, rows_axis, flipped(matrix)), nothing), nothing)
                end
                return entry[1]  # UNTESTED
            end
        end
    end
    @assert false
end

function Formats.format_delete_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(chain)

    if !for_set
        for daf in reverse(chain.dafs[1:(end - 1)])
            if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
               Formats.format_has_axis(daf, columns_axis; for_change = false) &&
               Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
                error(chomp("""
                      failed to delete the matrix: $(name)
                      for the rows axis: $(rows_axis)
                      and the columns axis: $(columns_axis)
                      from the daf data: $(chain.daf.name)
                      of the chain: $(chain.name)
                      because it exists in the earlier: $(daf.name)
                      """))
            end
        end
    end

    if Formats.format_has_axis(chain.daf, rows_axis; for_change = false) &&
       Formats.format_has_axis(chain.daf, columns_axis; for_change = false) &&
       Formats.format_has_matrix(chain.daf, rows_axis, columns_axis, name)
        Formats.format_delete_matrix!(chain.daf, rows_axis, columns_axis, name; for_set = for_set)
    end

    return nothing
end

function Formats.format_matrices_set(
    chain::AnyChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(chain)
    return reduce(
        union,
        [
            Formats.get_matrices_set_through_cache(daf, rows_axis, columns_axis) for
            daf in chain.dafs if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
            Formats.format_has_axis(daf, columns_axis; for_change = false)
        ];
        init = Set{AbstractString}(),
    )
end

function Formats.format_get_matrix(
    chain::AnyChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Any, Maybe{Formats.CacheGroup}}
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
            return (
                Formats.read_only_array(Formats.get_matrix_through_cache(daf, rows_axis, columns_axis, name)),
                nothing,
                Formats.MemoryData,
            )
        end
    end
    @assert false
end

function Formats.format_is_packed_matrix(
    chain::AnyChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            return Formats.format_is_packed_matrix(daf, rows_axis, columns_axis, name)
        end
    end
    @assert false
end

function Formats.format_description_header(
    chain::AnyChain,
    indent::AbstractString,
    lines::Vector{String},
    deep::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(chain)

    if chain isa ReadOnlyChain
        push!(lines, "$(indent)type: ReadOnly Chain")
    elseif chain isa WriteChain
        push!(lines, "$(indent)type: Write Chain")
    else
        @assert false
    end

    if !deep
        push!(lines, "$(indent)chain:")
        for daf in chain.dafs
            push!(lines, "$(indent)- $(brief(daf))")
        end
    end

    return nothing
end

function Formats.format_description_footer(
    chain::AnyChain,
    indent::AbstractString,
    lines::Vector{String};
    cache::Bool,
    deep::Bool,
    tensors::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(chain)
    if deep
        push!(lines, "$(indent)chain:")
        for daf in chain.dafs
            description(daf, indent * "- ", lines; cache, deep, tensors)  # NOJET
        end
    end
    return nothing
end

function Formats.invalidate_cached!(chain::AnyChain, cache_key::CacheKey)::Nothing
    invoke(Formats.invalidate_cached!, Tuple{FormatReader, CacheKey}, chain, cache_key)
    for daf in chain.dafs
        Formats.invalidate_cached!(daf, cache_key)
    end
end

function Formats.format_get_version_counter(chain::AnyChain, version_key::PropertyKey)::UInt32
    version_counter = UInt32(0)
    for daf in chain.dafs
        version_counter += Formats.format_get_version_counter(daf, version_key)
    end
    return version_counter
end

function Formats.format_increment_version_counter(chain::WriteChain, version_key::PropertyKey)::Nothing
    Formats.format_increment_version_counter(chain.daf, version_key)
    return nothing
end

function TanayLabUtilities.Brief.brief(value::ReadOnlyChain; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ReadOnly Chain $(name)"
end

function TanayLabUtilities.Brief.brief(value::WriteChain; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "Write Chain $(name)"
end

function ReadOnly.read_only(daf::ReadOnlyChain; name::Maybe{AbstractString} = nothing)::ReadOnlyChain
    if name === nothing
        return daf
    else
        return ReadOnlyChain(name, daf.internal, daf.dafs)
    end
end

# A chain has the path of its last repository only when it holds exactly what reopening that path would give: every
# repository the records lead to, and nothing besides. Since a repository records only its own immediate bases, this
# follows them outwards from the last repository rather than comparing the chain link by link, which a repository
# resting on several bases would not survive.
function Readers.complete_path(chain::AnyChain)::Maybe{AbstractString}
    path = complete_path(chain.dafs[end])
    if path === nothing
        return nothing
    end

    daf_of_path = Dict{AbstractString, DafReader}()
    for daf in chain.dafs
        daf_path = complete_path(daf)
        if daf_path === nothing
            return nothing
        end
        daf_of_path[daf_path] = daf
    end

    reached = Set{AbstractString}()
    unvisited = AbstractString[path]
    while !isempty(unvisited)
        daf_path = pop!(unvisited)
        push!(reached, daf_path)
        specification = get_scalar(daf_of_path[daf_path], "base_daf_repository"; default = nothing)
        if specification !== nothing
            for base in recorded_bases(specification)
                base_path = abspath(joinpath(dirname(daf_path), base.path))
                if !haskey(daf_of_path, base_path)
                    return nothing
                end
                if !(base_path in reached)
                    push!(unvisited, base_path)
                end
            end
        end
    end

    # A repository the records never lead to is one the caller chained in by hand, so reopening would not give this.
    if length(reached) != length(daf_of_path)
        return nothing
    end
    return path
end

end  # module
