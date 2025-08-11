"""
View a chain of `Daf` data as a single data set. This allows creating a small `Daf` data set that contains extra (or
overriding) data on top of a larger read-only data set. In particular this allows creating several such incompatible
extra data sets (e.g., different groupings of cells to metacells), without having to duplicate the common (read only)
data.
"""
module Chains

export chain_reader
export chain_writer

using ..Formats
using ..Keys
using ..ReadOnly
using ..Readers
using ..StorageTypes
using ..Writers
using NamedArrays
using SparseArrays
using TanayLabUtilities

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
    chain = ReadOnlyChain(name, Internal(; cache_group = nothing, is_frozen = true), internal_dafs)
    @debug "Daf: $(brief(chain)) chain: $(join([daf.name for daf in dafs], ";"))"
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

    if !(dafs[end] isa DafWriter) || dafs[end].internal.is_frozen
        error(chomp("""
              read-only final data: $(dafs[end].name)
              in write chain$(name_suffix(name))
              """))
    end

    if name === nothing
        if length(dafs) == 1
            return dafs[1]
        end
        name = join([daf.name for daf in dafs], ";")
        @assert name !== nothing
    else
        name = unique_name(name)
    end

    internal_dafs = reader_internal_dafs(dafs, name)
    reader = ReadOnlyChain(name, Internal(; cache_group = nothing, is_frozen = false), internal_dafs)
    chain = WriteChain(name, reader.internal, reader.dafs, dafs[end])
    @debug "Daf: $(brief(chain)) chain: $(join([daf.name for daf in dafs], ";"))"
    return chain
end

function reader_internal_dafs(dafs::AbstractVector, name::AbstractString)::Vector{DafReader}
    axes_entries = Dict{AbstractString, Tuple{AbstractString, AbstractVector{<:AbstractString}}}()
    internal_dafs = Vector{DafReader}()
    for daf in dafs
        if daf isa DafReadOnlyWrapper
            daf = daf.daf
        end
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

function Formats.begin_data_write_lock(::ReadOnlyChain, ::Any...)::Nothing  # UNTESTED
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
    for daf in chain.dafs
        if Formats.format_has_scalar(daf, name)
            return true
        end
    end
    return false
end

function Formats.format_set_scalar!(chain::WriteChain, name::AbstractString, value::StorageScalar)::Nothing
    @assert Formats.has_data_write_lock(chain)
    Formats.format_set_scalar!(chain.daf, name, value)
    return nothing
end

function Formats.format_delete_scalar!(chain::WriteChain, name::AbstractString; for_set::Bool)::Nothing
    @assert Formats.has_data_write_lock(chain)
    if !for_set
        for daf in chain.dafs[1:(end - 1)]
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

function Formats.format_get_scalar(chain::AnyChain, name::AbstractString)::StorageScalar
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_scalar(daf, name)
            return Formats.get_scalar_through_cache(daf, name)
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
    for daf in chain.dafs
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
    for daf in chain.dafs[1:(end - 1)]
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

function Formats.format_axis_vector(chain::AnyChain, axis::AbstractString)::AbstractVector{<:AbstractString}
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false)
            return Formats.get_axis_vector_through_cache(daf, axis)
        end
    end
    @assert false
end

function Formats.format_axis_length(chain::AnyChain, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(chain)
    for daf in chain.dafs
        if Formats.format_has_axis(daf, axis; for_change = false)
            return Formats.format_axis_length(daf, axis)
        end
    end
    @assert false
end

function Formats.format_has_vector(chain::AnyChain, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(chain)
    for daf in chain.dafs
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
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    if !Formats.format_has_axis(chain.daf, axis; for_change = false)
        add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
    end
    Formats.format_set_vector!(chain.daf, axis, name, vector)
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::AbstractVector{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(chain)
    if !Formats.format_has_axis(chain.daf, axis; for_change = false)
        add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
    end
    return Formats.format_get_empty_dense_vector!(chain.daf, axis, name, eltype)
end

function Formats.format_get_empty_sparse_vector!(
    chain::WriteChain,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(chain)
    if !Formats.format_has_axis(chain.daf, axis; for_change = false)
        add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
    end
    return Formats.format_get_empty_sparse_vector!(chain.daf, axis, name, eltype, nnz, indtype)
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
        for daf in chain.dafs[1:(end - 1)]
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

function Formats.format_get_vector(chain::AnyChain, axis::AbstractString, name::AbstractString)::StorageVector
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, axis; for_change = false) && Formats.format_has_vector(daf, axis, name)
            return Formats.read_only_array(Formats.get_vector_through_cache(daf, axis, name))
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
)::Nothing
    @assert Formats.has_data_write_lock(chain)
    for axis in (rows_axis, columns_axis)
        if !Formats.format_has_axis(chain.daf, axis; for_change = false)
            add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
        end
    end
    Formats.format_set_matrix!(chain.daf, rows_axis, columns_axis, name, matrix)
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
)::AbstractMatrix{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(chain)
    for axis in (rows_axis, columns_axis)
        if !Formats.format_has_axis(chain.daf, axis; for_change = false)
            add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
        end
    end
    return Formats.format_get_empty_dense_matrix!(chain.daf, rows_axis, columns_axis, name, eltype)
end

function Formats.format_get_empty_sparse_matrix!(
    chain::WriteChain,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(chain)
    for axis in (rows_axis, columns_axis)
        if !Formats.format_has_axis(chain.daf, axis; for_change = false)
            add_axis!(chain.daf, axis, Formats.get_axis_vector_through_cache(chain, axis))
        end
    end
    return Formats.format_get_empty_sparse_matrix!(chain.daf, rows_axis, columns_axis, name, eltype, nnz, indtype)
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
)::StorageMatrix
    @assert Formats.has_data_write_lock(chain)

    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
            if daf isa DafWriter && !daf.internal.is_frozen
                return Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name, matrix)
            else
                return Formats.get_through_cache(  # UNTESTED
                    daf,
                    Formats.matrix_cache_key(columns_axis, rows_axis, name),
                    StorageMatrix,
                    MemoryData;
                    is_slow = true,
                ) do
                    return (Formats.as_named_matrix(daf, columns_axis, rows_axis, flipped(matrix)), nothing)
                end
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
        for daf in chain.dafs[1:(end - 1)]
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
)::StorageMatrix
    @assert Formats.has_data_read_lock(chain)
    for daf in reverse(chain.dafs)
        if Formats.format_has_axis(daf, rows_axis; for_change = false) &&
           Formats.format_has_axis(daf, columns_axis; for_change = false) &&
           Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
            return Formats.read_only_array(Formats.get_matrix_through_cache(daf, rows_axis, columns_axis, name))
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
            description(daf, "- " * indent, lines; cache, deep, tensors)  # NOJET
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

end # module
