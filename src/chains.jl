"""
View a chain of `Daf` data as a single data set. This allows creating a small `Daf` data set that contains extra (or
overriding) data on top of a larger read-only data set. In particular this allows creating several such incompatible
extra data sets (e.g., different groupings of cells to metacells), without having to duplicate the common (read only)
data.
"""
module Chains

export chain_reader

using Daf.Data
using Daf.Formats
using Daf.ReadOnly
using Daf.StorageTypes

import Daf.Data.as_read_only
import Daf.Formats.Internal
import Daf.ReadOnly.ReadOnlyView

"""
Abstract type for all chains.
"""
abstract type ChainReader <: DafReader end

"""
    struct ReadOnlyChain <: ChainReader ... end

A read-only wrapper for a chain of [`DafReader`](@ref) data, presenting them as a single `DafReader`. When accessing the
content, the exposed value is that provided by the last data set that contains the data, that is, later data sets can
override earlier data sets. However, if an axis exists in more than one data set in the chain, then its entries must be
identical. This isn't typically created manually; instead call [`chain_reader`](@ref).
"""
struct ReadOnlyChain <: ChainReader
    internal::Internal
    dafs::Vector{DafReader}
end

"""
    chain_reader(name::AbstractString, dafs::Vector{F})::ReadOnlyChain where {F <: DafReader}

Create a read-only chain wrapper of [`DafReader`](@ref)s, presenting them as a single `DafReader`. When accessing the
content, the exposed value is that provided by the last data set that contains the data, that is, later data sets can
override earlier data sets. However, if an axis exists in more than one data set in the chain, then its entries must be
identical. This isn't typically created manually; instead call [`chain_reader`](@ref).

!!! note

    While this verifies the axes are consistent at the time of creating the chain, it's no defense against modifying the
    chained data after the fact, creating inconsistent axes. *Don't do that*.
"""
function chain_reader(name::AbstractString, dafs::Vector{F})::ReadOnlyChain where {F <: DafReader}
    if isempty(dafs)
        error("empty chain: $(name)")
    end
    axes_entries = Dict{String, Tuple{String, Vector{String}}}()
    internal_dafs = Vector{DafReader}()
    for daf in dafs
        if daf isa ReadOnlyView
            daf = daf.daf
        end
        push!(internal_dafs, daf)
        for axis in axis_names(daf)
            new_axis_entries = get_axis(daf, axis)
            old_axis_entries = get(axes_entries, axis, nothing)
            if old_axis_entries == nothing
                axes_entries[axis] = (daf.name, new_axis_entries)
            elseif new_axis_entries != old_axis_entries
                error(
                    "different entries for the axis: $(axis)\n" *
                    "in the Daf data: $(old_axis_entries[1])\n" *
                    "and the Daf data: $(daf.name)\n" *
                    "in the chain: $(name)",
                )
            end
        end
    end
    reverse!(internal_dafs)
    return ReadOnlyChain(Internal(name), internal_dafs)
end

function Formats.format_has_scalar(chain::ChainReader, name::AbstractString)::Bool
    for daf in chain.dafs
        if Formats.format_has_scalar(daf, name)
            return true
        end
    end
    return false
end

function Formats.format_get_scalar(chain::ChainReader, name::AbstractString)::StorageScalar
    for daf in chain.dafs
        if Formats.format_has_scalar(daf, name)
            return Formats.format_get_scalar(daf, name)
        end
    end
    @assert false  # untested
end

function Formats.format_scalar_names(chain::ChainReader)::AbstractSet{String}
    return reduce(union, [Formats.format_scalar_names(daf) for daf in chain.dafs])
end

function Formats.format_has_axis(chain::ChainReader, axis::AbstractString)::Bool
    for daf in chain.dafs
        if Formats.format_has_axis(daf, axis)
            return true
        end
    end
    return false
end

function Formats.format_axis_names(chain::ChainReader)::AbstractSet{String}
    return reduce(union, [Formats.format_axis_names(daf) for daf in chain.dafs])
end

function Formats.format_get_axis(chain::ChainReader, axis::AbstractString)::DenseVector{String}
    for daf in chain.dafs
        if Formats.format_has_axis(daf, axis)
            return Formats.format_get_axis(daf, axis)
        end
    end
    @assert false  # untested
end

function Formats.format_axis_length(chain::ChainReader, axis::AbstractString)::Int64
    for daf in chain.dafs
        if Formats.format_has_axis(daf, axis)
            return Formats.format_axis_length(daf, axis)
        end
    end
    @assert false  # untested
end

function Formats.format_has_vector(chain::ChainReader, axis::AbstractString, name::AbstractString)::Bool
    for daf in chain.dafs
        if Formats.format_has_axis(daf, axis) && Formats.format_has_vector(daf, axis, name)
            return true
        end
    end
    return false
end

function Formats.format_vector_names(chain::ChainReader, axis::AbstractString)::AbstractSet{String}
    return reduce(
        union,
        [Formats.format_vector_names(daf, axis) for daf in chain.dafs if Formats.format_has_axis(daf, axis)],
    )
end

function Formats.format_get_vector(chain::ChainReader, axis::AbstractString, name::AbstractString)::StorageVector
    for daf in chain.dafs
        if Formats.format_has_axis(daf, axis) && Formats.format_has_vector(daf, axis, name)
            return as_read_only(Formats.format_get_vector(daf, axis, name))
        end
    end
    @assert false  # untested
end

function Formats.format_has_matrix(
    chain::ChainReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    for daf in chain.dafs
        if Formats.format_has_axis(daf, rows_axis) &&
           Formats.format_has_axis(daf, columns_axis) &&
           Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            return true
        end
    end
    return false
end

function Formats.format_matrix_names(
    chain::ChainReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{String}
    return reduce(
        union,
        [
            Formats.format_matrix_names(daf, rows_axis, columns_axis) for
            daf in chain.dafs if Formats.format_has_axis(daf, rows_axis) && Formats.format_has_axis(daf, columns_axis)
        ],
    )
end

function Formats.format_get_matrix(
    chain::ChainReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    for daf in chain.dafs
        if Formats.format_has_axis(daf, rows_axis) &&
           Formats.format_has_axis(daf, columns_axis) &&
           Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            return as_read_only(Formats.format_get_matrix(daf, rows_axis, columns_axis, name))
        end
    end
    @assert false  # untested
end

function Formats.format_description_header(chain::ChainReader, indent::String, lines::Array{String})::Nothing
    push!(lines, "$(indent)type: ReadOnly Chain")
    return nothing
end

function Formats.format_description_footer(
    chain::ChainReader,
    indent::String,
    lines::Array{String},
    deep::Bool,
)::Nothing
    if deep
        push!(lines, "$(indent)chain:")
        for daf in reverse(chain.dafs)
            description(daf, indent * "  ", lines, deep)
        end
    end
    return nothing
end

end # module
