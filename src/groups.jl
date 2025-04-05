"""
Functions for dealing with computing groups of axis entries (typically for creating a new axis).
"""
module Groups

using ..Formats
using ..Readers
using SHA
using TanayLabUtilities

export collect_group_members
export compact_groups!
export group_names

"""
    group_names(
        daf::DafReader,
        axis::AbstractString,
        entries_of_groups::AbstractVector{<:AbstractVector{<:Integer}};
        prefix::AbstractString,
    )::Vector{String}

Given an `entries_of_groups` vector of vectors, one for each group, containing the (sorted) indices of the entries of
the group along some `axis` of some `daf` data set, return a vector giving a unique name for each group. This name
consists of the `prefix`, followed by the index of the group, followed by a `.XX` two-digit suffix which is a hash of
the names of the axis entries of the group.

The returned names strike a balance between readability and safety. A name like `M123.89` for group #123 is easy to deal
with manually, but is also reasonably safe in the common use case that groups are re-computed, and there is per-group
metadata lying around associated with the old groups, as the probability of the new group #123 having the same suffix is
only 1% (unless it is actually identical).
"""
@logged function group_names(
    daf::DafReader,
    axis::AbstractString,
    entries_of_groups::AbstractVector{<:AbstractVector{<:Integer}};
    prefix::AbstractString,
)::Vector{String}
    names_of_entries = axis_vector(daf, axis)
    names_of_groups = Vector{String}(undef, length(entries_of_groups))
    for (group_index, entries_of_group) in enumerate(entries_of_groups)
        context = SHA2_256_CTX()
        for entry_index in entries_of_group
            update!(context, transcode(UInt8, String(names_of_entries[entry_index])))
        end
        suffix = sum(digest!(context)) % 100
        names_of_groups[group_index] = "$(prefix)$(group_index).$(lpad(suffix, 2, "0"))"
    end
    return names_of_groups
end

"""
    compact_groups!(
        group_indices::AbstractVector{<:Integer},
    )::Int

Given an array `group_indices` which assigns each entry of some axis to a non-negative group index (with zero
meaning "no group"), compact it in-place so that the group indices will be `1...N`, and return `N`.
"""
@logged function compact_groups!(group_indices::AbstractVector{<:Integer})::Int
    n_groups = 0
    compacts_of_groups = Dict{Int, Int}()
    for (entry_index, group_index) in enumerate(group_indices)
        if group_index != 0
            compact_of_group = get(compacts_of_groups, group_index, nothing)
            if compact_of_group === nothing
                n_groups += 1
                compact_of_group = n_groups
                compacts_of_groups[group_index] = compact_of_group
            end
            group_indices[entry_index] = compact_of_group
        end
    end
    return n_groups
end

"""
    collect_group_members(
        group_indices::AbstractVector{T},
    )::Vector{Vector{T}} where {T <: Integer}

Given an array `group_indices` which assigns each entry of some axis to a non-negative group index (with zero
meaning "no group"), where the group indices are compact (in the range `1...N`), return a vector of vectors,
one for each group, containing the (sorted) indices of the entries of the group.
"""
@logged function collect_group_members(group_indices::AbstractVector{T})::Vector{Vector{T}} where {T <: Integer}
    entries_of_groups = Vector{Vector{T}}()
    for (entry_index, group_index) in enumerate(group_indices)
        if group_index != 0
            while length(entries_of_groups) < group_index
                push!(entries_of_groups, Vector{T}())
            end
            push!(entries_of_groups[group_index], entry_index)
        end
    end
    return entries_of_groups
end

end # module
