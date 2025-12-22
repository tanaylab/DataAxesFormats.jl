"""
Read-only `Daf` storage format.
"""
module ReadOnly

export DafReadOnly
export is_read_only_array
export read_only

using ..Formats
using ..Readers
using ..StorageTypes
using NamedArrays
using LinearAlgebra
using SparseArrays
using TanayLabUtilities

import ..Formats.Internal

"""
A common base type for a read-only [`DafReader`](@ref), which doesn't allow any modification of the data.
"""
abstract type DafReadOnly <: DafReader end

"""
    struct DafReadOnlyWrapper <: DafReader ... end

A wrapper for any [`DafWriter`](@ref) data, protecting it against accidental modification. This isn't exported and isn't
created manually; instead call [`read_only`](@ref).
"""
struct DafReadOnlyWrapper <: DafReadOnly
    name::AbstractString
    internal::Internal
    daf::DafReader
end

"""
    read_only(daf::DafReader[; name::Maybe{AbstractString]} = nothing)::DafReadOnlyWrapper

Wrap `daf` with a [`DafReadOnlyWrapper`](@ref) to protect it against accidental modification. If not specified, the
`name` of the `daf` is reused. If `name` is not specified and `daf` isa [`DafReadOnly`](@ref), return it as-is.
"""
function read_only(daf::DafReader; name::Maybe{AbstractString} = nothing)::DafReadOnly
    if name === nothing
        name = daf.name * ".read_only"
    end
    name = unique_name(name)  # NOJET
    wrapper = DafReadOnlyWrapper(name, daf.internal, daf)
    @debug "Daf: $(brief(wrapper)) base: $(daf)"
    return wrapper
end

function read_only(daf::DafReadOnly; name::Maybe{AbstractString} = nothing)::DafReadOnly
    if name === nothing
        return daf
    else
        wrapper = DafReadOnlyWrapper(name, daf.internal, daf.daf)
        @debug "Daf: $(brief(wrapper)) base: $(daf.daf)"
        return wrapper
    end
end

function Formats.begin_data_read_lock(read_only_view::DafReadOnlyWrapper, what::Any...)::Nothing
    Formats.begin_data_read_lock(read_only_view.daf, what...)
    return nothing
end

function Formats.end_data_read_lock(read_only_view::DafReadOnlyWrapper, what::Any...)::Nothing
    Formats.end_data_read_lock(read_only_view.daf, what...)
    return nothing
end

function Formats.has_data_read_lock(read_only_view::DafReadOnlyWrapper)::Bool
    return Formats.has_data_read_lock(read_only_view.daf)
end

function Formats.begin_data_write_lock(::DafReadOnlyWrapper, ::Any...)::Nothing  # UNTESTED
    @assert false
end

function Formats.end_data_write_lock(::DafReadOnlyWrapper, ::Any...)::Nothing
    @assert false
end

function Formats.has_data_write_lock(::DafReadOnlyWrapper)::Bool  # UNTESTED
    return false
end

function Formats.format_has_scalar(read_only_view::DafReadOnlyWrapper, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_scalar(read_only_view.daf, name)
end

function Formats.format_get_scalar(read_only_view::DafReadOnlyWrapper, name::AbstractString)::StorageScalar
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_get_scalar(read_only_view.daf, name)
end

function Formats.format_scalars_set(read_only_view::DafReadOnlyWrapper)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_scalars_set(read_only_view.daf)
end

function Formats.format_has_axis(read_only_view::DafReadOnlyWrapper, axis::AbstractString; for_change::Bool)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_axis(read_only_view.daf, axis; for_change)
end

function Formats.format_axes_set(read_only_view::DafReadOnlyWrapper)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_axes_set(read_only_view.daf)
end

function Formats.format_axis_vector(
    read_only_view::DafReadOnlyWrapper,
    axis::AbstractString,
)::AbstractVector{<:AbstractString}
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_axis_vector(read_only_view.daf, axis)
end

function Formats.format_axis_length(read_only_view::DafReadOnlyWrapper, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_axis_length(read_only_view.daf, axis)
end

function Formats.format_has_vector(read_only_view::DafReadOnlyWrapper, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_vector(read_only_view.daf, axis, name)
end

function Formats.format_vectors_set(
    read_only_view::DafReadOnlyWrapper,
    axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_vectors_set(read_only_view.daf, axis)
end

function Formats.format_get_vector(
    read_only_view::DafReadOnlyWrapper,
    axis::AbstractString,
    name::AbstractString,
)::StorageVector
    @assert Formats.has_data_read_lock(read_only_view)
    return read_only_array(Formats.format_get_vector(read_only_view.daf, axis, name))
end

function Formats.format_has_matrix(
    read_only_view::DafReadOnlyWrapper,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_matrix(read_only_view.daf, rows_axis, columns_axis, name)
end

function Formats.format_matrices_set(
    read_only_view::DafReadOnlyWrapper,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_matrices_set(read_only_view.daf, rows_axis, columns_axis)
end

function Formats.format_get_matrix(
    read_only_view::DafReadOnlyWrapper,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    @assert Formats.has_data_read_lock(read_only_view)
    return read_only_array(Formats.format_get_matrix(read_only_view.daf, rows_axis, columns_axis, name))
end

function Formats.format_description_header(  # UNTESTED
    read_only_view::DafReadOnlyWrapper,
    indent::AbstractString,
    lines::Vector{String},
    deep::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(read_only_view)
    push!(lines, "$(indent)type: ReadOnly")
    if !deep
        push!(lines, "$(indent)base: $(brief(read_only_view.daf))")
    end
    return nothing
end

function Formats.format_description_footer(
    read_only_view::DafReadOnlyWrapper,
    indent::AbstractString,
    lines::Vector{String};
    cache::Bool,
    deep::Bool,
    tensors::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(read_only_view)
    if deep
        push!(lines, "$(indent)base:")
        description(read_only_view.daf, indent * "  ", lines; cache, deep, tensors)  # NOJET
    end
    return nothing
end

function TanayLabUtilities.Brief.brief(value::DafReadOnlyWrapper; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ReadOnly $(brief(value.daf; name))"
end

end  # module
