"""
Read-only `Daf` storage format.
"""
module ReadOnly

export DafReadOnly
export read_only

using ..Formats
using ..GenericTypes
using ..Messages
using ..Readers
using ..StorageTypes
using SparseArrays

import ..Messages

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
    daf::DafReader
end

function Base.getproperty(read_only_view::DafReadOnlyWrapper, property::Symbol)::Any
    if property == :name || property == :daf
        return getfield(read_only_view, property)
    else
        daf = getfield(read_only_view, :daf)
        return getfield(daf, property)
    end
end

"""
    read_only(daf::DafReader[; name::Maybe{AbstractString]} = nothing)::DafReadOnlyWrapper

Wrap `daf` with a [`DafReadOnlyWrapper`](@ref) to protect it against accidental modification. If not specified, the
`name` of the `daf` is reused. If `name` is not specified and `daf` isa [`DafReadOnly`](@ref), return it as-is.
"""
function read_only(daf::DafReader; name::Maybe{AbstractString} = nothing)::DafReadOnly
    if name === nothing
        name = daf.internal.name
    end
    wrapper = DafReadOnlyWrapper(name, daf)
    @debug "Daf: $(depict(wrapper)) base: $(daf)"
    return wrapper
end

function read_only(daf::DafReadOnly; name::Maybe{AbstractString} = nothing)::DafReadOnly
    if name === nothing
        return daf
    else
        wrapper = DafReadOnlyWrapper(name, daf.daf)
        @debug "Daf: $(depict(wrapper)) base: $(daf.daf)"
        return wrapper
    end
end

function Formats.begin_data_read_lock(read_only_view::DafReadOnlyWrapper, what::AbstractString...)::Nothing
    Formats.begin_data_read_lock(read_only_view.daf, what...)
    return nothing
end

function Formats.end_data_read_lock(read_only_view::DafReadOnlyWrapper, what::AbstractString...)::Nothing
    Formats.end_data_read_lock(read_only_view.daf, what...)
    return nothing
end

function Formats.has_data_read_lock(read_only_view::DafReadOnlyWrapper)::Bool
    return Formats.has_data_read_lock(read_only_view.daf)
end

function Formats.begin_data_write_lock(::DafReadOnlyWrapper, ::AbstractString...)::Nothing  # untested
    @assert false
end

function Formats.end_data_write_lock(::DafReadOnlyWrapper, ::AbstractString...)::Nothing
    @assert false
end

function Formats.has_data_write_lock(::DafReadOnlyWrapper)::Bool  # untested
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

function Formats.format_scalars_set(read_only_view::DafReadOnlyWrapper)::AbstractStringSet
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_scalars_set(read_only_view.daf)
end

function Formats.format_has_axis(read_only_view::DafReadOnlyWrapper, axis::AbstractString; for_change::Bool)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_axis(read_only_view.daf, axis; for_change = for_change)
end

function Formats.format_axes_set(read_only_view::DafReadOnlyWrapper)::AbstractStringSet
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_axes_set(read_only_view.daf)
end

function Formats.format_axis_array(read_only_view::DafReadOnlyWrapper, axis::AbstractString)::AbstractStringVector
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_axis_array(read_only_view.daf, axis)
end

function Formats.format_axis_length(read_only_view::DafReadOnlyWrapper, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_axis_length(read_only_view.daf, axis)
end

function Formats.format_has_vector(read_only_view::DafReadOnlyWrapper, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_vector(read_only_view.daf, axis, name)
end

function Formats.format_vectors_set(read_only_view::DafReadOnlyWrapper, axis::AbstractString)::AbstractStringSet
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_vectors_set(read_only_view.daf, axis)
end

function Formats.format_get_vector(
    read_only_view::DafReadOnlyWrapper,
    axis::AbstractString,
    name::AbstractString,
)::StorageVector
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.as_read_only_array(Formats.format_get_vector(read_only_view.daf, axis, name))
end

function Formats.format_has_matrix(
    read_only_view::DafReadOnlyWrapper,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_relayout::Bool = false,
)::Bool
    @assert Formats.has_data_read_lock(read_only_view)
    return Formats.format_has_matrix(read_only_view.daf, rows_axis, columns_axis, name; for_relayout = for_relayout)
end

function Formats.format_matrices_set(
    read_only_view::DafReadOnlyWrapper,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractStringSet
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
    return Formats.as_read_only_array(Formats.format_get_matrix(read_only_view.daf, rows_axis, columns_axis, name))
end

function Formats.format_description_header(
    read_only_view::DafReadOnlyWrapper,
    indent::AbstractString,
    lines::Vector{String},
)::Nothing
    @assert Formats.has_data_read_lock(read_only_view)
    push!(lines, "$(indent)type: ReadOnly $(typeof(read_only_view.daf))")
    return nothing
end

function Messages.depict(value::DafReadOnlyWrapper; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ReadOnly $(depict(value.daf; name = name))"
end

end  # module
