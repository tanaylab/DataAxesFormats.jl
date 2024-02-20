"""
Read-only `Daf` storage format.
"""
module ReadOnly

export ReadOnlyView
export read_only

using Daf.Data
using Daf.Formats
using Daf.Messages
using Daf.StorageTypes
using Daf.Unions
using SparseArrays

import Daf.Data.as_read_only
import Daf.Messages

"""
    struct ReadOnlyView <: DafReader ... end

A wrapper for any [`DafWriter`](@ref) data, protecting it against accidental modification. This isn't typically created
manually; instead call [`read_only`](@ref).
"""
struct ReadOnlyView <: DafReader
    name::AbstractString
    daf::DafReader
end

function Base.getproperty(read_only_view::ReadOnlyView, property::Symbol)::Any
    if property == :name || property == :daf
        return getfield(read_only_view, property)
    else
        daf = getfield(read_only_view, :daf)
        return getfield(daf, property)
    end
end

"""
    read_only(daf::DafReader)::ReadOnlyView

Wrap `daf` with a `ReadOnlyView` to protect it against accidental modification. If given a read-only `daf`, return it
as-is.
"""
function read_only(daf::DafReader, name::Maybe{AbstractString} = nothing)::ReadOnlyView
    if name == nothing
        name = daf.name
    end
    return ReadOnlyView(name, daf)
end

function read_only(daf::ReadOnlyView, name::Maybe{AbstractString} = nothing)::ReadOnlyView
    if name == nothing
        return daf
    else
        return ReadOnlyView(name, daf.daf)
    end
end

function Formats.format_has_scalar(read_only_view::ReadOnlyView, name::AbstractString)::Bool
    return Formats.format_has_scalar(read_only_view.daf, name)
end

function Formats.format_get_scalar(read_only_view::ReadOnlyView, name::AbstractString)::StorageScalar
    return Formats.format_get_scalar(read_only_view.daf, name)
end

function Formats.format_scalar_names(read_only_view::ReadOnlyView)::AbstractStringSet
    return Formats.format_scalar_names(read_only_view.daf)
end

function Formats.format_has_axis(read_only_view::ReadOnlyView, axis::AbstractString)::Bool
    return Formats.format_has_axis(read_only_view.daf, axis)
end

function Formats.format_axis_names(read_only_view::ReadOnlyView)::AbstractStringSet
    return Formats.format_axis_names(read_only_view.daf)
end

function Formats.format_get_axis(read_only_view::ReadOnlyView, axis::AbstractString)::AbstractVector{String}
    return Formats.format_get_axis(read_only_view.daf, axis)
end

function Formats.format_axis_length(read_only_view::ReadOnlyView, axis::AbstractString)::Int64
    return Formats.format_axis_length(read_only_view.daf, axis)
end

function Formats.format_has_vector(read_only_view::ReadOnlyView, axis::AbstractString, name::AbstractString)::Bool
    return Formats.format_has_vector(read_only_view.daf, axis, name)
end

function Formats.format_vector_names(read_only_view::ReadOnlyView, axis::AbstractString)::AbstractStringSet
    return Formats.format_vector_names(read_only_view.daf, axis)
end

function Formats.format_get_vector(
    read_only_view::ReadOnlyView,
    axis::AbstractString,
    name::AbstractString,
)::StorageVector
    return as_read_only(Formats.format_get_vector(read_only_view.daf, axis, name))
end

function Formats.format_has_matrix(
    read_only_view::ReadOnlyView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    return Formats.format_has_matrix(read_only_view.daf, rows_axis, columns_axis, name)
end

function Formats.format_matrix_names(
    read_only_view::ReadOnlyView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractStringSet
    return Formats.format_matrix_names(read_only_view.daf, rows_axis, columns_axis)
end

function Formats.format_get_matrix(
    read_only_view::ReadOnlyView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    return as_read_only(Formats.format_get_matrix(read_only_view.daf, rows_axis, columns_axis, name))
end

function Formats.format_description_header(
    read_only_view::ReadOnlyView,
    indent::AbstractString,
    lines::Array{String},
)::Nothing
    push!(lines, "$(indent)type: ReadOnly $(typeof(read_only_view.daf))")
    return nothing
end

function Messages.present(value::ReadOnlyView)::String
    return "ReadOnly $(typeof(value.daf)) $(value.name)"
end

end
