"""
Read-only `Daf` storage format.

The `ReadOnlyView` class acts as a wrapper for any [`WriteDaf`](@ref) data, protecting it against accidental
modification.
"""
module ReadOnly

export ReadOnlyView
export read_only

using Daf.Data
using Daf.Formats
using Daf.Messages
using Daf.StorageTypes
using SparseArrays

"""
    ReadOnly(daf::F) where {F <: WriteDaf}

A wrapper for any [`WriteDaf`](@ref) data, protecting it against accidental modification. This isn't typically created
manually; instead call [`read_only`](@ref).
"""
struct ReadOnlyView{F} <: ReadDaf where {F <: WriteDaf}
    daf::F
end

function Base.getproperty(read_only_view::ReadOnlyView, property::Symbol)::Any
    daf = getfield(read_only_view, :daf)
    if property == :daf
        return daf
    elseif property == :internal
        return daf.internal
    elseif property == :name
        return daf.internal.name
    else
        return getfield(daf, property)  # untested
    end
end

"""
    read_only(daf::F)::ReadDaf where {F <: WriteDaf}
    read_only(daf::F)::F where {F <: ReadDaf}

Wrap `daf` with a `ReadOnlyView` to protect it against accidental modification. If given a read-only `daf`, return it
as-is.
"""
function read_only(daf::F)::ReadOnlyView{F} where {F <: WriteDaf}
    return ReadOnlyView(daf)
end

function read_only(daf::F)::F where {F <: ReadDaf}
    return daf
end

function Formats.format_has_scalar(read_only_view::ReadOnlyView, name::AbstractString)::Bool
    return Formats.format_has_scalar(read_only_view.daf, name)
end

function Formats.format_get_scalar(read_only_view::ReadOnlyView, name::AbstractString)::StorageScalar
    return Formats.format_get_scalar(read_only_view.daf, name)
end

function Formats.format_scalar_names(read_only_view::ReadOnlyView)::AbstractSet{String}
    return Formats.format_scalar_names(read_only_view.daf)
end

function Formats.format_has_axis(read_only_view::ReadOnlyView, axis::AbstractString)::Bool
    return Formats.format_has_axis(read_only_view.daf, axis)
end

function Formats.format_axis_names(read_only_view::ReadOnlyView)::AbstractSet{String}
    return Formats.format_axis_names(read_only_view.daf)
end

function Formats.format_get_axis(read_only_view::ReadOnlyView, axis::AbstractString)::DenseVector{String}
    return Formats.format_get_axis(read_only_view.daf, axis)
end

function Formats.format_axis_length(read_only_view::ReadOnlyView, axis::AbstractString)::Int64
    return Formats.format_axis_length(read_only_view.daf, axis)
end

function Formats.format_has_vector(read_only_view::ReadOnlyView, axis::AbstractString, name::AbstractString)::Bool
    return Formats.format_has_vector(read_only_view.daf, axis, name)
end

function Formats.format_vector_names(read_only_view::ReadOnlyView, axis::AbstractString)::AbstractSet{String}
    return Formats.format_vector_names(read_only_view.daf, axis)
end

function Formats.format_get_vector(
    read_only_view::ReadOnlyView,
    axis::AbstractString,
    name::AbstractString,
)::StorageVector
    return Formats.format_get_vector(read_only_view.daf, axis, name)
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
)::AbstractSet{String}
    return Formats.format_matrix_names(read_only_view.daf, rows_axis, columns_axis)
end

function Formats.format_get_matrix(
    read_only_view::ReadOnlyView,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::StorageMatrix
    return Formats.format_get_matrix(read_only_view.daf, rows_axis, columns_axis, name)
end

end
