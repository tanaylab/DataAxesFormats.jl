"""
Read-only `Daf` storage format.
"""
module ReadOnly

export DafReadOnly
export is_read_only_array
export read_only
export read_only_array

using ..Formats
using ..GenericTypes
using ..MatrixLayouts  # For documentation.
using ..Messages
using ..Readers
using ..StorageTypes
using NamedArrays
using LinearAlgebra
using SparseArrays

import ..Formats.Internal
import ..Messages
import ..MatrixLayouts.mutable_array

"""
    read_only_array(array::AbstractArray):AbstractArray

Return an immutable view of an `array`. This uses `SparseArrays.ReadOnly`, and properly deals with `NamedArray`. If the
array is already immutable, it is returned as-is.
"""
function read_only_array(array::AbstractArray)::AbstractArray
    return Formats.read_only_array(array)
end

"""
    is_read_only_array(array::AbstractArray)::Bool

Return whether an `array` is immutable.
"""
function is_read_only_array(array::AbstractArray)::Bool
    return mutable_array(array) !== array
end

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
    @debug "Daf: $(depict(wrapper)) base: $(daf)"
    return wrapper
end

function read_only(daf::DafReadOnly; name::Maybe{AbstractString} = nothing)::DafReadOnly
    if name === nothing
        return daf
    else
        wrapper = DafReadOnlyWrapper(name, daf.internal, daf.daf)
        @debug "Daf: $(depict(wrapper)) base: $(daf.daf)"
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
    return Formats.read_only_array(Formats.format_get_vector(read_only_view.daf, axis, name))
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
    return Formats.read_only_array(Formats.format_get_matrix(read_only_view.daf, rows_axis, columns_axis, name))
end

function Formats.format_description_header(
    read_only_view::DafReadOnlyWrapper,
    indent::AbstractString,
    lines::Vector{String},
    ::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(read_only_view)
    push!(lines, "$(indent)type: ReadOnly")
    push!(lines, "$(indent)base: $(depict(read_only_view.daf))")
    return nothing
end

function Messages.depict(value::DafReadOnlyWrapper; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ReadOnly $(depict(value.daf; name))"
end

end  # module
