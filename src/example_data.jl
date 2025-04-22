"""
Example data for doctest tests.
"""
module ExampleData

export example_cells_daf
export example_metacells_daf

using ..Formats
using ..MemoryFormat
using ..Writers

import ..FilesFormat.mmap_file_lines

"""
    example_cells_daf(; name::AbstractString = "cells!")::MemoryDaf

Load the cells example data into a `MemoryDaf`.
"""
function example_cells_daf(; name::AbstractString = "cells!")::MemoryDaf
    return example_daf('c'; name)
end

"""
    example_metacells_daf(; name::AbstractString = "cells!")::MemoryDaf

Load the metacells example data into a `MemoryDaf`.
"""
function example_metacells_daf(; name::AbstractString = "metacells!")::MemoryDaf
    return example_daf('m'; name)
end

function example_daf(which::Char; name::AbstractString)::MemoryDaf
    daf = MemoryDaf(; name)

    for file in readdir(joinpath(@__DIR__, "..", "test", "example_data", "axes"))
        load_axis(daf, which, file)
    end

    for file in readdir(joinpath(@__DIR__, "..", "test", "example_data", "vectors"))
        load_vector(daf, which, file)
    end

    for file in readdir(joinpath(@__DIR__, "..", "test", "example_data", "matrices"))
        load_matrix(daf, which, file)
    end

    return daf
end

function load_axis(daf::DafWriter, which::Char, file::AbstractString)::Nothing
    parts = split(file, ".")
    @assert length(parts) == 3
    kind, axis, suffix = parts
    @assert suffix == "txt"

    if which in kind
        entries = mmap_file_lines(joinpath(@__DIR__, "..", "test", "example_data", "axes", file))
        add_axis!(daf, axis, entries)
    end

    return nothing
end

function load_vector(daf::DafWriter, which::Char, file::AbstractString)::Nothing
    parts = split(file, ".")
    @assert length(parts) == 4
    kind, axis, property, suffix = parts
    @assert suffix == "txt"

    if which in kind
        vector = mmap_file_lines(joinpath(@__DIR__, "..", "test", "example_data", "vectors", file))
        vector = cast_vector(vector)
        set_vector!(daf, axis, property, vector)
    end

    return nothing
end

function cast_vector(vector::AbstractVector{<:AbstractString})::AbstractVector
    try
        return parse.(Bool, vector)
    catch
    end

    try
        vector = parse.(Float32, vector)
        for type in (UInt32, Int32)
            try
                return type.(vector)  # NOJET
            catch  # UNTESTED
            end
        end
        return vector  # UNTESTED
    catch
    end

    return vector
end

function load_matrix(daf::DafWriter, which::Char, file::AbstractString)::Nothing
    parts = split(file, ".")
    @assert length(parts) == 5
    kind, lines_axis, values_axis, property, suffix = parts
    @assert suffix == "csv"

    if which in kind
        lines = mmap_file_lines(joinpath(@__DIR__, "..", "test", "example_data", "matrices", file))
        n_lines = length(lines)
        n_values = length(split(lines[1], ","))

        matrix = Matrix{Float32}(undef, n_values, n_lines)
        for (line_index, line) in enumerate(lines)
            matrix[:, line_index] = parse.(Float32, split(line, ","))
        end

        matrix = cast_matrix(matrix)
        set_matrix!(daf, values_axis, lines_axis, property, matrix; relayout = eltype(matrix) <: Integer)
    end

    return nothing
end

function cast_matrix(matrix::AbstractMatrix{<:Real})::AbstractMatrix
    for type in (UInt8, UInt16)
        try
            return Matrix{type}(matrix)
        catch
        end
    end
    return matrix
end

end  # module
