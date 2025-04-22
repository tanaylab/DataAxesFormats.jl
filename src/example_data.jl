"""
Example data for doctest tests.
"""
module ExampleData

export example_daf

using ..Formats
using ..MemoryFormat
using ..Writers

import ..FilesFormat.mmap_file_lines

"""
    example_daf(; name::AbstractString = "example!")::MemoryDaf

Load the example data into a `MemoryDaf`.
"""
function example_daf(; name::AbstractString = "example!")::MemoryDaf
    daf = MemoryDaf(; name)

    for file in readdir(joinpath(@__DIR__, "..", "test", "example_data", "axes"))
        load_axis(daf, file)
    end

    for file in readdir(joinpath(@__DIR__, "..", "test", "example_data", "vectors"))
        load_vector(daf, file)
    end

    for file in readdir(joinpath(@__DIR__, "..", "test", "example_data", "matrices"))
        load_matrix(daf, file)
    end

    return daf
end

function load_axis(daf::DafWriter, file::AbstractString)::Nothing
    @assert endswith(file, ".entries.txt")
    axis = file[1:(end - 12)]
    entries = mmap_file_lines(joinpath(@__DIR__, "..", "test", "example_data", "axes", file))
    add_axis!(daf, axis, entries)
    return nothing
end

function load_vector(daf::DafWriter, file::AbstractString)::Nothing
    parts = split(file, ".")
    @assert length(parts) == 3
    axis, property, suffix = parts
    @assert suffix == "txt"

    vector = mmap_file_lines(joinpath(@__DIR__, "..", "test", "example_data", "vectors", file))

    vector = cast_vector(vector)
    set_vector!(daf, axis, property, vector)
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

function load_matrix(daf::DafWriter, file::AbstractString)::Nothing
    parts = split(file, ".")
    @assert length(parts) == 4
    lines_axis, values_axis, property, suffix = parts
    @assert suffix == "csv"

    lines = mmap_file_lines(joinpath(@__DIR__, "..", "test", "example_data", "matrices", file))
    n_lines = length(lines)
    n_values = length(split(lines[1], ","))

    matrix = Matrix{Float32}(undef, n_values, n_lines)
    for (line_index, line) in enumerate(lines)
        matrix[:, line_index] = parse.(Float32, split(line, ","))
    end

    matrix = cast_matrix(matrix)
    set_matrix!(daf, values_axis, lines_axis, property, matrix; relayout = eltype(matrix) <: Integer)
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
