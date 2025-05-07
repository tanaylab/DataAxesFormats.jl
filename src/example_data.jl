"""
Example data for doctest tests.
"""
module ExampleData

export example_cells_daf
export example_metacells_daf
export example_chain_daf

using ..Chains
using ..Formats
using ..MemoryFormat
using ..Writers

import ..FilesFormat.mmap_file_lines

"""
    example_cells_daf(; name::AbstractString = "cells!")::MemoryDaf

Load the cells example data into a `MemoryDaf`.

```jldoctest
print(description(example_cells_daf()))

# output

name: cells!
type: MemoryDaf
scalars:
  organism: "human"
axes:
  cell: 856 entries
  donor: 95 entries
  experiment: 23 entries
  gene: 683 entries
vectors:
  cell:
    donor: 856 x Str (Dense)
    experiment: 856 x Str (Dense)
  donor:
    age: 95 x UInt32 (Dense)
    sex: 95 x Str (Dense)
  gene:
    is_lateral: 683 x Bool (Dense; 64% true)
matrices:
  cell,gene:
    UMIs: 856 x 683 x UInt8 in Columns (Dense)
  gene,cell:
    UMIs: 683 x 856 x UInt8 in Columns (Dense)
```
"""
function example_cells_daf(; name::AbstractString = "cells!")::MemoryDaf
    return example_daf('c'; name)
end

"""
    example_metacells_daf(; name::AbstractString = "cells!")::MemoryDaf

Load the metacells example data into a `MemoryDaf`.

```jldoctest
print(description(example_metacells_daf()))

# output

name: metacells!
type: MemoryDaf
axes:
  cell: 856 entries
  gene: 683 entries
  metacell: 7 entries
  type: 4 entries
vectors:
  cell:
    metacell: 856 x Str (Dense)
  gene:
    is_marker: 683 x Bool (Dense; 95% true)
  metacell:
    type: 7 x Str (Dense)
  type:
    color: 4 x Str (Dense)
matrices:
  gene,metacell:
    fraction: 683 x 7 x Float32 in Columns (Dense)
  metacell,metacell:
    edge_weight: 7 x 7 x Float32 in Columns (Dense)
```
"""
function example_metacells_daf(; name::AbstractString = "metacells!")::MemoryDaf
    return example_daf('m'; name)
end

function example_daf(which::Char; name::AbstractString)::MemoryDaf
    daf = MemoryDaf(; name)

    if which == 'c'
        set_scalar!(daf, "organism", "human")
    end

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

"""
    example_chain_daf(; name::AbstractString = "chain!")::DafWriter

Load a chain of both the cells and metacells example data.

```jldoctest
print(description(example_chain_daf()))

# output

name: chain!
type: Write Chain
chain:
- MemoryDaf cells!
- MemoryDaf metacells!
scalars:
  organism: "human"
axes:
  cell: 856 entries
  donor: 95 entries
  experiment: 23 entries
  gene: 683 entries
  metacell: 7 entries
  type: 4 entries
vectors:
  cell:
    donor: 856 x Str (Dense)
    experiment: 856 x Str (Dense)
    metacell: 856 x Str (Dense)
  donor:
    age: 95 x UInt32 (Dense)
    sex: 95 x Str (Dense)
  gene:
    is_lateral: 683 x Bool (Dense; 64% true)
    is_marker: 683 x Bool (Dense; 95% true)
  metacell:
    type: 7 x Str (Dense)
  type:
    color: 4 x Str (Dense)
matrices:
  cell,gene:
    UMIs: 856 x 683 x UInt8 in Columns (Dense)
  gene,cell:
    UMIs: 683 x 856 x UInt8 in Columns (Dense)
  gene,metacell:
    fraction: 683 x 7 x Float32 in Columns (Dense)
  metacell,metacell:
    edge_weight: 7 x 7 x Float32 in Columns (Dense)
```
"""
function example_chain_daf(; name::AbstractString = "chain!")::DafWriter
    return chain_writer([example_cells_daf(), example_metacells_daf()]; name)
end

end  # module
