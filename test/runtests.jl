using Test

using Base: elsize
using Base.MathConstants
using Daf
using LinearAlgebra
using NamedArrays
using NestedTests
using SparseArrays
using TestContexts

test_prefixes(ARGS)

inefficient_action_policy(ErrorPolicy)

function dedent(string::AbstractString)::String
    lines = split(string, "\n")[1:(end - 1)]
    first_non_space = nothing
    for line in lines
        line_non_space = findfirst(character -> character != ' ', line)
        if first_non_space == nothing || (line_non_space != nothing && line_non_space < first_non_space)
            first_non_space = line_non_space
        end
    end
    if first_non_space == nothing
        return string  # untested
    else
        return join([line[first_non_space:end] for line in lines], "\n")
    end
end

#function test_similar(left::Any, right::Any)::Nothing
#    @test "$(left)" == "$(right)"
#    return nothing
#end

include("matrix_layouts.jl")
include("messages.jl")
include("data.jl")
include("oprec.jl")
include("registry.jl")
include("queries.jl")
include("views.jl")
include("chains.jl")
include("contracts.jl")
include("computations.jl")
include("example_data.jl")
