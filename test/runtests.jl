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
    first_non_space = min([findfirst(character -> character != ' ', line) for line in lines]...)
    return join([line[first_non_space:end] for line in lines], "\n")
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
include("example_data.jl")
