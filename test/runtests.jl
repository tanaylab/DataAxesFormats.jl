using Test

using Base: elsize
using Base.MathConstants
using Daf
using LinearAlgebra
using SparseArrays
using TestContexts

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

include("as_dense.jl")
include("matrix_layouts.jl")
include("data_types.jl")
include("messages.jl")
include("storage.jl")
include("oprec.jl")
include("registry.jl")
include("query.jl")
#include("operations.jl")
include("example_data.jl")
