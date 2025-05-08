using Test

using Base.MathConstants
using DataAxesFormats
using Documenter
using ExceptionUnwrapping
using HDF5
using LinearAlgebra
using Logging
using Muon
using NamedArrays
using NestedTests
using SparseArrays
using TanayLabUtilities

import Random

setup_logger(; level = Info)

test_prefixes(ARGS)

Random.seed!(123456)
TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
abort_on_first_failure(true)

function with_unwrapping_exceptions(action::Function)::Any
    try
        return action()
    catch exception
        while true
            if exception isa CompositeException
                inner_exception = exception.exceptions[1]
            elseif exception isa TaskFailedException
                inner_exception = unwrap_exception_to_root(exception)
            else
                inner_exception = exception
            end
            if inner_exception === exception
                throw(exception)
            end
            exception = inner_exception
        end
    end
end

nested_test("doctests") do
    DocMeta.setdocmeta!(DataAxesFormats, :DocTestSetup, :(using DataAxesFormats); recursive = true)
    return doctest(DataAxesFormats; manual = false)
end

include("read_only.jl")
include("data.jl")
include("anndata.jl")
include("reconstruction.jl")
include("tokens.jl")
include("queries.jl")
include("registry.jl")
include("operations.jl")
include("chains.jl")
include("views.jl")
include("contracts.jl")
include("computations.jl")
include("copies.jl")
include("concat.jl")
include("adapters.jl")
include("groups.jl")
include("example_data.jl")
