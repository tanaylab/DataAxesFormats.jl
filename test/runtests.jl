using Test

using Base: elsize
using Base.MathConstants
using DataAxesFormats
using DataAxesFormats.GenericFunctions
using DataAxesFormats.GenericLogging
using DataAxesFormats.GenericStorage
using DataAxesFormats.GenericTypes
using ExceptionUnwrapping
using HDF5
using LinearAlgebra
using Logging
using Muon
using NamedArrays
using NestedTests
using SparseArrays
using Statistics
using TestContexts

import Random

setup_logger(; level = Info)

test_prefixes(ARGS)

Random.seed!(123456)

inefficient_action_handler(ErrorHandler)
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

include("generic_storage.jl")
include("matrix_layouts.jl")
include("messages.jl")
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
