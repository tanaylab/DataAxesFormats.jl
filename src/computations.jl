"""
Support writing "well-behaved" computations. Such computations declare a [`Contract`](@ref) describing their inputs and
outputs. This is enforced, so that the implementation need not worry about missing inputs, and the caller can rely on
the results. It is also self-documenting, so the generated documentation is always contains a clear up-to-date
description of the contract.
"""
module Computations

export @computation
export CONTRACT
export CONTRACT1
export CONTRACT2

using Daf.Contracts
using Daf.Formats
using DocStringExtensions
using ExprTools

import Daf.Contracts.contract_documentation

function with_contract(contract::Contract, name::String, inner_function)
    return (daf::DafReader, args...; kwargs...) -> (verify_input(contract, name, daf);
    result = inner_function(daf, args...; kwargs...);
    verify_output(contract, name, daf);
    result)
end

function with_contract(first_contract::Contract, second_contract::Contract, name::String, inner_function)
    return (first_daf::DafReader, second_daf::DafReader, args...; kwargs...) ->
        (verify_input(first_contract, name, first_daf);
        verify_input(second_contract, name, second_daf);
        result = inner_function(first_daf, second_daf, args...; kwargs...);
        verify_output(first_contract, name, first_daf);
        verify_output(second_contract, name, second_daf);
        result)
end

const CONTRACT_OF_FUNCTION = Dict{String, Contract}()

"""
    @computation Contract(...) function something(daf::DafWriter, ...)
        return ...
    end

    @computation Contract(...) Contract(...) function something(
        first::DafReader/DafWriter, second::DafReader/DafWriter, ...
    )
        return ...
    end

Mark a function as a `daf` computation. This has two effects. First, it verifies that the `daf` data satisfies the
[`Contract`](@ref) when the computation is invoked and when it is complete (using [`verify_input`](@ref) and
[`verify_output`](@ref)); second, it stashed the contract in a global variable to allow expanding [`CONTRACT`](@ref) in
the documentation string.

Also allows for computations with two `daf` parameters, with a separate contract for each. In this case, use
[`CONTRACT1`](@ref) and [`CONTRACT2`](@ref) in the documentation string.

!!! note

    The first argument(s) of the function must be a [`DafReader`](@ref) or [`DafWriter`](@ref), which the contract(s)
    will be applied to.
"""
macro computation(contract, definition)
    inner_definition = ExprTools.splitdef(definition)
    outer_definition = copy(inner_definition)
    function_name = get(inner_definition, :name, nothing)
    if function_name == nothing
        error("@computation requires a named function")
    end
    @assert function_name isa Symbol
    inner_definition[:name] = Symbol(function_name, :_inner)
    full_name = "$(__module__).$(function_name)"
    global CONTRACT_OF_FUNCTION
    CONTRACT_OF_FUNCTION[full_name] = eval(contract)
    outer_definition[:body] = Expr(
        :call,
        :(Daf.Computations.with_contract(
            Daf.Computations.CONTRACT_OF_FUNCTION[$full_name],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        get(outer_definition, :args, [])...,
        get(outer_definition, :kwargs, [])...,
    )
    return esc(ExprTools.combinedef(outer_definition))
end

macro computation(first_contract, second_contract, definition)
    inner_definition = ExprTools.splitdef(definition)
    outer_definition = copy(inner_definition)
    function_name = get(inner_definition, :name, nothing)
    if function_name == nothing
        error("@computation requires a named function")
    end
    @assert function_name isa Symbol
    inner_definition[:name] = Symbol(function_name, :_inner)
    full_name = "$(__module__).$(function_name)"
    first_name = full_name * ".1"
    second_name = full_name * ".2"
    global CONTRACT_OF_FUNCTION
    CONTRACT_OF_FUNCTION[first_name] = eval(first_contract)
    CONTRACT_OF_FUNCTION[second_name] = eval(second_contract)
    outer_definition[:body] = Expr(
        :call,
        :(Daf.Computations.with_contract(
            Daf.Computations.CONTRACT_OF_FUNCTION[$first_name],
            Daf.Computations.CONTRACT_OF_FUNCTION[$second_name],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        get(outer_definition, :args, [])...,
        get(outer_definition, :kwargs, [])...,
    )
    return esc(ExprTools.combinedef(outer_definition))
end

struct ContractDocumentation <: DocStringExtensions.Abbreviation
    index::Int
end

function DocStringExtensions.format(which::ContractDocumentation, buffer::IOBuffer, doc_str::Base.Docs.DocStr)::Nothing
    binding = doc_str.data[:binding]
    object = Docs.resolve(binding)
    full_name = "$(doc_str.data[:module]).$(Symbol(object))"
    if which.index == 0
        contract = get(CONTRACT_OF_FUNCTION, full_name, missing)
        if contract === missing
            error(
                "no single contract associated with: $(full_name)\n" *
                "use: @computation Contract(...) function $(full_name)(...)",
            )
        end
    else
        contract = get(CONTRACT_OF_FUNCTION, "$(full_name).$(which.index)", missing)
        if contract === missing
            error(
                "no dual contract associated with: $(full_name)\n" *
                "use: @computation Contract(...) Contract(...) function $(full_name)(...)",
            )
        end
    end
    contract_documentation(contract, buffer)
    return nothing
end

"""
When using [`@computation`](@ref):

    '''
    ...
    # Contract
    ...
    \$(CONTRACT)
    ...
    '''
    @computation Contract(...)
    function something(daf::DafWriter, ...)
        return ...
    end

Then `\$(CONTRACT)` will be expanded with a description of the [`Contract`](@ref). This is based on `DocStringExtensions`.

!!! note

    The first argument of the function must be a [`DafWriter`](@ref), which the contract will be applied to.
"""
const CONTRACT = ContractDocumentation(0)

"""
Same as [`CONTRACT`](@ref), but reference the contract for the 1st `daf` argument for a [`@computation`](@ref) with two
such arguments.
"""
const CONTRACT1 = ContractDocumentation(1)

"""
Same as [`CONTRACT`](@ref), but reference the contract for the 2nd `daf` argument for a [`@computation`](@ref) with two
such arguments.
"""
const CONTRACT2 = ContractDocumentation(2)

end # module
