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
export function_contract

using ..Contracts
using ..Formats
using ..StorageTypes
using DocStringExtensions
using ExprTools
using TanayLabUtilities

import ..Contracts.contract_documentation
import TanayLabUtilities.Documentation.DefaultValue
import TanayLabUtilities.Documentation.FunctionMetadata
import TanayLabUtilities.Documentation.collect_defaults
import TanayLabUtilities.Documentation.function_metadata
import TanayLabUtilities.Documentation.get_metadata
import TanayLabUtilities.Documentation.set_metadata_of_function
import TanayLabUtilities.Logger.pass_args

function kwargs_overwrite(kwargs::Base.Pairs)::Bool
    for (name, value) in kwargs
        if name == :overwrite
            @assert value isa Bool "non-Bool overwrite keyword parameter type: $(typeof(value)) = $(value)"
            return value
        end
    end
    return false
end

function computation_wrapper(contract::Contract, name::AbstractString, inner_function)
    return (daf::DafReader, args...; kwargs...) -> (
        #! format: off
        contract_daf = contractor(name, contract, daf; overwrite = kwargs_overwrite(kwargs));
        verify_input(contract_daf);
        result = inner_function(contract_daf, args...; kwargs...);
        verify_output(contract_daf);
        result  # flaky tested
        #! format: on
    )
end

function computation_wrapper(first_contract::Contract, second_contract::Contract, name::AbstractString, inner_function)
    return (first_daf::DafReader, second_daf::DafReader, args...; kwargs...) -> (  # NOJET
        #! format: off
        first_contract_daf = contractor(name * ".1", first_contract, first_daf; overwrite = kwargs_overwrite(kwargs));
        second_contract_daf = contractor(name * ".2", second_contract, second_daf; overwrite = kwargs_overwrite(kwargs));  # NOJET
        verify_input(first_contract_daf);
        verify_input(second_contract_daf);
        result = inner_function(first_contract_daf, second_contract_daf, args...; kwargs...);
        verify_output(first_contract_daf);
        verify_output(second_contract_daf);
        result  # flaky tested
        #! format: on
    )
end

"""
    @computation Contract(...) function something(daf::DafWriter, ...)
        return ...
    end

    @computation Contract(...) Contract(...) function something(
        first::DafReader/DafWriter, second::DafReader/DafWriter, ...
    )
        return ...
    end

Mark a function as a `Daf` computation. This has the following effects:

  - It has the same effect as `@documented`, that is, allows using `DEFAULT` in the documentation string,
    and using `function_default` to access the default value of named parameters.
  - It verifies that the `Daf` data satisfies the [`Contract`](@ref), when the computation is invoked and when it is
    complete (using [`verify_input`](@ref) and [`verify_output`](@ref)).
  - It stashes the contract(s) (if any) in a global variable. This allows expanding [`CONTRACT`](@ref) in the
    documentation string (for a single contract case), or [`CONTRACT1`](@ref) and [`CONTRACT2`](@ref) (for the dual
    contract case).
  - It logs the invocation of the function (using `@debug`), including the actual values of the named arguments (using
    `brief`).

!!! note

    For each [`Contract`](@ref) parameter (if any), there needs to be a [`DafReader`](@ref) or [`DafWriter`](@ref),
    which the contract(s) will be applied to. These parameters should be the initial positional parameters of the
    function.
"""
macro computation(contract, definition)
    while definition.head === :macrocall
        definition = macroexpand(__module__, definition)
    end

    inner_definition = ExprTools.splitdef(definition)
    outer_definition = copy(inner_definition)

    function_name = get(inner_definition, :name, nothing)
    if function_name === nothing
        error("@computation requires a named function")
    end
    @assert function_name isa Symbol
    function_module = __module__
    full_name = "$(function_module).$(function_name)"

    set_metadata_of_function(
        function_module,
        function_name,
        FunctionMetadata([function_module.eval(contract)], collect_defaults(function_module, inner_definition)),
    )

    inner_definition[:name] = Symbol(function_name, :_compute)
    outer_definition[:body] = Expr(
        :call,
        :(DataAxesFormats.Computations.computation_wrapper(
            $function_module.__TLU_FUNCTION_METADATA__[Symbol($function_name)].contracts[1],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        pass_args(false, get(outer_definition, :args, []))...,
        pass_args(true, get(outer_definition, :kwargs, []))...,
    )

    return esc(ExprTools.combinedef(outer_definition))
end

macro computation(first_contract, second_contract, definition)
    while definition.head === :macrocall
        definition = macroexpand(__module__, definition)
    end

    inner_definition = ExprTools.splitdef(definition)
    outer_definition = copy(inner_definition)

    function_name = get(inner_definition, :name, nothing)
    if function_name === nothing
        error("@computation requires a named function")
    end
    @assert function_name isa Symbol
    function_module = __module__
    full_name = "$(function_module).$(function_name)"

    set_metadata_of_function(
        function_module,
        function_name,
        FunctionMetadata(
            [function_module.eval(first_contract), function_module.eval(second_contract)],
            collect_defaults(function_module, inner_definition),
        ),
    )

    inner_definition[:name] = Symbol(function_name, :_compute)
    outer_definition[:body] = Expr(
        :call,
        :(DataAxesFormats.Computations.computation_wrapper(
            $function_module.__TLU_FUNCTION_METADATA__[Symbol($function_name)].contracts[1],
            $function_module.__TLU_FUNCTION_METADATA__[Symbol($function_name)].contracts[2],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        pass_args(false, get(outer_definition, :args, []))...,
        pass_args(true, get(outer_definition, :kwargs, []))...,
    )

    return esc(ExprTools.combinedef(outer_definition))
end

struct ContractDocumentation <: DocStringExtensions.Abbreviation
    index::Int
end

function DocStringExtensions.format(which::ContractDocumentation, buffer::IOBuffer, doc_str::Base.Docs.DocStr)::Nothing
    full_name, metadata = get_metadata(doc_str)
    if which.index > length(metadata.contracts)
        @assert which.index == 2
        error(chomp("""
              no second contract associated with: $(full_name)
              use: @computation Contract(...) Contract(...) function $(full_name)(...)
              """))
    end
    contract_documentation(metadata.contracts[which.index], buffer)
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
    @computation Contract(...) function something(daf::DafWriter, ...)
        return ...
    end

Then `\$(CONTRACT)` will be expanded with a description of the [`Contract`](@ref). This is based on
`DocStringExtensions`.

!!! note

    The first argument of the function must be a [`DafWriter`](@ref), which the contract will be applied to.
"""
const CONTRACT = ContractDocumentation(1)

"""
Same as [`CONTRACT`](@ref), but reference the contract for the 1st `Daf` argument for a [`@computation`](@ref) with two
such arguments.
"""
const CONTRACT1 = ContractDocumentation(1)

"""
Same as [`CONTRACT`](@ref), but reference the contract for the 2nd `Daf` argument for a [`@computation`](@ref) with two
such arguments.
"""
const CONTRACT2 = ContractDocumentation(2)

"""
    function_contract(func::Function[, index::Integer = 1])::Contract

Access the contract of a function annotated by [`@computation`](@ref). By default the first contract is returned. If the
[`@computation`](@ref) has two contracts, you can specify the `index` of the contract to return.
"""
function function_contract(func::Function, index::Integer = 1)::Contract
    _, _, metadata = function_metadata(func)
    return metadata.contracts[index]
end

end # module
