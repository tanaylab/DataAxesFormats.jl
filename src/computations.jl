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
export DEFAULT

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

struct FunctionMetadata
    contracts::Vector{Contract}
    defaults::Dict{String, Any}
end

const METADATA_OF_FUNCTION = Dict{String, FunctionMetadata}()

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

    full_name = "$(__module__).$(function_name)"
    global METADATA_OF_FUNCTION
    METADATA_OF_FUNCTION[full_name] = FunctionMetadata([eval(contract)], collect_defaults(inner_definition))

    inner_definition[:name] = Symbol(function_name, :_inner)
    outer_definition[:body] = Expr(
        :call,
        :(Daf.Computations.with_contract(
            Daf.Computations.METADATA_OF_FUNCTION[$full_name].contracts[1],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        patch_args(get(outer_definition, :args, []))...,
        patch_kwargs(get(outer_definition, :kwargs, []))...,
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

    full_name = "$(__module__).$(function_name)"

    global METADATA_OF_FUNCTION
    METADATA_OF_FUNCTION[full_name] =
        FunctionMetadata([eval(first_contract), eval(second_contract)], collect_defaults(inner_definition))

    inner_definition[:name] = Symbol(function_name, :_inner)
    outer_definition[:body] = Expr(
        :call,
        :(Daf.Computations.with_contract(
            Daf.Computations.METADATA_OF_FUNCTION[$full_name].contracts[1],
            Daf.Computations.METADATA_OF_FUNCTION[$full_name].contracts[2],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        patch_args(get(outer_definition, :args, []))...,
        patch_kwargs(get(outer_definition, :kwargs, []))...,
    )

    return esc(ExprTools.combinedef(outer_definition))
end

function patch_args(args)::Any
    return [patch_arg(arg) for arg in args]
end

function patch_arg(arg::Symbol)::Any  # untested
    return arg
end

function patch_arg(arg::Expr)::Any
    if arg.head == :kw
        @assert length(arg.args) == 2
        return arg.args[1]
    end
    return arg
end

function patch_kwargs(args)::Any
    return [patch_kwarg(arg) for arg in args]  # NOJET
end

function patch_kwarg(arg::Expr)::Any
    if arg.head == :kw
        @assert length(arg.args) == 2
        arg = copy(arg)
        arg.args[2] = arg.args[1]
        if arg.args[1] isa Expr
            @assert arg.args[1].head == :(::)
            arg.args[1] = arg.args[1].args[1]
        end
    end
    return arg
end

function collect_defaults(inner_definition)::Dict{String, Any}
    defaults = Dict{String, Any}()
    for arg in get(inner_definition, :args, [])
        collect_arg_default(defaults, arg)
    end
    for kwarg in get(inner_definition, :kwargs, [])
        collect_arg_default(defaults, kwarg)
    end
    return defaults
end

function collect_arg_default(defaults::Dict{String, Any}, arg::Symbol)::Nothing  # untested
    return nothing  # untested
end

function collect_arg_default(defaults::Dict{String, Any}, arg::Expr)::Nothing
    if arg.head == :kw
        @assert length(arg.args) == 2
        name = arg.args[1]
        value = arg.args[2]
        if name isa Expr
            @assert name.head == :(::)
            @assert length(name.args) == 2
            name = name.args[1]
        end
        defaults[string(name)] = eval(value)
    end
    return nothing
end

struct ContractDocumentation <: DocStringExtensions.Abbreviation
    index::Int
end

function DocStringExtensions.format(which::ContractDocumentation, buffer::IOBuffer, doc_str::Base.Docs.DocStr)::Nothing
    full_name, metadata = get_metadata(doc_str)
    if which.index > length(metadata.contracts)
        @assert which.index == 2
        error(
            "no second contract associated with: $(full_name)\n" *
            "use: @computation Contract(...) Contract(...) function $(full_name)(...)",
        )
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
    @computation Contract(...)
    function something(daf::DafWriter, ...)
        return ...
    end

Then `\$(CONTRACT)` will be expanded with a description of the [`Contract`](@ref). This is based on `DocStringExtensions`.

!!! note

    The first argument of the function must be a [`DafWriter`](@ref), which the contract will be applied to.
"""
const CONTRACT = ContractDocumentation(1)

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

struct DefaultValue <: DocStringExtensions.Abbreviation
    name::String
end

function DocStringExtensions.format(what::DefaultValue, buffer::IOBuffer, doc_str::Base.Docs.DocStr)::Nothing
    full_name, metadata = get_metadata(doc_str)
    default = get(metadata.defaults, what.name, missing)
    if default === missing
        error("no default for a parameter: $(what.name)\n" * "in the computation: $(full_name)")
    end
    return print(buffer, default)
end

struct DefaultContainer end

function Base.getproperty(defaults::DefaultContainer, parameter::Symbol)::DefaultValue
    return DefaultValue(string(parameter))
end

"""
When using [`@computation`](@ref):

    '''
        something(daf::DafWriter, x::Int = \$(DEFAULT.x); y::Bool = \$(DEFAULT.y))

    ...
    If `x` (default: \$(DEFAULT.y)) is even, ...
    ...
    If `y` (default: \$(DEFAULT.y)) is set, ...
    ...
    '''
    @computation Contract(...)
    function something(daf::DafWriter, x::Int = 0; y::Bool = false)
        return ...
    end

Then `\$(DEFAULT.x)` will be expanded with the default value of the parameter `x`. It is good practice to contain a
description of the effects of each parameter somewhere in the documentation, and it is polite to also provide its
default value. This can be done in either the signature line or in the text, or both. Using `DEFAULT` ensures that the
correct value is used in the documentation.
"""
const DEFAULT = DefaultContainer()

function get_metadata(doc_str::Base.Docs.DocStr)::Tuple{String, FunctionMetadata}
    binding = doc_str.data[:binding]
    object = Docs.resolve(binding)
    full_name = "$(doc_str.data[:module]).$(Symbol(object))"
    metadata = get(METADATA_OF_FUNCTION, full_name, missing)
    if metadata === missing
        error(
            "no contract(s) associated with: $(full_name)\n" *
            "use: @computation Contract(...) function $(full_name)(...)",
        )
    end
    return full_name, metadata
end

end # module
