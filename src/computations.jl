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
export function_contract
export function_default

using ..Contracts
using ..Formats
using ..GenericFunctions
using ..Messages
using ..StorageTypes
using DocStringExtensions
using ExprTools

import ..Contracts.contract_documentation
import ..GenericLogging.pass_args

function computation_wrapper(::AbstractString, inner_function)
    return inner_function
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

function kwargs_overwrite(kwargs::Base.Pairs)::Bool
    for (name, value) in kwargs
        if name == :overwrite
            @assert value isa Bool "non-Bool overwrite keyword parameter type: $(typeof(value)) = $(value)"
            return value
        end
    end
    return false
end

struct FunctionMetadata
    contracts::Vector{Contract}
    defaults::Dict{Symbol, Any}
end

function set_metadata_of_function(
    function_module::Module,
    function_name::Symbol,
    function_metadata::FunctionMetadata,
)::Nothing
    if !isdefined(function_module, :__DAF_FUNCTION_METADATA__)
        function_module.__DAF_FUNCTION_METADATA__ = Dict{Symbol, FunctionMetadata}()
    end
    function_module.__DAF_FUNCTION_METADATA__[function_name] = function_metadata
    return nothing
end

"""
    @computation function something(...)
        return ...
    end

    @computation Contract(...) function something(daf::DafWriter, ...)
        return ...
    end

    @computation Contract(...) Contract(...) function something(
        first::DafReader/DafWriter, second::DafReader/DafWriter, ...
    )
        return ...
    end

Mark a function as a `Daf` computation. This has the following effects:

  - It verifies that the `Daf` data satisfies the [`Contract`](@ref), when the computation is invoked and when it is
    complete (using [`verify_input`](@ref) and [`verify_output`](@ref)).

  - It stashes the contract(s) (if any) in a global variable. This allows expanding [`CONTRACT`](@ref) in the
    documentation string (for a single contract case), or [`CONTRACT1`](@ref) and [`CONTRACT2`](@ref) (for the dual
    contract case).
  - It stashes the default value of named arguments. This allows expanding [`DEFAULT`](@ref) in the documentation
    string, which is especially useful if these defaults are computed, read from global constants, etc.
  - It logs the invocation of the function (using `@debug`), including the actual values of the named arguments (using
    [`depict`](@ref)).

!!! note

    For each [`Contract`](@ref) parameter (if any), there needs to be a [`DafReader`](@ref) or [`DafWriter`](@ref),
    which the contract(s) will be applied to. These parameters should be the initial positional parameters of the
    function.
"""
macro computation(definition)
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
        FunctionMetadata(Contract[], collect_defaults(function_module, inner_definition)),
    )

    inner_definition[:name] = Symbol(function_name, :_compute)
    outer_definition[:body] = Expr(
        :call,
        :(DafJL.Computations.computation_wrapper($full_name, $(ExprTools.combinedef(inner_definition)))),
        pass_args(false, get(outer_definition, :args, []))...,
        pass_args(true, get(outer_definition, :kwargs, []))...,
    )

    return esc(ExprTools.combinedef(outer_definition))
end

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
        :(DafJL.Computations.computation_wrapper(
            $function_module.__DAF_FUNCTION_METADATA__[Symbol($function_name)].contracts[1],
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
        :(DafJL.Computations.computation_wrapper(
            $function_module.__DAF_FUNCTION_METADATA__[Symbol($function_name)].contracts[1],
            $function_module.__DAF_FUNCTION_METADATA__[Symbol($function_name)].contracts[2],
            $full_name,
            $(ExprTools.combinedef(inner_definition)),
        )),
        pass_args(false, get(outer_definition, :args, []))...,
        pass_args(true, get(outer_definition, :kwargs, []))...,
    )

    return esc(ExprTools.combinedef(outer_definition))
end

function collect_defaults(function_module::Module, inner_definition)::Dict{Symbol, Any}
    defaults = Dict{Symbol, Any}()
    for arg in get(inner_definition, :args, [])
        collect_arg_default(function_module, defaults, arg)
    end
    for kwarg in get(inner_definition, :kwargs, [])
        collect_arg_default(function_module, defaults, kwarg)
    end
    return defaults
end

function collect_arg_default(::Module, ::Dict{Symbol, Any}, ::Symbol)::Nothing  # untested
    return nothing
end

function collect_arg_default(function_module::Module, defaults::Dict{Symbol, Any}, arg::Expr)::Nothing
    if arg.head == :kw
        @assert length(arg.args) == 2
        name = arg.args[1]
        value = arg.args[2]
        if name isa Expr
            @assert name.head == :(::)
            @assert length(name.args) == 2
            name = name.args[1]
        end
        defaults[name] = function_module.eval(value)
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
        error(dedent("""
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

struct DefaultValue <: DocStringExtensions.Abbreviation
    name::Symbol
end

function DocStringExtensions.format(what::DefaultValue, buffer::IOBuffer, doc_str::Base.Docs.DocStr)::Nothing
    full_name, metadata = get_metadata(doc_str)
    default = get(metadata.defaults, what.name, missing)
    if default === missing
        error(dedent("""
            no default for a parameter: $(what.name)
            in the computation: $(full_name)
        """))
    end
    if default isa AbstractString
        default = replace(default, "\\" => "\\\\", "\"" => "\\\"")
        default = "\"$(default)\""
    end
    return print(buffer, default)
end

struct DefaultContainer end

function Base.getproperty(::DefaultContainer, parameter::Symbol)::DefaultValue
    return DefaultValue(parameter)
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
    @computation Contract(...) function something(daf::DafWriter, x::Int = 0; y::Bool = false)
        return ...
    end

Then `\$(DEFAULT.x)` will be expanded with the default value of the parameter `x`. It is good practice to contain a
description of the effects of each parameter somewhere in the documentation, and it is polite to also provide its
default value. This can be done in either the signature line or in the text, or both. Using `DEFAULT` ensures that the
correct value is used in the documentation.
"""
const DEFAULT = DefaultContainer()

function get_metadata(doc_str::Base.Docs.DocStr)::Tuple{AbstractString, FunctionMetadata}
    binding = doc_str.data[:binding]
    object = Docs.resolve(binding)
    object_module = nothing
    for method in methods(object)
        try
            object_module = method.module
            if isdefined(object_module, :__DAF_FUNCTION_METADATA__)
                break
            end
        catch  # untested
        end
    end
    if object_module === nothing
        metadata = nothing  # untested
    else
        metadata = get(object_module.__DAF_FUNCTION_METADATA__, Symbol(object), nothing)
    end
    if metadata === nothing
        error(dedent("""
            no contract(s) associated with: $(object_module).$(object)
            use: @computation Contract(...) function $(object_module).$(object)(...)
        """))
    end
    return "$(object_module).$(object)", metadata
end

"""
    function_contract(func::Function[, index::Integer = 1])::Contract

Access the contract of a function annotated by [`@computation`](@ref). By default the first contract is returned. If the
[`@computation`](@ref) has two contracts, you can specify the `index` of the contract to return.
"""
function function_contract(func::Function, index::Integer = 1)::Contract
    _, _, metadata = function_metadata(func)
    return metadata.contracts[index]
end

"""
    function_default(func::Function, parameter::Symbol)::Contract

Access the default of a parameter of a function annotated by [`@computation`](@ref).
"""
function function_default(func::Function, parameter::Symbol)::Any
    function_module, function_name, metadata = function_metadata(func)
    default = get(metadata.defaults, parameter, missing)
    if default === missing
        error(dedent("""
            no parameter with default: $(parameter)
            for the function: $(function_module).$(function_name)
        """))
    end
    return default
end

function function_metadata(func::Function)::Tuple{Module, Symbol, FunctionMetadata}
    method = methods(func)[1]
    try
        return method.module, method.name, method.module.__DAF_FUNCTION_METADATA__[method.name]
    catch
        error("not a @computation function: $(method.module).$(method.name)")
    end
end

end # module
