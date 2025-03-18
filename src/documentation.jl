"""
Enhanced documentation for functions. This also allows using the default values of function parameters as the basis for
default values of other function parameters. We **do** re-export these from the top-level `DataAxesFormats` namespace,
because this functionality is tightly coupled with [`Computations`](@ref DataAxesFormats.Computations).
"""
module Documentation

export @documented
export function_default
export documented_wrapper
export DEFAULT

using DocStringExtensions
using ExprTools

using ..GenericFunctions

import ..GenericLogging.pass_args

function documented_wrapper(::AbstractString, inner_function)
    return inner_function
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
    contracts::Vector{Any}  # Should be `Vector{Contract}` but that would make cyclical dependencies.
    defaults::Dict{Symbol, Any}
end

function set_metadata_of_function(
    function_module::Module,
    function_name::Symbol,
    function_metadata::FunctionMetadata,
)::Nothing
    if !isdefined(function_module, :__DAF_FUNCTION_METADATA__)
        @eval function_module __DAF_FUNCTION_METADATA__ = Dict{Symbol, Any}()
    end
    function_module.__DAF_FUNCTION_METADATA__[function_name] = function_metadata
    return nothing
end

"""
    @documented function something(...)
        return ...
    end

Enhance the documentation of a function. This stashes the default value of named arguments. This allows expanding
[`DEFAULT`](@ref) in the documentation string, which is especially useful if these defaults are computed, read from
global constants, copied from other functions via [`function_default`](@ref), etc.
"""
macro documented(definition)
    while definition.head === :macrocall
        definition = macroexpand(__module__, definition)
    end

    inner_definition = ExprTools.splitdef(definition)
    outer_definition = copy(inner_definition)

    function_name = get(inner_definition, :name, nothing)
    if function_name === nothing
        error("@documented requires a named function")
    end
    @assert function_name isa Symbol
    function_module = __module__
    full_name = "$(function_module).$(function_name)"

    set_metadata_of_function(
        function_module,
        function_name,
        FunctionMetadata(Any[], collect_defaults(function_module, inner_definition)),
    )

    inner_definition[:name] = Symbol(function_name, :_compute)
    outer_definition[:body] = Expr(
        :call,
        :(documented_wrapper($full_name, $(ExprTools.combinedef(inner_definition)))),
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

function collect_arg_default(::Module, ::Dict{Symbol, Any}, ::Symbol)::Nothing  # UNTESTED
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

"""
    function_default(func::Function, parameter::Symbol)::Contract

Access the default of a parameter of a function annotated by [`@documented`](@ref) or [`@computation`](@ref
DataAxesFormats.Computations.@computation).
"""
function function_default(func::Function, parameter::Symbol)::Any
    function_module, function_name, metadata = function_metadata(func)
    default = get(metadata.defaults, parameter, missing)
    if default === missing
        error(dedent("""
            no parameter with default: $(parameter)
            exists for the function: $(function_module).$(function_name)
        """))
    end
    return default
end

function function_metadata(func::Function)::Tuple{Module, Symbol, FunctionMetadata}
    method = methods(func)[1]
    try
        return method.module, method.name, method.module.__DAF_FUNCTION_METADATA__[method.name]
    catch
        error("not a @documented or @computation function: $(method.module).$(method.name)")
    end
end

struct DefaultContainer end

struct DefaultValue <: DocStringExtensions.Abbreviation
    name::Symbol
end

function Base.getproperty(::DefaultContainer, parameter::Symbol)::DefaultValue
    return DefaultValue(parameter)
end

"""
When using [`@documented`](@ref) or [`@computation`](@ref DataAxesFormats.Computations.@computation):

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
        catch  # UNTESTED
        end
    end
    if object_module === nothing
        metadata = nothing  # UNTESTED
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

end # module
