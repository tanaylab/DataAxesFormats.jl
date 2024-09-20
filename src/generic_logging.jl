"""
Generic macros and functions for logging, that arguably should belong in a more general-purpose package.

We do not re-export the macros and functions defined here from the top-level `Daf` namespace. That is, even if
`using DafJL`, you will **not** have these generic names polluting your namespace. If you do want to reuse them
in your code, explicitly write `using DafJL.GenericLogging`.
"""
module GenericLogging

export @logged
export setup_logger

using ..GenericTypes
using ..Messages
using Base.Threads
using Dates
using Distributed
using ExprTools
using Logging

"""
    setup_logger(
        io::IO = stderr;
        [level::LogLevel = Warn,
        show_time::Bool = true,
        show_module::Bool = true,
        show_location::Bool = false]
    )::Nothing

Setup a global logger that will print into `io`, printing messages with a timestamp prefix.

By default, this will only print warnings. Note that increasing the log level will apply to **everything**. An
alternative is to set up the environment variable `JULIA_DEBUG` to a comma-separated list of modules you wish to see the
debug messages of.

If `show_time`, each message will be prefixed with a `yyyy-dd-mm HH:MM:SS.sss` timestamp prefix.

If `show_module`, each message will be prefixed with the name of the module emitting the message.

If `show_location`, each message will be prefixed with the file name and the line number emitting the message.

!!! note

    When multi-processing is used, a `P<id>:` process index is added to the log entries. When multi-threading is used, a
    `T<id>:` thread index is added to the log entries, as well as a `K<id>:` task index. To generate the latter, this
    stores a unique `:task_id` index in the `task_local_storage`. This is important since a task may migrate between
    threads.
"""
function setup_logger(
    io::IO = stderr;
    level::LogLevel = Warn,
    show_time::Bool = true,
    show_module::Bool = true,
    show_location::Bool = false,
)::Nothing
    global_logger(
        ConsoleLogger(io, level; meta_formatter = (args...) -> metafmt(show_time, show_module, show_location, args...)),
    )
    return nothing
end

"""
    @logged function something(...)
        return ...
    end

Automatically log (in `Debug` level) every invocation to the function. This will also log the values of the arguments.
Emits a second log entry when the function returns, with the result (if any).
"""
macro logged(definition)
    while definition.head === :macrocall
        definition = macroexpand(__module__, definition)
    end

    inner_definition = ExprTools.splitdef(definition)
    outer_definition = copy(inner_definition)

    function_name = get(inner_definition, :name, nothing)
    if function_name === nothing
        error("@logged requires a named function")
    end
    @assert function_name isa Symbol
    function_module = __module__
    function_file = string(__source__.file)
    function_line = __source__.line
    full_name = "$(function_module).$(function_name)"

    has_result = get(inner_definition, :rtype, :Any) != :Nothing
    arg_names = [parse_arg(arg) for arg in get(outer_definition, :args, [])]
    inner_definition[:name] = Symbol(function_name, :_logged)
    if startswith(full_name, "DafJL.") || contains(full_name, ".DafJL.")
        outer_definition[:body] = Expr(
            :call,
            :(GenericLogging.logged_wrapper(
                $function_module,
                $function_file,
                $function_line,
                $full_name,
                $arg_names,
                $has_result,
                $(ExprTools.combinedef(inner_definition)),
            )),
            pass_args(false, get(outer_definition, :args, []))...,
            pass_args(true, get(outer_definition, :kwargs, []))...,
        )
    else
        outer_definition[:body] = Expr(
            :call,
            :(DafJL.GenericLogging.logged_wrapper(
                $function_module,
                $function_file,
                $function_line,
                $full_name,
                $arg_names,
                $has_result,
                $(ExprTools.combinedef(inner_definition)),
            )),
            pass_args(false, get(outer_definition, :args, []))...,
            pass_args(true, get(outer_definition, :kwargs, []))...,
        )
    end

    return esc(ExprTools.combinedef(outer_definition))
end

function parse_arg(arg::Symbol)::AbstractString
    return split(string(arg), "::"; limit = 2)[1]
end

function parse_arg(arg::Expr)::AbstractString
    return parse_arg(arg.args[1])
end

function logged_wrapper(
    _module::Module,
    _file::AbstractString,
    _line::Integer,
    name::AbstractString,
    arg_names::AbstractVector{<:AbstractString},
    has_result::Bool,
    inner_function,
)
    return (args...; kwargs...) -> (@debug "$(name) {" _module = _module _file = _file _line = _line;
    for (arg_name, value) in zip(arg_names, args)
        @debug "- $(arg_name): $(depict(value))" _module = _module _file = _file _line = _line
    end;
    for (name, value) in kwargs
        @debug "- $(name): $(depict(value))" _module = _module _file = _file _line = _line
    end;
    result = inner_function(args...; kwargs...);
    if has_result
        @debug "$(name) return: $(depict(result)) }" _module = _module _file = _file _line = _line
    else
        @debug "$(name) return }" _module = _module _file = _file _line = _line
    end;
    result)  # flaky tested
end

NEXT_TASK_ID = Atomic{Int}(1)

function metafmt(
    show_time::Bool,
    show_module::Bool,
    show_location::Bool,
    level::LogLevel,
    _module,
    ::Symbol,
    ::Symbol,
    file::AbstractString,
    line::Integer,
)::Tuple{Symbol, AbstractString, AbstractString}
    @nospecialize
    color = Logging.default_logcolor(level)
    prefix_parts = []
    if show_time
        push!(prefix_parts, Dates.format(now(), "yyyy-mm-dd HH:MM:SS.sss"))
    end
    if nprocs() > 1
        push!(prefix_parts, "P$(myid())")  # untested
    end
    if nthreads() > 1
        push!(prefix_parts, "T$(threadid())")
        task_id = get!(task_local_storage(), :task_id) do
            return atomic_add!(NEXT_TASK_ID, 1)
        end
        push!(prefix_parts, "K$(task_id)")
    end
    push!(prefix_parts, string(level == Warn ? "Warning" : string(level)))
    if show_module
        push!(prefix_parts, string(_module))
    end
    if show_location
        push!(prefix_parts, "$(file):$(line)")  # untested
    end
    prefix = join(prefix_parts, ": ") * ":"
    return color, prefix, ""
end

function pass_args(is_named::Bool, args)::Vector{Union{Expr, Symbol}}
    return [pass_arg(is_named, arg) for arg in args]  # NOJET
end

function pass_arg(is_named::Bool, arg::Symbol)::Union{Expr, Symbol}
    arg = Symbol(parse_arg(arg))
    if is_named
        return Expr(:kw, arg, arg)
    else
        return arg
    end
end

function pass_arg(is_named::Bool, arg::Expr)::Union{Expr, Symbol}
    return pass_arg(is_named, arg.args[1])
end

end  # module
