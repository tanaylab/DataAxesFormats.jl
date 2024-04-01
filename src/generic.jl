"""
Types that arguably should belong in a more general-purpose package.

We do not re-export the types and functions defined here from the top-level `Daf` namespace. That is, even if
`using Daf`, you will **not** have these generic names polluting your namespace. If you do want to reuse them in your
code, explicitly write `using Daf.Generic`.
"""
module Generic

export dedent
export ErrorHandler
export handle_abnormal
export AbnormalHandler
export IgnoreHandler
export Maybe
export Unsure
export WarnHandler

"""
The action to take when encountering an "abnormal" (but recoverable) operation.

Valid values are:

`IgnoreHandler` - ignore the issue and perform the recovery operation.

`WarnHandler` - emit a warning using `@warn`.

`ErrorHandler` - abort the program with an error message.
"""
@enum AbnormalHandler IgnoreHandler WarnHandler ErrorHandler

"""
    handle_abnormal(message::Function, handler::AbnormalHandler)::Nothing

Call this when encountering some abnormal, but recoverable, condition. Follow it by the recovery code.

This will `error` if the `handler` is `ErrorHandler`, and abort the program. If it is `WarnHandler`, it will just
`@warn` and return. If it is `IgnoreHandler` it will just return.

The `message` is a function that should return an `AbstractString` to use. For efficiency, it is not invoked if ignoring
the condition.
"""
function handle_abnormal(message::Function, handler::AbnormalHandler)::Nothing
    if handler == ErrorHandler
        error(message())
    elseif handler == WarnHandler
        @warn message()
    else
        @assert handler == IgnoreHandler
    end
    return nothing
end

"""
    Maybe{T} = Union{T, Nothing}

The type to use when maybe there is a value, maybe there isn't. This is exactly as if writing the explicit `Union`
with `Nothing` but is shorter and more readable. This is extremely common.
"""
Maybe = Union{T, Nothing} where {T}

"""
    Unsure{T} = Union{T, Missing}

The type to use when maybe there always is a value, but sometimes we are not sure what it is. This is exactly as if
writing the explicit `Union` with `Missing` but is shorter and more readable. This is only used in code dealing with
statistics to represent missing (that is, unknown) data. It is only provided here for completeness.
"""
Unsure = Union{T, Missing} where {T}

"""
    function dedent(string::AbstractString; indent::AbstractString = "")::String

Given a possibly multi-line string with a common indentation in each line, strip this indentation from all lines, and
replace it with `indent`. Will also strip any initial and/or final line breaks.
"""
function dedent(string::AbstractString; indent::AbstractString = "")::String
    lines = split(string, "\n")
    while !isempty(lines) && isempty(lines[1])
        @views lines = lines[2:end]  # untested
    end
    while !isempty(lines) && isempty(lines[end])
        @views lines = lines[1:(end - 1)]
    end

    first_non_space = nothing
    for line in lines
        line_non_space = findfirst(character -> character != ' ', line)
        if first_non_space == nothing || (line_non_space != nothing && line_non_space < first_non_space)
            first_non_space = line_non_space
        end
    end

    if first_non_space == nothing
        return indent * string  # untested NOJET
    else
        return join([indent * line[first_non_space:end] for line in lines], "\n")  # NOJET
    end
end

end  # module
