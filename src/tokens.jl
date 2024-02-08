"""
The only exported functions from this module are [`escape_value`](@ref) and [`unescape_value`](@ref) which are useful
when embedding values into query strings. The rest of the module is documented to give insight into how a query string
is broken into [`Token`](@ref)s.

Ideally `Daf` should have used some established parser generator module for parsing queries, making all this
unnecessary. However, As of writing this code, Julia doesn't seem to have such a parser generator solution. Therefore,
this module provides a simple [`tokenize`](@ref) function with rudimentary pattern matching which is all we need to
parse queries (whose structure is "trivial").
"""
module Tokens

export escape_value
export unescape_value

using URIs

"""
    escape_value(value::AbstractString)::String

Given some raw `value` (name of an axis, axis entry or property, or a parameter value), which may contain special
characters, return an escaped version to be used as a single value [`Token`](@ref).

We need to consider the following kinds of characters:

  - **Safe** ([`is_value_char`](@ref)) characters include `a` - `z`, `A` - `Z`, `0` - `9`, `_`, `+`, `-`, and `.`, as
    well as any non-ASCII (that is, Unicode) characters. Any sequence of these characters will be considered a single
    value [`Token`](@ref). These cover all the common cases (including signed integer and floating point values).

  - All other ASCII characters are (at least potentially) **special**, that is, may be used to describe an operation.
  - Prefixing *any* character with a `\\` allows using it inside a value [`Token`](@ref). This is useful if some name or
    value contains a special character. For example, if you have a cell whose name is `ACTG:Plate1`, and you want to
    access the name of the batch of this specific cell, you will have to write `/ cell = ACTG\\:Plate1 : batch`.

!!! note

    The `\\` character is also used by Julia inside `"..."` string literals, to escape writing non-printable characters.
    For example, `"\\n"` is a single-character string containing a line break, and therefore `"\\\\"` is used to write a
    single `\\`. Thus the above example would have to be written as `"cell = ACTG\\\\:Plate1 : batch"`. This isn't nice.

    Luckily, Julia also has `raw"..."` string literals that work similarly to Python's `r"..."` strings (in Julia,
    `r"..."` is a regular expression, not a string). Inside raw string literals, a `\\` is a `\\` (unless it precedes a
    `"`). Therefore the above example could also be written as `raw"/ cell = ACTG\\:Plate1 : batch`, which is more
    readable.

Back to `escape_value` - it will prefix any special character with a `\\`. It is useful if you want to programmatically
inject a value. Often this happens when using `\$(...)` to embed values into a query string, e.g., do not write a query
`/ \$(axis) @ \$(property)` as it is unsafe, as any of the embedded variables may contain unsafe characters. You should
instead write something like `/ \$(escape_value(axis)) @ \$(escape_value(property))`.
"""
function escape_value(value::AbstractString)::String
    return replace(value, (character -> !is_value_char(character)) => s"\\\0")  # NOJET
end

"""
    unescape_value(escaped::AbstractString)::String

Undo [`escape_value`](@ref), that is, given an `escaped` value with a `\\` characters escaping special characters, drop
the `\\` to get back the original string value.
"""
function unescape_value(escaped::AbstractString)::String
    return replace(escaped, r"\\(.)" => s"\1")
end

"""
    SPACE_REGEX = r"(?:[\\s\\n\\r]|#[^\\n\\r]*(?:[\\r\\n]|\$))+"sm

Optional white space can separate [`Token`](@ref). It is required when there are two consecutive value tokens, but is
typically optional around operators. White space includes spaces, tabs, line breaks, and a `# ...` comment suffix of a
line.
"""
SPACE_REGEX = r"(?:[\s\n\r]|#[^\n\r]*(?:[\r\n]|$))+"sm

"""
    VALUE_REGEX = r"^(?:[0-9a-zA-Z_.+-]|[^\\x00-\\xFF])+"

A sequence of [`is_value_char`](@ref) is considered to be a single value [`Token`](@ref). This set of characters was
chosen to allow expressing numbers, Booleans and simple names. Any other (ASCII, non-space) character may in principle
be used as an operator (possibly in a future version of the code). Therefore, use [`escape_value`](@ref) to protect any
value you embed into the expression.
"""
VALUE_REGEX = r"^(?:[0-9a-zA-Z_.+-]|[^\x00-\xFF])+"

"""
    is_value_char(character::Char)::Bool

Return whether a character is safe to use inside a value [`Token`](@ref) (name of an axis, axis entry or property, or a
parameter value).

The safe characters are `a` - `z`, `A` - `Z`, `0` - `9`, `_`, `+`, `-`, and `.`, as well as any non-ASCII (that is,
Unicode) characters.
"""
function is_value_char(character::Char)::Bool
    return character == '_' ||
           character == '.' ||
           character == '+' ||
           character == '-' ||
           !isascii(character) ||
           isletter(character) ||
           isdigit(character)
end

"""
    encode_expression(expr_string::AbstractString)::String

Given an expression string to parse, encode any non-ASCII (that is, Unicode) character, as well as any character escaped
by a `\\`, such that the result will only use [`is_value_char`](@ref) characters. Every encoded character is replaced by
`_XX` using URI encoding, but replacing the `%` with a `_` so we can deal with unescaped `%` as an operator, so we also
need to encode `_` as `_5F`, so we need to encode `\\_` as `_5C_5F`. Isn't encoding *fun*?
"""
function encode_expression(expr_string::AbstractString)::String
    return replace(expr_string, "\\_" => "_5C_5F", "_" => "_5F", r"\\." => encode_expression_char)
end

function encode_expression_char(escaped_char::AbstractString)::AbstractString
    return replace(escapeuri(escaped_char[2:end], character -> false), r"%" => "_")
end

"""
    decode_expression(encoded_string::AbstractString)::String

Given the results of [`encode_expression`](@ref), decode it back to its original form.
"""
function decode_expression(encoded_string::AbstractString)::String
    return unescapeuri(replace(encoded_string, "_5C_5F" => "\\_", "_5F" => "_", "%" => "%25", r"_(..)" => s"\\%\1"))
end

# Given an index of a character in the decoded string, return the matching index in the original string.
function decode_index(encoded_string::AbstractString, index::Int)::Int
    return length(decode_expression(encoded_string[1:index]))
end

"""
    struct Token
        is_operator::Bool
        value::AbstractString
        token_index::Int
        first_index::Int
        last_index::Int
        encoded_string::AbstractString
    end

A parsed token of an expression.

We distinguish between "value" tokens and "operator" tokens using `is_operator`. A value token holds the name of an
axis, axis entry or property, or a parameter value, while an operator token is used to identify a query operation to
perform. In both cases, the `value` contains the token string. This goes through both [`decode_expression`](@ref) and
[`unescape_value`](@ref) so it can be directly used as-is for value tokens.

We also keep the location (`first_index` .. `last_index`) and the (encoded) expression string, to enable generating
friendly error messages. There are no line numbers in locations because in `Daf` we squash our queries to a single-line,
under the assumption they are "relatively simple". This allows us to simplify the code.
"""
struct Token
    is_operator::Bool
    value::AbstractString
    token_index::Int
    first_index::Int
    last_index::Int
    encoded_string::AbstractString
end

"""
    tokenize(string::AbstractString, operators::Regex)::Vector{Token}

Given an expression string, convert it into a vector of [`Token`](@ref).

We first convert everything that matches the [`SPACE_REGEX`](@ref) into a single space. This squashed the expression
into a single line (discarding line breaks and comments), and the squashed expression is used for reporting errors. This
is reasonable for dealing with `daf` queries which are expected to be "relatively simple".

When tokenizing, we discard the spaces. Anything that matches the [`VALUE_REGEX`](@ref) is considered to be a value
[`Token`](@ref). Anything that matches the `operators` is considered to be an operator [`Token`](@ref). Anything else is
reported as an invalid character.

!!! note

    The `operators` regex should only match the start of the string (that is, must start with `^`). Also, when using
    `|`, you need to list the longer operators first (e.g., `^(?:++|+)` as opposed to `^(?:+|++)`).
"""
function tokenize(string::AbstractString, operators::Regex)::Vector{Token}
    encoded_string = encode_expression(string)
    encoded_string = replace(encoded_string, SPACE_REGEX => " ")

    rest_of_string = encoded_string
    first_index = 1
    token_index = 1
    tokens = Vector{Token}()

    while !isempty(rest_of_string)
        if rest_of_string[1] == ' '
            first_index += 1
            rest_of_string = rest_of_string[2:end]
            @assert isempty(rest_of_string) || rest_of_string[1] != ' '
            continue
        end

        value = match(VALUE_REGEX, rest_of_string)
        if value != nothing
            @assert value.offset == 1
            value_string = value.match
            @assert !isempty(value_string)

            push!(
                tokens,
                Token(
                    false,
                    unescape_value(decode_expression(value_string)),
                    token_index,
                    first_index,
                    first_index + sizeof(value_string) - 1,
                    encoded_string,
                ),
            )

            token_index += 1
            first_index += sizeof(value_string)
            rest_of_string = rest_of_string[(sizeof(value_string) + 1):end]
            continue
        end

        operator = match(operators, rest_of_string)
        if operator != nothing
            @assert operator.offset == 1
            operator_string = operator.match
            @assert !isempty(operator_string)

            push!(
                tokens,
                Token(
                    true,
                    unescape_value(decode_expression(operator_string)),
                    token_index,
                    first_index,
                    first_index + sizeof(operator_string) - 1,
                    encoded_string,
                ),
            )

            token_index += 1
            first_index += sizeof(operator_string)
            rest_of_string = rest_of_string[(sizeof(operator_string) + 1):end]
            continue
        end

        indent = repeat(" ", decode_index(encoded_string, first_index - 1))
        error(
            "unexpected character: \'$(escape_string(rest_of_string[1:1]))\'\n" *
            "in: $(decode_expression(encoded_string))\n" *
            "at: $(indent)▲",
        )
    end

    return tokens
end

function error_at_token(token::Token, message::AbstractString; at_end::Bool = false)::Union{}
    if at_end
        indent = repeat(" ", decode_index(token.encoded_string, token.last_index))
        marker = "▲"
    else
        indent = repeat(" ", decode_index(token.encoded_string, token.first_index - 1))
        marker = repeat("▲", length(token.value))
    end
    return error(message * "\n" * "in: $(decode_expression(token.encoded_string))\n" * "at: $(indent)$(marker)")
end

end  # module
