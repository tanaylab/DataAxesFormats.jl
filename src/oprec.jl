"""
As of writing this code, Julia doesn't seem to have a robust parser generator solution. Therefore, this module provides
a simple operator precedence based parser library to use for parsing `Daf` queries.

Operator-precedence based parsers are very simple. You define a set of patterns for tokens, which we use to tokenize the
input. Some tokens are operators, with precedence; we construct and return an expression tree based on these.

It turns out that this approach allows describing a wide range of languages, including languages which one wouldn't
intuitively think of as "expressions", all the way up to a full programming language. That said, the implementation here
is tailored to parsing `Daf` queries, and is not general enough to be a package on its own.

Specifically, we assume that `_[0-9A-F][0-9A-F]` can't appear inside an operator, which is mostly reasonable; that `\\x`
is used to escape "special" characters to allow them in "normal" tokens, which isn't typically true in most languages;
and we don't support postfix operators (e.g., `;` in C), we assume all operators are infix (e.g., `*` in arithmetic),
but some can also be prefix (e.g., `-` in arithmetic); there's no support for parenthesis; and we assume the parsed
expressions are "small" (can be squashed into a single line) for the purpose of creating friendly error messages.

For now, this isn't even reexported by default when `using Daf`. It is still documented to provide some insight on how
`Daf` query parsing works.
"""
module Oprec

export Associativity
export Context
export check_operation
export decode_expression
export encode_expression
export error_in_context
export Expression
export LeftAssociative
export Operation
export Operator
export parse_encoded_expression
export parse_in_context
export parse_list_in_context
export parse_operand_in_context
export parse_operation_in_context
export parse_with_list_in_context
export parse_string_in_context
export RightAssociative
export Syntax
export Token

using URIs

"""
Given an expression string to parse, encode any character escaped by a `\\` such that it will be considered a normal
[`Token`](@ref) character. Every escaped character is replaced by `_XX` using URI encoding, but replacing the `%` with a
`_` so we can deal with unescaped `%` as an operator, so we also need to encode `_` as `_5F`, so we need to encode `\\_`
as `_5C_5F`. Isn't encoding *fun*?
"""
function encode_expression(expr_string::AbstractString)::String
    return replace(expr_string, "\\_" => "_5C_5F", "_" => "_5F", r"\\." => encode_expression_char)
end

function encode_expression_char(escaped_char::AbstractString)::String
    @assert escaped_char[1] == '\\'
    return replace(escapeuri(escaped_char[2:end], character -> false), r"%" => "_")
end

"""
Given the results of [`encode_expression`](@ref), decode it back to its original form.
"""
function decode_expression(encoded_string::AbstractString)::String
    return unescapeuri(replace(encoded_string, "_5C_5F" => "\\_", "_5F" => "_", "%" => "%25", r"_(..)" => s"\\%\1"))
end

# Given an index in the decoded string, return the matching index in the original string.
function decode_index(encoded_string::AbstractString, index::Int)::Int
    return length(decode_expression(encoded_string[1:index]))
end

"""
For infix operators, how to group a sequence of operators.

`LeftAssociative` - groups `1 + 2 + 3` as `(1 + 2) + 3`. Parsing these operators is marginally faster.

`RightAssociative` - groups `1 + 2 + 3` as `1 + (2 + 3)`. The current implementation requires this for parsing lists.
"""
@enum Associativity LeftAssociative RightAssociative

"""
Describe an operator token.

The implementation is restricted to operators which are either infix (e.g., `+` in arithmetic expressions), prefix
(e.g., `!` in boolean expressions), or both (e.g., `-` in arithmetic expressions).

Higher precedence operators (e.g., `*` in arithmetic expressions) will bind more strongly than lower precedence
operators (e.g., `+` in arithmetic expressions). That is, `a + b * c` will be parsed as `a + (b * c)`.

We allow attaching an arbitrary `id` to the operators, which is typically used to carry some `@enum` to identify it.
"""
struct Operator{E}
    """
    Identify the operator for post-processing. This typically carries some `@enum` value.
    """
    id::E

    """
    Whether the operator can be used as a prefix, e.g., `-` in arithmetic expressions.
    """
    is_prefix::Bool

    """
    If the operator can be used as an infix, e.g., `*` in arithmetic expressions, how to group a sequence of operators
    (of the same precedence).
    """
    associativity::Associativity

    """
    How strongly does the operator bind to its operand, e.g., in arithmetic expressions, `*` binds more strongly than
    `+`.
    """
    precedence::Int
end

"""
A parsed token of the expression.

This contains the location in the (encoded) expression string to facilitate generating friendly error messages. Ideally
such locations should include line numbers but in `Daf` we squash our queries to a single-line, under the assumption
they are "relatively simple". This allows us to simplify the code.

The indices and the string all refer to the `encode_expression`.
"""
struct Token{E}
    """
    Index of first [`encode_expression`](@ref) character of token.
    """
    first_index::Int

    """
    Index of last [`encode_expression`](@ref) character of token.
    """
    last_index::Int

    """
    The original (**not** [`encode_expression`](@ref)) string of the token.
    """
    string::String

    """
    If the token is an [`Operator`](@ref), its description.
    """
    operator::Union{Operator{E}, Nothing}
end

"""
An operation in an expression tree.
"""
struct Operation{E}
    """
    The left operand of the [`Operator`](@ref), if it is an infix operator. This can be a sub-[`Expression`](@ref) or an
    operand [`Token`](@ref).
    """
    left::Union{Operation{E}, Token{E}, Nothing}

    """
    The [`Token`](@ref) of the operator.
    """
    token::Token{E}

    """
    The right operand of the [`Operator`](@ref). This can be a sub-[`Expression`](@ref) or an operand [`Token`](@ref).
    """
    right::Union{Operation{E}, Token{E}}
end

"""
An expression tree - either an [`Operation`](@ref) or an operand ([`Token`](@ref)).
"""
const Expression{E} = Union{Operation{E}, Token{E}}

function as_string(::Nothing)::String
    return ""
end

function as_string(token::Token{E})::String where {E}
    return token.string
end

function as_string(operation::Operation{E})::String where {E}
    return "(" * as_string(operation.left) * " " * as_string(operation.token) * " " * as_string(operation.right) * ")"
end

"""
Describe the syntax to parse using the patterns for tokens.

When tokenizing, we try matching the `space_regex` first, then the `operand_regex`, and only then the `operator_regex`.
"""
struct Syntax{E}
    """
    A regular expression that matches any white space used to (optionally) separate between [`Token`](@ref). It must
    only match at the start of the string and not match an empty string, e.g., `^\\s+`. Anything that matches is
    silently discarded.
    """
    space_regex::Regex

    """
    A regular expression that matches an operand [`Token`](@ref). It must only match at the start of the string and not
    match an empty string, e.g., `^\\d+`.
    """
    operand_regex::Regex

    """
    A regular expression that matches any of the `operators`. It must only match at the start of the string and not
    match an empty string. If what it matches doesn't appear in the `operators` dictionary, we ignore the match.
    """
    operator_regex::Regex

    """
    The description of the allowed operators.
    """
    operators::Dict{String, Operator{E}}
end

function tokenize(encoded_string::AbstractString, syntax::Syntax{E})::Vector{Token{E}} where {E}
    rest_of_string = encoded_string
    first_index = 1
    tokens = Vector{Token{E}}()

    while !isempty(rest_of_string)
        spaces = match(syntax.space_regex, rest_of_string)
        if spaces != nothing
            @assert spaces.offset == 1
            spaces_string = spaces.match
            @assert !isempty(spaces_string)

            first_index += length(spaces_string)
            rest_of_string = rest_of_string[(length(spaces_string) + 1):end]
            continue
        end

        operand = match(syntax.operand_regex, rest_of_string)
        if operand != nothing
            @assert operand.offset == 1
            operand_string = operand.match
            @assert !isempty(operand_string)

            push!(
                tokens,
                Token{E}(
                    first_index,
                    first_index + length(operand_string) - 1,
                    decode_expression(operand_string),
                    nothing,
                ),
            )

            first_index += length(operand_string)
            rest_of_string = rest_of_string[(length(operand_string) + 1):end]
            continue
        end

        operator = match(syntax.operator_regex, rest_of_string)
        if operator != nothing
            @assert operator.offset == 1
            operator_string = operator.match
            @assert !isempty(operator_string)

            operator = get(syntax.operators, operator_string, nothing)
            if operator != nothing
                push!(
                    tokens,
                    Token(
                        first_index,
                        first_index + length(operator_string) - 1,
                        decode_expression(operator_string),
                        operator,
                    ),
                )

                first_index += length(operator_string)
                rest_of_string = rest_of_string[(length(operator_string) + 1):end]
                continue
            end
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

# Whether a token is an infix operator, a prefix operator, or an operand.
@enum Fixness LeftInfix RightInfix Prefix Operand

# A partial parsed expression.
struct Part{E}
    fixness::Fixness
    precedence::Int
    tree::Expression{E}
end

#function as_string(parts::Vector{Part{E}})::String where {E}
#    return join(["$(part.fixness) # $(part.precedence) : $(as_string(part.tree))" for part in parts], " , ")
#end

# Decide on the [`Fixness`](@ref) of each token.
function fix(encoded_string::AbstractString, tokens::Vector{Token{E}})::Vector{Part{E}} where {E}
    fixed = Vector{Part}(undef, length(tokens))
    is_after_op = true
    for (index, token) in enumerate(tokens)
        if is_after_op
            if token.operator == nothing
                fixed[index] = Part(Operand, 0, token)
                is_after_op = false
            elseif token.operator.is_prefix
                fixed[index] = Part(Prefix, token.operator.precedence, token)
            else
                error_at_token(encoded_string, tokens, index, "expected: operand")
            end
        else # !is_after_op
            if token.operator == nothing
                error_at_token(encoded_string, tokens, index, "expected: operator")
            else
                fixed[index] = Part(
                    token.operator.associativity == LeftAssociative ? LeftInfix : RightInfix,
                    token.operator.precedence,
                    token,
                )
                is_after_op = true
            end
        end
    end
    if fixed[end].fixness != Operand
        error_at_token(encoded_string, tokens, length(fixed) + 1, "expected: operand")
    end
    return fixed
end

# This is definitely not the most efficient way to do this, but it is simple and our queries are small.
function connect(parts::Vector{Part{E}})::Expression{E} where {E}
    index = 1
    while length(parts) > 1
        part = parts[index]

        if part.fixness == Prefix &&
           parts[index + 1].fixness == Operand &&
           part.precedence >= right_precedence(parts, index + 2, part.fixness == RightInfix)
            part = Part(Operand, part.precedence, Operation(nothing, part.tree, parts[index + 1].tree))
            deleteat!(parts, index + 1)
            parts[index] = part

        elseif (part.fixness == LeftInfix || part.fixness == RightInfix) &&
               parts[index - 1].fixness == Operand &&
               parts[index + 1].fixness == Operand &&
               part.precedence >= right_precedence(parts, index + 2, part.fixness == RightInfix) &&
               part.precedence >= left_precedence(parts, index - 2, part.fixness == LeftInfix)
            part = Part(Operand, part.precedence, Operation(parts[index - 1].tree, part.tree, parts[index + 1].tree))
            deleteat!(parts, (index, index + 1))
            index -= 1
            parts[index] = part

        else
            index = index + 1
            if index > length(parts)
                index = 1
            end
        end
    end

    return parts[1].tree
end

function right_precedence(parts::Vector{Part{E}}, index::Int, is_right_infix::Bool)::Int where {E}
    if index > length(parts)
        return -1
    end

    part = parts[index]
    @assert part.fixness != Operand

    if is_right_infix
        return part.precedence + 1
    else
        return part.precedence
    end
end

function left_precedence(parts::Vector{Part{E}}, index::Int, is_left_infix::Bool)::Int where {E}
    if index < 1
        return -1
    end

    part = parts[index]
    @assert part.fixness != Operand

    if is_left_infix
        return part.precedence + 1
    else
        return part.precedence
    end
end

function error_at_token(
    encoded_string::AbstractString,
    tokens::Vector{Token{E}},
    at_index::Int,
    message::AbstractString,
)::Nothing where {E}
    if at_index <= length(tokens)
        token = tokens[at_index]
        indent = repeat(" ", decode_index(encoded_string, token.first_index - 1))
        marker = repeat("▲", length(decode_expression(token.string)))
    else
        token = tokens[end]
        indent = repeat(" ", decode_index(encoded_string, token.last_index))
        marker = "▲"
    end
    return error(message * "\n" * "in: $(decode_expression(encoded_string))\n" * "at: $(indent)$(marker)")
end

"""
Parse an expression string into a sequence of [`Token`](@ref)s and organize them into an [`Expression`](@ref) tree.

This assumes the string went through [`encode_expression`](@ref).
"""
function parse_encoded_expression(
    encoded_string::AbstractString,
    syntax::Syntax{E},
)::Union{Expression{E}, Nothing} where {E}
    tokens = tokenize(encoded_string, syntax)
    if isempty(tokens)
        return nothing
    else
        return connect(fix(encoded_string, tokens))
    end
end

struct ContextEntry{E}
    expression::Expression{E}
    name::AbstractString
    operators::Vector{E}
end

"""
    Context(encoded_string::AbstractString)

A context for parsing a sub `Expression`.

When processing an `Expression` tree, we go top-down, collecting context entries along the way. This way, when we
discover some invalid construct, we can show this in an error message, helping the user understand how the expression
was parsed, and why it is invalid.
"""
struct Context{E}
    encoded_string::AbstractString
    entries_stack::Vector{ContextEntry{E}}
end

function Context(encoded_string::AbstractString, operators::Type)::Context
    return Context(encoded_string, ContextEntry{operators}[])
end

"""
Report a parsing error in some [`Context`](@ref).

This provides location markers for the nested [`Context`](@ref) that led us to the point where the error occurred. It
only works for small (one-line) inputs, where there's little or no recursion in the parsing. Therefore, it is a good fit
for `Daf` queries, but not for more general parsed languages.
"""
function error_in_context(context::Context{E}, message::AbstractString)::Nothing where {E}
    located_names = [
        (
            expression_locator(context.encoded_string, depth, context_entry.expression, context_entry.operators),
            context_entry.name,
        ) #
        for (depth, context_entry) in enumerate(reverse(context.entries_stack))
    ]

    max_locator_length = max([length(locator) for (locator, name) in located_names]...)

    message *= "\nin: $(decode_expression(context.encoded_string))"
    for (locator, name) in located_names
        padding = repeat(" ", max_locator_length - length(locator))
        message *= "\nin: $(locator)$(padding) ($(name))"
    end

    return error(message)
end

"""
Parse a node of an [`Expression`](@ref), using it as the [`Context`](@ref) when parsing any sub-expression. That is,
push the top-level `expression` into the context, invoke the `parse` function, and pop the `expression` from the
context. Will return the results of the `parse` function.
"""
function parse_in_context(
    parse::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    name::AbstractString,
    operators::Vector{E} = E[],
)::Any where {E}
    same_expression = !isempty(context.entries_stack) && context.entries_stack[end].expression === expression
    if same_expression
        old_entry = context.entries_stack[end]
        context.entries_stack[end] = ContextEntry(expression, name, operators)
    else
        push!(context.entries_stack, ContextEntry(expression, name, operators))
    end
    try
        return parse()
    finally
        if same_expression
            context.entries_stack[end] = old_entry
        else
            pop!(context.entries_stack)
        end
    end
end

function expression_locator(
    encoded_string::AbstractString,
    depth::Int,
    token::Token{E},
    operators::Vector{E},
)::String where {E}
    indent = repeat(" ", decode_index(encoded_string, token.first_index) - 1)
    marker = repeat(depth == 1 ? "▲" : "·", length(token.string))
    return indent * marker
end

function expression_locator(
    encoded_string::AbstractString,
    depth::Int,
    operation::Operation{E},
    operators::Vector{E},
)::String where {E}
    locator = ""
    last_index = 0

    while true
        if operation.left != nothing
            left_locator, last_index = operand_locator(encoded_string, last_index, operation.left)
            locator *= left_locator
        end

        operator_first_index = decode_index(encoded_string, operation.token.first_index)
        operator_length = length(operation.token.string)
        operator_last_index = operator_first_index + operator_length - 1
        operator_marker = repeat(depth == 1 ? "▲" : "•", operator_length)
        operator_indent = repeat(" ", operator_first_index - last_index - 1)

        locator = locator * operator_indent * operator_marker
        last_index = operator_last_index

        if !(operation.token.operator.id in operators) ||
           operation.right isa Token ||
           !(operation.right.token.operator.id in operators)
            break
        end

        operation = operation.right
    end

    right_locator, right_last_index = operand_locator(encoded_string, last_index, operation.right)
    return locator * right_locator
end

function operand_locator(
    encoded_string::AbstractString,
    last_index::Int,
    operand::Expression{E},
)::Tuple{String, Int} where {E}
    operand_first_index = expression_first_index(encoded_string, operand)
    operand_last_index = expression_last_index(encoded_string, operand)
    operand_length = operand_last_index - operand_first_index + 1
    operand_marker = repeat("·", operand_length)
    operand_indent = repeat(" ", operand_first_index - last_index - 1)
    operand_locator = operand_indent * operand_marker
    return operand_locator, operand_last_index
end

function expression_first_index(encoded_string::AbstractString, token::Token{E})::Int where {E}
    return decode_index(encoded_string, token.first_index)
end

function expression_first_index(encoded_string::AbstractString, operation::Operation{E})::Int where {E}
    if operation.left == nothing
        return expression_first_index(encoded_string, operation.token)
    else
        return expression_first_index(encoded_string, operation.left)
    end
end

function expression_last_index(encoded_string::AbstractString, token::Token{E})::Int where {E}
    return decode_index(encoded_string, token.first_index) + length(token.string) - 1
end

function expression_last_index(encoded_string::AbstractString, operation::Operation{E})::Int where {E}
    return expression_last_index(encoded_string, operation.right)
end

"""
Parse an operand in an [`Expression`](@ref) in some [`Context`](@ref).

Passes the `parse` function the [`Token`](@ref) of the operand, and returns whatever the result it.

If the `expression` is not an operand (simple [`Token`]), report an [`error_in_context`](@ref).
"""
function parse_operand_in_context(
    parse::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    name::AbstractString,
)::Any where {E}
    return parse_in_context(context, expression; name = name) do
        if expression isa Token
            return parse(expression)
        else
            error_in_context(context, "unexpected operator: $(expression.token.string)")
        end
    end
end

"""
Parse a string operand in an [`Expression`](@ref) in some [`Context`](@ref).

If the `expression` is not an operand (simple [`Token`]), report an [`error_in_context`](@ref).
"""
function parse_string_in_context(
    context::Context{E},
    expression::Expression{E};
    name::AbstractString,
)::AbstractString where {E}
    return parse_operand_in_context(token -> token.string, context, expression; name = name)
end

"""
Check whether a (sub-)[`Expression`](@ref) is an [`Operation`](@ref) using one of the specified operators. If so, return
the operator's [`Token`](@ref); otherwise, return `nothing`.
"""
function check_operation(expression::Expression{E}, operators::Vector{E})::Union{Token{E}, Nothing} where {E}
    if !(expression isa Operation)
        return nothing
    end

    for operator in operators
        if expression.token.operator.id == operator
            return expression.token
        end
    end

    return nothing
end

"""
Parse an operation in an [`Expression`](@ref).

Passes the `parse` function the left sub-[`Expression`](@ref) (or `nothing` for a prefix operator), the [`Token`](@ref)
of the operator, and the right sub-[`Expression`](@ref).

If the `expression` is not an [`Operation`](@ref) using one of the listed `operators`, report an
[`error_in_context`](@ref).
"""
function parse_operation_in_context(
    parse::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    expression_name::AbstractString,
    operator_name::AbstractString,
    operators::Vector{E},
)::Any where {E}
    return parse_in_context(context, expression; name = expression_name) do
        if !(expression isa Operation)
            error_in_context(context, "expected operator: $(operator_name)")
        end

        operator = check_operation(expression, operators)
        if operator == nothing
            error_in_context(
                context,
                "unexpected operator: $(decode_expression(expression.token.string))\nexpected operator: $(operator_name)",
            )
        end

        return parse(expression.left, operator, expression.right)
    end
end

"""
Parse a variable-length list of some elements.

The implementation here requires the operators to have right [`Associativity`](@ref).
"""
function parse_list_in_context(
    parse_field::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    name::AbstractString,
    element_type::Type{T},
    first_operator::Union{Token{E}, Nothing} = nothing,
    operators::Vector{E},
)::Vector{T} where {T, E}
    return parse_in_context(context, expression; name = name, operators = operators) do
        elements = T[]
        operator = first_operator

        while check_operation(expression, operators) != nothing && expression.left != nothing
            push!(elements, parse_field(context, operator, expression.left))
            operator = expression.token
            expression = expression.right
        end

        push!(elements, parse_field(context, operator, expression))
        return elements
    end
end

function parse_list_in_context(
    context::Context{E},
    expression::Expression{E};
    name::AbstractString,
    element_type::Type{T},
    first_operator::Union{Token{E}, Nothing} = nothing,
    operators::Vector{E},
)::Vector{T} where {T, E}
    return parse_list_in_context(
        element_type,
        context,
        expression;
        name = name,
        element_type = element_type,
        first_operator = first_operator,
        operators = operators,
    )
end

"""
Parse an object with some field followed by optional sequence of elements.

This is used several times in a `Daf` query, e.g., `operation; parameter, ...` and `lookup % eltwise % ...`, so it gets
its own specialized function.
"""
function parse_with_list_in_context(
    parse::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    expression_name::AbstractString,
    separator_name::AbstractString,
    separator_operators::Vector{E},
    list_name::AbstractString,
    parse_field::Union{Function, Type, Nothing} = nothing,
    element_type::Type{L},
    first_operator::Bool = false,
    operators::Vector{E},
)::Any where {L, E}
    if parse_field == nothing
        parse_field = element_type
    end
    if check_operation(expression, separator_operators) == nothing
        return parse_in_context(context, expression; name = expression_name) do
            return parse(expression, element_type[])
        end
    else
        parse_operation_in_context(
            context,
            expression;
            expression_name = expression_name,
            operator_name = separator_name,
            operators = separator_operators,
        ) do field, separator, elements
            return parse(
                field,
                parse_list_in_context(
                    parse_field,
                    context,
                    elements;
                    name = list_name,
                    element_type = element_type,
                    first_operator = first_operator ? separator : nothing,
                    operators = operators,
                ),
            )
        end
    end
end

end  # module
