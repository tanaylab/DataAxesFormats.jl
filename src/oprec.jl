"""
You can safely skip this module if you are only using `Daf`. It is an internal module which is only of interest for
maintainers. Some of the types here are also used when implementing additional query element-wise or reduction
operations. In particular, this isn't reexported by default when `using Daf`.

Ideally `Daf` should have used some established parser generator module for parsing queries, making this unnecessary.
However, As of writing this code, Julia doesn't seem to have such a parser generator solution. Therefore, this module
provides a simple operator precedence based parser generator. The upside is that we can tailor this to our needs (in
particular, provide tailored error messages when parsing fails).

Operator-precedence based parsers are simple to define and implement. A set of `Regex` patterns are used for converting
the input string into [`Token`](@ref)s. Some tokens are [`Operator`](@ref)s, which have [`Associativity`](@ref) and
precedence. We therefore build an [`Expression`](@ref) tree from the tokens, and then "parse" it into something more
convenient to work with.

This is pretty intuitive when thinking about something like arithmetic expressions `1 + 2 * 3`. However, it turns out
that this approach allows for parsing a wide range of languages, including languages which one wouldn't immediately
think of as "expressions", all the way up to full programming languages.

That said, the implementation here is tailored for parsing `Daf` queries, and is not general enough to be a package on
its own.

Specifically, we assume that `_[0-9A-F][0-9A-F]` can't appear inside an operator, which is mostly reasonable; that `\\x`
is used to escape "special" characters to allow them in "normal" tokens, which isn't typically true in most languages;
we don't support postfix operators (e.g., `;` in C), we assume all operators are infix (e.g., `*` in arithmetic), but
some can also be prefix (e.g., `-` in arithmetic); there's no support for parenthesis; and we assume the parsed
expressions are "small" (can be squashed into a single line) for the purpose of creating friendly error messages.

We only reexport [`escape_query`](@ref), [`unescape_query`](@ref) and [`is_safe_query_char`](@ref) from the top-level
`Daf` module itself, as these are all you might be interested in from outside the `Daf` package.
"""
module Oprec

export Associativity
export build_encoded_expression
export check_operation
export Context
export decode_expression
export encode_expression
export error_in_context
export escape_query
export Expression
export is_safe_query_char
export LeftAssociative
export Operation
export Operator
export parse_in_context
export parse_list_in_context
export parse_operand_in_context
export parse_operation_in_context
export parse_string_in_context
export parse_with_list_in_context
export RightAssociative
export Syntax
export Token
export unescape_query

using URIs

"""
    escape_query(token::AbstractString)::String

Given some raw `token` (name of an axis, axis entry or property, or a parameter value), which may contain special
characters, return an escaped version to be used in a query string.

We need to consider the following kinds of characters:

  - **Safe** ([`is_safe_query_char`](@ref)) characters include `a` - `z`, `A` - `Z`, `0` - `9`, `_`, `+`, `-`, and `.`,
    as well as any non-ASCII (that is, Unicode) characters. Any sequence of these characters will be considered a single
    token, used to write names (of axes, axis entries, properties, operations, parameters), and also values (for
    parameters). These cover all the common cases (including signed integer and floating point values).

  - All other ASCII characters are (at least potentially) **special**, that is, may be used to describe the query
    structure. Currently only a subset of these are actually used: `#`, `\\`, `@`, `:`, `<`, `=`, `,`, `;`, `!`, `&`,
    `|`, `^` and `%`, and, of course, white space (spaces, tabs and line breaks) which can be used for readability.
    Additional characters may be used in future version, if we choose to enhance the query language.
  - Prefixing *any* character with a `\\` allows using it inside a token. This is useful if some name or value contains
    a special character. For example, if you have a cell whose name is `ACTG:Plate1`, and you want to access the name of
    the batch of this specific cell, you will have to write `cell = ACTG\\:Plate1 : batch`.

!!! note

    The `\\` character is also used by Julia inside `"..."` string literals, to escape writing non-printable characters.
    For example, `"\\n"` is a single-character string containing a line break, and therefore `"\\\\"` is used to write a
    single `\\`. Thus the above example would have to be written as `"cell = ACTG\\\\:Plate1 : batch"`. This isn't nice.

    Luckily, Julia also has `raw"..."` string literals that work similarly to Python's `r"..."` strings (in Julia,
    `r"..."` is a regular expression, not a string). Inside raw string literals, a `\\` is a `\\` (unless it precedes a
    `"`). Therefore the above example could also be written as `raw"cell = ACTG\\:Plate1 : batch`, which is more
    readable.

Back to `escape_query` - it will prefix any (potentially) special character with a `\\`. It is useful if you want to
inject a data into a query. Often this happens when using `\$(...)` to embed values into a query string, e.g., the query
`\$(axis) @ \$(property) > \$(value)` is unsafe, as any of the embedded variables may contain unsafe characters. You
should instead write something like `\$(escape_query(axis)) @ \$(escape_query(property)) > \$(escape_query(value))`.
"""
function escape_query(token::AbstractString)::String
    return replace(token, (character -> !is_safe_query_char(character)) => s"\\\0")
end

"""
    unescape_query(escaped_token::AbstractString)::String

Undo [`escape_query`](@ref), that is, given a query token with a `\\` characters escaping special characters, drop the
`\\` to get back the original string value.
"""
function unescape_query(escaped_token::AbstractString)::String
    return replace(escaped_token, r"\\(.)" => s"\1")
end

"""
    is_safe_query_char(character::Char)::Bool

Return whether a character is safe to use inside a query token (name of an axis, axis entry or property, or a parameter
value).

The safe characters are `a` - `z`, `A` - `Z`, `0` - `9`, `_`, `+`, `-`, and `.`, as well as any non-ASCII (that is,
Unicode) characters.
"""
function is_safe_query_char(character::Char)::Bool
    return character == '_' ||
           character == '.' ||
           character == '+' ||
           character == '-' ||
           isletter(character) ||
           isdigit(character) ||
           !isascii(character)
end

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
    struct Operator{E}
        id::E
        is_prefix::Bool
        associativity::Associativity
        precedence::Int
    end

Describe an operator token.

We attach an arbitrary `id` to the operators, which is typically used to carry some `@enum` to identify it for parsing
the expression tree.

The implementation is restricted to operators which are always infix (e.g., `+` in arithmetic expressions). Some
operators can also be `is_prefix` (e.g., `-` in arithmetic expressions).

Higher `precedence` operators (e.g., `*` in arithmetic expressions) will bind more strongly than lower precedence
operators (e.g., `+` in arithmetic expressions). That is, `a + b * c` will be parsed as `a + (b * c)`.
"""
struct Operator{E}
    id::E
    is_prefix::Bool
    associativity::Associativity
    precedence::Int
end

"""
    struct Token{E}
        first_index::Int
        last_index::Int
        string::String
        operator::Union{Operator{E}, Nothing}
    end

A parsed token of the expression (leaf node in an [`Expression`](@ref) tree).

This contains the location (`first_index` .. `last_index`) in the (encoded) expression string to enable generating
friendly error messages. There are no line numbers in locations because in `Daf` we squash our queries to a single-line,
under the assumption they are "relatively simple". This allows us to simplify the code.

We also hold the (decoded!) `string` of the token. If the token is an [`Operator`](@ref), we also provide its
description.
"""
struct Token{E}
    first_index::Int
    last_index::Int
    string::String
    operator::Union{Operator{E}, Nothing}
end

"""
    struct Operation{E}
        left::Union{Operation{E}, Token{E}, Nothing}
        token::Token{E}
        right::Union{Operation{E}, Token{E}}
    end

An operation (node in an [`Expression`](@ref) tree).

The `token` describes the [`Operator`](@ref). There's always a `right` sub-tree, but for prefix operators, the `left`
sub-tree is `nothing`.
"""
struct Operation{E}
    left::Union{Operation{E}, Token{E}, Nothing}
    token::Token{E}
    right::Union{Operation{E}, Token{E}}
end

"""
    Expression{E} = Union{Operation{E}, Token{E}}

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
    struct Syntax{E}
        space_regex::Regex
        operand_regex::Regex
        operator_regex::Regex
        operators::Dict{String, Operator{E}}
    end

Describe the syntax to parse using the patterns for tokens.

When tokenizing, we try matching the `space_regex` first. Anything that matches is considered to be white space and
discarded. We then try to match the `operand_regex`. Anything that matches is considered to be an operand
[`Token`](@ref). Otherwise, we try to match the `operator_regex`, and look up the result in the `operators` dictionary
to obtain an [`Operator`](@ref) [`Token`](@ref). Anything that doesn't match (or that doesn't exist in the `operators`)
is reported as an invalid character.
"""
struct Syntax{E}
    space_regex::Regex
    operand_regex::Regex
    operator_regex::Regex
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
                    unescape_query(decode_expression(operand_string)),
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
                        unescape_query(decode_expression(operator_string)),
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
        marker = repeat("▲", length(unescape_query(decode_expression(token.string))))
    else
        token = tokens[end]
        indent = repeat(" ", decode_index(encoded_string, token.last_index))
        marker = "▲"
    end
    return error(message * "\n" * "in: $(decode_expression(encoded_string))\n" * "at: $(indent)$(marker)")
end

"""
    build_encoded_expression(
        encoded_string::AbstractString,
        syntax::Syntax{E},
    )::Union{Expression{E}, Nothing} where {E}

Build an [`Expression`](@ref) tree from an `encoded_string` (that went through [`encode_expression`](@ref)).
"""
function build_encoded_expression(
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
    error_in_context(context::Context{E}, message::AbstractString)::Nothing where {E}

Report a parsing error in some [`Context`](@ref).

This provides location markers for the nested [`Context`](@ref) that led us to the point where the error occurred. It
only works for small (one-line) inputs, where there's little or no recursion in the parsing. Therefore, it is a good fit
for `Daf` queries, but not for a more general parsed languages.
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
    parse_in_context(
        parse::Union{Function, Type},
        context::Context{E},
        expression::Expression{E};
        name::AbstractString,
        operators::Vector{E} = E[],
    )::Any where {E}

Parse a node of an [`Expression`](@ref), using it as the [`Context`](@ref) when parsing any sub-expression. That is,
push the top-level `expression` into the context, invoke the `parse()` function, and pop the `expression` from the
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
        operator_marker = depth == 1 ? repeat("▲", operator_length) : operation.token.string
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
    parse_operand_in_context(
        parse::Union{Function, Type},
        context::Context{E},
        expression::Expression{E};
        name::AbstractString,
    )::Any where {E}

Parse an operand in an [`Expression`](@ref) in some [`Context`](@ref).

If the `expression` is not an operand [`Token`], report an [`error_in_context`](@ref), using the `name`.

Otherwise, give the `parse` function the operand [`Token`](@ref), and returns whatever the result it.
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
    parse_string_in_context(
        context::Context{E},
        expression::Expression{E};
        name::AbstractString,
    )::AbstractString where {E}

If the `expression` is not an operand [`Token`], report an [`error_in_context`](@ref), using the `name`.

Otherwise, return the string of the operand [`Token`](@ref).
"""
function parse_string_in_context(
    context::Context{E},
    expression::Expression{E};
    name::AbstractString,
)::AbstractString where {E}
    return parse_operand_in_context(token -> token.string, context, expression; name = name)
end

"""
    check_operation(
        expression::Expression{E},
        operators::Vector{E}
    )::Union{Token{E}, Nothing} where {E}

Check whether an `expression` is an [`Operation`](@ref) using one of the specified `operators`. If so, return the
operator's [`Token`](@ref); otherwise, return `nothing`.
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
    parse_operation_in_context(
        parse::Union{Function, Type},
        context::Context{E},
        expression::Expression{E};
        expression_name::AbstractString,
        operator_name::AbstractString,
        operators::Vector{E},
    )::Any where {E}

Parse an operation in an [`Expression`](@ref).

If the `expression` is not an [`Operation`](@ref) using one of the listed `operators`, report an
[`error_in_context`](@ref) using the `expression_name` and the `operator_name`.

Otherwise, give the `parse` function the left sub-[`Expression`](@ref) (or `nothing` for a prefix operator), the
[`Token`](@ref) of the operator, and the right sub-[`Expression`](@ref).
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
                "unexpected operator: $(unescape_query(decode_expression(expression.token.string)))\nexpected operator: $(operator_name)",
            )
        end

        return parse(expression.left, operator, expression.right)
    end
end

"""
    parse_list_in_context(
        [parse_element::Union{Function, Type},]
        context::Context{E},
        expression::Expression{E};
        list_name::AbstractString,
        element_type::Type{T},
        first_operator::Union{Token{E}, Nothing} = nothing,
        operators::Vector{E},
    )::Vector{T} where {T, E}

This converts an expression of the form `element ( operator element )*` into a vector of `element`, assuming the
`operators` have right [`Associativity`](@ref). For example, `property_name : property_name : ...` for a chained
property lookup.

We repeatedly invoke the `parse_element` function (or the `element_type` constructor, if `parse_element` is not given),
with the `context` (using the `list_name`), the operator immediately to the left of each sub-`expression` (using the
`first_operator` for the 1st one), and the sub-expression. We collect the results into a `Vector` of the `element_type`.

This always matches. If the `expression` isn't an [`Operation`](@ref) using one of the `operators`, then this simply
returns a single-element vector. That said, naturally parsing the field may fail.
"""
function parse_list_in_context(
    parse_element::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    list_name::AbstractString,
    element_type::Type{T},
    first_operator::Union{Token{E}, Nothing} = nothing,
    operators::Vector{E},
)::Vector{T} where {T, E}
    return parse_in_context(context, expression; name = list_name, operators = operators) do
        elements = T[]
        operator = first_operator

        while check_operation(expression, operators) != nothing && expression.left != nothing
            push!(elements, parse_element(context, operator, expression.left))
            operator = expression.token
            expression = expression.right
        end

        push!(elements, parse_element(context, operator, expression))
        return elements
    end
end

function parse_list_in_context(
    context::Context{E},
    expression::Expression{E};
    list_name::AbstractString,
    element_type::Type{T},
    first_operator::Union{Token{E}, Nothing} = nothing,
    operators::Vector{E},
)::Vector{T} where {T, E}
    return parse_list_in_context(
        element_type,
        context,
        expression;
        list_name = list_name,
        element_type = element_type,
        first_operator = first_operator,
        operators = operators,
    )
end

"""
    parse_with_list_in_context(
        parse::Union{Function, Type},
        context::Context{E},
        expression::Expression{E};
        expression_name::AbstractString,
        separator_name::AbstractString,
        separator_operators::Vector{E},
        list_name::AbstractString,
        parse_element::Union{Function, Type, Nothing} = nothing,
        element_type::Type{L},
        first_operator::Bool = false,
        operators::Vector{E},
    )::Any where {L, E}

This converts an expression of the form `something separator element ( operator element )*` into a combined object,
which typically has two members, a field for `something` and a vector for one or more `element`. For example, `operation ; parameter_assignment , parameter_assignment , ...` for invoking an element-wise or reduction operation.

If the `expression` isn't an [`Operation`](@ref) using one of the `separator_operators`, then we assume there is no list
of `elements`. Otherwise this invokes [`parse_list_in_context`](@ref) for collecting the elements. If `first_operator`,
we give it the separator operator as the first operator.
"""
function parse_with_list_in_context(
    parse::Union{Function, Type},
    context::Context{E},
    expression::Expression{E};
    expression_name::AbstractString,
    separator_name::AbstractString,
    separator_operators::Vector{E},
    list_name::AbstractString,
    parse_element::Union{Function, Type, Nothing} = nothing,
    element_type::Type{L},
    first_operator::Bool = false,
    operators::Vector{E},
)::Any where {L, E}
    if parse_element == nothing
        parse_element = element_type
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
                    parse_element,
                    context,
                    elements;
                    list_name = list_name,
                    element_type = element_type,
                    first_operator = first_operator ? separator : nothing,
                    operators = operators,
                ),
            )
        end
    end
end

end  # module
