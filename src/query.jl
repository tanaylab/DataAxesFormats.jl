"""
`Daf` provides a query language that allows for convenient extraction of data from the `Storage`. This isn't a
"beautiful" query language, but it is terse, consistent, and reasonably flexible.

We use an operator precedence parser for queries. That is, one can think of a query as an expression, using specific
operators that combine into the overall query expression. For example, the query `cell @ batch` can be thought of as the
expression `@("cell", "batch")` which means "lookup the value of the property `batch` for each entry of the `cell`
axis".

We separately describe queries that produce matrix data ([`MatrixQuery`](@ref)), vector data ([`VectorQuery`](@ref)),
and scalar data ([`ScalarQuery`](@ref)).

When parsing a query, we will will properly encode escaped characters, treat any `#...<LineBreak>` characters sequence
as white space, and also condense all consecutive white space characters into a single space (which will convert the
query string to a single line). This allows using arbitrary comments, white space and line breaks in complex queries,
and also allows error messages to visually refer to the part of the query that triggered them, without having to deal
with thorny issues of visually indicating messages inside multi-line query strings. This comes at the cost that the
error messages refer to the one-line version of the query string, instead of the original.

We only reexport [`escape_query`](@ref), [`unescape_query`](@ref) and [`is_safe_query_char`](@ref) from the top-level
`Daf` module itself, as these are all you might be interested in from outside the `Daf` package. The other entities
listed here describe the syntax of a query and give insight into how the query is computed.
"""
module Query

export AxisEntry
export AxisFilter
export AxisLookup
export canonical
export ComparisonOperator
export escape_query
export FilteredAxis
export FilterOperator
export is_safe_query_char
export MatrixAxes
export MatrixEntryAxes
export MatrixLayout
export MatrixEntryLookup
export MatrixLookup
export MatrixQuery
export MatrixSliceAxes
export ParameterAssignment
export parse_matrix_query
export parse_scalar_query
export parse_vector_query
export PropertyComparison
export PropertyLookup
export QueryContext
export QueryOperation
export QueryToken
export ReduceMatrixQuery
export ReduceVectorQuery
export ScalarDataLookup
export ScalarLookup
export ScalarQuery
export MatrixSliceLookup
export unescape_query
export VectorDataLookup
export VectorLookup
export VectorEntryLookup
export VectorQuery

using Daf.Oprec
using Daf.Registry
using URIs

import Base.MathConstants.e
import Daf.Registry.AbstractOperation
import Daf.Registry.ELTWISE_REGISTERED_OPERATIONS
import Daf.Registry.REDUCTION_REGISTERED_OPERATIONS
import Daf.Registry.RegisteredOperation

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

Note that `\\` is also used by Julia inside `"..."` string literals, to escape writing non-printable characters. For
example, `"\\n"` is a single-character string containing a line break, and therefore `"\\\\"` is used to write a single
`\\`. Thus the above example would have to be written as `"cell = ACTG\\\\:Plate1 : batch"`. This isn't nice.

Luckily, Julia also has `raw"..."` string literals that work similarly to Python's `r"..."` strings (in Julia, `r"..."`
is a regular expression, not a string). Inside raw string literals, a `\\` is a `\\` (unless it precedes a `"`).
Therefore the above example could also be written as `raw"cell = ACTG\\:Plate1 : batch`, which is more readable.

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

function prepare_query_string(query_string::AbstractString)::String
    query_string = encode_expression(query_string)
    query_string = replace(query_string, r"#[^\n\r]*([\r\n]|$)"sm => " ")
    query_string = strip(replace(query_string, r"\s+" => " "))
    return query_string
end

@enum QueryOperators OpAnd OpChain OpEltwise OpEqual OpGreaterOrEqual OpGreaterThan OpLessOrEqual OpLessThan OpLookup OpMatch OpNotEqual OpNotMatch OpOr OpPrimarySeparator OpReduce OpSecondarySeparator OpValue OpXor

const OpParameterSeparator = OpPrimarySeparator
const OpParametersSeparator = OpSecondarySeparator
const OpRowMajorSeparator = OpPrimarySeparator
const OpColumnMajorSeparator = OpSecondarySeparator
const OpInvert = OpMatch

const QueryContext = Context{QueryOperators}
const QueryExpression = Expression{QueryOperators}
const QueryOperation = Operation{QueryOperators}
const QueryOperator = Operator{QueryOperators}
const QuerySyntax = Syntax{QueryOperators}
const QueryToken = Token{QueryOperators}

QUERY_SYNTAX = QuerySyntax(
    r"^\s+",                             # Spaces
    r"^[0-9a-zA-Z_.+-]+",                # Operand
    r"^(?:[<!>]=|!~|%>|[%@;,:&|^<=~>])", # Operators
    Dict(
        "%>" => Operator(OpReduce, false, LeftAssociative, 0),
        "%" => Operator(OpEltwise, false, RightAssociative, 1),
        "@" => Operator(OpLookup, false, RightAssociative, 2),
        ";" => Operator(OpSecondarySeparator, false, RightAssociative, 3),
        "," => Operator(OpPrimarySeparator, false, RightAssociative, 3),
        "&" => Operator(OpAnd, false, RightAssociative, 4),
        "|" => Operator(OpOr, false, RightAssociative, 4),
        "^" => Operator(OpXor, false, RightAssociative, 4),
        "<" => Operator(OpLessThan, false, RightAssociative, 5),
        "<=" => Operator(OpLessOrEqual, false, RightAssociative, 5),
        "≤" => Operator(OpLessOrEqual, false, RightAssociative, 5),
        "!=" => Operator(OpNotEqual, false, RightAssociative, 5),
        "≠" => Operator(OpNotEqual, false, RightAssociative, 5),
        "=" => Operator(OpEqual, false, RightAssociative, 5),
        ">=" => Operator(OpGreaterOrEqual, false, RightAssociative, 5),
        "≥" => Operator(OpGreaterOrEqual, false, RightAssociative, 5),
        ">" => Operator(OpGreaterThan, false, RightAssociative, 5),
        "!~" => Operator(OpNotMatch, false, RightAssociative, 5),
        "≁" => Operator(OpNotMatch, false, RightAssociative, 5),
        "~" => Operator(OpMatch, true, RightAssociative, 5),
        ":" => Operator(OpChain, false, RightAssociative, 6),
    ),
)

"""
Assignment of a value to a single parameter of an element-wise or reduction operation.

The parameter value is parsed as a string token, which is passed to the operation, which will convert it to the
appropriate type. Therefore you will need to [`escape`](@ref escape_query) any special characters in a value. "Luckily",
numbers (including floating point numbers) need no escaping.

**ParameterAssignment** = [`Token`](@ref is_safe_query_char) `=` [`Token`](@ref is_safe_query_char)

Examples: `base = 2`, `dtype = Int64`, `eps = 1e-5`.
"""
struct ParameterAssignment
    assignment::QueryOperation
end

function ParameterAssignment(
    context::QueryContext,
    operator::Union{QueryToken, Nothing},
    query_tree::QueryExpression,
)::ParameterAssignment
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "parameter_assignment",
        operator_name = "assignment operator",
        operators = [OpEqual],
    ) do parameter_name, assignment_operator, parameter_value
        parse_string_in_context(context, parameter_name; name = "parameter name")
        parse_string_in_context(context, parameter_value; name = "parameter value")
        return ParameterAssignment(query_tree)
    end
end

function parse_operation_type(
    context::QueryContext,
    query_tree::QueryExpression,
    kind::String,
    registered_operations::Dict{String, RegisteredOperation},
)::Type
    return parse_operand_in_context(context, query_tree; name = "$(kind) type") do operation_type_name
        if !(operation_type_name.string in keys(registered_operations))
            error_in_context(context, "unknown $(kind) type: $(operation_type_name.string)")
        end
        return registered_operations[operation_type_name.string].type
    end
end

function parse_query_operation(
    context::QueryContext,
    query_tree::QueryExpression,
    kind::String,
    registered_operations::Dict{String, RegisteredOperation},
)::AbstractOperation
    return parse_with_list_in_context(
        context,
        query_tree;
        expression_name = "$(kind) operation",
        separator_name = "parameters separator",
        separator_operators = [OpParametersSeparator],
        list_name = "parameters assignments",
        element_type = ParameterAssignment,
        operators = [OpParameterSeparator],
    ) do operation_type_name, parameters_assignments
        operation_type = parse_operation_type(context, operation_type_name, kind, registered_operations)

        parameters_dict = Dict{String, QueryOperation}()
        parameter_symbols = fieldnames(operation_type)
        for parameter_assignment in parameters_assignments
            if !(Symbol(parameter_assignment.assignment.left.string) in parameter_symbols)
                parse_in_context(context, parameter_assignment.assignment; name = "parameter assignment") do
                    parse_in_context(context, parameter_assignment.assignment.left; name = "parameter name") do
                        return error_in_context(
                            context,
                            "unknown parameter: $(parameter_assignment.assignment.left.string)\n" *
                            "for the $(kind) type: $(operation_type)",
                        )
                    end
                end
            end

            previous_assignment = get(parameters_dict, parameter_assignment.assignment.left.string, nothing)
            if previous_assignment != nothing
                parse_in_context(context, previous_assignment; name = "first parameter assignment") do
                    parse_in_context(context, parameter_assignment.assignment; name = "second parameter assignment") do
                        parse_in_context(context, parameter_assignment.assignment.left; name = "parameter name") do
                            return error_in_context(
                                context,
                                "repeated parameter: $(parameter_assignment.assignment.left.string)\n" *
                                "for the $(kind) type: $(operation_type)",
                            )
                        end
                    end
                end
            end

            parameters_dict[parameter_assignment.assignment.left.string] = parameter_assignment.assignment
        end

        return operation_type(context, parameters_dict)
    end
end

"""
Parse a [`EltwiseOperation`](@ref).

**EltwiseOperation** = [`Token`](@ref is_safe_query_char) ( `;` [`ParameterAssignment`](@ref) ( `,` [`ParameterAssignment`](@ref) )* )?
"""
function parse_eltwise_operation(
    context::QueryContext,
    operator::Union{QueryToken, Nothing},
    query_tree::QueryExpression,
)::EltwiseOperation
    return parse_query_operation(context, query_tree, "eltwise", ELTWISE_REGISTERED_OPERATIONS)
end

"""
Parse a [`ReductionOperation`](@ref).

**ReductionOperation** = [`Token`](@ref is_safe_query_char) ( `;` [`ParameterAssignment`](@ref) ( `,` [`ParameterAssignment`](@ref) )* )?
"""
function parse_reduction_operation(
    context::QueryContext,
    operator::Union{QueryToken, Nothing},
    query_tree::QueryExpression,
)::ReductionOperation
    return parse_query_operation(context, query_tree, "reduction", REDUCTION_REGISTERED_OPERATIONS)
end

"""
    function canonical(query::Union{MatrixQuery, VectorQuery, ScalarQuery})::String

Return a canonical form for a query. This strips away any comments, uses a standard white space policy between tokens,
and even reorders [`AxisFilter`](@ref)s in a [`FilteredAxis`](@ref) where possible, so that if two queries are "the
same", they will have the same `canonical` form.
"""
function canonical(operation::AbstractOperation)::String
    return "$(typeof(operation))" *
           " ; " *
           join(
               [
                   (String(field_name) *
                    " = " * #
                    if field_name == :dtype && getfield(operation, :dtype) == nothing
                        "auto"
                    elseif getfield(operation, field_name) == Float64(e)
                        "e"
                    else
                        escape_query("$(getfield(operation, field_name))")
                    end) for field_name in fieldnames(typeof(operation))
               ],
               " , ",
           )
end

"""
Lookup the value of some property for a single axis (for vector data) or a pair of axes (for matrix data).

This is typically just the name of the property to lookup. However, we commonly find that a property of one axis
contains names of entries in another axis. For example, we may have a `batch` property per `cell`, and an `age` property
per `batch`. In such cases, we allow a chained lookup of the color of the type of each cell by writing `batch : age`.
The chain can be as long as necessary (e.g., `batch : donor : sex`).

**PropertyLookup** = [`Token`](@ref is_safe_query_char) ( `:` [`Token`](@ref is_safe_query_char) )*
"""
struct PropertyLookup
    """
    The chain of property names to look up.
    """
    property_names::Vector{String}
end

function PropertyLookup(context::QueryContext, query_tree::QueryExpression)::PropertyLookup
    return PropertyLookup(
        parse_list_in_context(
            context,
            query_tree;
            name = "property lookup",
            element_type = String,
            operators = [OpChain],
        ) do context, operator, property_name
            return parse_string_in_context(context, property_name; name = "property name")
        end,
    )
end

function canonical(property_lookup::PropertyLookup)::String
    return (join([escape_query(property_name) for property_name in property_lookup.property_names], " : "))
end

function Base.isless(left::PropertyLookup, right::PropertyLookup)::Bool
    return left.property_names < right.property_names
end

function Base.:(==)(left::PropertyLookup, right::PropertyLookup)::Bool
    return left.property_names == right.property_names
end

"""
How to compare a each value of a property with some constant value to generate a filter mask.

**ComparisonOperator** _(e.g., `>=`)_ =

`<` _(less than)_

| `<=` | `≤` _(less than or equal)_

| `=` _(equal)_

| `!=` | `≠` _(not equal)_

| `>=` | `≥` _(greater than or equal)_

| `>` _(greater than)_

| `~` _(match a regexp)_

| `!~` | `≁` _(do not match a regexp)_

Note that for matching, you will have to [`escape`](@ref escape_query) any special characters used in regexp; for
example, you will need to write `raw"gene ~ RP\\[LS\\].\\*"` to match all the ribosomal gene names.
"""
@enum ComparisonOperator CmpLessThan CmpLessOrEqual CmpEqual CmpNotEqual CmpGreaterOrEqual CmpGreaterThan CmpMatch CmpNotMatch

PARSE_COMPARISON_OPERATOR = Dict(
    OpLessThan => CmpLessThan,
    OpLessOrEqual => CmpLessOrEqual,
    OpNotEqual => CmpNotEqual,
    OpEqual => CmpEqual,
    OpMatch => CmpMatch,
    OpNotMatch => CmpNotMatch,
    OpGreaterThan => CmpGreaterThan,
    OpGreaterOrEqual => CmpGreaterOrEqual,
)

CANONICAL_COMPARISON_OPERATOR = Dict(
    CmpLessThan => "<",
    CmpLessOrEqual => "<=",
    CmpNotEqual => "!=",
    CmpEqual => "=",
    CmpMatch => "~",
    CmpNotMatch => "!~",
    CmpGreaterThan => ">",
    CmpGreaterOrEqual => ">=",
)

"""
Compare a (non-Boolean) property to a constant value.

This is used to convert any set of non-Boolean property values for the axis entries into a Boolean mask which we can
then use to filter the axis entries, e.g. `> 1` will create a mask of all the entries whose value is larger than one.

**PropertyComparison** = [`ComparisonOperator`](@ref) [`Token`](@ref is_safe_query_char)
"""
struct PropertyComparison
    """
    How to compare the value with the property we looked up.
    """
    comparison_operator::ComparisonOperator

    """
    The constant value to compare against. This is always a string. We convert it to the same (numeric) data type as the
    property values when doing the comparison.
    """
    property_value::String
end

function PropertyComparison(
    context::QueryContext,
    comparison_operator::QueryToken,
    property_value::QueryExpression,
)::PropertyComparison
    return PropertyComparison(
        PARSE_COMPARISON_OPERATOR[comparison_operator.operator.id],
        parse_string_in_context(context, property_value; name = "property value"),
    )
end

function canonical(property_comparison::PropertyComparison)::String
    return CANONICAL_COMPARISON_OPERATOR[property_comparison.comparison_operator] *
           " " *
           escape_query(property_comparison.property_value)
end

function Base.isless(left::PropertyComparison, right::PropertyComparison)::Bool
    return (left.property_value, left.comparison_operator) < (right.property_value, right.comparison_operator)
end

function Base.:(==)(left::PropertyComparison, right::PropertyComparison)::Bool
    return (left.property_value, left.comparison_operator) == (right.property_value, right.comparison_operator)
end

"""
Compute some value for each entry of an axis.

This can simply look up the value of some property of the axis, e.g., `batch : age`. In addition, we allow extra
features for dealing with Boolean masks. First, if looking up a Boolean property, then prefixing it with a `~` will
invert the result, e.g. `~ marker`. Second, when looking up a non-Boolean property, it is possible to convert it into
Boolean values by comparing it with a constant value, e.g., `batch : age > 1`. This allows us to use the result as a
mask, e.g., when filtering which entries of an axis we want to fetch results for.

**AxisLookup** = `~` [`PropertyLookup`](@ref) | [`PropertyLookup`](@ref) [`PropertyComparison`](@ref)?
"""
struct AxisLookup
    """
    Whether to inverse the mask before applying it to the filter.
    """
    is_inverse::Bool

    """
    How to lookup a property value for each entry of the axis.
    """
    property_lookup::PropertyLookup

    """
    How to compare the property values with some constant value.
    """
    property_comparison::Union{PropertyComparison, Nothing}
end

function AxisLookup(context::QueryContext, query_tree::QueryExpression)::AxisLookup
    if check_operation(query_tree, [OpInvert]) != nothing && query_tree.left == nothing
        return parse_in_context(context, query_tree; name = "inverted filter mask") do
            return AxisLookup(true, PropertyLookup(context, query_tree.right), nothing)
        end

    elseif check_operation(
        query_tree,
        [OpLessThan, OpLessOrEqual, OpNotEqual, OpEqual, OpMatch, OpNotMatch, OpGreaterThan, OpGreaterOrEqual],
    ) != nothing
        return parse_operation_in_context(
            context,
            query_tree;
            expression_name = "filter mask",
            operator_name = "comparison operator",
            operators = [
                OpLessThan,
                OpLessOrEqual,
                OpNotEqual,
                OpEqual,
                OpMatch,
                OpNotMatch,
                OpGreaterThan,
                OpGreaterOrEqual,
            ],
        ) do property_lookup, comparison_operator, property_value
            return AxisLookup(
                false,
                PropertyLookup(context, property_lookup),
                PropertyComparison(context, comparison_operator, property_value),
            )
        end

    else
        return parse_in_context(context, query_tree; name = "filter mask, no comparison operator") do
            return AxisLookup(false, PropertyLookup(context, query_tree), nothing)
        end
    end
end

function canonical(axis_lookup::AxisLookup)::String
    result = canonical(axis_lookup.property_lookup)
    if axis_lookup.is_inverse
        result = "~ " * result
    end
    if axis_lookup.property_comparison != nothing
        result *= " " * canonical(axis_lookup.property_comparison)
    end
    return result
end

function Base.isless(left::AxisLookup, right::AxisLookup)::Bool
    left_property_comparison =
        left.property_comparison == nothing ? PropertyComparison(CmpLessThan, "") : left.property_comparison
    right_property_comparison =
        right.property_comparison == nothing ? PropertyComparison(CmpLessThan, "") : right.property_comparison
    return (left.property_lookup, left_property_comparison, left.is_inverse) <
           (right.property_lookup, right_property_comparison, right.is_inverse)
end

"""
A Boolean operator for updating the mask of a filter.

**FilterOperator** =

`&` _(AND - restrict the filter to only the mask entries)_

| `|` _(OR - increase the filter to also include the mask entries)_

| `^` _(XOR - flip the inclusion of the mask entries)_
"""
@enum FilterOperator FilterAnd FilterOr FilterXor

PARSE_FILTER_OPERATOR = Dict(OpAnd => FilterAnd, OpOr => FilterOr, OpXor => FilterXor)

CANONICAL_FILTER_OPERATOR = Dict(FilterAnd => "&", FilterOr => "|", FilterXor => "^")

"""
A filter to apply to an axis.

By default we fetch results for each entry of each axis. We can restrict the set of entries we fetch results for by
applying filters. Each filter applies a Boolean mask to the set of entries we'll return results for. Filters are applied
in a strict left to right order. Each filter can restrict the set of entries (`&`, AND), increase it (`|`, OR) or flip
entries (`^`, XOR). For example, `gene & noisy | lateral & ~ marker` will start with all the genes, restrict the set to
just the noisy genes, increase the set to also include lateral genes, and finally decrease the set to exclude marker
genes. That is, it will return the set of non-marker genes that are also either noisy or lateral.

**AxisFilter** = [`FilterOperator`](@ref) [`AxisLookup`](@ref)
"""
struct AxisFilter
    """
    How to combine the filter and the mask.
    """
    filter_operator::FilterOperator

    """
    How to compute the mask to combine with the filter.
    """
    axis_lookup::AxisLookup
end

function AxisFilter(context::QueryContext, filter_operator::QueryToken, axis_lookup::QueryExpression)::AxisFilter
    return AxisFilter(PARSE_FILTER_OPERATOR[filter_operator.operator.id], AxisLookup(context, axis_lookup))
end

function canonical(axis_filter::AxisFilter)::String
    return CANONICAL_FILTER_OPERATOR[axis_filter.filter_operator] * " " * canonical(axis_filter.axis_lookup)
end

function Base.isless(left::AxisFilter, right::AxisFilter)::Bool
    @assert left.filter_operator == right.filter_operator
    return left.axis_lookup < right.axis_lookup
end

"""
(Possibly filtered) axis to lookup a property for.

By default, all the axis entries will be used. Applying a filter will restrict the results just to the axis entries that
match the result of the filter.

**FilteredAxis** = `Token` [`AxisFilter`](@ref)*
"""
struct FilteredAxis
    """
    The name of the axis to filter.
    """
    axis_name::String

    """
    The sequence of axis filters to apply to the axis.
    """
    axis_filters::Vector{AxisFilter}
end

function FilteredAxis(context::QueryContext, query_tree::QueryExpression)::FilteredAxis
    return parse_with_list_in_context(
        context,
        query_tree;
        expression_name = "filtered axis",
        separator_name = "filter operator",
        separator_operators = [OpAnd, OpOr, OpXor],
        list_name = "axis filters",
        element_type = AxisFilter,
        first_operator = true,
        operators = [OpAnd, OpOr, OpXor],
    ) do axis_name, axis_filters
        return FilteredAxis(
            parse_string_in_context(context, axis_name; name = "axis name"),
            sorted_axis_filters(axis_filters),
        )
    end
end

function sorted_axis_filters(axis_filters::Vector{AxisFilter})::Vector{AxisFilter}
    first_index = 1
    while first_index < length(axis_filters)
        last_index = first_index
        while last_index + 1 <= length(axis_filters) &&
            axis_filters[last_index].filter_operator == axis_filters[last_index + 1].filter_operator
            last_index += 1
        end
        sortable_filters = @view axis_filters[first_index:last_index]
        sort!(sortable_filters)
        first_index = last_index + 1
    end
    return axis_filters
end

function canonical(filtered_axis::FilteredAxis)::String
    result = escape_query(filtered_axis.axis_name)
    for axis_filter in filtered_axis.axis_filters
        result *= " " * canonical(axis_filter)
    end
    return result
end

"""
The layout of the matrix result.

Julia "likes" `ColumnMajor` layout, where each column is consecutive in memory. Numpy "likes" `RowMajor` layout, where
each row is consecutive in memory. What really matters is that the layout will match the operations performed on the
data.
"""
@enum MatrixLayout RowMajor ColumnMajor

PARSE_MATRIX_LAYOUT = Dict(OpRowMajorSeparator => RowMajor, OpColumnMajorSeparator => ColumnMajor)

CANONICAL_MATRIX_LAYOUT = Dict(RowMajor => ",", ColumnMajor => ";")

"""
(Possibly filtered) axes of matrix to lookup a property for.

The first one specifies the matrix rows, the second specifies the matrix columns. The separator specifies whether the
matrix will be in row-major layout (`,`) or column-major layout (`;`).

**MatrixAxes** = [`FilteredAxis`](@ref) [`MatrixLayout`](@ref) [`FilteredAxis`](@ref)
"""
struct MatrixAxes
    """
    Specify the (possibly filtered) axis for the rows of the matrix.
    """
    rows_axis::FilteredAxis

    """
    Whether the matrix should be in row-major or column-major layout.
    """
    matrix_layout::MatrixLayout

    """
    Specify the (possibly filtered) axis for the columns of the matrix.
    """
    columns_axis::FilteredAxis
end

function MatrixAxes(context::QueryContext, query_tree::QueryExpression)::MatrixAxes
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "matrix axes",
        operator_name = "matrix layout separator",
        operators = [OpRowMajorSeparator, OpColumnMajorSeparator],
    ) do rows_axis, matrix_layout, columns_axis
        return MatrixAxes(
            FilteredAxis(context, rows_axis),
            PARSE_MATRIX_LAYOUT[matrix_layout.operator.id],
            FilteredAxis(context, columns_axis),
        )
    end
end

function canonical(matrix_axes::MatrixAxes)::String
    return canonical(matrix_axes.rows_axis) *
           " " *
           CANONICAL_MATRIX_LAYOUT[matrix_axes.matrix_layout] *
           " " *
           canonical(matrix_axes.columns_axis)
end

"""
Lookup a matrix property (that is, a property that gives a value to each combination of entries of two axes).

**MatrixLookup** [`MatrixAxes`](@ref) `@` [`Token`](@ref is_safe_query_char)
"""
struct MatrixLookup
    """
    Specify the two axes to lookup data for.
    """
    matrix_axes::MatrixAxes

    """
    The name of the property to lookup a matrix of values for.
    """
    property_name::String
end

function MatrixLookup(context::QueryContext, query_tree::QueryExpression)::MatrixLookup
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "matrix lookup",
        operator_name = "lookup operator",
        operators = [OpLookup],
    ) do matrix_axes, lookup_operator, property_name
        return MatrixLookup(
            MatrixAxes(context, matrix_axes),
            parse_string_in_context(context, property_name; name = "property name"),
        )
    end
end

function canonical(matrix_lookup::MatrixLookup)::String
    return canonical(matrix_lookup.matrix_axes) * " @ " * escape_query(matrix_lookup.property_name)
end

"""
A query that returns matrix data.

There's only one variant of this: looking up a matrix property and optionally passing it through a sequence of
element-wise operations, using the `%` operator.

**MatrixQuery** = [`MatrixLookup`](@ref) ( `%` [`EltwiseOperation`](@ref parse_eltwise_operation) )*
"""
struct MatrixQuery
    """
    How to lookup a matrix of property values.
    """
    matrix_lookup::MatrixLookup

    """
    Zero or more element-wise operations to apply to the values in the matrix.
    """
    eltwise_operations::Vector{EltwiseOperation}
end

function MatrixQuery(context::QueryContext, query_tree::QueryExpression)::MatrixQuery
    return parse_with_list_in_context(
        context,
        query_tree;
        expression_name = "matrix query",
        separator_name = "eltwise operator",
        separator_operators = [OpEltwise],
        list_name = "eltwise operations",
        parse_field = parse_eltwise_operation,
        element_type = EltwiseOperation,
        operators = [OpEltwise],
    ) do matrix_lookup, eltwise_operations
        return MatrixQuery(MatrixLookup(context, matrix_lookup), eltwise_operations)
    end
end

"""
Parse a [`MatrixQuery`](@ref) from a query string.
"""
function parse_matrix_query(query_string::AbstractString)::MatrixQuery
    query_string = prepare_query_string(query_string)
    if isempty(query_string)
        error("empty query")
    end
    query_tree = parse_encoded_expression(query_string, QUERY_SYNTAX)
    context = Context(query_string, QueryOperators)
    return MatrixQuery(context, query_tree)
end

function canonical(matrix_query::MatrixQuery)::String
    result = canonical(matrix_query.matrix_lookup)
    for eltwise_operation in matrix_query.eltwise_operations
        result *= " % " * canonical(eltwise_operation)
    end
    return result
end

"""
Lookup a vector property (that is, a property that gives a value to each entry of an axis).

**VectorLookup** [`FilteredAxis`](@ref) `@` [`AxisLookup`](@ref)
"""
struct VectorLookup
    """
    Specify the axis to lookup data for.
    """
    filtered_axis::FilteredAxis

    """
    How to lookup some value for each axis entry.
    """
    axis_lookup::AxisLookup
end

function VectorLookup(context::QueryContext, query_tree::QueryExpression)::VectorLookup
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "vector lookup",
        operator_name = "lookup operator",
        operators = [OpLookup],
    ) do filtered_axis, lookup_operator, axis_lookup
        return VectorLookup(FilteredAxis(context, filtered_axis), AxisLookup(context, axis_lookup))
    end
end

function canonical(vector_lookup::VectorLookup)::String
    return canonical(vector_lookup.filtered_axis) * " @ " * canonical(vector_lookup.axis_lookup)
end

"""
Slice a single entry from an axis.

**AxisEntry** = [`Token`](@ref is_safe_query_char) `=` [`Token`](@ref)
"""
struct AxisEntry
    axis_name::String
    entry_name::String
end

function AxisEntry(context::QueryContext, query_tree::QueryExpression)::AxisEntry
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "axis entry",
        operator_name = "equality operator",
        operators = [OpEqual],
    ) do axis_name, assignment_operator, entry_name
        return AxisEntry(
            parse_string_in_context(context, axis_name; name = "axis name"),
            parse_string_in_context(context, entry_name; name = "entry name"),
        )
    end
end

function canonical(axis_entry::AxisEntry)::String
    return escape_query(axis_entry.axis_name) * " = " * escape_query(axis_entry.entry_name)
end

"""
(Possibly filtered) axes of a slice of a matrix to lookup a property for.

The first axis specifies the result entries, and the second specifies the specific entry of an axis to slice.

**MatrixSliceAxes** = [`FilteredAxis`](@ref) `,` [`AxisEntry`](@ref)
"""
struct MatrixSliceAxes
    filtered_axis::FilteredAxis
    axis_entry::AxisEntry
end

function MatrixSliceAxes(context::QueryContext, query_tree::QueryExpression)::MatrixSliceAxes
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "matrix slice axes",
        operator_name = "axes operator",
        operators = [OpParameterSeparator],
    ) do filtered_axis, axes_separator, axis_entry
        return MatrixSliceAxes(FilteredAxis(context, filtered_axis), AxisEntry(context, axis_entry))
    end
end

function canonical(matrix_slice_axes::MatrixSliceAxes)::String
    return canonical(matrix_slice_axes.filtered_axis) * " , " * canonical(matrix_slice_axes.axis_entry)
end

"""
Lookup a vector slice of a matrix property.

**MatrixSliceLookup** [`MatrixSliceAxes`](@ref) `@` [`Token`](@ref)
"""
struct MatrixSliceLookup
    """
    Specify the axes to lookup a slice of the data for.
    """
    matrix_slice_axes::MatrixSliceAxes

    """
    The property to lookup a slice of the data for.
    """
    property_name::String
end

function MatrixSliceLookup(context::QueryContext, query_tree::QueryExpression)::MatrixSliceLookup
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "matrix slice lookup",
        operator_name = "lookup operator",
        operators = [OpLookup],
    ) do matrix_slice_axes, lookup_operator, property_name
        return MatrixSliceLookup(
            MatrixSliceAxes(context, matrix_slice_axes),
            parse_string_in_context(context, property_name; name = "property name"),
        )
    end
end

function canonical(matrix_slice_lookup::MatrixSliceLookup)::String
    return canonical(matrix_slice_lookup.matrix_slice_axes) * " @ " * escape_query(matrix_slice_lookup.property_name)
end

"""
Query for matrix data and reduce it to a vector.

**ReduceMatrixQuery** = [`MatrixQuery`](@ref) `%>` [`ReductionOperation`](@ref parse_reduction_operation)
"""
struct ReduceMatrixQuery
    matrix_query::MatrixQuery
    reduction_operation::ReductionOperation
end

function canonical(reduce_matrix_query::ReduceMatrixQuery)::String
    return canonical(reduce_matrix_query.matrix_query) * " %> " * canonical(reduce_matrix_query.reduction_operation)
end

"""
Lookup vector data.

This can be looking up a vector property, looking up a slice of a matrix property, or reducing the results of matrix
query to a vector.

**VectorDataLookup** = [`VectorLookup`](@ref) | [`MatrixSliceLookup`](@ref) | [`ReduceMatrixQuery`](@ref)
"""
const VectorDataLookup = Union{VectorLookup, MatrixSliceLookup, ReduceMatrixQuery}

function parse_vector_data_lookup(context::QueryContext, query_tree::QueryExpression)::VectorDataLookup
    if check_operation(query_tree, [OpLookup]) != nothing &&
       check_operation(query_tree.left, [OpPrimarySeparator]) != nothing
        return MatrixSliceLookup(context, query_tree)
    else
        return VectorLookup(context, query_tree)
    end
end

"""
A query that returns vector data.

**VectorQuery** = [`VectorDataLookup`](@ref) ( `%` [`EltwiseOperation`](@ref parse_eltwise_operation) )*
"""
struct VectorQuery
    """
    How to lookup a vector of property values.
    """
    vector_data_lookup::VectorDataLookup

    """
    Zero or more element-wise operations to apply to the values in the vector.
    """
    eltwise_operations::Vector{EltwiseOperation}
end

function VectorQuery(context::QueryContext, query_tree::QueryExpression)::VectorQuery
    if check_operation(query_tree, [OpReduce]) != nothing
        return parse_operation_in_context(
            context,
            query_tree;
            expression_name = "reduce matrix query",
            operator_name = "reduction operator",
            operators = [OpReduce],
        ) do matrix_query, reduction_operator, reduction_to_vector
            reduction_operation, eltwise_operations = parse_with_list_in_context(
                context,
                reduction_to_vector;
                expression_name = "reduction to vector",
                separator_name = "eltwise operator",
                separator_operators = [OpEltwise],
                list_name = "eltwise operations",
                parse_field = parse_eltwise_operation,
                element_type = EltwiseOperation,
                operators = [OpEltwise],
            ) do reduction_operation, eltwise_operations
                return parse_reduction_operation(context, reduction_operator, reduction_operation), eltwise_operations
            end
            return VectorQuery(
                ReduceMatrixQuery(MatrixQuery(context, matrix_query), reduction_operation),
                eltwise_operations,
            )
        end

    else
        return parse_with_list_in_context(
            context,
            query_tree;
            expression_name = "vector query",
            separator_name = "eltwise operator",
            separator_operators = [OpEltwise],
            list_name = "eltwise operations",
            parse_field = parse_eltwise_operation,
            element_type = EltwiseOperation,
            operators = [OpEltwise],
        ) do vector_data_lookup, eltwise_operations
            return VectorQuery(parse_vector_data_lookup(context, vector_data_lookup), eltwise_operations)
        end
    end
end

"""
Parse a [`VectorQuery`](@ref) from a query string.
"""
function parse_vector_query(query_string::AbstractString)::VectorQuery
    query_string = prepare_query_string(query_string)
    if isempty(query_string)
        error("empty query")
    end
    query_tree = parse_encoded_expression(query_string, QUERY_SYNTAX)
    context = Context(query_string, QueryOperators)
    return VectorQuery(context, query_tree)
end

function canonical(vector_query::VectorQuery)::String
    result = canonical(vector_query.vector_data_lookup)
    for eltwise_operation in vector_query.eltwise_operations
        result *= " % " * canonical(eltwise_operation)
    end
    return result
end

"""
Lookup vector data.

**ScalarLookup** = [`Token`](@ref)
"""
struct ScalarLookup
    property_name::String
end

function ScalarLookup(context::QueryContext, query_tree::QueryExpression)::ScalarLookup
    return ScalarLookup(parse_string_in_context(context, query_tree; name = "property name"))
end

function canonical(scalar_lookup::ScalarLookup)::String
    return escape_query(scalar_lookup.property_name)
end

"""
Lookup an entry of a vector property.

**VectorEntryLookup** [`AxisEntry`](@ref) `@` [`AxisLookup`](@ref)
"""
struct VectorEntryLookup
    axis_entry::AxisEntry
    axis_lookup::AxisLookup
end

function VectorEntryLookup(context::QueryContext, query_tree::QueryExpression)::VectorEntryLookup
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "vector entry lookup",
        operator_name = "lookup operator",
        operators = [OpLookup],
    ) do axis_entry, lookup_operator, axis_lookup
        return VectorEntryLookup(AxisEntry(context, axis_entry), AxisLookup(context, axis_lookup))
    end
end

function canonical(vector_entry_lookup::VectorEntryLookup)::String
    return canonical(vector_entry_lookup.axis_entry) * " @ " * canonical(vector_entry_lookup.axis_lookup)
end

"""
Locate a single entry of both axes of a a matrix.

**MatrixEntryAxes** [`AxisEntry`](@ref) `,` [`AxisEntry`](@ref)
"""
struct MatrixEntryAxes
    rows_entry::AxisEntry
    columns_entry::AxisEntry
end

function MatrixEntryAxes(context::QueryContext, query_tree::QueryExpression)::MatrixEntryAxes
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "matrix entry axes",
        operator_name = "lookup operator",
        operators = [OpPrimarySeparator],
    ) do rows_entry, axes_operator, columns_entry
        return MatrixEntryAxes(AxisEntry(context, rows_entry), AxisEntry(context, columns_entry))
    end
end

function canonical(matrix_entry_axes::MatrixEntryAxes)::String
    return canonical(matrix_entry_axes.rows_entry) * " , " * canonical(matrix_entry_axes.columns_entry)
end

"""
Lookup an entry of a matrix property.

**MatrixEntryLookup** [`MatrixEntryAxes`](@ref) `@` [`Token`](@ref)
"""
struct MatrixEntryLookup
    matrix_entry_axes::MatrixEntryAxes
    property_name::String
end

function MatrixEntryLookup(context::QueryContext, query_tree::QueryExpression)::MatrixEntryLookup
    return parse_operation_in_context(
        context,
        query_tree;
        expression_name = "matrix entry lookup",
        operator_name = "lookup operator",
        operators = [OpLookup],
    ) do matrix_entry_axes, lookup_operator, property_name
        return MatrixEntryLookup(
            MatrixEntryAxes(context, matrix_entry_axes),
            parse_string_in_context(context, property_name; name = "property name"),
        )
    end
end

function canonical(matrix_entry_lookup::MatrixEntryLookup)::String
    return canonical(matrix_entry_lookup.matrix_entry_axes) * " @ " * escape_query(matrix_entry_lookup.property_name)
end

"""
Query for vector data and reduce it to a scalar. Note that the vector query may itself be a reduction of a matrix to a
vector, allowing reducing a metrix to a scalar (in two steps).

**ReduceVectorQuery** = [`VectorQuery`](@ref) `%>` [`ReductionOperation`](@ref parse_reduction_operation)
"""
struct ReduceVectorQuery
    vector_query::VectorQuery
    reduction_operation::ReductionOperation
end

function canonical(reduce_vector_query::ReduceVectorQuery)::String
    return canonical(reduce_vector_query.vector_query) * " %> " * canonical(reduce_vector_query.reduction_operation)
end

"""
Lookup scalar data.

**ScalarDataLookup** = [`ScalarLookup`](@ref) | [`ReduceVectorQuery`](@ref) | [`VectorEntryLookup`](@ref) | [`MatrixEntryLookup`](@ref)
"""
const ScalarDataLookup = Union{ScalarLookup, ReduceVectorQuery, VectorEntryLookup, MatrixEntryLookup}

function parse_scalar_data_lookup(context::QueryContext, query_tree::QueryExpression)::ScalarDataLookup
    if check_operation(query_tree, [OpLookup]) == nothing
        return ScalarLookup(context, query_tree)
    elseif check_operation(query_tree.left, [OpPrimarySeparator]) != nothing
        return MatrixEntryLookup(context, query_tree)
    else
        return VectorEntryLookup(context, query_tree)
    end
end

"""
A query that returns scalar data.

**ScalarQuery** = [`ScalarDataLookup`](@ref) ( `%` [`EltwiseOperation`](@ref parse_eltwise_operation) )*
"""
struct ScalarQuery
    scalar_data_lookup::ScalarDataLookup
    eltwise_operations::Vector{EltwiseOperation}
end

function canonical(scalar_query::ScalarQuery)::String
    result = canonical(scalar_query.scalar_data_lookup)
    for eltwise_operation in scalar_query.eltwise_operations
        result *= " % " * canonical(eltwise_operation)
    end
    return result
end

"""
Parse a [`ScalarQuery`](@ref) from a query string.
"""
function parse_scalar_query(query_string::AbstractString)::ScalarQuery
    query_string = prepare_query_string(query_string)
    if isempty(query_string)
        error("empty query")
    end
    query_tree = parse_encoded_expression(query_string, QUERY_SYNTAX)
    context = Context(query_string, QueryOperators)

    if check_operation(query_tree, [OpReduce]) != nothing
        return parse_operation_in_context(
            context,
            query_tree;
            expression_name = "reduce vector query",
            operator_name = "reduction operator",
            operators = [OpReduce],
        ) do matrix_query, reduction_operator, reduction_to_vector
            reduction_operation, eltwise_operations = parse_with_list_in_context(
                context,
                reduction_to_vector;
                expression_name = "reduction to scalar",
                separator_name = "eltwise operator",
                separator_operators = [OpEltwise],
                list_name = "eltwise operations",
                parse_field = parse_eltwise_operation,
                element_type = EltwiseOperation,
                operators = [OpEltwise],
            ) do reduction_operation, eltwise_operations
                return parse_reduction_operation(context, reduction_operator, reduction_operation), eltwise_operations
            end
            return ScalarQuery(
                ReduceVectorQuery(VectorQuery(context, matrix_query), reduction_operation),
                eltwise_operations,
            )
        end

    elseif check_operation(query_tree, [OpEltwise]) != nothing
        return parse_with_list_in_context(
            context,
            query_tree;
            expression_name = "scalar data lookup",
            separator_name = "eltwise operator",
            separator_operators = [OpEltwise],
            list_name = "eltwise operations",
            parse_field = parse_eltwise_operation,
            element_type = EltwiseOperation,
            operators = [OpEltwise],
        ) do scalar_data_lookup, eltwise_operations
            return ScalarQuery(parse_scalar_data_lookup(context, scalar_data_lookup), eltwise_operations)
        end

    else
        return ScalarQuery(parse_scalar_data_lookup(context, query_tree), EltwiseOperation[])
    end
end

end  # module
