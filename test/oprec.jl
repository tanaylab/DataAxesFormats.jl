using Daf.Oprec

function test_encode_expression(decoded::String, encoded::String)::Nothing
    @test encode_expression(decoded) == encoded
    @test decode_expression(encoded) == decoded
    return nothing
end

@enum Operators Plus Minus Mul Div Power Bang

SYNTAX = Syntax{Operators}(
    r"^\s+",                 # Spaces
    r"^[0-9a-zA-Z_]+",       # Operand
    r"^\*\*|(?:/|[!\*%+-])", # Operators
    Dict(
        "**" => Operator(Power, false, LeftAssociative, 3),
        "*" => Operator(Mul, false, LeftAssociative, 2),
        "/" => Operator(Div, false, LeftAssociative, 2),
        "+" => Operator(Plus, false, RightAssociative, 1),
        "-" => Operator(Minus, true, RightAssociative, 1),
        "!" => Operator(Bang, false, LeftAssociative, 0),
    ),
)

function token_strings(string::AbstractString)::Vector{String}
    return [decode_expression(token.string) for token in Daf.Oprec.tokenize(encode_expression(string), SYNTAX)]
end

function parsed_string(string::AbstractString)::String
    return Daf.Oprec.as_string(build_encoded_expression(encode_expression(string), SYNTAX))
end

struct TestSum
    operator::AbstractString
    operand::Expression{Operators}
end

function TestSum(
    context::Context{Operators},
    operator::Union{Token{Operators}, Nothing},
    operand::Expression{Operators},
)::TestSum
    if operand isa Operation && operand.token.operator.id == Power
        parse_operand_in_context(context, operand; name = "multiplication") do operand
            @assert false
        end
    end
    return TestSum(operator == nothing ? "(+)" : operator.string, operand)
end

function as_string(sums::Vector{TestSum})::String
    return join([sum.operator * " " * Daf.Oprec.as_string(sum.operand) for sum in sums], " ")
end

struct TestWithSums
    field::String
    sums::Vector{TestSum}
end

function TestWithSums(context::Context{Operators}, field::Expression{Operators}, sums::Vector{TestSum})::TestWithSums
    return TestWithSums(Daf.Oprec.as_string(field), sums)
end

function as_string(with_sums::TestWithSums)::String
    return with_sums.field * " ! " * as_string(with_sums.sums)
end

test_set("oprec") do
    test_set("encode") do
        test_encode_expression("a", "a")
        test_encode_expression("\\a", "_61")
        test_encode_expression("z", "z")
        test_encode_expression("\\z", "_7A")
        test_encode_expression("A", "A")
        test_encode_expression("\\A", "_41")
        test_encode_expression("Z", "Z")
        test_encode_expression("\\Z", "_5A")
        test_encode_expression("0", "0")
        test_encode_expression("\\0", "_30")
        test_encode_expression("9", "9")
        test_encode_expression("\\9", "_39")
        test_encode_expression("_", "_5F")
        test_encode_expression("\\_", "_5C_5F")
        test_encode_expression(".", ".")
        test_encode_expression("\\.", "_2E")
        test_encode_expression("+", "+")
        test_encode_expression("\\+", "_2B")
        test_encode_expression("-", "-")
        test_encode_expression("\\-", "_2D")
        test_encode_expression(" ", " ")
        test_encode_expression("\\ ", "_20")
        test_encode_expression("\\", "\\")
        test_encode_expression("\\\\", "_5C")
        test_encode_expression("%", "%")
        test_encode_expression("\\%", "_25")
        test_encode_expression(":", ":")
        test_encode_expression("\\:", "_3A")
        return nothing
    end

    test_set("tokenize") do
        @test token_strings("") == String[]

        @test token_strings("1") == String["1"]

        @test token_strings(" 1  x ") == String["1", "x"]

        @test token_strings("+- * /") == String["+", "-", "*", "/"]

        @test_throws dedent("""
            unexpected character: ':'
            in: 1 : x
            at:   ▲
        """) token_strings("1 : x")

        return nothing
    end

    test_set("organize") do
        @test parsed_string("") == ""

        @test parsed_string("1") == "1"

        @test parsed_string("x") == "x"

        @test parsed_string("\\#") == "\\#"

        @test parsed_string("-1") == "( - 1)"

        @test parsed_string("1 + x") == "(1 + x)"

        @test parsed_string("1 * x") == "(1 * x)"

        @test parsed_string("1 * x * 2 * 3") == "(((1 * x) * 2) * 3)"

        @test parsed_string("1 + x + 2 + 3") == "(1 + (x + (2 + 3)))"

        @test parsed_string("1 + x * 2 + 3") == "(1 + ((x * 2) + 3))"

        @test parsed_string("1 * x ** 2 * 3") == "((1 * (x ** 2)) * 3)"

        @test parsed_string("1 + x - 3") == "(1 + (x - 3))"

        @test parsed_string("1 + x - - 3") == "(1 + (x - ( - 3)))"

        @test parsed_string("- x * 3") == "( - (x * 3))"

        @test parsed_string("1 ! 2") == "(1 ! 2)"

        @test parsed_string("1 ! - 2") == "(1 ! ( - 2))"

        @test parsed_string("1 ! 2 + 3 + 4") == "(1 ! (2 + (3 + 4)))"

        @test parsed_string("1 ! -2 + 3 + 4") == "(1 ! (( - 2) + (3 + 4)))"

        @test parsed_string("1 ! 2 + 3 * 4 + 5") == "(1 ! (2 + ((3 * 4) + 5)))"

        @test parsed_string("1 ! -2 + 3 * 4 + 5") == "(1 ! (( - 2) + ((3 * 4) + 5)))"

        @test parsed_string("1 ! 2 + 3 * 4 ** 5 * 6 + 7") == "(1 ! (2 + (((3 * (4 ** 5)) * 6) + 7)))"

        @test parsed_string("1 ! -2 + 3 * 4 ** 5 * 6 + 7") == "(1 ! (( - 2) + (((3 * (4 ** 5)) * 6) + 7)))"

        @test_throws dedent("""
            expected: operand
            in: -
            at:  ▲
        """) parsed_string("-")

        @test_throws dedent("""
            expected: operand
            in: \\# +
            at:     ▲
        """) parsed_string("\\# +")

        @test_throws dedent("""
            expected: operand
            in: 1 + +
            at:     ▲
        """) parsed_string("1 + +")

        @test_throws dedent("""
            expected: operator
            in: 1 \\#
            at:   ▲▲
        """) parsed_string("1 \\#")

        return nothing
    end

    test_set("error_in_context") do
        encoded_string = encode_expression("-1 + x / 0")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)

        @test check_operation(tree, [Plus, Minus]).operator.id == Plus
        @test check_operation(tree, [Div, Mul]) == nothing
        @test check_operation(tree.right.right, [Plus, Minus]) == nothing

        @test parse_operation_in_context(
            context,
            tree;
            expression_name = "plus-minus",
            operator_name = "+/-",
            operators = [Plus, Minus],
        ) do left, operator, right
            @test Daf.Oprec.as_string(left) == "( - 1)"
            @test Daf.Oprec.as_string(operator) == "+"
            @test Daf.Oprec.as_string(right) == "(x / 0)"
            return true
        end

        @test "0" == parse_string_in_context(context, tree.right.right; name = "zero")

        @test_throws dedent("""
            expected operator: **
            in: -1 + x / 0
            in:  ▲         (pow)
            in: •·         (negate)
            in: ·· • ····· (add)
        """) parse_operation_in_context(
            context,
            tree;
            expression_name = "add",
            operator_name = "+",
            operators = [Plus],
        ) do add_left, add_operator, add_right
            parse_operation_in_context(
                context,
                add_left;
                expression_name = "negate",
                operator_name = "-",
                operators = [Minus],
            ) do sub_left, sub_operator, sub_right
                parse_operation_in_context(
                    context,
                    sub_right;
                    expression_name = "pow",
                    operator_name = "**",
                    operators = [Power],
                ) do pow_left, pow_operator, pow_right
                    @assert false
                end
            end
        end

        @test_throws dedent("""
          unexpected operator: /
          expected operator: -
          in: -1 + x / 0
          in:      · ▲ · (sub)
          in: ·· • ····· (add)
        """) parse_operation_in_context(
            context,
            tree;
            expression_name = "add",
            operator_name = "+",
            operators = [Plus],
        ) do add_left, add_operator, add_right
            parse_operation_in_context(
                context,
                add_right;
                expression_name = "sub",
                operator_name = "-",
                operators = [Minus],
            ) do sub_left, sub_operator, sub_right
                @assert false
            end
        end

        @test_throws dedent("""
          zero denominator
          in: -1 + x / 0
          in:          ▲ (denominator)
          in:      · • · (div)
          in: ·· • ····· (add)
        """) parse_operation_in_context(
            context,
            tree;
            expression_name = "add",
            operator_name = "+",
            operators = [Plus],
        ) do add_left, add_operator, add_right
            parse_operation_in_context(
                context,
                add_right;
                expression_name = "div",
                operator_name = "/",
                operators = [Div],
            ) do div_left, div_operator, div_right
                parse_operand_in_context(context, div_right; name = "denominator") do denominator
                    return error_in_context(context, "zero denominator")
                end
            end
        end

        @test_throws dedent("""
          unexpected operator: -
          in: -1 + x / 0
          in: ▲·         (number)
          in: ·· • ····· (add)
        """) parse_operation_in_context(
            context,
            tree;
            expression_name = "add",
            operator_name = "+",
            operators = [Plus],
        ) do add_left, add_operator, add_right
            parse_operand_in_context(context, add_left; name = "number") do
                @assert false
            end
        end
    end

    test_set("parse_list_in_context") do
        encoded_string = encode_expression("1")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test as_string(
            parse_list_in_context(context, tree; list_name = "sum", element_type = TestSum, operators = [Plus, Minus]),
        ) == "(+) 1"

        encoded_string = encode_expression("1 + 2 - 3")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test as_string(
            parse_list_in_context(context, tree; list_name = "sum", element_type = TestSum, operators = [Plus, Minus]),
        ) == "(+) 1 + 2 - 3"

        encoded_string = encode_expression("- 1 + 2 * 3 - 4")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test as_string(
            parse_list_in_context(context, tree; list_name = "sum", element_type = TestSum, operators = [Plus, Minus]),
        ) == "(+) ( - 1) + (2 * 3) - 4"

        encoded_string = encode_expression("1 ** 2")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test_throws dedent("""
            unexpected operator: **
            in: 1 ** 2
            in: · ▲▲ · (multiplication)
        """) parse_list_in_context(context, tree; list_name = "sum", element_type = TestSum, operators = [Plus, Minus])

        encoded_string = encode_expression("1 + 2 ** 3 + 4")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test_throws dedent("""
            unexpected operator: **
            in: 1 + 2 ** 3 + 4
            in:     · ▲▲ ·     (multiplication)
            in: · • ······ • · (sum)
        """) parse_list_in_context(context, tree; list_name = "sum", element_type = TestSum, operators = [Plus, Minus])
    end

    test_set("parse_with_list_in_context") do
        encoded_string = encode_expression("1")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test as_string(
            parse_with_list_in_context(
                context,
                tree;
                expression_name = "with sums",
                separator_name = "bang operator",
                separator_operators = [Bang],
                list_name = "sum",
                element_type = TestSum,
                operators = [Plus, Minus],
            ) do field, sums
                return TestWithSums(context, field, sums)
            end,
        ) == "1 ! "

        encoded_string = encode_expression("1 ! 2")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test as_string(
            parse_with_list_in_context(
                context,
                tree;
                expression_name = "with sums",
                separator_name = "bang operator",
                separator_operators = [Bang],
                list_name = "sum",
                element_type = TestSum,
                operators = [Plus, Minus],
            ) do field, sums
                return TestWithSums(context, field, sums)
            end,
        ) == "1 ! (+) 2"

        encoded_string = encode_expression("1 ! 2 + 3 * 4 ** 5 + 6")
        tree = build_encoded_expression(encoded_string, SYNTAX)
        context = Context(encoded_string, Operators)
        @test as_string(
            parse_with_list_in_context(
                context,
                tree;
                expression_name = "with sums",
                separator_name = "bang operator",
                separator_operators = [Bang],
                list_name = "sum",
                element_type = TestSum,
                operators = [Plus, Minus],
            ) do field, sums
                return TestWithSums(context, field, sums)
            end,
        ) == "1 ! (+) 2 + (3 * (4 ** 5)) + 6"
    end
end
