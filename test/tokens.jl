function test_escape_value(unescaped::String, escaped::String)::Nothing
    @test escape_value(unescaped) == escaped
    @test unescape_value(escaped) == unescaped
    return nothing
end

function test_encode_expression(decoded::String, encoded::String)::Nothing
    @test Daf.Tokens.encode_expression(decoded) == encoded
    @test Daf.Tokens.decode_expression(encoded) == decoded
    return nothing
end

OPERATORS = r"^(?:\*\*|\*)"

function token_strings(string::AbstractString)::Vector{String}
    return [token.value for token in Daf.Tokens.tokenize(string, OPERATORS)]
end

nested_test("tokens") do
    nested_test("escape") do
        nested_test("unicode") do
            test_escape_value("א", "א")
            return test_escape_value("ת", "ת")
        end

        nested_test("alpha") do
            test_escape_value("a", "a")
            test_escape_value("z", "z")
            test_escape_value("A", "A")
            test_escape_value("Z", "Z")
            return nothing
        end

        nested_test("digits") do
            test_escape_value("0", "0")
            test_escape_value("9", "9")
            return nothing
        end

        nested_test("allowed") do
            test_escape_value("_", "_")
            test_escape_value("+", "+")
            test_escape_value("-", "-")
            test_escape_value(".", ".")
            return nothing
        end

        nested_test("special") do
            test_escape_value(" ", "\\ ")
            test_escape_value("\\", "\\\\")
            test_escape_value("%", "\\%")
            test_escape_value(":", "\\:")
            return nothing
        end

        return nothing
    end

    nested_test("encode") do
        nested_test("unicode") do
            test_encode_expression("א", "א")
            return test_encode_expression("ת", "ת")
        end

        nested_test("alpha") do
            test_encode_expression("a", "a")
            test_encode_expression("\\a", "_61")
            test_encode_expression("z", "z")
            test_encode_expression("\\z", "_7A")
            test_encode_expression("A", "A")
            test_encode_expression("\\A", "_41")
            test_encode_expression("Z", "Z")
            test_encode_expression("\\Z", "_5A")
            return nothing
        end

        nested_test("digits") do
            test_encode_expression("0", "0")
            test_encode_expression("\\0", "_30")
            test_encode_expression("9", "9")
            test_encode_expression("\\9", "_39")
            return nothing
        end

        nested_test("allowed") do
            test_encode_expression("_", "_5F")
            test_encode_expression("\\_", "_5C_5F")
            test_encode_expression("+", "+")
            test_encode_expression("\\+", "_2B")
            test_encode_expression("-", "-")
            test_encode_expression("\\-", "_2D")
            test_encode_expression(".", ".")
            test_encode_expression("\\.", "_2E")
            return nothing
        end

        nested_test("special") do
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

        return nothing
    end

    nested_test("tokenize") do
        nested_test("empty") do
            @test token_strings("") == String[]
        end

        nested_test("single") do
            @test token_strings("1") == String["1"]
            @test token_strings("x") == String["x"]
            @test token_strings("''") == String[""]
            @test token_strings("\\'\\'") == String["''"]
        end

        nested_test("multiple") do
            @test token_strings(" 10  foo  0א ") == String["10", "foo", "0א"]
        end

        nested_test("specials") do
            @test token_strings("***") == String["**", "*"]
        end

        nested_test("unexpected") do
            @test_throws dedent("""
                unexpected character: ':'
                in: א : x
                at:   ▲
            """) token_strings("א : x")
        end
    end
end
