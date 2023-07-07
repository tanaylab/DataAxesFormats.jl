using Daf.Query

function test_escape_query(unescaped::String, escaped::String)::Nothing
    @test escape_query(unescaped) == escaped
    @test unescape_query(escaped) == unescaped
    return nothing
end

test_set("query") do
    test_set("escape") do
        test_escape_query("a", "a")
        test_escape_query("z", "z")
        test_escape_query("A", "A")
        test_escape_query("Z", "Z")
        test_escape_query("0", "0")
        test_escape_query("9", "9")
        test_escape_query("_", "_")
        test_escape_query(".", ".")
        test_escape_query("+", "+")
        test_escape_query("-", "-")
        test_escape_query(" ", "\\ ")
        test_escape_query("\\", "\\\\")
        test_escape_query("%", "\\%")
        test_escape_query(":", "\\:")
        return nothing
    end

    test_set("prepare") do
        @test Daf.Query.prepare_query_string("a") == "a"
        @test Daf.Query.prepare_query_string("z") == "z"
        @test Daf.Query.prepare_query_string("A") == "A"
        @test Daf.Query.prepare_query_string("Z") == "Z"
        @test Daf.Query.prepare_query_string("0") == "0"
        @test Daf.Query.prepare_query_string("9") == "9"
        @test Daf.Query.prepare_query_string("_") == "_5F"
        @test Daf.Query.prepare_query_string(".") == "."
        @test Daf.Query.prepare_query_string("+") == "+"
        @test Daf.Query.prepare_query_string("-") == "-"
        @test Daf.Query.prepare_query_string(" ") == ""
        @test Daf.Query.prepare_query_string("\\a") == "_61"
        @test Daf.Query.prepare_query_string("a # multi-\n # line comment!\nb") == "a b"
        @test Daf.Query.prepare_query_string("\\") == "\\"
        @test Daf.Query.prepare_query_string("\\\\") == "_5C"
        @test Daf.Query.prepare_query_string("%") == "%"
        @test Daf.Query.prepare_query_string("\\%") == "_25"
        @test Daf.Query.prepare_query_string(":") == ":"
        @test Daf.Query.prepare_query_string("\\:") == "_3A"
    end

    test_set("matrix query") do
        @test_throws "empty query" parse_matrix_query("# This is empty")

        @test canonical(parse_matrix_query("a,b@c")) == "a , b @ c"

        @test canonical(parse_matrix_query("a ; b @ c")) == "a ; b @ c"

        @test canonical(parse_matrix_query("a & b , c @ d")) == "a & b , c @ d"

        @test canonical(parse_matrix_query("a & e & ~ d | c , f @ g")) == "a & ~ d & e | c , f @ g"

        @test canonical(parse_matrix_query("a & b : c , d @ e")) == "a & b : c , d @ e"

        @test canonical(parse_matrix_query("a & b : c < 2 & b : c > 1 , d @ e")) == "a & b : c > 1 & b : c < 2 , d @ e"

        @test_throws dedent("""
            unexpected operator: ~
            in: a & ~ b < 1 , c @ d
            in:     ▲ ·             (property name)
            in:     ··· • ·         (filter mask)
            in: · • ·······         (filtered axis)
            in: ··········· • ·     (matrix axes)
            in: ··············· • · (matrix lookup)
        """) canonical(parse_matrix_query("a & ~ b < 1 , c @ d"))

        @test canonical(parse_matrix_query("a , b @ c % Abs")) == "a , b @ c % Abs ; dtype = auto"

        @test canonical(parse_matrix_query("a , b @ c % Abs % Log ; dtype = Float32")) ==
              "a , b @ c % Abs ; dtype = auto % Log ; dtype = Float32 , base = e , eps = 0.0"

        @test canonical(parse_matrix_query("a , b @ c % Abs % Log ; base = 2 , eps = 1e-5")) ==
              "a , b @ c % Abs ; dtype = auto % Log ; dtype = auto , base = 2.0 , eps = 1.0e-5"

        @test_throws dedent("""
            unknown parameter: base
            for the eltwise type: Abs
            in: a , b @ c % Abs ; base = 2
            in:                   ▲▲▲▲     (parameter name)
            in:                   ···· • · (parameter assignment)
            in:             ··· • ········ (eltwise operation)
            in: ········· • ·············· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Abs ; base = 2"))

        @test_throws dedent("""
            repeated parameter: base
            for the eltwise type: Log
            in: a , b @ c % Log ; base = 2 , base = 10
            in:                              ▲▲▲▲      (parameter name)
            in:                              ···· • ·· (second parameter assignment)
            in:                   ···· • ·             (first parameter assignment)
            in:             ··· • ···················· (eltwise operation)
            in: ········· • ·························· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Log ; base = 2 , base = 10"))

        @test_throws dedent("""
            unknown eltwise type: Sum
            in: a , b @ c % Sum
            in:             ▲▲▲ (eltwise type)
            in: ········· • ··· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Sum"))

        @test_throws dedent("""
            invalid value: "String"
            value must be: a number type
            for the parameter: dtype
            in: a , b @ c % Abs ; dtype = String
            in:                           ▲▲▲▲▲▲ (parameter value)
            in:                   ····· • ······ (parameter assignment)
            in:             ··· • ·············· (eltwise operation)
            in: ········· • ···················· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Abs ; dtype = String"))

        @test_throws dedent("""
            invalid value: "0"
            value must be: positive
            for the parameter: base
            in: a , b @ c % Abs % Log ; base = 0
            in:                                ▲ (parameter value)
            in:                         ···· • · (parameter assignment)
            in:                   ··· • ········ (eltwise operation)
            in:             ··· • ·············· (eltwise operations)
            in: ········· • ···················· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Abs % Log ; base = 0"))

        @test_throws dedent("""
            invalid value: "-1"
            value must be: non-negative
            for the parameter: eps
            in: a , b @ c % Abs % Log ; eps = -1
            in:                               ▲▲ (parameter value)
            in:                         ··· • ·· (parameter assignment)
            in:                   ··· • ········ (eltwise operation)
            in:             ··· • ·············· (eltwise operations)
            in: ········· • ···················· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Abs % Log ; eps = -1"))

        @test_throws dedent("""
            invalid value: "x"
            value must be: a valid Float64
            for the parameter: base
            in: a , b @ c % Abs % Log ; base = x
            in:                                ▲ (parameter value)
            in:                         ···· • · (parameter assignment)
            in:                   ··· • ········ (eltwise operation)
            in:             ··· • ·············· (eltwise operations)
            in: ········· • ···················· (matrix query)
        """) canonical(parse_matrix_query("a , b @ c % Abs % Log ; base = x"))
    end

    test_set("vector query") do
        @test_throws "empty query" parse_vector_query("# This is empty")

        @test canonical(parse_vector_query("a @ b")) == "a @ b"

        @test canonical(parse_vector_query("a & b @ c")) == "a & b @ c"

        @test canonical(parse_vector_query("a @ ~ c")) == "a @ ~ c"

        @test canonical(parse_vector_query("a @ c : d")) == "a @ c : d"

        @test canonical(parse_vector_query("a @ c : d > 1")) == "a @ c : d > 1"

        @test canonical(parse_vector_query("a @ b % Log ; base = e")) ==
              "a @ b % Log ; dtype = auto , base = e , eps = 0.0"

        @test canonical(parse_vector_query("a , b = c @ d")) == "a , b = c @ d"

        @test_throws dedent("""
            unexpected operator: =
            in: a = b , c @ d
            in: · ▲ ·         (axis name)
            in: ····· • ·     (matrix slice axes)
            in: ········· • · (matrix slice lookup)
        """) parse_vector_query("a = b , c @ d")

        @test_throws dedent("""
            unexpected operator: :
            in: a , b = c @ d : e
            in:             · ▲ · (property name)
            in: ········· • ····· (matrix slice lookup)
        """) parse_vector_query("a , b = c @ d : e")

        @test canonical(parse_vector_query("a , b @ d %> Sum")) == "a , b @ d %> Sum ; dtype = auto"

        @test canonical(parse_vector_query("a , b @ d % Abs %> Sum % Log")) ==
              "a , b @ d % Abs ; dtype = auto %> Sum ; dtype = auto % Log ; dtype = auto , base = e , eps = 0.0"
    end

    test_set("scalar query") do
        @test_throws "empty query" parse_scalar_query("# This is empty")

        @test canonical(parse_scalar_query("a")) == "a"

        @test canonical(parse_scalar_query("a @ b %> Sum")) == "a @ b %> Sum ; dtype = auto"

        @test canonical(parse_scalar_query("a @ b % Abs %> Sum")) == "a @ b % Abs ; dtype = auto %> Sum ; dtype = auto"

        @test_throws dedent("""
            unknown reduction type: Log
            in: a , b @ c % Abs %> Sum %> Log %> Max
            in:                           ▲▲▲        (reduction type)
            in: ······················ •• ···        (reduce matrix query)
            in: ····························· •• ··· (reduce vector query)
        """) parse_scalar_query("a , b @ c % Abs %> Sum %> Log %> Max")

        @test canonical(parse_scalar_query("a , b @ c % Abs %> Sum % Log %> Max")) ==
              "a , b @ c % Abs ; dtype = auto %> Sum ; dtype = auto % Log ; dtype = auto , base = e , eps = 0.0 %> Max ; dtype = auto"

        @test canonical(parse_scalar_query("a = b @ c")) == "a = b @ c"

        @test canonical(parse_scalar_query("a = b @ c : d")) == "a = b @ c : d"

        @test canonical(parse_scalar_query("a = b , c = d @ e")) == "a = b , c = d @ e"

        @test canonical(parse_scalar_query("a % Abs")) == "a % Abs ; dtype = auto"

        @test canonical(parse_scalar_query("a @ b %> Sum % Abs")) == "a @ b %> Sum ; dtype = auto % Abs ; dtype = auto"

        @test canonical(parse_scalar_query("a = b @ c % Abs")) == "a = b @ c % Abs ; dtype = auto"

        @test canonical(parse_scalar_query("a = b @ c : d % Abs")) == "a = b @ c : d % Abs ; dtype = auto"

        @test canonical(parse_scalar_query("a = b , c = d @ e % Abs")) == "a = b , c = d @ e % Abs ; dtype = auto"
    end
end
