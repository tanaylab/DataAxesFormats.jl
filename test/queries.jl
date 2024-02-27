function get_result(
    daf::DafReader,
    query::Union{String, Query};
    cache::Bool = true,
)::Union{Vector{String}, StorageScalar, Tuple{String, Vector}, Tuple{Tuple{String, String}, Vector}}
    value = get_query(daf, query; cache = cache)
    if value isa AbstractStringSet
        @test query_result_dimensions(query) == -1
        names = collect(value)
        sort!(names)
        return names
    elseif value isa NamedVector
        @test query_result_dimensions(query) == 1
        return (value.dimnames[1], [(name => value[name]) for name in keys(value.dicts[1])])
    elseif value isa NamedMatrix
        @test query_result_dimensions(query) == 2
        return (
            value.dimnames,
            [(row, column) => value[row, column] for row in keys(value.dicts[1]) for column in keys(value.dicts[2])],
        )
    else
        @test query_result_dimensions(query) == 0
        return value
    end
end

function test_invalid(daf::DafReader, query::Union{String, Query}, message::String)::Nothing
    message = dedent(message)
    @test_throws message query_result_dimensions(query)
    @test_throws (message * "\nfor the daf data: memory!") with_unwrapping_exceptions() do
        return daf[query]
    end
    return nothing
end

function test_invalid(daf::DafReader, query::Union{String, Query}, dimensions::Int, message::String)::Nothing
    @test query_result_dimensions(query) == dimensions
    message = dedent(message)
    @test_throws message with_unwrapping_exceptions() do
        return daf[query]
    end
    return nothing
end

nested_test("queries") do
    daf = MemoryDaf(; name = "memory!")

    nested_test("combine") do
        nested_test("one") do
            @test string(Lookup("score")) == ": score"
        end

        nested_test("two") do
            @test string(Axis("cell") |> Lookup("age")) == "/ cell : age"
        end

        nested_test("three") do
            @test string(Axis("cell") |> Axis("gene") |> Lookup("UMIs")) == "/ cell / gene : UMIs"
            @test string(Axis("cell") |> (Axis("gene") |> Lookup("UMIs"))) == "/ cell / gene : UMIs"
        end

        nested_test("four") do
            @test string(Axis("cell") |> Axis("gene") |> Lookup("UMIs") |> Sum()) == "/ cell / gene : UMIs %> Sum"
            @test string((Axis("cell") |> Axis("gene")) |> (Lookup("UMIs") |> Sum())) == "/ cell / gene : UMIs %> Sum"
        end
    end

    nested_test("names") do
        nested_test("invalid") do
            nested_test("!kind") do
                return test_invalid(
                    daf,
                    Names(),
                    """
                        no kind specified for names
                        in the query: ?
                        at operation: ▲
                    """,
                )
            end

            nested_test("unexpected") do
                set_scalar!(daf, "score", 1.0)
                return test_invalid(
                    daf,
                    q": score ?",
                    """
                        unexpected operation: Names
                        in the query: : score ?
                        at operation:         ▲
                    """,
                )
            end

            nested_test("kind") do
                return test_invalid(
                    daf,
                    q"? vectors",
                    """
                        invalid kind: vectors
                        in the query: ? vectors
                        at operation: ▲▲▲▲▲▲▲▲▲
                    """,
                )
            end

            nested_test("vectors") do
                add_axis!(daf, "cell", ["A", "B"])

                nested_test("kind") do
                    return test_invalid(
                        daf,
                        q"/ cell ? vectors",
                        """
                            unexpected kind: vectors
                            specified for vector names
                            in the query: / cell ? vectors
                            at operation:        ▲▲▲▲▲▲▲▲▲
                        """,
                    )
                end

                nested_test("entry") do
                    return test_invalid(
                        daf,
                        q"/ cell = A ?",
                        """
                            sliced/masked axis for vector names
                            in the query: / cell = A ?
                            at operation:            ▲
                        """,
                    )
                end

                nested_test("slice") do
                    return test_invalid(
                        daf,
                        q"/ cell & name != A ?",
                        """
                            sliced/masked axis for vector names
                            in the query: / cell & name != A ?
                            at operation:                    ▲
                        """,
                    )
                end
            end

            nested_test("matrices") do
                add_axis!(daf, "cell", ["A", "B"])
                add_axis!(daf, "gene", ["X", "Y", "Z"])

                nested_test("kind") do
                    return test_invalid(
                        daf,
                        q"/ cell / gene ? matrices",
                        """
                            unexpected kind: matrices
                            specified for matrix names
                            in the query: / cell / gene ? matrices
                            at operation:               ▲▲▲▲▲▲▲▲▲▲
                        """,
                    )
                end

                nested_test("entry") do
                    return test_invalid(
                        daf,
                        q"/ cell = A / gene ?",
                        """
                            sliced/masked axis for matrix names
                            in the query: / cell = A / gene ?
                            at operation:                   ▲
                        """,
                    )
                end

                nested_test("slice") do
                    return test_invalid(
                        daf,
                        q"/ gene / cell & name != A ?",
                        """
                            sliced/masked axis for matrix names
                            in the query: / gene / cell & name != A ?
                            at operation:                           ▲
                        """,
                    )
                end
            end
        end

        nested_test("scalars") do
            @test get_result(daf, q"? scalars") == []
            set_scalar!(daf, "score", 1.0)
            @test get_result(daf, q"? scalars") == ["score"]
            set_scalar!(daf, "version", "1.0.1")
            @test get_result(daf, q"? scalars") == ["score", "version"]
        end

        nested_test("axes") do
            @test get_result(daf, q"? axes") == []
            add_axis!(daf, "cell", ["A", "B"])
            @test get_result(daf, q"? axes") == ["cell"]
            add_axis!(daf, "batch", ["U", "V"])
            @test get_result(daf, q"? axes") == ["batch", "cell"]
        end

        nested_test("vectors") do
            add_axis!(daf, "cell", ["A", "B"])
            @test get_result(daf, q"/ cell ?") == []
            set_vector!(daf, "cell", "age", [0, 1])
            @test get_result(daf, q"/ cell ?") == ["age"]
            set_vector!(daf, "cell", "type", ["U", "V"])
            @test get_result(daf, q"/ cell ?") == ["age", "type"]
        end

        nested_test("matrices") do
            add_axis!(daf, "cell", ["A", "B"])
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            @test get_result(daf, q"/ cell / gene ?") == []
            @test get_result(daf, q"/ gene / cell ?") == []
            set_matrix!(daf, "cell", "gene", "UMIs", [0 -1 2; -3 4 -5])
            @test get_result(daf, q"/ cell / gene ?") == ["UMIs"]
            @test get_result(daf, q"/ gene / cell ?") == ["UMIs"]
        end
    end

    nested_test("scalar") do
        nested_test("()") do
            set_scalar!(daf, "score", 1.0)

            nested_test("()") do
                @test get_result(daf, ": score") == 1.0
                @test get_result(daf, q": score") == 1.0
                @test get_result(daf, Lookup("score")) == 1.0
            end

            nested_test("eltwise") do
                nested_test("()") do
                    @test get_result(daf, q": score % Log") == 0.0
                end

                nested_test("string") do
                    set_scalar!(daf, "version", "1.0.1")
                    return test_invalid(
                        daf,
                        q": version % Abs",
                        0,
                        """
                            unsupported input type: String
                            for the eltwise operation: Abs
                            in the query: : version % Abs
                            at operation:           ▲▲▲▲▲
                        """,
                    )
                end
            end
        end

        nested_test("over") do
            set_scalar!(daf, "score", 1.0)

            return test_invalid(
                daf,
                q": score : version",
                """
                    unexpected operation: Lookup
                    in the query: : score : version
                    at operation:         ▲▲▲▲▲▲▲▲▲
                """,
            )
        end

        nested_test("+if_missing") do
            set_scalar!(daf, "score", 1.0)
            @test query_result_dimensions(q": score || -1.0") == 0
            @test get_result(daf, q": score || -1.0") == 1.0
        end

        nested_test("-if_missing") do
            nested_test("()") do
                @test get_result(daf, q": score || -1") == -1
                @test get_result(daf, q": score || pi") == Float64(pi)
                @test get_result(daf, q": score || e") == Float64(e)
                @test get_result(daf, q": score || true") == true
                @test get_result(daf, q": score || false") == false
                @test get_result(daf, q": score || Q") == "Q"
            end

            nested_test("String") do
                @test get_result(daf, q": score || -1 String") == "-1"
            end

            nested_test("int") do
                result = get_result(daf, q": score || -1 Float32")
                @test result == -1
                @test result isa Float32
            end
        end
    end

    nested_test("vector") do
        add_axis!(daf, "cell", ["A", "B"])

        nested_test("()") do
            set_vector!(daf, "cell", "age", [0, 1])

            nested_test("()") do
                @test get_result(daf, q"/ cell : age") == ("cell", ["A" => 0, "B" => 1])
            end

            nested_test("axis") do
                @test get_result(daf, q"/ cell") == ("cell", ["A" => "A", "B" => "B"])
                @test get_result(daf, Axis("cell")) == ("cell", ["A" => "A", "B" => "B"])
            end

            nested_test("eltwise") do
                nested_test("()") do
                    @test get_result(daf, q"/ cell : age % Log base 2 eps 1") == ("cell", ["A" => 0.0, "B" => 1.0])
                end

                nested_test("string") do
                    set_vector!(daf, "cell", "type", ["U", "V"])
                    return test_invalid(
                        daf,
                        q"/ cell : type % Abs",
                        1,
                        """
                            unsupported input type: String
                            for the eltwise operation: Abs
                            in the query: / cell : type % Abs
                            at operation:               ▲▲▲▲▲
                        """,
                    )
                end
            end

            nested_test("reduction") do
                nested_test("()") do
                    @test get_result(daf, q"/ cell : age %> Sum") == 1
                end

                nested_test("string") do
                    set_vector!(daf, "cell", "type", ["U", "V"])
                    return test_invalid(
                        daf,
                        q"/ cell : type %> Sum",
                        0,
                        """
                            unsupported input type: String
                            for the reduction operation: Sum
                            in the query: / cell : type %> Sum
                            at operation:               ▲▲▲▲▲▲
                        """,
                    )
                end
            end
        end

        nested_test("entry") do
            set_vector!(daf, "cell", "age", [0, 1])

            nested_test("()") do
                @test get_result(daf, q"/ cell = A") == "A"
            end

            nested_test("!value") do
                return test_invalid(
                    daf,
                    q"/ cell = 1.0",
                    0,
                    """
                        the entry: 1.0
                        does not exist in the axis: cell
                        in the query: / cell = 1.0
                        at operation:        ▲▲▲▲▲
                    """,
                )
            end

            nested_test("non-String") do
                return test_invalid(
                    daf,
                    Axis("cell") |> IsEqual(1.0),
                    0,
                    """
                        comparing a non-String (Float64): 1.0
                        with entries of the axis: cell
                        in the query: / cell = 1.0
                        at operation:        ▲▲▲▲▲
                    """,
                )
            end

            nested_test("fetch") do
                add_axis!(daf, "type", ["U", "V"])
                set_vector!(daf, "type", "color", ["red", "green"])

                nested_test("!if_missing") do
                    @test get_result(daf, q"/ cell = B : type || black => color") == "black"
                end

                set_vector!(daf, "cell", "type", ["", "U"])

                nested_test("()") do
                    @test get_result(daf, q"/ cell = B : type") == "U"
                end

                nested_test("!String") do
                    return test_invalid(
                        daf,
                        q"/ cell : age => batch",
                        1,
                        """
                            fetching with a non-String vector of: Int64
                            of the property: age
                            of the axis: cell
                            in the query: / cell : age => batch
                            at operation:              ▲▲▲▲▲▲▲▲
                        """,
                    )
                end

                nested_test("+if_missing") do
                    @test get_result(daf, q"/ cell = B : type || V") == "U"
                end

                nested_test("-if_missing") do
                    @test get_result(daf, q"/ cell = A : score || -1") == -1
                end

                nested_test("+if_not") do
                    @test get_result(daf, q"/ cell = B : type ?? black => color") == "red"
                end

                nested_test("-if_not") do
                    @test get_result(daf, q"/ cell = A : type ?? black => color") == "black"
                end

                nested_test("deep") do
                    add_axis!(daf, "batch", ["U", "V"])
                    add_axis!(daf, "donor", ["M", "N"])
                    set_vector!(daf, "batch", "donor", ["M", "N"])
                    set_vector!(daf, "cell", "batch", ["", "U"])
                    @test get_result(daf, q"/ cell = A : batch ?? -1 => donor => sex || 1.0") === -1.0
                    set_vector!(daf, "donor", "sex", ["Male", "Female"])
                    @test get_result(daf, q"/ cell = A : batch ?? Unknown => donor => sex") == "Unknown"
                end

                nested_test("invalid") do
                    add_axis!(daf, "batch", ["U", "V"])
                    set_vector!(daf, "batch", "donor", ["M", "N"])
                    set_vector!(daf, "cell", "batch", ["V", "W"])
                    return test_invalid(
                        daf,
                        q"/ cell = B : batch => donor",
                        0,
                        """
                            invalid value: W
                            of the property: batch
                            of the axis: cell
                            is missing from the fetched axis: batch
                            in the query: / cell = B : batch => donor
                            at operation:                    ▲▲▲▲▲▲▲▲
                        """,
                    )
                end

                nested_test("empty") do
                    add_axis!(daf, "batch", ["U", "V"])
                    set_vector!(daf, "batch", "donor", ["M", "N"])
                    set_vector!(daf, "cell", "batch", ["V", ""])
                    return test_invalid(
                        daf,
                        q"/ cell = B : batch => donor",
                        0,
                        """
                            empty value of the property: batch
                            of the axis: cell
                            used for the fetched axis: batch
                            in the query: / cell = B : batch => donor
                            at operation:                    ▲▲▲▲▲▲▲▲
                        """,
                    )
                end
            end
        end

        nested_test("mask") do
            set_vector!(daf, "cell", "age", [0, 1])
            set_vector!(daf, "cell", "type", ["Tcell", "Bcell"])
            set_vector!(daf, "cell", "is_doublet", [true, false])

            nested_test("match") do
                nested_test("()") do
                    @test get_result(daf, q"/ cell & type ~ T.\*") == ("cell", ["A" => "A"])
                    @test get_result(daf, q"/ cell & type !~ T.\*") == ("cell", ["B" => "B"])
                end

                nested_test("!regex") do
                    return test_invalid(
                        daf,
                        q"/ cell & type ~ \[",
                        1,
                        """
                            ErrorException: PCRE compilation error: missing terminating ] for character class at offset 7
                            in the regular expression: ^(:?[)\$
                            in the query: / cell & type ~ \\[
                            at operation:               ▲▲▲▲
                        """,
                    )
                end

                nested_test("!String") do
                    return test_invalid(
                        daf,
                        q"/ cell & age ~ T.\*",
                        1,
                        """
                            matching non-string vector: Int64
                            of the property: age
                            of the axis: cell
                            in the query: / cell & age ~ T.\\*
                            at operation:              ▲▲▲▲▲▲
                        """,
                    )
                end
            end

            nested_test("!type") do
                return test_invalid(
                    daf,
                    q"/ cell & age < Q",
                    1,
                    """
                        ArgumentError: invalid base 10 digit 'Q' in "Q"
                        in the query: / cell & age < Q
                        at operation:              ▲▲▲
                    """,
                )
            end

            nested_test("and") do
                @test get_result(daf, q"/ cell & is_doublet") == ("cell", ["A" => "A"])
                @test get_result(daf, q"/ cell & age") == ("cell", ["B" => "B"])
                @test get_result(daf, q"/ cell & age = 0") == ("cell", ["A" => "A"])
                @test get_result(daf, q"/ cell & age != 0") == ("cell", ["B" => "B"])
                @test get_result(daf, q"/ cell & age < 0") == ("cell", [])
                @test get_result(daf, q"/ cell & age > 0") == ("cell", ["B" => "B"])
                @test get_result(daf, q"/ cell & age <= 0") == ("cell", ["A" => "A"])
                @test get_result(daf, q"/ cell & age >= 0") == ("cell", ["A" => "A", "B" => "B"])
            end

            nested_test("and_not") do
                @test get_result(daf, q"/ cell &! is_doublet") == ("cell", ["B" => "B"])
                @test get_result(daf, q"/ cell &! age") == ("cell", ["A" => "A"])
            end

            nested_test("or") do
                @test get_result(daf, q"/ cell & age | is_doublet") == ("cell", ["A" => "A", "B" => "B"])
            end

            nested_test("or_not") do
                @test get_result(daf, q"/ cell & age |! is_doublet") == ("cell", ["B" => "B"])
            end

            nested_test("xor") do
                @test get_result(daf, q"/ cell & age ^ is_doublet") == ("cell", ["A" => "A", "B" => "B"])
            end

            nested_test("xor_not") do
                @test get_result(daf, q"/ cell & age ^! is_doublet") == ("cell", [])
            end
        end

        nested_test("+if_missing") do
            set_vector!(daf, "cell", "age", [0, 1])
            @test get_result(daf, q"/ cell : age || -1") == ("cell", ["A" => 0, "B" => 1])
        end

        nested_test("-if_missing") do
            @test get_result(daf, q"/ cell : age || -1") == ("cell", ["A" => -1, "B" => -1])
        end

        nested_test("fetch") do
            nested_test("()") do
                add_axis!(daf, "type", ["U", "V"])
                set_vector!(daf, "type", "color", ["red", "green"])
                set_vector!(daf, "cell", "type", ["V", "U"])

                nested_test("()") do
                    @test get_result(daf, q"/ cell = A : type => color") == "green"
                    @test get_result(daf, q"/ cell : type => color") == ("cell", ["A" => "green", "B" => "red"])
                    @test get_result(daf, q"/ cell : type || magenta => color") ==
                          ("cell", ["A" => "green", "B" => "red"])
                end

                nested_test("as_axis") do
                    nested_test("()") do
                        @test get_result(daf, q"/ cell : type ! => color") == ("cell", ["A" => "green", "B" => "red"])
                    end

                    nested_test("implicit") do
                        set_vector!(daf, "cell", "type.manual", ["V", "U"])
                        @test get_result(daf, q"/ cell : type.manual ! => color") ==
                              ("cell", ["A" => "green", "B" => "red"])
                    end

                    nested_test("explicit") do
                        set_vector!(daf, "cell", "manual", ["V", "U"])
                        @test get_result(daf, q"/ cell : manual ! type => color") ==
                              ("cell", ["A" => "green", "B" => "red"])
                    end
                end
            end

            nested_test("invalid") do
                add_axis!(daf, "type", ["U", "V"])
                set_vector!(daf, "type", "color", ["red", "green"])
                set_vector!(daf, "cell", "type", ["V", "W"])
                return test_invalid(
                    daf,
                    q"/ cell : type => color",
                    1,
                    """
                        invalid value: W
                        of the property: type
                        of the axis: cell
                        is missing from the fetched axis: type
                        in the query: / cell : type => color
                        at operation:               ▲▲▲▲▲▲▲▲
                    """,
                )
            end

            nested_test("empty") do
                add_axis!(daf, "type", ["U", "V"])
                set_vector!(daf, "type", "color", ["red", "green"])
                set_vector!(daf, "cell", "type", ["V", ""])
                return test_invalid(
                    daf,
                    q"/ cell : type => color",
                    1,
                    """
                        empty value of the property: type
                        of the axis: cell
                        used for the fetched axis: type
                        in the query: / cell : type => color
                        at operation:               ▲▲▲▲▲▲▲▲
                    """,
                )
            end

            nested_test("-if_missing") do
                add_axis!(daf, "type", ["U", "V"])
                set_vector!(daf, "cell", "age", [0, 1])
                @test get_result(daf, q"/ cell & age : type || magenta") == ("cell", ["B" => "magenta"])
                @test get_result(daf, q"/ cell : type || magenta => color || black") ==
                      ("cell", ["A" => "magenta", "B" => "magenta"])
                set_vector!(daf, "cell", "type", ["V", "U"])
                @test get_result(daf, q"/ cell : type || magenta => color || black") ==
                      ("cell", ["A" => "black", "B" => "black"])
            end

            nested_test("if_not") do
                nested_test("()") do
                    add_axis!(daf, "type", ["U", "V"])
                    set_vector!(daf, "type", "color", ["red", "green"])
                    set_vector!(daf, "cell", "type", ["", "U"])
                    @test get_result(daf, q"/ cell : type ?? => color") == ("cell", ["B" => "red"])
                    @test get_result(daf, q"/ cell & type : type => color") == ("cell", ["B" => "red"])
                    @test get_result(daf, q"/ cell & type : type ?? => color") == ("cell", ["B" => "red"])
                    @test get_result(daf, q"/ cell : type ?? black => color") ==
                          ("cell", ["A" => "black", "B" => "red"])
                end

                nested_test("unexpected") do
                    add_axis!(daf, "type", ["U", "V"])
                    set_vector!(daf, "type", "color", ["red", "green"])
                    set_vector!(daf, "cell", "type", ["V", "U"])
                    return test_invalid(
                        daf,
                        q"/ cell : type => color ??",
                        """
                            unexpected operation: IfNot
                            in the query: / cell : type => color ??
                            at operation:                        ▲▲
                        """,
                    )
                    return test_invalid(
                        daf,
                        q"/ cell : type => color !",
                        """
                            unexpected operation: AsAxis
                            in the query: / cell : type => color !
                            at operation:                        ▲
                        """,
                    )
                    return test_invalid(
                        daf,
                        q"/ cell : type => color ?? !",
                        """
                            unexpected operation: IfNot
                            in the query: / cell : type => color ?? !
                            at operation:                        ▲
                        """,
                    )
                end

                nested_test("deep") do
                    add_axis!(daf, "batch", ["U", "V"])
                    add_axis!(daf, "donor", ["M", "N"])
                    set_vector!(daf, "donor", "sex", ["Male", "Female"])
                    set_vector!(daf, "batch", "donor", ["", "N"])
                    set_vector!(daf, "cell", "batch", ["", "U"])
                    @test get_result(daf, q"/ cell : batch ?? => donor ?? => sex") == ("cell", [])
                    @test get_result(daf, q"/ cell : batch ?? Unknown => donor ?? => sex") ==
                          ("cell", ["A" => "Unknown"])
                    @test get_result(daf, q"/ cell : batch ?? => donor ?? Other => sex") == ("cell", ["B" => "Other"])
                end

                nested_test("unexpected") do
                    set_vector!(daf, "cell", "type", ["V", "U"])

                    nested_test("if_not") do
                        return test_invalid(
                            daf,
                            q"/ cell ??",
                            """
                                unexpected operation: IfNot
                                in the query: / cell ??
                                at operation:        ▲▲
                            """,
                        )
                    end

                    nested_test("as_axis") do
                        return test_invalid(
                            daf,
                            q"/ cell !",
                            """
                                unexpected operation: AsAxis
                                in the query: / cell !
                                at operation:        ▲
                            """,
                        )
                    end

                    nested_test("count_by") do
                        return test_invalid(
                            daf,
                            q"/ cell * age",
                            """
                                unexpected operation: CountBy
                                in the query: / cell * age
                                at operation:        ▲▲▲▲▲
                            """,
                        )
                    end

                    nested_test("group_by") do
                        return test_invalid(
                            daf,
                            q"/ cell @ age",
                            """
                                unexpected operation: GroupBy
                                in the query: / cell @ age
                                at operation:        ▲▲▲▲▲
                            """,
                        )
                    end
                end
            end
        end
    end

    nested_test("matrix") do
        nested_test("()") do
            add_axis!(daf, "cell", ["A", "B"])
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            set_matrix!(daf, "cell", "gene", "UMIs", [0 -1 2; -3 4 -5])

            nested_test("nothing_nothing") do
                @test get_result(daf, q"/ cell / gene : UMIs") == (
                    ("cell", "gene"),
                    [
                        ("A", "X") => 0,
                        ("A", "Y") => -1,
                        ("A", "Z") => 2,
                        ("B", "X") => -3,
                        ("B", "Y") => 4,
                        ("B", "Z") => -5,
                    ],
                )
            end

            nested_test("nothing_entry") do
                @test get_result(daf, q"/ cell / gene = Y : UMIs") == ("cell", ["A" => -1, "B" => 4])
            end

            nested_test("nothing_mask") do
                set_vector!(daf, "gene", "is_marker", [true, false, true])
                @test get_result(daf, q"/ cell / gene & is_marker : UMIs") ==
                      (("cell", "gene"), [("A", "X") => 0, ("A", "Z") => 2, ("B", "X") => -3, ("B", "Z") => -5])
            end

            nested_test("entry_nothing") do
                @test get_result(daf, q"/ cell = A / gene : UMIs") == ("gene", ["X" => 0, "Y" => -1, "Z" => 2])
            end

            nested_test("entry_entry") do
                @test get_result(daf, q"/ cell = A / gene = Y : UMIs") == -1
            end

            nested_test("entry_mask") do
                set_vector!(daf, "gene", "is_marker", [true, false, true])
                @test get_result(daf, q"/ cell = A / gene & is_marker : UMIs") == ("gene", ["X" => 0, "Z" => 2])
            end

            nested_test("mask_nothing") do
                set_vector!(daf, "cell", "is_doublet", [true, false])
                @test get_result(daf, q"/ cell & is_doublet / gene : UMIs") ==
                      (("cell", "gene"), [("A", "X") => 0, ("A", "Y") => -1, ("A", "Z") => 2])
            end

            nested_test("mask_entry") do
                set_vector!(daf, "cell", "is_doublet", [true, false])
                @test get_result(daf, q"/ cell & is_doublet / gene = Y : UMIs") == ("cell", ["A" => -1])
            end

            nested_test("mask_mask") do
                set_vector!(daf, "cell", "is_doublet", [true, false])
                set_vector!(daf, "gene", "is_marker", [true, false, true])
                @test get_result(daf, q"/ cell & is_doublet / gene & is_marker : UMIs") ==
                      (("cell", "gene"), [("A", "X") => 0, ("A", "Z") => 2])
            end

            nested_test("eltwise") do
                @test get_result(daf, q"/ cell / gene : UMIs % Abs") == (
                    ("cell", "gene"),
                    [
                        ("A", "X") => 0,
                        ("A", "Y") => 1,
                        ("A", "Z") => 2,
                        ("B", "X") => 3,
                        ("B", "Y") => 4,
                        ("B", "Z") => 5,
                    ],
                )
            end

            nested_test("reduction") do
                nested_test("once") do
                    @test get_result(daf, q"/ cell / gene : UMIs %> Sum") == ("gene", ["X" => -3, "Y" => 3, "Z" => -3])
                end

                nested_test("twice") do
                    @test get_result(daf, q"/ cell / gene : UMIs %> Sum %> Sum") == -3
                end
            end
        end

        nested_test("+if_missing") do
            add_axis!(daf, "cell", ["A", "B"])
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            set_matrix!(daf, "cell", "gene", "UMIs", [0 -1 2; -3 4 -5])
            @test get_result(daf, q"/ cell / gene : UMIs || -1") == (
                ("cell", "gene"),
                [
                    ("A", "X") => 0,
                    ("A", "Y") => -1,
                    ("A", "Z") => 2,
                    ("B", "X") => -3,
                    ("B", "Y") => 4,
                    ("B", "Z") => -5,
                ],
            )
        end

        nested_test("-if_missing") do
            add_axis!(daf, "cell", ["A", "B"])
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            @test get_result(daf, q"/ cell / gene : UMIs || -1") == (
                ("cell", "gene"),
                [
                    ("A", "X") => -1,
                    ("A", "Y") => -1,
                    ("A", "Z") => -1,
                    ("B", "X") => -1,
                    ("B", "Y") => -1,
                    ("B", "Z") => -1,
                ],
            )
        end
    end

    nested_test("count_by") do
        add_axis!(daf, "cell", ["A", "B", "C"])

        nested_test("()") do
            set_vector!(daf, "cell", "batch", ["U", "V", "U"])
            set_vector!(daf, "cell", "type", ["X", "X", "Y"])
            @test get_result(daf, q"/ cell : batch * type") ==
                  (("batch", "type"), [("U", "X") => 1, ("U", "Y") => 1, ("V", "X") => 1, ("V", "Y") => 0])
        end

        nested_test("as_axis") do
            set_vector!(daf, "cell", "batch", ["U", "V", "U"])
            add_axis!(daf, "type", ["X", "Y", "Z"])

            nested_test("()") do
                set_vector!(daf, "cell", "type", ["X", "X", "Y"])
                @test get_result(daf, q"/ cell : batch * type !") == (
                    ("batch", "type"),
                    [
                        ("U", "X") => 1,
                        ("U", "Y") => 1,
                        ("U", "Z") => 0,
                        ("V", "X") => 1,
                        ("V", "Y") => 0,
                        ("V", "Z") => 0,
                    ],
                )

                @test get_result(daf, q"/ cell : type ! * batch") == (
                    ("type", "batch"),
                    [
                        ("X", "U") => 1,
                        ("X", "V") => 1,
                        ("Y", "U") => 1,
                        ("Y", "V") => 0,
                        ("Z", "U") => 0,
                        ("Z", "V") => 0,
                    ],
                )
            end

            nested_test("implicit") do
                set_vector!(daf, "cell", "type.manual", ["X", "X", "Y"])
                @test get_result(daf, q"/ cell : batch * type.manual !") == (
                    ("batch", "type"),
                    [
                        ("U", "X") => 1,
                        ("U", "Y") => 1,
                        ("U", "Z") => 0,
                        ("V", "X") => 1,
                        ("V", "Y") => 0,
                        ("V", "Z") => 0,
                    ],
                )

                @test get_result(daf, q"/ cell : type.manual ! * batch") == (
                    ("type", "batch"),
                    [
                        ("X", "U") => 1,
                        ("X", "V") => 1,
                        ("Y", "U") => 1,
                        ("Y", "V") => 0,
                        ("Z", "U") => 0,
                        ("Z", "V") => 0,
                    ],
                )
            end

            nested_test("explicit") do
                set_vector!(daf, "cell", "manual", ["X", "X", "Y"])
                @test get_result(daf, q"/ cell : batch * manual ! type") == (
                    ("batch", "type"),
                    [
                        ("U", "X") => 1,
                        ("U", "Y") => 1,
                        ("U", "Z") => 0,
                        ("V", "X") => 1,
                        ("V", "Y") => 0,
                        ("V", "Z") => 0,
                    ],
                )

                @test get_result(daf, q"/ cell : manual ! type * batch") == (
                    ("type", "batch"),
                    [
                        ("X", "U") => 1,
                        ("X", "V") => 1,
                        ("Y", "U") => 1,
                        ("Y", "V") => 0,
                        ("Z", "U") => 0,
                        ("Z", "V") => 0,
                    ],
                )
            end
        end

        nested_test("masked") do
            set_vector!(daf, "cell", "batch", ["U", "", "U"])
            set_vector!(daf, "cell", "type", ["X", "X", ""])
            add_axis!(daf, "batch", ["U", "V", "W"])
            add_axis!(daf, "type", ["X", "Y", "Z"])
            set_vector!(daf, "batch", "age", [1, 2, 3])
            set_vector!(daf, "type", "color", ["red", "green", "blue"])

            @test get_result(daf, q"/ cell : batch ?? => age * type") ==
                  (("age", "type"), [("1", "") => 1, ("1", "X") => 1])

            @test get_result(daf, q"/ cell : batch * type ?? => color") ==
                  (("batch", "color"), [("", "red") => 1, ("U", "red") => 1])

            @test get_result(daf, q"/ cell : batch ?? => age * type ?? => color") ==
                  (("age", "color"), [("1", "red") => 1])
        end
    end

    nested_test("group_by") do
        add_axis!(daf, "cell", ["A", "B", "C"])
        set_vector!(daf, "cell", "batch", ["U", "V", "U"])

        nested_test("vector") do
            set_vector!(daf, "cell", "age", [1, 2, 3])

            nested_test("()") do
                @test get_result(daf, q"/ cell : age @ batch %> Max") == ("batch", ["U" => 3, "V" => 2])
            end

            nested_test("!reduction") do
                return test_invalid(
                    daf,
                    q"/ cell : age @ batch % Abs",
                    """
                        unexpected operation: EltwiseOperation
                        in the query: / cell : age @ batch % Abs
                        at operation:                      ▲▲▲▲▲
                    """,
                )
            end

            nested_test("as_axis") do
                add_axis!(daf, "batch", ["U", "V", "W"])
                @test get_result(daf, q"/ cell : age @ batch ! %> Max || 0") ==
                      ("batch", ["U" => 3, "V" => 2, "W" => 0])
            end
        end

        nested_test("matrix") do
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            set_matrix!(daf, "cell", "gene", "UMIs", [1 2 3; 4 5 6; 7 8 9])
            nested_test("()") do
                @test get_result(daf, q"/ cell / gene : UMIs @ batch %> Sum") == (
                    ("batch", "gene"),
                    [
                        ("U", "X") => 8,
                        ("U", "Y") => 10,
                        ("U", "Z") => 12,
                        ("V", "X") => 4,
                        ("V", "Y") => 5,
                        ("V", "Z") => 6,
                    ],
                )
            end

            nested_test("!reduction") do
                return test_invalid(
                    daf,
                    q"/ cell / gene : UMIs @ batch % Abs",
                    """
                        unexpected operation: EltwiseOperation
                        in the query: / cell / gene : UMIs @ batch % Abs
                        at operation:                              ▲▲▲▲▲
                    """,
                )
            end

            nested_test("partial") do
                set_vector!(daf, "gene", "is_marker", [true, false, true])
                @test get_result(daf, q"/ cell / gene & is_marker : UMIs @ batch %> Sum") ==
                      (("batch", "gene"), [("U", "X") => 8, ("U", "Z") => 12, ("V", "X") => 4, ("V", "Z") => 6])
            end

            nested_test("masked") do
                add_axis!(daf, "batch", ["U", "V", "W"])
                set_vector!(daf, "cell", "is_doublet", [true, false, true])
                set_vector!(daf, "cell", "batch", ["U", "V", ""]; overwrite = true)
                set_vector!(daf, "batch", "age", [1, 2, 3]; overwrite = true)
                @test get_result(daf, q"/ cell / gene : UMIs @ batch ?? => age %> Sum") == (
                    ("age", "gene"),
                    [
                        ("1", "X") => 1,
                        ("1", "Y") => 2,
                        ("1", "Z") => 3,
                        ("2", "X") => 4,
                        ("2", "Y") => 5,
                        ("2", "Z") => 6,
                    ],
                )
                @test get_result(daf, q"/ cell & is_doublet / gene : UMIs @ batch ?? => age %> Sum") ==
                      (("age", "gene"), [("1", "X") => 1, ("1", "Y") => 2, ("1", "Z") => 3])
            end

            nested_test("as_axis") do
                add_axis!(daf, "batch", ["U", "V", "W"])

                nested_test("+if_missing") do
                    @test get_result(daf, q"/ cell / gene : UMIs @ batch ! %> Sum || 0") == (
                        ("batch", "gene"),
                        [
                            ("U", "X") => 8,
                            ("U", "Y") => 10,
                            ("U", "Z") => 12,
                            ("V", "X") => 4,
                            ("V", "Y") => 5,
                            ("V", "Z") => 6,
                            ("W", "X") => 0,
                            ("W", "Y") => 0,
                            ("W", "Z") => 0,
                        ],
                    )
                end

                nested_test("!if_missing") do
                    return test_invalid(
                        daf,
                        q"/ cell / gene : UMIs @ batch ! %> Sum",
                        2,
                        """
                            no values for the group: W
                            and no IfMissing value was specified: || value_for_empty_groups
                            in the query: / cell / gene : UMIs @ batch ! %> Sum
                            at operation:                                ▲▲▲▲▲▲
                        """,
                    )
                end
            end
        end
    end

    nested_test("!parse") do
        nested_test("operand") do
            @test_throws dedent("""
                expected: operator
                in: operand
                at: ▲▲▲▲▲▲▲
            """) Query("operand")
        end

        nested_test("trailing") do
            @test_throws dedent("""
                expected: operator
                in: : operand trailing
                at:           ▲▲▲▲▲▲▲▲
            """) Query(": operand trailing")
        end

        nested_test("operator") do
            @test_throws dedent("""
                expected: value
                in: : :
                at:   ▲
            """) Query(": :")
        end

        nested_test("operation") do
            @test_throws dedent("""
                unknown eltwise operation: Frobulate
                in: : score % Frobulate
                at:           ▲▲▲▲▲▲▲▲▲
            """) Query(": score % Frobulate")
        end

        nested_test("parameter") do
            @test_throws dedent("""
                the parameter: phase
                does not exist for the operation: Log
                in: : score % Log phase 2
                at:               ▲▲▲▲▲
            """) Query(": score % Log phase 2")
        end

        nested_test("parameters") do
            @test_throws dedent("""
                repeated parameter: base
                for the operation: Log
                in: : score % Log base pi base e
                at:                       ▲▲▲▲
            """) Query(": score % Log base pi base e")
        end
    end

    nested_test("invalid") do
        add_axis!(daf, "cell", ["A", "B"])

        nested_test("partial") do
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            return test_invalid(
                daf,
                q"/ cell / gene",
                """
                    partial query: / cell / gene
                """,
            )
        end

        nested_test("unexpected") do
            nested_test("comparison") do
                return test_invalid(
                    daf,
                    q"/ cell > 0",
                    """
                        unexpected operation: IsGreater
                        in the query: / cell > 0
                        at operation:        ▲▲▲
                    """,
                )
            end

            nested_test("eltwise") do
                return test_invalid(
                    daf,
                    q"/ cell % Log",
                    """
                        unexpected operation: EltwiseOperation
                        in the query: / cell % Log base e eps 0.0
                        at operation:        ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
                    """,
                )
            end

            nested_test("reduction") do
                return test_invalid(
                    daf,
                    q"/ cell %> Sum",
                    """
                        unexpected operation: ReductionOperation
                        in the query: / cell %> Sum
                        at operation:        ▲▲▲▲▲▲
                    """,
                )
            end

            nested_test("mask") do
                set_scalar!(daf, "score", 1.0)
                return test_invalid(
                    daf,
                    q": score & is_first",
                    """
                        unexpected operation: And
                        in the query: : score & is_first
                        at operation:         ▲▲▲▲▲▲▲▲▲▲
                    """,
                )
            end
        end

        nested_test("over") do
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            add_axis!(daf, "batch", ["U", "V"])

            return test_invalid(
                daf,
                q"/ cell / gene / batch",
                """
                    unexpected operation: Axis
                    in the query: / cell / gene / batch
                    at operation:               ▲▲▲▲▲▲▲
                """,
            )
        end
    end

    nested_test("cache") do
        add_axis!(daf, "cell", ["A", "B"])
        set_vector!(daf, "cell", "is_doublet", [true, false])
        set_vector!(daf, "cell", "age", [0, 1])

        nested_test("()") do
            @test get_result(daf, q"/ cell & is_doublet : age") == ("cell", ["A" => 0])
            masked = daf[q"/ cell & is_doublet : age"]
            remasked = daf[q"/ cell & is_doublet : age"]
            @test remasked === masked
        end

        nested_test("!") do
            @test get_result(daf, q"/ cell & is_doublet : age"; cache = false) == ("cell", ["A" => 0])
            masked = get_query(daf, q"/ cell & is_doublet : age"; cache = false)
            remasked = daf[q"/ cell & is_doublet : age"]
            @test remasked == masked
            @test remasked !== masked
        end

        nested_test("empty") do
            nested_test("all") do
                @test get_result(daf, q"/ cell & is_doublet : age") == ("cell", ["A" => 0])
                masked = daf[q"/ cell & is_doublet : age"]
                empty_cache!(daf)
                remasked = daf[q"/ cell & is_doublet : age"]
                @test remasked == masked
                @test remasked !== masked
            end

            nested_test("query") do
                @test get_result(daf, q"/ cell & is_doublet : age") == ("cell", ["A" => 0])
                masked = daf[q"/ cell & is_doublet : age"]
                empty_cache!(daf; clear = QueryData)
                remasked = daf[q"/ cell & is_doublet : age"]
                @test remasked == masked
                @test remasked !== masked
            end

            nested_test("!query") do
                @test get_result(daf, q"/ cell & is_doublet : age") == ("cell", ["A" => 0])
                masked = daf[q"/ cell & is_doublet : age"]
                empty_cache!(daf; keep = QueryData)
                remasked = daf[q"/ cell & is_doublet : age"]
                @test remasked === masked
            end
        end
    end
end
