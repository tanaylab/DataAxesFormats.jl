function get_result(
    daf::DafReader,
    query::Union{String, Query};
    cache::Bool = true,
    axis_name::Maybe{AbstractString} = nothing,
    requires_relayout::Bool = false,
)::Any
    if axis_name === nothing
        @test !is_axis_query(query)
    else
        @test is_axis_query(query)
        @test query_axis_name(query) == axis_name
    end

    @test has_query(daf, query)
    @test query_requires_relayout(daf, query) == requires_relayout

    value = query |> get_query(daf; cache)

    if value isa AbstractSet{<:AbstractString}
        @test query_result_dimensions(query) == -1
        names = collect(value)
        sort!(names)
        return names

    elseif value isa NamedVector
        @test query_result_dimensions(query) == 1
        return (value.dimnames[1], [(name => value[name]) for name in keys(value.dicts[1])])

    elseif value isa AbstractVector{<:AbstractString}
        @test query_result_dimensions(query) == 1  # UNTESTED
        return value  # UNTESTED

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

function test_invalid(daf::DafReader, query::Union{AbstractString, Query}, message::AbstractString)::Nothing
    message = chomp(message)
    @test_throws message query_result_dimensions(query)
    @test_throws (message * "\nfor the daf data: memory!") with_unwrapping_exceptions() do
        return daf[query]
    end
    @test !has_query(daf, query)
    return nothing
end

function test_invalid(  # UNTESTED
    daf::DafReader,
    query::Union{AbstractString, Query},
    dimensions::Int,
    message::AbstractString,
)::Nothing
    @test query_result_dimensions(query) == dimensions
    @test_throws message with_unwrapping_exceptions() do
        return daf[query]
    end
    return nothing
end

nested_test("queries") do
    daf = MemoryDaf(; name = "memory!")

    nested_test("invalid") do
        add_axis!(daf, "cell", ["A", "B"])

        nested_test("!operator") do
            @test_throws chomp("""
                               expected: operator
                               in: cell
                               at: ▲▲▲▲
                               """) parse_query("cell")
            return nothing
        end

        nested_test("!value") do
            @test_throws chomp("""
                               expected: value
                               in: .
                               at:  ▲
                               """) parse_query(".")
            return nothing
        end

        nested_test("~value") do
            @test_throws chomp("""
                               expected: value
                               in: . .
                               at:   ▲
                               """) parse_query(". .")
            return nothing
        end

        nested_test("partial") do
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            test_invalid(daf, q"@ cell @ gene", "invalid query: @ cell @ gene")
            return nothing
        end

        nested_test("unexpected") do
            nested_test("mask") do
                set_scalar!(daf, "score", 1.0)
                test_invalid(daf, q". score [ is_first ]", chomp("""
                                                                 invalid operation(s)
                                                                 in the query: . score [ is_first ]
                                                                 at location:         ▲
                                                                 """))
                return nothing
            end
        end

        nested_test("operation") do
            @test_throws chomp("""
                         unknown eltwise operation: Frobulate
                         in: . score % Frobulate
                         at:           ▲▲▲▲▲▲▲▲▲
                         """) parse_query(". score % Frobulate")
        end

        nested_test("parameter") do
            @test_throws chomp("""
                         the parameter: phase
                         does not exist for the operation: Log
                         in: . score % Log phase 2
                         at:               ▲▲▲▲▲
                         """) parse_query(". score % Log phase 2")
        end

        nested_test("parameters") do
            @test_throws chomp("""
                         repeated parameter: base
                         for the operation: Log
                         in: . score % Log base pi base e
                         at:                       ▲▲▲▲
                         """) parse_query(". score % Log base pi base e")
        end
    end

    nested_test("empty") do
        @test string(parse_query("@ metacell != ''")) == "@ metacell != ''"
    end

    nested_test("combine") do
        nested_test("one") do
            @test string(LookupScalar("score")) == ". score"
        end

        nested_test("two") do
            nested_test("()") do
                @test string(QuerySequence(Axis("cell"), LookupVector("age"))) == "@ cell : age"
            end

            nested_test("str") do
                @test string(Axis("cell") |> ": age") == "@ cell : age"
                @test string("@ cell" |> LookupVector("age")) == "@ cell : age"
            end
        end

        nested_test("three") do
            @test string(Axis("cell") |> Axis("gene") |> LookupMatrix("UMIs")) == "@ cell @ gene :: UMIs"
            @test string(Axis("cell") |> (Axis("gene") |> LookupMatrix("UMIs"))) == "@ cell @ gene :: UMIs"
        end

        nested_test("four") do
            @test string(Axis("cell") |> Axis("gene") |> LookupMatrix("UMIs") |> Sum()) ==
                  "@ cell @ gene :: UMIs >> Sum"
            @test string((Axis("cell") |> Axis("gene")) |> (LookupMatrix("UMIs") |> Sum())) ==
                  "@ cell @ gene :: UMIs >> Sum"
        end
    end

    nested_test("cache") do
        add_axis!(daf, "cell", ["A", "B"])
        set_vector!(daf, "cell", "is_doublet", [true, false])
        set_vector!(daf, "cell", "age", [0, 1])

        nested_test("()") do
            @test get_result(daf, q"@ cell [ is_doublet ] : age") == ("cell", ["A" => 0])
            masked = daf[q"@ cell [ is_doublet ] : age"]
            remasked = daf[q"@ cell [ is_doublet ] : age"]
            @test remasked === masked
        end

        nested_test("!") do
            @test get_result(daf, q"@ cell [ is_doublet ] : age"; cache = false) == ("cell", ["A" => 0])
            masked = get_query(daf, q"@ cell [ is_doublet ] : age"; cache = false)
            remasked = daf[q"@ cell [ is_doublet ] : age"]
            @test remasked == masked
            @test remasked !== masked
        end

        nested_test("empty") do
            nested_test("all") do
                @test get_result(daf, q"@ cell [ is_doublet ] : age") == ("cell", ["A" => 0])
                masked = daf[q"@ cell [ is_doublet ] : age"]
                empty_cache!(daf)
                remasked = daf[q"@ cell [ is_doublet ] : age"]
                @test remasked == masked
                @test remasked !== masked
            end

            nested_test("query") do
                @test get_result(daf, q"@ cell [ is_doublet ] : age") == ("cell", ["A" => 0])
                masked = daf[q"@ cell [ is_doublet ] : age"]
                empty_cache!(daf; clear = QueryData)
                remasked = daf[q"@ cell [ is_doublet ] : age"]
                @test remasked == masked
                @test remasked !== masked
            end

            nested_test("!query") do
                @test get_result(daf, q"@ cell [ is_doublet ] : age") == ("cell", ["A" => 0])
                masked = daf[q"@ cell [ is_doublet ] : age"]
                empty_cache!(daf; keep = QueryData)
                remasked = daf[q"@ cell [ is_doublet ] : age"]
                @test remasked === masked
            end
        end
    end

    nested_test("names") do
        nested_test("unexpected") do
            test_invalid(daf, q"? ?", "invalid query: ? ?")
            return nothing
        end

        nested_test("scalars") do
            set_scalar!(daf, "version", "0.1.2")
            set_scalar!(daf, "species", "mouse")
            @test get_result(daf, q"?") == ["species", "version"]
        end

        nested_test("axes") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B"])
            @test get_result(daf, "@ ?") == ["cell", "gene"]
        end

        nested_test("vectors") do
            add_axis!(daf, "gene", ["A", "B"])
            set_vector!(daf, "gene", "is_lateral", [true, false])
            set_vector!(daf, "gene", "is_marker", [true, true])
            @test get_result(daf, "@ gene ?") == ["is_lateral", "is_marker"]
        end

        nested_test("matrices") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B"])
            set_matrix!(daf, "cell", "gene", "UMIs", [1 2; 3 4])
            @test get_result(daf, "@ cell @ gene ?") == ["UMIs"]
        end
    end

    nested_test("scalar") do
        nested_test("lookup") do
            nested_test("()") do
                set_scalar!(daf, "version", "0.1.2")
                @test get_result(daf, ". version") == "0.1.2"
                @test get_result(daf, parse_query("version", LookupScalar)) == "0.1.2"
            end

            nested_test("with_default") do
                nested_test("()") do
                    @test get_result(daf, ". version || 1.0") == 1.0
                end

                nested_test("string") do
                    @test get_result(daf, ". version || 1.0 String") == "1.0"
                    @test get_result(daf, ". version || foo") == "foo"
                end

                nested_test("float") do
                    @test get_result(daf, ". version || 1.0 Float32") == 1.0
                end

                nested_test("const") do
                    nested_test("pi") do
                        @test get_result(daf, ". version || pi") == Float64(pi)
                    end

                    nested_test("e") do
                        @test get_result(daf, ". version || e") == Float64(e)
                    end

                    nested_test("true") do
                        @test get_result(daf, ". version || true") == true
                    end

                    nested_test("false") do
                        @test get_result(daf, ". version || false") == false
                    end
                end

                nested_test("!int") do
                    @test_throws chomp("""
                                       invalid value: "1.0"
                                       value must be: a valid Int32
                                       for the parameter: value
                                       for the operation: ||
                                       in: . version || 1.0 Int32
                                       at:              ▲▲▲
                                       """) daf[". version || 1.0 Int32"]
                end
            end
        end

        nested_test("vector") do
            add_axis!(daf, "cell", ["X", "Y"])

            nested_test("()") do
                set_vector!(daf, "cell", "type", ["U", "V"])
                @test get_result(daf, ": type @ cell = X") == "U"
            end

            nested_test("missing") do
                @test get_result(daf, ": age || 1 @ cell = X") == 1
            end

            nested_test("reduction") do
                nested_test("()") do
                    set_vector!(daf, "cell", "age", [1, 2])
                    @test get_result(daf, "@ cell : age >> Sum") == 3
                end

                nested_test("empty") do
                    set_vector!(daf, "cell", "age", [1, 2])
                    @test get_result(daf, "@ cell [ age < 0 ] : age >> Sum || 0") == 0
                end

                nested_test("!empty") do
                    set_vector!(daf, "cell", "age", [1, 2])
                    @test_throws chomp("""
                                       no IfMissing value specified for reducing an empty vector
                                       in the query: @ cell [ age < 0 ] : age >> Sum
                                       at location:                           ▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ cell [ age < 0 ] : age >> Sum"]
                end

                nested_test("!string") do
                    set_vector!(daf, "cell", "donor", ["A", "B"])
                    @test_throws chomp("""
                                       unsupported input type: String
                                       for the reduction operation: Sum
                                       in the query: @ cell : donor >> Sum
                                       at location:                 ▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ cell : donor >> Sum"]
                end
            end
        end

        nested_test("matrix") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B"])

            nested_test("()") do
                set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3])
                @test get_result(daf, ":: UMIs @ cell = Y @ gene = B") == 3
            end

            nested_test("missing") do
                @test get_result(daf, ":: UMIs || 0 @ cell = Y @ gene = B") == 0
            end

            nested_test("reduction") do
                nested_test("()") do
                    set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3])
                    @test get_result(daf, "@ cell @ gene :: UMIs >> Sum") == 6
                end

                nested_test("empty") do
                    set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3])
                    set_vector!(daf, "cell", "age", [1, 2])
                    @test get_result(daf, "@ cell [ age < 0 ] @ gene :: UMIs >> Sum || 0") == 0
                end

                nested_test("!empty") do
                    set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3])
                    set_vector!(daf, "cell", "age", [1, 2])
                    @test_throws chomp("""
                                       no IfMissing value specified for reducing an empty matrix
                                       in the query: @ cell [ age < 0 ] @ gene :: UMIs >> Sum
                                       at location:                                    ▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ cell [ age < 0 ] @ gene :: UMIs >> Sum"]
                end

                nested_test("!string") do
                    set_matrix!(daf, "cell", "gene", "kind", ["A" "B"; "C" "D"])
                    @test_throws chomp("""
                                       unsupported input type: String
                                       for the reduction operation: Sum
                                       in the query: @ cell @ gene :: kind >> Sum
                                       at location:                        ▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ cell @gene :: kind >> Sum"]
                end
            end
        end

        nested_test("eltwise") do
            nested_test("()") do
                set_scalar!(daf, "score", -0.5)
                @test get_result(daf, ". score % Abs") == 0.5
            end

            nested_test("!string") do
                set_scalar!(daf, "version", "0.1.2")
                @test_throws chomp("""
                                   unsupported input type: String
                                   for the eltwise operation: Abs
                                   in the query: . version % Abs
                                   at location:            ▲▲▲▲▲
                                   """) daf[". version % Abs"]
            end
        end
    end

    nested_test("vector") do
        nested_test("axis") do
            add_axis!(daf, "gene", ["A", "B"])
            @test get_result(daf, "@ gene"; axis_name = "gene") == ("gene", ["A" => "A", "B" => "B"])
        end

        nested_test("mask") do
            nested_test("()") do
                add_axis!(daf, "gene", ["A", "B"])
                set_vector!(daf, "gene", "is_lateral", [true, false])
                @test get_result(daf, "@ gene [ is_lateral ]"; axis_name = "gene") == ("gene", ["A" => "A"])
            end

            nested_test("negated") do
                add_axis!(daf, "gene", ["A", "B"])
                set_vector!(daf, "gene", "is_lateral", [true, false])
                @test get_result(daf, "@ gene [ ! is_lateral ]"; axis_name = "gene") == ("gene", ["B" => "B"])
            end

            nested_test("matrix") do
                add_axis!(daf, "cell", ["X", "Y"])
                add_axis!(daf, "gene", ["A", "B"])
                set_matrix!(daf, "gene", "cell", "UMIs", [1 0; 0 1])
                @test get_result(daf, "@ cell [ UMIs @ gene = A > 0 ]"; axis_name = "cell") == ("cell", ["X" => "X"])
                @test get_result(daf, "@ cell [ ! UMIs @ gene = A > 0 ]"; axis_name = "cell") == ("cell", ["Y" => "Y"])
            end

            nested_test("square") do
                add_axis!(daf, "cell", ["X", "Y"])
                set_matrix!(daf, "cell", "cell", "distance", [0 1; 1 0])

                nested_test("column") do
                    daf["@ cell [ distance @| X ]"]
                    @test get_result(daf, "@ cell [ distance @| X ]"; axis_name = "cell") == ("cell", ["Y" => "Y"])
                    @test get_result(daf, "@ cell [ ! distance @| X ]"; axis_name = "cell") == ("cell", ["X" => "X"])
                    @test get_result(daf, "@ cell [ distance @| Y ]"; axis_name = "cell") == ("cell", ["X" => "X"])
                    @test get_result(daf, "@ cell [ ! distance @| Y ]"; axis_name = "cell") == ("cell", ["Y" => "Y"])
                end

                nested_test("row") do
                    @test get_result(daf, "@ cell [ distance @- X ]"; axis_name = "cell") == ("cell", ["Y" => "Y"])
                    @test get_result(daf, "@ cell [ ! distance @- X ]"; axis_name = "cell") == ("cell", ["X" => "X"])
                    @test get_result(daf, "@ cell [ distance @- Y ]"; axis_name = "cell") == ("cell", ["X" => "X"])
                    @test get_result(daf, "@ cell [ ! distance @- Y ]"; axis_name = "cell") == ("cell", ["Y" => "Y"])
                end
            end

            nested_test("operation") do
                add_axis!(daf, "cell", ["LE", "LO", "HE", "HO"])
                add_axis!(daf, "gene", ["A", "B"])
                set_vector!(daf, "cell", "is_low", [true, true, false, false])
                set_vector!(daf, "cell", "is_even", [true, false, true, false])

                nested_test("and") do
                    nested_test("()") do
                        @test get_result(daf, "@ cell [ is_low & is_even ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE"])
                        @test get_result(daf, "@ cell [ ! is_low & is_even ]"; axis_name = "cell") ==
                              ("cell", ["HE" => "HE"])
                        @test get_result(daf, "@ cell [ is_low & ! is_even ]"; axis_name = "cell") ==
                              ("cell", ["LO" => "LO"])
                        @test get_result(daf, "@ cell [ ! is_low & ! is_even ]"; axis_name = "cell") ==
                              ("cell", ["HO" => "HO"])
                    end
                end

                nested_test("or") do
                    nested_test("()") do
                        @test get_result(daf, "@ cell [ is_low | is_even ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE", "LO" => "LO", "HE" => "HE"])
                        @test get_result(daf, "@ cell [ ! is_low | is_even ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE", "HE" => "HE", "HO" => "HO"])
                        @test get_result(daf, "@ cell [ is_low | ! is_even ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE", "LO" => "LO", "HO" => "HO"])
                        @test get_result(daf, "@ cell [ ! is_low | ! is_even ]"; axis_name = "cell") ==
                              ("cell", ["LO" => "LO", "HE" => "HE", "HO" => "HO"])
                    end
                end

                nested_test("xor") do
                    nested_test("()") do
                        @test get_result(daf, "@ cell [ is_low ^ is_even ]"; axis_name = "cell") ==
                              ("cell", ["LO" => "LO", "HE" => "HE"])
                        @test get_result(daf, "@ cell [ ! is_low ^ is_even ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE", "HO" => "HO"])
                        @test get_result(daf, "@ cell [ is_low ^ ! is_even ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE", "HO" => "HO"])
                        @test get_result(daf, "@ cell [ ! is_low ^ ! is_even ]"; axis_name = "cell") ==
                              ("cell", ["LO" => "LO", "HE" => "HE"])
                    end
                end

                nested_test("column") do
                    set_matrix!(daf, "gene", "cell", "UMIs", [1 1 0 0; 1 0 1 0])
                    @test get_result(daf, "@ cell [ is_low & UMIs @ gene = B ]"; axis_name = "cell") ==
                          ("cell", ["LE" => "LE"])
                end

                nested_test("square") do
                    set_matrix!(daf, "cell", "cell", "distance", [0 1 1 1; 0 0 1 1; 0 0 0 1; 0 0 0 0])

                    nested_test("column") do
                        @test get_result(daf, "@ cell [ is_low & distance @| LO ]"; axis_name = "cell") ==
                              ("cell", ["LE" => "LE"])
                    end

                    nested_test("row") do
                        @test get_result(daf, "@ cell [ ! is_low & distance @- LO ]"; axis_name = "cell") ==
                              ("cell", ["HE" => "HE", "HO" => "HO"])
                    end
                end
            end
        end

        nested_test("lookup") do
            add_axis!(daf, "type", ["U", "V"])
            set_vector!(daf, "type", "color", ["red", "green"])

            nested_test("()") do
                @test get_result(daf, "@ type : color") == ("type", ["U" => "red", "V" => "green"])
            end

            nested_test("as_axis") do
                add_axis!(daf, "cell", ["X", "Y"])

                nested_test("implicit") do
                    set_vector!(daf, "cell", "type", ["U", "V"])
                    set_vector!(daf, "cell", "type.manual", ["V", "U"])
                    @test get_result(daf, "@ cell : type : color") == ("cell", ["X" => "red", "Y" => "green"])
                    @test get_result(daf, "@ cell : type.manual : color") == ("cell", ["X" => "green", "Y" => "red"])
                end

                nested_test("explicit") do
                    set_vector!(daf, "cell", "type", ["U", "V"])
                    set_vector!(daf, "cell", "type.manual", ["V", "U"])
                    @test get_result(daf, "@ cell : type =@ : color") == ("cell", ["X" => "red", "Y" => "green"])
                    @test get_result(daf, "@ cell : type.manual =@ : color") == ("cell", ["X" => "green", "Y" => "red"])
                end

                nested_test("named") do
                    set_vector!(daf, "cell", "manual", ["V", "U"])
                    @test get_result(daf, "@ cell : manual =@ type : color") == ("cell", ["X" => "green", "Y" => "red"])
                end
            end

            nested_test("missing") do
                @test get_result(daf, "@ type : phase || 1") == ("type", ["U" => 1, "V" => 1])
            end

            nested_test("if_not") do
                add_axis!(daf, "cell", ["X", "Y"])
                set_vector!(daf, "cell", "type", ["U", ""])

                nested_test("mask") do
                    @test get_result(daf, "@ cell : type ?? : color") == ("cell", ["X" => "red"])
                end

                nested_test("value") do
                    @test get_result(daf, "@ cell : type ?? blue : color") == ("cell", ["X" => "red", "Y" => "blue"])
                end

                nested_test("missing") do
                    @test get_result(daf, "@ cell : type ?? 0 : phase || 1") == ("cell", ["X" => 1, "Y" => 0])
                end

                nested_test("!missing") do
                    set_vector!(daf, "type", "phase", [0, 1])
                    @test_throws chomp("""
                                       error parsing final value: foo
                                       as type: Int64
                                       ArgumentError("invalid base 10 digit 'f' in \\"foo\\"")
                                       in the query: @ cell : type ?? foo : phase
                                       at location:                ▲▲▲▲▲▲▲▲▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ cell : type ?? foo : phase"]
                end
            end
        end

        nested_test("matrix") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B", "C"])
            set_matrix!(daf, "cell", "gene", "UMIs", [0 1 2; 3 4 5])

            nested_test("column") do
                @test get_result(daf, "@ cell :: UMIs @ gene = A") == ("cell", ["X" => 0, "Y" => 3])
                @test get_result(daf, "@ gene :: UMIs @ cell = X") == ("gene", ["A" => 0, "B" => 1, "C" => 2])
            end

            nested_test("reduction") do
                nested_test("column") do
                    nested_test("()") do
                        @test get_result(daf, "@ cell @ gene :: UMIs >| Sum") == ("cell", ["X" => 3, "Y" => 12])
                    end

                    nested_test("empty") do
                        @test get_result(daf, "@ cell [ name = Q ] @ gene :: UMIs >| Sum || 0") ==
                              ("cell", Pair{String, Int64}[])
                        @test get_result(daf, "@ cell @ gene [ name = Q ] :: UMIs >| Sum || 0") ==
                              ("cell", ["X" => 0, "Y" => 0])
                    end

                    nested_test("!empty") do
                        @test_throws chomp("""
                                           no IfMissing value specified for reducing an empty matrix
                                           in the query: @ cell [ name = Q ] @ gene :: UMIs >| Sum
                                           at location:                                     ▲▲▲▲▲▲
                                           """) daf["@ cell [ name = Q ] @ gene :: UMIs >| Sum"]
                        @test_throws chomp("""
                                           no IfMissing value specified for reducing an empty matrix
                                           in the query: @ cell @ gene [ name = Q ] :: UMIs >| Sum
                                           at location:                                     ▲▲▲▲▲▲
                                           """) daf["@ cell @ gene [ name = Q ] :: UMIs >| Sum"]
                    end

                    nested_test("!string") do
                        set_matrix!(daf, "cell", "gene", "kind", ["A" "B" "A"; "B" "A" "B"])
                        @test_throws chomp("""
                                           unsupported input type: String
                                           for the reduction operation: Sum
                                           in the query: @ cell @ gene :: kind >| Sum
                                           at location:                        ▲▲▲▲▲▲
                                           """) daf["@ cell @ gene :: kind >| Sum"]
                    end
                end

                nested_test("row") do
                    nested_test("()") do
                        @test get_result(daf, "@ cell @ gene :: UMIs >- Sum") ==
                              ("gene", ["A" => 3, "B" => 5, "C" => 7])
                    end

                    nested_test("empty") do
                        @test get_result(daf, "@ cell [ name = Q ] @ gene :: UMIs >- Sum || 0") ==
                              ("gene", ["A" => 0, "B" => 0, "C" => 0])
                        @test get_result(daf, "@ cell @ gene [ name = Q ] :: UMIs >- Sum || 0") ==
                              ("gene", Pair{String, Int64}[])
                    end

                    nested_test("!empty") do
                        @test_throws chomp("""
                                           no IfMissing value specified for reducing an empty matrix
                                           in the query: @ cell [ name = Q ] @ gene :: UMIs >- Sum
                                           at location:                                     ▲▲▲▲▲▲
                                           """) daf["@ cell [ name = Q ] @ gene :: UMIs >- Sum"]
                        @test_throws chomp("""
                                           no IfMissing value specified for reducing an empty matrix
                                           in the query: @ cell @ gene [ name = Q ] :: UMIs >- Sum
                                           at location:                                     ▲▲▲▲▲▲
                                           """) daf["@ cell @ gene [ name = Q ] :: UMIs >- Sum"]
                    end

                    nested_test("!string") do
                        set_matrix!(daf, "cell", "gene", "kind", ["A" "B" "A"; "B" "A" "B"])
                        @test_throws chomp("""
                                           unsupported input type: String
                                           for the reduction operation: Sum
                                           in the query: @ cell @ gene :: kind >- Sum
                                           at location:                        ▲▲▲▲▲▲
                                           """) daf["@ cell @ gene :: kind >- Sum"]
                    end
                end
            end
        end

        nested_test("square") do
            add_axis!(daf, "cell", ["X", "Y"])
            set_matrix!(daf, "cell", "cell", "distance", [0 1; -1 0])

            nested_test("column") do
                @test get_result(daf, "@ cell :: distance @| X") == ("cell", ["X" => 0, "Y" => -1])
            end

            nested_test("row") do
                @test get_result(daf, "@ cell :: distance @- X") == ("cell", ["X" => 0, "Y" => 1])
            end
        end

        nested_test("eltwise") do
            add_axis!(daf, "cell", ["X", "Y"])

            nested_test("()") do
                set_vector!(daf, "cell", "score", [-0.25, 0.5])
                @test get_result(daf, "@cell : score % Abs") == ("cell", ["X" => 0.25, "Y" => 0.5])
            end

            nested_test("!string") do
                set_vector!(daf, "cell", "type", ["U", "V"])
                @test_throws chomp("""
                                   unsupported input type: String
                                   for the eltwise operation: Abs
                                   in the query: @ cell : type % Abs
                                   at location:                ▲▲▲▲▲
                                   """) daf["@ cell : type % Abs"]
            end
        end

        nested_test("compare") do
            add_axis!(daf, "cell", ["X", "Y"])
            set_vector!(daf, "cell", "type", ["U", "V"])
            set_vector!(daf, "cell", "score", [0.5, 1.5])

            nested_test("!string") do
                @test_throws chomp("""
                                   unsupported vector element type: Float64
                                   for the comparison operation: IsMatch
                                   in the query: @ cell : score ~ \\[UV\\]
                                   at location:                 ▲▲▲▲▲▲▲▲
                                   for the daf data: memory!
                                   """) daf[q"@ cell : score ~ \[UV\]"]
            end

            nested_test("!regex") do
                @test_throws chomp(
                    """
                    invalid regular expression: [UV
                    for the comparison operation: IsMatch
                    ErrorException("PCRE compilation error: missing terminating ] for character class at offset 3")
                    in the query: @ cell : type ~ \\[UV
                    at location:                ▲▲▲▲▲▲
                    for the daf data: memory!
                    """,
                ) daf[q"@ cell : type ~ \[UV"]
            end

            nested_test("!number") do
                @test_throws chomp("""
                                   error parsing number comparison value: U
                                   for comparison with a vector of type: Float64
                                   ArgumentError("cannot parse \\"U\\" as Float64")
                                   in the query: @ cell : score = U
                                   at location:                 ▲▲▲
                                   for the daf data: memory!
                                   """) daf["@ cell : score = U"]
            end

            nested_test("<") do
                @test get_result(daf, "@ cell [ type < V ] : type") == ("cell", ["X" => "U"])
                @test get_result(daf, "@ cell [ score < 1.0 ] : score") == ("cell", ["X" => 0.5])
            end

            nested_test("<=") do
                @test get_result(daf, "@ cell [ type <= U ] : type") == ("cell", ["X" => "U"])
                @test get_result(daf, "@ cell [ score <= 0.5 ] : score") == ("cell", ["X" => 0.5])
            end

            nested_test("=") do
                @test get_result(daf, "@ cell [ type = U ] : type") == ("cell", ["X" => "U"])
                @test get_result(daf, "@ cell [ score = 0.5 ] : score") == ("cell", ["X" => 0.5])
            end

            nested_test("!=") do
                @test get_result(daf, "@ cell [ type != V ] : type") == ("cell", ["X" => "U"])
                @test get_result(daf, "@ cell [ score != 1.5 ] : score") == ("cell", ["X" => 0.5])
            end

            nested_test(">=") do
                @test get_result(daf, "@ cell [ type >= V ] : type") == ("cell", ["Y" => "V"])
                @test get_result(daf, "@ cell [ score >= 1.5 ] : score") == ("cell", ["Y" => 1.5])
            end

            nested_test(">") do
                @test get_result(daf, "@ cell [ type > U ] : type") == ("cell", ["Y" => "V"])
                @test get_result(daf, "@ cell [ score > 1.0 ] : score") == ("cell", ["Y" => 1.5])
            end

            nested_test("~") do
                @test get_result(daf, q"@ cell [ type ~ \^\[A-U\] ] : type") == ("cell", ["X" => "U"])
            end

            nested_test("!~") do
                @test get_result(daf, q"@ cell [ type !~ \^\[A-U\] ] : type") == ("cell", ["Y" => "V"])
            end
        end

        nested_test("group") do
            nested_test("vector") do
                add_axis!(daf, "cell", ["A", "B", "C", "D"])
                add_axis!(daf, "gene", ["X", "Y"])
                set_vector!(daf, "cell", "type", ["U", "U", "V", "V"])
                set_vector!(daf, "cell", "score", [0.0, 1.0, 2.0, 3.0])
                add_axis!(daf, "type", ["U", "V", "W"])

                nested_test("()") do
                    @test get_result(daf, "@ cell : score / type >> Sum") == (:A, ["U" => 1.0, "V" => 5.0])
                end

                nested_test("!string") do
                    @test_throws chomp("""
                                       unsupported input type: String
                                       for the reduction operation: Sum
                                       in the query: @ cell : type / score >> Sum
                                       at location:                        ▲▲▲▲▲▲
                                       for the daf data: memory
                                       """) daf["@ cell : type / score >> Sum"]
                end

                nested_test("matrix") do
                    set_matrix!(
                        daf,
                        "gene",
                        "cell",
                        "level",
                        ["low" "middle" "middle" "high"; "middle" "high" "low" "high"],
                    )
                    @test get_result(daf, "@ cell : score / level @ gene = X >> Sum") ==
                          (:A, ["high" => 3.0, "low" => 0.0, "middle" => 3.0])
                end

                nested_test("square") do
                    set_matrix!(daf, "cell", "cell", "distance", [0 1 1 1; 0 0 1 1; 0 0 0 1; 0 0 0 0])

                    nested_test("column") do
                        @test get_result(daf, "@ cell : score / distance @| C >> Sum") == (:A, ["0" => 5.0, "1" => 1.0])
                    end

                    nested_test("row") do
                        @test get_result(daf, "@ cell : score / distance @- A >> Sum") == (:A, ["0" => 0.0, "1" => 6.0])
                    end
                end

                nested_test("as_axis") do
                    @test get_result(daf, "@ cell : score / type =@ >> Sum || 0.0") ==
                          ("type", ["U" => 1.0, "V" => 5.0, "W" => 0.0])
                end

                nested_test("missing") do
                    @test_throws chomp("""
                                       no IfMissing value specified for the unused entry: W
                                       of the axis: type
                                       in the query: @ cell : score / type =@ >> Sum
                                       at location:                           ▲▲▲▲▲▲
                                       """) daf["@ cell : score / type =@ >> Sum"]
                end
            end
        end
    end

    nested_test("matrix") do
        nested_test("lookup") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B", "C"])

            nested_test("()") do
                set_matrix!(daf, "cell", "gene", "UMIs", [0 1 2; 3 4 5])
                @test get_result(daf, "@ cell @ gene :: UMIs") == (
                    ("cell", "gene"),
                    [
                        ("X", "A") => 0,
                        ("X", "B") => 1,
                        ("X", "C") => 2,
                        ("Y", "A") => 3,
                        ("Y", "B") => 4,
                        ("Y", "C") => 5,
                    ],
                )
            end

            nested_test("vector") do
                add_axis!(daf, "tag", ["U", "V", "W"])
                set_vector!(daf, "tag", "color", ["red", "green", "blue"])

                nested_test("()") do
                    set_matrix!(daf, "cell", "gene", "tag", ["U" "V" "W"; "W" "V" "U"])
                    @test get_result(daf, "@ cell @ gene :: tag : color") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "red",
                            ("X", "B") => "green",
                            ("X", "C") => "blue",
                            ("Y", "A") => "blue",
                            ("Y", "B") => "green",
                            ("Y", "C") => "red",
                        ],
                    )
                end

                nested_test("as_axis") do
                    set_matrix!(daf, "cell", "gene", "tig", ["U" "V" "W"; "W" "V" "U"])
                    @test get_result(daf, "@ cell @ gene :: tig =@ tag : color") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "red",
                            ("X", "B") => "green",
                            ("X", "C") => "blue",
                            ("Y", "A") => "blue",
                            ("Y", "B") => "green",
                            ("Y", "C") => "red",
                        ],
                    )
                end

                nested_test("if_not") do
                    set_matrix!(daf, "cell", "gene", "tag", ["" "V" "W"; "" "V" "U"])
                    @test get_result(daf, "@ cell @ gene :: tag ?? purple : color") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "purple",
                            ("X", "B") => "green",
                            ("X", "C") => "blue",
                            ("Y", "A") => "purple",
                            ("Y", "B") => "green",
                            ("Y", "C") => "red",
                        ],
                    )
                end
            end

            nested_test("matrix") do
                add_axis!(daf, "tag", ["U", "V", "W"])
                add_axis!(daf, "kind", ["K", "L"])
                set_matrix!(daf, "kind", "tag", "color", ["red" "green" "blue"; "cyan" "magenta" "yellow"])

                nested_test("()") do
                    set_matrix!(daf, "cell", "gene", "tag", ["U" "V" "W"; "W" "V" "U"])
                    @test get_result(daf, "@ cell @ gene :: tag :: color @ kind = K") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "red",
                            ("X", "B") => "green",
                            ("X", "C") => "blue",
                            ("Y", "A") => "blue",
                            ("Y", "B") => "green",
                            ("Y", "C") => "red",
                        ],
                    )
                end

                nested_test("as_axis") do
                    set_matrix!(daf, "cell", "gene", "tig", ["U" "V" "W"; "W" "V" "U"])
                    @test get_result(daf, "@ cell @ gene :: tig =@ tag :: color @ kind = K") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "red",
                            ("X", "B") => "green",
                            ("X", "C") => "blue",
                            ("Y", "A") => "blue",
                            ("Y", "B") => "green",
                            ("Y", "C") => "red",
                        ],
                    )
                end
            end

            nested_test("square") do
                add_axis!(daf, "tag", ["U", "V", "W"])
                set_matrix!(
                    daf,
                    "tag",
                    "tag",
                    "color",
                    ["red" "green" "blue"; "cyan" "magenta" "yellow"; "black" "white" "gray"],
                )
                set_matrix!(daf, "cell", "gene", "tag", ["U" "V" "W"; "W" "V" "U"])

                nested_test("column") do
                    @test get_result(daf, "@ cell @ gene :: tag :: color @| U") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "red",
                            ("X", "B") => "cyan",
                            ("X", "C") => "black",
                            ("Y", "A") => "black",
                            ("Y", "B") => "cyan",
                            ("Y", "C") => "red",
                        ],
                    )
                end

                nested_test("row") do
                    @test get_result(daf, "@ cell @ gene :: tag :: color @- U") == (
                        ("cell", "gene"),
                        [
                            ("X", "A") => "red",
                            ("X", "B") => "green",
                            ("X", "C") => "blue",
                            ("Y", "A") => "blue",
                            ("Y", "B") => "green",
                            ("Y", "C") => "red",
                        ],
                    )
                end
            end
        end

        nested_test("eltwise") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B", "C"])

            nested_test("()") do
                set_matrix!(daf, "cell", "gene", "score", [0 -1 2; 3 -4 5])
                @test get_result(daf, "@ cell @ gene :: score % Abs") == (
                    ("cell", "gene"),
                    [
                        ("X", "A") => 0,
                        ("X", "B") => 1,
                        ("X", "C") => 2,
                        ("Y", "A") => 3,
                        ("Y", "B") => 4,
                        ("Y", "C") => 5,
                    ],
                )
            end

            nested_test("!string") do
                set_matrix!(daf, "cell", "gene", "level", ["low" "middle" "high"; "high" "middle" "low"])
                @test_throws chomp("""
                                   unsupported input type: String
                                   for the eltwise operation: Abs
                                   in the query: @ cell @ gene :: level % Abs
                                   at location:                         ▲▲▲▲▲
                                   """) daf["@ cell @gene :: level % Abs"]
            end
        end

        nested_test("count") do
            add_axis!(daf, "cell", ["X", "Y"])
            add_axis!(daf, "gene", ["A", "B", "C"])
            set_vector!(daf, "gene", "width", [1, 2, 1])

            nested_test("vector") do
                add_axis!(daf, "type", ["U", "V", "W"])
                set_vector!(daf, "gene", "type", ["U", "U", "V"])
                @test get_result(daf, "@ gene : width * type =@") == (
                    (:A, "type"),
                    [
                        ("1", "U") => 1,
                        ("1", "V") => 1,
                        ("1", "W") => 0,
                        ("2", "U") => 1,
                        ("2", "V") => 0,
                        ("2", "W") => 0,
                    ],
                )
            end

            nested_test("column") do
                set_matrix!(daf, "cell", "gene", "level", ["low" "middle" "high"; "high" "middle" "low"])
                @test get_result(daf, "@ gene : width * level @ cell = X") == (
                    (:A, :B),
                    [
                        ("1", "high") => 1,
                        ("1", "low") => 1,
                        ("1", "middle") => 0,
                        ("2", "high") => 0,
                        ("2", "low") => 0,
                        ("2", "middle") => 1,
                    ],
                )
            end

            nested_test("square") do
                set_matrix!(daf, "gene", "gene", "distance", [0 1 1; 1 1 0; 1 0 1])

                nested_test("column") do
                    @test get_result(daf, "@ gene : width * distance @| C") ==
                          ((:A, :B), [("1", "0") => 0, ("1", "1") => 2, ("2", "0") => 1, ("2", "1") => 0])
                end

                nested_test("row") do
                    @test get_result(daf, "@ gene : width * distance @- A") ==
                          ((:A, :B), [("1", "0") => 1, ("1", "1") => 1, ("2", "0") => 0, ("2", "1") => 1])
                end
            end
        end

        nested_test("group") do
            add_axis!(daf, "cell", ["A", "B", "C", "D"])
            add_axis!(daf, "gene", ["X", "Y"])
            add_axis!(daf, "type", ["U", "V", "W"])
            set_vector!(daf, "cell", "type", ["U", "U", "V", "V"])
            set_matrix!(daf, "gene", "cell", "UMIs", [0 1 2 3; 4 5 6 7])
            set_matrix!(daf, "gene", "cell", "kind", ["A" "B" "C" "A"; "C" "A" "B" "C"])
            set_matrix!(daf, "cell", "cell", "distance", [0 1 1 1; 0 0 1 1; 0 0 0 1; 0 0 0 0])

            nested_test("column") do
                nested_test("()") do
                    @test get_result(daf, "@ gene @ cell :: UMIs |/ type >| Sum") ==
                          (("gene", :B), [("X", "U") => 1, ("X", "V") => 5, ("Y", "U") => 9, ("Y", "V") => 13])
                end

                nested_test("()") do
                    @test get_result(daf, "@ gene @ cell :: UMIs |/ type >| Sum") ==
                          (("gene", :B), [("X", "U") => 1, ("X", "V") => 5, ("Y", "U") => 9, ("Y", "V") => 13])
                end

                nested_test("slice") do
                    @test get_result(daf, "@ gene @ cell :: UMIs |/ kind @ gene = X >| Sum") == (
                        ("gene", :B),
                        [
                            ("X", "A") => 3,
                            ("X", "B") => 1,
                            ("X", "C") => 2,
                            ("Y", "A") => 11,
                            ("Y", "B") => 5,
                            ("Y", "C") => 6,
                        ],
                    )
                end

                nested_test("square") do
                    nested_test("column") do
                        @test get_result(daf, "@ gene @ cell :: UMIs |/ distance @| B >| Sum") ==
                              (("gene", :B), [("X", "0") => 6, ("X", "1") => 0, ("Y", "0") => 18, ("Y", "1") => 4])
                    end

                    nested_test("row") do
                        @test get_result(daf, "@ gene @ cell :: UMIs |/ distance @- B >| Sum") ==
                              (("gene", :B), [("X", "0") => 1, ("X", "1") => 5, ("Y", "0") => 9, ("Y", "1") => 13])
                    end
                end

                nested_test("!string") do
                    @test_throws chomp("""
                                       unsupported input type: String
                                       for the reduction operation: Sum
                                       in the query: @ gene @ cell :: kind |/ type >| Sum
                                       at location:                                ▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ gene @ cell :: kind |/ type >| Sum"]
                end

                nested_test("as_axis") do
                    nested_test("()") do
                        @test get_result(daf, "@ gene @ cell :: UMIs |/ type =@ >| Sum || 0") == (
                            ("gene", "type"),
                            [
                                ("X", "U") => 1,
                                ("X", "V") => 5,
                                ("X", "W") => 0,
                                ("Y", "U") => 9,
                                ("Y", "V") => 13,
                                ("Y", "W") => 0,
                            ],
                        )
                    end

                    nested_test("~missing") do
                        @test_throws chomp("""
                                           error converting: Float64
                                           missing value: 0.5
                                           to type: Int64
                                           InexactError(:Int64, (Int64, 0.5))
                                           in the query: @ gene @ cell :: UMIs |/ type =@ >| Sum || 0.5
                                           at location:                                   ▲▲▲▲▲▲▲▲▲▲▲▲▲
                                           for the daf data: memory!
                                           """) daf["@ gene @ cell :: UMIs |/ type =@ >| Sum || 0.5"]
                    end
                end

                nested_test("missing") do
                    @test_throws chomp("""
                                       no IfMissing value specified for the unused entry: W
                                       of the axis: type
                                       in the query: @ gene @ cell :: UMIs |/ type =@ >| Sum
                                       at location:                                   ▲▲▲▲▲▲
                                       """) daf["@ gene @ cell :: UMIs |/ type =@ >| Sum"]
                end
            end

            nested_test("row") do
                nested_test("()") do
                    @test get_result(daf, "@ cell @ gene :: UMIs -/ type >- Sum") ==
                          ((:A, "gene"), [("U", "X") => 1, ("U", "Y") => 9, ("V", "X") => 5, ("V", "Y") => 13])
                end

                nested_test("slice") do
                    @test get_result(daf, "@ cell @ gene :: UMIs -/ kind @ gene = X >- Sum") == (
                        (:A, "gene"),
                        [
                            ("A", "X") => 3,
                            ("A", "Y") => 11,
                            ("B", "X") => 1,
                            ("B", "Y") => 5,
                            ("C", "X") => 2,
                            ("C", "Y") => 6,
                        ],
                    )
                end

                nested_test("square") do
                    nested_test("column") do
                        @test get_result(daf, "@ cell @ gene :: UMIs -/ distance @| B >- Sum") ==
                              ((:A, "gene"), [("0", "X") => 6, ("0", "Y") => 18, ("1", "X") => 0, ("1", "Y") => 4])
                    end

                    nested_test("row") do
                        @test get_result(daf, "@ cell @ gene :: UMIs -/ distance @- B >- Sum") ==
                              ((:A, "gene"), [("0", "X") => 1, ("0", "Y") => 9, ("1", "X") => 5, ("1", "Y") => 13])
                    end
                end

                nested_test("as_axis") do
                    nested_test("()") do
                        @test get_result(daf, "@ cell @ gene :: UMIs -/ type =@ >- Sum || 0") == (
                            ("type", "gene"),
                            [
                                ("U", "X") => 1,
                                ("U", "Y") => 9,
                                ("V", "X") => 5,
                                ("V", "Y") => 13,
                                ("W", "X") => 0,
                                ("W", "Y") => 0,
                            ],
                        )
                    end

                    nested_test("~missing") do
                        @test_throws chomp("""
                                           error converting: Float64
                                           missing value: 0.5
                                           to type: Int64
                                           InexactError(:Int64, (Int64, 0.5))
                                           in the query: @ cell @ gene :: UMIs -/ type =@ >- Sum || 0.5
                                           at location:                                   ▲▲▲▲▲▲▲▲▲▲▲▲▲
                                           for the daf data: memory!
                                           """) daf["@ cell @ gene :: UMIs -/ type =@ >- Sum || 0.5"]
                    end
                end

                nested_test("!string") do
                    @test_throws chomp("""
                                       unsupported input type: String
                                       for the reduction operation: Sum
                                       in the query: @ cell @ gene :: kind -/ type >- Sum
                                       at location:                                ▲▲▲▲▲▲
                                       for the daf data: memory!
                                       """) daf["@ cell @ gene :: kind -/ type >- Sum"]
                end

                nested_test("missing") do
                    @test_throws chomp("""
                                       no IfMissing value specified for the unused entry: W
                                       of the axis: type
                                       in the query: @ cell @ gene :: UMIs -/ type =@ >- Sum
                                       at location:                                   ▲▲▲▲▲▲
                                       """) daf["@ cell @ gene :: UMIs -/ type =@ >- Sum"]
                end
            end
        end
    end

    nested_test("dataframes") do
        nested_test("simple") do
            add_axis!(daf, "cell", ["A", "B"])
            set_vector!(daf, "cell", "is_doublet", [true, false])
            set_vector!(daf, "cell", "age", [0, 1])

            nested_test("axis") do
                nested_test("name") do
                    @test "$(get_frame(daf, "cell"))" == chomp("""
                        2×3 DataFrame
                         Row │ name    age    is_doublet
                             │ String  Int64  Bool
                        ─────┼───────────────────────────
                           1 │ A           0        true
                           2 │ B           1       false
                        """)
                end

                nested_test("query") do
                    @test "$(get_frame(daf, Axis("cell")))" == chomp("""
                        2×3 DataFrame
                         Row │ name    age    is_doublet
                             │ String  Int64  Bool
                        ─────┼───────────────────────────
                           1 │ A           0        true
                           2 │ B           1       false
                        """)
                end

                nested_test("!query") do
                    @test_throws "invalid axis query: @ cell : age" get_frame(daf, "@ cell : age")
                end

                nested_test("mask") do
                    @test "$(get_frame(daf, q"@ cell [ is_doublet ]"))" == chomp("""
                        1×3 DataFrame
                         Row │ name    age    is_doublet
                             │ String  Int64  Bool
                        ─────┼───────────────────────────
                           1 │ A           0        true
                        """)
                end
            end

            nested_test("columns") do
                nested_test("names") do
                    @test "$(get_frame(daf, "cell", ["age", "is_doublet"]))" == chomp("""
                        2×2 DataFrame
                         Row │ age    is_doublet
                             │ Int64  Bool
                        ─────┼───────────────────
                           1 │     0        true
                           2 │     1       false
                        """)
                end

                nested_test("queries") do
                    @test "$(get_frame(daf, "cell", ["age" => ": age", "doublet" => ": is_doublet"]))" == chomp("""
                        2×2 DataFrame
                         Row │ age    doublet
                             │ Int64  Bool
                        ─────┼────────────────
                           1 │     0     true
                           2 │     1    false
                        """)
                end

                nested_test("shorthands") do
                    @test "$(get_frame(daf, "cell", FrameColumn["age", "doublet" => "is_doublet"]))" == chomp("""
                        2×2 DataFrame
                         Row │ age    doublet
                             │ Int64  Bool
                        ─────┼────────────────
                           1 │     0     true
                           2 │     1    false
                        """)
                end

                nested_test("!queries") do
                    @test_throws chomp("""
                                       invalid column query: @ cell : age >> Sum
                                       for the axis query: @ cell
                                       of the daf data: memory!
                                       """) get_frame(
                        daf,
                        "cell",
                        ["age" => ": age >> Sum", "doublet" => ": is_doublet"],
                    )
                end
            end
        end

        nested_test("complex") do
            add_axis!(daf, "cell", ["A", "B", "D", "E"])
            add_axis!(daf, "metacell", ["X", "Y"])
            set_vector!(daf, "cell", "metacell", ["X", "X", "Y", "Y"])
            set_vector!(daf, "metacell", "type", ["U", "V"])
            set_vector!(daf, "cell", "age", [0, 1, 2, 3])

            nested_test("axis") do
                @test "$(get_frame(daf, "metacell", ["mean_age" => "@ cell : age / metacell >> Mean"]))" == chomp("""
                    2×1 DataFrame
                     Row │ mean_age
                         │ Float64
                    ─────┼──────────
                       1 │      0.5
                       2 │      2.5
                    """)
            end

            nested_test("masked") do
                @test "$(get_frame(daf, "@ metacell [ type = U ]", ["mean_age" => "@ cell [ metacell : type = U ] : age / metacell >> Mean"]))" ==
                      chomp("""
                            1×1 DataFrame
                             Row │ mean_age
                                 │ Float64
                            ─────┼──────────
                               1 │      0.5
                            """)
            end

            nested_test("!masked") do
                @test_throws chomp("""
                                   invalid column query: @ cell [ metacell ] : age / metacell >> Mean
                                   for the axis query: @ metacell [ type = U ]
                                   of the daf data: memory!
                                   """) get_frame(
                    daf,
                    q"@ metacell [ type = U ]",
                    ["mean_age" => "@ cell [ metacell ] : age / metacell >> Mean"],
                )
            end
        end
    end
end
