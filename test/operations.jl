function with_type(value::StorageScalar)::Tuple{StorageScalar, Type}
    return (value, typeof(value))
end

function with_type(array::AbstractArray)::Tuple{AbstractArray, Type}
    return (array, eltype(array))
end

nested_test("operations") do
    daf = MemoryDaf(; name = "memory!")
    add_axis!(daf, "cell", ["A", "B"])
    add_axis!(daf, "gene", ["X", "Y", "Z"])

    nested_test("eltwise") do
        nested_test("abs") do
            nested_test("string") do
                @test_throws dedent("""
                    invalid value: "String"
                    value must be: a number type
                    for the parameter: dtype
                    for the operation: Abs
                    in: / cell : value % Abs dtype String
                    at:                            ▲▲▲▲▲▲
                """) daf["/ cell : value % Abs dtype String"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", -1)
                @test with_type(daf[Lookup("value") |> Abs()]) == (1, UInt64)
                @test with_type(daf[Lookup("value") |> Abs(; dtype = Int8)]) == (1, Int8)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1.0, 2.0])
                @test with_type(daf["/ cell : value % Abs"]) == ([1.0, 2.0], Float64)
                @test with_type(daf["/ cell : value % Abs dtype Int8"]) == ([1, 2], Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1 2; -3 4 5])
                @test with_type(daf["/ cell / gene : value % Abs"]) == ([0 1 2; 3 4 5], UInt64)
                @test with_type(daf["/ cell / gene : value % Abs dtype Int8"]) == ([0 1 2; 3 4 5], Int8)
            end
        end

        nested_test("round") do
            nested_test("!dtype") do
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1.3)
                @test with_type(daf[Lookup("value") |> Round()]) == (1, Int64)
                @test with_type(daf[Lookup("value") |> Round(; dtype = Int8)]) == (1, Int8)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1.7, 2.3])
                @test with_type(daf["/ cell : value % Round"]) == ([-2.0, 2.0], Int64)
                @test with_type(daf["/ cell : value % Round dtype Int8"]) == ([-2, 2], Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1.7 2.3; -3.3 4.7 5.2])
                @test with_type(daf["/ cell / gene : value % Round"]) == ([0 -2 2; -3 5 5], Int64)
                @test with_type(daf["/ cell / gene : value % Round dtype Int8"]) == ([0 -2 2; -3 5 5], Int8)
            end
        end

        nested_test("clamp") do
            nested_test("!number") do
                @test_throws dedent("""
                    invalid value: "q"
                    value must be: a valid Float64
                    for the parameter: max
                    for the operation: Clamp
                    in: : value % Clamp max q
                    at:                     ▲
                """) daf[": value % Clamp max q"]
            end

            nested_test("!max") do
                @test_throws dedent("""
                    invalid value: "0"
                    value must be: larger than min (1.0)
                    for the parameter: max
                    for the operation: Clamp
                    in: : value % Clamp min 1 max 0
                    at:                           ▲
                """) daf[": value % Clamp min 1 max 0"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1.3)
                @test with_type(daf[Lookup("value") |> Clamp(; min = 0.5, max = 1.5)]) == (1.3, Float64)
                @test with_type(daf[Lookup("value") |> Clamp(; max = 0.5)]) == (0.5, Float64)
                @test with_type(daf[Lookup("value") |> Clamp(; min = 1.5)]) == (1.5, Float64)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1.7, 2.3])
                @test with_type(daf["/ cell : value % Clamp max 0"]) == ([-1.7, 0.0], Float64)
                @test with_type(daf["/ cell : value % Clamp min 0"]) == ([0.0, 2.3], Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1.7 2.3; -3.3 4.7 5.2])
                @test with_type(daf["/ cell / gene : value % Clamp min 0 max 4"]) == ([0 0 2.3; 0 4 4], Float64)
            end
        end

        nested_test("convert") do
            nested_test("invalid") do
                @test_throws dedent("""
                    missing required parameter: dtype
                    for the eltwise operation: Convert
                    in: / cell : value % Convert
                    at:                  ▲▲▲▲▲▲▲
                """) daf["/ cell : value % Convert"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test with_type(daf[Lookup("value") |> Convert(; dtype = Int8)]) == (1, Int8)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1, 2])
                @test with_type(daf["/ cell : value % Convert dtype Int8"]) == ([-1, 2], Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1.7 2.3; -3.3 4.7 5.2])
                @test with_type(daf["/ cell / gene : value % Convert dtype Float32"]) ==
                      (Float32[0 -1.7 2.3; -3.3 4.7 5.2], Float32)
            end
        end

        nested_test("fraction") do
            nested_test("integer") do
                @test_throws dedent("""
                    invalid value: "Int32"
                    value must be: a float type
                    for the parameter: dtype
                    for the operation: Fraction
                    in: / cell : value % Fraction dtype Int32
                    at:                                 ▲▲▲▲▲
                """) with_type(daf["/ cell : value % Fraction dtype Int32"])
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test_throws "applying Fraction eltwise operation to a scalar" daf[Lookup("value") |> Fraction()]
            end

            nested_test("vector") do
                nested_test("()") do
                    set_vector!(daf, "cell", "value", [1, 3])
                    @test with_type(daf["/ cell : value % Fraction"]) == ([0.25, 0.75], Float64)
                    @test with_type(daf["/ cell : value % Fraction dtype Float32"]) == ([0.25, 0.75], Float32)
                end

                nested_test("zero") do
                    set_vector!(daf, "cell", "value", [0, 0])
                    @test with_type(daf["/ cell : value % Fraction"]) == ([0, 0], Float64)
                end
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 0 2; 1 0 6])
                @test with_type(daf["/ cell / gene : value % Fraction"]) == ([0 0 0.25; 1 0 0.75], Float64)
                @test with_type(daf["/ cell / gene : value % Fraction dtype Float32"]) ==
                      ([0 0 0.25; 1 0 0.75], Float32)
            end
        end

        nested_test("log") do
            nested_test("!positive") do
                @test_throws dedent("""
                    invalid value: "0"
                    value must be: positive
                    for the parameter: base
                    for the operation: Log
                    in: : value % Log base 0
                    at:                    ▲
                """) daf[": value % Log base 0"]
            end

            nested_test("negative") do
                @test_throws dedent("""
                    invalid value: "-1"
                    value must be: not negative
                    for the parameter: eps
                    for the operation: Log
                    in: : value % Log eps -1
                    at:                   ▲▲
                """) daf[": value % Log eps -1"]
            end

            nested_test("special") do
                set_scalar!(daf, "value", 0)
                @test isapprox(daf[": value % Log base e eps pi"], log(pi))
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test with_type(daf[Lookup("value") |> Log()]) == (0, Float64)
                @test with_type(daf[Lookup("value") |> Log(; dtype = Float32)]) == (0, Float32)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1.0, 2.0])
                @test with_type(daf["/ cell : value % Log base 2"]) == ([0.0, 1.0], Float64)
                @test with_type(daf["/ cell : value % Log base 2 eps 1 dtype Float32"]) ==
                      (Float32[1.0, log2(3)], Float32)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 1 2; 3 4 5])
                @test with_type(daf["/ cell / gene : value % Log eps 1"]) ==
                      ([log(1) log(2) log(3); log(4) log(5) log(6)], Float64)
            end
        end

        nested_test("significant") do
            nested_test("!positive") do
                @test_throws dedent("""
                    invalid value: "0"
                    value must be: positive
                    for the parameter: high
                    for the operation: Significant
                    in: / cell : value % Significant high 0
                    at:                                   ▲
                """) daf["/ cell : value % Significant high 0"]
            end

            nested_test("negative") do
                @test_throws dedent("""
                    invalid value: "-1"
                    value must be: not negative
                    for the parameter: low
                    for the operation: Significant
                    in: / cell : value % Significant high 1 low -1
                    at:                                         ▲▲
                """) daf["/ cell : value % Significant high 1 low -1"]
            end

            nested_test("!low") do
                @test_throws dedent("""
                    invalid value: "2"
                    value must be: at most high (1.0)
                    for the parameter: low
                    for the operation: Significant
                    in: / cell : value % Significant high 1 low 2
                    at:                                         ▲
                """) daf["/ cell : value % Significant high 1 low 2"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test_throws "applying Significant eltwise operation to a scalar" daf[Lookup(
                    "value",
                ) |> Significant(; high = 3.0)]
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf["/ cell : value % Significant high 3"]) == ([0, 3], Int64)
                @test with_type(daf["/ cell : value % Significant high 3 low 1"]) == ([1, 3], Int64)
            end

            nested_test("matrix") do
                nested_test("dense") do
                    set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                    @test with_type(daf["/ cell / gene : value % Significant high 3 low 2"]) ==
                          ([0.0 0.0 2.0; -3.0 0.0 6.0], Float64)
                end

                nested_test("sparse") do
                    set_matrix!(daf, "cell", "gene", "value", sparse_matrix_csc([0.0 2.0 2.0; -3.0 0.0 6.0]))
                    @test with_type(daf["/ cell / gene : value % Significant high 3 low 2"]) ==
                          ([0.0 0.0 2.0; -3.0 0.0 6.0], Float64)
                end
            end
        end
    end

    nested_test("reduction") do
        nested_test("count") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Count()]) == (2, UInt32)
                @test with_type(daf["/ cell : value %> Count dtype Int8"]) == (2, Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["/ cell / gene : value %> Count"]) == ([2, 2, 2], UInt32)
            end
        end

        nested_test("sum") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Sum()]) == (4, Int64)
                @test with_type(daf["/ cell : value %> Sum dtype Int8"]) == (4, Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["/ cell / gene : value %> Sum"]) == ([-2.0, 3.0, 8.0], Float64)
            end
        end

        nested_test("max") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Max()]) == (3, Int64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["/ cell / gene : value %> Max"]) == ([1.0, 2.0, 6.0], Float64)
            end
        end

        nested_test("min") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Min()]) == (1, Int64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["/ cell / gene : value %> Min"]) == ([-3.0, 1.0, 2.0], Float64)
            end
        end

        nested_test("median") do
            nested_test("vector") do
                set_vector!(daf, "gene", "value", [1, 3, 7])
                @test with_type(daf[Axis("gene") |> Lookup("value") |> Median()]) == (3.0, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; 1.0 -3.0 6.0])
                @test with_type(daf["/ gene / cell : value %> Median dtype Float32"]) == ([2.0, 1.0], Float32)
            end
        end

        nested_test("quantile") do
            nested_test("negative") do
                @test_throws dedent("""
                    invalid value: "-0.5"
                    value must be: at least 0
                    for the parameter: p
                    for the operation: Quantile
                    in: / gene / cell : value %> Quantile p -0.5
                    at:                                     ▲▲▲▲
                """) daf["/ gene / cell : value %> Quantile p -0.5"]
            end

            nested_test("high") do
                @test_throws dedent("""
                    invalid value: "1.5"
                    value must be: at most 1
                    for the parameter: p
                    for the operation: Quantile
                    in: / gene / cell : value %> Quantile p 1.5
                    at:                                     ▲▲▲
                """) daf["/ gene / cell : value %> Quantile p 1.5"]
            end

            nested_test("vector") do
                set_vector!(daf, "gene", "value", [1, 3, 7])
                @test with_type(daf[Axis("gene") |> Lookup("value") |> Quantile(; p = 0.0)]) == (1.0, Float64)
                @test with_type(daf[Axis("gene") |> Lookup("value") |> Quantile(; p = 0.25)]) == (2.0, Float64)
                @test with_type(daf[Axis("gene") |> Lookup("value") |> Quantile(; p = 0.5)]) == (3.0, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; 1.0 -3.0 6.0])
                @test with_type(daf["/ gene / cell : value %> Quantile p 0.5"]) == ([2.0, 1.0], Float64)
                @test with_type(daf["/ gene / cell : value %> Quantile p 1.0 dtype Float32"]) == ([2.0, 6.0], Float32)
            end
        end

        nested_test("mean") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Mean()]) == (2, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["/ cell / gene : value %> Mean dtype Float32"]) == ([-1.0, 1.5, 4.0], Float32)
            end
        end

        nested_test("var") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Var()]) == (1, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["/ cell / gene : value %> Var dtype Float32"]) == ([4.0, 0.25, 4.0], Float32)
            end
        end

        nested_test("var_n") do
            nested_test("negative") do
                @test_throws dedent("""
                    invalid value: "-1"
                    value must be: not negative
                    for the parameter: eps
                    for the operation: VarN
                    in: / cell / gene : value %> VarN eps -1
                    at:                                   ▲▲
                """) daf["/ cell / gene : value %> VarN eps -1"]
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> VarN()]) == (0.5, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 3.0 2.0; -3.0 9.0 6.0])
                @test with_type(daf["/ cell / gene : value %> VarN dtype Float32 eps 0"]) == ([-4.0, 1.5, 1.0], Float32)
            end
        end

        nested_test("std") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> Std()]) == (1, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [-1.0 2.0 4.0; -3.0 6.0 12.0])
                @test with_type(daf["/ cell / gene : value %> Std dtype Float32"]) == (Float32[1.0, 2.0, 4.0], Float32)
            end
        end

        nested_test("std_n") do
            nested_test("negative") do
                @test_throws dedent("""
                    invalid value: "-1"
                    value must be: not negative
                    for the parameter: eps
                    for the operation: StdN
                    in: / cell / gene : value %> StdN eps -1
                    at:                                   ▲▲
                """) daf["/ cell / gene : value %> StdN eps -1"]
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> Lookup("value") |> StdN()]) == (0.5, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [-1.0 2.0 4.0; -3.0 6.0 12.0])
                @test with_type(daf["/ cell / gene : value %> StdN dtype Float32 eps 0"]) ==
                      (Float32[-0.5, 0.5, 0.5], Float32)
            end
        end
    end
end
