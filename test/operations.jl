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
                @test_throws chomp("""
                             invalid value: "String"
                             value must be: a number type
                             for the parameter: type
                             for the operation: Abs
                             in: @ cell : value % Abs type String
                             at:                           ▲▲▲▲▲▲
                             """) daf["@ cell : value % Abs type String"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", -1)
                @test with_type(daf[LookupScalar("value") |> Abs()]) == (1, UInt64)
                @test with_type(daf[LookupScalar("value") |> Abs(; type = Int8)]) == (1, Int8)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1.0, 2.0])
                @test with_type(daf["@ cell : value % Abs"]) == ([1.0, 2.0], Float64)
                @test with_type(daf["@ cell : value % Abs type Int8"]) == ([1, 2], Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1 2; -3 4 5])
                @test with_type(daf["@ cell @ gene :: value % Abs"]) == ([0 1 2; 3 4 5], UInt64)
                @test with_type(daf["@ cell @ gene :: value % Abs type Int8"]) == ([0 1 2; 3 4 5], Int8)
            end
        end

        nested_test("round") do
            nested_test("!type") do
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1.3)
                @test with_type(daf[LookupScalar("value") |> Round()]) == (1, Int64)
                @test with_type(daf[LookupScalar("value") |> Round(; type = Int8)]) == (1, Int8)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1.7, 2.3])
                @test with_type(daf["@ cell : value % Round"]) == ([-2.0, 2.0], Int64)
                @test with_type(daf["@ cell : value % Round type Int8"]) == ([-2, 2], Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1.7 2.3; -3.3 4.7 5.2])
                @test with_type(daf["@ cell @ gene :: value % Round"]) == ([0 -2 2; -3 5 5], Int64)
                @test with_type(daf["@ cell @ gene :: value % Round type Int8"]) == ([0 -2 2; -3 5 5], Int8)
            end
        end

        nested_test("clamp") do
            nested_test("!number") do
                @test_throws chomp("""
                             invalid value: "q"
                             value must be: a valid Float64
                             for the parameter: max
                             for the operation: Clamp
                             in: . value % Clamp max q
                             at:                     ▲
                             """) daf[". value % Clamp max q"]
            end

            nested_test("!max") do
                @test_throws chomp("""
                             invalid value: "0"
                             value must be: larger than min (1.0)
                             for the parameter: max
                             for the operation: Clamp
                             in: . value % Clamp min 1 max 0
                             at:                           ▲
                             """) daf[". value % Clamp min 1 max 0"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1.3)
                @test with_type(daf[LookupScalar("value") |> Clamp(; min = 0.5, max = 1.5)]) == (1.3, Float64)
                @test with_type(daf[LookupScalar("value") |> Clamp(; max = 0.5)]) == (0.5, Float64)
                @test with_type(daf[LookupScalar("value") |> Clamp(; min = 1.5)]) == (1.5, Float64)
            end

            nested_test("vector") do
                nested_test("float") do
                    set_vector!(daf, "cell", "value", [-1.7, 2.3])
                    @test with_type(daf["@ cell : value % Clamp max 0"]) == ([-1.7, 0.0], Float64)
                    @test with_type(daf["@ cell : value % Clamp min 0"]) == ([0.0, 2.3], Float64)
                end

                nested_test("int") do
                    set_vector!(daf, "cell", "value", [-1, 2])
                    @test with_type(daf["@ cell : value % Clamp max 0"]) == ([-1, 0], Int64)
                    @test with_type(daf["@ cell : value % Clamp min 0"]) == ([0, 2], Int64)
                end

                nested_test("mix") do
                    set_vector!(daf, "cell", "value", [-1, 2])
                    @test with_type(daf["@ cell : value % Clamp max 0.5"]) == ([-1.0, 0.5], Float64)
                    @test with_type(daf["@ cell : value % Clamp min 0.5"]) == ([0.5, 2.0], Float64)
                end

                nested_test("type") do
                    set_vector!(daf, "cell", "value", [-1, 2])
                    @test with_type(daf["@ cell : value % Clamp type Float32 max 0"]) == ([-1, 0], Float32)
                    @test with_type(daf["@ cell : value % Clamp type Float32 min 0"]) == ([0, 2], Float32)
                end
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1.7 2.3; -3.3 4.7 5.2])
                @test with_type(daf["@ cell @ gene :: value % Clamp min 0 max 4"]) == ([0 0 2.3; 0 4 4], Float64)
            end

            nested_test("sparse") do
                values = sprand(Random.MersenneTwister(123), 2000, 200, 0.05)
                result = DataAxesFormats.Registry.compute_eltwise(Clamp(; min = 0.5, max = 0.8), values)
                @test result == clamp.(Matrix(values), 0.5, 0.8)
            end
        end

        nested_test("convert") do
            nested_test("invalid") do
                @test_throws chomp("""
                             missing required parameter: type
                             for the eltwise operation: Convert
                             in: @ cell : value % Convert
                             at:                  ▲▲▲▲▲▲▲
                             """) daf["@ cell : value % Convert"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test with_type(daf[LookupScalar("value") |> Convert(; type = Int8)]) == (1, Int8)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [-1, 2])
                @test with_type(daf["@ cell : value % Convert type Int8"]) == ([-1, 2], Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 -1.7 2.3; -3.3 4.7 5.2])
                @test with_type(daf["@ cell @ gene :: value % Convert type Float32"]) ==
                      (Float32[0 -1.7 2.3; -3.3 4.7 5.2], Float32)
            end
        end

        nested_test("fraction") do
            nested_test("integer") do
                @test_throws chomp("""
                             invalid value: "Int32"
                             value must be: a float type
                             for the parameter: type
                             for the operation: Fraction
                             in: @ cell : value % Fraction type Int32
                             at:                                ▲▲▲▲▲
                             """) with_type(daf["@ cell : value % Fraction type Int32"])
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test_throws "applying Fraction eltwise operation to a scalar" daf[LookupScalar("value") |> Fraction()]
            end

            nested_test("vector") do
                nested_test("()") do
                    set_vector!(daf, "cell", "value", [1, 3])
                    @test with_type(daf["@ cell : value % Fraction"]) == ([0.25, 0.75], Float64)
                    @test with_type(daf["@ cell : value % Fraction type Float32"]) == ([0.25, 0.75], Float32)
                end

                nested_test("zero") do
                    set_vector!(daf, "cell", "value", [0, 0])
                    @test with_type(daf["@ cell : value % Fraction"]) == ([0, 0], Float64)
                end
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 0 2; 1 0 6])
                @test with_type(daf["@ cell @ gene :: value % Fraction"]) == ([0 0 0.25; 1 0 0.75], Float64)
                @test with_type(daf["@ cell @ gene :: value % Fraction type Float32"]) ==
                      ([0 0 0.25; 1 0 0.75], Float32)
            end
        end

        nested_test("log") do
            nested_test("!positive") do
                @test_throws chomp("""
                             invalid value: "0"
                             value must be: positive
                             for the parameter: base
                             for the operation: Log
                             in: . value % Log base 0
                             at:                    ▲
                             """) daf[". value % Log base 0"]
            end

            nested_test("negative") do
                @test_throws chomp("""
                             invalid value: "-1"
                             value must be: not negative
                             for the parameter: eps
                             for the operation: Log
                             in: . value % Log eps -1
                             at:                   ▲▲
                             """) daf[". value % Log eps -1"]
            end

            nested_test("special") do
                set_scalar!(daf, "value", 0)
                @test isapprox(daf[". value % Log base e eps pi"], log(pi))
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test with_type(daf[LookupScalar("value") |> Log()]) == (0, Float64)
                @test with_type(daf[LookupScalar("value") |> Log(; type = Float32)]) == (0, Float32)
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1.0, 2.0])
                @test with_type(daf["@ cell : value % Log base 2"]) == ([0.0, 1.0], Float64)
                @test with_type(daf["@ cell : value % Log base 2 eps 1 type Float32"]) ==
                      (Float32[1.0, log2(3)], Float32)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [0 1 2; 3 4 5])
                @test with_type(daf["@ cell @ gene :: value % Log eps 1"]) ==
                      ([log(1) log(2) log(3); log(4) log(5) log(6)], Float64)
            end

            nested_test("sparse") do
                set_vector!(daf, "cell", "value", sparse([0.0, 3.0]))
                set_matrix!(daf, "cell", "gene", "value", sparse([0.0 1.0 0.0; 3.0 0.0 5.0]); relayout = false)
                @test daf["@ cell : value % Log eps 1"] == [log(1), log(4)]
                @test daf["@ cell @ gene :: value % Log eps 1"] == [log(1) log(2) log(1); log(4) log(1) log(6)]
            end

            nested_test("rows") do
                values = Float64[0 1 2; 3 4 5]
                result = DataAxesFormats.Registry.compute_eltwise(Log(; eps = 1), transpose(values))
                @test major_axis(result) == Rows
                @test result ≈ transpose(log.(values .+ 1))
            end

            nested_test("packed") do
                mktempdir() do path
                    packed = FilesDaf(path * "/packed.daf", "w+"; name = "packed!", packed = true)
                    add_axis!(packed, "cell", ["c$(index)" for index in 1:4096])
                    add_axis!(packed, "gene", ["X", "Y"])
                    values = Float32[row + column for row in 1:4096, column in 1:2]
                    set_vector!(packed, "cell", "value", values[:, 1])
                    set_matrix!(packed, "cell", "gene", "value", values; relayout = false)
                    @test packed["@ cell : value % Log eps 1"] ≈ log.(values[:, 1] .+ 1)
                    @test packed["@ cell @ gene :: value % Log eps 1"] ≈ log.(values .+ 1)
                    # The cached query results hold no materialization of the lazy inputs. The tasks of the last
                    # parallel loop stay reachable (with the input they captured) until other tasks run.
                    parallel_loop_wo_rng(1:1000; name = "release") do _
                        return nothing
                    end
                    GC.gc()
                    GC.gc()
                    for lazy in (
                        get_vector(packed, "cell", "value").array,
                        get_matrix(packed, "cell", "gene", "value"; relayout = false).array,
                    )
                        @test DataAxesFormats.PackedFormat.existing_materialization(lazy.materialization_cell) ===
                              nothing
                    end
                    return nothing
                end
            end
        end

        nested_test("significant") do
            nested_test("!positive") do
                @test_throws chomp("""
                             invalid value: "0"
                             value must be: positive
                             for the parameter: high
                             for the operation: Significant
                             in: @ cell : value % Significant high 0
                             at:                                   ▲
                             """) daf["@ cell : value % Significant high 0"]
            end

            nested_test("negative") do
                @test_throws chomp("""
                             invalid value: "-1"
                             value must be: not negative
                             for the parameter: low
                             for the operation: Significant
                             in: @ cell : value % Significant high 1 low -1
                             at:                                         ▲▲
                             """) daf["@ cell : value % Significant high 1 low -1"]
            end

            nested_test("!low") do
                @test_throws chomp("""
                             invalid value: "2"
                             value must be: at most high (1.0)
                             for the parameter: low
                             for the operation: Significant
                             in: @ cell : value % Significant high 1 low 2
                             at:                                         ▲
                             """) daf["@ cell : value % Significant high 1 low 2"]
            end

            nested_test("scalar") do
                set_scalar!(daf, "value", 1)
                @test_throws "applying Significant eltwise operation to a scalar" daf[LookupScalar(
                    "value",
                ) |> Significant(; high = 3.0)]
            end

            nested_test("vector") do
                nested_test("dense") do
                    set_vector!(daf, "cell", "value", [1, 3])
                    @test with_type(daf["@ cell : value"]) == ([1, 3], Int64)
                    @test with_type(daf["@ cell : value % Significant high 3"]) == ([0, 3], Int64)
                    @test with_type(daf["@ cell : value % Significant high 3 low 1"]) == ([1, 3], Int64)
                end

                nested_test("sparse") do
                    set_vector!(daf, "cell", "value", SparseVector([1, 3]))
                    @test with_type(daf["@ cell : value"]) == ([1, 3], Int64)
                    @test with_type(daf["@ cell : value % Significant high 3"]) == ([0, 3], Int64)
                    @test with_type(daf["@ cell : value % Significant high 3 low 1"]) == ([1, 3], Int64)
                end
            end

            nested_test("matrix") do
                nested_test("dense") do
                    set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                    @test with_type(daf["@ cell @ gene :: value % Significant high 3 low 2"]) ==
                          ([0.0 0.0 2.0; -3.0 0.0 6.0], Float64)
                end

                nested_test("sparse") do
                    set_matrix!(daf, "cell", "gene", "value", sparse_matrix_csc([0.0 2.0 2.0; -3.0 0.0 6.0]))
                    @test with_type(daf["@ cell @ gene :: value % Significant high 3 low 2"]) ==
                          ([0.0 0.0 2.0; -3.0 0.0 6.0], Float64)
                end
            end
        end
    end

    nested_test("reduction") do
        # Reducing the rows of each group in one pass over each column gives the same results as reducing each group's
        # rows on their own.
        nested_test("grouped") do
            compute_reduction = DataAxesFormats.Registry.compute_reduction
            reduction_result_type = DataAxesFormats.Registry.reduction_result_type
            supports_grouped_reduction = DataAxesFormats.Registry.supports_grouped_reduction
            group_index_per_row = [1, 2, 0, 1, 3, 2, 1, 0, 3, 3, 2, 1]
            integers = Int64[
                0 3 0 1 0
                2 0 0 0 5
                0 0 7 0 0
                4 1 0 0 0
                0 0 0 2 1
                1 0 3 0 0
                0 6 0 0 2
                9 0 0 4 0
                0 0 1 0 0
                3 0 0 0 0
                0 2 0 5 0
                0 0 0 0 3
            ]
            floats = integers .* 0.5
            operations = (
                Count(),
                Sum(),
                Max(),
                Min(),
                Mean(),
                GeoMean(; eps = 1.0),
                Var(),
                VarN(; eps = 1.0),
                Std(),
                StdN(; eps = 1.0),
            )
            for operation in operations
                @test supports_grouped_reduction(operation)
                for values in (integers, floats, sparse(integers), sparse(floats))
                    result = compute_grouped_reduction(operation, values, group_index_per_row, 3)
                    @test size(result) == (3, 5)
                    @test eltype(result) == reduction_result_type(operation, eltype(values))
                    for group_index in 1:3
                        expected = compute_reduction(operation, values[group_index_per_row .== group_index, :], 1)
                        @test result[group_index, :] ≈ expected
                    end
                end
            end
            @test !supports_grouped_reduction(Median())
            return nothing
        end

        nested_test("buffered") do
            compute_reduction = DataAxesFormats.Registry.compute_reduction
            reduction_result_type = DataAxesFormats.Registry.reduction_result_type
            supports_buffer_reduction = DataAxesFormats.Registry.supports_buffer_reduction
            supports_grouped_reduction = DataAxesFormats.Registry.supports_grouped_reduction
            group_index_per_row = [1, 2, 3, 1, 2, 3, 1, 0]
            integers = [
                0 0 0 2 1
                1 0 3 0 0
                0 6 0 0 2
                9 0 0 4 0
                0 0 1 0 0
                3 0 0 0 0
                0 2 0 5 0
                0 0 0 0 3
            ]
            floats = integers .* 0.5
            for operation in (Median(), Quantile(; p = 0.3), Mode())
                @test !supports_grouped_reduction(operation)
                @test supports_buffer_reduction(operation)
                for values in (integers, floats, sparse(integers), sparse(floats))
                    result = compute_grouped_reduction(operation, values, group_index_per_row, 3)
                    @test size(result) == (3, 5)
                    @test eltype(result) == reduction_result_type(operation, eltype(values))
                    for group_index in 1:3
                        expected = compute_reduction(operation, values[group_index_per_row .== group_index, :], 1)
                        @test result[group_index, :] ≈ expected
                    end
                end
            end
            strings = ["a" "b"; "b" "b"; "a" "c"; "c" "c"]
            @test compute_grouped_reduction(Mode(), strings, [1, 1, 2, 2], 2) == ["a" "b"; "a" "c"]
            return nothing
        end

        nested_test("grouped_columns") do
            compute_reduction = DataAxesFormats.Registry.compute_reduction
            reduction_result_type = DataAxesFormats.Registry.reduction_result_type
            compute_grouped_columns_reduction! = DataAxesFormats.Operations.compute_grouped_columns_reduction!
            column_indices_per_group = [[5, 1], [2, 4, 7], [6]]
            integers = [
                0 0 0 2 1 0 4
                1 0 3 0 0 0 0
                0 6 0 0 2 0 1
                9 0 0 4 0 0 0
            ]
            floats = integers .* 0.5
            for operation in (Sum(), Count(), Mean(), Max(), Min(), Var(), Std(), GeoMean(), Median(), Mode())
                for values in (integers, floats, sparse(integers), sparse(floats))
                    result = Matrix{reduction_result_type(operation, eltype(values))}(undef, 4, 3)
                    compute_grouped_columns_reduction!(result, operation, values, column_indices_per_group)
                    for (group_index, column_indices) in enumerate(column_indices_per_group)
                        expected = compute_reduction(operation, permutedims(values[:, column_indices]), 1)
                        @test result[:, group_index] ≈ expected
                    end
                end
            end
            strings = ["a" "b" "b" "c"; "c" "c" "a" "a"]
            result = Matrix{String}(undef, 2, 2)
            compute_grouped_columns_reduction!(result, Mode(), strings, [[1, 2, 3], [4, 3]])
            @test result == ["b" "b"; "c" "a"]
            return nothing
        end

        nested_test("columns") do
            compute_reduction = DataAxesFormats.Registry.compute_reduction
            compute_columns_reduction = DataAxesFormats.Operations.compute_columns_reduction
            compute_streamed_matrix_reduction = DataAxesFormats.Operations.compute_streamed_matrix_reduction
            integers = [
                0 0 0 2 1 0 4
                1 0 3 0 0 0 0
                0 6 0 0 2 0 1
                9 0 0 4 0 0 0
            ]
            floats = integers .* 0.5
            for operation in (Sum(), Count(), Mean(), Max(), Min(), Var(), StdN(), GeoMean(), Median(), Mode())
                for values in (integers, floats, sparse(integers), sparse(floats))
                    @test compute_columns_reduction(operation, values) ≈
                          compute_reduction(operation, permutedims(values), 1)
                    if DataAxesFormats.Registry.supports_grouped_reduction(operation)
                        @test compute_streamed_matrix_reduction(operation, values) ≈
                              compute_reduction(operation, values)
                    end
                end
            end
            @test compute_columns_reduction(Mode(), ["a" "b" "b"; "c" "a" "a"]) == ["b", "a"]
            return nothing
        end

        nested_test("merge") do
            empty_reduction_state = DataAxesFormats.Registry.empty_reduction_state
            update_reduction_state = DataAxesFormats.Registry.update_reduction_state
            merge_reduction_states = DataAxesFormats.Registry.merge_reduction_states
            final_reduction_value = DataAxesFormats.Registry.final_reduction_value
            values = [3.0, 1.5, 4.0, 1.0, 5.5, 9.0, 2.0]
            for operation in (Sum(), Count(), Mean(), Max(), Min(), Var(), VarN(), Std(), StdN(), GeoMean())
                states = [empty_reduction_state(operation, Float64) for _ in 1:3]
                for (index, value) in enumerate(values)
                    part_index = index <= 2 ? 1 : 2
                    states[part_index] = update_reduction_state(operation, states[part_index], value)
                    states[3] = update_reduction_state(operation, states[3], value)
                end
                merged_state = merge_reduction_states(operation, states[1], states[2])
                @test final_reduction_value(operation, merged_state, 7, 9) ≈
                      final_reduction_value(operation, states[3], 7, 9)
                @test final_reduction_value(
                    operation,
                    merge_reduction_states(operation, empty_reduction_state(operation, Float64), states[3]),
                    7,
                    9,
                ) ≈ final_reduction_value(operation, states[3], 7, 9)
            end
            return nothing
        end

        nested_test("mode_ties") do
            compute_reduction = DataAxesFormats.Registry.compute_reduction
            @test compute_reduction(Mode(), [3, 1, 3, 1]) == 1
            @test compute_reduction(Mode(), sparse([0, 2, 0, 2])) == 0
            @test compute_reduction(Mode(), sparse([0, 0, -1, -1])) == -1
            @test compute_reduction(Mode(), ["b", "a", "b", "a"]) == "a"
            @test compute_reduction(Mode(), sparse([0 2; 0 2; 2 0; 0 2]), 1) == [0, 2]
            return nothing
        end

        nested_test("packed") do
            mktempdir() do path
                packed = FilesDaf(path * "/packed.daf", "w+"; name = "packed!", packed = true)
                memory = MemoryDaf(; name = "memory!")
                values = Float32[row + column for row in 1:4096, column in 1:3]
                for target in (packed, memory)
                    add_axis!(target, "cell", ["c$(index)" for index in 1:4096])
                    add_axis!(target, "gene", ["X", "Y", "Z"])
                    set_vector!(target, "cell", "type", ["T$(index % 3)" for index in 1:4096])
                    set_vector!(target, "gene", "kind", ["A", "B", "A"])
                    set_matrix!(target, "cell", "gene", "value", values; relayout = false)
                end
                for operation in ("Median", "Quantile p 0.3", "Mode", "Sum", "Var", "VarN", "Std", "StdN")
                    for query in (
                        "@ cell @ gene :: value >> $(operation)",
                        "@ cell @ gene :: value >| $(operation)",
                        "@ cell @ gene :: value -/ type >- $(operation)",
                        "@ cell @ gene :: value |/ kind >| $(operation)",
                    )
                        @test packed[query] ≈ memory[query]
                    end
                end
                return nothing
            end
        end

        nested_test("most_frequent") do
            nested_test("vector") do
                nested_test("numeric") do
                    set_vector!(daf, "gene", "value", [1, 1, 3])
                    @test with_type(daf[Axis("gene") |> LookupVector("value") |> Mode()]) == (1, Int64)
                end

                nested_test("string") do
                    set_vector!(daf, "gene", "value", ["Foo", "Foo", "Bar"])
                    @test with_type(daf[Axis("gene") |> LookupVector("value") |> Mode()]) == ("Foo", String)
                end
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; 1.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >| Mode"]) == ([2.0, 1.0], Float64)
                @test with_type(daf["@ gene @ cell :: value >- Mode"]) == ([2.0, 1.0], Float64)
            end

            nested_test("string_matrix") do
                set_matrix!(daf, "cell", "gene", "value", ["a" "b" "b"; "a" "a" "c"])
                @test daf["@ cell @ gene :: value >| Mode"].array == ["b", "a"]
            end
        end

        nested_test("count") do
            nested_test("vector") do
                nested_test("numeric") do
                    set_vector!(daf, "cell", "value", [1, 3])
                    @test with_type(daf[Axis("cell") |> LookupVector("value") |> Count()]) == (2, UInt32)
                    @test with_type(daf["@ cell : value >> Count type Int8"]) == (2, Int8)
                end

                nested_test("string") do
                    set_vector!(daf, "cell", "value", ["Foo", "Bar"])
                    @test with_type(daf[Axis("cell") |> LookupVector("value") |> Count()]) == (2, UInt32)
                    @test with_type(daf["@ cell >> Count type Int8"]) == (2, Int8)
                end
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- Count"]) == ([2, 2, 2], UInt32)
            end
        end

        nested_test("sum") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> Sum()]) == (4, Int64)
                @test with_type(daf["@ cell : value >> Sum type Int8"]) == (4, Int8)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- Sum"]) == ([-2.0, 3.0, 8.0], Float64)
            end
        end

        nested_test("max") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> Max()]) == (3, Int64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- Max"]) == ([1.0, 2.0, 6.0], Float64)
            end

            nested_test("result_type") do
                @test DataAxesFormats.Registry.reduction_result_type(Max(), Int8) === Int8
            end
        end

        nested_test("min") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> Min()]) == (1, Int64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- Min"]) == ([-3.0, 1.0, 2.0], Float64)
            end

            nested_test("result_type") do
                @test DataAxesFormats.Registry.reduction_result_type(Min(), Int8) === Int8
            end
        end

        nested_test("median") do
            nested_test("vector") do
                set_vector!(daf, "gene", "value", [1, 3, 7])
                @test with_type(daf[Axis("gene") |> LookupVector("value") |> Median()]) == (3.0, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; 1.0 -3.0 6.0])
                @test with_type(daf["@ gene @ cell :: value >- Median type Float32"]) == ([2.0, 1.0], Float32)
                try
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                    @test with_type(daf["@ cell @ gene :: value >| Median type Float32"]) == ([2.0, 1.0], Float32)
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
                end
            end
        end

        nested_test("quantile") do
            nested_test("negative") do
                @test_throws chomp("""
                             invalid value: "-0.5"
                             value must be: at least 0
                             for the parameter: p
                             for the operation: Quantile
                             in: @ gene @ cell :: value >| Quantile p -0.5
                             at:                                      ▲▲▲▲
                             """) daf["@ gene @ cell :: value >| Quantile p -0.5"]
            end

            nested_test("high") do
                @test_throws chomp("""
                             invalid value: "1.5"
                             value must be: at most 1
                             for the parameter: p
                             for the operation: Quantile
                             in: @ gene @ cell :: value >- Quantile p 1.5
                             at:                                      ▲▲▲
                             """) daf["@ gene @ cell :: value >- Quantile p 1.5"]
            end

            nested_test("vector") do
                set_vector!(daf, "gene", "value", [1, 3, 7])
                @test with_type(daf[Axis("gene") |> LookupVector("value") |> Quantile(; p = 0.0)]) == (1.0, Float64)
                @test with_type(daf[Axis("gene") |> LookupVector("value") |> Quantile(; p = 0.25)]) == (2.0, Float64)
                @test with_type(daf[Axis("gene") |> LookupVector("value") |> Quantile(; p = 0.5)]) == (3.0, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; 1.0 -3.0 6.0])
                @test with_type(daf["@ gene @ cell :: value >- Quantile p 0.5"]) == ([2.0, 1.0], Float64)
                try
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                    @test with_type(daf["@ cell @ gene :: value >| Quantile p 1.0 type Float32"]) ==
                          ([2.0, 6.0], Float32)
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
                end
            end
        end

        nested_test("mean") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> Mean()]) == (2, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- Mean type Float32"]) == ([-1.0, 1.5, 4.0], Float32)
                @test with_type(daf["@ gene @ cell :: value >| Mean type Float32"]) == ([-1.0, 1.5, 4.0], Float32)
            end
        end

        nested_test("geomean") do
            nested_test("negative") do
                @test_throws chomp("""
                             invalid value: "-1"
                             value must be: not negative
                             for the parameter: eps
                             for the operation: GeoMean
                             in: @ cell @ gene :: value >| GeoMean eps -1
                             at:                                       ▲▲
                             """) daf["@ cell @ gene :: value >| GeoMean eps -1"]
            end

            nested_test("vector") do
                nested_test("!eps") do
                    set_vector!(daf, "cell", "value", [1, 4])
                    @test with_type(daf[Axis("cell") |> LookupVector("value") |> GeoMean()]) == (2, Float64)
                end

                nested_test("eps") do
                    set_vector!(daf, "cell", "value", [0, 3])
                    @test with_type(daf[Axis("cell") |> LookupVector("value") |> GeoMean(; eps = 1)]) == (1, Float64)
                end
            end

            nested_test("matrix") do
                nested_test("!eps") do
                    set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; 4.0 8.0 2.0])
                    @test with_type(daf["@ cell @ gene :: value >- GeoMean type Float32"]) == ([2.0, 4.0, 2.0], Float32)
                    @test with_type(daf["@ gene @ cell :: value >| GeoMean type Float32"]) == ([2.0, 4.0, 2.0], Float32)
                end

                nested_test("eps") do
                    set_matrix!(daf, "cell", "gene", "value", [0.0 1.0 1.0; 3.0 7.0 1.0])
                    @test with_type(daf["@ cell @ gene :: value >- GeoMean type Float32 eps 1"]) ==
                          ([1.0, 3.0, 1.0], Float32)
                    @test with_type(daf["@ gene @ cell :: value >| GeoMean type Float32 eps 1"]) ==
                          ([1.0, 3.0, 1.0], Float32)
                end
            end
        end

        nested_test("var") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> Var()]) == (1, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 2.0 2.0; -3.0 1.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- Var type Float32"]) == ([4.0, 0.25, 4.0], Float32)
                try
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                    @test with_type(daf["@ gene @ cell :: value >| Var type Float32"]) == ([4.0, 0.25, 4.0], Float32)
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
                end
            end
        end

        nested_test("var_n") do
            nested_test("negative") do
                @test_throws chomp("""
                             invalid value: "-1"
                             value must be: not negative
                             for the parameter: eps
                             for the operation: VarN
                             in: @ cell @ gene :: value >| VarN eps -1
                             at:                                    ▲▲
                             """) daf["@ cell @ gene :: value >| VarN eps -1"]
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> VarN()]) == (0.5, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [1.0 3.0 2.0; -3.0 9.0 6.0])
                @test with_type(daf["@ cell @ gene :: value >- VarN type Float32 eps 0"]) == ([-4.0, 1.5, 1.0], Float32)
                try
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                    @test with_type(daf["@ gene @ cell :: value >| VarN type Float32 eps 0"]) ==
                          ([-4.0, 1.5, 1.0], Float32)
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
                end
            end
        end

        nested_test("std") do
            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> Std()]) == (1, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [-1.0 2.0 4.0; -3.0 6.0 12.0])
                @test with_type(daf["@ cell @ gene :: value >- Std type Float32"]) == (Float32[1.0, 2.0, 4.0], Float32)
                try
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                    @test with_type(daf["@ gene @ cell :: value >| Std type Float32"]) ==
                          (Float32[1.0, 2.0, 4.0], Float32)
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
                end
            end
        end

        nested_test("std_n") do
            nested_test("negative") do
                @test_throws chomp("""
                             invalid value: "-1"
                             value must be: not negative
                             for the parameter: eps
                             for the operation: StdN
                             in: @ cell @ gene :: value >| StdN eps -1
                             at:                                    ▲▲
                             """) daf["@ cell @ gene :: value >| StdN eps -1"]
            end

            nested_test("vector") do
                set_vector!(daf, "cell", "value", [1, 3])
                @test with_type(daf[Axis("cell") |> LookupVector("value") |> StdN()]) == (0.5, Float64)
            end

            nested_test("matrix") do
                set_matrix!(daf, "cell", "gene", "value", [-1.0 2.0 4.0; -3.0 6.0 12.0])
                @test with_type(daf["@ cell @ gene :: value >- StdN type Float32 eps 0"]) ==
                      (Float32[-0.5, 0.5, 0.5], Float32)
                try
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                    @test with_type(daf["@ gene @ cell :: value >| StdN type Float32 eps 0"]) ==
                          (Float32[-0.5, 0.5, 0.5], Float32)
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = ErrorHandler
                end
            end
        end
    end
end
