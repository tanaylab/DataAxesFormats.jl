nested_test("contracts") do
    daf = MemoryDaf(; name = "memory!")

    nested_test("scalar") do
        nested_test("()") do
            @test set_scalar!(daf, "version", 1) === nothing
            for (name, expectation) in (
                ("required", RequiredInput),
                ("optional", OptionalInput),
                ("guaranteed", GuaranteedOutput),
                ("contingent", OptionalOutput),
            )
                contract = Contract(; data = ["version" => (expectation, Int64, "description")])
                nested_test(name) do
                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; data = ["version" => (RequiredInput, Int64, "description")])

                nested_test("input") do
                    @test_throws dedent("""
                        missing input scalar: version
                        with type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract, "computation", daf)
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; data = ["version" => (OptionalInput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; data = ["version" => (GuaranteedOutput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output scalar: version
                        with type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract, "computation", daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; data = ["version" => (OptionalOutput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end
        end

        nested_test("!type") do
            @test set_scalar!(daf, "version", "1.0") === nothing

            for (name, expectation) in (("required", RequiredInput), ("optional", OptionalInput))
                nested_test(name) do
                    contract = Contract(; data = ["version" => (expectation, Int64, "description")])

                    nested_test("input") do
                        @test_throws dedent("""
                            unexpected type: String
                            instead of type: Int64
                            for the input scalar: version
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_input(contract, "computation", daf)
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end

            for (name, expectation) in (("guaranteed", GuaranteedOutput), ("contingent", OptionalOutput))
                nested_test(name) do
                    contract = Contract(; data = ["version" => (expectation, Int64, "description")])

                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test_throws dedent("""
                            unexpected type: String
                            instead of type: Int64
                            for the output scalar: version
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_output(contract, "computation", daf)
                    end
                end
            end
        end
    end

    nested_test("axis") do
        nested_test("()") do
            @test add_axis!(daf, "cell", ["A", "B"]) === nothing
            for (name, expectation) in (
                ("required", RequiredInput),
                ("optional", OptionalInput),
                ("guaranteed", GuaranteedOutput),
                ("contingent", OptionalOutput),
            )
                contract = Contract(; axes = ["cell" => (expectation, "description")])
                nested_test(name) do
                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; axes = ["cell" => (RequiredInput, "description")])

                nested_test("input") do
                    @test_throws dedent("""
                        missing input axis: cell
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract, "computation", daf)
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; axes = ["cell" => (OptionalInput, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; axes = ["cell" => (GuaranteedOutput, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output axis: cell
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract, "computation", daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; axes = ["cell" => (OptionalOutput, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end
        end
    end

    nested_test("vectors") do
        @test add_axis!(daf, "cell", ["A", "B"]) === nothing

        nested_test("()") do
            @test set_vector!(daf, "cell", "age", [1, 2]) === nothing
            for (name, expectation) in (
                ("required", RequiredInput),
                ("optional", OptionalInput),
                ("guaranteed", GuaranteedOutput),
                ("contingent", OptionalOutput),
            )
                contract = Contract(; data = [("cell", "age") => (expectation, Int64, "description")])
                nested_test(name) do
                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; data = [("cell", "age") => (RequiredInput, Int64, "description")])

                nested_test("input") do
                    @test_throws dedent("""
                        missing input vector: age
                        of the axis: cell
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract, "computation", daf)
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; data = [("cell", "age") => (OptionalInput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; data = [("cell", "age") => (GuaranteedOutput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output vector: age
                        of the axis: cell
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract, "computation", daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; data = [("cell", "age") => (OptionalOutput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end
        end

        nested_test("!type") do
            @test set_vector!(daf, "cell", "age", [1.0, 2.0]) === nothing

            for (name, expectation) in (("required", RequiredInput), ("optional", OptionalInput))
                nested_test(name) do
                    contract = Contract(; data = [("cell", "age") => (expectation, Int64, "description")])

                    nested_test("input") do
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the input vector: age
                            of the axis: cell
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_input(contract, "computation", daf)
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end

            for (name, expectation) in (("guaranteed", GuaranteedOutput), ("contingent", OptionalOutput))
                nested_test(name) do
                    contract = Contract(; data = [("cell", "age") => (expectation, Int64, "description")])

                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the output vector: age
                            of the axis: cell
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_output(contract, "computation", daf)
                    end
                end
            end
        end
    end

    nested_test("matrix") do
        @test add_axis!(daf, "cell", ["A", "B"]) === nothing
        @test add_axis!(daf, "gene", ["X", "Y", "Z"]) === nothing

        nested_test("()") do
            @test set_matrix!(daf, "cell", "gene", "UMIs", [0 1 2; 3 4 5]) === nothing
            for (name, expectation) in (
                ("required", RequiredInput),
                ("optional", OptionalInput),
                ("guaranteed", GuaranteedOutput),
                ("contingent", OptionalOutput),
            )
                contract = Contract(; data = [("cell", "gene", "UMIs") => (expectation, Int64, "description")])
                nested_test(name) do
                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (RequiredInput, Int64, "description")])

                nested_test("input") do
                    @test_throws dedent("""
                        missing input matrix: UMIs
                        of the rows axis: cell
                        and the columns axis: gene
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract, "computation", daf)
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (OptionalInput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (GuaranteedOutput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output matrix: UMIs
                        of the rows axis: cell
                        and the columns axis: gene
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract, "computation", daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (OptionalOutput, Int64, "description")])

                nested_test("input") do
                    @test verify_input(contract, "computation", daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract, "computation", daf) === nothing
                end
            end
        end

        nested_test("!axis") do
            for axis in ("cell", "gene")
                nested_test(axis) do
                    @test delete_axis!(daf, axis) === nothing

                    nested_test("required") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (RequiredInput, Int64, "description")])

                        nested_test("input") do
                            @test_throws dedent("""
                                missing input axis: $(axis)
                                for the computation: computation
                                on the daf data: memory!
                            """) verify_input(contract, "computation", daf)
                        end

                        nested_test("output") do
                            @test verify_output(contract, "computation", daf) === nothing
                        end
                    end

                    nested_test("optional") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (OptionalInput, Int64, "description")])

                        nested_test("input") do
                            @test verify_input(contract, "computation", daf) === nothing
                        end

                        nested_test("output") do
                            @test verify_output(contract, "computation", daf) === nothing
                        end
                    end

                    nested_test("guaranteed") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (GuaranteedOutput, Int64, "description")])

                        nested_test("input") do
                            @test verify_input(contract, "computation", daf) === nothing
                        end

                        nested_test("output") do
                            @test_throws dedent("""
                                missing output axis: $(axis)
                                for the computation: computation
                                on the daf data: memory!
                            """) verify_output(contract, "computation", daf)
                        end
                    end

                    nested_test("contingent") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (OptionalOutput, Int64, "description")])

                        nested_test("input") do
                            @test verify_input(contract, "computation", daf) === nothing
                        end

                        nested_test("output") do
                            @test verify_output(contract, "computation", daf) === nothing
                        end
                    end
                end
            end
        end

        nested_test("!type") do
            @test set_matrix!(daf, "cell", "gene", "UMIs", [0.0 1.0 2.0; 3.0 4.0 5.0]) === nothing

            for (name, expectation) in (("required", RequiredInput), ("optional", OptionalInput))
                nested_test(name) do
                    contract = Contract(; data = [("cell", "gene", "UMIs") => (expectation, Int64, "description")])

                    nested_test("input") do
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the input matrix: UMIs
                            of the rows axis: cell
                            and the columns axis: gene
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_input(contract, "computation", daf)
                    end

                    nested_test("output") do
                        @test verify_output(contract, "computation", daf) === nothing
                    end
                end
            end

            for (name, expectation) in (("guaranteed", GuaranteedOutput), ("contingent", OptionalOutput))
                nested_test(name) do
                    contract = Contract(; data = [("cell", "gene", "UMIs") => (expectation, Int64, "description")])

                    nested_test("input") do
                        @test verify_input(contract, "computation", daf) === nothing
                    end

                    nested_test("output") do
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the output matrix: UMIs
                            of the rows axis: cell
                            and the columns axis: gene
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_output(contract, "computation", daf)
                    end
                end
            end
        end
    end
end
