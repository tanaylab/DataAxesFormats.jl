function is_output(expectation::ContractExpectation)::Bool
    return expectation in (GuaranteedOutput, OptionalOutput)
end

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
                    nested_test("overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf; overwrite = true)
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput scalar: version
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert get_scalar(contract_daf, "version") == 1
                                    @test verify(contract_daf) === nothing
                                end
                            end
                        end
                    end

                    nested_test("!overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf)
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput scalar: version
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    elseif is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) scalar: version
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert get_scalar(contract_daf, "version") == 1
                                    if is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) scalar: version
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end
                            end
                        end
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; data = ["version" => (RequiredInput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test_throws dedent("""
                        missing input scalar: version
                        with type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract_daf)
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; data = ["version" => (OptionalInput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; data = ["version" => (GuaranteedOutput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output scalar: version
                        with type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract_daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; data = ["version" => (OptionalOutput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end
        end

        nested_test("!type") do
            @test set_scalar!(daf, "version", "1.0") === nothing

            for (name, expectation) in (
                ("required", RequiredInput),
                ("optional", OptionalInput),
                ("guaranteed", GuaranteedOutput),
                ("contingent", OptionalOutput),
            )
                nested_test(name) do
                    contract = Contract(; data = ["version" => (expectation, Int64, "description")])
                    contract_daf = contractor("computation", contract, daf; overwrite = true)

                    for (direction, verify) in (("input", verify_input), ("output", verify_output))
                        nested_test(direction) do
                            @test_throws dedent("""
                                unexpected type: String
                                instead of type: Int64
                                for the $(direction) scalar: version
                                for the computation: computation
                                on the daf data: memory!
                            """) verify(contract_daf)
                        end
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
                    nested_test("overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf; overwrite = true)
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput axis: cell
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert axis_length(contract_daf, "cell") == 2
                                    @test verify(contract_daf) === nothing
                                end
                            end
                        end
                    end

                    nested_test("!overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf)
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput axis: cell
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    elseif is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) axis: cell
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert axis_length(contract_daf, "cell") == 2
                                    if is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) axis: cell
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end
                            end
                        end
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; axes = ["cell" => (RequiredInput, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test_throws dedent("""
                        missing input axis: cell
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract_daf)
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; axes = ["cell" => (OptionalInput, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; axes = ["cell" => (GuaranteedOutput, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output axis: cell
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract_daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; axes = ["cell" => (OptionalOutput, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
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
                    nested_test("overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf; overwrite = true)
                            @assert axis_length(contract_daf, "cell") == 2
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput vector: age
                                            of the axis: cell
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert get_vector(contract_daf, "cell", "age") == [1, 2]
                                    @test verify(contract_daf) === nothing
                                end
                            end
                        end
                    end

                    nested_test("!overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf)
                            @assert axis_length(contract_daf, "cell") == 2
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput vector: age
                                            of the axis: cell
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    elseif is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) vector: age
                                            of the axis: cell
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert get_vector(contract_daf, "cell", "age") == [1, 2]
                                    if is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) vector: age
                                            of the axis: cell
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end
                            end
                        end
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; data = [("cell", "age") => (RequiredInput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)
                @assert axis_length(contract_daf, "cell") == 2

                nested_test("input") do
                    @test_throws dedent("""
                        missing input vector: age
                        of the axis: cell
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract_daf)
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; data = [("cell", "age") => (OptionalInput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; data = [("cell", "age") => (GuaranteedOutput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)
                @assert axis_length(contract_daf, "cell") == 2

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output vector: age
                        of the axis: cell
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract_daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; data = [("cell", "age") => (OptionalOutput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end
        end

        nested_test("!type") do
            @test set_vector!(daf, "cell", "age", [1.0, 2.0]) === nothing

            nested_test("input") do
                for (name, expectation) in (("required", RequiredInput), ("optional", OptionalInput))
                    nested_test(name) do
                        contract = Contract(; data = [("cell", "age") => (expectation, Int64, "description")])
                        contract_daf = contractor("computation", contract, daf)
                        @assert axis_length(contract_daf, "cell") == 2
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the input vector: age
                            of the axis: cell
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_input(contract_daf)
                    end
                end
            end

            nested_test("output") do
                for (name, expectation) in (("guaranteed", GuaranteedOutput), ("contingent", OptionalOutput))
                    nested_test(name) do
                        contract = Contract(; data = [("cell", "age") => (expectation, Int64, "description")])
                        contract_daf = contractor("computation", contract, daf)
                        @assert axis_length(contract_daf, "cell") == 2
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the output vector: age
                            of the axis: cell
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_output(contract_daf)
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
                    nested_test("overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf; overwrite = true)
                            @assert axis_length(contract_daf, "cell") == 2
                            @assert axis_length(contract_daf, "gene") == 3
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput matrix: UMIs
                                            of the rows axis: cell
                                            and the columns axis: gene
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert get_matrix(contract_daf, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                                    @test verify(contract_daf) === nothing
                                end
                            end
                        end
                    end

                    nested_test("!overwrite") do
                        for (direction, verify) in (("input", verify_input), ("output", verify_output))
                            contract_daf = contractor("computation", contract, daf)
                            @assert axis_length(contract_daf, "cell") == 2
                            @assert axis_length(contract_daf, "gene") == 3
                            nested_test(direction) do
                                nested_test("!accessed") do
                                    if direction == "output" && expectation == RequiredInput
                                        @test_throws dedent("""
                                            unused RequiredInput matrix: UMIs
                                            of the rows axis: cell
                                            and the columns axis: gene
                                            of the computation: computation
                                            on the daf data: memory!
                                        """) verify(contract_daf)
                                    elseif is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) matrix: UMIs
                                            of the rows axis: cell
                                            and the columns axis: gene
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end

                                nested_test("accessed") do
                                    @assert get_matrix(contract_daf, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                                    if is_output(expectation)
                                        @test_throws dedent("""
                                            pre-existing $(expectation) matrix: UMIs
                                            of the rows axis: cell
                                            and the columns axis: gene
                                            for the computation: computation
                                            on the daf data: memory!
                                        """) verify_input(contract_daf)
                                    else
                                        @test verify(contract_daf) === nothing
                                    end
                                end
                            end
                        end
                    end
                end
            end
        end

        nested_test("missing") do
            nested_test("required") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (RequiredInput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)
                @assert axis_length(contract_daf, "cell") == 2
                @assert axis_length(contract_daf, "gene") == 3

                nested_test("input") do
                    @test_throws dedent("""
                        missing input matrix: UMIs
                        of the rows axis: cell
                        and the columns axis: gene
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_input(contract_daf)
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("optional") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (OptionalInput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
                end
            end

            nested_test("guaranteed") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (GuaranteedOutput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)
                @assert axis_length(contract_daf, "cell") == 2
                @assert axis_length(contract_daf, "gene") == 3

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test_throws dedent("""
                        missing output matrix: UMIs
                        of the rows axis: cell
                        and the columns axis: gene
                        with element type: Int64
                        for the computation: computation
                        on the daf data: memory!
                    """) verify_output(contract_daf)
                end
            end

            nested_test("contingent") do
                contract = Contract(; data = [("cell", "gene", "UMIs") => (OptionalOutput, Int64, "description")])
                contract_daf = contractor("computation", contract, daf)

                nested_test("input") do
                    @test verify_input(contract_daf) === nothing
                end

                nested_test("output") do
                    @test verify_output(contract_daf) === nothing
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
                        contract_daf = contractor("computation", contract, daf)

                        nested_test("input") do
                            @test_throws dedent("""
                                missing input axis: $(axis)
                                for the computation: computation
                                on the daf data: memory!
                            """) verify_input(contract_daf)
                        end
                    end

                    nested_test("optional") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (OptionalInput, Int64, "description")])
                        contract_daf = contractor("computation", contract, daf)

                        nested_test("input") do
                            @test verify_input(contract_daf) === nothing
                        end

                        nested_test("output") do
                            @test verify_output(contract_daf) === nothing
                        end
                    end

                    nested_test("guaranteed") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (GuaranteedOutput, Int64, "description")])
                        contract_daf = contractor("computation", contract, daf)

                        nested_test("input") do
                            @test verify_input(contract_daf) === nothing
                        end

                        nested_test("output") do
                            @test_throws dedent("""
                                missing output matrix: UMIs
                                of the rows axis: cell
                                and the columns axis: gene
                                with element type: Int64
                                for the computation: computation
                                on the daf data: memory!
                            """) verify_output(contract_daf)
                        end
                    end

                    nested_test("contingent") do
                        contract =
                            Contract(; data = [("cell", "gene", "UMIs") => (OptionalOutput, Int64, "description")])
                        contract_daf = contractor("computation", contract, daf)

                        nested_test("input") do
                            @test verify_input(contract_daf) === nothing
                        end

                        nested_test("output") do
                            @test verify_output(contract_daf) === nothing
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
                    contract_daf = contractor("computation", contract, daf)

                    nested_test("input") do
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the input matrix: UMIs
                            of the rows axis: cell
                            and the columns axis: gene
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_input(contract_daf)
                    end
                end
            end

            for (name, expectation) in (("guaranteed", GuaranteedOutput), ("contingent", OptionalOutput))
                nested_test(name) do
                    contract = Contract(; data = [("cell", "gene", "UMIs") => (expectation, Int64, "description")])
                    contract_daf = contractor("computation", contract, daf)

                    nested_test("output") do
                        @test_throws dedent("""
                            unexpected type: Float64
                            instead of type: Int64
                            for the output matrix: UMIs
                            of the rows axis: cell
                            and the columns axis: gene
                            for the computation: computation
                            on the daf data: memory!
                        """) verify_output(contract_daf)
                    end
                end
            end
        end
    end

    nested_test("access") do
        @test set_scalar!(daf, "version", 1) === nothing
        @test add_axis!(daf, "cell", ["A", "B"]) === nothing
        @test add_axis!(daf, "gene", ["X", "Y", "Z"]) === nothing
        @test set_vector!(daf, "cell", "age", [1.0, 2.0]) === nothing
        @test set_matrix!(daf, "cell", "gene", "UMIs", [0.0 1.0 2.0; 3.0 4.0 5.0]) === nothing

        nested_test("relaxed") do
            contract = Contract(; is_relaxed = true)
            contract_daf = contractor("computation", contract, daf)

            nested_test("axes_set") do
                @test axes_set(contract_daf) === axes_set(daf)
            end

            nested_test("axis_array") do
                @test axis_array(contract_daf, "cell") == ["A", "B"]
            end

            nested_test("axis_dict") do
                @test axis_dict(contract_daf, "cell") == Dict("A" => 1, "B" => 2)
            end

            nested_test("axis_indices") do
                @test axis_indices(contract_daf, "cell", ["A", "B"]) == [1, 2]
            end

            nested_test("axis_length") do
                @test axis_length(contract_daf, "cell") == 2
            end

            nested_test("axis_version_counter") do
                return axis_version_counter(contract_daf, "cell") == 1
            end

            nested_test("depict") do
                @test depict(contract_daf) == "Contract MemoryDaf memory!.for.computation"
            end

            nested_test("description") do
                @test description(contract_daf) == dedent("""
                    name: memory!
                    type: MemoryDaf
                    scalars:
                      version: 1 (Int64)
                    axes:
                      cell: 2 entries
                      gene: 3 entries
                    vectors:
                      cell:
                        age: 2 x Float64 (Dense)
                    matrices:
                      cell,gene:
                        UMIs: 2 x 3 x Float64 in Columns (Dense)
                      gene,cell:
                        UMIs: 3 x 2 x Float64 in Columns (Dense)
                """) * "\n"
            end

            nested_test("empty_cache!") do
                @test empty_cache!(contract_daf) === nothing
            end

            nested_test("get_scalar") do
                @test get_scalar(contract_daf, "version") == 1
            end

            nested_test("has_scalar") do
                @test has_scalar(contract_daf, "version")
                @test !has_scalar(contract_daf, "quality")
            end

            nested_test("scalars_set") do
                @test scalars_set(contract_daf) === scalars_set(daf)
            end

            nested_test("has_axis") do
                @test has_axis(contract_daf, "cell")
                @test !has_axis(contract_daf, "type")
            end

            nested_test("has_vector") do
                @test has_vector(contract_daf, "cell", "age")
                @test !has_vector(contract_daf, "cell", "batch")
            end

            nested_test("vectors_set") do
                @test vectors_set(contract_daf, "cell") == Set(["age"])
            end

            nested_test("has_matrix") do
                @test has_matrix(contract_daf, "cell", "gene", "UMIs")
                @test !has_matrix(contract_daf, "cell", "gene", "fraction")
            end

            nested_test("matrices_set") do
                @test matrices_set(contract_daf, "cell", "gene") == Set(["UMIs"])
            end
        end

        nested_test("empty") do
            contract = Contract()
            contract_daf = contractor("computation", contract, daf)

            nested_test("axes_set") do
                @test axes_set(contract_daf) === axes_set(daf)
            end

            nested_test("axis_array") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) axis_array(contract_daf, "cell")
            end

            nested_test("axis_dict") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) axis_dict(contract_daf, "cell")
            end

            nested_test("axis_indices") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) axis_indices(contract_daf, "cell", ["A", "B"])
            end

            nested_test("axis_length") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) axis_length(contract_daf, "cell")
            end

            nested_test("axis_version_counter") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) axis_version_counter(contract_daf, "cell")
            end

            nested_test("depict") do
                @test depict(contract_daf) == "Contract MemoryDaf memory!.for.computation"
            end

            nested_test("description") do
                @test description(contract_daf) == dedent("""
                    name: memory!
                    type: MemoryDaf
                    scalars:
                      version: 1 (Int64)
                    axes:
                      cell: 2 entries
                      gene: 3 entries
                    vectors:
                      cell:
                        age: 2 x Float64 (Dense)
                    matrices:
                      cell,gene:
                        UMIs: 2 x 3 x Float64 in Columns (Dense)
                      gene,cell:
                        UMIs: 3 x 2 x Float64 in Columns (Dense)
                """) * "\n"
            end

            nested_test("empty_cache!") do
                @test empty_cache!(contract_daf) === nothing
            end

            nested_test("get_scalar") do
                @test_throws dedent("""
                    accessing non-contract scalar: version
                    for the computation: computation
                    on the daf data: memory!
                """) get_scalar(contract_daf, "version")
            end

            nested_test("has_scalar") do
                @test_throws dedent("""
                    accessing non-contract scalar: version
                    for the computation: computation
                    on the daf data: memory!
                """) has_scalar(contract_daf, "version")
            end

            nested_test("scalars_set") do
                @test scalars_set(contract_daf) === scalars_set(daf)
            end

            nested_test("has_axis") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) has_axis(contract_daf, "cell")
            end

            nested_test("vectors_set") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) vectors_set(contract_daf, "cell")
            end

            nested_test("matrices_set") do
                @test_throws dedent("""
                    accessing non-contract axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) matrices_set(contract_daf, "cell", "gene")
            end
        end

        nested_test("axes") do
            contract =
                Contract(; axes = ["cell" => (OptionalInput, "description"), "gene" => (OptionalInput, "description")])
            contract_daf = contractor("computation", contract, daf)

            nested_test("vectors_set") do
                @test vectors_set(contract_daf, "cell") === vectors_set(daf, "cell")
            end

            nested_test("get_vector") do
                @test_throws dedent("""
                    accessing non-contract vector: age
                    of the axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) get_vector(contract_daf, "cell", "age")
            end

            nested_test("has_vector") do
                @test_throws dedent("""
                    accessing non-contract vector: age
                    of the axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) has_vector(contract_daf, "cell", "age")
            end

            nested_test("vector_version_counter") do
                @test_throws dedent("""
                    accessing non-contract vector: age
                    of the axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) vector_version_counter(contract_daf, "cell", "age")
            end

            nested_test("matrices_set") do
                @test matrices_set(contract_daf, "cell", "gene") == matrices_set(daf, "cell", "gene")
            end

            nested_test("get_matrix") do
                @test_throws dedent("""
                    accessing non-contract matrix: UMIs
                    of the rows axis: cell
                    and the columns axis: gene
                    for the computation: computation
                    on the daf data: memory!
                """) get_matrix(contract_daf, "cell", "gene", "UMIs")
            end

            nested_test("has_matrix") do
                @test_throws dedent("""
                    accessing non-contract matrix: UMIs
                    of the rows axis: cell
                    and the columns axis: gene
                    for the computation: computation
                    on the daf data: memory!
                """) has_matrix(contract_daf, "cell", "gene", "UMIs")
            end

            nested_test("matrix_version_counter") do
                @test_throws dedent("""
                    accessing non-contract matrix: UMIs
                    of the rows axis: cell
                    and the columns axis: gene
                    for the computation: computation
                    on the daf data: memory!
                """) matrix_version_counter(contract_daf, "cell", "gene", "UMIs")
            end
        end

        nested_test("full") do
            contract = Contract(;
                axes = ["cell" => (OptionalInput, "description"), "gene" => (OptionalInput, "description")],
                data = [
                    "version" => (OptionalInput, StorageInteger, "description"),
                    ("cell", "age") => (OptionalInput, StorageFloat, "description"),
                    ("cell", "gene", "UMIs") => (OptionalInput, StorageFloat, "description"),
                ],
            )
            contract_daf = contractor("computation", contract, daf)

            nested_test("axes_set") do
                @test axes_set(contract_daf) === axes_set(daf)
            end

            nested_test("axis_array") do
                @test axis_array(contract_daf, "cell") === axis_array(daf, "cell")
            end

            nested_test("axis_dict") do
                @test axis_dict(contract_daf, "cell") === axis_dict(daf, "cell")
            end

            nested_test("axis_indices") do
                @test axis_indices(contract_daf, "cell", ["A", "B"]) == axis_indices(daf, "cell", ["A", "B"])
            end

            nested_test("axis_length") do
                @test axis_length(contract_daf, "cell") == axis_length(daf, "cell")
            end

            nested_test("axis_version_counter") do
                @test axis_version_counter(contract_daf, "cell") == axis_version_counter(daf, "cell")
            end

            nested_test("empty_cache!") do
                @test empty_cache!(contract_daf) === nothing
            end

            nested_test("get_scalar") do
                @test get_scalar(contract_daf, "version") == get_scalar(daf, "version")
            end

            nested_test("has_scalar") do
                @test has_scalar(contract_daf, "version") == has_scalar(daf, "version")
            end

            nested_test("scalars_set") do
                @test scalars_set(contract_daf) == scalars_set(daf)
            end

            nested_test("has_axis") do
                @test has_axis(contract_daf, "cell") == has_axis(daf, "cell")
            end

            nested_test("vectors_set") do
                @test vectors_set(contract_daf, "cell") == vectors_set(daf, "cell")
            end

            nested_test("matrices_set") do
                @test matrices_set(contract_daf, "cell", "gene") == matrices_set(daf, "cell", "gene")
            end

            nested_test("get_vector") do
                @test get_vector(contract_daf, "cell", "age") == get_vector(daf, "cell", "age")
            end

            nested_test("has_vector") do
                @test has_vector(contract_daf, "cell", "age") == has_vector(daf, "cell", "age")
            end

            nested_test("vector_version_counter") do
                @test vector_version_counter(contract_daf, "cell", "age") == vector_version_counter(daf, "cell", "age")
            end

            nested_test("matrices_set") do
                @test matrices_set(contract_daf, "cell", "gene") == matrices_set(daf, "cell", "gene")
            end

            nested_test("get_matrix") do
                @test get_matrix(contract_daf, "cell", "gene", "UMIs") == get_matrix(daf, "cell", "gene", "UMIs")
            end

            nested_test("has_matrix") do
                @test has_matrix(contract_daf, "cell", "gene", "UMIs") == has_matrix(daf, "cell", "gene", "UMIs")
            end

            nested_test("matrix_version_counter") do
                @test matrix_version_counter(contract_daf, "cell", "gene", "UMIs") ==
                      matrix_version_counter(daf, "cell", "gene", "UMIs")
            end

            nested_test("add_axis!") do
                @test_throws dedent("""
                    modifying OptionalInput axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) add_axis!(contract_daf, "cell", ["C", "D"])
            end

            nested_test("delete_axis!") do
                @test_throws dedent("""
                    modifying OptionalInput axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) delete_axis!(contract_daf, "cell")
            end

            nested_test("delete_matrix!") do
                @test_throws dedent("""
                    modifying OptionalInput matrix: UMIs
                    of the rows_axis: cell
                    and the columns_axis: gene
                    for the computation: computation
                    on the daf data: memory!
                """) delete_matrix!(contract_daf, "cell", "gene", "UMIs")
            end

            nested_test("delete_vector!") do
                @test_throws dedent("""
                    modifying OptionalInput vector: age
                    of the axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) delete_vector!(contract_daf, "cell", "age")
            end

            nested_test("delete_scalar!") do
                @test_throws dedent("""
                    modifying OptionalInput scalar: version
                    for the computation: computation
                    on the daf data: memory!
                """) delete_scalar!(contract_daf, "version")
            end

            #src/contracts.jl:787: - function Writers.empty_dense_matrix!(
            #src/contracts.jl:800: - function Writers.empty_dense_vector!(
            #src/contracts.jl:812: - function Writers.empty_sparse_matrix!(
            #src/contracts.jl:837: - function Writers.empty_sparse_vector!(
            #src/contracts.jl:851: - function Writers.filled_empty_dense_matrix!(
            #src/contracts.jl:863: - function Writers.filled_empty_dense_vector!(
            #src/contracts.jl:874: - function Writers.filled_empty_sparse_matrix!(
            #src/contracts.jl:889: - function Writers.filled_empty_sparse_vector!(
            #src/contracts.jl:902: - function Writers.get_empty_dense_matrix!(
            #src/contracts.jl:915: - function Writers.get_empty_dense_vector!(
            #src/contracts.jl:926: - function Writers.get_empty_sparse_matrix!(
            #src/contracts.jl:949: - function Writers.get_empty_sparse_vector!(

            nested_test("relayout_matrix!") do
                @test_throws dedent("""
                    modifying OptionalInput matrix: UMIs
                    of the rows_axis: cell
                    and the columns_axis: gene
                    for the computation: computation
                    on the daf data: memory!
                """) relayout_matrix!(contract_daf, "cell", "gene", "UMIs")
            end

            nested_test("set_scalar!") do
                @test_throws dedent("""
                    modifying OptionalInput scalar: version
                    for the computation: computation
                    on the daf data: memory!
                """) set_scalar!(contract_daf, "version", 2)
            end

            nested_test("set_matrix!") do
                @test_throws dedent("""
                    modifying OptionalInput matrix: UMIs
                    of the rows_axis: cell
                    and the columns_axis: gene
                    for the computation: computation
                    on the daf data: memory!
                """) set_matrix!(contract_daf, "cell", "gene", "UMIs", [0 1 2; 3 4 5])
            end

            nested_test("set_vector!") do
                @test_throws dedent("""
                    modifying OptionalInput vector: age
                    of the axis: cell
                    for the computation: computation
                    on the daf data: memory!
                """) set_vector!(contract_daf, "cell", "age", [1, 2])
            end
        end

        nested_test("fill") do
            contract = Contract(;
                axes = ["cell" => (OptionalOutput, "description"), "gene" => (OptionalOutput, "description")],
                data = [
                    "version" => (OptionalOutput, StorageInteger, "description"),
                    ("cell", "age") => (OptionalOutput, StorageFloat, "description"),
                    ("cell", "gene", "UMIs") => (OptionalOutput, StorageFloat, "description"),
                ],
            )
            contract_daf = contractor("computation", contract, MemoryDaf())

            @test set_scalar!(contract_daf, "version", 1) === nothing
            @test add_axis!(contract_daf, "cell", ["A", "B"]) === nothing
            @test add_axis!(contract_daf, "gene", ["X", "Y", "Z"]) === nothing
            @test set_vector!(contract_daf, "cell", "age", [1.0, 2.0]) === nothing
            @test set_matrix!(contract_daf, "cell", "gene", "UMIs", [0.0 1.0 2.0; 3.0 4.0 5.0]) === nothing

            nested_test("delete_axis!") do
                @test delete_axis!(contract_daf, "cell") == nothing
                @assert !has_axis(contract_daf, "cell")
            end

            nested_test("delete_matrix!") do
                @test delete_matrix!(contract_daf, "cell", "gene", "UMIs") == nothing
                @assert !has_matrix(contract_daf, "cell", "gene", "UMIs")
            end

            nested_test("delete_vector!") do
                @test delete_vector!(contract_daf, "cell", "age") == nothing
                @assert !has_vector(contract_daf, "cell", "age")
            end

            nested_test("delete_scalar!") do
                @test delete_scalar!(contract_daf, "version") == nothing
                @assert !has_scalar(contract_daf, "version")
            end

            nested_test("empty_dense_matrix!") do
                umis = rand(Float32, 2, 3)
                @test empty_dense_matrix!(contract_daf, "cell", "gene", "UMIs", Float32; overwrite = true) do empty
                    empty .= umis
                    return true
                end
                @test get_matrix(contract_daf, "cell", "gene", "UMIs") == umis
            end

            nested_test("empty_dense_vector!") do
                ages = rand(Float32, 2)
                @test empty_dense_vector!(contract_daf, "cell", "age", Float32; overwrite = true) do empty
                    empty .= ages
                    return true
                end
                @test get_vector(contract_daf, "cell", "age") == ages
            end

            nested_test("empty_sparse_matrix!") do
                umis = sprand(Float32, 2, 3, 0.5)
                @test empty_sparse_matrix!(
                    contract_daf,
                    "cell",
                    "gene",
                    "UMIs",
                    Float32,
                    nnz(umis),
                    Int32;
                    overwrite = true,
                ) do colptr, rowval, nzval
                    colptr .= umis.colptr
                    rowval .= umis.rowval
                    nzval .= umis.nzval
                    return true
                end
                @test get_matrix(contract_daf, "cell", "gene", "UMIs") == umis
            end

            nested_test("empty_sparse_vector!") do
                ages = sprand(Float32, 2, 0.5)
                @test empty_sparse_vector!(
                    contract_daf,
                    "cell",
                    "age",
                    Float32,
                    nnz(ages),
                    Int32;
                    overwrite = true,
                ) do nzind, nzval
                    nzind .= ages.nzind
                    nzval .= ages.nzval
                    return true
                end
                @test get_vector(contract_daf, "cell", "age") == ages
            end

            nested_test("relayout_matrix!") do
                @test relayout_matrix!(contract_daf, "cell", "gene", "UMIs"; overwrite = true) == nothing
            end
        end
    end
end
