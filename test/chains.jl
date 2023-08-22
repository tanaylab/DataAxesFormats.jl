nested_test("chains") do
    nested_test("empty") do
        @test_throws dedent("""
            empty chain: chain!
        """) chain_reader("chain!", Vector{DafReader}())
    end

    first = MemoryDaf("first!")
    second = MemoryDaf("second!")
    chain = chain_reader("chain!", [first, read_only(second)])

    nested_test("scalar") do
        nested_test("first") do
            set_scalar!(first, "version", 1.0)
            @test has_scalar(chain, "version")
            @test get_scalar(chain, "version") == 1.0
            @test scalar_names(chain) == Set(["version"])
            @test !has_scalar(chain, "author")
        end

        nested_test("second") do
            set_scalar!(second, "version", 2.0)
            @test has_scalar(chain, "version")
            @test get_scalar(chain, "version") == 2.0
            @test scalar_names(chain) == Set(["version"])
            @test !has_scalar(chain, "author")
        end

        nested_test("both") do
            set_scalar!(first, "version", 1.0)
            set_scalar!(second, "version", 2.0)
            @test has_scalar(chain, "version")
            @test get_scalar(chain, "version") == 2.0
            @test scalar_names(chain) == Set(["version"])
            @test !has_scalar(chain, "author")
            nested_test("description") do
                nested_test("()") do
                    @test description(chain) == dedent("""
                        name: chain!
                        type: ReadOnly Chain
                        scalars:
                          version: 2.0
                    """) * "\n"
                end

                nested_test("!deep") do
                    @test description(chain; deep = false) == dedent("""
                        name: chain!
                        type: ReadOnly Chain
                        scalars:
                          version: 2.0
                      """) * "\n"
                end

                nested_test("deep") do
                    @test description(chain; deep = true) == dedent("""
                        name: chain!
                        type: ReadOnly Chain
                        scalars:
                          version: 2.0
                        chain:
                        - name: first!
                          type: MemoryDaf
                          scalars:
                            version: 1.0
                        - name: second!
                          type: MemoryDaf
                          scalars:
                            version: 2.0
                      """) * "\n"
                end
            end
        end
    end

    nested_test("axis") do
        nested_test("first") do
            add_axis!(first, "cell", ["A", "B"])
            @test has_axis(chain, "cell")
            @test get_axis(chain, "cell") == ["A", "B"]
            @test axis_names(chain) == Set(["cell"])
            @test !has_axis(chain, "gene")
        end

        nested_test("second") do
            add_axis!(second, "cell", ["A", "B"])
            @test has_axis(chain, "cell")
            @test get_axis(chain, "cell") == ["A", "B"]
            @test axis_names(chain) == Set(["cell"])
            @test !has_axis(chain, "gene")
        end

        nested_test("both") do
            add_axis!(first, "cell", ["A", "B"])
            add_axis!(second, "cell", ["A", "B"])
            @test has_axis(chain, "cell")
            @test get_axis(chain, "cell") == ["A", "B"]
            @test axis_names(chain) == Set(["cell"])
            @test !has_axis(chain, "gene")
        end

        nested_test("!both") do
            add_axis!(first, "cell", ["A", "B"])
            add_axis!(second, "cell", ["A", "C"])
            @test_throws dedent("""
                different entries for the axis: cell
                in the Daf data: first!
                and the Daf data: second!
                in the chain: chain!
            """) chain_reader("chain!", [first, second])
            @test !has_axis(chain, "gene")
        end
    end

    nested_test("vector") do
        nested_test("first") do
            add_axis!(first, "cell", ["A", "B"])
            set_vector!(first, "cell", "age", [1, 2])
            @test has_vector(chain, "cell", "age")
            @test get_vector(chain, "cell", "age") == [1, 2]
            @test vector_names(chain, "cell") == Set(["age"])
            @test !has_vector(chain, "cell", "batch")
        end

        nested_test("second") do
            add_axis!(second, "cell", ["A", "B"])
            set_vector!(second, "cell", "age", [2, 3])
            @test has_vector(chain, "cell", "age")
            @test get_vector(chain, "cell", "age") == [2, 3]
            @test vector_names(chain, "cell") == Set(["age"])
            @test !has_vector(chain, "cell", "batch")
        end

        nested_test("both") do
            add_axis!(first, "cell", ["A", "B"])
            set_vector!(first, "cell", "age", [1, 2])
            add_axis!(second, "cell", ["A", "B"])
            set_vector!(second, "cell", "age", [2, 3])
            @test has_vector(chain, "cell", "age")
            @test get_vector(chain, "cell", "age") == [2, 3]
            @test vector_names(chain, "cell") == Set(["age"])
            @test !has_vector(chain, "cell", "batch")
        end
    end

    nested_test("matrix") do
        nested_test("first") do
            add_axis!(first, "cell", ["A", "B"])
            add_axis!(first, "gene", ["X", "Y", "Z"])
            set_matrix!(first, "cell", "gene", "UMIs", [0 1 2; 3 4 5])
            @test has_matrix(chain, "cell", "gene", "UMIs")
            @test get_matrix(chain, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
            @test matrix_names(chain, "cell", "gene") == Set(["UMIs"])
            @test !has_matrix(chain, "cell", "gene", "fraction")
        end

        nested_test("second") do
            add_axis!(second, "cell", ["A", "B"])
            add_axis!(second, "gene", ["X", "Y", "Z"])
            set_matrix!(second, "cell", "gene", "UMIs", [5 4 3; 2 1 0])
            @test has_matrix(chain, "cell", "gene", "UMIs")
            @test get_matrix(chain, "cell", "gene", "UMIs") == [5 4 3; 2 1 0]
            @test matrix_names(chain, "cell", "gene") == Set(["UMIs"])
            @test !has_matrix(chain, "cell", "gene", "fraction")
        end

        nested_test("both") do
            add_axis!(first, "cell", ["A", "B"])
            add_axis!(first, "gene", ["X", "Y", "Z"])
            set_matrix!(first, "cell", "gene", "UMIs", [0 1 2; 3 4 5])
            add_axis!(second, "cell", ["A", "B"])
            add_axis!(second, "gene", ["X", "Y", "Z"])
            set_matrix!(second, "cell", "gene", "UMIs", [5 4 3; 2 1 0])
            @test has_matrix(chain, "cell", "gene", "UMIs")
            @test get_matrix(chain, "cell", "gene", "UMIs") == [5 4 3; 2 1 0]
            @test matrix_names(chain, "cell", "gene") == Set(["UMIs"])
            @test !has_matrix(chain, "cell", "gene", "fraction")
        end
    end
end
