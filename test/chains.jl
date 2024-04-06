nested_test("chains") do
    first = MemoryDaf(; name = "first!")
    second = MemoryDaf(; name = "second!")

    nested_test("empty") do
        nested_test("name") do
            nested_test("reader") do
                @test_throws dedent("""
                    empty chain: chain!
                """) chain_reader(Vector{DafReader}(); name = "chain!")
            end

            nested_test("writer") do
                @test_throws dedent("""
                    empty chain: chain!
                """) chain_writer(Vector{DafWriter}(); name = "chain!")
            end
        end

        nested_test("!name") do
            nested_test("reader") do
                @test_throws dedent("""
                    empty chain
                """) chain_reader(Vector{DafReader}())
            end

            nested_test("writer") do
                @test_throws dedent("""
                    empty chain
                """) chain_writer(Vector{DafWriter}())
            end
        end
    end

    nested_test("one") do
        nested_test("reader") do
            read_first = read_only(first)
            read_chain = chain_reader([read_first])
            @assert read_chain === read_first
        end

        nested_test("writer") do
            write_chain = chain_writer([first])
            @assert write_chain === first
        end
    end

    nested_test("two") do
        @test_throws "read-only final data: second!\nin write chain: chain!" chain_writer(
            [first, read_only(second)];
            name = "chain!",
        )
        read_chain = chain_reader([first, second])
        @assert read_only(read_chain) === read_chain
        @assert read_only(read_chain; name = "read-only first!;second!") !== read_chain
        write_chain = chain_writer([first, second])
        @assert read_only(write_chain) !== write_chain
    end

    nested_test("access") do
        for (name, type_name, chain) in [
            ("read", "ReadOnly", chain_reader([first, read_only(second)]; name = "chain!")),
            ("write", "Write", chain_writer([first, second]; name = "chain!")),
        ]
            nested_test(name) do
                @test describe(chain) == "$(type_name) Chain chain!"

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
                                    type: $(type_name) Chain
                                    scalars:
                                      version: 2.0 (Float64)
                                """) * "\n"
                            end

                            nested_test("!deep") do
                                @test description(chain; deep = false) == dedent("""
                                    name: chain!
                                    type: $(type_name) Chain
                                    scalars:
                                      version: 2.0 (Float64)
                                  """) * "\n"
                            end

                            nested_test("deep") do
                                @test description(chain; deep = true) == dedent("""
                                    name: chain!
                                    type: $(type_name) Chain
                                    scalars:
                                      version: 2.0 (Float64)
                                    chain:
                                    - name: first!
                                      type: MemoryDaf
                                      scalars:
                                        version: 1.0 (Float64)
                                    - name: second!
                                      type: MemoryDaf
                                      scalars:
                                        version: 2.0 (Float64)
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
                            in the daf data: first!
                            and the daf data: second!
                            in the chain: chain!
                        """) chain_reader([first, second]; name = "chain!")
                        @test !has_axis(chain, "gene")
                    end
                end

                nested_test("vector") do
                    nested_test("first") do
                        add_axis!(first, "cell", ["A", "B"])
                        previous_version_counter = vector_version_counter(chain, "cell", "age")
                        set_vector!(first, "cell", "age", [1, 2])
                        @test vector_version_counter(chain, "cell", "age") == previous_version_counter + 1
                        @test has_vector(chain, "cell", "age")
                        @test get_vector(chain, "cell", "age") == [1, 2]
                        @test vector_names(chain, "cell") == Set(["age"])
                        @test !has_vector(chain, "cell", "batch")
                    end

                    nested_test("second") do
                        add_axis!(second, "cell", ["A", "B"])
                        previous_version_counter = vector_version_counter(chain, "cell", "age")
                        set_vector!(second, "cell", "age", [2, 3])
                        @test vector_version_counter(chain, "cell", "age") == previous_version_counter + 1
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
        end
    end

    nested_test("write") do
        chain = chain_writer([read_only(first), second]; name = "chain!")

        nested_test("scalar") do
            @test !has_scalar(first, "version")
            @test !has_scalar(second, "version")
            @test !has_scalar(chain, "version")

            nested_test("add") do
                set_scalar!(chain, "version", 1.0)
                @test !has_scalar(first, "version")
                @test get_scalar(second, "version") == 1.0
                @test get_scalar(chain, "version") == 1.0
                @test delete_scalar!(chain, "version") === nothing
                @test !has_scalar(second, "version")
                @test !has_scalar(chain, "version")
            end

            nested_test("override") do
                set_scalar!(first, "version", 1.0)
                @test get_scalar(first, "version") == 1.0
                @test get_scalar(chain, "version") == 1.0
                @test_throws dedent("""
                    failed to delete the scalar: version
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_scalar!(chain, "version")
                set_scalar!(chain, "version", 2.0; overwrite = true)
                @test get_scalar(first, "version") == 1.0
                @test get_scalar(second, "version") == 2.0
                @test get_scalar(chain, "version") == 2.0
                @test_throws dedent("""
                    failed to delete the scalar: version
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_scalar!(chain, "version")
            end

            nested_test("change") do
                set_scalar!(first, "version", 1.0)
                set_scalar!(second, "version", 2.0)
                @test get_scalar(first, "version") == 1.0
                @test get_scalar(second, "version") == 2.0
                @test get_scalar(chain, "version") == 2.0
                @test_throws dedent("""
                    failed to delete the scalar: version
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_scalar!(chain, "version")
                set_scalar!(chain, "version", 3.0; overwrite = true)
                @test get_scalar(first, "version") == 1.0
                @test get_scalar(second, "version") == 3.0
                @test get_scalar(chain, "version") == 3.0
                @test_throws dedent("""
                    failed to delete the scalar: version
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_scalar!(chain, "version")
            end
        end

        nested_test("axis") do
            @test !has_axis(first, "cell")
            @test !has_axis(second, "cell")
            @test !has_axis(chain, "cell")

            nested_test("late") do
                add_axis!(chain, "cell", ["A", "B"])
                @test !has_axis(first, "cell")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_axis(chain, "cell") == ["A", "B"]
                @test delete_axis!(chain, "cell") === nothing
                @test !has_axis(second, "cell")
                @test !has_axis(chain, "cell")
            end

            nested_test("early") do
                add_axis!(first, "cell", ["A", "B"])
                @test get_axis(first, "cell") == ["A", "B"]
                @test !has_axis(second, "cell")
                @test get_axis(chain, "cell") == ["A", "B"]
                @test_throws dedent("""
                    failed to delete the axis: cell
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_axis!(chain, "cell")
            end

            nested_test("both") do
                add_axis!(first, "cell", ["A", "B"])
                add_axis!(second, "cell", ["A", "B"])
                @test get_axis(first, "cell") == ["A", "B"]
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_axis(chain, "cell") == ["A", "B"]
                @test_throws dedent("""
                    failed to delete the axis: cell
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_axis!(chain, "cell")
            end
        end

        nested_test("vector") do
            add_axis!(first, "cell", ["A", "B"])
            nested_test("add") do
                @test set_vector!(chain, "cell", "age", [1, 2]) === nothing
                @test !has_vector(first, "cell", "age")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_vector(second, "cell", "age") == [1, 2]
                @test get_vector(chain, "cell", "age") == [1, 2]
                @test delete_vector!(chain, "cell", "age") === nothing
                @test has_axis(second, "cell")
                @test !has_vector(chain, "cell", "age")
            end

            nested_test("empty_dense") do
                @test empty_dense_vector!(chain, "cell", "age", Int16) do empty
                    empty .= [1, 2]
                    return 7
                end == 7
                @test !has_vector(first, "cell", "age")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_vector(second, "cell", "age") == [1, 2]
                @test get_vector(chain, "cell", "age") == [1, 2]
                @test delete_vector!(chain, "cell", "age") === nothing
                @test has_axis(second, "cell")
                @test !has_vector(chain, "cell", "age")
            end

            nested_test("empty_sparse") do
                @test empty_sparse_vector!(chain, "cell", "age", Int16, 2, Int16) do nzind, nzval
                    sparse = sparse_vector([1, 2])
                    nzind .= sparse.nzind
                    nzval .= sparse.nzval
                    return 7
                end == 7
                @test !has_vector(first, "cell", "age")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_vector(second, "cell", "age") == [1, 2]
                @test get_vector(chain, "cell", "age") == [1, 2]
                @test delete_vector!(chain, "cell", "age") === nothing
                @test has_axis(second, "cell")
                @test !has_vector(chain, "cell", "age")
            end

            nested_test("override") do
                @test set_vector!(first, "cell", "age", [1, 2]) === nothing
                @test get_vector(chain, "cell", "age") == [1, 2]
                @test set_vector!(chain, "cell", "age", [2, 3]; overwrite = true) === nothing
                @test get_vector(second, "cell", "age") == [2, 3]
                @test get_vector(chain, "cell", "age") == [2, 3]
                @test_throws dedent("""
                    failed to delete the vector: age
                    of the axis: cell
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_vector!(chain, "cell", "age")
            end

            nested_test("change") do
                add_axis!(second, "cell", ["A", "B"])
                @test set_vector!(first, "cell", "age", [1, 2]) === nothing
                @test set_vector!(second, "cell", "age", [2, 3]) === nothing
                @test get_vector(chain, "cell", "age") == [2, 3]
                @test set_vector!(chain, "cell", "age", [3, 4]; overwrite = true) === nothing
                @test get_vector(first, "cell", "age") == [1, 2]
                @test get_vector(second, "cell", "age") == [3, 4]
                @test get_vector(chain, "cell", "age") == [3, 4]
                @test_throws dedent("""
                    failed to delete the vector: age
                    of the axis: cell
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_vector!(chain, "cell", "age")
            end
        end

        nested_test("matrix") do
            add_axis!(first, "cell", ["A", "B"])
            add_axis!(first, "gene", ["X", "Y", "Z"])
            nested_test("add") do
                @test set_matrix!(chain, "cell", "gene", "UMIs", [0 1 2; 3 4 5]) === nothing
                @test !has_matrix(first, "cell", "gene", "UMIs")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_axis(second, "gene") == ["X", "Y", "Z"]
                @test get_matrix(second, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test get_matrix(chain, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test delete_matrix!(chain, "cell", "gene", "UMIs") === nothing
                @test has_axis(second, "cell")
                @test has_axis(second, "gene")
                @test !has_matrix(chain, "cell", "gene", "UMIs")
            end

            nested_test("empty_dense") do
                @test empty_dense_matrix!(chain, "cell", "gene", "UMIs", Int16) do empty
                    empty .= [0 1 2; 3 4 5]
                    return 17
                end == 17
                @test !has_matrix(first, "cell", "gene", "UMIs")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_axis(second, "gene") == ["X", "Y", "Z"]
                @test get_matrix(second, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test get_matrix(chain, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test delete_matrix!(chain, "cell", "gene", "UMIs") === nothing
                @test has_axis(second, "cell")
                @test has_axis(second, "gene")
                @test !has_matrix(chain, "cell", "gene", "UMIs")
            end

            nested_test("empty_sparse") do
                @test empty_sparse_matrix!(chain, "cell", "gene", "UMIs", Int16, 5, Int16) do colptr, rowval, nzval
                    sparse = sparse_matrix_csc([0 1 2; 3 4 5])
                    colptr .= sparse.colptr
                    rowval .= sparse.rowval
                    nzval .= sparse.nzval
                    return 17
                end == 17
                @test !has_matrix(first, "cell", "gene", "UMIs")
                @test get_axis(second, "cell") == ["A", "B"]
                @test get_axis(second, "gene") == ["X", "Y", "Z"]
                @test get_matrix(second, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test get_matrix(chain, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test delete_matrix!(chain, "cell", "gene", "UMIs") === nothing
                @test has_axis(second, "cell")
                @test has_axis(second, "gene")
                @test !has_matrix(chain, "cell", "gene", "UMIs")
            end

            nested_test("override") do
                @test set_matrix!(first, "cell", "gene", "UMIs", [0 1 2; 3 4 5]) === nothing
                @test get_matrix(chain, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test set_matrix!(chain, "cell", "gene", "UMIs", [1 2 3; 4 5 6]; overwrite = true) === nothing
                @test get_matrix(second, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                @test get_matrix(chain, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                @test_throws dedent("""
                    failed to delete the matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_matrix!(chain, "cell", "gene", "UMIs")
            end

            nested_test("change") do
                add_axis!(second, "cell", ["A", "B"])
                add_axis!(second, "gene", ["X", "Y", "Z"])
                @test set_matrix!(first, "cell", "gene", "UMIs", [0 1 2; 3 4 5]) === nothing
                @test set_matrix!(second, "cell", "gene", "UMIs", [1 2 3; 4 5 6]) === nothing
                @test get_matrix(chain, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                @test set_matrix!(chain, "cell", "gene", "UMIs", [2 3 4; 5 6 7]; overwrite = true) === nothing
                @test get_matrix(first, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
                @test get_matrix(second, "cell", "gene", "UMIs") == [2 3 4; 5 6 7]
                @test get_matrix(chain, "cell", "gene", "UMIs") == [2 3 4; 5 6 7]
                @test_throws dedent("""
                    failed to delete the matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    from the daf data: second!
                    of the chain: chain!
                    because it exists in the earlier: first!
                """) delete_matrix!(chain, "cell", "gene", "UMIs")
            end
        end
    end
end
