nested_test("copies") do
    from = MemoryDaf(; name = "from!")
    into = MemoryDaf(; name = "into!")

    nested_test("scalar") do
        nested_test("missing") do
            nested_test("()") do
                @test_throws dedent("""
                    missing scalar: version
                    in the daf data: from!
                """) copy_scalar!(from = from, into = into, name = "version")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing scalar: version
                        in the daf data: from!
                    """) copy_scalar!(from = from, into = into, name = "version"; default = undef)
                end

                nested_test("nothing") do
                    @test copy_scalar!(; from = from, into = into, name = "version", default = nothing) == nothing
                    @test !has_scalar(into, "version")
                end

                nested_test("value") do
                    @test copy_scalar!(; from = from, into = into, name = "version", default = "2.0") == nothing
                    @test get_scalar(into, "version") == "2.0"
                end
            end
        end

        nested_test("existing") do
            @test set_scalar!(from, "version", "1.0") == nothing

            nested_test("()") do
                @test copy_scalar!(; from = from, into = into, name = "version") == nothing
                @test get_scalar(into, "version") == "1.0"
            end

            nested_test("default") do
                @test copy_scalar!(; from = from, into = into, name = "version", default = "2.0") == nothing
                @test get_scalar(into, "version") == "1.0"
            end
        end
    end

    nested_test("axis") do
        nested_test("missing") do
            @test_throws dedent("""
                missing axis: cell
                in the daf data: from!
            """) copy_axis!(from = from, into = into, name = "cell")
        end

        nested_test("existing") do
            @test add_axis!(from, "cell", ["A", "B"]) == nothing
            @test copy_axis!(; from = from, into = into, name = "cell") == nothing
            @test get_axis(into, "cell") == ["A", "B"]
        end
    end

    nested_test("vector") do
        @test add_axis!(from, "cell", ["A", "B"]) == nothing

        nested_test("!axis") do
            nested_test("()") do
                @test_throws dedent("""
                    missing axis: cell
                    in the daf data: into!
                """) copy_vector!(from = from, into = into, axis = "cell", name = "age")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: into!
                    """) copy_vector!(from = from, into = into, axis = "cell", name = "age"; default = undef)
                end

                nested_test("nothing") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: into!
                    """) copy_vector!(; from = from, into = into, axis = "cell", name = "age", default = nothing) ==
                         nothing
                end

                nested_test("value") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: into!
                    """) copy_vector!(; from = from, into = into, axis = "cell", name = "age", default = [1, 2]) ==
                         nothing
                end
            end
        end

        nested_test("missing") do
            @test add_axis!(into, "cell", ["A", "B"]) == nothing

            nested_test("()") do
                @test_throws dedent("""
                    missing vector: age
                    for the axis: cell
                    in the daf data: from!
                """) copy_vector!(from = from, into = into, axis = "cell", name = "age")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing vector: age
                        for the axis: cell
                        in the daf data: from!
                    """) copy_vector!(from = from, into = into, axis = "cell", name = "age"; default = undef)
                end

                nested_test("nothing") do
                    @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", default = nothing) ==
                          nothing
                    @test !has_vector(into, "cell", "age")
                end

                nested_test("value") do
                    @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", default = [1, 2]) ==
                          nothing
                    @test get_vector(into, "cell", "age") == [1, 2]
                end
            end
        end

        nested_test("dense") do
            @test set_vector!(from, "cell", "age", [1, 2]) == nothing

            nested_test("existing") do
                @test add_axis!(into, "cell", ["A", "B"]) == nothing

                nested_test("()") do
                    @test copy_vector!(; from = from, into = into, axis = "cell", name = "age") == nothing
                    @test get_vector(into, "cell", "age") == [1, 2]
                end

                nested_test("default") do
                    @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", default = [2, 3]) ==
                          nothing
                    @test get_vector(into, "cell", "age") == [1, 2]
                end
            end

            nested_test("subset") do
                @test add_axis!(into, "cell", ["A"]) == nothing

                @test copy_vector!(; from = from, into = into, axis = "cell", name = "age") == nothing
                @test get_vector(into, "cell", "age") == [1]
            end

            nested_test("superset") do
                @test add_axis!(into, "cell", ["A", "B", "C"]) == nothing

                nested_test("()") do
                    @test_throws dedent("""
                        missing entries from the axis: cell
                        of the source daf data: from!
                        which are needed for the axis: cell
                        of the target daf data: into!
                    """) copy_vector!(from = from, into = into, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            missing entries from the axis: cell
                            of the source daf data: from!
                            which are needed for the axis: cell
                            of the target daf data: into!
                        """) copy_vector!(from = from, into = into, axis = "cell", name = "age", empty = nothing)
                    end

                    nested_test("value") do
                        @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", empty = -1) ==
                              nothing
                        @test get_vector(into, "cell", "age") == [1, 2, -1]
                    end
                end
            end

            nested_test("disjoint") do
                @test add_axis!(into, "cell", ["B", "C"]) == nothing

                nested_test("()") do
                    @test_throws dedent("""
                        disjoint entries in the axis: cell
                        of the source daf data: from!
                        and the axis: cell
                        of the target daf data: into!
                    """) copy_vector!(from = from, into = into, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: from!
                            and the axis: cell
                            of the target daf data: into!
                        """) copy_vector!(from = from, into = into, axis = "cell", name = "age", empty = nothing)
                    end

                    nested_test("value") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: from!
                            and the axis: cell
                            of the target daf data: into!
                        """) copy_vector!(from = from, into = into, axis = "cell", name = "age", empty = nothing)
                    end
                end
            end
        end

        nested_test("sparse") do
            @test set_vector!(from, "cell", "age", SparseVector([1, 0])) == nothing

            nested_test("existing") do
                @test add_axis!(into, "cell", ["A", "B"]) == nothing

                nested_test("()") do
                    @test copy_vector!(; from = from, into = into, axis = "cell", name = "age") == nothing
                    @test get_vector(into, "cell", "age") == [1, 0]
                    @test nnz(get_vector(into, "cell", "age").array) == 1
                end

                nested_test("default") do
                    @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", default = [2, 3]) ==
                          nothing
                    @test get_vector(into, "cell", "age") == [1, 0]
                    @test nnz(get_vector(into, "cell", "age").array) == 1
                end
            end

            nested_test("subset") do
                @test add_axis!(into, "cell", ["A"]) == nothing

                @test copy_vector!(; from = from, into = into, axis = "cell", name = "age") == nothing
                @test get_vector(into, "cell", "age") == [1]
                @test nnz(get_vector(into, "cell", "age").array) == 1
            end

            nested_test("superset") do
                @test add_axis!(into, "cell", ["A", "B", "C"]) == nothing

                nested_test("()") do
                    @test_throws dedent("""
                        missing entries from the axis: cell
                        of the source daf data: from!
                        which are needed for the axis: cell
                        of the target daf data: into!
                    """) copy_vector!(from = from, into = into, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            missing entries from the axis: cell
                            of the source daf data: from!
                            which are needed for the axis: cell
                            of the target daf data: into!
                        """) copy_vector!(from = from, into = into, axis = "cell", name = "age", empty = nothing)
                    end

                    nested_test("!zero") do
                        @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", empty = -1) ==
                              nothing
                        @test get_vector(into, "cell", "age") == [1, 0, -1]
                        @test nnz(get_vector(into, "cell", "age").array) == 2
                    end

                    nested_test("zero") do
                        @test copy_vector!(; from = from, into = into, axis = "cell", name = "age", empty = 0) ==
                              nothing
                        @test get_vector(into, "cell", "age") == [1, 0, 0]
                        @test nnz(get_vector(into, "cell", "age").array) == 1
                    end
                end
            end

            nested_test("disjoint") do
                @test add_axis!(into, "cell", ["B", "C"]) == nothing

                nested_test("()") do
                    @test_throws dedent("""
                        disjoint entries in the axis: cell
                        of the source daf data: from!
                        and the axis: cell
                        of the target daf data: into!
                    """) copy_vector!(from = from, into = into, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: from!
                            and the axis: cell
                            of the target daf data: into!
                        """) copy_vector!(from = from, into = into, axis = "cell", name = "age", empty = nothing)
                    end

                    nested_test("value") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: from!
                            and the axis: cell
                            of the target daf data: into!
                        """) copy_vector!(from = from, into = into, axis = "cell", name = "age", empty = 1)
                    end
                end
            end
        end
    end

    nested_test("matrix") do
        @test add_axis!(from, "cell", ["A", "B"]) == nothing
        @test add_axis!(from, "gene", ["X", "Y", "Z"]) == nothing

        nested_test("!axis") do
            nested_test("()") do
                @test_throws dedent("""
                    missing axis: cell
                    in the daf data: into!
                """) copy_matrix!(from = from, into = into, rows_axis = "cell", columns_axis = "gene", name = "age")

                @test add_axis!(into, "cell", ["A", "B"]) == nothing

                @test_throws dedent("""
                    missing axis: gene
                    in the daf data: into!
                """) copy_matrix!(from = from, into = into, rows_axis = "cell", columns_axis = "gene", name = "UMIs")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: into!
                    """) copy_matrix!(
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = undef,
                    )

                    @test add_axis!(into, "cell", ["A", "B"]) == nothing

                    @test_throws dedent("""
                        missing axis: gene
                        in the daf data: into!
                    """) copy_matrix!(
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = undef,
                    )
                end

                nested_test("nothing") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: into!
                    """) copy_matrix!(
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = nothing,
                    )

                    @test add_axis!(into, "cell", ["A", "B"]) == nothing

                    @test_throws dedent("""
                        missing axis: gene
                        in the daf data: into!
                    """) copy_matrix!(
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = nothing,
                    )
                end
            end
        end

        nested_test("missing") do
            @test add_axis!(into, "cell", ["A", "B"]) == nothing
            @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

            nested_test("()") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    (and the other way around)
                    in the daf data: from!
                """) copy_matrix!(from = from, into = into, rows_axis = "cell", columns_axis = "gene", name = "UMIs")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        (and the other way around)
                        in the daf data: from!
                    """) copy_matrix!(
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = undef,
                    )
                end

                nested_test("nothing") do
                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = nothing,
                    ) == nothing
                    @test !has_matrix(into, "cell", "gene", "UMIs")
                end

                nested_test("value") do
                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = [1 2 3; 4 5 6],
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                end
            end
        end

        nested_test("dense") do
            @test set_matrix!(from, "cell", "gene", "UMIs", [1 2 3; 4 5 6]) == nothing

            nested_test("existing") do
                @test add_axis!(into, "cell", ["A", "B"]) == nothing
                @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                nested_test("()") do
                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                end

                nested_test("default") do
                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = [2 3 4; 5 6 7],
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                end
            end

            nested_test("subset") do
                nested_test("rows") do
                    @test add_axis!(into, "cell", ["A"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2 3]
                end

                nested_test("columns") do
                    @test add_axis!(into, "cell", ["A", "B"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y"]) == nothing

                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2; 4 5]
                end
            end

            nested_test("superset") do
                nested_test("rows") do
                    @test add_axis!(into, "cell", ["A", "B", "C"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries from the axis: cell
                            of the source daf data: from!
                            which are needed for the axis: cell
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries from the axis: cell
                                of the source daf data: from!
                                which are needed for the axis: cell
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test copy_matrix!(;
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) == nothing
                            @test get_matrix(into, "cell", "gene", "UMIs") == [1 2 3; 4 5 6; -1 -1 -1]
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(into, "cell", ["A", "B"]) == nothing
                    @test add_axis!(into, "gene", ["W", "X", "Y", "Z"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries from the axis: gene
                            of the source daf data: from!
                            which are needed for the axis: gene
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries from the axis: gene
                                of the source daf data: from!
                                which are needed for the axis: gene
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test copy_matrix!(;
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) == nothing
                            @test get_matrix(into, "cell", "gene", "UMIs") == [-1 1 2 3; -1 4 5 6]
                        end
                    end
                end
            end

            nested_test("disjoint") do
                nested_test("rows") do
                    @test add_axis!(into, "cell", ["B", "C"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: from!
                            and the axis: cell
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: from!
                                and the axis: cell
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: from!
                                and the axis: cell
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(into, "cell", ["A", "B"]) == nothing
                    @test add_axis!(into, "gene", ["W", "X", "Y"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: gene
                            of the source daf data: from!
                            and the axis: gene
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: from!
                                and the axis: gene
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: from!
                                and the axis: gene
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end
                    end
                end
            end
        end

        nested_test("sparse") do
            @test set_matrix!(from, "cell", "gene", "UMIs", SparseMatrixCSC([0 1 2; 3 4 0])) == nothing

            nested_test("existing") do
                @test add_axis!(into, "cell", ["A", "B"]) == nothing
                @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                nested_test("()") do
                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [0 1 2; 3 4 0]
                    @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 4
                end

                nested_test("default") do
                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = [2 3 4; 5 6 7],
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [0 1 2; 3 4 0]
                    @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 4
                end
            end

            nested_test("subset") do
                nested_test("rows") do
                    @test add_axis!(into, "cell", ["A"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [0 1 2]
                    @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 2
                end

                nested_test("columns") do
                    @test add_axis!(into, "cell", ["A", "B"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y"]) == nothing

                    @test copy_matrix!(;
                        from = from,
                        into = into,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) == nothing
                    @test get_matrix(into, "cell", "gene", "UMIs") == [0 1; 3 4]
                    @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 3
                end
            end

            nested_test("superset") do
                nested_test("rows") do
                    @test add_axis!(into, "cell", ["A", "B", "C"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries from the axis: cell
                            of the source daf data: from!
                            which are needed for the axis: cell
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries from the axis: cell
                                of the source daf data: from!
                                which are needed for the axis: cell
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("!zero") do
                            @test copy_matrix!(;
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) == nothing
                            @test get_matrix(into, "cell", "gene", "UMIs") == [0 1 2; 3 4 0; -1 -1 -1]
                            @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 7
                        end

                        nested_test("zero") do
                            @test copy_matrix!(;
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = 0,
                            ) == nothing
                            @test get_matrix(into, "cell", "gene", "UMIs") == [0 1 2; 3 4 0; 0 0 0]
                            @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 4
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(into, "cell", ["A", "B"]) == nothing
                    @test add_axis!(into, "gene", ["W", "X", "Y", "Z"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries from the axis: gene
                            of the source daf data: from!
                            which are needed for the axis: gene
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries from the axis: gene
                                of the source daf data: from!
                                which are needed for the axis: gene
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("!zero") do
                            @test copy_matrix!(;
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) == nothing
                            @test get_matrix(into, "cell", "gene", "UMIs") == [-1 0 1 2; -1 3 4 0]
                            @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 6
                        end

                        nested_test("zero") do
                            @test copy_matrix!(;
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = 0,
                            ) == nothing
                            @test get_matrix(into, "cell", "gene", "UMIs") == [0 0 1 2; 0 3 4 0]
                            @test nnz(get_matrix(into, "cell", "gene", "UMIs").array) == 4
                        end
                    end
                end
            end

            nested_test("disjoint") do
                nested_test("rows") do
                    @test add_axis!(into, "cell", ["B", "C"]) == nothing
                    @test add_axis!(into, "gene", ["X", "Y", "Z"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: from!
                            and the axis: cell
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: from!
                                and the axis: cell
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: from!
                                and the axis: cell
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(into, "cell", ["A", "B"]) == nothing
                    @test add_axis!(into, "gene", ["W", "X", "Y"]) == nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: gene
                            of the source daf data: from!
                            and the axis: gene
                            of the target daf data: into!
                        """) copy_matrix!(
                            from = from,
                            into = into,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: from!
                                and the axis: gene
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: from!
                                and the axis: gene
                                of the target daf data: into!
                            """) copy_matrix!(
                                from = from,
                                into = into,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end
                    end
                end
            end
        end
    end

    nested_test("all") do
        nested_test("empty") do
            @test set_scalar!(from, "version", "1.0") == nothing
            @test add_axis!(from, "cell", ["A"]) == nothing
            @test add_axis!(from, "gene", ["W", "X"]) == nothing
            @test set_vector!(from, "cell", "age", [1.0]) == nothing
            @test set_matrix!(from, "cell", "gene", "UMIs", [1 2]) == nothing

            copy_all!(; from = from, into = into)

            @test get_scalar(into, "version") == "1.0"
            @test get_axis(into, "cell") == ["A"]
            @test get_vector(into, "cell", "age") == [1.0]
            @test get_matrix(into, "cell", "gene", "UMIs") == [1 2]
        end

        nested_test("subset") do
            @test add_axis!(into, "cell", ["A", "B"]) == nothing
            @test add_axis!(into, "gene", ["W", "X", "Z"]) == nothing

            @test set_scalar!(from, "version", "1.0") == nothing
            @test add_axis!(from, "cell", ["A"]) == nothing
            @test add_axis!(from, "gene", ["W", "X"]) == nothing
            @test set_vector!(from, "cell", "age", [1.0]) == nothing
            @test set_matrix!(from, "cell", "gene", "UMIs", [1 2]) == nothing

            copy_all!(; from = from, into = into, empty = Dict(("cell", "age") => 0.0, ("gene", "cell", "UMIs") => 0))

            @test get_scalar(into, "version") == "1.0"
            @test get_vector(into, "cell", "age") == [1.0, 0.0]
            @test get_matrix(into, "cell", "gene", "UMIs") == [1 2 0; 0 0 0]
        end
    end
end
