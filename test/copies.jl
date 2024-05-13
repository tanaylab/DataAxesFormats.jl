nested_test("copies") do
    source = MemoryDaf(; name = "source!")
    destination = MemoryDaf(; name = "destination!")

    nested_test("scalar") do
        nested_test("missing") do
            nested_test("()") do
                @test_throws dedent("""
                    missing scalar: version
                    in the daf data: source!
                """) copy_scalar!(source = source, destination = destination, name = "version")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing scalar: version
                        in the daf data: source!
                    """) copy_scalar!(source = source, destination = destination, name = "version"; default = undef)
                end

                nested_test("nothing") do
                    @test copy_scalar!(;
                        source = source,
                        destination = destination,
                        name = "version",
                        default = nothing,
                    ) === nothing
                    @test !has_scalar(destination, "version")
                end

                nested_test("value") do
                    @test copy_scalar!(;
                        source = source,
                        destination = destination,
                        name = "version",
                        default = "2.0",
                    ) === nothing
                    @test get_scalar(destination, "version") == "2.0"
                end
            end
        end

        nested_test("existing") do
            @test set_scalar!(source, "version", "1.0") === nothing

            nested_test("()") do
                @test copy_scalar!(; source = source, destination = destination, name = "version") === nothing
                @test get_scalar(destination, "version") == "1.0"
            end

            nested_test("default") do
                @test copy_scalar!(; source = source, destination = destination, name = "version", default = "2.0") ==
                      nothing
                @test get_scalar(destination, "version") == "1.0"
            end

            nested_test("dtype") do
                nested_test("same") do
                    @test copy_scalar!(;
                        source = source,
                        destination = destination,
                        name = "version",
                        dtype = String,
                    ) == nothing
                    @test get_scalar(destination, "version") == "1.0"
                end

                nested_test("parse") do
                    @test copy_scalar!(;
                        source = source,
                        destination = destination,
                        name = "version",
                        dtype = Float32,
                    ) == nothing
                    @test get_scalar(destination, "version") isa Float32
                    @test get_scalar(destination, "version") == 1.0
                end

                nested_test("string") do
                    @test set_scalar!(source, "version", 1.0; overwrite = true) === nothing
                    @test copy_scalar!(;
                        source = source,
                        destination = destination,
                        name = "version",
                        dtype = String,
                    ) == nothing
                    @test get_scalar(destination, "version") == "1.0"
                end

                nested_test("convert") do
                    @test set_scalar!(source, "version", 1.0; overwrite = true) === nothing
                    @test copy_scalar!(; source = source, destination = destination, name = "version", dtype = Int32) ==
                          nothing
                    @test get_scalar(destination, "version") isa Int32
                    @test get_scalar(destination, "version") == 1
                end
            end
        end
    end

    nested_test("axis") do
        nested_test("missing") do
            @test_throws dedent("""
                missing axis: cell
                in the daf data: source!
            """) copy_axis!(source = source, destination = destination, axis = "cell")
        end

        nested_test("existing") do
            @test add_axis!(source, "cell", ["A", "B"]) === nothing
            @test copy_axis!(; source = source, destination = destination, axis = "cell") === nothing
            @test axis_array(destination, "cell") == ["A", "B"]
        end
    end

    nested_test("vector") do
        @test add_axis!(source, "cell", ["A", "B"]) === nothing

        nested_test("!axis") do
            nested_test("()") do
                @test_throws dedent("""
                    missing axis: cell
                    in the daf data: destination!
                """) copy_vector!(source = source, destination = destination, axis = "cell", name = "age")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: destination!
                    """) copy_vector!(
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age";
                        default = undef,
                    )
                end

                nested_test("nothing") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: destination!
                    """) copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        default = nothing,
                    ) === nothing
                end

                nested_test("value") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: destination!
                    """) copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        default = [1, 2],
                    ) === nothing
                end
            end
        end

        nested_test("missing") do
            @test add_axis!(destination, "cell", ["A", "B"]) === nothing

            nested_test("()") do
                @test_throws dedent("""
                    missing vector: age
                    for the axis: cell
                    in the daf data: source!
                """) copy_vector!(source = source, destination = destination, axis = "cell", name = "age")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing vector: age
                        for the axis: cell
                        in the daf data: source!
                    """) copy_vector!(
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age";
                        default = undef,
                    )
                end

                nested_test("nothing") do
                    @test copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        default = nothing,
                    ) === nothing
                    @test !has_vector(destination, "cell", "age")
                end

                nested_test("value") do
                    @test copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        default = [1, 2],
                    ) === nothing
                    @test get_vector(destination, "cell", "age") == [1, 2]
                end
            end
        end

        nested_test("dense") do
            @test set_vector!(source, "cell", "age", [1, 2]) === nothing

            nested_test("existing") do
                @test add_axis!(destination, "cell", ["A", "B"]) === nothing

                nested_test("()") do
                    @test copy_vector!(; source = source, destination = destination, axis = "cell", name = "age") ==
                          nothing
                    @test get_vector(destination, "cell", "age") == [1, 2]
                end

                nested_test("default") do
                    @test copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        default = [2, 3],
                    ) === nothing
                    @test get_vector(destination, "cell", "age") == [1, 2]
                end

                nested_test("convert") do
                    @test copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        dtype = Float32,
                    ) == nothing
                    @test get_vector(destination, "cell", "age") == [1.0, 2.0]
                    @test eltype(get_vector(destination, "cell", "age")) == Float32
                end
            end

            nested_test("subset") do
                @test add_axis!(destination, "cell", ["A"]) === nothing

                @test copy_vector!(; source = source, destination = destination, axis = "cell", name = "age") ===
                      nothing
                @test get_vector(destination, "cell", "age") == [1]
            end

            nested_test("superset") do
                @test add_axis!(destination, "cell", ["A", "B", "C"]) === nothing

                nested_test("()") do
                    @test_throws dedent("""
                        missing entries in the axis: cell
                        of the source daf data: source!
                        which are needed for copying the vector: age
                        to the vector: age
                        of the axis: cell
                        of the target daf data: destination!
                    """) copy_vector!(source = source, destination = destination, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            missing entries in the axis: cell
                            of the source daf data: source!
                            which are needed for copying the vector: age
                            to the vector: age
                            of the axis: cell
                            of the target daf data: destination!
                        """) copy_vector!(
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = nothing,
                        )
                    end

                    nested_test("value") do
                        @test copy_vector!(;
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = -1,
                        ) === nothing
                        @test get_vector(destination, "cell", "age") == [1, 2, -1]
                    end
                end
            end

            nested_test("disjoint") do
                @test add_axis!(destination, "cell", ["B", "C"]) === nothing

                nested_test("()") do
                    @test_throws dedent("""
                        disjoint entries in the axis: cell
                        of the source daf data: source!
                        and the axis: cell
                        of the target daf data: destination!
                    """) copy_vector!(source = source, destination = destination, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: source!
                            and the axis: cell
                            of the target daf data: destination!
                        """) copy_vector!(
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = nothing,
                        )
                    end

                    nested_test("value") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: source!
                            and the axis: cell
                            of the target daf data: destination!
                        """) copy_vector!(
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = nothing,
                        )
                    end
                end
            end
        end

        nested_test("sparse") do
            @test set_vector!(source, "cell", "age", sparse_vector([1, 0])) === nothing

            nested_test("existing") do
                @test add_axis!(destination, "cell", ["A", "B"]) === nothing

                nested_test("()") do
                    @test copy_vector!(; source = source, destination = destination, axis = "cell", name = "age") ==
                          nothing
                    @test get_vector(destination, "cell", "age") == [1, 0]
                    @test nnz(get_vector(destination, "cell", "age").array) == 1
                end

                nested_test("default") do
                    @test copy_vector!(;
                        source = source,
                        destination = destination,
                        axis = "cell",
                        name = "age",
                        default = [2, 3],
                    ) === nothing
                    @test get_vector(destination, "cell", "age") == [1, 0]
                    @test nnz(get_vector(destination, "cell", "age").array) == 1
                end

                nested_test("dtype") do
                    nested_test("string") do
                        @test copy_vector!(;
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            dtype = String,
                        ) == nothing
                        @test get_vector(destination, "cell", "age") == ["1", "0"]
                    end

                    nested_test("convert") do
                        @test copy_vector!(;
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            dtype = Float32,
                        ) == nothing
                        @test get_vector(destination, "cell", "age") == [1.0, 0.0]
                        @test eltype(get_vector(destination, "cell", "age")) == Float32
                    end
                end
            end

            nested_test("subset") do
                @test add_axis!(destination, "cell", ["A"]) === nothing

                @test copy_vector!(; source = source, destination = destination, axis = "cell", name = "age") ===
                      nothing
                @test get_vector(destination, "cell", "age") == [1]
                @test nnz(get_vector(destination, "cell", "age").array) == 1
            end

            nested_test("superset") do
                @test add_axis!(destination, "cell", ["A", "B", "C"]) === nothing

                nested_test("()") do
                    @test_throws dedent("""
                        missing entries in the axis: cell
                        of the source daf data: source!
                        which are needed for copying the vector: age
                        to the vector: age
                        of the axis: cell
                        of the target daf data: destination!
                    """) copy_vector!(source = source, destination = destination, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            missing entries in the axis: cell
                            of the source daf data: source!
                            which are needed for copying the vector: age
                            to the vector: age
                            of the axis: cell
                            of the target daf data: destination!
                        """) copy_vector!(
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = nothing,
                        )
                    end

                    nested_test("!zero") do
                        @test copy_vector!(;
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = -1,
                        ) === nothing
                        @test get_vector(destination, "cell", "age") == [1, 0, -1]
                        @test nnz(get_vector(destination, "cell", "age").array) == 2
                    end

                    nested_test("zero") do
                        @test copy_vector!(;
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = 0,
                        ) === nothing
                        @test get_vector(destination, "cell", "age") == [1, 0, 0]
                        @test nnz(get_vector(destination, "cell", "age").array) == 1
                    end
                end
            end

            nested_test("disjoint") do
                @test add_axis!(destination, "cell", ["B", "C"]) === nothing

                nested_test("()") do
                    @test_throws dedent("""
                        disjoint entries in the axis: cell
                        of the source daf data: source!
                        and the axis: cell
                        of the target daf data: destination!
                    """) copy_vector!(source = source, destination = destination, axis = "cell", name = "age")
                end

                nested_test("empty") do
                    nested_test("nothing") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: source!
                            and the axis: cell
                            of the target daf data: destination!
                        """) copy_vector!(
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = nothing,
                        )
                    end

                    nested_test("value") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: source!
                            and the axis: cell
                            of the target daf data: destination!
                        """) copy_vector!(
                            source = source,
                            destination = destination,
                            axis = "cell",
                            name = "age",
                            empty = 1,
                        )
                    end
                end
            end
        end
    end

    nested_test("matrix") do
        @test add_axis!(source, "cell", ["A", "B"]) === nothing
        @test add_axis!(source, "gene", ["X", "Y", "Z"]) === nothing

        nested_test("!axis") do
            nested_test("()") do
                @test_throws dedent("""
                    missing axis: cell
                    in the daf data: destination!
                """) copy_matrix!(
                    source = source,
                    destination = destination,
                    rows_axis = "cell",
                    columns_axis = "gene",
                    name = "age",
                )

                @test add_axis!(destination, "cell", ["A", "B"]) === nothing

                @test_throws dedent("""
                    missing axis: gene
                    in the daf data: destination!
                """) copy_matrix!(
                    source = source,
                    destination = destination,
                    rows_axis = "cell",
                    columns_axis = "gene",
                    name = "UMIs",
                )
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: destination!
                    """) copy_matrix!(
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = undef,
                    )

                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing

                    @test_throws dedent("""
                        missing axis: gene
                        in the daf data: destination!
                    """) copy_matrix!(
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = undef,
                    )
                end

                nested_test("nothing") do
                    @test_throws dedent("""
                        missing axis: cell
                        in the daf data: destination!
                    """) copy_matrix!(
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = nothing,
                    )

                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing

                    @test_throws dedent("""
                        missing axis: gene
                        in the daf data: destination!
                    """) copy_matrix!(
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = nothing,
                    )
                end
            end
        end

        nested_test("missing") do
            @test add_axis!(destination, "cell", ["A", "B"]) === nothing
            @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

            nested_test("()") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    (and the other way around)
                    in the daf data: source!
                """) copy_matrix!(
                    source = source,
                    destination = destination,
                    rows_axis = "cell",
                    columns_axis = "gene",
                    name = "UMIs",
                )
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        missing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        (and the other way around)
                        in the daf data: source!
                    """) copy_matrix!(
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs";
                        default = undef,
                    )
                end

                nested_test("nothing") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = nothing,
                    ) === nothing
                    @test !has_matrix(destination, "cell", "gene", "UMIs")
                end

                nested_test("value") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = [1 2 3; 4 5 6],
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                end
            end
        end

        nested_test("dense") do
            @test set_matrix!(source, "cell", "gene", "UMIs", [1 2 3; 4 5 6]) === nothing

            nested_test("existing") do
                @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                nested_test("()") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                end

                nested_test("default") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = [2 3 4; 5 6 7],
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2 3; 4 5 6]
                end

                nested_test("convert") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        dtype = Float32,
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1.0 2.0 3.0; 4.0 5.0 6.0]
                    @test eltype(get_matrix(destination, "cell", "gene", "UMIs")) == Float32
                end
            end

            nested_test("subset") do
                nested_test("rows") do
                    @test add_axis!(destination, "cell", ["A"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2 3]
                end

                nested_test("columns") do
                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y"]) === nothing

                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2; 4 5]
                end
            end

            nested_test("superset") do
                nested_test("rows") do
                    @test add_axis!(destination, "cell", ["A", "B", "C"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries in the axis: cell
                            of the source daf data: source!
                            which are needed for copying the matrix: UMIs
                            to the matrix: UMIs
                            of the axis: cell
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries in the axis: cell
                                of the source daf data: source!
                                which are needed for copying the matrix: UMIs
                                to the matrix: UMIs
                                of the axis: cell
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test copy_matrix!(;
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) === nothing
                            @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2 3; 4 5 6; -1 -1 -1]
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                    @test add_axis!(destination, "gene", ["W", "X", "Y", "Z"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries in the axis: gene
                            of the source daf data: source!
                            which are needed for copying the matrix: UMIs
                            to the matrix: UMIs
                            of the axis: gene
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries in the axis: gene
                                of the source daf data: source!
                                which are needed for copying the matrix: UMIs
                                to the matrix: UMIs
                                of the axis: gene
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test copy_matrix!(;
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) === nothing
                            @test get_matrix(destination, "cell", "gene", "UMIs") == [-1 1 2 3; -1 4 5 6]
                        end
                    end
                end
            end

            nested_test("disjoint") do
                nested_test("rows") do
                    @test add_axis!(destination, "cell", ["B", "C"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: source!
                            and the axis: cell
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: source!
                                and the axis: cell
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: source!
                                and the axis: cell
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                    @test add_axis!(destination, "gene", ["W", "X", "Y"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: gene
                            of the source daf data: source!
                            and the axis: gene
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: source!
                                and the axis: gene
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: source!
                                and the axis: gene
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
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
            @test set_matrix!(source, "cell", "gene", "UMIs", sparse_matrix_csc([0 1 2; 3 4 0])) === nothing

            nested_test("existing") do
                @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                nested_test("()") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [0 1 2; 3 4 0]
                    @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 4
                end

                nested_test("default") do
                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                        default = [2 3 4; 5 6 7],
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [0 1 2; 3 4 0]
                    @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 4
                end
            end

            nested_test("subset") do
                nested_test("rows") do
                    @test add_axis!(destination, "cell", ["A"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [0 1 2]
                    @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 2
                end

                nested_test("columns") do
                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y"]) === nothing

                    @test copy_matrix!(;
                        source = source,
                        destination = destination,
                        rows_axis = "cell",
                        columns_axis = "gene",
                        name = "UMIs",
                    ) === nothing
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [0 1; 3 4]
                    @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 3
                end
            end

            nested_test("superset") do
                nested_test("rows") do
                    @test add_axis!(destination, "cell", ["A", "B", "C"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries in the axis: cell
                            of the source daf data: source!
                            which are needed for copying the matrix: UMIs
                            to the matrix: UMIs
                            of the axis: cell
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries in the axis: cell
                                of the source daf data: source!
                                which are needed for copying the matrix: UMIs
                                to the matrix: UMIs
                                of the axis: cell
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("!zero") do
                            @test copy_matrix!(;
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) === nothing
                            @test get_matrix(destination, "cell", "gene", "UMIs") == [0 1 2; 3 4 0; -1 -1 -1]
                            @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 7
                        end

                        nested_test("zero") do
                            @test copy_matrix!(;
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = 0,
                            ) === nothing
                            @test get_matrix(destination, "cell", "gene", "UMIs") == [0 1 2; 3 4 0; 0 0 0]
                            @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 4
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                    @test add_axis!(destination, "gene", ["W", "X", "Y", "Z"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            missing entries in the axis: gene
                            of the source daf data: source!
                            which are needed for copying the matrix: UMIs
                            to the matrix: UMIs
                            of the axis: gene
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                missing entries in the axis: gene
                                of the source daf data: source!
                                which are needed for copying the matrix: UMIs
                                to the matrix: UMIs
                                of the axis: gene
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("!zero") do
                            @test copy_matrix!(;
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = -1,
                            ) === nothing
                            @test get_matrix(destination, "cell", "gene", "UMIs") == [-1 0 1 2; -1 3 4 0]
                            @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 6
                        end

                        nested_test("zero") do
                            @test copy_matrix!(;
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = 0,
                            ) === nothing
                            @test get_matrix(destination, "cell", "gene", "UMIs") == [0 0 1 2; 0 3 4 0]
                            @test nnz(get_matrix(destination, "cell", "gene", "UMIs").array) == 4
                        end
                    end
                end
            end

            nested_test("disjoint") do
                nested_test("rows") do
                    @test add_axis!(destination, "cell", ["B", "C"]) === nothing
                    @test add_axis!(destination, "gene", ["X", "Y", "Z"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: cell
                            of the source daf data: source!
                            and the axis: cell
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: source!
                                and the axis: cell
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: cell
                                of the source daf data: source!
                                and the axis: cell
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end
                    end
                end

                nested_test("columns") do
                    @test add_axis!(destination, "cell", ["A", "B"]) === nothing
                    @test add_axis!(destination, "gene", ["W", "X", "Y"]) === nothing

                    nested_test("()") do
                        @test_throws dedent("""
                            disjoint entries in the axis: gene
                            of the source daf data: source!
                            and the axis: gene
                            of the target daf data: destination!
                        """) copy_matrix!(
                            source = source,
                            destination = destination,
                            rows_axis = "cell",
                            columns_axis = "gene",
                            name = "UMIs",
                        )
                    end

                    nested_test("empty") do
                        nested_test("nothing") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: source!
                                and the axis: gene
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
                                rows_axis = "cell",
                                columns_axis = "gene",
                                name = "UMIs",
                                empty = nothing,
                            )
                        end

                        nested_test("value") do
                            @test_throws dedent("""
                                disjoint entries in the axis: gene
                                of the source daf data: source!
                                and the axis: gene
                                of the target daf data: destination!
                            """) copy_matrix!(
                                source = source,
                                destination = destination,
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
            @test set_scalar!(source, "version", "1.0") === nothing
            @test add_axis!(source, "cell", ["A"]) === nothing
            @test add_axis!(source, "gene", ["W", "X"]) === nothing
            @test set_vector!(source, "cell", "age", [1.0]) === nothing
            @test set_matrix!(source, "cell", "gene", "UMIs", [1 2]) === nothing

            copy_all!(; source = source, destination = destination)

            @test get_scalar(destination, "version") == "1.0"
            @test axis_array(destination, "cell") == ["A"]
            @test get_vector(destination, "cell", "age") == [1.0]
            @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2]
        end

        nested_test("subset") do
            @test add_axis!(destination, "cell", ["A", "B"]) === nothing
            @test add_axis!(destination, "gene", ["W", "X", "Z"]) === nothing

            @test set_scalar!(source, "version", "1.5") === nothing
            @test add_axis!(source, "cell", ["A"]) === nothing
            @test add_axis!(source, "gene", ["W", "X"]) === nothing
            @test set_vector!(source, "cell", "age", [1.0]) === nothing
            @test set_matrix!(source, "cell", "gene", "UMIs", [1 2]) === nothing

            copy_all!(;
                source = source,
                destination = destination,
                empty = Dict(("cell", "age") => 0.0, ("gene", "cell", "UMIs") => 0),
                dtypes = Dict("version" => Float32),
            )

            @test get_scalar(destination, "version") == 1.5
            @test get_vector(destination, "cell", "age") == [1.0, 0.0]
            @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2 0; 0 0 0]
        end
    end
end
