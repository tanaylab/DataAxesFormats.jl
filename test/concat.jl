nested_test("concat") do
    into = MemoryDaf(; name = "into!")
    from = [MemoryDaf(; name = "from.1!"), MemoryDaf(; name = "from.2!")]
    add_axis!(from[1], "cell", ["A", "B"])
    add_axis!(from[2], "cell", ["C", "D", "E"])

    nested_test("!square") do
        set_matrix!(from[1], "cell", "cell", "outgoing_edges", [1.0 0.0; 0.0 1.0])
        @test_throws dedent("""
            can't concatenate the matrix: outgoing_edges
            for the concatenated rows axis: cell
            and the concatenated columns axis: cell
            in the daf data: from.1!
            concatenated into the daf data: into!
        """) concatenate(into, "cell", from)
    end

    nested_test("!entries") do
        add_axis!(from[1], "gene", ["X", "Y"])
        add_axis!(from[2], "gene", ["X", "Y", "Z"])
        @test_throws dedent("""
            different entries for the axis: gene
            between the daf data: from.1!
            and the daf data: from.2!
            concatenated into the daf data: into!
        """) concatenate(into, "cell", from)
    end

    nested_test("concatenate") do
        nested_test("empty") do
            concatenate(into, "cell", from)
            @test get_axis(into, "cell") == ["A", "B", "C", "D", "E"]
            @test get_axis(into, "dataset") == ["from.1!", "from.2!"]
            @test get_vector(into, "cell", "dataset") == ["from.1!", "from.1!", "from.2!", "from.2!", "from.2!"]
        end

        nested_test("!dataset_axis") do
            concatenate(into, "cell", from; dataset_axis = nothing)
            @test get_axis(into, "cell") == ["A", "B", "C", "D", "E"]
            @test !has_axis(into, "dataset")
            @test !has_vector(into, "cell", "dataset")
        end

        nested_test("!dataset_property") do
            concatenate(into, "cell", from; dataset_property = false)
            @test get_axis(into, "cell") == ["A", "B", "C", "D", "E"]
            @test get_axis(into, "dataset") == ["from.1!", "from.2!"]
            @test !has_vector(into, "cell", "dataset")
        end

        nested_test("vector") do
            nested_test("!string") do
                set_vector!(from[1], "cell", "age", [1, 2])
                set_vector!(from[2], "cell", "age", [3, 4, 5])
                concatenate(into, "cell", from)
                @test get_vector(into, "cell", "age") == [1, 2, 3, 4, 5]
            end

            nested_test("string") do
                set_vector!(from[1], "cell", "color", ["red", "green"])
                set_vector!(from[2], "cell", "color", ["blue", "red", "yellow"])
                concatenate(into, "cell", from)
                @test get_vector(into, "cell", "color") == ["red", "green", "blue", "red", "yellow"]
            end

            nested_test("string!") do
                set_vector!(from[1], "cell", "color", [1, 2])
                set_vector!(from[2], "cell", "color", ["blue", "red", "yellow"])
                concatenate(into, "cell", from)
                @test get_vector(into, "cell", "color") == ["1", "2", "blue", "red", "yellow"]
            end

            nested_test("!empty") do
                set_vector!(from[1], "cell", "color", ["red", "green"])
                @test_throws dedent("""
                    no empty value for the vector: color
                        of the axis: cell
                        which is missing from the daf data: from.2!
                        concatenated into the daf data: into!
                """) concatenate(into, "cell", from)
            end

            nested_test("empty") do
                set_vector!(from[1], "cell", "color", ["red", "green"])
                concatenate(into, "cell", from; empty = Dict(("cell", "color") => "black"))
                @test get_vector(into, "cell", "color") == ["red", "green", "black", "black", "black"]
            end
        end

        nested_test("matrix") do
            add_axis!(from[1], "gene", ["X", "Y"])
            add_axis!(from[2], "gene", ["X", "Y"])
            set_matrix!(from[1], "cell", "gene", "UMIs", [1 2; 3 4])
            set_matrix!(from[2], "cell", "gene", "UMIs", [5 6; 7 8; 9 10])
            concatenate(into, "cell", from)
            @test get_matrix(into, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6; 7 8; 9 10]
        end
    end

    nested_test("prefix") do
        add_axis!(from[1], "metacell", ["M1"])
        add_axis!(from[2], "metacell", ["M1", "M2"])
        set_vector!(from[1], "cell", "metacell", ["M1", "M1"])
        set_vector!(from[2], "cell", "metacell", ["M1", "M2", "M1"])
        set_vector!(from[1], "cell", "!metacell", ["M1", "M1"])
        set_vector!(from[2], "cell", "!metacell", ["M1", "M2", "M1"])

        nested_test("false") do
            @test_throws dedent("""
                non-unique entries for new axis: metacell
                in the daf data: into!
            """) concatenate(into, ["cell", "metacell"], from)
        end

        nested_test("true") do
            concatenate(into, ["cell", "metacell"], from; prefix = [false, true])
            @test get_axis(into, "cell") == ["A", "B", "C", "D", "E"]
            @test get_axis(into, "metacell") == ["from.1!.M1", "from.2!.M1", "from.2!.M2"]
            @test get_axis(into, "dataset") == ["from.1!", "from.2!"]
            @test get_vector(into, "cell", "dataset") == ["from.1!", "from.1!", "from.2!", "from.2!", "from.2!"]
            @test get_vector(into, "metacell", "dataset") == ["from.1!", "from.2!", "from.2!"]
            @test get_vector(into, "cell", "metacell") ==
                  ["from.1!.M1", "from.1!.M1", "from.2!.M1", "from.2!.M2", "from.2!.M1"]
            @test get_vector(into, "cell", "!metacell") == ["M1", "M1", "M1", "M2", "M1"]
        end

        nested_test("names") do
            concatenate(into, ["cell", "metacell"], from; prefix = [false, true], names = ["D1", "D2"])
            @test get_axis(into, "cell") == ["A", "B", "C", "D", "E"]
            @test get_axis(into, "metacell") == ["D1.M1", "D2.M1", "D2.M2"]
            @test get_axis(into, "dataset") == ["D1", "D2"]
            @test get_vector(into, "cell", "dataset") == ["D1", "D1", "D2", "D2", "D2"]
            @test get_vector(into, "metacell", "dataset") == ["D1", "D2", "D2"]
            @test get_vector(into, "cell", "metacell") == ["D1.M1", "D1.M1", "D2.M1", "D2.M2", "D2.M1"]
            @test get_vector(into, "cell", "!metacell") == ["M1", "M1", "M1", "M2", "M1"]
        end

        nested_test("prefixes") do
            concatenate(
                into,
                ["cell", "metacell"],
                from;
                prefix = [false, true],
                prefixed = [Set(["metacell", "!metacell"]), Set{String}()],
            )
            @test get_axis(into, "cell") == ["A", "B", "C", "D", "E"]
            @test get_axis(into, "metacell") == ["from.1!.M1", "from.2!.M1", "from.2!.M2"]
            @test get_axis(into, "dataset") == ["from.1!", "from.2!"]
            @test get_vector(into, "cell", "dataset") == ["from.1!", "from.1!", "from.2!", "from.2!", "from.2!"]
            @test get_vector(into, "metacell", "dataset") == ["from.1!", "from.2!", "from.2!"]
            @test get_vector(into, "cell", "metacell") ==
                  ["from.1!.M1", "from.1!.M1", "from.2!.M1", "from.2!.M2", "from.2!.M1"]
            @test get_vector(into, "cell", "!metacell") ==
                  ["from.1!.M1", "from.1!.M1", "from.2!.M1", "from.2!.M2", "from.2!.M1"]
        end
    end

    nested_test("sparse") do
        nested_test("vector") do
            nested_test("dense") do
                set_vector!(from[1], "cell", "age", SparseVector([1, 2]))
                set_vector!(from[2], "cell", "age", SparseVector([3, 4, 5]))
                concatenate(into, "cell", from)
                @test get_vector(into, "cell", "age") == [1, 2, 3, 4, 5]
                @test !(get_vector(into, "cell", "age").array isa SparseVector)
            end

            nested_test("sparse") do
                set_vector!(from[1], "cell", "age", SparseVector([1, 0]))
                set_vector!(from[2], "cell", "age", SparseVector([0, 0, 2]))
                concatenate(into, "cell", from)
                @test get_vector(into, "cell", "age") == [1, 0, 0, 0, 2]
                @test get_vector(into, "cell", "age").array isa SparseVector
            end

            nested_test("!empty") do
                set_vector!(from[1], "cell", "age", [1, 2])
                @test_throws dedent("""
                    nested task error: no empty value for the vector: age
                        of the axis: cell
                        which is missing from the daf data: from.2!
                        concatenated into the daf data: into!
                """) concatenate(into, "cell", from)
            end

            nested_test("~empty") do
                set_vector!(from[1], "cell", "age", [1, 2])
                @test_throws dedent("""
                    nested task error: no empty value for the vector: age
                        of the axis: cell
                        which is missing from the daf data: from.2!
                        concatenated into the daf data: into!
                """) concatenate(into, "cell", from; empty = Dict("version" => 0))
            end

            nested_test("empty") do
                nested_test("zero") do
                    set_vector!(from[1], "cell", "age", [1, 2])
                    concatenate(into, "cell", from; empty = Dict(("cell", "age") => 0))
                    @test get_vector(into, "cell", "age") == [1, 2, 0, 0, 0]
                    @test get_vector(into, "cell", "age").array isa SparseVector
                end

                nested_test("!zero") do
                    set_vector!(from[1], "cell", "age", [1, 0])
                    concatenate(into, "cell", from; empty = Dict(("cell", "age") => 2))
                    @test get_vector(into, "cell", "age") == [1, 0, 2, 2, 2]
                    @test !(get_vector(into, "cell", "age").array isa SparseVector)
                end
            end
        end

        nested_test("matrix") do
            add_axis!(from[1], "gene", ["X", "Y"])
            add_axis!(from[2], "gene", ["X", "Y"])

            nested_test("dense") do
                set_matrix!(from[1], "cell", "gene", "UMIs", SparseMatrixCSC([1 2; 3 4]))
                set_matrix!(from[2], "cell", "gene", "UMIs", SparseMatrixCSC([5 6; 7 8; 9 10]))
                concatenate(into, "cell", from)
                @test get_matrix(into, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6; 7 8; 9 10]
                @test !(get_matrix(into, "cell", "gene", "UMIs").array isa SparseMatrixCSC)
            end

            nested_test("sparse") do
                set_matrix!(from[1], "cell", "gene", "UMIs", SparseMatrixCSC([1 0; 0 2]))
                set_matrix!(from[2], "cell", "gene", "UMIs", SparseMatrixCSC([0 3; 4 0; 0 5]))
                concatenate(into, "cell", from)
                @test get_matrix(into, "cell", "gene", "UMIs") == [1 0; 0 2; 0 3; 4 0; 0 5]
                @test get_matrix(into, "cell", "gene", "UMIs").array isa SparseMatrixCSC
            end

            nested_test("!empty") do
                set_matrix!(from[1], "cell", "gene", "UMIs", SparseMatrixCSC([1 2; 3 4]))
                @test_throws dedent("""
                    nested task error: no empty value for the matrix: UMIs
                        of the rows axis: gene
                        and the columns axis: cell
                        which is missing from the daf data: from.2!
                        concatenated into the daf data: into!
                """) concatenate(into, "cell", from)
            end

            nested_test("~empty") do
                set_matrix!(from[1], "cell", "gene", "UMIs", SparseMatrixCSC([1 2; 3 4]))
                @test_throws dedent("""
                    nested task error: no empty value for the matrix: UMIs
                        of the rows axis: gene
                        and the columns axis: cell
                        which is missing from the daf data: from.2!
                        concatenated into the daf data: into!
                """) concatenate(into, "cell", from; empty = Dict("version" => 0))
            end

            nested_test("empty") do
                nested_test("zero") do
                    set_matrix!(from[1], "cell", "gene", "UMIs", SparseMatrixCSC([1 2; 3 4]))
                    concatenate(into, "cell", from; empty = Dict(("cell", "gene", "UMIs") => 0))
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2; 3 4; 0 0; 0 0; 0 0]
                    @test get_matrix(into, "cell", "gene", "UMIs").array isa SparseMatrixCSC
                end

                nested_test("!zero") do
                    set_matrix!(from[1], "cell", "gene", "UMIs", SparseMatrixCSC([1 2; 3 4]))
                    concatenate(into, "cell", from; empty = Dict(("gene", "cell", "UMIs") => 5))
                    @test get_matrix(into, "cell", "gene", "UMIs") == [1 2; 3 4; 5 5; 5 5; 5 5]
                    @test !(get_matrix(into, "cell", "gene", "UMIs").array isa SparseMatrixCSC)
                end
            end
        end
    end

    nested_test("merge") do
        nested_test("scalar") do
            set_scalar!(from[1], "version", 1)
            set_scalar!(from[2], "version", 2)

            nested_test("skip") do
                concatenate(into, "cell", from; merge = [ALL_VECTORS => LastValue])
                @test !has_scalar(into, "version")
                @test !has_vector(into, "dataset", "version")
            end

            nested_test("last") do
                concatenate(into, "cell", from; merge = ["version" => LastValue])
                @test get_scalar(into, "version") == 2
                @test !has_vector(into, "dataset", "version")
            end

            nested_test("collect") do
                concatenate(into, "cell", from; merge = [ALL_SCALARS => CollectAxis])
                @test !has_scalar(into, "version")
                @test get_vector(into, "dataset", "version") == [1, 2]
            end

            nested_test("!collect") do
                @test_throws dedent("""
                    can't collect axis for the scalar: version
                    of the daf data sets concatenated into the daf data: into!
                    because no data set axis was created
                """) concatenate(
                    into,
                    "cell",
                    from;
                    dataset_axis = nothing,
                    merge = [ALL_SCALARS => CollectAxis],
                    prefixed = Set{String}(),
                )
            end
        end

        nested_test("vector") do
            add_axis!(from[1], "gene", ["X", "Y"])
            add_axis!(from[2], "gene", ["X", "Y"])
            set_vector!(from[1], "gene", "weight", [UInt8(1), UInt8(2)])
            set_vector!(from[2], "gene", "weight", [UInt16(3), UInt16(4)])

            nested_test("skip") do
                concatenate(into, "cell", from; merge = [ALL_SCALARS => LastValue])
                @test !has_vector(into, "gene", "weight")
                @test !has_matrix(into, "dataset", "gene", "weight")
            end

            nested_test("last") do
                concatenate(into, "cell", from; merge = [("gene", "weight") => LastValue])
                @test get_vector(into, "gene", "weight") == [3, 4]
                @test !has_matrix(into, "dataset", "gene", "weight")
            end

            nested_test("collect") do
                nested_test("dense") do
                    nested_test("full") do
                        concatenate(into, "cell", from; merge = [ALL_VECTORS => CollectAxis])
                        @test !has_vector(into, "gene", "weight")
                        @test get_matrix(into, "dataset", "gene", "weight") == [1 2; 3 4]
                        @test eltype(get_matrix(into, "dataset", "gene", "weight")) == UInt16
                    end

                    nested_test("empty") do
                        delete_vector!(from[2], "gene", "weight")

                        nested_test("zero") do
                            concatenate(
                                into,
                                "cell",
                                from;
                                merge = [ALL_VECTORS => CollectAxis],
                                empty = Dict(("gene", "weight") => 0.0),
                            )
                            @test !has_vector(into, "gene", "weight")
                            @test get_matrix(into, "dataset", "gene", "weight") == [1.0 2.0; 0.0 0.0]
                            @test eltype(get_matrix(into, "dataset", "gene", "weight")) == Float64
                            @test get_matrix(into, "dataset", "gene", "weight").array isa SparseMatrixCSC
                        end

                        nested_test("!zero") do
                            concatenate(
                                into,
                                "cell",
                                from;
                                merge = [ALL_VECTORS => CollectAxis],
                                empty = Dict(("gene", "weight") => Int32(3)),
                            )
                            @test !has_vector(into, "gene", "weight")
                            @test get_matrix(into, "dataset", "gene", "weight") == [1 2; 3 3]
                            @test eltype(get_matrix(into, "dataset", "gene", "weight")) == Int32
                            @test !(get_matrix(into, "dataset", "gene", "weight").array isa SparseMatrixCSC)
                        end
                    end
                end

                nested_test("sparse") do
                    set_vector!(from[1], "gene", "weight", SparseVector([1, 0]); overwrite = true)
                    set_vector!(from[2], "gene", "weight", SparseVector([0, 2]); overwrite = true)
                    concatenate(into, "cell", from; merge = [ALL_VECTORS => CollectAxis])
                    @test !has_vector(into, "gene", "weight")
                    @test get_matrix(into, "dataset", "gene", "weight") == [1 0; 0 2]
                    @test get_matrix(into, "dataset", "gene", "weight").array isa SparseMatrixCSC
                end
            end

            nested_test("!collect") do
                @test_throws dedent("""
                    can't collect axis for the vector: weight
                    of the axis: gene
                    of the daf data sets concatenated into the daf data: into!
                    because no data set axis was created
                """) concatenate(into, "cell", from; dataset_axis = nothing, merge = [ALL_VECTORS => CollectAxis])
            end
        end

        nested_test("matrix") do
            add_axis!(from[1], "gene", ["X", "Y"])
            add_axis!(from[2], "gene", ["X", "Y"])

            nested_test("square") do
                set_matrix!(from[1], "gene", "gene", "outgoing_edges", [1 2; 3 4])
                set_matrix!(from[2], "gene", "gene", "outgoing_edges", [5 6; 7 8])

                nested_test("skip") do
                    concatenate(into, "cell", from; merge = [ALL_VECTORS => LastValue])
                    @test !has_matrix(into, "gene", "gene", "outgoing_edges")
                end

                nested_test("last") do
                    concatenate(into, "cell", from; merge = [("gene", "gene", "outgoing_edges") => LastValue])
                    @test get_matrix(into, "gene", "gene", "outgoing_edges") == [5 6; 7 8]
                end

                nested_test("!collect") do
                    @test_throws dedent("""
                        can't collect axis for the matrix: outgoing_edges
                        of the rows axis: gene
                        and the columns axis: gene
                        of the daf data sets concatenated into the daf data: into!
                        because that would create a 3D tensor, which is not supported
                    """) concatenate(into, "cell", from; merge = [ALL_MATRICES => CollectAxis])
                end
            end

            nested_test("rectangle") do
                add_axis!(from[1], "batch", ["B1", "B2"])
                add_axis!(from[2], "batch", ["B1", "B2"])

                set_matrix!(from[1], "gene", "batch", "scale", [1 2; 3 4])
                set_matrix!(from[2], "gene", "batch", "scale", [5 6; 7 8])

                nested_test("skip") do
                    concatenate(into, "cell", from; merge = [ALL_VECTORS => LastValue])
                    @test !has_matrix(into, "gene", "batch", "scale")
                end

                nested_test("last") do
                    concatenate(into, "cell", from; merge = [("gene", "batch", "scale") => LastValue])
                    @test get_matrix(into, "gene", "batch", "scale") == [5 6; 7 8]
                end

                nested_test("!collect") do
                    @test_throws dedent("""
                        can't collect axis for the matrix: scale
                        of the rows axis: batch
                        and the columns axis: gene
                        of the daf data sets concatenated into the daf data: into!
                        because that would create a 3D tensor, which is not supported
                    """) concatenate(into, "cell", from; merge = [ALL_MATRICES => CollectAxis])
                end
            end
        end
    end
end
