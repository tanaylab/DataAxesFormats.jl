nested_test("concat") do
    destination = MemoryDaf(; name = "destination!")
    sources = [MemoryDaf(; name = "source.1!"), MemoryDaf(; name = "source.2!")]
    add_axis!(sources[1], "cell", ["A", "B"])
    add_axis!(sources[2], "cell", ["C", "D", "E"])

    nested_test("!square") do
        set_matrix!(sources[1], "cell", "cell", "outgoing_edges", [1.0 0.0; 0.0 1.0])
        @test_throws dedent("""
            can't concatenate the matrix: outgoing_edges
            for the concatenated rows axis: cell
            and the concatenated columns axis: cell
            in the daf data: source.1!
            concatenated into the daf data: destination!
        """) concatenate(destination, "cell", sources)
    end

    nested_test("!entries") do
        add_axis!(sources[1], "gene", ["X", "Y"])
        add_axis!(sources[2], "gene", ["X", "Y", "Z"])
        @test_throws dedent("""
            different entries for the axis: gene
            between the daf data: source.1!
            and the daf data: source.2!
            concatenated into the daf data: destination!
        """) concatenate(destination, "cell", sources)
    end

    nested_test("concatenate") do
        nested_test("empty") do
            concatenate(destination, "cell", sources)
            @test axis_array(destination, "cell") == ["A", "B", "C", "D", "E"]
            @test axis_array(destination, "dataset") == ["source.1!", "source.2!"]
            @test get_vector(destination, "cell", "dataset") ==
                  ["source.1!", "source.1!", "source.2!", "source.2!", "source.2!"]
        end

        nested_test("!dataset_axis") do
            concatenate(destination, "cell", sources; dataset_axis = nothing)
            @test axis_array(destination, "cell") == ["A", "B", "C", "D", "E"]
            @test !has_axis(destination, "dataset")
            @test !has_vector(destination, "cell", "dataset")
        end

        nested_test("!dataset_property") do
            concatenate(destination, "cell", sources; dataset_property = false)
            @test axis_array(destination, "cell") == ["A", "B", "C", "D", "E"]
            @test axis_array(destination, "dataset") == ["source.1!", "source.2!"]
            @test !has_vector(destination, "cell", "dataset")
        end

        nested_test("vector") do
            nested_test("!string") do
                set_vector!(sources[1], "cell", "age", [1, 2])
                set_vector!(sources[2], "cell", "age", [3, 4, 5])
                concatenate(destination, "cell", sources)
                @test get_vector(destination, "cell", "age") == [1, 2, 3, 4, 5]
            end

            nested_test("string") do
                set_vector!(sources[1], "cell", "color", ["red", "green"])
                set_vector!(sources[2], "cell", "color", ["blue", "red", "yellow"])
                concatenate(destination, "cell", sources)
                @test get_vector(destination, "cell", "color") == ["red", "green", "blue", "red", "yellow"]
            end

            nested_test("string!") do
                set_vector!(sources[1], "cell", "color", [1, 2])
                set_vector!(sources[2], "cell", "color", ["blue", "red", "yellow"])
                concatenate(destination, "cell", sources)
                @test get_vector(destination, "cell", "color") == ["1", "2", "blue", "red", "yellow"]
            end

            nested_test("!empty") do
                set_vector!(sources[1], "cell", "color", ["red", "green"])
                @test_throws dedent("""
                    no empty value for the vector: color
                        of the axis: cell
                        which is missing from the daf data: source.2!
                        concatenated into the daf data: destination!
                """) concatenate(destination, "cell", sources)
            end

            nested_test("empty") do
                set_vector!(sources[1], "cell", "color", ["red", "green"])
                concatenate(destination, "cell", sources; empty = Dict(("cell", "color") => "black"))
                @test get_vector(destination, "cell", "color") == ["red", "green", "black", "black", "black"]
            end
        end

        nested_test("matrix") do
            add_axis!(sources[1], "gene", ["X", "Y"])
            add_axis!(sources[2], "gene", ["X", "Y"])
            set_matrix!(sources[1], "cell", "gene", "UMIs", [1 2; 3 4])
            set_matrix!(sources[2], "cell", "gene", "UMIs", [5 6; 7 8; 9 10])
            concatenate(destination, "cell", sources)
            @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6; 7 8; 9 10]
        end
    end

    nested_test("prefix") do
        add_axis!(sources[1], "metacell", ["M1"])
        add_axis!(sources[2], "metacell", ["M1", "M2"])
        set_vector!(sources[1], "cell", "metacell", ["M1", "M1"])
        set_vector!(sources[2], "cell", "metacell", ["M1", "M2", "M1"])
        set_vector!(sources[1], "cell", "!metacell", ["M1", "M1"])
        set_vector!(sources[2], "cell", "!metacell", ["M1", "M2", "M1"])

        nested_test("false") do
            @test_throws dedent("""
                non-unique entries for new axis: metacell
                in the daf data: destination!
            """) concatenate(destination, ["cell", "metacell"], sources)
        end

        nested_test("true") do
            concatenate(destination, ["cell", "metacell"], sources; prefix = [false, true])
            @test axis_array(destination, "cell") == ["A", "B", "C", "D", "E"]
            @test axis_array(destination, "metacell") == ["source.1!.M1", "source.2!.M1", "source.2!.M2"]
            @test axis_array(destination, "dataset") == ["source.1!", "source.2!"]
            @test get_vector(destination, "cell", "dataset") ==
                  ["source.1!", "source.1!", "source.2!", "source.2!", "source.2!"]
            @test get_vector(destination, "metacell", "dataset") == ["source.1!", "source.2!", "source.2!"]
            @test get_vector(destination, "cell", "metacell") ==
                  ["source.1!.M1", "source.1!.M1", "source.2!.M1", "source.2!.M2", "source.2!.M1"]
            @test get_vector(destination, "cell", "!metacell") == ["M1", "M1", "M1", "M2", "M1"]
        end

        nested_test("names") do
            concatenate(destination, ["cell", "metacell"], sources; prefix = [false, true], names = ["D1", "D2"])
            @test axis_array(destination, "cell") == ["A", "B", "C", "D", "E"]
            @test axis_array(destination, "metacell") == ["D1.M1", "D2.M1", "D2.M2"]
            @test axis_array(destination, "dataset") == ["D1", "D2"]
            @test get_vector(destination, "cell", "dataset") == ["D1", "D1", "D2", "D2", "D2"]
            @test get_vector(destination, "metacell", "dataset") == ["D1", "D2", "D2"]
            @test get_vector(destination, "cell", "metacell") == ["D1.M1", "D1.M1", "D2.M1", "D2.M2", "D2.M1"]
            @test get_vector(destination, "cell", "!metacell") == ["M1", "M1", "M1", "M2", "M1"]
        end

        nested_test("prefixes") do
            concatenate(
                destination,
                ["cell", "metacell"],
                sources;
                prefix = [false, true],
                prefixed = [Set(["metacell", "!metacell"]), Set{String}()],
            )
            @test axis_array(destination, "cell") == ["A", "B", "C", "D", "E"]
            @test axis_array(destination, "metacell") == ["source.1!.M1", "source.2!.M1", "source.2!.M2"]
            @test axis_array(destination, "dataset") == ["source.1!", "source.2!"]
            @test get_vector(destination, "cell", "dataset") ==
                  ["source.1!", "source.1!", "source.2!", "source.2!", "source.2!"]
            @test get_vector(destination, "metacell", "dataset") == ["source.1!", "source.2!", "source.2!"]
            @test get_vector(destination, "cell", "metacell") ==
                  ["source.1!.M1", "source.1!.M1", "source.2!.M1", "source.2!.M2", "source.2!.M1"]
            @test get_vector(destination, "cell", "!metacell") ==
                  ["source.1!.M1", "source.1!.M1", "source.2!.M1", "source.2!.M2", "source.2!.M1"]
        end
    end

    nested_test("sparse") do
        nested_test("vector") do
            nested_test("dense") do
                set_vector!(sources[1], "cell", "age", sparse_vector([1, 2]))
                set_vector!(sources[2], "cell", "age", sparse_vector([3, 4, 5]))
                concatenate(destination, "cell", sources)
                @test get_vector(destination, "cell", "age") == [1, 2, 3, 4, 5]
                @test !(get_vector(destination, "cell", "age").array isa SparseVector)
            end

            nested_test("sparse") do
                set_vector!(sources[1], "cell", "age", sparse_vector([1, 0]))
                set_vector!(sources[2], "cell", "age", sparse_vector([0, 0, 2]))
                concatenate(destination, "cell", sources)
                @test get_vector(destination, "cell", "age") == [1, 0, 0, 0, 2]
                @test get_vector(destination, "cell", "age").array isa SparseVector
            end

            nested_test("!empty") do
                set_vector!(sources[1], "cell", "age", [1, 2])
                @test_throws dedent("""
                    nested task error: no empty value for the vector: age
                        of the axis: cell
                        which is missing from the daf data: source.2!
                        concatenated into the daf data: destination!
                """) concatenate(destination, "cell", sources)
            end

            nested_test("~empty") do
                set_vector!(sources[1], "cell", "age", [1, 2])
                @test_throws dedent("""
                    nested task error: no empty value for the vector: age
                        of the axis: cell
                        which is missing from the daf data: source.2!
                        concatenated into the daf data: destination!
                """) concatenate(destination, "cell", sources; empty = Dict("version" => 0))
            end

            nested_test("empty") do
                nested_test("zero") do
                    set_vector!(sources[1], "cell", "age", [1, 2])
                    concatenate(destination, "cell", sources; empty = Dict(("cell", "age") => 0))
                    @test get_vector(destination, "cell", "age") == [1, 2, 0, 0, 0]
                    @test get_vector(destination, "cell", "age").array isa SparseVector
                end

                nested_test("!zero") do
                    set_vector!(sources[1], "cell", "age", [1, 0])
                    concatenate(destination, "cell", sources; empty = Dict(("cell", "age") => 2))
                    @test get_vector(destination, "cell", "age") == [1, 0, 2, 2, 2]
                    @test !(get_vector(destination, "cell", "age").array isa SparseVector)
                end
            end
        end

        nested_test("matrix") do
            add_axis!(sources[1], "gene", ["X", "Y"])
            add_axis!(sources[2], "gene", ["X", "Y"])

            nested_test("dense") do
                set_matrix!(sources[1], "cell", "gene", "UMIs", sparse_matrix_csc([1 2; 3 4]))
                set_matrix!(sources[2], "cell", "gene", "UMIs", sparse_matrix_csc([5 6; 7 8; 9 10]))
                concatenate(destination, "cell", sources)
                @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6; 7 8; 9 10]
                @test !(get_matrix(destination, "cell", "gene", "UMIs").array isa SparseMatrixCSC)
            end

            nested_test("sparse") do
                set_matrix!(sources[1], "cell", "gene", "UMIs", sparse_matrix_csc([1 0; 0 2]))
                set_matrix!(sources[2], "cell", "gene", "UMIs", sparse_matrix_csc([0 3; 4 0; 0 5]))
                concatenate(destination, "cell", sources)
                @test get_matrix(destination, "cell", "gene", "UMIs") == [1 0; 0 2; 0 3; 4 0; 0 5]
                @test get_matrix(destination, "cell", "gene", "UMIs").array isa SparseMatrixCSC
            end

            nested_test("!empty") do
                set_matrix!(sources[1], "cell", "gene", "UMIs", sparse_matrix_csc([1 2; 3 4]))
                @test_throws dedent("""
                    nested task error: no empty value for the matrix: UMIs
                        of the rows axis: gene
                        and the columns axis: cell
                        which is missing from the daf data: source.2!
                        concatenated into the daf data: destination!
                """) concatenate(destination, "cell", sources)
            end

            nested_test("~empty") do
                set_matrix!(sources[1], "cell", "gene", "UMIs", sparse_matrix_csc([1 2; 3 4]))
                @test_throws dedent("""
                    nested task error: no empty value for the matrix: UMIs
                        of the rows axis: gene
                        and the columns axis: cell
                        which is missing from the daf data: source.2!
                        concatenated into the daf data: destination!
                """) concatenate(destination, "cell", sources; empty = Dict("version" => 0))
            end

            nested_test("empty") do
                nested_test("zero") do
                    set_matrix!(sources[1], "cell", "gene", "UMIs", sparse_matrix_csc([1 2; 3 4]))
                    concatenate(destination, "cell", sources; empty = Dict(("cell", "gene", "UMIs") => 0))
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2; 3 4; 0 0; 0 0; 0 0]
                    @test get_matrix(destination, "cell", "gene", "UMIs").array isa SparseMatrixCSC
                end

                nested_test("!zero") do
                    set_matrix!(sources[1], "cell", "gene", "UMIs", sparse_matrix_csc([1 2; 3 4]))
                    concatenate(destination, "cell", sources; empty = Dict(("gene", "cell", "UMIs") => 5))
                    @test get_matrix(destination, "cell", "gene", "UMIs") == [1 2; 3 4; 5 5; 5 5; 5 5]
                    @test !(get_matrix(destination, "cell", "gene", "UMIs").array isa SparseMatrixCSC)
                end
            end
        end
    end

    nested_test("merge") do
        nested_test("scalar") do
            set_scalar!(sources[1], "version", 1)
            set_scalar!(sources[2], "version", 2)

            nested_test("skip") do
                concatenate(destination, "cell", sources; merge = [ALL_VECTORS => LastValue])
                @test !has_scalar(destination, "version")
                @test !has_vector(destination, "dataset", "version")
            end

            nested_test("last") do
                concatenate(destination, "cell", sources; merge = ["version" => LastValue])
                @test get_scalar(destination, "version") == 2
                @test !has_vector(destination, "dataset", "version")
            end

            nested_test("collect") do
                concatenate(destination, "cell", sources; merge = [ALL_SCALARS => CollectAxis])
                @test !has_scalar(destination, "version")
                @test get_vector(destination, "dataset", "version") == [1, 2]
            end

            nested_test("!collect") do
                @test_throws dedent("""
                    can't collect axis for the scalar: version
                    of the daf data sets concatenated into the daf data: destination!
                    because no data set axis was created
                """) concatenate(
                    destination,
                    "cell",
                    sources;
                    dataset_axis = nothing,
                    merge = [ALL_SCALARS => CollectAxis],
                    prefixed = Set{String}(),
                )
            end
        end

        nested_test("vector") do
            add_axis!(sources[1], "gene", ["X", "Y"])
            add_axis!(sources[2], "gene", ["X", "Y"])
            set_vector!(sources[1], "gene", "weight", [UInt8(1), UInt8(2)])
            set_vector!(sources[2], "gene", "weight", [UInt16(3), UInt16(4)])

            nested_test("skip") do
                concatenate(destination, "cell", sources; merge = [ALL_SCALARS => LastValue])
                @test !has_vector(destination, "gene", "weight")
                @test !has_matrix(destination, "dataset", "gene", "weight")
            end

            nested_test("last") do
                concatenate(destination, "cell", sources; merge = [("gene", "weight") => LastValue])
                @test get_vector(destination, "gene", "weight") == [3, 4]
                @test !has_matrix(destination, "dataset", "gene", "weight")
            end

            nested_test("collect") do
                nested_test("dense") do
                    nested_test("full") do
                        concatenate(destination, "cell", sources; merge = [ALL_VECTORS => CollectAxis])
                        @test !has_vector(destination, "gene", "weight")
                        @test get_matrix(destination, "dataset", "gene", "weight") == [1 2; 3 4]
                        @test eltype(get_matrix(destination, "dataset", "gene", "weight")) == UInt16
                    end

                    nested_test("empty") do
                        delete_vector!(sources[2], "gene", "weight")

                        nested_test("zero") do
                            concatenate(
                                destination,
                                "cell",
                                sources;
                                merge = [ALL_VECTORS => CollectAxis],
                                empty = Dict(("gene", "weight") => 0.0),
                            )
                            @test !has_vector(destination, "gene", "weight")
                            @test get_matrix(destination, "dataset", "gene", "weight") == [1.0 2.0; 0.0 0.0]
                            @test eltype(get_matrix(destination, "dataset", "gene", "weight")) == Float64
                            @test get_matrix(destination, "dataset", "gene", "weight").array isa SparseMatrixCSC
                        end

                        nested_test("!zero") do
                            concatenate(
                                destination,
                                "cell",
                                sources;
                                merge = [ALL_VECTORS => CollectAxis],
                                empty = Dict(("gene", "weight") => Int32(3)),
                            )
                            @test !has_vector(destination, "gene", "weight")
                            @test get_matrix(destination, "dataset", "gene", "weight") == [1 2; 3 3]
                            @test eltype(get_matrix(destination, "dataset", "gene", "weight")) == Int32
                            @test !(get_matrix(destination, "dataset", "gene", "weight").array isa SparseMatrixCSC)
                        end
                    end
                end

                nested_test("sparse") do
                    set_vector!(sources[1], "gene", "weight", sparse_vector([1, 0]); overwrite = true)
                    set_vector!(sources[2], "gene", "weight", sparse_vector([0, 2]); overwrite = true)
                    concatenate(destination, "cell", sources; merge = [ALL_VECTORS => CollectAxis])
                    @test !has_vector(destination, "gene", "weight")
                    @test get_matrix(destination, "dataset", "gene", "weight") == [1 0; 0 2]
                    @test get_matrix(destination, "dataset", "gene", "weight").array isa SparseMatrixCSC
                end
            end

            nested_test("!collect") do
                @test_throws dedent("""
                    can't collect axis for the vector: weight
                    of the axis: gene
                    of the daf data sets concatenated into the daf data: destination!
                    because no data set axis was created
                """) concatenate(
                    destination,
                    "cell",
                    sources;
                    dataset_axis = nothing,
                    merge = [ALL_VECTORS => CollectAxis],
                )
            end
        end

        nested_test("matrix") do
            add_axis!(sources[1], "gene", ["X", "Y"])
            add_axis!(sources[2], "gene", ["X", "Y"])

            nested_test("square") do
                set_matrix!(sources[1], "gene", "gene", "outgoing_edges", [1 2; 3 4])
                set_matrix!(sources[2], "gene", "gene", "outgoing_edges", [5 6; 7 8])

                nested_test("skip") do
                    concatenate(destination, "cell", sources; merge = [ALL_VECTORS => LastValue])
                    @test !has_matrix(destination, "gene", "gene", "outgoing_edges")
                end

                nested_test("last") do
                    concatenate(destination, "cell", sources; merge = [("gene", "gene", "outgoing_edges") => LastValue])
                    @test get_matrix(destination, "gene", "gene", "outgoing_edges") == [5 6; 7 8]
                end

                nested_test("!collect") do
                    @test_throws dedent("""
                        can't collect axis for the matrix: outgoing_edges
                        of the rows axis: gene
                        and the columns axis: gene
                        of the daf data sets concatenated into the daf data: destination!
                        because that would create a 3D tensor, which is not supported
                    """) concatenate(destination, "cell", sources; merge = [ALL_MATRICES => CollectAxis])
                end
            end

            nested_test("rectangle") do
                add_axis!(sources[1], "batch", ["B1", "B2"])
                add_axis!(sources[2], "batch", ["B1", "B2"])

                set_matrix!(sources[1], "gene", "batch", "scale", [1 2; 3 4])
                set_matrix!(sources[2], "gene", "batch", "scale", [5 6; 7 8])

                nested_test("skip") do
                    concatenate(destination, "cell", sources; merge = [ALL_VECTORS => LastValue])
                    @test !has_matrix(destination, "gene", "batch", "scale")
                end

                nested_test("last") do
                    concatenate(destination, "cell", sources; merge = [("gene", "batch", "scale") => LastValue])
                    @test get_matrix(destination, "gene", "batch", "scale") == [5 6; 7 8]
                end

                nested_test("!collect") do
                    @test_throws dedent("""
                        can't collect axis for the matrix: scale
                        of the rows axis: batch
                        and the columns axis: gene
                        of the daf data sets concatenated into the daf data: destination!
                        because that would create a 3D tensor, which is not supported
                    """) concatenate(destination, "cell", sources; merge = [ALL_MATRICES => CollectAxis])
                end
            end
        end
    end
end
