function populate_reorder_test_data!(daf::DafWriter)::Nothing
    add_axis!(daf, "cell", ["A", "B", "C"])
    add_axis!(daf, "gene", ["X", "Y", "Z", "W"])

    set_vector!(daf, "cell", "age", [10, 20, 30])
    set_vector!(daf, "gene", "marker", SparseVector([1, 0, 1, 0]))
    set_vector!(daf, "cell", "color", ["aa", "bb", "cc"])
    set_vector!(daf, "gene", "label", ["", "", "", "hi"])

    set_matrix!(daf, "cell", "gene", "UMIs", [1 2 3 4; 5 6 7 8; 9 10 11 12])
    set_matrix!(daf, "cell", "gene", "sparse", sparse_matrix_csc([1 0 3 0; 0 6 0 8; 9 0 0 12]))
    set_matrix!(daf, "cell", "gene", "annotation", ["a" "b" "c" "d"; "e" "f" "g" "h"; "i" "j" "k" "l"])
    set_matrix!(daf, "cell", "gene", "sparse_text", ["" "" "" ""; "" "" "" ""; "" "" "" "ab"])
    return nothing
end

function test_reorder_both_axes!(daf::DafWriter)::Nothing
    reorder_axes!(daf, Dict("cell" => [3, 1, 2], "gene" => [4, 3, 2, 1]))

    @test axis_entries(daf, "cell") == ["C", "A", "B"]
    @test axis_entries(daf, "gene") == ["W", "Z", "Y", "X"]

    @test get_vector(daf, "cell", "age") == [30, 10, 20]
    @test get_vector(daf, "gene", "marker") == [0, 1, 0, 1]
    @test get_vector(daf, "cell", "color") == ["cc", "aa", "bb"]
    @test get_vector(daf, "gene", "label") == ["hi", "", "", ""]

    @test get_matrix(daf, "cell", "gene", "UMIs") == [12 11 10 9; 4 3 2 1; 8 7 6 5]
    @test get_matrix(daf, "cell", "gene", "sparse") == [12 0 0 9; 0 3 0 1; 8 0 6 0]
    @test get_matrix(daf, "cell", "gene", "annotation") == ["l" "k" "j" "i"; "d" "c" "b" "a"; "h" "g" "f" "e"]
    @test get_matrix(daf, "cell", "gene", "sparse_text") == ["ab" "" "" ""; "" "" "" ""; "" "" "" ""]
    return nothing
end

function test_reorder_single_axis!(daf::DafWriter)::Nothing
    reorder_axes!(daf, Dict("cell" => [3, 1, 2]))

    @test axis_entries(daf, "cell") == ["C", "A", "B"]
    @test axis_entries(daf, "gene") == ["X", "Y", "Z", "W"]

    @test get_vector(daf, "cell", "age") == [30, 10, 20]
    @test get_vector(daf, "gene", "marker") == [1, 0, 1, 0]
    @test get_vector(daf, "cell", "color") == ["cc", "aa", "bb"]
    @test get_vector(daf, "gene", "label") == ["", "", "", "hi"]

    @test get_matrix(daf, "cell", "gene", "UMIs") == [9 10 11 12; 1 2 3 4; 5 6 7 8]
    @test get_matrix(daf, "cell", "gene", "sparse") == [9 0 0 12; 1 0 3 0; 0 6 0 8]
    @test get_matrix(daf, "cell", "gene", "annotation") == ["i" "j" "k" "l"; "a" "b" "c" "d"; "e" "f" "g" "h"]
    @test get_matrix(daf, "cell", "gene", "sparse_text") == ["" "" "" "ab"; "" "" "" ""; "" "" "" ""]
    return nothing
end

function test_original_data!(daf::DafWriter)::Nothing
    @test axis_entries(daf, "cell") == ["A", "B", "C"]
    @test axis_entries(daf, "gene") == ["X", "Y", "Z", "W"]

    @test get_vector(daf, "cell", "age") == [10, 20, 30]
    @test get_vector(daf, "gene", "marker") == [1, 0, 1, 0]
    @test get_vector(daf, "cell", "color") == ["aa", "bb", "cc"]
    @test get_vector(daf, "gene", "label") == ["", "", "", "hi"]

    @test get_matrix(daf, "cell", "gene", "UMIs") == [1 2 3 4; 5 6 7 8; 9 10 11 12]
    @test get_matrix(daf, "cell", "gene", "sparse") == [1 0 3 0; 0 6 0 8; 9 0 0 12]
    @test get_matrix(daf, "cell", "gene", "annotation") == ["a" "b" "c" "d"; "e" "f" "g" "h"; "i" "j" "k" "l"]
    @test get_matrix(daf, "cell", "gene", "sparse_text") == ["" "" "" ""; "" "" "" ""; "" "" "" "ab"]
    return nothing
end

function test_crash_recovery!(daf::DafWriter, crash_after::Int)::Nothing
    perms = Dict("cell" => [3, 1, 2], "gene" => [4, 3, 2, 1])
    @test_throws DataAxesFormats.Reorder.SimulatedCrash reorder_axes!(daf, perms; _simulate_crash = crash_after)

    @test reset_reorder_axes!(daf)
    test_original_data!(daf)

    test_reorder_both_axes!(daf)
    return nothing
end

function test_reorder_identity!(daf::DafWriter)::Nothing
    reorder_axes!(daf, Dict("cell" => [1, 2, 3]))

    @test axis_entries(daf, "cell") == ["A", "B", "C"]
    @test get_vector(daf, "cell", "age") == [10, 20, 30]
    @test get_vector(daf, "cell", "color") == ["aa", "bb", "cc"]
    @test get_matrix(daf, "cell", "gene", "UMIs") == [1 2 3 4; 5 6 7 8; 9 10 11 12]
    @test get_matrix(daf, "cell", "gene", "annotation") == ["a" "b" "c" "d"; "e" "f" "g" "h"; "i" "j" "k" "l"]
    return nothing
end

nested_test("reorder") do
    nested_test("is_leaf") do
        nested_test("types") do
            @test is_leaf(MemoryDaf)
            @test is_leaf(FilesDaf)
            @test is_leaf(H5df)
            @test is_leaf(ZarrDaf)
            @test !is_leaf(DafReader)
            @test !is_leaf(DafWriter)
        end

        nested_test("memory") do
            daf = MemoryDaf(; name = "memory!")
            @test is_leaf(daf)
        end

        nested_test("files") do
            mktempdir() do path
                daf = FilesDaf("$(path)/files", "w"; name = "files!")
                @test is_leaf(daf)
                return nothing
            end
        end

        nested_test("h5df") do
            mktempdir() do path
                daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                @test is_leaf(daf)
                return nothing
            end
        end

        nested_test("zarr") do
            mktempdir() do path
                daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                @test is_leaf(daf)
                return nothing
            end
        end

        nested_test("wrappers") do
            first = MemoryDaf(; name = "first!")
            second = MemoryDaf(; name = "second!")
            add_axis!(first, "cell", ["A", "B"])
            set_vector!(first, "cell", "age", [1, 2])

            nested_test("read_only") do
                @test !is_leaf(read_only(first))
            end

            nested_test("view") do
                @test !is_leaf(viewer(first; name = "view!"))
            end

            nested_test("read_chain") do
                @test !is_leaf(chain_reader([first, second]; name = "read_chain!"))
            end

            nested_test("write_chain") do
                @test !is_leaf(chain_writer([first, second]; name = "write_chain!"))
            end

            if DAF_ENFORCE_CONTRACTS
                nested_test("contract") do
                    contract = Contract(; axes = ["cell" => (OptionalOutput, "cell")])
                    @test !is_leaf(contractor("computation", contract, first; overwrite = true))
                end
            end
        end
    end

    nested_test("reorder_axes!") do
        nested_test("errors") do
            nested_test("non_leaf") do
                first = MemoryDaf(; name = "first!")
                second = MemoryDaf(; name = "second!")
                chain = chain_writer([first, second]; name = "chain!")
                @test_throws chomp("""
                             non-leaf type: WriteChain
                             for the daf data: chain!
                             given to reorder_axes!
                             """) reorder_axes!(chain, Dict("cell" => [1]))
            end

            nested_test("missing_axis") do
                daf = MemoryDaf(; name = "memory!")
                add_axis!(daf, "cell", ["A", "B"])
                @test_throws "axis: gene\ndoes not exist in any of the writers" reorder_axes!(
                    daf,
                    Dict("gene" => [1, 2]),
                )
            end

            nested_test("bad_length") do
                daf = MemoryDaf(; name = "memory!")
                add_axis!(daf, "cell", ["A", "B"])
                @test_throws chomp("""
                             permutation length: 3
                             does not match axis: cell
                             length: 2
                             in the daf data: memory!
                             """) reorder_axes!(daf, Dict("cell" => [1, 2, 3]))
            end

            nested_test("bad_permutation") do
                daf = MemoryDaf(; name = "memory!")
                add_axis!(daf, "cell", ["A", "B"])
                @test_throws ArgumentError reorder_axes!(daf, Dict("cell" => [1, 1]))
            end
        end

        nested_test("empty") do
            daf = MemoryDaf(; name = "memory!")
            add_axis!(daf, "cell", ["A", "B"])
            reorder_axes!(daf, Dict{String, Vector{Int}}())
            @test axis_entries(daf, "cell") == ["A", "B"]
        end

        nested_test("memory") do
            nested_test("both_axes") do
                daf = MemoryDaf(; name = "memory!")
                populate_reorder_test_data!(daf)
                test_reorder_both_axes!(daf)
                return nothing
            end

            nested_test("single_axis") do
                daf = MemoryDaf(; name = "memory!")
                populate_reorder_test_data!(daf)
                test_reorder_single_axis!(daf)
                return nothing
            end

            nested_test("identity") do
                daf = MemoryDaf(; name = "memory!")
                populate_reorder_test_data!(daf)
                test_reorder_identity!(daf)
                return nothing
            end

            nested_test("crash_recovery") do
                nested_test("after_1") do
                    daf = MemoryDaf(; name = "memory!")
                    populate_reorder_test_data!(daf)
                    test_crash_recovery!(daf, 1)
                    return nothing
                end

                nested_test("after_4") do
                    daf = MemoryDaf(; name = "memory!")
                    populate_reorder_test_data!(daf)
                    test_crash_recovery!(daf, 4)
                    return nothing
                end

                nested_test("no_pending") do
                    daf = MemoryDaf(; name = "memory!")
                    populate_reorder_test_data!(daf)
                    @test !reset_reorder_axes!(daf)
                    return nothing
                end
            end
        end

        nested_test("files") do
            nested_test("both_axes") do
                mktempdir() do path
                    daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                    populate_reorder_test_data!(daf)
                    test_reorder_both_axes!(daf)
                    return nothing
                end
            end

            nested_test("single_axis") do
                mktempdir() do path
                    daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                    populate_reorder_test_data!(daf)
                    test_reorder_single_axis!(daf)
                    return nothing
                end
            end

            nested_test("identity") do
                mktempdir() do path
                    daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                    populate_reorder_test_data!(daf)
                    test_reorder_identity!(daf)
                    return nothing
                end
            end

            nested_test("crash_recovery") do
                nested_test("after_1") do
                    mktempdir() do path
                        daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 1)
                        return nothing
                    end
                end

                nested_test("after_4") do
                    mktempdir() do path
                        daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 4)
                        return nothing
                    end
                end

                nested_test("no_pending") do
                    mktempdir() do path
                        daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                        populate_reorder_test_data!(daf)
                        @test !reset_reorder_axes!(daf)
                        return nothing
                    end
                end
            end
        end

        nested_test("h5df") do
            nested_test("both_axes") do
                mktempdir() do path
                    daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                    populate_reorder_test_data!(daf)
                    test_reorder_both_axes!(daf)
                    return nothing
                end
            end

            nested_test("single_axis") do
                mktempdir() do path
                    daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                    populate_reorder_test_data!(daf)
                    test_reorder_single_axis!(daf)
                    return nothing
                end
            end

            nested_test("identity") do
                mktempdir() do path
                    daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                    populate_reorder_test_data!(daf)
                    test_reorder_identity!(daf)
                    return nothing
                end
            end

            nested_test("crash_recovery") do
                nested_test("after_1") do
                    mktempdir() do path
                        daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 1)
                        return nothing
                    end
                end

                nested_test("after_4") do
                    mktempdir() do path
                        daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 4)
                        return nothing
                    end
                end

                nested_test("no_pending") do
                    mktempdir() do path
                        daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                        populate_reorder_test_data!(daf)
                        @test !reset_reorder_axes!(daf)
                        return nothing
                    end
                end
            end

            nested_test("sparse_strings") do
                mktempdir() do path
                    daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                    entries = ["e$(i)" for i in 1:10]
                    add_axis!(daf, "item", entries)
                    add_axis!(daf, "feature", ["F1", "F2", "F3"])

                    sparse_labels = fill("", 10)
                    sparse_labels[3] = "hello"
                    set_vector!(daf, "item", "tag", sparse_labels)

                    sparse_text = fill("", 10, 3)
                    sparse_text[2, 1] = "ab"
                    set_matrix!(daf, "item", "feature", "note", sparse_text)

                    perm = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
                    reorder_axes!(daf, Dict("item" => perm))

                    @test axis_entries(daf, "item") == reverse(entries)

                    expected_labels = fill("", 10)
                    expected_labels[8] = "hello"
                    @test get_vector(daf, "item", "tag") == expected_labels

                    expected_text = fill("", 10, 3)
                    expected_text[9, 1] = "ab"
                    @test get_matrix(daf, "item", "feature", "note") == expected_text
                    return nothing
                end
            end
        end

        nested_test("zarr") do
            nested_test("both_axes") do
                mktempdir() do path
                    daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                    populate_reorder_test_data!(daf)
                    test_reorder_both_axes!(daf)
                    return nothing
                end
            end

            nested_test("single_axis") do
                mktempdir() do path
                    daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                    populate_reorder_test_data!(daf)
                    test_reorder_single_axis!(daf)
                    return nothing
                end
            end

            nested_test("identity") do
                mktempdir() do path
                    daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                    populate_reorder_test_data!(daf)
                    test_reorder_identity!(daf)
                    return nothing
                end
            end

            nested_test("crash_recovery") do
                nested_test("after_1") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 1)
                        return nothing
                    end
                end

                nested_test("after_4") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 4)
                        return nothing
                    end
                end

                nested_test("no_pending") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.zarr", "w"; name = "zarr!")
                        populate_reorder_test_data!(daf)
                        @test !reset_reorder_axes!(daf)
                        return nothing
                    end
                end
            end
        end

        nested_test("multiple_writers") do
            nested_test("memory_pair") do
                daf1 = MemoryDaf(; name = "first!")
                daf2 = MemoryDaf(; name = "second!")
                populate_reorder_test_data!(daf1)
                populate_reorder_test_data!(daf2)
                reorder_axes!([daf1, daf2], Dict("cell" => [3, 1, 2]))

                @test axis_entries(daf1, "cell") == ["C", "A", "B"]
                @test axis_entries(daf2, "cell") == ["C", "A", "B"]
                @test get_vector(daf1, "cell", "age") == [30, 10, 20]
                @test get_vector(daf2, "cell", "age") == [30, 10, 20]
                return nothing
            end

            nested_test("mixed_axes") do
                daf1 = MemoryDaf(; name = "first!")
                daf2 = MemoryDaf(; name = "second!")
                add_axis!(daf1, "cell", ["A", "B", "C"])
                set_vector!(daf1, "cell", "age", [10, 20, 30])
                add_axis!(daf2, "gene", ["X", "Y"])
                set_vector!(daf2, "gene", "marker", [1, 0])
                reorder_axes!([daf1, daf2], Dict("cell" => [3, 1, 2], "gene" => [2, 1]))

                @test axis_entries(daf1, "cell") == ["C", "A", "B"]
                @test get_vector(daf1, "cell", "age") == [30, 10, 20]
                @test axis_entries(daf2, "gene") == ["Y", "X"]
                @test get_vector(daf2, "gene", "marker") == [0, 1]
                return nothing
            end

            nested_test("mismatched_entries") do
                daf1 = MemoryDaf(; name = "first!")
                daf2 = MemoryDaf(; name = "second!")
                add_axis!(daf1, "cell", ["A", "B"])
                add_axis!(daf2, "cell", ["X", "Y"])
                @test_throws "axis: cell entries differ" reorder_axes!([daf1, daf2], Dict("cell" => [2, 1]))
            end
        end
    end
end
