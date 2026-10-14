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

# 4096 cells: each Float32 cell vector, each Float32 matrix column, and the `nzval` of each half-zero sparse property
# clears the packed threshold.
N_PACKED_CELLS = 4096
PACKED_CELLS_PERMUTATION = collect(N_PACKED_CELLS:-1:1)
PACKED_GENES_PERMUTATION = [3, 1, 2]

function packed_reorder_dense_vector()::Vector{Float32}
    return Float32.(1:N_PACKED_CELLS)
end

function packed_reorder_sparse_vector()::SparseVector{Float32}
    return sparse(Float32[isodd(index) ? index : 0 for index in 1:N_PACKED_CELLS])
end

function packed_reorder_string_vector()::Vector{String}
    return ["cell_$(index)" for index in 1:N_PACKED_CELLS]
end

function packed_reorder_dense_matrix()::Matrix{Float32}
    return Float32.(reshape(1:(N_PACKED_CELLS * 3), N_PACKED_CELLS, 3))
end

function packed_reorder_sparse_matrix()::SparseMatrixCSC{Float32}
    return sparse_matrix_csc(Float32[isodd(index) ? index : 0 for index in 1:N_PACKED_CELLS, _ in 1:3])
end

function populate_packed_reorder_test_data!(daf::DafWriter)::Nothing
    add_axis!(daf, "cell", ["C$(index)" for index in 1:N_PACKED_CELLS])
    add_axis!(daf, "gene", ["X", "Y", "Z"])

    set_vector!(daf, "cell", "dense", packed_reorder_dense_vector(); packed = true)
    set_vector!(daf, "cell", "sparse", packed_reorder_sparse_vector(); packed = true)
    set_vector!(daf, "cell", "label", packed_reorder_string_vector(); packed = true)

    set_matrix!(daf, "cell", "gene", "dense", packed_reorder_dense_matrix(); relayout = false, packed = true)
    set_matrix!(daf, "cell", "gene", "sparse", packed_reorder_sparse_matrix(); relayout = false, packed = true)
    return nothing
end

function test_packed_properties!(daf::DafWriter)::Nothing
    for name in ("dense", "sparse", "label")
        verify_vector_metadata(daf, "cell", name; is_packed = true)
    end
    for name in ("dense", "sparse")
        verify_matrix_metadata(daf, "cell", "gene", name; is_packed = true)
    end
    return nothing
end

function test_packed_reorder!(daf::DafWriter)::Nothing
    test_packed_properties!(daf)
    reorder_axes!(daf, Dict("cell" => PACKED_CELLS_PERMUTATION, "gene" => PACKED_GENES_PERMUTATION))
    test_packed_properties!(daf)

    @test get_vector(daf, "cell", "dense") == packed_reorder_dense_vector()[PACKED_CELLS_PERMUTATION]
    @test get_vector(daf, "cell", "sparse") == packed_reorder_sparse_vector()[PACKED_CELLS_PERMUTATION]
    @test get_vector(daf, "cell", "label") == packed_reorder_string_vector()[PACKED_CELLS_PERMUTATION]
    @test get_matrix(daf, "cell", "gene", "dense") ==
          packed_reorder_dense_matrix()[PACKED_CELLS_PERMUTATION, PACKED_GENES_PERMUTATION]
    @test get_matrix(daf, "cell", "gene", "sparse") ==
          packed_reorder_sparse_matrix()[PACKED_CELLS_PERMUTATION, PACKED_GENES_PERMUTATION]
    return nothing
end

function test_packed_crash_recovery!(daf::DafWriter)::Nothing
    permutations = Dict("cell" => PACKED_CELLS_PERMUTATION, "gene" => PACKED_GENES_PERMUTATION)
    @test_throws DataAxesFormats.Reorder.SimulatedCrash reorder_axes!(daf, permutations; _simulate_crash = 3)

    @test reset_reorder_axes!(daf)
    test_packed_properties!(daf)
    @test get_vector(daf, "cell", "dense") == packed_reorder_dense_vector()
    @test get_vector(daf, "cell", "sparse") == packed_reorder_sparse_vector()
    @test get_vector(daf, "cell", "label") == packed_reorder_string_vector()
    @test get_matrix(daf, "cell", "gene", "dense") == packed_reorder_dense_matrix()
    @test get_matrix(daf, "cell", "gene", "sparse") == packed_reorder_sparse_matrix()

    test_packed_reorder!(daf)
    return nothing
end

# The data sets the combination tests run on, by format name. Each creates a writable data set under a directory.
COMBINATIONS_FORMATS = [
    "memory" => path -> MemoryDaf(; name = "memory!"),
    "files" => path -> FilesDaf("$(path)/daf", "w"; name = "files!"),
    "h5df" => path -> H5df("$(path)/test.h5df", "w"; name = "h5df!"),
    "zarr" => path -> ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!"),
]

# The size of the combination tests data set (see `populate_packed_round_trip!`).
N_COMBINATIONS_ROWS = 4096
N_COMBINATIONS_COLUMNS = 3

# The reorders the combination tests apply, by name: rows only, columns only, or both.
COMBINATIONS_PERMUTATIONS = [
    "rows" => Dict("row" => collect(N_COMBINATIONS_ROWS:-1:1)),
    "columns" => Dict("col" => [3, 1, 2]),
    "both" => Dict("row" => collect(N_COMBINATIONS_ROWS:-1:1), "col" => [3, 1, 2]),
]

function permuted_round_trip_expectations(
    expected::Tuple{Dict{String, AbstractVector}, Dict{String, AbstractMatrix}},
    axes_permutations::AbstractDict{<:AbstractString, <:AbstractVector{<:Integer}},
)::Tuple{Dict{String, AbstractVector}, Dict{String, AbstractMatrix}}
    expected_vectors, expected_matrices = expected
    rows_permutation = get(axes_permutations, "row", 1:N_COMBINATIONS_ROWS)
    columns_permutation = get(axes_permutations, "col", 1:N_COMBINATIONS_COLUMNS)
    return (
        Dict{String, AbstractVector}(name => vector[rows_permutation] for (name, vector) in expected_vectors),
        Dict{String, AbstractMatrix}(
            name => matrix[rows_permutation, columns_permutation] for (name, matrix) in expected_matrices
        ),
    )
end

# Every stored object of a data set: the relative path of each file of a FilesDaf or ZarrDaf, the path of each HDF5
# object of an H5df, and each property key of a MemoryDaf. Used to verify a reset leaves no stray objects behind.
function stored_objects(daf::Union{FilesDaf, ZarrDaf})::Set{String}
    names = Set{String}()
    for (root, _, filenames) in walkdir(daf.path)
        for filename in filenames
            push!(names, relpath(joinpath(root, filename), daf.path))
        end
    end
    return names
end

function stored_objects(daf::H5df)::Set{String}
    return hdf5_object_names(daf.root, "")
end

function hdf5_object_names(group::Union{HDF5.File, HDF5.Group}, prefix::AbstractString)::Set{String}
    names = Set{String}()
    for key in keys(group)
        name = "$(prefix)/$(key)"
        push!(names, name)
        object = group[key]
        if object isa HDF5.Group
            union!(names, hdf5_object_names(object, name))
        end
    end
    return names
end

function stored_objects(daf::MemoryDaf)::Set{String}
    names = Set{String}()
    for axis in axes_set(daf)
        push!(names, axis)
        for name in vectors_set(daf, axis)
            push!(names, "$(axis)/$(name)")
        end
        for other_axis in axes_set(daf)
            for name in matrices_set(daf, axis, other_axis; relayout = false)
                push!(names, "$(axis)/$(other_axis)/$(name)")
            end
        end
    end
    return names
end

nested_test("reorder") do
    nested_test("reset_combinations") do
        axes_permutations = Dict(COMBINATIONS_PERMUTATIONS)["both"]
        for (format_name, create_daf) in COMBINATIONS_FORMATS
            is_packing_supported = format_name != "memory"
            nested_test(format_name) do
                nested_test("before_backup") do
                    mktempdir() do path
                        daf = create_daf(path)
                        expected = populate_packed_round_trip!(daf, N_COMBINATIONS_ROWS, N_COMBINATIONS_COLUMNS)
                        original_objects = stored_objects(daf)
                        DataAxesFormats.Formats.with_data_write_lock(daf, "test") do
                            return DataAxesFormats.Reorder.format_lock_reorder!(daf, "test")
                        end
                        @test reset_reorder_axes!(daf)
                        verify_packed_round_trip(daf, expected; is_packing_supported)
                        @test stored_objects(daf) == original_objects
                        return nothing
                    end
                end

                # One property is replaced per crash point: 9 vectors (8 combinations and one `Bool`) and 8 matrices.
                n_replacements = 17
                for crash_after in 1:n_replacements
                    nested_test("crash_after_$(crash_after)") do
                        mktempdir() do path
                            daf = create_daf(path)
                            expected = populate_packed_round_trip!(daf, N_COMBINATIONS_ROWS, N_COMBINATIONS_COLUMNS)
                            original_objects = stored_objects(daf)
                            @test_throws DataAxesFormats.Reorder.SimulatedCrash reorder_axes!(
                                daf,
                                axes_permutations;
                                _simulate_crash = crash_after,
                            )
                            @test reset_reorder_axes!(daf)
                            verify_packed_round_trip(daf, expected; is_packing_supported)
                            @test stored_objects(daf) == original_objects

                            reorder_axes!(daf, axes_permutations)
                            verify_packed_round_trip(
                                daf,
                                permuted_round_trip_expectations(expected, axes_permutations);
                                is_packing_supported,
                            )
                            return nothing
                        end
                    end
                end
            end
        end
    end

    nested_test("combinations") do
        for (format_name, create_daf) in COMBINATIONS_FORMATS
            nested_test(format_name) do
                for (permutation_name, axes_permutations) in COMBINATIONS_PERMUTATIONS
                    nested_test(permutation_name) do
                        mktempdir() do path
                            daf = create_daf(path)
                            expected = populate_packed_round_trip!(daf, N_COMBINATIONS_ROWS, N_COMBINATIONS_COLUMNS)
                            reorder_axes!(daf, axes_permutations)
                            verify_packed_round_trip(
                                daf,
                                permuted_round_trip_expectations(expected, axes_permutations);
                                is_packing_supported = format_name != "memory",
                            )
                            return nothing
                        end
                    end
                end
            end
        end
    end

    nested_test("is_leaf") do
        nested_test("types") do
            @test is_leaf(MemoryDaf)
            @test is_leaf(FilesDaf)
            @test is_leaf(H5df)
            @test is_leaf(ZarrDaf)
            @test is_leaf(ZipDaf)
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
                daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
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
                    daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                    populate_reorder_test_data!(daf)
                    test_reorder_both_axes!(daf)
                    return nothing
                end
            end

            nested_test("single_axis") do
                mktempdir() do path
                    daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                    populate_reorder_test_data!(daf)
                    test_reorder_single_axis!(daf)
                    return nothing
                end
            end

            nested_test("identity") do
                mktempdir() do path
                    daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                    populate_reorder_test_data!(daf)
                    test_reorder_identity!(daf)
                    return nothing
                end
            end

            nested_test("crash_recovery") do
                nested_test("after_1") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 1)
                        return nothing
                    end
                end

                nested_test("after_4") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                        populate_reorder_test_data!(daf)
                        test_crash_recovery!(daf, 4)
                        return nothing
                    end
                end

                nested_test("no_pending") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                        populate_reorder_test_data!(daf)
                        @test !reset_reorder_axes!(daf)
                        return nothing
                    end
                end
            end
        end

        nested_test("packed") do
            nested_test("files") do
                nested_test("reorder") do
                    mktempdir() do path
                        daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                        populate_packed_reorder_test_data!(daf)
                        test_packed_reorder!(daf)
                        return nothing
                    end
                end

                nested_test("crash_recovery") do
                    mktempdir() do path
                        daf = FilesDaf("$(path)/daf", "w"; name = "files!")
                        populate_packed_reorder_test_data!(daf)
                        test_packed_crash_recovery!(daf)
                        return nothing
                    end
                end
            end

            nested_test("h5df") do
                nested_test("reorder") do
                    mktempdir() do path
                        daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                        populate_packed_reorder_test_data!(daf)
                        test_packed_reorder!(daf)
                        return nothing
                    end
                end

                nested_test("crash_recovery") do
                    mktempdir() do path
                        daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                        populate_packed_reorder_test_data!(daf)
                        test_packed_crash_recovery!(daf)
                        return nothing
                    end
                end
            end

            nested_test("zarr") do
                nested_test("reorder") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                        populate_packed_reorder_test_data!(daf)
                        test_packed_reorder!(daf)
                        return nothing
                    end
                end

                nested_test("crash_recovery") do
                    mktempdir() do path
                        daf = ZarrDaf("$(path)/test.daf.zarr", "w"; name = "zarr!")
                        populate_packed_reorder_test_data!(daf)
                        test_packed_crash_recovery!(daf)
                        return nothing
                    end
                end
            end
        end

        nested_test("zip") do
            mktempdir() do path
                daf = ZipDaf("$(path)/test.daf.zip", "w+"; name = "zip!")
                add_axis!(daf, "cell", ["A", "B", "C"])
                @test_throws "ZipDaf is append-only; can't reorder" reorder_axes!(daf, Dict("cell" => [3, 1, 2]))
                return nothing
            end
        end

        nested_test("multiple_writers") do
            # File data sets are locked in the order of their paths.
            nested_test("files_pair") do
                mktempdir() do path
                    daf1 = FilesDaf("$(path)/first", "w"; name = "first!")
                    daf2 = FilesDaf("$(path)/second", "w"; name = "second!")
                    populate_reorder_test_data!(daf1)
                    populate_reorder_test_data!(daf2)
                    reorder_axes!([daf2, daf1], Dict("cell" => [3, 1, 2]))

                    @test axis_entries(daf1, "cell") == ["C", "A", "B"]
                    @test axis_entries(daf2, "cell") == ["C", "A", "B"]
                    @test get_vector(daf1, "cell", "age") == [30, 10, 20]
                    @test get_vector(daf2, "cell", "age") == [30, 10, 20]
                    return nothing
                end
            end

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
