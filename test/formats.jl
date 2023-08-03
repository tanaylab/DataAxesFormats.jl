import Daf.Formats.WriteFormat

function test_format_scalar(daf::WriteFormat)::Nothing
    @test !has_scalar(daf, "version")
    @test length(scalar_names(daf)) == 0

    @test_throws dedent("""
        missing scalar property: version
        in the daf data: memory!
    """) get_scalar(daf, "version")
    @test get_scalar(daf, "version"; default = 3) == 3

    @test_throws dedent("""
        missing scalar property: version
        in the daf data: memory!
    """) delete_scalar!(daf, "version")
    delete_scalar!(daf, "version"; must_exist = false)

    set_scalar!(daf, "version", "1.2")
    @test_throws dedent("""
        existing scalar property: version
        in the daf data: memory!
    """) set_scalar!(daf, "version", "4.5")

    @test length(scalar_names(daf)) == 1
    @test "version" in scalar_names(daf)

    @test get_scalar(daf, "version") == "1.2"
    @test get_scalar(daf, "version"; default = "3.4") == "1.2"

    read_only_daf = read_only(daf)
    @test read_only(read_only_daf) == read_only_daf
    @test description(read_only_daf) == dedent("""
        type: ReadOnlyView{MemoryDaf}
        name: memory!
        scalars:
          version: "1.2"
    """) * "\n"

    @test description(daf) == dedent("""
        type: MemoryDaf
        name: memory!
        scalars:
          version: "1.2"
    """) * "\n"

    @test_throws MethodError delete_scalar!(read_only(daf), "version")

    delete_scalar!(daf, "version")

    @test !has_scalar(daf, "version")
    @test length(scalar_names(daf)) == 0

    return nothing
end

function test_format_axis(daf::WriteFormat)::Nothing
    name = daf.name

    @test !has_axis(daf, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) get_axis(daf, "cell")
    delete_axis!(daf, "cell"; must_exist = false)
    @test length(axis_names(daf)) == 0

    repeated_cell_names = ["cell1", "cell1", "cell3"]
    @test_throws dedent("""
        non-unique entries for new axis: cell
        in the daf data: memory!
    """) add_axis!(daf, "cell", repeated_cell_names)

    cell_names = ["cell1", "cell2", "cell3"]
    add_axis!(daf, "cell", cell_names)
    @test length(axis_names(daf)) == 1
    @test "cell" in axis_names(daf)

    @test has_axis(daf, "cell")
    @test axis_length(daf, "cell") == 3
    @test get_axis(daf, "cell") == cell_names
    @test get_axis(read_only(daf), "cell") == cell_names

    @test_throws dedent("""
        existing axis: cell
        in the daf data: memory!
    """) add_axis!(daf, "cell", cell_names)

    @test description(daf) == dedent("""
        type: MemoryDaf
        name: memory!
        axes:
          cell: 3 entries
    """) * "\n"

    delete_axis!(daf, "cell")
    @test !has_axis(daf, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) delete_axis!(daf, "cell")
    @test length(axis_names(daf)) == 0

    return nothing
end

function test_format_vector(daf::WriteFormat)::Nothing
    name = daf.name

    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) has_vector(daf, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) vector_names(daf, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) delete_vector!(daf, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) get_vector(daf, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) set_vector!(daf, "cell", "age", [0, 1, 2])

    add_axis!(daf, "cell", ["cell0", "cell1", "cell3"])
    @test !has_vector(daf, "cell", "age")
    @test length(vector_names(daf, "cell")) == 0
    @test_throws dedent("""
        missing vector property: age
        for the axis: cell
        in the daf data: memory!
    """) delete_vector!(daf, "cell", "age")
    delete_vector!(daf, "cell", "age"; must_exist = false)
    @test_throws dedent("""
        missing vector property: age
        for the axis: cell
        in the daf data: memory!
    """) get_vector(daf, "cell", "age")
    @test_throws dedent("""
        vector length: 2
        is different from the length: 3
        of the axis: cell
        in the daf data: memory!
    """) set_vector!(daf, "cell", "age", [0, 1])

    bad_names = NamedArray([0, 1, 2])
    setnames!(bad_names, ["cell0", "cell1", "cell2"], 1)

    good_names = NamedArray([2, 1, 0])
    setnames!(good_names, ["cell0", "cell1", "cell3"], 1)

    @test_throws dedent("""
        entry names of the: vector
        mismatch the entry names of the axis: cell
        in the daf data: memory!
    """) set_vector!(daf, "cell", "age", bad_names)

    @test_throws dedent("""
        default length: 2
        is different from the length: 3
        of the axis: cell
        in the daf data: memory!
    """) get_vector(daf, "cell", "age"; default = [1, 2])
    @test get_vector(daf, "cell", "age"; default = [1, 2, 3]) == [1, 2, 3]
    @test get_vector(read_only(daf), "cell", "age"; default = 1) == [1, 1, 1]
    @test_throws dedent("""
        entry names of the: default vector
        mismatch the entry names of the axis: cell
        in the daf data: memory!
    """) get_vector(daf, "cell", "age"; default = bad_names)
    @test get_vector(daf, "cell", "age"; default = good_names) == good_names

    @test_throws dedent("""
        setting the reserved property: name
        for the axis: cell
        in the daf data: memory!
    """) set_vector!(daf, "cell", "name", [0, 1])
    @test has_vector(daf, "cell", "name")
    @test get_vector(daf, "cell", "name") == get_axis(daf, "cell")

    set_vector!(daf, "cell", "age", good_names)
    @test get_vector(daf, "cell", "age") == [2, 1, 0]
    @test get_vector(read_only(daf), "cell", "age") == [2, 1, 0]

    set_vector!(daf, "cell", "age", [0, 1, 2]; overwrite = true)
    @test_throws dedent("""
        existing vector property: age
        for the axis: cell
        in the daf data: memory!
    """) set_vector!(daf, "cell", "age", [1, 2, 3])
    @test length(vector_names(daf, "cell")) == 1
    @test "age" in vector_names(daf, "cell")
    @test get_vector(daf, "cell", "age") == [0, 1, 2]
    @test names(get_vector(daf, "cell", "age"), 1) == ["cell0", "cell1", "cell3"]

    @test description(daf) == dedent("""
        type: MemoryDaf
        name: memory!
        axes:
          cell: 3 entries
        vectors:
          cell:
            age: 3 x Int64 (Dense)
    """) * "\n"

    delete_vector!(daf, "cell", "age")
    @test !has_vector(daf, "cell", "age")

    set_vector!(daf, "cell", "age", 1)
    @test has_vector(daf, "cell", "age")
    @test get_vector(daf, "cell", "age") == [1, 1, 1]

    delete_vector!(daf, "cell", "age")
    empty_dense = empty_dense_vector!(daf, "cell", "age", Int64)
    @test empty_dense.array isa Vector{Int64}
    empty_dense .= [0, 1, 2]
    @test get_vector(daf, "cell", "age") == [0, 1, 2]

    sparse = SparseVector(empty_dense)
    delete_vector!(daf, "cell", "age")
    empty_sparse = empty_sparse_vector!(daf, "cell", "age", Int64, nnz(sparse), Int8)
    @test empty_sparse.array isa SparseVector{Int64, Int8}
    empty_sparse.array.nzind .= sparse.nzind
    empty_sparse.array.nzval .= sparse.nzval
    @test empty_sparse == sparse
    @test get_vector(daf, "cell", "age") == sparse

    delete_axis!(daf, "cell")
    add_axis!(daf, "cell", ["cell0", "cell1"])

    @test !has_vector(daf, "cell", "age")

    return nothing
end

function test_format_matrix(daf::WriteFormat)::Nothing
    name = daf.name

    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) has_matrix(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) matrix_names(daf, "cell", "gene")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) delete_matrix!(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) get_matrix(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the daf data: memory!
    """) set_matrix!(daf, "cell", "gene", "UMIS", [0 1 2; 3 4 5])

    add_axis!(daf, "cell", ["cell0", "cell1", "cell2"])

    @test_throws dedent("""
        missing axis: gene
        in the daf data: memory!
    """) has_matrix(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the daf data: memory!
    """) matrix_names(daf, "cell", "gene")
    @test_throws dedent("""
        missing axis: gene
        in the daf data: memory!
    """) delete_matrix!(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the daf data: memory!
    """) get_matrix(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the daf data: memory!
    """) set_matrix!(daf, "cell", "gene", "UMIS", [0 1 2; 3 4 5])

    add_axis!(daf, "gene", ["gene0", "gene1"])

    @test !has_matrix(daf, "cell", "gene", "UMIs")
    @test length(matrix_names(daf, "cell", "gene")) == 0
    @test length(matrix_names(daf, "gene", "cell")) == 0
    @test_throws dedent("""
        missing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the daf data: memory!
    """) delete_matrix!(daf, "cell", "gene", "UMIs")
    delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = false)
    @test_throws dedent("""
        missing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the daf data: memory!
    """) get_matrix(daf, "cell", "gene", "UMIs")
    @test_throws dedent("""
        matrix rows: 2
        is different from the length: 3
        of the axis: cell
        in the daf data: memory!
    """) set_matrix!(daf, "cell", "gene", "UMIS", [0 1; 2 3])
    @test_throws dedent("""
        matrix columns: 3
        is different from the length: 2
        of the axis: gene
        in the daf data: memory!
    """) set_matrix!(daf, "cell", "gene", "UMIS", [0 1 3; 4 5 6; 7 8 9])
    @test_throws dedent("""
        default rows: 2
        is different from the length: 3
        of the axis: cell
        in the daf data: memory!
    """) get_matrix(daf, "cell", "gene", "UMIs", default = [0 1; 2 3])
    @test_throws dedent("""
        default columns: 3
        is different from the length: 2
        of the axis: gene
        in the daf data: memory!
    """) get_matrix(daf, "cell", "gene", "UMIs", default = [0 1 3; 4 5 6; 7 8 9])

    @test get_matrix(daf, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [1 2; 3 4; 5 6]
    @test get_matrix(read_only(daf), "cell", "gene", "UMIs"; default = 1) == [1 1; 1 1; 1 1]

    @test_throws dedent("""
        type: Transpose{Int64, Matrix{Int64}} is not in column-major layout
    """) set_matrix!(daf, "cell", "gene", "UMIs", transpose([0 1 2; 3 4 5]))

    bad_names = NamedArray([1 0; 3 2; 5 4])
    setnames!(bad_names, ["cell0", "cell1", "cell3"], 1)
    setnames!(bad_names, ["gene0", "gene1"], 2)

    good_names = NamedArray([1 0; 3 2; 5 4])
    setnames!(good_names, ["cell0", "cell1", "cell2"], 1)
    setnames!(good_names, ["gene0", "gene1"], 2)

    @test_throws dedent("""
        row names of the: matrix
        mismatch the entry names of the axis: cell
        in the daf data: memory!
    """) set_matrix!(daf, "cell", "gene", "UMIs", bad_names)

    @test get_matrix(daf, "cell", "gene", "UMIs"; default = good_names) == good_names

    set_matrix!(daf, "cell", "gene", "UMIs", good_names)
    @test get_matrix(daf, "cell", "gene", "UMIs") == good_names

    set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3; 4 5]; overwrite = true)
    @test_throws dedent("""
        existing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the daf data: memory!
    """) set_matrix!(daf, "cell", "gene", "UMIs", [1 2; 3 4; 5 6])
    @test get_matrix(daf, "cell", "gene", "UMIs") == [0 1; 2 3; 4 5]
    @test get_matrix(daf, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [0 1; 2 3; 4 5]
    @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1) == [0 1; 2 3; 4 5]

    @test description(daf) == dedent("""
        type: MemoryDaf
        name: memory!
        axes:
          cell: 3 entries
          gene: 2 entries
        matrices:
          cell,gene:
            UMIs: 3 x 2 x Int64 (Dense in Columns)
    """) * "\n"

    delete_matrix!(daf, "cell", "gene", "UMIs")

    set_matrix!(daf, "cell", "gene", "UMIs", 1)
    @test get_matrix(daf, "cell", "gene", "UMIs") == [1 1; 1 1; 1 1]

    delete_matrix!(daf, "cell", "gene", "UMIs")
    empty_dense = empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int64)
    @test empty_dense.array isa Matrix{Int64}
    empty_dense .= [0 1; 2 3; 4 0]
    @test get_matrix(daf, "cell", "gene", "UMIs") == [0 1; 2 3; 4 0]

    sparse = SparseMatrixCSC(empty_dense)
    delete_matrix!(daf, "cell", "gene", "UMIs")
    empty_sparse = empty_sparse_matrix!(daf, "cell", "gene", "UMIs", Int64, nnz(sparse), Int8)
    @test empty_sparse.array isa SparseMatrixCSC{Int64, Int8}
    empty_sparse.array.colptr .= sparse.colptr
    empty_sparse.array.rowval .= sparse.rowval
    empty_sparse.array.nzval .= sparse.nzval
    @test empty_sparse == sparse
    @test get_matrix(daf, "cell", "gene", "UMIs") == sparse

    @test description(read_only(daf)) == dedent("""
        type: ReadOnlyView{MemoryDaf}
        name: memory!
        axes:
          cell: 3 entries
          gene: 2 entries
        matrices:
          cell,gene:
            UMIs: 3 x 2 x Int64 (Sparse 67% in Columns)
    """) * "\n"

    delete_axis!(daf, "cell")
    delete_axis!(daf, "gene")

    add_axis!(daf, "cell", ["cell0", "cell1", "cell2"])
    add_axis!(daf, "gene", ["gene0", "gene1"])

    @test !has_matrix(daf, "cell", "gene", "UMIs")

    return nothing
end

@testset "daf" begin
    @testset "memory" begin
        test_format_scalar(MemoryDaf("memory!"))
        test_format_axis(MemoryDaf("memory!"))
        test_format_vector(MemoryDaf("memory!"))
        test_format_matrix(MemoryDaf("memory!"))
    end
end
