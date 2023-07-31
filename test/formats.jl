import Daf.Formats.Format

function test_storage_scalar(storage::Format)::Nothing
    @test !has_scalar(storage, "version")
    @test length(scalar_names(storage)) == 0

    @test_throws dedent("""
        missing scalar property: version
        in the Daf.Container: memory!
    """) get_scalar(storage, "version")
    @test get_scalar(storage, "version"; default = 3) == 3

    @test_throws dedent("""
        missing scalar property: version
        in the Daf.Container: memory!
    """) delete_scalar!(storage, "version")
    delete_scalar!(storage, "version"; must_exist = false)

    set_scalar!(storage, "version", "1.2")
    @test_throws dedent("""
        existing scalar property: version
        in the Daf.Container: memory!
    """) set_scalar!(storage, "version", "4.5")

    @test length(scalar_names(storage)) == 1
    @test "version" in scalar_names(storage)

    @test get_scalar(storage, "version") == "1.2"
    @test get_scalar(storage, "version"; default = "3.4") == "1.2"

    with_read_only!(storage) do
        @test description(storage) == dedent("""
            type: MemoryContainer
            name: memory!
            is_read_only: True
            scalars:
              version: "1.2"
        """) * "\n"
    end

    @test description(storage) == dedent("""
        type: MemoryContainer
        name: memory!
        is_read_only: False
        scalars:
          version: "1.2"
    """) * "\n"

    @test !is_read_only(storage)
    @test read_only!(storage) == false
    @test is_read_only(storage)
    @test_throws "delete_scalar! for read-only Daf.Container: memory!" delete_scalar!(storage, "version")
    @test read_only!(storage)
    @test read_only!(storage, false) == true
    @test !is_read_only(storage)

    with_read_only!(storage) do
        @test is_read_only(storage)
        @test_throws "delete_scalar! for read-only Daf.Container: memory!" delete_scalar!(storage, "version")
    end
    @test !is_read_only(storage)

    delete_scalar!(storage, "version")

    @test !has_scalar(storage, "version")
    @test length(scalar_names(storage)) == 0

    return nothing
end

function test_storage_axis(storage::Format)::Nothing
    name = storage.name

    @test !has_axis(storage, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) get_axis(storage, "cell")
    delete_axis!(storage, "cell"; must_exist = false)
    @test length(axis_names(storage)) == 0

    repeated_cell_names = ["cell1", "cell1", "cell3"]
    @test_throws dedent("""
        non-unique entries for new axis: cell
        in the Daf.Container: memory!
    """) add_axis!(storage, "cell", repeated_cell_names)

    cell_names = ["cell1", "cell2", "cell3"]
    add_axis!(storage, "cell", cell_names)
    @test length(axis_names(storage)) == 1
    @test "cell" in axis_names(storage)

    @test has_axis(storage, "cell")
    @test axis_length(storage, "cell") == 3
    @test get_axis(storage, "cell") == cell_names

    @test_throws dedent("""
        existing axis: cell
        in the Daf.Container: memory!
    """) add_axis!(storage, "cell", cell_names)

    @test description(storage) == dedent("""
        type: MemoryContainer
        name: memory!
        is_read_only: False
        axes:
          cell: 3 entries
    """) * "\n"

    delete_axis!(storage, "cell")
    @test !has_axis(storage, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) delete_axis!(storage, "cell")
    @test length(axis_names(storage)) == 0

    return nothing
end

function test_storage_vector(storage::Format)::Nothing
    name = storage.name

    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) has_vector(storage, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) vector_names(storage, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) delete_vector!(storage, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) get_vector(storage, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) set_vector!(storage, "cell", "age", [0, 1, 2])

    add_axis!(storage, "cell", ["cell0", "cell1", "cell3"])
    @test !has_vector(storage, "cell", "age")
    @test length(vector_names(storage, "cell")) == 0
    @test_throws dedent("""
        missing vector property: age
        for the axis: cell
        in the Daf.Container: memory!
    """) delete_vector!(storage, "cell", "age")
    delete_vector!(storage, "cell", "age"; must_exist = false)
    @test_throws dedent("""
        missing vector property: age
        for the axis: cell
        in the Daf.Container: memory!
    """) get_vector(storage, "cell", "age")
    @test_throws dedent("""
        vector length: 2
        is different from the length: 3
        of the axis: cell
        in the Daf.Container: memory!
    """) set_vector!(storage, "cell", "age", [0, 1])

    bad_names = NamedArray([0, 1, 2])
    setnames!(bad_names, ["cell0", "cell1", "cell2"], 1)

    good_names = NamedArray([2, 1, 0])
    setnames!(good_names, ["cell0", "cell1", "cell3"], 1)

    @test_throws dedent("""
        entry names of the: vector
        mismatch the entry names of the axis: cell
        in the Daf.Container: memory!
    """) set_vector!(storage, "cell", "age", bad_names)

    @test_throws dedent("""
        default length: 2
        is different from the length: 3
        of the axis: cell
        in the Daf.Container: memory!
    """) get_vector(storage, "cell", "age"; default = [1, 2])
    @test get_vector(storage, "cell", "age"; default = [1, 2, 3]) == [1, 2, 3]
    with_read_only!(storage) do
        @test get_vector(storage, "cell", "age"; default = 1) == [1, 1, 1]
    end
    @test_throws dedent("""
        entry names of the: default vector
        mismatch the entry names of the axis: cell
        in the Daf.Container: memory!
    """) get_vector(storage, "cell", "age"; default = bad_names)
    @test get_vector(storage, "cell", "age"; default = good_names) == good_names

    @test_throws dedent("""
        setting the reserved property: name
        for the axis: cell
        in the Daf.Container: memory!
    """) set_vector!(storage, "cell", "name", [0, 1])
    @test has_vector(storage, "cell", "name")
    @test get_vector(storage, "cell", "name") == get_axis(storage, "cell")

    set_vector!(storage, "cell", "age", good_names)
    @test get_vector(storage, "cell", "age") == [2, 1, 0]

    set_vector!(storage, "cell", "age", [0, 1, 2]; overwrite = true)
    @test_throws dedent("""
        existing vector property: age
        for the axis: cell
        in the Daf.Container: memory!
    """) set_vector!(storage, "cell", "age", [1, 2, 3])
    @test length(vector_names(storage, "cell")) == 1
    @test "age" in vector_names(storage, "cell")
    @test get_vector(storage, "cell", "age") == [0, 1, 2]
    @test names(get_vector(storage, "cell", "age"), 1) == ["cell0", "cell1", "cell3"]

    @test description(storage) == dedent("""
        type: MemoryContainer
        name: memory!
        is_read_only: False
        axes:
          cell: 3 entries
        vectors:
          cell:
            age: 3 x Int64 (Dense)
    """) * "\n"

    delete_vector!(storage, "cell", "age")
    @test !has_vector(storage, "cell", "age")

    set_vector!(storage, "cell", "age", 1)
    @test has_vector(storage, "cell", "age")
    @test get_vector(storage, "cell", "age") == [1, 1, 1]

    delete_vector!(storage, "cell", "age")
    empty_dense = empty_dense_vector!(storage, "cell", "age", Int64)
    @test empty_dense.array isa Vector{Int64}
    empty_dense .= [0, 1, 2]
    @test get_vector(storage, "cell", "age") == [0, 1, 2]

    sparse = SparseVector(empty_dense)
    delete_vector!(storage, "cell", "age")
    empty_sparse = empty_sparse_vector!(storage, "cell", "age", Int64, nnz(sparse), Int8)
    @test empty_sparse.array isa SparseVector{Int64, Int8}
    empty_sparse.array.nzind .= sparse.nzind
    empty_sparse.array.nzval .= sparse.nzval
    @test empty_sparse == sparse
    @test get_vector(storage, "cell", "age") == sparse

    delete_axis!(storage, "cell")
    add_axis!(storage, "cell", ["cell0", "cell1"])

    @test !has_vector(storage, "cell", "age")

    return nothing
end

function test_storage_matrix(storage::Format)::Nothing
    name = storage.name

    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) has_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) matrix_names(storage, "cell", "gene")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) delete_matrix!(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the Daf.Container: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1 2; 3 4 5])

    add_axis!(storage, "cell", ["cell0", "cell1", "cell2"])

    @test_throws dedent("""
        missing axis: gene
        in the Daf.Container: memory!
    """) has_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the Daf.Container: memory!
    """) matrix_names(storage, "cell", "gene")
    @test_throws dedent("""
        missing axis: gene
        in the Daf.Container: memory!
    """) delete_matrix!(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the Daf.Container: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the Daf.Container: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1 2; 3 4 5])

    add_axis!(storage, "gene", ["gene0", "gene1"])

    @test !has_matrix(storage, "cell", "gene", "UMIs")
    @test length(matrix_names(storage, "cell", "gene")) == 0
    @test length(matrix_names(storage, "gene", "cell")) == 0
    @test_throws dedent("""
        missing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the Daf.Container: memory!
    """) delete_matrix!(storage, "cell", "gene", "UMIs")
    delete_matrix!(storage, "cell", "gene", "UMIs"; must_exist = false)
    @test_throws dedent("""
        missing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the Daf.Container: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        matrix rows: 2
        is different from the length: 3
        of the axis: cell
        in the Daf.Container: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1; 2 3])
    @test_throws dedent("""
        matrix columns: 3
        is different from the length: 2
        of the axis: gene
        in the Daf.Container: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1 3; 4 5 6; 7 8 9])
    @test_throws dedent("""
        default rows: 2
        is different from the length: 3
        of the axis: cell
        in the Daf.Container: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs", default = [0 1; 2 3])
    @test_throws dedent("""
        default columns: 3
        is different from the length: 2
        of the axis: gene
        in the Daf.Container: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs", default = [0 1 3; 4 5 6; 7 8 9])

    @test get_matrix(storage, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [1 2; 3 4; 5 6]
    with_read_only!(storage) do
        @test get_matrix(storage, "cell", "gene", "UMIs"; default = 1) == [1 1; 1 1; 1 1]
    end

    @test_throws dedent("""
        type: Transpose{Int64, Matrix{Int64}} is not in column-major layout
    """) set_matrix!(storage, "cell", "gene", "UMIs", transpose([0 1 2; 3 4 5]))

    bad_names = NamedArray([1 0; 3 2; 5 4])
    setnames!(bad_names, ["cell0", "cell1", "cell3"], 1)
    setnames!(bad_names, ["gene0", "gene1"], 2)

    good_names = NamedArray([1 0; 3 2; 5 4])
    setnames!(good_names, ["cell0", "cell1", "cell2"], 1)
    setnames!(good_names, ["gene0", "gene1"], 2)

    @test_throws dedent("""
        row names of the: matrix
        mismatch the entry names of the axis: cell
        in the Daf.Container: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIs", bad_names)

    @test get_matrix(storage, "cell", "gene", "UMIs"; default = good_names) == good_names

    set_matrix!(storage, "cell", "gene", "UMIs", good_names)
    @test get_matrix(storage, "cell", "gene", "UMIs") == good_names

    set_matrix!(storage, "cell", "gene", "UMIs", [0 1; 2 3; 4 5]; overwrite = true)
    @test_throws dedent("""
        existing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the Daf.Container: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIs", [1 2; 3 4; 5 6])
    @test get_matrix(storage, "cell", "gene", "UMIs") == [0 1; 2 3; 4 5]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [0 1; 2 3; 4 5]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = 1) == [0 1; 2 3; 4 5]

    @test description(storage) == dedent("""
        type: MemoryContainer
        name: memory!
        is_read_only: False
        axes:
          cell: 3 entries
          gene: 2 entries
        matrices:
          cell,gene:
            UMIs: 3 x 2 x Int64 (Dense in Columns)
    """) * "\n"

    delete_matrix!(storage, "cell", "gene", "UMIs")

    set_matrix!(storage, "cell", "gene", "UMIs", 1)
    @test get_matrix(storage, "cell", "gene", "UMIs") == [1 1; 1 1; 1 1]

    delete_matrix!(storage, "cell", "gene", "UMIs")
    empty_dense = empty_dense_matrix!(storage, "cell", "gene", "UMIs", Int64)
    @test empty_dense.array isa Matrix{Int64}
    empty_dense .= [0 1; 2 3; 4 0]
    @test get_matrix(storage, "cell", "gene", "UMIs") == [0 1; 2 3; 4 0]

    sparse = SparseMatrixCSC(empty_dense)
    delete_matrix!(storage, "cell", "gene", "UMIs")
    empty_sparse = empty_sparse_matrix!(storage, "cell", "gene", "UMIs", Int64, nnz(sparse), Int8)
    @test empty_sparse.array isa SparseMatrixCSC{Int64, Int8}
    empty_sparse.array.colptr .= sparse.colptr
    empty_sparse.array.rowval .= sparse.rowval
    empty_sparse.array.nzval .= sparse.nzval
    @test empty_sparse == sparse
    @test get_matrix(storage, "cell", "gene", "UMIs") == sparse

    with_read_only!(storage) do
        @test description(storage) == dedent("""
            type: MemoryContainer
            name: memory!
            is_read_only: True
            axes:
              cell: 3 entries
              gene: 2 entries
            matrices:
              cell,gene:
                UMIs: 3 x 2 x Int64 (Sparse 67% in Columns)
        """) * "\n"
    end

    delete_axis!(storage, "cell")
    delete_axis!(storage, "gene")

    add_axis!(storage, "cell", ["cell0", "cell1", "cell2"])
    add_axis!(storage, "gene", ["gene0", "gene1"])

    @test !has_matrix(storage, "cell", "gene", "UMIs")

    return nothing
end

@testset "storage" begin
    @testset "memory" begin
        test_storage_scalar(MemoryContainer("memory!"))
        test_storage_axis(MemoryContainer("memory!"))
        test_storage_vector(MemoryContainer("memory!"))
        test_storage_matrix(MemoryContainer("memory!"))
    end
end