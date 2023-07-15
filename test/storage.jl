function test_storage_scalar(storage::AbstractStorage)::Nothing
    @test !has_scalar(storage, "version")
    @test length(scalar_names(storage)) == 0

    @test_throws dedent("""
        missing scalar property: version
        in the storage: memory!
    """) get_scalar(storage, "version")
    @test get_scalar(storage, "version"; default = 3) == 3

    @test_throws dedent("""
        missing scalar property: version
        in the storage: memory!
    """) delete_scalar!(storage, "version")
    delete_scalar!(storage, "version"; must_exist = false)

    set_scalar!(storage, "version", "1.2")
    @test_throws dedent("""
        existing scalar property: version
        in the storage: memory!
    """) set_scalar!(storage, "version", "4.5")

    @test length(scalar_names(storage)) == 1
    @test "version" in scalar_names(storage)

    @test get_scalar(storage, "version") == "1.2"
    @test get_scalar(storage, "version"; default = "3.4") == "1.2"

    @test description(storage) == dedent("""
        type: MemoryStorage
        name: memory!
        scalars:
          version: "1.2"
    """) * "\n"

    delete_scalar!(storage, "version")
    @test !has_scalar(storage, "version")
    @test length(scalar_names(storage)) == 0

    return nothing
end

function test_storage_axis(storage::AbstractStorage)::Nothing
    name = storage.name

    @test !has_axis(storage, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) get_axis(storage, "cell")
    delete_axis!(storage, "cell"; must_exist = false)
    @test length(axis_names(storage)) == 0

    repeated_cell_names = vec(["cell1", "cell1", "cell3"])
    @test_throws dedent("""
        non-unique entries for new axis: cell
        in the storage: memory!
    """) add_axis!(storage, "cell", repeated_cell_names)

    cell_names = vec(["cell1", "cell2", "cell3"])
    add_axis!(storage, "cell", cell_names)
    @test length(axis_names(storage)) == 1
    @test "cell" in axis_names(storage)

    @test has_axis(storage, "cell")
    @test axis_length(storage, "cell") == 3
    @test get_axis(storage, "cell") === cell_names

    @test_throws dedent("""
        existing axis: cell
        in the storage: memory!
    """) add_axis!(storage, "cell", cell_names)

    @test description(storage) == dedent("""
        type: MemoryStorage
        name: memory!
        axes:
          cell: 3 entries
    """) * "\n"

    delete_axis!(storage, "cell")
    @test !has_axis(storage, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) delete_axis!(storage, "cell")
    @test length(axis_names(storage)) == 0

    return nothing
end

function test_storage_vector(storage::AbstractStorage)::Nothing
    name = storage.name

    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) has_vector(storage, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) vector_names(storage, "cell")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) delete_vector!(storage, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) get_vector(storage, "cell", "age")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) set_vector!(storage, "cell", "age", vec([0 1 2]))

    add_axis!(storage, "cell", vec(["cell0", "cell1", "cell3"]))
    @test !has_vector(storage, "cell", "age")
    @test length(vector_names(storage, "cell")) == 0
    @test_throws dedent("""
        missing vector property: age
        for the axis: cell
        in the storage: memory!
    """) delete_vector!(storage, "cell", "age")
    delete_vector!(storage, "cell", "age"; must_exist = false)
    @test_throws dedent("""
        missing vector property: age
        for the axis: cell
        in the storage: memory!
    """) get_vector(storage, "cell", "age")
    @test_throws dedent("""
        vector length: 2
        is different from the length: 3
        of the axis: cell
        in the storage: memory!
    """) set_vector!(storage, "cell", "age", vec([0 1]))
    @test_throws dedent("""
        default length: 2
        is different from the length: 3
        of the axis: cell
        in the storage: memory!
    """) get_vector(storage, "cell", "age"; default = vec([1 2]))
    @test get_vector(storage, "cell", "age"; default = vec([1 2 3])) == vec([1 2 3])
    @test get_vector(storage, "cell", "age"; default = 1) == vec([1 1 1])

    @test_throws dedent("""
        setting the reserved property: name
        for the axis: cell
        in the storage: memory!
    """) set_vector!(storage, "cell", "name", vec([0 1]))
    @test has_vector(storage, "cell", "name")
    @test get_vector(storage, "cell", "name") == get_axis(storage, "cell")

    set_vector!(storage, "cell", "age", vec([0 1 2]))
    @test_throws dedent("""
        existing vector property: age
        for the axis: cell
        in the storage: memory!
    """) set_vector!(storage, "cell", "age", vec([1 2 3]))
    @test length(vector_names(storage, "cell")) == 1
    @test "age" in vector_names(storage, "cell")
    @test get_vector(storage, "cell", "age") == vec([0 1 2])

    @test description(storage) == dedent("""
        type: MemoryStorage
        name: memory!
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
    @test get_vector(storage, "cell", "age") == vec([1 1 1])

    delete_vector!(storage, "cell", "age")
    empty_dense = empty_dense_vector!(storage, "cell", "age", Int64)
    @test empty_dense isa Vector{Int64}
    empty_dense .= vec([0 1 2])
    @test get_vector(storage, "cell", "age") == vec([0 1 2])

    sparse = SparseVector(empty_dense)
    delete_vector!(storage, "cell", "age")
    empty_sparse = empty_sparse_vector!(storage, "cell", "age", Int64, nnz(sparse), Int8)
    @test empty_sparse isa SparseVector{Int64, Int8}
    empty_sparse.nzind .= sparse.nzind
    empty_sparse.nzval .= sparse.nzval
    @test empty_sparse == sparse
    @test get_vector(storage, "cell", "age") == sparse

    delete_axis!(storage, "cell")
    add_axis!(storage, "cell", vec(["cell0", "cell1"]))

    @test !has_vector(storage, "cell", "age")

    return nothing
end

function test_storage_matrix(storage::AbstractStorage)::Nothing
    name = storage.name

    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) has_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) matrix_names(storage, "cell", "gene")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) delete_matrix!(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: cell
        in the storage: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1 2; 3 4 5])

    add_axis!(storage, "cell", vec(["cell0", "cell1", "cell2"]))

    @test_throws dedent("""
        missing axis: gene
        in the storage: memory!
    """) has_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the storage: memory!
    """) matrix_names(storage, "cell", "gene")
    @test_throws dedent("""
        missing axis: gene
        in the storage: memory!
    """) delete_matrix!(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the storage: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        missing axis: gene
        in the storage: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1 2; 3 4 5])

    add_axis!(storage, "gene", vec(["gene0", "gene1"]))

    @test !has_matrix(storage, "cell", "gene", "UMIs")
    @test length(matrix_names(storage, "cell", "gene")) == 0
    @test length(matrix_names(storage, "gene", "cell")) == 0
    @test_throws dedent("""
        missing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the storage: memory!
    """) delete_matrix!(storage, "cell", "gene", "UMIs")
    delete_matrix!(storage, "cell", "gene", "UMIs"; must_exist = false)
    @test_throws dedent("""
        missing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the storage: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws dedent("""
        matrix rows: 2
        is different from the length: 3
        of the axis: cell
        in the storage: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1; 2 3])
    @test_throws dedent("""
        matrix columns: 3
        is different from the length: 2
        of the axis: gene
        in the storage: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIS", [0 1 3; 4 5 6; 7 8 9])
    @test_throws dedent("""
        default rows: 2
        is different from the length: 3
        of the axis: cell
        in the storage: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs", default = [0 1; 2 3])
    @test_throws dedent("""
        default columns: 3
        is different from the length: 2
        of the axis: gene
        in the storage: memory!
    """) get_matrix(storage, "cell", "gene", "UMIs", default = [0 1 3; 4 5 6; 7 8 9])

    @test get_matrix(storage, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [1 2; 3 4; 5 6]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = 1) == [1 1; 1 1; 1 1]

    @test_throws dedent("""
        type: Daf.AsDense.DenseView{Int64, 2, Transpose{Int64, Matrix{Int64}}} is not in column-major layout
    """) set_matrix!(storage, "cell", "gene", "UMIs", as_dense_or_fail(transpose([0 1 2; 3 4 5])))

    set_matrix!(storage, "cell", "gene", "UMIs", [0 1; 2 3; 4 5])
    @test_throws dedent("""
        existing matrix property: UMIs
        for the rows axis: cell
        and the columns axis: gene
        in the storage: memory!
    """) set_matrix!(storage, "cell", "gene", "UMIs", [1 2; 3 4; 5 6])
    @test get_matrix(storage, "cell", "gene", "UMIs") == [0 1; 2 3; 4 5]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [0 1; 2 3; 4 5]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = 1) == [0 1; 2 3; 4 5]

    @test description(storage) == dedent("""
        type: MemoryStorage
        name: memory!
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
    @test empty_dense isa Matrix{Int64}
    empty_dense .= [0 1; 2 3; 4 0]
    @test get_matrix(storage, "cell", "gene", "UMIs") == [0 1; 2 3; 4 0]

    sparse = SparseMatrixCSC(empty_dense)
    delete_matrix!(storage, "cell", "gene", "UMIs")
    empty_sparse = empty_sparse_matrix!(storage, "cell", "gene", "UMIs", Int64, nnz(sparse), Int8)
    @test empty_sparse isa SparseMatrixCSC{Int64, Int8}
    empty_sparse.colptr .= sparse.colptr
    empty_sparse.rowval .= sparse.rowval
    empty_sparse.nzval .= sparse.nzval
    @test empty_sparse == sparse
    @test get_matrix(storage, "cell", "gene", "UMIs") == sparse

    delete_axis!(storage, "cell")
    delete_axis!(storage, "gene")

    add_axis!(storage, "cell", vec(["cell0", "cell1", "cell2"]))
    add_axis!(storage, "gene", vec(["gene0", "gene1"]))

    @test !has_matrix(storage, "cell", "gene", "UMIs")

    return nothing
end

@testset "storage" begin
    @testset "memory" begin
        test_storage_scalar(MemoryStorage("memory!"))
        test_storage_axis(MemoryStorage("memory!"))
        test_storage_vector(MemoryStorage("memory!"))
        test_storage_matrix(MemoryStorage("memory!"))
    end
end
