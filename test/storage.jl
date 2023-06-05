using Daf

function test_storage_scalar(storage::AbstractStorage)::Nothing
    @test !has_scalar(storage, "version")
    @test length(scalar_names(storage)) == 0

    @test_throws "missing scalar: version in the storage: memory" get_scalar(storage, "version")
    @test get_scalar(storage, "version"; default = (3, 4)) == (3, 4)

    @test_throws "missing scalar: version in the storage: memory" delete_scalar!(storage, "version")
    delete_scalar!(storage, "version"; must_exist = false)

    @test !is_frozen(storage)
    freeze(storage)
    @test is_frozen(storage)
    @test_throws "frozen storage: memory" delete_scalar!(storage, "version")
    unfreeze(storage)
    @test !is_frozen(storage)

    set_scalar!(storage, "version", (1, 2))
    @test_throws "existing scalar: version in the storage: memory" set_scalar!(storage, "version", (4, 5))

    @test length(scalar_names(storage)) == 1
    @test "version" in scalar_names(storage)

    @test get_scalar(storage, "version") == (1, 2)
    @test get_scalar(storage, "version"; default = (3, 4)) == (1, 2)

    delete_scalar!(storage, "version")
    @test !has_scalar(storage, "version")
    @test length(scalar_names(storage)) == 0

    return nothing
end

function test_storage_axis(storage::AbstractStorage)::Nothing
    @test !has_axis(storage, "cell")
    @test_throws "missing axis: cell in the storage: memory" get_axis(storage, "cell")
    delete_axis!(storage, "cell"; must_exist = false)
    @test length(axis_names(storage)) == 0

    repeated_cell_names = vec(["cell1", "cell1", "cell3"])
    @test_throws "non-unique entries for new axis: cell in the storage: memory" add_axis!(
        storage,  # only seems untested
        "cell",  # only seems untested
        repeated_cell_names,  # only seems untested
    )

    cell_names = vec(["cell1", "cell2", "cell3"])
    add_axis!(storage, "cell", cell_names)
    @test length(axis_names(storage)) == 1
    @test "cell" in axis_names(storage)

    @test has_axis(storage, "cell")
    @test axis_length(storage, "cell") == 3
    @test get_axis(storage, "cell") === cell_names

    @test_throws "existing axis: cell in the storage: memory" add_axis!(storage, "cell", cell_names)

    delete_axis!(storage, "cell")
    @test !has_axis(storage, "cell")
    @test_throws "missing axis: cell in the storage: memory" delete_axis!(storage, "cell")
    @test length(axis_names(storage)) == 0

    return nothing
end

function test_storage_vector(storage::AbstractStorage)::Nothing
    @test_throws "missing axis: cell in the storage: memory" has_vector(storage, "cell", "age")
    @test_throws "missing axis: cell in the storage: memory" vector_names(storage, "cell")
    @test_throws "missing axis: cell in the storage: memory" delete_vector!(storage, "cell", "age")
    @test_throws "missing axis: cell in the storage: memory" get_vector(storage, "cell", "age")
    @test_throws "missing axis: cell in the storage: memory" set_vector!(storage, "cell", "age", vec([0 1 2]))

    add_axis!(storage, "cell", vec(["cell0", "cell1", "cell3"]))
    @test !has_vector(storage, "cell", "age")
    @test length(vector_names(storage, "cell")) == 0
    @test_throws "missing vector: age for the axis: cell in the storage: memory" delete_vector!(storage, "cell", "age")
    delete_vector!(storage, "cell", "age"; must_exist = false)
    @test_throws "missing vector: age for the axis: cell in the storage: memory" get_vector(storage, "cell", "age")
    @test_throws "vector length: 2 is different from axis: cell length: 3 in the storage: storage_name" set_vector!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "age",  # only seems untested
        vec([0 1]),  # only seems untested
    )
    @test_throws "default length: 2 is different from axis: cell length: 3 in the storage: storage_name" get_vector(
        storage,  # only seems untested
        "cell",  # only seems untested
        "age";  # only seems untested
        default = vec([1 2]),  # only seems untested
    )
    @test get_vector(storage, "cell", "age"; default = vec([1 2 3])) == vec([1 2 3])
    @test get_vector(storage, "cell", "age"; default = 1) == vec([1 1 1])

    set_vector!(storage, "cell", "age", vec([0 1 2]))
    @test_throws "existing vector: age for the axis: cell in the storage: memory" set_vector!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "age",  # only seems untested
        vec([1 2 3]),  # only seems untested
    )
    @test length(vector_names(storage, "cell")) == 1
    @test "age" in vector_names(storage, "cell")
    @test get_vector(storage, "cell", "age") == vec([0 1 2])

    delete_vector!(storage, "cell", "age")
    @test !has_vector(storage, "cell", "age")

    set_vector!(storage, "cell", "age", 1)
    @test has_vector(storage, "cell", "age")
    @test get_vector(storage, "cell", "age") == vec([1 1 1])
    delete_axis!(storage, "cell")
    add_axis!(storage, "cell", vec(["cell0", "cell1"]))
    @test !has_vector(storage, "cell", "age")

    return nothing
end

function test_storage_matrix(storage::AbstractStorage)::Nothing
    @test_throws "missing axis: cell in the storage: memory" has_matrix(storage, "cell", "gene", "UMIs")
    @test_throws "missing axis: cell in the storage: memory" matrix_names(storage, "cell", "gene")
    @test_throws "missing axis: cell in the storage: memory" delete_matrix!(storage, "cell", "gene", "UMIs")
    @test_throws "missing axis: cell in the storage: memory" get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws "missing axis: cell in the storage: memory" set_matrix!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIS",  # only seems untested
        [0 1 2; 3 4 5],  # only seems untested
    )

    add_axis!(storage, "cell", vec(["cell0", "cell1", "cell2"]))

    @test_throws "missing axis: gene in the storage: memory" has_matrix(storage, "cell", "gene", "UMIs")
    @test_throws "missing axis: gene in the storage: memory" matrix_names(storage, "cell", "gene")
    @test_throws "missing axis: gene in the storage: memory" delete_matrix!(storage, "cell", "gene", "UMIs")
    @test_throws "missing axis: gene in the storage: memory" get_matrix(storage, "cell", "gene", "UMIs")
    @test_throws "missing axis: gene in the storage: memory" set_matrix!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIS",  # only seems untested
        [0 1 2; 3 4 5],  # only seems untested
    )

    add_axis!(storage, "gene", vec(["gene0", "gene1"]))

    @test !has_matrix(storage, "cell", "gene", "UMIs")
    @test length(matrix_names(storage, "cell", "gene")) == 0
    @test length(matrix_names(storage, "gene", "cell")) == 0
    @test_throws "missing matrix: UMIs for the rows axis: cell and the columns axis: gene in the storage: memory" delete_matrix!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIs",  # only seems untested
    )
    delete_matrix!(storage, "cell", "gene", "UMIs"; must_exist = false)
    @test_throws "missing matrix: UMIs for the rows axis: cell and the columns axis: gene in the storage: memory" get_matrix(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIs",  # only seems untested
    )
    @test_throws "matrix rows: 2 is different from axis: cell length: 3 in the storage: storage_name" set_matrix!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIS",  # only seems untested
        [0 1; 2 3],  # only seems untested
    )
    @test_throws "matrix columns: 3 is different from axis: gene length: 2 in the storage: storage_name" set_matrix!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIS",  # only seems untested
        [0 1 3; 4 5 6; 7 8 9],  # only seems untested
    )
    @test_throws "default rows: 2 is different from axis: cell length: 3 in the storage: storage_name" get_matrix(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIs",  # only seems untested
        default = [0 1; 2 3],  # only seems untested
    )
    @test_throws "default columns: 3 is different from axis: gene length: 2 in the storage: storage_name" get_matrix(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIs",  # only seems untested
        default = [0 1 3; 4 5 6; 7 8 9],  # only seems untested
    )

    @test get_matrix(storage, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [1 2; 3 4; 5 6]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = 1) == [1 1; 1 1; 1 1]

    set_matrix!(storage, "cell", "gene", "UMIs", [0 1; 2 3; 4 5])
    @test_throws "existing matrix: UMIs for the rows axis: cell and the columns axis: gene in the storage: memory" set_matrix!(
        storage,  # only seems untested
        "cell",  # only seems untested
        "gene",  # only seems untested
        "UMIs",  # only seems untested
        [1 2; 3 4; 5 6],  # only seems untested
    )
    @test get_matrix(storage, "cell", "gene", "UMIs") == [0 1; 2 3; 4 5]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = [1 2; 3 4; 5 6]) == [0 1; 2 3; 4 5]
    @test get_matrix(storage, "cell", "gene", "UMIs"; default = 1) == [0 1; 2 3; 4 5]

    delete_matrix!(storage, "cell", "gene", "UMIs")

    set_matrix!(storage, "cell", "gene", "UMIs", 1)
    @test get_matrix(storage, "cell", "gene", "UMIs") == [1 1; 1 1; 1 1]

    delete_axis!(storage, "cell")
    delete_axis!(storage, "gene")

    add_axis!(storage, "cell", vec(["cell0", "cell1", "cell2"]))
    add_axis!(storage, "gene", vec(["gene0", "gene1"]))

    @test !has_matrix(storage, "cell", "gene", "UMIs")

    return nothing
end

@testset "storage" begin
    @testset "memory" begin
        @test storage_name(MemoryStorage("memory")) == "memory"
        test_storage_scalar(MemoryStorage("memory"))
        test_storage_axis(MemoryStorage("memory"))
        test_storage_vector(MemoryStorage("memory"))
        test_storage_matrix(MemoryStorage("memory"))
    end
end
