using Daf

function test_storage_scalar(storage::AbstractStorage)::Nothing
    @test !has_scalar(storage, "version")

    @test_throws "missing scalar: version in storage: memory" get_scalar(storage, "version")
    @test get_scalar(storage, "version"; default = (3, 4)) == (3, 4)

    @test_throws "missing scalar: version in storage: memory" delete_scalar!(storage, "version")
    delete_scalar!(storage, "version"; must_exist = false)

    set_scalar!(storage, "version", (1, 2))
    @test_throws "existing scalar: version in storage: memory" set_scalar!(storage, "version", (4, 5))

    @test get_scalar(storage, "version") == (1, 2)
    @test get_scalar(storage, "version"; default = (3, 4)) == (1, 2)

    delete_scalar!(storage, "version")
    @test !has_scalar(storage, "version")

    return nothing
end

function test_storage_axis(storage::AbstractStorage)::Nothing
    @test !has_axis(storage, "cell")
    @test_throws "missing axis: cell in storage: memory" get_axis(storage, "cell")
    delete_axis!(storage, "cell"; must_exist = false)

    repeated_cell_names = vec(["cell1", "cell1", "cell3"])
    @test_throws "non-unique entries for new axis: cell in storage: memory" add_axis!(
        storage,  # only seems untested
        "cell",  # only seems untested
        repeated_cell_names,  # only seems untested
    )

    cell_names = vec(["cell1", "cell2", "cell3"])
    add_axis!(storage, "cell", cell_names)

    @test has_axis(storage, "cell")
    @test axis_length(storage, "cell") == 3
    @test get_axis(storage, "cell") === cell_names

    @test_throws "existing axis: cell in storage: memory" add_axis!(storage, "cell", cell_names)

    delete_axis!(storage, "cell")
    @test !has_axis(storage, "cell")
    @test_throws "missing axis: cell in storage: memory" delete_axis!(storage, "cell")

    return nothing
end

struct BadStorage <: AbstractStorage
    BadStorage() = new()
end

struct LyingStorage <: AbstractStorage
    lie::Bool
end

function Storage.has_scalar(storage::LyingStorage, name::String)::Bool
    return storage.lie
end

function Storage.has_axis(storage::LyingStorage, axis::String)::Bool
    return storage.lie
end

@testset "storage" begin
    @testset "bad_storage" begin
        bad_storage = BadStorage()

        @test_throws "missing method: storage_name for storage type: BadStorage" storage_name(bad_storage)
        @test_throws "missing method: has_scalar for storage type: BadStorage" has_scalar(bad_storage, "version")
        @test_throws "missing method: has_axis for storage type: BadStorage" has_axis(bad_storage, "cell")

        bad_storage = LyingStorage(true)

        @test_throws "missing method: unsafe_delete_scalar! for storage type: LyingStorage" delete_scalar!(
            bad_storage,
            "version",
        )
        @test_throws "missing method: unsafe_get_scalar for storage type: LyingStorage" get_scalar(
            bad_storage,
            "version",
        )
        @test_throws "missing method: unsafe_delete_axis! for storage type: LyingStorage" delete_axis!(
            bad_storage,
            "cell",
        )
        @test_throws "missing method: unsafe_get_axis for storage type: LyingStorage" get_axis(bad_storage, "cell")
        @test_throws "missing method: unsafe_axis_length for storage type: LyingStorage" axis_length(
            bad_storage,
            "cell",
        )

        bad_storage = LyingStorage(false)

        delete_scalar!(bad_storage, "version"; must_exist = false)
        @test get_scalar(bad_storage, "version"; default = (1, 2)) == (1, 2)
        @test_throws "missing method: unsafe_set_scalar! for storage type: LyingStorage" set_scalar!(
            bad_storage,
            "version",
            (1, 2),
        )
        @test_throws "missing method: unsafe_add_axis! for storage type: LyingStorage" add_axis!(
            bad_storage,
            "cell",
            vec(["cell0"]),
        )
    end

    @testset "memory" begin
        storage = MemoryStorage("memory")
        @test storage_name(storage) == "memory"
        test_storage_scalar(storage)
        test_storage_axis(storage)
    end
end
