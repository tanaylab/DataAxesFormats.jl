using Daf

function test_storage(storage::AbstractStorage, storage_name::String)
    @test name(storage) == storage_name

    @test !has_axis(storage, "cell")
    @test_throws "missing axis: cell in storage: memory" axis_entries(storage, "cell")

    repeated_cell_names = vec(["cell1", "cell1", "cell3"])
    @test_throws "non-unique entries for new axis: cell in storage: memory" add_axis!(
        storage,
        "cell",
        repeated_cell_names,
    )

    cell_names = vec(["cell1", "cell2", "cell3"])
    add_axis!(storage, "cell", cell_names)

    @test has_axis(storage, "cell")
    @test axis_length(storage, "cell") == 3
    @test axis_entries(storage, "cell") === cell_names

    @test_throws "existing axis: cell in storage: memory" add_axis!(storage, "cell", cell_names)

    delete_axis!(storage, "cell")
    @test !has_axis(storage, "cell")
    @test_throws "missing axis: cell in storage: memory" delete_axis!(storage, "cell")
end

struct BadStorage <: AbstractStorage
    BadStorage() = new()
end

struct LyingStorage <: AbstractStorage
    lie::Bool
end

function Storage.has_axis(storage::LyingStorage, axis::String)::Bool
    return storage.lie
end

@testset "storage" begin
    @testset "bad_storage" begin
        bad_storage = BadStorage()
        @test_throws "missing method: name for storage type: BadStorage" name(bad_storage)
        @test_throws "missing method: has_axis for storage type: BadStorage" has_axis(bad_storage, "cell")

        bad_storage = LyingStorage(true)
        @test_throws "missing method: unsafe_delete_axis! for storage type: LyingStorage" delete_axis!(
            bad_storage,
            "cell",
        )
        @test_throws "missing method: unsafe_axis_length for storage type: LyingStorage" axis_length(
            bad_storage,
            "cell",
        )
        @test_throws "missing method: unsafe_axis_entries for storage type: LyingStorage" axis_entries(
            bad_storage,
            "cell",
        )

        bad_storage = LyingStorage(false)
        @test_throws "missing method: unsafe_add_axis! for storage type: LyingStorage" add_axis!(
            bad_storage,
            "cell",
            vec(["cell0"]),
        )
    end

    @testset "memory" begin
        return test_storage(MemoryStorage("memory"), "memory")
    end
end
