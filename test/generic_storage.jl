mutable struct SomeStorage
    is_used::Bool
end

function create()::SomeStorage
    return SomeStorage(false)
end

function reset(some_storage::SomeStorage)::Nothing
    some_storage.is_used = false
    return nothing
end

nested_test("generic_storage") do
    storage = ReusableStorage(create, reset)
    with_reusable(storage) do one
        last = nothing

        @test 2 == with_reusable(storage) do two
            @test !two.is_used
            two.is_used = true
            last = two
            return 2
        end

        @test 3 == with_reusable(storage) do three
            @test !three.is_used
            @test three == last
            return 3
        end
    end
end
