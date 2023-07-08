test_set("frozen_arrays") do
    test_set("vector") do
        mutable = rand(6)
        immutable = frozen(mutable)
        @test frozen(immutable) === immutable
        mutable[1] = -1
        @test immutable[1] == -1
        @test_throws CanonicalIndexError immutable[1] = -2
        @test_throws CanonicalIndexError immutable .= mutable
    end
    test_set("dense") do
        mutable = rand(4, 6)
        immutable = frozen(mutable)
        @test frozen(immutable) === immutable
        mutable[1, 1] = -1
        @test immutable[1, 1] == -1
        @test_throws CanonicalIndexError immutable[1, 1] = -2
        @test_throws CanonicalIndexError immutable .= mutable
    end
    test_set("sparse") do
        mutable = sprand(4, 6, 0.5)
        immutable = frozen(mutable)
        @test frozen(immutable) === immutable
        mutable[1, 1] = -1
        @test immutable[1, 1] == -1
        @test_throws CanonicalIndexError immutable[1, 1] = -2
        @test_throws CanonicalIndexError immutable .= mutable
    end
end
