nested_test("reconstruction") do
    memory = MemoryDaf(; name = "memory!")

    add_axis!(memory, "cell", ["A", "B", "C", "D"])
    set_vector!(memory, "cell", "age", [1, 1, 2, 3])
    set_vector!(memory, "cell", "score", [0.0, 0.5, 1.0, 2.0])

    nested_test("default") do
        set_vector!(memory, "cell", "batch", ["X", "X", "Y", ""])
        results = reconstruct_axis!(memory; existing_axis = "cell", implicit_axis = "batch")
        @test keys(results) == Set(["age"])
        @test results["age"] == 3

        @test description(memory) == dedent("""
            name: memory!
            type: MemoryDaf
            axes:
              batch: 2 entries
              cell: 4 entries
            vectors:
              batch:
                age: 2 x Int64 (Dense)
              cell:
                batch: 4 x String (Dense)
                score: 4 x Float64 (Dense)
        """) * "\n"
    end

    nested_test("inconsistent") do
        set_vector!(memory, "cell", "batch", ["X", "X", "Y", ""])
        @test_throws dedent("""
            inconsistent values of the property: score
            of the axis: cell
            for the reconstructed axis: batch
            in the daf data: memory!
        """) reconstruct_axis!(
            memory,
            existing_axis = "cell",
            implicit_axis = "batch",
            implicit_properties = Set(["age", "score"]),
        )
    end

    nested_test("integer") do
        set_vector!(memory, "cell", "batch", [1, 1, 2, 0])
        results = reconstruct_axis!(memory; existing_axis = "cell", implicit_axis = "batch", empty_implicit = 0)
        @test keys(results) == Set(["age"])
        @test results["age"] == 3

        @test description(memory) == dedent("""
            name: memory!
            type: MemoryDaf
            axes:
              batch: 2 entries
              cell: 4 entries
            vectors:
              batch:
                age: 2 x Int64 (Dense)
              cell:
                batch: 4 x String (Dense)
                score: 4 x Float64 (Dense)
        """) * "\n"
    end
end
