nested_test("groups") do
    nested_test("get_group_vector") do
        daf = MemoryDaf("memory!")
        @test add_axis!(daf, "cell", ["A", "B", "C", "D"]) == nothing
        @test add_axis!(daf, "type", ["X", "Y"]) == nothing
        @test set_vector!(daf, "type", "color", ["red", "green"]) == nothing

        nested_test("complete") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Y"]) == nothing
            @test get_group_vector(daf; axis = "cell", group = "type", name = "color") ==
                  ["red", "green", "red", "green"]
            @test names(get_group_vector(daf; axis = "cell", group = "type", name = "color"), 1) == ["A", "B", "C", "D"]
        end

        nested_test("!string") do
            @test set_vector!(daf, "cell", "age", [1, 1, 2, 2]) == nothing
            @test_throws dedent("""
                non-String data type: Int64
                for the group: age
                for the axis: cell
                in the daf data: memory!
            """) get_group_vector(daf, axis = "cell", group = "age", name = "color")
        end

        nested_test("invalid") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Z"]) == nothing
            @test_throws dedent("""
                invalid value: Z
                of the group: type
                of the entry: D
                with the index: 4
                of the axis: cell
                is missing from the group axis: type
                in the daf data: memory!
            """) get_group_vector(daf, axis = "cell", group = "type", name = "color")
        end

        nested_test("partial") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", ""]) == nothing

            nested_test("()") do
                @test_throws dedent("""
                    ungrouped entry: D
                    with the index: 4
                    of the axis: cell
                    has empty group: type
                    in the daf data: memory!
                """) get_group_vector(daf, axis = "cell", group = "type", name = "color")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        ungrouped entry: D
                        with the index: 4
                        of the axis: cell
                        has empty group: type
                        in the daf data: memory!
                    """) get_group_vector(daf, axis = "cell", group = "type", name = "color", default = undef)
                end

                nested_test("value") do
                    @test get_group_vector(daf; axis = "cell", group = "type", name = "color", default = "blue") ==
                          ["red", "green", "red", "blue"]
                end
            end
        end
    end

    nested_test("aggregate_group_vector") do
        daf = MemoryDaf("memory!")
        @test add_axis!(daf, "cell", ["A", "B", "C", "D"]) == nothing
        @test set_vector!(daf, "cell", "age", [1, 2, 3, 4]) == nothing
        @test add_axis!(daf, "type", ["X", "Y"]) == nothing

        nested_test("complete") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Y"]) == nothing
            @test aggregate_group_vector(mean, daf; axis = "cell", name = "age", group = "type") == [2.0, 3.0]
            @test names(aggregate_group_vector(mean, daf; axis = "cell", name = "age", group = "type"), 1) == ["X", "Y"]
        end

        nested_test("!string") do
            @test_throws dedent("""
                non-String data type: Int64
                for the group: age
                for the axis: cell
                in the daf data: memory!
            """) aggregate_group_vector(mean, daf, axis = "cell", name = "age", group = "age")
        end

        nested_test("partial") do
            @test set_vector!(daf, "cell", "type", ["X", "X", "X", ""]) == nothing

            nested_test("()") do
                @test_throws dedent("""
                    empty group: Y
                    with the index: 2
                    in the group: type
                    for the axis: cell
                    in the daf data: memory!
                """) aggregate_group_vector(mean, daf; axis = "cell", name = "age", group = "type")
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        empty group: Y
                        with the index: 2
                        in the group: type
                        for the axis: cell
                        in the daf data: memory!
                    """) aggregate_group_vector(mean, daf; axis = "cell", name = "age", group = "type")
                end

                nested_test("value") do
                    @test aggregate_group_vector(
                        mean,
                        daf;
                        axis = "cell",
                        name = "age",
                        group = "type",
                        default = 0.0,
                    ) == [2.0, 0.0]
                end
            end
        end
    end
end
