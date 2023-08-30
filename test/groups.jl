nested_test("groups") do
    daf = MemoryDaf("memory!")

    @test add_axis!(daf, "cell", ["A", "B", "C", "D"]) == nothing
    @test add_axis!(daf, "type", ["X", "Y"]) == nothing

    nested_test("get_chained_vector") do
        @test set_vector!(daf, "type", "color", ["red", "green"]) == nothing

        nested_test("empty") do
            @test_throws "names for get_chained_vector" get_chained_vector(daf, "cell", String[])
        end

        nested_test("complete") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Y"]) == nothing
            @test get_chained_vector(daf, "cell", ["type", "color"]) == ["red", "green", "red", "green"]
            @test names(get_chained_vector(daf, "cell", ["type", "color"]), 1) == ["A", "B", "C", "D"]
        end

        nested_test("!string") do
            @test set_vector!(daf, "cell", "age", [1, 1, 2, 2]) == nothing
            @test_throws dedent("""
                non-String data type: Int64
                for the chained: age
                for the axis: cell
                in the daf data: memory!
            """) get_chained_vector(daf, "cell", ["age", "color"])
        end

        nested_test("invalid") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Z"]) == nothing
            @test_throws dedent("""
                invalid value: Z
                of the chained: type
                entry index: 4
                of the axis: cell
                is missing from the next axis: type
                in the daf data: memory!
            """) get_chained_vector(daf, "cell", ["type", "color"])
        end

        nested_test("partial") do
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", ""]) == nothing

            nested_test("()") do
                @test_throws dedent("""
                    empty value
                    of the chained: type
                    entry index: 4
                    of the axis: cell
                    in the daf data: memory!
                """) get_chained_vector(daf, "cell", ["type", "color"])
            end

            nested_test("default") do
                nested_test("undef") do
                    @test_throws dedent("""
                        empty value
                        of the chained: type
                        entry index: 4
                        of the axis: cell
                        in the daf data: memory!
                    """) get_chained_vector(daf, "cell", ["type", "color"]; default = undef)
                end

                nested_test("value") do
                    @test get_chained_vector(daf, "cell", ["type", "color"]; default = "blue") ==
                          ["red", "green", "red", "blue"]
                end
            end
        end
    end

    nested_test("aggregate_group_vector") do
        @test set_vector!(daf, "cell", "age", [1, 2, 3, 4]) == nothing

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

    nested_test("count_groups_matrix") do
        nested_test("numbers") do
            @test set_vector!(daf, "cell", "age", [1, 1, 2, 1]) == nothing
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Y"]) == nothing
            @test count_groups_matrix(daf; axis = "cell", rows_name = "type", columns_name = "age") == [1 1; 2 0]
            @test names(count_groups_matrix(daf; axis = "cell", rows_name = "type", columns_name = "age"), 1) ==
                  ["X", "Y"]
            @test names(count_groups_matrix(daf; axis = "cell", rows_name = "type", columns_name = "age"), 2) ==
                  ["1", "2"]
        end

        nested_test("strings") do
            @test set_vector!(daf, "cell", "age", ["Young", "Young", "", "Old"]) == nothing
            @test set_vector!(daf, "cell", "type", ["X", "Y", "X", "Y"]) == nothing
            @test count_groups_matrix(daf; axis = "cell", rows_name = "type", columns_name = "age") == [0 1; 1 1]
            @test names(count_groups_matrix(daf; axis = "cell", rows_name = "type", columns_name = "age"), 1) ==
                  ["X", "Y"]
            @test names(count_groups_matrix(daf; axis = "cell", rows_name = "type", columns_name = "age"), 2) ==
                  ["Old", "Young"]
        end
    end
end
