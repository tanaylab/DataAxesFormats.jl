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
            @test_throws dedent("""
                ungrouped entry: D
                with the index: 4
                of the axis: cell
                has empty group: type
                in the daf data: memory!
            """) get_group_vector(daf, axis = "cell", group = "type", name = "color")

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
end
