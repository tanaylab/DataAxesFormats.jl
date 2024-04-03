nested_test("groups") do
    nested_test("compact") do
        group_indices = [0, 3, 1, 3, 1]
        @test compact_groups!(group_indices) == 2
        @test group_indices == [0, 1, 2, 1, 2]
    end

    nested_test("collect") do
        @test collect_group_members([0, 1, 2, 1, 2]) == [[2, 4], [3, 5]]
    end

    nested_test("names") do
        daf = MemoryDaf(; name = "memory!")
        add_axis!(daf, "entry", ["A", "B", "C", "D", "E"])
        @test group_names(daf, "entry", [[2, 4], [3, 5]]; prefix = "G") == ["G1.61", "G2.95"]
        @test group_names(daf, "entry", [[2, 3], [4, 5]]; prefix = "G") == ["G1.43", "G2.04"]
    end
end
