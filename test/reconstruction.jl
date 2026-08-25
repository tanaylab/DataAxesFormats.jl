nested_test("reconstruction") do
    memory = MemoryDaf(; name = "memory!")

    add_axis!(memory, "cell", ["A", "B", "C", "D"])
    set_vector!(memory, "cell", "age", [1, 1, 2, 3])
    set_vector!(memory, "cell", "score", [0.0, 0.5, 1.0, 2.0])

    nested_test("connect") do
        # Plates and sequencing runs are both properties of a batch, and each plate belongs to one run, but nothing
        # says so where a plate can be asked about it. The last batch has no plate, which is not a problem: nothing is
        # moved, so it keeps its own run.
        add_axis!(memory, "batch", ["B1", "B2", "B3", "B4"])
        add_axis!(memory, "plate", ["P1", "P2", "P3"])
        add_axis!(memory, "run", ["R1", "R2"])
        set_vector!(memory, "batch", "plate", ["P1", "P1", "P2", ""])
        set_vector!(memory, "batch", "run", ["R1", "R1", "R2", "R2"])

        nested_test("()") do
            connect_axes!(memory; base_axis = "batch", from_axis = "plate", to_axis = "run")

            # P3 is named by no batch, so it is connected to nothing.
            @test get_vector(memory, "plate", "run").array == ["R1", "R2", ""]

            # Nothing was moved, so the batch with no plate still has its run.
            @test get_vector(memory, "batch", "run").array == ["R1", "R1", "R2", "R2"]
        end

        nested_test("properties") do
            # The properties holding the references need not be named after the axes they refer to, and a base axis
            # may refer to the same axis twice, in which case only the property names tell them apart.
            set_vector!(memory, "batch", "on_plate", ["P1", "P1", "P2", ""])
            set_vector!(memory, "batch", "sequenced_by", ["R1", "R1", "R2", "R2"])

            connect_axes!(
                memory;
                base_axis = "batch",
                from_axis = "plate",
                from_property = "on_plate",
                to_axis = "run",
                to_property = "sequenced_by",
                connect_property = "sequenced_by",
            )

            @test get_vector(memory, "plate", "sequenced_by").array == ["R1", "R2", ""]

            # The properties named after the axes are untouched, so the two can coexist.
            @test !has_vector(memory, "plate", "run")
        end

        nested_test("!agree") do
            set_vector!(memory, "batch", "run", ["R1", "R2", "R2", "R2"]; overwrite = true)
            @test_throws chomp("""
                         conflicting entries: "R1" != "R2"
                         of the axis: run
                         named by the property: run
                         of the axis: batch
                         for the entry: P1
                         of the axis: plate
                         named by the property: plate
                         in the daf data: memory!
                         """) connect_axes!(memory; base_axis = "batch", from_axis = "plate", to_axis = "run")
        end

        nested_test("!empty") do
            # An empty value is a value: batches of one plate disagreeing on whether they have a run at all is as much
            # a conflict as naming two different runs.
            set_vector!(memory, "batch", "run", ["R1", "", "R2", "R2"]; overwrite = true)
            @test_throws chomp("""
                         conflicting entries: "R1" != ""
                         of the axis: run
                         named by the property: run
                         of the axis: batch
                         for the entry: P1
                         of the axis: plate
                         named by the property: plate
                         in the daf data: memory!
                         """) connect_axes!(memory; base_axis = "batch", from_axis = "plate", to_axis = "run")
        end

        nested_test("!to_entry") do
            # On the third batch, which has a plate: a batch with no plate is skipped before its run is looked at.
            set_vector!(memory, "batch", "run", ["R1", "R1", "R9", "R2"]; overwrite = true)
            @test_throws chomp("""
                         missing entry: R9
                         of the axis: run
                         named by the property: run
                         of the axis: batch
                         in the daf data: memory!
                         """) connect_axes!(memory; base_axis = "batch", from_axis = "plate", to_axis = "run")
        end

        nested_test("!from_entry") do
            set_vector!(memory, "batch", "plate", ["P1", "P1", "P9", ""]; overwrite = true)
            @test_throws chomp("""
                         missing entry: P9
                         of the axis: plate
                         named by the property: plate
                         of the axis: batch
                         in the daf data: memory!
                         """) connect_axes!(memory; base_axis = "batch", from_axis = "plate", to_axis = "run")
        end
    end

    nested_test("default") do
        set_vector!(memory, "cell", "batch", ["X", "X", "Y", ""])
        results = reconstruct_axis!(memory; existing_axis = "cell", implicit_axis = "batch")
        @test keys(results) == Set(["age"])
        @test results["age"] == 3

        @test description(memory) == """
            name: memory!
            type: MemoryDaf
            axes:
              batch: 2 entries
              cell: 4 entries
            vectors:
              batch:
                age: 2 x Int64 (Dense)
              cell:
                batch: 4 x Str (Dense)
                score: 4 x Float64 (Dense)
            """
    end

    nested_test("empties") do
        set_vector!(memory, "cell", "age", [1, 1, 3, 3]; overwrite = true)
        set_vector!(memory, "cell", "batch", ["X", "X", "Outliers", "Doublet"])
        results = reconstruct_axis!(
            memory;
            existing_axis = "cell",
            implicit_axis = "batch",
            empty_implicit = ("Outliers", "Doublet"),
        )
        @test keys(results) == Set(["age"])
        @test results["age"] == 3
        @test get_vector(memory, "cell", "batch").array == ["X", "X", "", ""]

        @test description(memory) == """
            name: memory!
            type: MemoryDaf
            axes:
              batch: 1 entries
              cell: 4 entries
            vectors:
              batch:
                age: 1 x Int64 (Dense)
              cell:
                batch: 4 x Str (Dense)
                score: 4 x Float64 (Dense)
            """
    end

    nested_test("inconsistent") do
        set_vector!(memory, "cell", "batch", ["X", "X", "Y", ""])
        @test_throws chomp("""
                     inconsistent values: 0.5 != 0.0
                     of the property: score
                     for the same implicit axis value: X
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

        @test description(memory) == """
            name: memory!
            type: MemoryDaf
            axes:
              batch: 2 entries
              cell: 4 entries
            vectors:
              batch:
                age: 2 x Int64 (Dense)
              cell:
                batch: 4 x Str (Dense)
                score: 4 x Float64 (Dense)
            """
    end

    nested_test("manual") do
        set_vector!(memory, "cell", "batch", ["X", "X", "Y", ""])

        nested_test("!entry") do
            add_axis!(memory, "batch", ["X", "Z"])
            @test_throws chomp("""
                         missing used entry: Y
                         from the existing reconstructed axis: batch
                         in the daf data: memory!
                         """) reconstruct_axis!(memory; existing_axis = "cell", implicit_axis = "batch")
        end

        nested_test("!default") do
            add_axis!(memory, "batch", ["X", "Y", "Z"])
            @test_throws chomp("""
                         no default value specified for the unused entry: Z
                         of the reconstructed property: age
                         in the daf data: memory!
                         """) reconstruct_axis!(memory; existing_axis = "cell", implicit_axis = "batch")
        end

        nested_test("default") do
            add_axis!(memory, "batch", ["X", "Y", "Z"])
            results = reconstruct_axis!(
                memory;
                existing_axis = "cell",
                implicit_axis = "batch",
                properties_defaults = (; age = 4),
            )
            @test keys(results) == Set(["age"])
            @test results["age"] == 3

            @test description(memory) == """
                name: memory!
                type: MemoryDaf
                axes:
                  batch: 3 entries
                  cell: 4 entries
                vectors:
                  batch:
                    age: 3 x Int64 (Dense)
                  cell:
                    batch: 4 x Str (Dense)
                    score: 4 x Float64 (Dense)
                """
        end
    end
end
