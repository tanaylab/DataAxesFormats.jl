nested_test("views") do
    daf = MemoryDaf("memory!")

    nested_test("scalar") do
        set_scalar!(daf, "version", "1.0")
        add_axis!(daf, "cell", ["X", "Y"])
        set_vector!(daf, "cell", "age", [1, 2])
        nested_test("copy") do
            view = viewer("view!", read_only(daf); scalars = ["*" => "="])
            @test present(view) == "View MemoryDaf memory!"
            @test scalar_names(view) == Set(["version"])
            @test get_scalar(view, "version") == "1.0"
        end

        nested_test("reduction") do
            view = viewer("view!", daf; scalars = ["sum_ages" => "cell @ age %> Sum"])
            @test scalar_names(view) == Set(["sum_ages"])
            @test get_scalar(view, "sum_ages") == 3
        end

        nested_test("hidden") do
            view = viewer("view!", daf; scalars = ["*" => "=", "version" => nothing])
            @test isempty(scalar_names(view))
        end
    end

    nested_test("axis") do
        add_axis!(daf, "cell", ["X", "Y"])
        add_axis!(daf, "gene", ["A", "B", "C"])
        set_vector!(daf, "gene", "marker", [true, false, true])
        set_vector!(daf, "gene", "noisy", [true, true, false])

        nested_test("copy") do
            view = viewer("view!", daf; axes = ["*" => "="])
            @test axis_names(view) == Set(["cell", "gene"])
        end

        nested_test("hidden") do
            view = viewer("view!", daf; axes = ["*" => "=", "cell" => nothing])
            @test axis_names(view) == Set(["gene"])
        end

        nested_test("masked") do
            view = viewer("view!", daf; axes = ["gene" => "gene & marker"])
            @test get_axis(view, "gene") == ["A", "C"]
        end
    end

    nested_test("vector") do
        add_axis!(daf, "cell", ["X", "Y", "Z"])
        set_vector!(daf, "cell", "age", [1, 2, 3])
        set_vector!(daf, "cell", "batch", [1, 2, 2])

        nested_test("copy") do
            view = viewer("view!", daf; axes = ["*" => "="], vectors = [("*", "*") => "="])
            @test vector_names(view, "cell") == Set(["age", "batch"])
            @test get_vector(view, "cell", "age") == [1, 2, 3]
            @test get_vector(view, "cell", "batch") == [1, 2, 2]
        end

        nested_test("missing") do
            @test_throws dedent("""
                missing axis: cell
                for the view: view!
                of the daf data: memory!
            """) viewer("view!", daf; vectors = [("cell", "age") => "="])
        end

        nested_test("hidden") do
            view = viewer("view!", daf; axes = ["*" => "="], vectors = [("*", "*") => "=", ("cell", "age") => nothing])
            @test vector_names(view, "cell") == Set(["batch"])
            @test get_vector(view, "cell", "batch") == [1, 2, 2]
        end

        nested_test("renamed") do
            view = viewer("view!", daf; axes = ["*" => "="], vectors = [("cell", "day") => "age"])
            @test vector_names(view, "cell") == Set(["day"])
            @test get_vector(view, "cell", "day") == [1, 2, 3]
        end

        nested_test("masked") do
            view = viewer("view!", daf; axes = ["cell" => "cell & batch = 2"], vectors = [("cell", "age") => "="])
            @test vector_names(view, "cell") == Set(["age"])
            @test get_vector(view, "cell", "age") == [2, 3]
        end

        nested_test("empty") do
            view = viewer("view!", daf; axes = ["cell" => "cell & batch < 0"], vectors = [("cell", "age") => "="])
            @test vector_names(view, "cell") == Set(["age"])
            @test_throws dedent("""
                empty result for query: cell & batch < 0 @ name
                for the axis: cell
                for the view: view!
                of the daf data: memory!
            """) get_axis(view, "cell")
            @test_throws dedent("""
                empty result for query: cell & batch < 0 @ age
                for the vector: age
                for the axis: cell
                for the view: view!
                of the daf data: memory!
            """) get_vector(view, "cell", "age")
        end

        nested_test("reduced") do
            add_axis!(daf, "gene", ["A", "B"])
            set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3; 4 5])
            view = viewer("view!", daf; axes = ["*" => "="], vectors = [("cell", "total_umis") => "gene @ UMIs %> Sum"])
            @test get_vector(view, "cell", "total_umis") == [1, 5, 9]
        end
    end

    nested_test("matrix") do
        add_axis!(daf, "cell", ["X", "Y"])
        add_axis!(daf, "gene", ["A", "B", "C"])
        set_vector!(daf, "gene", "marker", [true, false, true])
        set_matrix!(daf, "cell", "gene", "UMIs", [0 1 2; 3 4 5])

        nested_test("copy") do
            view = viewer("view!", daf; axes = ["*" => "="], matrices = [("*", "*", "*") => "="])
            @test matrix_names(view, "gene", "cell"; relayout = false) == Set(["UMIs"])
            @test matrix_names(view, "cell", "gene"; relayout = false) == Set(["UMIs"])
            @test get_matrix(view, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
            @test get_matrix(view, "gene", "cell", "UMIs") == [0 3; 1 4; 2 5]
        end

        nested_test("hidden") do
            view = viewer(
                "view!",
                daf;
                axes = ["*" => "="],
                matrices = [("*", "*", "*") => "=", ("cell", "gene", "UMIs") => nothing],
            )
            @test isempty(matrix_names(view, "gene", "cell"))
        end

        nested_test("masked") do
            view = viewer(
                "view!",
                daf;
                axes = ["cell" => "=", "gene" => "gene & marker"],
                matrices = [("*", "*", "*") => "="],
            )
            @test matrix_names(view, "gene", "cell"; relayout = false) == Set(["UMIs"])
            @test matrix_names(view, "cell", "gene"; relayout = false) == Set(["UMIs"])
            @test get_matrix(view, "cell", "gene", "UMIs") == [0 2; 3 5]
            @test get_matrix(view, "gene", "cell", "UMIs") == [0 3; 2 5]
        end

        nested_test("empty") do
            view = viewer(
                "view!",
                daf;
                axes = ["cell" => "=", "gene" => "gene & marker & ~marker"],
                matrices = [("*", "*", "*") => "="],
            )
            @test_throws dedent("""
                empty result for query: cell, gene & marker & ~marker @ UMIs
                for the matrix: UMIs
                for the rows_axis: cell
                and the columns: gene
                for the view: view!
                of the daf data: memory!
            """) get_matrix(view, "cell", "gene", "UMIs")
        end
    end
end
