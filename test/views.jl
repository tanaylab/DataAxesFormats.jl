nested_test("views") do
    daf = MemoryDaf(; name = "memory!")

    nested_test("none") do
        view = viewer(daf; name = "read_only")
        @test view isa DafReadOnly
        @test view.daf === daf
        @test viewer(view) === view
        renamed = viewer(view; name = "renamed")
        @test renamed isa DafReadOnly
        @test renamed.daf === daf
    end

    nested_test("scalar") do
        set_scalar!(daf, "version", "1.0")
        add_axis!(daf, "cell", ["X", "Y"])
        set_vector!(daf, "cell", "age", [1, 2])

        nested_test("copy") do
            view = viewer(read_only(daf); data = [VIEW_ALL_SCALARS])
            @test read_only(view) === view
            @test depict(view) == "View MemoryDaf memory!.view"
            @test scalars_set(view) == Set(["version"])
            @test get_scalar(view, "version") == "1.0"
            renamed = read_only(view; name = "renamed")
            @test renamed !== view
        end

        nested_test("reduction") do
            view = viewer(daf; data = ["sum_ages" => "/ cell : age %> Sum"])
            @test scalars_set(view) == Set(["sum_ages"])
            @test get_scalar(view, "sum_ages") == 3
        end

        nested_test("vector") do
            @test_throws dedent("""
                vector query: / cell : age
                for the scalar: sum_ages
                for the view: view!
                of the daf data: memory!
            """) viewer(daf; name = "view!", data = ["sum_ages" => "/ cell : age"])
        end

        nested_test("hidden") do
            view = viewer(daf; data = [VIEW_ALL_SCALARS, "version" => nothing])
            @test isempty(scalars_set(view))
        end
    end

    nested_test("axis") do
        add_axis!(daf, "cell", ["X", "Y"])
        add_axis!(daf, "gene", ["A", "B", "C"])
        set_vector!(daf, "gene", "marker", [true, false, true])
        set_vector!(daf, "gene", "noisy", [true, true, false])

        nested_test("copy") do
            view = viewer(daf; axes = [VIEW_ALL_AXES])
            @test axes_set(view) == Set(["cell", "gene"])
        end

        nested_test("hidden") do
            view = viewer(daf; axes = [VIEW_ALL_AXES, "cell" => nothing])
            @test axes_set(view) == Set(["gene"])
        end

        nested_test("masked") do
            view = viewer(daf; axes = ["gene" => "/ gene & marker"])
            @test axis_array(view, "gene") == ["A", "C"]
        end

        nested_test("scalar") do
            @test_throws dedent("""
                not an axis query: / gene : marker %> Sum
                for the axis: gene
                for the view: view!
                of the daf data: memory!
            """) viewer(daf; name = "view!", axes = ["gene" => "/ gene : marker %> Sum"])
        end

        nested_test("vector") do
            set_vector!(daf, "cell", "age", [1, 2])
            @test_throws dedent("""
                not an axis query: / cell : age
                for the axis: cell
                for the view: view!
                of the daf data: memory!
            """) viewer(daf; name = "view!", axes = ["cell" => "/ cell : age"])
        end
    end

    nested_test("vector") do
        add_axis!(daf, "cell", ["X", "Y", "Z"])
        set_vector!(daf, "cell", "age", [1, 2, 3])
        set_vector!(daf, "cell", "batch", [1, 2, 2])

        nested_test("copy") do
            view = viewer(daf; name = "view!", axes = [VIEW_ALL_AXES], data = [VIEW_ALL_VECTORS])
            @test vectors_set(view, "cell") == Set(["age", "batch"])
            @test get_vector(view, "cell", "age") == [1, 2, 3]
            @test get_vector(view, "cell", "batch") == [1, 2, 2]
        end

        nested_test("missing") do
            @test_throws dedent("""
                the axis: cell
                is not exposed by the view: view!
                of the daf data: memory!
            """) viewer(daf; name = "view!", axes = ["cell" => nothing], data = [("cell", "age") => "="])
        end

        nested_test("hidden") do
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [VIEW_ALL_VECTORS, ("cell", "age") => nothing])
            @test vectors_set(view, "cell") == Set(["batch"])
            @test get_vector(view, "cell", "batch") == [1, 2, 2]
        end

        nested_test("renamed") do
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [("cell", "day") => ": age"])
            @test vectors_set(view, "cell") == Set(["day"])
            @test get_vector(view, "cell", "day") == [1, 2, 3]
        end

        nested_test("masked") do
            view = viewer(daf; axes = ["cell" => "/ cell & batch = 2"], data = [("cell", "age") => "="])
            @test vectors_set(view, "cell") == Set(["age"])
            @test get_vector(view, "cell", "age") == [2, 3]
        end

        nested_test("reduced") do
            add_axis!(daf, "gene", ["A", "B"])
            set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3; 4 5])
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [("cell", "total_umis") => "/ gene : UMIs %> Sum"])
            @test get_vector(view, "cell", "total_umis") == [1, 5, 9]
        end

        nested_test("matrix") do
            add_axis!(daf, "gene", ["A", "B"])
            set_matrix!(daf, "cell", "gene", "UMIs", [0 1; 2 3; 4 5])
            @test_throws dedent("""
                matrix query: / gene / cell : UMIs
                for the vector: total_umis
                for the axis: cell
                for the view: view!
                of the daf data: memory!
            """) viewer(daf; name = "view!", axes = [VIEW_ALL_AXES], data = [("cell", "total_umis") => "/ gene : UMIs"])
        end
    end

    nested_test("matrix") do
        add_axis!(daf, "cell", ["X", "Y"])
        add_axis!(daf, "gene", ["A", "B", "C"])
        set_vector!(daf, "gene", "marker", [true, false, true])
        set_matrix!(daf, "cell", "gene", "UMIs", [0 1 2; 3 4 5])

        nested_test("copy") do
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [VIEW_ALL_MATRICES])
            @test matrices_set(view, "gene", "cell"; relayout = false) == Set(["UMIs"])
            @test matrices_set(view, "cell", "gene"; relayout = false) == Set(["UMIs"])
            @test get_matrix(view, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
            @test get_matrix(view, "gene", "cell", "UMIs") == [0 3; 1 4; 2 5]
        end

        nested_test("query") do
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [("cell", "gene", "UMIs") => ": UMIs % Abs"])
            @test matrices_set(view, "gene", "cell"; relayout = true) == Set(["UMIs"])
            @test matrices_set(view, "cell", "gene"; relayout = false) == Set(["UMIs"])
            @test get_matrix(view, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
            @test get_matrix(view, "gene", "cell", "UMIs") == [0 3; 1 4; 2 5]
        end

        nested_test("hidden") do
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [VIEW_ALL_MATRICES, ("cell", "gene", "UMIs") => nothing])
            @test isempty(matrices_set(view, "gene", "cell"))
        end

        nested_test("masked") do
            view = viewer(daf; axes = ["cell" => "=", "gene" => "/ gene & marker"], data = [VIEW_ALL_MATRICES])
            @test matrices_set(view, "gene", "cell"; relayout = false) == Set(["UMIs"])
            @test matrices_set(view, "cell", "gene"; relayout = false) == Set(["UMIs"])
            @test get_matrix(view, "cell", "gene", "UMIs") == [0 2; 3 5]
            @test get_matrix(view, "gene", "cell", "UMIs") == [0 3; 2 5]
        end

        nested_test("vector") do
            @test_throws dedent("""
                vector query: / cell / gene : UMIS %> Sum
                for the matrix: UMIs
                for the rows axis: cell
                and the columns axis: gene
                for the view: view!
                of the daf data: memory!
            """) viewer(
                daf;
                name = "view!",
                axes = [VIEW_ALL_AXES],
                data = [("cell", "gene", "UMIs") => ": UMIS %> Sum"],
            )
        end
    end

    nested_test("requires_relayout") do
        add_axis!(daf, "cell", ["A", "B"])
        add_axis!(daf, "gene", ["X", "Y", "Z"])
        set_vector!(daf, "cell", "batch", ["U", "V"])
        set_vector!(daf, "cell", "age", [-1.0, 2.0])
        set_matrix!(daf, "gene", "cell", "UMIs", [1 2; 3 4; 5 6]; relayout = false)
        add_axis!(daf, "batch", ["U", "V", "W"])
        set_vector!(daf, "batch", "sex", ["Male", "Female", "Male"])

        view = viewer(
            daf;
            name = "view!",
            axes = ["obs" => "/ cell", "var" => "/ gene"],
            data = [ALL_SCALARS => nothing, ALL_VECTORS => "=", ("obs", "var", "X") => ": UMIs"],
        )

        @test description(view) == dedent("""
            name: view!
            type: View MemoryDaf
            axes:
              obs: 2 entries
              var: 3 entries
            vectors:
              obs:
                age: 2 x Float64 (Dense)
                batch: 2 x String (Dense)
            matrices:
              var,obs:
                X: 3 x 2 x Int64 in Columns (Dense)
        """) * "\n"
    end
end
