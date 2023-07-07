test_set("matrix layouts") do
    test_set("axis_name") do
        @test axis_name(Rows) == "Rows"
        @test axis_name(Columns) == "Columns"
        @test axis_name(nothing) == "nothing"
        @test_throws "invalid matrix axis: -1" axis_name(-1)
    end

    test_set("other_axis") do
        @test other_axis(Rows) == Columns
        @test other_axis(Columns) == Rows
        @test other_axis(nothing) == nothing
        @test_throws "invalid matrix axis: -1" other_axis(-1)
    end

    test_set("major_axis") do
        @test major_axis(rand(4, 4)) == Columns
        @test major_axis(transpose(rand(4, 4))) == Rows
        @test major_axis(sprand(4, 4, 0.5)) == Columns
        @test major_axis(transpose(sprand(4, 4, 0.5))) == Rows
    end

    test_set("minor_axis") do
        @test minor_axis(rand(4, 4)) == Rows
        @test minor_axis(transpose(rand(4, 4))) == Columns
        @test minor_axis(sprand(4, 4, 0.5)) == Rows
        @test minor_axis(transpose(sprand(4, 4, 0.5))) == Columns
    end

    test_set("inefficient_action_policy") do
        @test inefficient_action_policy(nothing) == ErrorPolicy

        matrix = rand(4, 4)

        check_efficient_action("test", Rows, "input", matrix)
        check_efficient_action("test", Columns, "input", matrix)

        @test inefficient_action_policy(WarnPolicy) == nothing

        check_efficient_action("test", Columns, "input", matrix)
        @test_logs (:warn, dedent("""
                               the major axis: Rows
                               of the action: test
                               is different from the major axis: Columns
                               of the input matrix: Matrix{Float64}
                           """)) check_efficient_action("test", Rows, "input", matrix)

        @test inefficient_action_policy(ErrorPolicy) == WarnPolicy

        check_efficient_action("test", Columns, "input", matrix)
        @test_throws dedent("""
            the major axis: Rows
            of the action: test
            is different from the major axis: Columns
            of the input matrix: Matrix{Float64}
        """) check_efficient_action("test", Rows, "input", matrix)
    end

    test_set("relayout!") do
        sparse = sprand(4, 6, 0.5)
        dense = Matrix(sparse)

        @test sparse == dense
        @test major_axis(sparse) == Columns
        @test major_axis(dense) == Columns

        transposed_sparse = transpose(sparse)
        transposed_dense = transpose(dense)
        @test transposed_sparse == transposed_dense

        @test major_axis(transposed_sparse) == Rows
        @test major_axis(transposed_dense) == Rows

        relayout_sparse = relayout!(sparse)
        relayout_dense = relayout!(dense)
        similar_dense = similar(transposed_dense)
        @test relayout!(similar_dense, dense) === similar_dense

        @test major_axis(relayout_sparse) == Columns
        @test major_axis(relayout_dense) == Columns
        @test major_axis(similar_dense) == Columns

        @test relayout_sparse == transposed_sparse
        @test relayout_dense == transposed_dense
        @test similar_dense == transposed_dense
    end
end
