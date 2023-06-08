test_set("data_types") do
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

    test_set("is_storage_matrix") do
        @test !is_storage_matrix(1)
        @test is_storage_matrix(rand(4, 4))
        @test is_storage_matrix(sprand(4, 4, 0.5))
        @test !is_storage_matrix(transpose(rand(4, 4)))
        @test !is_storage_matrix(transpose(sprand(4, 4, 0.5)))
    end

    test_set("require_storage_matrix") do
        @test_throws "type: Int64 is not a valid Daf.StorageMatrix" require_storage_matrix(1)
        require_storage_matrix(rand(4, 4))
        require_storage_matrix(sprand(4, 4, 0.5))
        @test_throws "type: Transpose{Float64, Matrix{Float64}} is not a valid Daf.StorageMatrix" require_storage_matrix(
            transpose(rand(4, 4)),
        )
        @test_throws "type: Transpose{Float64, SparseMatrixCSC{Float64, Int64}} is not a valid Daf.StorageMatrix" require_storage_matrix(
            transpose(sprand(4, 4, 0.5)),
        )
    end

    test_set("require_storage_vector") do
        require_storage_vector(as_dense_or_fail(rand(4, 4)[:, 1]))
        require_storage_vector(as_dense_or_fail(rand(4, 4)[1, :]))
        require_storage_vector(sprand(4, 4, 0.5)[:, 1])
        require_storage_vector(sprand(4, 4, 0.5)[1, :])
        require_storage_vector(as_dense_or_fail(selectdim(rand(4, 4), Columns, 2)))
        @test_throws "SubArray{Float64, 1, Matrix{Float64}, Tuple{Int64, Base.Slice{Base.OneTo{Int64}}}, true} " *
                     "is not a valid Daf.StorageVector" require_storage_vector(
            as_dense_if_possible(selectdim(rand(4, 4), Rows, 2)),
        )
        @test_throws "type: Int64 is not a valid Daf.StorageVector" require_storage_vector(1)
    end

    test_set("inefficient_action_policy") do
        inefficient_action_policy(nothing)

        verify_efficient_action("test", rand(4, 4), Rows)
        verify_efficient_action("test", rand(4, 4), Columns)

        inefficient_action_policy(WarnPolicy)

        verify_efficient_action("test", rand(4, 4), Columns)
        @test_logs (
            :warn,
            "action: test major axis: Rows is different from the matrix: Matrix{Float64} major axis: Columns",
        ) verify_efficient_action("test", rand(4, 4), Rows)

        inefficient_action_policy(ErrorPolicy)

        verify_efficient_action("test", rand(4, 4), Columns)
        @test_throws "action: test major axis: Rows " *
                     "is different from the matrix: Matrix{Float64} " *
                     "major axis: Columns" verify_efficient_action("test", rand(4, 4), Rows)
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
