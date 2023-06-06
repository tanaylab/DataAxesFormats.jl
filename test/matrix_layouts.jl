using Daf

function test_matrix_layout()
    @test major_axis(tc.matrix) == tc.major_axis
    @test minor_axis(tc.matrix) == tc.minor_axis

    rows_count = length(tc.rows)
    columns_count = length(tc.columns)

    @test naxis(tc.matrix, Row) == rows_count
    @test nrows(tc.matrix) == rows_count
    @test naxis(tc.matrix, Column) == columns_count
    @test ncolumns(tc.matrix) == columns_count

    for row_index in 1:rows_count
        @test view_axis(tc.matrix, Row, row_index) == tc.rows[row_index]
        @test view_row(tc.matrix, row_index) == tc.rows[row_index]

        @test count_nnz(tc.matrix[row_index, :]) == tc.rows_nnz[row_index]
        @test count_nnz(tc.matrix[row_index, :]; structural = false) == tc.rows_nnz[row_index]

        if tc.major_axis == Row
            @test count_nnz(tc.matrix; per = Row) == tc.rows_nnz
            @test count_nnz(tc.matrix; per = Row, structural = false) == tc.rows_nnz
        else
            inefficient_policy(nothing)
            @test count_nnz(tc.matrix; per = Row) == tc.rows_nnz
            @test count_nnz(tc.matrix; per = Row, structural = true) == tc.rows_nnz

            inefficient_policy(Error)
            @test_throws ErrorException count_nnz(tc.matrix, per = Row)
            @test_throws ErrorException count_nnz(tc.matrix, per = Row, structural = true)
        end
    end

    for column_index in 1:columns_count
        @test view_axis(tc.matrix, Column, column_index) == tc.columns[column_index]
        @test view_column(tc.matrix, column_index) == tc.columns[column_index]

        @test count_nnz(tc.matrix[:, column_index]) == tc.columns_nnz[column_index]
        @test count_nnz(tc.matrix[:, column_index]; structural = false) == tc.columns_nnz[column_index]

        if tc.major_axis == Column
            @test count_nnz(tc.matrix; per = Column) == tc.columns_nnz
            @test count_nnz(tc.matrix; per = Column, structural = false) == tc.columns_nnz
        else
            inefficient_policy(nothing)
            @test count_nnz(tc.matrix[:, column_index]) == tc.columns_nnz[column_index]
            @test count_nnz(tc.matrix[:, column_index]; structural = false) == tc.columns_nnz[column_index]

            inefficient_policy(Error)
            @test_throws ErrorException count_nnz(tc.matrix, per = Column)
            @test_throws ErrorException count_nnz(tc.matrix, per = Column, structural = true)
        end
    end

    relayout_matrix = relayout(tc.matrix, tc.major_axis)
    @test relayout_matrix === tc.matrix

    relayout_matrix = relayout(tc.matrix, tc.major_axis; copy = true)
    @test relayout_matrix !== tc.matrix
    @test typeof(relayout_matrix) == typeof(tc.matrix)
    @test relayout_matrix == tc.matrix
end

function test_base_matrix()
    test_set(
        "base",
        :rows => PrivateValue(() -> tc.base_rows),
        :rows_nnz => PrivateValue(() -> tc.base_rows_nnz),
        :columns => PrivateValue(() -> tc.base_columns),
        :columns_nnz => PrivateValue(() -> tc.base_columns_nnz),
    ) do
        test_case(
            "column_major",
            :matrix => PrivateValue(() -> tc.base_matrix),
            :major_axis => SharedValue(Column),
            :minor_axis => SharedValue(Row),
        ) do
            return test_matrix_layout()
        end

        test_case(
            "row_major",
            :matrix => PrivateValue(() -> relayout(tc.base_matrix, Row)),
            :major_axis => SharedValue(Row),
            :minor_axis => SharedValue(Column),
        ) do
            return test_matrix_layout()
        end
    end

    test_set(
        "transposed",
        :rows => PrivateValue(() -> tc.base_columns),
        :rows_nnz => PrivateValue(() -> tc.base_columns_nnz),
        :columns => PrivateValue(() -> tc.base_rows),
        :columns_nnz => PrivateValue(() -> tc.base_rows_nnz),
    ) do
        test_case(
            "row_major",
            :matrix => PrivateValue(() -> transpose(tc.base_matrix)),
            :major_axis => SharedValue(Row),
            :minor_axis => SharedValue(Column),
        ) do
            return test_matrix_layout()
        end

        test_case(
            "column_major",
            :matrix => PrivateValue(() -> relayout(transpose(tc.base_matrix), Column)),
            :major_axis => SharedValue(Column),
            :minor_axis => SharedValue(Row),
        ) do
            return test_matrix_layout()
        end
    end
end

test_set("matrix_layouts") do
    test_set("other_axis") do
        @test other_axis(Row) == Column
        @test other_axis(Column) == Row
        @test other_axis(nothing) == nothing
    end

    matrix = [
        1.0 0.0 2.0 3.0
        0.0 0.0 4.0 5.0
        6.0 0.0 0.0 7.0
    ]

    rows = [[1.0, 0.0, 2.0, 3.0], [0.0, 0.0, 4.0, 5.0], [6.0, 0.0, 0.0, 7.0]]

    rows_nnz = [3, 2, 2]

    columns = [[1.0, 0.0, 6.0], [0.0, 0.0, 0.0], [2.0, 4.0, 0.0], [3.0, 5.0, 7.0]]

    columns_nnz = [2, 0, 2, 3]

    test_set(
        "matrix",
        :base_rows => SharedValue(rows),
        :base_rows_nnz => SharedValue(rows_nnz),
        :base_columns => SharedValue(columns),
        :base_columns_nnz => SharedValue(columns_nnz),
    ) do
        test_set("dense", :base_matrix => SharedValue(matrix)) do
            return test_base_matrix()
        end
        test_set("sparse", :base_matrix => SharedValue(SparseMatrixCSC(matrix))) do
            return test_base_matrix()
        end
    end
end
