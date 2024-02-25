nested_test("messages") do
    nested_test("unique_name") do
        @test unique_name("foo") == "foo"
        @test unique_name("foo") == "foo#2"
        @test unique_name("foo!") == "foo!"
    end

    nested_test("present") do
        nested_test("undef") do
            @test present(undef) == "undef"
        end

        nested_test("missing") do
            @test present(missing) == "missing"
        end

        nested_test("string") do
            @test present("foo") == "\"foo\""
        end

        nested_test("symbol") do
            @test present(:foo) == ":foo"
        end

        nested_test("integer") do
            @test present(1) == "1"
        end

        nested_test("float") do
            @test present(1.0) == "1.0"
        end

        nested_test("vector") do
            nested_test("dense") do
                vector = [0, 1, 2]

                nested_test("base") do
                    @test present(vector) == "3 x Int64 (Dense)"
                end

                nested_test("read_only") do
                    @test present(SparseArrays.ReadOnly(vector)) == "3 x Int64 (ReadOnly Dense)"
                end

                nested_test("named") do
                    @test present(NamedArray(vector)) == "3 x Int64 (Named Dense)"
                end
            end

            nested_test("sparse") do
                vector = SparseVector([0, 1, 2])

                nested_test("base") do
                    @test present(vector) == "3 x Int64 (Sparse 67%)"
                end

                nested_test("read_only") do
                    @test present(SparseArrays.ReadOnly(vector)) == "3 x Int64 (ReadOnly Sparse 67%)"
                end

                nested_test("named") do
                    @test present(NamedArray(vector)) == "3 x Int64 (Named Sparse 67%)"
                end
            end
        end

        nested_test("matrix") do
            nested_test("dense") do
                matrix = [0 1 2; 3 4 5]

                nested_test("base") do
                    @test present(matrix) == "2 x 3 x Int64 (Dense in Columns)"
                end

                nested_test("transpose") do
                    @test present(transpose(matrix)) == "3 x 2 x Int64 (Dense in Rows)"
                end

                nested_test("read_only") do
                    @test present(SparseArrays.ReadOnly(matrix)) == "2 x 3 x Int64 (ReadOnly Dense in Columns)"
                end

                nested_test("named") do
                    @test present(NamedArray(matrix)) == "2 x 3 x Int64 (Named Dense in Columns)"
                end
            end

            nested_test("sparse") do
                matrix = SparseMatrixCSC([0 1 2; 3 4 5])

                nested_test("base") do
                    @test present(matrix) == "2 x 3 x Int64 (Sparse 83% in Columns)"
                end

                nested_test("transpose") do
                    @test present(transpose(matrix)) == "3 x 2 x Int64 (Sparse 83% in Rows)"
                end

                nested_test("read_only") do
                    @test present(SparseArrays.ReadOnly(matrix)) == "2 x 3 x Int64 (ReadOnly Sparse 83% in Columns)"
                end

                nested_test("named") do
                    @test present(NamedArray(matrix)) == "2 x 3 x Int64 (Named Sparse 83% in Columns)"
                end
            end
        end

        nested_test("tensor") do
            @test present(zeros(1, 2, 3)) == "1 x 2 x 3 x Float64 (Array{Float64, 3})"
        end
    end
end
