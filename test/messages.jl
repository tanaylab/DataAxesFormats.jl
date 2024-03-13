nested_test("messages") do
    nested_test("unique_name") do
        @test unique_name("foo") == "foo"
        @test unique_name("foo") == "foo#2"
        @test unique_name("foo!") == "foo!"
    end

    nested_test("describe") do
        nested_test("tuple") do
            @test describe((1, 2)) == "(1, 2) (Tuple{Int64, Int64})"
        end

        nested_test("undef") do
            @test describe(undef) == "undef"
        end

        nested_test("missing") do
            @test describe(missing) == "missing"
        end

        nested_test("string") do
            @test describe("foo") == "\"foo\""
        end

        nested_test("symbol") do
            @test describe(:foo) == ":foo"
        end

        nested_test("bool") do
            @test describe(true) == "true"
        end

        nested_test("integer") do
            @test describe(1) == "1 (Int64)"
            @test describe(Int8(1)) == "1 (Int8)"
        end

        nested_test("float") do
            @test describe(1.0) == "1.0 (Float64)"
        end

        nested_test("vector") do
            nested_test("dense") do
                vector = [0, 1, 2]

                nested_test("base") do
                    @test describe(vector) == "3 x Int64 (Dense)"
                end

                nested_test("read_only") do
                    @test describe(SparseArrays.ReadOnly(vector)) == "3 x Int64 (ReadOnly Dense)"
                end

                nested_test("named") do
                    @test describe(NamedArray(vector)) == "3 x Int64 (Named Dense)"
                end
            end

            nested_test("sparse") do
                vector = SparseVector([0, 1, 2])

                nested_test("base") do
                    @test describe(vector) == "3 x Int64 (Sparse 67%)"
                end

                nested_test("read_only") do
                    @test describe(SparseArrays.ReadOnly(vector)) == "3 x Int64 (ReadOnly Sparse 67%)"
                end

                nested_test("named") do
                    @test describe(NamedArray(vector)) == "3 x Int64 (Named Sparse 67%)"
                end
            end
        end

        nested_test("matrix") do
            nested_test("dense") do
                matrix = [0 1 2; 3 4 5]

                nested_test("base") do
                    @test describe(matrix) == "2 x 3 x Int64 in Columns (Dense)"
                end

                nested_test("transpose") do
                    @test describe(transpose(matrix)) == "3 x 2 x Int64 in Rows (transposed Dense)"
                end

                nested_test("read_only") do
                    @test describe(SparseArrays.ReadOnly(matrix)) == "2 x 3 x Int64 in Columns (ReadOnly Dense)"
                end

                nested_test("named") do
                    @test describe(NamedArray(matrix)) == "2 x 3 x Int64 in Columns (Named Dense)"
                end
            end

            nested_test("sparse") do
                matrix = SparseMatrixCSC([0 1 2; 3 4 5])

                nested_test("base") do
                    @test describe(matrix) == "2 x 3 x Int64 in Columns (Sparse 83%)"
                end

                nested_test("transpose") do
                    @test describe(transpose(matrix)) == "3 x 2 x Int64 in Rows (transposed Sparse 83%)"
                end

                nested_test("read_only") do
                    @test describe(SparseArrays.ReadOnly(matrix)) == "2 x 3 x Int64 in Columns (ReadOnly Sparse 83%)"
                end

                nested_test("named") do
                    @test describe(NamedArray(matrix)) == "2 x 3 x Int64 in Columns (Named Sparse 83%)"
                end
            end
        end

        nested_test("tensor") do
            @test describe(zeros(1, 2, 3)) == "1 x 2 x 3 x Float64 (Array{Float64, 3})"
        end
    end
end
