struct Foo end

nested_test("messages") do
    nested_test("unique_name") do
        @test unique_name("foo") == "foo"
        @test unique_name("foo") == "foo#2"
        @test unique_name("foo!") == "foo!"
    end

    nested_test("depict") do
        nested_test("any") do
            @test depict(Foo()) == "(Foo)"
        end

        nested_test("tuple") do
            @test depict((1, 2)) == "(1 (Int64), 2 (Int64))"
        end

        nested_test("undef") do
            @test depict(undef) == "undef"
        end

        nested_test("missing") do
            @test depict(missing) == "missing"
        end

        nested_test("string") do
            @test depict("foo") == "\"foo\""
        end

        nested_test("symbol") do
            @test depict(:foo) == ":foo"
        end

        nested_test("bool") do
            @test depict(true) == "true"
        end

        nested_test("integer") do
            @test depict(1) == "1 (Int64)"
            @test depict(Int8(1)) == "1 (Int8)"
        end

        nested_test("float") do
            @test depict(1.0) == "1.0 (Float64)"
        end

        nested_test("vector") do
            nested_test("dense") do
                vector = [0, 1, 2]

                nested_test("base") do
                    @test depict(vector) == "3 x Int64 (Dense)"
                end

                nested_test("read_only") do
                    @test depict(SparseArrays.ReadOnly(vector)) == "3 x Int64 (ReadOnly Dense)"
                end

                nested_test("named") do
                    @test depict(NamedArray(vector)) == "3 x Int64 (Named Dense)"
                end
            end

            nested_test("sparse") do
                vector = sparse_vector([0, 1, 2])

                nested_test("base") do
                    @test depict(vector) == "3 x Int64 (Sparse UInt8 67%)"
                end

                nested_test("read_only") do
                    @test depict(SparseArrays.ReadOnly(vector)) == "3 x Int64 (ReadOnly Sparse UInt8 67%)"
                end

                nested_test("named") do
                    @test depict(NamedArray(vector)) == "3 x Int64 (Named Sparse UInt8 67%)"
                end
            end
        end

        nested_test("matrix") do
            nested_test("dense") do
                matrix = [0 1 2; 3 4 5]

                nested_test("base") do
                    @test depict(matrix) == "2 x 3 x Int64 in Columns (Dense)"
                end

                nested_test("transpose") do
                    @test depict(transpose(matrix)) == "3 x 2 x Int64 in Rows (Transpose Dense)"
                end

                nested_test("read_only") do
                    @test depict(SparseArrays.ReadOnly(matrix)) == "2 x 3 x Int64 in Columns (ReadOnly Dense)"
                end

                nested_test("named") do
                    @test depict(NamedArray(matrix)) == "2 x 3 x Int64 in Columns (Named Dense)"
                end
            end

            nested_test("sparse") do
                matrix = sparse_matrix_csc([0 1 2; 3 4 5])

                nested_test("base") do
                    @test depict(matrix) == "2 x 3 x Int64 in Columns (Sparse UInt8 83%)"
                end

                nested_test("transpose") do
                    @test depict(transpose(matrix)) == "3 x 2 x Int64 in Rows (Transpose Sparse UInt8 83%)"
                end

                nested_test("read_only") do
                    @test depict(SparseArrays.ReadOnly(matrix)) ==
                          "2 x 3 x Int64 in Columns (ReadOnly Sparse UInt8 83%)"
                end

                nested_test("named") do
                    @test depict(NamedArray(matrix)) == "2 x 3 x Int64 in Columns (Named Sparse UInt8 83%)"
                end
            end
        end

        nested_test("tensor") do
            @test depict(zeros(1, 2, 3)) == "1 x 2 x 3 x Float64 (Array{Float64, 3})"
        end
    end
end
