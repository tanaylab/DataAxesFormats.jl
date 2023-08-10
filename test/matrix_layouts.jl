nested_test("matrix_layouts") do
    nested_test("axes") do
        nested_test("name") do
            @test axis_name(Rows) == "Rows"
            @test axis_name(Columns) == "Columns"
            @test axis_name(nothing) == "nothing"
            @test_throws "invalid matrix axis: -1" axis_name(-1)
        end

        nested_test("other") do
            @test other_axis(Rows) == Columns
            @test other_axis(Columns) == Rows
            @test other_axis(nothing) == nothing
            @test_throws "invalid matrix axis: -1" other_axis(-1)
        end

        nested_test("dense") do
            dense = rand(4, 4)

            nested_test("matrix") do
                @test major_axis(dense) == Columns
                @test minor_axis(dense) == Rows
            end

            nested_test("transpose") do
                @test major_axis(transpose(dense)) == Rows
                @test minor_axis(transpose(dense)) == Columns
            end

            nested_test("read_only") do
                @test major_axis(SparseArrays.ReadOnly(dense)) == Columns
                @test minor_axis(SparseArrays.ReadOnly(dense)) == Rows
            end

            nested_test("named") do
                @test major_axis(NamedArray(dense)) == Columns
                @test minor_axis(NamedArray(dense)) == Rows
            end
        end

        nested_test("sparse") do
            sparse = sprand(4, 4, 0.5)

            nested_test("matrix") do
                @test major_axis(sparse) == Columns
                @test minor_axis(sparse) == Rows
            end

            nested_test("transpose") do
                @test major_axis(transpose(sparse)) == Rows
                @test minor_axis(transpose(sparse)) == Columns
            end

            nested_test("read_only") do
                @test major_axis(SparseArrays.ReadOnly(sparse)) == Columns
                @test minor_axis(SparseArrays.ReadOnly(sparse)) == Rows
            end

            nested_test("named") do
                @test major_axis(NamedArray(sparse)) == Columns
                @test minor_axis(NamedArray(sparse)) == Rows
            end
        end
    end

    nested_test("inefficient_action_policy") do
        matrix = rand(4, 4)
        inefficient_action_policy(ErrorPolicy)

        nested_test("nothing") do
            @test inefficient_action_policy(nothing) == ErrorPolicy

            check_efficient_action("test", Columns, "input", matrix)
            return check_efficient_action("test", Rows, "input", matrix)
        end

        nested_test("WarnPolicy") do
            @test inefficient_action_policy(WarnPolicy) == ErrorPolicy

            check_efficient_action("test", Columns, "input", matrix)
            @test_logs (:warn, dedent("""
                                   the major axis: Rows
                                   of the action: test
                                   is different from the major axis: Columns
                                   of the input matrix: Matrix{Float64}
                               """)) check_efficient_action("test", Rows, "input", matrix)
        end

        nested_test("ErrorPolicy") do
            @test inefficient_action_policy(ErrorPolicy) == ErrorPolicy

            check_efficient_action("test", Columns, "input", matrix)
            @test_throws dedent("""
                the major axis: Rows
                of the action: test
                is different from the major axis: Columns
                of the input matrix: Matrix{Float64}
            """) check_efficient_action("test", Rows, "input", matrix)
        end
    end

    nested_test("relayout!") do
        nested_test("automatic") do
            nested_test("dense") do
                dense = rand(4, 6)
                @test major_axis(dense) == Columns
                @test !issparse(dense)

                nested_test("matrix") do
                    relayout = relayout!(dense)
                    @test major_axis(relayout) == Rows
                    @test relayout == dense
                    @test !issparse(relayout)
                end

                nested_test("transpose") do
                    transposed_dense = transpose(dense)
                    @test major_axis(transposed_dense) == Rows

                    relayout = relayout!(transposed_dense)
                    @test major_axis(relayout) == Columns
                    @test relayout == transposed_dense
                    @test !issparse(relayout)
                end

                nested_test("read_only") do
                    relayout = relayout!(SparseArrays.ReadOnly(dense))
                    @test major_axis(relayout) == Rows
                    @test relayout == dense
                    @test !issparse(relayout)
                end

                nested_test("named") do
                    named = NamedArray(dense)
                    relayout = relayout!(named)
                    @test major_axis(relayout) == Rows
                    @test relayout == dense
                    @test !issparse(relayout.array)
                    @test relayout.dicts === named.dicts
                    @test relayout.dimnames === named.dimnames
                end
            end

            nested_test("sparse") do
                sparse = sprand(4, 6, 0.5)
                @test major_axis(sparse) == Columns
                @test issparse(sparse)

                nested_test("matrix") do
                    relayout = relayout!(sparse)
                    @test major_axis(relayout) == Rows
                    @test relayout == sparse
                    @test issparse(relayout)
                end

                nested_test("transpose") do
                    transposed_sparse = transpose(sparse)
                    @test major_axis(transposed_sparse) == Rows

                    relayout = relayout!(transposed_sparse)
                    @test major_axis(relayout) == Columns
                    @test relayout == transposed_sparse
                    @test issparse(relayout)
                end

                nested_test("read_only") do
                    relayout = relayout!(SparseArrays.ReadOnly(sparse))
                    @test major_axis(relayout) == Rows
                    @test relayout == sparse
                    @test issparse(relayout)
                end

                nested_test("named") do
                    named = NamedArray(sparse)
                    relayout = relayout!(named)
                    @test major_axis(relayout) == Rows
                    @test relayout == sparse
                    @test issparse(relayout.array)
                    @test relayout.dicts === named.dicts
                    @test relayout.dimnames === named.dimnames
                end
            end
        end

        nested_test("manual") do
            nested_test("dense") do
                from = rand(4, 6)
                @test major_axis(from) == Columns
                into = transpose(rand(6, 4))

                nested_test("matrix") do
                    relayout!(into, from)
                    @test major_axis(into) == Rows
                    @test into == from
                end

                nested_test("wrong_size") do
                    into = sprand(5, 5, 0.5)
                    @test_throws "relayout into size: (5, 5)\nis different from size: (4, 6)" relayout!(into, from)
                end

                nested_test("into_sparse") do
                    into = sprand(4, 6, 0.5)
                    @test_throws "relayout into sparse: SparseMatrixCSC{Float64, Int64} of non-sparse matrix: Matrix{Float64}" relayout!(
                        into,
                        from,
                    )
                end

                nested_test("read_only") do
                    relayout!(into, SparseArrays.ReadOnly(from))
                    @test major_axis(into) == Rows
                    @test into == from
                end

                nested_test("transpose") do
                    relayout!(transpose(into), transpose(from))
                    @test major_axis(into) == Rows
                    @test into == from
                end

                nested_test("named") do
                    nested_test("from") do
                        named_from = NamedArray(from)
                        relayout!(into, from)
                        @test major_axis(into) == Rows
                        @test into == from
                    end

                    nested_test("into") do
                        named_into = NamedArray(into)
                        relayout!(into, from)
                        @test major_axis(into) == Rows
                        @test into == from
                    end

                    nested_test("both") do
                        named_from = NamedArray(from)
                        named_into = NamedArray(into)
                        relayout!(into, from)
                        @test major_axis(into) == Rows
                        @test into == from
                    end
                end
            end

            nested_test("sparse") do
                from = sprand(4, 6, 0.5)
                @test major_axis(from) == Columns

                into = sprand(6, 4, 0.5)
                while nnz(into) != nnz(from)  # Lazy way to get a fitting target matrix.
                    into = sprand(6, 4, 0.5)
                end
                into = transpose(into)

                nested_test("matrix") do
                    relayout!(into, from)
                    @test major_axis(into) == Rows
                    @test into == from
                end

                nested_test("wrong_size") do
                    into = rand(5, 5)
                    @test_throws "relayout into size: (5, 5)\nis different from size: (4, 6)" relayout!(into, from)
                end

                nested_test("into_dense") do
                    into = rand(4, 6)
                    @test_throws "relayout into dense: Matrix{Float64} of sparse matrix: SparseMatrixCSC{Float64, Int64}" relayout!(
                        into,
                        from,
                    )
                end

                nested_test("read_only") do
                    relayout!(into, SparseArrays.ReadOnly(from))
                    @test major_axis(into) == Rows
                    @test into == from
                end

                nested_test("transpose") do
                    relayout!(transpose(into), transpose(from))
                    @test major_axis(into) == Rows
                    @test into == from
                end

                nested_test("named") do
                    nested_test("from") do
                        named_from = NamedArray(from)
                        relayout!(into, named_from)
                        @test major_axis(into) == Rows
                        @test into == from
                    end

                    nested_test("into") do
                        named_into = NamedArray(into)
                        relayout!(named_into, from)
                        @test major_axis(named_into) == Rows
                        @test into == from
                    end

                    nested_test("both") do
                        named_from = NamedArray(from)
                        named_into = NamedArray(into)
                        relayout!(named_into, named_from)
                        @test major_axis(named_into) == Rows
                        @test into == from
                    end
                end
            end
        end
    end
end
