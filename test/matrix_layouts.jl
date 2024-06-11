nested_test("matrix_layouts") do
    nested_test("copy_array") do
        @test eltype(split("a,b", ",")) != AbstractString
        @test eltype(copy_array(split("a,b", ","))) == AbstractString
    end

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
            @test other_axis(nothing) === nothing
            @test_throws "invalid matrix axis: -1" other_axis(-1)
        end

        nested_test("dense") do
            dense = rand(4, 4)

            nested_test("base") do
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

            nested_test("base") do
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

    nested_test("inefficient_action_handler") do
        matrix = rand(4, 4)
        inefficient_action_handler(ErrorHandler)

        nested_test("ignore") do
            @test inefficient_action_handler(IgnoreHandler) == ErrorHandler

            check_efficient_action("test", Columns, "input", matrix)
            return check_efficient_action("test", Rows, "input", matrix)
        end

        nested_test("warn") do
            @test inefficient_action_handler(WarnHandler) == ErrorHandler

            check_efficient_action("test", Columns, "input", matrix)
            @test_logs (:warn, dedent("""
                                   the major axis: Rows
                                   of the action: test
                                   is different from the major axis: Columns
                                   of the input matrix: Matrix{Float64}
                               """)) check_efficient_action("test", Rows, "input", matrix)
        end

        nested_test("error") do
            @test inefficient_action_handler(ErrorHandler) == ErrorHandler

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

                nested_test("base") do
                    relayout = relayout!(dense)
                    @test major_axis(relayout) == Rows
                    @test relayout == dense
                    @test !issparse(relayout)
                end

                nested_test("adjoint") do
                    adjointed_dense = adjoint(dense)
                    @test major_axis(adjointed_dense) == Rows

                    relayout = relayout!(adjointed_dense)
                    @test major_axis(relayout) == Columns
                    @test relayout == adjointed_dense
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

                nested_test("base") do
                    relayout = relayout!(sparse)
                    @test major_axis(relayout) == Rows
                    @test relayout == sparse
                    @test issparse(relayout)
                end

                nested_test("adjoint") do
                    adjointed_sparse = adjoint(sparse)
                    @test major_axis(adjointed_sparse) == Rows

                    relayout = relayout!(adjointed_sparse)
                    @test major_axis(relayout) == Columns
                    @test relayout == adjointed_sparse
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
                source = rand(4, 6)
                @test major_axis(source) == Columns
                destination = transpose(rand(6, 4))

                nested_test("base") do
                    relayout!(destination, source)
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("wrong_size") do
                    destination = sprand(5, 5, 0.5)
                    @test_throws "relayout destination size: (5, 5)\nis different from source size: (4, 6)" relayout!(
                        destination,
                        source,
                    )
                end

                nested_test("destination_sparse") do
                    destination = sprand(4, 6, 0.5)
                    @test_throws dedent("""
                        relayout sparse destination: SparseMatrixCSC{Float64, Int64}
                        and non-sparse source: Matrix{Float64}
                    """) relayout!(destination, source)
                end

                nested_test("read_only") do
                    relayout!(destination, SparseArrays.ReadOnly(source))
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("transpose") do
                    relayout!(transpose(destination), transpose(source))
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("named") do
                    nested_test("source") do
                        named_source = NamedArray(source)
                        relayout!(destination, source)
                        @test major_axis(destination) == Rows
                        @test destination == source
                    end

                    nested_test("destination") do
                        named_destination = NamedArray(destination)
                        relayout!(destination, source)
                        @test major_axis(destination) == Rows
                        @test destination == source
                    end

                    nested_test("both") do
                        named_source = NamedArray(source)
                        named_destination = NamedArray(destination)
                        relayout!(destination, source)
                        @test major_axis(destination) == Rows
                        @test destination == source
                    end
                end
            end

            nested_test("sparse") do
                source = sprand(4, 6, 0.5)
                @test major_axis(source) == Columns

                destination = sprand(6, 4, 0.5)
                while nnz(destination) != nnz(source)  # Lazy way to get a fitting target matrix.
                    destination = sprand(6, 4, 0.5)
                end
                destination = transpose(destination)

                nested_test("base") do
                    relayout!(destination, source)
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("wrong_size") do
                    destination = rand(5, 5)
                    @test_throws dedent("""
                        relayout destination size: (5, 5)
                        is different from source size: (4, 6)
                    """) relayout!(destination, source)
                end

                nested_test("destination_dense") do
                    destination = transpose(rand(6, 4))
                    relayout!(destination, source)
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("read_only") do
                    relayout!(destination, SparseArrays.ReadOnly(source))
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("transpose") do
                    relayout!(transpose(destination), transpose(source))
                    @test major_axis(destination) == Rows
                    @test destination == source
                end

                nested_test("named") do
                    nested_test("source") do
                        named_source = NamedArray(source)
                        relayout!(destination, named_source)
                        @test major_axis(destination) == Rows
                        @test destination == source
                    end

                    nested_test("destination") do
                        named_destination = NamedArray(destination)
                        relayout!(named_destination, source)
                        @test major_axis(named_destination) == Rows
                        @test destination == source
                    end

                    nested_test("both") do
                        named_source = NamedArray(source)
                        named_destination = NamedArray(destination)
                        relayout!(named_destination, named_source)
                        @test major_axis(named_destination) == Rows
                        @test destination == source
                    end
                end
            end
        end
    end
end
