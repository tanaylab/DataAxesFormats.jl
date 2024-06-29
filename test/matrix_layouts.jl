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

    nested_test("relayout") do
        nested_test("automatic") do
            nested_test("dense") do
                dense = rand(4, 6)
                @test major_axis(dense) == Columns
                @test !issparse(dense)

                nested_test("base") do
                    result = relayout(dense)
                    @test major_axis(result) == Rows
                    @test result == dense
                    @test !issparse(result)
                end

                nested_test("adjoint") do
                    adjointed_dense = adjoint(dense)
                    @test major_axis(adjointed_dense) == Rows

                    result = relayout(adjointed_dense)
                    @test major_axis(result) == Columns
                    @test result == adjointed_dense
                    @test !issparse(result)
                end

                nested_test("transpose") do
                    transposed_dense = transpose(dense)
                    @test major_axis(transposed_dense) == Rows

                    result = relayout(transposed_dense)
                    @test major_axis(result) == Columns
                    @test result == transposed_dense
                    @test !issparse(result)
                end

                nested_test("read_only") do
                    result = relayout(SparseArrays.ReadOnly(dense))
                    @test major_axis(result) == Rows
                    @test result == dense
                    @test !issparse(result)
                end

                nested_test("named") do
                    named = NamedArray(dense)
                    result = relayout(named)
                    @test major_axis(result) == Rows
                    @test result == dense
                    @test !issparse(result.array)
                    @test result.dicts === named.dicts
                    @test result.dimnames === named.dimnames
                end
            end

            nested_test("sparse") do
                sparse = sprand(4, 6, 0.5)
                @test major_axis(sparse) == Columns
                @test issparse(sparse)

                nested_test("base") do
                    result = relayout(sparse)
                    @test major_axis(result) == Rows
                    @test result == sparse
                    @test issparse(result)
                end

                nested_test("adjoint") do
                    adjointed_sparse = adjoint(sparse)
                    @test major_axis(adjointed_sparse) == Rows

                    result = relayout(adjointed_sparse)
                    @test major_axis(result) == Columns
                    @test result == adjointed_sparse
                    @test issparse(result)
                end

                nested_test("transpose") do
                    transposed_sparse = transpose(sparse)
                    @test major_axis(transposed_sparse) == Rows

                    result = relayout(transposed_sparse)
                    @test major_axis(result) == Columns
                    @test result == transposed_sparse
                    @test issparse(result)
                end

                nested_test("read_only") do
                    result = relayout(SparseArrays.ReadOnly(sparse))
                    @test major_axis(result) == Rows
                    @test result == sparse
                    @test issparse(result)
                end

                nested_test("named") do
                    named = NamedArray(sparse)
                    result = relayout(named)
                    @test major_axis(result) == Rows
                    @test result == sparse
                    @test issparse(result.array)
                    @test result.dicts === named.dicts
                    @test result.dimnames === named.dimnames
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

    nested_test("reformat") do
        nested_test("densify") do
            nested_test("dense") do
                matrix = [1 2 3; 4 5 6]
                vector = [1, 2, 3]

                nested_test("()") do
                    @test densify(matrix) === matrix
                    @test densify(vector) === vector
                end

                nested_test("transpose") do
                    matrix = transpose(matrix)
                    @test parent(densify(matrix)) === parent(matrix)
                end

                nested_test("named") do
                    matrix = NamedArray(matrix)
                    @test densify(matrix).array === matrix.array
                    vector = NamedArray(vector)
                    @test densify(vector).array === vector.array
                end

                nested_test("copy") do
                    @test densify(matrix; copy = true) !== matrix
                    @test densify(vector; copy = true) !== vector
                    @test densify(matrix; copy = true) == matrix
                    @test densify(vector; copy = true) == vector
                    @test densify(matrix; copy = true) isa Matrix
                    @test densify(vector; copy = true) isa Vector
                end
            end

            nested_test("sparse") do
                matrix = SparseMatrixCSC([1 0 0; 0 0 0; 0 0 0])
                vector = SparseVector([1, 0, 0])

                @test densify(matrix) !== matrix
                @test densify(vector) !== vector
                @test densify(matrix) == matrix
                @test densify(vector) == vector
                @test densify(matrix) isa Matrix
                @test densify(vector) isa Vector
            end
        end

        nested_test("sparsify") do
            nested_test("sparse") do
                matrix = SparseMatrixCSC([1 0 0; 0 0 0; 0 0 0])
                vector = SparseVector([1, 0, 0])

                nested_test("()") do
                    @test sparsify(matrix) === matrix
                    @test sparsify(vector) === vector
                end

                nested_test("transpose") do
                    matrix = transpose(matrix)
                    @test parent(sparsify(matrix)) === parent(matrix)
                end

                nested_test("named") do
                    matrix = NamedArray(matrix)
                    @test sparsify(matrix).array === matrix.array
                    vector = NamedArray(vector)
                    @test sparsify(vector).array === vector.array
                end

                nested_test("copy") do
                    @test sparsify(matrix; copy = true) !== matrix
                    @test sparsify(vector; copy = true) !== vector
                    @test sparsify(matrix; copy = true) == matrix
                    @test sparsify(vector; copy = true) == vector
                    @test sparsify(matrix; copy = true) isa SparseMatrixCSC
                    @test sparsify(vector; copy = true) isa SparseVector
                end
            end

            nested_test("dense") do
                matrix = [1 2 3; 4 5 6]
                vector = [1, 2, 3]

                @test sparsify(matrix) !== matrix
                @test sparsify(vector) !== vector
                @test sparsify(matrix) == matrix
                @test sparsify(vector) == vector
                @test sparsify(matrix) isa SparseMatrixCSC
                @test sparsify(vector) isa SparseVector
            end
        end

        nested_test("bestify") do
            nested_test("light") do
                nested_test("dense") do
                    matrix = [1 0 0; 0 0 0; 0 0 0]
                    vector = [1, 0, 0]

                    @test bestify(matrix) !== matrix
                    @test bestify(vector) !== vector
                    @test bestify(matrix) == matrix
                    @test bestify(vector) == vector
                    @test bestify(matrix) isa SparseMatrixCSC
                    @test bestify(vector) isa SparseVector
                end

                nested_test("sparse") do
                    matrix = SparseMatrixCSC([1 0 0; 0 0 0; 0 0 0])
                    vector = SparseVector([1, 0, 0])

                    nested_test("()") do
                        @test bestify(matrix) === matrix
                        @test bestify(vector) === vector
                    end

                    nested_test("transpose") do
                        matrix = transpose(matrix)
                        @test parent(bestify(matrix)) === parent(matrix)
                    end

                    nested_test("named") do
                        matrix = NamedArray(matrix)
                        @test bestify(matrix).array === matrix.array
                        vector = NamedArray(vector)
                        @test bestify(vector).array === vector.array
                    end

                    nested_test("copy") do
                        @test bestify(matrix; copy = true) !== matrix
                        @test bestify(vector; copy = true) !== vector
                        @test bestify(matrix; copy = true) == matrix
                        @test bestify(vector; copy = true) == vector
                        @test bestify(matrix; copy = true) isa SparseMatrixCSC
                        @test bestify(vector; copy = true) isa SparseVector
                    end
                end
            end

            nested_test("heavy") do
                nested_test("dense") do
                    matrix = [1 2 3; 4 5 6]
                    vector = [1, 2, 3]

                    nested_test("()") do
                        @test bestify(matrix) === matrix
                        @test bestify(vector) === vector
                    end

                    nested_test("transpose") do
                        matrix = transpose(matrix)
                        @test bestify(matrix) === matrix
                    end

                    nested_test("named") do
                        matrix = NamedArray(matrix)
                        @test bestify(matrix).array === matrix.array
                        vector = NamedArray(vector)
                        @test bestify(vector).array === vector.array
                    end

                    nested_test("copy") do
                        @test bestify(matrix; copy = true) !== matrix
                        @test bestify(vector; copy = true) !== vector
                        @test bestify(matrix; copy = true) == matrix
                        @test bestify(vector; copy = true) == vector
                        @test bestify(matrix; copy = true) isa Matrix
                        @test bestify(vector; copy = true) isa Vector
                    end
                end

                nested_test("sparse") do
                    matrix = SparseMatrixCSC([1 2 3; 4 5 6])
                    vector = SparseVector([1, 2, 3])

                    @test bestify(matrix; copy = true) !== matrix
                    @test bestify(vector; copy = true) !== vector
                    @test bestify(matrix; copy = true) == matrix
                    @test bestify(vector; copy = true) == vector
                    @test bestify(matrix; copy = true) isa Matrix
                    @test bestify(vector; copy = true) isa Vector
                end
            end
        end
    end
end
