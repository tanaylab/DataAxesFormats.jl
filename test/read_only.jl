nested_test("read_only") do
    nested_test("vector") do
        mutable_vector = [0, 1]

        nested_test("dense") do
            @test depict(mutable_vector) == "2 x Int64 (Dense)"
            @test !is_read_only_array(mutable_vector)

            read_only_vector = SparseArrays.ReadOnly(mutable_vector)
            @test is_read_only_array(read_only_vector)
            @test read_only_array(read_only_vector) === read_only_vector
        end

        nested_test("sparse") do
            mutable_vector = SparseVector(mutable_vector)
            @test depict(mutable_vector) == "2 x Int64 (Sparse Int64 50%)"
            @test !is_read_only_array(mutable_vector)

            read_only_vector = SparseArrays.ReadOnly(mutable_vector)
            @test depict(read_only_vector) == "2 x Int64 (ReadOnly Sparse Int64 50%)"
            @test is_read_only_array(read_only_vector)
            @test read_only_array(read_only_vector) === read_only_vector
        end

        nested_test("named") do
            nested_test("()") do
                mutable_vector = NamedArray(mutable_vector)
                @test depict(mutable_vector) == "2 x Int64 (Named Dense)"
                @test !is_read_only_array(mutable_vector)

                read_only_vector = SparseArrays.ReadOnly(mutable_vector)
                @test depict(read_only_vector) == "2 x Int64 (ReadOnly Named Dense)"
                @test is_read_only_array(read_only_vector)
                @test read_only_array(read_only_vector) === read_only_vector
            end

            nested_test("read_only") do
                read_only_vector = NamedArray(SparseArrays.ReadOnly(mutable_vector))
                @test depict(read_only_vector) == "2 x Int64 (Named ReadOnly Dense)"
                @test is_read_only_array(read_only_vector)
                @test read_only_array(read_only_vector) === read_only_vector
            end

            nested_test("sparse") do
                nested_test("()") do
                    mutable_vector = NamedArray(SparseVector(mutable_vector))
                    @test depict(mutable_vector) == "2 x Int64 (Named Sparse Int64 50%)"
                    @test !is_read_only_array(mutable_vector)

                    read_only_vector = SparseArrays.ReadOnly(mutable_vector)
                    @test depict(read_only_vector) == "2 x Int64 (ReadOnly Named Sparse Int64 50%)"
                    @test is_read_only_array(read_only_vector)
                    @test read_only_array(read_only_vector) === read_only_vector
                end

                nested_test("read_only") do
                    read_only_vector = NamedArray(SparseArrays.ReadOnly(SparseVector(mutable_vector)))
                    @test depict(read_only_vector) == "2 x Int64 (Named ReadOnly Sparse Int64 50%)"
                    @test is_read_only_array(read_only_vector)
                    @test read_only_array(read_only_vector) === read_only_vector
                end
            end
        end
    end

    nested_test("matrix") do
        mutable_matrix = [0 1 2; 0 3 0]

        nested_test("dense") do
            nested_test("()") do
                @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (Dense)"
                @test !is_read_only_array(mutable_matrix)

                read_only_matrix = SparseArrays.ReadOnly(mutable_matrix)
                @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (ReadOnly Dense)"
                @test is_read_only_array(read_only_matrix)
                @test read_only_array(read_only_matrix) === read_only_matrix
            end

            nested_test("permuted_dims") do
                nested_test("1,2") do
                    permuted_matrix = PermutedDimsArray(mutable_matrix, (1, 2))
                    @test depict(permuted_matrix) == "2 x 3 x Int64 in Columns (!Permuted Dense)"
                    @test !is_read_only_array(mutable_matrix)

                    read_only_matrix = SparseArrays.ReadOnly(permuted_matrix)
                    @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (ReadOnly !Permuted Dense)"
                    @test is_read_only_array(read_only_matrix)
                    @test read_only_array(read_only_matrix) === read_only_matrix
                end

                nested_test("2,1") do
                    permuted_matrix = PermutedDimsArray(mutable_matrix, (2, 1))
                    @test depict(permuted_matrix) == "3 x 2 x Int64 in Rows (Permuted Dense)"
                    @test !is_read_only_array(mutable_matrix)

                    read_only_permuted_matrix = SparseArrays.ReadOnly(permuted_matrix)
                    @test depict(read_only_permuted_matrix) == "3 x 2 x Int64 in Rows (ReadOnly Permuted Dense)"
                    @test is_read_only_array(read_only_permuted_matrix)
                    @test read_only_array(read_only_permuted_matrix) === read_only_permuted_matrix

                    permuted_read_only_matrix = PermutedDimsArray(SparseArrays.ReadOnly(mutable_matrix), (2, 1))
                    @test depict(permuted_read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted ReadOnly Dense)"
                    @test is_read_only_array(permuted_read_only_matrix)
                    @test read_only_array(permuted_read_only_matrix) === permuted_read_only_matrix
                end
            end

            nested_test("transpose") do
                transpose_matrix = transpose(mutable_matrix)
                @test depict(transpose_matrix) == "3 x 2 x Int64 in Rows (Transpose Dense)"
                @test !is_read_only_array(mutable_matrix)

                read_only_transpose_matrix = SparseArrays.ReadOnly(transpose_matrix)
                @test depict(read_only_transpose_matrix) == "3 x 2 x Int64 in Rows (ReadOnly Transpose Dense)"
                @test is_read_only_array(read_only_transpose_matrix)
                @test read_only_array(read_only_transpose_matrix) === read_only_transpose_matrix

                transpose_read_only_matrix = transpose(SparseArrays.ReadOnly(mutable_matrix))
                @test depict(transpose_read_only_matrix) == "3 x 2 x Int64 in Rows (Transpose ReadOnly Dense)"
                @test is_read_only_array(transpose_read_only_matrix)
                @test read_only_array(transpose_read_only_matrix) === transpose_read_only_matrix
            end

            nested_test("adjoint") do
                adjoint_matrix = adjoint(mutable_matrix)
                @test depict(adjoint_matrix) == "3 x 2 x Int64 in Rows (Adjoint Dense)"
                @test !is_read_only_array(adjoint_matrix)

                read_only_adjoint_matrix = SparseArrays.ReadOnly(adjoint_matrix)
                @test depict(read_only_adjoint_matrix) == "3 x 2 x Int64 in Rows (ReadOnly Adjoint Dense)"
                @test is_read_only_array(read_only_adjoint_matrix)
                @test read_only_array(read_only_adjoint_matrix) === read_only_adjoint_matrix

                adjoint_read_only_matrix = adjoint(SparseArrays.ReadOnly(mutable_matrix))
                @test depict(adjoint_read_only_matrix) == "3 x 2 x Int64 in Rows (Adjoint ReadOnly Dense)"
                @test is_read_only_array(adjoint_read_only_matrix)
                @test read_only_array(adjoint_read_only_matrix) === adjoint_read_only_matrix
            end
        end

        nested_test("sparse") do
            mutable_matrix = SparseMatrixCSC(mutable_matrix)

            nested_test("()") do
                @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (Sparse Int64 50%)"
                @test !is_read_only_array(mutable_matrix)

                read_only_matrix = SparseArrays.ReadOnly(mutable_matrix)
                @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (ReadOnly Sparse Int64 50%)"
                @test is_read_only_array(read_only_matrix)
                @test read_only_array(read_only_matrix) === read_only_matrix
            end

            nested_test("permuted_dims") do
                nested_test("1,2") do
                    permuted_matrix = PermutedDimsArray(mutable_matrix, (1, 2))
                    @test depict(permuted_matrix) == "2 x 3 x Int64 in Columns (!Permuted Sparse Int64 50%)"
                    @test !is_read_only_array(mutable_matrix)

                    read_only_matrix = SparseArrays.ReadOnly(mutable_matrix)
                    @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (ReadOnly Sparse Int64 50%)"
                    @test is_read_only_array(read_only_matrix)
                    @test read_only_array(read_only_matrix) === read_only_matrix
                end

                nested_test("2,1") do
                    permuted_matrix = PermutedDimsArray(mutable_matrix, (2, 1))
                    @test depict(permuted_matrix) == "3 x 2 x Int64 in Rows (Permuted Sparse Int64 50%)"
                    @test !is_read_only_array(mutable_matrix)

                    read_only_permuted_matrix = SparseArrays.ReadOnly(permuted_matrix)
                    @test depict(read_only_permuted_matrix) ==
                          "3 x 2 x Int64 in Rows (ReadOnly Permuted Sparse Int64 50%)"
                    @test is_read_only_array(read_only_permuted_matrix)
                    @test read_only_array(read_only_permuted_matrix) === read_only_permuted_matrix

                    permuted_read_only_matrix = PermutedDimsArray(SparseArrays.ReadOnly(mutable_matrix), (2, 1))
                    @test depict(permuted_read_only_matrix) ==
                          "3 x 2 x Int64 in Rows (Permuted ReadOnly Sparse Int64 50%)"
                    @test is_read_only_array(permuted_read_only_matrix)
                    @test read_only_array(permuted_read_only_matrix) === permuted_read_only_matrix
                end
            end

            nested_test("transpose") do
                transpose_matrix = transpose(mutable_matrix)
                @test depict(transpose_matrix) == "3 x 2 x Int64 in Rows (Transpose Sparse Int64 50%)"
                @test !is_read_only_array(mutable_matrix)

                read_only_transpose_matrix = SparseArrays.ReadOnly(transpose_matrix)
                @test depict(read_only_transpose_matrix) ==
                      "3 x 2 x Int64 in Rows (ReadOnly Transpose Sparse Int64 50%)"
                @test is_read_only_array(read_only_transpose_matrix)
                @test read_only_array(read_only_transpose_matrix) === read_only_transpose_matrix

                transpose_read_only_matrix = transpose(SparseArrays.ReadOnly(mutable_matrix))
                @test depict(transpose_read_only_matrix) ==
                      "3 x 2 x Int64 in Rows (Transpose ReadOnly Sparse Int64 50%)"
                @test is_read_only_array(transpose_read_only_matrix)
                @test read_only_array(transpose_read_only_matrix) === transpose_read_only_matrix
            end

            nested_test("adjoint") do
                adjoint_matrix = adjoint(mutable_matrix)
                @test depict(adjoint_matrix) == "3 x 2 x Int64 in Rows (Adjoint Sparse Int64 50%)"
                @test !is_read_only_array(adjoint_matrix)

                read_only_adjoint_matrix = SparseArrays.ReadOnly(adjoint_matrix)
                @test depict(read_only_adjoint_matrix) == "3 x 2 x Int64 in Rows (ReadOnly Adjoint Sparse Int64 50%)"
                @test is_read_only_array(read_only_adjoint_matrix)
                @test read_only_array(read_only_adjoint_matrix) === read_only_adjoint_matrix

                adjoint_read_only_matrix = adjoint(SparseArrays.ReadOnly(mutable_matrix))
                @test depict(adjoint_read_only_matrix) == "3 x 2 x Int64 in Rows (Adjoint ReadOnly Sparse Int64 50%)"
                @test is_read_only_array(adjoint_read_only_matrix)
                @test read_only_array(adjoint_read_only_matrix) === adjoint_read_only_matrix
            end
        end

        nested_test("named") do
            nested_test("dense") do
                named_matrix = NamedArray(mutable_matrix)

                nested_test("()") do
                    @test depict(named_matrix) == "2 x 3 x Int64 in Columns (Named Dense)"
                    @test !is_read_only_array(named_matrix)

                    read_only_named_matrix = SparseArrays.ReadOnly(named_matrix)
                    @test depict(read_only_named_matrix) == "2 x 3 x Int64 in Columns (ReadOnly Named Dense)"
                    @test is_read_only_array(read_only_named_matrix)
                    @test read_only_array(read_only_named_matrix) === read_only_named_matrix
                end

                nested_test("read_only") do
                    named_read_only_matrix = NamedArray(SparseArrays.ReadOnly(mutable_matrix))
                    @test depict(named_read_only_matrix) == "2 x 3 x Int64 in Columns (Named ReadOnly Dense)"
                    @test is_read_only_array(named_read_only_matrix)
                    @test read_only_array(named_read_only_matrix) === named_read_only_matrix

                    nested_test("permuted_dims") do
                        nested_test("1,2") do
                            named_read_only_permuted_matrix =
                                NamedArray(SparseArrays.ReadOnly(PermutedDimsArray(mutable_matrix, (1, 2))))
                            @test depict(named_read_only_permuted_matrix) ==
                                  "2 x 3 x Int64 in Columns (Named ReadOnly !Permuted Dense)"
                            @test is_read_only_array(named_read_only_permuted_matrix)
                            @test read_only_array(named_read_only_permuted_matrix) === named_read_only_permuted_matrix
                        end

                        nested_test("2,1") do
                            named_read_only_permuted_matrix =
                                NamedArray(SparseArrays.ReadOnly(PermutedDimsArray(mutable_matrix, (2, 1))))
                            @test depict(named_read_only_permuted_matrix) ==
                                  "3 x 2 x Int64 in Rows (Named ReadOnly Permuted Dense)"
                            @test is_read_only_array(named_read_only_permuted_matrix)
                            @test read_only_array(named_read_only_permuted_matrix) === named_read_only_permuted_matrix
                        end
                    end

                    nested_test("transpose") do
                        named_read_only_transpose_matrix = NamedArray(SparseArrays.ReadOnly(transpose(mutable_matrix)))
                        @test depict(named_read_only_transpose_matrix) ==
                              "3 x 2 x Int64 in Rows (Named ReadOnly Transpose Dense)"
                        @test is_read_only_array(named_read_only_transpose_matrix)
                        @test read_only_array(named_read_only_transpose_matrix) === named_read_only_transpose_matrix
                    end

                    nested_test("adjoint") do
                        named_read_only_adjoint_matrix = NamedArray(SparseArrays.ReadOnly(adjoint(mutable_matrix)))
                        @test depict(named_read_only_adjoint_matrix) ==
                              "3 x 2 x Int64 in Rows (Named ReadOnly Adjoint Dense)"
                        @test is_read_only_array(named_read_only_adjoint_matrix)
                        @test read_only_array(named_read_only_adjoint_matrix) === named_read_only_adjoint_matrix
                    end
                end

                nested_test("permuted_dims") do
                    nested_test("1,2") do
                        nested_test("()") do
                            permuted_named_matrix = PermutedDimsArray(named_matrix, (1, 2))
                            @test !is_read_only_array(permuted_named_matrix)
                            @test depict(permuted_named_matrix) == "2 x 3 x Int64 in Columns (!Permuted Named Dense)"

                            read_only_permuted_named_matrix = SparseArrays.ReadOnly(permuted_named_matrix)
                            @test depict(read_only_permuted_named_matrix) ==
                                  "2 x 3 x Int64 in Columns (ReadOnly !Permuted Named Dense)"
                            @test is_read_only_array(read_only_permuted_named_matrix)
                            @test read_only_array(read_only_permuted_named_matrix) === read_only_permuted_named_matrix
                        end

                        nested_test("read_only") do
                            permuted_named_read_only_matrix =
                                PermutedDimsArray(NamedArray(SparseArrays.ReadOnly(mutable_matrix)), (1, 2))
                            @test depict(permuted_named_read_only_matrix) ==
                                  "2 x 3 x Int64 in Columns (!Permuted Named ReadOnly Dense)"
                            @test is_read_only_array(permuted_named_read_only_matrix)
                            @test read_only_array(permuted_named_read_only_matrix) === permuted_named_read_only_matrix
                        end
                    end

                    nested_test("2,1") do
                        nested_test("()") do
                            permuted_named_matrix = PermutedDimsArray(named_matrix, (2, 1))
                            @test !is_read_only_array(permuted_named_matrix)
                            @test depict(permuted_named_matrix) == "3 x 2 x Int64 in Rows (Permuted Named Dense)"

                            read_only_permuted_named_matrix = SparseArrays.ReadOnly(permuted_named_matrix)
                            @test depict(read_only_permuted_named_matrix) ==
                                  "3 x 2 x Int64 in Rows (ReadOnly Permuted Named Dense)"
                            @test is_read_only_array(read_only_permuted_named_matrix)
                            @test read_only_array(read_only_permuted_named_matrix) === read_only_permuted_named_matrix
                        end

                        nested_test("read_only") do
                            permuted_named_read_only_matrix =
                                PermutedDimsArray(NamedArray(SparseArrays.ReadOnly(mutable_matrix)), (2, 1))
                            @test depict(permuted_named_read_only_matrix) ==
                                  "3 x 2 x Int64 in Rows (Permuted Named ReadOnly Dense)"
                            @test is_read_only_array(permuted_named_read_only_matrix)
                            @test read_only_array(permuted_named_read_only_matrix) === permuted_named_read_only_matrix
                        end
                    end
                end

                nested_test("permute_dims") do
                    nested_test("1,2") do
                        nested_test("()") do
                            named_permuted_matrix = NamedArray(PermutedDimsArray(mutable_matrix, (1, 2)))
                            @test depict(named_permuted_matrix) == "2 x 3 x Int64 in Columns (Named !Permuted Dense)"
                            @test !is_read_only_array(named_permuted_matrix)

                            read_only_named_permuted_matrix = SparseArrays.ReadOnly(named_permuted_matrix)
                            @test depict(read_only_named_permuted_matrix) ==
                                  "2 x 3 x Int64 in Columns (ReadOnly Named !Permuted Dense)"
                            @test is_read_only_array(read_only_named_permuted_matrix)
                            @test read_only_array(read_only_named_permuted_matrix) === read_only_named_permuted_matrix
                        end

                        nested_test("read_only") do
                            named_permuted_read_only_matrix =
                                NamedArray(PermutedDimsArray(SparseArrays.ReadOnly(mutable_matrix), (1, 2)))
                            @test depict(named_permuted_read_only_matrix) ==
                                  "2 x 3 x Int64 in Columns (Named !Permuted ReadOnly Dense)"
                            @test is_read_only_array(named_permuted_read_only_matrix)
                            @test read_only_array(named_permuted_read_only_matrix) === named_permuted_read_only_matrix
                        end
                    end

                    nested_test("2,1") do
                        nested_test("()") do
                            named_permuted_matrix = NamedArray(PermutedDimsArray(mutable_matrix, (2, 1)))
                            @test depict(named_permuted_matrix) == "3 x 2 x Int64 in Rows (Named Permuted Dense)"
                            @test !is_read_only_array(named_permuted_matrix)

                            read_only_named_permuted_matrix = SparseArrays.ReadOnly(named_permuted_matrix)
                            @test depict(read_only_named_permuted_matrix) ==
                                  "3 x 2 x Int64 in Rows (ReadOnly Named Permuted Dense)"
                            @test is_read_only_array(read_only_named_permuted_matrix)
                            @test read_only_array(read_only_named_permuted_matrix) === read_only_named_permuted_matrix
                        end

                        nested_test("read_only") do
                            named_permuted_read_only_matrix =
                                NamedArray(PermutedDimsArray(SparseArrays.ReadOnly(mutable_matrix), (2, 1)))
                            @test depict(named_permuted_read_only_matrix) ==
                                  "3 x 2 x Int64 in Rows (Named Permuted ReadOnly Dense)"
                            @test is_read_only_array(named_permuted_read_only_matrix)
                            @test read_only_array(named_permuted_read_only_matrix) === named_permuted_read_only_matrix
                        end
                    end
                end

                nested_test("transposed") do
                    nested_test("()") do
                        transpose_named_matrix = transpose(named_matrix)
                        @test !is_read_only_array(transpose_named_matrix)
                        @test depict(transpose_named_matrix) == "3 x 2 x Int64 in Rows (Named Transpose Dense)"

                        read_only_transpose_named_matrix = SparseArrays.ReadOnly(transpose_named_matrix)
                        @test depict(read_only_transpose_named_matrix) ==
                              "3 x 2 x Int64 in Rows (ReadOnly Named Transpose Dense)"
                        @test is_read_only_array(read_only_transpose_named_matrix)
                        @test read_only_array(read_only_transpose_named_matrix) === read_only_transpose_named_matrix
                    end

                    nested_test("read_only") do
                        transpose_named_read_only_matrix = transpose(NamedArray(SparseArrays.ReadOnly(mutable_matrix)))
                        @test depict(transpose_named_read_only_matrix) ==
                              "3 x 2 x Int64 in Rows (Named Transpose ReadOnly Dense)"
                        @test is_read_only_array(transpose_named_read_only_matrix)
                        @test read_only_array(transpose_named_read_only_matrix) === transpose_named_read_only_matrix
                    end
                end

                nested_test("transpose") do
                    nested_test("()") do
                        named_transpose_matrix = NamedArray(transpose(mutable_matrix))
                        @test depict(named_transpose_matrix) == "3 x 2 x Int64 in Rows (Named Transpose Dense)"
                        @test !is_read_only_array(named_transpose_matrix)

                        read_only_named_transpose_matrix = SparseArrays.ReadOnly(named_transpose_matrix)
                        @test depict(read_only_named_transpose_matrix) ==
                              "3 x 2 x Int64 in Rows (ReadOnly Named Transpose Dense)"
                        @test is_read_only_array(read_only_named_transpose_matrix)
                        @test read_only_array(read_only_named_transpose_matrix) === read_only_named_transpose_matrix
                    end

                    nested_test("read_only") do
                        named_transpose_read_only_matrix = NamedArray(transpose(SparseArrays.ReadOnly(mutable_matrix)))
                        @test depict(named_transpose_read_only_matrix) ==
                              "3 x 2 x Int64 in Rows (Named Transpose ReadOnly Dense)"
                        @test is_read_only_array(named_transpose_read_only_matrix)
                        @test read_only_array(named_transpose_read_only_matrix) === named_transpose_read_only_matrix
                    end
                end

                nested_test("adjointed") do
                    nested_test("()") do
                        adjoint_named_matrix = adjoint(named_matrix)
                        @test !is_read_only_array(adjoint_named_matrix)
                        @test depict(adjoint_named_matrix) == "3 x 2 x Int64 in Rows (Named Adjoint Dense)"

                        read_only_adjoint_named_matrix = SparseArrays.ReadOnly(adjoint_named_matrix)
                        @test depict(read_only_adjoint_named_matrix) ==
                              "3 x 2 x Int64 in Rows (ReadOnly Named Adjoint Dense)"
                        @test is_read_only_array(read_only_adjoint_named_matrix)
                        @test read_only_array(read_only_adjoint_named_matrix) === read_only_adjoint_named_matrix
                    end

                    nested_test("read_only") do
                        adjoint_named_read_only_matrix = adjoint(NamedArray(SparseArrays.ReadOnly(mutable_matrix)))
                        @test depict(adjoint_named_read_only_matrix) ==
                              "3 x 2 x Int64 in Rows (Named Adjoint ReadOnly Dense)"
                        @test is_read_only_array(adjoint_named_read_only_matrix)
                        @test read_only_array(adjoint_named_read_only_matrix) === adjoint_named_read_only_matrix
                    end
                end

                nested_test("adjoint") do
                    nested_test("()") do
                        named_adjoint_matrix = NamedArray(adjoint(mutable_matrix))
                        @test depict(named_adjoint_matrix) == "3 x 2 x Int64 in Rows (Named Adjoint Dense)"
                        @test !is_read_only_array(named_adjoint_matrix)

                        read_only_named_adjoint_matrix = SparseArrays.ReadOnly(named_adjoint_matrix)
                        @test depict(read_only_named_adjoint_matrix) ==
                              "3 x 2 x Int64 in Rows (ReadOnly Named Adjoint Dense)"
                        @test is_read_only_array(read_only_named_adjoint_matrix)
                        @test read_only_array(read_only_named_adjoint_matrix) === read_only_named_adjoint_matrix
                    end

                    nested_test("read_only") do
                        named_adjoint_read_only_matrix = NamedArray(adjoint(SparseArrays.ReadOnly(mutable_matrix)))
                        @test depict(named_adjoint_read_only_matrix) ==
                              "3 x 2 x Int64 in Rows (Named Adjoint ReadOnly Dense)"
                        @test is_read_only_array(named_adjoint_read_only_matrix)
                        @test read_only_array(named_adjoint_read_only_matrix) === named_adjoint_read_only_matrix
                    end
                end
            end
        end
    end

    nested_test("copy") do
        nested_test("vector") do
            mutable_vector = [0, 1]

            nested_test("dense") do
                @test !is_read_only_array(mutable_vector)
                @test depict(mutable_vector) == "2 x Int64 (Dense)"

                read_only_vector = read_only_array(mutable_vector)
                @test is_read_only_array(read_only_vector)
                @test depict(read_only_vector) == "2 x Int64 (ReadOnly Dense)"
                @test parent(read_only_vector) === mutable_vector

                copy_read_only_vector = copy_array(read_only_vector)
                @test !is_read_only_array(copy_read_only_vector)
                @test depict(copy_read_only_vector) == "2 x Int64 (Dense)"
                @test copy_read_only_vector !== mutable_vector
            end

            nested_test("sparse") do
                mutable_vector = SparseVector(mutable_vector)
                @test !is_read_only_array(mutable_vector)
                @test depict(mutable_vector) == "2 x Int64 (Sparse Int64 50%)"

                read_only_vector = read_only_array(mutable_vector)
                @test is_read_only_array(read_only_vector)
                @test depict(read_only_vector) == "2 x Int64 (ReadOnly Sparse Int64 50%)"
                @test parent(read_only_vector) === mutable_vector

                copy_read_only_vector = copy_array(read_only_vector)
                @test !is_read_only_array(copy_read_only_vector)
                @test depict(copy_read_only_vector) == "2 x Int64 (Sparse Int64 50%)"
                @test copy_read_only_vector !== mutable_vector
            end
        end

        nested_test("matrix") do
            mutable_matrix = [0 1 2; 0 3 0]

            nested_test("dense") do
                nested_test("()") do
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (Dense)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (ReadOnly Dense)"
                    @test parent(read_only_matrix) === mutable_matrix

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "2 x 3 x Int64 in Columns (Dense)"
                    @test copy_read_only_matrix !== mutable_matrix
                end

                nested_test("permuted_dims") do
                    nested_test("1,2") do
                        mutable_matrix = PermutedDimsArray(mutable_matrix, (1, 2))
                        @test !is_read_only_array(mutable_matrix)
                        @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (!Permuted Dense)"

                        read_only_matrix = read_only_array(mutable_matrix)
                        @test is_read_only_array(read_only_matrix)
                        @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (!Permuted ReadOnly Dense)"
                        @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                        copy_read_only_matrix = copy_array(read_only_matrix)
                        @test !is_read_only_array(copy_read_only_matrix)
                        @test depict(copy_read_only_matrix) == "2 x 3 x Int64 in Columns (!Permuted Dense)"
                        @test copy_read_only_matrix !== mutable_matrix
                    end

                    nested_test("2,1") do
                        mutable_matrix = PermutedDimsArray(mutable_matrix, (2, 1))
                        @test !is_read_only_array(mutable_matrix)
                        @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Permuted Dense)"

                        read_only_matrix = read_only_array(mutable_matrix)
                        @test is_read_only_array(read_only_matrix)
                        @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted ReadOnly Dense)"
                        @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                        copy_read_only_matrix = copy_array(read_only_matrix)
                        @test !is_read_only_array(copy_read_only_matrix)
                        @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted Dense)"
                        @test copy_read_only_matrix !== mutable_matrix
                    end
                end

                nested_test("transpose") do
                    mutable_matrix = transpose(mutable_matrix)
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Transpose Dense)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Transpose ReadOnly Dense)"
                    @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Transpose Dense)"
                    @test copy_read_only_matrix !== mutable_matrix
                end

                nested_test("adjoint") do
                    mutable_matrix = adjoint(mutable_matrix)
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Adjoint Dense)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Adjoint ReadOnly Dense)"
                    @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Adjoint Dense)"
                    @test copy_read_only_matrix !== mutable_matrix
                end
            end

            nested_test("sparse") do
                mutable_matrix = SparseMatrixCSC(mutable_matrix)

                nested_test("()") do
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (Sparse Int64 50%)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (ReadOnly Sparse Int64 50%)"
                    @test parent(read_only_matrix) === mutable_matrix

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "2 x 3 x Int64 in Columns (Sparse Int64 50%)"
                    @test copy_read_only_matrix !== mutable_matrix
                end

                nested_test("permuted_dims") do
                    nested_test("1,2") do
                        mutable_matrix = PermutedDimsArray(mutable_matrix, (1, 2))
                        @test !is_read_only_array(mutable_matrix)
                        @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (!Permuted Sparse Int64 50%)"

                        read_only_matrix = read_only_array(mutable_matrix)
                        @test is_read_only_array(read_only_matrix)
                        @test depict(read_only_matrix) ==
                              "2 x 3 x Int64 in Columns (!Permuted ReadOnly Sparse Int64 50%)"
                        @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                        copy_read_only_matrix = copy_array(read_only_matrix)
                        @test !is_read_only_array(copy_read_only_matrix)
                        @test depict(copy_read_only_matrix) == "2 x 3 x Int64 in Columns (!Permuted Sparse Int64 50%)"
                        @test copy_read_only_matrix !== mutable_matrix
                    end

                    nested_test("2,1") do
                        mutable_matrix = PermutedDimsArray(mutable_matrix, (2, 1))
                        @test !is_read_only_array(mutable_matrix)
                        @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Permuted Sparse Int64 50%)"

                        read_only_matrix = read_only_array(mutable_matrix)
                        @test is_read_only_array(read_only_matrix)
                        @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted ReadOnly Sparse Int64 50%)"
                        @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                        copy_read_only_matrix = copy_array(read_only_matrix)
                        @test !is_read_only_array(copy_read_only_matrix)
                        @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted Sparse Int64 50%)"
                        @test copy_read_only_matrix !== mutable_matrix
                    end
                end

                nested_test("transpose") do
                    mutable_matrix = transpose(mutable_matrix)
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Transpose Sparse Int64 50%)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Transpose ReadOnly Sparse Int64 50%)"
                    @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Transpose Sparse Int64 50%)"
                    @test copy_read_only_matrix !== mutable_matrix
                end

                nested_test("adjoint") do
                    mutable_matrix = adjoint(mutable_matrix)
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Adjoint Sparse Int64 50%)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Adjoint ReadOnly Sparse Int64 50%)"
                    @test parent(parent(read_only_matrix)) === parent(mutable_matrix)

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Adjoint Sparse Int64 50%)"
                    @test copy_read_only_matrix !== mutable_matrix
                end
            end

            nested_test("named") do
                mutable_matrix = NamedArray(mutable_matrix)

                nested_test("()") do
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (Named Dense)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (Named ReadOnly Dense)"
                    @test parent(read_only_matrix.array) === mutable_matrix.array

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "2 x 3 x Int64 in Columns (Named Dense)"
                    @test copy_read_only_matrix !== mutable_matrix
                end

                nested_test("permuted_dims") do
                    nested_test("1,2") do
                        mutable_matrix = PermutedDimsArray(mutable_matrix, (1, 2))
                        @test !is_read_only_array(mutable_matrix)
                        @test depict(mutable_matrix) == "2 x 3 x Int64 in Columns (!Permuted Named Dense)"

                        read_only_matrix = read_only_array(mutable_matrix)
                        @test is_read_only_array(read_only_matrix)
                        @test depict(read_only_matrix) == "2 x 3 x Int64 in Columns (!Permuted Named ReadOnly Dense)"
                        @test parent(parent(parent(read_only_matrix))) === parent(parent(mutable_matrix))

                        copy_read_only_matrix = copy_array(read_only_matrix)
                        @test !is_read_only_array(copy_read_only_matrix)
                        @test depict(copy_read_only_matrix) == "2 x 3 x Int64 in Columns (!Permuted Named Dense)"
                        @test copy_read_only_matrix !== mutable_matrix
                    end

                    nested_test("2,1") do
                        mutable_matrix = PermutedDimsArray(mutable_matrix, (2, 1))
                        @test !is_read_only_array(mutable_matrix)
                        @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Permuted Named Dense)"

                        read_only_matrix = read_only_array(mutable_matrix)
                        @test is_read_only_array(read_only_matrix)
                        @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted Named ReadOnly Dense)"
                        @test parent(parent(parent(read_only_matrix))) === parent(parent(mutable_matrix))

                        copy_read_only_matrix = copy_array(read_only_matrix)
                        @test !is_read_only_array(copy_read_only_matrix)
                        @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Permuted Named Dense)"
                        @test copy_read_only_matrix !== mutable_matrix
                    end
                end

                nested_test("transpose") do
                    mutable_matrix = transpose(mutable_matrix)
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Named Transpose Dense)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Named Transpose ReadOnly Dense)"
                    @test parent(parent(read_only_matrix.array)) === parent(mutable_matrix.array)

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Named Transpose Dense)"
                    @test copy_read_only_matrix !== mutable_matrix
                end

                nested_test("adjoint") do
                    mutable_matrix = adjoint(mutable_matrix)
                    @test !is_read_only_array(mutable_matrix)
                    @test depict(mutable_matrix) == "3 x 2 x Int64 in Rows (Named Adjoint Dense)"

                    read_only_matrix = read_only_array(mutable_matrix)
                    @test is_read_only_array(read_only_matrix)
                    @test depict(read_only_matrix) == "3 x 2 x Int64 in Rows (Named Adjoint ReadOnly Dense)"
                    @test parent(parent(read_only_matrix.array)) === parent(mutable_matrix.array)

                    copy_read_only_matrix = copy_array(read_only_matrix)
                    @test !is_read_only_array(copy_read_only_matrix)
                    @test depict(copy_read_only_matrix) == "3 x 2 x Int64 in Rows (Named Adjoint Dense)"
                    @test copy_read_only_matrix !== mutable_matrix
                end
            end
        end
    end
end
