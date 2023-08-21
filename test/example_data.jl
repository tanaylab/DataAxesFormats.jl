function test_description(
    daf::DafReader;
    cache::String = "",
    kind::String = "ReadOnly ",
    name::String = "example!",
)::Nothing
    if cache == ""
        suffix = "\n"
    else
        suffix = "\n" * cache * "\n"
    end

    @test description(daf) == dedent("""
        name: $(name)
        type: $(kind)MemoryDaf
        scalars:
          version: "1.0"
        axes:
          batch: 4 entries
          cell: 20 entries
          gene: 10 entries
          module: 3 entries
          type: 3 entries
        vectors:
          batch:
            age: 4 x Int8 (Dense)
            sex: 4 x String (Dense)
          cell:
            batch: 20 x String (Dense)
            batch.invalid: 20 x String (Dense)
            batch.partial: 20 x String (Dense)
            type: 20 x String (Dense)
          gene:
            lateral: 10 x Bool (Dense)
            marker: 10 x Bool (Dense)
            module: 10 x String (Dense)
            noisy: 10 x Bool (Dense)
          type:
            color: 3 x String (Dense)
        matrices:
          cell,gene:
            UMIs: 20 x 10 x Int16 (Dense in Columns)
          gene,cell:
            UMIs: 10 x 20 x Int16 (Dense in Columns)
    """) * suffix

    if cache != ""
        empty_cache!(daf)
        test_description(daf)
    end

    return nothing
end

nested_test("example_data") do
    daf = read_only(Daf.ExampleData.example_daf())
    @test read_only(daf) === daf

    nested_test("description") do
        nested_test("base") do
            nested_test("()") do
                return test_description(daf.daf; kind = "")
            end

            nested_test("view") do
                view = viewer(
                    "view!",
                    daf.daf;
                    scalars = [ALL_SCALARS],
                    axes = [ALL_AXES],
                    vectors = [ALL_VECTORS],
                    matrices = [ALL_MATRICES],
                )
                test_description(view; kind = "View ", name = "view!")
                return nothing
            end
        end

        nested_test("read_only") do
            nested_test("()") do
                return test_description(daf)
            end

            nested_test("view") do
                view = viewer(
                    "view!",
                    daf;
                    scalars = [ALL_SCALARS],
                    axes = [ALL_AXES],
                    vectors = [ALL_VECTORS],
                    matrices = [ALL_MATRICES],
                )
                test_description(view; kind = "View ", name = "view!")
                return nothing
            end
        end
    end

    nested_test("query") do
        nested_test("matrix") do
            nested_test("()") do
                @test matrix_query(daf, "cell , gene @ UMIs") == Int16[
                    1   23  9   5   50  4  13  12  12  2
                    20  3   2   16  17  6  3   4   2   22
                    5   2   2   3   4   29 14  0   2   15
                    8   14  13  0   12  5  1   34  26  6
                    7   6   4   11  5   57 1   0   3   1
                    9   6   3   13  8   16 7   21  3   10
                    4   26  1   20  4   11 1   0   2   17
                    2   62  18  19  20  9  15  30  23  14
                    4   19  56  6   8   12 12  1   2   8
                    1   27  19  46  2   8  8   8   2   12
                    3   5   1   6   16  0  19  4   5   0
                    5   3   1   1   10  3  7   2   5   38
                    13  1   16  10  4   3  6   21  3   1
                    3   29  8   52  17  1  5   2   6   20
                    5   7   0   0   2   11 0   11  21  4
                    4   1   3   10  21  15 4   1   5   4
                    4   13  11  35  18  5  12  13  4   7
                    3   11  4   6   2   1  4   14  9   13
                    13  2   9   5   1   28 9   10  0   14
                    12  9   14  43  11  5  2   32  12  4
                ]

                test_description(daf; cache = dedent("""
                      cache:
                        cell, gene @ UMIs: 20 x 10 x Int16 (Dense in Columns)
                  """))

                return nothing
            end

            nested_test("eltwise") do
                nested_test("single") do
                    @test matrix_query(daf, "cell , gene @ UMIs % Abs") == Int16[
                        1   23  9   5   50  4  13  12  12  2
                        20  3   2   16  17  6  3   4   2   22
                        5   2   2   3   4   29 14  0   2   15
                        8   14  13  0   12  5  1   34  26  6
                        7   6   4   11  5   57 1   0   3   1
                        9   6   3   13  8   16 7   21  3   10
                        4   26  1   20  4   11 1   0   2   17
                        2   62  18  19  20  9  15  30  23  14
                        4   19  56  6   8   12 12  1   2   8
                        1   27  19  46  2   8  8   8   2   12
                        3   5   1   6   16  0  19  4   5   0
                        5   3   1   1   10  3  7   2   5   38
                        13  1   16  10  4   3  6   21  3   1
                        3   29  8   52  17  1  5   2   6   20
                        5   7   0   0   2   11 0   11  21  4
                        4   1   3   10  21  15 4   1   5   4
                        4   13  11  35  18  5  12  13  4   7
                        3   11  4   6   2   1  4   14  9   13
                        13  2   9   5   1   28 9   10  0   14
                        12  9   14  43  11  5  2   32  12  4
                    ]

                    test_description(daf; cache = dedent("""
                                 cache:
                                   cell, gene @ UMIs % Abs: 20 x 10 x Int16 (Dense in Columns)
                             """))
                    return nothing
                end

                nested_test("dtype") do
                    @test matrix_query(daf, "cell , gene @ UMIs % Round; dtype = Int8") == Int8[
                        1   23  9   5   50  4  13  12  12  2
                        20  3   2   16  17  6  3   4   2   22
                        5   2   2   3   4   29 14  0   2   15
                        8   14  13  0   12  5  1   34  26  6
                        7   6   4   11  5   57 1   0   3   1
                        9   6   3   13  8   16 7   21  3   10
                        4   26  1   20  4   11 1   0   2   17
                        2   62  18  19  20  9  15  30  23  14
                        4   19  56  6   8   12 12  1   2   8
                        1   27  19  46  2   8  8   8   2   12
                        3   5   1   6   16  0  19  4   5   0
                        5   3   1   1   10  3  7   2   5   38
                        13  1   16  10  4   3  6   21  3   1
                        3   29  8   52  17  1  5   2   6   20
                        5   7   0   0   2   11 0   11  21  4
                        4   1   3   10  21  15 4   1   5   4
                        4   13  11  35  18  5  12  13  4   7
                        3   11  4   6   2   1  4   14  9   13
                        13  2   9   5   1   28 9   10  0   14
                        12  9   14  43  11  5  2   32  12  4
                    ]

                    test_description(daf; cache = dedent("""
                                 cache:
                                   cell, gene @ UMIs % Round; dtype = Int8: 20 x 10 x Int8 (Dense in Columns)
                             """))
                    return nothing
                end

                nested_test("multiple") do
                    @test matrix_query(daf, "cell , gene @ UMIs % Abs % Log; base = 2, eps = 1") == Float32[
                        1.0         4.5849624   3.321928   2.5849626  5.6724253  2.321928   3.807355   3.7004397  3.7004397  1.5849625
                        4.392318    2.0         1.5849625  4.087463   4.169925   2.807355   2.0        2.321928   1.5849625  4.523562
                        2.5849626   1.5849625   1.5849625  2.0        2.321928   4.906891   3.9068906  0.0        1.5849625  4.0
                        3.169925    3.9068906   3.807355   0.0        3.7004397  2.5849626  1.0        5.129283   4.7548876  2.807355
                        3.0         2.807355    2.321928   3.5849626  2.5849626  5.8579807  1.0        0.0        2.0        1.0
                        3.321928    2.807355    2.0        3.807355   3.169925   4.087463   3.0        4.4594316  2.0        3.4594316
                        2.321928    4.7548876   1.0        4.392318   2.321928   3.5849626  1.0        0.0        1.5849625  4.169925
                        1.5849625   5.9772797   4.2479277  4.321928   4.392318   3.321928   4.0        4.954196   4.5849624  3.9068906
                        2.321928    4.321928    5.83289    2.807355   3.169925   3.7004397  3.7004397  1.0        1.5849625  3.169925
                        1.0         4.807355    4.321928   5.554589   1.5849625  3.169925   3.169925   3.169925   1.5849625  3.7004397
                        2.0         2.5849626   1.0        2.807355   4.087463   0.0        4.321928   2.321928   2.5849626  0.0
                        2.5849626   2.0         1.0        1.0        3.4594316  2.0        3.0        1.5849625  2.5849626  5.2854023
                        3.807355    1.0         4.087463   3.4594316  2.321928   2.0        2.807355   4.4594316  2.0        1.0
                        2.0         4.906891    3.169925   5.7279205  4.169925   1.0        2.5849626  1.5849625  2.807355   4.392318
                        2.5849626   3.0         0.0        0.0        1.5849625  3.5849626  0.0        3.5849626  4.4594316  2.321928
                        2.321928    1.0         2.0        3.4594316  4.4594316  4.0        2.321928   1.0        2.5849626  2.321928
                        2.321928    3.807355    3.5849626  5.169925   4.2479277  2.5849626  3.7004397  3.807355   2.321928   3.0
                        2.0         3.5849626   2.321928   2.807355   1.5849625  1.0        2.321928   3.9068906  3.321928   3.807355
                        3.807355    1.5849625   3.321928   2.5849626  1.0        4.8579807  3.321928   3.4594316  0.0        3.9068906
                        3.7004397   3.321928    3.9068906  5.4594316  3.5849626  2.5849626  1.5849625  5.044394   3.7004397  2.321928
                    ]

                    test_description(daf; cache = dedent("""
                          cache:
                            cell, gene @ UMIs % Abs % Log; base = 2.0, eps = 1.0: 20 x 10 x Float32 (Dense in Columns)
                      """))
                    return nothing
                end
            end

            nested_test("mask") do
                nested_test("comparison") do
                    nested_test("()") do
                        @test matrix_query(daf, "cell & batch = B1, gene & module = M1 @ UMIs") == Int16[
                            3  2
                            2  2
                            6  3
                            5  5
                            1  5
                            9  12
                        ]

                        test_description(daf; cache = dedent("""
                                     cache:
                                       cell & batch = B1, gene & module = M1 @ UMIs: 6 x 2 x Int16 (Dense in Columns)
                                 """))
                        return nothing
                    end

                    nested_test("!value") do
                        @test_throws dedent("""
                            invalid eltype value: "Q"
                            for the axis lookup: batch : age > Q
                            for the axis: cell
                            in the daf data: example!
                        """) matrix_query(daf, "cell & batch : age > Q, gene @ UMIs")
                    end
                end
            end

            nested_test("chained") do
                nested_test("()") do
                    @test matrix_query(daf, "cell & batch : age < 3, gene & ~noisy & ~lateral @ UMIs") == Int16[
                        4   12
                        57  0
                        11  0
                        9   30
                        12  1
                        8   8
                        3   2
                        3   21
                        1   2
                        11  11
                        5   13
                        1   14
                    ]
                end

                nested_test("!name") do
                    @test_throws dedent("""
                      invalid value: I1
                      of the chained: batch.invalid
                      of the axis: cell
                      is missing from the next axis: batch
                      in the daf data: example!
                    """) matrix_query(daf, "cell & batch.invalid : age > 1, gene @ UMIs")
                end

                nested_test("!string") do
                    @test_throws dedent("""
                        non-String data type: Bool
                        for the chained: marker
                        for the axis: gene
                        in the daf data: example!
                    """) matrix_query(daf, "cell, gene & marker : noisy @ UMIs")
                end
            end

            nested_test("match") do
                nested_test("()") do
                    @test matrix_query(daf, "cell & batch = B1, gene & module ~ .1 @ UMIs") == Int16[
                        3  2
                        2  2
                        6  3
                        5  5
                        1  5
                        9  12
                    ]
                end

                nested_test("!string") do
                    @test_throws dedent("""
                        non-String data type: Int8
                        for the match axis lookup: batch : age ~ .
                        for the axis: cell
                        in the daf data: example!
                    """) matrix_query(daf, "cell & batch : age ~ ., gene @ UMIs")
                end

                nested_test("!regex") do
                    @test_throws dedent("""
                        invalid Regex: "["
                        for the axis lookup: batch ~ \\[
                        for the axis: cell
                        in the daf data: example!
                    """) matrix_query(daf, "cell & batch ~ \\[, gene @ UMIs")
                end

                nested_test("names") do
                    @test names(matrix_query(daf, "cell & batch = B1, gene & module ~ .1 @ UMIs"), 1) ==
                          ["C2", "C3", "C6", "C11", "C16", "C20"]
                    @test names(matrix_query(daf, "cell & batch = B1, gene & module ~ .1 @ UMIs"), 2) ==
                          ["FOXA1", "ITGA4"]
                end

                nested_test("empty") do
                    @test matrix_query(daf, "cell, gene & module ~ Q. @ UMIs") == nothing
                end

                nested_test("special") do
                    @test matrix_query(daf, "cell & batch ~ \\\\\\[, gene @ UMIs") == nothing
                end
            end
        end

        nested_test("vector") do
            nested_test("()") do
                @test vector_query(daf, "batch @ age") == Int8[3, 2, 2, 4]
            end

            nested_test("chained") do
                nested_test("valid") do
                    @test vector_query(daf, "cell @ batch : age") ==
                          Int8[2, 3, 3, 4, 2, 3, 2, 2, 2, 2, 3, 2, 2, 2, 2, 3, 2, 2, 4, 3]
                end

                nested_test("invalid") do
                    @test_throws dedent("""
                        invalid value: I1
                        of the chained: batch.invalid
                        of the axis: cell
                        is missing from the next axis: batch
                        in the daf data: example!
                    """) vector_query(daf, "cell @ batch.invalid : age")
                end

                nested_test("partial") do
                    @test_throws dedent("""
                        invalid value: 
                        of the chained: batch.partial
                        of the axis: cell
                        is missing from the next axis: batch
                        in the daf data: example!
                    """) vector_query(daf, "cell @ batch.partial : age")
                end

                nested_test("default") do
                    @test vector_query(daf, "cell @ batch.partial : age ? -1") ==
                          Int8[4, 4, -1, 2, 2, 2, 2, 2, 4, 4, 4, 4, 2, 2, 2, 4, -1, 4, 4, 4]
                end
            end

            nested_test("comparison") do
                @test vector_query(daf, "batch & age < 0 @ age") == nothing
            end

            nested_test("masked") do
                @test vector_query(daf, "cell & batch.partial @ batch.partial : age") ==
                      Int8[4, 4, 2, 2, 2, 2, 2, 4, 4, 4, 4, 2, 2, 2, 4, 4, 4, 4]
            end

            nested_test("slice") do
                nested_test("()") do
                    @test vector_query(daf, "cell, gene = FOXA1 @ UMIs % Abs") ==
                          Int16[23, 3, 2, 14, 6, 6, 26, 62, 19, 27, 5, 3, 1, 29, 7, 1, 13, 11, 2, 9]
                end

                nested_test("comparison") do
                    @test vector_query(daf, "cell & batch : age > 2, gene = FOXA1 @ UMIs") ==
                          Int16[3, 2, 14, 6, 5, 1, 2, 9]
                end

                nested_test("!name") do
                    @test_throws dedent("""
                        the entry: DOGB2
                        is missing from the axis: gene
                        in the daf data: example!
                    """) vector_query(daf, "cell, gene = DOGB2 @ UMIs")
                end
            end

            nested_test("mask") do
                nested_test("()") do
                    @test vector_query(daf, "gene & marker @ module") == ["M3", "M3", "M1", "M3"]
                end

                nested_test("names") do
                    @test names(vector_query(daf, "gene & marker @ module"), 1) == ["WNT6", "SFRP5", "ITGA4", "FOXA2"]
                end
            end

            nested_test("reduction") do
                @test vector_query(daf, "cell, gene @ UMIs %> Sum") ==
                      Int16[126, 269, 194, 307, 232, 229, 143, 220, 147, 212]
            end

            nested_test("match") do
                @test vector_query(daf, "cell, gene & module ~ Q. @ UMIs %> Sum") == nothing
            end
        end

        nested_test("scalar") do
            nested_test("()") do
                @test scalar_query(daf, "version") == "1.0"
            end

            nested_test("eltwise") do
                nested_test("!type") do
                    @test_throws dedent("""
                      non-numeric input: String
                      for the eltwise operation: Abs
                    """) scalar_query(daf, "version % Abs")
                end
            end

            nested_test("slice") do
                nested_test("vector") do
                    @test scalar_query(daf, "gene = FOXA1 @ module") == "M1"
                end

                nested_test("matrix") do
                    @test scalar_query(daf, "cell = C4, gene = FOXA1 @ UMIs") == 14
                end
            end

            nested_test("reduction") do
                nested_test("vector") do
                    nested_test("mask") do
                        @test scalar_query(daf, "batch & age < 0 @ age %> Sum") == nothing
                    end

                    nested_test("eltwise") do
                        @test scalar_query(daf, "batch @ age %> Sum % Abs") == 11
                    end

                    nested_test("!type") do
                        @test_throws dedent("""
                          non-numeric input: Vector{String}
                          for the reduction operation: Sum
                        """) scalar_query(daf, "gene @ module %> Sum")
                    end
                end

                nested_test("slice") do
                    @test scalar_query(daf, "cell & batch : age > 2, gene = FOXA1 @ UMIs %> Sum") == 42
                end

                nested_test("matrix") do
                    @test scalar_query(daf, "cell, gene @ UMIs %> Sum %> Sum") == 2079
                end
            end
        end
    end
end
