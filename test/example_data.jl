test_set("example_data") do
    container = Daf.ExampleData.example_container()

    test_set("description") do
        @test description(container) == dedent("""
            type: MemoryContainer
            name: example!
            is_read_only: False
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
        """) * "\n"
    end

    test_set("matrix queries") do
        matrix_query(container, "cell , gene @ UMIs") == Int16[
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

        matrix_query(container, "cell , gene @ UMIs % Abs") == Int16[
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

        matrix_query(container, "cell , gene @ UMIs % Abs; dtype = Int8") == Int8[
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

        matrix_query(container, "cell , gene @ UMIs % Abs % Log; base = 2, eps = 1") == Float32[
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

        matrix_query(container, "cell , gene @ UMIs % Abs % Log; dtype = Float64, base = 2, eps = 1") == Float64[
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

        @test matrix_query(container, "cell & batch = B1, gene & module = M1 @ UMIs") == Int16[
            3  2
            2  2
            6  3
            5  5
            1  5
            9  12
        ]

        @test matrix_query(container, "cell & batch : age < 3, gene & ~noisy & ~lateral @ UMIs") == Int16[
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

        @test matrix_query(container, "cell & batch = B1, gene & module ~ .1 @ UMIs") == Int16[
            3  2
            2  2
            6  3
            5  5
            1  5
            9  12
        ]

        @test names(matrix_query(container, "cell & batch = B1, gene & module ~ .1 @ UMIs"), 1) ==
              ["C2", "C3", "C6", "C11", "C16", "C20"]

        @test names(matrix_query(container, "cell & batch = B1, gene & module ~ .1 @ UMIs"), 2) == ["FOXA1", "ITGA4"]

        @test matrix_query(container, "cell, gene & module ~ Q. @ UMIs") == nothing

        @test_throws dedent("""
          invalid value: I1
          of the chained property: batch.invalid
          of the axis: cell
          is missing from the next axis: batch
          in the Daf.Container: example!
        """) matrix_query(container, "cell & batch.invalid : age > 1, gene @ UMIs")

        @test_throws dedent("""
            non-Bool data type: Int8
            for the axis filter: & batch : age
            in the Daf.Container: example!
        """) matrix_query(container, "cell & batch : age, gene @ UMIs")

        @test_throws dedent("""
            non-String data type: Bool
            for the chained property: marker
            for the axis: gene
            in the Daf.Container: example!
        """) matrix_query(container, "cell, gene & marker : noisy @ UMIs")

        @test_throws dedent("""
            invalid eltype value: "Q"
            for the axis lookup: batch : age > Q
            for the axis: cell
            in the Daf.Container: example!
        """) matrix_query(container, "cell & batch : age > Q, gene @ UMIs")

        @test_throws dedent("""
            invalid Regex: "["
            for the axis lookup: batch ~ \\[
            for the axis: cell
            in the Daf.Container: example!
        """) matrix_query(container, "cell & batch ~ \\[, gene @ UMIs")

        matrix_query(container, "cell & batch ~ \\\\\\[, gene @ UMIs") == nothing

        @test_throws dedent("""
            non-String data type: Int8
            for the match axis lookup: batch : age ~ .
            for the axis: cell
            in the Daf.Container: example!
        """) matrix_query(container, "cell & batch : age ~ ., gene @ UMIs")

        return nothing
    end

    test_set("vector queries") do
        @test vector_query(container, "batch @ age") == Int8[3, 2, 2, 4]

        @test vector_query(container, "batch & age < 0 @ age") == nothing

        @test vector_query(container, "cell, gene = FOXA1 @ UMIs % Abs") ==
              Int16[23, 3, 2, 14, 6, 6, 26, 62, 19, 27, 5, 3, 1, 29, 7, 1, 13, 11, 2, 9]

        @test vector_query(container, "cell & batch : age > 2, gene = FOXA1 @ UMIs") == Int16[3, 2, 14, 6, 5, 1, 2, 9]

        @test vector_query(container, "gene & marker @ module") == ["M3", "M3", "M1", "M3"]
        @test names(vector_query(container, "gene & marker @ module"), 1) == ["WNT6", "SFRP5", "ITGA4", "FOXA2"]

        @test vector_query(container, "cell, gene @ UMIs %> Sum") ==
              Int16[126, 269, 194, 307, 232, 229, 143, 220, 147, 212]

        @test_throws dedent("""
            the entry: DOGB2
            is missing from the axis: gene
            in the Daf.Container: example!
        """) vector_query(container, "cell, gene = DOGB2 @ UMIs")

        @test vector_query(container, "cell, gene & module ~ Q. @ UMIs %> Sum") == nothing

        return nothing
    end

    test_set("scalar queries") do
        @test scalar_query(container, "version") == "1.0"

        @test_throws dedent("""
          non-numeric input: String
          for the eltwise operation: Abs; dtype = auto
        """) scalar_query(container, "version % Abs")

        @test_throws dedent("""
          non-numeric input: Vector{String}
          for the reduction operation: Sum; dtype = auto
        """) scalar_query(container, "gene @ module %> Sum")

        scalar_query(container, "gene = FOXA1 @ module") == "M1"

        @test scalar_query(container, "batch @ age %> Sum % Abs") == 11

        @test scalar_query(container, "cell & batch : age > 2, gene = FOXA1 @ UMIs %> Sum") == 42

        @test scalar_query(container, "cell, gene @ UMIs %> Sum %> Sum") == 2079

        @test scalar_query(container, "cell = C4, gene = FOXA1 @ UMIs") == 14

        @test scalar_query(container, "batch & age < 0 @ age %> Sum") == nothing
    end
end
