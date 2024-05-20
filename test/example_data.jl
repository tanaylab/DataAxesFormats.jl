function test_description(
    daf::DafReader;
    cache::String = "",
    kind::String = "ReadOnly ",
    name::String = "example!",
)::Nothing
    prefix = dedent("""
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
            lateral: 10 x Bool (Dense) (7 true, 70%)
            marker: 10 x Bool (Dense) (4 true, 40%)
            module: 10 x String (Dense)
            noisy: 10 x Bool (Dense) (4 true, 40%)
          type:
            color: 3 x String (Dense)
        matrices:
          cell,gene:
            UMIs: 20 x 10 x Int16 in Columns (Dense)
          gene,cell:
            UMIs: 10 x 20 x Int16 in Columns (Dense)
    """) * "\n"

    @test description(daf) == prefix

    if cache != ""
        @test description(daf; cache = true) == prefix * cache * "\n"
    end

    return nothing
end

nested_test("example_data") do
    daf = read_only(Daf.ExampleData.example_daf())
    @test read_only(daf) === daf
    renamed = read_only(daf; name = "new name!")
    @test renamed !== daf
    @test read_only(renamed) === renamed
    rerenamed = read_only(renamed; name = "newer name!")
    @test rerenamed !== renamed

    nested_test("description") do
        nested_test("base") do
            nested_test("()") do
                return test_description(daf.daf; kind = "")
            end

            nested_test("view") do
                view = viewer(daf.daf; name = "view!", axes = [VIEW_ALL_AXES], data = VIEW_ALL_DATA)
                test_description(view; kind = "View ", name = "view!")
                return nothing
            end
        end

        nested_test("read_only") do
            nested_test("()") do
                return test_description(daf)
            end

            nested_test("view") do
                view = viewer(daf; name = "view!", axes = [VIEW_ALL_AXES], data = VIEW_ALL_DATA)
                test_description(view; kind = "View ", name = "view!")
                return nothing
            end
        end
    end

    nested_test("query") do
        nested_test("matrix") do
            nested_test("()") do
                @test get_query(daf, q"/ cell / gene : UMIs") == Int16[
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
                        '# batch': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 4)
                        '# cell': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 20)
                        '# gene': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 10)
                        '# type': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 3)
                        '/ cell / gene : UMIs': (QueryData) 20 x 10 x Int16 in Columns (Dense)
                  """))

                return nothing
            end

            nested_test("eltwise") do
                @test get_query(daf, q"/ cell / gene : UMIs % Abs") == UInt16[
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
                      '# batch': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 4)
                      '# cell': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 20)
                      '# gene': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 10)
                      '# type': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 3)
                      '/ cell / gene : UMIs % Abs': (QueryData) 20 x 10 x UInt16 in Columns (Dense)
                """))
                return nothing
            end

            nested_test("mask") do
                @test get_query(daf, q"/ cell & batch = B1 / gene & module = M1 : UMIs") == Int16[
                    3  2
                    2  2
                    6  3
                    5  5
                    1  5
                    9  12
                ]

                test_description(daf; cache = dedent("""
                    cache:
                      '# batch': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 4)
                      '# cell': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 20)
                      '# gene': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 10)
                      '# type': (MemoryData) (OrderedCollections.OrderedDict{String, Int64} length: 3)
                      '/ cell & batch = B1 / gene & module = M1 : UMIs': (QueryData) 6 x 2 x Int16 in Columns (Dense)
                """))
                return nothing
            end
        end
    end
end
