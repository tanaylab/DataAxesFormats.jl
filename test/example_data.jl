function test_description(
    daf::DafReader;
    cache::String = "",
    kind::String = "ReadOnly",
    name::String = "example!",
)::Nothing
    if kind == ""
        header = """
            name: $(name)
            type: MemoryDaf
            """
    else
        header = """
            name: $(name)
            type: $(kind)
            base: MemoryDaf example!
            """
    end
    prefix = header * """
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
            sex: 4 x Str (Dense)
          cell:
            batch: 20 x Str (Dense)
            batch.invalid: 20 x Str (Dense)
            batch.partial: 20 x Str (Dense)
            type: 20 x Str (Dense)
          gene:
            lateral: 10 x Bool (Dense; 50% true)
            marker: 10 x Bool (Dense; 40% true)
            module: 10 x Str (Dense)
            noisy: 10 x Bool (Dense; 50% true)
          type:
            color: 3 x Str (Dense)
        matrices:
          cell,gene:
            UMIs: 20 x 10 x Int16 in Columns (Dense)
          gene,cell:
            UMIs: 10 x 20 x Int16 in Columns (Dense)
        """
    @test description(daf) == prefix

    if cache != ""
        @test description(daf; cache = true) == prefix * cache
    end

    return nothing
end

nested_test("example_data") do
    daf = read_only(DataAxesFormats.ExampleData.example_daf())
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
                test_description(view; kind = "View", name = "view!")
                return nothing
            end
        end

        nested_test("read_only") do
            nested_test("()") do
                return test_description(daf; name = "example!.read_only")
            end

            nested_test("view") do
                view = viewer(daf; name = "view!", axes = [VIEW_ALL_AXES], data = VIEW_ALL_DATA)
                test_description(view; kind = "View", name = "view!")
                return nothing
            end
        end
    end

    nested_test("query") do
        nested_test("matrix") do
            nested_test("()") do
                @test get_query(daf, q"/ cell / gene : UMIs") == Int16[
                    4   4   8   31  6   8   3   16  18  16
                    18  33  1   16  18  18  19  15  2   14
                    1   22  7   5   1   1   2   11  0   5
                    1   1   3   23  47  0   4   7   10  9
                    11  1   11  5   4   16  22  10  5   5
                    17  0   7   17  6   2   5   13  6   8
                    6   28  13  3   33  3   4   19  9   9
                    9   22  1   16  27  1   14  18  24  16
                    15  12  1   72  28  5   12  7   5   12
                    22  1   1   2   2   7   9   33  6   13
                    12  12  11  1   2   7   3   9   5   9
                    21  2   1   7   25  36  15  4   1   20
                    19  5   12  37  9   11  31  15  17  13
                    8   18  4   4   14  6   21  3   14  9
                    23  7   5   8   3   25  3   2   11  7
                    9   7   17  12  4   10  3   2   2   2
                    11  11  6   10  5   15  36  4   2   24
                    10  9   2   12  3   3   16  17  2   4
                    2   4   20  1   6   23  4   15  16  19
                    18  8   20  3   8   6   1   18  2   1
                ]

                test_description(daf; name = "example!.read_only", cache = """
                    cache:
                      'axis_dict[axis: batch]': (MemoryData) 4 x Str => Int64 (OrderedDict)
                      'axis_dict[axis: cell]': (MemoryData) 20 x Str => Int64 (OrderedDict)
                      'axis_dict[axis: gene]': (MemoryData) 10 x Str => Int64 (OrderedDict)
                      'axis_dict[axis: type]': (MemoryData) 3 x Str => Int64 (OrderedDict)
                      'query[/ cell / gene : UMIs]': (QueryData) 20 x 10 x Int16 in Columns (Dense)
                      'names[axes]': (MemoryData) 5 x Str (KeySet)
                      'vectors[axis: batch]': (MemoryData) 2 x Str (KeySet)
                      'vectors[axis: cell]': (MemoryData) 4 x Str (KeySet)
                      'vectors[axis: gene]': (MemoryData) 4 x Str (KeySet)
                      'vectors[axis: module]': (MemoryData) 0 x Str (KeySet)
                      'names[scalars]': (MemoryData) 1 x Str (KeySet)
                      'vectors[axis: type]': (MemoryData) 1 x Str (KeySet)
                    """)

                return nothing
            end

            nested_test("eltwise") do
                @test get_query(daf, q"/ cell / gene : UMIs % Abs") == UInt16[
                    4   4   8   31  6   8   3   16  18  16
                    18  33  1   16  18  18  19  15   2  14
                    1   22  7   5   1   1   2   11   0  5
                    1   1   3   23  47  0   4   7   10  9
                    11  1   11  5   4   16  22  10  5   5
                    17  0   7   17  6   2   5   13  6   8
                    6   28  13  3   33  3   4   19  9   9
                    9   22  1   16  27  1   14  18  24  16
                    15  12  1   72  28  5   12  7   5   12
                    22  1   1   2   2   7   9   33  6   13
                    12  12  11  1   2   7   3   9   5   9
                    21  2   1   7   25  36  15  4   1   20
                    19  5   12  37  9   11  31  15  17  13
                    8   18  4   4   14  6   21  3   14  9
                    23  7   5   8   3   25  3   2   11  7
                    9   7   17  12  4   10  3   2   2   2
                    11  11  6   10  5   15  36  4   2   24
                    10  9   2   12  3   3   16  17  2   4
                    2   4   20  1   6   23  4   15  16  19
                    18  8   20  3   8   6   1   18  2   1
                ]

                test_description(daf; name = "example!.read_only", cache = """
                    cache:
                      'axis_dict[axis: batch]': (MemoryData) 4 x Str => Int64 (OrderedDict)
                      'axis_dict[axis: cell]': (MemoryData) 20 x Str => Int64 (OrderedDict)
                      'axis_dict[axis: gene]': (MemoryData) 10 x Str => Int64 (OrderedDict)
                      'axis_dict[axis: type]': (MemoryData) 3 x Str => Int64 (OrderedDict)
                      'query[/ cell / gene : UMIs % Abs]': (QueryData) 20 x 10 x UInt16 in Columns (Dense)
                      'names[axes]': (MemoryData) 5 x Str (KeySet)
                      'vectors[axis: batch]': (MemoryData) 2 x Str (KeySet)
                      'vectors[axis: cell]': (MemoryData) 4 x Str (KeySet)
                      'vectors[axis: gene]': (MemoryData) 4 x Str (KeySet)
                      'vectors[axis: module]': (MemoryData) 0 x Str (KeySet)
                      'names[scalars]': (MemoryData) 1 x Str (KeySet)
                      'vectors[axis: type]': (MemoryData) 1 x Str (KeySet)
                    """)
                return nothing
            end

            nested_test("mask") do
                @test get_query(daf, q"/ cell & batch = B1 / gene & module = M1 : UMIs") == Int16[
                    4  8  6  3  16
                    1  3  47 4  9
                    11 11 4  22 5
                    9  17 4  3  2
                    11 6  5  36 24
                    10 2  3  16 4
                ]

                test_description(
                    daf;
                    name = "example!.read_only",
                    cache = """
                            cache:
                              'axis_dict[axis: batch]': (MemoryData) 4 x Str => Int64 (OrderedDict)
                              'axis_dict[axis: cell]': (MemoryData) 20 x Str => Int64 (OrderedDict)
                              'axis_dict[axis: gene]': (MemoryData) 10 x Str => Int64 (OrderedDict)
                              'axis_dict[axis: type]': (MemoryData) 3 x Str => Int64 (OrderedDict)
                              'query[/ cell & batch = B1 / gene & module = M1 : UMIs]': (QueryData) 6 x 5 x Int16 in Columns (Dense)
                              'names[axes]': (MemoryData) 5 x Str (KeySet)
                              'vectors[axis: batch]': (MemoryData) 2 x Str (KeySet)
                              'vectors[axis: cell]': (MemoryData) 4 x Str (KeySet)
                              'vectors[axis: gene]': (MemoryData) 4 x Str (KeySet)
                              'vectors[axis: module]': (MemoryData) 0 x Str (KeySet)
                              'names[scalars]': (MemoryData) 1 x Str (KeySet)
                              'vectors[axis: type]': (MemoryData) 1 x Str (KeySet)
                            """,
                )
                return nothing
            end
        end
    end
end
