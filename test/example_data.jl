nested_test("example_data") do
    daf = example_daf()
    @test description(daf) == """
                              name: example!
                              type: MemoryDaf
                              axes:
                                cell: 856 entries
                                donor: 95 entries
                                experiment: 23 entries
                                gene: 683 entries
                                metacell: 7 entries
                                type: 4 entries
                              vectors:
                                cell:
                                  donor: 856 x Str (Dense)
                                  experiment: 856 x Str (Dense)
                                  metacell: 856 x Str (Dense)
                                donor:
                                  age: 95 x UInt32 (Dense)
                                  sex: 95 x Str (Dense)
                                gene:
                                  is_lateral: 683 x Bool (Dense; 64% true)
                                  is_marker: 683 x Bool (Dense; 95% true)
                                metacell:
                                  type: 7 x Str (Dense)
                                type:
                                  color: 4 x Str (Dense)
                              matrices:
                                cell,gene:
                                  UMIs: 856 x 683 x UInt8 in Columns (Dense)
                                gene,cell:
                                  UMIs: 683 x 856 x UInt8 in Columns (Dense)
                                gene,metacell:
                                  fraction: 683 x 7 x Float32 in Columns (Dense)
                              """
end
