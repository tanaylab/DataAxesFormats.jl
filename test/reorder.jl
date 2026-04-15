nested_test("reorder") do
    nested_test("is_leaf") do
        nested_test("types") do
            @test is_leaf(MemoryDaf)
            @test is_leaf(FilesDaf)
            @test is_leaf(H5df)
            @test !is_leaf(DafReader)
            @test !is_leaf(DafWriter)
        end

        nested_test("memory") do
            daf = MemoryDaf(; name = "memory!")
            @test is_leaf(daf)
        end

        nested_test("files") do
            mktempdir() do path
                daf = FilesDaf("$(path)/files", "w"; name = "files!")
                @test is_leaf(daf)
                return nothing
            end
        end

        nested_test("h5df") do
            mktempdir() do path
                daf = H5df("$(path)/test.h5df", "w"; name = "h5df!")
                @test is_leaf(daf)
                return nothing
            end
        end

        nested_test("wrappers") do
            first = MemoryDaf(; name = "first!")
            second = MemoryDaf(; name = "second!")
            add_axis!(first, "cell", ["A", "B"])
            set_vector!(first, "cell", "age", [1, 2])

            nested_test("read_only") do
                @test !is_leaf(read_only(first))
            end

            nested_test("view") do
                @test !is_leaf(viewer(first; name = "view!"))
            end

            nested_test("read_chain") do
                @test !is_leaf(chain_reader([first, second]; name = "read_chain!"))
            end

            nested_test("write_chain") do
                @test !is_leaf(chain_writer([first, second]; name = "write_chain!"))
            end

            nested_test("contract") do
                contract = Contract(; axes = ["cell" => (OptionalOutput, "cell")])
                @test !is_leaf(contractor("computation", contract, first; overwrite = true))
            end
        end
    end
end
