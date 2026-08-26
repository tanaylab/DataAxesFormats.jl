function populate_complete_cells!(daf::DafWriter)::Nothing
    add_axis!(daf, "cell", ["A", "B", "C"])
    add_axis!(daf, "gene", ["X", "Y"])
    set_vector!(daf, "cell", "age", [10, 20, 30])
    return nothing
end

# The three repositories a diamond is made of: the cells, two repositories resting on them, and nothing yet resting on
# those. Returns them and the chains they were created as.
function populate_complete_diamond!(path::AbstractString)
    cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
    populate_complete_cells!(cells)

    results = FilesDaf("$(path)/results", "w"; name = "results!")
    results_chain = complete_chain!(; base_daf = cells, new_daf = results)
    set_vector!(results_chain, "cell", "score", [1.0, 2.0, 3.0])

    masks = FilesDaf("$(path)/masks", "w"; name = "masks!")
    masks_chain = complete_chain!(; base_daf = cells, new_daf = masks)
    set_vector!(masks_chain, "gene", "is_marker", [true, false])

    return (cells, results_chain, masks_chain)
end

nested_test("complete") do
    nested_test("record") do
        nested_test("single") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                # The common case is stored as the path itself, not as JSON, and is relative to the new repository.
                @test get_scalar(metacells, "base_daf_repository") == "cells"
                return nothing
            end
        end

        nested_test("absolute") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells, absolute = true)

                @test get_scalar(metacells, "base_daf_repository") == abspath("$(path)/cells")
                return nothing
            end
        end

        nested_test("viewed") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = BaseDaf(; daf = cells, axes = [VIEW_ALL_AXES]), new_daf = metacells)

                json = JSON.parse(get_scalar(metacells, "base_daf_repository"))
                @test json == [Dict("path" => "cells", "axes" => [Dict("*" => "=")])]
                return nothing
            end
        end

        nested_test("several") do
            mktempdir() do path
                _, results_chain, masks_chain = populate_complete_diamond!(path)
                leaf = FilesDaf("$(path)/leaf", "w"; name = "leaf!")
                complete_chain!(; base_daf = [results_chain, masks_chain], new_daf = leaf)

                # Only the immediate bases are recorded; that both rest on the cells is recorded in them.
                @test JSON.parse(get_scalar(leaf, "base_daf_repository")) == ["results", "masks"]
                return nothing
            end
        end

        nested_test("duplicate") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain = complete_chain!(; base_daf = [cells, cells], new_daf = metacells)

                # The same base twice is the same data twice, so it is recorded and chained once - and what is left is
                # a lone unviewed base, so it is stored as its path.
                @test get_scalar(metacells, "base_daf_repository") == "cells"
                @test [daf.name for daf in chain.dafs] == ["cells!", "metacells!"]
                return nothing
            end
        end

        nested_test("different_views") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain = complete_chain!(;
                    base_daf = [
                        BaseDaf(; daf = cells, axes = ["cell" => "="]),
                        BaseDaf(; daf = cells, axes = ["gene" => "="]),
                    ],
                    new_daf = metacells,
                )

                # Two views of one repository expose different data, so they are two bases rather than a duplicate.
                @test length(JSON.parse(get_scalar(metacells, "base_daf_repository"))) == 2
                @test length(chain.dafs) == 3
                @test axes_set(chain) == Set(["cell", "gene"])
                return nothing
            end
        end
    end

    nested_test("reopen") do
        nested_test("single") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain = complete_chain!(; base_daf = cells, new_daf = metacells)
                set_vector!(chain, "cell", "metacell", ["M1", "M1", "M2"])

                reopened = complete_daf("$(path)/metacells"; name = "reopened!")
                @test get_vector(reopened, "cell", "age") == [10, 20, 30]
                @test get_vector(reopened, "cell", "metacell") == ["M1", "M1", "M2"]
                return nothing
            end
        end

        nested_test("viewed") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(;
                    base_daf = BaseDaf(; daf = cells, axes = [VIEW_ALL_AXES], data = [("cell", "years") => "age"]),
                    new_daf = metacells,
                )

                # Reopening applies the recorded view, so only what it exposes is there, under the name it gives it.
                reopened = complete_daf("$(path)/metacells"; name = "reopened!")
                @test get_vector(reopened, "cell", "years") == [10, 20, 30]
                @test !has_vector(reopened, "cell", "age")
                return nothing
            end
        end

        nested_test("axes_only") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = BaseDaf(; daf = cells, axes = ["cell" => "="]), new_daf = metacells)

                # Restricting which axes are exposed says nothing about the data, and the data of an axis which is not
                # exposed goes with it.
                reopened = complete_daf("$(path)/metacells")
                @test axes_set(reopened) == Set(["cell"])
                @test !has_axis(reopened, "gene")
                return nothing
            end
        end

        nested_test("object") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)

                # A repository resting on a view of a single base may say so as one object rather than as an array of
                # one, which is what someone writing the property by hand would do.
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                set_scalar!(metacells, "base_daf_repository", """{"path": "cells", "axes": [{"cell": "="}]}""")

                reopened = complete_daf("$(path)/metacells"; name = "reopened!")
                @test axes_set(reopened) == Set(["cell"])
                @test !has_axis(reopened, "gene")
                return nothing
            end
        end

        nested_test("diamond") do
            mktempdir() do path
                _, results_chain, masks_chain = populate_complete_diamond!(path)
                leaf = FilesDaf("$(path)/leaf", "w"; name = "leaf!")
                complete_chain!(; base_daf = [results_chain, masks_chain], new_daf = leaf)

                reopened = complete_daf("$(path)/leaf"; name = "reopened!")
                @test get_vector(reopened, "cell", "age") == [10, 20, 30]
                @test get_vector(reopened, "cell", "score") == [1.0, 2.0, 3.0]
                @test get_vector(reopened, "gene", "is_marker") == [true, false]
                return nothing
            end
        end

        nested_test("write") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                # Only the leaf is opened for writing, so what is written lands in it rather than in the cells.
                reopened = complete_daf("$(path)/metacells", "r+"; name = "reopened!")
                set_vector!(reopened, "cell", "metacell", ["M1", "M1", "M2"])
                @test !has_vector(cells, "cell", "metacell")
                @test get_vector(FilesDaf("$(path)/metacells"; name = "alone!"), "cell", "metacell") ==
                      ["M1", "M1", "M2"]
                return nothing
            end
        end
    end

    nested_test("chain") do
        nested_test("nested") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                metacells_chain = complete_chain!(; base_daf = cells, new_daf = metacells)

                # A chain of chains is one long chain, so chaining onto a chain does not nest it.
                blocks = FilesDaf("$(path)/blocks", "w"; name = "blocks!")
                chain = chain_writer(DafReader[metacells_chain, blocks]; name = "chain!")
                @test [daf.name for daf in chain.dafs] == ["cells!", "metacells!", "blocks!"]
                return nothing
            end
        end

        nested_test("order") do
            mktempdir() do path
                _, results_chain, masks_chain = populate_complete_diamond!(path)
                leaf = FilesDaf("$(path)/leaf", "w"; name = "leaf!")
                chain = complete_chain!(; base_daf = [results_chain, masks_chain], new_daf = leaf)

                # The cells are reached through both arms and appear once, before everything resting on them.
                @test [daf.name for daf in chain.dafs] == ["cells!", "results!", "masks!", "leaf!"]
                return nothing
            end
        end

        nested_test("memory") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)

                # A repository with no path is recognized by nothing, so two of them are never the same one.
                first = MemoryDaf(; name = "first!")
                second = MemoryDaf(; name = "second!")
                chain = chain_writer(DafReader[cells, first, second]; name = "chain!")
                @test [daf.name for daf in chain.dafs] == ["cells!", "first!", "second!"]
                return nothing
            end
        end
    end

    nested_test("path") do
        nested_test("single") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain = complete_chain!(; base_daf = cells, new_daf = metacells)

                @test complete_path(chain) == abspath("$(path)/metacells")
                return nothing
            end
        end

        nested_test("diamond") do
            mktempdir() do path
                _, results_chain, masks_chain = populate_complete_diamond!(path)
                leaf = FilesDaf("$(path)/leaf", "w"; name = "leaf!")
                chain = complete_chain!(; base_daf = [results_chain, masks_chain], new_daf = leaf)

                @test complete_path(chain) == abspath("$(path)/leaf")
                return nothing
            end
        end

        nested_test("extra") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                other = FilesDaf("$(path)/other", "w"; name = "other!")
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                # A repository the records do not lead to means reopening the leaf would not give this chain.
                chain = chain_writer(DafReader[cells, other, metacells]; name = "chain!")
                @test complete_path(chain) === nothing
                return nothing
            end
        end

        nested_test("missing") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                # A base the records name but which is not here means this is only part of the complete chain.
                chain = chain_writer(DafReader[metacells]; name = "chain!")
                @test complete_path(chain) === nothing
                return nothing
            end
        end

        nested_test("pathless") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                # A repository which is not persistent cannot be reopened, so neither can the chain holding it.
                chain = chain_writer(DafReader[MemoryDaf(; name = "memory!"), cells, metacells]; name = "chain!")
                @test complete_path(chain) === nothing
                return nothing
            end
        end
    end

    nested_test("open") do
        mktempdir() do path
            nested_test("files") do
                @test open_daf("$(path)/test", "w"; name = "files!") isa DafWriter
                return nothing
            end

            nested_test("zarr") do
                @test open_daf("$(path)/test.daf.zarr", "w"; name = "zarr!") isa DafWriter
                return nothing
            end

            nested_test("zip") do
                @test open_daf("$(path)/test.daf.zip", "w"; name = "zip!") isa DafWriter
                return nothing
            end

            nested_test("h5df") do
                @test open_daf("$(path)/test.h5df", "w+"; name = "h5df!") isa DafWriter
                return nothing
            end

            nested_test("http") do
                # The HTTP backend serves what is already there, so there is nothing to write to.
                @test_throws "the HTTP backend is read-only: https://example.com/daf" open_daf(
                    "https://example.com/daf",
                    "r+",
                )

                # Reaching the backend is all this dispatches; what is at the other end is its own business.
                @test_throws "for URL: http://localhost:1/daf.json" open_daf("http://localhost:1")
                return nothing
            end
        end
    end

    nested_test("cycle") do
        mktempdir() do path
            cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
            populate_complete_cells!(cells)

            @test_throws chomp("""
                          cyclic repository: $(abspath("$(path)/cells"))
                          is also a base of itself
                          in the chain: cycle!
                          """) chain_writer(DafReader[cells, cells]; name = "cycle!")
            return nothing
        end
    end
end
