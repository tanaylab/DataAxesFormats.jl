# The record of the bases of a stored repository, as stored.
function stored_record(daf::DafReader)::Maybe{AbstractString}
    return DataAxesFormats.Readers.stored_base_daf_links(daf)
end

# Whether a repository holds a scalar on disk, including one the scalar functions refuse.
function has_raw_scalar(daf::DafReader, name::AbstractString)::Bool
    return DataAxesFormats.Formats.with_data_read_lock(daf, "has_raw_scalar") do
        return DataAxesFormats.Formats.format_has_scalar(daf, name)
    end
end

# Write a scalar the scalar functions refuse, as a repository holds it on disk.
function write_raw_scalar!(daf::DafWriter, name::AbstractString, value::StorageScalar)::Nothing
    DataAxesFormats.Formats.with_data_write_lock(daf, "write_raw_scalar!") do
        DataAxesFormats.Formats.format_set_scalar!(daf, name, value)
        return nothing
    end
    empty_cache!(daf)
    return nothing
end

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

# Three repositories published separately: the UMIs, the cell metadata resting on them, and the metacells resting on
# both, with their own copy of the cell axis.
function populate_complete_published!(path::AbstractString)::Nothing
    umis = FilesDaf("$(path)/umis", "w"; name = "umis!")
    add_axis!(umis, "cell", ["A", "B", "C"])
    add_axis!(umis, "gene", ["X", "Y"])
    set_matrix!(umis, "cell", "gene", "UMIs", [1 2; 3 4; 5 6])

    metadata = FilesDaf("$(path)/metadata", "w"; name = "metadata!")
    metadata_chain = complete_chain!(; base_daf = umis, new_daf = metadata)
    set_vector!(metadata_chain, "cell", "batch", ["U", "V", "U"])

    metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
    add_axis!(metacells, "cell", ["A", "B", "C"])
    add_axis!(metacells, "metacell", ["M1", "M2"])
    set_vector!(metacells, "cell", "metacell", ["M1", "M1", "M2"])
    set_base_daf_links!(metacells, [umis, metadata])
    return nothing
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
                @test stored_record(metacells) == "cells"
                return nothing
            end
        end

        nested_test("absolute") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells, absolute = true)

                @test stored_record(metacells) == abspath("$(path)/cells")
                return nothing
            end
        end

        nested_test("viewed") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = BaseDaf(; daf = cells, axes = [VIEW_ALL_AXES]), new_daf = metacells)

                json = JSON.parse(stored_record(metacells))
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
                @test JSON.parse(stored_record(leaf)) == ["results", "masks"]
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
                @test stored_record(metacells) == "cells"
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
                @test length(JSON.parse(stored_record(metacells))) == 2
                @test length(chain.dafs) == 3
                @test axes_set(chain) == Set(["cell", "gene"])
                return nothing
            end
        end
    end

    nested_test("reopen") do
        nested_test("non_string_base") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                write_raw_scalar!(cells, "_base_daf_links", 1)
                @test_throws "non-string scalar: _base_daf_links" complete_daf("$(path)/cells")
                return nothing
            end
        end

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
                write_raw_scalar!(metacells, "_base_daf_links", """{"path": "cells", "axes": [{"cell": "="}]}""")

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

    nested_test("skip_missing") do
        nested_test("all") do
            mktempdir() do path
                populate_complete_published!(path)
                reopened = complete_daf("$(path)/metacells"; name = "reopened!", skip_missing = true)
                @test get_vector(reopened, "cell", "metacell") == ["M1", "M1", "M2"]
                @test get_vector(reopened, "cell", "batch") == ["U", "V", "U"]
                @test get_matrix(reopened, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6]

                # The UMIs are reached through both bases, and are chained once.
                chained_paths = [abspath("$(path)/$(name)") for name in ("umis", "metadata", "metacells")]
                @test [complete_path(daf) for daf in reopened.dafs] == chained_paths
                @test complete_path(reopened) == abspath("$(path)/metacells")
                return nothing
            end
        end

        nested_test("!umis") do
            mktempdir() do path
                populate_complete_published!(path)
                rm("$(path)/umis"; recursive = true)

                # The metadata records the UMIs too, so they are skipped twice.
                skipped_umis = (:info, r"skipped missing base: .*/umis")
                reopened = @test_logs skipped_umis skipped_umis complete_daf(
                    "$(path)/metacells";
                    name = "reopened!",
                    skip_missing = true,
                )
                @test get_vector(reopened, "cell", "batch") == ["U", "V", "U"]
                @test !has_axis(reopened, "gene")

                # Reopening without skipping would fail, so the chain has no path.
                @test complete_path(reopened) === nothing
                return nothing
            end
        end

        nested_test("!metadata") do
            mktempdir() do path
                populate_complete_published!(path)
                rm("$(path)/metadata"; recursive = true)

                reopened = @test_logs (:info, r"skipped missing base: .*/metadata") complete_daf(
                    "$(path)/metacells";
                    name = "reopened!",
                    skip_missing = true,
                )
                @test get_matrix(reopened, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6]
                @test !has_vector(reopened, "cell", "batch")
                return nothing
            end
        end

        nested_test("!both") do
            mktempdir() do path
                populate_complete_published!(path)
                rm("$(path)/umis"; recursive = true)
                rm("$(path)/metadata"; recursive = true)

                reopened = complete_daf("$(path)/metacells"; name = "reopened!", skip_missing = true)
                @test axes_set(reopened) == Set(["cell", "metacell"])
                @test get_vector(reopened, "cell", "metacell") == ["M1", "M1", "M2"]
                return nothing
            end
        end

        nested_test("hidden") do
            mktempdir() do path
                populate_complete_published!(path)
                set_base_daf_links!(FilesDaf("$(path)/metacells", "r+"), FilesDaf("$(path)/metadata"))
                rm("$(path)/metadata"; recursive = true)

                # The UMIs are recorded only inside the skipped metadata, so they are skipped with it.
                reopened = complete_daf("$(path)/metacells"; name = "reopened!", skip_missing = true)
                @test !has_axis(reopened, "gene")
                return nothing
            end
        end

        nested_test("!skip") do
            mktempdir() do path
                populate_complete_published!(path)
                rm("$(path)/umis"; recursive = true)
                @test_throws "not a daf directory: $(path)/umis" complete_daf("$(path)/metacells")
                return nothing
            end
        end

        nested_test("!daf") do
            mktempdir() do path
                populate_complete_published!(path)
                rm("$(path)/umis"; recursive = true)
                mkdir("$(path)/umis")

                # A base that exists but fails to open isn't missing.
                @test_throws "not a daf directory: $(path)/umis" complete_daf("$(path)/metacells"; skip_missing = true)
                return nothing
            end
        end

        nested_test("group") do
            mktempdir() do path
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                add_axis!(metacells, "cell", ["A", "B", "C"])
                write_raw_scalar!(metacells, "_base_daf_links", "umis.h5dfs#/umis")

                reopened = @test_logs (:info, r"skipped missing base: .*/umis.h5dfs#/umis") complete_daf(
                    "$(path)/metacells";
                    name = "reopened!",
                    skip_missing = true,
                )
                @test axes_set(reopened) == Set(["cell"])
                return nothing
            end
        end

        nested_test("!group") do
            mktempdir() do path
                H5df("$(path)/umis.h5dfs#/other", "w"; name = "other!")
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                write_raw_scalar!(metacells, "_base_daf_links", "umis.h5dfs#/umis")

                # Only the file counts, so a missing group inside an existing file isn't missing.
                @test_throws "umis" complete_daf("$(path)/metacells"; skip_missing = true)
                return nothing
            end
        end

        nested_test("http") do
            mktempdir() do path
                populate_complete_published!(path)
                rm("$(path)/umis"; recursive = true)

                # An HTTP base is never missing, so it fails to open.
                with_served_directory(path) do url
                    @test_throws "$(url)/umis" complete_daf("$(url)/metacells"; skip_missing = true)
                    return nothing
                end
                return nothing
            end
        end
    end

    nested_test("bases") do
        nested_test("get") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain =
                    complete_chain!(; base_daf = BaseDaf(; daf = cells, axes = [VIEW_ALL_AXES]), new_daf = metacells)

                @test get_base_daf_links(cells) === nothing
                @test get_base_daf_links(metacells) == [RecordedBase("cells", [VIEW_ALL_AXES], nothing)]
                @test get_base_daf_links(read_only(metacells)) == get_base_daf_links(metacells)

                # Only a stored repository has bases of its own.
                @test get_base_daf_links(chain) === nothing
                @test get_base_daf_links(viewer(metacells; axes = [VIEW_ALL_AXES])) === nothing
                return nothing
            end
        end

        nested_test("rebase") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                other_cells = FilesDaf("$(path)/other_cells", "w"; name = "other_cells!")
                populate_complete_cells!(other_cells)
                set_vector!(other_cells, "cell", "age", [11, 21, 31]; overwrite = true)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                set_base_daf_links!(metacells, other_cells)
                @test get_base_daf_links(metacells) == [RecordedBase("other_cells", nothing, nothing)]
                @test get_vector(complete_daf("$(path)/metacells"), "cell", "age") == [11, 21, 31]
                return nothing
            end
        end

        nested_test("detach") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                set_base_daf_links!(metacells, nothing)
                @test get_base_daf_links(metacells) === nothing
                @test !has_axis(complete_daf("$(path)/metacells"), "cell")
                return nothing
            end
        end

        nested_test("!stored") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain = complete_chain!(; base_daf = cells, new_daf = metacells)
                @test_throws "not a stored repository: " set_base_daf_links!(chain, nothing)
                return nothing
            end
        end

        nested_test("!axes") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                other_cells = FilesDaf("$(path)/other_cells", "w"; name = "other_cells!")
                add_axis!(other_cells, "cell", ["D", "E"])
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)
                add_axis!(metacells, "cell", ["A", "B", "C"])

                # The rebased repository would not reopen, so it is not recorded.
                @test_throws "different number of entries: 3" set_base_daf_links!(metacells, other_cells)
                @test get_base_daf_links(metacells) == [RecordedBase("cells", nothing, nothing)]
                return nothing
            end
        end

        nested_test("reserved") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                chain = complete_chain!(; base_daf = cells, new_daf = metacells)

                # The bases are storage metadata, not data.
                for name in ("_base_daf_links", "base_daf_repository")
                    @test !has_scalar(metacells, name)
                    @test !has_scalar(chain, name)
                    @test_throws "reserved scalar: $(name)" get_scalar(metacells, name)
                    @test_throws "reserved scalar: $(name)" set_scalar!(metacells, name, "cells")
                    @test_throws "reserved scalar: $(name)" delete_scalar!(metacells, name)
                end
                @test isempty(scalars_set(metacells))
                @test isempty(scalars_set(chain))
                @test contains(description(metacells), "base_daf_links: cells")
                return nothing
            end
        end

        nested_test("legacy") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)

                # A repository of an earlier format version holds its bases in the legacy scalar.
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                write_raw_scalar!(metacells, "base_daf_repository", "cells")
                write("$(path)/metacells/daf.json", "{\"version\":[1,1]}\n")
                reopened = complete_daf("$(path)/metacells")
                @test get_vector(reopened, "cell", "age") == [10, 20, 30]
                @test get_base_daf_links(metacells) == [RecordedBase("cells", nothing, nothing)]

                # Recording the bases raises its version, and moves them to the new scalar.
                set_base_daf_links!(metacells, cells)
                @test JSON.parsefile("$(path)/metacells/daf.json")["version"] == [1, 2]
                @test has_raw_scalar(metacells, "_base_daf_links")
                @test !has_raw_scalar(metacells, "base_daf_repository")
                @test get_vector(complete_daf("$(path)/metacells"), "cell", "age") == [10, 20, 30]
                return nothing
            end
        end

        nested_test("zip_legacy") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)

                # An archive of an earlier format version can't be raised, so it records its bases as that version does.
                writer = ZipArchives.ZipWriter("$(path)/metacells.daf.zip")
                ZipArchives.zip_writefile(writer, "daf.json", Vector{UInt8}("{\"version\":[1,1]}\n"))
                close(writer)
                metacells = ZipDaf("$(path)/metacells.daf.zip", "r+"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)
                @test has_raw_scalar(metacells, "base_daf_repository")
                @test !has_raw_scalar(metacells, "_base_daf_links")
                @test get_vector(complete_daf("$(path)/metacells.daf.zip"), "cell", "age") == [10, 20, 30]
                return nothing
            end
        end

        nested_test("copy") do
            mktempdir() do path
                mkdir("$(path)/a")
                mkdir("$(path)/b")
                cells = FilesDaf("$(path)/a/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/a/metacells", "w"; name = "metacells!")
                metacells_chain = complete_chain!(; base_daf = cells, new_daf = metacells)
                set_vector!(metacells_chain, "cell", "metacell", ["M1", "M1", "M2"])

                # A copy of a repository resting on bases rests on the same bases.
                relocated = FilesDaf("$(path)/b/metacells", "w"; name = "relocated!")
                copy_all!(; destination = relocated, source = FilesDaf("$(path)/a/metacells"))
                @test get_base_daf_links(relocated) == [RecordedBase("../a/cells", nothing, nothing)]
                reopened = complete_daf("$(path)/b/metacells")
                @test get_vector(reopened, "cell", "age") == [10, 20, 30]
                @test get_vector(reopened, "cell", "metacell") == ["M1", "M1", "M2"]

                # A copy of a chain holds all its data, and rests on nothing.
                flat = FilesDaf("$(path)/b/flat", "w"; name = "flat!")
                copy_all!(; destination = flat, source = complete_daf("$(path)/a/metacells"))
                @test get_base_daf_links(flat) === nothing
                @test get_vector(flat, "cell", "age") == [10, 20, 30]

                # The destination's own bases are never replaced.
                rebased = FilesDaf("$(path)/b/rebased", "w"; name = "rebased!")
                complete_chain!(; base_daf = cells, new_daf = rebased)
                @test_throws "existing bases in the destination: rebased!" copy_all!(;
                    destination = rebased,
                    source = FilesDaf("$(path)/a/metacells"),
                )
                return nothing
            end
        end

        nested_test("convert") do
            mktempdir() do path
                mkdir("$(path)/a")
                mkdir("$(path)/b")
                cells = FilesDaf("$(path)/a/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/a/metacells", "w"; name = "metacells!")
                complete_chain!(; base_daf = cells, new_daf = metacells)

                files_to_zarr(; files_path = "$(path)/a/metacells", zarr_path = "$(path)/b/metacells.daf.zarr")
                @test get_base_daf_links(ZarrDaf("$(path)/b/metacells.daf.zarr")) ==
                      [RecordedBase("../a/cells", nothing, nothing)]
                @test get_vector(complete_daf("$(path)/b/metacells.daf.zarr"), "cell", "age") == [10, 20, 30]

                zarr_to_files(; zarr_path = "$(path)/b/metacells.daf.zarr", files_path = "$(path)/a/converted")
                @test get_base_daf_links(FilesDaf("$(path)/a/converted")) == [RecordedBase("cells", nothing, nothing)]
                return nothing
            end
        end
    end

    nested_test("chain") do
        nested_test("on_view") do
            mktempdir() do path
                cells = FilesDaf("$(path)/cells", "w"; name = "cells!")
                populate_complete_cells!(cells)
                metacells = FilesDaf("$(path)/metacells", "w"; name = "metacells!")
                metacells_chain = complete_chain!(; base_daf = cells, new_daf = metacells)
                set_vector!(metacells_chain, "cell", "metacell", ["M1", "M1", "M2"])

                # The view exposes all the data of the chained metacells, which rests in turn on the cells.
                focus = FilesDaf("$(path)/focus", "w"; name = "focus!")
                complete_chain!(;
                    base_daf = BaseDaf(;
                        daf = complete_daf("$(path)/metacells"),
                        axes = [VIEW_ALL_AXES],
                        data = [VIEW_ALL_SCALARS, VIEW_ALL_VECTORS],
                    ),
                    new_daf = focus,
                )

                reopened_focus = complete_daf("$(path)/focus"; name = "reopened_focus!")
                @test complete_path(reopened_focus) == abspath("$(path)/focus")

                round = FilesDaf("$(path)/round", "w"; name = "round!")
                complete_chain!(; base_daf = reopened_focus, new_daf = round)
                reopened_round = complete_daf("$(path)/round"; name = "reopened_round!")
                @test get_vector(reopened_round, "cell", "age") == [10, 20, 30]
                @test get_vector(reopened_round, "cell", "metacell") == ["M1", "M1", "M2"]
                return nothing
            end
        end

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
