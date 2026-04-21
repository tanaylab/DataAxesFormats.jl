nested_test("mmap_zip_stores") do
    mktempdir() do tmp_directory
        zip_path = joinpath(tmp_directory, "test.zip")

        nested_test("missing_file_without_create") do
            @test_throws ErrorException MmapZipStore(joinpath(tmp_directory, "does_not_exist.zip"))
        end

        nested_test("create_empty_archive") do
            store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                @test sprint(show, store) == "MmapZipStore($(zip_path))"
                @test !haskey(store, "anything")
                @test store["anything"] === nothing
                @test !Zarr.isinitialized(store, "anything")
                @test isempty(Zarr.subkeys(store, ""))
                @test isempty(Zarr.subdirs(store, ""))
                @test Zarr.storagesize(store, "") == 0
            finally
                close(store)
            end

            read_only_store = MmapZipStore(zip_path)
            try
                @test isempty(Zarr.subkeys(read_only_store, ""))
            finally
                close(read_only_store)
            end
        end

        nested_test("write_and_read_back") do
            store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                store[".zgroup"] = Vector{UInt8}("{}")
                store["alpha"] = Vector{UInt8}("hello")
                store["sub/beta"] = Vector{UInt8}("world")
                store["sub/gamma/.zarray"] = Vector{UInt8}("{}")

                @test store["alpha"] == Vector{UInt8}("hello")
                @test store["sub/beta"] == Vector{UInt8}("world")
                @test haskey(store, "alpha")
                @test Zarr.isinitialized(store, "alpha")
                @test !haskey(store, "absent")

                @test Set(Zarr.subkeys(store, "")) == Set([".zgroup", "alpha"])
                @test Set(Zarr.subdirs(store, "")) == Set(["sub"])
                @test Set(Zarr.subkeys(store, "sub")) == Set(["beta"])
                @test Set(Zarr.subdirs(store, "sub")) == Set(["gamma"])

                @test Zarr.storagesize(store, "") == length("hello") + length("world") + length("{}")
                @test Zarr.storagesize(store, "sub") == length("world") + length("{}")
            finally
                close(store)
            end

            reopened = MmapZipStore(zip_path)
            try
                @test reopened["alpha"] == Vector{UInt8}("hello")
                @test reopened["sub/beta"] == Vector{UInt8}("world")
                @test Set(Zarr.subkeys(reopened, "")) == Set([".zgroup", "alpha"])
                @test Set(Zarr.subdirs(reopened, "")) == Set(["sub"])
            finally
                close(reopened)
            end
        end

        nested_test("append_to_existing_archive") do
            first_store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                first_store["first"] = Vector{UInt8}("AAA")
                first_store["second"] = Vector{UInt8}("BBBB")
            finally
                close(first_store)
            end

            second_store = MmapZipStore(zip_path; writable = true, create = true)
            try
                @test second_store["first"] == Vector{UInt8}("AAA")
                @test second_store["second"] == Vector{UInt8}("BBBB")
                second_store["third"] = Vector{UInt8}("CCCCC")
                @test second_store["third"] == Vector{UInt8}("CCCCC")
            finally
                close(second_store)
            end

            final_store = MmapZipStore(zip_path)
            try
                @test final_store["first"] == Vector{UInt8}("AAA")
                @test final_store["second"] == Vector{UInt8}("BBBB")
                @test final_store["third"] == Vector{UInt8}("CCCCC")
            finally
                close(final_store)
            end
        end

        nested_test("reserve_and_patch_large_entry") do
            reserved_size = 4096
            store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                store["before"] = Vector{UInt8}("X")
                reserved_bytes = reserve_mmap_zip_entry!(store, "bulk", reserved_size)
                @test length(reserved_bytes) == reserved_size
                for index in 1:reserved_size
                    reserved_bytes[index] = UInt8((index - 1) % 256)
                end
                patch_mmap_zip_entry_crc!(store, "bulk")
                store["after"] = Vector{UInt8}("Y")
            finally
                close(store)
            end

            reopened = MmapZipStore(zip_path)
            try
                @test reopened["before"] == Vector{UInt8}("X")
                @test reopened["after"] == Vector{UInt8}("Y")
                bulk_bytes = reopened["bulk"]
                @test length(bulk_bytes) == reserved_size
                for index in 1:reserved_size
                    @test bulk_bytes[index] == UInt8((index - 1) % 256)
                end
            finally
                close(reopened)
            end
        end

        nested_test("recovery_rolls_back_unpatched_reservation") do
            first_store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                first_store["keep_one"] = Vector{UInt8}("one")
                first_store["keep_two"] = Vector{UInt8}("two")
            finally
                close(first_store)
            end

            second_store = MmapZipStore(zip_path; writable = true)
            try
                reserved_bytes = reserve_mmap_zip_entry!(second_store, "abandoned", 32)
                for index in 1:32
                    reserved_bytes[index] = UInt8(index)
                end
                # Deliberately do NOT call patch_mmap_zip_entry_crc!, simulating a crash
                # between reservation and patching. The stored CRC32 placeholder of 0 will
                # not match the actual CRC32 of the now-filled data, so the entry must be
                # rolled back on the next write-mode open.
            finally
                close(second_store)
            end

            recovered_store = MmapZipStore(zip_path; writable = true)
            try
                @test recovered_store["keep_one"] == Vector{UInt8}("one")
                @test recovered_store["keep_two"] == Vector{UInt8}("two")
                @test !haskey(recovered_store, "abandoned")
                @test recovered_store["abandoned"] === nothing
                recovered_store["after_recovery"] = Vector{UInt8}("post")
                @test recovered_store["after_recovery"] == Vector{UInt8}("post")
            finally
                close(recovered_store)
            end

            final_store = MmapZipStore(zip_path)
            try
                @test final_store["keep_one"] == Vector{UInt8}("one")
                @test final_store["keep_two"] == Vector{UInt8}("two")
                @test final_store["after_recovery"] == Vector{UInt8}("post")
                @test !haskey(final_store, "abandoned")
            finally
                close(final_store)
            end
        end

        nested_test("overwriting_existing_entry_is_rejected") do
            store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                store["entry"] = Vector{UInt8}("first")
                @test_throws ErrorException store["entry"] = Vector{UInt8}("second")
                @test_throws ErrorException reserve_mmap_zip_entry!(store, "entry", 16)
            finally
                close(store)
            end
        end

        nested_test("storefromstring_opens_existing_archive") do
            create_store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                create_store["sample"] = Vector{UInt8}("payload")
            finally
                close(create_store)
            end

            opened_store, subkey = Zarr.storefromstring(MmapZipStore, zip_path, nothing)
            try
                @test subkey == ""
                @test opened_store["sample"] == Vector{UInt8}("payload")
            finally
                close(opened_store)
            end
        end

        nested_test("truncate_discards_existing_archive") do
            first_store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                first_store["stale"] = Vector{UInt8}("old")
            finally
                close(first_store)
            end

            truncated_store = MmapZipStore(zip_path; writable = true, create = true, truncate = true)
            try
                @test !haskey(truncated_store, "stale")
                @test isempty(Zarr.subkeys(truncated_store, ""))
            finally
                close(truncated_store)
            end
        end

        nested_test("reopen_read_only_foreign_deflate_archive") do
            foreign_path = joinpath(tmp_directory, "foreign.zip")
            ZipArchives.ZipWriter(foreign_path) do writer
                ZipArchives.zip_newfile(writer, "compressed"; compress = true, compression_method = ZipArchives.Deflate)
                write(writer, repeat("compressible ", 64))
                ZipArchives.zip_newfile(writer, "plain"; compression_method = ZipArchives.Store)
                return write(writer, "verbatim payload")
            end

            store = MmapZipStore(foreign_path)
            try
                @test String(copy(store["compressed"])) == repeat("compressible ", 64)
                @test String(copy(store["plain"])) == "verbatim payload"
            finally
                close(store)
            end
        end
    end
end
