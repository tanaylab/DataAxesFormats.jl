# Round-trip helpers shared by per-backend packed-write tests (`test/zarr_packed.jl`, future `test/files_packed.jl`,
# `test/h5df_packed.jl`, …). Each helper opens a writable daf via `create_daf(path; packed)`, writes a representative
# property at a fixed test shape, asserts the round-trip equality, then yields `(daf, path)` to the caller's `action`
# block for backend-specific introspection (chunk shape on disk, compressor type, dataset-level marker file, …).

function with_packed_dense_matrix_round_trip(action::Function, create_daf::Function)::Nothing
    # 4096 × 3 Float32: per-column 16 KB ≥ threshold; `chunks_for` returns `(2048, 1)` → 6 chunks.
    mktempdir() do path
        n_rows = 4096
        n_cols = 3
        original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
        daf = create_daf(path; packed = true)
        add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
        add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
        set_matrix!(daf, "row", "col", "data", original; relayout = false)
        @test get_matrix(daf, "row", "col", "data") == original
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_packed_dense_vector_round_trip(action::Function, create_daf::Function)::Nothing
    # 10 000-element Float32: 40 KB ≥ threshold; `chunks_for` returns `(2048,)` → 5 chunks.
    mktempdir() do path
        n_elements = 10_000
        original = Float32.(1:n_elements)
        daf = create_daf(path; packed = true)
        add_axis!(daf, "elem", ["e$(index)" for index in 1:n_elements])
        set_vector!(daf, "elem", "data", original)
        @test get_vector(daf, "elem", "data") == original
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_packed_sparse_matrix_round_trip(action::Function, create_daf::Function)::Nothing
    # 8192 × 4 Float32 sparse matrix with one nonzero per row → `nnz = 32 768`. Per-`nzval` 128 KB ≥ threshold;
    # `chunks_for` returns `(2048,)` → 16 chunks for `nzval`. `colptr` (5 entries) and `rowval` (32 768 entries) stay
    # flat per Plan §8 Phase 2 (only `nzval` packs in this phase).
    mktempdir() do path
        n_rows = 8192
        n_cols = 4
        # Diagonal-like nonzero placement: each column gets the rows `[1, 2, ..., n_rows]` with values `column * row`.
        column_pointers = Int32[1 + (column_index - 1) * n_rows for column_index in 1:(n_cols + 1)]
        row_indices = Int32[((position - 1) % n_rows) + 1 for position in 1:(n_cols * n_rows)]
        nz_values =
            Float32[((position - 1) ÷ n_rows + 1) * (((position - 1) % n_rows) + 1) for position in 1:(n_cols * n_rows)]
        original = SparseMatrixCSC{Float32, Int32}(n_rows, n_cols, column_pointers, row_indices, nz_values)
        daf = create_daf(path; packed = true)
        add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
        add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
        set_matrix!(daf, "row", "col", "data", original; relayout = false)
        @test get_matrix(daf, "row", "col", "data") == original
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_packed_string_vector_round_trip(action::Function, create_daf::Function)::Nothing
    # 30 000 strings × 16-byte estimate ≈ 480 KB ≥ threshold; `chunks_for` returns `(512,)` → ~60 chunks.
    mktempdir() do path
        n_elements = 30_000
        gene_names = ["gene_$(index)" for index in 1:n_elements]
        daf = create_daf(path; packed = true)
        add_axis!(daf, "gene", gene_names)
        set_vector!(daf, "gene", "label", gene_names)
        @test get_vector(daf, "gene", "label") == gene_names
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_below_threshold_matrix_round_trip(action::Function, create_daf::Function)::Nothing
    # 100 × 100 Float32: per-column 400 bytes < threshold; `chunks_for` returns `nothing`. The writer falls back to the
    # flat single-chunk encoding even though `packed = true` was requested.
    mktempdir() do path
        n_rows = 100
        n_cols = 100
        original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
        daf = create_daf(path; packed = true)
        add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
        add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
        set_matrix!(daf, "row", "col", "data", original; relayout = false)
        @test get_matrix(daf, "row", "col", "data") == original
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_unpacked_dense_matrix_round_trip(action::Function, create_daf::Function)::Nothing
    # Same shape as `with_packed_dense_matrix_round_trip` but `packed = false`: the dataset-level packing default is
    # off, so the property writes through the flat single-chunk encoding inside a v1.1-marked dataset.
    mktempdir() do path
        n_rows = 4096
        n_cols = 3
        original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
        daf = create_daf(path; packed = false)
        add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
        add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
        set_matrix!(daf, "row", "col", "data", original; relayout = false)
        @test get_matrix(daf, "row", "col", "data") == original
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_zarr_legacy_v1_0_round_trip(action::Function, create_daf::Function)::Nothing
    # Build a v1.0-marked ZarrDaf by temporarily setting `ZarrFormat.MINOR_VERSION = 0` during creation, restore the
    # global, drop the writable handle so the process-wide `MmapZipStore` weak cache (no-op for `DirectoryStore`)
    # releases it, then reopen read-only to exercise the new code's v1.0 read-compat path. The dataset's flat
    # below-threshold layout is byte-equivalent between v1.0 and v1.1 — only the marker differs.
    mktempdir() do path
        n_rows = 100
        n_cols = 5
        original_matrix = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
        original_vector = Float32.(1:n_rows)

        saved_minor = DataAxesFormats.ZarrFormat.MINOR_VERSION
        try
            DataAxesFormats.ZarrFormat.MINOR_VERSION = UInt8(0)
            daf = create_daf(path; packed = false)
            add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
            add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
            set_matrix!(daf, "row", "col", "data", original_matrix; relayout = false)
            set_vector!(daf, "row", "score", original_vector)
        finally
            DataAxesFormats.ZarrFormat.MINOR_VERSION = saved_minor
        end
        GC.gc()

        legacy_daf = create_daf(path, "r")
        @test get_matrix(legacy_daf, "row", "col", "data") == original_matrix
        @test get_vector(legacy_daf, "row", "score") == original_vector
        action(legacy_daf, path)
        return nothing
    end
    return nothing
end

nested_test("packed_format") do
    nested_test("globals") do
        @test DAF_PACKED_TARGET_CHUNK_KB == 8
        @test DAF_PACKED_COMPRESSION == :blosc_zstd_bitshuffle
        @test DAF_PACKED_COMPRESSION_LEVEL == 5
        @test DAF_PACKED_LOCAL_CACHE_KB == 65536
        @test DAF_PACKED_HTTP_CACHE_KB == 262144
    end

    nested_test("packed_target_chunk_bytes") do
        @test DataAxesFormats.PackedFormat.packed_target_chunk_bytes() == 8 * 1024
    end

    nested_test("compressor_for") do
        nested_test("default") do
            codec = DataAxesFormats.PackedFormat.compressor_for()
            @test codec.compression == DAF_PACKED_COMPRESSION
            @test codec.compression_level == DAF_PACKED_COMPRESSION_LEVEL
        end

        nested_test("explicit_codec") do
            codec = DataAxesFormats.PackedFormat.compressor_for(:gzip)
            @test codec.compression == :gzip
            @test codec.compression_level == DAF_PACKED_COMPRESSION_LEVEL
        end

        nested_test("explicit_level") do
            codec = DataAxesFormats.PackedFormat.compressor_for(:zstd, 7)
            @test codec.compression == :zstd
            @test codec.compression_level == 7
        end

        nested_test("supported_codecs") do
            for codec_symbol in
                (:blosc_zstd_bitshuffle, :blosc_lz4_bitshuffle, :zstd_bitshuffle, :zstd, :gzip, :gzip_shuffle)
                result = DataAxesFormats.PackedFormat.compressor_for(codec_symbol)
                @test result.compression == codec_symbol
            end
        end

        nested_test("unsupported_codec") do
            @test_throws "unsupported packed compression codec: :snappy" DataAxesFormats.PackedFormat.compressor_for(
                :snappy,
            )
        end

        nested_test("error_lists_supported") do
            try
                DataAxesFormats.PackedFormat.compressor_for(:bogus)
                @test false  # should have thrown
            catch exception
                message = sprint(showerror, exception)
                @test occursin(":blosc_zstd_bitshuffle", message)
                @test occursin(":gzip", message)
                @test occursin(":zstd", message)
            end
        end

        nested_test("level_in_range_blosc") do
            @test DataAxesFormats.PackedFormat.compressor_for(:blosc_zstd_bitshuffle, 1).compression_level == 1
            @test DataAxesFormats.PackedFormat.compressor_for(:blosc_zstd_bitshuffle, 9).compression_level == 9
            @test_throws "out-of-range packed compression level: 0" DataAxesFormats.PackedFormat.compressor_for(
                :blosc_zstd_bitshuffle,
                0,
            )
            @test_throws "out-of-range packed compression level: 10" DataAxesFormats.PackedFormat.compressor_for(
                :blosc_zstd_bitshuffle,
                10,
            )
        end

        nested_test("level_in_range_zstd") do
            @test DataAxesFormats.PackedFormat.compressor_for(:zstd, 1).compression_level == 1
            @test DataAxesFormats.PackedFormat.compressor_for(:zstd, 22).compression_level == 22
            @test_throws "out-of-range packed compression level: 0" DataAxesFormats.PackedFormat.compressor_for(
                :zstd,
                0,
            )
            @test_throws "out-of-range packed compression level: 23" DataAxesFormats.PackedFormat.compressor_for(
                :zstd,
                23,
            )
        end

        nested_test("level_in_range_gzip") do
            @test DataAxesFormats.PackedFormat.compressor_for(:gzip, 1).compression_level == 1
            @test DataAxesFormats.PackedFormat.compressor_for(:gzip, 9).compression_level == 9
            @test_throws "out-of-range packed compression level: 0" DataAxesFormats.PackedFormat.compressor_for(
                :gzip,
                0,
            )
            @test_throws "out-of-range packed compression level: 10" DataAxesFormats.PackedFormat.compressor_for(
                :gzip,
                10,
            )
        end

        nested_test("level_error_lists_range") do
            try
                DataAxesFormats.PackedFormat.compressor_for(:zstd, 100)
                @test false  # should have thrown
            catch exception
                message = sprint(showerror, exception)
                @test occursin(":zstd", message)
                @test occursin("1:22", message)
            end
        end
    end

    nested_test("effective_sizeof") do
        @test DataAxesFormats.PackedFormat.effective_sizeof(Float32) == 4
        @test DataAxesFormats.PackedFormat.effective_sizeof(Bool) == 1
        @test DataAxesFormats.PackedFormat.effective_sizeof(Int64) == 8
        @test DataAxesFormats.PackedFormat.effective_sizeof(String) ==
              DataAxesFormats.PackedFormat.STRING_SIZEOF_ESTIMATE
        @test DataAxesFormats.PackedFormat.STRING_SIZEOF_ESTIMATE == 16
    end

    nested_test("chunks_for") do
        nested_test("not_packed_returns_nothing") do
            @test DataAxesFormats.PackedFormat.chunks_for(false, (10,), Float32) === nothing
            @test DataAxesFormats.PackedFormat.chunks_for(false, (1_000_000,), Float32) === nothing
            @test DataAxesFormats.PackedFormat.chunks_for(false, (10, 20), Float32) === nothing
            @test DataAxesFormats.PackedFormat.chunks_for(false, (1_000_000, 30), Float32) === nothing
        end

        # Default threshold = 8 KB = 8192 bytes; Float32 → 2048 elements per chunk at threshold.
        # Sanity table (also serves as the design's worked examples):
        nested_test("vector_at_5x_target") do
            # 10000 × 4 = 40000 ≥ 8192 → pack; 5 chunks of 2048 elements.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (10_000,), Float32) == (2048,)
        end

        nested_test("vector_packed_as_single_chunk") do
            # 2048 × 4 = 8192 = threshold → pack as one chunk that equals the shape.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (2048,), Float32) == (2048,)
        end

        nested_test("vector_below_threshold") do
            # 2047 × 4 = 8188 < 8192 → flat.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (2047,), Float32) === nothing
        end

        nested_test("typical_bio_matrix_shape") do
            # 10000 × 30 Float32: per-column 40000 ≥ threshold → row-chunked.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (10_000, 30), Float32) == (2048, 1)
        end

        nested_test("short_column_matrix_stays_flat") do
            # 100 × 30000 Float32 (block × gene): per-column 400 < threshold; chunks of 400 bytes are not worth packing.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (100, 30_000), Float32) === nothing
        end

        nested_test("single_row_matrix_stays_flat") do
            # 1 × 2049 Float32: per-column 4 < threshold → flat. Avoids 2049 single-element chunks.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (1, 2049), Float32) === nothing
        end

        nested_test("single_column_matrix_above_threshold") do
            # 2049 × 1 Float32: per-column 8196 ≥ threshold → 2 chunks of (2048, 1) and (1, 1).
            @test DataAxesFormats.PackedFormat.chunks_for(true, (2049, 1), Float32) == (2048, 1)
        end

        nested_test("string_vector_above_threshold") do
            # 30000 strings × 16 (estimate) = 480000 ≥ threshold → 60 chunks of 512 strings each.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (30_000,), String) == (512,)
        end

        nested_test("string_vector_below_threshold") do
            # 100 × 16 = 1600 < 8192 → flat.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (100,), String) === nothing
        end

        nested_test("uint8_one_byte_boundary") do
            # `UInt8` = 1 byte/element gives exact one-byte resolution at the threshold.
            @test DataAxesFormats.PackedFormat.chunks_for(true, (8191,), UInt8) === nothing
            @test DataAxesFormats.PackedFormat.chunks_for(true, (8192,), UInt8) == (8192,)
            @test DataAxesFormats.PackedFormat.chunks_for(true, (8193,), UInt8) == (8192,)
        end

        nested_test("respects_global_override") do
            saved_target = DAF_PACKED_TARGET_CHUNK_KB
            try
                DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB = 16
                # 16 KB = 16384 bytes; Float32 → 4096 elements at threshold.
                @test DataAxesFormats.PackedFormat.chunks_for(true, (4095,), Float32) === nothing
                @test DataAxesFormats.PackedFormat.chunks_for(true, (4096,), Float32) == (4096,)
                @test DataAxesFormats.PackedFormat.chunks_for(true, (4097,), Float32) == (4096,)
            finally
                DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB = saved_target
            end
        end
    end

    nested_test("memory_packed_kwarg") do
        memory_packed = MemoryDaf(; name = "packed!", packed = true)
        @test memory_packed isa MemoryDaf
    end

    nested_test("relayout_explicit_packed") do
        daf = MemoryDaf(; name = "memory!")
        add_axis!(daf, "cell", ["A", "B", "C"])
        add_axis!(daf, "gene", ["X", "Y"])
        set_matrix!(daf, "cell", "gene", "UMIs", [1 2; 3 4; 5 6]; relayout = false)
        @test relayout_matrix!(daf, "cell", "gene", "UMIs"; packed = false) === nothing
    end

    nested_test("format_is_packed") do
        daf = MemoryDaf(; name = "memory!")
        add_axis!(daf, "cell", ["A", "B", "C"])
        add_axis!(daf, "gene", ["X", "Y"])
        set_vector!(daf, "cell", "age", [1, 2, 3])
        set_matrix!(daf, "cell", "gene", "UMIs", [1 2; 3 4; 5 6])

        nested_test("read_only") do
            ro = read_only(daf)
            DataAxesFormats.Formats.with_data_read_lock(ro, "test") do
                @test !DataAxesFormats.Formats.format_is_packed_vector(ro, "cell", "age")
                @test !DataAxesFormats.Formats.format_is_packed_matrix(ro, "cell", "gene", "UMIs")
                return nothing
            end
        end

        nested_test("chain") do
            chain = chain_writer([daf]; name = "chain!")
            DataAxesFormats.Formats.with_data_read_lock(chain, "test") do
                @test !DataAxesFormats.Formats.format_is_packed_matrix(chain, "cell", "gene", "UMIs")
                return nothing
            end
        end

        nested_test("view_identity") do
            view = viewer(daf; axes = [VIEW_ALL_AXES], data = [VIEW_ALL_VECTORS, VIEW_ALL_MATRICES])
            DataAxesFormats.Formats.with_data_read_lock(view, "test") do
                @test !DataAxesFormats.Formats.format_is_packed_vector(view, "cell", "age")
                @test !DataAxesFormats.Formats.format_is_packed_matrix(view, "cell", "gene", "UMIs")
                return nothing
            end
        end

        nested_test("view_non_identity") do
            view = viewer(
                daf;
                data = [("cell", "age") => "@ cell : age", ("cell", "gene", "UMIs") => "@ cell @ gene : UMIs"],
            )
            DataAxesFormats.Formats.with_data_read_lock(view, "test") do
                @test !DataAxesFormats.Formats.format_is_packed_vector(view, "cell", "age")
                @test !DataAxesFormats.Formats.format_is_packed_matrix(view, "cell", "gene", "UMIs")
                return nothing
            end
        end
    end

    nested_test("view_tensor_identity_branch") do
        daf = MemoryDaf(; name = "memory!")
        add_axis!(daf, "cell", ["X", "Y"])
        add_axis!(daf, "gene", ["A", "B", "C"])
        add_axis!(daf, "batch", ["U", "V"])
        set_matrix!(daf, "gene", "cell", "U_is_high", [true false; false true; true false]; relayout = false)
        set_matrix!(daf, "gene", "cell", "V_is_high", [true true; false false; true false]; relayout = false)
        set_matrix!(daf, "gene", "cell", "score", [0.1 0.2; 0.3 0.4; 0.5 0.6]; relayout = false)
        view = viewer(
            daf;
            axes = [VIEW_ALL_AXES],
            data = [("batch", "gene", "cell", "is_high") => "=", ("gene", "cell", "score") => "@ gene @ cell : score"],
        )
        DataAxesFormats.Formats.with_data_read_lock(view, "test") do
            @test !DataAxesFormats.Formats.format_is_packed_matrix(view, "gene", "cell", "U_is_high")
            return nothing
        end
    end

    nested_test("packed_dense_matrix") do
        no_op_encoder(::Int, ::Vector{Float32})::Nothing = nothing
        matrix = DataAxesFormats.PackedFormat.PackedDenseMatrix{Float32}(5000, 30000, no_op_encoder)
        @test size(matrix) == (5000, 30000)
        @test eltype(matrix) === Float32

        # Per-thread slot is reused across columns: same buffer object on the same thread, distinct contents per column.
        first_view = view(matrix, :, 1)
        @test first_view isa Vector{Float32}
        @test length(first_view) == 5000
        first_view .= Float32(1)

        second_view = view(matrix, :, 2)
        @test second_view === first_view  # buffer object reused on the same thread.
    end

    nested_test("http_striped_matrix_stub") do
        matrix =
            DataAxesFormats.PackedFormat.HttpStripedMatrix{Float32}("http://example.com/data", 128, 5000, 30000, 2048)
        @test size(matrix) == (5000, 30000)
        @test eltype(matrix) === Float32
        @test matrix.url == "http://example.com/data"
        @test matrix.header_size == 128
        @test matrix.stripe_n_rows == 2048
    end

    nested_test("http_striped_vector_stub") do
        vector = DataAxesFormats.PackedFormat.HttpStripedVector{Int32}("http://example.com/data", 64, 1_000_000, 2048)
        @test size(vector) == (1_000_000,)
        @test length(vector) == 1_000_000
        @test eltype(vector) === Int32
        @test vector.url == "http://example.com/data"
        @test vector.header_size == 64
        @test vector.stripe_n_elements == 2048
    end

    nested_test("valid_compression_level_range") do
        @test DataAxesFormats.PackedFormat.valid_compression_level_range(:blosc_zstd_bitshuffle) == 1:9
        @test DataAxesFormats.PackedFormat.valid_compression_level_range(:blosc_lz4_bitshuffle) == 1:9
        @test DataAxesFormats.PackedFormat.valid_compression_level_range(:zstd_bitshuffle) == 1:22
        @test DataAxesFormats.PackedFormat.valid_compression_level_range(:zstd) == 1:22
        @test DataAxesFormats.PackedFormat.valid_compression_level_range(:gzip) == 1:9
        @test DataAxesFormats.PackedFormat.valid_compression_level_range(:gzip_shuffle) == 1:9
        @test_throws "unsupported packed compression codec: :bogus" DataAxesFormats.PackedFormat.valid_compression_level_range(
            :bogus,
        )
    end

    nested_test("zarr") do
        # In-memory `ZArray` lookup — works the same regardless of whether the daf is `DirectoryStore`-backed or
        # `MmapZipStore`-backed.
        function zarr_array_at(daf::ZarrDaf, group_name::AbstractString, axes_and_name::AbstractString...)::Zarr.ZArray
            group = daf.root.groups[group_name]
            for axis in axes_and_name[1:(end - 1)]
                group = group.groups[axis]
            end
            return group.arrays[axes_and_name[end]]
        end

        # Per-backend test bodies. `create_daf(path; packed)` returns a writable `ZarrDaf` rooted in `path`.
        # `daf_marker_for(path)`, when supplied, returns the dataset's on-disk `daf` group attribute
        # (`[major, minor]`) and is used by the few tests that assert the version marker; backends
        # without a cheap byte-level reader pass `nothing`.
        function run_zarr_packed_tests(create_daf::Function; daf_marker_for::Maybe{Function} = nothing)::Nothing
            nested_test("dense_matrix_round_trip") do
                with_packed_dense_matrix_round_trip(create_daf) do daf, path
                    zarr_array = zarr_array_at(daf, "matrices", "row", "col", "data")
                    sharding = zarr_array.metadata.pipeline.array_bytes
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test sharding.chunk_shape == (2048, 1)
                    @test !isempty(sharding.codecs.bytes_bytes)
                    if daf_marker_for !== nothing
                        @test daf_marker_for(path) == [1, 0]
                    end
                    @test parent(get_matrix(daf, "row", "col", "data")) isa DiskArrays.CachedDiskArray
                    return nothing
                end
            end

            nested_test("streaming_dense_matrix_fill") do
                mktempdir() do path
                    n_rows = 4096
                    n_cols = 3
                    original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))

                    daf = create_daf(path; packed = true)
                    add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                    add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])

                    empty_dense_matrix!(daf, "row", "col", "data", Float32; packed = true) do filled
                        parallel_loop_wo_rng(1:n_cols; name = "streaming_fill_columns") do column_index
                            @views filled[:, column_index] .= original[:, column_index]
                        end
                    end

                    @test get_matrix(daf, "row", "col", "data") == original

                    zarr_array = zarr_array_at(daf, "matrices", "row", "col", "data")
                    sharding = zarr_array.metadata.pipeline.array_bytes
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test sharding.chunk_shape == (n_rows, 1)
                    @test !isempty(sharding.codecs.bytes_bytes)
                    return nothing
                end
            end

            nested_test("packed_read_cache_lifecycle") do
                mktempdir() do path
                    n_rows = 4096
                    n_cols = 3
                    original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
                    daf = create_daf(path; packed = true)
                    add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                    add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                    set_matrix!(daf, "row", "col", "data", original; relayout = false)

                    first_named = get_matrix(daf, "row", "col", "data")
                    first_wrapper = parent(first_named)
                    @test first_wrapper isa DiskArrays.CachedDiskArray

                    empty_cache!(daf)
                    second_named = get_matrix(daf, "row", "col", "data")
                    second_wrapper = parent(second_named)
                    @test second_wrapper isa DiskArrays.CachedDiskArray
                    @test second_wrapper !== first_wrapper  # Cache rebuild produces a fresh wrapper.
                    @test second_named == first_named
                    return nothing
                end
            end

            nested_test("sparse_matrix_round_trip") do
                with_packed_sparse_matrix_round_trip(create_daf) do daf, path
                    nzval_array = zarr_array_at(daf, "matrices", "row", "col", "data", "nzval")
                    nzval_sharding = nzval_array.metadata.pipeline.array_bytes
                    @test nzval_array.metadata.chunks == size(nzval_array)
                    @test nzval_sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test nzval_sharding.chunk_shape == (2048,)
                    @test !isempty(nzval_sharding.codecs.bytes_bytes)
                    colptr_array = zarr_array_at(daf, "matrices", "row", "col", "data", "colptr")
                    @test colptr_array.metadata.chunks == size(colptr_array)
                    @test isempty(colptr_array.metadata.pipeline.bytes_bytes)
                    rowval_array = zarr_array_at(daf, "matrices", "row", "col", "data", "rowval")
                    @test isempty(rowval_array.metadata.pipeline.bytes_bytes)
                    return nothing
                end
            end

            nested_test("dense_vector_round_trip") do
                with_packed_dense_vector_round_trip(create_daf) do daf, path
                    zarr_array = zarr_array_at(daf, "vectors", "elem", "data")
                    sharding = zarr_array.metadata.pipeline.array_bytes
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test sharding.chunk_shape == (2048,)
                    @test !isempty(sharding.codecs.bytes_bytes)
                    return nothing
                end
            end

            nested_test("empty_dense_vector_packed_round_trip") do
                mktempdir() do path
                    n_elements = 10_000
                    original = Float32.(1:n_elements)

                    daf = create_daf(path; packed = true)
                    add_axis!(daf, "elem", ["e$(index)" for index in 1:n_elements])

                    empty_dense_vector!(daf, "elem", "data", Float32; packed = true) do filled
                        return filled .= original
                    end

                    @test get_vector(daf, "elem", "data") == original

                    zarr_array = zarr_array_at(daf, "vectors", "elem", "data")
                    sharding = zarr_array.metadata.pipeline.array_bytes
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test sharding.chunk_shape == (2048,)
                    @test !isempty(sharding.codecs.bytes_bytes)
                    return nothing
                end
            end

            nested_test("string_vector_round_trip") do
                with_packed_string_vector_round_trip(create_daf) do daf, path
                    zarr_array = zarr_array_at(daf, "vectors", "gene", "label")
                    @test zarr_array.metadata.chunks == (512,)
                    @test !isempty(zarr_array.metadata.pipeline.bytes_bytes)
                    return nothing
                end
            end

            nested_test("below_threshold_stays_flat") do
                with_below_threshold_matrix_round_trip(create_daf) do daf, path
                    zarr_array = zarr_array_at(daf, "matrices", "row", "col", "data")
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test isempty(zarr_array.metadata.pipeline.bytes_bytes)
                    return nothing
                end
            end

            nested_test("unpacked_dense_matrix_in_v1_1_dataset") do
                with_unpacked_dense_matrix_round_trip(create_daf) do daf, path
                    zarr_array = zarr_array_at(daf, "matrices", "row", "col", "data")
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test isempty(zarr_array.metadata.pipeline.bytes_bytes)
                    if daf_marker_for !== nothing
                        @test daf_marker_for(path) == [1, 0]
                    end
                    return nothing
                end
            end

            nested_test("legacy_v1_0_read_compat") do
                with_zarr_legacy_v1_0_round_trip(create_daf) do _, _
                    return nothing
                end
            end

            return nothing
        end

        nested_test("directory") do
            function directory_factory(
                path::AbstractString,
                mode::AbstractString = "w+";
                packed::Bool = false,
            )::Union{ZarrDaf, DafReadOnly}
                return ZarrDaf(joinpath(path, "test.daf.zarr"), mode; name = "zarr_dir!", packed = packed)
            end

            function directory_marker_for(path::AbstractString)::Vector{Int}
                root_metadata = JSON.parse(read(joinpath(path, "test.daf.zarr", "zarr.json"), String))
                version = root_metadata["attributes"]["daf"]
                return Int[Int(version[1]), Int(version[2])]
            end

            return run_zarr_packed_tests(directory_factory; daf_marker_for = directory_marker_for)
        end

        nested_test("zip") do
            function zip_factory(
                path::AbstractString,
                mode::AbstractString = "w+";
                packed::Bool = false,
            )::Union{ZarrDaf, DafReadOnly}
                return ZarrDaf(joinpath(path, "test.daf.zarr.zip"), mode; name = "zarr_zip!", packed = packed)
            end

            # The `MmapZipStore`-backed daf marker lives as a zip entry rather than a filesystem path; reading it
            # would mean going through the store's API, which the rest of the round-trip already exercises. Skipping
            # the marker check here keeps this block focused on the storage-backend coverage.
            return run_zarr_packed_tests(zip_factory)
        end
    end
end
