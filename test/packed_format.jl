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
    # 8192 × 4 Float32 sparse matrix with one nonzero per row → `nnz = 32 768`. `nzval` (32 768 × 4 bytes = 128 KB)
    # and `rowval` (same byte size at Int32) both clear the threshold and pack with `chunk_shape = (2048,)`; `colptr`
    # (5 entries × 4 bytes = 20 bytes) stays flat.
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
    # 30 000 strings × 16-byte estimate ≈ 480 KB ≥ threshold; `chunks_for` returns `(512,)` as the inner-chunk shape
    # of the single sharded array.
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

function populate_packed_round_trip!(daf::DafWriter, n_rows::Int, n_cols::Int)::Tuple
    # Shared property-set used by the unzip↔zip equivalence tests. Writes one above-threshold dense matrix and one
    # above-threshold sparse matrix on (row, col), one above-threshold dense vector, one above-threshold string vector
    # (length-`n_rows`), and one below-threshold dense vector on a tiny `small` axis. Returns the originals as a tuple
    # for the read-side verifier.
    dense_matrix = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
    dense_vector = Float32.(1:n_rows)
    sparse_matrix = SparseMatrixCSC{Float32, Int32}(
        n_rows,
        n_cols,
        Int32[1 + (column_index - 1) * n_rows for column_index in 1:(n_cols + 1)],
        Int32[((position - 1) % n_rows) + 1 for position in 1:(n_cols * n_rows)],
        Float32[((position - 1) ÷ n_rows + 1) * (((position - 1) % n_rows) + 1) for position in 1:(n_cols * n_rows)],
    )
    small_dense = Float32[1.0, 2.0, 3.0, 4.0]
    string_vector = ["row_$(index)" for index in 1:n_rows]

    add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
    add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
    add_axis!(daf, "small", ["s1", "s2", "s3", "s4"])
    set_matrix!(daf, "row", "col", "data", dense_matrix; relayout = false)
    set_matrix!(daf, "row", "col", "sparse", sparse_matrix; relayout = false)
    set_vector!(daf, "row", "score", dense_vector)
    set_vector!(daf, "row", "label", string_vector)
    set_vector!(daf, "small", "marker", small_dense)
    return (dense_matrix, dense_vector, sparse_matrix, small_dense, string_vector)
end

function verify_packed_round_trip(daf::DafReader, expected::Tuple)::Nothing
    dense_matrix, dense_vector, sparse_matrix, small_dense, string_vector = expected
    @test get_matrix(daf, "row", "col", "data") == dense_matrix
    @test get_matrix(daf, "row", "col", "sparse") == sparse_matrix
    @test get_vector(daf, "row", "score") == dense_vector
    @test get_vector(daf, "row", "label") == string_vector
    @test get_vector(daf, "small", "marker") == small_dense
    return nothing
end

function with_packed_streaming_dense_matrix_fill(
    action::Function,
    create_daf::Function;
    n_rows::Int = 4096,
    n_cols::Int = 3,
)::Nothing
    # Streaming-fill round-trip: `empty_dense_matrix!(...; packed = true)` returns a `PackedDenseMatrix` wrapper that
    # streams chunks to the per-property `.shard` file as columns finalize. Default 4096 × 3 Float32 hits the standard
    # `chunk_shape = (2048, 1)`; pass `n_rows = 5000` to exercise the partial-tail tile (`n_rows % 2048 ≠ 0`) where the
    # encoder pads the last tile to `n_chunk_rows` with `zero(T)` before submitting.
    mktempdir() do path
        original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
        daf = create_daf(path; packed = true)
        add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
        add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
        empty_dense_matrix!(daf, "row", "col", "data", Float32; packed = true) do filled
            parallel_loop_wo_rng(1:n_cols; name = "streaming_fill_columns", policy = :static) do column_index
                @views filled[:, column_index] .= original[:, column_index]
            end
        end
        @test get_matrix(daf, "row", "col", "data") == original
        action(daf, path)
        return nothing
    end
    return nothing
end

function with_packed_empty_dense_vector_round_trip(action::Function, create_daf::Function)::Nothing
    # `empty_dense_vector!(...; packed = true)` allocates the full `Vector{T}` in RAM, the user fills it via the public
    # API, and `format_filled_*!` encodes the buffer to a `.shard` at finalize.
    mktempdir() do path
        n_elements = 10_000
        original = Float32.(1:n_elements)
        daf = create_daf(path; packed = true)
        add_axis!(daf, "elem", ["e$(index)" for index in 1:n_elements])
        empty_dense_vector!(daf, "elem", "data", Float32; packed = true) do filled
            return filled .= original
        end
        @test get_vector(daf, "elem", "data") == original
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

        # Same column twice on the same thread short-circuits to the existing slot buffer; nothing flushes and the
        # contents persist across the second `view` call.
        second_view .= Float32(7)
        same_view = view(matrix, :, 2)
        @test same_view === second_view
        @test all(==(Float32(7)), same_view)
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

    nested_test("open_shard_as_zarray") do
        # Write a packed dense property via `ZarrDaf` (which produces a single v3 shard file per array at
        # `<group>/<name>/c/0[/0]`), then point [`open_shard_as_zarray`](@ref) at the chunk file and verify
        # the standalone read reconstructs the same array. The shard bytes are byte-identical to what the
        # `FilesDaf` packed writer (Step 3.5) will emit at `<name>.shard`, so this test validates the
        # `FilesDaf` reader's helper without needing the writer in place.
        nested_test("vector") do
            mktempdir() do path
                n_elements = 10_000
                original = Float32.(1:n_elements)
                zarr_path = joinpath(path, "test.daf.zarr")
                daf = ZarrDaf(zarr_path, "w+"; name = "src!", packed = true)
                add_axis!(daf, "elem", ["e$(index)" for index in 1:n_elements])
                set_vector!(daf, "elem", "data", original)

                shard_path = joinpath(zarr_path, "vectors", "elem", "data", "c", "0")
                @test isfile(shard_path)

                zarr_array = DataAxesFormats.PackedFormat.open_shard_as_zarray(
                    shard_path,
                    Float32,
                    (n_elements,),
                    (2048,),
                    DataAxesFormats.PackedFormat.v3_bytes_codecs_for(
                        DataAxesFormats.PackedFormat.compressor_for(),
                        Float32,
                    ),
                    :end,
                )
                @test zarr_array[:] == original
                return nothing
            end
        end

        nested_test("matrix") do
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
                zarr_path = joinpath(path, "test.daf.zarr")
                daf = ZarrDaf(zarr_path, "w+"; name = "src!", packed = true)
                add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(daf, "row", "col", "data", original; relayout = false)

                shard_path = joinpath(zarr_path, "matrices", "row", "col", "data", "c", "0", "0")
                @test isfile(shard_path)

                zarr_array = DataAxesFormats.PackedFormat.open_shard_as_zarray(
                    shard_path,
                    Float32,
                    (n_rows, n_cols),
                    (2048, 1),
                    DataAxesFormats.PackedFormat.v3_bytes_codecs_for(
                        DataAxesFormats.PackedFormat.compressor_for(),
                        Float32,
                    ),
                    :end,
                )
                @test zarr_array[:, :] == original
                return nothing
            end
        end
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
                    @test parent(parent(get_matrix(daf, "row", "col", "data"))) isa DiskArrays.CachedDiskArray
                    return nothing
                end
            end

            nested_test("streaming_dense_matrix_fill") do
                with_packed_streaming_dense_matrix_fill(create_daf) do daf, path
                    zarr_array = zarr_array_at(daf, "matrices", "row", "col", "data")
                    sharding = zarr_array.metadata.pipeline.array_bytes
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test sharding.chunk_shape == (4096, 1)
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
                    first_wrapper = parent(parent(first_named))
                    @test first_wrapper isa DiskArrays.CachedDiskArray

                    empty_cache!(daf)
                    second_named = get_matrix(daf, "row", "col", "data")
                    second_wrapper = parent(parent(second_named))
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
                with_packed_empty_dense_vector_round_trip(create_daf) do daf, path
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
                    sharding = zarr_array.metadata.pipeline.array_bytes
                    @test zarr_array.metadata.chunks == size(zarr_array)
                    @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
                    @test sharding.chunk_shape == (512,)
                    @test !isempty(sharding.codecs.bytes_bytes)
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

        # In-memory `ZarrDaf` (`DictStore`-backed) has no incremental sink, so `packed_streaming_dense_matrix` falls
        # back to the per-column `numeric_zcreate` + `array[:, column] = chunk_buffer` encoder. Only the streaming
        # write path differs from the disk-backed branches; the round-trip equality check confirms the fallback
        # encoder produces a readable array.
        nested_test("memory_streaming_dense_matrix_fill") do
            n_rows = 4096
            n_cols = 3
            original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
            daf = ZarrDaf(; name = "memory!", packed = true)
            add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
            add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
            empty_dense_matrix!(daf, "row", "col", "data", Float32; packed = true) do filled
                @test filled isa DataAxesFormats.PackedFormat.PackedDenseMatrix{Float32}
                parallel_loop_wo_rng(1:n_cols; name = "memory_streaming_fill_columns", policy = :static) do column_index
                    @views filled[:, column_index] .= original[:, column_index]
                end
            end
            @test get_matrix(daf, "row", "col", "data") == original
            return nothing
        end
    end

    nested_test("files") do
        function files_factory(
            path::AbstractString,
            mode::AbstractString = "w+";
            packed::Bool = false,
        )::Union{FilesDaf, DafReadOnly}
            return FilesDaf(joinpath(path, "test.daf"), mode; name = "files!", packed = packed)
        end

        nested_test("dense_matrix_round_trip") do
            with_packed_dense_matrix_round_trip(files_factory) do daf, path
                shard_path = joinpath(path, "test.daf", "matrices", "row", "col", "data.shard")
                @test isfile(shard_path)
                json = JSON.parsefile(joinpath(path, "test.daf", "matrices", "row", "col", "data.json"))
                @test json["packed"] === true
                @test json["chunk_shape"] == [2048, 1]
                @test parent(parent(get_matrix(daf, "row", "col", "data"))) isa DiskArrays.CachedDiskArray
                return nothing
            end
        end

        nested_test("dense_vector_round_trip") do
            with_packed_dense_vector_round_trip(files_factory) do daf, path
                shard_path = joinpath(path, "test.daf", "vectors", "elem", "data.shard")
                @test isfile(shard_path)
                json = JSON.parsefile(joinpath(path, "test.daf", "vectors", "elem", "data.json"))
                @test json["packed"] === true
                @test json["chunk_shape"] == [2048]
                return nothing
            end
        end

        nested_test("streaming_dense_matrix_fill") do
            with_packed_streaming_dense_matrix_fill(files_factory) do daf, path
                @test isfile(joinpath(path, "test.daf", "matrices", "row", "col", "data.shard"))
                return nothing
            end
        end

        nested_test("streaming_dense_matrix_fill_uneven_rows") do
            with_packed_streaming_dense_matrix_fill(files_factory; n_rows = 5000) do _daf, _path
                return nothing
            end
        end

        nested_test("empty_dense_vector_packed_round_trip") do
            with_packed_empty_dense_vector_round_trip(files_factory) do _daf, path
                @test isfile(joinpath(path, "test.daf", "vectors", "elem", "data.shard"))
                return nothing
            end
        end

        nested_test("below_threshold_stays_flat") do
            with_below_threshold_matrix_round_trip(files_factory) do daf, path
                @test isfile(joinpath(path, "test.daf", "matrices", "row", "col", "data.data"))
                @test !isfile(joinpath(path, "test.daf", "matrices", "row", "col", "data.shard"))
                return nothing
            end
        end

        nested_test("byte_identical_to_zarr_dir") do
            # The on-disk `.shard` bytes must match what `ZarrDaf` writes for the same content
            # (same codec, same chunk shape) so `zarr_convert.jl` can hard-link across backends.
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))

                files_daf = FilesDaf(joinpath(path, "files.daf"), "w+"; name = "files!", packed = true)
                add_axis!(files_daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(files_daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(files_daf, "row", "col", "data", original; relayout = false)

                zarr_daf = ZarrDaf(joinpath(path, "zarr.daf.zarr"), "w+"; name = "zarr!", packed = true)
                add_axis!(zarr_daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(zarr_daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(zarr_daf, "row", "col", "data", original; relayout = false)

                files_shard = read(joinpath(path, "files.daf", "matrices", "row", "col", "data.shard"))
                zarr_shard = read(joinpath(path, "zarr.daf.zarr", "matrices", "row", "col", "data", "c", "0", "0"))
                @test files_shard == zarr_shard
            end
        end

        nested_test("packed_sparse_matrix_round_trip") do
            with_packed_sparse_matrix_round_trip(files_factory) do _daf, path
                base = joinpath(path, "test.daf", "matrices", "row", "col", "data")
                @test isfile("$(base).colptr")  # 5 * 4 bytes = 20 bytes < threshold → flat
                @test isfile("$(base).rowval.shard")  # 32 768 * 4 bytes = 128 KB ≥ threshold → packed
                @test isfile("$(base).nzval.shard")
                json = JSON.parsefile("$(base).json")
                @test get(json["colptr"], "packed", false) === false
                @test json["rowval"]["packed"] === true
                @test json["nzval"]["packed"] === true
                return nothing
            end
        end

        nested_test("packed_sparse_string_vector_round_trip") do
            mktempdir() do path
                # 3 K nonempty strings out of 100 K elements (sparse enough that the sparse on-disk
                # encoding wins); both components above threshold: nzind = 3 K × 4 bytes = 12 KB,
                # nzval = 3 K × 16-byte estimate = 48 KB.
                n_elements = 100_000
                original = Vector{String}(undef, n_elements)
                fill!(original, "")
                for index in 1:33:n_elements
                    original[index] = "value_$(index)"
                end

                daf = files_factory(path; packed = true)
                add_axis!(daf, "elem", ["e$(index)" for index in 1:n_elements])
                set_vector!(daf, "elem", "label", original)

                @test get_vector(daf, "elem", "label") == original
                base = joinpath(path, "test.daf", "vectors", "elem", "label")
                @test isfile("$(base).nzind.shard")
                @test isfile("$(base).nzval.shard")
                @test !isfile("$(base).nztxt")
                json = JSON.parsefile("$(base).json")
                @test json["format"] == "sparse"
                @test json["nzind"]["packed"] === true
                @test json["nzval"]["packed"] === true
                @test json["nzval"]["eltype"] == "String"
                return nothing
            end
        end
    end

    # `ZipDaf` packed write / read paths mirror `FilesDaf`: the JSON sidecar lives at the same per-property entry name,
    # and the per-property `.shard` (or per-component `<comp>.shard`) bytes are byte-identical to FilesDaf's. The tests
    # exercise each variant end-to-end (write → read), then validate byte-equivalence with FilesDaf for one canonical
    # property to lock the cross-format invariant.
    nested_test("zip") do
        function zip_factory(
            path::AbstractString,
            mode::AbstractString = "w+";
            packed::Bool = false,
        )::Union{ZipDaf, DafReadOnly}
            return ZipDaf(joinpath(path, "test.daf.zip"), mode; name = "zip!", packed)
        end

        function entry_keys(zip_path::AbstractString)::Set{String}
            return Set(ZipArchives.zip_names(ZipArchives.ZipReader(read(zip_path))))
        end

        function read_entry_json(zip_path::AbstractString, key::AbstractString)::AbstractDict
            zip_reader = ZipArchives.ZipReader(read(zip_path))
            entry_index = findfirst(name -> name == key, ZipArchives.zip_names(zip_reader))
            @assert entry_index !== nothing "missing entry: $(key)"
            return JSON.parse(String(ZipArchives.zip_readentry(zip_reader, entry_index)))
        end

        nested_test("dense_matrix_round_trip") do
            with_packed_dense_matrix_round_trip(zip_factory) do daf, path
                zip_path = joinpath(path, "test.daf.zip")
                keys = entry_keys(zip_path)
                @test "matrices/row/col/data.shard" in keys
                @test !("matrices/row/col/data.data" in keys)
                json = read_entry_json(zip_path, "matrices/row/col/data.json")
                @test json["packed"] === true
                @test json["chunk_shape"] == [2048, 1]
                return nothing
            end
        end

        nested_test("dense_vector_round_trip") do
            with_packed_dense_vector_round_trip(zip_factory) do daf, path
                zip_path = joinpath(path, "test.daf.zip")
                @test "vectors/elem/data.shard" in entry_keys(zip_path)
                json = read_entry_json(zip_path, "vectors/elem/data.json")
                @test json["packed"] === true
                @test json["chunk_shape"] == [2048]
                return nothing
            end
        end

        nested_test("streaming_dense_matrix_fill") do
            with_packed_streaming_dense_matrix_fill(zip_factory) do _daf, path
                @test "matrices/row/col/data.shard" in entry_keys(joinpath(path, "test.daf.zip"))
                return nothing
            end
        end

        nested_test("streaming_dense_matrix_fill_uneven_rows") do
            with_packed_streaming_dense_matrix_fill(zip_factory; n_rows = 5000) do _daf, _path
                return nothing
            end
        end

        nested_test("empty_dense_vector_packed_round_trip") do
            with_packed_empty_dense_vector_round_trip(zip_factory) do _daf, path
                @test "vectors/elem/data.shard" in entry_keys(joinpath(path, "test.daf.zip"))
                return nothing
            end
        end

        nested_test("below_threshold_stays_flat") do
            with_below_threshold_matrix_round_trip(zip_factory) do daf, path
                keys = entry_keys(joinpath(path, "test.daf.zip"))
                @test "matrices/row/col/data.data" in keys
                @test !("matrices/row/col/data.shard" in keys)
                return nothing
            end
        end

        nested_test("packed_sparse_matrix_round_trip") do
            with_packed_sparse_matrix_round_trip(zip_factory) do _daf, path
                zip_path = joinpath(path, "test.daf.zip")
                keys = entry_keys(zip_path)
                @test "matrices/row/col/data.colptr" in keys
                @test "matrices/row/col/data.rowval.shard" in keys
                @test "matrices/row/col/data.nzval.shard" in keys
                json = read_entry_json(zip_path, "matrices/row/col/data.json")
                @test get(json["colptr"], "packed", false) === false
                @test json["rowval"]["packed"] === true
                @test json["nzval"]["packed"] === true
                return nothing
            end
        end

        nested_test("byte_identical_to_files") do
            # The on-disk `.shard` bytes for a packed property are byte-identical to FilesDaf's, so unzip(ZipDaf)
            # produces a FilesDaf directory.
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))

                files_daf = FilesDaf(joinpath(path, "files.daf"), "w+"; name = "files!", packed = true)
                add_axis!(files_daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(files_daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(files_daf, "row", "col", "data", original; relayout = false)

                zip_daf = ZipDaf(joinpath(path, "zip.daf.zip"), "w+"; name = "zip!", packed = true)
                add_axis!(zip_daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(zip_daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(zip_daf, "row", "col", "data", original; relayout = false)

                files_shard = read(joinpath(path, "files.daf", "matrices", "row", "col", "data.shard"))
                zip_reader = ZipArchives.ZipReader(read(joinpath(path, "zip.daf.zip")))
                entry_index =
                    findfirst(name -> name == "matrices/row/col/data.shard", ZipArchives.zip_names(zip_reader))
                zip_shard = ZipArchives.zip_readentry(zip_reader, entry_index)
                @test files_shard == zip_shard
                return nothing
            end
        end

        # `empty_sparse_vector!` / `empty_sparse_matrix!` round-trip on `ZipDaf`. The reserve-and-fill lifecycle is
        # the only path that exercises `format_filled_empty_sparse_*!` (and its supporting `packed_reserve_typed_*!`
        # entry points) — `set_*!` writes the components in one shot.
        nested_test("empty_sparse_vector_lifecycle") do
            mktempdir() do path
                daf = zip_factory(path)
                add_axis!(daf, "elem", ["e1", "e2", "e3", "e4"])
                empty_sparse_vector!(daf, "elem", "data", Float32, 2, Int32) do nzind, nzval
                    nzind .= Int32[2, 4]
                    nzval .= Float32[10.5, 20.5]
                    return nothing
                end
                @test get_vector(daf, "elem", "data") == sparse_vector(Float32[0, 10.5, 0, 20.5])
                return nothing
            end
        end

        nested_test("empty_sparse_matrix_lifecycle") do
            mktempdir() do path
                daf = zip_factory(path)
                add_axis!(daf, "row", ["r1", "r2", "r3"])
                add_axis!(daf, "col", ["c1", "c2"])
                empty_sparse_matrix!(daf, "row", "col", "data", Float32, 2, Int32) do colptr, rowval, nzval
                    colptr .= Int32[1, 2, 3]
                    rowval .= Int32[1, 3]
                    nzval .= Float32[7.5, 9.5]
                    return nothing
                end
                expected = SparseMatrixCSC{Float32, Int32}(3, 2, Int32[1, 2, 3], Int32[1, 3], Float32[7.5, 9.5])
                @test SparseMatrixCSC{Float32, Int32}(parent(get_matrix(daf, "row", "col", "data"))) == expected
                return nothing
            end
        end
    end

    # `H5df` packed write / read paths use the same `chunks_for` decisions as `FilesDaf` / `ZipDaf` / `ZarrDaf` but
    # encode through HDF5's chunked-dataset layout with a registered filter pipeline (no `.shard` files; the bytes
    # live inside the HDF5 file). The tests exercise each variant end-to-end (write → read), validate the on-disk
    # chunk shape via `HDF5.get_chunk`, and assert that packed datasets surface as `H5dfDiskArray`-backed
    # `DiskArrays.CachedDiskArray` (dense) or `LazySparseMatrix` (sparse) on read while flat datasets keep today's
    # mmap fast path.
    nested_test("h5df") do
        function h5df_factory(
            path::AbstractString,
            mode::AbstractString = "w";
            packed::Bool = false,
        )::Union{H5df, DafReadOnly}
            return H5df(joinpath(path, "test.h5df"), mode; name = "h5df!", packed)
        end

        function h5df_dataset_at(daf::H5df, group_name::AbstractString, axes_and_name::AbstractString...)::HDF5.Dataset
            object = daf.root[group_name]
            for component in axes_and_name
                object = object[component]
            end
            @assert object isa HDF5.Dataset
            return object
        end

        nested_test("dense_matrix_round_trip") do
            with_packed_dense_matrix_round_trip(h5df_factory) do daf, _path
                dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data")
                @test HDF5.ischunked(dataset)
                @test HDF5.get_chunk(dataset) == (2048, 1)
                @test parent(parent(get_matrix(daf, "row", "col", "data"))) isa DiskArrays.CachedDiskArray
                return nothing
            end
        end

        nested_test("dense_vector_round_trip") do
            with_packed_dense_vector_round_trip(h5df_factory) do daf, _path
                dataset = h5df_dataset_at(daf, "vectors", "elem", "data")
                @test HDF5.ischunked(dataset)
                @test HDF5.get_chunk(dataset) == (2048,)
                return nothing
            end
        end

        nested_test("streaming_dense_matrix_fill") do
            with_packed_streaming_dense_matrix_fill(h5df_factory) do daf, _path
                dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data")
                @test HDF5.ischunked(dataset)
                @test HDF5.get_chunk(dataset) == (2048, 1)
                return nothing
            end
        end

        nested_test("streaming_dense_matrix_fill_uneven_rows") do
            # 5000 rows is not a multiple of 2048: the last row tile of every column is a partial chunk that HDF5
            # transparently zero-pads to the chunk shape. Validate that the round-trip recovers the original
            # matrix bytes (no corruption from the partial-tile path).
            with_packed_streaming_dense_matrix_fill(h5df_factory; n_rows = 5000) do daf, _path
                dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data")
                @test HDF5.ischunked(dataset)
                @test HDF5.get_chunk(dataset) == (2048, 1)
                return nothing
            end
        end

        nested_test("empty_dense_vector_packed_round_trip") do
            with_packed_empty_dense_vector_round_trip(h5df_factory) do daf, _path
                dataset = h5df_dataset_at(daf, "vectors", "elem", "data")
                @test HDF5.ischunked(dataset)
                @test HDF5.get_chunk(dataset) == (2048,)
                return nothing
            end
        end

        nested_test("below_threshold_stays_flat") do
            with_below_threshold_matrix_round_trip(h5df_factory) do daf, _path
                dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data")
                @test !HDF5.ischunked(dataset)
                @test HDF5.iscontiguous(dataset)
                return nothing
            end
        end

        nested_test("packed_sparse_matrix_round_trip") do
            with_packed_sparse_matrix_round_trip(h5df_factory) do daf, _path
                colptr_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data", "colptr")
                rowval_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data", "rowval")
                nzval_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data", "nzval")
                # 5 × Int32 = 20 bytes < threshold → flat. 32 768 × 4 bytes = 128 KB ≥ threshold → packed.
                @test !HDF5.ischunked(colptr_dataset)
                @test HDF5.iscontiguous(colptr_dataset)
                @test HDF5.ischunked(rowval_dataset)
                @test HDF5.get_chunk(rowval_dataset) == (2048,)
                @test HDF5.ischunked(nzval_dataset)
                @test HDF5.get_chunk(nzval_dataset) == (2048,)
                lazy_matrix = parent(parent(get_matrix(daf, "row", "col", "data")))
                @test lazy_matrix isa LazySparseMatrix{Float32, Int32}
                return nothing
            end
        end

        nested_test("empty_sparse_vector_lifecycle") do
            # `empty_sparse_vector!` lifecycle: `format_get_empty_sparse_vector!` returns in-RAM `Vector{I}` /
            # `Vector{T}`, the user fills them via the public API, and `format_filled_empty_sparse_vector!` writes
            # both components to disk via `write_packed_dense_dataset!`.
            mktempdir() do path
                n_elements = 16_384
                nnz_count = 4_096
                daf = h5df_factory(path; packed = true)
                add_axis!(daf, "elem", ["e$(index)" for index in 1:n_elements])
                empty_sparse_vector!(daf, "elem", "data", Float32, nnz_count, Int32) do nzind, nzval
                    nzind .= Int32.(1:nnz_count)
                    nzval .= Float32.(1:nnz_count)
                    return nothing
                end

                expected = sparse_vector(
                    [Float32(index) for index in 1:n_elements] .* [index <= nnz_count ? 1.0f0 : 0.0f0 for index in 1:n_elements],
                )
                @test get_vector(daf, "elem", "data") == expected
                # Both components above the byte threshold → packed datasets.
                nzind_dataset = h5df_dataset_at(daf, "vectors", "elem", "data", "nzind")
                nzval_dataset = h5df_dataset_at(daf, "vectors", "elem", "data", "nzval")
                @test HDF5.ischunked(nzind_dataset)
                @test HDF5.ischunked(nzval_dataset)
                return nothing
            end
        end

        nested_test("empty_sparse_matrix_lifecycle") do
            mktempdir() do path
                n_rows = 8192
                n_cols = 4
                nnz_per_col = 1024
                nnz_total = nnz_per_col * n_cols
                daf = h5df_factory(path; packed = true)
                add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                empty_sparse_matrix!(daf, "row", "col", "data", Float32, nnz_total, Int32) do colptr, rowval, nzval
                    for column in 1:n_cols
                        colptr[column] = Int32((column - 1) * nnz_per_col + 1)
                        nz_range = ((column - 1) * nnz_per_col + 1):(column * nnz_per_col)
                        rowval[nz_range] .= Int32.(1:nnz_per_col)
                        nzval[nz_range] .= Float32.((1:nnz_per_col) .+ (column - 1) * nnz_per_col)
                    end
                    colptr[n_cols + 1] = Int32(nnz_total + 1)
                    return nothing
                end

                lazy_matrix = parent(parent(get_matrix(daf, "row", "col", "data")))
                @test lazy_matrix isa LazySparseMatrix{Float32, Int32}
                # `colptr` (5 × Int32 = 20 B) stays flat; `rowval` and `nzval` (4096 × 4 B = 16 KB ≥ threshold) pack.
                colptr_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data", "colptr")
                rowval_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data", "rowval")
                nzval_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data", "nzval")
                @test !HDF5.ischunked(colptr_dataset)
                @test HDF5.ischunked(rowval_dataset)
                @test HDF5.ischunked(nzval_dataset)
                return nothing
            end
        end

        nested_test("packed_relayout_dense_matrix") do
            # `relayout = true` calls `format_relayout_matrix!` which (when the original direction packs) routes
            # through the streaming `PackedDenseMatrix` and must `flush_packed_dense_matrix!` to commit the bytes.
            # The 4096 × 3 shape packs in the original direction (column byte-size 16 KB ≥ threshold) but the
            # relayouted (3 × 4096) shape stays flat (`chunks_for` returns `nothing` because `shape[1] * sizeof(T)
            # < target_bytes` — the 3-row direction has only 12 bytes per "column"). Dual-packed relayout would
            # require `relayout!` to support a `PackedDenseMatrix` destination, which it currently doesn't (latent
            # gap shared with `FilesDaf` / `ZarrDaf` / `ZipDaf`).
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
                daf = h5df_factory(path; packed = true)
                add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(daf, "row", "col", "data", original; relayout = true)
                @test get_matrix(daf, "row", "col", "data") == original
                @test get_matrix(daf, "col", "row", "data") == original'
                row_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data")
                col_dataset = h5df_dataset_at(daf, "matrices", "col", "row", "data")
                @test HDF5.ischunked(row_dataset)
                @test !HDF5.ischunked(col_dataset)
                @test HDF5.iscontiguous(col_dataset)
                return nothing
            end
        end

        nested_test("packed_relayout_dense_matrix_dual_packed") do
            # Both directions cross the byte threshold: original (2048 × 2048) Float32 and relayouted
            # (2048 × 2048) Float32 — column byte-size 8 KB ≥ DAF_PACKED_TARGET_CHUNK_KB. The relayouted
            # destination is allocated as a `PackedDenseMatrix` streaming wrapper, and `relayout!` must walk
            # `MatrixLayouts.unnamed_relayout(::PackedDenseMatrix, ::AbstractMatrix)` to populate it column-
            # by-column. Validates the dual-packed-relayout fix.
            mktempdir() do path
                n_rows = 2048
                n_cols = 2048
                original = Matrix{Float32}(reshape(Float32.(1:(n_rows * n_cols)), n_rows, n_cols))
                daf = h5df_factory(path; packed = true)
                add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(daf, "row", "col", "data", original; relayout = true)
                @test get_matrix(daf, "row", "col", "data") == original
                @test get_matrix(daf, "col", "row", "data") == original'
                row_dataset = h5df_dataset_at(daf, "matrices", "row", "col", "data")
                col_dataset = h5df_dataset_at(daf, "matrices", "col", "row", "data")
                @test HDF5.ischunked(row_dataset)
                @test HDF5.ischunked(col_dataset)
                return nothing
            end
        end

        nested_test("packed_relayout_sparse_matrix") do
            mktempdir() do path
                n_rows = 8192
                n_cols = 4
                column_pointers = Int32[1 + (index - 1) * n_rows for index in 1:(n_cols + 1)]
                row_indices = Int32[((position - 1) % n_rows) + 1 for position in 1:(n_cols * n_rows)]
                nz_values = Float32[
                    ((position - 1) ÷ n_rows + 1) * (((position - 1) % n_rows) + 1) for position in 1:(n_cols * n_rows)
                ]
                original = SparseMatrixCSC{Float32, Int32}(n_rows, n_cols, column_pointers, row_indices, nz_values)
                daf = h5df_factory(path; packed = true)
                add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(daf, "row", "col", "data", original; relayout = true)
                @test get_matrix(daf, "row", "col", "data") == original
                @test get_matrix(daf, "col", "row", "data") == original'
                # The relayouted property's `rowval` / `nzval` should both pack.
                rowval_dataset = h5df_dataset_at(daf, "matrices", "col", "row", "data", "rowval")
                nzval_dataset = h5df_dataset_at(daf, "matrices", "col", "row", "data", "nzval")
                @test HDF5.ischunked(rowval_dataset)
                @test HDF5.ischunked(nzval_dataset)
                return nothing
            end
        end

        nested_test("hdf5_filters_for") do
            # `hdf5_filters_for` translates each user-facing `DAF_PACKED_COMPRESSION` symbol into the matching
            # `HDF5.Filters.Filter` chain. Cover all six codecs at the function level — full end-to-end packed
            # writes-per-codec are exercised indirectly by the default-codec tests above.
            for_codec = DataAxesFormats.H5dfFormat.hdf5_filters_for ∘ DataAxesFormats.PackedFormat.compressor_for

            blosc_zstd = for_codec(:blosc_zstd_bitshuffle)
            @test length(blosc_zstd) == 1
            @test blosc_zstd[1] isa BloscFilter
            @test blosc_zstd[1].shuffle == H5Zblosc.BITSHUFFLE

            blosc_lz4 = for_codec(:blosc_lz4_bitshuffle)
            @test length(blosc_lz4) == 1
            @test blosc_lz4[1] isa BloscFilter
            @test blosc_lz4[1].shuffle == H5Zblosc.BITSHUFFLE

            zstd_bs = for_codec(:zstd_bitshuffle)
            @test length(zstd_bs) == 1
            @test zstd_bs[1] isa BitshuffleFilter

            zstd = for_codec(:zstd)
            @test length(zstd) == 1
            @test zstd[1] isa ZstdFilter

            gzip = for_codec(:gzip)
            @test length(gzip) == 1
            @test gzip[1] isa HDF5.Filters.Deflate

            gzip_shuffle = for_codec(:gzip_shuffle)
            @test length(gzip_shuffle) == 2
            @test gzip_shuffle[1] isa HDF5.Filters.Shuffle
            @test gzip_shuffle[2] isa HDF5.Filters.Deflate
            return nothing
        end

        nested_test("packed_sparse_bool_all_true_round_trip") do
            # Bool sparse properties whose non-zero values are all `true` skip writing `nzval` on disk; the read
            # path then materialises `nzval_source = fill(true, length(rowval_dataset))` inside the
            # `LazySparseMatrix` arm. Need `rowval` packed to hit that arm: 2048 nonzeros × 4 B = 8 KB ≥ threshold.
            mktempdir() do path
                n_rows = 8192
                n_cols = 1
                nnz_total = 2048
                column_pointers = Int32[1, nnz_total + 1]
                row_indices = Int32.(1:nnz_total)
                nz_values = fill(true, nnz_total)
                original = SparseMatrixCSC{Bool, Int32}(n_rows, n_cols, column_pointers, row_indices, nz_values)
                daf = h5df_factory(path; packed = true)
                add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                add_axis!(daf, "col", ["c$(index)" for index in 1:n_cols])
                set_matrix!(daf, "row", "col", "data", original; relayout = false)

                matrix_object = daf.root["matrices"]["row"]["col"]["data"]
                @test matrix_object isa HDF5.Group
                @test !haskey(matrix_object, "nzval")  # all-true → nzval skipped on disk

                roundtripped = parent(parent(get_matrix(daf, "row", "col", "data")))
                @test roundtripped isa LazySparseMatrix{Bool, Int32}
                @test SparseMatrixCSC(roundtripped) == original
                return nothing
            end
        end
    end

    # Goal: `zip -r foo.daf.zip foo.daf/` over a packed FilesDaf produces a valid ZipDaf, and conversely
    # `unzip foo.daf.zip` of a packed ZipDaf produces a valid FilesDaf. Each direction is verified by handing the
    # bundled / unbundled tree to the other backend's reader and round-tripping the same dataset.
    nested_test("unzip_zip_equivalence") do
        nested_test("zip_filesdaf_then_open_as_zipdaf") do
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                files_path = joinpath(path, "source.daf")
                files_daf = FilesDaf(files_path, "w+"; name = "src!", packed = true)
                expected = populate_packed_round_trip!(files_daf, n_rows, n_cols)
                verify_packed_round_trip(files_daf, expected)

                # Mimic `zip -r bundled.daf.zip source.daf/` — every file in the FilesDaf tree becomes a zip entry
                # whose name is the path relative to the daf root. ZipDaf strips the FilesDaf-only `metadata.json`
                # sidecar on first writable open.
                zip_path = joinpath(path, "bundled.daf.zip")
                store = MmapZipStore(zip_path; writable = true, create = true)
                try
                    for (root, _, filenames) in walkdir(files_path)
                        for filename in filenames
                            absolute = joinpath(root, filename)
                            relative = relpath(absolute, files_path)
                            store[relative] = read(absolute)
                        end
                    end
                finally
                    close(store)
                end
                GC.gc()

                zip_daf = ZipDaf(zip_path, "r"; name = "bundled!")
                verify_packed_round_trip(zip_daf, expected)
                return nothing
            end
        end

        nested_test("unzip_zipdaf_then_open_as_filesdaf") do
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                zip_path = joinpath(path, "source.daf.zip")
                zip_daf = ZipDaf(zip_path, "w+"; name = "src!", packed = true)
                expected = populate_packed_round_trip!(zip_daf, n_rows, n_cols)
                verify_packed_round_trip(zip_daf, expected)
                GC.gc()

                # Mimic `unzip source.daf.zip -d unbundled.daf/` — every zip entry becomes a file at the matching
                # relative path. The unbundled directory has no `metadata.json` sidecar; FilesDaf rebuilds it on
                # first writable open.
                files_path = joinpath(path, "unbundled.daf")
                mkdir(files_path)
                source_reader = ZipArchives.ZipReader(read(zip_path))
                for entry_index in 1:length(ZipArchives.zip_names(source_reader))
                    name = ZipArchives.zip_names(source_reader)[entry_index]
                    absolute = joinpath(files_path, name)
                    mkpath(dirname(absolute))
                    open(absolute, "w") do io
                        return write(io, ZipArchives.zip_readentry(source_reader, entry_index))
                    end
                end

                files_daf = FilesDaf(files_path, "r"; name = "unbundled!")
                verify_packed_round_trip(files_daf, expected)
                return nothing
            end
        end
    end

    # Goal: `zip -r foo.daf.zarr.zip foo.daf.zarr/` over a packed ZarrDaf-Directory produces a valid ZarrDaf-Zip,
    # and conversely `unzip foo.daf.zarr.zip` of a packed ZarrDaf-Zip produces a valid ZarrDaf-Directory. The two
    # backends share Zarr.jl so the per-property `zarr.json` and chunk files are byte-identical between formats; the
    # only divergence is the inline `consolidated_metadata` field that the `DirectoryStore` backend embeds in the
    # root `zarr.json` and the ZIP backend does not — `ensure_consolidated_metadata!` rebuilds it on first writable
    # open of the unbundled tree.
    nested_test("zarr_unzip_zip_equivalence") do
        nested_test("zip_zarrdir_then_open_as_zarrzip") do
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                zarr_dir_path = joinpath(path, "source.daf.zarr")
                zarr_dir_daf = ZarrDaf(zarr_dir_path, "w+"; name = "src!", packed = true)
                expected = populate_packed_round_trip!(zarr_dir_daf, n_rows, n_cols)
                verify_packed_round_trip(zarr_dir_daf, expected)

                zip_path = joinpath(path, "bundled.daf.zarr.zip")
                store = MmapZipStore(zip_path; writable = true, create = true)
                try
                    for (root, _, filenames) in walkdir(zarr_dir_path)
                        for filename in filenames
                            absolute = joinpath(root, filename)
                            relative = relpath(absolute, zarr_dir_path)
                            store[relative] = read(absolute)
                        end
                    end
                finally
                    close(store)
                end
                GC.gc()

                zarr_zip_daf = ZarrDaf(zip_path, "r"; name = "bundled!")
                verify_packed_round_trip(zarr_zip_daf, expected)
                return nothing
            end
        end

        nested_test("unzip_zarrzip_then_open_as_zarrdir") do
            mktempdir() do path
                n_rows = 4096
                n_cols = 3
                zip_path = joinpath(path, "source.daf.zarr.zip")
                zarr_zip_daf = ZarrDaf(zip_path, "w+"; name = "src!", packed = true)
                expected = populate_packed_round_trip!(zarr_zip_daf, n_rows, n_cols)
                verify_packed_round_trip(zarr_zip_daf, expected)
                GC.gc()

                zarr_dir_path = joinpath(path, "unbundled.daf.zarr")
                mkdir(zarr_dir_path)
                source_reader = ZipArchives.ZipReader(read(zip_path))
                names_list = ZipArchives.zip_names(source_reader)
                for entry_index in 1:length(names_list)
                    relative = names_list[entry_index]
                    absolute = joinpath(zarr_dir_path, relative)
                    mkpath(dirname(absolute))
                    open(absolute, "w") do io
                        return write(io, ZipArchives.zip_readentry(source_reader, entry_index))
                    end
                end

                root_zarr_path = joinpath(zarr_dir_path, "zarr.json")
                @test !haskey(JSON.parse(read(root_zarr_path, String)), "consolidated_metadata")
                zarr_dir_daf = ZarrDaf(zarr_dir_path, "r+"; name = "unbundled!")
                @test haskey(JSON.parse(read(root_zarr_path, String)), "consolidated_metadata")
                verify_packed_round_trip(zarr_dir_daf, expected)
                return nothing
            end
        end
    end
end
