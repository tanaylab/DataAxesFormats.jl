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

    nested_test("packed_dense_matrix_stub") do
        matrix = DataAxesFormats.PackedFormat.PackedDenseMatrix{Float32}(5000, 30000)
        @test size(matrix) == (5000, 30000)
        @test eltype(matrix) === Float32
    end

    nested_test("http_striped_matrix_stub") do
        matrix = DataAxesFormats.PackedFormat.HttpStripedMatrix{Float32}(
            "http://example.com/data",
            128,
            5000,
            30000,
            2048,
        )
        @test size(matrix) == (5000, 30000)
        @test eltype(matrix) === Float32
        @test matrix.url == "http://example.com/data"
        @test matrix.header_size == 128
        @test matrix.stripe_n_rows == 2048
    end

    nested_test("http_striped_vector_stub") do
        vector = DataAxesFormats.PackedFormat.HttpStripedVector{Int32}(
            "http://example.com/data",
            64,
            1_000_000,
            2048,
        )
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
end
