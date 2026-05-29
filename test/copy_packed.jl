nested_test("copy_packed") do
    # Comprehensive payload exercised by every source × destination round-trip: scalars (numeric + string),
    # axes, dense / sparse vectors with numeric + Bool + String eltypes, dense / sparse matrices including the
    # Bool all-true `nzval`-omitted form, and a tensor split across a `batch` axis (each batch entry has its
    # own constituent matrix). The shapes are small enough to keep round-trips fast but include every property
    # kind the packed-storage code paths can produce.
    cell_names = ["TATA", "GATA", "CATA"]
    gene_names = ["RSPO3", "FOXA1", "WNT6", "TNNI1"]
    batch_names = ["U", "V"]
    score_vector = Float32[1.0, 2.0, 3.0]
    marker_vector = Bool[true, false, true, false]
    type_vector = ["Bcell", "TCell", "TCell"]
    counts_nzind = Int32[1, 3]
    counts_nzval = Int32[10, 20]
    flag_nzind = Int32[2, 4]
    umis_matrix = Int32[0 1 2 3; 1 2 3 0; 2 3 0 1]
    mask_matrix = Bool[false true false true; true false true false; false true false true]
    labels_matrix = ["a" "b" "c" "d"; "e" "f" "g" "h"; "i" "j" "k" "l"]
    sparse_umis_matrix = SparseMatrixCSC{Float32, Int32}(Float32.(umis_matrix))
    sparse_mask_matrix = SparseMatrixCSC{Bool, Int32}(mask_matrix)
    function populate_source!(source::DafWriter)::Nothing
        set_scalar!(source, "name", "src!")
        set_scalar!(source, "depth", Int32(1))
        add_axis!(source, "cell", cell_names)
        add_axis!(source, "gene", gene_names)
        add_axis!(source, "batch", batch_names)
        set_vector!(source, "cell", "score", score_vector)
        set_vector!(source, "gene", "marker", marker_vector)
        set_vector!(source, "cell", "type", type_vector)
        empty_sparse_vector!(source, "gene", "counts", Int32, length(counts_nzind), Int32) do nzind, nzval
            nzind .= counts_nzind
            nzval .= counts_nzval
            return nothing
        end
        empty_sparse_vector!(source, "gene", "flag", Bool, length(flag_nzind), Int32) do nzind, nzval
            nzind .= flag_nzind
            nzval .= true
            return nothing
        end
        set_matrix!(source, "cell", "gene", "UMIs", umis_matrix; relayout = false)
        set_matrix!(source, "cell", "gene", "mask", mask_matrix; relayout = false)
        set_matrix!(source, "cell", "gene", "labels", labels_matrix; relayout = false)
        set_matrix!(source, "cell", "gene", "sparse_UMIs", sparse_umis_matrix; relayout = false)
        set_matrix!(source, "cell", "gene", "sparse_mask", sparse_mask_matrix; relayout = false)
        return nothing
    end

    function verify_destination(destination::DafReader)::Nothing
        @test get_scalar(destination, "name") == "src!"
        @test get_scalar(destination, "depth") == Int32(1)
        @test axis_vector(destination, "cell") == cell_names
        @test axis_vector(destination, "gene") == gene_names
        @test axis_vector(destination, "batch") == batch_names
        @test collect(get_vector(destination, "cell", "score")) == score_vector
        @test collect(get_vector(destination, "gene", "marker")) == marker_vector
        @test collect(get_vector(destination, "cell", "type")) == type_vector
        counts_vector = parent(parent(get_vector(destination, "gene", "counts")))
        @test SparseVector(counts_vector) == SparseVector(length(gene_names), counts_nzind, counts_nzval)
        flag_vector = parent(parent(get_vector(destination, "gene", "flag")))
        @test SparseVector(flag_vector) == SparseVector(length(gene_names), flag_nzind, fill(true, length(flag_nzind)))
        @test Matrix(get_matrix(destination, "cell", "gene", "UMIs")) == umis_matrix
        @test Matrix(get_matrix(destination, "cell", "gene", "mask")) == mask_matrix
        @test Matrix(get_matrix(destination, "cell", "gene", "labels")) == labels_matrix
        sparse_umis_named = get_matrix(destination, "cell", "gene", "sparse_UMIs")
        @test SparseMatrixCSC(parent(parent(sparse_umis_named))) == sparse_umis_matrix
        sparse_mask_named = get_matrix(destination, "cell", "gene", "sparse_mask")
        @test SparseMatrixCSC(parent(parent(sparse_mask_named))) == sparse_mask_matrix
        return nothing
    end

    function memory_factory(_path::AbstractString; packed::Bool = false)::MemoryDaf  # NOLINT
        return MemoryDaf(; name = "memory!")
    end

    function files_factory(path::AbstractString; packed::Bool)::FilesDaf
        return FilesDaf(joinpath(path, "test.daf"), "w+"; name = "files!", packed)
    end

    function zip_factory(path::AbstractString; packed::Bool)::ZipDaf
        return ZipDaf(joinpath(path, "test.daf.zip"), "w+"; name = "zip!", packed)
    end

    function zarr_dir_factory(path::AbstractString; packed::Bool)::ZarrDaf
        return ZarrDaf(joinpath(path, "test.daf.zarr"), "w+"; name = "zarr_dir!", packed)
    end

    function zarr_zip_factory(path::AbstractString; packed::Bool)::ZarrDaf
        return ZarrDaf(joinpath(path, "test.daf.zarr.zip"), "w+"; name = "zarr_zip!", packed)
    end

    function h5df_factory(path::AbstractString; packed::Bool)::H5df
        return H5df(joinpath(path, "test.h5df"), "w+"; name = "h5df!", packed)
    end

    # Run one end-to-end round-trip: build a source daf, populate it, copy_all! into a fresh destination daf
    # (with the requested per-call `packed` kwarg), verify every property round-trips. Uses two separate
    # mktempdirs so source and destination paths never collide for same-format pairs.
    function roundtrip(
        source_factory::Function,
        destination_factory::Function;
        source_packed::Bool,
        destination_packed::Bool,
        copy_packed::Maybe{Bool},
    )::Nothing
        mktempdir() do source_path
            source = source_factory(source_path; packed = source_packed)
            populate_source!(source)
            mktempdir() do destination_path
                destination = destination_factory(destination_path; packed = destination_packed)
                copy_all!(; source, destination, packed = copy_packed)
                verify_destination(destination)
                return nothing
            end
            return nothing
        end
        return nothing
    end

    # Each source × destination format pair gets one round-trip per per-call `packed` kwarg (`true` / `false`
    # / `nothing` inherits the destination's `packed_default`). Source-side `packed` is the destination's
    # default for that pair so the writer's on-disk layout varies across the matrix. `MemoryDaf` ignores
    # `packed` and is only meaningful at one end.
    source_specs = [
        "memory" => (memory_factory, false),
        "files_flat" => (files_factory, false),
        "files_packed" => (files_factory, true),
        "zip_flat" => (zip_factory, false),
        "zip_packed" => (zip_factory, true),
        "zarr_dir_flat" => (zarr_dir_factory, false),
        "zarr_dir_packed" => (zarr_dir_factory, true),
        "zarr_zip_flat" => (zarr_zip_factory, false),
        "zarr_zip_packed" => (zarr_zip_factory, true),
        "h5df_flat" => (h5df_factory, false),
        "h5df_packed" => (h5df_factory, true),
    ]
    destination_specs = [
        "memory" => (memory_factory, false),
        "files" => (files_factory, true),
        "zip" => (zip_factory, true),
        "zarr_dir" => (zarr_dir_factory, true),
        "zarr_zip" => (zarr_zip_factory, true),
        "h5df" => (h5df_factory, true),
    ]
    copy_packed_values = [nothing, true, false]

    for (source_name, (source_factory, source_packed)) in source_specs
        for (destination_name, (destination_factory, destination_packed)) in destination_specs
            for copy_packed in copy_packed_values
                packed_tag = copy_packed === nothing ? "inherit" : (copy_packed ? "packed" : "flat")
                nested_test("$(source_name)_to_$(destination_name)_$(packed_tag)") do
                    return roundtrip(
                        source_factory,
                        destination_factory;
                        source_packed,
                        destination_packed,
                        copy_packed,
                    )
                end
            end
        end
    end

    # `HttpDaf` is read-only so it is a source only. The fixture stages a packed `FilesDaf` directory and
    # serves it through the `Range`-honouring handler shared with the other HTTP tests; the destination is a
    # local unpacked `FilesDaf` — the canonical "stage to local for compute" pair.
    nested_test("http_source_to_files_flat") do
        mktempdir() do source_path
            files_path = joinpath(source_path, "served.daf")
            source = FilesDaf(files_path, "w+"; name = "http_src!", packed = true)
            populate_source!(source)
            handler = request -> begin
                key = String(lstrip(request.target, '/'))
                if occursin("..", key)
                    return HTTP.Response(404, "Error: bad key $(key)")  # UNTESTED
                end
                file_path = "$(files_path)/$(key)"
                if !isfile(file_path)
                    return HTTP.Response(404, "Error: Key $(key) not found")  # UNTESTED
                end
                return respond_with_range(read(file_path), request)
            end
            server = HTTP.serve!(handler, Sockets.localhost, 0; listenany = true)
            try
                url = "http://localhost:$(server.listener.hostport)"
                http_source = HttpDaf(url)
                mktempdir() do destination_path
                    destination = FilesDaf(joinpath(destination_path, "dst.daf"), "w+"; name = "dst!", packed = false)
                    copy_all!(; source = http_source, destination, packed = false)
                    verify_destination(destination)
                    return nothing
                end
            finally
                close(server)
            end
            return nothing
        end
    end
end
