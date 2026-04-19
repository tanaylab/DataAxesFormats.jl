import DataAxesFormats.Formats

function cache_group_of_scalar(daf::DafReader, name::AbstractString)::Maybe{CacheGroup}
    entry = get(daf.internal.cache, Formats.scalar_cache_key(name), nothing)
    return entry === nothing ? nothing : entry.cache_group
end

function cache_group_of_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Maybe{CacheGroup}
    entry = get(daf.internal.cache, Formats.vector_cache_key(axis, name), nothing)
    return entry === nothing ? nothing : entry.cache_group
end

function cache_group_of_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Maybe{CacheGroup}
    entry = get(daf.internal.cache, Formats.matrix_cache_key(rows_axis, columns_axis, name), nothing)
    return entry === nothing ? nothing : entry.cache_group
end

function populate_data!(daf::DafWriter)::Nothing
    set_scalar!(daf, "version", "1.0")
    add_axis!(daf, "cell", ["A", "B", "C"])
    add_axis!(daf, "gene", ["X", "Y"])
    set_vector!(daf, "cell", "age", [1, 2, 3])
    set_matrix!(daf, "cell", "gene", "UMIs", [1 2; 3 4; 5 6])
    return nothing
end

nested_test("cache_groups") do
    nested_test("memory") do
        daf = MemoryDaf(; name = "memory!")
        populate_data!(daf)
        empty_cache!(daf)

        nested_test("reads_skip_cache") do
            @test get_scalar(daf, "version") == "1.0"
            @test cache_group_of_scalar(daf, "version") === nothing

            @test get_vector(daf, "cell", "age") == [1, 2, 3]
            @test cache_group_of_vector(daf, "cell", "age") === nothing

            @test get_matrix(daf, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6]
            @test cache_group_of_matrix(daf, "cell", "gene", "UMIs") === nothing
        end

        nested_test("empty_fills") do
            empty_dense_vector!(daf, "cell", "weight", Float32) do vector
                vector .= [0.5, 1.5, 2.5]
                return nothing
            end
            @test cache_group_of_vector(daf, "cell", "weight") === nothing

            empty_dense_matrix!(daf, "cell", "gene", "score", Float32) do matrix
                matrix .= Float32[0.1 0.2; 0.3 0.4; 0.5 0.6]
                return nothing
            end
            @test cache_group_of_matrix(daf, "cell", "gene", "score") === nothing
        end
    end

    nested_test("files") do
        mktempdir() do path
            daf = FilesDaf(path * "/files", "w+"; name = "files!")
            populate_data!(daf)
            empty_cache!(daf)

            nested_test("reads_cache_as_mapped") do
                @test get_scalar(daf, "version") == "1.0"
                @test cache_group_of_scalar(daf, "version") == MemoryData

                @test get_vector(daf, "cell", "age") == [1, 2, 3]
                @test cache_group_of_vector(daf, "cell", "age") == MappedData

                @test get_matrix(daf, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6]
                @test cache_group_of_matrix(daf, "cell", "gene", "UMIs") == MappedData
            end

            nested_test("empty_fills_cache_as_mapped") do
                empty_dense_vector!(daf, "cell", "weight", Float32) do vector
                    vector .= [0.5, 1.5, 2.5]
                    return nothing
                end
                @test cache_group_of_vector(daf, "cell", "weight") == MappedData

                empty_dense_matrix!(daf, "cell", "gene", "score", Float32) do matrix
                    matrix .= Float32[0.1 0.2; 0.3 0.4; 0.5 0.6]
                    return nothing
                end
                @test cache_group_of_matrix(daf, "cell", "gene", "score") == MappedData
            end

            nested_test("sparse_fill_caches_as_mapped") do
                empty_sparse_vector!(daf, "cell", "sparse_v", Float32, 2, Int32) do nzind, nzval
                    nzind .= Int32[1, 3]
                    nzval .= Float32[1.5, 2.5]
                    return nothing
                end
                @test cache_group_of_vector(daf, "cell", "sparse_v") == MappedData

                empty_sparse_matrix!(daf, "cell", "gene", "sparse_m", Float32, 2, Int32) do colptr, rowval, nzval
                    colptr .= Int32[1, 2, 3]
                    rowval .= Int32[1, 2]
                    nzval .= Float32[1.5, 2.5]
                    return nothing
                end
                @test cache_group_of_matrix(daf, "cell", "gene", "sparse_m") == MappedData
            end

            nested_test("clear_mapped_only") do
                get_vector(daf, "cell", "age")
                get_scalar(daf, "version")
                @test cache_group_of_vector(daf, "cell", "age") == MappedData
                @test cache_group_of_scalar(daf, "version") == MemoryData

                empty_cache!(daf; clear = MappedData)
                @test cache_group_of_vector(daf, "cell", "age") === nothing
                @test cache_group_of_scalar(daf, "version") == MemoryData
            end
        end
    end

    nested_test("h5df") do
        mktempdir() do path
            daf = H5df(path * "/test.h5df", "w+"; name = "h5df!")
            populate_data!(daf)
            empty_cache!(daf)

            nested_test("reads_cache_as_mapped") do
                @test get_scalar(daf, "version") == "1.0"
                @test cache_group_of_scalar(daf, "version") == MemoryData

                @test get_vector(daf, "cell", "age") == [1, 2, 3]
                @test cache_group_of_vector(daf, "cell", "age") == MappedData

                @test get_matrix(daf, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6]
                @test cache_group_of_matrix(daf, "cell", "gene", "UMIs") == MappedData
            end

            nested_test("empty_fills_cache_as_mapped") do
                empty_dense_vector!(daf, "cell", "weight", Float32) do vector
                    vector .= [0.5, 1.5, 2.5]
                    return nothing
                end
                @test cache_group_of_vector(daf, "cell", "weight") == MappedData

                empty_dense_matrix!(daf, "cell", "gene", "score", Float32) do matrix
                    matrix .= Float32[0.1 0.2; 0.3 0.4; 0.5 0.6]
                    return nothing
                end
                @test cache_group_of_matrix(daf, "cell", "gene", "score") == MappedData
            end
        end
    end

    nested_test("view") do
        daf = MemoryDaf(; name = "memory!")
        populate_data!(daf)
        empty_cache!(daf)

        view = viewer(
            daf;
            name = "view!",
            axes = [VIEW_ALL_AXES],
            data = [VIEW_ALL_SCALARS, VIEW_ALL_VECTORS, VIEW_ALL_MATRICES],
        )
        empty_cache!(view)

        @test get_scalar(view, "version") == "1.0"
        @test cache_group_of_scalar(view, "version") == QueryData

        @test get_vector(view, "cell", "age") == [1, 2, 3]
        @test cache_group_of_vector(view, "cell", "age") == QueryData

        @test get_matrix(view, "cell", "gene", "UMIs") == [1 2; 3 4; 5 6]
        @test cache_group_of_matrix(view, "cell", "gene", "UMIs") == QueryData
    end
end
