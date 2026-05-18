nested_test("lazy_sparse") do
    # Build a packed sparse matrix through `ZarrDaf` once so the `nzval` and `rowval` ZArrays end up sharded; the
    # nested tests then wrap those ZArrays in a `LazySparseMatrix` and exercise slicing without ever materialising.
    function build_lazy(action::Function)::Nothing
        mktempdir() do path
            n_rows = 8192
            n_columns = 4
            column_pointers = Int32[1 + (column_index - 1) * n_rows for column_index in 1:(n_columns + 1)]
            row_indices = Int32[((position - 1) % n_rows) + 1 for position in 1:(n_columns * n_rows)]
            nz_values = Float32[
                ((position - 1) ÷ n_rows + 1) * (((position - 1) % n_rows) + 1) for position in 1:(n_columns * n_rows)
            ]
            original = SparseMatrixCSC{Float32, Int32}(n_rows, n_columns, column_pointers, row_indices, nz_values)

            daf = ZarrDaf(joinpath(path, "test.daf.zarr"), "w+"; name = "lazy!", packed = true)
            add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
            add_axis!(daf, "col", ["c$(index)" for index in 1:n_columns])
            set_matrix!(daf, "row", "col", "data", original; relayout = false)

            matrix_group = daf.root.groups["matrices"].groups["row"].groups["col"].groups["data"]
            rowval_source = matrix_group.arrays["rowval"]
            nzval_source = matrix_group.arrays["nzval"]

            lazy = LazySparseMatrix(n_rows, copy(column_pointers), rowval_source, nzval_source)
            action(lazy, rowval_source, nzval_source)
            return nothing
        end
        return nothing
    end

    nested_test("constructs_over_zarr_sources") do
        build_lazy() do lazy, _, _
            @test size(lazy) == (8192, 4)
            @test eltype(lazy) === Float32
            @test lazy.full_n_rows == 8192
            @test lazy.full_n_columns == 4
            @test length(lazy.full_colptr) == 5
            @test lazy.row_select isa AllOf
            @test lazy.column_select isa AllOf
            @test lazy.materialized === nothing
            return nothing
        end
    end

    nested_test("selection_lengths") do
        @test length(AllOf(7)) == 7
        @test length(RangeOf(3:9)) == 7
        @test length(IndicesOf([2, 5, 8, 11])) == 4
        @test length(MaskOf(BitVector([true, false, true, true, false]))) == 3
        return nothing
    end

    # Exercise the `compose(new, existing)` rules directly, independent of the matrix wrapper. Each rule's
    # pure-Julia output is checked here so the slicing tests below can rely on the correct algebra.
    nested_test("compose") do
        compose = DataAxesFormats.LazySparse.compose

        nested_test("allof_collapses") do
            @test compose(AllOf(5), RangeOf(10:14)) === RangeOf(10:14) ||
                  compose(AllOf(5), RangeOf(10:14)).range == 10:14
            existing_indices = IndicesOf([2, 4, 6])
            @test compose(AllOf(3), existing_indices) === existing_indices
            new_range = RangeOf(2:5)
            @test compose(new_range, AllOf(10)) === new_range
            return nothing
        end

        nested_test("range_then_range") do
            result = compose(RangeOf(2:4), RangeOf(10:20))
            @test result isa RangeOf
            @test result.range == 11:13
            return nothing
        end

        nested_test("range_then_indices") do
            result = compose(RangeOf(2:3), IndicesOf([7, 11, 13, 17, 19]))
            @test result isa IndicesOf
            @test result.indices == [11, 13]
            return nothing
        end

        nested_test("range_then_mask") do
            result = compose(RangeOf(2:3), MaskOf(BitVector([false, true, true, false, true, true])))
            @test result isa IndicesOf
            @test result.indices == [3, 5]
            return nothing
        end

        nested_test("indices_then_range") do
            result = compose(IndicesOf([1, 3]), RangeOf(10:14))
            @test result isa IndicesOf
            @test result.indices == [10, 12]
            return nothing
        end

        nested_test("indices_then_indices") do
            result = compose(IndicesOf([1, 3]), IndicesOf([5, 7, 9, 11]))
            @test result isa IndicesOf
            @test result.indices == [5, 9]
            return nothing
        end

        nested_test("indices_then_mask") do
            result = compose(IndicesOf([1, 3]), MaskOf(BitVector([true, false, true, true, false])))
            @test result isa IndicesOf
            @test result.indices == [1, 4]
            return nothing
        end

        nested_test("mask_then_range") do
            result = compose(MaskOf(BitVector([true, false, true, true])), RangeOf(10:13))
            @test result isa IndicesOf
            @test result.indices == [10, 12, 13]
            return nothing
        end

        nested_test("mask_then_indices") do
            result = compose(MaskOf(BitVector([true, false, true])), IndicesOf([5, 7, 9]))
            @test result isa IndicesOf
            @test result.indices == [5, 9]
            return nothing
        end

        nested_test("mask_then_mask") do
            result = compose(MaskOf(BitVector([true, false, true])), MaskOf(BitVector([true, true, false, true])))
            @test result isa MaskOf
            @test result.mask == BitVector([true, false, false, true])
            return nothing
        end
    end

    # `Base.getindex` on a `LazySparseMatrix` rebinds row / column selections; `with_selections` resets the
    # `materialized` cache and re-uses the underlying `full_colptr` / `rowval_source` / `nzval_source` by reference.
    # The `ZArray` sources are checked with `===` to confirm slicing does not allocate new arrays and never touches
    # the chunked storage.
    nested_test("slicing") do
        nested_test("column_range") do
            build_lazy() do lazy, rowval_source, nzval_source
                sliced = lazy[:, 2:3]
                @test size(sliced) == (8192, 2)
                @test length(sliced.row_select) == 8192
                @test length(sliced.column_select) == 2
                @test sliced.rowval_source === rowval_source
                @test sliced.nzval_source === nzval_source
                @test sliced.materialized === nothing
                return nothing
            end
        end

        nested_test("row_range") do
            build_lazy() do lazy, _, _
                sliced = lazy[1024:2048, :]
                @test size(sliced) == (1025, 4)
                @test length(sliced.row_select) == 1025
                @test length(sliced.column_select) == 4
                return nothing
            end
        end

        nested_test("column_indices") do
            build_lazy() do lazy, _, _
                sliced = lazy[:, [1, 3]]
                @test size(sliced) == (8192, 2)
                @test sliced.column_select isa IndicesOf
                @test sliced.column_select.indices == [1, 3]
                return nothing
            end
        end

        nested_test("row_mask") do
            build_lazy() do lazy, _, _
                mask = falses(8192)
                mask[1:512] .= true
                sliced = lazy[mask, :]
                @test size(sliced) == (512, 4)
                @test sliced.row_select isa MaskOf
                @test count(sliced.row_select.mask) == 512
                return nothing
            end
        end

        nested_test("chained_compose_via_indices") do
            build_lazy() do lazy, _, _
                first_slice = lazy[:, 1:3]
                second_slice = first_slice[:, [1, 3]]
                @test size(second_slice) == (8192, 2)
                @test second_slice.column_select isa IndicesOf
                @test second_slice.column_select.indices == [1, 3]
                return nothing
            end
        end

        # Direct dispatch on the explicit ambiguity overrides — `SparseArrays`'s parametric `getindex(::SparseMatrixCSC,
        # ...)` methods are more specific on indexer types than the `LazySparseMatrix.getindex(::NonColonIndexer,
        # ::NonColonIndexer)` fallback, so each combination below has its own override that delegates to
        # `slice_with_indexers`.
        nested_test("ambiguity_override_bool_bool") do
            build_lazy() do lazy, _, _
                row_mask = falses(8192)
                row_mask[1:512] .= true
                column_mask = BitVector([true, false, true, false])
                sliced = lazy[row_mask, column_mask]
                @test size(sliced) == (512, 2)
                @test sliced.row_select isa MaskOf
                @test sliced.column_select isa MaskOf
                return nothing
            end
        end

        nested_test("ambiguity_override_indices_bool") do
            build_lazy() do lazy, _, _
                column_mask = BitVector([true, false, true, false])
                sliced = lazy[[1, 100, 8192], column_mask]
                @test size(sliced) == (3, 2)
                @test sliced.row_select isa IndicesOf
                @test sliced.column_select isa MaskOf
                return nothing
            end
        end

        nested_test("ambiguity_override_range_bool") do
            build_lazy() do lazy, _, _
                column_mask = BitVector([true, false, true, false])
                sliced = lazy[1:100, column_mask]
                @test size(sliced) == (100, 2)
                @test sliced.row_select isa RangeOf
                @test sliced.column_select isa MaskOf
                return nothing
            end
        end
    end

    # Materialisation triggers and the cached `SparseMatrixCSC`. The test data is a fully-populated CSC matrix
    # where every (row, column) pair is a structural nonzero with value `column * row`, so each materialised slice's
    # contents are predictable from the slicing parameters.
    nested_test("materialise") do
        nested_test("full_matrix_round_trip") do
            build_lazy() do lazy, _, _
                materialised = SparseMatrixCSC(lazy)
                @test size(materialised) == (8192, 4)
                @test nnz(materialised) == 8192 * 4
                @test materialised[1, 1] == 1
                @test materialised[2, 3] == 6
                @test materialised[100, 4] == 400
                @test lazy.materialized === materialised
                return nothing
            end
        end

        nested_test("column_range_slice") do
            build_lazy() do lazy, _, _
                sliced = lazy[:, 2:3]
                materialised = SparseMatrixCSC(sliced)
                @test size(materialised) == (8192, 2)
                @test nnz(materialised) == 8192 * 2
                @test materialised[1, 1] == 2 * 1
                @test materialised[5, 2] == 3 * 5
                return nothing
            end
        end

        nested_test("row_mask_slice") do
            build_lazy() do lazy, _, _
                mask = falses(8192)
                mask[1] = true
                mask[100] = true
                mask[8192] = true
                sliced = lazy[mask, :]
                materialised = SparseMatrixCSC(sliced)
                @test size(materialised) == (3, 4)
                @test nnz(materialised) == 3 * 4
                @test materialised[1, 1] == 1
                @test materialised[2, 1] == 100
                @test materialised[3, 4] == 4 * 8192
                return nothing
            end
        end

        nested_test("rowvals_nonzeros_getcolptr_consistent") do
            build_lazy() do lazy, _, _
                sliced = lazy[:, [1, 3]]
                @test sliced.materialized === nothing
                colptr = SparseArrays.getcolptr(sliced)
                @test sliced.materialized !== nothing
                @test length(colptr) == 3
                @test colptr[1] == 1
                @test colptr[end] == nnz(sliced) + 1
                # Repeat calls return the cache (`===` on the result vectors).
                @test SparseArrays.getcolptr(sliced) === colptr
                @test rowvals(sliced) === rowvals(sliced.materialized)
                @test nonzeros(sliced) === nonzeros(sliced.materialized)
                return nothing
            end
        end

        nested_test("nnz_triggers_materialise") do
            build_lazy() do lazy, _, _
                sliced = lazy[1024:2048, :][:, [1, 3]]
                @test sliced.materialized === nothing
                @test nnz(sliced) == (2048 - 1024 + 1) * 2
                @test sliced.materialized !== nothing
                return nothing
            end
        end

        # `convert(SparseMatrixCSC{...}, lazy)` triggers the same materialisation pipeline as the call constructor
        # `SparseMatrixCSC(lazy)`, exercising the explicit `Base.convert` override (separate from the inner constructor
        # method) that user code reaches through `convert`-style coercion sites.
        nested_test("convert_triggers_materialise") do
            build_lazy() do lazy, _, _
                materialised = convert(SparseMatrixCSC{Float32, Int32}, lazy)
                @test materialised isa SparseMatrixCSC{Float32, Int32}
                @test size(materialised) == (8192, 4)
                @test nnz(materialised) == 8192 * 4
                @test lazy.materialized === materialised
                return nothing
            end
        end
    end

    # Scalar `Base.getindex(lazy, ::Int, ::Int)` decompresses the target column on the fly without populating
    # the materialised cache.
    nested_test("scalar_getindex") do
        nested_test("returns_value_without_materialise") do
            build_lazy() do lazy, _, _
                @test lazy[1, 1] == 1
                @test lazy[100, 3] == 3 * 100
                @test lazy[8192, 4] == 4 * 8192
                @test lazy.materialized === nothing
                return nothing
            end
        end

        nested_test("on_sliced_matrix") do
            build_lazy() do lazy, _, _
                sliced = lazy[:, 2:3]
                @test sliced[1, 1] == 2 * 1
                @test sliced[5, 2] == 3 * 5
                @test sliced.materialized === nothing
                return nothing
            end
        end

        nested_test("delegates_to_cache_after_materialise") do
            build_lazy() do lazy, _, _
                sliced = lazy[:, [1, 3]]
                _ = SparseMatrixCSC(sliced)
                @test sliced.materialized !== nothing
                @test sliced[1, 1] == 1 * 1
                @test sliced[100, 2] == 3 * 100
                return nothing
            end
        end

        # Hand-built `LazySparseMatrix` with a structurally empty middle column (`full_colptr[2] == full_colptr[3]`)
        # so the scalar `getindex` path returns `zero(Tv)` for any `(row, 2)` cell without ever touching the rowval /
        # nzval sources.
        nested_test("returns_zero_for_empty_column") do
            full_colptr = Int32[1, 3, 3, 5]
            rowval_source = Int32[1, 2, 1, 3]
            nzval_source = Float32[10, 20, 30, 40]
            empty_col_lazy = LazySparseMatrix(3, full_colptr, rowval_source, nzval_source)
            @test empty_col_lazy[1, 1] == 10
            @test empty_col_lazy[2, 1] == 20
            @test empty_col_lazy[1, 2] == 0
            @test empty_col_lazy[3, 2] == 0
            @test empty_col_lazy[1, 3] == 30
            @test empty_col_lazy[3, 3] == 40
            @test empty_col_lazy.materialized === nothing
            return nothing
        end
    end

    # Unit tests for `LazySparseVector` exercise the materialisation paths that the format-level tests don't:
    # unsorted output from non-trivial selections, `convert(SparseVector, ...)`, and scalar `getindex` after
    # the materialisation cache has been populated.
    nested_test("lazy_sparse_vector_unit") do
        nested_test("materialise_unsorted_indices") do
            vector = LazySparseVector(10, Int32[2, 5, 8], Float32[20.0, 50.0, 80.0])
            sliced = vector[[8, 2, 5]]
            @test SparseVector(sliced) == SparseVector(3, [1, 2, 3], Float32[80.0, 20.0, 50.0])
            return nothing
        end

        nested_test("cached_scalar_getindex") do
            vector = LazySparseVector(10, Int32[2, 5, 8], Float32[20.0, 50.0, 80.0])
            SparseVector(vector)
            @test vector.materialized !== nothing
            @test vector[2] == Float32(20.0)
            @test vector[3] == Float32(0.0)
            return nothing
        end
    end

    # `format_get_matrix` for a packed sparse property routes through `LazySparseMatrix` on every backend that
    # produces sharded `nzval` / `rowval` storage (FilesDaf, ZarrDaf-Directory, ZarrDaf-Zip). Below-threshold sparse
    # properties keep the eager mmap-backed `SparseMatrixCSC` path. Each backend's daf is built through the public
    # `set_matrix!` API so the test exercises the same write/read pipeline as user code.
    nested_test("format_get_matrix") do
        function build_packed_sparse_daf(
            create_daf::Function,
            path::AbstractString,
        )::Tuple{Any, SparseMatrixCSC{Float32, Int32}}
            n_rows = 8192
            n_columns = 4
            column_pointers = Int32[1 + (column_index - 1) * n_rows for column_index in 1:(n_columns + 1)]
            row_indices = Int32[((position - 1) % n_rows) + 1 for position in 1:(n_columns * n_rows)]
            nz_values = Float32[
                ((position - 1) ÷ n_rows + 1) * (((position - 1) % n_rows) + 1) for position in 1:(n_columns * n_rows)
            ]
            original = SparseMatrixCSC{Float32, Int32}(n_rows, n_columns, column_pointers, row_indices, nz_values)
            daf = create_daf(path; packed = true)
            add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
            add_axis!(daf, "col", ["c$(index)" for index in 1:n_columns])
            set_matrix!(daf, "row", "col", "data", original; relayout = false)
            return (daf, original)
        end

        function build_unpacked_sparse_daf(
            create_daf::Function,
            path::AbstractString,
        )::Tuple{Any, SparseMatrixCSC{Float32, Int32}}
            original = sparse_matrix_csc(Float32[1 0 3 0; 0 6 0 8; 9 0 0 12])
            daf = create_daf(path; packed = true)
            add_axis!(daf, "row", ["r1", "r2", "r3"])
            add_axis!(daf, "col", ["c1", "c2", "c3", "c4"])
            set_matrix!(daf, "row", "col", "data", original; relayout = false)
            return (daf, original)
        end

        function check_packed_lazy_read(daf, original::SparseMatrixCSC)::Nothing
            named = get_matrix(daf, "row", "col", "data")
            wrapped = parent(parent(named))
            @test wrapped isa LazySparseMatrix
            @test size(wrapped) == size(original)
            @test wrapped.materialized === nothing
            @test SparseMatrixCSC(wrapped) == original
            return nothing
        end

        function check_eager_below_threshold_read(daf, original::SparseMatrixCSC)::Nothing
            named = get_matrix(daf, "row", "col", "data")
            wrapped = parent(parent(named))
            @test wrapped isa SparseMatrixCSC
            @test wrapped == original
            return nothing
        end

        nested_test("files") do
            files_factory(path; packed) = FilesDaf(joinpath(path, "test.daf"), "w+"; name = "lazy_files!", packed)
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_daf(files_factory, path)
                    check_packed_lazy_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_daf(files_factory, path)
                    check_eager_below_threshold_read(daf, original)
                    return nothing
                end
            end
            # Mixed encoding: an `Int64` indtype with a `Float32` eltype puts `rowval` above and `nzval` below the
            # `chunks_for` threshold, so the lazy path opens `nzval` through the unpacked-source branch (mmap-backed
            # `Vector` rather than the packed `ZArray`).
            nested_test("mixed_packed_flat_components") do
                mktempdir() do path
                    n_rows = 2000
                    n_columns = 2
                    nnz_per_column = 750
                    column_pointers = Int64[1, nnz_per_column + 1, 2 * nnz_per_column + 1]
                    row_indices = Int64[((i - 1) % nnz_per_column) + 1 for i in 1:(2 * nnz_per_column)]
                    nz_values = Float32[i for i in 1:(2 * nnz_per_column)]
                    original =
                        SparseMatrixCSC{Float32, Int64}(n_rows, n_columns, column_pointers, row_indices, nz_values)
                    daf = files_factory(path; packed = true)
                    add_axis!(daf, "row", ["r$(index)" for index in 1:n_rows])
                    add_axis!(daf, "col", ["c$(index)" for index in 1:n_columns])
                    set_matrix!(daf, "row", "col", "data", original; relayout = false)
                    named = get_matrix(daf, "row", "col", "data")
                    wrapped = parent(parent(named))
                    @test wrapped isa LazySparseMatrix
                    @test SparseMatrixCSC(wrapped) == original
                    return nothing
                end
            end
        end

        nested_test("zarr_directory") do
            zarr_factory(path; packed) = ZarrDaf(joinpath(path, "test.daf.zarr"), "w+"; name = "lazy_zarr_dir!", packed)
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_daf(zarr_factory, path)
                    check_packed_lazy_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_daf(zarr_factory, path)
                    check_eager_below_threshold_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("zarr_zip") do
            function zarr_zip_factory(path; packed)
                return ZarrDaf(joinpath(path, "test.daf.zarr.zip"), "w+"; name = "lazy_zarr_zip!", packed)
            end
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_daf(zarr_zip_factory, path)
                    check_packed_lazy_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_daf(zarr_zip_factory, path)
                    check_eager_below_threshold_read(daf, original)
                    return nothing
                end
            end
        end

        # Slicing through the public `NamedArray` (the wrapper that `get_matrix` returns) flows down to
        # `LazySparseMatrix.getindex` and produces a fresh `NamedArray` over a `LazySparseMatrix`; chunk reads only
        # happen when the sliced wrapper is materialised (here, by the equality check against the expected slice).
        nested_test("slicing_through_named_array") do
            mktempdir() do path
                daf = FilesDaf(joinpath(path, "test.daf"), "w+"; name = "named_slice!", packed = true)
                _, original = build_packed_sparse_daf((p; packed) -> daf, path)
                full = get_matrix(daf, "row", "col", "data")
                sliced_named = full[:, 2:3]
                @test parent(sliced_named) isa LazySparseMatrix
                @test size(sliced_named) == (size(original, 1), 2)
                @test SparseMatrixCSC(parent(sliced_named)) == original[:, 2:3]
                return nothing
            end
        end
    end

    # `format_get_vector` for a packed sparse property routes through `LazySparseVector` on every backend that
    # produces sharded `nzind` / `nzval` storage. Below-threshold sparse properties keep the eager `SparseVector`
    # path. Each backend's daf is built through the public `set_vector!` API so the test exercises the same
    # write/read pipeline as user code.
    nested_test("format_get_vector") do
        function build_packed_sparse_vector_daf(
            create_daf::Function,
            path::AbstractString,
        )::Tuple{Any, SparseVector{Float32, Int32}}
            n_elements = 8192
            nnz_count = n_elements
            indices = Int32[index for index in 1:nnz_count]
            values = Float32[index for index in 1:nnz_count]
            original = SparseVector{Float32, Int32}(n_elements, indices, values)
            daf = create_daf(path; packed = true)
            add_axis!(daf, "row", ["r$(index)" for index in 1:n_elements])
            set_vector!(daf, "row", "data", original)
            return (daf, original)
        end

        function build_unpacked_sparse_vector_daf(
            create_daf::Function,
            path::AbstractString,
        )::Tuple{Any, SparseVector{Float32, Int32}}
            original = SparseVector{Float32, Int32}(4, Int32[2, 4], Float32[10.0, 20.0])
            daf = create_daf(path; packed = true)
            add_axis!(daf, "row", ["r1", "r2", "r3", "r4"])
            set_vector!(daf, "row", "data", original)
            return (daf, original)
        end

        # Bool sparse vectors with all-true nzval skip writing `.nzval` on disk; the lazy path synthesises
        # `fill(true, length(nzind))` instead of opening a chunked source. Triggered by `packed = true` plus
        # enough non-zeros to push `nzind` over the chunk-byte threshold.
        function build_packed_bool_sparse_vector_daf(
            create_daf::Function,
            path::AbstractString,
        )::Tuple{Any, SparseVector{Bool, Int32}}
            n_elements = 8192
            indices = Int32[index for index in 1:n_elements]
            values = fill(true, n_elements)
            original = SparseVector{Bool, Int32}(n_elements, indices, values)
            daf = create_daf(path; packed = true)
            add_axis!(daf, "row", ["r$(index)" for index in 1:n_elements])
            set_vector!(daf, "row", "data", original)
            return (daf, original)
        end

        function check_packed_lazy_vector_read(daf, original::SparseVector)::Nothing
            named = get_vector(daf, "row", "data")
            wrapped = parent(parent(named))
            @test wrapped isa LazySparseVector
            @test size(wrapped) == size(original)
            @test wrapped.materialized === nothing
            @test SparseVector(wrapped) == original
            return nothing
        end

        function check_eager_vector_below_threshold_read(daf, original::SparseVector)::Nothing
            named = get_vector(daf, "row", "data")
            wrapped = parent(parent(named))
            @test wrapped isa SparseVector
            @test wrapped == original
            return nothing
        end

        nested_test("files") do
            files_factory(path; packed) = FilesDaf(joinpath(path, "test.daf"), "w+"; name = "lazy_files!", packed)
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(files_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(files_factory, path)
                    check_eager_vector_below_threshold_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("h5df") do
            h5df_factory(path; packed) = H5df(joinpath(path, "test.h5df"), "w+"; name = "lazy_h5df!", packed)
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(h5df_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("bool_packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_bool_sparse_vector_daf(h5df_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(h5df_factory, path)
                    check_eager_vector_below_threshold_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("zarr_directory") do
            zarr_factory(path; packed) = ZarrDaf(joinpath(path, "test.daf.zarr"), "w+"; name = "lazy_zarr_dir!", packed)
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(zarr_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("bool_packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_bool_sparse_vector_daf(zarr_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(zarr_factory, path)
                    check_eager_vector_below_threshold_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("zarr_zip") do
            function zarr_zip_factory(path; packed)
                return ZarrDaf(joinpath(path, "test.daf.zarr.zip"), "w+"; name = "lazy_zarr_zip!", packed)
            end
            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(zarr_zip_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("bool_packed_returns_lazy") do
                mktempdir() do path
                    daf, original = build_packed_bool_sparse_vector_daf(zarr_zip_factory, path)
                    check_packed_lazy_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(zarr_zip_factory, path)
                    check_eager_vector_below_threshold_read(daf, original)
                    return nothing
                end
            end
        end

        # `HttpDaf` exercises the lazy path two ways: a `packed = true` daf produces sharded `.shard` components
        # opened as `HttpPackedDenseArray`; a `packed = false` daf produces flat `.nzind` / `.nzval` files served
        # over `Range` GETs as `HttpStripedVector`. Either lazy source kind triggers `LazySparseVector`.
        nested_test("http") do
            function serve_files_daf(action::Function, path::AbstractString, packed::Bool)::Nothing
                files_path = joinpath(path, "served.daf")
                writer = FilesDaf(files_path, "w+"; name = "lazy_http!", packed)
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
                    action(writer, url)
                finally
                    close(server)
                end
                return nothing
            end

            nested_test("packed_returns_lazy") do
                mktempdir() do path
                    serve_files_daf(path, true) do writer, url
                        n_elements = 8192
                        indices = Int32[index for index in 1:n_elements]
                        values = Float32[index for index in 1:n_elements]
                        original = SparseVector{Float32, Int32}(n_elements, indices, values)
                        add_axis!(writer, "row", ["r$(index)" for index in 1:n_elements])
                        set_vector!(writer, "row", "data", original)
                        check_packed_lazy_vector_read(HttpDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end

            nested_test("striped_returns_lazy") do
                mktempdir() do path
                    serve_files_daf(path, false) do writer, url
                        n_elements = 8192
                        indices = Int32[index for index in 1:n_elements]
                        values = Float32[index for index in 1:n_elements]
                        original = SparseVector{Float32, Int32}(n_elements, indices, values)
                        add_axis!(writer, "row", ["r$(index)" for index in 1:n_elements])
                        set_vector!(writer, "row", "data", original)
                        check_packed_lazy_vector_read(HttpDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end

            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    serve_files_daf(path, false) do writer, url
                        original = SparseVector{Float32, Int32}(4, Int32[2, 4], Float32[10.0, 20.0])
                        add_axis!(writer, "row", ["r1", "r2", "r3", "r4"])
                        set_vector!(writer, "row", "data", original)
                        check_eager_vector_below_threshold_read(HttpDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end
        end
    end
end
