nested_test("lazy_sparse") do
    is_materialized = DataAxesFormats.LazySparse.is_materialized
    shared_materialization! = DataAxesFormats.LazySparse.shared_materialization!

    # Whether the `access` materializes the whole `lazy` matrix. Nothing holds that materialization, so the GC is
    # disabled meanwhile, or it could free it before the check.
    function does_materialize(access::Function, lazy::LazySparseMatrix)::Bool
        was_enabled = GC.enable(false)
        try
            access()
            return is_materialized(lazy)
        finally
            GC.enable(was_enabled)
        end
    end

    # Build a packed sparse matrix through `ZarrDaf` once so the `nzval` and `rowval` ZArrays end up sharded; the
    # nested tests then wrap those ZArrays in a `LazySparseMatrix`.
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
            @test lazy.n_rows == 8192
            @test length(lazy.colptr) == 5
            @test !is_materialized(lazy)
            return nothing
        end
    end

    # A small matrix with an empty column, and with rows missing from some columns.
    expected = SparseMatrixCSC{Float32, Int32}(sparse(Float32[10 0 30 0; 20 0 0 5; 0 0 40 6]))
    function new_lazy()::LazySparseMatrix
        return LazySparseMatrix(3, copy(expected.colptr), copy(expected.rowval), copy(expected.nzval))
    end

    # Reading a subset of the columns reads only those columns, and does not materialize the whole matrix.
    nested_test("column_subsets") do
        nested_test("column") do
            lazy = new_lazy()
            @test lazy[:, 1] isa SparseVector{Float32, Int32}
            @test lazy[:, 1] == expected[:, 1]
            @test lazy[:, 2] == expected[:, 2]
            @test lazy[:, 4] == expected[:, 4]
            @test !is_materialized(lazy)
        end

        nested_test("columns") do
            lazy = new_lazy()
            @test lazy[:, 2:4] isa SparseMatrixCSC{Float32, Int32}
            @test lazy[:, 2:4] == expected[:, 2:4]
            @test lazy[:, [4, 1]] == expected[:, [4, 1]]
            @test lazy[:, [true, false, true, false]] == expected[:, [1, 3]]
            @test !is_materialized(lazy)
        end

        nested_test("rows_of_columns") do
            lazy = new_lazy()
            reordered = lazy[[3, 1], 3:4]
            @test reordered == expected[[3, 1], 3:4]
            @test all(issorted(view(rowvals(reordered), nzrange(reordered, column))) for column in 1:2)
            @test lazy[[true, false, true], [1, 3]] == expected[[1, 3], [1, 3]]
            @test lazy[2:3, 4] == expected[2:3, 4]
            @test lazy[[false, true, true], 4] == expected[2:3, 4]
            @test !is_materialized(lazy)
        end

        nested_test("all_rows") do
            lazy = new_lazy()
            @test lazy[collect(1:3), [1, 3]] == expected[:, [1, 3]]
            @test lazy[trues(3), 2:3] == expected[:, 2:3]
            @test lazy[1:3, 4] == expected[:, 4]
            @test !is_materialized(lazy)
        end

        nested_test("view") do
            lazy = new_lazy()
            @test sum(view(lazy, :, 3)) == 70
            @test view(lazy, :, 2:3) == expected[:, 2:3]
            @test !is_materialized(lazy)
        end

        nested_test("named") do
            lazy = new_lazy()
            named = NamedArray(lazy, (["r1", "r2", "r3"], ["c1", "c2", "c3", "c4"]), ("row", "col"))
            @test named[:, "c3"] == expected[:, 3]
            @test named[:, ["c1", "c4"]] == expected[:, [1, 4]]
            @test named[["r3", "r1"], "c3"] == expected[[3, 1], 3]
            @test !is_materialized(lazy)
        end

        nested_test("owned_by_caller") do
            lazy = new_lazy()
            columns = lazy[:, 1:2]
            nonzeros(columns)[1] = -1
            @test lazy[:, 1:2] == expected[:, 1:2]
        end

        nested_test("after_materialization") do
            lazy = new_lazy()
            materialized = shared_materialization!(lazy)
            @test lazy[:, 3] == expected[:, 3]
            @test lazy[[3, 1], 3:4] == expected[[3, 1], 3:4]
        end
    end

    # Any other access reads the whole matrix once.
    nested_test("other_access") do
        # Scalar access is reported as inefficient. The lazy matrix then holds its materialization, so a loop over
        # elements reads it only once.
        nested_test("scalar") do
            nested_test("error") do
                lazy = new_lazy()
                @test_throws "inefficient scalar access to a lazy sparse matrix" lazy[2, 1]
                return nothing
            end

            nested_test("ignore") do
                lazy = new_lazy()
                previous_handler = TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER
                TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = IgnoreHandler
                try
                    @test lazy[2, 1] == 20
                    @test lazy[1, 2] == 0
                    @test lazy[3, 3] == 40
                finally
                    TanayLabUtilities.MatrixLayouts.GLOBAL_INEFFICIENT_ACTION_HANDLER = previous_handler
                end
                GC.gc()
                GC.gc()
                @test is_materialized(lazy)

                # A purge (e.g. by `empty_cache!`) drops the held materialization.
                DataAxesFormats.Formats.purge_lazy_array!(lazy)
                GC.gc()
                GC.gc()
                @test !is_materialized(lazy)
                @test !lazy.did_report_scalar_access[]
                return nothing
            end
        end

        nested_test("row") do
            lazy = new_lazy()
            @test does_materialize(lazy) do
                @test lazy[3, :] == expected[3, :]
            end
        end

        nested_test("rows") do
            lazy = new_lazy()
            @test lazy[[1, 3], :] == expected[[1, 3], :]
        end

        nested_test("generic") do
            lazy = new_lazy()
            @test does_materialize(lazy) do
                @test sum(lazy) == sum(expected)
                @test lazy * Float32[1, 1, 1, 1] == expected * Float32[1, 1, 1, 1]
            end
        end
    end

    nested_test("materialize") do
        nested_test("full_matrix_round_trip") do
            build_lazy() do lazy, _, _
                materialized = SparseMatrixCSC(lazy)
                @test size(materialized) == (8192, 4)
                @test nnz(materialized) == 8192 * 4
                @test materialized[1, 1] == 1
                @test materialized[2, 3] == 6
                @test materialized[100, 4] == 400
                # Like `SparseArrays`' own constructors, this returns a matrix owned by the caller, without sharing it,
                # so editing it leaves the lazy matrix intact.
                @test !is_materialized(lazy)
                materialized.nzval[1] = -1
                @test SparseMatrixCSC(lazy)[1, 1] == 1
                return nothing
            end
        end

        nested_test("accessors") do
            build_lazy() do lazy, _, _
                colptr = SparseArrays.getcolptr(lazy)
                @test length(colptr) == 5
                @test colptr[end] == nnz(lazy) + 1
                @test !is_materialized(lazy)
                @test does_materialize(() -> rowvals(lazy), lazy)
                # While the materialization is held, calls return read-only views of its vectors.
                materialized = shared_materialization!(lazy)
                @test parent(rowvals(lazy)) === rowvals(materialized)
                @test parent(nonzeros(lazy)) === nonzeros(materialized)
                return nothing
            end
        end

        nested_test("nnz") do
            build_lazy() do lazy, _, _
                @test nnz(lazy) == 8192 * 4
                @test !is_materialized(lazy)
                return nothing
            end
        end

        # Base's `view` checks whether its indices alias the matrix, through `dataids`.
        nested_test("row_view") do
            lazy = new_lazy()
            @test !does_materialize(lazy) do
                n_stored_per_column = zeros(Int, 4)
                parallel_foreach_column(view(lazy, [1, 3], :)) do _, column_index, column
                    n_stored_per_column[column_index] = nnz(column)
                    return nothing
                end
                @test n_stored_per_column == [1, 0, 2, 1]
            end
        end

        # `convert(SparseMatrixCSC{...}, lazy)` returns the shared materialization, building it if no caller holds it.
        # This is unlike the constructor `SparseMatrixCSC(lazy)`, which returns a copy owned by the caller.
        nested_test("convert") do
            build_lazy() do lazy, _, _
                materialized = convert(SparseMatrixCSC{Float32, Int32}, lazy)
                @test materialized isa SparseMatrixCSC{Float32, Int32}
                @test size(materialized) == (8192, 4)
                @test nnz(materialized) == 8192 * 4
                @test shared_materialization!(lazy) === materialized
                @test plain_array(lazy) === materialized
                return nothing
            end
        end

        # Concurrent first readers wait for a single materialization, so all get the same matrix.
        nested_test("concurrent") do
            build_lazy() do lazy, _, _
                n_readers = 4 * Threads.nthreads()
                materialized_per_reader = Vector{Any}(undef, n_readers)
                Threads.@threads for reader in 1:n_readers
                    materialized_per_reader[reader] = shared_materialization!(lazy)
                end
                @test all(materialized === materialized_per_reader[1] for materialized in materialized_per_reader)
                return nothing
            end
        end

        # The materialization is shared while a caller holds it, and is freed once no caller holds it.
        nested_test("weakly_held") do
            lazy = new_lazy()
            materialized = plain_array(lazy)
            @test plain_array(lazy) === materialized
            @test is_materialized(lazy)

            dropped_lazy = new_lazy()
            weak_reference = weak_materialization(dropped_lazy)
            GC.gc()
            GC.gc()
            @test weak_reference.value === nothing
            @test !is_materialized(dropped_lazy)
            return nothing
        end
    end

    nested_test("unit") do
        nested_test("colon_colon") do
            lazy = new_lazy()
            @test lazy[:, :] isa SparseMatrixCSC{Float32, Int32}
            @test lazy[:, :] == expected
            return nothing
        end

        nested_test("typed_constructor") do
            lazy = new_lazy()
            typed = SparseMatrixCSC{Float32, Int32}(lazy)
            @test typed == expected
            typed.nzval[1] = -1
            @test SparseMatrixCSC(lazy) == expected
            materialized = convert(SparseMatrixCSC{Float32, Int32}, lazy)
            @test shared_materialization!(lazy) === materialized
            return nothing
        end

        nested_test("read_only") do
            lazy = new_lazy()
            @test is_read_only_array(lazy)
            @test_throws "Can't change" lazy[1, 1] = 5
            @test_throws "Can't change" nonzeros(lazy)[1] = 5
            @test SparseMatrixCSC(lazy) == expected
            return nothing
        end

        # Equality and display use the plain storage, so they are not reported as scalar access.
        nested_test("equality") do
            lazy = new_lazy()
            named = NamedArray(lazy, (["r1", "r2", "r3"], ["c1", "c2", "c3", "c4"]), ("row", "col"))
            @test lazy == expected
            @test expected == lazy
            @test lazy == Matrix(expected)
            @test named == expected
            @test expected == named
            @test named == named
            @test lazy == new_lazy()
            transposed = transpose(sparse(transpose(expected)))
            @test lazy == transposed
            @test transposed == lazy
            @test adjoint(sparse(adjoint(expected))) == lazy
            @test isequal(named, expected)
            @test isequal(expected, lazy)
            @test lazy != expected .* 2
            @test !lazy.did_report_scalar_access[]
            return nothing
        end

        nested_test("show") do
            lazy = new_lazy()
            named = NamedArray(lazy, (["r1", "r2", "r3"], ["c1", "c2", "c3", "c4"]), ("row", "col"))
            plain_named = NamedArray(expected, named.dicts, named.dimnames)
            @test sprint(show, MIME"text/plain"(), lazy) == sprint(show, MIME"text/plain"(), expected)
            @test sprint(show, lazy) == sprint(show, expected)
            @test sprint(show, MIME"text/plain"(), named) == sprint(show, MIME"text/plain"(), plain_named)
            @test sprint(show, named) == sprint(show, plain_named)
            @test !lazy.did_report_scalar_access[]
            return nothing
        end

        nested_test("copy") do
            lazy = new_lazy()
            copied = copy(lazy)
            @test copied isa SparseMatrixCSC{Float32, Int32}
            @test copied == expected
            @test !is_materialized(lazy)
            return nothing
        end
    end

    # `format_get_matrix` for a packed sparse property returns a `LazySparseMatrix` on every backend that produces
    # sharded `nzval` / `rowval` storage (FilesDaf, ZarrDaf-Directory, ZarrDaf-Zip). Below-threshold sparse properties
    # keep the eager mmap-backed `SparseMatrixCSC` path. Each backend's daf is built through the public `set_matrix!`
    # API so the test exercises the same write/read pipeline as user code.
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
            lazy = get_matrix(daf, "row", "col", "data").array
            @test lazy isa LazySparseMatrix
            @test size(lazy) == size(original)
            @test !is_materialized(lazy)
            @test SparseMatrixCSC(lazy) == original
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
            # `Significant` copies its input and edits the copy in place, so the stored property must not change.
            nested_test("query_keeps_stored") do
                mktempdir() do path
                    daf, original = build_packed_sparse_daf(files_factory, path)
                    @test daf["@ row @ col :: data % Significant high 3"] isa AbstractMatrix
                    @test get_matrix(daf, "row", "col", "data"; relayout = false) == original
                    return nothing
                end
            end
            nested_test("relayout_packed") do
                mktempdir() do path
                    daf, original = build_packed_sparse_daf(files_factory, path)
                    relayout_matrix!(daf, "row", "col", "data")
                    @test get_matrix(daf, "col", "row", "data"; relayout = false) == transpose(original)
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
            # `chunks_for` threshold, so the lazy matrix reads `nzval` from a flat (mmap-backed) `Vector`.
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
                    lazy = get_matrix(daf, "row", "col", "data").array
                    @test lazy isa LazySparseMatrix
                    @test lazy[:, 2] == original[:, 2]
                    @test SparseMatrixCSC(lazy) == original
                    return nothing
                end
            end
        end

        nested_test("zip") do
            zip_factory(path; packed) = ZipDaf(joinpath(path, "test.daf.zip"), "w+"; name = "lazy_zip!", packed)
            nested_test("relayout_packed") do
                mktempdir() do path
                    daf, original = build_packed_sparse_daf(zip_factory, path)
                    relayout_matrix!(daf, "row", "col", "data")
                    @test get_matrix(daf, "col", "row", "data"; relayout = false) == transpose(original)
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

        # Slicing the columns through the public `NamedArray` (the wrapper that `get_matrix` returns) reads only those
        # columns, into a new `SparseMatrixCSC`.
        nested_test("slicing_through_named_array") do
            mktempdir() do path
                daf = FilesDaf(joinpath(path, "test.daf"), "w+"; name = "named_slice!", packed = true)
                _, original = build_packed_sparse_daf((p; packed) -> daf, path)
                full = get_matrix(daf, "row", "col", "data")
                sliced_named = full[:, 2:3]
                @test parent(sliced_named) isa SparseMatrixCSC
                @test sliced_named == original[:, 2:3]
                @test !is_materialized(full.array)
                return nothing
            end
        end
    end

    # `format_get_vector` for a sparse property always returns an eager `SparseVector`, packed or not. Each backend's
    # daf is built through the public `set_vector!` API so the test exercises the same write/read pipeline as user code.
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

        # Bool sparse vectors with all-true nzval skip writing `.nzval` on disk; the read path synthesises
        # `fill(true, length(nzind))`. Triggered by `packed = true` plus enough non-zeros to push `nzind` over the
        # chunk-byte threshold.
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

        function check_sparse_vector_read(daf, original::SparseVector)::Nothing
            named = get_vector(daf, "row", "data")
            wrapped = parent(parent(named))
            @test wrapped isa SparseVector
            @test wrapped == original
            return nothing
        end

        nested_test("files") do
            files_factory(path; packed) = FilesDaf(joinpath(path, "test.daf"), "w+"; name = "lazy_files!", packed)
            nested_test("packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(files_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(files_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("h5df") do
            h5df_factory(path; packed) = H5df(joinpath(path, "test.h5df"), "w+"; name = "lazy_h5df!", packed)
            nested_test("packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(h5df_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("bool_packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_bool_sparse_vector_daf(h5df_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(h5df_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("zarr_directory") do
            zarr_factory(path; packed) = ZarrDaf(joinpath(path, "test.daf.zarr"), "w+"; name = "lazy_zarr_dir!", packed)
            nested_test("packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(zarr_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("bool_packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_bool_sparse_vector_daf(zarr_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(zarr_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
        end

        nested_test("zarr_zip") do
            function zarr_zip_factory(path; packed)
                return ZarrDaf(joinpath(path, "test.daf.zarr.zip"), "w+"; name = "lazy_zarr_zip!", packed)
            end
            nested_test("packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_sparse_vector_daf(zarr_zip_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("bool_packed_returns_eager") do
                mktempdir() do path
                    daf, original = build_packed_bool_sparse_vector_daf(zarr_zip_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    daf, original = build_unpacked_sparse_vector_daf(zarr_zip_factory, path)
                    check_sparse_vector_read(daf, original)
                    return nothing
                end
            end
        end

        # `HttpDaf` reads the components two ways: a `packed = true` daf produces sharded `.zip` components; a
        # `packed = false` daf produces flat `.nzind` / `.nzval` files. Both are read eagerly.
        nested_test("http") do
            function serve_files_daf(action::Function, path::AbstractString, packed::Bool)::Nothing
                files_path = joinpath(path, "served.daf")
                writer = FilesDaf(files_path, "w+"; name = "lazy_http!", packed)
                with_served_directory(files_path) do url
                    action(writer, url)
                    return nothing
                end
                return nothing
            end

            nested_test("packed_returns_eager") do
                mktempdir() do path
                    serve_files_daf(path, true) do writer, url
                        n_elements = 8192
                        indices = Int32[index for index in 1:n_elements]
                        values = Float32[index for index in 1:n_elements]
                        original = SparseVector{Float32, Int32}(n_elements, indices, values)
                        add_axis!(writer, "row", ["r$(index)" for index in 1:n_elements])
                        set_vector!(writer, "row", "data", original)
                        check_sparse_vector_read(HttpDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end

            nested_test("flat_returns_eager") do
                mktempdir() do path
                    serve_files_daf(path, false) do writer, url
                        n_elements = 8192
                        indices = Int32[index for index in 1:n_elements]
                        values = Float32[index for index in 1:n_elements]
                        original = SparseVector{Float32, Int32}(n_elements, indices, values)
                        add_axis!(writer, "row", ["r$(index)" for index in 1:n_elements])
                        set_vector!(writer, "row", "data", original)
                        check_sparse_vector_read(HttpDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end

            # All-`true` Bool sparse vector over HTTP: the writer omits `nzval` on disk, and the read path synthesises
            # `fill(true, nnz)`.
            nested_test("packed_all_true_bool_returns_eager") do
                mktempdir() do path
                    serve_files_daf(path, true) do writer, url
                        n_elements = 8192
                        nnz_total = 2048
                        indices = Int32.(1:nnz_total)
                        original = SparseVector{Bool, Int32}(n_elements, indices, fill(true, nnz_total))
                        add_axis!(writer, "row", ["r$(index)" for index in 1:n_elements])
                        set_vector!(writer, "row", "data", original)
                        check_sparse_vector_read(HttpDaf(url), original)
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
                        check_sparse_vector_read(HttpDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end
        end

        # `ZarrDaf` over HTTP reads each sparse component from a Zarr `nzind` / `nzval` sub-array, eagerly.
        nested_test("zarr_http") do
            function serve_zarr_daf(action::Function, path::AbstractString, packed::Bool)::Nothing
                zarr_path = joinpath(path, "served.daf.zarr")
                writer = ZarrDaf(zarr_path, "w+"; name = "lazy_zarr_http!", packed)
                with_served_directory(zarr_path) do url
                    action(writer, url)
                    return nothing
                end
                return nothing
            end

            nested_test("packed_returns_eager") do
                mktempdir() do path
                    serve_zarr_daf(path, true) do writer, url
                        n_elements = 8192
                        indices = Int32[index for index in 1:n_elements]
                        values = Float32[index for index in 1:n_elements]
                        original = SparseVector{Float32, Int32}(n_elements, indices, values)
                        add_axis!(writer, "row", ["r$(index)" for index in 1:n_elements])
                        set_vector!(writer, "row", "data", original)
                        check_sparse_vector_read(ZarrDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end

            nested_test("below_threshold_returns_eager") do
                mktempdir() do path
                    serve_zarr_daf(path, false) do writer, url
                        original = SparseVector{Float32, Int32}(4, Int32[2, 4], Float32[10.0, 20.0])
                        add_axis!(writer, "row", ["r1", "r2", "r3", "r4"])
                        set_vector!(writer, "row", "data", original)
                        check_sparse_vector_read(ZarrDaf(url), original)
                        return nothing
                    end
                    return nothing
                end
            end
        end
    end
end
