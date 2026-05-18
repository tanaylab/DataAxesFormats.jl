# (unreleased)

  - `HttpDaf` now reads above-threshold dense and packed properties
    via lazy `Range`-GET adapters instead of one whole-property `GET`.
    Flat dense vectors / matrices stripe through `HttpStripedVector` /
    `HttpStripedMatrix` (column-tile shape `(stripe_n_rows, 1)` for
    matrices, byte-contiguity coalesced up to
    `DAF_HTTP_MAX_COALESCE_GAP_KB`); packed v3-sharded properties open
    through `HttpPackedDenseArray` (one suffix Range GET fetches the
    shard index footer on first access; subsequent reads look up
    per-chunk byte ranges in the cached index, coalesce adjacent
    ones, and decode each fetched chunk through the codec pipeline).
    All three factories share a unified `HttpChunkedArray{T, N}`
    `DiskArrays.AbstractDiskArray` wrapper parameterised by closures
    for chunk-byte-range, decode, byte-fetcher, and index-ensure.
    Below-threshold properties keep the existing whole-property GET
    path. Per-chunk decoded buffers are LRU-cached sized by
    `DAF_PACKED_HTTP_CACHE_KB`. New `DAF_HTTP_MAX_COALESCE_GAP_KB`
    global (default = `DAF_PACKED_TARGET_CHUNK_KB`) controls the
    gap threshold the coalescer uses when grouping non-contiguous
    missing chunks into spans. The server is assumed to honour
    `Range: bytes=A-B` and suffix `Range: bytes=-N` (any static HTTP
    server matching the HTTP/1.1 spec qualifies).

  - `HttpDaf` packed sparse matrices return a `LazySparseMatrix`
    when at least one of `rowval` / `nzval` is packed on the server,
    OR when it is flat but above the chunk-byte threshold (the
    striped path). Bool sparse matrices with the all-true `nzval`
    omission synthesise the `nzval` source via an
    `HttpStripedVector{Bool}` whose byte fetcher manufactures `0x01`
    bytes — same code path as a real fetched vector, but no HTTP
    traffic and bounded memory.

  - Add `LazySparseVector{Tv, Ti}` parallel to `LazySparseMatrix`:
    holds `nzind` / `nzval` as one-dimensional indexable sources
    (`Zarr.ZArray{T, 1}`, `DiskArrays.cache`-wrapped `ZArray`,
    `H5dfDiskArray`, or `HttpChunkedArray`) that decompress per-chunk
    on access. Slicing via `lazy[range]` / `lazy[indices]` /
    `lazy[mask]` accumulates a `SparseSelection` without touching the
    chunked storage. Materialisation copies the selected slice into a
    plain `SparseVector{Tv, Ti}` the first time `SparseArrays.nonzeroinds`
    / `nonzeros` / `nnz` / `SparseVector(lazy)` is called (and caches
    it). Scalar `lazy[i]` binary-searches `nzind_source` for the
    original-axis index (`O(log nnz)` chunk reads) without populating
    the cache. `format_get_vector` returns a `LazySparseVector` for
    sparse vectors whose `nzind` or `nzval` components are lazy
    sources on every backend (`FilesDaf`, `H5df`, `ZarrDaf`-Directory,
    `ZarrDaf`-Zip, `HttpDaf`); below-threshold sparse vectors stay
    on the eager `SparseVector` path.

  - `ZarrDaf` (directory + zip) sparse-component writes now respect
    the `packed` flag for *every* component, matching `FilesDaf` and
    `H5df`. Previously `write_sparse_vector` and
    `write_sparse_matrix` hardcoded `nzind` / `colptr` / `rowval` to
    `packed = false` and only honoured the flag for `nzval`. With
    the fix, an above-threshold `nzind` (vector) / `rowval`
    (matrix) / `colptr` (wide matrix) is chunked on disk when the
    daf is opened with `packed = true`. This matters because the
    lazy read path triggers when at least one component is chunked
    on disk; the prior asymmetry meant Bool sparse vectors (whose
    `nzval` is omitted entirely) had no chunked component on
    `ZarrDaf`, blocking the lazy path.

  - `respond_with_range` test helper (in
    `test/http_helpers.jl`) honours `Range: bytes=N-M` and suffix
    `Range: bytes=-N` headers on the static HTTP server used by the
    `HttpDaf` test fixtures, so the striped + packed read paths
    actually exercise `Range` slicing during tests. Both the
    `FilesDaf`-backed and `ZarrDaf`-backed HTTP handlers route
    response bodies through it.

  - Public reads return read-only arrays consistently. Previously
    `get_vector` wrapped only string vectors in `SparseArrays.ReadOnly`
    and `get_matrix` wrapped nothing; numeric vectors and all matrices
    came back mutable. Now `get_vector_through_cache` /
    `get_matrix_through_cache` /
    `get_relayout_matrix_through_cache` (formats.jl) and the
    default-supplied branches in `get_vector` / `get_matrix`
    (readers.jl) all route through `read_only_array` before
    `as_named_*`, so every public read is shaped
    `NamedArray → ReadOnly → underlying`. Callers that previously
    did `parent(named) isa SomeStorage` now need
    `parent(parent(named))`; callers that need the mutable bottom
    storage (e.g. mmap shared-memory or `@turbo` inputs) drill via
    `base_array`.

  - `base_array` (originally `DataAxesFormats.Readers.base_array`,
    used by `description` / `daf_chunk_info` to peel
    `NamedArray` + `SparseArrays.ReadOnly`) is now in
    `TanayLabUtilities.MatrixFormats` and extended to also recurse
    into `SubArray` / `Transpose` / `Adjoint` /
    `PermutedDimsArray` — reconstructing the wrapper around the
    drilled parent so `array[i, j]` integer-index semantics are
    preserved while the inner `ReadOnly` is peeled. This makes it
    safe to call inside `LoopVectorization.@turbo` loops, which
    silently fall back to scalar `@inbounds @fastmath` when their
    inputs are `ReadOnly`-wrapped. `log_vector!` (operations.jl)
    drills its `output` / `input` at function entry so the new
    consistent-ReadOnly policy doesn't regress its `@turbo`
    speedup. The six `import ..Readers.base_array` lines across
    DataAxesFormats's backends were dropped (now in scope via the
    existing `using TanayLabUtilities`).

  - `concat.jl` `sparse_vectors_storage_fraction` /
    `sparse_matrices_storage_fraction` now use `base_array` to
    drill through the `NamedArray` + `ReadOnly` wrappers before
    the `array isa AbstractSparseArray` check; previously the
    `.array` peel only stripped the `NamedArray`, leaving a
    `SparseArrays.ReadOnly{<:AbstractSparseArray}` that the `isa`
    check rejected. The bug silently densified concatenated sparse
    vectors / matrices once the consistent-ReadOnly policy
    landed.

  - `Chains.complete_chain!` (chains.jl) used a bare `json(...)`
    that JSON.jl 1.x no longer exports; qualified to
    `JSON.json(...)`. Caught by JET.

  - `H5df` honors the `packed` flag end-to-end: dense and sparse
    `set!` and `empty_*!` paths route each property (or each sparse
    component independently) through `write_packed_dense_dataset!`,
    which decides flat-vs-packed via `chunks_for`. Flat datasets stay
    contiguous and unfiltered (mmap-friendly); packed datasets are
    chunked (HDF5's only filter-pipeline-bearing layout) with the
    codec resolved from `DAF_PACKED_COMPRESSION` (`H5Zblosc`,
    `H5Zzstd`, `H5Zbitshuffle` are imported at module load to register
    filter ids `32001` / `32008` / `32015`; `:gzip*` codecs use
    HDF5.jl's built-in `Deflate` / `Shuffle`). Packed dense matrices
    are streamed column-by-column through a `PackedDenseMatrix` whose
    encoder issues `dataset[:, column] = chunk_buffer` hyperslab
    writes serialized by an internal `ReentrantLock` (HDF5.jl is not
    thread-safe in default builds even when distinct threads target
    distinct chunks). The packed read path wraps packed datasets in a
    new `H5dfDiskArray` (a `DiskArrays.AbstractDiskArray` adapter that
    delegates `readblock!` to HDF5 hyperslab reads) and caches
    decompressed chunks in `DiskArrays.cache`; packed sparse matrices
    are returned as a `LazySparseMatrix` over the cached `rowval` /
    `nzval` with `colptr` materialised eagerly (matching `ZarrDaf`
    and `FilesDaf`). No version bump — `H5df` repos written by
    previous releases do not exist in the wild, so the on-disk
    `daf` group attribute stays at `[1, 0]`.

  - `FilesDaf` consolidated metadata moved from a `metadata.zip`
    archive of per-property JSON sidecars to a single-line
    `metadata.json` object mapping `"<relative_path>"` → descriptor.
    Each `set!` byte-surgery-appends one entry (truncate the trailing
    `}`, write `,"k":v}`) — O(size of the new descriptor) per write,
    independent of total property count. `delete!` rebuilds the file
    from scratch. Every open rebuilds the file if it is missing or
    parses, with the rebuild silently swallowed for read-only opens
    on a frozen filesystem. `ZipDaf` strips a stale `metadata.json`
    entry from the central directory on every writable open so an
    `unzip foo.daf.zip` of a `zip -r foo.daf` does not preserve a
    snapshot inconsistent with the destination tree. `HttpDaf` reads
    `metadata.json` once at open instead of `metadata.zip`.

  - `ZarrDaf` consolidated metadata moved from a separate `.zmetadata`
    sidecar (Zarr v2 shape) to an inline `consolidated_metadata`
    field in the root `zarr.json` matching the on-disk shape that
    `zarr-python` 3.x writes (informally tracking
    [`zarr-specs#309`](https://github.com/zarr-developers/zarr-specs/pull/309),
    still open). The field is `{kind: "inline", must_understand: false, metadata: {<path>: <full v3 metadata blob>}}`. Each `set!` updates
    a cached byte buffer of the inner `metadata` dict via byte-level
    append (`register_consolidated_node!` / `register_consolidated_subtree!`
    for sparse and `add_axis` paths) and rewrites root `zarr.json`
    once via `flush_consolidated_metadata!`; `delete!`/reorder calls
    `refresh_consolidated_metadata!` for full rebuild via in-memory
    `ZGroup` walk. The HTTP `ZarrDaf` open path fetches root
    `zarr.json` directly, parses the inline field, translates
    `zarr-python`'s flat path keys (`<path>`) to Zarr.jl's
    `ConsolidatedStore` shape (`<path>/zarr.json`), and constructs the
    store manually — no dependence on Zarr.jl's `.zmetadata` lookup.
    `ZarrDaf-Zip` strips a stale `consolidated_metadata` field from
    root `zarr.json` on every writable open (parallel to `ZipDaf`'s
    `metadata.json` strip) so a `zip -r` of a directory daf followed
    by an unzip does not preserve a snapshot inconsistent with the
    destination tree.

  - `zarr_convert` reads the source's consolidated dict once at the
    top of each conversion and threads it through to per-property
    converters, replacing six `read_json_dict("$(source_dir)/zarr.json")`
    calls with O(1) dict lookups. Hard-link byte-equivalence path
    unchanged. The `<path>` ↔ FilesDaf-descriptor bijection between
    the two consolidated formats is the formal contract of this module
    (per the module docstring).

  - `zarr_convert.jl` hard-links packed properties between
    `FilesDaf` and `ZarrDaf` in both directions, exploiting the
    byte-identity between `FilesDaf` `<name>.shard` (or
    per-component shard) and `ZarrDaf`'s single-shard `c/0[/0]`
    chunk file under the same codec / `chunk_shape`. The hard-link
    path fires only when the source's on-disk encoding is
    byte-identical to what the destination would write right now
    (matching `chunks_for(...)` and `compressor_for()`). Sources
    whose chunk_shape, codec, or sharding layout differ — e.g.
    foreign multi-chunk Zarr arrays, single-shard arrays whose
    inner chunks are not the canonical `(rows_per_tile, 1)` shape,
    or arrays compressed with a non-default codec — fall back to a
    re-encode through the standard writer: streaming column-by-column
    for dense matrices (bounded RAM), eager `set_*!` for vectors and
    sparse properties.

  - JSON sidecar builders (`component_descriptor_json`,
    `sparse_vector_json_bytes`, `sparse_matrix_json_bytes`) accept
    explicit per-component codec arguments, defaulting to
    `compressor_for()`. `zarr_convert` passes the source's codec
    so a hard-linked `.shard` decodes correctly even when the
    destination's global compressor differs from the source's.
    New `PackedFormat` helpers `is_zarr_array_packed`,
    `packed_codec_from_zarray`, and `packed_codec_from_v3_codec`
    map a Zarr `ShardingCodec` instance back to the `PackedCodec`
    parameters that produced it.

  - `LazySparseMatrix` gains explicit `TanayLabUtilities.colptr` /
    `rowval` / `nzval` methods so it survives the FilesFormat /
    ZipFormat sparse write paths (those helpers have no generic
    `AbstractSparseMatrixCSC` fallback, only per-type methods for
    `SparseMatrixCSC` / `NamedArray` / `SparseArrays.ReadOnly`).
    Each delegates through the materialised cache to a concrete
    `Vector`. `SparseArrays.rowvals` / `nonzeros` / `getcolptr` /
    `nnz` / `SparseMatrixCSC(lazy)` were already wired in earlier;
    these are the Daf-internal helpers that complete the AbstractSparseMatrixCSC
    behaviour for the cross-format conversion path.

  - `ZipDaf` ↔ `FilesDaf` byte-equivalence: bundling a packed
    `FilesDaf` directory into a zip (`zip -r foo.daf.zip foo.daf/`)
    produces a valid `ZipDaf`, and conversely unzipping a packed
    `ZipDaf` produces a valid `FilesDaf` (the `metadata.zip` /
    `axes/metadata.json` sidecars are rebuilt on first writable
    open). Same equivalence for `ZarrDaf` directory ↔ zip
    archive — the per-property `zarr.json` and chunk files are
    byte-identical, and the `DirectoryStore`-only `.zmetadata`
    consolidated sidecar rebuilds via
    `ensure_consolidated_metadata!` on first writable open of an
    unbundled tree.

  - `ZipDaf` honors the `packed` flag end-to-end: dense / sparse
    `set_*!` and the streaming `empty_dense_matrix!` write packed
    `<name>.shard` (or per-component shard) entries directly into
    the outer zip via `MmapShardRegion` + `IncrementalShardWriter`
    (reserve the worst-case upper bound, shrink on finalize, patch
    CRC). The on-disk `.shard` bytes are byte-identical to what
    `FilesDaf` writes for the same content, so unzipping a packed
    `ZipDaf` produces a `FilesDaf` directory whose `.shard` files
    can be hard-linked.

  - `ZipDaf` adopts the v1.1 sparse JSON schema (per-component
    descriptors with `eltype` / `n_elements` and optional `packed`
    / `chunk_shape` / `compression` / `compression_level` /
    `index_location`), matching `FilesDaf` and ending the
    schema-shape drift between the two formats. The reader
    recognises both v1.0 (top-level `eltype` / `indtype`) and v1.1
    (per-component) sparse layouts via `parse_sparse_descriptor`.
    Packed sparse `format_get_matrix` returns a `LazySparseMatrix`
    over the packed `rowval` / `nzval` sources, matching
    `FilesDaf` and `ZarrDaf`.

  - JSON sidecar bytes builders moved from `FilesFormat` into
    `PackedFormat`: `dense_array_json_bytes`,
    `packed_array_json_bytes`, `sparse_vector_json_bytes`,
    `sparse_matrix_json_bytes`, plus `parse_sparse_descriptor` /
    `eltype_for_descriptor` / `component_descriptor_json`. Both
    `FilesDaf` and `ZipDaf` route their write paths through these
    builders so the on-disk JSON shape is identical across
    backends. `open_packed_dense_array` and `open_shard_as_zarray`
    each gained a bytes-taking overload alongside the path-taking
    one, so backends that already hold mmap'd shard bytes (the
    outer-zip read path) avoid an unnecessary round-trip through
    the filesystem.

  - Add `LazySparse` module with the `LazySparseMatrix{Tv, Ti}`
    skeleton and the `SparseSelection` family (`AllOf`, `RangeOf`,
    `IndicesOf`, `MaskOf`). The matrix wraps the in-memory `colptr`
    plus the per-property `rowval` / `nzval` `Zarr.ZArray` sources
    produced by the packed read paths; row/column selections start
    as `AllOf`. Slicing and materialisation arrive in later phases.

  - `LazySparseMatrix` slicing forms `lazy[:, range]` /
    `lazy[range, :]` / `lazy[:, indices]` / `lazy[mask, :]` each
    return a fresh `LazySparseMatrix` whose row / column selection
    composes with the prior selection (`AllOf` collapses,
    `RangeOf ∘ RangeOf` stays a `RangeOf`, `MaskOf ∘ MaskOf` stays
    a `MaskOf`, every other combination becomes `IndicesOf`).
    Slicing rebinds selections only; the `full_colptr` vector and
    the `rowval` / `nzval` `ZArray` sources are shared by reference
    and never read.

  - `LazySparseMatrix` materialisation: `SparseArrays.rowvals` /
    `nonzeros` / `getcolptr` / `nnz`, `SparseMatrixCSC(lazy)` /
    `convert(SparseMatrixCSC, lazy)`, and any generic
    `AbstractSparseMatrixCSC` operation that reaches those
    primitives, materialise the current slice into a
    `SparseMatrixCSC{Tv, Ti}` cached in `matrix.materialized`.
    The materialiser walks the `column_select` iterator, decompresses
    each original column's `rowval` / `nzval` slabs through the
    `ZArray` sources, and filters rows through an `O(1)`-per-call
    `original_to_new` lookup (a `Dict` for `IndicesOf`, a
    cumulative-sum table for `MaskOf`). The `colptr` field renamed
    to `full_colptr` so `lazy.full_colptr` (original, unsliced)
    no longer shadows `SparseArrays.getcolptr(lazy)` (sliced,
    materialised). Scalar `lazy[i, j]` decompresses the target
    column's `rowval` slab on the fly without populating the cache.

  - `format_get_matrix` returns a `LazySparseMatrix` for packed
    sparse properties on every backend (`FilesDaf`, `ZipDaf`,
    `ZarrDaf`-Directory, `ZarrDaf`-Zip). The decision to wrap is
    per-property: a sparse matrix whose `rowval` or `nzval`
    components are stored chunked goes through the lazy wrapper;
    all-flat-component sparse matrices stay on the eager
    `SparseMatrixCSC` path with mmap-backed components. The lazy
    wrapper's `rowval_source` / `nzval_source` field type is
    `AbstractVector{T}` so a mixed-flat/packed property can hold
    an mmap'd `Vector{T}` for one component and a `ZArray{T, 1}`
    for the other; `read_chunk_range` dispatches to a zero-copy
    `view` for `DenseVector` sources and to eager-decompress
    indexing for chunked sources. Slicing through the public
    `NamedArray` wrapper (which `get_matrix` returns) flows down
    to `LazySparseMatrix.getindex` and produces a fresh
    `NamedArray` over a `LazySparseMatrix` without touching the
    chunked storage.

  - `FilesDaf` packs every above-threshold component as a single
    `<name>.<component>.shard` v3 sharded-array file: numeric dense
    properties (`<name>.shard`), the integer index components of
    sparse properties (`<name>.colptr.shard`, `<name>.rowval.shard`,
    `<name>.nzind.shard`), and the string text of dense or sparse
    string properties (the dense string flat path stays at
    `<name>.txt`; packed dense and sparse string values use the same
    `VLenUTF8` encoding inside a `ShardingCodec`). Each per-property
    descriptor in the sparse JSON sidecar carries its own
    `packed` / `chunk_shape` / codec parameters so each component is
    independently flat or packed.

  - `FilesDaf` `empty_dense_matrix!` with `packed = true` streams
    columns into a single `<name>.shard` file via the same incremental
    shard writer the `ZarrDaf` streaming path uses; the user's
    column-by-column fill is sliced into row-tile-sized inner chunks
    matching the byte-target heuristic, so streamed and non-streaming
    writes produce the same on-disk inner-chunk shape.

  - `FilesDaf` packed dense vectors and matrices write to a single
    per-property `<name>.shard` file (Zarr v3 sharded-array bytes).
    The on-disk bytes are byte-identical to what `ZarrDaf` writes for
    the same content under the same codec / chunk shape, so
    `zarr_convert.jl` can hard-link across backends. Below-threshold
    properties keep the existing flat `<name>.data` mmap path.

  - `FilesDaf` / `ZipDaf` on-disk format minor version bumped from
    `[1,0]` to `[1,1]`. The JSON descriptor for sparse properties now
    carries per-property descriptors (`colptr` / `rowval` / `nzval`,
    or `nzind` / `nzval` for vectors) instead of top-level
    `eltype` / `indtype` keys. Binary data files are unchanged. The
    reader accepts both shapes; the writer always emits v1.1. Old
    code refuses to read v1.1 files (the version-mismatch error
    message names `1.1`).

  - `ZarrDaf`-zip packed shards reclaim per-shard upper-bound slack at
    finalize via `shrink_mmap_zip_entry!`; on-disk size now matches the
    actual encoded data plus the index slab plus ZIP64 extra-field
    bytes.

  - Add `PackedFormat` module with the configuration globals for the upcoming
    packed (chunked + compressed) on-disk encoding (`DAF_PACKED_TARGET_CHUNK_KB`,
    `DAF_PACKED_COMPRESSION`, `DAF_PACKED_COMPRESSION_LEVEL`,
    `DAF_PACKED_LOCAL_CACHE_KB`, `DAF_PACKED_HTTP_CACHE_KB`). Sizes use
    binary kilobytes (1 KB = 1024 bytes); chunk-size default is 8 KB
    (page-sized) to enable sub-column slice access.

  - Add internal `compressor_for` / `valid_compression_level_range` /
    `PackedCodec` / `resolve_packed` helpers (in `PackedFormat`) for
    resolving codec selection and `packed=true` resolution. Validates the
    compression symbol against the supported whitelist and the level
    against per-codec ranges.

  - Add `packed_default::Bool` field to the internal `Formats.Internal`
    struct (default `false`) for upcoming per-daf packed-encoding default.

  - Add `packed::Bool = false` kwarg to every disk-format constructor
    (`FilesDaf`, `ZipDaf`, `ZarrDaf`, `H5df`, `HttpDaf`, `MemoryDaf`),
    plus `open_daf` and `complete_daf`. Threads to `Internal.packed_default`
    on writable formats; `MemoryDaf` accepts and ignores (with a `@debug`
    line on `packed=true`); `HttpDaf` accepts and ignores (read-only).
    No write-path behaviour change yet — the field is set but no format
    consults it until later steps.

  - Add `packed::Maybe{Bool} = nothing` kwarg to the high-level write API
    (`set_vector!`, `set_matrix!`, `empty_dense_*!`, `empty_sparse_*!`,
    `get_empty_*!`, `relayout_matrix!`) and to `copy_vector!` /
    `copy_matrix!` / `copy_tensor!` / `copy_all!`. Resolution happens at
    the format-call boundary via `resolve_packed`; backends still
    accept-and-ignore the flag pending per-format implementations.

  - Add `format_is_packed_vector` / `format_is_packed_matrix` predicates
    (default `false`); chains, contracts, read-only wrappers, and views
    delegate or short-circuit appropriately. View identity-passthrough
    (sub-column / sub-matrix views with all-identity queries) propagates
    packed-ness from the underlying daf.

  - Add forward-declaration stub types `PackedDenseMatrix{T}`,
    `HttpStripedMatrix{T}`, `HttpStripedVector{T}` in `PackedFormat`.
    They support `Base.size` / `eltype` only; the streaming-write and
    HTTP stripe-synthesis semantics land in later phases.

  - Add `effective_sizeof(::Type{T})` and `STRING_SIZEOF_ESTIMATE = 16`
    to `PackedFormat` so chunk-size and threshold calculations work for
    non-bits types (e.g. `String`). Update `chunks_for` to return
    `Maybe{NTuple{N, Int}}` — `nothing` ⇔ "do not pack", a chunk shape
    otherwise. The pack decision uses a per-column threshold
    (`shape[1] * effective_sizeof(T) ≥ DAF_PACKED_TARGET_CHUNK_KB × 1024`); matrices with short columns (e.g. block × gene) stay flat
    even when their total size is large. Chunks are always subsets of
    a single column — `(rows_per_tile,)` for vectors, `(rows_per_tile, 1)` for matrices.

  - Bump `ZarrFormat.MINOR_VERSION` from `0` to `1`. New code writes
    `[1,1]`-marked datasets and reads both `[1,0]` and `[1,1]`; old
    code refuses `[1,1]` cleanly. The on-disk encoding for
    below-threshold properties is byte-equivalent to `[1,0]`; only
    the version marker differs in that case.

  - `ZarrDaf` writes dense numeric properties via a new
    `dense_zcreate(T, group, name, packed, shape)` helper that
    consults `chunks_for` and the codec resolved through
    `zarr_compressor_for(compressor_for(), T)`. The six whitelisted
    codecs map to `Zarr.jl`'s `BloscCompressor` (with `shuffle = 2`
    for the `*_bitshuffle` variants), `ZstdCompressor`,
    `ZlibCompressor`, and `ShuffleFilter` (for `:gzip_shuffle`).
    `:zstd_bitshuffle` is rejected on the ZarrDaf backend with a
    message pointing to `:blosc_zstd_bitshuffle` (Zarr.jl exposes no
    bitshuffle filter). For non-bits types (`String` properties),
    `dense_zcreate` lets `Zarr.jl`'s default `VLenUTF8Filter` survive
    in the filter chain ahead of the compressor; bitshuffle on the
    post-VLenUTF8 byte stream sees `typesize = 1` and produces the
    natural 8-bit-plane decomposition that compresses ASCII-heavy
    axis labels well.

  - Sparse property writes (`format_set_vector!` / `format_set_matrix!`
    sparse arms) route the `nzval` array through `dense_zcreate`;
    `colptr`, `rowval`, and sparse-vector `nzind` stay flat.

  - `ZarrDaf` chunked-compressed reads return a
    `DiskArrays.CachedDiskArray` wrapper sized at
    `DAF_PACKED_LOCAL_CACHE_KB` (`MemoryData` cache group), so cold
    reads don't materialise the whole array up-front.
    `empty_cache!` releases the wrapper cleanly. The flat
    single-chunk-uncompressed `mmap`-Strided fast path is unchanged
    (`MappedData` cache group).

  - `empty_dense_matrix!(daf, …; packed = true)` returns a streaming
    `PackedDenseMatrix{T}` wrapper. Each thread holds one column at
    a time in a per-thread `Vector{T}` slot; `view(matrix, :, column)` returns the slot's buffer; switching columns on the
    same thread flushes the previous column via the wrapper's
    encoder closure. Chunk shape is forced to `(n_rows, 1)`. The
    wrapper supports `parallel_loop_wo_rng(1:n_columns)`-shaped
    fills directly. `empty_dense_vector!(daf, …; packed = true)`
    skips the streaming wrapper (per Plan §5) — hands the user a
    fresh in-RAM `Vector{T}`, encodes through the chunked array at
    `format_filled_empty_dense_vector!`.

  - Bump `Zarr` compat to `0.10`. Packed `zcreate` sites pass
    `dimension_separator = "/"` so chunks nest under per-page
    subdirectories (partial mitigation for the flat-chunks-per-array
    fan-out on `ZarrDaf-DirectoryStore`); the mmap fast read path
    (`single_chunk_matrix_suffix`) reads the per-array separator from
    `array.metadata.chunk_key_encoding.sep`.

  - Add `DiskArrays` (`0.4`) as a direct dependency. The
    `MatrixLayouts.major_axis(::DiskArrays.CachedDiskArray)` method
    in `TanayLabUtilities.jl` (also a new dependency edge there)
    forwards layout to the wrapped parent so the Daf reader's
    `assert_valid_matrix` accepts the cached wrapper. ZarrDaf
    declares `major_axis(::ZArray{T, 2})` as `Columns` (the
    `C`-order-with-reversed-shape convention).

# v0.2.0

  - Improve query language.
  - Make views lazy.
  - Improve contracts.
  - Track performance (flame graphs).
  - Improve performance (control over contracts and other expensive verifications).

# v0.1.2

  - Add tensors to documentation.
  - Rename `axis_array` to `axis_vector` (breaking change).
  - Adapt to Julia 1.11.
  - Minor bug fixes.

# v0.1.1

  - Add tensors to APIs (a 3D tensor is just a collection of 2D matrices with a naming convention).
  - Minor bug fixes.

# v0.1.0

First (alpha) release.
