# (unreleased)

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
