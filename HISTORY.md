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
