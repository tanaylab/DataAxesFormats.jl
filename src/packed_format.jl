"""
Configuration globals for the packed (chunked + compressed) on-disk encoding used by `Daf` storage formats.

Packed encoding stores large dense matrices and sparse-matrix components as chunked, compressed arrays on disk and over
the wire, in exchange for paying decompression CPU at read time. It is most useful when the data lives on a slow tier
(NFS, HTTP, archival storage) where the bandwidth saving outweighs the read overhead; for compute-intensive work on a
fast local SSD, prefer staging an unpacked copy via [`copy_all!`](@ref DataAxesFormats.Copies.copy_all!).

The exported globals in this module are the only knobs tuning the packed encoding. They apply process-wide; there is
no per-daf override.
"""
module PackedFormat

export DAF_PACKED_COMPRESSION
export DAF_PACKED_COMPRESSION_LEVEL
export DAF_PACKED_HTTP_CACHE_KB
export DAF_PACKED_LOCAL_CACHE_KB
export DAF_PACKED_TARGET_CHUNK_KB

using ..Formats
using TanayLabUtilities

"""
The target uncompressed size in kilobytes for a single chunk (page) of a packed property. Doubles as the threshold
below which a property is stored or fetched flat instead of being chunked. Properties whose uncompressed size is below
this value get the flat (mmap-friendly single-chunk uncompressed) on-disk path on every backend regardless of
`packed=true`. (The HTTP backend uses the same threshold to gate stripe-synthesized fetches versus single-`GET`
fetches; see the HTTP backend documentation.)

Default `8` (8 KB) — page-sized, enabling sub-column slice access (the common K-marker query pattern fetches the first
page of each of K columns rather than the full column). Small enough to keep network slice fetches cheap, but large
enough that compression codecs still produce reasonable ratios.

Kilobytes are binary (1 KB = 1024 bytes), matching OS page size and the conventions of [`DAF_PACKED_LOCAL_CACHE_KB`](@ref)
and [`DAF_PACKED_HTTP_CACHE_KB`](@ref).

To tune: `DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB = 16` at the top of your script or REPL session.
"""
DAF_PACKED_TARGET_CHUNK_KB::Int = 8

# Convert `DAF_PACKED_TARGET_CHUNK_KB` to bytes (binary kilobytes × `1024`). Internal helper for chunk-size
# calculations.
function packed_target_chunk_bytes()::Int
    return DAF_PACKED_TARGET_CHUNK_KB * 1024
end

# Convert `DAF_PACKED_LOCAL_CACHE_KB` to the decimal-megabyte unit (1 MB = `1_000_000` bytes) used by
# `DiskArrays.cache`'s `maxsize` parameter. Internal helper for the local read-side cache wrapper. The 5 % skew between
# binary and decimal sizing is irrelevant for cache sizing — `DAF_PACKED_LOCAL_CACHE_KB` is a soft target.
function packed_local_cache_mb()::Int
    return DAF_PACKED_LOCAL_CACHE_KB * 1024 ÷ 1_000_000
end

# Convert `DAF_PACKED_HTTP_CACHE_KB` to the same decimal-megabyte unit, for the over-HTTP cache wrapper introduced in
# Phase 5. Same 5 % skew applies.
function packed_http_cache_mb()::Int
    return DAF_PACKED_HTTP_CACHE_KB * 1024 ÷ 1_000_000
end

# Assumed average bytes-per-element for non-bits-typed properties (e.g. `String`-valued vectors), used by
# `effective_sizeof` and through it by `chunks_for` when sizing chunks for variable-length data. The value (16) reflects
# typical bio-data axis labels (cell IDs, gene names) — it undersizes very long strings and oversizes short ones, but
# only nudges chunk shape, never correctness.
const STRING_SIZEOF_ESTIMATE = 16

# Effective per-element byte size for chunk-shape and threshold calculations. Returns `sizeof(T)` for bits types and
# `STRING_SIZEOF_ESTIMATE` for non-bits types. `chunks_for` uses this so the same helper sizes chunks consistently for
# numeric and string properties without the caller having to special-case.
function effective_sizeof(::Type{T})::Int where {T}
    return isbitstype(T) ? sizeof(T) : STRING_SIZEOF_ESTIMATE
end

"""
The compression codec used for packed properties. The default `:blosc_zstd_bitshuffle` combines `zstd` compression with
a bitshuffle pre-filter, giving good ratios for integer-typical scientific data (e.g. UMI counts, gene indices) where
bitshuffle isolates high-zero bytes; for floats with clustered exponents it does similarly well on the exponent bytes.

Supported codecs:

| Symbol                             | Zarr backend                                        | HDF5 backend                        | Plug-in needed for non-Julia consumers    |
|:---------------------------------- |:--------------------------------------------------- |:----------------------------------- |:----------------------------------------- |
| `:blosc_zstd_bitshuffle` (default) | `BloscCompressor(cname="zstd", shuffle=BITSHUFFLE)` | `H5Zblosc` filter                   | HDF5 readers only                         |
| `:blosc_lz4_bitshuffle`            | `BloscCompressor(cname="lz4", shuffle=BITSHUFFLE)`  | `H5Zblosc` filter                   | HDF5 readers only                         |
| `:zstd_bitshuffle`                 | bitshuffle filter + `ZstdCompressor`                | `H5Zbitshuffle` + `H5Zzstd` filters | HDF5 readers; Zarr readers for bitshuffle |
| `:zstd`                            | `ZstdCompressor`                                    | `H5Zzstd` filter                    | HDF5 readers only                         |
| `:gzip`                            | `ZlibCompressor`                                    | built-in deflate                    | None                                      |
| `:gzip_shuffle`                    | `ZlibCompressor` + byte-shuffle filter              | built-in deflate + built-in shuffle | None                                      |

The "plug-in needed" column reflects what a consumer of the produced files needs to add beyond a stock install of their
HDF5 / Zarr library to be able to read the data. `:gzip` and `:gzip_shuffle` use only HDF5 / Zarr built-in filters and
require no plug-ins anywhere. All other codecs require the consumer to load filter libraries; the exact recipes per
language are listed below.

**Plug-in installation by codec and language**, for tools that need to read the produced files:

| Codec                                             | Python                                       | R                       |
|:------------------------------------------------- |:-------------------------------------------- |:----------------------- |
| `:gzip`, `:gzip_shuffle`                          | none                                         | none                    |
| `:blosc_zstd_bitshuffle`, `:blosc_lz4_bitshuffle` | `hdf5plugin` for HDF5                        | `rhdf5filters` for HDF5 |
| `:zstd`                                           | `hdf5plugin` for HDF5                        | `rhdf5filters` for HDF5 |
| `:zstd_bitshuffle`                                | `hdf5plugin` for HDF5; `bitshuffle` for Zarr | `rhdf5filters` for HDF5 |

In Julia, no extra installation step is needed: `DataAxesFormats` declares `H5Zblosc`, `H5Zzstd`,
and (where available) `H5Zbitshuffle` as direct dependencies and loads them at module init, so
`using DataAxesFormats` is enough to register all required filters with HDF5.jl's filter registry.
Zarr.jl already ships with blosc, zstd, and zlib built in; the bitshuffle Zarr filter is registered
on the same module init when its adapter is available.

Install commands for non-Julia consumers:

  - **Python**: `pip install hdf5plugin` (one library covers blosc / zstd / lz4 / bitshuffle for HDF5); `pip install zarr`
    brings `numcodecs` which already includes blosc, zstd, and zlib; `pip install bitshuffle` adds bitshuffle support for
    Zarr (`:zstd_bitshuffle`).
  - **R**: `BiocManager::install("rhdf5filters")` covers blosc, zstd, lz4, and bitshuffle for `rhdf5`.

Setting `DAF_PACKED_COMPRESSION` to any value outside the table causes a runtime error listing the supported codecs at
the first write that needs to resolve the codec.

To tune: `DataAxesFormats.PackedFormat.DAF_PACKED_COMPRESSION = :gzip_shuffle` for plug-in-free interop with vanilla
HDF5 / Zarr tooling, at the cost of weaker compression ratios.
"""
DAF_PACKED_COMPRESSION::Symbol = :blosc_zstd_bitshuffle

"""
The compression level passed to the inner codec (`zstd` / `lz4` / `zlib`). Default `5`, the standard Blosc clevel
default — a balanced speed-vs-ratio choice for the default `:blosc_zstd_bitshuffle` codec.

Higher levels (e.g. `9`) produce smaller files at the cost of slower writes; reads are roughly unaffected by level.
Lower levels (`1`) are faster to write at the cost of larger files.

The numeric meaning of the level varies per codec — Blosc and `zlib` use a `1:9` scale, `zstd` uses a `1:22` scale.
The codec-specific valid range is enforced when the codec is resolved. Level `0` (which means "no compression" in Blosc
and `zlib`, and "library default" in `zstd`) is excluded for all codecs because picking a packed codec implies you
actually want compression applied.
"""
DAF_PACKED_COMPRESSION_LEVEL::Int = 5

"""
The cache size in kilobytes used by the per-property `DiskArrays.cache` LRU when reading packed properties on
local-disk backends. Larger caches reduce repeat decompression cost when scattered scalar access patterns revisit the
same chunks.

Default `65536` (64 MiB). Each `get_matrix` / `get_vector` call on a packed local-disk property returns a
`DiskArrays.CachedDiskArray` wrapper sized at this value (in bytes, after multiplying by 1024); the cache is held alive
by the daf's internal cache (`MemoryData` cache group) and released by
[`empty_cache!`](@ref DataAxesFormats.Formats.empty_cache!).

Kilobytes are binary (1 KB = 1024 bytes), consistent with [`DAF_PACKED_TARGET_CHUNK_KB`](@ref).

This is independent of the HTTP cache (see [`DAF_PACKED_HTTP_CACHE_KB`](@ref)) because re-fetching over the network is
much more expensive than re-decompressing on local disk.
"""
DAF_PACKED_LOCAL_CACHE_KB::Int = 65536

"""
The cache size in kilobytes used by the per-property `DiskArrays.cache` LRU when reading packed properties (or
stripe-synthesised unpacked properties) over HTTP.

Default `262144` (256 MiB), meaningfully larger than [`DAF_PACKED_LOCAL_CACHE_KB`](@ref) because re-fetches over HTTP
are far more expensive (network round-trip + bandwidth) than local re-decompressions.

Kilobytes are binary (1 KB = 1024 bytes), consistent with [`DAF_PACKED_TARGET_CHUNK_KB`](@ref).
"""
DAF_PACKED_HTTP_CACHE_KB::Int = 262144

# The whitelist of supported `DAF_PACKED_COMPRESSION` codec symbols. Updating this requires also updating
# `valid_compression_level_range` and the per-codec backends.
const SUPPORTED_COMPRESSION_CODECS =
    (:blosc_zstd_bitshuffle, :blosc_lz4_bitshuffle, :zstd_bitshuffle, :zstd, :gzip, :gzip_shuffle)

# Return the valid range of compression levels for the given supported codec symbol. Each codec family has its own
# range. Level `0` is excluded for all codecs because picking a packed codec implies you actually want compression
# applied. Errors if `compression` is not in `SUPPORTED_COMPRESSION_CODECS`.
function valid_compression_level_range(compression::Symbol)::UnitRange{Int}
    if compression == :blosc_zstd_bitshuffle || compression == :blosc_lz4_bitshuffle
        return 1:9
    elseif compression == :zstd_bitshuffle || compression == :zstd
        return 1:22
    elseif compression == :gzip || compression == :gzip_shuffle
        return 1:9
    else
        supported_list = join((":$(name)" for name in SUPPORTED_COMPRESSION_CODECS), ", ")
        return error("unsupported packed compression codec: :$(compression)\n" * "supported codecs: $(supported_list)")
    end
end

# A resolved packed-encoding codec descriptor, returned by `compressor_for`. Holds the codec name (one of
# `SUPPORTED_COMPRESSION_CODECS`) and the compression level that should be applied. Format-specific code converts this
# descriptor into the appropriate backend-specific compressor / filter objects (Zarr's `BloscCompressor`, HDF5 filter
# kwargs, etc.).
#
# The constructor validates both fields: `compression` must be in the supported whitelist, and `compression_level` must
# be in the codec-specific valid range (see `valid_compression_level_range`).
struct PackedCodec  # NOLINT
    compression::Symbol
    compression_level::Int

    function PackedCodec(compression::Symbol, compression_level::Int)
        valid_range = valid_compression_level_range(compression)  # also validates `compression`
        if !(compression_level in valid_range)
            return error(
                "out-of-range packed compression level: $(compression_level)\n" *
                "for codec: :$(compression)\n" *
                "valid range: $(valid_range)",
            )
        end
        return new(compression, compression_level)
    end
end

# Return a `PackedCodec` for the given codec name and level. Errors if `compression` is not in the supported whitelist
# (see `DAF_PACKED_COMPRESSION`) or if `compression_level` is outside the codec's valid range (see
# `valid_compression_level_range`). Internal helper for format-level write paths.
function compressor_for(
    compression::Symbol = DAF_PACKED_COMPRESSION,
    compression_level::Int = DAF_PACKED_COMPRESSION_LEVEL,
)::PackedCodec
    return PackedCodec(compression, compression_level)
end

# Return the chunk shape for a property of element type `T` and shape `shape`, given the per-call resolved `packed`
# flag. Returns `nothing` when the property should not be packed: either `packed = false`, or a single column's
# uncompressed bytes (`shape[1] * effective_sizeof(T)`) is below `packed_target_chunk_bytes()`. The format-level writer
# treats `nothing` as "use the flat single-chunk uncompressed encoding". Otherwise returns the packed chunk shape:
# `(rows_per_tile,)` for vectors and `(rows_per_tile, 1)` for matrices, where
# `rows_per_tile = min(packed_target_chunk_bytes() ÷ effective_sizeof(T), shape[1])`. Internal helper for format-level
# write paths. Only 1-D vectors and 2-D matrices are supported. The per-column threshold (rather than total bytes)
# means matrices with short columns (e.g. `block × gene` shapes) stay flat even if they're large in total — chunks of
# a few hundred bytes aren't worth the codec overhead.
function chunks_for(packed::Bool, shape::NTuple{N, Int}, ::Type{T})::Maybe{NTuple{N, Int}} where {N, T}
    @assert N == 1 || N == 2
    if !packed
        return nothing
    end
    target_bytes = packed_target_chunk_bytes()
    element_bytes = effective_sizeof(T)
    if shape[1] * element_bytes < target_bytes
        return nothing
    end
    rows_per_tile = min(target_bytes ÷ element_bytes, shape[1])
    if N == 1
        return (rows_per_tile,)
    end
    @assert N == 2
    return (rows_per_tile, 1)
end

# Resolve the effective `packed` flag for a write or copy operation. The resolution rule is:
#
#   1. If `per_call` is non-`nothing`, use it.
#   2. Otherwise, return `daf.internal.packed_default`.
#
# `daf` is `DafReader` so the helper works equally on read-only wrappers, views, and chains, which all forward
# `.internal` to the underlying daf. Internal helper for high-level write/copy entry points.
function resolve_packed(per_call::Maybe{Bool}, daf::DafReader)::Bool
    if per_call !== nothing
        return per_call
    end
    return daf.internal.packed_default
end

# Per-thread fill state used by `PackedDenseMatrix`. Each thread holds at most one column at a time in `chunk_buffer`,
# reused across columns. `current_column` is `0` while the slot is uninitialized; it becomes the column index once the
# user starts writing to a column.
mutable struct PackedThreadSlot{T}
    current_column::Int
    chunk_buffer::Vector{T}
end

# Streaming write wrapper handed to user code by `format_get_empty_dense_matrix!` when packing is requested. Each
# thread fills one column at a time through `view(matrix, :, column)`; switching to a different column on the same
# thread triggers `encoder(prev_column, chunk_buffer)` to flush the previous column's chunk before the buffer is
# reused. Cross-thread column order is unconstrained; the only contract is that no two threads ever touch the same
# column and that each column is completely filled by its thread before that thread moves on.
struct PackedDenseMatrix{T} <: AbstractMatrix{T}  # NOLINT
    n_rows::Int
    n_columns::Int
    thread_slots::Vector{PackedThreadSlot{T}}
    encoder::Function
end

# Allocate a `PackedDenseMatrix` over the given encoder. The encoder closure has signature
# `(column::Int, chunk_buffer::Vector{T}) -> Nothing` and writes the chunk for `column` (the buffer's contents) to
# storage. One `PackedThreadSlot{T}` is allocated per `Threads.maxthreadid()`; threads index into `thread_slots` by
# `Threads.threadid()`.
function PackedDenseMatrix{T}(n_rows::Int, n_columns::Int, encoder::Function)::PackedDenseMatrix{T} where {T}
    thread_slots = [PackedThreadSlot{T}(0, Vector{T}(undef, n_rows)) for _ in 1:Threads.maxthreadid()]
    return PackedDenseMatrix{T}(n_rows, n_columns, thread_slots, encoder)
end

function Base.size(matrix::PackedDenseMatrix)::Tuple{Int, Int}
    return (matrix.n_rows, matrix.n_columns)
end

function Base.view(matrix::PackedDenseMatrix{T}, ::Colon, column::Int)::Vector{T} where {T}
    @assert 1 <= column <= matrix.n_columns
    slot = matrix.thread_slots[Threads.threadid()]
    if slot.current_column == column
        return slot.chunk_buffer
    end
    if slot.current_column != 0
        matrix.encoder(slot.current_column, slot.chunk_buffer)
    end
    slot.current_column = column
    return slot.chunk_buffer
end

function Base.getindex(matrix::PackedDenseMatrix{T}, row::Int, column::Int)::T where {T}
    return view(matrix, :, column)[row]
end

function Base.setindex!(matrix::PackedDenseMatrix{T}, value, row::Int, column::Int)::T where {T}
    column_view = view(matrix, :, column)
    column_view[row] = value
    return T(value)
end

function TanayLabUtilities.MatrixLayouts.major_axis(::PackedDenseMatrix)::Maybe{Int8}
    return TanayLabUtilities.MatrixLayouts.Columns
end

# Flush every active thread slot's chunk via `matrix.encoder` and reset the slot. Called by the format-level
# `format_filled_empty_dense_matrix!` so the last-written column on each thread is committed to disk before the
# wrapper goes out of scope.
function flush_packed_dense_matrix!(matrix::PackedDenseMatrix)::Nothing
    for slot in matrix.thread_slots
        if slot.current_column != 0
            matrix.encoder(slot.current_column, slot.chunk_buffer)
            slot.current_column = 0
        end
    end
    return nothing
end

# Forward-declaration stub for the HTTP stripe-synthesis read wrapper used
# by `HttpDaf` for unpacked dense matrices at or above
# `DAF_PACKED_TARGET_CHUNK_KB`. Stripe shape is always `(stripe_n_rows, 1)`
# by design — one column per Range GET, since access is column-oriented for
# the column-major-stored matrix. Real `readblock!` / `eachchunk` semantics
# land in Phase 5 (Steps 5.1 / 5.2).
mutable struct HttpStripedMatrix{T} <: AbstractMatrix{T}
    url::AbstractString
    header_size::Int
    n_rows::Int
    n_columns::Int
    stripe_n_rows::Int
end

function Base.size(matrix::HttpStripedMatrix)::Tuple{Int, Int}
    return (matrix.n_rows, matrix.n_columns)
end

# Forward-declaration stub for the HTTP stripe-synthesis read wrapper used
# by `HttpDaf` for unpacked dense vectors at or above
# `DAF_PACKED_TARGET_CHUNK_KB`. Real `readblock!` / `eachchunk` semantics
# land in Phase 5 (Step 5.1).
mutable struct HttpStripedVector{T} <: AbstractVector{T}
    url::AbstractString
    header_size::Int
    n_elements::Int
    stripe_n_elements::Int
end

function Base.size(vector::HttpStripedVector)::Tuple{Int}
    return (vector.n_elements,)
end

end  # module
