"""
Configuration globals and the shared implementation of the packed (chunked + compressed) on-disk encoding used by
`Daf` storage formats.

Packed encoding stores large dense matrices and sparse-matrix components as chunked, compressed arrays on disk and over
the wire, in exchange for paying decompression CPU at read time. It is most useful when the data lives on a slow tier
(NFS, HTTP, archival storage) where the bandwidth saving outweighs the read overhead; for compute-intensive work on a
fast local SSD, prefer staging an unpacked copy via [`copy_all!`](@ref DataAxesFormats.Copies.copy_all!).

The exported globals in this module are the only knobs tuning the packed encoding. They apply process-wide; there is
no per-daf override.

# Dual-format shard layout

A packed property is stored as a single *shard* — one file (`<name>.zip` in
[`FilesFormat`](@ref DataAxesFormats.FilesFormat), one archive entry per property in
[`ZipFormat`](@ref DataAxesFormats.ZipFormat)) holding all of the property's inner chunks. A shard written by this
package is *dual-format*: the same bytes are simultaneously

  - a valid **Zarr v3 sharded array** (ZEP-0002) — a shard index at file offset `0` maps each inner chunk to a
    `(offset, n_bytes)` byte range, so a Zarr reader decodes chunks by index lookup; and
  - a valid **ZIP archive** — a central directory at the file tail lists one entry per inner chunk, so a ZIP reader
    (and this package's `FilesDaf` / `ZipDaf` / `HttpDaf` backends) decodes chunks through the central directory.

The two indices point into the same chunk bytes; the dead bytes each format ignores (the Zarr index for ZIP readers,
the ZIP local file headers for Zarr readers) are tolerated by both. Which indices are present is recorded by the
`"packed_format"` key in a `FilesDaf` / `ZipDaf` JSON descriptor and, in parallel, by the `daf_packed_format`
attribute on a `ZarrDaf` sharded array. This package writes `"indexed+zipped"` for the dual-format shard above in
both places. A foreign source carries one of:

  - a `FilesDaf` / `ZipDaf` descriptor with `"packed_format" => "zipped"` — a ZIP-only archive of chunks with no
    leading Zarr index; or
  - a `ZarrDaf` array with no `daf_packed_format` attribute — a Zarr-only sharded array with no ZIP framing
    (conceptually "indexed"; there is no literal `"indexed"` value, the absence of the attribute is the signal).

# Random-access central directory

The ZIP central-directory entries are **fixed-length and written in chunk order**, so a chunk's entry is located by
index arithmetic (`cd_offset + chunk_index * cd_entry_size`) without scanning the directory. This is a hard
requirement of the format — readers assert it and a 3rd-party producer must honor it. The fixed length implies a
fixed inner-chunk entry-name width: for STORED / ZSTD shards the name is `c/<i_N>/.../<i_1>` with each dimension
zero-padded to a fixed width; for DEFLATE shards the name field is repurposed (see below).

# Per-codec ZIP method and self-description

Each chunk entry is stamped, where possible, with the ZIP compression method that matches its codec, so that a
generic ZIP tool which recognises the method decompresses the chunk to its uncompressed bytes automatically:

  - **zstd** → ZIP method `93`. The entry data is the raw zstd frame, which is exactly what the Zarr zstd codec
    consumes, so both readers share it directly.
  - **gzip** → ZIP method `8` (DEFLATE). A ZIP method-8 entry needs a raw DEFLATE payload, but a Zarr gzip codec
    needs a full gzip stream (`[header][DEFLATE][trailer]`). Both are satisfied at once: the 10-byte gzip header is
    placed in the entry's *name* field (binary but NUL-free), the entry data is the raw DEFLATE payload, and the
    8-byte gzip trailer follows as dead bytes. A single contiguous Zarr range from the name start spans a valid gzip
    stream, while the ZIP entry points only at the DEFLATE payload.
  - **blosc, bitshuffle variants, or any codec with no matching ZIP method** → method `0` (STORED). The entry data
    is the codec output verbatim, which a generic ZIP tool cannot decode. For these shards a final `codec.json`
    entry (after the last chunk, before the central directory) records the inner codec pipeline as a Zarr v3 codec
    list, so an external tool with a Zarr v3 codec implementation can decode the otherwise-opaque STORED bytes.
    This package's own readers ignore `codec.json` — they recover the codec from the descriptor — and the Zarr
    shard index has slots only for the inner chunks, so it is invisible to that read path too.

On read, the chunk decoder validates that a chunk's ZIP method matches what its descriptor codec implies and errors
otherwise — a STORED entry under a zstd descriptor, for example, would be contradictory metadata.
"""
module PackedFormat

export DAF_HTTP_MAX_COALESCE_GAP_KB
export DAF_PACKED_COMPRESSION
export DAF_PACKED_COMPRESSION_LEVEL
export DAF_PACKED_HTTP_CACHE_KB
export DAF_PACKED_LOCAL_CACHE_KB
export DAF_PACKED_TARGET_CHUNK_KB

using ..Formats
using ..MmapZipStores
using ..StorageTypes
using Base64
using Base.Threads
using CodecZlib
using DiskArrays
using HTTP
using JSON
using LRUCache
using Mmap
using SparseArrays
using TanayLabUtilities
using Zarr

import ..MmapZipStores.CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE
import ..MmapZipStores.CENTRAL_DIRECTORY_ENTRY_SIGNATURE
import ..MmapZipStores.DEFLATE_COMPRESSION_METHOD
import ..MmapZipStores.END_OF_CENTRAL_DIRECTORY_SIGNATURE
import ..MmapZipStores.END_OF_CENTRAL_DIRECTORY_SIZE
import ..MmapZipStores.LOCAL_FILE_HEADER_FIXED_SIZE
import ..MmapZipStores.STORED_COMPRESSION_METHOD
import ..MmapZipStores.TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE
import ..MmapZipStores.ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
import ..MmapZipStores.ZIP64_END_OF_CENTRAL_DIRECTORY_SIGNATURE
import ..MmapZipStores.ZIP64_EXTRA_HEADER_ID
import ..MmapZipStores.ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE
import ..MmapZipStores.ZSTD_COMPRESSION_METHOD
import ..MmapZipStores.read_little_endian_uint16
import ..MmapZipStores.read_little_endian_uint32
import ..MmapZipStores.read_little_endian_uint64
import ..MmapZipStores.write_central_directory_entry!
import ..MmapZipStores.write_end_of_central_directory_region!
import ..MmapZipStores.write_little_endian_uint32!
import ..MmapZipStores.write_local_file_header!
import ..Operations.DTYPE_BY_NAME

import Base.Threads.maxthreadid
import SparseArrays.indtype
import ZipArchives.zip_crc32

# Common supertype for `DafWriter` backends that store properties as packed (chunked + compressed) shards using the
# shared write/read paths defined in this module. `FilesDaf` and `ZipDaf` both subtype it; `MemoryDaf`, `H5df`, and
# `ZarrDaf` do not (they use HDF5 / Zarr's own storage). The abstract type names the contract: every concrete
# `PackedDaf` must implement the `packed_*` primitives declared below; the shared `packed_format_*` helpers compose
# those primitives into the public Daf write/read API.
abstract type PackedDaf <: Formats.DafWriter end  # NOLINT

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

# Convert `DAF_PACKED_HTTP_CACHE_KB` to the same decimal-megabyte unit, for the HTTP cache wrapper sized
# separately from the local cache. Same 5 % skew (binary vs decimal) applies.
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

"""
The maximum gap (in kilobytes) between two needed byte ranges that the HTTP read path coalesces into a single
Range GET. When materialising a slice that needs chunks `c1` and `c2` with an unneeded gap of `g` bytes between
them, the path issues one Range GET covering `[c1_start, c2_end)` if `g ≤ DAF_HTTP_MAX_COALESCE_GAP_KB * 1024`,
otherwise two separate Range GETs.

Default matches [`DAF_PACKED_TARGET_CHUNK_KB`](@ref) — i.e., coalesce across at most one chunk's worth of
unneeded bytes per gap.

Kilobytes are binary (1 KB = 1024 bytes).
"""
DAF_HTTP_MAX_COALESCE_GAP_KB::Int = DAF_PACKED_TARGET_CHUNK_KB

# Convert `DAF_HTTP_MAX_COALESCE_GAP_KB` to bytes (binary kilobytes × `1024`). Internal helper for the HTTP read
# path's range-coalescing logic.
function packed_http_max_coalesce_gap_bytes()::Int
    return DAF_HTTP_MAX_COALESCE_GAP_KB * 1024
end

# Issue an HTTP `GET` against `url` and return the response body. Errors with the URL and underlying exception on
# transport / non-200 status. Shared by `HttpDaf` and the HTTP-mode `ZarrDaf` open path.
function http_get(url::AbstractString)::Vector{UInt8}
    response = try
        HTTP.get(url; retry = false, status_exception = false)  # NOJET
    catch exception
        error(chomp("""
                    HTTP GET error: $(exception)
                    for URL: $(url)
                    """))
    end
    if response.status != 200
        error(chomp("""
                    HTTP GET non-200 status: $(response.status)
                    for URL: $(url)
                    """))
    end
    return response.body
end

# Issue an HTTP `GET` with a `Range: bytes=offset-offset+nbytes-1` header, return exactly `nbytes` bytes from the
# server. Errors with the URL, range, and underlying exception on transport / non-206-or-200 status, or if the
# server returned a different number of bytes than requested. Shared by the striped + packed HTTP read paths in
# both `HttpDaf` and (future) HTTP-mode `ZarrDaf` packed reads.
function http_range_get(url::AbstractString, offset::Integer, nbytes::Integer)::Vector{UInt8}
    range_header = "bytes=$(offset)-$(offset + nbytes - 1)"
    response = try
        HTTP.get(url; headers = ["Range" => range_header], retry = false, status_exception = false)  # NOJET
    catch exception
        error(chomp("""  # UNTESTED
                    HTTP Range GET error: $(exception)
                    for URL: $(url)
                    range: $(range_header)
                    """))
    end
    if response.status != 206 && response.status != 200
        error(chomp("""  # UNTESTED
                    HTTP Range GET non-2xx status: $(response.status)
                    for URL: $(url)
                    range: $(range_header)
                    """))
    end
    @assert length(response.body) == nbytes "expected $(nbytes) bytes, got $(length(response.body)) for: $(url) range: $(range_header)"
    return response.body
end

# Issue an HTTP `GET` with a `Range: bytes=-N` suffix-range header, return exactly the last `n_bytes` of the
# resource. Used by the packed HTTP read path to fetch the shard's index footer when `index_location == :end`
# without an extra HEAD request to learn the file size.
function http_range_get_suffix(url::AbstractString, n_bytes::Integer)::Vector{UInt8}
    range_header = "bytes=-$(n_bytes)"
    response = try
        HTTP.get(url; headers = ["Range" => range_header], retry = false, status_exception = false)  # NOJET
    catch exception
        error(chomp("""  # UNTESTED
                    HTTP suffix Range GET error: $(exception)
                    for URL: $(url)
                    range: $(range_header)
                    """))
    end
    if response.status != 206 && response.status != 200
        error(chomp("""  # UNTESTED
                    HTTP suffix Range GET non-2xx status: $(response.status)
                    for URL: $(url)
                    range: $(range_header)
                    """))
    end
    @assert length(response.body) == n_bytes "expected $(n_bytes) bytes, got $(length(response.body)) for: $(url) range: $(range_header)"
    return response.body
end

# Closure that captures `url` and resolves to [`http_range_get`](@ref) — the `byte_fetcher` shape the
# `ChunkedArray` factories accept. Used by `HttpDaf` and `ZarrDaf`-over-HTTP to avoid repeating the same
# two-line closure at every call site that wraps a URL into a fetcher.
function url_byte_fetcher(url::AbstractString)::Function
    return (offset::Int, n_bytes::Int) -> http_range_get(url, offset, n_bytes)
end

# Closure that captures `url` and resolves to [`http_range_get_suffix`](@ref) — the `suffix_byte_fetcher` shape
# the `PackedDenseArray` factory accepts.
function url_suffix_byte_fetcher(url::AbstractString)::Function
    return (n_bytes::Int) -> http_range_get_suffix(url, n_bytes)
end

# Join an HTTP base URL and a path suffix with exactly one `/` separator. Both `HttpDaf` and `ZarrDaf` HTTP
# read paths use this so the slash policy (`base` without trailing `/`, `suffix` without leading `/`) is a
# single rule rather than scattered string interpolation.
function join_url(base::AbstractString, suffix::AbstractString)::String
    base_stripped = rstrip(base, '/')
    suffix_stripped = lstrip(suffix, '/')
    return "$(base_stripped)/$(suffix_stripped)"
end

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

# Return the chunk shape for a property of element type `T` and shape `shape`, given the per-call resolved `is_packed`
# flag. Returns `nothing` when the property should not be packed: either `is_packed = false`, or a single column's
# uncompressed bytes (`shape[1] * effective_sizeof(T)`) is below `packed_target_chunk_bytes()`. The format-level writer
# treats `nothing` as "use the flat single-chunk uncompressed encoding". Otherwise returns the packed chunk shape:
# `(n_chunk_rows,)` for vectors and `(n_chunk_rows, 1)` for matrices, where
# `n_chunk_rows = min(packed_target_chunk_bytes() ÷ effective_sizeof(T), shape[1])`. Internal helper for format-level
# write paths. Only 1-D vectors and 2-D matrices are supported. The per-column threshold (rather than total bytes)
# means matrices with short columns (e.g. `block × gene` shapes) stay flat even if they're large in total — chunks of
# a few hundred bytes aren't worth the codec overhead.
function chunks_for(is_packed::Bool, shape::NTuple{N, Int}, ::Type{T})::Maybe{NTuple{N, Int}} where {N, T}
    @assert N == 1 || N == 2
    if !is_packed
        return nothing
    end
    target_bytes = packed_target_chunk_bytes()
    element_bytes = effective_sizeof(T)
    if shape[1] * element_bytes < target_bytes
        return nothing
    end
    n_chunk_rows = min(target_bytes ÷ element_bytes, shape[1])
    if N == 1
        return (n_chunk_rows,)
    else
        @assert N == 2
        return (n_chunk_rows, 1)
    end
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
    else
        return daf.internal.packed_default
    end
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
    finalizer::Function
end

# Allocate a `PackedDenseMatrix` over the given encoder. The encoder closure has signature
# `(column::Int, chunk_buffer::Vector{T}) -> Nothing` and writes the chunk for `column` (the buffer's contents) to
# storage. The optional `finalizer` closure (signature `() -> Nothing`) runs once after every column slot has been
# flushed; it lets the format-level write path commit any per-array post-processing (e.g. emitting a shard index
# footer).
function PackedDenseMatrix{T}(
    n_rows::Int,
    n_columns::Int,
    encoder::Function;
    finalizer::Function = () -> nothing,
)::PackedDenseMatrix{T} where {T}
    thread_slots = [PackedThreadSlot{T}(0, Vector{T}(undef, n_rows)) for _ in 1:maxthreadid()]
    return PackedDenseMatrix{T}(n_rows, n_columns, thread_slots, encoder, finalizer)
end

function Base.size(matrix::PackedDenseMatrix)::Tuple{Int, Int}
    return (matrix.n_rows, matrix.n_columns)
end

function Base.view(matrix::PackedDenseMatrix{T}, ::Colon, column::Int)::Vector{T} where {T}
    @assert 1 <= column <= matrix.n_columns
    slot = matrix.thread_slots[threadid()]
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
    matrix.finalizer()
    return nothing
end

function TanayLabUtilities.MatrixLayouts.unnamed_relayout(
    destination::PackedDenseMatrix{T},
    source::AbstractMatrix,
)::AbstractMatrix where {T}
    @assert size(destination) == size(source)
    @assert !issparse(source)
    n_columns = size(destination, 2)
    parallel_loop_wo_rng(
        1:n_columns;
        name = "PackedDenseMatrix.relayout",
        progress = DebugProgress(n_columns; group = :daf_loops, desc = "PackedDenseMatrix.relayout"),
    ) do column_index
        view(destination, :, column_index) .= view(source, :, column_index)
        return nothing
    end
    return destination
end

# `DiskArrays.AbstractDiskArray` wrapper for an N-dimensional numeric array served as a byte stream via the
# `byte_fetcher` closure (HTTP range GETs, an mmap of a local file, the bytes of a ZIP entry, etc.).
# `readblock!` enumerates the chunks intersecting the requested slice, coalesces the missing ones into the
# smallest set of byte-contiguous fetches (gap ≤ [`DAF_HTTP_MAX_COALESCE_GAP_KB`](@ref)), decodes each fetched
# chunk through the configured pipeline, and caches per-chunk decoded data in an LRU sized by
# [`DAF_PACKED_HTTP_CACHE_KB`](@ref).
#
# The wrapper is configured by closures so the same machinery serves flat dense vector
# ([`StripedVector`](@ref)), flat dense matrix ([`StripedMatrix`](@ref)), v3-sharded packed dense array
# ([`PackedDenseArray`](@ref)), and ZIP-CD-based packed shard reads ([`ZipShardArray`](@ref)).
#
# - `decode_chunk(encoded_bytes::Vector{UInt8}, chunk_shape_in_array::NTuple{N, Int}, method::UInt16) -> Array{T, N}`
#   converts raw fetched bytes into a chunk-shaped buffer. `method` is the ZIP compression method (relevant
#   only to the CD-based reader); flat and shard-index decoders ignore it (callers supply sentinel
#   `UInt16(0)`).
# - `ensure_index() -> Nothing` is called before any chunk's byte range is queried. For flat data it's a no-op;
#   for packed data it fetches and parses the metadata (Zarr shard index or ZIP central directory) on first
#   call (cached after).
# - `chunk_byte_range(chunk_coords::CartesianIndex{N}) -> Maybe{Tuple{Int, Int, UInt16}}` returns
#   `(byte_offset, byte_end_exclusive, compression_method)` of the chunk's encoded bytes, or `nothing` for an
#   empty (fill-only) chunk. The compression method is the per-entry ZIP method for CD-based readers and
#   `UInt16(0)` for flat / shard-index readers.
# - `byte_fetcher(offset::Int, n_bytes::Int) -> Vector{UInt8}` reads one byte slab from the underlying
#   resource.
struct ChunkedArray{T, N} <: DiskArrays.AbstractDiskArray{T, N}  # NOLINT
    shape::NTuple{N, Int}
    chunk_shape::NTuple{N, Int}
    decode_chunk::Function
    ensure_index::Function
    chunk_byte_range::Function
    byte_fetcher::Function
    cache::LRUCache.LRU{CartesianIndex{N}, Array{T, N}}
end

function Base.size(array::ChunkedArray{T, N})::NTuple{N, Int} where {T, N}
    return array.shape
end

function DiskArrays.haschunks(::ChunkedArray)::DiskArrays.Chunked
    return DiskArrays.Chunked()
end

function DiskArrays.eachchunk(array::ChunkedArray{T, N})::DiskArrays.GridChunks where {T, N}
    return DiskArrays.GridChunks(array, array.chunk_shape)
end

# `ChunkedArray` doesn't expose `strides` (chunked storage), so the default `MatrixLayouts.major_axis`
# fallback returns `nothing` and breaks `assert_valid_matrix`. The wire format is column-major (flat matrices
# decode column tiles directly; packed shards use chunk shape `(_, 1)` slabbed along columns) — declare it.
function TanayLabUtilities.MatrixLayouts.major_axis(::ChunkedArray{T, 2})::Maybe{Int8} where {T}
    return TanayLabUtilities.MatrixLayouts.Columns
end

function DiskArrays.readblock!(
    array::ChunkedArray{T, N},
    destination::AbstractArray{<:Any, N},
    ranges::Vararg{AbstractUnitRange, N},
)::Nothing where {T, N}
    array.ensure_index()
    first_chunk_coords = ntuple(dim -> (first(ranges[dim]) - 1) ÷ array.chunk_shape[dim] + 1, N)
    last_chunk_coords = ntuple(dim -> (last(ranges[dim]) - 1) ÷ array.chunk_shape[dim] + 1, N)
    chunk_range = CartesianIndices(ntuple(dim -> first_chunk_coords[dim]:last_chunk_coords[dim], N))

    missing_chunk_coords = CartesianIndex{N}[]
    for chunk_coords in chunk_range
        if !haskey(array.cache, chunk_coords)
            push!(missing_chunk_coords, chunk_coords)
        end
    end
    fetch_chunks_coalesced!(array, missing_chunk_coords)

    for chunk_coords in chunk_range
        copy_chunk_into_destination!(destination, array.cache[chunk_coords], chunk_coords, array.chunk_shape, ranges)
    end
    return nothing
end

# Clamped chunk extent (in elements) along each dim. For interior chunks this equals `array.chunk_shape`; for
# chunks at the trailing edge of any dim it is smaller.
function chunk_shape_in_array(array::ChunkedArray{T, N}, chunk_coords::CartesianIndex{N})::NTuple{N, Int} where {T, N}
    return ntuple(N) do dim
        first_in_array = (chunk_coords[dim] - 1) * array.chunk_shape[dim] + 1
        return min(array.chunk_shape[dim], array.shape[dim] - first_in_array + 1)
    end
end

# Copy the portion of an in-cache decoded `chunk` that intersects the user-requested `ranges` into the
# corresponding slice of `destination`.
function copy_chunk_into_destination!(
    destination::AbstractArray{<:Any, N},
    chunk::Array{T, N},
    chunk_coords::CartesianIndex{N},
    chunk_shape::NTuple{N, Int},
    ranges::NTuple{N, <:AbstractUnitRange},
)::Nothing where {T, N}
    chunk_first_in_array = ntuple(dim -> (chunk_coords[dim] - 1) * chunk_shape[dim] + 1, N)
    chunk_last_in_array = ntuple(dim -> chunk_first_in_array[dim] + size(chunk, dim) - 1, N)
    copy_first_in_array = ntuple(dim -> max(first(ranges[dim]), chunk_first_in_array[dim]), N)
    copy_last_in_array = ntuple(dim -> min(last(ranges[dim]), chunk_last_in_array[dim]), N)
    destination_ranges = ntuple(N) do dim
        return (copy_first_in_array[dim] - first(ranges[dim]) + 1):(copy_last_in_array[dim] - first(ranges[dim]) + 1)
    end
    chunk_ranges = ntuple(N) do dim
        return (copy_first_in_array[dim] - chunk_first_in_array[dim] + 1):(copy_last_in_array[dim] - chunk_first_in_array[dim] + 1)
    end
    @views destination[destination_ranges...] .= chunk[chunk_ranges...]  # NOJET
    return nothing
end

# Sort missing chunks by byte offset, group consecutive ones into byte-contiguous Range GET spans (gap ≤
# `DAF_HTTP_MAX_COALESCE_GAP_KB`), fetch each span, decode each chunk via `array.decode_chunk`, and populate the
# cache. Empty chunks (those for which `array.chunk_byte_range` returns `nothing`, i.e. packed fill chunks) are
# cached as zero-filled tiles without any HTTP traffic.
function fetch_chunks_coalesced!(
    array::ChunkedArray{T, N},
    missing_chunk_coords::Vector{CartesianIndex{N}},
)::Nothing where {T, N}
    if isempty(missing_chunk_coords)
        return nothing
    end
    empty_chunk_coords = CartesianIndex{N}[]
    nonempty_with_offset = Tuple{CartesianIndex{N}, Int, Int, UInt16}[]
    for chunk_coords in missing_chunk_coords
        range = array.chunk_byte_range(chunk_coords)
        if range === nothing
            push!(empty_chunk_coords, chunk_coords)  # UNTESTED
        else
            push!(nonempty_with_offset, (chunk_coords, range[1], range[2], range[3]))
        end
    end
    for chunk_coords in empty_chunk_coords
        array.cache[chunk_coords] = zeros(T, chunk_shape_in_array(array, chunk_coords))  # NOJET  # UNTESTED
    end

    if isempty(nonempty_with_offset)
        return nothing  # UNTESTED
    end
    sort!(nonempty_with_offset; by = entry -> entry[2])
    max_gap_bytes = packed_http_max_coalesce_gap_bytes()

    span_first_index = 1
    span_last_byte_end = nonempty_with_offset[1][3]
    for next_index in 2:length(nonempty_with_offset)  # NOLINT
        next_offset = nonempty_with_offset[next_index][2]  # NOLINT
        gap_n_bytes = next_offset - span_last_byte_end
        if gap_n_bytes <= max_gap_bytes
            span_last_byte_end = nonempty_with_offset[next_index][3]
        else
            fetch_chunk_span!(array, nonempty_with_offset, span_first_index, next_index - 1)
            span_first_index = next_index
            span_last_byte_end = nonempty_with_offset[next_index][3]
        end
    end
    fetch_chunk_span!(array, nonempty_with_offset, span_first_index, length(nonempty_with_offset))
    return nothing
end

# Issue one Range GET covering the byte-contiguous span from `span_first_index` through `span_last_index`
# (inclusive, indices into `nonempty_with_offset`), decode each chunk in the span via `array.decode_chunk`, and
# populate the cache.
function fetch_chunk_span!(
    array::ChunkedArray{T, N},
    nonempty_with_offset::Vector{Tuple{CartesianIndex{N}, Int, Int, UInt16}},
    span_first_index::Int,
    span_last_index::Int,
)::Nothing where {T, N}
    first_offset = nonempty_with_offset[span_first_index][2]
    last_end = nonempty_with_offset[span_last_index][3]
    span_bytes = array.byte_fetcher(first_offset, last_end - first_offset)

    for span_index in span_first_index:span_last_index
        chunk_coords, chunk_offset, chunk_end, chunk_method = nonempty_with_offset[span_index]
        offset_in_span = chunk_offset - first_offset
        chunk_byte_view = view(span_bytes, (offset_in_span + 1):(chunk_end - first_offset))
        array.cache[chunk_coords] =
            array.decode_chunk(Vector{UInt8}(chunk_byte_view), chunk_shape_in_array(array, chunk_coords), chunk_method)
    end
    return nothing
end

# Closure factory for the chunk-byte-range function over a flat (column-major, contiguous, uncompressed) byte
# stream where every chunk has shape `chunk_shape` (with `1` in every dim past the first, so each chunk is
# byte-contiguous on disk). Used by `StripedVector` and `StripedMatrix`.
function flat_chunk_byte_range_closure(  # ONLY SEEMS UNTESTED
    shape::NTuple{N, Int},
    chunk_shape::NTuple{N, Int},
    sizeof_T::Int,
)::Function where {N}
    function compute_range(chunk_coords::CartesianIndex{N})::Tuple{Int, Int, UInt16}
        first_linear_position = 0
        stride = 1
        for dim in 1:N
            first_in_dim = (chunk_coords[dim] - 1) * chunk_shape[dim] + 1
            first_linear_position += (first_in_dim - 1) * stride
            stride *= shape[dim]
        end
        chunk_n_elements = 1
        for dim in 1:N
            first_in_dim = (chunk_coords[dim] - 1) * chunk_shape[dim] + 1
            chunk_n_elements *= min(chunk_shape[dim], shape[dim] - first_in_dim + 1)
        end
        byte_offset = first_linear_position * sizeof_T
        return (byte_offset, byte_offset + chunk_n_elements * sizeof_T, UInt16(0))
    end
    return compute_range
end

# Closure factory for the decoder of a flat chunk — `reinterpret` the raw bytes as `T` and `reshape` to the
# (possibly partial-at-edge) chunk shape inside the array. The `method` argument is part of the shared
# `ChunkedArray` decode contract and ignored here (flat reads have no ZIP framing).
function flat_decode_closure(::Type{T})::Function where {T}
    function decode_flat(bytes::Vector{UInt8}, chunk_shape_in_array::NTuple{N, Int}, ::UInt16)::Array{T, N} where {N}
        n_elements = prod(chunk_shape_in_array)
        @assert length(bytes) == n_elements * sizeof(T)
        return reshape(collect(reinterpret(T, bytes)), chunk_shape_in_array)
    end
    return decode_flat
end

# Pick the LRU capacity (number of cached chunks) from the global HTTP cache size budget. The budget is
# expressed in bytes; we divide by the per-chunk byte estimate and clamp to at least 1 entry.
function http_chunk_cache_capacity(n_bytes_per_chunk::Int)::Int
    return max(1, packed_http_cache_mb() * 1_000_000 ÷ n_bytes_per_chunk)
end

# Same as [`http_chunk_cache_capacity`](@ref) but draws from the local-read cache budget. Used by FilesDaf
# and ZipDaf packed reads, which mmap or hold the shard bytes in memory and so warrant a smaller per-array
# decoded-chunk cache than HTTP reads (where each miss costs a network round-trip).
function local_chunk_cache_capacity(n_bytes_per_chunk::Int)::Int
    return max(1, packed_local_cache_mb() * 1_000_000 ÷ n_bytes_per_chunk)
end

"""
    StripedVector(::Type{T}, n_elements::Integer, stripe_n_elements::Integer, byte_fetcher::Function)::ChunkedArray{T, 1}

Factory for a lazy 1-D `DiskArrays.AbstractDiskArray` that fetches a flat dense numeric vector served over
HTTP in stripes (Range GETs of `stripe_n_elements` elements at a time, coalesced when adjacent).
"""
function StripedVector(
    ::Type{T},
    n_elements::Integer,
    stripe_n_elements::Integer,
    byte_fetcher::Function,
)::ChunkedArray{T, 1} where {T}
    shape = (Int(n_elements),)
    chunk_shape = (Int(stripe_n_elements),)
    capacity = http_chunk_cache_capacity(Int(stripe_n_elements) * sizeof(T))
    return ChunkedArray{T, 1}(  # NOJET
        shape,
        chunk_shape,
        flat_decode_closure(T),
        () -> nothing,
        flat_chunk_byte_range_closure(shape, chunk_shape, sizeof(T)),
        byte_fetcher,
        LRUCache.LRU{CartesianIndex{1}, Array{T, 1}}(; maxsize = capacity),  # NOJET
    )
end

"""
    StripedMatrix(::Type{T}, n_rows::Integer, n_columns::Integer, stripe_n_rows::Integer, byte_fetcher::Function)::ChunkedArray{T, 2}

Factory for a lazy 2-D `DiskArrays.AbstractDiskArray` that fetches a flat dense numeric column-major matrix
served over HTTP in column tiles of shape `(stripe_n_rows, 1)` (coalesced when adjacent).
"""
function StripedMatrix(
    ::Type{T},
    n_rows::Integer,
    n_columns::Integer,
    stripe_n_rows::Integer,
    byte_fetcher::Function,
)::ChunkedArray{T, 2} where {T}
    shape = (Int(n_rows), Int(n_columns))
    chunk_shape = (Int(stripe_n_rows), 1)
    capacity = http_chunk_cache_capacity(Int(stripe_n_rows) * sizeof(T))
    return ChunkedArray{T, 2}(
        shape,
        chunk_shape,
        flat_decode_closure(T),
        () -> nothing,
        flat_chunk_byte_range_closure(shape, chunk_shape, sizeof(T)),
        byte_fetcher,
        LRUCache.LRU{CartesianIndex{2}, Array{T, 2}}(; maxsize = capacity),
    )
end

"""
    PackedDenseArray(
        ::Type{T},
        shape::NTuple{N, Int},
        chunk_shape::NTuple{N, Int},
        codec::PackedCodec,
        index_location::Symbol,
        byte_fetcher::Function,
        suffix_byte_fetcher::Function,
    )::ChunkedArray{T, N}

Factory for a lazy N-dimensional `DiskArrays.AbstractDiskArray` that fetches a v3-sharded packed dense array
served over HTTP. The shard index footer is fetched once on first read (one Range GET via
`suffix_byte_fetcher` when `index_location == :end`, otherwise via `byte_fetcher(0, index_size)`); subsequent
reads look up per-chunk byte ranges in the cached index, coalesce adjacent ones, and decode each fetched
chunk through the codec pipeline.
"""
function PackedDenseArray(
    ::Type{T},
    shape::NTuple{N, Int},
    chunk_shape::NTuple{N, Int},
    codec::PackedCodec,
    index_location::Symbol,
    byte_fetcher::Function,
    suffix_byte_fetcher::Function,
)::ChunkedArray{T, N} where {T, N}
    chunks_per_shard = Zarr.Codecs.V3Codecs.calculate_chunks_per_shard(shape, chunk_shape)
    sharding_codec =
        build_shard_metadata(T, shape, chunk_shape, v3_bytes_codecs_for(codec, T), index_location).pipeline.array_bytes
    index_size = Zarr.Codecs.V3Codecs.compute_encoded_index_size(chunks_per_shard, sharding_codec)
    shard_index_ref = Ref{Maybe{Zarr.Codecs.V3Codecs.ShardIndex{N}}}(nothing)

    function ensure_index()::Nothing
        if shard_index_ref[] !== nothing
            return nothing
        end
        index_bytes = if index_location == :end
            suffix_byte_fetcher(index_size)  # UNTESTED
        else
            byte_fetcher(0, index_size)
        end
        shard_index_ref[] = Zarr.Codecs.V3Codecs.decode_shard_index(index_bytes, chunks_per_shard, sharding_codec)
        return nothing
    end

    function chunk_byte_range(chunk_coords::CartesianIndex{N})::Maybe{Tuple{Int, Int, UInt16}}
        shard_index = shard_index_ref[]
        @assert shard_index !== nothing
        range = Zarr.Codecs.V3Codecs.get_chunk_slice(shard_index, Tuple(chunk_coords))
        return range === nothing ? nothing : (range[1], range[2], UInt16(0))
    end

    function decode_chunk(encoded::Vector{UInt8}, this_chunk_shape::NTuple{N, Int}, ::UInt16)::Array{T, N}
        # Allocate the full inner-chunk shape and trim for trailing-edge partial chunks: the sharding codec
        # knows the inner chunk shape from its metadata and pads short chunks with the array's fill value, so
        # decoding into the full-shape buffer is safe and the trim drops only padding.
        full_chunk = Array{T}(undef, chunk_shape)
        Zarr.pipeline_decode!(sharding_codec.codecs, full_chunk, encoded)
        if this_chunk_shape == chunk_shape
            return full_chunk
        else
            return full_chunk[ntuple(dim -> 1:this_chunk_shape[dim], N)...]
        end
    end

    capacity = http_chunk_cache_capacity(prod(chunk_shape) * sizeof(T))
    return ChunkedArray{T, N}(
        shape,
        chunk_shape,
        decode_chunk,
        ensure_index,
        chunk_byte_range,
        byte_fetcher,
        LRUCache.LRU{CartesianIndex{N}, Array{T, N}}(; maxsize = capacity),
    )
end

"""
    ZipShardArray(
        ::Type{T},
        shape::NTuple{N, Int},
        chunk_shape::NTuple{N, Int},
        codec::PackedCodec,
        byte_fetcher::Function,
        suffix_byte_fetcher::Function,
    )::ChunkedArray{T, N}

Factory for a lazy N-dimensional `DiskArrays.AbstractDiskArray` that reads a ZIP-archive shard file via its
central directory. `byte_fetcher` and `suffix_byte_fetcher` abstract over the source (HTTP range GETs, an
mmap of a local file, the in-memory bytes of an outer-ZIP entry).

The EOCD region (a fixed-size tail block) is fetched once on first read to learn the CD's absolute offset;
each chunk's CD entry is then random-accessed by `cd_offset + (chunk_index - 1) * cd_entry_size` (fixed-size
entries, see `inner_chunk_name_bytes`) and parsed on demand. No code walks the whole CD. Per-chunk reads
coalesce adjacent byte ranges and decode through `decode_zip_entry`.
"""
function ZipShardArray(
    ::Type{T},
    shape::NTuple{N, Int},
    chunk_shape::NTuple{N, Int},
    codec::PackedCodec,
    byte_fetcher::Function,
    suffix_byte_fetcher::Function;
    cache_capacity::Int = http_chunk_cache_capacity(prod(chunk_shape) * sizeof(T)),
)::ChunkedArray{T, N} where {T, N}
    chunks_per_shard = Zarr.Codecs.V3Codecs.calculate_chunks_per_shard(shape, chunk_shape)
    inner_pipeline = build_inner_pipeline(T, v3_bytes_codecs_for(codec, T))
    method = zip_method_for_pipeline(inner_pipeline)
    cd_entry_size =
        CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE +
        inner_chunk_name_length(chunks_per_shard, method) +
        ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
    cd_buffer_ref = Ref{Maybe{Vector{UInt8}}}(nothing)
    chunk_indices = CartesianIndices(chunks_per_shard)

    function ensure_index()::Nothing
        if cd_buffer_ref[] !== nothing
            return nothing
        end
        eocd_region = suffix_byte_fetcher(TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
        # ZIP64 EOCD record sits first in the EOCD region; its signature is at byte 1 and the CD's absolute
        # offset is at byte 49 (skipping signature, record size, version-made, version-needed, disk numbers,
        # entries-on-disk, total entries, and CD size — 48 preceding bytes).
        @assert read_little_endian_uint32(eocd_region, 1) == ZIP64_END_OF_CENTRAL_DIRECTORY_SIGNATURE
        cd_offset = read_little_endian_uint64(eocd_region, 1 + 48)
        # Materialize to a concrete `Vector{UInt8}` once at storage time so the typed `Ref` keeps every
        # `chunk_byte_range`'s `parse_zip_central_directory_entry` calls type-stable. An abstract Ref
        # boxes every `getindex` (measured ~1800× slowdown + N allocations per read on the hot path);
        # one upfront copy of the CD span is dwarfed by every subsequent chunk read.
        cd_buffer_ref[] = Vector{UInt8}(byte_fetcher(Int(cd_offset), Int(prod(chunks_per_shard) * cd_entry_size)))
        return nothing
    end

    function chunk_byte_range(chunk_coords::CartesianIndex{N})::Maybe{Tuple{Int, Int, UInt16}}
        chunk_index = LinearIndices(chunk_indices)[chunk_coords]
        cd_buffer = cd_buffer_ref[]
        @assert cd_buffer !== nothing
        position = (chunk_index - 1) * cd_entry_size + 1
        data_offset, chunk_size, entry_method = parse_zip_central_directory_entry(cd_buffer, position)
        expected_name = inner_chunk_name_bytes(chunks_per_shard, chunk_index, entry_method)
        name_position = position + CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE
        actual_name = view(cd_buffer, name_position:(name_position + length(expected_name) - 1))
        if actual_name != expected_name
            error(chomp("""  # UNTESTED
                        unexpected ZIP CD entry name: $(format_zip_entry_name(actual_name))
                        expected name: $(format_zip_entry_name(expected_name))
                        at chunk: $(chunk_index)
                        """))
        end
        return (Int(data_offset), Int(data_offset + chunk_size), entry_method)
    end

    function decode_chunk(encoded::Vector{UInt8}, this_chunk_shape::NTuple{N, Int}, method::UInt16)::Array{T, N}
        full_chunk = decode_zip_entry(method, encoded, chunk_shape, inner_pipeline, T)
        if this_chunk_shape == chunk_shape
            return full_chunk
        else
            return full_chunk[ntuple(dim -> 1:this_chunk_shape[dim], N)...]
        end
    end

    return ChunkedArray{T, N}(  # NOJET
        shape,
        chunk_shape,
        decode_chunk,
        ensure_index,
        chunk_byte_range,
        byte_fetcher,
        LRUCache.LRU{CartesianIndex{N}, Array{T, N}}(; maxsize = cache_capacity),
    )
end

# Parse a single CD entry at `position` (1-based) in `cd_region_bytes` and return
# `(data_offset, compressed_size, compression_method)`. Callers compute the position by index
# arithmetic over the fixed-size CD entries (`cd_offset + i * cd_entry_size`), so this provides
# O(1) random access to any chunk's metadata without walking predecessors.
function parse_zip_central_directory_entry(
    cd_region_bytes::AbstractVector{UInt8},
    position::Int,
)::Tuple{UInt64, UInt64, UInt16}
    @assert read_little_endian_uint32(cd_region_bytes, position) == CENTRAL_DIRECTORY_ENTRY_SIGNATURE
    compression_method = read_little_endian_uint16(cd_region_bytes, position + 10)
    name_length = read_little_endian_uint16(cd_region_bytes, position + 28)
    extra_position = position + CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + Int(name_length)
    @assert read_little_endian_uint16(cd_region_bytes, extra_position) == ZIP64_EXTRA_HEADER_ID
    compressed_size = read_little_endian_uint64(cd_region_bytes, extra_position + 12)
    lfh_offset = read_little_endian_uint64(cd_region_bytes, extra_position + 20)
    # DEFLATE entries omit the ZIP64 extra from their LFH (the writer places the gzip header in
    # the name field, which must end exactly at the DEFLATE payload start — no extra fields allowed
    # in between). Every other method's LFH carries the standard ZIP64 extra.
    lfh_extra_size = compression_method == DEFLATE_COMPRESSION_METHOD ? 0 : ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE
    lfh_size = UInt64(LOCAL_FILE_HEADER_FIXED_SIZE) + UInt64(name_length) + UInt64(lfh_extra_size)
    return (lfh_offset + lfh_size, compressed_size, compression_method)
end

# Decode one ZIP entry's bytes into a chunk-shaped array per its CD-declared compression method.
# Method 0 (STORED) feeds the entry bytes through `inner_pipeline` (the Zarr v3 codec pipeline the
# descriptor implies): the entry bytes are the codec output verbatim. Method 93 (ZSTD) likewise
# feeds the entry bytes through `inner_pipeline` because the Zarr ZstdV3Codec consumes the same
# raw zstd frame the ZIP entry data holds. Method 8 (DEFLATE) entries hold a raw DEFLATE payload
# (the gzip header sits in the LFH name field and the trailer as dead bytes after the entry); we
# decompress the payload manually and then run only the array→bytes step of the pipeline.
#
# The CD-declared method must match what the descriptor codec implies — STORED iff descriptor has
# no bytes-bytes codec, DEFLATE iff gzip, ZSTD iff zstd. Anything else is a contradiction between
# the two layers of metadata (e.g. STORED + zstd descriptor would claim "uncompressed" entry bytes
# that are actually a zstd frame).
function decode_zip_entry(
    method::UInt16,
    encoded::Vector{UInt8},
    chunk_shape::NTuple{N, Int},
    inner_pipeline::Zarr.AbstractCodecPipeline,
    ::Type{T},
)::Array{T, N} where {T, N}
    expected_method = zip_method_for_pipeline(inner_pipeline)
    if method != expected_method
        descriptor_codec = # UNTESTED
            isempty(inner_pipeline.bytes_bytes) ? :none : nameof(typeof(inner_pipeline.bytes_bytes[1]))
        error(chomp("""  # UNTESTED
                    unexpected ZIP compression method: $(Int(method))
                    expected ZIP compression method: $(Int(expected_method))
                    for descriptor codec: $(descriptor_codec)
                    """))
    end
    full_chunk = Array{T}(undef, chunk_shape)
    if method == STORED_COMPRESSION_METHOD || method == ZSTD_COMPRESSION_METHOD
        Zarr.pipeline_decode!(inner_pipeline, full_chunk, encoded)  # NOJET
    elseif method == DEFLATE_COMPRESSION_METHOD
        raw_bytes = transcode(DeflateDecompressor, encoded)
        array_bytes_pipeline = Zarr.V3Pipeline((), inner_pipeline.array_bytes, ())
        Zarr.pipeline_decode!(array_bytes_pipeline, full_chunk, raw_bytes)  # NOJET
    else
        error("unsupported ZIP compression method $(Int(method)) for chunk decode")  # UNTESTED
    end
    return full_chunk
end

# Closure that captures `buffer` (an mmap of a shard file or the in-memory bytes of a ZIP entry)
# and resolves to a `(offset, n_bytes) -> AbstractVector{UInt8}` fetcher with the same signature as
# `url_byte_fetcher` (which returns `Vector{UInt8}`). The local-buffer variant returns a zero-copy
# view of the buffer slice.
function buffer_byte_fetcher(buffer::AbstractVector{UInt8})::Function
    return (offset::Int, n_bytes::Int) -> view(buffer, (offset + 1):(offset + n_bytes))
end

# Closure that captures `buffer` and resolves to a `(n_bytes) -> AbstractVector{UInt8}` suffix
# fetcher with the same signature as `url_suffix_byte_fetcher` (which returns `Vector{UInt8}`).
# The local-buffer variant returns a zero-copy view of the buffer tail.
function buffer_suffix_byte_fetcher(buffer::AbstractVector{UInt8})::Function
    file_size = length(buffer)
    return (n_bytes::Int) -> view(buffer, (file_size - n_bytes + 1):file_size)
end

# Reconstruct the [`PackedCodec`](@ref) recorded in a packed-property descriptor (JSON sidecar for
# FilesDaf / ZipDaf / HttpDaf, or a `Zarr` array descriptor lifted into the same shape).
function packed_codec_from_descriptor(descriptor::AbstractDict)::PackedCodec
    return PackedCodec(Symbol(descriptor["compression"]), Int(descriptor["compression_level"]))
end

# Open a packed shard whose bytes are already in `buffer` (mmap of a `<name>.zip` file for FilesDaf,
# `MmapZipStore`-served entry bytes for ZipDaf) as a [`ZipShardArray`](@ref) parameterised by the
# `descriptor`'s codec and inner chunk shape. The local-read cache budget is used so on-disk shards
# don't compete with the HTTP read cache for memory.
function open_packed_shard_from_buffer(
    buffer::AbstractVector{UInt8},
    ::Type{T},
    descriptor::AbstractDict,
    shape::NTuple{N, Int},
)::ChunkedArray{T, N} where {T, N}
    chunk_shape = NTuple{N, Int}(descriptor["chunk_shape"])  # NOJET
    codec = packed_codec_from_descriptor(descriptor)
    return ZipShardArray(
        T,
        shape,
        chunk_shape,
        codec,
        buffer_byte_fetcher(buffer),
        buffer_suffix_byte_fetcher(buffer);
        cache_capacity = local_chunk_cache_capacity(prod(chunk_shape) * sizeof(T)),
    )
end

# ==============================================================================
# BEGIN: Zarr.jl 0.10 v3 workarounds.
#
# Zarr.jl 0.10's V3 codec registry has no `vlen-utf8` entry, so writing a Zarr v3 array of `String` fails (the
# default `BytesCodec` cannot reinterpret `String`). The codec below mirrors the wire format of Zarr.jl's v2
# `VLenUTF8Filter` (numcodecs `vlen-utf8`) so v3 stores written here round-trip through the same standard
# encoding. `__init__` checks at module load whether Zarr.jl now ships its own `vlen-utf8` v3 codec; if so, it
# logs a warning and skips the local registration. Also defines a `Base.sizeof(::Type{String})` shim because
# Zarr.jl 0.10's v3 metadata parser computes `sizeof(T)` unconditionally for the blosc-typesize default of the
# codec context (irrelevant on our path because every blosc instance we emit carries an explicit `typesize`).
# This entire block is meant to be deleted in favour of upstream support when Zarr.jl ships v3 vlen-utf8 and
# fixes the `sizeof` call site.
# ==============================================================================

struct VLenUTF8V3Codec <: Zarr.Codecs.V3Codecs.V3Codec{:array, :bytes} end  # NOLINT

Zarr.Codecs.V3Codecs.name(::VLenUTF8V3Codec) = "vlen-utf8"
Zarr.Codecs.V3Codecs.is_fixed_size(::VLenUTF8V3Codec) = false

JSON.lower(::VLenUTF8V3Codec) = Dict{String, Any}("name" => "vlen-utf8", "configuration" => Dict{String, Any}())

function Zarr.Codecs.V3Codecs.codec_encode(::VLenUTF8V3Codec, data::AbstractArray)::Vector{UInt8}
    buffer = IOBuffer()
    write(buffer, UInt32(length(data)))
    for element in data
        utf8_bytes = codeunits(element)
        write(buffer, UInt32(length(utf8_bytes)))
        write(buffer, utf8_bytes)
    end
    return take!(buffer)
end

function Zarr.Codecs.V3Codecs.codec_decode(
    ::VLenUTF8V3Codec,
    encoded::Vector{UInt8},
    ::Type{T},
    shape::NTuple{N, Int};
    fill_value = nothing,  # NOLINT
)::Array{String, N} where {T, N}
    buffer = IOBuffer(encoded)
    nitems = Int(read(buffer, UInt32))
    output = Array{String, N}(undef, shape)
    for index in 1:nitems
        len = Int(read(buffer, UInt32))
        output[index] = String(read(buffer, len))
    end
    close(buffer)
    return output
end

# Type piracy on `Base.sizeof` is bounded — `sizeof(::Type{String})` is otherwise undefined in Julia and is
# virtually never called outside the Zarr.jl 0.10 v3 metadata-parse code path that needs this shim.
Base.sizeof(::Type{String}) = 1

function __init__()
    if haskey(Zarr.Codecs.V3Codecs.codec_parsers, "vlen-utf8")
        @warn(  # UNTESTED
            "Zarr.jl now ships a built-in `vlen-utf8` v3 codec. The local copy in " *
            "`DataAxesFormats.PackedFormat` (the BEGIN/END `vlen-utf8` block in " *
            "`src/packed_format.jl` plus this `__init__`) should be removed in " *
            "favour of the upstream version."
        )
    else
        Zarr.Codecs.V3Codecs.register_codec("vlen-utf8", VLenUTF8V3Codec) do _config, _ctx  # NOLINT
            return VLenUTF8V3Codec()
        end
    end
    Zarr.typemap3["string"] = String
    return nothing
end

# ==============================================================================
# END: Zarr.jl 0.10 v3 workarounds.
# ==============================================================================

# Mmap-backed sink that lets `IncrementalShardWriter` write into a region reserved by
# [`reserve_mmap_zip_entry!`](@ref). The reserved region is over-allocated to an upper bound (raw chunk
# bytes plus exact index size); on close the entry is shrunk via [`shrink_mmap_zip_entry!`](@ref) to the
# high-water mark recorded in `max_offset`, so the only on-disk slack left is the ZIP64 extra-field bytes
# per entry.
mutable struct MmapShardRegion  # NOLINT
    store::MmapZipStore
    key::String
    region::AbstractVector{UInt8}
    cursor::UInt64
    max_offset::UInt64
    reserved_size::UInt64
end

# Growable in-memory sink (`InMemorySink`) used when the caller wants the encoded dual-format shard bytes
# returned as a `Vector{UInt8}` rather than persisted directly. After `finalize_shard!`, `take_bytes!`
# returns the underlying buffer trimmed to the high-water mark.
mutable struct InMemorySink  # NOLINT
    buffer::Vector{UInt8}
    cursor::UInt64
    max_offset::UInt64
end

InMemorySink()::InMemorySink = InMemorySink(UInt8[], UInt64(0), UInt64(0))

function take_bytes!(sink::InMemorySink)::Vector{UInt8}
    resize!(sink.buffer, Int(sink.max_offset))
    return sink.buffer
end

# Sink-abstraction methods. Each `IncrementalShardWriter` operates on its sink through:
# `position_in_sink` reads the current write cursor, `write_to_sink!` appends bytes (advancing the cursor),
# `seek_in_sink!` repositions the cursor for `:start` index emission, and `Base.close` finishes any
# backend-specific bookkeeping (closes the IO handle, or shrinks the reserved ZIP entry to its
# actual byte size and patches its CRC).
position_in_sink(io::IOStream)::UInt64 = UInt64(position(io))
position_in_sink(region::MmapShardRegion)::UInt64 = region.cursor
position_in_sink(sink::InMemorySink)::UInt64 = sink.cursor

function seek_in_sink!(io::IOStream, offset::UInt64)::Nothing
    seek(io, Int(offset))
    return nothing
end

function seek_in_sink!(region::MmapShardRegion, offset::UInt64)::Nothing
    region.cursor = offset
    return nothing
end

function seek_in_sink!(sink::InMemorySink, offset::UInt64)::Nothing
    sink.cursor = offset
    return nothing
end

function write_to_sink!(io::IOStream, bytes::AbstractVector{UInt8})::Nothing
    write(io, bytes)
    return nothing
end

function write_to_sink!(region::MmapShardRegion, bytes::AbstractVector{UInt8})::Nothing
    n = UInt64(length(bytes))
    region.region[(region.cursor + 1):(region.cursor + n)] .= bytes
    region.cursor += n
    if region.cursor > region.max_offset
        region.max_offset = region.cursor
    end
    return nothing
end

function write_to_sink!(sink::InMemorySink, bytes::AbstractVector{UInt8})::Nothing
    n = UInt64(length(bytes))
    needed = sink.cursor + n
    if needed > UInt64(length(sink.buffer))
        resize!(sink.buffer, Int(needed))
    end
    sink.buffer[(sink.cursor + 1):(sink.cursor + n)] .= bytes
    sink.cursor += n
    if sink.cursor > sink.max_offset
        sink.max_offset = sink.cursor
    end
    return nothing
end

function Base.close(region::MmapShardRegion)::Nothing
    shrink_mmap_zip_entry!(region.store, region.key, region.max_offset)
    patch_mmap_zip_entry_crc!(region.store, region.key)
    return nothing
end

Base.close(::InMemorySink)::Nothing = nothing

# Streaming writer for dual-format shards — simultaneously a valid Zarr v3 sharded-array byte stream AND a
# valid ZIP archive whose entries are the per-inner-chunk byte blobs. Layout: `[Zarr index | LFH 0 | chunk 0
# bytes | LFH 1 | chunk 1 bytes | ... | CD entries | ZIP64 EOCD region]`. The Zarr index lives at offset 0
# (placeholder reserved at construction; written on `finalize_shard!` after all chunks); the EOCD owns the
# file's tail so the byte stream is also `unzip`-readable. Inner-chunk ZIP entry names are zero-padded
# fixed-width (see [`inner_chunk_name_bytes`](@ref)) so every CD entry and LFH is fixed-stride, enabling
# O(1) random access to any chunk's CD entry by index arithmetic.
#
# Concurrent submitters call `submit_shard_chunk!` with an inner-chunk index and the chunk's `AbstractArray`
# of data; the writer encodes the chunk through `inner_pipeline` (outside the lock) and writes
# `[LFH | encoded chunk bytes]` to the sink under the lock, recording the chunk's offset/size in the in-memory
# Zarr index slab AND a per-chunk CD entry. Chunks may arrive in any order — the index slab and CD slot
# are keyed by chunk index. Elided chunks (encoder returns empty bytes — currently unreachable with the
# shipped codecs) leave the index slot at the `MAX_UINT64` sentinel and produce no ZIP entry.
mutable struct IncrementalShardWriter{S, P1 <: Zarr.AbstractCodecPipeline, P2 <: Zarr.AbstractCodecPipeline, F}  # NOLINT
    sink::S
    inner_pipeline::P1
    index_pipeline::P2
    fill_value::F
    index_data::Vector{UInt64}
    index_size::UInt64
    chunks_per_shard::Tuple
    cd_entries::Vector{Maybe{Tuple{UInt32, UInt64, UInt64, UInt64, Vector{UInt8}}}}
    codec_json_cd_entry::Maybe{Tuple{UInt32, UInt64, UInt64, UInt64, Vector{UInt8}}}
    write_lock::ReentrantLock
end

function IncrementalShardWriter(
    sink,
    inner_pipeline::Zarr.AbstractCodecPipeline,
    index_pipeline::Zarr.AbstractCodecPipeline,
    fill_value,
    chunks_per_shard::Tuple,
)::IncrementalShardWriter
    n_chunks = prod(chunks_per_shard)
    index_data = fill(typemax(UInt64), 2 * n_chunks)
    # 16 bytes per inner chunk (8-byte offset + 8-byte length) + 4-byte CRC32c, matching the standard
    # index_pipeline (BytesCodec little-endian + CRC32cV3Codec).
    index_size = UInt64(16 * n_chunks + 4)
    seek_in_sink!(sink, index_size)
    cd_entries = Vector{Maybe{Tuple{UInt32, UInt64, UInt64, UInt64, Vector{UInt8}}}}(nothing, n_chunks)
    return IncrementalShardWriter(
        sink,
        inner_pipeline,
        index_pipeline,
        fill_value,
        index_data,
        index_size,
        chunks_per_shard,
        cd_entries,
        nothing,
        ReentrantLock(),
    )
end

function submit_shard_chunk!(writer::IncrementalShardWriter, chunk_index::Int, chunk_data::AbstractArray)::Nothing
    inner = writer.inner_pipeline
    method = zip_method_for_pipeline(inner)
    # Run the array→bytes step (BytesCodec / VLenUTF8) separately from the bytes→bytes codec chain so
    # we can use the BytesCodec output for the ZIP CRC + uncompressed_size fields when the entry method
    # is a ZIP-known codec (ZSTD 93, DEFLATE 8) — those fields must match what a generic ZIP tool sees
    # after auto-decompression, which is the BytesCodec output, not the codec-encoded entry bytes.
    raw_bytes = Zarr.Codecs.V3Codecs.codec_encode(inner.array_bytes, chunk_data)
    name_bytes = inner_chunk_name_bytes(writer.chunks_per_shard, chunk_index, method)

    if method == DEFLATE_COMPRESSION_METHOD
        # Gzip lie: the LFH name carries the 10-byte gzip header, the entry data is the raw DEFLATE
        # payload, and 8 bytes of gzip trailer follow as dead bytes (ZIP-side ignores; Zarr-side reads
        # them via the shard-index range). The LFH has no ZIP64 extra so that the name field ends
        # exactly at the DEFLATE payload start — a single contiguous Zarr range from name-start spans
        # `[gzip header][DEFLATE payload][gzip trailer]`, a valid gzip stream for the Gzip codec.
        crc = zip_crc32(raw_bytes)
        payload = transcode(DeflateCompressor, raw_bytes)
        # 8-byte gzip trailer: little-endian CRC32 of the uncompressed data + ISIZE (uncompressed
        # length modulo 2^32). Written as dead bytes after the entry data; ZIP-side ignores them.
        trailer = Vector{UInt8}(undef, 8)
        write_little_endian_uint32!(trailer, 1, crc)
        write_little_endian_uint32!(trailer, 5, UInt32(length(raw_bytes) % UInt32))
        compressed_size = UInt64(length(payload))
        uncompressed_size = UInt64(length(raw_bytes))
        lfh_size = UInt64(LOCAL_FILE_HEADER_FIXED_SIZE + length(name_bytes))
        lfh_buffer = Vector{UInt8}(undef, lfh_size)
        write_local_file_header!(
            lfh_buffer,
            1,
            name_bytes,
            compressed_size,
            uncompressed_size,
            crc,
            0,
            method;
            include_zip64_extra = false,
        )
        @lock writer.write_lock begin
            lfh_offset = position_in_sink(writer.sink)
            writer.index_data[2 * chunk_index - 1] = lfh_offset + UInt64(LOCAL_FILE_HEADER_FIXED_SIZE)
            writer.index_data[2 * chunk_index] = UInt64(length(name_bytes)) + compressed_size + UInt64(8)
            writer.cd_entries[chunk_index] = (crc, lfh_offset, compressed_size, uncompressed_size, name_bytes)
            write_to_sink!(writer.sink, lfh_buffer)
            write_to_sink!(writer.sink, payload)
            write_to_sink!(writer.sink, trailer)
        end
        return nothing
    end

    encoded = raw_bytes
    for bytes_codec in inner.bytes_bytes
        encoded = Zarr.Codecs.V3Codecs.codec_encode(bytes_codec, encoded)
    end
    # Defensive elision: the v3 sharding spec lets the writer omit chunks whose encoding is empty (e.g. an all
    # `fill_value` chunk under a codec that signals elision by returning `nothing`). The currently shipped
    # `compressor_for()` codecs all produce non-empty output even for all-`fill_value` data, so this branch is not
    # reachable from user code today; it future-proofs the writer against eliding codecs.
    if encoded === nothing || isempty(encoded)
        return nothing  # UNTESTED
    end
    compressed_size = UInt64(length(encoded))
    uncompressed_size = method == STORED_COMPRESSION_METHOD ? compressed_size : UInt64(length(raw_bytes))
    crc = method == STORED_COMPRESSION_METHOD ? zip_crc32(encoded) : zip_crc32(raw_bytes)
    lfh_size = UInt64(LOCAL_FILE_HEADER_FIXED_SIZE + length(name_bytes) + ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE)
    lfh_buffer = Vector{UInt8}(undef, lfh_size)
    write_local_file_header!(lfh_buffer, 1, name_bytes, compressed_size, uncompressed_size, crc, 0, method)
    @lock writer.write_lock begin
        lfh_offset = position_in_sink(writer.sink)
        chunk_offset = lfh_offset + lfh_size
        writer.index_data[2 * chunk_index - 1] = chunk_offset
        writer.index_data[2 * chunk_index] = compressed_size
        writer.cd_entries[chunk_index] = (crc, lfh_offset, compressed_size, uncompressed_size, name_bytes)
        write_to_sink!(writer.sink, lfh_buffer)
        write_to_sink!(writer.sink, encoded)
    end
    return nothing
end

# Inner-chunk ZIP entry name for the chunk at the linear `chunk_index` (1-based, column-major over
# `chunks_per_shard` — matches both `CartesianIndices` iteration and Zarr.jl's `encode_shard_index`
# linearisation). The on-disk shape is shard-codec-dependent (per-shard fixed-stride is preserved
# either way, so the CD random-access invariant holds):
#
#   - For ZIP method DEFLATE (gzip codec): the name is the 10-byte gzip header. Bytes 0–3 are the
#     gzip magic and flags (`1f 8b 08 01` — FTEXT to avoid a NUL in byte 3); bytes 4–7 carry the
#     chunk index as 4-char Base64 (24-bit cap, asserted); byte 8 is `0x02` (xfl=max compression)
#     and byte 9 is `0xff` (OS=unknown), both non-NUL. The bytes look like binary garbage to ZIP
#     listing tools but are a valid C string, and the Zarr shard-index range covers
#     `[name (= gzip header)][DEFLATE payload][8B trailer]` — a contiguous valid gzip stream.
#   - For every other method (STORED, ZSTD 93, …): the name is `c/<i_N>/.../<i_1>` (dims reversed
#     Julia→C-order, matching Zarr's `ChunkKeyEncoding('/', true)` ordering), with each dim
#     component zero-padded to `ndigits(chunks_per_shard[d] - 1)`. Per-shard fixed-stride.
function inner_chunk_name_bytes(chunks_per_shard::Tuple, chunk_index::Int, method::UInt16)::Vector{UInt8}
    if method == DEFLATE_COMPRESSION_METHOD
        # The 10-byte gzip header doubles as the LFH name. `chunk_index` rides in the `mtime` field as
        # 4 Base64 chars (24-bit cap, asserted) so per-chunk names are distinct in `unzip -l` listings;
        # bytes 0–3 / 8 / 9 are non-NUL constants so the name is a valid C string.
        @assert 1 <= chunk_index <= (1 << 24) "chunk_index $(chunk_index) exceeds 2^24 (gzip-header naming cap)"
        mtime_bytes = UInt8[((chunk_index - 1) >> 16) & 0xff, ((chunk_index - 1) >> 8) & 0xff, (chunk_index - 1) & 0xff]
        base64_chars = codeunits(base64encode(mtime_bytes))
        @assert length(base64_chars) == 4
        return UInt8[
            0x1f,
            0x8b,
            0x08,
            0x01,
            base64_chars[1],
            base64_chars[2],
            base64_chars[3],
            base64_chars[4],
            0x02,
            0xff,
        ]
    end
    cart_index = CartesianIndices(chunks_per_shard)[chunk_index]
    io = IOBuffer()
    write(io, 'c')
    for julia_dim in length(chunks_per_shard):-1:1
        width = ndigits(chunks_per_shard[julia_dim] - 1)
        write(io, '/')
        write(io, lpad(string(cart_index[julia_dim] - 1), width, '0'))
    end
    return take!(io)
end

# Fixed length (in bytes) of every inner-chunk ZIP entry name produced by `inner_chunk_name_bytes`
# for the given `chunks_per_shard` and `method`. DEFLATE entries always use a 10-byte gzip-header
# name; other methods use the `c` prefix plus, per dimension, one `/` and a zero-padded index of
# width `ndigits(chunks_per_shard[d] - 1)`.
function inner_chunk_name_length(chunks_per_shard::Tuple, method::UInt16)::Int
    if method == DEFLATE_COMPRESSION_METHOD
        return 10
    end
    name_length = 1
    for K in chunks_per_shard
        name_length += 1 + ndigits(K - 1)
    end
    return name_length
end

# Render a ZIP entry's name bytes for display in error messages. Printable ASCII (0x20–0x7e, except
# `\` which is escaped to avoid ambiguity with the escape sequences) is emitted as-is; every other byte
# is shown as `\xNN`. Gzip-codec names (10-byte binary headers) come out as e.g. `\x1f\x8b\x08\x01AAAB\x02\xff`.
function format_zip_entry_name(bytes::AbstractVector{UInt8})::String
    io = IOBuffer()
    for b in bytes
        if 0x20 <= b <= 0x7e && b != UInt8('\\')
            write(io, Char(b))
        else
            print(io, "\\x", string(b; base = 16, pad = 2))
        end
    end
    return String(take!(io))
end

function finalize_shard!(writer::IncrementalShardWriter)::Nothing
    @lock writer.write_lock begin
        encoded_index = Zarr.pipeline_encode(writer.index_pipeline, writer.index_data, nothing)
        @assert UInt64(length(encoded_index)) == writer.index_size
        if zip_method_for_pipeline(writer.inner_pipeline) == STORED_COMPRESSION_METHOD
            write_codec_json_entry!(writer)
        end
        write_central_directory_and_eocd!(writer)
        seek_in_sink!(writer.sink, UInt64(0))
        write_to_sink!(writer.sink, encoded_index)
        close(writer.sink)
    end
    return nothing
end

const CODEC_JSON_NAME_BYTES = Vector{UInt8}("codec.json")

# Emit a `codec.json` ZIP entry after the last chunk and before the central directory. Used by STORED shards
# (codecs whose entry data is the codec-encoded bytes — blosc, bitshuffle variants) so that an external tool
# with a Zarr v3 codec implementation can decode the chunks given only the ZIP archive. The entry is a
# regular STORED ZIP entry; daf's reader never reads it (the codec is recovered from the descriptor). The
# Zarr shard index ignores it too — it has slots only for the `chunks_per_shard` inner chunks.
function write_codec_json_entry!(writer::IncrementalShardWriter)::Nothing
    codecs_list = Any[JSON.lower(writer.inner_pipeline.array_bytes)]
    for bytes_codec in writer.inner_pipeline.bytes_bytes
        push!(codecs_list, JSON.lower(bytes_codec))
    end
    content_buffer = IOBuffer()
    JSON.print(content_buffer, codecs_list)
    content = take!(content_buffer)
    name_bytes = CODEC_JSON_NAME_BYTES
    csize = UInt64(length(content))
    crc = zip_crc32(content)
    lfh_size = UInt64(LOCAL_FILE_HEADER_FIXED_SIZE + length(name_bytes) + ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE)
    lfh_buffer = Vector{UInt8}(undef, lfh_size)
    write_local_file_header!(lfh_buffer, 1, name_bytes, csize, csize, crc, 0)
    lfh_offset = position_in_sink(writer.sink)
    write_to_sink!(writer.sink, lfh_buffer)
    write_to_sink!(writer.sink, content)
    writer.codec_json_cd_entry = (crc, lfh_offset, csize, csize, name_bytes)
    return nothing
end

# Emit the ZIP central directory (chunks in chunk-index order, then an optional `codec.json` entry for
# STORED shards) and the ZIP64-aware EOCD region at the sink's current cursor. Called from
# `finalize_shard!` before the Zarr index seek-back.
function write_central_directory_and_eocd!(writer::IncrementalShardWriter)::Nothing
    cd_offset = position_in_sink(writer.sink)
    method = zip_method_for_pipeline(writer.inner_pipeline)
    codec_json_entry = writer.codec_json_cd_entry
    cd_entries_size = 0
    entry_count = 0
    for entry in writer.cd_entries
        if entry !== nothing
            cd_entries_size +=
                CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + length(entry[5]) + ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
            entry_count += 1
        end
    end
    if codec_json_entry !== nothing
        cd_entries_size +=
            CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + length(codec_json_entry[5]) + ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
        entry_count += 1
    end
    cd_buffer = Vector{UInt8}(undef, cd_entries_size + TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    position = 1
    for entry in writer.cd_entries
        if entry !== nothing
            (crc, lfh_offset, compressed_size, uncompressed_size, name_bytes) = entry
            write_central_directory_entry!(
                cd_buffer,
                position,
                name_bytes,
                compressed_size,
                uncompressed_size,
                crc,
                lfh_offset,
                method,
            )
            position += CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + length(name_bytes) + ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
        end
    end
    if codec_json_entry !== nothing
        (crc, lfh_offset, compressed_size, uncompressed_size, name_bytes) = codec_json_entry
        write_central_directory_entry!(
            cd_buffer,
            position,
            name_bytes,
            compressed_size,
            uncompressed_size,
            crc,
            lfh_offset,
            STORED_COMPRESSION_METHOD,
        )
        position += CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + length(name_bytes) + ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
    end
    write_end_of_central_directory_region!(cd_buffer, position, cd_offset, UInt64(cd_entries_size), UInt64(entry_count))
    write_to_sink!(writer.sink, cd_buffer)
    return nothing
end

# Translate a `PackedCodec` descriptor to the bytes→bytes codec tuple of a v3 codec pipeline. Shared by the
# `ZarrDaf` and `FilesDaf` packed write/read paths. For non-bits eltypes (e.g. any `AbstractString`),
# `typesize` is `1` because the in-memory representation has no fixed bit width and isn't meaningful for
# blosc bitshuffle. `:gzip_shuffle` and `:zstd_bitshuffle` are rejected because Zarr.jl 0.10's v3 codec
# registry has no standalone shuffle or bitshuffle codec.
function v3_bytes_codecs_for(codec::PackedCodec, ::Type{T})::Tuple where {T}
    compression = codec.compression
    compression_level = codec.compression_level
    typesize = isbitstype(T) ? sizeof(T) : 1
    if compression == :blosc_zstd_bitshuffle
        return (Zarr.Codecs.V3Codecs.BloscV3Codec("zstd", compression_level, 2, 0, typesize),)
    elseif compression == :blosc_lz4_bitshuffle
        return (Zarr.Codecs.V3Codecs.BloscV3Codec("lz4", compression_level, 2, 0, typesize),)
    elseif compression == :zstd
        return (Zarr.Codecs.V3Codecs.ZstdV3Codec(compression_level),)
    elseif compression == :gzip
        return (Zarr.Codecs.V3Codecs.GzipV3Codec(compression_level),)
    elseif compression == :gzip_shuffle
        return error(
            "packed compression codec :gzip_shuffle is not supported on the v3 sharded backend " *
            "(Zarr.jl has no v3 standalone shuffle codec); use :gzip or :blosc_zstd_bitshuffle",
        )
    else
        @assert compression == :zstd_bitshuffle
        return error(
            "packed compression codec :zstd_bitshuffle is not supported on the v3 sharded backend " *
            "(Zarr.jl has no v3 bitshuffle codec); use :blosc_zstd_bitshuffle (Blosc bundles its own bitshuffle)",
        )
    end
end

# Inverse of [`v3_bytes_codecs_for`](@ref): map a Zarr.jl v3 bytes→bytes codec instance back to a [`PackedCodec`](@ref).
# Used by `zarr_convert.jl` when copying packed-property metadata from a `ZarrDaf` source to a `FilesDaf` JSON sidecar.
# Only the codec instances [`v3_bytes_codecs_for`](@ref) emits are recognised; anything else errors out.
function packed_codec_from_v3_codec(codec)::PackedCodec
    if codec isa Zarr.Codecs.V3Codecs.BloscV3Codec
        if codec.cname == "zstd" && codec.shuffle == 2
            return PackedCodec(:blosc_zstd_bitshuffle, codec.clevel)
        elseif codec.cname == "lz4" && codec.shuffle == 2
            return PackedCodec(:blosc_lz4_bitshuffle, codec.clevel)
        end
    elseif codec isa Zarr.Codecs.V3Codecs.ZstdV3Codec
        return PackedCodec(:zstd, codec.level)
    elseif codec isa Zarr.Codecs.V3Codecs.GzipV3Codec
        return PackedCodec(:gzip, codec.level)
    end
    return error("unsupported v3 codec for packed conversion: $(codec)")
end

# ZIP compression method that `IncrementalShardWriter` stamps on each entry for the given inner-pipeline's
# bytes→bytes codec. For `:zstd` the entry data is a raw zstd frame which matches ZIP method 93, so generic
# ZIP tools auto-decompress it. For `:gzip` the entry data is a raw DEFLATE payload (the gzip header sits in
# the LFH's name field, the gzip trailer sits as dead bytes after the entry data — see
# [`submit_shard_chunk!`](@ref)) which matches ZIP method 8. Everything else is STORED — the entry data is
# the codec-encoded bytes and foreign ZIP tools see opaque bytes (daf's reader recovers the chunk via the
# descriptor's codec pipeline).
function zip_method_for_pipeline(inner_pipeline::Zarr.AbstractCodecPipeline)::UInt16
    if !isempty(inner_pipeline.bytes_bytes)
        codec = inner_pipeline.bytes_bytes[1]
        if codec isa Zarr.Codecs.V3Codecs.ZstdV3Codec
            return ZSTD_COMPRESSION_METHOD
        elseif codec isa Zarr.Codecs.V3Codecs.GzipV3Codec
            return DEFLATE_COMPRESSION_METHOD
        end
    end
    return STORED_COMPRESSION_METHOD
end

# Whether a `Zarr.ZArray`'s on-disk encoding is a one-shard-per-array packed layout (the `ShardingCodec` wrapper that
# `sharded_zcreate` produces). Flat single-chunk uncompressed arrays return `false`.
function is_zarr_array_packed(zarray::Zarr.ZArray)::Bool
    return zarray.metadata.pipeline.array_bytes isa Zarr.Codecs.V3Codecs.ShardingCodec
end

# Extract `(inner_chunk_shape, packed_codec)` from a sharded `ZArray`. Errors if the array is flat or uses a codec
# pipeline this module does not produce.
function packed_codec_from_zarray(zarray::Zarr.ZArray)::Tuple{Tuple, PackedCodec}
    sharding = zarray.metadata.pipeline.array_bytes
    @assert sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
    bytes_bytes = sharding.codecs.bytes_bytes
    @assert length(bytes_bytes) == 1
    return (sharding.chunk_shape, packed_codec_from_v3_codec(bytes_bytes[1]))
end

# Build the v3 `MetadataV3` for a one-shard-per-array sharded layout: outer chunks equal the array
# shape (one shard covers everything), the shard's `array_bytes` codec is a `ShardingCodec` whose
# inner-chunk shape and inner pipeline match the caller-supplied parameters, and the index slab
# lives at the file's `index_location` (`:end` or `:start`). Used by the writer side
# ([`write_packed_dense_array`](@ref), [`rewrite_index_only_as_dual_format_shard`](@ref)) to build the
# Zarr shard-index view of a dual-format shard.
function build_shard_metadata(
    ::Type{T},
    shape::NTuple{N, Int},
    inner_chunk_shape::NTuple{N, Int},
    bytes_bytes_codecs::Tuple,
    index_location::Symbol,
)::Zarr.MetadataV3 where {T, N}
    is_string = T <: AbstractString
    metadata_eltype = is_string ? String : T
    inner_pipeline = build_inner_pipeline(T, bytes_bytes_codecs)
    index_pipeline =
        Zarr.V3Pipeline((), Zarr.Codecs.V3Codecs.BytesCodec(:little), (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),))
    sharding = Zarr.Codecs.V3Codecs.ShardingCodec(inner_chunk_shape, inner_pipeline, index_pipeline, index_location)
    pipeline = Zarr.V3Pipeline((), sharding, ())
    chunk_key_encoding = Zarr.ChunkKeyEncoding('/', true)
    typestr = is_string ? "string" : Zarr.typestr3(T)
    return Zarr.MetadataV3{metadata_eltype, N, typeof(pipeline)}(
        3,
        "array",
        shape,
        shape,
        typestr,
        pipeline,
        shard_fill_value(T),
        chunk_key_encoding,
    )
end

# Build the inner-pipeline of a packed-array shard: the array→bytes step is `BytesCodec` for numeric
# `T` or `VLenUTF8V3Codec` for `T <: AbstractString` (strings round-trip through Zarr.jl's
# `vlen-utf8` v3 wire format), followed by the caller-supplied bytes→bytes codec chain (compression).
function build_inner_pipeline(::Type{T}, bytes_bytes_codecs::Tuple)::Zarr.V3Pipeline where {T}
    array_bytes_codec = T <: AbstractString ? VLenUTF8V3Codec() : Zarr.Codecs.V3Codecs.BytesCodec(:little)
    return Zarr.V3Pipeline((), array_bytes_codec, bytes_bytes_codecs)
end

# Fill value for an inner chunk of a packed-array shard. `""` for strings and `zero(T)` for numerics.
function shard_fill_value(::Type{T}) where {T}
    if T <: AbstractString
        return ""
    else
        return zero(T)
    end
end

# Canonical JSON `eltype` name for a Julia type. Any `AbstractString` subtype is canonicalized to
# `"String"` because the on-disk storage and in-memory materialization are always concrete `String`
# regardless of the user's input subtype (`SubString`, `InlineString7`, ...). Numeric types render
# via `string(T)` which produces names like `"Float32"` or `"Int32"`.
function json_eltype_name(::Type{T})::String where {T}
    if T <: AbstractString
        return "String"
    else
        return string(T)
    end
end

# Resolve a JSON descriptor's `eltype` string (`"String"` / `"string"` for the variable-length UTF-8 encoding, or one
# of [`DTYPE_BY_NAME`](@ref)'s numeric keys) to a Julia type. Centralizes the dispatch so dense and sparse read paths
# handle string-typed properties uniformly.
function eltype_for_descriptor(eltype_name::AbstractString)::Type
    if eltype_name == "String" || eltype_name == "string"
        return String
    else
        eltype = DTYPE_BY_NAME[eltype_name]
        @assert eltype !== nothing
        return eltype
    end
end

# Parse a sparse JSON descriptor into `(eltype_name, indtype_name)`. Accepts both the v1.0 schema (top-level `eltype`
# / `indtype` keys) and the v1.1 per-property schema (top-level `colptr` / `rowval` / `nzval` for matrices or
# `nzind` / `nzval` for vectors, each holding a dense-vector descriptor). The shapes are mutually exclusive — old
# code wrote the v1.0 form, new code emits only v1.1, but a v1.1 dataset may legally still contain v1.0 sparse files
# written by earlier code that have not been rewritten since the version bump. When the v1.1 `nzval` field is
# omitted (writer skipped the file for a Bool all-`true` property), the `eltype` is `"Bool"` by construction.
function parse_sparse_descriptor(json::AbstractDict, indtype_property::AbstractString)::Tuple{String, String}
    if haskey(json, "eltype")
        return (String(json["eltype"]), String(json["indtype"]))
    else
        indtype_descriptor = json[indtype_property]
        @assert indtype_descriptor isa AbstractDict
        eltype_name = if haskey(json, "nzval")
            nzval_descriptor = json["nzval"]
            @assert nzval_descriptor isa AbstractDict
            String(nzval_descriptor["eltype"])
        else
            "Bool"
        end
        return (eltype_name, String(indtype_descriptor["eltype"]))
    end
end

# Build the JSON object string for a single dense component nested inside a sparse-property descriptor.
# `chunk_shape === nothing` ⇒ flat (`{format,eltype,n_elements}`); otherwise packed
# (`{format,eltype,n_elements,packed,chunk_shape,compression,compression_level,index_location}`). `n_elements` lets
# the reader size each component without falling back to `filesize / sizeof(T)`, which doesn't work for packed
# components (the on-disk file is the compressed shard, not the raw data). The optional `codec` argument lets the
# `zarr_convert.jl` hard-link path declare the source's codec rather than the local global, so the linked bytes stay
# decodable when the writer's `compressor_for()` differs from the source.
function component_descriptor_json(
    ::Type{T},
    n_elements::Integer,
    chunk_shape::Maybe{NTuple{N, Int}},
    codec::PackedCodec = compressor_for(),
)::String where {T, N}
    n_str = string(n_elements)
    eltype_name = json_eltype_name(T)
    if chunk_shape === nothing
        return "{\"format\":\"dense\",\"eltype\":\"$(eltype_name)\",\"n_elements\":$(n_str)}"
    else
        chunk_shape_str = "[" * join(string.(chunk_shape), ",") * "]"
        return "{\"format\":\"dense\",\"eltype\":\"$(eltype_name)\",\"n_elements\":$(n_str)," *
               "\"packed_format\":\"indexed+zipped\",\"chunk_shape\":$(chunk_shape_str)," *
               "\"compression\":\"$(codec.compression)\",\"compression_level\":$(codec.compression_level)," *
               "\"index_location\":\"start\"}"
    end
end

# v1.1 dense flat sidecar: `{format,eltype}`. The bytes-returning form lets every backend route the result through
# its own storage layer (the FilesDaf write helpers wrap `write(path, …)`, the ZipDaf write paths put the bytes into
# a zip entry, and so on).
function dense_array_json_bytes(eltype::Type{<:StorageScalarBase})::Vector{UInt8}
    return Vector{UInt8}("{\"format\":\"dense\",\"eltype\":\"$(json_eltype_name(eltype))\"}\n")
end

# v1.1 dense packed sidecar: `{format,eltype,packed_format,chunk_shape,compression,compression_level,index_location}`. The
# bytes describe the inner-pipeline parameters of the corresponding `<name>.zip` (or per-component shard).
function packed_array_json_bytes(
    eltype::Type{<:StorageScalarBase},
    chunk_shape::NTuple{N, Int},
    codec::PackedCodec,
)::Vector{UInt8} where {N}
    chunk_shape_str = "[" * join(string.(chunk_shape), ",") * "]"
    return Vector{UInt8}(
        "{\"format\":\"dense\",\"eltype\":\"$(json_eltype_name(eltype))\"," *
        "\"packed_format\":\"indexed+zipped\",\"chunk_shape\":$(chunk_shape_str)," *
        "\"compression\":\"$(codec.compression)\",\"compression_level\":$(codec.compression_level)," *
        "\"index_location\":\"start\"}\n",
    )
end

# v1.1 sparse vector sidecar: `{format,nzind,nzval}` where each component is a `component_descriptor_json` (flat or
# packed depending on its `*_chunk_shape` argument). The `nzval` field is omitted when the writer skips storing the
# `nzval` file on disk (Bool all-`true` optimisation) — readers gate on `haskey(json, "nzval")` to synthesise the
# `nzval` source. Per-component `*_codec` overrides default to `compressor_for()`; `zarr_convert.jl` passes
# explicit codecs so the linked bytes stay decodable when the destination's global compressor setting differs.
function sparse_vector_json_bytes(
    eltype::Type{<:StorageScalarBase},
    indtype::Type{<:StorageInteger},
    n_elements::Integer;
    nzind_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzind_codec::PackedCodec = compressor_for(),
    nzval_codec::PackedCodec = compressor_for(),
    nzval_present::Bool = true,
)::Vector{UInt8}
    nzind_desc = component_descriptor_json(indtype, n_elements, nzind_chunk_shape, nzind_codec)
    if nzval_present
        nzval_desc = component_descriptor_json(eltype, n_elements, nzval_chunk_shape, nzval_codec)
        return Vector{UInt8}("{\"format\":\"sparse\",\"nzind\":$(nzind_desc),\"nzval\":$(nzval_desc)}\n")
    else
        return Vector{UInt8}("{\"format\":\"sparse\",\"nzind\":$(nzind_desc)}\n")
    end
end

# v1.1 sparse matrix sidecar: `{format,colptr,rowval,nzval}`. `colptr` is sized at `n_columns + 1` and the
# value-bearing components at `nnz`; each is flat or packed independently per its `*_chunk_shape` argument. The
# `nzval` field is omitted when the writer skips storing the `nzval` file on disk (Bool all-`true` optimisation).
# Per-component `*_codec` overrides default to `compressor_for()`; `zarr_convert.jl` passes explicit codecs.
function sparse_matrix_json_bytes(
    eltype::Type{<:StorageScalarBase},
    indtype::Type{<:StorageInteger},
    nnz::Integer,
    n_columns::Integer;
    colptr_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    rowval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    colptr_codec::PackedCodec = compressor_for(),
    rowval_codec::PackedCodec = compressor_for(),
    nzval_codec::PackedCodec = compressor_for(),
    nzval_present::Bool = true,
)::Vector{UInt8}
    colptr_desc = component_descriptor_json(indtype, n_columns + 1, colptr_chunk_shape, colptr_codec)
    rowval_desc = component_descriptor_json(indtype, nnz, rowval_chunk_shape, rowval_codec)
    if nzval_present
        nzval_desc = component_descriptor_json(eltype, nnz, nzval_chunk_shape, nzval_codec)
        return Vector{UInt8}(
            "{\"format\":\"sparse\"," *
            "\"colptr\":$(colptr_desc),\"rowval\":$(rowval_desc),\"nzval\":$(nzval_desc)}\n",
        )
    else
        return Vector{UInt8}("{\"format\":\"sparse\",\"colptr\":$(colptr_desc),\"rowval\":$(rowval_desc)}\n")
    end
end

# Encode `data` as a single dual-format shard byte blob and return the bytes. The caller decides where to
# put them: the `FilesDaf` packed write path writes them to a fresh `<name>.zip` `IOStream`; the `ZipDaf`
# packed write path writes them into a reserved outer-zip mmap region via [`reserve_mmap_zip_entry!`](@ref).
# Implementation: drive [`IncrementalShardWriter`](@ref) over an [`InMemorySink`](@ref), slicing `data` into
# the inner-chunk grid and submitting one chunk at a time (partial-edge chunks are padded with the
# `shard_fill_value(T)`). The resulting bytes are byte-identical to what a `ZarrDaf` in `DirectoryStore` mode
# would write at `<group>/<name>/c/0[/0]` for the same data, which is the property `zarr_convert.jl` relies
# on for hard-link conversion across backends.
function encode_packed_dense_array(
    data::AbstractArray{T, N},
    inner_chunk_shape::NTuple{N, Int},
    bytes_bytes_codecs::Tuple,
)::Vector{UInt8} where {T, N}
    chunks_per_shard = Zarr.Codecs.V3Codecs.calculate_chunks_per_shard(size(data), inner_chunk_shape)
    sink = InMemorySink()
    writer = make_streaming_shard_writer(sink, T, chunks_per_shard, bytes_bytes_codecs)
    fill_value = shard_fill_value(T)
    for chunk_index in 1:prod(chunks_per_shard)
        submit_shard_chunk!(
            writer,
            chunk_index,
            extract_inner_chunk(data, chunk_index, chunks_per_shard, inner_chunk_shape, fill_value),
        )
    end
    finalize_shard!(writer)
    return take_bytes!(sink)
end

# Decode the v3-sharded byte stream at `source_path` (any `index_location`) via the Zarr shard index and
# re-encode it as a dual-format shard at `destination_path`. Used by `zarr_convert` when a ZarrDaf-side
# source carries only the Zarr index (a foreign Zarr producer that wrote no ZIP framing, or an explicit
# `"indexed"` `daf_packed_format`); the source bytes cannot be hard-linked into a destination served
# through the ZIP-CD read path, so the data is re-emitted via [`encode_packed_dense_array`](@ref).
function rewrite_index_only_as_dual_format_shard(
    source_path::AbstractString,
    destination_path::AbstractString,
    ::Type{T},
    shape::NTuple{N, Int},
    inner_chunk_shape::NTuple{N, Int},
    bytes_bytes_codecs::Tuple,
    source_index_location::Symbol,
)::Nothing where {T, N}
    source_metadata = build_shard_metadata(T, shape, inner_chunk_shape, bytes_bytes_codecs, source_index_location)
    decoded = Array{T}(undef, shape)
    source_bytes = open(io -> Mmap.mmap(io, Vector{UInt8}), source_path, "r")
    Zarr.pipeline_decode!(source_metadata.pipeline, decoded, source_bytes)  # NOJET
    write(destination_path, encode_packed_dense_array(decoded, inner_chunk_shape, bytes_bytes_codecs))
    return nothing
end

# Decode the ZIP-only byte stream at `source_path` via its central directory (no Zarr index present) and
# re-encode it as a dual-format shard at `destination_path`. Used by `zarr_convert` when a FilesDaf-side
# source carries `packed_format == "zipped"` — a foreign FilesDaf-style producer wrote a ZIP archive of
# chunks without prepending the Zarr index, so the source cannot be hard-linked into a ZarrDaf
# destination (which reads through the index).
function rewrite_zip_only_as_dual_format_shard(
    source_path::AbstractString,
    destination_path::AbstractString,
    ::Type{T},
    shape::NTuple{N, Int},
    inner_chunk_shape::NTuple{N, Int},
    codec::PackedCodec,
)::Nothing where {T, N}
    mmap_bytes = open(io -> Mmap.mmap(io, Vector{UInt8}), source_path, "r")
    source_array = ZipShardArray(
        T,
        shape,
        inner_chunk_shape,
        codec,
        buffer_byte_fetcher(mmap_bytes),
        buffer_suffix_byte_fetcher(mmap_bytes);
        cache_capacity = local_chunk_cache_capacity(prod(inner_chunk_shape) * sizeof(T)),
    )
    decoded = source_array[ntuple(_ -> Colon(), N)...]
    bytes_bytes_codecs = v3_bytes_codecs_for(codec, T)
    write(destination_path, encode_packed_dense_array(decoded, inner_chunk_shape, bytes_bytes_codecs))
    return nothing
end

# Return one inner chunk's `AbstractArray{T, N}` view for `chunk_index` (1-based linear over
# `chunks_per_shard`). When the chunk grid does not divide `size(data)` evenly, the trailing-edge chunks are
# padded with `fill_value` so every submitted chunk has exactly `inner_chunk_shape` — the v3 reader expects
# every inner chunk to be of the declared shape and crops to the array bounds at access time. `Val(N)` keeps
# `ntuple` type-stable so the per-dim tuple accesses are statically resolvable.
function extract_inner_chunk(
    data::AbstractArray{T, N},
    chunk_index::Int,
    chunks_per_shard::NTuple{N, Int},
    inner_chunk_shape::NTuple{N, Int},
    fill_value::T,
)::AbstractArray{T, N} where {T, N}
    coord = Tuple(CartesianIndices(chunks_per_shard)[chunk_index])
    starts = ntuple(dim -> (coord[dim] - 1) * inner_chunk_shape[dim] + 1, Val(N))  # NOJET
    stops = ntuple(dim -> min(coord[dim] * inner_chunk_shape[dim], size(data, dim)), Val(N))  # NOJET
    if all(stops .- starts .+ 1 .== inner_chunk_shape)
        return view(data, ntuple(dim -> starts[dim]:stops[dim], Val(N))...)  # NOJET
    else
        padded = fill(fill_value, inner_chunk_shape)
        live_view = view(padded, ntuple(dim -> 1:(stops[dim] - starts[dim] + 1), Val(N))...)  # NOJET
        live_view .= view(data, ntuple(dim -> starts[dim]:stops[dim], Val(N))...)  # NOJET
        return padded
    end
end

# Build an `IncrementalShardWriter` over the caller-supplied `sink`. The sink can be anything that implements the
# `position_in_sink` / `write_to_sink!` / `seek_in_sink!` interface plus `Base.close` — typically an `IOStream`
# (FilesDaf) or an [`MmapShardRegion`](@ref) (ZipDaf, ZarrDaf-Zip). The inner pipeline is reconstructed for `T`
# from `bytes_bytes_codecs` so the on-disk shard bytes match what the equivalent `ZarrDaf` write would produce
# for the same content under the same codec. `chunks_per_shard` is the inner-chunk grid shape (column-major over
# the shard's array shape), used to name ZIP entries via `Zarr.ChunkKeyEncoding('/', true)`.
function make_streaming_shard_writer(
    sink,
    ::Type{T},
    chunks_per_shard::Tuple,
    bytes_bytes_codecs::Tuple,
)::IncrementalShardWriter where {T}
    inner_pipeline = build_inner_pipeline(T, bytes_bytes_codecs)
    index_pipeline =
        Zarr.V3Pipeline((), Zarr.Codecs.V3Codecs.BytesCodec(:little), (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),))
    return IncrementalShardWriter(sink, inner_pipeline, index_pipeline, shard_fill_value(T), chunks_per_shard)
end

# Open an `IOStream`-sinked `IncrementalShardWriter` for streaming a packed dense array to a single
# `<name>.zip` file. The caller submits inner chunks via `submit_shard_chunk!` and finalizes via
# `finalize_shard!`; the writer emits the dual-format bytes (Zarr index at offset 0, per-chunk ZIP framing,
# CD + EOCD at the tail) and closes the stream.
function open_streaming_shard_writer(
    shard_path::AbstractString,
    ::Type{T},
    chunks_per_shard::Tuple,
    bytes_bytes_codecs::Tuple,
)::IncrementalShardWriter where {T}
    return make_streaming_shard_writer(open(shard_path, "w"), T, chunks_per_shard, bytes_bytes_codecs)
end

# Storage dispatch surface: each on-disk backend (`FilesDaf`, `ZipDaf`) implements these primitives so the shared
# packed-format helpers can address entries by logical relative key (e.g. `"vectors/r/n.json"`) without knowing the
# physical layout. `FilesDaf` resolves a key to `files.path/<key>` (and triggers the cache-invalidation hook on writes);
# `ZipDaf` resolves a key to `group_prefix * <key>` and stores bytes in the underlying `MmapZipStore`.

# Abstract methods every concrete `PackedDaf` backend implements; the typed bodies here exist so static analysis can
# resolve dispatch through the shared `packed_format_*` helpers and serve as a runtime guard if a new subtype forgets
# one. The actual work happens in the `FilesDaf` and `ZipDaf` overrides.

# Write a raw byte blob at the logical key, replacing any existing entry. Used by code paths that have already
# assembled the bytes (e.g. JSON descriptors, encoded shard payloads).
function packed_write_bytes!(daf::PackedDaf, ::AbstractString, ::AbstractVector{UInt8})::Nothing
    return error("packed_write_bytes! not implemented for $(typeof(daf))")
end

# Write a numeric vector at the logical key as raw little-endian bytes (zero-copy where possible). Used for the flat
# (unpacked) sparse component path and for the flat dense vector path.
function packed_write_typed_array!(daf::PackedDaf, ::AbstractString, ::AbstractVector)::Nothing
    return error("packed_write_typed_array! not implemented for $(typeof(daf))")
end

# Remove the entry at the logical key if it exists. Defensive cleanup before a property rewrite — `ZipDaf` is
# append-only so this is only relevant on `FilesDaf` in practice, but the dispatch is uniform.
function packed_delete_entry!(daf::PackedDaf, ::AbstractString)::Nothing
    return error("packed_delete_entry! not implemented for $(typeof(daf))")
end

# Register a property's JSON descriptor in the format's consolidated index so HTTP clients can enumerate properties
# without fetching data files. `key` is the property's logical relative key (e.g. `vectors/cell/batch`, no `.json`
# suffix); `descriptor` is the JSON bytes that the corresponding sidecar holds (with or without a trailing newline).
# `FilesDaf` appends one entry to `metadata.json` via byte surgery; `ZipDaf` is a no-op (the zip's central directory
# is the index, and we strip any stale `metadata.json` entry on append — see `src/zip_files.jl`).
function packed_register_metadata!(daf::PackedDaf, ::AbstractString, ::AbstractVector{UInt8})::Nothing
    return error("packed_register_metadata! not implemented for $(typeof(daf))")
end

# Write one component vector of a sparse property (`nzind`, `nzval`, `colptr`, `rowval`) at `<base_key>.<suffix>` (raw
# little-endian bytes, zero-copy mmap-able) when `chunk_shape === nothing`, or at `<base_key>.<suffix>.zip` (v3
# sharded-array bytes via [`encode_packed_dense_array`](@ref)) otherwise. `base_key` is a logical relative key like
# `"vectors/r/n"` or `"matrices/r/c/n"` (no leading prefix); each backend's `packed_write_*` dispatch resolves it to its
# physical address.
function packed_format_write_sparse_component!(
    daf::PackedDaf,
    base_key::AbstractString,
    suffix::AbstractString,
    data::AbstractVector{T},
    chunk_shape::Maybe{NTuple{1, Int}},
)::Nothing where {T}
    if chunk_shape === nothing
        packed_write_typed_array!(daf, "$(base_key).$(suffix)", data)
    else
        encoded = encode_packed_dense_array(data, chunk_shape, v3_bytes_codecs_for(compressor_for(), T))
        packed_write_bytes!(daf, "$(base_key).$(suffix).zip", encoded)
    end
    return nothing
end

# Write a packed dense numeric or string property as the descriptor JSON at `<base_key>.json` and the encoded v3
# sharded-array bytes at `<base_key>.zip`. `base_key` is a logical relative key like `"vectors/r/n"` or
# `"matrices/r/c/n"`; each backend's `packed_write_bytes!` dispatch resolves it to its physical address. The encoder
# handles both numeric eltypes (raw little-endian inner chunks) and `<: AbstractString` eltypes (`VLenUTF8V3Codec`).
function packed_format_write_dense_array!(
    daf::PackedDaf,
    base_key::AbstractString,
    data::AbstractArray{T, N},
    chunk_shape::NTuple{N, Int},
)::Nothing where {T, N}
    codec = compressor_for()
    json_bytes = packed_array_json_bytes(T, chunk_shape, codec)
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    encoded = encode_packed_dense_array(data, chunk_shape, v3_bytes_codecs_for(codec, T))
    packed_write_bytes!(daf, "$(base_key).zip", encoded)
    packed_register_metadata!(daf, base_key, json_bytes)
    return nothing
end

# Write a sparse numeric vector property as the descriptor JSON at `vectors/<axis>/<name>.json` plus per-component
# shard / flat blobs (`nzind`, optionally `nzval`). The `nzval` blob is omitted whenever `eltype == Bool` and all
# nonzeros are `true` — the v1.1 reader synthesizes the values in that one case. `is_packed` selects flat (`chunk_shape
# == nothing`) versus packed (`chunk_shape !== nothing`) per-component layout via [`chunks_for`](@ref).
function packed_format_write_sparse_numeric_vector!(
    daf::PackedDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::AbstractVector,
    is_packed::Bool,
)::Nothing
    nzind_vector = nzind(vector)
    nzval_vector = nzval(vector)
    nnz = length(nzind_vector)
    nzval_present = !(eltype(vector) == Bool && all(nzval_vector))
    nzind_chunk_shape = chunks_for(is_packed, (nnz,), indtype(vector))
    nzval_chunk_shape = nzval_present ? chunks_for(is_packed, (nnz,), eltype(vector)) : nothing
    base_key = "vectors/$(axis)/$(name)"
    json_bytes = sparse_vector_json_bytes(
        eltype(vector),
        indtype(vector),
        nnz;
        nzind_chunk_shape,
        nzval_chunk_shape,
        nzval_present,
    )
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_format_write_sparse_component!(daf, base_key, "nzind", nzind_vector, nzind_chunk_shape)
    if nzval_present
        packed_format_write_sparse_component!(daf, base_key, "nzval", nzval_vector, nzval_chunk_shape)
    end
    packed_register_metadata!(daf, base_key, json_bytes)
    return nothing
end

# Reserve a writable mmap-backed `Vector{T}` of length `n_elements` for the entry at the logical key, zero-initialised.
# `FilesDaf` creates the underlying file (filled with zeros), then mmaps it `r+`. `ZipDaf` reserves an upper-bound
# entry in the underlying `MmapZipStore` and returns a typed view onto its bytes.
function packed_reserve_typed_vector!(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::Integer,
)::Vector{T} where {T <: StorageReal}
    return error("packed_reserve_typed_vector! not implemented for $(typeof(daf))")
end

# Reserve a writable mmap-backed `Matrix{T}` of size `(nrows, ncols)` for the entry at the logical key. Same semantics
# as [`packed_reserve_typed_vector!`](@ref).
function packed_reserve_typed_matrix!(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::Integer,
    ::Integer,
)::Matrix{T} where {T <: StorageReal}
    return error("packed_reserve_typed_matrix! not implemented for $(typeof(daf))")
end

# Mark a previously reserved entry as filled. `FilesDaf` is a no-op (file mtime tracks freshness). `ZipDaf` recomputes
# the entry's CRC32 from the user-written bytes and patches the local + central directory headers.
function packed_finalize_entry!(daf::PackedDaf, ::AbstractString)::Nothing
    return error("packed_finalize_entry! not implemented for $(typeof(daf))")
end

# Build an [`IncrementalShardWriter`](@ref) bound to a backend-specific sink covering the entry at the logical key.
# `FilesDaf` opens the shard file as an `IOStream` and ignores `chunk_shape`; `ZipDaf` reserves an upper-bound zip
# entry sized from `chunk_shape` (raw uncompressed inner-chunk bytes plus per-chunk slack and the index slab) and
# returns an `MmapShardRegion` over its bytes (the entry is shrunk to actual size and CRC-patched at finalize).
function packed_make_streaming_shard_writer(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::Tuple,
    ::NTuple{2, Int},
    ::PackedCodec,
)::IncrementalShardWriter where {T <: StorageReal}
    return error("packed_make_streaming_shard_writer not implemented for $(typeof(daf))")
end

# Read a numeric `Vector{T}` from the entry at the logical key. Returns `(vector, byte_owner, cache_group)` where
# `byte_owner` is whatever object backs `vector`'s storage and must outlive it (`nothing` for the zero-copy mmap path).
# `FilesDaf` mmap-loads from a file; `ZipDaf` mmap-loads when the entry is uncompressed and aligned, otherwise decodes
# into a fresh `Vector{UInt8}` and wraps it as a typed view.
function packed_read_typed_vector(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::Integer,
)::Tuple{Vector{T}, Any, Formats.CacheGroup} where {T}
    return error("packed_read_typed_vector not implemented for $(typeof(daf))")
end

# Read a `Matrix{T}` of the given `(nrows, ncols)` shape from the entry at the logical key. Same return convention as
# [`packed_read_typed_vector`](@ref).
function packed_read_typed_matrix(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::Integer,
    ::Integer,
)::Tuple{Matrix{T}, Any, Formats.CacheGroup} where {T}
    return error("packed_read_typed_matrix not implemented for $(typeof(daf))")
end

# Read the entry at the logical key as one `SubString` per line. Returns `(lines, cache_group)`.
function packed_read_lines(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    return error("packed_read_lines not implemented for $(typeof(daf))")
end

# Whether the entry at the logical key exists.
function packed_has_entry(daf::PackedDaf, ::AbstractString)::Bool
    return error("packed_has_entry not implemented for $(typeof(daf))")
end

# The byte size of the entry at the logical key. Used by the sparse-component reader to derive `n_elements` for v1.0
# sparse properties whose JSON descriptor predates the `n_elements` field.
function packed_entry_size(daf::PackedDaf, ::AbstractString)::Int
    return error("packed_entry_size not implemented for $(typeof(daf))")
end

# Open the entry at the logical key as a lazy `ChunkedArray{T, N}` of the given shape, reading the shard
# through the ZIP central directory. `FilesDaf` mmaps the shard file and wraps it in [`buffer_byte_fetcher`](@ref)
# closures; `ZipDaf` reads the shard bytes from the outer archive and wraps those.
function packed_open_array(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::AbstractDict,
    ::NTuple{N, Int},
)::ChunkedArray{T, N} where {T, N}
    return error("packed_open_array not implemented for $(typeof(daf))")
end

# Read the JSON descriptor at the entry's logical key as an `AbstractDict`. `FilesDaf` parses the file; `ZipDaf` reads
# the entry bytes and parses the resulting `String`.
function packed_read_json(daf::PackedDaf, ::AbstractString)::AbstractDict
    return error("packed_read_json not implemented for $(typeof(daf))")
end

# Open one sparse-property component as an eager `Vector{T}` — flat (mmap- or memory-backed via
# [`packed_read_typed_vector`](@ref)) or packed (decoded once via [`packed_open_array`](@ref)). Resolves the
# component descriptor (v1.1 nested per-component dict) or synthesizes one from the v1.0 top-level `eltype` /
# `indtype`. Returns `(vector, byte_owner, n_elements, cache_group)`.
function packed_format_open_sparse_component_eager(
    daf::PackedDaf,
    base_key::AbstractString,
    component::AbstractString,
    ::Type{T},
    descriptor_json::AbstractDict,
    n_elements_known::Maybe{Int},
)::Tuple{Vector{T}, Any, Int, Formats.CacheGroup} where {T}
    descriptor = component_descriptor_for(descriptor_json, component)
    flat_key = "$(base_key).$(component)"
    is_packed = is_packed_component(descriptor)
    n_elements = if n_elements_known !== nothing
        n_elements_known
    elseif haskey(descriptor, "n_elements")
        Int(descriptor["n_elements"])
    else
        div(packed_entry_size(daf, flat_key), sizeof(T))
    end
    if is_packed
        zarray = packed_open_array(daf, "$(flat_key).zip", T, descriptor, (n_elements,))
        return (zarray[:], nothing, n_elements, Formats.MemoryData)  # NOJET
    end
    vector, byte_owner, cache_group = packed_read_typed_vector(daf, flat_key, T, n_elements)
    return (vector, byte_owner, n_elements, cache_group)
end

# Whether a descriptor (a top-level dense descriptor, or a v1.1 sparse-component dict) describes a packed
# (chunked-on-disk) component, i.e. carries a `packed_format` field (regardless of its value). `nothing` (v1.0
# layout with no per-component dict) and flat descriptors return `false`. Used to decide between the flat read
# path and the Zarr-shard-index packed read path.
function is_packed_component(descriptor::Maybe{AbstractDict})::Bool
    return descriptor isa AbstractDict && haskey(descriptor, "packed_format")
end

# Open one sparse-property component as a lazy `AbstractVector{T}` source — a `DiskArrays.cache`-wrapped `ZArray`
# for packed components (chunks decode on demand), or a flat-backed `Vector{T}` (mmap or memory) for unpacked
# components. Used by the lazy sparse-read path: the caller wraps the resulting sources in a
# [`LazySparse.LazySparseMatrix`](@ref) and defers materialisation until a consumer asks for one of the
# `SparseArrays` accessors. Returns `(source, byte_owner, n_elements, cache_group)`.
function packed_format_open_sparse_component_source(
    daf::PackedDaf,
    base_key::AbstractString,
    component::AbstractString,
    ::Type{T},
    descriptor_json::AbstractDict,
    n_elements_known::Maybe{Int},
)::Tuple{AbstractVector{T}, Any, Int, Formats.CacheGroup} where {T}
    descriptor = component_descriptor_for(descriptor_json, component)
    flat_key = "$(base_key).$(component)"
    is_packed = is_packed_component(descriptor)
    n_elements = if n_elements_known !== nothing
        n_elements_known
    elseif haskey(descriptor, "n_elements")
        Int(descriptor["n_elements"])
    else
        div(packed_entry_size(daf, flat_key), sizeof(T))
    end
    if is_packed
        zarray = packed_open_array(daf, "$(flat_key).zip", T, descriptor, (n_elements,))
        return (zarray, nothing, n_elements, Formats.MemoryData)
    end
    vector, byte_owner, cache_group = packed_read_typed_vector(daf, flat_key, T, n_elements)
    return (vector, byte_owner, n_elements, cache_group)
end

# Resolve a per-component descriptor: prefer the v1.1 nested dict (`json[component]`), otherwise synthesize a flat one
# from the v1.0 top-level `eltype` / `indtype` fields. Used by both the eager and lazy sparse openers.
function component_descriptor_for(json::AbstractDict, component::AbstractString)::AbstractDict
    if haskey(json, component)
        descriptor = json[component]
        @assert descriptor isa AbstractDict
        return descriptor
    end
    eltype_key = component == "nzval" ? "eltype" : "indtype"
    return Dict("format" => "dense", "eltype" => String(json[eltype_key]))
end

# Empty/Filled dense and sparse property paths shared between `FilesDaf` and `ZipDaf`. The `format_get_empty_*!`
# variants reserve the underlying storage and return a writable view; the user fills the view via the public API; the
# matching `format_filled_empty_*!` variant finalises the entry (e.g. CRC patching for `ZipDaf`) and registers the JSON
# sidecar in the format's secondary index. The packed dense paths defer the on-disk encoding to the fill side: get
# returns an in-RAM `Vector{T}` (vector path) or a [`PackedDenseMatrix`](@ref) streaming wrapper (matrix path), and
# fill encodes/streams to the per-property `.zip` entry plus the descriptor JSON.

function packed_format_get_empty_dense_vector!(
    daf::PackedDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    is_packed::Bool,
    n_elements::Integer,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    chunk_shape = chunks_for(is_packed, (Int(n_elements),), T)
    if chunk_shape !== nothing
        # The vector packed path has no streaming contract: hand the user a fresh in-RAM `Vector{T}` and encode it to
        # the per-property `.zip` entry at fill time.
        return (Vector{T}(undef, Int(n_elements)), nothing)
    end
    base_key = "vectors/$(axis)/$(name)"
    json_bytes = dense_array_json_bytes(T)
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_register_metadata!(daf, base_key, json_bytes)
    return (packed_reserve_typed_vector!(daf, "$(base_key).data", T, n_elements), Formats.MappedData)
end

function packed_format_filled_empty_dense_vector!(
    daf::PackedDaf,
    axis::AbstractString,
    name::AbstractString,
    filled::AbstractVector{T},
)::Nothing where {T <: StorageReal}
    base_key = "vectors/$(axis)/$(name)"
    if !packed_has_entry(daf, "$(base_key).json")  # NOJET
        # The packed branch in `packed_format_get_empty_dense_vector!` defers JSON-write until fill time so the chunk
        # shape can be picked from the actual `filled` size; route through `packed_format_write_dense_array!`, which
        # writes the JSON, the shard, and registers metadata in one step.
        chunk_shape = chunks_for(true, size(filled), T)
        @assert chunk_shape !== nothing
        packed_format_write_dense_array!(daf, base_key, filled, chunk_shape)
    else
        packed_finalize_entry!(daf, "$(base_key).data")
    end
    return nothing
end

function packed_format_get_empty_sparse_vector!(
    daf::PackedDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::Integer,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal, I <: StorageInteger}
    base_key = "vectors/$(axis)/$(name)"
    json_bytes = sparse_vector_json_bytes(T, I, Int(nnz))
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_register_metadata!(daf, base_key, json_bytes)
    nzind_vector = packed_reserve_typed_vector!(daf, "$(base_key).nzind", I, nnz)
    nzval_vector = packed_reserve_typed_vector!(daf, "$(base_key).nzval", T, nnz)
    return (nzind_vector, nzval_vector, Formats.MappedData)
end

function packed_format_filled_empty_sparse_vector!(daf, axis::AbstractString, name::AbstractString)::Nothing
    base_key = "vectors/$(axis)/$(name)"
    packed_finalize_entry!(daf, "$(base_key).nzind")
    packed_finalize_entry!(daf, "$(base_key).nzval")
    return nothing
end

function packed_format_get_empty_dense_matrix!(
    daf::PackedDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    is_packed::Bool,
    nrows::Integer,
    ncols::Integer,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    chunk_shape = chunks_for(is_packed, (Int(nrows), Int(ncols)), T)
    if chunk_shape !== nothing
        return packed_format_streaming_dense_matrix(daf, rows_axis, columns_axis, name, T, nrows, ncols, chunk_shape)
    end
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    json_bytes = dense_array_json_bytes(T)
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_register_metadata!(daf, base_key, json_bytes)
    matrix = packed_reserve_typed_matrix!(daf, "$(base_key).data", T, nrows, ncols)
    return (matrix, Formats.MappedData)
end

function packed_format_filled_empty_dense_matrix!(
    daf::PackedDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::AbstractMatrix{<:StorageReal},
)::Nothing
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    if filled isa PackedDenseMatrix
        flush_packed_dense_matrix!(filled)
    else
        packed_finalize_entry!(daf, "$(base_key).data")
    end
    return nothing
end

function packed_format_get_empty_sparse_matrix!(
    daf::PackedDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::Integer,
    ::Type{I},
    n_columns::Integer,
)::Tuple{
    AbstractVector{I},
    AbstractVector{I},
    AbstractVector{T},
    Maybe{Formats.CacheGroup},
} where {T <: StorageReal, I <: StorageInteger}
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    json_bytes = sparse_matrix_json_bytes(T, I, Int(nnz), Int(n_columns))
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_register_metadata!(daf, base_key, json_bytes)
    colptr_vector = packed_reserve_typed_vector!(daf, "$(base_key).colptr", I, n_columns + 1)
    rowval_vector = packed_reserve_typed_vector!(daf, "$(base_key).rowval", I, nnz)
    nzval_vector = packed_reserve_typed_vector!(daf, "$(base_key).nzval", T, nnz)
    return (colptr_vector, rowval_vector, nzval_vector, Formats.MappedData)
end

function packed_format_filled_empty_sparse_matrix!(
    daf::PackedDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    packed_finalize_entry!(daf, "$(base_key).colptr")
    packed_finalize_entry!(daf, "$(base_key).rowval")
    packed_finalize_entry!(daf, "$(base_key).nzval")
    return nothing
end

# Build the streaming wrapper for a packed dense matrix. The streaming contract fills one column at a time, but the
# on-disk inner-chunk shape is whatever `chunks_for(...)` picked — the `(n_chunk_rows, 1)` shape from the byte-target
# heuristic, same as the non-streaming `set_matrix!` path. The encoder slices the just-filled column buffer into
# `n_row_tiles` row-tile slices and submits each as a separate inner chunk. Partial-edge tiles (when `nrows` doesn't
# divide evenly into `n_chunk_rows`) are padded with `zero(T)` before encoding; the v3 reader expects every inner
# chunk to be of full `inner_chunk_shape` and crops to the array bounds at access time. The whole matrix lives in a
# single `<name>.zip` entry written incrementally as columns finalize.
function packed_format_streaming_dense_matrix(
    daf::PackedDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
    chunk_shape::NTuple{2, Int},
)::Tuple{PackedDenseMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert chunk_shape[2] == 1
    n_chunk_rows = chunk_shape[1]
    n_row_tiles = cld(Int(nrows), n_chunk_rows)
    n_last_tile_rows = Int(nrows) - (n_row_tiles - 1) * n_chunk_rows
    chunks_per_shard = (n_row_tiles, Int(ncols))
    codec = compressor_for()
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    writer = packed_make_streaming_shard_writer(daf, "$(base_key).zip", T, chunks_per_shard, chunk_shape, codec)

    encoder =
        (column::Int, chunk_buffer::Vector{T}) -> begin
            column_base = (column - 1) * n_row_tiles
            for row_tile in 1:(n_row_tiles - 1)
                start_row = (row_tile - 1) * n_chunk_rows + 1
                tile_view = reshape(view(chunk_buffer, start_row:(start_row + n_chunk_rows - 1)), n_chunk_rows, 1)
                submit_shard_chunk!(writer, column_base + row_tile, tile_view)
            end
            last_start = (n_row_tiles - 1) * n_chunk_rows + 1
            if n_last_tile_rows == n_chunk_rows
                tile_view = reshape(view(chunk_buffer, last_start:Int(nrows)), n_chunk_rows, 1)
                submit_shard_chunk!(writer, column_base + n_row_tiles, tile_view)
            else
                padded = zeros(T, n_chunk_rows)
                padded[1:n_last_tile_rows] = view(chunk_buffer, last_start:Int(nrows))
                submit_shard_chunk!(writer, column_base + n_row_tiles, reshape(padded, n_chunk_rows, 1))
            end
            return nothing
        end
    finalizer = () -> begin
        finalize_shard!(writer)
        json_bytes = packed_array_json_bytes(T, chunk_shape, codec)
        packed_write_bytes!(daf, "$(base_key).json", json_bytes)
        packed_register_metadata!(daf, base_key, json_bytes)
        return nothing
    end
    return (PackedDenseMatrix{T}(Int(nrows), Int(ncols), encoder; finalizer), nothing)
end

# Write a sparse numeric matrix property as the descriptor JSON plus per-component blobs (`colptr`, `rowval`, optional
# `nzval`). Same `nzval`-omission rule as [`packed_format_write_sparse_numeric_vector!`](@ref) for an all-`true` `Bool` matrix.
function packed_format_write_sparse_numeric_matrix!(
    daf::PackedDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::AbstractMatrix,
    is_packed::Bool,
)::Nothing
    colptr_vector = colptr(matrix)
    rowval_vector = rowval(matrix)
    nzval_vector = nzval(matrix)
    nnz = length(rowval_vector)
    n_columns = size(matrix, 2)
    nzval_present = !(eltype(matrix) == Bool && all(nzval_vector))
    colptr_chunk_shape = chunks_for(is_packed, (n_columns + 1,), indtype(matrix))
    rowval_chunk_shape = chunks_for(is_packed, (nnz,), indtype(matrix))
    nzval_chunk_shape = nzval_present ? chunks_for(is_packed, (nnz,), eltype(matrix)) : nothing
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    json_bytes = sparse_matrix_json_bytes(
        eltype(matrix),
        indtype(matrix),
        nnz,
        n_columns;
        colptr_chunk_shape,
        rowval_chunk_shape,
        nzval_chunk_shape,
        nzval_present,
    )
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_format_write_sparse_component!(daf, base_key, "colptr", colptr_vector, colptr_chunk_shape)
    packed_format_write_sparse_component!(daf, base_key, "rowval", rowval_vector, rowval_chunk_shape)
    if nzval_present
        packed_format_write_sparse_component!(daf, base_key, "nzval", nzval_vector, nzval_chunk_shape)
    end
    packed_register_metadata!(daf, base_key, json_bytes)
    return nothing
end

end  # module
