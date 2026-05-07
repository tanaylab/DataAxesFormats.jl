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
using ..MmapZipStores
using ..StorageTypes
using DiskArrays
using JSON
using Mmap
using SparseArrays
using TanayLabUtilities
using Zarr

import ..Operations.DTYPE_BY_NAME
import SparseArrays.indtype

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
# `(n_chunk_rows,)` for vectors and `(n_chunk_rows, 1)` for matrices, where
# `n_chunk_rows = min(packed_target_chunk_bytes() ÷ effective_sizeof(T), shape[1])`. Internal helper for format-level
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
# footer). One `PackedThreadSlot{T}` is allocated per `Threads.maxthreadid()`; threads index into `thread_slots`
# by `Threads.threadid()`.
function PackedDenseMatrix{T}(
    n_rows::Int,
    n_columns::Int,
    encoder::Function;
    finalizer::Function = () -> nothing,
)::PackedDenseMatrix{T} where {T}
    thread_slots = [PackedThreadSlot{T}(0, Vector{T}(undef, n_rows)) for _ in 1:Threads.maxthreadid()]
    return PackedDenseMatrix{T}(n_rows, n_columns, thread_slots, encoder, finalizer)
end

function Base.size(matrix::PackedDenseMatrix)::Tuple{Int, Int}
    return (matrix.n_rows, matrix.n_columns)
end

function Base.view(matrix::PackedDenseMatrix{T}, ::Colon, column::Int)::Vector{T} where {T}
    @assert 1 <= column <= matrix.n_columns
    slot = matrix.thread_slots[Threads.threadid()]
    if slot.current_column == column
        return slot.chunk_buffer  # FLAKY TESTED
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
# bytes plus exact index size); on `finalize_sink!` the index is written immediately after the chunk data
# and the entry is shrunk via [`shrink_mmap_zip_entry!`](@ref), so the only on-disk slack left is the
# ZIP64 extra-field bytes per entry.
mutable struct MmapShardRegion  # NOLINT
    store::MmapZipStore
    key::String
    region::AbstractVector{UInt8}
    cursor::UInt64
    reserved_size::UInt64
end

# Sink-abstraction methods. Each `IncrementalShardWriter` operates on its sink through these three calls:
# `position_in_sink` reads the current write cursor, `write_to_sink!` appends bytes (advancing the cursor),
# and `finalize_sink!` emits the encoded shard index (at the tail) and finishes off any backend-specific
# bookkeeping (closing the IO handle, patching the outer-zip CRC).
position_in_sink(io::IOStream)::UInt64 = UInt64(position(io))
position_in_sink(region::MmapShardRegion)::UInt64 = region.cursor

function write_to_sink!(io::IOStream, bytes::AbstractVector{UInt8})::Nothing
    write(io, bytes)
    return nothing
end

function write_to_sink!(region::MmapShardRegion, bytes::AbstractVector{UInt8})::Nothing
    n = UInt64(length(bytes))
    region.region[(region.cursor + 1):(region.cursor + n)] .= bytes
    region.cursor += n
    return nothing
end

function finalize_sink!(io::IOStream, encoded_index::AbstractVector{UInt8})::Nothing
    write(io, encoded_index)
    close(io)
    return nothing
end

function finalize_sink!(region::MmapShardRegion, encoded_index::AbstractVector{UInt8})::Nothing
    index_size = UInt64(length(encoded_index))
    region.region[(region.cursor + 1):(region.cursor + index_size)] .= encoded_index
    actual_size = region.cursor + index_size
    shrink_mmap_zip_entry!(region.store, region.key, actual_size)
    patch_mmap_zip_entry_crc!(region.store, region.key)
    return nothing
end

# Streaming writer for the v3 sharded-array binary format. Concurrent submitters call `submit_shard_chunk!`
# with an inner-chunk index and the chunk's `AbstractArray` of data; the writer encodes the chunk through
# `inner_pipeline`, appends the encoded bytes to its sink, and records `(file_offset, n_bytes)` in the
# in-memory index slab at the slot for that chunk. Encoding runs outside the lock; only the cursor read
# and the byte append run under the lock, so submitters serialize on disk I/O but not on encoding.
# Chunks may arrive in any order — the index slab is keyed by chunk index, not arrival order. Empty (all
# fill-value) inner chunks are elided per the sharding spec; their slots stay at the `MAX_UINT64` sentinel
# that marks "no data". `finalize_shard!` encodes the index slab through `index_pipeline` (BytesCodec
# little-endian + CRC32c) and emits it as the shard's footer, matching `index_location = :end`.
mutable struct IncrementalShardWriter{S, P1 <: Zarr.AbstractCodecPipeline, P2 <: Zarr.AbstractCodecPipeline, F}  # NOLINT
    sink::S
    inner_pipeline::P1
    index_pipeline::P2
    fill_value::F
    index_data::Vector{UInt64}
    write_lock::ReentrantLock
end

function IncrementalShardWriter(  # FLAKY TESTED
    sink,
    inner_pipeline::Zarr.AbstractCodecPipeline,
    index_pipeline::Zarr.AbstractCodecPipeline,
    fill_value,
    n_chunks::Int,
)::IncrementalShardWriter
    index_data = fill(typemax(UInt64), 2 * n_chunks)
    return IncrementalShardWriter(sink, inner_pipeline, index_pipeline, fill_value, index_data, ReentrantLock())
end

function submit_shard_chunk!(writer::IncrementalShardWriter, chunk_index::Int, chunk_data::AbstractArray)::Nothing
    encoded = Zarr.pipeline_encode(writer.inner_pipeline, chunk_data, writer.fill_value)
    # Defensive elision: the v3 sharding spec lets the writer omit chunks whose encoding is empty (e.g. an all
    # `fill_value` chunk under a codec that signals elision by returning `nothing`). The currently shipped
    # `compressor_for()` codecs all produce non-empty output even for all-`fill_value` data, so this branch is not
    # reachable from user code today; it future-proofs the writer against eliding codecs.
    if encoded === nothing || isempty(encoded)
        return nothing  # UNTESTED
    end
    @lock writer.write_lock begin
        offset = position_in_sink(writer.sink)
        writer.index_data[2 * chunk_index - 1] = offset
        writer.index_data[2 * chunk_index] = UInt64(length(encoded))
        write_to_sink!(writer.sink, encoded)
    end
    return nothing
end

function finalize_shard!(writer::IncrementalShardWriter)::Nothing
    @lock writer.write_lock begin
        encoded_index = Zarr.pipeline_encode(writer.index_pipeline, writer.index_data, nothing)
        finalize_sink!(writer.sink, encoded_index)
    end
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

# Open a standalone v3 sharded-array byte buffer (e.g. a `FilesDaf` `<name>.shard` on disk, or the bytes of a
# `ZipDaf` packed entry served straight out of the outer-zip mmap) as a `Zarr.ZArray` for read access. Wraps the
# bytes in a single-key `Zarr.DictStore` (one outer chunk covers the whole array, so only the chunk key produced
# by `ChunkKeyEncoding('/', true)` for the all-zero shard index is present), reconstructs `MetadataV3` from the
# passed-in parameters, and builds the `ZArray`. Reads go through the standard `ShardingCodec` decode path: the
# index slab is parsed once, then per-inner-chunk slices are decoded on demand.
function open_shard_as_zarray(
    bytes::Vector{UInt8},
    ::Type{T},
    shape::NTuple{N, Int},
    inner_chunk_shape::NTuple{N, Int},
    bytes_bytes_codecs::Tuple,
    index_location::Symbol,
)::Zarr.ZArray{T, N} where {T, N}
    metadata = build_shard_metadata(T, shape, inner_chunk_shape, bytes_bytes_codecs, index_location)
    chunk_key_str = Zarr.citostring(metadata.chunk_key_encoding, CartesianIndex(ntuple(_ -> 1, N)))
    store = Zarr.DictStore(Dict{String, Vector{UInt8}}(chunk_key_str => bytes))  # NOJET
    return Zarr.ZArray(metadata, store, "", Dict{String, Any}(), false)
end

# Path-taking convenience: mmap the file at `path` and delegate to the byte-taking overload above. Used by FilesDaf's
# packed read paths.
function open_shard_as_zarray(  # FLAKY TESTED
    path::AbstractString,
    ::Type{T},
    shape::NTuple{N, Int},
    inner_chunk_shape::NTuple{N, Int},
    bytes_bytes_codecs::Tuple,
    index_location::Symbol,
)::Zarr.ZArray{T, N} where {T, N}
    mmap_bytes = open(io -> Mmap.mmap(io, Vector{UInt8}), path, "r")
    return open_shard_as_zarray(mmap_bytes, T, shape, inner_chunk_shape, bytes_bytes_codecs, index_location)
end

# Open a packed dense property whose JSON descriptor carries `packed: true`. Wraps the result of
# [`open_shard_as_zarray`](@ref) in a `DiskArrays.cache` for re-fetch amortization. Codec parameters (compression,
# level, inner-chunk shape, index location) are reconstructed from the sidecar. Two overloads: the path-taking form
# (`FilesDaf` `<name>.shard` on disk) and the bytes-taking form (`ZipDaf` packed entry served straight out of the
# outer-zip mmap).
function open_packed_dense_array(
    shard_path::AbstractString,
    ::Type{T},
    json::AbstractDict,
    shape::NTuple{N, Int},
)::DiskArrays.CachedDiskArray where {T, N}
    chunk_shape = NTuple{N, Int}(json["chunk_shape"])  # NOJET
    codec = PackedCodec(Symbol(json["compression"]), Int(json["compression_level"]))
    index_location = Symbol(json["index_location"])
    zarray = open_shard_as_zarray(shard_path, T, shape, chunk_shape, v3_bytes_codecs_for(codec, T), index_location)
    return DiskArrays.cache(zarray; maxsize = packed_local_cache_mb())  # NOJET
end

function open_packed_dense_array(
    bytes::Vector{UInt8},
    ::Type{T},
    json::AbstractDict,
    shape::NTuple{N, Int},
)::DiskArrays.CachedDiskArray where {T, N}
    chunk_shape = NTuple{N, Int}(json["chunk_shape"])  # NOJET
    codec = PackedCodec(Symbol(json["compression"]), Int(json["compression_level"]))
    index_location = Symbol(json["index_location"])
    zarray = open_shard_as_zarray(bytes, T, shape, chunk_shape, v3_bytes_codecs_for(codec, T), index_location)
    return DiskArrays.cache(zarray; maxsize = packed_local_cache_mb())  # NOJET
end

# Build the v3 `MetadataV3` for a one-shard-per-array sharded layout: outer chunks equal the array
# shape (one shard covers everything), the shard's `array_bytes` codec is a `ShardingCodec` whose
# inner-chunk shape and inner pipeline match the caller-supplied parameters, and the index slab
# lives at the file's `index_location` (`:end` or `:start`). Shared by [`open_shard_as_zarray`](@ref)
# and [`write_packed_dense_array`](@ref).
function build_shard_metadata(  # FLAKY TESTED
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
function shard_fill_value(::Type{T}) where {T}  # FLAKY TESTED
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
# written by earlier code that have not been rewritten since the version bump.
function parse_sparse_descriptor(json::AbstractDict, indtype_property::AbstractString)::Tuple{String, String}
    if haskey(json, "eltype")
        return (String(json["eltype"]), String(json["indtype"]))
    else
        nzval_descriptor = json["nzval"]
        @assert nzval_descriptor isa AbstractDict
        indtype_descriptor = json[indtype_property]
        @assert indtype_descriptor isa AbstractDict
        return (String(nzval_descriptor["eltype"]), String(indtype_descriptor["eltype"]))
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
               "\"packed\":true,\"chunk_shape\":$(chunk_shape_str)," *
               "\"compression\":\"$(codec.compression)\",\"compression_level\":$(codec.compression_level)," *
               "\"index_location\":\"end\"}"
    end
end

# v1.1 dense flat sidecar: `{format,eltype}`. The bytes-returning form lets every backend route the result through
# its own storage layer (the FilesDaf write helpers wrap `write(path, …)`, the ZipDaf write paths put the bytes into
# a zip entry, and so on).
function dense_array_json_bytes(eltype::Type{<:StorageScalarBase})::Vector{UInt8}
    return Vector{UInt8}("{\"format\":\"dense\",\"eltype\":\"$(json_eltype_name(eltype))\"}\n")
end

# v1.1 dense packed sidecar: `{format,eltype,packed,chunk_shape,compression,compression_level,index_location}`. The
# bytes describe the inner-pipeline parameters of the corresponding `<name>.shard` (or per-component shard).
function packed_array_json_bytes(
    eltype::Type{<:StorageScalarBase},
    chunk_shape::NTuple{N, Int},
    codec::PackedCodec,
)::Vector{UInt8} where {N}
    chunk_shape_str = "[" * join(string.(chunk_shape), ",") * "]"
    return Vector{UInt8}(
        "{\"format\":\"dense\",\"eltype\":\"$(json_eltype_name(eltype))\"," *
        "\"packed\":true,\"chunk_shape\":$(chunk_shape_str)," *
        "\"compression\":\"$(codec.compression)\",\"compression_level\":$(codec.compression_level)," *
        "\"index_location\":\"end\"}\n",
    )
end

# v1.1 sparse vector sidecar: `{format,nzind,nzval}` where each component is a `component_descriptor_json` (flat or
# packed depending on its `*_chunk_shape` argument). Per-component `*_codec` overrides default to
# `compressor_for()`; `zarr_convert.jl` passes explicit codecs so the linked bytes stay decodable when the
# destination's global compressor setting differs from the source's.
function sparse_vector_json_bytes(
    eltype::Type{<:StorageScalarBase},
    indtype::Type{<:StorageInteger},
    n_elements::Integer;
    nzind_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzind_codec::PackedCodec = compressor_for(),
    nzval_codec::PackedCodec = compressor_for(),
)::Vector{UInt8}
    nzind_desc = component_descriptor_json(indtype, n_elements, nzind_chunk_shape, nzind_codec)
    nzval_desc = component_descriptor_json(eltype, n_elements, nzval_chunk_shape, nzval_codec)
    return Vector{UInt8}("{\"format\":\"sparse\",\"nzind\":$(nzind_desc),\"nzval\":$(nzval_desc)}\n")
end

# v1.1 sparse matrix sidecar: `{format,colptr,rowval,nzval}`. `colptr` is sized at `n_columns + 1` and the
# value-bearing components at `nnz`; each is flat or packed independently per its `*_chunk_shape` argument.
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
)::Vector{UInt8}
    colptr_desc = component_descriptor_json(indtype, n_columns + 1, colptr_chunk_shape, colptr_codec)
    rowval_desc = component_descriptor_json(indtype, nnz, rowval_chunk_shape, rowval_codec)
    nzval_desc = component_descriptor_json(eltype, nnz, nzval_chunk_shape, nzval_codec)
    return Vector{UInt8}(
        "{\"format\":\"sparse\"," * "\"colptr\":$(colptr_desc),\"rowval\":$(rowval_desc),\"nzval\":$(nzval_desc)}\n",
    )
end

# Encode `data` as a single v3 sharded-array byte blob and return the bytes. The caller decides
# where to put them: the `FilesDaf` packed write path writes them to a fresh `<name>.shard`
# `IOStream`; the `ZipDaf` packed write path writes them into a reserved outer-zip mmap region
# via [`reserve_mmap_zip_entry!`](@ref). Implementation: build a sharded `Zarr.ZArray` over an
# in-memory `DictStore`, write the data through `ShardingCodec`'s encode (which produces one
# byte blob — the whole shard — under the array's chunk key), then return those bytes. The
# resulting bytes are byte-identical to what a `ZarrDaf` in `DirectoryStore` mode would write at
# `<group>/<name>/c/0[/0]` for the same data, which is the property `zarr_convert.jl` relies on
# for hard-link conversion across backends.
function encode_packed_dense_array(
    data::AbstractArray{T, N},
    inner_chunk_shape::NTuple{N, Int},
    bytes_bytes_codecs::Tuple,
    index_location::Symbol,
)::Vector{UInt8} where {T, N}
    metadata = build_shard_metadata(T, size(data), inner_chunk_shape, bytes_bytes_codecs, index_location)
    return Zarr.pipeline_encode(metadata.pipeline, data, shard_fill_value(T))  # NOJET
end

# Build an `IncrementalShardWriter` over the caller-supplied `sink`. The sink can be anything that implements the
# `position_in_sink` / `write_to_sink!` / `finalize_sink!` interface — typically an `IOStream` (FilesDaf) or an
# [`MmapShardRegion`](@ref) (ZipDaf, ZarrDaf-Zip). The inner pipeline is reconstructed for `T` from
# `bytes_bytes_codecs` so the on-disk shard bytes match what the equivalent `ZarrDaf` write would produce for the
# same content under the same codec.
function make_streaming_shard_writer(  # FLAKY TESTED
    sink,
    ::Type{T},
    n_chunks::Int,
    bytes_bytes_codecs::Tuple,
)::IncrementalShardWriter where {T}
    inner_pipeline = build_inner_pipeline(T, bytes_bytes_codecs)
    index_pipeline =
        Zarr.V3Pipeline((), Zarr.Codecs.V3Codecs.BytesCodec(:little), (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),))
    return IncrementalShardWriter(sink, inner_pipeline, index_pipeline, shard_fill_value(T), n_chunks)
end

# Open an `IOStream`-sinked `IncrementalShardWriter` for streaming a packed dense array to a single
# `<name>.shard` file. The caller submits inner chunks via `submit_shard_chunk!` and finalizes via
# `finalize_shard!`; the writer writes the index slab at the file's tail and closes the stream.
# Used by the `FilesDaf` streaming write path (and, conceptually, the `ZarrDaf`-`DirectoryStore`
# streaming write path — that one constructs the equivalent inline because it derives the codec
# pipelines from a pre-built sharded `ZArray`).
function open_streaming_shard_writer(  # FLAKY TESTED
    shard_path::AbstractString,
    ::Type{T},
    n_chunks::Int,
    bytes_bytes_codecs::Tuple,
)::IncrementalShardWriter where {T}
    return make_streaming_shard_writer(open(shard_path, "w"), T, n_chunks, bytes_bytes_codecs)
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
# little-endian bytes, zero-copy mmap-able) when `chunk_shape === nothing`, or at `<base_key>.<suffix>.shard` (v3
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
        encoded = encode_packed_dense_array(data, chunk_shape, v3_bytes_codecs_for(compressor_for(), T), :end)
        packed_write_bytes!(daf, "$(base_key).$(suffix).shard", encoded)
    end
    return nothing
end

# Write a packed dense numeric or string property as the descriptor JSON at `<base_key>.json` and the encoded v3
# sharded-array bytes at `<base_key>.shard`. `base_key` is a logical relative key like `"vectors/r/n"` or
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
    encoded = encode_packed_dense_array(data, chunk_shape, v3_bytes_codecs_for(codec, T), :end)
    packed_write_bytes!(daf, "$(base_key).shard", encoded)
    packed_register_metadata!(daf, base_key, json_bytes)
    return nothing
end

# Write a sparse numeric vector property as the descriptor JSON at `vectors/<axis>/<name>.json` plus per-component
# shard / flat blobs (`nzind`, optionally `nzval`). The `nzval` blob is omitted whenever `eltype == Bool` and all
# nonzeros are `true` — the v1.1 reader synthesizes the values in that one case. `packed` selects flat (`chunk_shape ==
# nothing`) versus packed (`chunk_shape !== nothing`) per-component layout via [`chunks_for`](@ref).
function packed_format_write_sparse_numeric_vector!(
    daf::PackedDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::AbstractVector,
    packed::Bool,
)::Nothing
    nzind_vector = nzind(vector)
    nzval_vector = nzval(vector)
    nnz = length(nzind_vector)
    nzval_omitted = eltype(vector) == Bool && all(nzval_vector)
    nzind_chunk_shape = chunks_for(packed, (nnz,), indtype(vector))
    nzval_chunk_shape = nzval_omitted ? nothing : chunks_for(packed, (nnz,), eltype(vector))
    base_key = "vectors/$(axis)/$(name)"
    json_bytes = sparse_vector_json_bytes(eltype(vector), indtype(vector), nnz; nzind_chunk_shape, nzval_chunk_shape)
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_format_write_sparse_component!(daf, base_key, "nzind", nzind_vector, nzind_chunk_shape)
    if !nzval_omitted
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
    ::Integer,
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

# Open the entry at the logical key as a packed `Zarr.ZArray{T, N}` of the given shape, dispatching `descriptor` to the
# appropriate codec pipeline. `FilesDaf` opens directly from the file path; `ZipDaf` reads the shard bytes from the
# archive and opens an in-memory `ZArray`. The returned array is wrapped in `DiskArrays.cache` so chunks decode on
# demand (and are reused across slices).
function packed_open_array(  # UNTESTED
    daf::PackedDaf,
    ::AbstractString,
    ::Type{T},
    ::AbstractDict,
    ::NTuple{N, Int},
)::DiskArrays.CachedDiskArray where {T, N}
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
    packed = get(descriptor, "packed", false) === true
    n_elements = if n_elements_known !== nothing
        n_elements_known
    elseif haskey(descriptor, "n_elements")
        Int(descriptor["n_elements"])
    else
        div(packed_entry_size(daf, flat_key), sizeof(T))
    end
    if packed
        zarray = packed_open_array(daf, "$(flat_key).shard", T, descriptor, (n_elements,))
        return (Vector{T}(zarray[:]), nothing, n_elements, Formats.MemoryData)  # NOJET
    end
    vector, byte_owner, cache_group = packed_read_typed_vector(daf, flat_key, T, n_elements)
    return (Vector{T}(vector), byte_owner, n_elements, cache_group)
end

# Open one sparse-property component as a lazy `AbstractVector{T}` source — a `DiskArrays.cache`-wrapped `ZArray` for
# packed components (chunks decode on demand), or a flat-backed `Vector{T}` (mmap or memory) for unpacked components.
# Used by the lazy sparse-read path: the caller wraps the resulting sources in a [`LazySparse.LazySparseMatrix`](@ref)
# and defers materialisation until a consumer asks for one of the `SparseArrays` accessors. Returns `(source,
# byte_owner, n_elements, cache_group)`.
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
    packed = get(descriptor, "packed", false) === true
    n_elements = if n_elements_known !== nothing
        n_elements_known
    elseif haskey(descriptor, "n_elements")
        Int(descriptor["n_elements"])
    else
        div(packed_entry_size(daf, flat_key), sizeof(T))
    end
    if packed
        zarray = packed_open_array(daf, "$(flat_key).shard", T, descriptor, (n_elements,))
        return (zarray, nothing, n_elements, Formats.MemoryData)
    end
    vector, byte_owner, cache_group = packed_read_typed_vector(daf, flat_key, T, n_elements)  # FLAKY TESTED
    return (vector, byte_owner, n_elements, cache_group)  # FLAKY TESTED
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
# fill encodes/streams to the per-property `.shard` entry plus the descriptor JSON.

function packed_format_get_empty_dense_vector!(
    daf::PackedDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
    n_elements::Integer,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    chunk_shape = chunks_for(packed, (Int(n_elements),), T)
    if chunk_shape !== nothing
        # The vector packed path has no streaming contract: hand the user a fresh in-RAM `Vector{T}` and encode it to
        # the per-property `.shard` entry at fill time.
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
    packed::Bool,
    nrows::Integer,
    ncols::Integer,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    chunk_shape = chunks_for(packed, (Int(nrows), Int(ncols)), T)
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

function packed_format_filled_empty_dense_matrix!(  # FLAKY TESTED
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
# single `<name>.shard` entry written incrementally as columns finalize.
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
    n_chunks = n_row_tiles * Int(ncols)
    codec = compressor_for()
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    writer = packed_make_streaming_shard_writer(daf, "$(base_key).shard", T, n_chunks, chunk_shape, codec)

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
    packed::Bool,
)::Nothing
    colptr_vector = colptr(matrix)
    rowval_vector = rowval(matrix)
    nzval_vector = nzval(matrix)
    nnz = length(rowval_vector)
    n_columns = size(matrix, 2)
    nzval_omitted = eltype(matrix) == Bool && all(nzval_vector)
    colptr_chunk_shape = chunks_for(packed, (n_columns + 1,), indtype(matrix))
    rowval_chunk_shape = chunks_for(packed, (nnz,), indtype(matrix))
    nzval_chunk_shape = nzval_omitted ? nothing : chunks_for(packed, (nnz,), eltype(matrix))
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    json_bytes = sparse_matrix_json_bytes(
        eltype(matrix),
        indtype(matrix),
        nnz,
        n_columns;
        colptr_chunk_shape,
        rowval_chunk_shape,
        nzval_chunk_shape,
    )
    packed_write_bytes!(daf, "$(base_key).json", json_bytes)
    packed_format_write_sparse_component!(daf, base_key, "colptr", colptr_vector, colptr_chunk_shape)
    packed_format_write_sparse_component!(daf, base_key, "rowval", rowval_vector, rowval_chunk_shape)
    if !nzval_omitted
        packed_format_write_sparse_component!(daf, base_key, "nzval", nzval_vector, nzval_chunk_shape)
    end
    packed_register_metadata!(daf, base_key, json_bytes)
    return nothing
end

end  # module
