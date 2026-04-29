"""
A `Daf` storage format in a [Zarr](https://zarr.readthedocs.io/) directory tree or ZIP archive. Like
[`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf), the data can live in a directory of files on the filesystem
(so standard filesystem tools work, and deleting a property immediately frees its storage), and offers a different
trade-off compared to [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) and
[`H5df`](@ref DataAxesFormats.H5dfFormat.H5df).

`FilesDaf` uses its own `Daf`-specific layout, but the individual files are in deliberately simple formats (`JSON` for
metadata, one-line-per-entry text for axis entries, raw little-endian binary for numeric data), so they are easy to
inspect or produce with standard command-line tools even without any `Daf`-aware library. `ZarrDaf` instead lays the
files out according to the Zarr specification: the per-array `.zarray` metadata and the chunk files are more opaque
than `FilesDaf`'s plain text/JSON, but in exchange the directory can be read directly by any Zarr library (e.g. the
Python `zarr` package) without that library having to know anything about `Daf`.

A Zarr directory is still a directory rather than a single file, so for convenient publication or transport we also
support storing a `Daf` data set inside a single ZIP archive via
[`MmapZipStore`](@ref DataAxesFormats.MmapZipStores.MmapZipStore). Archives written by this package hold every chunk
uncompressed (method `0`) so it can be memory-mapped for direct access just like the directory backend. On the ZIP
backend the archive is append-only: properties cannot be deleted and axes cannot be reordered. For read access, any
Zarr v2 ZIP archive that matches the internal structure described below is accepted (including ones produced by
foreign tools such as Python's `zarr` package, even if the chunks are chunked and/or compressed, subject to `Zarr.jl`'s
support for data types, filters, and compressors). Remote object stores (S3, GCS, …) are not supported.

!!! note

    Zarr stores all chunks of an array as flat sibling files in a single directory (this is dictated by the Zarr
    specification, not by this package). For packed (chunked) matrices with many chunks, that directory can hold
    hundreds of thousands of files, which stresses both filesystem performance and interactive tools like `ls` or
    file explorers. The [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) backend buckets its chunks into a
    hierarchy and avoids this issue, so it is friendlier than `ZarrDaf` for very large packed matrices. Pick
    `ZarrDaf` when interoperability with the wider Zarr ecosystem matters more than per-directory file count.

We use the following internal structure under some root Zarr group (which is **not** compatible with any specific
existing Zarr-based convention such as [`OME-NGFF`](https://ngff.openmicroscopy.org/)):

  - The directory will contain 4 sub-groups: `scalars`, `axes`, `vectors`, and `matrices`, and a `daf` group attribute.

  - The `daf` group attribute signifies that the group contains `Daf` data. It is a two-element array of integers, the
    first being the major version number and the second the minor version number, using
    [semantic versioning](https://semver.org/). This makes it easy to test whether some Zarr group does/n't contain
    `Daf` data, and which version of the internal structure it is using. The defined version is `[1,0]`. The
    underlying Zarr store is Zarr v3.

  - The `scalars` group contains scalar properties, each as a single-element Zarr array. The only supported scalar data
    types are these included in [`StorageScalar`](@ref). If you **really** need something else, serialize it to JSON
    and store the result as a string scalar. This should be **extremely** rare.

  - The `axes` group contains a Zarr array per axis, which contains a vector of strings (the names of the axis
    entries).

  - The `vectors` group contains a sub-group for each axis. Each such sub-group contains vector properties. If the
    vector is dense, it is stored directly as a Zarr array. Otherwise, it is stored as a sub-group containing two child
    Zarr arrays: `nzind` containing the indices of the non-zero values, and `nzval` containing the actual values. See
    Julia's `SparseVector` implementation for details. The only supported vector element types are these included in
    [`StorageScalar`](@ref), same as [`StorageVector`](@ref).

    If the data type is `Bool` then the data vector is typically all-`true` values; in this case we simply skip storing
    the `nzval` child array.

  - The `matrices` group contains a sub-group for each rows axis, which contains a sub-group for each columns axis.
    Each such sub-sub-group contains matrix properties. If the matrix is dense, it is stored directly as a Zarr array
    (in column-major layout). Otherwise, it is stored as a sub-group containing three child Zarr arrays: `colptr`
    containing the indices of the rows of each column in `rowval`, `rowval` containing the indices of the non-zero rows
    of the columns, and `nzval` containing the non-zero matrix entry values. See Julia's `SparseMatrixCSC`
    implementation for details. The only supported matrix element types are these included in [`StorageReal`](@ref) -
    this explicitly excludes matrices of strings, same as [`StorageMatrix`](@ref).

    If the data type is `Bool` then the data matrix is typically all-`true` values; in this case we simply skip storing
    the `nzval` child array.

  - Flat properties (the default) are stored as a single Zarr chunk covering the full array, without compression, so
    the chunk file on disk is a raw binary image that we can memory-map. Packed properties (`packed = true`, uncompressed
    size at or above [`DAF_PACKED_TARGET_CHUNK_KB`](@ref)) are stored chunked + compressed via the codec resolved from
    [`DAF_PACKED_COMPRESSION`](@ref). Both encodings coexist within a `1.1`-marked dataset.

Example Zarr directory structure:

    example-daf-dataset-root-directory.zarr/
    ├─ .zgroup
    ├─ daf/
    │  ├─ .zarray
    │  └─ 0
    ├─ scalars/
    │  ├─ .zgroup
    │  └─ version/
    │     ├─ .zarray
    │     └─ 0
    ├─ axes/
    │  ├─ .zgroup
    │  ├─ cell/
    │  └─ gene/
    ├─ vectors/
    │  ├─ .zgroup
    │  ├─ cell/
    │  │  ├─ .zgroup
    │  │  └─ batch/
    │  └─ gene/
    │     ├─ .zgroup
    │     └─ is_marker/
    └─ matrices/
       ├─ .zgroup
       ├─ cell/
       │  ├─ .zgroup
       │  └─ gene/
       │     ├─ .zgroup
       │     └─ UMIs/
       │        ├─ .zgroup
       │        ├─ colptr/
       │        ├─ rowval/
       │        └─ nzval/
       └─ gene/
          ├─ .zgroup
          ├─ cell/
          └─ gene/

!!! note

    `Zarr.jl` writes matrices in C storage order (the only order it supports) with the `.zarray` `shape` listed in the
    reverse of the Julia matrix shape, so the raw chunk bytes match Julia's native column-major layout. A `Daf` matrix
    whose `(rows_axis, columns_axis)` are `(cell, gene)` (a Julia `(n_cells, n_genes)` matrix) is therefore written with
    `.zarray` containing `"shape": [n_genes, n_cells]` and `"order": "C"`. A client using a different Zarr
    implementation — most notably Python's `zarr` package — reads this as a C-contiguous NumPy array of shape
    `(n_genes, n_cells)`, which is the **transpose** of the Julia view. The bytes on disk are identical; only the shape
    labels are swapped. To obtain the `Daf`-canonical `(cell, gene)` orientation in Python, apply `.T` (a zero-copy
    view) to the loaded array. This affects only dense matrices (the `colptr`/`rowval`/`nzval` child arrays of sparse
    matrices are 1D vectors, unaffected); 1D axis-entry arrays and vector properties have the same shape in both
    languages.

!!! note

    The code here assumes the Zarr data obeys all the above conventions and restrictions. As long as you only create
    and access `Daf` data in Zarr directories using [`ZarrDaf`](@ref), then the code will work as expected (assuming no
    bugs). However, if you do this in some other way (e.g., a Zarr library in another language producing compressed or
    multi-chunk arrays), and the result is invalid, then the code here may fail with "less than friendly" error
    messages.
"""
module ZarrFormat

export ZarrDaf

using ..Formats
using ..MmapZipStores
using ..ReadOnly
using ..Readers
using ..StorageTypes
using ..Writers
using Base.Filesystem
using DiskArrays
using JSON
using Mmap
using ProgressMeter
using SparseArrays
using TanayLabUtilities
using Zarr

import ..Formats
import ..Formats.Internal
import ..PackedFormat.PackedCodec
import ..PackedFormat.PackedDenseMatrix
import ..PackedFormat.chunks_for
import ..PackedFormat.compressor_for
import ..PackedFormat.flush_packed_dense_matrix!
import ..PackedFormat.packed_local_cache_mb
import ..Readers.base_array
import ..Reorder
import ..ZipFormat.SharedMmapZipStoreHandle
import ..ZipFormat.acquire_shared_mmap_zip_store!
import ..ZipFormat.parse_zip_archive_path

"""
The major version of the [`ZarrDaf`](@ref) on-disk format supported by this code.
"""
MAJOR_VERSION::UInt8 = 1

"""
The highest minor version of the [`ZarrDaf`](@ref) on-disk format supported by this code.
"""
MINOR_VERSION::UInt8 = 0

const DAF_KEY = "daf"
const SCALARS = "scalars"
const AXES = "axes"
const VECTORS = "vectors"
const MATRICES = "matrices"

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
# fixes the `sizeof` call site; `string_zcreate` (in the permanent helpers section below) then needs its
# `VLenUTF8V3Codec()` reference swapped for the upstream codec constructor.
# ==============================================================================

struct VLenUTF8V3Codec <: Zarr.Codecs.V3Codecs.V3Codec{:array, :bytes}
end

Zarr.Codecs.V3Codecs.name(::VLenUTF8V3Codec) = "vlen-utf8"
Zarr.Codecs.V3Codecs.is_fixed_size(::VLenUTF8V3Codec) = false

JSON.lower(::VLenUTF8V3Codec) =
    Dict{String, Any}("name" => "vlen-utf8", "configuration" => Dict{String, Any}())

function Zarr.Codecs.V3Codecs.codec_encode(::VLenUTF8V3Codec, data::AbstractArray)::Vector{UInt8}
    buffer = IOBuffer()
    write(buffer, UInt32(length(data)))
    for element in data
        utf8_bytes = transcode(String, String(element))
        write(buffer, UInt32(ncodeunits(utf8_bytes)))
        write(buffer, utf8_bytes)
    end
    return take!(buffer)
end

function Zarr.Codecs.V3Codecs.codec_decode(
    ::VLenUTF8V3Codec,
    encoded::Vector{UInt8},
    ::Type{T},
    shape::NTuple{N, Int};
    fill_value = nothing,
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

# ==============================================================================
# END: Zarr.jl 0.10 v3 workarounds.
# ==============================================================================

# Build a v3 numeric ZArray under `group` at `name` with the given shape, inner-chunk shape, and bytes→bytes
# codec chain (empty for the single-chunk uncompressed encoding). Bypasses `Zarr.zcreate` so the metadata is
# the only write per-array (Zarr.jl 0.10's `zcreate` always calls `writeattrs` after `writemetadata`, which
# read-modify-writes `zarr.json` and is rejected by append-only stores like `MmapZipStore`).
function numeric_zcreate(
    ::Type{T},
    group::ZGroup,
    name::AbstractString,
    shape::NTuple{N, Int},
    chunks::NTuple{N, Int} = shape,
    bytes_bytes_codecs::Tuple = (),
)::ZArray{T, N} where {T, N}
    pipeline = Zarr.V3Pipeline((), Zarr.Codecs.V3Codecs.BytesCodec(:little), bytes_bytes_codecs)
    metadata = Zarr.MetadataV3{T, N, typeof(pipeline)}(
        3,
        "array",
        shape,
        chunks,
        Zarr.typestr3(T),
        pipeline,
        zero(T),
        Zarr.ChunkKeyEncoding('/', true),
    )
    storage = group.storage
    array_path = Zarr._concatpath(group.path, String(name))
    if !Zarr.isemptysub(storage, array_path)
        error("non-empty Zarr path: $(array_path)")  # UNTESTED
    end
    Zarr.writemetadata(Zarr.ZarrFormat(3), storage, array_path, metadata)
    array = Zarr.ZArray(metadata, storage, array_path, Dict{String, Any}(), true)
    group.arrays[String(name)] = array
    return array
end

# Build a v3 `String` ZArray under `group` at `name` with the given shape and inner-chunk shape, the local
# `VLenUTF8V3Codec` as the array→bytes step, and the given (possibly empty) bytes→bytes codec tuple as the
# post-encoding compression chain.
function string_zcreate(
    group::ZGroup,
    name::AbstractString,
    shape::NTuple{N, Int},
    chunks::NTuple{N, Int} = shape,
    bytes_bytes_codecs::Tuple = (),
)::ZArray{String, N} where {N}
    pipeline = Zarr.V3Pipeline((), VLenUTF8V3Codec(), bytes_bytes_codecs)
    metadata = Zarr.MetadataV3{String, N, typeof(pipeline)}(
        3,
        "array",
        shape,
        chunks,
        "string",
        pipeline,
        "",
        Zarr.ChunkKeyEncoding('/', true),
    )
    storage = group.storage
    array_path = Zarr._concatpath(group.path, String(name))
    if !Zarr.isemptysub(storage, array_path)
        error("non-empty Zarr path: $(array_path)")  # UNTESTED
    end
    Zarr.writemetadata(Zarr.ZarrFormat(3), storage, array_path, metadata)
    array = ZArray(metadata, storage, array_path, Dict{String, Any}(), true)
    group.arrays[String(name)] = array
    return array
end

# Build a v3 numeric ZArray under `group` at `name` as a single-shard sharded array per ZEP-0002: the outer chunk
# shape equals the array shape (one shard covers the whole array), the inner chunks have shape `inner_chunks`,
# and `bytes_bytes_codecs` is the bytes→bytes chain applied per inner chunk (compression, etc.). The shard's
# index lives at the tail (`index_location = :end`) and is protected by CRC32c.
function sharded_zcreate(
    ::Type{T},
    group::ZGroup,
    name::AbstractString,
    shape::NTuple{N, Int},
    inner_chunks::NTuple{N, Int},
    bytes_bytes_codecs::Tuple = (),
)::ZArray{T, N} where {T, N}
    inner_pipeline = Zarr.V3Pipeline((), Zarr.Codecs.V3Codecs.BytesCodec(:little), bytes_bytes_codecs)
    index_pipeline = Zarr.V3Pipeline((), Zarr.Codecs.V3Codecs.BytesCodec(:little), (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),))
    sharding = Zarr.Codecs.V3Codecs.ShardingCodec(inner_chunks, inner_pipeline, index_pipeline, :end)
    pipeline = Zarr.V3Pipeline((), sharding, ())
    metadata = Zarr.MetadataV3{T, N, typeof(pipeline)}(
        3,
        "array",
        shape,
        shape,
        Zarr.typestr3(T),
        pipeline,
        zero(T),
        Zarr.ChunkKeyEncoding('/', true),
    )
    storage = group.storage
    array_path = Zarr._concatpath(group.path, String(name))
    if !Zarr.isemptysub(storage, array_path)
        error("non-empty Zarr path: $(array_path)")  # UNTESTED
    end
    Zarr.writemetadata(Zarr.ZarrFormat(3), storage, array_path, metadata)
    array = Zarr.ZArray(metadata, storage, array_path, Dict{String, Any}(), true)
    group.arrays[String(name)] = array
    return array
end

# Mmap-backed sink that lets `IncrementalShardWriter` write into a region reserved by
# [`reserve_mmap_zip_entry!`](@ref). The reserved region is over-allocated to an upper bound (raw chunk
# bytes plus exact index size); the index is written at the tail of the region and any unused bytes between
# the last data chunk and the index are padding that the reader skips because the index records the actual
# `(offset, nbytes)` of every chunk.
mutable struct MmapShardRegion
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
    index_offset = region.reserved_size - index_size
    region.region[(index_offset + 1):region.reserved_size] .= encoded_index
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
mutable struct IncrementalShardWriter{S, P1 <: Zarr.AbstractCodecPipeline, P2 <: Zarr.AbstractCodecPipeline, F}
    sink::S
    inner_pipeline::P1
    index_pipeline::P2
    fill_value::F
    index_data::Vector{UInt64}
    write_lock::ReentrantLock
end

function IncrementalShardWriter(
    sink,
    inner_pipeline::Zarr.AbstractCodecPipeline,
    index_pipeline::Zarr.AbstractCodecPipeline,
    fill_value,
    n_chunks::Int,
)::IncrementalShardWriter
    index_data = fill(typemax(UInt64), 2 * n_chunks)
    return IncrementalShardWriter(sink, inner_pipeline, index_pipeline, fill_value, index_data, ReentrantLock())
end

function submit_shard_chunk!(
    writer::IncrementalShardWriter,
    chunk_index::Int,
    chunk_data::AbstractArray,
)::Nothing
    encoded = Zarr.pipeline_encode(writer.inner_pipeline, chunk_data, writer.fill_value)
    if encoded === nothing || isempty(encoded)
        return nothing
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

"""
The virtual address reservation size used for writable [`MmapZipStore`](@ref
DataAxesFormats.MmapZipStores.MmapZipStore) opens of a [`ZarrDaf`](@ref) (modes `r+`, `w+`, `w`).
Each such open reserves this much virtual address space via a single anonymous `PROT_NONE` mapping
and overlays the real file onto its first `filesize` bytes; subsequent `ftruncate` + re-overlay
calls extend the accessible portion as the archive grows. The physical file stays at its real size
— only VA is reserved. Defaults to 128 GiB, leaving plenty of room for concurrent live stores on
platforms with ~128 TiB of user VA (Apple Silicon). Set to a larger value before opening a
`ZarrDaf` whose ZIP archive might grow past this bound. An append that would cross the bound fails
with an explicit error pointing back here.
"""
DAF_ZARR_ZIP_MAX_FILE_SIZE::Int = 1 << 37

"""
    ZarrDaf(
        path::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing,
        packed::Bool = false]
    )

Storage in a Zarr directory tree, Zarr ZIP archive, or remote HTTP(S) Zarr group.

The `path` is a filesystem path that follows one of these conventions:

  - `something.daf.zarr` — a Zarr directory containing a single `Daf` data set at its root.
  - `something.daf.zarr.zip` — a Zarr ZIP archive containing a single `Daf` data set at its root.
  - `something.dafs.zarr.zip#/group` — a Zarr ZIP archive containing `Daf` data sets in sub-groups, addressed by
    `group`.
  - `http://…` or `https://…` — a URL pointing at a remote Zarr directory that contains a `Daf` data set, served over
    HTTP (e.g. via a static file server, `HTTP.serve(store, path, …)`, or `xpublish`). Only `mode = "r"` is supported;
    the HTTP backend is strictly read-only and returns a [`DafReadOnly`](@ref). The remote directory **must** contain
    a consolidated `.zmetadata` file, and the served content **must** be stable for the lifetime of the open handle:
    per-chunk GETs happen lazily, so if the underlying data set is rewritten or relocated while the handle is open,
    subsequent reads may see inconsistent bytes.

The backend (directory, ZIP, or HTTP) is selected from the path prefix / file-name suffix. The ZIP backend is
append-only: properties cannot be deleted and axes cannot be reordered (attempts to do so raise an error).

!!! note

    If you create a directory whose name is `something.dafs.zarr.zip#` and place `Daf` ZIP archives in it, this scheme
    will fail. So don't.

When opening an existing data set, if `name` is not specified, and there exists a "name" scalar property, it is used as
the name. Otherwise, the `path` (including any `#/group` suffix) will be used as the name.

If `packed` is `true`, subsequent writes through this handle default to the packed (chunked + compressed) on-disk
encoding for properties whose uncompressed size is at or above
[`DAF_PACKED_TARGET_CHUNK_KB`](@ref DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB). Per-call `packed` kwargs
on `set_*!` / `empty_*!` / `copy_*!` override this default. The default is `false` (flat single-chunk uncompressed
encoding). HTTP-mode opens are read-only and ignore the `packed` kwarg.

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type         |
|:---- |:-------------------- |:------------------------- |:------------------- |:--------------------- |
| `r`  | No                   | No                        | No                  | [`DafReadOnly`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`ZarrDaf`](@ref)     |
| `w+` | Yes                  | Yes                       | No                  | [`ZarrDaf`](@ref)     |
| `w`  | Yes                  | Yes                       | Yes                 | [`ZarrDaf`](@ref)     |

Truncating a sub-daf inside a ZIP archive is not supported (because the ZIP backend is append-only) and raises an
error; use `r+` or `w+` to open a sub-daf for writing without truncation.

!!! note

    When several [`ZarrDaf`](@ref) instances in the same process share a ZIP archive path (typically different
    `#/group` sub-dafs of the same `.dafs.zarr.zip` file, or repeated opens of the same single-daf `.daf.zarr.zip`),
    they share a single underlying [`MmapZipStore`](@ref DataAxesFormats.MmapZipStores.MmapZipStore) and a single
    `data_lock`, so that concurrent calls serialize correctly and the archive is never mmap-ed twice. The first such
    open determines the store's writability: a later open of the same archive that requests write access will raise
    an error if the first open was read-only. Release the read-only handle first, or open the writable instance first.
    The directory backend does not share a store — each open creates its own independent `DirectoryStore` over the
    same filesystem tree.

!!! note

    `.daf.zarr.zip` archives written by this code do not contain the `.zmetadata` consolidated metadata sidecar,
    because the ZIP central directory plays the same enumeration role. Consequently, an
    `unzip foo.daf.zarr.zip -d foo.daf.zarr/` produces a directory that lacks `.zmetadata`. Before exposing such a
    directory over HTTP, open it once locally with `ZarrDaf("foo.daf.zarr")` (any mode) so
    `ensure_consolidated_metadata!` builds the sidecar.
"""
struct ZarrDaf <: DafWriter
    name::AbstractString
    internal::Internal
    root::ZGroup
    mode::AbstractString
    path::AbstractString
end

function ZarrDaf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
    packed::Bool = false,
)::Union{ZarrDaf, DafReadOnly}
    if startswith(path, "http://") || startswith(path, "https://")
        return open_http_zarr_daf(path, mode; name)
    end
    (is_read_only, create_if_missing, truncate_if_exists) = Formats.parse_mode(mode)
    (container_path, group_path, is_zip) = parse_zarr_path(path)
    full_container_path = abspath(container_path)
    full_path = group_path === nothing ? full_container_path : full_container_path * "#/" * group_path

    shared_handle::Maybe{SharedMmapZipStoreHandle} = nothing
    if is_zip
        if truncate_if_exists && group_path !== nothing
            error(
                "can't truncate a sub-daf inside a zip-backed ZarrDaf; " *
                "the ZIP backend is append-only: $(full_path)",
            )
        end
        if !isfile(full_container_path) && !create_if_missing
            error("no such file: $(full_container_path)")
        end
        purge = truncate_if_exists && group_path === nothing

        shared_handle = acquire_shared_mmap_zip_store!(;
            container_path = full_container_path,
            is_read_only = is_read_only,
            create_if_missing = create_if_missing,
            truncate = purge,
            max_file_size = DAF_ZARR_ZIP_MAX_FILE_SIZE,
        )
        store = shared_handle.store
    else
        @assert group_path === nothing
        if !isdir(full_container_path)
            if !create_if_missing
                error("no such directory: $(full_container_path)")
            end
        elseif truncate_if_exists
            rm(full_container_path; recursive = true)  # NOJET
        end
        store = Zarr.DirectoryStore(full_container_path)
    end

    zpath = group_path === nothing ? "" : String(group_path)
    root = open_or_create_daf_group(store, zpath, full_path, is_read_only, create_if_missing)

    if name === nothing && haskey(root.groups, SCALARS)
        scalars_group = root.groups[SCALARS]
        if haskey(scalars_group.arrays, "name")
            name = string(read_scalar_value(scalars_group.arrays["name"]))
        end
    end

    if name === nothing
        name = full_path
    end
    name = unique_name(name)

    internal = if shared_handle === nothing
        Internal(; is_frozen = is_read_only, packed_default = packed)
    else
        Internal(;
            is_frozen = is_read_only,
            data_lock = shared_handle.data_lock,
            packed_default = packed,
            shared_resource = shared_handle,
        )
    end
    daf = ZarrDaf(name, internal, root, mode, full_path)
    ensure_consolidated_metadata!(daf)
    @debug "Daf: $(brief(daf)) root: $(root)" _group = :daf_repos
    if is_read_only
        return read_only(daf)
    else
        return daf
    end
end

function open_http_zarr_daf(url::AbstractString, mode::AbstractString; name::Maybe{AbstractString})::DafReadOnly
    if mode != "r"
        error("can't open an http(s)://... ZarrDaf in mode: $(mode); the HTTP backend is read-only: $(url)")
    end
    root = try
        Zarr.zopen(String(url))  # NOJET
    catch exception
        error(
            "failed to open remote zarr group: $(url)\n" *
            "the remote directory must contain a consolidated `.zmetadata` file\n" *
            "underlying error: $(exception)",
        )
    end
    if !(root isa ZGroup)
        error("not a zarr group: $(url)")  # UNTESTED
    end
    if !haskey(zarr_group_attrs(root), DAF_KEY)
        error("not a daf data set: $(url)")  # UNTESTED
    end
    verify_daf(root, url)
    if name === nothing && haskey(root.groups, SCALARS)
        scalars_sub = root.groups[SCALARS]
        if haskey(scalars_sub.arrays, "name")
            name = string(read_scalar_value(scalars_sub.arrays["name"]))
        end
    end
    if name === nothing
        name = String(url)  # UNTESTED
    end
    name = unique_name(name)
    daf = ZarrDaf(name, Internal(; is_frozen = true), root, "r", String(url))
    @debug "Daf: $(brief(daf)) root: $(root)" _group = :daf_repos
    return read_only(daf)
end

"""
    ZarrDaf(; [name::Maybe{AbstractString} = nothing, packed::Bool = false])::ZarrDaf

In-memory [`ZarrDaf`](@ref) backed by a fresh `Zarr.DictStore`. The data lives in process memory as a
dictionary of chunk byte buffers, with no filesystem path. Zero-copy reads are served via
`unsafe_wrap` over the stored `Vector{UInt8}` chunks, so typed array accesses alias the dict's
buffers without additional allocation. Always writable; wrap in `read_only(daf)` if read-only access
is required.

Prefer [`MemoryDaf`](@ref DataAxesFormats.MemoryFormat.MemoryDaf) for the common "scratch data set in
RAM" case: it stores typed arrays directly, so references returned by `get_vector`/`get_matrix`
remain valid under any subsequent mutation. Reach for this in-memory `ZarrDaf` only when downstream
code specifically requires a Zarr group (e.g. handing the `root` to a non-`Daf`-aware Zarr
consumer), or for building a data set in memory before dumping it to a `.daf.zarr` directory or
`.daf.zarr.zip` archive without re-encoding.

!!! warning

    Zero-copy views from this backend are **not** retained across overwrites. A view obtained from
    `get_vector(daf, axis, name)` (or `get_matrix`) aliases the `Vector{UInt8}` chunk held by the
    backing `Zarr.DictStore`. A subsequent `set_vector!(daf, axis, name, ...; overwrite = true)` (or
    `delete_vector!`, similarly for matrices) calls Zarr's write path, which replaces the dict entry
    with a fresh `Vector{UInt8}`; the old buffer loses its last strong reference (the daf's cache is
    invalidated on overwrite) and becomes eligible for GC, so the earlier view may dangle. Do not
    hold `get_*` results across writes that touch the same property. `MemoryDaf` does not have this
    hazard because its storage *is* the typed array the caller already holds.
"""
function ZarrDaf(; name::Maybe{AbstractString} = nothing, packed::Bool = false)::ZarrDaf
    store = Zarr.DictStore()
    root = create_fresh_daf_group(store, "")
    if name === nothing
        name = "memory"  # UNTESTED
    end
    name = unique_name(name)
    daf = ZarrDaf(name, Internal(; is_frozen = false, packed_default = packed), root, "w+", "<memory>")
    @debug "Daf: $(brief(daf)) root: $(root)" _group = :daf_repos
    return daf
end

# Parse the user-facing `path` into `(container_path, group_path, is_zip)`. Three valid forms:
#   foo.daf.zarr              → (foo.daf.zarr,      nothing, false)  # directory
#   foo.daf.zarr.zip          → (foo.daf.zarr.zip,  nothing, true)   # singular ZIP, no group
#   foo.dafs.zarr.zip#/group  → (foo.dafs.zarr.zip, "group", true)   # plural ZIP, group required
# The two ZIP forms are recognized by `parse_zip_archive_path`, which also rejects the
# cardinality near-misses with explicit errors: a `#/group` fragment on a `.daf.zarr.zip`
# path, and a bare `.dafs.zarr.zip` path with no `#/group`. Only the directory form
# `.daf.zarr` is local to ZarrDaf. Anything else is a hard error.
function parse_zarr_path(path::AbstractString)::Tuple{String, Maybe{String}, Bool}
    zip_match = parse_zip_archive_path(
        path;
        single_daf_suffix = ".daf.zarr.zip",
        multi_dafs_suffix = ".dafs.zarr.zip",
        multi_dafs_marker = ".dafs.zarr.zip#/",
        format_name = "ZarrDaf",
    )
    if zip_match !== nothing
        (container_path, group_path) = zip_match
        return (container_path, group_path, true)
    end
    if endswith(path, ".daf.zarr")
        return (String(path), nothing, false)
    end
    return error(
        "can't parse as ZarrDaf path: $(path)\n" *
        "expected one of: <stem>.daf.zarr, <stem>.daf.zarr.zip, <stem>.dafs.zarr.zip#/<group>",
    )
end

# Return `group`'s on-disk attributes. For most stores this is just `group.attrs`, but `Zarr.ConsolidatedStore`
# in Zarr.jl 0.10 populates `group.attrs` from the v2 `.zattrs` key (which never appears in v3 stores), so for
# that store we extract the attributes directly from the consolidated `zarr.json` entry. Drops the second
# branch when Zarr.jl ships a v3-correct `getattrs(::ConsolidatedStore)`.
function zarr_group_attrs(group::ZGroup)::AbstractDict
    storage = group.storage
    if storage isa Zarr.ConsolidatedStore
        json_key = Zarr._unconcpath(storage, group.path, "zarr.json")
        if !haskey(storage.cons, json_key)
            return Dict{String, Any}()
        end
        return get(storage.cons[json_key], "attributes", Dict{String, Any}())
    end
    return group.attrs
end

function open_or_create_daf_group(
    store::Zarr.AbstractStore,
    zpath::AbstractString,
    full_path::AbstractString,
    is_read_only::Bool,
    create_if_missing::Bool,
)::ZGroup
    if Zarr.is_zgroup(Zarr.ZarrFormat(3), store, zpath)  # NOJET
        root =
            Zarr.zopen_noerr(store, is_read_only ? "r" : "w", Zarr.ZarrFormat(3); path = zpath, fill_as_missing = false)  # NOJET
        if !(root isa ZGroup)
            error("not a daf zarr group: $(full_path)")  # UNTESTED
        end
        if haskey(zarr_group_attrs(root), DAF_KEY)
            verify_daf(root, full_path)
        elseif create_if_missing
            create_daf(root)
        else
            error("not a daf data set: $(full_path)")
        end
        return root
    end
    if Zarr.is_zgroup(Zarr.ZarrFormat(2), store, zpath)  # NOJET
        error(chomp("""
                    Zarr v2 store at: $(full_path)
                    DataAxesFormats requires a Zarr v3 store.
                    Convert via `python -m zarr v2_to_v3 <path>` (zarr-python 3.1.2+),
                    then reopen.
                    """))
    end
    if !create_if_missing
        error("not a zarr group: $(full_path)")
    end
    root = create_fresh_daf_group(store, String(zpath))
    return root
end

# Build a fresh `Daf` root group: a v3 Zarr group at `path` in `store` whose `daf` attribute marks it as
# Daf-formatted, plus the four required sub-groups. The `daf` attribute is set at group-creation time so
# the entire root metadata lands in a single store write — required for append-only stores like
# `MmapZipStore`, which reject overwrites of the just-written `zarr.json`.
function create_fresh_daf_group(store::Zarr.AbstractStore, path::AbstractString)::ZGroup
    attrs = Dict{String, Any}(DAF_KEY => [Int(MAJOR_VERSION), Int(MINOR_VERSION)])
    root = zgroup(store, String(path), Zarr.ZarrFormat(3); attrs)  # NOJET
    zgroup(root, SCALARS)
    zgroup(root, AXES)
    zgroup(root, VECTORS)
    zgroup(root, MATRICES)
    return root
end

# Adopt an existing v3 group at `root` as a `Daf` root by adding the `daf` attribute and the four required
# sub-groups. Used when the path already contains a non-Daf v3 group and the caller asked for create-if-missing.
# Writes the attribute via `Zarr.writeattrs`, which read-modify-writes `zarr.json`; this path is unsupported on
# append-only stores (e.g. `MmapZipStore`) and the underlying setindex! will throw a clear error there.
function create_daf(root::ZGroup)::Nothing
    root.attrs[DAF_KEY] = [Int(MAJOR_VERSION), Int(MINOR_VERSION)]
    Zarr.writeattrs(root.zarr_format, root.storage, root.path, root.attrs)

    zgroup(root, SCALARS)
    zgroup(root, AXES)
    zgroup(root, VECTORS)
    zgroup(root, MATRICES)

    return nothing
end

function verify_daf(root::ZGroup, full_path::AbstractString)::Nothing
    version = zarr_group_attrs(root)[DAF_KEY]
    if !(version isa AbstractVector) || length(version) != 2
        error(chomp("""  # UNTESTED
                    malformed daf version marker: $(version)
                    for the daf zarr group: $(full_path)
                    expected: [major, minor]
                    """))
    end
    major = Int(version[1])
    minor = Int(version[2])
    if major != MAJOR_VERSION || minor > MINOR_VERSION
        error(chomp("""
                    incompatible format version: $(major).$(minor)
                    for the daf zarr group: $(full_path)
                    the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
                    """))
    end
    return nothing
end

function Readers.is_leaf(::ZarrDaf)::Bool
    return true
end

function Readers.is_leaf(::Type{ZarrDaf})::Bool  # FLAKY TESTED
    return true
end

function Readers.complete_path(daf::ZarrDaf)::Maybe{AbstractString}
    return daf.path
end

function Formats.format_description_header(daf::ZarrDaf, indent::AbstractString, lines::Vector{String}, ::Bool)::Nothing
    @assert Formats.has_data_read_lock(daf)
    push!(lines, "$(indent)type: ZarrDaf")
    push!(lines, "$(indent)path: $(daf.path)")
    push!(lines, "$(indent)mode: $(daf.mode)")
    return nothing
end

function scalars_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[SCALARS]
end

function axes_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[AXES]
end

function vectors_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[VECTORS]
end

function matrices_group(daf::ZarrDaf)::ZGroup  # FLAKY TESTED
    return daf.root.groups[MATRICES]
end

function is_writable(daf::ZarrDaf)::Bool
    return daf.mode != "r"
end

function chunk_key(array::ZArray, suffix::AbstractString)::String  # FLAKY TESTED
    array_key = lstrip(array.path, '/')
    return isempty(array_key) ? String(suffix) : array_key * '/' * String(suffix)
end

# Single-chunk-uncompressed chunk filename for a 1D array. Used by the flat (single-chunk) read fast path.
function single_chunk_vector_suffix(array::ZArray)::String
    return Zarr.citostring(array.metadata.chunk_key_encoding, CartesianIndex(1))
end

# Single-chunk-uncompressed chunk filename for a 2D array. Used by the flat (single-chunk) read fast path and the
# post-fill CRC patch on the ZIP backend.
function single_chunk_matrix_suffix(array::ZArray)::String
    return Zarr.citostring(array.metadata.chunk_key_encoding, CartesianIndex(1, 1))
end

# Create a Zarr array at `name` under `group` for a dense property of element type `T` and shape `shape`. When
# `chunks_for` returns `nothing` (no packing) or for non-bits types (e.g. `String`), falls back to a single-chunk
# uncompressed encoding and lets `Zarr.jl` apply its default per-type filter (e.g. `VLenUTF8Filter` for strings).
# Otherwise applies the codec resolved from `DAF_PACKED_COMPRESSION` / `DAF_PACKED_COMPRESSION_LEVEL` and the
# `chunks_for` chunk shape; the explicit `filters` kwarg is omitted for non-bits types so `Zarr.jl`'s default filter
# stays in the chain ahead of the compressor. Callers fill data into the returned `ZArray` afterward.
function dense_zcreate(
    ::Type{String},
    group::ZGroup,
    name::AbstractString,
    packed::Bool,
    shape::NTuple{N, Int},
)::ZArray{String, N} where {N}
    chunks = chunks_for(packed, shape, String)
    if chunks === nothing
        return string_zcreate(group, name, shape)
    else
        return string_zcreate(group, name, shape, chunks, v3_bytes_codecs_for(compressor_for(), String))
    end
end

# Translate a `PackedCodec` descriptor to the bytes→bytes codec tuple of a v3 codec pipeline. Used by the
# chunked-compressed write paths in `dense_zcreate` (numeric and string arms). For `String`, `typesize` is `1`
# because `sizeof(String) == 0` is not meaningful for blosc bitshuffle. `:gzip_shuffle` and `:zstd_bitshuffle`
# are rejected because Zarr.jl 0.10's v3 codec registry has no standalone shuffle or bitshuffle codec.
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
            "packed compression codec :gzip_shuffle is not supported on the ZarrDaf backend " *
            "(Zarr.jl has no v3 standalone shuffle codec); use :gzip or :blosc_zstd_bitshuffle",
        )
    else
        @assert compression == :zstd_bitshuffle
        return error(
            "packed compression codec :zstd_bitshuffle is not supported on the ZarrDaf backend " *
            "(Zarr.jl has no v3 bitshuffle codec); use :blosc_zstd_bitshuffle (Blosc bundles its own bitshuffle)",
        )
    end
end

function dense_zcreate(
    ::Type{T},
    group::ZGroup,
    name::AbstractString,
    packed::Bool,
    shape::NTuple{N, Int},
)::ZArray{T, N} where {T, N}
    inner_chunks = chunks_for(packed, shape, T)
    if inner_chunks === nothing
        return numeric_zcreate(T, group, name, shape)
    else
        return sharded_zcreate(T, group, name, shape, inner_chunks, v3_bytes_codecs_for(compressor_for(), T))
    end
end

function patch_chunk_crc_if_needed(array::ZArray, chunk_suffix::AbstractString)::Nothing
    storage = array.storage
    if storage isa MmapZipStore
        key = chunk_key(array, chunk_suffix)
        if haskey(storage.name_to_index, key)
            patch_mmap_zip_entry_crc!(storage, key)
        end
    end
    return nothing
end

# Rewrite the consolidated `.zmetadata` file for `daf`'s root group so live readers (including the
# `ConsolidatedStore` wrapper used for HTTP access) observe the newly-committed state. The rewrite
# is atomic: the serialized JSON is printed to a neighboring `.zmetadata.new` staging file which is
# then replaced over `.zmetadata` via `rename(2)`. `JSON.print` throws on any write failure and
# `Base.Filesystem.rename` throws an `IOError` on any rename failure, so any problem here propagates
# as a hard error to the caller. Currently only implemented for [`Zarr.DirectoryStore`](@extref) —
# other backends (in-memory `DictStore`, [`MmapZipStore`](@ref)) are a no-op and rely on separate
# backend-specific mechanisms for visibility.
function refresh_consolidated_metadata!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    prefix = daf.root.path
    consolidated = Dict{String, Any}()
    consolidate_v3_metadata!(consolidated, storage, prefix)
    group_directory = joinpath(storage.folder, lstrip(prefix, '/'))
    target_path = joinpath(group_directory, ".zmetadata")
    staging_path = target_path * ".new"
    open(staging_path, "w") do io
        return JSON.print(io, Dict("metadata" => consolidated, "zarr_consolidated_format" => 1), 4)
    end
    Base.Filesystem.rename(staging_path, target_path)
    return nothing
end

# Walk every `zarr.json` under `prefix` in `storage` and accumulate them into `consolidated`, keyed by their store-relative
# path. The result is the metadata dict that `Zarr.ConsolidatedStore` consumes when opening a store with
# `consolidated = true`. Mirrors what `Zarr.consolidate_metadata` does for v2 stores; Zarr.jl 0.10's `consolidate_metadata`
# only walks v2 metadata files so we walk v3 ones ourselves.
function consolidate_v3_metadata!(
    consolidated::Dict{String, Any},
    storage::Zarr.AbstractStore,
    prefix::AbstractString,
)::Nothing
    raw = storage[prefix, "zarr.json"]
    if raw !== nothing
        key = isempty(prefix) ? "zarr.json" : prefix * "/zarr.json"
        consolidated[lstrip(key, '/')] = JSON.parse(String(copy(raw)); dicttype = Dict{String, Any})
    end
    foreach(Zarr.subdirs(storage, prefix)) do subname
        sub_prefix = isempty(prefix) ? subname : prefix * "/" * subname
        consolidate_v3_metadata!(consolidated, storage, sub_prefix)
    end
    return nothing
end

# Lazy bootstrap of `.zmetadata` on every `ZarrDaf` open, mirroring `FilesFormat.ensure_metadata_zip!`
# (`src/files_format.jl:1300`). Only the `DirectoryStore` backend has a `.zmetadata` sidecar to
# rebuild — the ZIP backend's central directory plays the same role, and `DictStore` lives in
# memory — so this is a no-op for every other backend. If the sidecar already exists the
# function is also a no-op (the assumption is that all writes go through `ZarrDaf`, which keeps
# `.zmetadata` in sync via `refresh_consolidated_metadata!`). On a read-only filesystem the
# rebuild attempt fails and is silently swallowed when `daf.mode == "r"`, so opening a frozen
# directory still succeeds — HTTP serving from such a directory then requires one prior writable
# open to seed the sidecar.
function ensure_consolidated_metadata!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    group_directory = joinpath(storage.folder, lstrip(daf.root.path, '/'))
    target_path = joinpath(group_directory, ".zmetadata")
    if isfile(target_path)
        return nothing
    end
    try
        refresh_consolidated_metadata!(daf)
    catch  # FLAKY TESTED
        if daf.mode == "r"  # UNTESTED
            return nothing  # UNTESTED
        end
        rethrow()  # UNTESTED
    end
    return nothing
end

function try_mmap_vector_chunk(daf::ZarrDaf, array::ZArray{T})::Maybe{AbstractVector{T}} where {T}  # FLAKY TESTED
    storage = array.storage
    key = chunk_key(array, single_chunk_vector_suffix(array))
    if storage isa Zarr.DirectoryStore
        chunk_path = joinpath(storage.folder, key)
        if !isfile(chunk_path)
            return nothing  # UNTESTED
        end
        return open(chunk_path, is_writable(daf) ? "r+" : "r") do io
            return Mmap.mmap(io, Vector{T}, (length(array),))
        end
    end
    if storage isa MmapZipStore
        return try_mmap_entry_as(storage, key, T, length(array))
    end
    if storage isa Zarr.DictStore
        chunk_bytes = get(storage.a, key, nothing)
        if chunk_bytes === nothing
            return nothing  # UNTESTED
        end
        return unsafe_wrap(Array, Ptr{T}(pointer(chunk_bytes)), length(array); own = false)
    end
    return nothing  # UNTESTED
end

function try_mmap_matrix_chunk(daf::ZarrDaf, array::ZArray{T})::Maybe{AbstractMatrix{T}} where {T}  # FLAKY TESTED
    storage = array.storage
    key = chunk_key(array, single_chunk_matrix_suffix(array))
    if storage isa Zarr.DirectoryStore
        chunk_path = joinpath(storage.folder, key)
        if !isfile(chunk_path)
            return nothing  # UNTESTED
        end
        return open(chunk_path, is_writable(daf) ? "r+" : "r") do io
            return Mmap.mmap(io, Matrix{T}, size(array))
        end
    end
    if storage isa MmapZipStore
        return try_mmap_entry_as(storage, key, T, size(array))
    end
    if storage isa Zarr.DictStore
        chunk_bytes = get(storage.a, key, nothing)
        if chunk_bytes === nothing
            return nothing  # UNTESTED
        end
        return unsafe_wrap(Array, Ptr{T}(pointer(chunk_bytes)), size(array); own = false)
    end
    return nothing  # UNTESTED
end

# `Zarr.ZArray` doesn't expose `strides` (chunked storage), so the default `MatrixLayouts.major_axis(::AbstractMatrix)`
# fallback returns `nothing`. ZarrDaf writes matrices in C order with reversed `shape` (see the module docstring),
# which is column-major in Julia's view; declare it so that the layout-forwarding chain through
# `MatrixLayouts.major_axis(::DiskArrays.CachedDiskArray)` (defined in `TanayLabUtilities`) resolves correctly for the
# packed read path.
function TanayLabUtilities.MatrixLayouts.major_axis(::ZArray{T, 2})::Maybe{Int8} where {T}
    return Columns
end

# Materialise a 1-D Zarr array as a concrete `Vector{T}`, used by the sparse-component reader where the consumer
# (`SparseMatrixCSC`) requires a concrete `Vector` rather than an `AbstractVector`. For flat (single-chunk
# uncompressed) arrays this returns the zero-copy mmap view (also a `Vector{T}`); for chunked compressed arrays it
# materialises eagerly via `array[:]`.
function array_as_materialized_vector(daf::ZarrDaf, array::ZArray{T})::Tuple{Vector{T}, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        vector = try_mmap_vector_chunk(daf, array)
        if vector !== nothing
            return (vector, Formats.MappedData)
        end
    end
    return (Vector{T}(array[:]), Formats.MemoryData)
end

function array_as_vector(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageVector, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        vector = try_mmap_vector_chunk(daf, array)
        if vector !== nothing
            return (vector, Formats.MappedData)
        end
    end
    return (DiskArrays.cache(array; maxsize = packed_local_cache_mb()), Formats.MemoryData)
end

function array_as_matrix(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageMatrix, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        matrix = try_mmap_matrix_chunk(daf, array)
        if matrix !== nothing
            return (matrix, Formats.MappedData)
        end
    end
    return (DiskArrays.cache(array; maxsize = packed_local_cache_mb()), Formats.MemoryData)
end

function can_mmap(array::ZArray{T})::Bool where {T}  # FLAKY TESTED
    if !isbitstype(T)
        return false
    end
    if array.metadata.chunks != size(array)
        return false
    end
    if !(array.storage isa Zarr.DirectoryStore || array.storage isa MmapZipStore || array.storage isa Zarr.DictStore)
        return false
    end
    pipeline = array.metadata.pipeline
    return isempty(pipeline.array_array) &&
           pipeline.array_bytes isa Zarr.Codecs.V3Codecs.BytesCodec &&
           isempty(pipeline.bytes_bytes)
end

function Formats.format_has_scalar(daf::ZarrDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(daf)
    return haskey(scalars_group(daf).arrays, name)
end

function read_scalar_value(array::ZArray{T})::StorageScalar where {T}
    return array[1]
end

function Formats.format_get_scalar(daf::ZarrDaf, name::AbstractString)::Tuple{StorageScalar, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    array = scalars_group(daf).arrays[name]
    return (read_scalar_value(array), Formats.MemoryData)
end

function Formats.format_set_scalar!(daf::ZarrDaf, name::AbstractString, value::StorageScalar)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(daf)
    array = dense_zcreate(typeof(value), scalars_group(daf), name, false, (1,))
    array[1] = value  # NOJET
    refresh_consolidated_metadata!(daf)
    return Formats.MemoryData
end

function Formats.format_delete_scalar!(daf::ZarrDaf, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(daf)
    delete_child(scalars_group(daf), name)
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_scalars_set(daf::ZarrDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    return Set(keys(scalars_group(daf).arrays))
end

function Formats.format_has_axis(daf::ZarrDaf, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(daf)
    return haskey(axes_group(daf).arrays, axis)
end

function Formats.format_add_axis!(
    daf::ZarrDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    axis_array = string_zcreate(axes_group(daf), axis, (length(entries),))
    axis_array[:] = String.(entries)  # NOJET

    zgroup(vectors_group(daf), axis)

    axes = keys(axes_group(daf).arrays)
    @assert axis in axes

    axis_matrices = zgroup(matrices_group(daf), axis)
    for other_axis in axes
        if other_axis != axis
            zgroup(axis_matrices, other_axis)
        end
    end

    for other_axis in axes
        zgroup(matrices_group(daf).groups[other_axis], axis)
    end

    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_delete_axis!(daf::ZarrDaf, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(daf)
    delete_child(axes_group(daf), axis)
    delete_child(vectors_group(daf), axis)
    delete_child(matrices_group(daf), axis)

    for (_, other_group) in matrices_group(daf).groups
        if haskey(other_group.groups, axis)
            delete_child(other_group, axis)
        end
    end

    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_axes_set(daf::ZarrDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    return Set(keys(axes_group(daf).arrays))
end

function Formats.format_axis_vector(
    daf::ZarrDaf,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Maybe{Formats.CacheGroup}}
    @assert Formats.has_data_read_lock(daf)
    return (axes_group(daf).arrays[axis][:], Formats.MemoryData)
end

function Formats.format_axis_length(daf::ZarrDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(daf)
    return length(axes_group(daf).arrays[axis])
end

function axis_vectors_group(daf::ZarrDaf, axis::AbstractString)::ZGroup
    return vectors_group(daf).groups[axis]
end

function Formats.format_has_vector(daf::ZarrDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    return haskey(group.arrays, name) || haskey(group.groups, name)
end

function Formats.format_set_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
    packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)

    if vector isa StorageReal
        array = dense_zcreate(typeof(vector), group, name, packed, (nelements,))
        array[:] = fill(vector, nelements)  # NOJET
    elseif vector isa AbstractString
        array = dense_zcreate(String, group, name, packed, (nelements,))
        array[:] = fill(String(vector), nelements)  # NOJET
    else
        @assert vector isa AbstractVector
        vector = base_array(vector)
        if issparse(vector)
            write_sparse_vector(group, name, vector, packed)
        elseif eltype(vector) <: AbstractString
            array = dense_zcreate(String, group, name, packed, (nelements,))
            array[:] = String.(vector)  # NOJET
        else
            array = dense_zcreate(eltype(vector), group, name, packed, (nelements,))
            array[:] = Vector(vector)  # NOJET
        end
    end
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)

    array = dense_zcreate(T, group, name, packed, (nelements,))
    if can_mmap(array)
        # Flat single-chunk: poke the last element with a non-fill-value (the array's fill_value is `zero(T)`) so
        # Zarr's pipeline does not elide the write and the chunk file actually materialises, then return the
        # mmap-backed view for in-place fill.
        array[nelements] = oneunit(T)
        return array_as_vector(daf, array)
    else
        # Packed (sharded): hand the user the ZArray directly. Fill writes go through the sharded pipeline as a
        # single shard write at finalize time; no intermediate buffer is allocated.
        return (array, nothing)
    end
end

function Formats.format_filled_empty_dense_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    filled::AbstractVector{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    array = axis_vectors_group(daf, axis).arrays[name]
    if can_mmap(array)
        # Flat: data was filled in place via the mmap view; patch the CRC on the ZIP backend.
        patch_chunk_crc_if_needed(array, single_chunk_vector_suffix(array))
    end
    # Packed: the data was written into the ZArray directly (no intermediate buffer). Nothing more to do.
    refresh_consolidated_metadata!(daf)
    return nothing
end

function write_sparse_vector(parent::ZGroup, name::AbstractString, vector::AbstractVector, packed::Bool)::Nothing
    vector_group = zgroup(parent, name)

    nzind_vector = nzind(vector)
    nzind_array =
        dense_zcreate(eltype(nzind_vector), vector_group, "nzind", false, (length(nzind_vector),))
    nzind_array[:] = nzind_vector

    if eltype(vector) != Bool || !all(nzval(vector))
        nzval_vector = nzval(vector)
        nzval_array = dense_zcreate(eltype(nzval_vector), vector_group, "nzval", packed, (length(nzval_vector),))
        nzval_array[:] = nzval_vector  # NOJET
    end
    return nothing
end

function Formats.format_get_empty_sparse_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
    _packed::Bool,
)::Tuple{AbstractVector{I}, AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    vector_group = zgroup(group, name)

    nnz_int = Int(nnz)
    nzind_array = dense_zcreate(I, vector_group, "nzind", false, (nnz_int,))
    nzval_array = dense_zcreate(T, vector_group, "nzval", false, (nnz_int,))
    if nnz_int > 0
        # Poke the last element with a non-fill-value so the chunk file is materialised (Zarr's pipeline elides
        # writes when every cell equals `fill_value = zero(T)`); the user fills the mmap view in place.
        nzind_array[nnz_int] = oneunit(I)
        nzval_array[nnz_int] = oneunit(T)
    end

    nzind_vec, _ = array_as_vector(daf, nzind_array)
    nzval_vec, _ = array_as_vector(daf, nzval_array)
    return (nzind_vec, nzval_vec, Formats.MappedData)
end

function Formats.format_filled_empty_sparse_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    vector_group = axis_vectors_group(daf, axis).groups[name]
    nzind_array = vector_group.arrays["nzind"]
    patch_chunk_crc_if_needed(nzind_array, single_chunk_vector_suffix(nzind_array))
    if haskey(vector_group.arrays, "nzval")
        nzval_array = vector_group.arrays["nzval"]
        patch_chunk_crc_if_needed(nzval_array, single_chunk_vector_suffix(nzval_array))
    end
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_delete_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    delete_child(axis_vectors_group(daf, axis), name)
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_vectors_set(daf::ZarrDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    return union(Set(keys(group.arrays)), Set(keys(group.groups)))
end

function Formats.format_get_vector(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    if haskey(group.arrays, name)
        vector, cache_group = array_as_vector(daf, group.arrays[name])
        return (vector, nothing, cache_group)
    end

    @assert haskey(group.groups, name)
    vector_group = group.groups[name]
    nelements = Formats.format_axis_length(daf, axis)

    nzind_vector, nzind_cache_group = array_as_materialized_vector(daf, vector_group.arrays["nzind"])
    if haskey(vector_group.arrays, "nzval")
        nzval_vector, nzval_cache_group = array_as_materialized_vector(daf, vector_group.arrays["nzval"])
    else
        nzval_vector = fill(true, length(nzind_vector))
        nzval_cache_group = Formats.MemoryData
    end

    vector = SparseVector(nelements, nzind_vector, nzval_vector)
    cache_group = if nzind_cache_group == Formats.MappedData && nzval_cache_group == Formats.MappedData
        Formats.MappedData
    else
        Formats.MemoryData
    end
    return (vector, nothing, cache_group)
end

function columns_axis_group(daf::ZarrDaf, rows_axis::AbstractString, columns_axis::AbstractString)::ZGroup
    return matrices_group(daf).groups[rows_axis].groups[columns_axis]
end

function Formats.format_has_matrix(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    return haskey(group.arrays, name) || haskey(group.groups, name)
end

function Formats.format_set_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalarBase, StorageMatrix},
    packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    if matrix isa StorageReal
        array = dense_zcreate(typeof(matrix), group, name, packed, (nrows, ncols))
        array[:, :] = fill(matrix, nrows, ncols)  # NOJET
    elseif matrix isa AbstractString
        array = dense_zcreate(String, group, name, false, (nrows, ncols))
        array[:, :] = fill(String(matrix), nrows, ncols)  # NOJET
    elseif eltype(matrix) <: AbstractString
        array = dense_zcreate(String, group, name, false, (nrows, ncols))
        array[:, :] = String.(matrix)  # NOJET
    else
        @assert matrix isa AbstractMatrix
        @assert major_axis(matrix) != Rows
        matrix = base_array(matrix)
        if issparse(matrix)
            write_sparse_matrix(group, name, matrix, packed)
            refresh_consolidated_metadata!(daf)
            return nothing
        else
            array = dense_zcreate(eltype(matrix), group, name, packed, (nrows, ncols))
            array[:, :] = Matrix(matrix)  # NOJET
        end
    end
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    if packed && chunks_for(packed, (nrows, ncols), T) !== nothing
        return packed_streaming_dense_matrix(group, name, T, nrows, ncols)
    end

    array = numeric_zcreate(T, group, name, (nrows, ncols))
    array[nrows, ncols] = oneunit(T)
    return array_as_matrix(daf, array)
end

# Build the streaming wrapper for a packed dense matrix. The inner-chunk shape is `(n_rows, 1)` so each
# `view(matrix, :, column)` returns the buffer for one full inner chunk. The whole matrix lives in a single
# shard file written incrementally as columns finalize: the encoder pipes encoded chunk bytes into an
# `IncrementalShardWriter` (one shard per array), and the finalizer emits the shard's index footer.
function packed_streaming_dense_matrix(
    group::ZGroup,
    name::AbstractString,
    ::Type{T},
    nrows::Int,
    ncols::Int,
)::Tuple{PackedDenseMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    storage = group.storage
    if !(storage isa Zarr.DirectoryStore || storage isa MmapZipStore)
        # Stores with no incremental sink (e.g. `DictStore`) fall back to per-column chunk files.
        array = numeric_zcreate(T, group, name, (nrows, ncols), (nrows, 1), v3_bytes_codecs_for(compressor_for(), T))
        encoder = (column::Int, chunk_buffer::Vector{T}) -> begin
            array[:, column] = chunk_buffer
            return nothing
        end
        return (PackedDenseMatrix{T}(nrows, ncols, encoder), nothing)
    end

    array = sharded_zcreate(T, group, name, (nrows, ncols), (nrows, 1), v3_bytes_codecs_for(compressor_for(), T))
    sharding_codec = array.metadata.pipeline.array_bytes
    chunks_per_shard = (1, ncols)
    chunk_key_str = chunk_key(array, single_chunk_matrix_suffix(array))

    sink = if storage isa Zarr.DirectoryStore
        chunk_path = joinpath(storage.folder, chunk_key_str)
        mkpath(dirname(chunk_path))
        open(chunk_path, "w")
    else
        # `MmapZipStore` reserves the entry up front. The reservation is an upper bound: each column's
        # encoded inner chunk might in the worst case grow against the codec, so we book `2 * uncompressed +
        # 4096` bytes per column, plus the exact encoded index size at the tail. Unused bytes between the
        # last chunk and the index are padding the reader skips because the index records each chunk's
        # actual `(offset, nbytes)`.
        index_size = Zarr.Codecs.V3Codecs.compute_encoded_index_size(chunks_per_shard, sharding_codec)
        per_column_upper_bound = UInt64(2 * nrows * sizeof(T) + 4096)
        reserved_size = UInt64(ncols) * per_column_upper_bound + UInt64(index_size)
        region = reserve_mmap_zip_entry!(storage, chunk_key_str, reserved_size)
        MmapShardRegion(storage, chunk_key_str, region, UInt64(0), reserved_size)
    end

    writer =
        IncrementalShardWriter(sink, sharding_codec.codecs, sharding_codec.index_codecs, zero(T), ncols)
    encoder = (column::Int, chunk_buffer::Vector{T}) -> begin
        submit_shard_chunk!(writer, column, reshape(chunk_buffer, nrows, 1))
        return nothing
    end
    finalizer = () -> finalize_shard!(writer)
    return (PackedDenseMatrix{T}(nrows, ncols, encoder; finalizer), nothing)
end

function Formats.format_filled_empty_dense_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::AbstractMatrix{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    array = columns_axis_group(daf, rows_axis, columns_axis).arrays[name]
    if filled isa PackedDenseMatrix
        flush_packed_dense_matrix!(filled)
    else
        patch_chunk_crc_if_needed(array, single_chunk_matrix_suffix(array))
    end
    refresh_consolidated_metadata!(daf)
    return nothing
end

function write_sparse_matrix(parent::ZGroup, name::AbstractString, matrix::AbstractMatrix, packed::Bool)::Nothing
    matrix_group = zgroup(parent, name)

    colptr_vector = colptr(matrix)
    colptr_array =
        dense_zcreate(eltype(colptr_vector), matrix_group, "colptr", false, (length(colptr_vector),))
    colptr_array[:] = colptr_vector

    rowval_vector = rowval(matrix)
    rowval_array =
        dense_zcreate(eltype(rowval_vector), matrix_group, "rowval", false, (length(rowval_vector),))
    rowval_array[:] = rowval_vector

    if eltype(matrix) != Bool || !all(nzval(matrix))
        nzval_vector = nzval(matrix)
        nzval_array = dense_zcreate(eltype(nzval_vector), matrix_group, "nzval", packed, (length(nzval_vector),))
        nzval_array[:] = nzval_vector  # NOJET
    end
    return nothing
end

function Formats.format_get_empty_sparse_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
    _packed::Bool,
)::Tuple{
    AbstractVector{I},
    AbstractVector{I},
    AbstractVector{T},
    Maybe{Formats.CacheGroup},
} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)
    matrix_group = zgroup(group, name)

    nnz_int = Int(nnz)
    colptr_array = dense_zcreate(I, matrix_group, "colptr", false, (ncols + 1,))
    rowval_array = dense_zcreate(I, matrix_group, "rowval", false, (nnz_int,))
    nzval_array = dense_zcreate(T, matrix_group, "nzval", false, (nnz_int,))

    colptr_init = fill(I(nnz_int + 1), ncols + 1)
    colptr_init[1] = I(1)
    colptr_array[:] = colptr_init
    if nnz_int > 0
        # Poke the last element with a non-fill-value so the chunk file is materialised (Zarr's pipeline elides
        # writes when every cell equals `fill_value = zero(T)`); the user fills the mmap view in place.
        rowval_array[nnz_int] = oneunit(I)
        nzval_array[nnz_int] = oneunit(T)
    end

    colptr_vec, _ = array_as_vector(daf, colptr_array)
    rowval_vec, _ = array_as_vector(daf, rowval_array)
    nzval_vec, _ = array_as_vector(daf, nzval_array)
    return (colptr_vec, rowval_vec, nzval_vec, Formats.MappedData)
end

function Formats.format_filled_empty_sparse_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    matrix_group = columns_axis_group(daf, rows_axis, columns_axis).groups[name]
    colptr_array = matrix_group.arrays["colptr"]
    rowval_array = matrix_group.arrays["rowval"]
    patch_chunk_crc_if_needed(colptr_array, single_chunk_vector_suffix(colptr_array))
    patch_chunk_crc_if_needed(rowval_array, single_chunk_vector_suffix(rowval_array))
    if haskey(matrix_group.arrays, "nzval")
        nzval_array = matrix_group.arrays["nzval"]
        patch_chunk_crc_if_needed(nzval_array, single_chunk_vector_suffix(nzval_array))
    end
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_relayout_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
    packed::Bool,
)::StorageMatrix
    @assert Formats.has_data_write_lock(daf)
    if eltype(matrix) <: AbstractString
        group = columns_axis_group(daf, columns_axis, rows_axis)
        nrows = axis_length(daf, columns_axis)
        ncols = axis_length(daf, rows_axis)
        relayout_matrix = flipped(matrix)
        array = dense_zcreate(String, group, name, false, (nrows, ncols))
        array[:, :] = String.(relayout_matrix)
        refresh_consolidated_metadata!(daf)
        return relayout_matrix
    end
    if issparse(matrix)
        sparse_colptr, sparse_rowval, sparse_nzval, _ = Formats.format_get_empty_sparse_matrix!(
            daf,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(colptr(matrix)),
            packed,
        )
        sparse_colptr .= length(sparse_nzval) + 1
        sparse_colptr[1] = 1
        relayout_matrix = SparseMatrixCSC(
            axis_length(daf, columns_axis),
            axis_length(daf, rows_axis),
            sparse_colptr,
            sparse_rowval,
            sparse_nzval,
        )
        relayout!(flip(relayout_matrix), matrix)
        Formats.format_filled_empty_sparse_matrix!(daf, columns_axis, rows_axis, name, relayout_matrix)
        return relayout_matrix
    end
    relayout_matrix, _ =
        Formats.format_get_empty_dense_matrix!(daf, columns_axis, rows_axis, name, eltype(matrix), packed)
    relayout!(flip(relayout_matrix), matrix)
    Formats.format_filled_empty_dense_matrix!(daf, columns_axis, rows_axis, name, relayout_matrix)
    return relayout_matrix
end

function Formats.format_delete_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    delete_child(columns_axis_group(daf, rows_axis, columns_axis), name)
    refresh_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_matrices_set(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    return union(Set(keys(group.arrays)), Set(keys(group.groups)))
end

function Formats.format_get_matrix(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    if haskey(group.arrays, name)
        matrix, cache_group = array_as_matrix(daf, group.arrays[name])
        return (matrix, nothing, cache_group)
    end

    @assert haskey(group.groups, name)
    matrix_group = group.groups[name]
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    colptr_vector, colptr_cache_group = array_as_materialized_vector(daf, matrix_group.arrays["colptr"])
    rowval_vector, rowval_cache_group = array_as_materialized_vector(daf, matrix_group.arrays["rowval"])
    if haskey(matrix_group.arrays, "nzval")
        nzval_vector, nzval_cache_group = array_as_materialized_vector(daf, matrix_group.arrays["nzval"])
    else
        nzval_vector = fill(true, length(rowval_vector))
        nzval_cache_group = Formats.MemoryData
    end

    matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
    cache_group =
        if colptr_cache_group == Formats.MappedData &&
           rowval_cache_group == Formats.MappedData &&
           nzval_cache_group == Formats.MappedData
            Formats.MappedData
        else
            Formats.MemoryData
        end
    return (matrix, nothing, cache_group)
end

function delete_child(parent::ZGroup, name::AbstractString)::Nothing
    if parent.storage isa MmapZipStore
        error("can't delete or overwrite properties in a zip-backed ZarrDaf; the ZIP backend is append-only")
    end
    if haskey(parent.arrays, name)
        delete!(parent.arrays, name)
    elseif haskey(parent.groups, name)
        delete!(parent.groups, name)
    end
    if parent.storage isa Zarr.DirectoryStore
        child_path = joinpath(parent.storage.folder, lstrip(parent.path, '/'), name)
        if ispath(child_path)
            rm(child_path; recursive = true)
        end
    end
    if parent.storage isa Zarr.DictStore
        prefix = child_zarr_path(parent, name)
        prefix_slash = prefix * "/"
        for key in collect(keys(parent.storage.a))
            if key == prefix || startswith(key, prefix_slash)
                delete!(parent.storage.a, key)
            end
        end
    end
    return nothing
end

const REORDER_BACKUP_DIR = ".reorder.backup"

function reorder_backup_root(daf::ZarrDaf)::String
    return "$(daf.path)/$(REORDER_BACKUP_DIR)"
end

function recursive_hardlink(src::AbstractString, dst::AbstractString)::Nothing
    mkpath(dst)
    for (root, _, files) in walkdir(src)
        rel = relpath(root, src)
        out_dir = rel == "." ? dst : joinpath(dst, rel)
        mkpath(out_dir)
        for f in files
            hardlink(joinpath(root, f), joinpath(out_dir, f))
        end
    end
    return nothing
end

function child_zarr_path(group::ZGroup, name::AbstractString)::String  # FLAKY TESTED
    return isempty(group.path) ? String(name) : rstrip(group.path, '/') * '/' * String(name)
end

function reopen_zgroup_child!(group::ZGroup, name::AbstractString)::Nothing
    child = Zarr.zopen_noerr(
        group.storage,
        "w",
        Zarr.ZarrFormat(2);
        path = child_zarr_path(group, name),
        fill_as_missing = false,
    )  # NOJET
    if child isa ZArray
        group.arrays[name] = child
    elseif child isa ZGroup
        group.groups[name] = child
    else
        error("failed to reopen zarr child: $(child_zarr_path(group, name))")  # UNTESTEd
    end
    return nothing
end

function Reorder.format_lock_reorder!(daf::ZarrDaf, ::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    @assert !isdir(backup_root)
    mkdir(backup_root)
    return nothing
end

function Reorder.format_has_reorder_lock(daf::ZarrDaf)::Bool
    @assert Formats.has_data_write_lock(daf)
    return isdir(reorder_backup_root(daf))
end

function Reorder.format_backup_reorder!(daf::ZarrDaf, plan::Reorder.FormatReorderPlan)::Nothing
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    @assert isdir(backup_root)

    for (axis, _) in plan.planned_axes
        src = "$(daf.path)/$(AXES)/$(axis)"
        if isdir(src)
            recursive_hardlink(src, "$(backup_root)/$(AXES)/$(axis)")
        end
    end

    for planned in plan.planned_vectors
        src = "$(daf.path)/$(VECTORS)/$(planned.axis)/$(planned.name)"
        if isdir(src)
            recursive_hardlink(src, "$(backup_root)/$(VECTORS)/$(planned.axis)/$(planned.name)")
        end
    end

    for planned in plan.planned_matrices
        src = "$(daf.path)/$(MATRICES)/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)"
        if isdir(src)
            recursive_hardlink(
                src,
                "$(backup_root)/$(MATRICES)/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)",
            )
        end
    end

    return nothing
end

function Reorder.format_replace_reorder!(
    daf::ZarrDaf,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    crash_counter::Maybe{Ref{Int}},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    @assert isdir(reorder_backup_root(daf))

    for (axis, planned_axis) in plan.planned_axes
        if Formats.format_has_axis(daf, axis; for_change = false)
            delete_child(axes_group(daf), axis)
            axis_array = dense_zcreate(String, axes_group(daf), axis, false, (length(planned_axis.new_entries),))
            axis_array[:] = String.(planned_axis.new_entries)  # NOJET
        end
    end

    for planned in plan.planned_vectors
        replace_reorder_vector(daf, planned, plan, replacement_progress)
        Reorder.tick_crash_counter!(crash_counter)
    end

    for planned in plan.planned_matrices
        replace_reorder_matrix(daf, planned, plan, replacement_progress)
        Reorder.tick_crash_counter!(crash_counter)
    end

    refresh_consolidated_metadata!(daf)
    return nothing
end

function replace_reorder_vector(
    daf::ZarrDaf,
    planned::Reorder.PlannedVector,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_vector, _, _ = Formats.format_get_vector(daf, planned.axis, planned.name)
    is_source_packed = Formats.format_is_packed_vector(daf, planned.axis, planned.name)
    planned_axis = plan.planned_axes[planned.axis]
    group = axis_vectors_group(daf, planned.axis)

    if eltype(source_vector) <: AbstractString
        permuted = Vector{String}(undef, length(source_vector))
        permute_vector!(;
            destination = permuted,
            source = source_vector,
            permutation = planned_axis.permutation,
            progress = replacement_progress,
        )
        delete_child(group, planned.name)
        array = dense_zcreate(String, group, planned.name, false, (length(permuted),))
        array[:] = permuted  # NOJET
    elseif source_vector isa SparseVector
        T = eltype(source_vector)
        I = eltype(SparseArrays.nonzeroinds(source_vector))
        source_length = length(source_vector)
        source_nnz = nnz(source_vector)
        source_nzind = copy(SparseArrays.nonzeroinds(source_vector))
        source_nzval = copy(nonzeros(source_vector))

        delete_child(group, planned.name)
        destination_nzind, destination_nzval =
            Formats.format_get_empty_sparse_vector!(daf, planned.axis, planned.name, T, source_nnz, I, is_source_packed)
        permute_sparse_vector_buffers!(;
            destination_nzind,
            destination_nzval,
            source_length,
            source_nzind,
            source_nzval,
            inverse_permutation = planned_axis.inverse_permutation,
            progress = replacement_progress,
        )
    else
        T = eltype(source_vector)
        materialized = Vector{T}(source_vector)
        delete_child(group, planned.name)
        destination, _ = Formats.format_get_empty_dense_vector!(daf, planned.axis, planned.name, T, is_source_packed)
        permute_vector!(;
            destination,
            source = materialized,
            permutation = planned_axis.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

function replace_reorder_matrix(
    daf::ZarrDaf,
    planned::Reorder.PlannedMatrix,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_matrix, _, _ = Formats.format_get_matrix(daf, planned.rows_axis, planned.columns_axis, planned.name)
    is_source_packed = Formats.format_is_packed_matrix(daf, planned.rows_axis, planned.columns_axis, planned.name)
    planned_rows = get(plan.planned_axes, planned.rows_axis, nothing)
    planned_columns = get(plan.planned_axes, planned.columns_axis, nothing)
    @assert planned_rows !== nothing || planned_columns !== nothing

    group = columns_axis_group(daf, planned.rows_axis, planned.columns_axis)
    nrows, ncols = size(source_matrix)

    if eltype(source_matrix) <: AbstractString
        permuted = Matrix{String}(undef, nrows, ncols)
        permute_matrix_into!(permuted, source_matrix, planned_rows, planned_columns, replacement_progress)
        delete_child(group, planned.name)
        array = dense_zcreate(String, group, planned.name, false, (nrows, ncols))
        array[:, :] = permuted  # NOJET
    elseif source_matrix isa SparseMatrixCSC
        T = eltype(source_matrix)
        I = eltype(source_matrix.colptr)
        source_nnz_val = nnz(source_matrix)
        src_colptr = copy(source_matrix.colptr)
        src_rowval = copy(source_matrix.rowval)
        src_nzval = copy(source_matrix.nzval)

        delete_child(group, planned.name)
        destination_colptr, destination_rowval, destination_nzval, _ = Formats.format_get_empty_sparse_matrix!(
            daf,
            planned.rows_axis,
            planned.columns_axis,
            planned.name,
            T,
            source_nnz_val,
            I,
            is_source_packed,
        )
        if planned_rows !== nothing && planned_columns !== nothing
            permute_sparse_matrix_both_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = src_colptr,
                source_rowval = src_rowval,
                source_nzval = src_nzval,
                inverse_rows_permutation = planned_rows.inverse_permutation,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        elseif planned_rows !== nothing
            permute_sparse_matrix_rows_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = src_colptr,
                source_rowval = src_rowval,
                source_nzval = src_nzval,
                inverse_rows_permutation = planned_rows.inverse_permutation,
                progress = replacement_progress,
            )
        else
            permute_sparse_matrix_columns_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = src_colptr,
                source_rowval = src_rowval,
                source_nzval = src_nzval,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        end
    else
        T = eltype(source_matrix)
        materialized = Matrix{T}(source_matrix)
        delete_child(group, planned.name)
        destination, _ = Formats.format_get_empty_dense_matrix!(
            daf,
            planned.rows_axis,
            planned.columns_axis,
            planned.name,
            T,
            is_source_packed,
        )
        permute_matrix_into!(destination, materialized, planned_rows, planned_columns, replacement_progress)
    end
    return nothing
end

function permute_matrix_into!(
    destination::AbstractMatrix,
    source::AbstractMatrix,
    planned_rows::Maybe{Reorder.PlannedAxis},
    planned_columns::Maybe{Reorder.PlannedAxis},
    replacement_progress::Maybe{Progress},
)::Nothing
    if planned_rows !== nothing && planned_columns !== nothing
        permute_dense_matrix_both!(;
            destination,
            source,
            rows_permutation = planned_rows.permutation,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    elseif planned_rows !== nothing
        permute_dense_matrix_rows!(;
            destination,
            source,
            rows_permutation = planned_rows.permutation,
            progress = replacement_progress,
        )
    else
        permute_dense_matrix_columns!(;
            destination,
            source,
            columns_permutation = planned_columns.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

function Reorder.format_cleanup_reorder!(daf::ZarrDaf)::Nothing
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    @assert isdir(backup_root)
    rm(backup_root; force = true, recursive = true)
    return nothing
end

function Reorder.format_reset_reorder!(daf::ZarrDaf)::Bool
    @assert Formats.has_data_write_lock(daf)
    backup_root = reorder_backup_root(daf)
    if !isdir(backup_root)
        return false
    end

    axes_backup = "$(backup_root)/$(AXES)"
    if isdir(axes_backup)
        for axis in readdir(axes_backup)
            delete_child(axes_group(daf), axis)
            recursive_hardlink("$(axes_backup)/$(axis)", "$(daf.path)/$(AXES)/$(axis)")
            reopen_zgroup_child!(axes_group(daf), axis)
        end
    end

    vectors_backup = "$(backup_root)/$(VECTORS)"
    if isdir(vectors_backup)
        for axis in readdir(vectors_backup)
            parent = axis_vectors_group(daf, axis)
            axis_backup = "$(vectors_backup)/$(axis)"
            for name in readdir(axis_backup)
                delete_child(parent, name)
                recursive_hardlink("$(axis_backup)/$(name)", "$(daf.path)/$(VECTORS)/$(axis)/$(name)")
                reopen_zgroup_child!(parent, name)
            end
        end
    end

    matrices_backup = "$(backup_root)/$(MATRICES)"
    if isdir(matrices_backup)
        for rows_axis in readdir(matrices_backup)
            rows_backup = "$(matrices_backup)/$(rows_axis)"
            for columns_axis in readdir(rows_backup)
                parent = columns_axis_group(daf, rows_axis, columns_axis)
                cols_backup = "$(rows_backup)/$(columns_axis)"
                for name in readdir(cols_backup)
                    delete_child(parent, name)
                    recursive_hardlink(
                        "$(cols_backup)/$(name)",
                        "$(daf.path)/$(MATRICES)/$(rows_axis)/$(columns_axis)/$(name)",
                    )
                    reopen_zgroup_child!(parent, name)
                end
            end
        end
    end

    rm(backup_root; force = true, recursive = true)
    refresh_consolidated_metadata!(daf)
    return true
end

function TanayLabUtilities.Brief.brief(value::ZarrDaf; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ZarrDaf $(name)"
end

function __init__()
    if haskey(Zarr.Codecs.V3Codecs.codec_parsers, "vlen-utf8")
        @warn(
            "Zarr.jl now ships a built-in `vlen-utf8` v3 codec. The local copy in " *
            "`DataAxesFormats.ZarrFormat` (the BEGIN/END `vlen-utf8` block in " *
            "`src/zarr_format.jl` plus this branch of `__init__`) should be removed in " *
            "favour of the upstream version."
        )
    else
        Zarr.Codecs.V3Codecs.register_codec("vlen-utf8", VLenUTF8V3Codec) do _config, _ctx
            return VLenUTF8V3Codec()
        end
    end
    Zarr.typemap3["string"] = String
    return nothing
end

end  # module
