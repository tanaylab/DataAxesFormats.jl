"""
A `Daf` storage format in a [Zarr](https://zarr.readthedocs.io/) directory tree or ZIP archive. Like
[`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf), the data can live in a directory of files on the filesystem
(so standard filesystem tools work, and deleting a property immediately frees its storage), and offers a different
trade-off compared to [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) and
[`H5df`](@ref DataAxesFormats.H5dfFormat.H5df).

`FilesDaf` uses its own `Daf`-specific layout, but the individual files are in deliberately simple formats (`JSON` for
metadata, one-line-per-entry text for axis entries, raw little-endian binary for numeric data), so they are easy to
inspect or produce with standard command-line tools even without any `Daf`-aware library. `ZarrDaf` instead lays the
files out according to the Zarr v3 specification: the per-node `zarr.json` metadata and the chunk files are more opaque
than `FilesDaf`'s plain text/JSON, but in exchange the directory can be read directly by any Zarr library (e.g. the
Python `zarr` package) without that library having to know anything about `Daf`.

A Zarr directory is still a directory rather than a single file, so for convenient publication or transport we also
support storing a `Daf` data set inside a single ZIP archive; zipping a Zarr directory (or, actually, a tree containing
several such directories) would give a valid Zarr ZIP archive. An advantage of this is that a single ZIP file can hold
several Daf repositories, while `ZipDaf` is restricted to a single repository per zip file.

ZIP archives written by this package hold every flat chunk uncompressed (ZIP method `0`) so it can be memory-mapped for
direct access just like the directory backend (you should also force this if manually zipping a directory yourself);
packed chunks live inside one dual-format shard file per property (see the packed-property notes below), still in the
same archive. On the ZIP backend the archive is append-only: properties cannot be deleted and axes cannot be reordered.
For read access, any Zarr v2 ZIP archive that matches the internal structure described below is accepted (including ones
produced by foreign tools such as Python's `zarr` package, even if the chunks are chunked and/or compressed, subject to
`Zarr.jl`'s support for data types, filters, and compressors). Remote object stores (S3, GCS, …) are not supported.

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
    the chunk file on disk is a raw binary image that we can memory-map. Packed properties (`packed = true`,
    uncompressed size at or above
    [`DAF_PACKED_TARGET_CHUNK_KB`](@ref DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB)) are stored as v3
    sharded arrays (ZEP-0002) with one shard file per property — directory backend at `<name>/c/0[/0]`, ZIP backend
    inside one archive entry per property — and chunked + compressed by the codec resolved from
    [`DAF_PACKED_COMPRESSION`](@ref DataAxesFormats.PackedFormat.DAF_PACKED_COMPRESSION). Both encodings coexist
    within a single `Daf` data set without a version bump (`[1,0]` covers both).

    Daf-written packed shards carry a `daf_packed_format` attribute on each sharded `ZArray`. Its value drives the
    dispatch in [`zarr_convert`](@ref DataAxesFormats.ZarrConvert) (hard-link vs. re-encode) and in the read paths:

      + `"indexed+zipped"` — produced by this package's writer: the shard bytes are simultaneously a valid Zarr v3
        sharded array (with a shard index at offset 0) and a valid ZIP archive (with a central directory at the tail).
        The same shard bytes can hard-link between `ZarrDaf` and `FilesDaf` directories, and `ZipDaf` / `FilesDaf` /
        `HttpDaf` read them via the ZIP central directory while `ZarrDaf` reads them via the Zarr shard index.
      + attribute absent (or any other value) — produced by a foreign Zarr writer: the shard has only the index, no
        ZIP framing. `ZarrDaf` still reads via the index; conversion to `FilesDaf` falls back to
        `rewrite_index_only_as_dual_format_shard`.

  - The root group's `zarr.json` carries a `consolidated_metadata` field — an inline index of every per-node
    `zarr.json` under the root, so an open / HTTP-served reader does not have to issue one GET per node. Its
    content is bijective with the consolidated metadata that
    [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) writes in its `metadata.json` (see the `FilesDaf`
    documentation for the formal mapping; [`zarr_to_files`](@ref DataAxesFormats.ZarrConvert.zarr_to_files) and
    [`files_to_zarr`](@ref DataAxesFormats.ZarrConvert.files_to_zarr) translate between them).
    The on-disk shape is the one `zarr-python` 3.x writes, which informally tracks
    [zarr-specs PR #309](https://github.com/zarr-developers/zarr-specs/pull/309) (still open as of writing,
    so the spec PR is **not** the authoritative reference — `zarr-python`'s `ConsolidatedMetadata` class is). The
    field lives at the top level of the root group's `zarr.json`, sibling to `attributes`:

    ```json
    {
      "zarr_format": 3,
      "node_type": "group",
      "attributes": {"daf": [1, 0]},
      "consolidated_metadata": {
        "kind": "inline",
        "must_understand": false,
        "metadata": {
          "<relative_path>": <full v3 metadata blob>,
          ...
        }
      }
    }
    ```

    where `<relative_path>` is each node's path relative to the root group (no leading slash, no trailing
    `/zarr.json`) and the value is the verbatim parsed content of that node's `zarr.json` — i.e. the same
    `MetadataV3` that the per-node file holds, including its own `attributes`. `kind` is always `"inline"`;
    `must_understand` is always `false`. Order of keys inside `metadata` is not significant for interop;
    `zarr-python`'s writer happens to sort by (depth, casefolded NFKC name) but its reader does not require it.
    On every property `set!` / `delete!` we update the field in the root `zarr.json` (full file rewrite per
    operation, which is unavoidable since `zarr.json` is one document; we cache the serialized bytes of the
    `metadata` sub-dict and append in place rather than re-serializing every existing entry, so per-`set!` CPU
    work is O(size-of-one-descriptor)). On every open (read or write), if the field is missing we attempt to
    rebuild it by walking the per-node `zarr.json` files; the rebuild is best-effort with the same
    swallow-on-read-only-frozen-filesystem semantics as `FilesDaf`'s `metadata.json`.

Example Zarr v3 directory structure (every group and every array has its own `zarr.json`; an array's chunk data lives
under its `c/` directory — `c/0` for a 1D array, `c/0/0` for a 2D array; the root group's `zarr.json` holds the `daf`
attribute and the consolidated metadata):

    example-daf-dataset-root-directory.daf.zarr/
    ├─ zarr.json                     # root group (attributes.daf = [1, 0], consolidated_metadata)
    ├─ scalars/
    │  ├─ zarr.json
    │  └─ version/
    │     ├─ zarr.json
    │     └─ c/0
    ├─ axes/
    │  ├─ zarr.json
    │  ├─ cell/
    │  │  ├─ zarr.json
    │  │  └─ c/0
    │  └─ gene/
    │     ├─ zarr.json
    │     └─ c/0
    ├─ vectors/
    │  ├─ zarr.json
    │  ├─ cell/
    │  │  ├─ zarr.json
    │  │  └─ batch/
    │  │     ├─ zarr.json
    │  │     └─ c/0
    │  └─ gene/
    │     ├─ zarr.json
    │     └─ is_marker/
    │        ├─ zarr.json
    │        └─ c/0
    └─ matrices/
       ├─ zarr.json
       ├─ cell/
       │  ├─ zarr.json
       │  └─ gene/
       │     ├─ zarr.json
       │     ├─ UMIs/                # sparse → sub-group with one array per component
       │     │  ├─ zarr.json
       │     │  ├─ colptr/
       │     │  │  ├─ zarr.json
       │     │  │  └─ c/0
       │     │  ├─ rowval/
       │     │  │  ├─ zarr.json
       │     │  │  └─ c/0
       │     │  └─ nzval/
       │     │     ├─ zarr.json
       │     │     └─ c/0
       │     └─ fractions/           # dense, packed → single v3 sharded array
       │        ├─ zarr.json
       │        └─ c/0/0
       └─ gene/
          ├─ zarr.json
          ├─ cell/
          └─ gene/

!!! note

    `Zarr.jl` maps Julia's column-major arrays onto Zarr v3's row-major model by listing the `zarr.json` `shape` in
    the reverse of the `Daf` (Julia) matrix shape, so the raw chunk bytes match Julia's native column-major layout. A
    `Daf` matrix whose `(rows_axis, columns_axis)` are `(cell, gene)` (a Julia `(n_cells, n_genes)` matrix) is
    therefore written with `zarr.json` containing `"shape": [n_genes, n_cells]`. A client using a different Zarr
    implementation — most notably Python's `zarr` package — reads this as a C-contiguous NumPy array of shape `(n_genes, n_cells)`, which is the **transpose** of the `Daf` (Julia) view. The bytes on disk are identical; only the shape
    labels are swapped. To obtain the `Daf`-canonical `(cell, gene)` orientation in Python, apply `.T` (a zero-copy
    view) to the loaded array. This affects only dense matrices (the `colptr`/`rowval`/`nzval` child arrays of sparse
    matrices are 1D vectors, unaffected); 1D axis-entry arrays and vector properties have the same shape in both
    languages.

!!! note

    The code here assumes the Zarr data obeys all the above conventions and restrictions. As long as you only create and
    access `Daf` data in Zarr directories using [`ZarrDaf`](@ref), then the code will work as expected (assuming no
    bugs). However, if you do this in some other way (e.g., a Zarr library in another language producing compressed or
    multi-chunk arrays), and the result is invalid, then the code here may fail with "less than friendly" error
    messages.
"""
module ZarrFormat

export ZarrDaf

using ..Formats
using ..LazySparse
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
import ..MmapZipStores.CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE
import ..MmapZipStores.LOCAL_FILE_HEADER_FIXED_SIZE
import ..MmapZipStores.TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE
import ..MmapZipStores.ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
import ..MmapZipStores.ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE
import ..PackedFormat.build_shard_metadata
import ..PackedFormat.ChunkedArray
import ..PackedFormat.chunks_for
import ..PackedFormat.compressor_for
import ..PackedFormat.extract_inner_chunk
import ..PackedFormat.finalize_shard!
import ..PackedFormat.flush_packed_dense_matrix!
import ..PackedFormat.PackedDenseArray
import ..PackedFormat.StripedMatrix
import ..PackedFormat.StripedVector
import ..PackedFormat.IncrementalShardWriter
import ..PackedFormat.InMemorySink
import ..PackedFormat.is_zarr_array_packed
import ..PackedFormat.join_url
import ..PackedFormat.MmapShardRegion
import ..PackedFormat.packed_codec_from_zarray
import ..PackedFormat.packed_http_cache_mb
import ..PackedFormat.packed_local_cache_mb
import ..PackedFormat.PackedCodec
import ..PackedFormat.PackedDenseMatrix
import ..PackedFormat.position_in_sink
import ..PackedFormat.seek_in_sink!
import ..PackedFormat.shard_fill_value
import ..PackedFormat.submit_shard_chunk!
import ..PackedFormat.take_bytes!
import ..PackedFormat.url_byte_fetcher
import ..PackedFormat.url_suffix_byte_fetcher
import ..PackedFormat.v3_bytes_codecs_for
import ..PackedFormat.VLenUTF8V3Codec
import ..PackedFormat.write_to_sink!
import ..Reorder
import ..ZipFormat.acquire_shared_mmap_zip_store!
import ..ZipFormat.parse_zip_archive_path
import ..ZipFormat.SharedMmapZipStoreHandle

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

# Build a v3 uncompressed `String` ZArray under `group` at `name` with inner pipeline `VLenUTF8V3Codec`. The
# default `chunks == shape` produces the single-chunk layout used for axis-entry arrays and the `daf`
# version-marker. Packed string properties go through [`sharded_zcreate`](@ref) instead, which produces a
# single-shard layout matching the numeric packed path. The optional `chunks` parameter exists for the
# non-canonical read-tolerance test exercising the reader against a multi-chunk-per-file vlen-utf8 layout
# that we never write ourselves.
function string_zcreate(
    group::ZGroup,
    name::AbstractString,
    shape::NTuple{N, Int},
    chunks::NTuple{N, Int} = shape,
)::ZArray{String, N} where {N}
    pipeline = Zarr.V3Pipeline((), VLenUTF8V3Codec(), ())
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

# Build a v3 ZArray under `group` at `name` as a single-shard sharded array per ZEP-0002: the outer chunk
# shape equals the array shape (one shard covers the whole array), the inner chunks have shape `inner_chunks`,
# and `bytes_bytes_codecs` is the bytes→bytes chain applied per inner chunk (compression, etc.). The shard's
# index lives at the tail (`index_location = :end`) and is protected by CRC32c. For numeric `T` the inner
# pipeline's array→bytes step is `BytesCodec`; for `T <: AbstractString` it's `VLenUTF8V3Codec`. Dispatch is
# centralized in [`build_shard_metadata`](@ref).
function sharded_zcreate(
    ::Type{T},
    group::ZGroup,
    name::AbstractString,
    shape::NTuple{N, Int},
    inner_chunks::NTuple{N, Int},
    bytes_bytes_codecs::Tuple = (),
)::ZArray where {T, N}
    metadata = build_shard_metadata(T, shape, inner_chunks, bytes_bytes_codecs, :start)
    storage = group.storage
    array_path = Zarr._concatpath(group.path, String(name))
    if !Zarr.isemptysub(storage, array_path)
        error("non-empty Zarr path: $(array_path)")  # UNTESTED
    end
    # Mark the shard as dual-format (a Zarr sharded array that is also a valid ZIP) so `zarr_convert` can hardlink
    # it as-is. The attribute is merged into the single `zarr.json` write by hand because `lower3` drops attributes.
    attributes = Dict{String, Any}("daf_packed_format" => "indexed+zipped")
    metadata_json = JSON.lower(metadata)  # NOJET
    metadata_json["attributes"] = attributes
    buffer = IOBuffer()
    JSON.print(buffer, metadata_json)
    storage[array_path, "zarr.json"] = take!(buffer)
    array = Zarr.ZArray(metadata, storage, array_path, attributes, true)
    group.arrays[String(name)] = array
    return array
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
    the HTTP backend is strictly read-only and returns a [`DafReadOnly`](@ref). The remote root `zarr.json` **must**
    carry an inline `consolidated_metadata` field (the on-disk shape `zarr-python` 3.x writes — see the module
    docstring), and the served content **must** be stable for the lifetime of the open handle: per-chunk GETs
    happen lazily, so if the underlying data set is rewritten or relocated while the handle is open, subsequent
    reads may see inconsistent bytes.

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

    `.daf.zarr.zip` archives written by this code do not carry the inline `consolidated_metadata` field in their
    root `zarr.json`, because the ZIP central directory plays the same enumeration role. Consequently, an
    `unzip foo.daf.zarr.zip -d foo.daf.zarr/` produces a directory whose root `zarr.json` lacks
    `consolidated_metadata`. Before exposing such a directory over HTTP, open it once locally with
    `ZarrDaf("foo.daf.zarr")` (any mode) so `ensure_consolidated_metadata!` builds it. Symmetrically, when ZarrDaf
    appends to a `.daf.zarr.zip` whose root `zarr.json` does have an inline `consolidated_metadata` (e.g. from a
    `zip -r` of a directory daf), the field is stripped from the rewritten root `zarr.json` so subsequent unzips
    can't see a stale snapshot.

!!! warning

    The byte-surgery append on `consolidated_metadata.metadata` and the atomic stage-rename rewrite of root
    `zarr.json` assume a single writer at a time. The in-process Daf write lock serializes writers within one Julia
    process; opening the same `.daf.zarr` directory from multiple processes (or multiple machines, e.g. NFS) and
    writing concurrently can interleave rewrites and corrupt the consolidated metadata. Open writable from one
    process at a time.
"""
mutable struct ZarrDaf <: DafWriter
    name::AbstractString
    internal::Internal
    root::ZGroup
    mode::AbstractString
    path::AbstractString
    # Cached serialized form of `consolidated_metadata.metadata` (the inner `{<relative_path>: <full v3 metadata
    # blob>}` dict — see the module docstring). Mutated by `register_consolidated_metadata!` (byte-surgery append on
    # `set!`) and `refresh_consolidated_metadata!` (full rebuild on `delete!` / reorder). Always a valid JSON object
    # literal whose last byte is `}` so the byte-surgery insertion can locate the splice point in O(1). Empty for
    # backends that do not maintain consolidated metadata (`MmapZipStore`, `DictStore`).
    consolidated_metadata_bytes::Vector{UInt8}
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
    daf = ZarrDaf(name, internal, root, mode, full_path, Vector{UInt8}("{}"))
    strip_zarr_zip_consolidated_metadata!(daf)
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
    http_store = Zarr.HTTPStore(String(url))
    root_zarr_bytes = try
        http_store["", "zarr.json"]
    catch exception
        error("failed to fetch remote zarr group: $(url)/zarr.json\n" * "underlying error: $(exception)")  # UNTESTED
    end
    if root_zarr_bytes === nothing
        error("failed to fetch remote zarr group: $(url)/zarr.json")
    end
    root_metadata = JSON.parse(String(copy(root_zarr_bytes)); dicttype = Dict{String, Any})::Dict{String, Any}  # NOJET
    if !(get(root_metadata, "node_type", "") == "group")
        error("not a zarr group: $(url)")
    end
    consolidated_field = get(root_metadata, "consolidated_metadata", nothing)
    if !(consolidated_field isa AbstractDict) ||
       get(consolidated_field, "kind", "") != "inline" ||
       !(get(consolidated_field, "metadata", nothing) isa AbstractDict)
        error(
            "remote zarr group lacks an inline `consolidated_metadata` field: $(url)\n" *
            "expose the directory after any `ZarrDaf(...)` open on a writable filesystem so the field is built",
        )
    end
    inline_metadata = consolidated_field["metadata"]::AbstractDict

    # Translate zarr-python's flat path keys (`<path>`) to Zarr.jl's `ConsolidatedStore` key shape (`<path>/zarr.json`),
    # and inject the root group's own metadata under `"zarr.json"` (with the `consolidated_metadata` field stripped so
    # the consolidated dict the parent store sees doesn't recurse on itself).
    cons_dict = Dict{String, Any}()
    root_metadata_without_field = copy(root_metadata)
    delete!(root_metadata_without_field, "consolidated_metadata")
    cons_dict["zarr.json"] = root_metadata_without_field
    for (path, blob) in inline_metadata
        cons_dict["$(path)/zarr.json"] = blob
    end

    consolidated_store = Zarr.ConsolidatedStore(http_store, "", cons_dict)
    root = try
        Zarr.zopen(consolidated_store, "r"; zarr_format = 3)  # NOJET
    catch exception
        error("failed to open remote zarr group: $(url)\n" * "underlying error: $(exception)")  # UNTESTED
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
        name = String(url)
    end
    name = unique_name(name)
    daf = ZarrDaf(name, Internal(; is_frozen = true), root, "r", String(url), Vector{UInt8}("{}"))
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
    daf = ZarrDaf(
        name,
        Internal(; is_frozen = false, packed_default = packed),
        root,
        "w+",
        "<memory>",
        Vector{UInt8}("{}"),
    )
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
            # `refresh_consolidated_metadata!` always writes every group's `zarr.json` into the consolidated entry,
            # so this branch fires only for a malformed or partial snapshot.
            return Dict{String, Any}()  # UNTESTED
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
        root =  # NOJET
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
                    expected: [major, minor]
                    in daf zarr group: $(full_path)
                    """))
    end
    major = Int(version[1])
    minor = Int(version[2])
    if major != MAJOR_VERSION || minor > MINOR_VERSION
        error(chomp("""
                    incompatible format version: $(major).$(minor)
                    the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
                    in daf zarr group: $(full_path)
                    """))
    end
    return nothing
end

function Readers.is_leaf(::ZarrDaf)::Bool
    return true
end

function Readers.is_leaf(::Type{ZarrDaf})::Bool
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

function scalars_group(daf::ZarrDaf)::ZGroup
    return daf.root.groups[SCALARS]
end

function axes_group(daf::ZarrDaf)::ZGroup
    return daf.root.groups[AXES]
end

function vectors_group(daf::ZarrDaf)::ZGroup
    return daf.root.groups[VECTORS]
end

function matrices_group(daf::ZarrDaf)::ZGroup
    return daf.root.groups[MATRICES]
end

function is_writable(daf::ZarrDaf)::Bool
    return daf.mode != "r"
end

function chunk_key(array::ZArray, suffix::AbstractString)::String
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

# Create a Zarr array for a dense property. Falls back to a single-chunk uncompressed encoding when `chunks_for`
# returns `nothing` or `T` is non-bits (e.g. `String`); otherwise applies the codec from `DAF_PACKED_COMPRESSION` /
# `DAF_PACKED_COMPRESSION_LEVEL`. Callers fill data into the returned `ZArray` afterward.
function dense_zcreate(
    ::Type{T},
    group::ZGroup,
    name::AbstractString,
    is_packed::Bool,
    shape::NTuple{N, Int},
)::ZArray{String, N} where {N, T <: AbstractString}
    inner_chunks = chunks_for(is_packed, shape, String)
    if inner_chunks === nothing
        return string_zcreate(group, name, shape)
    else
        return sharded_zcreate(String, group, name, shape, inner_chunks, v3_bytes_codecs_for(compressor_for(), String))
    end
end

function dense_zcreate(
    ::Type{T},
    group::ZGroup,
    name::AbstractString,
    is_packed::Bool,
    shape::NTuple{N, Int},
)::ZArray{T, N} where {T, N}
    inner_chunks = chunks_for(is_packed, shape, T)
    if inner_chunks === nothing
        return numeric_zcreate(T, group, name, shape)
    else
        return sharded_zcreate(T, group, name, shape, inner_chunks, v3_bytes_codecs_for(compressor_for(), T))
    end
end

# Predicate: does the metadata of `array` describe a sharded (packed) v3 array? Used to gate the dual-format
# write path against the plain `array[:] = data` path used for flat arrays.
function is_sharded_zarray(array::ZArray)::Bool
    return array.metadata.pipeline.array_bytes isa Zarr.Codecs.V3Codecs.ShardingCodec
end

# Write `data` into the shard file backing a sharded `ZArray` using the dual-format [`IncrementalShardWriter`](@ref),
# so the on-disk bytes are simultaneously a valid Zarr v3 sharded array AND a valid ZIP archive. Replaces
# `array[:] = data` for packed properties in `ZarrDaf` write paths.
function write_packed_shard!(array::ZArray{T, N}, data::AbstractArray{T, N})::Nothing where {T, N}
    @assert is_sharded_zarray(array)
    @assert size(data) == size(array)
    sharding_codec = array.metadata.pipeline.array_bytes
    inner_chunk_shape = sharding_codec.chunk_shape
    chunks_per_shard = Zarr.Codecs.V3Codecs.calculate_chunks_per_shard(size(array), inner_chunk_shape)
    chunk_key_str = chunk_key(array, single_chunk_suffix(array))
    sink = open_dual_shard_sink(array.storage, chunk_key_str, chunks_per_shard, inner_chunk_shape, T)
    writer = IncrementalShardWriter(
        sink,
        sharding_codec.codecs,
        sharding_codec.index_codecs,
        shard_fill_value(T),
        chunks_per_shard,
    )
    fill_value = shard_fill_value(T)
    for chunk_index in 1:prod(chunks_per_shard)
        chunk_view = extract_inner_chunk(data, chunk_index, chunks_per_shard, inner_chunk_shape, fill_value)
        submit_shard_chunk!(writer, chunk_index, chunk_view)
    end
    finalize_shard!(writer)
    return nothing
end

# Open a sink for [`write_packed_shard!`](@ref). The sink type depends on the underlying Zarr store: `DirectoryStore`
# gets an `IOStream` over the chunk file; `MmapZipStore` gets an [`MmapShardRegion`](@ref) over a freshly-reserved
# outer-zip entry; `DictStore` gets a [`DictStoreSink`](@ref) that writes the accumulated bytes back into the dict
# on `close`.
function open_dual_shard_sink(storage::Zarr.DirectoryStore, chunk_key_str::AbstractString, ::Tuple, ::Tuple, ::Type)
    chunk_path = joinpath(storage.folder, chunk_key_str)
    mkpath(dirname(chunk_path))
    return open(chunk_path, "w")
end

function open_dual_shard_sink(
    storage::MmapZipStore,
    chunk_key_str::AbstractString,
    chunks_per_shard::Tuple,
    inner_chunk_shape::Tuple,
    ::Type{T},
)::MmapShardRegion where {T}
    n_chunks = UInt64(prod(chunks_per_shard))
    per_chunk_overhead = UInt64(LOCAL_FILE_HEADER_FIXED_SIZE + 64 + ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE)
    per_chunk_upper_bound = UInt64(2 * prod(inner_chunk_shape) * sizeof(T) + 4096) + per_chunk_overhead
    per_cd_entry_overhead = UInt64(CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + 64 + ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE)
    index_size = UInt64(16 * n_chunks + 4)
    cd_region_size = n_chunks * per_cd_entry_overhead + UInt64(TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    reserved_size = index_size + n_chunks * per_chunk_upper_bound + cd_region_size
    region = reserve_mmap_zip_entry!(storage, chunk_key_str, reserved_size)
    return MmapShardRegion(storage, chunk_key_str, region, UInt64(0), UInt64(0), reserved_size)
end

function open_dual_shard_sink(
    storage::Zarr.DictStore,
    chunk_key_str::AbstractString,
    ::Tuple,
    ::Tuple,
    ::Type,
)::DictStoreSink
    return DictStoreSink(storage, String(chunk_key_str), InMemorySink())
end

# Sink wrapper around an [`InMemorySink`](@ref) that flushes the accumulated bytes into a `Zarr.DictStore` entry on
# close. Used by [`write_packed_shard!`](@ref) when the array lives in an in-memory `DictStore`.
struct DictStoreSink
    store::Zarr.DictStore
    key::String
    inner::InMemorySink
end

position_in_sink(sink::DictStoreSink)::UInt64 = position_in_sink(sink.inner)
seek_in_sink!(sink::DictStoreSink, offset::UInt64)::Nothing = seek_in_sink!(sink.inner, offset)
write_to_sink!(sink::DictStoreSink, bytes::AbstractVector{UInt8})::Nothing = write_to_sink!(sink.inner, bytes)

function Base.close(sink::DictStoreSink)::Nothing
    sink.store[sink.key] = take_bytes!(sink.inner)
    return nothing
end

# Convenience: write `data` into `array`, picking the dual-format shard writer for sharded (packed) arrays and
# Zarr.jl's plain `setindex!` for flat (unpacked) arrays. Used everywhere `ZarrDaf` populates an array whose
# shardedness depends on `chunks_for(is_packed, ...)`. Permissive in `data`'s eltype: a string `array[T=String]`
# typically receives a `SubString` matrix, so coerce when the eltypes differ.
function write_dense_data!(array::ZArray, data::AbstractArray)::Nothing
    if is_sharded_zarray(array)
        T = eltype(array)
        write_packed_shard!(array, eltype(data) === T ? data : T.(data))
    else
        setindex!(array, data, ntuple(_ -> Colon(), ndims(array))...)  # NOJET
    end
    return nothing
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

# Drop a stale `consolidated_metadata` field from the root `zarr.json` of a ZIP-backed `ZarrDaf` on writable open.
# A field surviving from `zip -r` of a previous `DirectoryStore` daf would go stale on the next append; ZarrDaf-Zip
# doesn't maintain consolidated metadata (the central directory is the index). Strip is remove + append; the old
# entry's data region remains as orphan bytes.
function strip_zarr_zip_consolidated_metadata!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    if !(storage isa MmapZipStore) || !is_writable(daf)
        return nothing
    end
    prefix = daf.root.path
    key = isempty(prefix) ? "zarr.json" : prefix * "/zarr.json"
    raw = storage[prefix, "zarr.json"]
    if raw === nothing
        return nothing  # UNTESTED
    end
    parsed = JSON.parse(String(copy(raw)); dicttype = Dict{String, Any})::Dict{String, Any}  # NOJET
    if !haskey(parsed, "consolidated_metadata")
        return nothing
    end
    delete!(parsed, "consolidated_metadata")
    io = IOBuffer()
    JSON.print(io, parsed)
    remove_entries_from_central_directory!(storage, [key])
    storage[key] = take!(io)
    return nothing
end

function refresh_consolidated_metadata!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    daf.consolidated_metadata_bytes = build_consolidated_metadata_bytes(daf.root)
    flush_consolidated_metadata!(daf)
    return nothing
end

# Append a single entry to `daf.consolidated_metadata_bytes` via byte-level surgery on the trailing `}`. `path` is
# the new node's path relative to the root group (no leading slash, no `/zarr.json` suffix); `descriptor` is the JSON
# bytes of that node's full v3 metadata blob (matching what `zarr.json` for that node holds on disk). The cache is
# updated in place; the caller is expected to call [`flush_consolidated_metadata!`](@ref) once after all node
# registrations to commit the new state to disk. Use multiple `register_consolidated_node!` calls in sequence for
# multi-node operations (sparse property writes that create a sub-group + sub-arrays, `add_axis` that creates several
# groups) followed by a single flush, so the per-`set!` cost is one root `zarr.json` rewrite regardless of how many
# nodes the operation touched. Per-call CPU work is O(size of the new descriptor); rebuild of the cache is O(N) and
# done only on `delete!`/reorder via [`refresh_consolidated_metadata!`](@ref).
function register_consolidated_node!(daf::ZarrDaf, path::AbstractString, descriptor::AbstractVector{UInt8})::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    cached = daf.consolidated_metadata_bytes
    @assert !isempty(cached) && cached[end] == UInt8('}')
    encoded_key = Vector{UInt8}(JSON.json(String(path)))
    io = IOBuffer()
    # The "was `{}`" branch is unreachable in normal flow but kept for correctness.
    if length(cached) == 2  # was the empty object `{}`  # UNTESTED
        write(io, UInt8('{'))  # UNTESTED
    else
        write(io, @view cached[1:(end - 1)])
        write(io, UInt8(','))
    end
    write(io, encoded_key, UInt8(':'), descriptor, UInt8('}'))
    daf.consolidated_metadata_bytes = take!(io)
    return nothing
end

# Register every node in a freshly-written subtree rooted at `prefix` (a `ZGroup` whose ancestors already exist in the
# cache). Used by sparse property writes (sub-group + `nzind`/`nzval` sub-arrays) and `add_axis` (one new group per
# axis, recursive into the matrices cross-product). Cache-only — caller invokes [`flush_consolidated_metadata!`](@ref)
# once at the end.
function register_consolidated_subtree!(daf::ZarrDaf, prefix::AbstractString, group::ZGroup)::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    register_consolidated_node!(daf, prefix, zgroup_metadata_bytes(group))
    for (name, subgroup) in pairs(group.groups)
        register_consolidated_subtree!(daf, "$(prefix)/$(name)", subgroup)
    end
    for (name, array) in pairs(group.arrays)
        register_consolidated_node!(daf, "$(prefix)/$(name)", Vector{UInt8}(JSON.json(array.metadata)))
    end
    return nothing
end

# Atomically rewrite root `zarr.json` from the current `daf.consolidated_metadata_bytes` cache. Called once at the end
# of any `set!` (after one or more `register_consolidated_node!` / `register_consolidated_subtree!` calls) and at the
# end of `refresh_consolidated_metadata!`.
function flush_consolidated_metadata!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    write_root_zarr_json!(daf)
    return nothing
end

# Walk the in-memory ZGroup tree and emit a JSON object literal mapping each child node's relative path to its full
# v3 metadata blob — the on-disk shape that `zarr-python`'s `ConsolidatedMetadata.from_dict` expects under
# `consolidated_metadata.metadata`. Group entries carry `{"zarr_format":3,"node_type":"group","attributes":<...>}`;
# array entries serialize the underlying `Zarr.MetadataV3` directly (which `JSON.print` already encodes as the
# canonical v3 array zarr.json shape). Sort order is not required for interop; we visit in `pairs(...)` order.
function build_consolidated_metadata_bytes(root::ZGroup)::Vector{UInt8}
    io = IOBuffer()
    write(io, UInt8('{'))
    first = Ref(true)
    walk_zgroup_subtree(io, root, ""; first)
    write(io, UInt8('}'))
    return take!(io)
end

function walk_zgroup_subtree(io::IO, group::ZGroup, prefix::AbstractString; first::Ref{Bool})::Nothing
    if !isempty(prefix)
        if !first[]
            write(io, UInt8(','))
        end
        first[] = false
        JSON.print(io, prefix)
        write(io, UInt8(':'))
        write(io, zgroup_metadata_bytes(group))
    end
    for (name, subgroup) in pairs(group.groups)
        sub_path = isempty(prefix) ? String(name) : "$(prefix)/$(name)"
        walk_zgroup_subtree(io, subgroup, sub_path; first)
    end
    for (name, array) in pairs(group.arrays)
        sub_path = isempty(prefix) ? String(name) : "$(prefix)/$(name)"
        if !first[]
            write(io, UInt8(','))
        end
        first[] = false
        JSON.print(io, sub_path)
        write(io, UInt8(':'))
        JSON.print(io, array.metadata)
    end
    return nothing
end

# Build a v3 group metadata JSON blob for a `ZGroup`. The on-disk form is `{"zarr_format":3,"node_type":"group"}`
# plus an optional `"attributes"` field; we omit `attributes` when empty for byte-identity with what `zarr-python`'s
# default group serializer emits.
function zgroup_metadata_bytes(group::ZGroup)::Vector{UInt8}
    io = IOBuffer()
    write(io, "{\"zarr_format\":3,\"node_type\":\"group\"")
    # Only the root group carries `attributes` (the `daf` marker), and `walk_zgroup_subtree` skips the root, so this
    # function is always called for a sub-group whose `attrs` are empty under daf-only writes. The branch is kept for
    # correctness against foreign tools that may have decorated a sub-group with attributes.
    if !isempty(group.attrs)
        write(io, ",\"attributes\":")  # UNTESTED
        JSON.print(io, group.attrs)  # UNTESTED
    end
    write(io, UInt8('}'))
    return take!(io)
end

# Atomically rewrite the root `zarr.json` of `daf` with the current attributes plus the cached consolidated metadata
# bytes embedded under `consolidated_metadata.metadata`. Stages to a `.new` sibling and `rename(2)`s on top so
# concurrent readers never observe a torn file. `JSON.print` and `Base.Filesystem.rename` propagate any I/O failure
# as a hard error to the caller.
function write_root_zarr_json!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    @assert storage isa Zarr.DirectoryStore
    group_directory = joinpath(storage.folder, lstrip(daf.root.path, '/'))
    target_path = joinpath(group_directory, "zarr.json")
    staging_path = target_path * ".new"
    open(staging_path, "w") do io
        write(io, "{\"zarr_format\":3,\"node_type\":\"group\",\"attributes\":")
        JSON.print(io, daf.root.attrs)
        write(io, ",\"consolidated_metadata\":{\"kind\":\"inline\",\"must_understand\":false,\"metadata\":")
        write(io, daf.consolidated_metadata_bytes)
        write(io, "}}")
        return nothing
    end
    Base.Filesystem.rename(staging_path, target_path)
    return nothing
end

# Lazy bootstrap of inline consolidated metadata on every `ZarrDaf` open, mirroring `FilesFormat.ensure_metadata_json!`.
# Only the `DirectoryStore` backend has a root `zarr.json` that we rewrite — the ZIP backend's central directory plays
# the same enumeration role, and `DictStore` lives in memory — so this is a no-op for every other backend. Always
# populates `daf.consolidated_metadata_bytes` from the in-memory ZGroup tree so the byte-surgery `register_*` helpers
# observe the correct prior state on the first `set!`. Only writes the on-disk root `zarr.json` if the existing file
# lacks a structurally valid `consolidated_metadata.metadata` field — read-only opens of a directory whose field is
# already present do not mutate the filesystem. If the existing field is missing or torn (e.g. a crashed write or a
# `zip -r`-bundled directory unzipped fresh), the rebuild is best-effort: it fails silently for read-only mode (so a
# frozen filesystem still opens) and rethrows for writable mode.
function ensure_consolidated_metadata!(daf::ZarrDaf)::Nothing
    storage = daf.root.storage
    if !(storage isa Zarr.DirectoryStore)
        return nothing
    end
    daf.consolidated_metadata_bytes = build_consolidated_metadata_bytes(daf.root)
    if root_zarr_has_valid_consolidated_metadata(daf)
        return nothing
    end
    try
        flush_consolidated_metadata!(daf)
    catch
        if daf.mode == "r"  # UNTESTED
            return nothing  # UNTESTED
        end
        rethrow()  # UNTESTED
    end
    return nothing
end

# Read root `zarr.json` and check that it carries a structurally valid inline consolidated metadata field. Used by
# [`ensure_consolidated_metadata!`](@ref) to skip the redundant rewrite when the on-disk file already has the field.
function root_zarr_has_valid_consolidated_metadata(daf::ZarrDaf)::Bool
    storage = daf.root.storage
    raw = storage[daf.root.path, "zarr.json"]
    if raw === nothing
        return false  # UNTESTED
    end
    parsed = try
        JSON.parse(String(copy(raw)); dicttype = Dict{String, Any})::Dict{String, Any}  # NOJET
    catch
        return false  # UNTESTED
    end
    consolidated = get(parsed, "consolidated_metadata", nothing)
    if !(consolidated isa AbstractDict)
        return false
    end
    metadata = get(consolidated, "metadata", nothing)
    return metadata isa AbstractDict
end

function try_mmap_vector_chunk(daf::ZarrDaf, array::ZArray{T})::Maybe{Vector{T}} where {T}
    storage = array.storage
    key = chunk_key(array, single_chunk_vector_suffix(array))
    if storage isa Zarr.DirectoryStore
        chunk_path = joinpath(storage.folder, key)
        if !isfile(chunk_path)
            return nothing
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
            return nothing
        end
        return unsafe_wrap(Array, Ptr{T}(pointer(chunk_bytes)), length(array); own = false)
    end
    return nothing  # UNTESTED
end

function try_mmap_matrix_chunk(daf::ZarrDaf, array::ZArray{T})::Maybe{Matrix{T}} where {T}
    storage = array.storage
    key = chunk_key(array, single_chunk_matrix_suffix(array))
    if storage isa Zarr.DirectoryStore
        chunk_path = joinpath(storage.folder, key)
        if !isfile(chunk_path)
            return nothing
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
            return nothing
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

# Whether `array.storage` (peeled of any `Zarr.ConsolidatedStore` wrapper) is a `Zarr.HTTPStore`. The consolidated
# wrapper holds metadata inline but forwards chunk reads through `parent`, so peeling once suffices.
function is_http_array(array::ZArray)::Bool
    storage = array.storage
    if storage isa Zarr.ConsolidatedStore
        storage = storage.parent
    end
    return storage isa Zarr.HTTPStore
end

# Pick the right `DiskArrays.cache` size budget for `array`: the HTTP cache size for `Zarr.HTTPStore`-backed
# arrays (re-fetch amortisation is expensive over the wire), the local cache size otherwise.
function array_cache_mb(array::ZArray)::Int
    if is_http_array(array)
        return packed_http_cache_mb()
    else
        return packed_local_cache_mb()
    end
end

# Construct the chunk's URL for an `HTTPStore`-backed array. The store URL is the daf root; the array's `path`
# field is the relative group/array path inside the daf; the chunk suffix is the per-chunk file name within the
# array (`Zarr.citostring(...)`).
function http_chunk_url(array::ZArray, chunk_suffix::AbstractString)::String
    storage = array.storage
    if storage isa Zarr.ConsolidatedStore
        storage = storage.parent
    end
    @assert storage isa Zarr.HTTPStore
    return join_url(storage.url, chunk_key(array, chunk_suffix))
end

# Single-chunk filename for a `ZArray` regardless of rank; routes to
# [`single_chunk_vector_suffix`](@ref) / [`single_chunk_matrix_suffix`](@ref) by dimensionality.
function single_chunk_suffix(array::ZArray{T, N})::String where {T, N}
    if N == 1
        return single_chunk_vector_suffix(array)
    else
        @assert N == 2
        return single_chunk_matrix_suffix(array)
    end
end

# Wrap a flat single-chunk uncompressed `Zarr.HTTPStore`-backed array in an `StripedVector` —
# same machinery `HttpDaf` uses for its flat striped path. Returns `nothing` when the property is below the
# chunk-byte threshold (the caller falls through to the `DiskArrays.cache` over the unmodified `ZArray`).
function try_http_striped_vector(array::ZArray{T})::Maybe{AbstractVector{T}} where {T}
    n_elements = length(array)
    chunk_shape = chunks_for(true, (n_elements,), T)
    if chunk_shape === nothing
        return nothing
    else
        url = http_chunk_url(array, single_chunk_vector_suffix(array))
        return StripedVector(T, n_elements, chunk_shape[1], url_byte_fetcher(url))
    end
end

# Matrix counterpart of [`try_http_striped_vector`](@ref): wraps the flat single-chunk uncompressed remote array's
# bytes in an `StripedMatrix` with column-tile stripes when the property crosses the chunk-byte threshold.
function try_http_striped_matrix(array::ZArray{T})::Maybe{AbstractMatrix{T}} where {T}
    n_rows, n_columns = size(array)
    chunk_shape = chunks_for(true, (n_rows, n_columns), T)
    if chunk_shape === nothing
        return nothing
    else
        url = http_chunk_url(array, single_chunk_matrix_suffix(array))
        return StripedMatrix(T, n_rows, n_columns, chunk_shape[1], url_byte_fetcher(url))
    end
end

# Wrap a v3-sharded `Zarr.HTTPStore`-backed array in an `PackedDenseArray` — per-chunk Range-GET read path
# over the single shard URL. Reuses the same factory `HttpDaf` uses for its packed dense properties.
function http_packed_array(array::ZArray{T, N})::ChunkedArray{T, N} where {T, N}
    inner_chunk_shape, codec = packed_codec_from_zarray(array)
    sharding = array.metadata.pipeline.array_bytes
    @assert sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
    url = http_chunk_url(array, single_chunk_suffix(array))
    return PackedDenseArray(
        T,
        size(array),
        inner_chunk_shape,
        codec,
        sharding.index_location,
        url_byte_fetcher(url),
        url_suffix_byte_fetcher(url),
    )
end

# Default fallback wrapper for a 1-D `ZArray`: `DiskArrays.cache` sized per the storage kind (local for local,
# HTTP for HTTP). Used when none of the mmap / striped / packed-Range fast paths apply.
function cached_array_as_vector(array::ZArray{T})::Tuple{StorageVector, Formats.CacheGroup} where {T}
    return (DiskArrays.cache(array; maxsize = array_cache_mb(array)), Formats.MemoryData)
end

# Matrix counterpart of [`cached_array_as_vector`](@ref).
function cached_array_as_matrix(array::ZArray{T})::Tuple{StorageMatrix, Formats.CacheGroup} where {T}
    return (DiskArrays.cache(array; maxsize = array_cache_mb(array)), Formats.MemoryData)
end

# Whether `array_as_vector` / `array_as_matrix` would return a wrapper that defers reads (chunked / packed-Range
# / striped-Range) versus an eagerly readable view (mmap or whole-property GET). Used by the sparse read path
# to decide between `LazySparseVector` / `LazySparseMatrix` and the concrete `SparseVector` / `SparseMatrixCSC`
# construction. The cases (is_packed, mmap, HTTP, on-disk-chunked-non-packed) are mutually exclusive — a v3 sharded
# array is never mmap-eligible, an `HTTPStore`-backed array is never mmap-eligible, etc. — so check order is
# purely cost-driven (cheapest predicate first).
function is_lazy_source(array::ZArray{T})::Bool where {T}
    if isempty(array)
        return false  # UNTESTED
    elseif is_zarr_array_packed(array)
        return true
    elseif can_mmap(array)
        return false
    elseif is_http_array(array)
        return chunks_for(true, size(array), T) !== nothing
    else
        return true
    end
end

# Try the mmap fast path for a 1-D Zarr array; returns `(vector, MappedData)` if the array is mmap-eligible and
# the chunk file is present, `nothing` otherwise. Used by both `array_as_vector` and `array_as_materialized_vector`
# so the mmap detection lives in one place.
function try_mmap_vector(daf::ZarrDaf, array::ZArray{T})::Maybe{Tuple{Vector{T}, Formats.CacheGroup}} where {T}
    if !can_mmap(array) || isempty(array)
        return nothing
    end
    vector = try_mmap_vector_chunk(daf, array)
    if vector === nothing
        return nothing
    else
        return (vector, Formats.MappedData)
    end
end

# Matrix counterpart of [`try_mmap_vector`](@ref).
function try_mmap_matrix(daf::ZarrDaf, array::ZArray{T})::Maybe{Tuple{Matrix{T}, Formats.CacheGroup}} where {T}
    if !can_mmap(array) || isempty(array)
        return nothing
    end
    matrix = try_mmap_matrix_chunk(daf, array)
    if matrix === nothing
        return nothing
    else
        return (matrix, Formats.MappedData)
    end
end

# Materialise a 1-D Zarr array as a concrete `Vector{T}`, used by the sparse-component reader where the consumer
# (`SparseMatrixCSC`) requires a concrete `Vector` rather than an `AbstractVector`. For flat (single-chunk
# uncompressed) arrays this returns the zero-copy mmap view (also a `Vector{T}`); for chunked compressed arrays it
# materialises eagerly via `array[:]`.
function array_as_materialized_vector(daf::ZarrDaf, array::ZArray{T})::Tuple{Vector{T}, Formats.CacheGroup} where {T}
    mmapped = try_mmap_vector(daf, array)
    if mmapped !== nothing
        return mmapped
    else
        return (array[:], Formats.MemoryData)
    end
end

# Wrap a 1-D `ZArray` as an `AbstractVector{T}` for the dense / lazy-sparse read paths. Local arrays go through
# the mmap fast path when flat; `Zarr.HTTPStore`-backed arrays go through the per-chunk-Range-GET path —
# `PackedDenseArray` for v3-sharded arrays, `StripedVector` for flat-and-above-threshold arrays. Anything
# else routes through `DiskArrays.cache` sized per the storage kind.
function array_as_vector(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageVector, Formats.CacheGroup} where {T}
    mmapped = try_mmap_vector(daf, array)
    if mmapped !== nothing
        return mmapped  # NOJET
    elseif !is_http_array(array) || isempty(array)
        return cached_array_as_vector(array)
    elseif is_zarr_array_packed(array)
        return (http_packed_array(array), Formats.MemoryData)
    else
        striped = try_http_striped_vector(array)
        if striped === nothing
            return cached_array_as_vector(array)
        else
            return (striped, Formats.MemoryData)
        end
    end
end

# Matrix counterpart of [`array_as_vector`](@ref).
function array_as_matrix(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageMatrix, Formats.CacheGroup} where {T}
    mmapped = try_mmap_matrix(daf, array)
    if mmapped !== nothing
        return mmapped  # NOJET
    elseif !is_http_array(array) || isempty(array)
        return cached_array_as_matrix(array)
    elseif is_zarr_array_packed(array)
        return (http_packed_array(array), Formats.MemoryData)
    else
        striped = try_http_striped_matrix(array)
        if striped === nothing
            return cached_array_as_matrix(array)
        else
            return (striped, Formats.MemoryData)
        end
    end
end

function can_mmap(array::ZArray{T})::Bool where {T}
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
    register_consolidated_node!(daf, "scalars/$(name)", Vector{UInt8}(JSON.json(array.metadata)))
    flush_consolidated_metadata!(daf)
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
    axis_array[:] = entries  # NOJET

    vectors_axis_group = zgroup(vectors_group(daf), axis)

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

    register_consolidated_node!(daf, "axes/$(axis)", Vector{UInt8}(JSON.json(axis_array.metadata)))
    register_consolidated_node!(daf, "vectors/$(axis)", zgroup_metadata_bytes(vectors_axis_group))
    register_consolidated_subtree!(daf, "matrices/$(axis)", axis_matrices)
    for other_axis in axes
        if other_axis != axis
            cross_group = matrices_group(daf).groups[other_axis].groups[axis]
            register_consolidated_node!(daf, "matrices/$(other_axis)/$(axis)", zgroup_metadata_bytes(cross_group))
        end
    end
    flush_consolidated_metadata!(daf)
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
    is_packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)
    base_key = "vectors/$(axis)/$(name)"

    if vector isa StorageReal
        array = dense_zcreate(typeof(vector), group, name, is_packed, (nelements,))
        write_dense_data!(array, fill(vector, nelements))
        register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
    elseif vector isa AbstractString
        array = dense_zcreate(String, group, name, is_packed, (nelements,))
        write_dense_data!(array, fill(vector, nelements))
        register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
    else
        @assert vector isa AbstractVector
        vector = base_array(vector)
        if eltype(vector) <: AbstractString
            array = dense_zcreate(String, group, name, is_packed, (nelements,))
            write_dense_data!(array, vector)  # NOJET
            register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
        elseif issparse(vector)
            write_sparse_vector(group, name, vector, is_packed)
            register_consolidated_subtree!(daf, base_key, group.groups[name])
        else
            array = dense_zcreate(eltype(vector), group, name, is_packed, (nelements,))
            write_dense_data!(array, vector)
            register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
        end
    end
    flush_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    is_packed::Bool,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)

    array = dense_zcreate(T, group, name, is_packed, (nelements,))
    if can_mmap(array)
        # Flat single-chunk: poke the last element with a non-fill-value (the array's fill_value is `zero(T)`) so
        # Zarr's pipeline does not elide the write and the chunk file actually materialises, then return the
        # mmap-backed view for in-place fill.
        array[nelements] = oneunit(T)
        return array_as_vector(daf, array)
    else
        # Packed (sharded): hand the user an intermediate `Vector{T}` buffer. `format_filled_empty_dense_vector!`
        # flushes it through [`write_packed_shard!`](@ref) so the on-disk shard is dual-format.
        return (Vector{T}(undef, nelements), nothing)
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
    else
        # Packed (sharded): user filled the intermediate `Vector{T}` buffer; push it through the dual writer.
        write_packed_shard!(array, filled)
    end
    register_consolidated_node!(daf, "vectors/$(axis)/$(name)", Vector{UInt8}(JSON.json(array.metadata)))
    flush_consolidated_metadata!(daf)
    return nothing
end

function write_sparse_vector(parent::ZGroup, name::AbstractString, vector::AbstractVector, is_packed::Bool)::Nothing
    vector_group = zgroup(parent, name)

    nzind_vector = nzind(vector)
    nzind_array = dense_zcreate(eltype(nzind_vector), vector_group, "nzind", is_packed, (length(nzind_vector),))
    write_dense_data!(nzind_array, nzind_vector)

    if eltype(vector) != Bool || !all(nzval(vector))
        nzval_vector = nzval(vector)
        nzval_array = dense_zcreate(eltype(nzval_vector), vector_group, "nzval", is_packed, (length(nzval_vector),))
        write_dense_data!(nzval_array, nzval_vector)
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
    _is_packed::Bool,
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
    register_consolidated_subtree!(daf, "vectors/$(axis)/$(name)", vector_group)
    flush_consolidated_metadata!(daf)
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

    nzind_array = vector_group.arrays["nzind"]
    nzval_array = get(vector_group.arrays, "nzval", nothing)

    is_nzind_packed = is_lazy_source(nzind_array)
    is_nzval_packed = nzval_array !== nothing && is_lazy_source(nzval_array)
    if is_nzind_packed || is_nzval_packed
        nzind_source, _ = array_as_vector(daf, nzind_array)
        nzval_source = if nzval_array === nothing
            fill(true, length(nzind_array))
        else
            first(array_as_vector(daf, nzval_array))
        end
        vector = LazySparseVector(nelements, nzind_source, nzval_source)
        return (vector, nothing, Formats.MemoryData)
    end

    nzind_vector, nzind_cache_group = array_as_materialized_vector(daf, nzind_array)
    if nzval_array === nothing
        nzval_vector = fill(true, length(nzind_vector))
        nzval_cache_group = Formats.MemoryData
    else
        nzval_vector, nzval_cache_group = array_as_materialized_vector(daf, nzval_array)
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
    is_packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"

    if matrix isa StorageReal
        array = dense_zcreate(typeof(matrix), group, name, is_packed, (nrows, ncols))
        write_dense_data!(array, fill(matrix, nrows, ncols))
        register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
    elseif matrix isa AbstractString
        array = dense_zcreate(String, group, name, false, (nrows, ncols))
        write_dense_data!(array, fill(matrix, nrows, ncols))
        register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
    elseif eltype(matrix) <: AbstractString
        array = dense_zcreate(String, group, name, false, (nrows, ncols))
        write_dense_data!(array, matrix)  # NOJET
        register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
    else
        @assert matrix isa AbstractMatrix
        @assert major_axis(matrix) != Rows
        matrix = base_array(matrix)
        if issparse(matrix)
            write_sparse_matrix(group, name, matrix, is_packed)
            register_consolidated_subtree!(daf, base_key, group.groups[name])
        else
            array = dense_zcreate(eltype(matrix), group, name, is_packed, (nrows, ncols))
            write_dense_data!(array, matrix)
            register_consolidated_node!(daf, base_key, Vector{UInt8}(JSON.json(array.metadata)))
        end
    end
    flush_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    is_packed::Bool,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    if is_packed && chunks_for(is_packed, (nrows, ncols), T) !== nothing
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
        MmapShardRegion(storage, chunk_key_str, region, UInt64(0), UInt64(0), reserved_size)
    end

    writer = IncrementalShardWriter(sink, sharding_codec.codecs, sharding_codec.index_codecs, zero(T), chunks_per_shard)
    encoder =
        (column::Int, chunk_buffer::Vector{T}) -> begin
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
    register_consolidated_node!(
        daf,
        "matrices/$(rows_axis)/$(columns_axis)/$(name)",
        Vector{UInt8}(JSON.json(array.metadata)),
    )
    flush_consolidated_metadata!(daf)
    return nothing
end

function write_sparse_matrix(parent::ZGroup, name::AbstractString, matrix::AbstractMatrix, is_packed::Bool)::Nothing
    matrix_group = zgroup(parent, name)

    colptr_vector = colptr(matrix)
    colptr_array = dense_zcreate(eltype(colptr_vector), matrix_group, "colptr", is_packed, (length(colptr_vector),))
    write_dense_data!(colptr_array, colptr_vector)

    rowval_vector = rowval(matrix)
    rowval_array = dense_zcreate(eltype(rowval_vector), matrix_group, "rowval", is_packed, (length(rowval_vector),))
    write_dense_data!(rowval_array, rowval_vector)

    if eltype(matrix) != Bool || !all(nzval(matrix))
        nzval_vector = nzval(matrix)
        nzval_array = dense_zcreate(eltype(nzval_vector), matrix_group, "nzval", is_packed, (length(nzval_vector),))
        write_dense_data!(nzval_array, nzval_vector)
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
    _is_packed::Bool,
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
    register_consolidated_subtree!(daf, "matrices/$(rows_axis)/$(columns_axis)/$(name)", matrix_group)
    flush_consolidated_metadata!(daf)
    return nothing
end

function Formats.format_relayout_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
    is_packed::Bool,
)::StorageMatrix
    @assert Formats.has_data_write_lock(daf)
    if eltype(matrix) <: AbstractString
        group = columns_axis_group(daf, columns_axis, rows_axis)
        nrows = axis_length(daf, columns_axis)
        ncols = axis_length(daf, rows_axis)
        relayout_matrix = flipped(matrix)
        array = dense_zcreate(String, group, name, false, (nrows, ncols))
        array[:, :] = relayout_matrix  # NOJET
        register_consolidated_node!(
            daf,
            "matrices/$(columns_axis)/$(rows_axis)/$(name)",
            Vector{UInt8}(JSON.json(array.metadata)),
        )
        flush_consolidated_metadata!(daf)
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
            is_packed,
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
        Formats.format_get_empty_dense_matrix!(daf, columns_axis, rows_axis, name, eltype(matrix), is_packed)
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

    colptr_array = matrix_group.arrays["colptr"]
    rowval_array = matrix_group.arrays["rowval"]
    nzval_array = get(matrix_group.arrays, "nzval", nothing)

    is_rowval_packed = is_lazy_source(rowval_array)
    is_nzval_packed = nzval_array !== nothing && is_lazy_source(nzval_array)
    if is_rowval_packed || is_nzval_packed
        # `colptr` is always materialised at read time even when it lives on disk in packed form: it is small
        # (`sizeof(eltype(colptr)) × (n_columns + 1)` bytes) and slicing needs random access to it.
        colptr_vector, _ = array_as_materialized_vector(daf, colptr_array)
        rowval_source, _ = array_as_vector(daf, rowval_array)
        nzval_source = if nzval_array === nothing
            fill(true, length(rowval_array))
        else
            first(array_as_vector(daf, nzval_array))
        end
        matrix = LazySparseMatrix(nrows, colptr_vector, rowval_source, nzval_source)
        return (matrix, nothing, Formats.MemoryData)
    end

    colptr_vector, colptr_cache_group = array_as_materialized_vector(daf, colptr_array)
    rowval_vector, rowval_cache_group = array_as_materialized_vector(daf, rowval_array)
    if nzval_array === nothing
        nzval_vector = fill(true, length(rowval_vector))
        nzval_cache_group = Formats.MemoryData
    else
        nzval_vector, nzval_cache_group = array_as_materialized_vector(daf, nzval_array)
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

function child_zarr_path(group::ZGroup, name::AbstractString)::String
    return isempty(group.path) ? String(name) : rstrip(group.path, '/') * '/' * String(name)
end

function reopen_zgroup_child!(group::ZGroup, name::AbstractString)::Nothing
    child = Zarr.zopen_noerr(
        group.storage,
        "w",
        Zarr.ZarrFormat(3);
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
            axis_array[:] = planned_axis.new_entries  # NOJET
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
        permuted = Vector{eltype(source_vector)}(undef, length(source_vector))
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
        permuted = Matrix{eltype(source_matrix)}(undef, nrows, ncols)
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

end  # module
