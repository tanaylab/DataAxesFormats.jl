"""
A memory-mapped, append-only Zarr storage backend implemented over a single ZIP archive.

This module provides [`MmapZipStore`](@ref), a `Zarr.AbstractStore` subtype that can back a
[`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf) (or, in principle, any Zarr array) with a ZIP
file on the local filesystem. It serves two complementary use cases:

  - **Reading** any valid Zarr v2 ZIP archive (including archives produced by foreign tools such as
    Python's `zarr` package), subject to Zarr.jl's existing support for data types, filters, and
    compressors. Stored (method `0`) entries are returned as zero-copy memory-mapped byte ranges;
    deflate-compressed (method `8`) and deflate64-compressed (method `9`) entries are decompressed
    on demand via `ZipArchives.jl`. Any other compression method raises a clear `ArgumentError`
    from `ZipArchives.jl` on first access. In practice Zarr-ZIPs in the wild are overwhelmingly
    method `0` (since the chunks are already compressed internally) or method `8`.

  - **Creating and appending to** a ZIP archive written by this package. Writes use stored (method
    `0`) uncompressed entries exclusively, so chunk data can be memory-mapped for direct access.
    Entries may only be appended; existing entries cannot be modified or deleted.

# Shared mmap

On open, `MmapZipStore` memory-maps the archive file once into a single `Vector{UInt8}` owned by
the store. A read-only open uses an ordinary file-backed `mmap` covering exactly the current file
size. A writable open uses a two-step mapping that keeps the virtual address of the archive stable
across file growth: first, `max_file_size` bytes of virtual address space are reserved via an
anonymous `PROT_NONE` mapping (which consumes VA only — zero RAM, zero disk, zero file bytes); then
the file is overlaid onto the first `filesize` bytes of that reservation via `MAP_SHARED | MAP_FIXED`.
Each append calls `ftruncate` to extend the real (non-sparse) file, followed by a re-overlay with
`MAP_SHARED | MAP_FIXED` at the same base address to extend the accessible portion of the
reservation to the new file size. Subsequent writes (local file header, data, central directory,
end-of-central-directory, CRC32 patches) are pure stores into `store.file_mmap`, not `write()`
syscalls. The only writes through the IO stream are the initial bootstrap of an empty archive and
`ftruncate` calls. Each open therefore consumes a single reservation plus one file overlay
regardless of entry count, and every stored (method-`0`) entry is served directly out of the shared
mapping with no copy. The file on disk remains a normal, non-sparse file of exactly `filesize`
bytes — copying the archive with ordinary tools does not inflate to `max_file_size`.

# On-disk protocol

`MmapZipStore` uses a two-step commit protocol that leaves the archive in a valid ZIP and valid
Zarr state after every append, with no need to wait for a final close:

 1. For each append, the file is extended via `ftruncate` to its new end-of-archive position. The
    new central directory (containing both the pre-existing and the new entries) and its
    end-of-central-directory record are built in memory and copied into the mmap at the offset
    where the new local file header region will end. This is the commit point: after this copy,
    the archive on disk describes the new entry, and the local file header region lies in a
    sparse hole in the file (the bytes zero-initialized by `ftruncate`).

 2. The new local file header is then copied at the offset that was previously occupied by the
    old central directory (and end-of-central-directory record), and the entry's stored data
    bytes are copied immediately after it. These copies may overlap what used to be the old
    central directory: that is safe, because step 1 already committed the superseding copy to a
    higher offset in the file.

If the process crashes between step 1 and step 2, the committed central directory claims an entry
whose local file header is still partly (or entirely) missing or whose data's CRC32 does not match
the recorded value. The next write-mode open detects this by validating the tail of the central
directory from back to front; the first trailing run of invalid entries is rolled back by writing
a new central directory and end-of-central-directory record at the oldest corrupt entry's local
header offset, and `ftruncate`-ing the file to the new end-of-central-directory.

# Two-phase append for `get_empty_*`

The store exposes [`reserve_mmap_zip_entry!`](@ref) and [`patch_mmap_zip_entry_crc!`](@ref) to
support Daf's two-phase `get_empty_*` / `filled_empty!` pattern without buffering gigabytes of
zeros in memory. [`reserve_mmap_zip_entry!`](@ref) runs the full commit protocol with a CRC32
placeholder of `0` and returns a byte view over the data region in the shared mmap (a file hole
until the user writes into it). [`patch_mmap_zip_entry_crc!`](@ref) then computes the real CRC32
from the now-filled data and patches the CRC32 field in both the local file header and the central
directory via two four-byte stores into the shared mmap.

If the process crashes between `reserve_mmap_zip_entry!` and `patch_mmap_zip_entry_crc!`, the
recovery pass on the next write-mode open discards the partial entries because their stored CRC32
placeholders of `0` do not match the actual data.

# Aligned data offsets

Every local file header written by `MmapZipStore` is padded (via a second opaque ZIP extra field)
so that the following data region starts at a `DAF_DATA_OFFSET_ALIGNMENT`-byte-aligned file offset.
This lets readers wrap the data region as an `Array{T}` of the appropriate element type via
`unsafe_wrap` with no copy. [`try_mmap_entry_as`](@ref) performs the alignment check at read time
and returns `nothing` for unaligned foreign archives, in which case the caller should fall back to
the ordinary decoded copy from `store[key]`.

# Limitations

Cross-process writers to the same ZIP archive are **not** supported and will corrupt the archive.
Concurrent access from multiple threads within the same process is not supported either — the
store mutates its in-memory entry tables during appends without any internal locking, matching the
thread-safety conventions of `Zarr.jl`'s other built-in stores (`DirectoryStore`, `DictStore`,
`ZipStore`). A higher-level writer lock (such as the one held by
[`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf)) is assumed to serialize writes. Concurrent
readers across threads, as long as no writer is active at the same time, are safe: the commit
protocol leaves a valid on-disk archive at every commit point.

All archives produced on write are ZIP64 archives: every local file header and every central
directory entry carries the ZIP64 extended information extra field, and the archive always ends
with a ZIP64 end-of-central-directory record, a ZIP64 end-of-central-directory locator, and a
legacy end-of-central-directory record (whose size/count fields are set to the ZIP64 sentinel
values). This accommodates the multi-gigabyte chunks and many-thousand-entry archives that are
routine for large Daf data sets, at the cost of ~28 bytes per entry in the central directory and a
98-byte trailing record region instead of the legacy 22-byte record. Modern ZIP readers (Info-ZIP,
Python `zipfile`, 7-Zip, `ZipArchives.jl`, Java, .NET) all handle this transparently.
"""
module MmapZipStores

export MmapZipStore
export patch_mmap_zip_entry_crc!
export reserve_mmap_zip_entry!
export try_mmap_entry_as

using TanayLabUtilities
using Zarr
using ZipArchives

import Zarr.isinitialized
import Zarr.storagesize
import Zarr.storefromstring
import Zarr.subdirs
import Zarr.subkeys

const LOCAL_FILE_HEADER_SIGNATURE = UInt32(0x04034b50)
const CENTRAL_DIRECTORY_ENTRY_SIGNATURE = UInt32(0x02014b50)
const END_OF_CENTRAL_DIRECTORY_SIGNATURE = UInt32(0x06054b50)
const ZIP64_END_OF_CENTRAL_DIRECTORY_SIGNATURE = UInt32(0x06064b50)
const ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE = UInt32(0x07064b50)
const END_OF_CENTRAL_DIRECTORY_SIZE = 22
const ZIP64_END_OF_CENTRAL_DIRECTORY_SIZE = 56
const ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIZE = 20
const TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE =
    ZIP64_END_OF_CENTRAL_DIRECTORY_SIZE + ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIZE + END_OF_CENTRAL_DIRECTORY_SIZE
const LOCAL_FILE_HEADER_FIXED_SIZE = 30
const CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE = 46
const ZIP64_EXTRA_HEADER_ID = UInt16(0x0001)
# header_id(2) + data_size(2) + uncompressed_size(8) + compressed_size(8)
const ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE = 20
# header_id(2) + data_size(2) + uncompressed_size(8) + compressed_size(8) + local_file_header_offset(8)
const ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE = 28
const STORED_COMPRESSION_METHOD = UInt16(0)
const ZIP64_VERSION_NEEDED = UInt16(45)
# Custom extra field ID used only to pad each local file header so that the following data region
# starts at an 8-byte-aligned file offset. Unknown to every reader but `MmapZipStores`, and ignored
# as an opaque extra field per the ZIP APPNOTE.
const DAF_PADDING_EXTRA_HEADER_ID = UInt16(0xDAF1)
const DAF_DATA_OFFSET_ALIGNMENT = 8

# An in-memory description of a single ZIP archive entry tracked by `MmapZipStore`. The fields
# mirror the minimal ZIP central directory information needed to locate and validate an entry.
# `data_offset` is populated eagerly (for both initial and appended entries) so that the read path
# does not need to mutate shared state. `central_directory_offset` is updated after every append,
# because each append rewrites the central directory at a new (higher) offset.
mutable struct ZipEntry
    name::String
    local_file_header_offset::UInt64
    data_offset::UInt64
    compressed_size::UInt64
    uncompressed_size::UInt64
    crc32::UInt32
    compression_method::UInt16
    central_directory_offset::UInt64
end

"""
    MmapZipStore(
        path::AbstractString;
        [writable::Bool = false,
        create::Bool = false,
        truncate::Bool = false,
        max_file_size::Integer = 1 << 40]
    )

Open (and optionally create or truncate) a ZIP archive at `path` as a Zarr store.

The `writable`, `create`, and `truncate` flags interact as follows (matching
[`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf)'s `r`/`r+`/`w+`/`w` modes):

| `writable` | `create` | `truncate` | Behavior                                                          |
|:---------- |:-------- |:---------- |:----------------------------------------------------------------- |
| `false`    | `false`  | `false`    | Read-only open of an existing archive (mode `r`)                  |
| `true`     | `false`  | `false`    | Read/write open of an existing archive (mode `r+`)                |
| `true`     | `true`   | `false`    | Read/write open, creating an empty archive if missing (mode `w+`) |
| `true`     | `true`   | `true`     | Discard any existing archive and create an empty one (mode `w`)   |

On a writable open, the store reserves `max_file_size` bytes of virtual address space via a single
anonymous `PROT_NONE` mapping and overlays the file onto the first `filesize` bytes of that
reservation (`MAP_SHARED | MAP_FIXED`). Each append calls `ftruncate` to grow the file by exactly
the bytes needed (real, non-sparse) and re-overlays the file at the same base address to extend
the accessible portion of the reservation. Reads slice into this single mapping, so the number of
VMAs per open is small and fixed regardless of entry count. An append that would grow the file
past `max_file_size` fails with an explicit error. Read-only opens memory-map exactly the current
file size and ignore `max_file_size`.

On open, the existing central directory is parsed and cached in memory. On a write-mode open, an
interrupted tail of the central directory (entries whose local file header or CRC32 does not
validate) is detected and rolled back; see the module documentation for the full protocol.
"""
mutable struct MmapZipStore <: Zarr.AbstractStore
    path::String
    io_stream::IOStream
    is_writable::Bool
    max_file_size::UInt64
    reservation_base::Ptr{Cvoid}
    reservation_length::UInt64
    overlay_length::UInt64
    file_mmap::Vector{UInt8}
    entries::Vector{ZipEntry}
    name_to_index::Dict{String, Int}
    central_directory_offset::UInt64
    central_directory_size::UInt64
end

# Platform-specific MAP_ANON flag value for `mmap(2)` (Darwin uses 0x1000; Linux and other BSDs use 0x20).
@static if Sys.isapple()
    const MAP_ANON_FLAG = Cint(0x1000)
else
    const MAP_ANON_FLAG = Cint(0x20)
end
const MAP_PRIVATE_FLAG = Cint(0x02)
const MAP_SHARED_FLAG = Cint(0x01)
const MAP_FIXED_FLAG = Cint(0x10)
const PROT_NONE_FLAG = Cint(0x00)
const PROT_READ_FLAG = Cint(0x01)
const PROT_WRITE_FLAG = Cint(0x02)

# Reserve `byte_length` bytes of virtual address space as an anonymous PROT_NONE mapping. Consumes
# VA only: no RAM, no disk, no file size. Accessing the returned range raises SIGSEGV until it is
# overlaid with a file mapping via `overlay_file_on_range!`. On first failure (typically because
# abandoned-but-not-yet-GC'd reservations from prior opens have filled the per-process VA), force a
# GC cycle to finalize those and retry once.
function reserve_virtual_range(byte_length::Integer)::Ptr{Cvoid}  # FLAKY TESTED
    ptr = raw_reserve_virtual_range(byte_length)
    if reinterpret(Int, ptr) == -1
        GC.gc()  # UNTESTED
        ptr = raw_reserve_virtual_range(byte_length)  # UNTESTED
        if reinterpret(Int, ptr) == -1  # UNTESTED
            error("mmap reservation of $(byte_length) bytes failed: $(Base.Libc.strerror())")  # UNTESTED
        end
    end
    return ptr
end

@inline function raw_reserve_virtual_range(byte_length::Integer)::Ptr{Cvoid}  # FLAKY TESTED
    return ccall(  # NOJET
        :mmap,
        Ptr{Cvoid},
        (Ptr{Cvoid}, Csize_t, Cint, Cint, Cint, Int64),
        Ptr{Cvoid}(0),
        Csize_t(byte_length),
        PROT_NONE_FLAG,
        MAP_PRIVATE_FLAG | MAP_ANON_FLAG,
        Cint(-1),
        Int64(0),
    )
end

# Overlay the file backing `io_stream` onto `byte_length` bytes starting at `base`, replacing any
# existing mapping in that range. `base` must be inside a region previously obtained from
# `reserve_virtual_range`, and `byte_length` must be <= filesize(io_stream) (macOS rejects
# file-backed MAP_SHARED mappings whose length exceeds the file size).
function overlay_file_on_range!(base::Ptr{Cvoid}, io_stream::IOStream, byte_length::Integer)::Nothing  # FLAKY TESTED
    file_desc = Cint(reinterpret(Int32, fd(io_stream)))
    ptr = ccall(  # NOJET
        :mmap,
        Ptr{Cvoid},
        (Ptr{Cvoid}, Csize_t, Cint, Cint, Cint, Int64),
        base,
        Csize_t(byte_length),
        PROT_READ_FLAG | PROT_WRITE_FLAG,
        MAP_SHARED_FLAG | MAP_FIXED_FLAG,
        file_desc,
        Int64(0),
    )
    if reinterpret(Int, ptr) == -1 || ptr != base
        error("mmap file overlay at $(base) for $(byte_length) bytes failed: $(Base.Libc.strerror())")  # UNTESTED
    end
    return nothing
end

# Tell the kernel not to coalesce 4 KiB pages into 2 MiB transparent huge pages across `byte_length`
# bytes at `base`. The advice is recorded on the VMA and applies to all future file overlays within
# the reservation. No-op on non-Linux platforms. See `mmap_with_small_pages` for the rationale.
function disable_transparent_huge_pages(base::Ptr{Cvoid}, byte_length::Integer)::Nothing  # FLAKY TESTED
    @static if Sys.islinux()
        if byte_length > 0
            MADV_NOHUGEPAGE = Cint(15)
            result = ccall(:madvise, Cint, (Ptr{Cvoid}, Csize_t, Cint), base, Csize_t(byte_length), MADV_NOHUGEPAGE)
            if result != 0
                error("madvise MADV_NOHUGEPAGE failed: $(Base.Libc.strerror())")  # FLAKY TESTED
            end
        end
    end
    return nothing
end

function Base.show(io::IO, store::MmapZipStore)::Nothing  # FLAKY TESTED
    print(io, "MmapZipStore($(store.path))")
    return nothing
end

function MmapZipStore(
    path::AbstractString;
    writable::Bool = false,
    create::Bool = false,
    truncate::Bool = false,
    max_file_size::Integer = 1 << 40,
)::MmapZipStore
    if truncate
        @assert writable
        @assert create
        rm(path; force = true)
    end

    if !isfile(path)
        if !create
            error("zip file does not exist: $(path)")
        end
        @assert writable
        bootstrap_io = open(path, "w+")
        try
            write_empty_zip_archive!(bootstrap_io)
        finally
            close(bootstrap_io)
        end
    end

    io_stream = open(path, writable ? "r+" : "r")
    store = try
        parse_existing_zip_archive(String(path), io_stream, writable, UInt64(max_file_size))
    catch exception
        close(io_stream)  # UNTESTED
        rethrow(exception)  # UNTESTED
    end

    if writable
        recover_interrupted_appends!(store)
    end

    return store
end

function write_empty_zip_archive!(io_stream::IOStream)::Nothing  # FLAKY TESTED
    header_buffer = Vector{UInt8}(undef, TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    write_end_of_central_directory_region!(header_buffer, 1, UInt64(0), UInt64(0), UInt64(0))
    write(io_stream, header_buffer)
    flush(io_stream)
    return nothing
end

function parse_existing_zip_archive(
    path::String,
    io_stream::IOStream,
    is_writable::Bool,
    max_file_size::UInt64,
)::MmapZipStore
    file_size = UInt64(filesize(path))
    if file_size < END_OF_CENTRAL_DIRECTORY_SIZE
        error("file too small to be a zip archive: $(path)")  # UNTESTED
    end

    if is_writable
        if file_size > max_file_size
            error(  # UNTESTED
                "zip file size: $(file_size) exceeds max_file_size: $(max_file_size) for writable open: $(path)",
            )
        end
        reservation_base = reserve_virtual_range(Int(max_file_size))
        overlay_file_on_range!(reservation_base, io_stream, Int(file_size))
        disable_transparent_huge_pages(reservation_base, Int(max_file_size))
        file_mmap = unsafe_wrap(Array, Ptr{UInt8}(reservation_base), Int(max_file_size); own = false)
        finalizer(file_mmap) do _
            return ccall(:munmap, Cint, (Ptr{Cvoid}, Csize_t), reservation_base, Csize_t(max_file_size))
        end
        reservation_length = max_file_size
        overlay_length = file_size
    else
        reservation_base = Ptr{Cvoid}(0)
        reservation_length = UInt64(0)
        overlay_length = file_size
        file_mmap = mmap_with_small_pages(io_stream, Vector{UInt8}, Int(file_size), 0; grow = false)
    end

    zip_view = view(file_mmap, 1:Int(file_size))
    zip_reader = ZipArchives.ZipReader(zip_view)
    entry_count = ZipArchives.zip_nentries(zip_reader)

    entries = Vector{ZipEntry}(undef, entry_count)
    name_to_index = Dict{String, Int}()
    for entry_index in 1:entry_count
        name = ZipArchives.zip_name(zip_reader, entry_index)
        entries[entry_index] = ZipEntry(
            name,
            UInt64(zip_reader.entries[entry_index].offset),
            UInt64(ZipArchives.zip_entry_data_offset(zip_reader, entry_index)),
            UInt64(ZipArchives.zip_compressed_size(zip_reader, entry_index)),
            UInt64(ZipArchives.zip_uncompressed_size(zip_reader, entry_index)),
            ZipArchives.zip_stored_crc32(zip_reader, entry_index),
            ZipArchives.zip_compression_method(zip_reader, entry_index),
            UInt64(0),
        )
        name_to_index[name] = entry_index
    end

    central_directory_offset = UInt64(zip_reader.central_dir_offset)
    central_directory_size = UInt64(length(zip_reader.central_dir_buffer))
    populate_central_directory_offsets!(entries, central_directory_offset)

    return MmapZipStore(
        path,
        io_stream,
        is_writable,
        max_file_size,
        reservation_base,
        reservation_length,
        overlay_length,
        file_mmap,
        entries,
        name_to_index,
        central_directory_offset,
        central_directory_size,
    )
end

function populate_central_directory_offsets!(entries::Vector{ZipEntry}, central_directory_offset::UInt64)::Nothing  # FLAKY TESTED
    current_offset = central_directory_offset
    for entry in entries
        entry.central_directory_offset = current_offset
        current_offset += central_directory_entry_size(entry)
    end
    return nothing
end

@inline function central_directory_entry_size(entry::ZipEntry)::UInt64  # FLAKY TESTED
    return UInt64(CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE) +
           UInt64(ncodeunits(entry.name)) +
           UInt64(ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE)
end

# Extend the real (non-sparse) file to `new_file_size` bytes and re-overlay the file onto the first
# `new_file_size` bytes of `store.reservation_base`. Works for both growth and shrinkage: the file's
# first `min(old, new)` bytes are untouched, and the overlay ends up covering exactly `new_file_size`
# bytes of the reservation (the remainder reverts to the original PROT_NONE reservation).
function resize_file_overlay!(store::MmapZipStore, new_file_size::UInt64)::Nothing  # FLAKY TESTED
    @assert store.is_writable
    @assert new_file_size <= store.max_file_size
    truncate(store.io_stream, Int(new_file_size))
    overlay_file_on_range!(store.reservation_base, store.io_stream, Int(new_file_size))
    store.overlay_length = new_file_size
    return nothing
end

# Number of bytes to append to the local file header (as a second, opaque extra field) so that the
# following data region starts at a `DAF_DATA_OFFSET_ALIGNMENT`-byte-aligned file offset. Returns 0
# when already aligned, or a value >= 4 (the minimum extra field header size) otherwise.
@inline function compute_alignment_padding(unpadded_data_offset::UInt64)::Int  # FLAKY TESTED
    remainder = Int(mod(unpadded_data_offset, UInt64(DAF_DATA_OFFSET_ALIGNMENT)))
    if remainder == 0
        return 0
    end
    total_mod = mod(-remainder, DAF_DATA_OFFSET_ALIGNMENT)
    return total_mod >= 4 ? total_mod : total_mod + DAF_DATA_OFFSET_ALIGNMENT
end

function Base.getindex(store::MmapZipStore, key::AbstractString)::Union{Nothing, AbstractVector{UInt8}}  # FLAKY TESTED
    entry_index = get(store.name_to_index, key, nothing)
    if entry_index === nothing
        return nothing
    end
    return read_entry_bytes(store, entry_index)
end

"""
    try_mmap_entry_as(
        store::MmapZipStore,
        key::AbstractString,
        ::Type{T},
        dims::Union{Integer, Tuple{Vararg{Integer}}},
    )::Union{Nothing, Array{T}} where {T}

If the entry named `key` exists in `store`, is held uncompressed (stored, method `0`), has exactly
the byte size implied by `T` and `dims`, and its data region is suitably aligned for `T`, return a
zero-copy `Array{T}` of shape `dims` viewing the mmap'd data region directly. Return `nothing`
otherwise (absent, compressed, wrong size, or unaligned) and let the caller fall back to the
ordinary decoded copy from `store[key]`.

For archives produced by `MmapZipStore` itself, the alignment precondition always holds: every
local file header is padded so the data region starts at an `DAF_DATA_OFFSET_ALIGNMENT`-byte-aligned
file offset, which matches the alignment required by every Daf element type. Foreign archives may
produce misaligned data offsets, in which case this returns `nothing`.

The returned array aliases `store.file_mmap` and remains valid as long as `store` is open.
"""
function try_mmap_entry_as(
    store::MmapZipStore,
    key::AbstractString,
    ::Type{T},
    dims::Union{Integer, Tuple{Vararg{Integer}}},
)::Union{Nothing, Array{T}} where {T}
    entry_index = get(store.name_to_index, key, nothing)
    if entry_index === nothing
        return nothing
    end
    entry = store.entries[entry_index]
    if entry.compression_method != STORED_COMPRESSION_METHOD
        return nothing
    end
    element_count = UInt64(dims isa Integer ? dims : prod(dims))
    expected_bytes = element_count * UInt64(sizeof(T))
    if entry.compressed_size != expected_bytes
        return nothing  # UNTESTED
    end
    if element_count == 0
        return Array{T}(undef, dims)  # UNTESTED
    end
    data_pointer = pointer(store.file_mmap, Int(entry.data_offset) + 1)
    if UInt(data_pointer) % UInt(DAF_DATA_OFFSET_ALIGNMENT) != 0
        return nothing  # UNTESTED
    end
    return unsafe_wrap(Array, Ptr{T}(data_pointer), dims; own = false)
end

# Return the raw data bytes for the entry at the given index. For stored (method-0) entries the
# result is an mmap-backed zero-copy view; for deflate / deflate64 entries the result is an
# in-memory decompressed copy from `ZipArchives.jl`. Any other method raises `ArgumentError` from
# `ZipArchives.jl`.
function read_entry_bytes(store::MmapZipStore, entry_index::Int)::AbstractVector{UInt8}  # FLAKY TESTED
    entry = store.entries[entry_index]
    if entry.compression_method == STORED_COMPRESSION_METHOD
        return mmap_stored_entry_bytes(store, entry_index)
    end
    return read_compressed_entry_bytes(store, entry_index)
end

function mmap_stored_entry_bytes(store::MmapZipStore, entry_index::Int)::AbstractVector{UInt8}  # FLAKY TESTED
    entry = store.entries[entry_index]
    if entry.compressed_size == 0
        return UInt8[]
    end
    start_position = Int(entry.data_offset) + 1
    end_position = start_position + Int(entry.compressed_size) - 1
    return view(store.file_mmap, start_position:end_position)
end

function read_compressed_entry_bytes(store::MmapZipStore, entry_index::Int)::Vector{UInt8}  # FLAKY TESTED
    zip_view = view(store.file_mmap, 1:Int(store.overlay_length))
    zip_reader = ZipArchives.ZipReader(zip_view)
    return ZipArchives.zip_readentry(zip_reader, entry_index)
end

function Zarr.subkeys(store::MmapZipStore, prefix_path::AbstractString)::Vector{String}  # FLAKY TESTED
    return collect_names_under_prefix(store, prefix_path, :file)
end

function Zarr.subdirs(store::MmapZipStore, prefix_path::AbstractString)::Vector{String}  # FLAKY TESTED
    return collect_names_under_prefix(store, prefix_path, :directory)
end

function collect_names_under_prefix(store::MmapZipStore, prefix_path::AbstractString, kind::Symbol)::Vector{String}
    normalized_prefix =
        isempty(prefix_path) || endswith(prefix_path, '/') ? String(prefix_path) : String(prefix_path) * "/"
    prefix_length = ncodeunits(normalized_prefix)

    collected_names = Set{String}()
    for entry in store.entries
        name = entry.name
        if startswith(name, normalized_prefix) && !endswith(name, '/')
            tail = SubString(name, prefix_length + 1)
            slash_position = findfirst('/', tail)
            if kind === :file && slash_position === nothing
                push!(collected_names, String(tail))
            elseif kind === :directory && slash_position !== nothing
                push!(collected_names, String(SubString(tail, 1, slash_position - 1)))
            end
        end
    end
    return collect(collected_names)
end

function Zarr.storagesize(store::MmapZipStore, prefix_path::AbstractString)::Int64
    normalized_prefix =
        isempty(prefix_path) || endswith(prefix_path, '/') ? String(prefix_path) : String(prefix_path) * "/"
    prefix_length = ncodeunits(normalized_prefix)

    total_size = Int64(0)
    for entry in store.entries
        name = entry.name
        if startswith(name, normalized_prefix)
            tail = SubString(name, prefix_length + 1)
            if !(tail in (".zattrs", ".zarray", ".zgroup"))
                total_size += Int64(entry.uncompressed_size)
            end
        end
    end
    return total_size
end

function Zarr.isinitialized(store::MmapZipStore, key::AbstractString)::Bool  # FLAKY TESTED
    return haskey(store.name_to_index, key)
end

function Base.haskey(store::MmapZipStore, key::AbstractString)::Bool  # FLAKY TESTED
    return haskey(store.name_to_index, key)
end

function Base.setindex!(store::MmapZipStore, value::Any, key::AbstractString)::Any  # FLAKY TESTED
    data_bytes = coerce_bytes_for_append(value)
    append_entry!(store, String(key), data_bytes)
    return value
end

function coerce_bytes_for_append(value::AbstractVector{UInt8})::Vector{UInt8}
    return value isa Vector{UInt8} ? value : Vector{UInt8}(value)
end

function append_entry!(store::MmapZipStore, key::String, data_bytes::Vector{UInt8})::Nothing
    @assert store.is_writable
    if haskey(store.name_to_index, key)
        error("MmapZipStore is append-only; cannot overwrite entry: $(key)")
    end
    name_bytes = Vector{UInt8}(codeunits(key))
    check_append_limits(store, name_bytes, length(data_bytes))
    computed_crc32 = ZipArchives.zip_crc32(data_bytes)
    entry = commit_new_entry!(store, key, name_bytes, computed_crc32, UInt64(length(data_bytes)))
    if length(data_bytes) > 0
        @inbounds copyto!(store.file_mmap, Int(entry.data_offset) + 1, data_bytes, 1, length(data_bytes))
    end
    return nothing
end

function check_append_limits(_::MmapZipStore, name_bytes::Vector{UInt8}, _::Integer)::Nothing  # FLAKY TESTED
    if length(name_bytes) > typemax(UInt16)
        error("zip entry name too long: $(length(name_bytes)) bytes")
    end
    return nothing
end

function commit_new_entry!(
    store::MmapZipStore,
    name::String,
    name_bytes::Vector{UInt8},
    crc32_value::UInt32,
    data_size::UInt64,
)::ZipEntry
    local_file_header_offset = store.central_directory_offset
    base_local_file_header_size =
        UInt64(LOCAL_FILE_HEADER_FIXED_SIZE) + UInt64(length(name_bytes)) + UInt64(ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE)
    alignment_padding = compute_alignment_padding(local_file_header_offset + base_local_file_header_size)
    local_file_header_size = base_local_file_header_size + UInt64(alignment_padding)
    data_offset = local_file_header_offset + local_file_header_size
    new_central_directory_offset = data_offset + data_size

    new_entry_record_size =
        UInt64(CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE) +
        UInt64(length(name_bytes)) +
        UInt64(ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE)
    new_central_directory_size = store.central_directory_size + new_entry_record_size

    required_file_size =
        new_central_directory_offset +
        new_central_directory_size +
        UInt64(TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    if required_file_size > store.max_file_size
        error(
            "append of: $(name) would grow zip file past max_file_size: $(store.max_file_size) bytes " *
            "(required: $(required_file_size) bytes) in: $(store.path)",
        )
    end

    new_entry = ZipEntry(
        name,
        local_file_header_offset,
        data_offset,
        data_size,
        data_size,
        crc32_value,
        STORED_COMPRESSION_METHOD,
        UInt64(0),
    )
    push!(store.entries, new_entry)
    store.name_to_index[name] = length(store.entries)

    commit_buffer =
        Vector{UInt8}(undef, Int(new_central_directory_size) + TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    write_central_directory_and_eocd!(
        commit_buffer,
        store.entries,
        new_central_directory_offset,
        new_central_directory_size,
    )

    local_file_header_buffer = Vector{UInt8}(undef, Int(local_file_header_size))
    write_local_file_header!(
        local_file_header_buffer,
        1,
        name_bytes,
        data_size,
        data_size,
        crc32_value,
        alignment_padding,
    )

    resize_file_overlay!(store, required_file_size)

    @inbounds copyto!(store.file_mmap, Int(new_central_directory_offset) + 1, commit_buffer, 1, length(commit_buffer))
    @inbounds copyto!(
        store.file_mmap,
        Int(local_file_header_offset) + 1,
        local_file_header_buffer,
        1,
        length(local_file_header_buffer),
    )

    store.central_directory_offset = new_central_directory_offset
    store.central_directory_size = new_central_directory_size
    populate_central_directory_offsets!(store.entries, store.central_directory_offset)
    return new_entry
end

# Materialize central directory entries for every `ZipEntry` in `entries`, followed by the ZIP64
# end-of-central-directory record, ZIP64 locator, and legacy end-of-central-directory record, into
# `commit_buffer`. Single point where a valid ZIP central directory is produced on disk: both
# `commit_new_entry!` (append) and `roll_back_to_entry!` (recovery) call into it. Legacy record is
# always emitted with ZIP64 sentinel values (0xFFFFFFFF / 0xFFFF).
function write_central_directory_and_eocd!(
    commit_buffer::Vector{UInt8},
    entries::Vector{ZipEntry},
    central_directory_offset::UInt64,
    central_directory_size::UInt64,
)::Nothing
    position = 1
    for entry in entries
        name_bytes = Vector{UInt8}(codeunits(entry.name))
        write_central_directory_entry!(
            commit_buffer,
            position,
            name_bytes,
            entry.compressed_size,
            entry.uncompressed_size,
            entry.crc32,
            entry.local_file_header_offset,
        )
        position += CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + length(name_bytes) + ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE
    end
    write_end_of_central_directory_region!(
        commit_buffer,
        Int(central_directory_size) + 1,
        central_directory_offset,
        central_directory_size,
        UInt64(length(entries)),
    )
    return nothing
end

function write_end_of_central_directory_region!(
    buffer::Vector{UInt8},
    position::Int,
    central_directory_offset::UInt64,
    central_directory_size::UInt64,
    total_entry_count::UInt64,
)::Nothing
    zip64_end_of_central_directory_offset = central_directory_offset + central_directory_size
    write_zip64_end_of_central_directory!(
        buffer,
        position,
        central_directory_offset,
        central_directory_size,
        total_entry_count,
    )
    write_zip64_end_of_central_directory_locator!(
        buffer,
        position + ZIP64_END_OF_CENTRAL_DIRECTORY_SIZE,
        zip64_end_of_central_directory_offset,
    )
    write_end_of_central_directory!(
        buffer,
        position + ZIP64_END_OF_CENTRAL_DIRECTORY_SIZE + ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIZE,
    )
    return nothing
end

function write_central_directory_entry!(
    buffer::Vector{UInt8},
    position::Int,
    name_bytes::Vector{UInt8},
    compressed_size::UInt64,
    uncompressed_size::UInt64,
    crc32_value::UInt32,
    local_file_header_offset::UInt64,
)::Nothing
    write_little_endian_uint32!(buffer, position, CENTRAL_DIRECTORY_ENTRY_SIGNATURE)
    write_little_endian_uint16!(buffer, position + 4, UInt16(0x031e))                # version made by (Unix v3.0)
    write_little_endian_uint16!(buffer, position + 6, ZIP64_VERSION_NEEDED)          # version needed to extract (ZIP64)
    write_little_endian_uint16!(buffer, position + 8, UInt16(1 << 11))               # general purpose flag: UTF-8 name
    write_little_endian_uint16!(buffer, position + 10, STORED_COMPRESSION_METHOD)
    write_little_endian_uint16!(buffer, position + 12, UInt16(0))                    # last modified time
    write_little_endian_uint16!(buffer, position + 14, UInt16(0x21))                 # last modified date (1980-01-01)
    write_little_endian_uint32!(buffer, position + 16, crc32_value)
    write_little_endian_uint32!(buffer, position + 20, typemax(UInt32))              # compressed size sentinel (real value in ZIP64 extra)
    write_little_endian_uint32!(buffer, position + 24, typemax(UInt32))              # uncompressed size sentinel (real value in ZIP64 extra)
    write_little_endian_uint16!(buffer, position + 28, UInt16(length(name_bytes)))
    write_little_endian_uint16!(buffer, position + 30, UInt16(ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE))  # extra field length
    write_little_endian_uint16!(buffer, position + 32, UInt16(0))                    # comment length
    write_little_endian_uint16!(buffer, position + 34, UInt16(0))                    # disk number start
    write_little_endian_uint16!(buffer, position + 36, UInt16(0))                    # internal attrs
    write_little_endian_uint32!(buffer, position + 38, UInt32(0o0100644) << 16)      # external attrs (Unix)
    write_little_endian_uint32!(buffer, position + 42, typemax(UInt32))              # local file header offset sentinel
    copyto!(buffer, position + CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE, name_bytes, 1, length(name_bytes))
    extra_field_position = position + CENTRAL_DIRECTORY_ENTRY_FIXED_SIZE + length(name_bytes)
    write_little_endian_uint16!(buffer, extra_field_position, ZIP64_EXTRA_HEADER_ID)
    write_little_endian_uint16!(buffer, extra_field_position + 2, UInt16(ZIP64_CENTRAL_DIRECTORY_EXTRA_SIZE - 4))
    write_little_endian_uint64!(buffer, extra_field_position + 4, uncompressed_size)
    write_little_endian_uint64!(buffer, extra_field_position + 12, compressed_size)
    write_little_endian_uint64!(buffer, extra_field_position + 20, local_file_header_offset)
    return nothing
end

function write_local_file_header!(
    buffer::Vector{UInt8},
    position::Int,
    name_bytes::Vector{UInt8},
    compressed_size::UInt64,
    uncompressed_size::UInt64,
    crc32_value::UInt32,
    alignment_padding::Int,
)::Nothing
    total_extra_size = ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE + alignment_padding
    write_little_endian_uint32!(buffer, position, LOCAL_FILE_HEADER_SIGNATURE)
    write_little_endian_uint16!(buffer, position + 4, ZIP64_VERSION_NEEDED)          # version needed to extract (ZIP64)
    write_little_endian_uint16!(buffer, position + 6, UInt16(1 << 11))               # general purpose flag: UTF-8 name
    write_little_endian_uint16!(buffer, position + 8, STORED_COMPRESSION_METHOD)
    write_little_endian_uint16!(buffer, position + 10, UInt16(0))                    # last modified time
    write_little_endian_uint16!(buffer, position + 12, UInt16(0x21))                 # last modified date (1980-01-01)
    write_little_endian_uint32!(buffer, position + 14, crc32_value)
    write_little_endian_uint32!(buffer, position + 18, typemax(UInt32))              # compressed size sentinel (real value in ZIP64 extra)
    write_little_endian_uint32!(buffer, position + 22, typemax(UInt32))              # uncompressed size sentinel (real value in ZIP64 extra)
    write_little_endian_uint16!(buffer, position + 26, UInt16(length(name_bytes)))
    write_little_endian_uint16!(buffer, position + 28, UInt16(total_extra_size))     # extra field length (ZIP64 + alignment padding)
    copyto!(buffer, position + LOCAL_FILE_HEADER_FIXED_SIZE, name_bytes, 1, length(name_bytes))
    extra_field_position = position + LOCAL_FILE_HEADER_FIXED_SIZE + length(name_bytes)
    write_little_endian_uint16!(buffer, extra_field_position, ZIP64_EXTRA_HEADER_ID)
    write_little_endian_uint16!(buffer, extra_field_position + 2, UInt16(ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE - 4))
    write_little_endian_uint64!(buffer, extra_field_position + 4, uncompressed_size)
    write_little_endian_uint64!(buffer, extra_field_position + 12, compressed_size)
    if alignment_padding > 0
        @assert alignment_padding >= 4
        padding_position = extra_field_position + ZIP64_LOCAL_FILE_HEADER_EXTRA_SIZE
        write_little_endian_uint16!(buffer, padding_position, DAF_PADDING_EXTRA_HEADER_ID)
        write_little_endian_uint16!(buffer, padding_position + 2, UInt16(alignment_padding - 4))
        if alignment_padding > 4
            fill!(view(buffer, (padding_position + 4):(padding_position + alignment_padding - 1)), 0x00)
        end
    end
    return nothing
end

function write_zip64_end_of_central_directory!(
    buffer::Vector{UInt8},
    position::Int,
    central_directory_offset::UInt64,
    central_directory_size::UInt64,
    total_entry_count::UInt64,
)::Nothing
    write_little_endian_uint32!(buffer, position, ZIP64_END_OF_CENTRAL_DIRECTORY_SIGNATURE)
    # size of this record excluding the signature and this field itself
    write_little_endian_uint64!(buffer, position + 4, UInt64(ZIP64_END_OF_CENTRAL_DIRECTORY_SIZE - 12))
    write_little_endian_uint16!(buffer, position + 12, UInt16(0x031e))               # version made by (Unix v3.0)
    write_little_endian_uint16!(buffer, position + 14, ZIP64_VERSION_NEEDED)         # version needed to extract
    write_little_endian_uint32!(buffer, position + 16, UInt32(0))                    # disk number
    write_little_endian_uint32!(buffer, position + 20, UInt32(0))                    # CD start disk
    write_little_endian_uint64!(buffer, position + 24, total_entry_count)            # entries on this disk
    write_little_endian_uint64!(buffer, position + 32, total_entry_count)            # total entries
    write_little_endian_uint64!(buffer, position + 40, central_directory_size)
    write_little_endian_uint64!(buffer, position + 48, central_directory_offset)
    return nothing
end

function write_zip64_end_of_central_directory_locator!(
    buffer::Vector{UInt8},
    position::Int,
    zip64_end_of_central_directory_offset::UInt64,
)::Nothing
    write_little_endian_uint32!(buffer, position, ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_SIGNATURE)
    write_little_endian_uint32!(buffer, position + 4, UInt32(0))                     # disk with ZIP64 EOCD
    write_little_endian_uint64!(buffer, position + 8, zip64_end_of_central_directory_offset)
    write_little_endian_uint32!(buffer, position + 16, UInt32(1))                    # total number of disks
    return nothing
end

function write_end_of_central_directory!(buffer::Vector{UInt8}, position::Int)::Nothing
    write_little_endian_uint32!(buffer, position, END_OF_CENTRAL_DIRECTORY_SIGNATURE)
    write_little_endian_uint16!(buffer, position + 4, UInt16(0))                     # disk number
    write_little_endian_uint16!(buffer, position + 6, UInt16(0))                     # CD start disk
    write_little_endian_uint16!(buffer, position + 8, typemax(UInt16))               # entries on this disk (ZIP64 sentinel)
    write_little_endian_uint16!(buffer, position + 10, typemax(UInt16))              # total entries (ZIP64 sentinel)
    write_little_endian_uint32!(buffer, position + 12, typemax(UInt32))              # CD size (ZIP64 sentinel)
    write_little_endian_uint32!(buffer, position + 16, typemax(UInt32))              # CD offset (ZIP64 sentinel)
    write_little_endian_uint16!(buffer, position + 20, UInt16(0))                    # comment length
    return nothing
end

# The rest of the package already requires a little-endian host for its memory-mapped array blobs,
# so we marshal the ZIP integer fields via direct stores and loads rather than byte-by-byte shifts.
@assert ENDIAN_BOM == 0x04030201 "MmapZipStore requires a little-endian host"

@inline function write_little_endian_uint16!(buffer::Vector{UInt8}, position::Integer, value::UInt16)::Nothing  # FLAKY TESTED
    GC.@preserve buffer unsafe_store!(Ptr{UInt16}(pointer(buffer, position)), value)
    return nothing
end

@inline function write_little_endian_uint32!(buffer::Vector{UInt8}, position::Integer, value::UInt32)::Nothing  # FLAKY TESTED
    GC.@preserve buffer unsafe_store!(Ptr{UInt32}(pointer(buffer, position)), value)
    return nothing
end

@inline function write_little_endian_uint64!(buffer::Vector{UInt8}, position::Integer, value::UInt64)::Nothing  # FLAKY TESTED
    GC.@preserve buffer unsafe_store!(Ptr{UInt64}(pointer(buffer, position)), value)
    return nothing
end

@inline function read_little_endian_uint32(buffer::AbstractVector{UInt8}, position::Integer)::UInt32  # FLAKY TESTED
    return GC.@preserve buffer unsafe_load(Ptr{UInt32}(pointer(buffer, position)))
end

"""
    reserve_mmap_zip_entry!(
        store::MmapZipStore,
        key::AbstractString,
        data_size::Integer,
    )::AbstractVector{UInt8}

Reserve space for a new entry of `data_size` bytes with a placeholder CRC32 of `0`, and return an
mmap-backed byte view over the reserved data region. The caller fills the returned buffer in place
and then must call [`patch_mmap_zip_entry_crc!`](@ref) before any further appends.

If the caller crashes between the reserve and patch steps, the next write-mode open will detect
the placeholder CRC mismatch and roll the reservation back.
"""
function reserve_mmap_zip_entry!(store::MmapZipStore, key::AbstractString, data_size::Integer)::AbstractVector{UInt8}
    @assert store.is_writable
    if haskey(store.name_to_index, key)
        error("MmapZipStore is append-only; cannot overwrite entry: $(key)")
    end
    name_bytes = Vector{UInt8}(codeunits(String(key)))
    check_append_limits(store, name_bytes, data_size)
    reserved_size = UInt64(data_size)
    reserved_entry = commit_new_entry!(store, String(key), name_bytes, UInt32(0), reserved_size)
    if reserved_size == 0
        return UInt8[]  # UNTESTED
    end
    start_position = Int(reserved_entry.data_offset) + 1
    end_position = start_position + Int(reserved_size) - 1
    return view(store.file_mmap, start_position:end_position)
end

"""
    patch_mmap_zip_entry_crc!(store::MmapZipStore, key::AbstractString)::Nothing

Compute the real CRC32 of the data region of the entry previously reserved via
[`reserve_mmap_zip_entry!`](@ref) and patch the CRC32 field in both the local file header and the
central directory record. Each patch is a single four-byte store into the shared mmap.
"""
function patch_mmap_zip_entry_crc!(store::MmapZipStore, key::AbstractString)::Nothing
    entry_index = store.name_to_index[key]
    entry = store.entries[entry_index]
    entry_bytes = mmap_stored_entry_bytes(store, entry_index)
    computed_crc32 = ZipArchives.zip_crc32(entry_bytes)
    entry.crc32 = computed_crc32
    write_little_endian_uint32!(store.file_mmap, Int(entry.local_file_header_offset) + 14 + 1, computed_crc32)
    write_little_endian_uint32!(store.file_mmap, Int(entry.central_directory_offset) + 16 + 1, computed_crc32)
    return nothing
end

# Validate the tail of the central directory on a write-mode open and roll back any trailing run
# of invalid entries. See the module docstring for the full protocol.
function recover_interrupted_appends!(store::MmapZipStore)::Nothing
    entry_count = length(store.entries)
    if entry_count == 0
        return nothing
    end

    last_valid_index = entry_count
    while last_valid_index > 0
        if entry_is_valid(store, last_valid_index)
            break
        end
        last_valid_index -= 1
    end

    if last_valid_index == entry_count
        return nothing
    end

    roll_back_to_entry!(store, last_valid_index)
    return nothing
end

function entry_is_valid(store::MmapZipStore, entry_index::Int)::Bool
    entry = store.entries[entry_index]
    if entry.compression_method != STORED_COMPRESSION_METHOD
        return true  # UNTESTED
    end

    local_file_header_signature_bytes = mmap_file_range(store, entry.local_file_header_offset, UInt64(4))
    if read_little_endian_uint32(local_file_header_signature_bytes, 1) != LOCAL_FILE_HEADER_SIGNATURE
        return false  # UNTESTED
    end

    data_bytes = mmap_file_range(store, entry.data_offset, entry.compressed_size)
    return ZipArchives.zip_crc32(data_bytes) == entry.crc32
end

function mmap_file_range(store::MmapZipStore, offset::UInt64, size::UInt64)::AbstractVector{UInt8}  # FLAKY TESTED
    @assert offset + size <= store.overlay_length
    if size == 0
        return UInt8[]
    end
    start_position = Int(offset) + 1
    end_position = start_position + Int(size) - 1
    return view(store.file_mmap, start_position:end_position)
end

function roll_back_to_entry!(store::MmapZipStore, keep_count::Int)::Nothing
    entry_count = length(store.entries)
    @assert keep_count < entry_count
    new_central_directory_offset = store.entries[keep_count + 1].local_file_header_offset

    for entry_index in (keep_count + 1):entry_count
        delete!(store.name_to_index, store.entries[entry_index].name)
    end
    resize!(store.entries, keep_count)

    new_central_directory_size = UInt64(0)
    for entry in store.entries
        new_central_directory_size += central_directory_entry_size(entry)
    end

    commit_buffer =
        Vector{UInt8}(undef, Int(new_central_directory_size) + TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    write_central_directory_and_eocd!(
        commit_buffer,
        store.entries,
        new_central_directory_offset,
        new_central_directory_size,
    )
    @inbounds copyto!(store.file_mmap, Int(new_central_directory_offset) + 1, commit_buffer, 1, length(commit_buffer))

    new_file_size =
        new_central_directory_offset +
        new_central_directory_size +
        UInt64(TRAILING_END_OF_CENTRAL_DIRECTORY_REGION_SIZE)
    resize_file_overlay!(store, new_file_size)

    store.central_directory_offset = new_central_directory_offset
    store.central_directory_size = new_central_directory_size
    populate_central_directory_offsets!(store.entries, store.central_directory_offset)
    return nothing
end

function Base.close(store::MmapZipStore)::Nothing  # FLAKY TESTED
    if isopen(store.io_stream)
        close(store.io_stream)
    end
    return nothing
end

function Zarr.storefromstring(::Type{MmapZipStore}, path::AbstractString, _::Any)::Tuple{MmapZipStore, String}  # FLAKY TESTED
    return (MmapZipStore(String(path); writable = false), "")
end

end  # module
