"""
A `Daf` storage format that packs the [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf)
on-disk layout verbatim into a single ZIP archive. This is a convenient single-file form for
publication and transport: copy one `*.daf.zip` instead of recursively copying a directory tree,
without giving up the zero-copy memory-mapped access that `FilesDaf` provides.

The encoding is the `FilesDaf` encoding — every entry name inside the archive is the relative
path of the matching `FilesDaf` file (`daf.json`, `scalars/<name>.json`, `axes/<axis>.txt`,
`vectors/<axis>/<name>.{json,txt,data,nzind,nzval,nztxt}`,
`matrices/<rows>/<cols>/<name>.{json,data,txt,colptr,rowval,nzval,nztxt}`), and every blob is the
same little-endian byte stream. As a consequence:

  - Unzipping a `*.daf.zip` produced by this format yields a directory that
    [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) opens directly. The unzipped tree
    will not contain the `metadata.json` consolidated index `FilesDaf` uses for
    [`HttpDaf`](@ref DataAxesFormats.HttpFormat.HttpDaf) enumeration — it is rebuilt
    automatically by `FilesDaf.ensure_metadata_json!` on the first local open on a writable
    filesystem.
  - Conversely, `zip -r foo.daf.zip foo.daf/` over an existing `FilesDaf` directory produces a
    valid `ZipDaf`. Since the source directory may hold a (now-stale) `metadata.json`, that
    entry can get bundled inside the archive too — `ZipDaf` strips it from the central
    directory on every writable open (see
    [`MmapZipStores.remove_entries_from_central_directory!`](@ref
    DataAxesFormats.MmapZipStores.remove_entries_from_central_directory!)). After unzipping
    such a stripped archive, the next writable `FilesDaf` open rebuilds `metadata.json` from
    scratch, so the consolidated index is always coherent with the directory tree.

The backend is built on [`MmapZipStores.MmapZipStore`](@ref
DataAxesFormats.MmapZipStores.MmapZipStore), so dense numeric blobs (and `colptr` / `rowval` /
`nzind` / `nzval` for sparse ones) are served zero-copy via memory-mapped views of the archive
file when possible; foreign archives whose entries are deflate-compressed or unaligned fall back
to in-memory decoded copies. Same archive-on-disk protocol as the ZIP backend of
[`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf): append-only, no entry deletes or
overwrites, no axis reorder, with crash-recovery on the next writable open.

Path conventions, mirroring [`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf):

  - `something.daf.zip` — single Daf at the archive root. A `#/group` fragment in this form is
    rejected.
  - `something.dafs.zip#/group` — a multi-Daf archive holding several `Daf` data sets under
    sub-paths; `group` selects one and must be non-empty. A bare `something.dafs.zip` without a
    `#/group` fragment is rejected.

Example archive entry layout (single-Daf form; the multi-Daf form prefixes every entry with
`<group>/`):

    daf.json
    scalars/version.json
    axes/cell.txt
    axes/gene.txt
    vectors/cell/batch.json
    vectors/cell/batch.txt
    vectors/gene/is_marker.json
    vectors/gene/is_marker.data
    matrices/cell/gene/UMIs.json
    matrices/cell/gene/UMIs.colptr
    matrices/cell/gene/UMIs.rowval
    matrices/cell/gene/UMIs.nzval

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type         |
|:---- |:-------------------- |:------------------------- |:------------------- |:--------------------- |
| `r`  | No                   | No                        | No                  | [`DafReadOnly`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`ZipDaf`](@ref)      |
| `w+` | Yes                  | Yes                       | No                  | [`ZipDaf`](@ref)      |
| `w`  | Yes                  | Yes                       | Yes                 | [`ZipDaf`](@ref)      |

Truncating a sub-daf inside a `.dafs.zip` is not supported (the ZIP backend is append-only) and
raises an error; use `r+` or `w+` to open a sub-daf for writing without truncation.

!!! note

    When several [`ZipDaf`](@ref) instances in the same process share an archive path
    (typically different `#/group` sub-dafs of the same `.dafs.zip` file, or repeated opens of
    the same single-daf `.daf.zip`), they share a single underlying [`MmapZipStore`](@ref
    DataAxesFormats.MmapZipStores.MmapZipStore) and a single `data_lock`, so concurrent calls
    serialize correctly and the archive is never mmap-ed twice. The first such open determines
    the store's writability: a later open of the same archive that requests write access raises
    an error if the first open was read-only. Release the read-only handle first, or open the
    writable instance first.

!!! note

    `ZipDaf` archives intentionally do not contain the `metadata.json` consolidated index that
    [`HttpDaf`](@ref DataAxesFormats.HttpFormat.HttpDaf) reads, because the archive's own
    central directory plays the same enumeration role. Consequently, an
    `unzip foo.daf.zip -d foo.daf/` produces a directory that lacks the index; before exposing
    such a directory over HTTP, open it once locally with `FilesDaf("foo.daf")` (any mode) so
    `FilesFormat.ensure_metadata_json!` builds it.
"""
module ZipFormat

export ZipDaf

using ..Formats
using ..LazySparse
using ..MmapZipStores
using ..ReadOnly
using ..Readers
using ..Reorder
using ..StorageTypes
using ..Writers
using DiskArrays
using JSON
using ProgressMeter
using SparseArrays
using StringViews
using TanayLabUtilities
using ZipArchives

import ..FilesFormat.MAJOR_VERSION
import ..FilesFormat.MINOR_VERSION
import ..Formats
import ..Formats.Internal
import ..Operations.DTYPE_BY_NAME
import ..PackedFormat.IncrementalShardWriter
import ..PackedFormat.MmapShardRegion
import ..PackedFormat.PackedCodec
import ..PackedFormat.PackedDaf
import ..PackedFormat.chunks_for
import ..PackedFormat.dense_array_json_bytes
import ..PackedFormat.eltype_for_descriptor
import ..PackedFormat.make_streaming_shard_writer
import ..PackedFormat.open_packed_dense_array
import ..PackedFormat.packed_delete_entry!
import ..PackedFormat.packed_entry_size
import ..PackedFormat.packed_finalize_entry!
import ..PackedFormat.packed_format_filled_empty_dense_matrix!
import ..PackedFormat.packed_format_filled_empty_dense_vector!
import ..PackedFormat.packed_format_filled_empty_sparse_matrix!
import ..PackedFormat.packed_format_filled_empty_sparse_vector!
import ..PackedFormat.packed_format_get_empty_dense_matrix!
import ..PackedFormat.packed_format_get_empty_dense_vector!
import ..PackedFormat.packed_format_get_empty_sparse_matrix!
import ..PackedFormat.packed_format_get_empty_sparse_vector!
import ..PackedFormat.packed_format_open_sparse_component_eager
import ..PackedFormat.packed_format_open_sparse_component_source
import ..PackedFormat.packed_format_write_dense_array!
import ..PackedFormat.packed_format_write_sparse_numeric_matrix!
import ..PackedFormat.packed_format_write_sparse_numeric_vector!
import ..PackedFormat.packed_has_entry
import ..PackedFormat.packed_make_streaming_shard_writer
import ..PackedFormat.packed_open_array
import ..PackedFormat.packed_read_json
import ..PackedFormat.packed_read_lines
import ..PackedFormat.packed_read_typed_matrix
import ..PackedFormat.packed_read_typed_vector
import ..PackedFormat.packed_register_metadata!
import ..PackedFormat.packed_reserve_typed_matrix!
import ..PackedFormat.packed_reserve_typed_vector!
import ..PackedFormat.packed_write_bytes!
import ..PackedFormat.packed_write_typed_array!
import ..PackedFormat.parse_sparse_descriptor
import ..PackedFormat.sparse_matrix_json_bytes
import ..PackedFormat.sparse_vector_json_bytes
import ..PackedFormat.v3_bytes_codecs_for

import SparseArrays.indtype

"""
The virtual address reservation size used for writable [`MmapZipStore`](@ref
DataAxesFormats.MmapZipStores.MmapZipStore) opens of a [`ZipDaf`](@ref) (modes `r+`, `w+`, `w`).
Each such open reserves this much virtual address space via a single anonymous `PROT_NONE`
mapping and overlays the real file onto its first `filesize` bytes; subsequent `ftruncate` +
re-overlay calls extend the accessible portion as the archive grows. The physical file stays at
its real size — only VA is reserved. Defaults to 128 GiB, leaving plenty of room for concurrent
live stores on platforms with ~128 TiB of user VA (Apple Silicon). Set to a larger value before
opening a `ZipDaf` whose ZIP archive might grow past this bound. An append that would cross the
bound fails with an explicit error pointing back here.
"""
DAF_ZIP_MAX_FILE_SIZE::Int = 1 << 37

"""
A weak-cache entry pairing an open [`MmapZipStore`](@ref) with the `data_lock` shared by every
sub-daf of that archive in the same process. `MmapZipStore` is stateful (single `io_stream`,
archive-wide mmap, appended-entry mmap table), so multiple sub-dafs of the same archive must
share one instance to avoid corrupting the archive with two independent writers. `is_writable`
records the mode the store was opened in, so that a later sub-daf opening the same archive in
an incompatible mode can be rejected cleanly.
"""
mutable struct SharedMmapZipStoreHandle  # NOLINT
    store::MmapZipStore
    data_lock::ExtendedReadWriteLock
    is_writable::Bool
end

"""
    acquire_shared_mmap_zip_store!(;
        container_path::AbstractString,
        is_read_only::Bool,
        create_if_missing::Bool,
        truncate::Bool,
        max_file_size::Integer,
    )::SharedMmapZipStoreHandle

Acquire (or open, if not already open in this process) the [`SharedMmapZipStoreHandle`](@ref)
for the ZIP archive at `container_path`. The handle is registered in a process-wide weak cache
keyed on `container_path`, so concurrent opens of the same archive in the same process share one
underlying [`MmapZipStore`](@ref) and one `data_lock` regardless of which `Daf` format
([`ZipDaf`](@ref) or [`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf)) opened it. This is
deliberate: `MmapZipStore` is stateful (single `io_stream`, archive-wide mmap, appended-entry
mmap table), so two independent stores over the same file would corrupt each other on writes.

If the archive is already cached as a read-only store and the new open requests write access,
this raises an explicit error pointing at the conflict; release the read-only handle first, or
open the writable instance first.
"""
function acquire_shared_mmap_zip_store!(;
    container_path::AbstractString,
    is_read_only::Bool,
    create_if_missing::Bool,
    truncate::Bool,
    max_file_size::Integer,
)::SharedMmapZipStoreHandle
    full_container_path = String(container_path)
    shared_handle = get_through_global_weak_cache(full_container_path, (:daf, :zip); purge = truncate) do _
        return SharedMmapZipStoreHandle(
            MmapZipStore(
                full_container_path;
                writable = !is_read_only,
                create = create_if_missing,
                truncate = truncate,
                max_file_size = max_file_size,
            ),
            ExtendedReadWriteLock(),
            !is_read_only,
        )
    end
    if !is_read_only && !shared_handle.is_writable
        error(  # UNTESTED
            "zip file is already open read-only in this process; " *
            "release the existing handle before opening it for write: $(full_container_path)",
        )
    end
    return shared_handle
end

"""
    parse_zip_archive_path(
        path::AbstractString;
        single_daf_suffix::AbstractString,
        multi_dafs_suffix::AbstractString,
        multi_dafs_marker::AbstractString,
        format_name::AbstractString,
    )::Maybe{Tuple{String, Maybe{String}}}

Parse `path` as either a single-Daf archive (`*<single_daf_suffix>`, no `#/group` fragment) or
a multi-Daf archive (`*<multi_dafs_suffix><#/group>`, group required and non-empty). Return
`(container_path, group_or_nothing)` for either valid form. Return `nothing` if `path` matches
neither form, leaving the caller free to try further format-specific suffix recognition (e.g.
[`ZarrFormat`](@ref DataAxesFormats.ZarrFormat)'s `.daf.zarr` directory case) before issuing
its own catch-all error.

Raises an explicit error for the two ill-formed near-miss cases:

  - `*<single_daf_suffix>#/group` — a singular path with a sub-daf fragment;
    `can't address a sub-daf in a singular <format_name> path …`.
  - `*<multi_dafs_suffix>` (no `#/group`) — a plural path missing its sub-daf;
    `missing '#/<group>' in plural <format_name> path …`.
  - `*<multi_dafs_marker>` (empty group) — `empty group name after '#/' in <format_name> path …`.

`format_name` is the human-readable label used in error messages
(`ZipDaf`, `ZarrDaf`).
"""
function parse_zip_archive_path(
    path::AbstractString;
    single_daf_suffix::AbstractString,
    multi_dafs_suffix::AbstractString,
    multi_dafs_marker::AbstractString,
    format_name::AbstractString,
)::Maybe{Tuple{String, Maybe{String}}}
    parts = split(path, multi_dafs_marker)
    if length(parts) != 1
        @assert length(parts) == 2 "can't parse as <stem>$(multi_dafs_marker)<group>: $(path)"
        group = String(parts[2])
        if isempty(group)
            error("empty group name after '#/' in $(format_name) path: $(path)")
        end
        return (String(parts[1]) * multi_dafs_suffix, group)
    end

    single_daf_with_fragment = single_daf_suffix * "#"
    if occursin(single_daf_with_fragment, path)
        error(
            "can't address a sub-daf in a singular $(format_name) path; " *
            "use $(multi_dafs_suffix)#/<group>: $(path)",
        )
    end

    if endswith(path, single_daf_suffix)
        return (String(path), nothing)
    end

    if endswith(path, multi_dafs_suffix)
        error("missing '#/<group>' in plural $(format_name) path: $(path)")
    end

    return nothing
end

"""
    ZipDaf(
        path::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing,
        packed::Bool = false]
    )

Storage in a single ZIP archive of the [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf)
on-disk layout. See the [module documentation](@ref ZipFormat) for the path conventions, the
mode table, the on-disk entry layout, the sub-daf sharing semantics, and the relationship to
`unzip` / `zip -r` round-trips.

When opening an existing data set, if `name` is not specified, and there exists a `name` scalar
property, it is used as the name. Otherwise, the `path` (including any `#/group` fragment) is
used as the name.

If `packed` is `true`, subsequent writes through this handle default to the packed (chunked +
compressed) on-disk encoding for properties whose uncompressed size is at or above
[`DAF_PACKED_TARGET_CHUNK_KB`](@ref DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB).
Per-call `packed` kwargs on `set_*!` / `empty_*!` / `copy_*!` override this default. The default
is `false` (today's flat encoding).
"""
struct ZipDaf <: PackedDaf
    name::AbstractString
    internal::Internal
    store::MmapZipStore
    group_prefix::String
    mode::AbstractString
    path::AbstractString
end

# Sidecar entries that may appear inside an archive produced by `zip -r` over a `FilesDaf`
# directory (see `FilesFormat.ensure_metadata_json!`). `ZipDaf` strips them from the central
# directory on every writable open: their data regions stay in the file as orphan bytes, but
# they no longer surface through ordinary ZIP enumeration. Stripping is what makes
# `unzip foo.daf.zip` produce a directory that triggers a `metadata.json` rebuild on its first
# writable `FilesDaf` open instead of preserving a stale snapshot.
const FILES_DAF_SIDECAR_NAMES = ("metadata.json",)

function ZipDaf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
    packed::Bool = false,
)::Union{ZipDaf, DafReadOnly}
    (is_read_only, create_if_missing, truncate_if_exists) = Formats.parse_mode(mode)

    parsed = parse_zip_archive_path(
        path;
        single_daf_suffix = ".daf.zip",
        multi_dafs_suffix = ".dafs.zip",
        multi_dafs_marker = ".dafs.zip#/",
        format_name = "ZipDaf",
    )
    if parsed === nothing
        error("can't parse as ZipDaf path: $(path)\n" * "expected one of: <stem>.daf.zip, <stem>.dafs.zip#/<group>")
    end
    (container_path, group_path) = parsed
    full_container_path = abspath(container_path)
    full_path = group_path === nothing ? full_container_path : full_container_path * "#/" * group_path

    if truncate_if_exists && group_path !== nothing
        error("can't truncate a sub-daf inside a ZipDaf; the ZIP backend is append-only: $(full_path)")
    end

    purge = truncate_if_exists && group_path === nothing
    if !isfile(full_container_path) && !create_if_missing
        error("no such file: $(full_container_path)")
    end

    shared_handle = acquire_shared_mmap_zip_store!(;
        container_path = full_container_path,
        is_read_only = is_read_only,
        create_if_missing = create_if_missing,
        truncate = purge,
        max_file_size = DAF_ZIP_MAX_FILE_SIZE,
    )
    store = shared_handle.store

    group_prefix = group_path === nothing ? "" : group_path * "/"
    daf_marker_key = group_prefix * "daf.json"

    if !haskey(store.name_to_index, daf_marker_key)
        if !create_if_missing
            error("not a daf data set: $(full_path)")
        end
        @assert !is_read_only
        write_daf_marker!(store, daf_marker_key)
    else
        verify_daf_marker(store, daf_marker_key, full_path)
    end

    if !is_read_only
        strip_files_daf_sidecars!(store, group_prefix)
    end

    if name === nothing
        scalar_name_key = group_prefix * "scalars/name.json"
        if haskey(store.name_to_index, scalar_name_key)
            name = string(read_scalar_value(store[scalar_name_key]))  # NOJET
        end
    end
    if name === nothing
        name = full_path
    end
    name = unique_name(name)

    internal = Internal(;
        is_frozen = is_read_only,
        data_lock = shared_handle.data_lock,
        packed_default = packed,
        shared_resource = shared_handle,
    )
    zip_daf = ZipDaf(name, internal, store, group_prefix, mode, full_path)
    @debug "Daf: $(brief(zip_daf)) path: $(full_path)" _group = :daf_repos
    if is_read_only
        return read_only(zip_daf)
    else
        return zip_daf
    end
end

function write_daf_marker!(store::MmapZipStore, daf_marker_key::AbstractString)::Nothing
    store[daf_marker_key] = Vector{UInt8}("{\"version\":[$(MAJOR_VERSION),$(MINOR_VERSION)]}\n")
    return nothing
end

function verify_daf_marker(store::MmapZipStore, daf_marker_key::AbstractString, full_path::AbstractString)::Nothing
    daf_json = JSON.parse(String(store[daf_marker_key]))  # NOJET
    @assert daf_json isa AbstractDict
    daf_version = daf_json["version"]
    @assert daf_version isa AbstractVector
    @assert length(daf_version) == 2

    if Int(daf_version[1]) != MAJOR_VERSION || Int(daf_version[2]) > MINOR_VERSION
        error(chomp("""
              incompatible format version: $(daf_version[1]).$(daf_version[2])
              for the daf zip archive: $(full_path)
              the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
              """))
    end
    return nothing
end

# Strip a stale `metadata.json` sidecar entry that may have been bundled
# into the archive by `zip -r` over a `FilesDaf` directory. The data bytes stay in the file as
# orphan regions (the backend is append-only with respect to data); only the central directory
# is rewritten so the entries become unreachable through normal ZIP enumeration. Idempotent.
function strip_files_daf_sidecars!(store::MmapZipStore, group_prefix::AbstractString)::Nothing
    sidecar_keys = String[group_prefix * sidecar_name for sidecar_name in FILES_DAF_SIDECAR_NAMES]
    remove_entries_from_central_directory!(store, sidecar_keys)
    return nothing
end

# --------------------------------------------------------------------------------------------
# Format identification & description
# --------------------------------------------------------------------------------------------

function Readers.is_leaf(::ZipDaf)::Bool  # FLAKY TESTED
    return true
end

function Readers.is_leaf(::Type{ZipDaf})::Bool  # FLAKY TESTED
    return true
end

function Readers.complete_path(zip_daf::ZipDaf)::Maybe{AbstractString}
    return zip_daf.path
end

function Formats.format_description_header(  # FLAKY TESTED
    zip_daf::ZipDaf,
    indent::AbstractString,
    lines::Vector{String},
    ::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(zip_daf)
    push!(lines, "$(indent)type: ZipDaf")
    push!(lines, "$(indent)path: $(zip_daf.path)")
    push!(lines, "$(indent)mode: $(zip_daf.mode)")
    return nothing
end

function TanayLabUtilities.Brief.brief(value::ZipDaf; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ZipDaf $(name)"
end

# --------------------------------------------------------------------------------------------
# Entry-key construction and enumeration helpers
# --------------------------------------------------------------------------------------------

@inline function entry_key(zip_daf::ZipDaf, parts::AbstractString...)::String  # FLAKY TESTED
    return zip_daf.group_prefix * join(parts)
end

# Names of ZIP entries whose key is exactly `<relative_directory>/<name><suffix>` (i.e. directly
# under the directory, no further `/`), with `<suffix>` stripped. Mirrors
# `FilesFormat.get_names_set` (`src/files_format.jl:1088`) for the archive backend.
function entries_in_directory(
    zip_daf::ZipDaf,
    relative_directory::AbstractString,
    suffix::AbstractString,
)::AbstractSet{<:AbstractString}
    names_set = Set{AbstractString}()
    suffix_length = length(suffix)
    prefix = zip_daf.group_prefix * relative_directory * "/"
    prefix_length = ncodeunits(prefix)
    for entry in zip_daf.store.entries
        name = entry.name
        if startswith(name, prefix) && endswith(name, suffix)
            tail = SubString(name, prefix_length + 1)
            slash_position = findfirst('/', tail)
            if slash_position === nothing
                push!(names_set, String(chop(tail; tail = suffix_length)))
            end
        end
    end
    return names_set
end

@inline function entry_byte_size(zip_daf::ZipDaf, key::AbstractString)::UInt64  # FLAKY TESTED
    return zip_daf.store.entries[zip_daf.store.name_to_index[key]].uncompressed_size
end

# Return the raw bytes of an entry, prefering a zero-copy memory-mapped view (returned with
# `MappedData`) when the entry is stored uncompressed at an aligned offset; otherwise returning
# a decoded in-memory copy (returned with `MemoryData`).
function read_entry_raw_bytes(zip_daf::ZipDaf, key::AbstractString)::Tuple{AbstractVector{UInt8}, Formats.CacheGroup}
    byte_count = Int(entry_byte_size(zip_daf, key))
    mapped_bytes = try_mmap_entry_as(zip_daf.store, key, UInt8, byte_count)
    if mapped_bytes !== nothing
        return (mapped_bytes, Formats.MappedData)
    end
    decoded_bytes = Vector{UInt8}(zip_daf.store[key])  # NOJET
    return (decoded_bytes, Formats.MemoryData)
end

# Read the contents of a `.txt` / `.nztxt` entry as one `SubString` per line. Mirrors
# `FilesFormat.mmap_file_lines` (`src/files_format.jl:1103`) but draws bytes from the archive
# rather than the filesystem.
function read_entry_lines(
    zip_daf::ZipDaf,
    key::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    raw_bytes, cache_group = read_entry_raw_bytes(zip_daf, key)
    text = StringView(raw_bytes)
    lines = split(text, '\n')
    last_line = pop!(lines)
    @assert last_line == ""
    return (lines, cache_group)
end

# Wrap the bytes of an entry as a typed `Vector{T}` of length `element_count`. Returns
# `(typed_view, byte_owner, cache_group)` where `byte_owner` is the `Vector{UInt8}` that backs
# the view (or `nothing` for the zero-copy mmap path); the caller stores it alongside the typed
# view to keep it alive as long as the typed view is reachable.
function read_entry_typed_vector(
    zip_daf::ZipDaf,
    key::AbstractString,
    ::Type{T},
    element_count::Integer,
)::Tuple{Vector{T}, Any, Formats.CacheGroup} where {T}
    mapped_view = try_mmap_entry_as(zip_daf.store, key, T, Int(element_count))
    if mapped_view !== nothing
        return (mapped_view, nothing, Formats.MappedData)
    end
    decoded_bytes = Vector{UInt8}(zip_daf.store[key])
    @assert length(decoded_bytes) == Int(element_count) * sizeof(T)
    typed_view = unsafe_wrap(Array, Ptr{T}(pointer(decoded_bytes)), Int(element_count); own = false)
    return (typed_view, decoded_bytes, Formats.MemoryData)
end

# Same as `read_entry_typed_vector`, but the wrap shape is a `(nrows, ncols)` matrix in the
# archive's column-major layout.
function read_entry_typed_matrix(  # FLAKY TESTED
    zip_daf::ZipDaf,
    key::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
)::Tuple{Matrix{T}, Any, Formats.CacheGroup} where {T}
    matrix_dims = (Int(nrows), Int(ncols))
    mapped_view = try_mmap_entry_as(zip_daf.store, key, T, matrix_dims)
    if mapped_view !== nothing
        return (mapped_view, nothing, Formats.MappedData)
    end
    decoded_bytes = Vector{UInt8}(zip_daf.store[key])
    @assert length(decoded_bytes) == prod(matrix_dims) * sizeof(T)
    typed_view = unsafe_wrap(Array, Ptr{T}(pointer(decoded_bytes)), matrix_dims; own = false)
    return (typed_view, decoded_bytes, Formats.MemoryData)
end

@inline function combine_cache_groups(left::Formats.CacheGroup, right::Formats.CacheGroup)::Formats.CacheGroup  # FLAKY TESTED
    if left == Formats.MappedData && right == Formats.MappedData
        return Formats.MappedData
    end
    return Formats.MemoryData
end

# --------------------------------------------------------------------------------------------
# Byte-buffer construction (matches the `FilesDaf` on-disk byte format exactly)
# --------------------------------------------------------------------------------------------

function scalar_json_bytes(value::StorageScalar)::Vector{UInt8}
    type = typeof(value)
    if type <: AbstractString
        type = String
    end
    io = IOBuffer()
    JSON.print(io, Dict("type" => "$(type)", "value" => value))
    write(io, '\n')
    return take!(io)
end

function lines_bytes(lines::AbstractArray{<:AbstractString})::Vector{UInt8}
    io = IOBuffer()
    for line in lines
        @assert !contains(line, '\n')
        println(io, line)
    end
    return take!(io)
end

function repeated_string_bytes(value::AbstractString, count::Integer)::Vector{UInt8}
    @assert !contains(value, '\n')
    io = IOBuffer()
    for _ in 1:count
        println(io, value)
    end
    return take!(io)
end

function repeated_value_bytes(value::T, count::Integer)::Vector{UInt8} where {T <: StorageReal}  # FLAKY TESTED
    typed_buffer = fill(value, Int(count))
    return typed_array_to_bytes(typed_buffer)
end

function typed_array_to_bytes(typed::AbstractArray{T})::Vector{UInt8} where {T}  # FLAKY TESTED
    bytes = Vector{UInt8}(undef, length(typed) * sizeof(T))
    if length(typed) > 0
        GC.@preserve typed bytes unsafe_copyto!(Ptr{T}(pointer(bytes)), pointer(typed), length(typed))
    end
    return bytes
end

function packed_write_bytes!(zip_daf::ZipDaf, key::AbstractString, bytes::AbstractVector{UInt8})::Nothing
    zip_daf.store[entry_key(zip_daf, key)] = bytes
    return nothing
end

function packed_write_typed_array!(zip_daf::ZipDaf, key::AbstractString, vector::AbstractVector)::Nothing
    zip_daf.store[entry_key(zip_daf, key)] = typed_array_to_bytes(vector)
    return nothing
end

# `ZipDaf` is append-only at the storage level so deletion is a no-op: the format-set caller's defensive cleanup
# loop is meaningful only on `FilesDaf` (where stale variant files must be removed before a property rewrite). On
# `ZipDaf` the same property is never written twice — `format_delete_vector!` / `format_delete_matrix!` error before
# any second write reaches us.
function packed_delete_entry!(::ZipDaf, ::AbstractString)::Nothing
    return nothing
end

# `ZipDaf` has no secondary metadata index — the zip itself is the index, so JSON sidecars are discoverable from
# `zip_names` directly. Stale `metadata.json` entries (e.g. from a `zip -r foo.daf` of a FilesDaf) are stripped on
# writable open by [`strip_files_daf_sidecars!`](@ref) so they cannot resurface in the central directory.
function packed_register_metadata!(::ZipDaf, ::AbstractString, ::AbstractVector{UInt8})::Nothing
    return nothing
end

function packed_has_entry(zip_daf::ZipDaf, key::AbstractString)::Bool
    return haskey(zip_daf.store.name_to_index, entry_key(zip_daf, key))
end

function packed_entry_size(zip_daf::ZipDaf, key::AbstractString)::Int
    return Int(entry_byte_size(zip_daf, entry_key(zip_daf, key)))
end

function packed_read_json(zip_daf::ZipDaf, key::AbstractString)::AbstractDict
    parsed = JSON.parse(String(zip_daf.store[entry_key(zip_daf, key)]))  # NOJET
    @assert parsed isa AbstractDict
    return parsed
end

function packed_read_lines(  # FLAKY TESTED
    zip_daf::ZipDaf,
    key::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    return read_entry_lines(zip_daf, entry_key(zip_daf, key))
end

function packed_read_typed_vector(
    zip_daf::ZipDaf,
    key::AbstractString,
    ::Type{T},
    n_elements::Integer,
)::Tuple{Vector{T}, Any, Formats.CacheGroup} where {T}
    return read_entry_typed_vector(zip_daf, entry_key(zip_daf, key), T, n_elements)
end

function packed_read_typed_matrix(
    zip_daf::ZipDaf,
    key::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
)::Tuple{Matrix{T}, Any, Formats.CacheGroup} where {T}
    return read_entry_typed_matrix(zip_daf, entry_key(zip_daf, key), T, nrows, ncols)
end

function packed_open_array(
    zip_daf::ZipDaf,
    shard_key::AbstractString,
    ::Type{T},
    descriptor::AbstractDict,
    dims::NTuple{N, Int},
)::DiskArrays.CachedDiskArray where {T, N}
    shard_bytes, _ = read_entry_raw_bytes(zip_daf, entry_key(zip_daf, shard_key))
    return open_packed_dense_array(Vector{UInt8}(shard_bytes), T, descriptor, dims)
end

function packed_reserve_typed_vector!(  # FLAKY TESTED
    zip_daf::ZipDaf,
    key::AbstractString,
    ::Type{T},
    n_elements::Integer,
)::Vector{T} where {T <: StorageReal}
    byte_count = Int(n_elements) * sizeof(T)
    raw_view = reserve_mmap_zip_entry!(zip_daf.store, entry_key(zip_daf, key), byte_count)
    if Int(n_elements) == 0
        return Vector{T}(undef, 0)
    end
    return unsafe_wrap(Array, Ptr{T}(pointer(raw_view)), Int(n_elements); own = false)
end

function packed_reserve_typed_matrix!(
    zip_daf::ZipDaf,
    key::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
)::Matrix{T} where {T <: StorageReal}
    matrix_dims = (Int(nrows), Int(ncols))
    byte_count = prod(matrix_dims) * sizeof(T)
    raw_view = reserve_mmap_zip_entry!(zip_daf.store, entry_key(zip_daf, key), byte_count)
    if prod(matrix_dims) == 0
        return Matrix{T}(undef, matrix_dims)  # UNTESTED
    end
    return unsafe_wrap(Array, Ptr{T}(pointer(raw_view)), matrix_dims; own = false)
end

# `ZipDaf` finalises a reserved entry by recomputing its CRC32 from the user-written bytes and patching the local +
# central directory headers. The streaming-shard sink does its own shrink + CRC patch on the shard entry, so this is
# only invoked for non-streaming reserved entries (`.data`, `.colptr`, `.rowval`, `.nzval`, `.nzind`).
function packed_finalize_entry!(zip_daf::ZipDaf, key::AbstractString)::Nothing
    patch_mmap_zip_entry_crc!(zip_daf.store, entry_key(zip_daf, key))
    return nothing
end

# Reserve an upper-bound outer-zip entry sized for the worst-case codec inflation per inner chunk plus the index slab,
# wrap it in an `MmapShardRegion` sink, and build the streaming `IncrementalShardWriter` over it. The sink's
# `finalize_sink!` shrinks the reservation to the actual shard size and patches the entry's CRC, so callers only need
# to drive `submit_shard_chunk!` / `finalize_shard!` against the writer.
function packed_make_streaming_shard_writer(
    zip_daf::ZipDaf,
    shard_key::AbstractString,
    ::Type{T},
    n_chunks::Integer,
    chunk_shape::NTuple{2, Int},
    codec::PackedCodec,
)::IncrementalShardWriter where {T <: StorageReal}
    per_chunk_upper_bound = UInt64(2 * prod(chunk_shape) * sizeof(T) + 4096)
    index_size = UInt64(16 * Int(n_chunks))
    reserved_size = UInt64(n_chunks) * per_chunk_upper_bound + index_size
    physical_key = entry_key(zip_daf, shard_key)
    region = reserve_mmap_zip_entry!(zip_daf.store, physical_key, reserved_size)
    sink = MmapShardRegion(zip_daf.store, physical_key, region, UInt64(0), reserved_size)
    return make_streaming_shard_writer(sink, T, Int(n_chunks), v3_bytes_codecs_for(codec, T))
end

# --------------------------------------------------------------------------------------------
# Append-only error helpers
# --------------------------------------------------------------------------------------------

@noinline function append_only_error(zip_daf::ZipDaf, what::AbstractString)::Union{}
    return error("ZipDaf is append-only; can't $(what) in: $(zip_daf.path)")
end

# --------------------------------------------------------------------------------------------
# Scalars
# --------------------------------------------------------------------------------------------

function Formats.format_has_scalar(zip_daf::ZipDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(zip_daf)
    return haskey(zip_daf.store.name_to_index, entry_key(zip_daf, "scalars/", name, ".json"))
end

function Formats.format_set_scalar!(
    zip_daf::ZipDaf,
    name::AbstractString,
    value::StorageScalar,
)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(zip_daf)
    zip_daf.store[entry_key(zip_daf, "scalars/", name, ".json")] = scalar_json_bytes(value)
    return Formats.MemoryData
end

function Formats.format_delete_scalar!(zip_daf::ZipDaf, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "delete scalar: $(name)")
end

function Formats.format_get_scalar(zip_daf::ZipDaf, name::AbstractString)::Tuple{StorageScalar, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(zip_daf)
    return (read_scalar_value(zip_daf.store[entry_key(zip_daf, "scalars/", name, ".json")]), Formats.MemoryData)  # NOJET
end

function read_scalar_value(bytes::AbstractVector{UInt8})::StorageScalar
    json = JSON.parse(String(bytes))
    @assert json isa AbstractDict
    dtype_name = json["type"]
    json_value = json["value"]
    if dtype_name == "String" || dtype_name == "string"
        @assert json_value isa AbstractString
        return String(json_value)
    end
    type = get(DTYPE_BY_NAME, dtype_name, nothing)
    @assert type !== nothing
    return convert(type, json_value)
end

function Formats.format_scalars_set(zip_daf::ZipDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(zip_daf)
    return entries_in_directory(zip_daf, "scalars", ".json")
end

# --------------------------------------------------------------------------------------------
# Axes
# --------------------------------------------------------------------------------------------

function Formats.format_has_axis(zip_daf::ZipDaf, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(zip_daf)
    return haskey(zip_daf.store.name_to_index, entry_key(zip_daf, "axes/", axis, ".txt"))
end

function Formats.format_add_axis!(
    zip_daf::ZipDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    zip_daf.store[entry_key(zip_daf, "axes/", axis, ".txt")] = lines_bytes(entries)
    return nothing
end

function Formats.format_delete_axis!(zip_daf::ZipDaf, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "delete axis: $(axis)")
end

function Formats.format_axes_set(zip_daf::ZipDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(zip_daf)
    return entries_in_directory(zip_daf, "axes", ".txt")
end

function Formats.format_axis_vector(
    zip_daf::ZipDaf,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(zip_daf)
    return read_entry_lines(zip_daf, entry_key(zip_daf, "axes/", axis, ".txt"))
end

function Formats.format_axis_length(zip_daf::ZipDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(zip_daf)
    entries = Formats.get_axis_vector_through_cache(zip_daf, axis)
    return length(entries)
end

# --------------------------------------------------------------------------------------------
# Vectors
# --------------------------------------------------------------------------------------------

function Formats.format_has_vector(zip_daf::ZipDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(zip_daf)
    return haskey(zip_daf.store.name_to_index, entry_key(zip_daf, "vectors/", axis, "/", name, ".json"))
end

function Formats.format_set_vector!(
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
    packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    n_elements = Formats.format_axis_length(zip_daf, axis)
    if vector == 0
        vector = spzeros(typeof(vector), n_elements)
    end

    if vector isa AbstractString
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".json")] = dense_array_json_bytes(String)
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".txt")] =
            repeated_string_bytes(vector, n_elements)

    elseif vector isa StorageScalar
        @assert vector isa StorageReal
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".json")] = dense_array_json_bytes(typeof(vector))
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".data")] =  # NOJET
            repeated_value_bytes(vector, n_elements)

    elseif issparse(vector)
        packed_format_write_sparse_numeric_vector!(zip_daf, axis, name, vector, packed)  # NOJET

    elseif eltype(vector) <: AbstractString
        write_string_vector(zip_daf, axis, name, vector)  # NOJET

    else
        chunk_shape = chunks_for(packed, size(vector), eltype(vector))
        if chunk_shape !== nothing
            packed_format_write_dense_array!(zip_daf, "vectors/$(axis)/$(name)", vector, chunk_shape)  # NOJET
        else
            zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".json")] =
                dense_array_json_bytes(eltype(vector))
            zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".data")] = typed_array_to_bytes(vector)  # NOJET
        end
    end
    return nothing
end

function write_string_vector(
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::AbstractVector{<:AbstractString},
)::Nothing
    n_empty = 0
    nonempty_size = 0
    for value in vector
        value_size = length(value)
        if value_size > 0
            nonempty_size += value_size
        else
            n_empty += 1
        end
    end

    n_values = length(vector)
    n_nonempty = n_values - n_empty
    ind_type = indtype_for_size(n_values)

    dense_size = nonempty_size + length(vector)
    sparse_size = nonempty_size + n_nonempty * (1 + sizeof(ind_type))

    if sparse_size <= dense_size * 0.75
        nzind_vector = Vector{ind_type}(undef, n_nonempty)
        nztxt_io = IOBuffer()
        position = 1
        for (index, value) in enumerate(vector)
            if length(value) > 0
                @assert !contains(value, '\n')
                println(nztxt_io, value)
                nzind_vector[position] = index
                position += 1
            end
        end
        @assert position == n_nonempty + 1

        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".json")] =
            sparse_vector_json_bytes(String, ind_type, n_nonempty)
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".nztxt")] = take!(nztxt_io)
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".nzind")] = typed_array_to_bytes(nzind_vector)
    else
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".json")] = dense_array_json_bytes(String)
        zip_daf.store[entry_key(zip_daf, "vectors/", axis, "/", name, ".txt")] = lines_bytes(vector)
    end
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(zip_daf)
    return packed_format_get_empty_dense_vector!(
        zip_daf,
        axis,
        name,
        T,
        packed,
        Formats.format_axis_length(zip_daf, axis),
    )
end

function Formats.format_filled_empty_dense_vector!(  # FLAKY TESTED
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
    filled::AbstractVector{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    packed_format_filled_empty_dense_vector!(zip_daf, axis, name, filled)
    return nothing
end

function Formats.format_get_empty_sparse_vector!(
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
    _packed::Bool,
)::Tuple{AbstractVector{I}, AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(zip_daf)
    return packed_format_get_empty_sparse_vector!(zip_daf, axis, name, T, nnz, I)
end

function Formats.format_filled_empty_sparse_vector!(  # FLAKY TESTED
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    packed_format_filled_empty_sparse_vector!(zip_daf, axis, name)
    return nothing
end

function Formats.format_delete_vector!(
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "delete vector: $(axis) / $(name)")
end

function Formats.format_vectors_set(zip_daf::ZipDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(zip_daf)
    return entries_in_directory(zip_daf, "vectors/" * axis, ".json")
end

function Formats.format_get_vector(
    zip_daf::ZipDaf,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(zip_daf)

    base_key = "vectors/$(axis)/$(name)"
    json = packed_read_json(zip_daf, "$(base_key).json")  # NOJET
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    n_elements = Formats.format_axis_length(zip_daf, axis)

    if format == "dense"
        eltype_name = String(json["eltype"])
        eltype = eltype_for_descriptor(eltype_name)
        if get(json, "packed", false) === true
            vector = packed_open_array(zip_daf, "$(base_key).shard", eltype, json, (n_elements,))
            return (vector, nothing, Formats.MemoryData)
        end
        if eltype_name == "string" || eltype_name == "String"
            vector, cache_group = packed_read_lines(zip_daf, "$(base_key).txt")
            @assert length(vector) == n_elements
            return (vector, nothing, cache_group)
        end
        vector, byte_owner, cache_group = packed_read_typed_vector(zip_daf, "$(base_key).data", eltype, n_elements)
        return (vector, byte_owner, cache_group)
    end

    @assert format == "sparse"
    eltype_name, indtype_name = parse_sparse_descriptor(json, "nzind")
    ind_type = eltype_for_descriptor(indtype_name)

    nzind_vector, nzind_owner, nnz, nzind_cache_group =
        packed_format_open_sparse_component_eager(zip_daf, base_key, "nzind", ind_type, json, nothing)

    if eltype_name == "string" || eltype_name == "String"
        nztxt_lines, _ = packed_read_lines(zip_daf, "$(base_key).nztxt")
        vector = Vector{AbstractString}(undef, n_elements)
        fill!(vector, "")
        vector[nzind_vector] .= nztxt_lines  # NOJET
        return (vector, nothing, Formats.MemoryData)
    end

    eltype = eltype_for_descriptor(eltype_name)
    nzval_present =
        packed_has_entry(zip_daf, "$(base_key).nzval") || packed_has_entry(zip_daf, "$(base_key).nzval.shard")

    if nzval_present
        nzval_vector, nzval_owner, _, nzval_cache_group =
            packed_format_open_sparse_component_eager(zip_daf, base_key, "nzval", eltype, json, nnz)
        sparse_vector = SparseVector(n_elements, nzind_vector, nzval_vector)
        return (sparse_vector, (nzind_owner, nzval_owner), combine_cache_groups(nzind_cache_group, nzval_cache_group))
    end

    nzval_vector = fill(true, nnz)
    sparse_vector = SparseVector(n_elements, nzind_vector, nzval_vector)
    return (sparse_vector, nzind_owner, Formats.MemoryData)
end

# --------------------------------------------------------------------------------------------
# Matrices
# --------------------------------------------------------------------------------------------

function Formats.format_has_matrix(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(zip_daf)
    return haskey(
        zip_daf.store.name_to_index,
        entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".json"),
    )
end

function Formats.format_set_matrix!(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalarBase, StorageMatrix},
    packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    nrows = Formats.format_axis_length(zip_daf, rows_axis)
    ncols = Formats.format_axis_length(zip_daf, columns_axis)
    if matrix == 0
        matrix = spzeros(typeof(matrix), nrows, ncols)
    end

    if matrix isa StorageReal
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".json")] =
            dense_array_json_bytes(typeof(matrix))
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".data")] =  # NOJET
            repeated_value_bytes(matrix, nrows * ncols)

    elseif matrix isa AbstractString
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".json")] =
            dense_array_json_bytes(String)
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".txt")] =
            repeated_string_bytes(matrix, nrows * ncols)

    elseif issparse(matrix)
        @assert matrix isa AbstractMatrix
        packed_format_write_sparse_numeric_matrix!(zip_daf, rows_axis, columns_axis, name, matrix, packed)

    elseif eltype(matrix) <: AbstractString
        write_string_matrix(zip_daf, rows_axis, columns_axis, name, matrix)  # NOJET

    else
        @assert eltype(matrix) <: Real
        chunk_shape = chunks_for(packed, (nrows, ncols), eltype(matrix))
        if chunk_shape !== nothing
            packed_format_write_dense_array!(  # NOJET
                zip_daf,
                "matrices/$(rows_axis)/$(columns_axis)/$(name)",
                matrix,
                chunk_shape,
            )  # NOJET
        else
            zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".json")] =
                dense_array_json_bytes(eltype(matrix))
            zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".data")] =  # NOJET
                typed_array_to_bytes(matrix)
        end
    end
    return nothing
end

function write_string_matrix(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::AbstractMatrix{<:AbstractString},
)::Nothing
    nrows, ncols = size(matrix)

    n_empty = 0
    nonempty_size = 0
    for value in matrix
        value_size = length(value)
        if value_size > 0
            nonempty_size += value_size
        else
            n_empty += 1
        end
    end

    n_values = nrows * ncols
    n_nonempty = n_values - n_empty
    ind_type = indtype_for_size(n_values)

    dense_size = nonempty_size + nrows * ncols
    sparse_size = nonempty_size + n_nonempty + (ncols + 1 + n_nonempty) * sizeof(ind_type)

    if sparse_size <= dense_size * 0.75
        colptr_vector = Vector{ind_type}(undef, ncols + 1)
        rowval_vector = Vector{ind_type}(undef, n_nonempty)
        nztxt_io = IOBuffer()

        position = 1
        for column_index in 1:ncols
            colptr_vector[column_index] = position
            for row_index in 1:nrows
                value = matrix[row_index, column_index]
                if length(value) > 0
                    @assert !contains(value, '\n')
                    println(nztxt_io, value)
                    rowval_vector[position] = row_index
                    position += 1
                end
            end
        end
        @assert position == n_nonempty + 1
        colptr_vector[ncols + 1] = n_nonempty + 1

        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".json")] =
            sparse_matrix_json_bytes(String, ind_type, n_nonempty, ncols)
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".nztxt")] =
            take!(nztxt_io)
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".colptr")] =
            typed_array_to_bytes(colptr_vector)
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".rowval")] =
            typed_array_to_bytes(rowval_vector)
    else
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".json")] =
            dense_array_json_bytes(String)
        zip_daf.store[entry_key(zip_daf, "matrices/", rows_axis, "/", columns_axis, "/", name, ".txt")] =
            lines_bytes(matrix)
    end
    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(zip_daf)
    nrows = Formats.format_axis_length(zip_daf, rows_axis)
    ncols = Formats.format_axis_length(zip_daf, columns_axis)
    return packed_format_get_empty_dense_matrix!(zip_daf, rows_axis, columns_axis, name, T, packed, nrows, ncols)
end

function Formats.format_filled_empty_dense_matrix!(  # FLAKY TESTED
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::AbstractMatrix{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    packed_format_filled_empty_dense_matrix!(zip_daf, rows_axis, columns_axis, name, filled)
    return nothing
end

function Formats.format_get_empty_sparse_matrix!(
    zip_daf::ZipDaf,
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
    @assert Formats.has_data_write_lock(zip_daf)
    ncols = Formats.format_axis_length(zip_daf, columns_axis)
    return packed_format_get_empty_sparse_matrix!(zip_daf, rows_axis, columns_axis, name, T, nnz, I, ncols)
end

function Formats.format_filled_empty_sparse_matrix!(  # FLAKY TESTED
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    packed_format_filled_empty_sparse_matrix!(zip_daf, rows_axis, columns_axis, name)
    return nothing
end

function Formats.format_relayout_matrix!(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
    packed::Bool,
)::StorageMatrix
    @assert Formats.has_data_write_lock(zip_daf)

    if issparse(matrix)
        colptr_vector, rowval_vector, nzval_vector, _ = Formats.format_get_empty_sparse_matrix!(
            zip_daf,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(matrix.colptr),
            packed,
        )
        colptr_vector[1] = 1
        colptr_vector[2:end] .= length(nzval_vector) + 1
        relayout_matrix = SparseMatrixCSC(
            axis_length(zip_daf, columns_axis),
            axis_length(zip_daf, rows_axis),
            colptr_vector,
            rowval_vector,
            nzval_vector,
        )
        relayout!(flip(relayout_matrix), matrix)
        Formats.format_filled_empty_sparse_matrix!(zip_daf, columns_axis, rows_axis, name, relayout_matrix)
        return relayout_matrix

    elseif eltype(matrix) <: AbstractString
        relayout_matrix = flipped(matrix)
        write_string_matrix(zip_daf, columns_axis, rows_axis, name, relayout_matrix)
        return relayout_matrix
    end

    @assert eltype(matrix) <: Real
    relayout_matrix, _ =
        Formats.format_get_empty_dense_matrix!(zip_daf, columns_axis, rows_axis, name, eltype(matrix), packed)
    relayout!(flip(relayout_matrix), matrix)
    Formats.format_filled_empty_dense_matrix!(zip_daf, columns_axis, rows_axis, name, relayout_matrix)
    return relayout_matrix
end

function Formats.format_delete_matrix!(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "delete matrix: $(rows_axis) / $(columns_axis) / $(name)")
end

function Formats.format_matrices_set(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(zip_daf)
    return entries_in_directory(zip_daf, "matrices/" * rows_axis * "/" * columns_axis, ".json")
end

function Formats.format_get_matrix(
    zip_daf::ZipDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(zip_daf)

    nrows = Formats.format_axis_length(zip_daf, rows_axis)
    ncols = Formats.format_axis_length(zip_daf, columns_axis)

    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    json = packed_read_json(zip_daf, "$(base_key).json")
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    if format == "dense"
        eltype_name = String(json["eltype"])
        eltype = eltype_for_descriptor(eltype_name)
        if get(json, "packed", false) === true
            matrix = packed_open_array(zip_daf, "$(base_key).shard", eltype, json, (nrows, ncols))
            return (matrix, nothing, Formats.MemoryData)
        end
        if eltype_name == "string" || eltype_name == "String"
            flat_lines, cache_group = packed_read_lines(zip_daf, "$(base_key).txt")
            @assert length(flat_lines) == nrows * ncols
            return (reshape(flat_lines, (nrows, ncols)), nothing, cache_group)
        end
        matrix, byte_owner, cache_group = packed_read_typed_matrix(zip_daf, "$(base_key).data", eltype, nrows, ncols)
        return (matrix, byte_owner, cache_group)
    end

    @assert format == "sparse"
    eltype_name, indtype_name = parse_sparse_descriptor(json, "colptr")
    ind_type = eltype_for_descriptor(indtype_name)

    rowval_descriptor = get(json, "rowval", nothing)
    rowval_packed = rowval_descriptor isa AbstractDict && get(rowval_descriptor, "packed", false) === true
    nzval_descriptor = get(json, "nzval", nothing)
    nzval_packed = nzval_descriptor isa AbstractDict && get(nzval_descriptor, "packed", false) === true

    nzval_present =
        packed_has_entry(zip_daf, "$(base_key).nzval") || packed_has_entry(zip_daf, "$(base_key).nzval.shard")

    # `colptr` is always materialised at read time (small; slicing needs random access).
    colptr_vector, colptr_owner, _, colptr_cache_group =
        packed_format_open_sparse_component_eager(zip_daf, base_key, "colptr", ind_type, json, ncols + 1)

    if eltype_name == "string" || eltype_name == "String"
        rowval_vector, _, nnz, _ =
            packed_format_open_sparse_component_eager(zip_daf, base_key, "rowval", ind_type, json, nothing)
        nztxt_lines, _ = packed_read_lines(zip_daf, "$(base_key).nztxt")
        matrix = Matrix{AbstractString}(undef, nrows, ncols)
        fill!(matrix, "")
        position = 1
        for column_index in 1:ncols
            first_row_position = colptr_vector[column_index]
            last_row_position = colptr_vector[column_index + 1] - 1
            for row_index in rowval_vector[first_row_position:last_row_position]
                matrix[row_index, column_index] = nztxt_lines[position]
                position += 1
            end
        end
        return (matrix, nothing, Formats.MemoryData)
    end

    eltype = eltype_for_descriptor(eltype_name)

    if rowval_packed || (nzval_present && nzval_packed)
        rowval_source, _, nnz, _ =
            packed_format_open_sparse_component_source(zip_daf, base_key, "rowval", ind_type, json, nothing)
        nzval_source = if nzval_present
            source, _, _, _ = packed_format_open_sparse_component_source(zip_daf, base_key, "nzval", eltype, json, nnz)
            source
        else
            fill(true, nnz)
        end
        matrix = LazySparseMatrix(nrows, colptr_vector, rowval_source, nzval_source)
        return (matrix, nothing, Formats.MemoryData)
    end

    rowval_vector, rowval_owner, nnz, rowval_cache_group =
        packed_format_open_sparse_component_eager(zip_daf, base_key, "rowval", ind_type, json, nothing)

    if nzval_present
        nzval_vector, nzval_owner, _, nzval_cache_group =
            packed_format_open_sparse_component_eager(zip_daf, base_key, "nzval", eltype, json, nnz)
        sparse_matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
        cache_group =
            combine_cache_groups(combine_cache_groups(colptr_cache_group, rowval_cache_group), nzval_cache_group)
        return (sparse_matrix, (colptr_owner, rowval_owner, nzval_owner), cache_group)
    end

    nzval_vector = fill(true, nnz)
    sparse_matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)
    return (sparse_matrix, (colptr_owner, rowval_owner), Formats.MemoryData)
end

# --------------------------------------------------------------------------------------------
# Reorder (entirely unsupported on the append-only ZIP backend)
# --------------------------------------------------------------------------------------------

function Reorder.format_lock_reorder!(zip_daf::ZipDaf, ::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "reorder")
end

function Reorder.format_backup_reorder!(zip_daf::ZipDaf, ::Reorder.FormatReorderPlan)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "reorder")
end

function Reorder.format_replace_reorder!(  # UNTESTED
    zip_daf::ZipDaf,
    ::Reorder.FormatReorderPlan,
    ::Maybe{Progress},
    ::Maybe{Ref{Int}},
)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "reorder")
end

function Reorder.format_cleanup_reorder!(zip_daf::ZipDaf)::Nothing
    @assert Formats.has_data_write_lock(zip_daf)
    return append_only_error(zip_daf, "reorder")
end

function Reorder.format_has_reorder_lock(zip_daf::ZipDaf)::Bool
    @assert Formats.has_data_write_lock(zip_daf)
    return false
end

function Reorder.format_reset_reorder!(zip_daf::ZipDaf)::Bool
    @assert Formats.has_data_write_lock(zip_daf)
    return false
end

end  # module
