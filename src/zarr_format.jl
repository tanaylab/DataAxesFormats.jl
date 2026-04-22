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

We use the following internal structure under some root Zarr group (which is **not** compatible with any specific
existing Zarr-based convention such as [`OME-NGFF`](https://ngff.openmicroscopy.org/)):

  - The directory will contain 4 sub-groups: `scalars`, `axes`, `vectors`, and `matrices`, and a `daf` array.

  - The `daf` array signifies that the group contains `Daf` data. It contains two `UInt8` integers, the first being the
    major version number and the second the minor version number, using [semantic versioning](https://semver.org/).
    This makes it easy to test whether some Zarr group does/n't contain `Daf` data, and which version of the internal
    structure it is using. Currently the only defined version is `[1,0]`.

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

  - Every Zarr array is created without compression, using a single chunk covering the full array, so the chunk file on
    disk is a raw binary image that we can memory-map for direct access.

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
using JSON
using Mmap
using ProgressMeter
using SparseArrays
using Zarr

import ..Formats
import ..Formats.Internal
import ..Readers.base_array
import ..Reorder
using TanayLabUtilities

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
        [name::Maybe{AbstractString} = nothing]
    )

Storage in a Zarr directory tree or Zarr ZIP archive.

The `path` is a filesystem path that follows one of these conventions:

  - `something.daf.zarr` — a Zarr directory containing a single `Daf` data set at its root.
  - `something.daf.zarr.zip` — a Zarr ZIP archive containing a single `Daf` data set at its root.
  - `something.dafs.zarr.zip#/group` — a Zarr ZIP archive containing `Daf` data sets in sub-groups, addressed by
    `group`.

The backend (directory or ZIP) is selected from the `.zip` file-name suffix. The ZIP backend is append-only:
properties cannot be deleted and axes cannot be reordered (attempts to do so raise an error).

!!! note

    If you create a directory whose name is `something.dafs.zarr.zip#` and place `Daf` ZIP archives in it, this scheme
    will fail. So don't.

When opening an existing data set, if `name` is not specified, and there exists a "name" scalar property, it is used as
the name. Otherwise, the `path` (including any `#/group` suffix) will be used as the name.

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
"""
struct ZarrDaf <: DafWriter
    name::AbstractString
    internal::Internal
    root::ZGroup
    mode::AbstractString
    path::AbstractString
end

# Weak-cache entry pairing an open [`MmapZipStore`](@ref) with the data_lock shared by every
# sub-ZarrDaf of that archive. `MmapZipStore` is stateful (single `io_stream`, archive-wide mmap,
# appended-entry mmap table), so sub-dafs of the same archive in the same process must share one
# instance to avoid corrupting the archive with two independent writers. `is_writable` records the
# mode the store was opened in, so that a later sub-ZarrDaf opening the same archive in an
# incompatible mode can be rejected cleanly.
mutable struct SharedMmapZipStoreHandle
    store::MmapZipStore
    data_lock::ExtendedReadWriteLock
    is_writable::Bool
    function SharedMmapZipStoreHandle(store::MmapZipStore, data_lock::ExtendedReadWriteLock, is_writable::Bool)  # FLAKY TESTED
        return new(store, data_lock, is_writable)
    end
end

function ZarrDaf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{ZarrDaf, DafReadOnly}
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

        shared_handle = get_through_global_weak_cache(full_container_path, (:daf, :zarr_zip); purge) do _
            return SharedMmapZipStoreHandle(
                MmapZipStore(
                    full_container_path;
                    writable = !is_read_only,
                    create = create_if_missing,
                    truncate = purge,
                    max_file_size = DAF_ZARR_ZIP_MAX_FILE_SIZE,
                ),
                ExtendedReadWriteLock(),
                !is_read_only,
            )
        end
        if !is_read_only && !shared_handle.is_writable
            error(  # UNTESTED
                "zarr zip file is already open read-only in this process; " *
                "release the existing handle before opening it for write: $(full_container_path)",
            )
        end
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
        Internal(; is_frozen = is_read_only)
    else
        Internal(; is_frozen = is_read_only, data_lock = shared_handle.data_lock, shared_resource = shared_handle)
    end
    daf = ZarrDaf(name, internal, root, mode, full_path)
    @debug "Daf: $(brief(daf)) root: $(root)" _group = :daf_repos
    if is_read_only
        return read_only(daf)
    else
        return daf
    end
end

"""
    ZarrDaf(; [name::Maybe{AbstractString} = nothing])::ZarrDaf

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
function ZarrDaf(; name::Maybe{AbstractString} = nothing)::ZarrDaf
    store = Zarr.DictStore()
    root = zgroup(store, "")
    create_daf(root)
    if name === nothing
        name = "memory"  # UNTESTED
    end
    name = unique_name(name)
    daf = ZarrDaf(name, Internal(; is_frozen = false), root, "w+", "<memory>")
    @debug "Daf: $(brief(daf)) root: $(root)" _group = :daf_repos
    return daf
end

# Parse the user-facing `path` into `(container_path, group_path, is_zip)`:
#   foo.daf.zarr              → (foo.daf.zarr,      nothing, false)
#   foo.daf.zarr.zip          → (foo.daf.zarr.zip,  nothing, true)
#   foo.dafs.zarr.zip#/group  → (foo.dafs.zarr.zip, "group", true)
# Anything else is a hard error.
function parse_zarr_path(path::AbstractString)::Tuple{String, Maybe{String}, Bool}
    parts = split(path, ".dafs.zarr.zip#/")
    if length(parts) != 1
        @assert length(parts) == 2 "can't parse as <stem>.dafs.zarr.zip#/<group>: $(path)"
        return (String(parts[1]) * ".dafs.zarr.zip", String(parts[2]), true)
    end
    if endswith(path, ".daf.zarr.zip")
        return (String(path), nothing, true)
    end
    if endswith(path, ".daf.zarr")
        return (String(path), nothing, false)
    end
    return error(
        "can't parse as ZarrDaf path: $(path)\n" *
        "expected one of: <stem>.daf.zarr, <stem>.daf.zarr.zip, <stem>.dafs.zarr.zip#/<group>",
    )
end

function open_or_create_daf_group(
    store::Zarr.AbstractStore,
    zpath::AbstractString,
    full_path::AbstractString,
    is_read_only::Bool,
    create_if_missing::Bool,
)::ZGroup
    if Zarr.is_zgroup(store, zpath)
        root = Zarr.zopen_noerr(store, is_read_only ? "r" : "w"; path = zpath, fill_as_missing = false)  # NOJET
        if !(root isa ZGroup)
            error("not a daf zarr group: $(full_path)")  # UNTESTED
        end
        if haskey(root.arrays, DAF_KEY)
            verify_daf(root, full_path)
        elseif create_if_missing
            create_daf(root)
        else
            error("not a daf data set: $(full_path)")
        end
        return root
    end
    if !create_if_missing
        error("not a zarr group: $(full_path)")
    end
    root = zgroup(store, String(zpath))  # NOJET
    create_daf(root)
    return root
end

function create_daf(root::ZGroup)::Nothing
    daf_array = zcreate(UInt8, root, DAF_KEY, 2; compressor = Zarr.NoCompressor())
    daf_array[:] = UInt8[MAJOR_VERSION, MINOR_VERSION]  # NOJET

    zgroup(root, SCALARS)
    zgroup(root, AXES)
    zgroup(root, VECTORS)
    zgroup(root, MATRICES)

    return nothing
end

function verify_daf(root::ZGroup, full_path::AbstractString)::Nothing
    version = root.arrays[DAF_KEY][:]
    @assert length(version) == 2
    if version[1] != MAJOR_VERSION || version[2] > MINOR_VERSION
        error(chomp("""
              incompatible format version: $(Int(version[1])).$(Int(version[2]))
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
    consolidated = Zarr.consolidate_metadata(storage, Dict{String, Any}(), prefix)
    group_directory = joinpath(storage.folder, lstrip(prefix, '/'))
    target_path = joinpath(group_directory, ".zmetadata")
    staging_path = target_path * ".new"
    open(staging_path, "w") do io
        return JSON.print(io, Dict("metadata" => consolidated, "zarr_consolidated_format" => 1), 4)
    end
    Base.Filesystem.rename(staging_path, target_path)
    return nothing
end

function try_mmap_vector_chunk(daf::ZarrDaf, array::ZArray{T})::Maybe{AbstractVector{T}} where {T}  # FLAKY TESTED
    storage = array.storage
    key = chunk_key(array, "0")
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
    key = chunk_key(array, "0.0")
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

function array_as_vector(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageVector, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        vector = try_mmap_vector_chunk(daf, array)
        if vector !== nothing
            return (vector, Formats.MappedData)
        end
    end
    return (array[:], Formats.MemoryData)
end

function array_as_matrix(daf::ZarrDaf, array::ZArray{T})::Tuple{StorageMatrix, Formats.CacheGroup} where {T}
    if can_mmap(array) && !isempty(array)
        matrix = try_mmap_matrix_chunk(daf, array)
        if matrix !== nothing
            return (matrix, Formats.MappedData)
        end
    end
    return (array[:, :], Formats.MemoryData)  # NOJET
end

function can_mmap(array::ZArray{T})::Bool where {T}  # FLAKY TESTED
    return isbitstype(T) &&
           array.metadata.compressor isa Zarr.NoCompressor &&
           array.metadata.filters === nothing &&
           array.metadata.chunks == size(array) &&
           (array.storage isa Zarr.DirectoryStore || array.storage isa MmapZipStore || array.storage isa Zarr.DictStore)
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
    array = zcreate(typeof(value), scalars_group(daf), name, 1; compressor = Zarr.NoCompressor())
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
    axis_array = zcreate(String, axes_group(daf), axis, length(entries); compressor = Zarr.NoCompressor())
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
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)

    if vector isa StorageScalar
        array = zcreate(typeof(vector), group, name, nelements; compressor = Zarr.NoCompressor())
        array[:] = fill(vector, nelements)  # NOJET
    else
        @assert vector isa AbstractVector
        vector = base_array(vector)
        if issparse(vector)
            write_sparse_vector(group, name, vector)
        elseif eltype(vector) <: AbstractString
            array = zcreate(String, group, name, nelements; compressor = Zarr.NoCompressor())
            array[:] = String.(vector)  # NOJET
        else
            array = zcreate(eltype(vector), group, name, nelements; compressor = Zarr.NoCompressor())
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
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    nelements = Formats.format_axis_length(daf, axis)
    array = zcreate(T, group, name, nelements; compressor = Zarr.NoCompressor())
    array[:] = zeros(T, nelements)
    return array_as_vector(daf, array)
end

function Formats.format_filled_empty_dense_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::AbstractVector{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    array = axis_vectors_group(daf, axis).arrays[name]
    patch_chunk_crc_if_needed(array, "0")
    refresh_consolidated_metadata!(daf)
    return nothing
end

function write_sparse_vector(parent::ZGroup, name::AbstractString, vector::AbstractVector)::Nothing
    vector_group = zgroup(parent, name)

    nzind_vector = nzind(vector)
    nzind_array =
        zcreate(eltype(nzind_vector), vector_group, "nzind", length(nzind_vector); compressor = Zarr.NoCompressor())
    nzind_array[:] = nzind_vector

    if eltype(vector) != Bool || !all(nzval(vector))
        nzval_vector = nzval(vector)
        nzval_array =
            zcreate(eltype(nzval_vector), vector_group, "nzval", length(nzval_vector); compressor = Zarr.NoCompressor())
        nzval_array[:] = nzval_vector
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
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(daf)
    group = axis_vectors_group(daf, axis)
    vector_group = zgroup(group, name)

    nnz_int = Int(nnz)
    nzind_array = zcreate(I, vector_group, "nzind", nnz_int; compressor = Zarr.NoCompressor())
    nzval_array = zcreate(T, vector_group, "nzval", nnz_int; compressor = Zarr.NoCompressor())
    nzind_array[:] = zeros(I, nnz_int)
    nzval_array[:] = zeros(T, nnz_int)

    nzind_vec, _ = array_as_vector(daf, nzind_array)
    nzval_vec, _ = array_as_vector(daf, nzval_array)
    return (nzind_vec, nzval_vec)
end

function Formats.format_filled_empty_sparse_vector!(
    daf::ZarrDaf,
    axis::AbstractString,
    name::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(daf)
    vector_group = axis_vectors_group(daf, axis).groups[name]
    patch_chunk_crc_if_needed(vector_group.arrays["nzind"], "0")
    if haskey(vector_group.arrays, "nzval")
        patch_chunk_crc_if_needed(vector_group.arrays["nzval"], "0")
    end
    refresh_consolidated_metadata!(daf)
    return Formats.MappedData
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
)::Tuple{StorageVector, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    group = axis_vectors_group(daf, axis)
    if haskey(group.arrays, name)
        return array_as_vector(daf, group.arrays[name])
    end

    @assert haskey(group.groups, name)
    vector_group = group.groups[name]
    nelements = Formats.format_axis_length(daf, axis)

    nzind_vector, nzind_cache_group = array_as_vector(daf, vector_group.arrays["nzind"])
    if haskey(vector_group.arrays, "nzval")
        nzval_vector, nzval_cache_group = array_as_vector(daf, vector_group.arrays["nzval"])
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
    return (vector, cache_group)
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
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    if matrix isa StorageReal
        array = zcreate(typeof(matrix), group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = fill(matrix, nrows, ncols)  # NOJET
    elseif matrix isa AbstractString
        array = zcreate(String, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = fill(String(matrix), nrows, ncols)  # NOJET
    elseif eltype(matrix) <: AbstractString
        array = zcreate(String, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = String.(matrix)  # NOJET
    else
        @assert matrix isa AbstractMatrix
        @assert major_axis(matrix) != Rows
        matrix = base_array(matrix)
        if issparse(matrix)
            write_sparse_matrix(group, name, matrix)
            refresh_consolidated_metadata!(daf)
            return nothing
        else
            array = zcreate(eltype(matrix), group, name, nrows, ncols; compressor = Zarr.NoCompressor())
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
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)
    array = zcreate(T, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
    array[:, :] = zeros(T, nrows, ncols)
    return array_as_matrix(daf, array)
end

function Formats.format_filled_empty_dense_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::AbstractMatrix{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(daf)
    array = columns_axis_group(daf, rows_axis, columns_axis).arrays[name]
    patch_chunk_crc_if_needed(array, "0.0")
    refresh_consolidated_metadata!(daf)
    return nothing
end

function write_sparse_matrix(parent::ZGroup, name::AbstractString, matrix::AbstractMatrix)::Nothing
    matrix_group = zgroup(parent, name)

    colptr_vector = colptr(matrix)
    colptr_array =
        zcreate(eltype(colptr_vector), matrix_group, "colptr", length(colptr_vector); compressor = Zarr.NoCompressor())
    colptr_array[:] = colptr_vector

    rowval_vector = rowval(matrix)
    rowval_array =
        zcreate(eltype(rowval_vector), matrix_group, "rowval", length(rowval_vector); compressor = Zarr.NoCompressor())
    rowval_array[:] = rowval_vector

    if eltype(matrix) != Bool || !all(nzval(matrix))
        nzval_vector = nzval(matrix)
        nzval_array =
            zcreate(eltype(nzval_vector), matrix_group, "nzval", length(nzval_vector); compressor = Zarr.NoCompressor())
        nzval_array[:] = nzval_vector
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
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)
    matrix_group = zgroup(group, name)

    nnz_int = Int(nnz)
    colptr_array = zcreate(I, matrix_group, "colptr", ncols + 1; compressor = Zarr.NoCompressor())
    rowval_array = zcreate(I, matrix_group, "rowval", nnz_int; compressor = Zarr.NoCompressor())
    nzval_array = zcreate(T, matrix_group, "nzval", nnz_int; compressor = Zarr.NoCompressor())

    colptr_init = fill(I(nnz_int + 1), ncols + 1)
    colptr_init[1] = I(1)
    colptr_array[:] = colptr_init
    rowval_array[:] = ones(I, nnz_int)
    nzval_array[:] = zeros(T, nnz_int)

    colptr_vec, _ = array_as_vector(daf, colptr_array)
    rowval_vec, _ = array_as_vector(daf, rowval_array)
    nzval_vec, _ = array_as_vector(daf, nzval_array)
    return (colptr_vec, rowval_vec, nzval_vec)
end

function Formats.format_filled_empty_sparse_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(daf)
    matrix_group = columns_axis_group(daf, rows_axis, columns_axis).groups[name]
    patch_chunk_crc_if_needed(matrix_group.arrays["colptr"], "0")
    patch_chunk_crc_if_needed(matrix_group.arrays["rowval"], "0")
    if haskey(matrix_group.arrays, "nzval")
        patch_chunk_crc_if_needed(matrix_group.arrays["nzval"], "0")
    end
    refresh_consolidated_metadata!(daf)
    return Formats.MappedData
end

function Formats.format_relayout_matrix!(
    daf::ZarrDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
)::StorageMatrix
    @assert Formats.has_data_write_lock(daf)
    if eltype(matrix) <: AbstractString
        group = columns_axis_group(daf, columns_axis, rows_axis)
        nrows = axis_length(daf, columns_axis)
        ncols = axis_length(daf, rows_axis)
        relayout_matrix = flipped(matrix)
        array = zcreate(String, group, name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = String.(relayout_matrix)
        refresh_consolidated_metadata!(daf)
        return relayout_matrix
    end
    if issparse(matrix)
        sparse_colptr, sparse_rowval, sparse_nzval = Formats.format_get_empty_sparse_matrix!(
            daf,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(colptr(matrix)),
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
    relayout_matrix, _ = Formats.format_get_empty_dense_matrix!(daf, columns_axis, rows_axis, name, eltype(matrix))
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
)::Tuple{StorageMatrix, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(daf)
    group = columns_axis_group(daf, rows_axis, columns_axis)
    if haskey(group.arrays, name)
        return array_as_matrix(daf, group.arrays[name])
    end

    @assert haskey(group.groups, name)
    matrix_group = group.groups[name]
    nrows = Formats.format_axis_length(daf, rows_axis)
    ncols = Formats.format_axis_length(daf, columns_axis)

    colptr_vector, colptr_cache_group = array_as_vector(daf, matrix_group.arrays["colptr"])
    rowval_vector, rowval_cache_group = array_as_vector(daf, matrix_group.arrays["rowval"])
    if haskey(matrix_group.arrays, "nzval")
        nzval_vector, nzval_cache_group = array_as_vector(daf, matrix_group.arrays["nzval"])
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
    return (matrix, cache_group)
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
    child = Zarr.zopen_noerr(group.storage, "w"; path = child_zarr_path(group, name), fill_as_missing = false)  # NOJET
    if child isa ZArray
        group.arrays[name] = child
    elseif child isa ZGroup
        group.groups[name] = child
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
            axis_array = zcreate(
                String,
                axes_group(daf),
                axis,
                length(planned_axis.new_entries);
                compressor = Zarr.NoCompressor(),
            )
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

    return nothing
end

function replace_reorder_vector(
    daf::ZarrDaf,
    planned::Reorder.PlannedVector,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_vector, _ = Formats.format_get_vector(daf, planned.axis, planned.name)
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
        array = zcreate(String, group, planned.name, length(permuted); compressor = Zarr.NoCompressor())
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
            Formats.format_get_empty_sparse_vector!(daf, planned.axis, planned.name, T, source_nnz, I)
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
        destination, _ = Formats.format_get_empty_dense_vector!(daf, planned.axis, planned.name, T)
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
    source_matrix, _ = Formats.format_get_matrix(daf, planned.rows_axis, planned.columns_axis, planned.name)
    planned_rows = get(plan.planned_axes, planned.rows_axis, nothing)
    planned_columns = get(plan.planned_axes, planned.columns_axis, nothing)
    @assert planned_rows !== nothing || planned_columns !== nothing

    group = columns_axis_group(daf, planned.rows_axis, planned.columns_axis)
    nrows, ncols = size(source_matrix)

    if eltype(source_matrix) <: AbstractString
        permuted = Matrix{String}(undef, nrows, ncols)
        permute_matrix_into!(permuted, source_matrix, planned_rows, planned_columns, replacement_progress)
        delete_child(group, planned.name)
        array = zcreate(String, group, planned.name, nrows, ncols; compressor = Zarr.NoCompressor())
        array[:, :] = permuted  # NOJET
    elseif source_matrix isa SparseMatrixCSC
        T = eltype(source_matrix)
        I = eltype(source_matrix.colptr)
        source_nnz_val = nnz(source_matrix)
        src_colptr = copy(source_matrix.colptr)
        src_rowval = copy(source_matrix.rowval)
        src_nzval = copy(source_matrix.nzval)

        delete_child(group, planned.name)
        destination_colptr, destination_rowval, destination_nzval = Formats.format_get_empty_sparse_matrix!(
            daf,
            planned.rows_axis,
            planned.columns_axis,
            planned.name,
            T,
            source_nnz_val,
            I,
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
        destination, _ =
            Formats.format_get_empty_dense_matrix!(daf, planned.rows_axis, planned.columns_axis, planned.name, T)
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
    return true
end

function TanayLabUtilities.Brief.brief(value::ZarrDaf; name::Maybe{AbstractString} = nothing)::String
    if name === nothing
        name = value.name
    end
    return "ZarrDaf $(name)"
end

end  # module
