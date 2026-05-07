"""
Hard-link conversion between [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) and [`ZarrDaf`](@ref
DataAxesFormats.ZarrFormat.ZarrDaf) directories.

The two on-disk formats differ in their per-property metadata encoding (`FilesDaf` uses one JSON sidecar per array,
`ZarrDaf` uses one `zarr.json` per array), but both store every numeric blob byte-identically:

  - Flat (unpacked) dense array chunks and the `colptr`/`rowval`/`nzind`/`nzval` components of flat sparse arrays
    are raw little-endian bytes without headers — bit-for-bit identical between `FilesDaf`'s `<name>.data` /
    `<name>.<component>` and `ZarrDaf`'s single-chunk file at `<name>/c/0[/0]`.
  - Packed (chunked + compressed) dense properties and packed sparse components are stored in the v3 sharded-array
    binary format (ZEP-0002): one shard file per property (`<name>.shard` in `FilesDaf`, `<name>/c/0[/0]` in
    `ZarrDaf`). For the same input data, same `chunk_shape`, and same codec, the writer emits byte-identical shard
    bytes regardless of which backend hosts it — so the shard files hard-link cleanly across formats.

The functions in this module exploit these equivalences to convert one tree into the other by hard-linking every
numeric blob (flat or packed) and re-serializing only the metadata and the string-valued properties, so the on-disk
cost of a conversion is close to zero.

The consolidated metadata files of the two formats — `FilesDaf`'s `metadata.json` (a single-line JSON object mapping
relative path to per-property descriptor) and `ZarrDaf`'s root `zarr.json#consolidated_metadata.metadata` (the same
mapping in the on-disk shape `zarr-python` 3.x writes) — carry the same information about the same set of properties;
the per-property descriptor schemas are bijective. The translation rules between the two descriptor schemas are
formally defined by this module: every dense / sparse / packed / flat / scalar / axis case has a documented mapping,
and the source's consolidated metadata is read once at the start of a conversion to drive the per-property iteration.
The destination's consolidated metadata is rebuilt at the end of the conversion (via `refresh_consolidated_metadata!`)
so the destination is immediately usable, including for HTTP serving.

Hard-linking requires the source and destination to live on the same filesystem; each conversion verifies this up front
with an actual hard-link probe and refuses otherwise. Each conversion also refuses if the destination path already
exists — it never overwrites pre-existing data. On any error the partially-populated destination is removed, so the
destination is always either fully-populated or fully-absent.

Only the directory-tree `ZarrDaf` backend is supported (paths ending with `.daf.zarr`). The ZIP archive backend
(`.daf.zarr.zip`, `.dafs.zarr.zip#/group`) and the HTTP backend (`http(s)://…`) are rejected — neither provides the
per-chunk on-disk file that hard-linking needs.
"""
module ZarrConvert

export files_to_zarr
export zarr_to_files

using ..FilesFormat
using ..Formats
using ..PackedFormat
using ..Readers
using ..StorageTypes
using ..Writers
using ..ZarrFormat
using Base.Filesystem
using JSON
using SparseArrays
using TanayLabUtilities
using Zarr

import ..FilesFormat
import ..Operations.DTYPE_BY_NAME
import ..PackedFormat
import ..ReadOnly.DafReadOnlyWrapper
import ..ZarrFormat
import SparseArrays.indtype

const PROBE_NAME = ".daf_convert.probe"

"""
    zarr_to_files(;
        zarr_path::AbstractString,
        files_path::AbstractString,
    )::Nothing

Convert a [`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf) directory at `zarr_path` into an equivalent
[`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) directory at `files_path`. Every numeric blob (dense chunks and
the `colptr`/`rowval`/`nzind`/`nzval` components of sparse arrays) is hard-linked from the source into the destination;
scalars, axes, string vectors and string matrices are re-serialized through the respective readers/writers. `zarr_path`
must be a directory whose name ends with `.daf.zarr` (the ZIP and HTTP backends are rejected); `files_path` must not
already exist, and the two paths must live on the same filesystem.
"""
function zarr_to_files(; zarr_path::AbstractString, files_path::AbstractString)::Nothing
    verify_zarr_source(zarr_path)
    verify_destination_absent(files_path)

    abs_zarr = abspath(zarr_path)
    abs_files = abspath(files_path)
    mkdir(abs_files)
    try
        verify_same_filesystem(abs_zarr, abs_files)
        zarr_to_files_populate(abs_zarr, abs_files)
        @debug "Daf: zarr_to_files $(abs_zarr) -> $(abs_files)" _group = :daf_repos
    catch  # FLAKY TESTED
        rm(abs_files; force = true, recursive = true)  # UNTESTED
        rethrow()  # UNTESTED
    end
    return nothing
end

"""
    files_to_zarr(;
        files_path::AbstractString,
        zarr_path::AbstractString,
    )::Nothing

Convert a [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) directory at `files_path` into an equivalent
[`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf) directory at `zarr_path`. Every numeric blob (dense chunks and the
`colptr`/`rowval`/`nzind`/`nzval` components of sparse arrays) is hard-linked from the source into the destination;
scalars, axes, string vectors and string matrices are re-serialized through the respective readers/writers. `files_path`
must be an existing `FilesDaf` directory; `zarr_path` must not already exist, its name must end with `.daf.zarr` (the
ZIP and HTTP backends are rejected), and the two paths must live on the same filesystem.
"""
function files_to_zarr(; files_path::AbstractString, zarr_path::AbstractString)::Nothing
    verify_files_source(files_path)
    verify_zarr_destination_name(zarr_path)
    verify_destination_absent(zarr_path)

    abs_files = abspath(files_path)
    abs_zarr = abspath(zarr_path)
    mkdir(abs_zarr)
    try
        verify_same_filesystem(abs_files, abs_zarr)
        files_to_zarr_populate(abs_files, abs_zarr)
        @debug "Daf: files_to_zarr $(abs_files) -> $(abs_zarr)" _group = :daf_repos
    catch  # FLAKY TESTED
        rm(abs_zarr; force = true, recursive = true)  # UNTESTED
        rethrow()  # UNTESTED
    end
    return nothing
end

function verify_zarr_source(zarr_path::AbstractString)::Nothing
    if startswith(zarr_path, "http://") || startswith(zarr_path, "https://")
        error("can't convert a remote ZarrDaf over HTTP: $(zarr_path)")
    end
    if contains(zarr_path, '#') || endswith(zarr_path, ".zip")
        error("can't convert a zip-backed ZarrDaf: $(zarr_path)")
    end
    if !endswith(zarr_path, ".daf.zarr")
        error("ZarrDaf directory path must end with .daf.zarr: $(zarr_path)")
    end
    if !isdir(zarr_path)
        error("not a directory: $(zarr_path)")
    end
    root_json_path = "$(zarr_path)/zarr.json"
    if !isfile(root_json_path)
        error("not a daf zarr directory: $(zarr_path)")
    end
    root_json = read_json_dict(root_json_path)
    if !(get(root_json, "node_type", "") == "group" && haskey(get(root_json, "attributes", Dict()), "daf"))
        error("not a daf zarr directory: $(zarr_path)")
    end
    return nothing
end

function verify_zarr_destination_name(zarr_path::AbstractString)::Nothing
    if startswith(zarr_path, "http://") || startswith(zarr_path, "https://")
        error("can't convert into a remote ZarrDaf over HTTP: $(zarr_path)")
    end
    if contains(zarr_path, '#') || endswith(zarr_path, ".zip")
        error("can't convert into a zip-backed ZarrDaf: $(zarr_path)")
    end
    if !endswith(zarr_path, ".daf.zarr")
        error("ZarrDaf directory path must end with .daf.zarr: $(zarr_path)")
    end
    return nothing
end

function verify_files_source(files_path::AbstractString)::Nothing
    if !isdir(files_path)
        error("not a directory: $(files_path)")
    end
    if !isfile("$(files_path)/daf.json")
        error("not a daf files directory: $(files_path)")
    end
    return nothing
end

function verify_destination_absent(destination_path::AbstractString)::Nothing  # FLAKY TESTED
    if ispath(destination_path)
        error("destination already exists: $(destination_path)")
    end
    return nothing
end

function verify_same_filesystem(source_path::AbstractString, destination_path::AbstractString)::Nothing
    source_probe = "$(source_path)/$(PROBE_NAME)"
    destination_probe = "$(destination_path)/$(PROBE_NAME)"
    rm(source_probe; force = true)
    rm(destination_probe; force = true)
    write(source_probe, UInt8[])
    try
        try
            hardlink(source_probe, destination_probe)
        catch exception  # FLAKY TESTED
            error(chomp("""  # UNTESTED
                        can't hard-link between filesystems
                        from the source: $(source_path)
                        to the destination: $(destination_path)
                        underlying error: $(exception)
                        """))
        end
        rm(destination_probe; force = true)
    finally
        rm(source_probe; force = true)
    end
    return nothing
end

function zarr_to_files_populate(zarr_path::AbstractString, files_path::AbstractString)::Nothing
    # `ZarrDaf(... "r")` returns a `DafReadOnlyWrapper` over a `ZarrDaf`; unwrap to the inner `ZarrDaf` so the
    # per-property paths can touch its `ZGroup`s directly (the `DafReadOnly` interface only exposes the higher-level
    # reader API). The `Union{ZarrDaf, DafReadOnly}` return type covers both writable and read-only modes; for `"r"`
    # the runtime is always the wrapper branch.
    opened = ZarrDaf(zarr_path, "r")
    source = opened isa DafReadOnlyWrapper ? opened.daf::ZarrDaf : opened  # NOLINT
    destination = FilesDaf(files_path, "w+")::FilesDaf

    consolidated = read_zarr_consolidated_dict(zarr_path)

    for name in scalars_set(source)
        set_scalar!(destination, name, get_scalar(source, name))
    end

    for axis in axes_set(source)
        add_axis!(destination, axis, axis_vector(source, axis))
    end

    for axis in axes_set(source)
        for name in vectors_set(source, axis)
            zarr_vector_to_files(consolidated, source, destination, zarr_path, files_path, axis, name)
        end
    end

    for rows_axis in axes_set(source)
        for columns_axis in axes_set(source)
            for name in matrices_set(source, rows_axis, columns_axis; relayout = false)
                zarr_matrix_to_files(
                    consolidated,
                    source,
                    destination,
                    zarr_path,
                    files_path,
                    rows_axis,
                    columns_axis,
                    name,
                )
            end
        end
    end

    FilesFormat.metadata_json_rebuild!(destination)
    return nothing
end

# Parse the source's `consolidated_metadata.metadata` dict from root `zarr.json` once at the top of a conversion;
# every per-property descriptor we need for translation is keyed by its store-relative path inside this dict, so the
# rest of the converter doesn't reach back to disk for individual node `zarr.json` files. Returns the inner
# `{<path>: <full v3 metadata blob>}` mapping; missing or malformed `consolidated_metadata` is a hard error since the
# format guarantees it exists after every writable open.
function read_zarr_consolidated_dict(zarr_path::AbstractString)::Dict{String, Any}
    root_zarr_json = read_json_dict("$(zarr_path)/zarr.json")
    consolidated_field = root_zarr_json["consolidated_metadata"]::AbstractDict
    metadata = consolidated_field["metadata"]::AbstractDict
    return Dict{String, Any}(String(key) => value for (key, value) in metadata)
end

function zarr_vector_to_files(
    consolidated::Dict{String, Any},
    source::DafReader,
    destination::FilesDaf,
    zarr_path::AbstractString,
    files_path::AbstractString,
    axis::AbstractString,
    name::AbstractString,
)::Nothing
    source_dir = "$(zarr_path)/vectors/$(axis)/$(name)"
    destination_base = "$(files_path)/vectors/$(axis)/$(name)"
    base_key = "vectors/$(axis)/$(name)"
    zarr_json = consolidated[base_key]::AbstractDict

    if zarr_json["node_type"] == "array"
        if is_string_v3_dtype(zarr_json)
            set_vector!(destination, axis, name, get_vector(source, axis, name))
        else
            zarray = ZarrFormat.vectors_group(source).groups[axis].arrays[name]
            element_type = julia_type_from_v3_dtype(zarr_json["data_type"])
            n_elements = size(zarray, 1)
            if is_canonical_for_files(zarray, element_type, (n_elements,))
                link_zarr_to_files_dense(zarray, "$(destination_base).json", destination_base, element_type)
            else
                set_vector!(destination, axis, name, get_vector(source, axis, name); packed = true)
            end
        end
    else
        @assert zarr_json["node_type"] == "group" "unexpected node_type: $(zarr_json["node_type"])"
        zarr_sparse_vector_to_files(
            consolidated,
            source,
            destination,
            axis,
            name,
            source_dir,
            base_key,
            destination_base,
        )
    end
    return nothing
end

function zarr_sparse_vector_to_files(
    consolidated::Dict{String, Any},
    source::DafReader,
    destination::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    source_dir::AbstractString,
    base_key::AbstractString,
    destination_base::AbstractString,
)::Nothing
    vector_group = ZarrFormat.vectors_group(source).groups[axis].groups[name]
    nzind_array = vector_group.arrays["nzind"]
    ind_type = julia_type_from_v3_dtype((consolidated["$(base_key)/nzind"]::AbstractDict)["data_type"])
    nnz_int = size(nzind_array, 1)

    nzval_present = haskey(consolidated, "$(base_key)/nzval")
    if nzval_present
        nzval_array = vector_group.arrays["nzval"]
        element_type = julia_type_from_v3_dtype((consolidated["$(base_key)/nzval"]::AbstractDict)["data_type"])
        nzval_canonical = is_canonical_for_files(nzval_array, element_type, (nnz_int,))
        nzval_packed = PackedFormat.is_zarr_array_packed(nzval_array)
        nzval_chunk_shape = nzval_packed ? PackedFormat.packed_codec_from_zarray(nzval_array)[1] : nothing
        nzval_codec =
            nzval_packed ? PackedFormat.packed_codec_from_zarray(nzval_array)[2] : PackedFormat.compressor_for()
    else
        element_type = Bool
        nzval_canonical = true
        nzval_packed = false
        nzval_chunk_shape = nothing
        nzval_codec = PackedFormat.compressor_for()
    end

    nzind_canonical = is_canonical_for_files(nzind_array, ind_type, (nnz_int,))

    if !(nzind_canonical && nzval_canonical)
        # Fallback: any component non-canonical → re-encode the whole sparse property eagerly through `set_vector!`.
        set_vector!(destination, axis, name, get_vector(source, axis, name); packed = true)
        return nothing
    end

    nzind_packed = PackedFormat.is_zarr_array_packed(nzind_array)
    nzind_chunk_shape = nzind_packed ? PackedFormat.packed_codec_from_zarray(nzind_array)[1] : nothing
    nzind_codec = nzind_packed ? PackedFormat.packed_codec_from_zarray(nzind_array)[2] : PackedFormat.compressor_for()

    json_bytes = PackedFormat.sparse_vector_json_bytes(
        element_type,
        ind_type,
        nnz_int;
        nzind_chunk_shape,
        nzval_chunk_shape,
        nzind_codec,
        nzval_codec,
    )
    write("$(destination_base).json", json_bytes)
    link_zarr_chunk_to_sparse_component("$(source_dir)/nzind", "$(destination_base).nzind", nzind_packed)
    if nzval_present
        link_zarr_chunk_to_sparse_component("$(source_dir)/nzval", "$(destination_base).nzval", nzval_packed)
    end
    return nothing
end

# Hard-link a Zarr-side dense property's chunk file to a FilesDaf flat or packed sidecar. For packed sources the
# JSON sidecar carries the source's `chunk_shape` / codec so the linked bytes decode unchanged.
function link_zarr_to_files_dense(
    zarray::Zarr.ZArray,
    destination_json_path::AbstractString,
    destination_base::AbstractString,
    ::Type{T},
)::Nothing where {T}
    storage = zarray.storage
    @assert storage isa Zarr.DirectoryStore
    chunk_key_relative = ndims(zarray) == 1 ? "c/0" : "c/0/0"
    chunk_path = joinpath(storage.folder, zarray.path, chunk_key_relative)
    if PackedFormat.is_zarr_array_packed(zarray)
        chunk_shape, codec = PackedFormat.packed_codec_from_zarray(zarray)
        json_bytes = PackedFormat.packed_array_json_bytes(T, chunk_shape, codec)
        write(destination_json_path, json_bytes)
        hardlink(chunk_path, "$(destination_base).shard")
    else
        write(destination_json_path, PackedFormat.dense_array_json_bytes(T))
        hardlink(chunk_path, "$(destination_base).data")
    end
    return nothing
end

# Hard-link the chunk file of a Zarr-side sparse component sub-array. `packed=true` ⇒ source is single-shard
# (`c/0`) and target is `<base>.shard`; `packed=false` ⇒ source is single-chunk-uncompressed (`c/0`) and target is
# the flat path.
function link_zarr_chunk_to_sparse_component(  # FLAKY TESTED
    source_array_dir::AbstractString,
    destination_flat_path::AbstractString,
    packed::Bool,
)::Nothing
    if packed
        hardlink("$(source_array_dir)/c/0", "$(destination_flat_path).shard")
    else
        hardlink("$(source_array_dir)/c/0", destination_flat_path)
    end
    return nothing
end

function zarr_matrix_to_files(
    consolidated::Dict{String, Any},
    source::DafReader,
    destination::FilesDaf,
    zarr_path::AbstractString,
    files_path::AbstractString,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    source_dir = "$(zarr_path)/matrices/$(rows_axis)/$(columns_axis)/$(name)"
    destination_base = "$(files_path)/matrices/$(rows_axis)/$(columns_axis)/$(name)"
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    zarr_json = consolidated[base_key]::AbstractDict

    if zarr_json["node_type"] == "array"
        if is_string_v3_dtype(zarr_json)
            set_matrix!(
                destination,
                rows_axis,
                columns_axis,
                name,
                get_matrix(source, rows_axis, columns_axis, name);
                relayout = false,
            )
        else
            zarray = ZarrFormat.columns_axis_group(source, rows_axis, columns_axis).arrays[name]
            element_type = julia_type_from_v3_dtype(zarr_json["data_type"])
            shape = (size(zarray, 1), size(zarray, 2))
            if is_canonical_for_files(zarray, element_type, shape)
                link_zarr_to_files_dense(zarray, "$(destination_base).json", destination_base, element_type)
            else
                reencode_dense_matrix(source, destination, rows_axis, columns_axis, name, element_type, shape)
            end
        end
    else
        @assert zarr_json["node_type"] == "group" "unexpected node_type: $(zarr_json["node_type"])"
        zarr_sparse_matrix_to_files(
            consolidated,
            source,
            destination,
            rows_axis,
            columns_axis,
            name,
            source_dir,
            base_key,
            destination_base,
        )
    end
    return nothing
end

function zarr_sparse_matrix_to_files(
    consolidated::Dict{String, Any},
    source::DafReader,
    destination::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    source_dir::AbstractString,
    base_key::AbstractString,
    destination_base::AbstractString,
)::Nothing
    matrix_group = ZarrFormat.columns_axis_group(source, rows_axis, columns_axis).groups[name]
    colptr_array = matrix_group.arrays["colptr"]
    rowval_array = matrix_group.arrays["rowval"]
    ind_type = julia_type_from_v3_dtype((consolidated["$(base_key)/colptr"]::AbstractDict)["data_type"])
    n_columns = size(colptr_array, 1) - 1
    nnz_int = size(rowval_array, 1)

    nzval_present = haskey(consolidated, "$(base_key)/nzval")
    if nzval_present
        nzval_array = matrix_group.arrays["nzval"]
        element_type = julia_type_from_v3_dtype((consolidated["$(base_key)/nzval"]::AbstractDict)["data_type"])
        nzval_canonical = is_canonical_for_files(nzval_array, element_type, (nnz_int,))
        nzval_packed = PackedFormat.is_zarr_array_packed(nzval_array)
        nzval_chunk_shape = nzval_packed ? PackedFormat.packed_codec_from_zarray(nzval_array)[1] : nothing
        nzval_codec =
            nzval_packed ? PackedFormat.packed_codec_from_zarray(nzval_array)[2] : PackedFormat.compressor_for()
    else
        element_type = Bool
        nzval_canonical = true
        nzval_packed = false
        nzval_chunk_shape = nothing
        nzval_codec = PackedFormat.compressor_for()
    end

    colptr_canonical = is_canonical_for_files(colptr_array, ind_type, (n_columns + 1,))
    rowval_canonical = is_canonical_for_files(rowval_array, ind_type, (nnz_int,))

    if !(colptr_canonical && rowval_canonical && nzval_canonical)
        # Fallback: any component non-canonical → re-encode the whole property eagerly through `set_matrix!`. The
        # destination uses FilesDaf's canonical chunk_shape and the destination's `compressor_for()` global.
        set_matrix!(
            destination,
            rows_axis,
            columns_axis,
            name,
            get_matrix(source, rows_axis, columns_axis, name);
            relayout = false,
            packed = true,
        )
        return nothing
    end

    colptr_packed = PackedFormat.is_zarr_array_packed(colptr_array)
    rowval_packed = PackedFormat.is_zarr_array_packed(rowval_array)
    colptr_chunk_shape = colptr_packed ? PackedFormat.packed_codec_from_zarray(colptr_array)[1] : nothing
    rowval_chunk_shape = rowval_packed ? PackedFormat.packed_codec_from_zarray(rowval_array)[1] : nothing
    colptr_codec =
        colptr_packed ? PackedFormat.packed_codec_from_zarray(colptr_array)[2] : PackedFormat.compressor_for()
    rowval_codec =
        rowval_packed ? PackedFormat.packed_codec_from_zarray(rowval_array)[2] : PackedFormat.compressor_for()

    json_bytes = PackedFormat.sparse_matrix_json_bytes(
        element_type,
        ind_type,
        nnz_int,
        n_columns;
        colptr_chunk_shape,
        rowval_chunk_shape,
        nzval_chunk_shape,
        colptr_codec,
        rowval_codec,
        nzval_codec,
    )
    write("$(destination_base).json", json_bytes)
    link_zarr_chunk_to_sparse_component("$(source_dir)/colptr", "$(destination_base).colptr", colptr_packed)
    link_zarr_chunk_to_sparse_component("$(source_dir)/rowval", "$(destination_base).rowval", rowval_packed)
    if nzval_present
        link_zarr_chunk_to_sparse_component("$(source_dir)/nzval", "$(destination_base).nzval", nzval_packed)
    end
    return nothing
end

# True if `zarray`'s on-disk encoding is byte-identical to what `FilesDaf` would write for the same data right now —
# the only condition under which `zarr_to_files` may safely hard-link the chunk file. Below-threshold flat ⇒ source
# must be `can_mmap` (single-chunk uncompressed). Above-threshold packed ⇒ source must be single-shard sharded with
# inner `chunk_shape == chunks_for(true, shape, T)` and inner codec matching `compressor_for()`.
# The `zarray.metadata.chunks != shape`, `sharding.chunk_shape != canonical_chunk_shape`, and `length(bytes_bytes) !=
# 1` checks defend against foreign or older zarr writers that produce multi-chunk, off-shape, or compound-codec
# arrays; the `compressor_for()` writer never produces any of those, so today's tests can only exercise the codec
# mismatch branch (the `return` at the bottom).
function is_canonical_for_files(zarray::Zarr.ZArray, ::Type{T}, shape::NTuple{N, Int})::Bool where {T, N}
    canonical_chunk_shape = PackedFormat.chunks_for(true, shape, T)
    if canonical_chunk_shape === nothing
        return ZarrFormat.can_mmap(zarray) && !isempty(zarray)
    end
    if !PackedFormat.is_zarr_array_packed(zarray)
        return false
    end
    if zarray.metadata.chunks != shape
        return false  # UNTESTED
    end
    sharding = zarray.metadata.pipeline.array_bytes
    if sharding.chunk_shape != canonical_chunk_shape
        return false  # UNTESTED
    end
    bytes_bytes = sharding.codecs.bytes_bytes
    if length(bytes_bytes) != 1
        return false  # UNTESTED
    end
    target_codec = PackedFormat.compressor_for()
    target_bytes_bytes = PackedFormat.v3_bytes_codecs_for(target_codec, T)
    return length(target_bytes_bytes) == 1 && bytes_bytes[1] == target_bytes_bytes[1]
end

# Re-encode a non-canonical Zarr-side dense matrix into the FilesDaf destination via the streaming write path.
# Reads the source column-by-column (`get_matrix(source, ...; relayout = false)` returns a column-major view; the
# Zarr decoder fetches only the chunks intersecting each column on demand), and submits each column to
# `empty_dense_matrix!`'s `PackedDenseMatrix` wrapper. Bounded RAM = one column buffer per thread.
function reencode_dense_matrix(  # FLAKY TESTED
    source::DafReader,
    destination::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    shape::NTuple{2, Int},
)::Nothing where {T}
    n_columns = shape[2]
    source_matrix = get_matrix(source, rows_axis, columns_axis, name; relayout = false)
    empty_dense_matrix!(destination, rows_axis, columns_axis, name, T; packed = true) do filled
        TanayLabUtilities.parallel_loop_wo_rng(1:n_columns; name = "zarr_to_files_reencode") do column_index
            @views filled[:, column_index] .= source_matrix[:, column_index]
        end
    end
    return nothing
end

function files_to_zarr_populate(files_path::AbstractString, zarr_path::AbstractString)::Nothing
    source = FilesDaf(files_path, "r")
    destination = ZarrDaf(zarr_path, "w+")::ZarrDaf  # NOJET

    consolidated = read_files_consolidated_dict(files_path)

    for name in scalars_set(source)
        set_scalar!(destination, name, get_scalar(source, name))
    end

    for axis in axes_set(source)
        add_axis!(destination, axis, axis_vector(source, axis))
    end

    for axis in axes_set(source)
        for name in vectors_set(source, axis)
            files_vector_to_zarr(consolidated, source, destination, files_path, zarr_path, axis, name)
        end
    end

    for rows_axis in axes_set(source)
        for columns_axis in axes_set(source)
            for name in matrices_set(source, rows_axis, columns_axis; relayout = false)
                files_matrix_to_zarr(
                    consolidated,
                    source,
                    destination,
                    files_path,
                    zarr_path,
                    rows_axis,
                    columns_axis,
                    name,
                )
            end
        end
    end

    ZarrFormat.refresh_consolidated_metadata!(destination)
    return nothing
end

# Parse the source FilesDaf's `metadata.json` once at the top of a conversion. Every per-property descriptor we need
# for translation is keyed by its store-relative path (no `.json` suffix) inside this dict, so the rest of the
# converter doesn't reach back to disk for individual `<path>.json` sidecars. Returns the `{<path>: <descriptor>}`
# mapping; missing or malformed `metadata.json` is a hard error since the format guarantees it exists after every
# writable open.
function read_files_consolidated_dict(files_path::AbstractString)::Dict{String, Any}
    parsed = read_json_dict("$(files_path)/metadata.json")
    return Dict{String, Any}(String(key) => value for (key, value) in parsed)
end

function files_vector_to_zarr(
    consolidated::Dict{String, Any},
    source::DafReader,
    destination::ZarrDaf,
    files_path::AbstractString,
    zarr_path::AbstractString,
    axis::AbstractString,
    name::AbstractString,
)::Nothing
    json = consolidated["vectors/$(axis)/$(name)"]::AbstractDict
    format = json["format"]

    parent_group = ZarrFormat.vectors_group(destination).groups[axis]
    source_base = "$(files_path)/vectors/$(axis)/$(name)"
    destination_dir = "$(zarr_path)/vectors/$(axis)/$(name)"

    if format == "dense"
        eltype_name = String(json["eltype"])
        if is_string_eltype(eltype_name)
            set_vector!(destination, axis, name, get_vector(source, axis, name))
            return nothing
        end
        element_type = julia_type_from_files_eltype(eltype_name)
        n_elements = axis_length(source, axis)
        if get(json, "packed", false) === true
            create_files_to_zarr_packed_dense(
                parent_group,
                name,
                element_type,
                (n_elements,),
                json,
                "$(source_base).shard",
                "$(destination_dir)/c/0",
            )
        else
            ZarrFormat.dense_zcreate(element_type, parent_group, name, false, (n_elements,))
            hardlink_v3_chunk("$(source_base).data", "$(destination_dir)/c/0")
        end
    else
        @assert format == "sparse"
        eltype_name, indtype_name = PackedFormat.parse_sparse_descriptor(json, "nzind")
        if is_string_eltype(eltype_name)
            # Sparse string vectors round-trip through `set_vector!`; no current writer produces this layout, but the
            # reader supports it for future-proofing.
            set_vector!(destination, axis, name, get_vector(source, axis, name))  # UNTESTED
            return nothing  # UNTESTED
        end
        ind_type = julia_type_from_files_eltype(indtype_name)
        element_type = julia_type_from_files_eltype(eltype_name)
        nzind_descriptor = get(json, "nzind", nothing)
        nzval_descriptor = get(json, "nzval", nothing)
        nnz_int = sparse_component_count(nzind_descriptor, source_base, "nzind", ind_type)

        vector_group = Zarr.zgroup(parent_group, name)
        link_files_to_zarr_sparse_component(
            vector_group,
            "nzind",
            ind_type,
            nnz_int,
            nzind_descriptor,
            "$(source_base).nzind",
            "$(destination_dir)/nzind",
        )
        if has_files_sparse_component(source_base, "nzval", nzval_descriptor)
            link_files_to_zarr_sparse_component(
                vector_group,
                "nzval",
                element_type,
                nnz_int,
                nzval_descriptor,
                "$(source_base).nzval",
                "$(destination_dir)/nzval",
            )
        end
    end
    return nothing
end

function files_matrix_to_zarr(
    consolidated::Dict{String, Any},
    source::DafReader,
    destination::ZarrDaf,
    files_path::AbstractString,
    zarr_path::AbstractString,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    json = consolidated["matrices/$(rows_axis)/$(columns_axis)/$(name)"]::AbstractDict
    format = json["format"]

    parent_group = ZarrFormat.columns_axis_group(destination, rows_axis, columns_axis)
    source_base = "$(files_path)/matrices/$(rows_axis)/$(columns_axis)/$(name)"
    destination_dir = "$(zarr_path)/matrices/$(rows_axis)/$(columns_axis)/$(name)"
    n_rows = axis_length(source, rows_axis)
    n_columns = axis_length(source, columns_axis)

    if format == "dense"
        eltype_name = String(json["eltype"])
        if is_string_eltype(eltype_name)
            set_matrix!(
                destination,
                rows_axis,
                columns_axis,
                name,
                get_matrix(source, rows_axis, columns_axis, name);
                relayout = false,
            )
            return nothing
        end
        element_type = julia_type_from_files_eltype(eltype_name)
        if get(json, "packed", false) === true
            create_files_to_zarr_packed_dense(
                parent_group,
                name,
                element_type,
                (n_rows, n_columns),
                json,
                "$(source_base).shard",
                "$(destination_dir)/c/0/0",
            )
        else
            ZarrFormat.dense_zcreate(element_type, parent_group, name, false, (n_rows, n_columns))
            hardlink_v3_chunk("$(source_base).data", "$(destination_dir)/c/0/0")
        end
    else
        @assert format == "sparse"
        eltype_name, indtype_name = PackedFormat.parse_sparse_descriptor(json, "rowval")
        if is_string_eltype(eltype_name)
            # Sparse string matrices round-trip through `set_matrix!`; no current writer produces this layout, but the
            # reader supports it for future-proofing.
            set_matrix!(  # UNTESTED
                destination,
                rows_axis,
                columns_axis,
                name,
                get_matrix(source, rows_axis, columns_axis, name);
                relayout = false,
            )
            return nothing  # UNTESTED
        end
        ind_type = julia_type_from_files_eltype(indtype_name)
        element_type = julia_type_from_files_eltype(eltype_name)
        colptr_descriptor = get(json, "colptr", nothing)
        rowval_descriptor = get(json, "rowval", nothing)
        nzval_descriptor = get(json, "nzval", nothing)
        nnz_int = sparse_component_count(rowval_descriptor, source_base, "rowval", ind_type)

        matrix_group = Zarr.zgroup(parent_group, name)
        link_files_to_zarr_sparse_component(
            matrix_group,
            "colptr",
            ind_type,
            n_columns + 1,
            colptr_descriptor,
            "$(source_base).colptr",
            "$(destination_dir)/colptr",
        )
        link_files_to_zarr_sparse_component(
            matrix_group,
            "rowval",
            ind_type,
            nnz_int,
            rowval_descriptor,
            "$(source_base).rowval",
            "$(destination_dir)/rowval",
        )
        if has_files_sparse_component(source_base, "nzval", nzval_descriptor)
            link_files_to_zarr_sparse_component(
                matrix_group,
                "nzval",
                element_type,
                nnz_int,
                nzval_descriptor,
                "$(source_base).nzval",
                "$(destination_dir)/nzval",
            )
        end
    end
    return nothing
end

# Resolve the source-side per-component element count from the descriptor's `n_elements` (v1.1) or the flat-file size
# (v1.0 fallback). Mirrors the logic FilesDaf's reader uses in `open_sparse_component`.
function sparse_component_count(
    descriptor::Maybe{AbstractDict},
    source_base::AbstractString,
    component::AbstractString,
    ::Type{T},
)::Int where {T}
    if descriptor isa AbstractDict && haskey(descriptor, "n_elements")
        return Int(descriptor["n_elements"])
    end
    return Int(div(filesize("$(source_base).$(component)"), sizeof(T)))  # UNTESTED
end

# Whether the FilesDaf source has a stored value for the named sparse component, considering both the flat
# (`<base>.<component>`) and packed (`<base>.<component>.shard`) on-disk paths. Mirrors FilesDaf's
# `has_sparse_component`. The descriptor is consulted only as a hint; the file system is authoritative.
function has_files_sparse_component(  # FLAKY TESTED
    source_base::AbstractString,
    component::AbstractString,
    descriptor::Maybe{AbstractDict},  # NOLINT
)::Bool
    return isfile("$(source_base).$(component)") || isfile("$(source_base).$(component).shard")
end

# Build the destination ZArray for a packed dense FilesDaf source and hard-link the source `.shard` to the
# destination's chunk-key path. The destination metadata reuses the source's `chunk_shape` / codec so the linked
# bytes decode unchanged.
function create_files_to_zarr_packed_dense(
    group::Zarr.ZGroup,
    name::AbstractString,
    ::Type{T},
    shape::NTuple{N, Int},
    json::AbstractDict,
    source_shard_path::AbstractString,
    destination_chunk_path::AbstractString,
)::Nothing where {T, N}
    chunk_shape = NTuple{N, Int}(json["chunk_shape"])  # NOJET
    codec = PackedFormat.PackedCodec(Symbol(json["compression"]), Int(json["compression_level"]))
    bytes_bytes_codecs = PackedFormat.v3_bytes_codecs_for(codec, T)
    ZarrFormat.sharded_zcreate(T, group, name, shape, chunk_shape, bytes_bytes_codecs)
    hardlink_v3_chunk(source_shard_path, destination_chunk_path)
    return nothing
end

# Hard-link a single sparse-component blob from a FilesDaf source into the matching Zarr destination sub-array.
# Routes flat (`<source_base>.<component>`) to a flat single-chunk-uncompressed ZArray and packed
# (`<source_base>.<component>.shard`) to a single-shard sharded ZArray with the source's codec.
function link_files_to_zarr_sparse_component(
    parent_group::Zarr.ZGroup,
    component::AbstractString,
    ::Type{T},
    n_elements::Int,
    descriptor::Maybe{AbstractDict},
    source_flat_path::AbstractString,
    destination_dir::AbstractString,
)::Nothing where {T}
    if descriptor isa AbstractDict && get(descriptor, "packed", false) === true
        chunk_shape = (Int(descriptor["chunk_shape"][1]),)
        codec = PackedFormat.PackedCodec(Symbol(descriptor["compression"]), Int(descriptor["compression_level"]))
        bytes_bytes_codecs = PackedFormat.v3_bytes_codecs_for(codec, T)
        ZarrFormat.sharded_zcreate(T, parent_group, component, (n_elements,), chunk_shape, bytes_bytes_codecs)
        hardlink_v3_chunk("$(source_flat_path).shard", "$(destination_dir)/c/0")
    else
        ZarrFormat.dense_zcreate(T, parent_group, component, false, (n_elements,))
        hardlink_v3_chunk(source_flat_path, "$(destination_dir)/c/0")
    end
    return nothing
end

# Hard-link `source` to `destination`, creating the destination's parent directory if it does not yet exist.
# The v3 `c/` chunk-prefix sub-directory of an array is not created by `numeric_zcreate` (only `zarr.json` is
# emitted there), so each chunk hard-link needs its parent.
function hardlink_v3_chunk(source::AbstractString, destination::AbstractString)::Nothing
    mkpath(dirname(destination))
    hardlink(source, destination)
    return nothing
end

function read_json_dict(path::AbstractString)::Dict{String, Any}
    json = JSON.parsefile(path)
    @assert json isa AbstractDict
    return Dict{String, Any}(String(k) => v for (k, v) in json)
end

function is_string_v3_dtype(zarr_json::AbstractDict)::Bool
    return zarr_json["data_type"] == "string"
end

function is_string_eltype(eltype_name::AbstractString)::Bool
    return eltype_name == "String" || eltype_name == "string"
end

function julia_type_from_files_eltype(eltype_name::AbstractString)::Type
    type = DTYPE_BY_NAME[eltype_name]
    @assert type !== nothing "unsupported element type: $(eltype_name)"
    return type
end

# Translate a Zarr v3 `data_type` string ("float32", "int8", "bool", ...) to its Julia type. The mapping mirrors
# `Zarr.typestr3(t)` which lower-cases the type name; `DTYPE_BY_NAME` already has the lower-cased keys.
function julia_type_from_v3_dtype(data_type::AbstractString)::Type
    type = get(DTYPE_BY_NAME, data_type, nothing)
    @assert type !== nothing "unsupported zarr v3 data_type: $(data_type)"
    return type
end

end  # module
