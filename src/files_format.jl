"""
A `Daf` storage format in disk files. This is an efficient way to persist `Daf` data in a filesystem, and offers a
different trade-off compared to storing the data in an HDF5 file.

On the downside, this being a directory, you need to create a `zip` or `tar` or some other form of archive file if you
want to publish it. Also, accessing `FilesDaf` will consume multiple file descriptors as opposed to just one for HDF5,
and, of course, HDF5 has libraries to support it in most systems.

On the upside, the format of the files is so simple that it is trivial to access them from any programming environment,
without requiring a complex library like HDF5. In addition, since each scalar, vector or matrix property is stored in a
separate file, deleting data automatically frees the storage (unlike in an HDF5 file, where you must manually repack the
file to actually release the storage). Also, you can use standard tools to look at the data (e.g. use `ls` or the
Windows file explorer to view the list of properties, how much space each one uses, when it was created, etc.). Most
importantly, this allows using standard tools like `make` to create automatic repeatable processing workflows.

For packed (chunked + compressed) vectors and matrices, `FilesDaf` lays the chunks out in a bucketed directory hierarchy (each path
component is a 3-digit zero-padded index, with extra levels accruing for higher counts). This keeps the number of
entries in any single directory bounded regardless of total chunk count, so `FilesDaf` scales gracefully to very large
packed matrices (hundreds of thousands of chunks) without stressing the filesystem or interactive tools like `ls` or
file explorers. This is a meaningful difference from
[`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf), which stores all chunks of an array as flat siblings in one
directory.

We use multiple files to store `Daf` data, under some root directory, as follows:

  - The directory will contain 4 sub-directories: `scalars`, `axes`, `vectors`, and `matrices`, and two files at the
    root: `daf.json` (always) and `metadata.json` (a consolidated index, regenerated on demand — see below).

  - The `daf.json` signifies that the directory contains `Daf` data. In this file, there should be a mapping with a
    `version` key whose value is an array of two integers. The first is the major version number and the second is the
    minor version number, using [semantic versioning](https://semver.org/). This makes it easy to test whether a
    directory does/n't contain `Daf` data, and which version of the internal structure it is using. Defined versions
    are `[1,0]` and `[1,1]`. New code emits `[1,1]`; the reader accepts both. The on-disk difference is the JSON
    descriptor for sparse properties (see below) — the binary data files are unchanged across versions.

  - The `metadata.json` is a consolidated index of every property's descriptor. After it has been seeded once (by
    walking the tree on a writable open), subsequent opens consume it directly instead of walking, and an
    HTTP-served `FilesDaf` can be browsed without per-property `readdir` round-trips. Its content is bijective with the per-property descriptors documented below, and with
    the consolidated metadata that
    [`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf) embeds in its root `zarr.json` (see the `ZarrDaf`
    documentation for the formal mapping; [`zarr_to_files`](@ref DataAxesFormats.ZarrConvert.zarr_to_files) and
    [`files_to_zarr`](@ref DataAxesFormats.ZarrConvert.files_to_zarr) translate between them).
    The file is a single-line JSON object mapping each property's relative path to its descriptor:
    `{"<relative_path>":<descriptor>,...}`, where `<relative_path>` is the property's location relative to the root
    (e.g. `vectors/cell/batch`, `matrices/cell/gene/UMIs`, `axes/cell`, `scalars/version`) and `<descriptor>` is
    byte-identical to the per-property sidecar JSON content described below (for axes, the descriptor is
    `{"format":"axis","n_entries":<N>}`). On `set!` the file is appended in place via byte-level surgery:
    `truncate` the trailing `}` and write `,"<new_path>":<descriptor>}` (or `"<new_path>":<descriptor>}` if the
    file was the empty object `{}`). This makes a `set!` an O(size-of-one-descriptor) write rather than an
    O(N-properties) rewrite. On `delete!` the file is rebuilt from scratch by walking the tree. On *every* open
    (read or write), if the file is missing or fails to parse (the latter handles a torn write from a crash
    mid-`set!`) we attempt to rebuild it by walking the tree. The rebuild is best-effort: if it fails (e.g.
    because the underlying filesystem is read-only) the error is swallowed for read-only opens and rethrown for
    writable opens. So the workflow `unzip foo.daf.zip; open foo.daf; serve over HTTP` works as long as the
    unzipped tree is on a writable filesystem at first-open time; an HTTP-served `FilesDaf` placed on a frozen
    filesystem without `metadata.json` requires one open on a writable filesystem to seed the file.

  - The `scalars` directory contains scalar properties, each as in its own `name.json` file, containing a mapping with
    a `type` key whose value is the data type of the scalar (one of the `StorageScalar` types, with `String` for a
    string scalar) and a `value` key whose value is the actual scalar value.

  - The `axes` directory contains a `name.txt` file per axis, where each line contains a name of an axis entry.

  - The `vectors` directory contains a directory per axis, containing the vectors. For every vector, a `name.json` file
    will contain a mapping with an `eltype` key specifying the type of the vector element, and a `format` key specifying
    how the data is stored on disk, one of `dense` and `sparse`.

    If the `format` is `dense`, then there will be a file containing the vector entries, either `name.txt` for strings
    (with a value per line), or `name.data` for binary data (which we can memory-map for direct access).

    If the format is `sparse`, then in v1.1 the JSON contains a per-property descriptor for each component:
    `nzind` and `nzval`, each shaped like a stand-alone dense-vector descriptor (a `format` key holding `"dense"` and
    an `eltype` key). The component bytes live in `name.nzind` (indices of the non-zero entries) and `name.nzval`
    (values of the non-zero entries), both memory-mappable. See Julia's `SparseVector` implementation for details.
    The legacy v1.0 schema instead writes top-level `eltype` and `indtype` keys; the reader accepts both shapes.

    If the data type is `Bool` then the data vector is typically all-`true` values; in this case we simply skip storing
    it.

    We switch to using this sparse format for sufficiently sparse string data (where the zero value is the empty
    string). This isn't supported by `SparseVector` because "reasons" so we load it into a dense vector. In this case we
    name the values file `name.nztxt`.

  - The `matrices` directly contains a directory per rows axis, which contains a directory per columns axis, which
    contains the matrices. For each matrix, a `name.json` file will contain a mapping with an `eltype` key specifying
    the type of the matrix element, and a `format` key specifying how the data is stored on disk, one of `dense` and
    `sparse`.

    If the `format` is `dense`, then there will be a `name.data` binary file in column-major layout (which we can
    memory-map for direct access).

    If the format is `sparse`, then in v1.1 the JSON contains a per-property descriptor for each component:
    `colptr`, `rowval`, and `nzval`, each shaped like a stand-alone dense-vector descriptor (a `format` key holding
    `"dense"` and an `eltype` key). The component bytes live in `name.colptr`, `name.rowval` (indices of the non-zero
    values) and `name.nzval` (values of the non-zero entries), all memory-mappable. See Julia's `SparseMatrixCSC`
    implementation for details. The legacy v1.0 schema instead writes top-level `eltype` and `indtype` keys; the
    reader accepts both shapes.

    If the data type is `Bool` then the data vector is typically all-`true` values; in this case we simply skip storing
    it.

    We switch to using this sparse format for sufficiently sparse string data (where the zero value is the empty
    string). This isn't supported by `SparseMatrixCSC` because "reasons" so we load it into a dense matrix. In this case
    we name the values file `name.nztxt`.

!!! note

    Since data is stored in files using the property names, we are sadly susceptible to the operating system vagaries
    when it comes to "what is a valid property name" (e.g., no `/` characters allowed) and whether property names
    are/not case sensitive. In theory, we could just encode the property names somehow but that would make the file
    names opaque, which would lose out on a lot of the benefit of using files. It **always** pays to have "sane",
    simple, unique property names, using only alphanumeric characters, that would be a valid variable name in most
    programming languages.

!!! warning

    The byte-surgery append on `metadata.json` and the staged-rename rebuild assume a single writer at a time. The
    in-process Daf write lock serializes writers within one Julia process; opening the same `FilesDaf` directory
    from multiple processes (or multiple machines, e.g. NFS) and writing concurrently can interleave appends and
    corrupt `metadata.json`. Open writable from one process at a time.

Example directory structure:

    example-daf-dataset-root-directory/
    ├─ daf.json
    ├─ metadata.json
    ├─ scalars/
    │  └─ version.json
    ├─ axes/
    │  ├─ cell.txt
    │  └─ gene.txt
    ├─ vectors/
    │  ├─ cell/
    │  │  ├─ batch.json
    │  │  └─ batch.txt
    │  └─ gene/
    │     ├─ is_marker.json
    │     └─ is_marker.data
    └─ matrices/
       ├─ cell/
       │  ├─ cell/
       │  └─ gene/
       │     ├─ UMIs.json
       │     ├─ UMIs.colptr
       │     ├─ UMIs.rowval
       │     └─ UMIs.nzval
       └─ gene/
          ├─ cell/
          └─ gene/

!!! note

    All binary data is stored as a sequence of elements, in little endian byte order (which is the native order for
    modern CPUs), without any headers or padding. (Dense) matrices are stored in column-major layout (which matches
    Julia's native matrix layout).

    All string data is stored in lines, one entry per line, separated by a `\n` character (regardless of the OS used).
    Therefore, you can't have a line break inside an axis entry name or in a vector property value, at least not
    when storing it in `FilesDaf`.

That's all there is to it. The format is intentionally simple and transparent to maximize its accessibility by other
(standard) tools. Still, it is easiest to create the data using the Julia `Daf` package.

!!! note

    The code here assumes the files data obeys all the above conventions and restrictions. As long as you only create
    and access `Daf` data in files using [`FilesDaf`](@ref), then the code will work as expected (assuming no bugs).
    However, if you do this in some other way (e.g., directly using the filesystem and custom tools), and the result is
    invalid, then the code here may fail with "less than friendly" error messages.
"""
module FilesFormat

export FilesDaf

using ..Formats
using ..LazySparse
using ..PackedFormat
using ..ReadOnly
using ..Readers
using ..Reorder
using ..StorageTypes
using ..Writers
using Base.Filesystem
using DiskArrays
using JSON
using Mmap
using ProgressMeter
using SparseArrays
using StringViews
using TanayLabUtilities

import ..Formats
import ..Formats.Internal
import ..Operations.DTYPE_BY_NAME
import ..PackedFormat.IncrementalShardWriter
import ..PackedFormat.PackedCodec
import ..PackedFormat.PackedDaf
import ..PackedFormat.PackedDenseMatrix
import ..PackedFormat.chunks_for
import ..PackedFormat.compressor_for
import ..PackedFormat.dense_array_json_bytes
import ..PackedFormat.eltype_for_descriptor
import ..PackedFormat.encode_packed_dense_array
import ..PackedFormat.finalize_shard!
import ..PackedFormat.flush_packed_dense_matrix!
import ..PackedFormat.json_eltype_name
import ..PackedFormat.open_packed_dense_array
import ..PackedFormat.open_shard_as_zarray
import ..PackedFormat.open_streaming_shard_writer
import ..PackedFormat.packed_array_json_bytes
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
import ..PackedFormat.packed_format_write_sparse_component!
import ..PackedFormat.packed_format_write_sparse_numeric_matrix!
import ..PackedFormat.packed_format_write_sparse_numeric_vector!
import ..PackedFormat.packed_has_entry
import ..PackedFormat.packed_local_cache_mb
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
import ..PackedFormat.submit_shard_chunk!
import ..PackedFormat.v3_bytes_codecs_for
import ..Reorder
import SparseArrays.indtype

"""
The specific major version of the [`FilesDaf`](@ref) format that is supported by this code (`1`). The code will refuse
to access data that is stored in a different major format.
"""
MAJOR_VERSION::UInt8 = 1

"""
The maximal minor version of the [`FilesDaf`](@ref) format that is supported by this code (`1`). The code will refuse to
access data that is stored with the expected major version (`1`), but that uses a higher minor version.

!!! note

    Modifying data that is stored with a lower minor version number **may** increase its minor version number.
"""
MINOR_VERSION::UInt8 = 1

"""
    FilesDaf(
        path::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing,
        packed::Bool = false]
    )

Storage in disk files in some directory.

By convention the root directory name carries the `.daf` suffix (e.g. `cells.daf/`), but this isn't enforced — any
directory containing a `daf.json` is a valid `FilesDaf`. The matching single-file ZIP form lives under
[`ZipDaf`](@ref DataAxesFormats.ZipFormat.ZipDaf), with the `.daf.zip` and `.dafs.zip#/group` path conventions.

When opening an existing data set, if `name` is not specified, and there exists a "name" scalar property, it is used as
the name. Otherwise, the `path` will be used as the name.

If `packed` is `true`, subsequent writes through this handle default to the packed (chunked + compressed) on-disk
encoding for properties whose uncompressed size is at or above
[`DAF_PACKED_TARGET_CHUNK_KB`](@ref DataAxesFormats.PackedFormat.DAF_PACKED_TARGET_CHUNK_KB). Per-call `packed` kwargs
on `set_*!` / `empty_*!` / `copy_*!` override this default. The default is `false` (today's flat encoding).

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type         |
|:---- |:-------------------- |:------------------------- |:------------------- |:--------------------- |
| `r`  | No                   | No                        | No                  | [`DafReadOnly`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`FilesDaf`](@ref)    |
| `w+` | Yes                  | Yes                       | No                  | [`FilesDaf`](@ref)    |
| `w`  | Yes                  | Yes                       | Yes                 | [`FilesDaf`](@ref)    |
"""
struct FilesDaf <: PackedDaf
    name::AbstractString
    internal::Internal
    path::AbstractString
    mode::AbstractString
    files_mode::String
end

function FilesDaf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
    packed::Bool = false,
)::Union{FilesDaf, DafReadOnly}
    is_read_only, create_if_missing, truncate_if_exists = Formats.parse_mode(mode)

    if isfile(path)
        error("not a directory: $(path)")
    end

    if truncate_if_exists && isdir(path)
        rm(path; force = true, recursive = true)
        report_modified!(path)
    end

    daf_file_path = "$(path)/daf.json"
    if create_if_missing
        if !isdir(path)
            mkpath(path)
            report_modified!(path)
        end

        if !cached_ispath(daf_file_path)  # NOJET
            write(daf_file_path, "{\"version\":[$(MAJOR_VERSION),$(MINOR_VERSION)]}\n")
            report_modified!(daf_file_path)
            for directory in ("scalars", "axes", "vectors", "matrices")
                full_path = "$(path)/$(directory)"
                mkdir(full_path)
                report_modified!(full_path)
            end
        end
    end

    if !isfile(daf_file_path)
        error("not a daf directory: $(path)")
    end
    daf_json = JSON.parsefile(daf_file_path)  # NOJET
    @assert daf_json isa AbstractDict
    daf_version = daf_json["version"]
    @assert daf_version isa AbstractVector
    @assert length(daf_version) == 2

    if Int(daf_version[1]) != MAJOR_VERSION || Int(daf_version[2]) > MINOR_VERSION
        error(chomp("""
              incompatible format version: $(daf_version[1]).$(daf_version[2])
              for the daf directory: $(path)
              the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
              """))
    end

    if name === nothing
        name_path = "$(path)/scalars/name.json"
        if cached_ispath(name_path)
            name = string(read_scalar(name_path))
        else
            name = path
        end
    end
    name = unique_name(name)

    if is_read_only
        writable_files = FilesDaf(name, Internal(; is_frozen = true, packed_default = packed), abspath(path), mode, "r")
        ensure_metadata_json!(writable_files)
        file = read_only(writable_files)
    else
        file = FilesDaf(name, Internal(; is_frozen = false, packed_default = packed), abspath(path), mode, "r+")
        ensure_metadata_json!(file)
    end
    @debug "Daf: $(brief(file)) path: $(path)" _group = :daf_repos
    return file
end

function Readers.is_leaf(::FilesDaf)::Bool  # FLAKY TESTED
    return true
end

function Readers.is_leaf(::Type{FilesDaf})::Bool  # FLAKY TESTED
    return true
end

function Formats.format_has_scalar(files::FilesDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(files)
    return cached_ispath("$(files.path)/scalars/$(name).json")
end

function Formats.format_set_scalar!(
    files::FilesDaf,
    name::AbstractString,
    value::StorageScalar,
)::Maybe{Formats.CacheGroup}
    @assert Formats.has_data_write_lock(files)
    type = typeof(value)
    if type <: AbstractString
        type = String
    end

    json_bytes = Vector{UInt8}(JSON.json(Dict("type" => "$(type)", "value" => value)) * "\n")
    write_property_json!(files, "scalars/$(name)", json_bytes)
    return Formats.MemoryData
end

function Formats.format_delete_scalar!(files::FilesDaf, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(files)
    json_path = "$(files.path)/scalars/$(name).json"
    rm(json_path; force = true)
    report_modified!(json_path)
    metadata_json_rebuild!(files)
    return nothing
end

function Formats.format_get_scalar(files::FilesDaf, name::AbstractString)::Tuple{StorageScalar, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(files)
    return (read_scalar("$(files.path)/scalars/$(name).json"), Formats.MemoryData)
end

function read_scalar(path::AbstractString)::StorageScalar
    json = JSON.parsefile(path)
    @assert json isa AbstractDict
    dtype_name = json["type"]
    json_value = json["value"]

    if dtype_name == "String" || dtype_name == "string"
        @assert json_value isa AbstractString
        value = json_value
    else
        type = get(DTYPE_BY_NAME, dtype_name, nothing)
        @assert type !== nothing
        value = convert(type, json_value)
    end

    return value
end

function Formats.format_scalars_set(files::FilesDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return get_names_set("$(files.path)/scalars", ".json")
end

function Formats.format_has_axis(files::FilesDaf, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(files)
    return cached_ispath("$(files.path)/axes/$(axis).txt")
end

function write_lines_file(path::AbstractString, lines::AbstractArray{<:AbstractString})::Nothing  # FLAKY TESTED
    open(path, "w") do file
        for line in lines
            @assert !contains(line, '\n')
            println(file, line)
        end
        return nothing
    end
    report_modified!(path)
    return nothing
end

function Formats.format_add_axis!(
    files::FilesDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    flame_timed("FilesDaf.write_axis_vector") do
        return write_lines_file("$(files.path)/axes/$(axis).txt", entries)
    end

    for path in ("$(files.path)/vectors/$(axis)", "$(files.path)/matrices/$(axis)")
        mkdir(path)
        report_modified!(path)
    end

    axes_set = Formats.get_axes_set_through_cache(files)
    for other_axis in axes_set
        path = "$(files.path)/matrices/$(other_axis)/$(axis)"
        mkdir(path)
        report_modified!(path)
        if other_axis != axis
            path = "$(files.path)/matrices/$(axis)/$(other_axis)"
            mkdir(path)
            report_modified!(path)
        end
    end

    descriptor = Vector{UInt8}(JSON.json(Dict("format" => "axis", "n_entries" => length(entries))))
    metadata_json_append!(files, "axes/$(axis)", descriptor)
    return nothing
end

function Formats.format_delete_axis!(files::FilesDaf, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(files)
    for path in ("$(files.path)/axes/$(axis).txt", "$(files.path)/vectors/$(axis)", "$(files.path)/matrices/$(axis)")
        rm(path; force = true, recursive = true)
        report_modified!(path)
    end

    axes_set = Formats.get_axes_set_through_cache(files)
    for other_axis in axes_set
        path = "$(files.path)/matrices/$(other_axis)/$(axis)"
        rm(path; force = true, recursive = true)
        report_modified!(path)
    end

    metadata_json_rebuild!(files)
    return nothing
end

function Formats.format_axes_set(files::FilesDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return get_names_set("$(files.path)/axes", ".txt")
end

function Formats.format_axis_vector(
    files::FilesDaf,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(files)
    return (mmap_file_lines("$(files.path)/axes/$(axis).txt"), Formats.MappedData)
end

function Formats.format_axis_length(files::FilesDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(files)
    entries = Formats.get_axis_vector_through_cache(files, axis)
    return length(entries)
end

function Formats.format_has_vector(files::FilesDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(files)
    return cached_ispath("$(files.path)/vectors/$(axis)") && cached_ispath("$(files.path)/vectors/$(axis)/$(name).json")
end

function Formats.format_set_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
    packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(files)
    if vector == 0
        vector = spzeros(typeof(vector), Formats.format_axis_length(files, axis))
    end

    # Contract: callers must have invoked `update_before_set_vector` (which delegates to `format_delete_vector!`)
    # before reaching here, so the per-property files have already been removed from disk. The public `set_vector!`
    # path in `writers.jl` does this; any internal caller that reaches this function directly must also do so to
    # avoid stale `.json` / `.data` / `.txt` / `.shard` / `.nzind*` / `.nzval*` / `.nztxt` files.
    base_key = "vectors/$(axis)/$(name)"
    if vector isa AbstractString
        @assert !(contains(vector, '\n'))
        write_dense_array_json(files, base_key, String)
        fill_file("$(files.path)/$(base_key).txt", vector, Formats.format_axis_length(files, axis))

    elseif vector isa StorageScalar
        @assert vector isa StorageReal
        write_dense_array_json(files, base_key, typeof(vector))
        fill_file("$(files.path)/$(base_key).data", vector, Formats.format_axis_length(files, axis))  # NOJET

    elseif issparse(vector)
        flame_timed("FilesDaf.write_sparse_vector") do
            return packed_format_write_sparse_numeric_vector!(files, axis, name, vector, packed)
        end

    elseif eltype(vector) <: AbstractString
        write_string_vector(files, axis, name, vector, packed)

    else
        chunk_shape = chunks_for(packed, size(vector), eltype(vector))
        if chunk_shape !== nothing
            packed_format_write_dense_array!(files, base_key, vector, chunk_shape)
        else
            write_dense_array_json(files, base_key, eltype(vector))
            flame_timed("FilesDaf.write_dense_vector") do
                return write("$(files.path)/$(base_key).data", vector)
            end
        end
    end
    return nothing
end

function write_string_vector( # UNTESTED
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
    packed::Bool,
)::Nothing
    flame_timed("FilesDaf.write_string_vector") do
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

        logical_base = "vectors/$(axis)/$(name)"
        base = "$(files.path)/$(logical_base)"
        if sparse_size <= dense_size * 0.75
            nzind_chunk_shape = chunks_for(packed, (n_nonempty,), ind_type)
            nzval_chunk_shape = chunks_for(packed, (n_nonempty,), String)
            write_sparse_vector_json(
                files,
                logical_base,
                String,
                ind_type,
                n_nonempty;
                nzind_chunk_shape,
                nzval_chunk_shape,
            )

            nzind_vector = Vector{ind_type}(undef, n_nonempty)
            nzval_buffer = Vector{eltype(vector)}(undef, n_nonempty)
            position = 1
            for (index, value) in enumerate(vector)
                if length(value) > 0
                    @assert !(contains(value, '\n'))
                    nzind_vector[position] = index
                    nzval_buffer[position] = value
                    position += 1
                end
            end
            @assert position == n_nonempty + 1

            packed_format_write_sparse_component!(files, logical_base, "nzind", nzind_vector, nzind_chunk_shape)
            write_sparse_string_nzval(base, nzval_buffer, nzval_chunk_shape)

        else
            write_dense_string_array(files, logical_base, vector, chunks_for(packed, (length(vector),), String))
        end
    end

    return nothing
end

function Formats.format_get_empty_dense_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
)::Tuple{AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(files)
    return packed_format_get_empty_dense_vector!(files, axis, name, T, packed, Formats.format_axis_length(files, axis))
end

function Formats.format_filled_empty_dense_vector!(  # FLAKY TESTED
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    filled::AbstractVector{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    packed_format_filled_empty_dense_vector!(files, axis, name, filled)
    return nothing
end

function Formats.format_get_empty_sparse_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
    _packed::Bool,
)::Tuple{AbstractVector{I}, AbstractVector{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(files)
    return packed_format_get_empty_sparse_vector!(files, axis, name, T, nnz, I)
end

function Formats.format_filled_empty_sparse_vector!(  # FLAKY TESTED
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    ::SparseVector{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    packed_format_filled_empty_sparse_vector!(files, axis, name)
    return nothing
end

function Formats.format_delete_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(files)
    for suffix in (".json", ".txt", ".data", ".shard", ".nzind", ".nzind.shard", ".nzval", ".nzval.shard", ".nztxt")
        path = "$(files.path)/vectors/$(axis)/$(name)$(suffix)"
        rm(path; force = true)
        report_modified!(path)
    end
    metadata_json_rebuild!(files)
    return nothing
end

function Formats.format_vectors_set(files::FilesDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return get_names_set("$(files.path)/vectors/$(axis)", ".json")
end

function Formats.format_get_vector(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(files)

    base_key = "vectors/$(axis)/$(name)"
    json = packed_read_json(files, "$(base_key).json")
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    size = Formats.format_axis_length(files, axis)
    cache_group = Formats.MappedData
    if format == "dense"
        eltype_name = json["eltype"]
        if get(json, "packed", false) === true
            vector =
                packed_open_array(files, "$(base_key).shard", eltype_for_descriptor(eltype_name), json, (Int(size),))
            cache_group = Formats.MemoryData
        elseif eltype_name == "string" || eltype_name == "String"
            vector, cache_group = packed_read_lines(files, "$(base_key).txt")
            @assert length(vector) == size
        else
            eltype = eltype_for_descriptor(eltype_name)
            vector, _, cache_group = packed_read_typed_vector(files, "$(base_key).data", eltype, size)
        end
    else
        @assert format == "sparse"
        eltype_name, indtype_name = parse_sparse_descriptor(json, "nzind")
        ind_type = DTYPE_BY_NAME[indtype_name]
        @assert ind_type !== nothing

        if eltype_name == "string" || eltype_name == "String"
            nzind_vector, _, _, nzind_cache =
                packed_format_open_sparse_component_eager(files, base_key, "nzind", ind_type, json, nothing)
            cache_group = max(cache_group, nzind_cache)
            vector = Vector{AbstractString}(undef, size)
            fill!(vector, "")
            nzval_descriptor = haskey(json, "nzval") ? json["nzval"] : Dict("format" => "dense", "eltype" => "String")
            if get(nzval_descriptor, "packed", false) === true
                nzval_strings, _, _, _ = packed_format_open_sparse_component_eager(
                    files,
                    base_key,
                    "nzval",
                    String,
                    json,
                    length(nzind_vector),
                )
                vector[nzind_vector] .= nzval_strings  # NOJET
            else
                nztxt_lines, _ = packed_read_lines(files, "$(base_key).nztxt")
                vector[nzind_vector] .= nztxt_lines
            end
            cache_group = Formats.MemoryData

        else
            eltype = eltype_for_descriptor(eltype_name)
            nzind_descriptor = get(json, "nzind", nothing)
            nzind_packed = nzind_descriptor isa AbstractDict && get(nzind_descriptor, "packed", false) === true
            nzval_descriptor = get(json, "nzval", nothing)
            nzval_packed = nzval_descriptor isa AbstractDict && get(nzval_descriptor, "packed", false) === true
            nzval_present =
                packed_has_entry(files, "$(base_key).nzval") || packed_has_entry(files, "$(base_key).nzval.shard")

            if nzind_packed || nzval_packed
                nzind_source, _, nnz, _ =
                    packed_format_open_sparse_component_source(files, base_key, "nzind", ind_type, json, nothing)
                nzval_source = if nzval_present
                    nzval_vector, _, _, _ =
                        packed_format_open_sparse_component_source(files, base_key, "nzval", eltype, json, nnz)
                    nzval_vector
                else
                    fill(true, nnz)
                end
                vector = LazySparseVector(size, nzind_source, nzval_source)
                cache_group = Formats.MemoryData
            else
                nzind_vector, _, nnz, nzind_cache =
                    packed_format_open_sparse_component_eager(files, base_key, "nzind", ind_type, json, nothing)
                cache_group = max(cache_group, nzind_cache)
                if nzval_present
                    nzval_vector, _, _, nzval_cache =
                        packed_format_open_sparse_component_eager(files, base_key, "nzval", eltype, json, nnz)
                    cache_group = max(cache_group, nzval_cache)
                else
                    nzval_vector = fill(true, nnz)
                    cache_group = Formats.MemoryData
                end
                vector = SparseVector(size, nzind_vector, nzval_vector)
            end
        end
    end

    return (vector, nothing, cache_group)
end

function Formats.format_has_matrix(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(files)
    return cached_ispath("$(files.path)/matrices/$(rows_axis)") &&
           cached_ispath("$(files.path)/matrices/$(rows_axis)/$(columns_axis)") &&
           cached_ispath("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json")
end

function Formats.format_set_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageScalarBase, StorageMatrix},
    packed::Bool,
)::Nothing
    @assert Formats.has_data_write_lock(files)
    nrows = Formats.format_axis_length(files, rows_axis)
    ncols = Formats.format_axis_length(files, columns_axis)
    if matrix == 0
        matrix = spzeros(typeof(matrix), nrows, ncols)
    end

    # Contract: callers must have invoked `update_before_set_matrix` (which delegates to `format_delete_matrix!`)
    # before reaching here, so the per-property files have already been removed from disk. The public `set_matrix!`
    # path in `writers.jl` does this. There is one internal caller — `MemoryDaf.format_relayout_matrix!` — that
    # relies on the surrounding `relayout!` orchestration in `writers.jl` having already cleaned the destination
    # `(columns_axis, rows_axis, name)` slot before invoking the relayout dispatch. Any new internal call site must
    # also satisfy this contract to avoid stale `.json` / `.data` / `.txt` / `.colptr` / `.rowval` / `.nzval*` /
    # `.nztxt` files.
    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    if matrix isa StorageReal
        write_dense_array_json(files, base_key, typeof(matrix))
        fill_file("$(files.path)/$(base_key).data", matrix, nrows * ncols)  # NOJET

    elseif matrix isa AbstractString
        write_dense_array_json(files, base_key, String)
        fill_file("$(files.path)/$(base_key).txt", matrix, nrows * ncols)  # NOJET

    elseif issparse(matrix)
        @assert matrix isa AbstractMatrix
        flame_timed("FilesDaf.write_sparse_matrix") do
            return packed_format_write_sparse_numeric_matrix!(files, rows_axis, columns_axis, name, matrix, packed)
        end

    elseif eltype(matrix) <: AbstractString
        write_string_matrix(files, rows_axis, columns_axis, name, matrix, packed)  # NOJET

    else
        @assert eltype(matrix) <: Real
        chunk_shape = chunks_for(packed, (nrows, ncols), eltype(matrix))
        if chunk_shape !== nothing
            packed_format_write_dense_array!(files, base_key, matrix, chunk_shape)
        else
            write_dense_array_json(files, base_key, eltype(matrix))
            flame_timed("FilesDaf.write_dense_matrix") do
                return write("$(files.path)/$(base_key).data", matrix)
            end
        end
    end
    return nothing
end

function write_string_matrix(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::AbstractMatrix{<:AbstractString},
    packed::Bool,
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

    logical_base = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    base = "$(files.path)/$(logical_base)"
    if sparse_size <= dense_size * 0.75
        colptr_chunk_shape = chunks_for(packed, (ncols + 1,), ind_type)
        rowval_chunk_shape = chunks_for(packed, (n_nonempty,), ind_type)
        nzval_chunk_shape = chunks_for(packed, (n_nonempty,), String)
        write_sparse_matrix_json(
            files,
            logical_base,
            String,
            ind_type,
            n_nonempty,
            ncols;
            colptr_chunk_shape,
            rowval_chunk_shape,
            nzval_chunk_shape,
        )

        colptr_vector = Vector{ind_type}(undef, ncols + 1)
        rowval_vector = Vector{ind_type}(undef, n_nonempty)
        nzval_buffer = Vector{eltype(matrix)}(undef, n_nonempty)

        flame_timed("FilesDaf.write_sparse_string_matrix") do
            position = 1
            for column_index in 1:ncols
                colptr_vector[column_index] = position
                for row_index in 1:nrows
                    value = matrix[row_index, column_index]
                    if length(value) > 0
                        @assert !contains(value, '\n')
                        rowval_vector[position] = row_index
                        nzval_buffer[position] = value
                        position += 1
                    end
                end
            end
            @assert position == n_nonempty + 1
            colptr_vector[ncols + 1] = n_nonempty + 1

            packed_format_write_sparse_component!(files, logical_base, "colptr", colptr_vector, colptr_chunk_shape)
            packed_format_write_sparse_component!(files, logical_base, "rowval", rowval_vector, rowval_chunk_shape)
            write_sparse_string_nzval(base, nzval_buffer, nzval_chunk_shape)
            return nothing
        end

    else
        write_dense_string_array(files, logical_base, matrix, chunks_for(packed, (nrows, ncols), String))
    end

    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    packed::Bool,
)::Tuple{AbstractMatrix{T}, Maybe{Formats.CacheGroup}} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(files)
    nrows = Formats.format_axis_length(files, rows_axis)
    ncols = Formats.format_axis_length(files, columns_axis)
    return packed_format_get_empty_dense_matrix!(files, rows_axis, columns_axis, name, T, packed, nrows, ncols)
end

function Formats.format_filled_empty_dense_matrix!(  # FLAKY TESTED
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled::AbstractMatrix{<:StorageReal},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    packed_format_filled_empty_dense_matrix!(files, rows_axis, columns_axis, name, filled)
    return nothing
end

function Formats.format_get_empty_sparse_matrix!(
    files::FilesDaf,
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
    @assert Formats.has_data_write_lock(files)
    ncols = Formats.format_axis_length(files, columns_axis)
    return packed_format_get_empty_sparse_matrix!(files, rows_axis, columns_axis, name, T, nnz, I, ncols)
end

function Formats.format_filled_empty_sparse_matrix!(  # FLAKY TESTED
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::SparseMatrixCSC{<:StorageReal, <:StorageInteger},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    packed_format_filled_empty_sparse_matrix!(files, rows_axis, columns_axis, name)
    return nothing
end

function Formats.format_relayout_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
    packed::Bool,
)::StorageMatrix
    @assert Formats.has_data_write_lock(files)

    if issparse(matrix)
        colptr, rowval, nzval, _ = Formats.format_get_empty_sparse_matrix!(
            files,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(matrix.colptr),
            packed,
        )
        flame_timed("FilesDaf.init_empty_sparse_matrix") do
            colptr[1] = 1
            return colptr[2:end] .= length(nzval) + 1
        end
        relayout_matrix =
            SparseMatrixCSC(axis_length(files, columns_axis), axis_length(files, rows_axis), colptr, rowval, nzval)
        relayout!(flip(relayout_matrix), matrix)

    elseif eltype(matrix) <: AbstractString
        relayout_matrix = flipped(matrix)
        write_string_matrix(files, columns_axis, rows_axis, name, relayout_matrix, packed)

    else
        @assert eltype(matrix) <: Real
        relayout_matrix, _ =
            Formats.format_get_empty_dense_matrix!(files, columns_axis, rows_axis, name, eltype(matrix), packed)
        relayout!(flip(relayout_matrix), matrix)
    end
    return relayout_matrix
end

function Formats.format_delete_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(files)
    for suffix in (
        ".json",
        ".data",
        ".txt",
        ".shard",
        ".colptr",
        ".colptr.shard",
        ".rowval",
        ".rowval.shard",
        ".nzval",
        ".nzval.shard",
        ".nztxt",
    )
        path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name)$(suffix)"
        rm(path; force = true)
        report_modified!(path)
    end
    metadata_json_rebuild!(files)
    return nothing
end

function Formats.format_matrices_set(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return get_names_set("$(files.path)/matrices/$(rows_axis)/$(columns_axis)", ".json")
end

function Formats.format_get_matrix(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(files)

    nrows = Formats.format_axis_length(files, rows_axis)
    ncols = Formats.format_axis_length(files, columns_axis)

    base_key = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    json = packed_read_json(files, "$(base_key).json")
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    cache_group = Formats.MappedData
    if format == "dense"
        eltype_name = json["eltype"]
        if get(json, "packed", false) === true
            matrix = packed_open_array(
                files,
                "$(base_key).shard",
                eltype_for_descriptor(eltype_name),
                json,
                (Int(nrows), Int(ncols)),
            )
            cache_group = Formats.MemoryData
        elseif eltype_name == "String" || eltype_name == "string"
            vector, cache_group = packed_read_lines(files, "$(base_key).txt")
            @assert length(vector) == nrows * ncols
            matrix = reshape(vector, (nrows, ncols))

        else
            eltype = eltype_for_descriptor(eltype_name)
            matrix, _, cache_group = packed_read_typed_matrix(files, "$(base_key).data", eltype, nrows, ncols)
        end

    else
        @assert format == "sparse"
        eltype_name, indtype_name = parse_sparse_descriptor(json, "colptr")
        ind_type = DTYPE_BY_NAME[indtype_name]
        @assert ind_type !== nothing

        # `colptr` is always materialised at read time even if it lives on disk in packed form: it is small
        # (`sizeof(ind_type) × (n_columns + 1)` bytes) and slicing needs random access to it.
        colptr_vector, _, _, colptr_cache =
            packed_format_open_sparse_component_eager(files, base_key, "colptr", ind_type, json, ncols + 1)
        cache_group = max(cache_group, colptr_cache)

        if eltype_name == "string" || eltype_name == "String"
            rowval_vector, _, nnz, rowval_cache =
                packed_format_open_sparse_component_eager(files, base_key, "rowval", ind_type, json, nothing)
            cache_group = max(cache_group, rowval_cache)
            matrix = Matrix{AbstractString}(undef, nrows, ncols)
            fill!(matrix, "")
            # Defensive packed-`nzval` arm: current writers always emit `.nztxt` for string-sparse matrices, but the
            # reader supports a hand-authored or future layout that packs the per-row strings into a v3 shard.
            nzval_descriptor = haskey(json, "nzval") ? json["nzval"] : Dict("format" => "dense", "eltype" => "String")
            nzval_strings = if get(nzval_descriptor, "packed", false) === true
                buffer, _, _, _ = packed_format_open_sparse_component_eager(files, base_key, "nzval", String, json, nnz)  # UNTESTED
                buffer  # UNTESTED
            else
                lines, _ = packed_read_lines(files, "$(base_key).nztxt")
                lines
            end
            position = 1
            for column_index in 1:ncols
                first_row_position = colptr_vector[column_index]
                last_row_position = colptr_vector[column_index + 1] - 1
                for row_index in rowval_vector[first_row_position:last_row_position]
                    matrix[row_index, column_index] = nzval_strings[position]
                    position += 1
                end
            end
            cache_group = Formats.MemoryData

        else
            eltype = eltype_for_descriptor(eltype_name)
            rowval_descriptor = get(json, "rowval", nothing)
            rowval_packed = rowval_descriptor isa AbstractDict && get(rowval_descriptor, "packed", false) === true
            nzval_descriptor = get(json, "nzval", nothing)
            nzval_packed = nzval_descriptor isa AbstractDict && get(nzval_descriptor, "packed", false) === true
            nzval_present =
                packed_has_entry(files, "$(base_key).nzval") || packed_has_entry(files, "$(base_key).nzval.shard")

            if rowval_packed || nzval_packed
                rowval_source, _, nnz, _ =
                    packed_format_open_sparse_component_source(files, base_key, "rowval", ind_type, json, nothing)
                nzval_source = if nzval_present
                    nzval_vector, _, _, _ =
                        packed_format_open_sparse_component_source(files, base_key, "nzval", eltype, json, nnz)
                    nzval_vector
                else
                    fill(true, nnz)
                end
                matrix = LazySparseMatrix(nrows, colptr_vector, rowval_source, nzval_source)
                cache_group = Formats.MemoryData
            else
                rowval_vector, _, nnz, rowval_cache =
                    packed_format_open_sparse_component_eager(files, base_key, "rowval", ind_type, json, nothing)
                cache_group = max(cache_group, rowval_cache)
                if nzval_present
                    nzval_vector, _, _, nzval_cache =
                        packed_format_open_sparse_component_eager(files, base_key, "nzval", eltype, json, nnz)
                    cache_group = max(cache_group, nzval_cache)
                else
                    nzval_vector = fill(true, nnz)
                    cache_group = Formats.MemoryData
                end
                matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)  # NOJET
            end
        end
    end

    return (matrix, nothing, cache_group)
end

function get_names_set(path::AbstractString, suffix::AbstractString)::AbstractSet{<:AbstractString}
    return flame_timed("FilesDaf.get_names_set") do
        names_set = Set{AbstractString}()
        suffix_length = length(suffix)

        for file_name in readdir(path)
            if endswith(file_name, suffix)
                push!(names_set, chop(file_name; tail = suffix_length))
            end
        end

        return names_set
    end
end

function mmap_file_lines(path::AbstractString)::AbstractVector{<:AbstractString}
    return flame_timed("FilesDaf.mmap_file_lines") do
        key = (:daf, :mmap_lines, "r")
        return get_through_global_weak_cache(abspath(path), key) do _
            size = filesize(path)
            text = StringView(mmap_file_data(path, Vector{UInt8}, size, "r"))
            lines = split(text, "\n")
            last_line = pop!(lines)
            @assert last_line == ""
            return lines
        end
    end
end

function mmap_file_data(
    path::AbstractString,
    ::Type{T},
    size::Union{Integer, Tuple{<:Integer, <:Integer}},
    mode::AbstractString,
)::T where {T <: Union{Vector, Matrix}}
    return flame_timed("FilesDaf.mmap_file_data") do
        key = (:daf, :mmap_data, mode)
        return get_through_global_weak_cache(abspath(path), key) do _
            return mmap_populate_if_old_linux_ramdisk(path, T, size, mode)  # NOJET
        end
    end
end

function fill_file(path::AbstractString, value::StorageScalar, size::Integer)::Nothing
    flame_timed("FilesDaf.fill_file") do
        if value isa AbstractString
            @assert !contains(value, '\n')
            open(path, "w") do file
                for _ in 1:size
                    println(file, value)
                end
            end

        elseif value == 0
            write_zeros_file(path, size * sizeof(value))

        else
            buffer_size = min(div(8192, sizeof(value)), size)
            buffer = fill(value, buffer_size)
            written = 0
            open(path, "w") do file
                while written < size
                    write(file, buffer)
                    written += buffer_size
                end
            end
        end
    end
end

function write_zeros_file(path::AbstractString, size::Integer)::Nothing
    flame_timed("FilesDaf.write_zeros_file") do
        open(path, "w") do file
            if size > 0
                seek(file, size - 1)
                write(file, UInt8(0))
            end
        end
    end
    return nothing
end

function write_dense_array_json(  # UNTESTED
    files::FilesDaf,
    key::AbstractString,
    eltype::Type{<:StorageScalarBase},
)::Nothing
    flame_timed("FilesDaf.write_dense_array_json") do
        return write_property_json!(files, key, dense_array_json_bytes(eltype))
    end
    return nothing
end

function write_packed_array_json(  # UNTESTED
    files::FilesDaf,
    key::AbstractString,
    eltype::Type{<:StorageScalarBase},
    chunk_shape::NTuple{N, Int},
    codec::PackedCodec,
)::Nothing where {N}
    flame_timed("FilesDaf.write_packed_array_json") do
        return write_property_json!(files, key, packed_array_json_bytes(eltype, chunk_shape, codec))
    end
    return nothing
end

function write_sparse_vector_json(
    files::FilesDaf,
    key::AbstractString,
    eltype::Type{<:StorageScalarBase},
    indtype::Type{<:StorageInteger},
    nnz::Integer;
    nzind_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
)::Nothing
    flame_timed("FilesDaf.write_sparse_vector_json") do
        json_bytes = sparse_vector_json_bytes(eltype, indtype, nnz; nzind_chunk_shape, nzval_chunk_shape)
        return write_property_json!(files, key, json_bytes)
    end
    return nothing
end

function write_sparse_matrix_json(
    files::FilesDaf,
    key::AbstractString,
    eltype::Type{<:StorageScalarBase},
    indtype::Type{<:StorageInteger},
    nnz::Integer,
    n_columns::Integer;
    colptr_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    rowval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
    nzval_chunk_shape::Maybe{NTuple{1, Int}} = nothing,
)::Nothing
    flame_timed("FilesDaf.write_sparse_matrix_json") do
        json_bytes = sparse_matrix_json_bytes(
            eltype,
            indtype,
            nnz,
            n_columns;
            colptr_chunk_shape,
            rowval_chunk_shape,
            nzval_chunk_shape,
        )
        return write_property_json!(files, key, json_bytes)
    end
    return nothing
end

# Write the per-property JSON sidecar at `<files.path>/<key>.json` (with a trailing newline) and append the
# corresponding entry to `metadata.json`. `descriptor` is the JSON bytes returned by one of the
# `*_json_bytes` helpers, which include the trailing newline; `metadata_json_append!` strips it before insertion.
function write_property_json!(files::FilesDaf, key::AbstractString, descriptor::AbstractVector{UInt8})::Nothing
    json_path = "$(files.path)/$(key).json"
    write(json_path, descriptor)
    report_modified!(json_path)
    metadata_json_append!(files, key, descriptor)
    return nothing
end

# Write a dense string property as either the flat `<base>.txt` line-per-element file or the packed
# `<base>.shard` v3 sharded-array bytes, plus its JSON descriptor at `<base>.json`.
function write_dense_string_array(
    files::FilesDaf,
    key::AbstractString,
    data::AbstractArray{T, N},
    chunk_shape::Maybe{NTuple{N, Int}},
)::Nothing where {T <: AbstractString, N}
    base_path = "$(files.path)/$(key)"
    if chunk_shape === nothing
        write_dense_array_json(files, key, String)
        write_lines_file("$(base_path).txt", data)
    else
        codec = compressor_for()
        encoded = encode_packed_dense_array(data, chunk_shape, v3_bytes_codecs_for(codec, T), :end)
        shard_path = "$(base_path).shard"
        open(io -> write(io, encoded), shard_path, "w")
        report_modified!(shard_path)
        write_packed_array_json(files, key, T, chunk_shape, codec)
    end
    return nothing
end

# Write the `nzval` component of a sparse string property either flat at `<base>.nztxt` (line-per-nonzero) or
# packed at `<base>.nzval.shard` (v3 shard via `VLenUTF8V3Codec`).
function write_sparse_string_nzval(
    base_path::AbstractString,
    nzval_buffer::AbstractVector{T},
    chunk_shape::Maybe{NTuple{1, Int}},
)::Nothing where {T <: AbstractString}
    if chunk_shape === nothing
        nztxt_path = "$(base_path).nztxt"
        open(nztxt_path, "w") do file
            for value in nzval_buffer
                @assert !contains(value, '\n')
                println(file, value)
            end
            return nothing
        end
        report_modified!(nztxt_path)
    else
        shard_path = "$(base_path).nzval.shard"
        encoded = encode_packed_dense_array(nzval_buffer, chunk_shape, v3_bytes_codecs_for(compressor_for(), T), :end)
        open(io -> write(io, encoded), shard_path, "w")
        report_modified!(shard_path)
    end
    return nothing
end

const METADATA_JSON = "metadata.json"

function packed_write_bytes!(files::FilesDaf, key::AbstractString, bytes::AbstractVector{UInt8})::Nothing
    path = "$(files.path)/$(key)"
    write(path, bytes)
    report_modified!(path)
    return nothing
end

function packed_write_typed_array!(files::FilesDaf, key::AbstractString, vector::AbstractVector)::Nothing
    path = "$(files.path)/$(key)"
    write(path, vector)  # NOJET
    report_modified!(path)
    return nothing
end

function packed_delete_entry!(files::FilesDaf, key::AbstractString)::Nothing
    path = "$(files.path)/$(key)"
    rm(path; force = true)
    report_modified!(path)
    return nothing
end

function packed_register_metadata!(files::FilesDaf, key::AbstractString, descriptor::AbstractVector{UInt8})::Nothing
    metadata_json_append!(files, key, descriptor)
    return nothing
end

function packed_has_entry(files::FilesDaf, key::AbstractString)::Bool
    return cached_ispath("$(files.path)/$(key)")
end

function packed_entry_size(files::FilesDaf, key::AbstractString)::Int
    return Int(filesize("$(files.path)/$(key)"))
end

function packed_read_json(files::FilesDaf, key::AbstractString)::AbstractDict
    parsed = JSON.parsefile("$(files.path)/$(key)")
    @assert parsed isa AbstractDict
    return parsed
end

function packed_read_lines(
    files::FilesDaf,
    key::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    return (mmap_file_lines("$(files.path)/$(key)"), Formats.MappedData)
end

function packed_read_typed_vector(
    files::FilesDaf,
    key::AbstractString,
    ::Type{T},
    n_elements::Integer,
)::Tuple{Vector{T}, Any, Formats.CacheGroup} where {T}
    vector = mmap_file_data("$(files.path)/$(key)", Vector{T}, Int(n_elements), files.files_mode)
    return (vector, nothing, Formats.MappedData)
end

function packed_read_typed_matrix(
    files::FilesDaf,
    key::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
)::Tuple{Matrix{T}, Any, Formats.CacheGroup} where {T}
    matrix = mmap_file_data("$(files.path)/$(key)", Matrix{T}, (Int(nrows), Int(ncols)), files.files_mode)
    return (matrix, nothing, Formats.MappedData)
end

function packed_open_array(
    files::FilesDaf,
    shard_key::AbstractString,
    ::Type{T},
    descriptor::AbstractDict,
    dims::NTuple{N, Int},
)::DiskArrays.CachedDiskArray where {T, N}
    return open_packed_dense_array("$(files.path)/$(shard_key)", T, descriptor, dims)
end

function packed_reserve_typed_vector!(
    files::FilesDaf,
    key::AbstractString,
    ::Type{T},
    n_elements::Integer,
)::Vector{T} where {T <: StorageReal}
    path = "$(files.path)/$(key)"
    fill_file(path, T(0), Int(n_elements))
    return mmap_file_data(path, Vector{T}, Int(n_elements), "r+")
end

function packed_reserve_typed_matrix!(
    files::FilesDaf,
    key::AbstractString,
    ::Type{T},
    nrows::Integer,
    ncols::Integer,
)::Matrix{T} where {T <: StorageReal}
    path = "$(files.path)/$(key)"
    fill_file(path, T(0), Int(nrows) * Int(ncols))
    return mmap_file_data(path, Matrix{T}, (Int(nrows), Int(ncols)), "r+")
end

# `FilesDaf` writes file mtime tracks freshness, so finalisation is a no-op — the file is already on disk.
function packed_finalize_entry!(::FilesDaf, ::AbstractString)::Nothing
    return nothing
end

function packed_make_streaming_shard_writer(
    files::FilesDaf,
    shard_key::AbstractString,
    ::Type{T},
    n_chunks::Integer,
    ::NTuple{2, Int},
    codec::PackedCodec,
)::IncrementalShardWriter where {T <: StorageReal}
    return open_streaming_shard_writer("$(files.path)/$(shard_key)", T, Int(n_chunks), v3_bytes_codecs_for(codec, T))
end

# Append `"key":<descriptor>` to `metadata.json` via byte-level surgery on the trailing `}`. A trailing newline in
# `descriptor` is stripped so the verbatim sidecar bytes can be passed through.
function metadata_json_append!(files::FilesDaf, key::AbstractString, descriptor::AbstractVector{UInt8})::Nothing
    if !isempty(descriptor) && descriptor[end] == UInt8('\n')
        descriptor = @view descriptor[1:(end - 1)]
    end
    metadata_path = "$(files.path)/$(METADATA_JSON)"
    encoded_key = Vector{UInt8}(JSON.json(String(key)))
    file_size = filesize(metadata_path)
    @assert file_size >= 2 "$(METADATA_JSON) missing or truncated; size=$(file_size)"
    open(metadata_path, "r+") do io
        truncate(io, file_size - 1)
        seekend(io)
        if file_size == 2  # was the empty object `{}`
            write(io, encoded_key, UInt8(':'), descriptor, UInt8('}'))
        else
            write(io, UInt8(','), encoded_key, UInt8(':'), descriptor, UInt8('}'))
        end
        return nothing
    end
    report_modified!(metadata_path)
    return nothing
end

# Rebuild `metadata.json` from scratch by walking the tree and emitting one entry per property (axes, scalars,
# vectors, matrices) in sorted-by-key order. Committed atomically via stage + rename so concurrent readers never
# observe a torn file. Called on every `delete!` and reorder, and at open time when the file is missing or
# unparseable.
function metadata_json_rebuild!(files::FilesDaf)::Nothing
    entries = Pair{String, Any}[]

    axes_directory = "$(files.path)/axes"
    if isdir(axes_directory)
        for name in sort!(readdir(axes_directory))
            if endswith(name, ".txt")
                axis = chop(name; tail = 4)
                n_entries = count_lines("$(axes_directory)/$(name)")
                push!(entries, "axes/$(axis)" => Dict("format" => "axis", "n_entries" => n_entries))
            end
        end
    end

    push_sidecars_in!(entries, files.path, "scalars")

    vectors_directory = "$(files.path)/vectors"
    if isdir(vectors_directory)
        for axis in sort!(readdir(vectors_directory))
            push_sidecars_in!(entries, files.path, "vectors/$(axis)")
        end
    end

    matrices_directory = "$(files.path)/matrices"
    if isdir(matrices_directory)
        for rows_axis in sort!(readdir(matrices_directory))
            rows_directory = "$(matrices_directory)/$(rows_axis)"
            if isdir(rows_directory)
                for columns_axis in sort!(readdir(rows_directory))
                    push_sidecars_in!(entries, files.path, "matrices/$(rows_axis)/$(columns_axis)")
                end
            end
        end
    end

    metadata_path = "$(files.path)/$(METADATA_JSON)"
    staging_path = metadata_path * ".new"
    open(staging_path, "w") do io
        write(io, UInt8('{'))
        for (index, (key, value)) in enumerate(entries)
            if index > 1
                write(io, UInt8(','))
            end
            JSON.print(io, key)
            write(io, UInt8(':'))
            JSON.print(io, value)
        end
        write(io, UInt8('}'))
        return nothing
    end
    Base.Filesystem.rename(staging_path, metadata_path)
    report_modified!(metadata_path)
    return nothing
end

# Append every `*.json` sidecar in `<base_directory>/<relative_directory>` (in sorted name order) to `entries`,
# keyed by `<relative_directory>/<sidecar-without-.json>`. Used by [`metadata_json_rebuild!`](@ref) to walk the
# `scalars/`, `vectors/<axis>/`, and `matrices/<rows>/<cols>/` directories.
function push_sidecars_in!(
    entries::Vector{Pair{String, Any}},
    base_directory::AbstractString,
    relative_directory::AbstractString,
)::Nothing
    full_directory = "$(base_directory)/$(relative_directory)"
    if !isdir(full_directory)
        return nothing
    end
    for name in sort!(readdir(full_directory))
        if endswith(name, ".json")
            key = "$(relative_directory)/$(chop(name; tail = 5))"
            content = JSON.parsefile("$(full_directory)/$(name)")
            push!(entries, key => content)
        end
    end
    return nothing
end

# Count newline-terminated lines in `path`. Used by [`metadata_json_rebuild!`](@ref) to populate the `n_entries`
# field of each axis descriptor without round-tripping through `mmap_file_lines`.
function count_lines(path::AbstractString)::Int
    n = 0
    open(path, "r") do io
        for _ in eachline(io)
            n += 1
        end
        return nothing
    end
    return n
end

# Ensure `metadata.json` exists and parses; rebuild if missing or torn. Any error in read-only mode is silently
# swallowed so a FilesDaf on a read-only filesystem still opens cleanly — HTTP serving from such a directory then
# requires one open on a writable filesystem to seed the sidecar.
function ensure_metadata_json!(files::FilesDaf)::Nothing
    metadata_path = "$(files.path)/$(METADATA_JSON)"
    if isfile(metadata_path)
        is_parseable = false
        try
            JSON.parsefile(metadata_path)
            is_parseable = true
        catch  # FLAKY TESTED
            # Torn write or corruption — fall through to rebuild.
        end
        if is_parseable
            return nothing
        end
    end
    try
        metadata_json_rebuild!(files)
    catch  # FLAKY TESTED
        if files.mode == "r"  # UNTESTED
            return nothing  # UNTESTED
        end
        rethrow()  # UNTESTED
    end
    return nothing
end

const REORDER_BACKUP_DIR = ".reorder.backup"

const REORDER_VECTOR_SUFFIXES = (".json", ".data", ".txt", ".nzind", ".nzval", ".nztxt")

const REORDER_MATRIX_SUFFIXES = (
    ".json",
    ".data",
    ".txt",
    ".shard",
    ".colptr",
    ".colptr.shard",
    ".rowval",
    ".rowval.shard",
    ".nzval",
    ".nzval.shard",
    ".nztxt",
)

function Reorder.format_lock_reorder!(files::FilesDaf, ::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(files)
    backup_root = "$(files.path)/$(REORDER_BACKUP_DIR)"
    @assert !isdir(backup_root)
    mkdir(backup_root)
    report_modified!(backup_root)
    return nothing
end

function Reorder.format_backup_reorder!(files::FilesDaf, plan::Reorder.FormatReorderPlan)::Nothing
    @assert Formats.has_data_write_lock(files)
    backup_root = "$(files.path)/$(REORDER_BACKUP_DIR)"
    mkpath("$(backup_root)/axes")

    for (axis, _) in plan.planned_axes
        src = "$(files.path)/axes/$(axis).txt"
        if cached_ispath(src)  # NOJET
            hardlink(src, "$(backup_root)/axes/$(axis).txt")
        end
    end
    for planned in plan.planned_vectors
        backup_vector_dir = "$(backup_root)/vectors/$(planned.axis)"
        mkpath(backup_vector_dir)
        for suffix in REORDER_VECTOR_SUFFIXES
            src = "$(files.path)/vectors/$(planned.axis)/$(planned.name)$(suffix)"
            if cached_ispath(src)
                hardlink(src, "$(backup_vector_dir)/$(planned.name)$(suffix)")
            end
        end
    end
    for planned in plan.planned_matrices
        backup_matrix_dir = "$(backup_root)/matrices/$(planned.rows_axis)/$(planned.columns_axis)"
        mkpath(backup_matrix_dir)
        for suffix in REORDER_MATRIX_SUFFIXES
            src = "$(files.path)/matrices/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)$(suffix)"
            if cached_ispath(src)
                hardlink(src, "$(backup_matrix_dir)/$(planned.name)$(suffix)")
            end
        end
    end
    return nothing
end

function Reorder.format_replace_reorder!(
    files::FilesDaf,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
    crash_counter::Maybe{Ref{Int}},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    @assert isdir("$(files.path)/$(REORDER_BACKUP_DIR)")

    for (axis, planned_axis) in plan.planned_axes
        axis_path = "$(files.path)/axes/$(axis).txt"
        if cached_ispath(axis_path)  # NOJET
            rm(axis_path)
            write_lines_file(axis_path, planned_axis.new_entries)
            report_modified!(axis_path)
        end
    end

    for planned in plan.planned_vectors
        replace_reorder_vector(files, planned, plan, replacement_progress)  # NOJET
        Reorder.tick_crash_counter!(crash_counter)
    end

    for planned in plan.planned_matrices
        replace_reorder_matrix(files, planned, plan, replacement_progress)  # NOJET
        Reorder.tick_crash_counter!(crash_counter)
    end

    metadata_json_rebuild!(files)
    return nothing
end

function replace_reorder_vector(
    files::FilesDaf,
    planned::Reorder.PlannedVector,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_vector, _, _ = Formats.format_get_vector(files, planned.axis, planned.name)
    planned_axis = plan.planned_axes[planned.axis]

    for suffix in REORDER_VECTOR_SUFFIXES
        path = "$(files.path)/vectors/$(planned.axis)/$(planned.name)$(suffix)"
        if cached_ispath(path)
            rm(path)
            report_modified!(path)
        end
    end

    if eltype(source_vector) <: AbstractString
        permuted = Vector{eltype(source_vector)}(undef, length(source_vector))
        permute_vector!(;
            destination = permuted,
            source = source_vector,
            permutation = planned_axis.permutation,
            progress = replacement_progress,
        )
        write_dense_array_json(files, "vectors/$(planned.axis)/$(planned.name)", String)
        write_lines_file("$(files.path)/vectors/$(planned.axis)/$(planned.name).txt", permuted)
    elseif source_vector isa SparseVector
        T = eltype(source_vector)
        I = eltype(SparseArrays.nonzeroinds(source_vector))
        source_nnz = nnz(source_vector)
        destination_nzind, destination_nzval, _ =
            packed_format_get_empty_sparse_vector!(files, planned.axis, planned.name, T, source_nnz, I)
        permute_sparse_vector_buffers!(;
            destination_nzind,
            destination_nzval,
            source_length = length(source_vector),
            source_nzind = SparseArrays.nonzeroinds(source_vector),
            source_nzval = nonzeros(source_vector),
            inverse_permutation = planned_axis.inverse_permutation,
            progress = replacement_progress,
        )
    else
        T = eltype(source_vector)
        destination, _ =
            packed_format_get_empty_dense_vector!(files, planned.axis, planned.name, T, false, length(source_vector))
        permute_vector!(;
            destination,
            source = source_vector,
            permutation = planned_axis.permutation,
            progress = replacement_progress,
        )
    end
    return nothing
end

function replace_reorder_matrix(
    files::FilesDaf,
    planned::Reorder.PlannedMatrix,
    plan::Reorder.FormatReorderPlan,
    replacement_progress::Maybe{Progress},
)::Nothing
    source_matrix, _, _ = Formats.format_get_matrix(files, planned.rows_axis, planned.columns_axis, planned.name)
    planned_rows = get(plan.planned_axes, planned.rows_axis, nothing)
    planned_columns = get(plan.planned_axes, planned.columns_axis, nothing)
    @assert planned_rows !== nothing || planned_columns !== nothing

    for suffix in REORDER_MATRIX_SUFFIXES
        path = "$(files.path)/matrices/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)$(suffix)"
        if cached_ispath(path)
            rm(path)
            report_modified!(path)
        end
    end

    nrows, ncols = size(source_matrix)
    if eltype(source_matrix) <: AbstractString
        permuted = Matrix{eltype(source_matrix)}(undef, nrows, ncols)
        if planned_rows !== nothing && planned_columns !== nothing
            permute_dense_matrix_both!(;
                destination = permuted,
                source = source_matrix,
                rows_permutation = planned_rows.permutation,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        elseif planned_rows !== nothing
            permute_dense_matrix_rows!(;
                destination = permuted,
                source = source_matrix,
                rows_permutation = planned_rows.permutation,
                progress = replacement_progress,
            )
        else
            permute_dense_matrix_columns!(;
                destination = permuted,
                source = source_matrix,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        end
        write_dense_array_json(files, "matrices/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name)", String)
        write_lines_file(
            "$(files.path)/matrices/$(planned.rows_axis)/$(planned.columns_axis)/$(planned.name).txt",
            permuted,
        )
    elseif source_matrix isa SparseMatrixCSC
        T = eltype(source_matrix)
        I = eltype(source_matrix.colptr)
        source_nnz = nnz(source_matrix)
        destination_colptr, destination_rowval, destination_nzval, _ = packed_format_get_empty_sparse_matrix!(
            files,
            planned.rows_axis,
            planned.columns_axis,
            planned.name,
            T,
            source_nnz,
            I,
            ncols,
        )
        if planned_rows !== nothing && planned_columns !== nothing
            permute_sparse_matrix_both_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = source_matrix.colptr,
                source_rowval = source_matrix.rowval,
                source_nzval = source_matrix.nzval,
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
                source_colptr = source_matrix.colptr,
                source_rowval = source_matrix.rowval,
                source_nzval = source_matrix.nzval,
                inverse_rows_permutation = planned_rows.inverse_permutation,
                progress = replacement_progress,
            )
        else
            permute_sparse_matrix_columns_buffers!(;
                destination_colptr,
                destination_rowval,
                destination_nzval,
                source_n_rows = nrows,
                source_colptr = source_matrix.colptr,
                source_rowval = source_matrix.rowval,
                source_nzval = source_matrix.nzval,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        end
    else
        T = eltype(source_matrix)
        destination, _ = packed_format_get_empty_dense_matrix!(
            files,
            planned.rows_axis,
            planned.columns_axis,
            planned.name,
            T,
            false,
            nrows,
            ncols,
        )
        if planned_rows !== nothing && planned_columns !== nothing
            permute_dense_matrix_both!(;
                destination,
                source = source_matrix,
                rows_permutation = planned_rows.permutation,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        elseif planned_rows !== nothing
            permute_dense_matrix_rows!(;
                destination,
                source = source_matrix,
                rows_permutation = planned_rows.permutation,
                progress = replacement_progress,
            )
        else
            permute_dense_matrix_columns!(;
                destination,
                source = source_matrix,
                columns_permutation = planned_columns.permutation,
                progress = replacement_progress,
            )
        end
    end
    return nothing
end

function Reorder.format_cleanup_reorder!(files::FilesDaf)::Nothing
    @assert Formats.has_data_write_lock(files)
    backup_root = "$(files.path)/$(REORDER_BACKUP_DIR)"
    @assert isdir(backup_root)
    rm(backup_root; force = true, recursive = true)
    report_modified!(backup_root)
    return nothing
end

function Reorder.format_has_reorder_lock(files::FilesDaf)::Bool
    @assert Formats.has_data_write_lock(files)
    return isdir("$(files.path)/$(REORDER_BACKUP_DIR)")
end

function Reorder.format_reset_reorder!(files::FilesDaf)::Bool
    @assert Formats.has_data_write_lock(files)
    backup_root = "$(files.path)/$(REORDER_BACKUP_DIR)"
    if !isdir(backup_root)
        return false
    end
    for (root, _, filenames) in walkdir(backup_root)
        rel = relpath(root, backup_root)
        live_dir = rel == "." ? files.path : "$(files.path)/$(rel)"
        for filename in filenames
            live_path = "$(live_dir)/$(filename)"
            rm(live_path; force = true)
            hardlink("$(root)/$(filename)", live_path)
            report_modified!(live_path)
        end
    end
    rm(backup_root; force = true, recursive = true)
    report_modified!(backup_root)
    metadata_json_rebuild!(files)
    return true
end

function Formats.format_description_header(
    files::FilesDaf,
    indent::AbstractString,
    lines::Vector{String},
    ::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(files)
    push!(lines, "$(indent)type: FilesDaf")
    push!(lines, "$(indent)path: $(files.path)")
    push!(lines, "$(indent)mode: $(files.mode)")
    return nothing
end

function Readers.complete_path(files::FilesDaf)::Maybe{AbstractString}
    return files.path
end

end  # module
