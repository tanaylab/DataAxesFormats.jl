"""
Hard-link conversion between [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) and [`ZarrDaf`](@ref
DataAxesFormats.ZarrFormat.ZarrDaf) directories.

The two on-disk formats differ in their per-property metadata encoding (`FilesDaf` uses one JSON sidecar per array,
`ZarrDaf` uses one `.zarray` per array plus a consolidated `.zmetadata`), but both store every numeric blob — dense
array chunks and the `colptr`/`rowval`/`nzind`/`nzval` components of sparse arrays — as the same raw little-endian bytes
without headers. The functions in this module exploit that equivalence to convert one tree into the other by
hard-linking every numeric blob and re-serializing only the metadata and the string-valued properties, so the on-disk
cost of a conversion is close to zero.

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
    source = ZarrDaf(zarr_path, "r")
    destination = FilesDaf(files_path, "w+")::FilesDaf

    for name in scalars_set(source)
        set_scalar!(destination, name, get_scalar(source, name))
    end

    for axis in axes_set(source)
        add_axis!(destination, axis, axis_vector(source, axis))
    end

    for axis in axes_set(source)
        for name in vectors_set(source, axis)
            zarr_vector_to_files(source, destination, zarr_path, files_path, axis, name)
        end
    end

    for rows_axis in axes_set(source)
        for columns_axis in axes_set(source)
            for name in matrices_set(source, rows_axis, columns_axis; relayout = false)
                zarr_matrix_to_files(source, destination, zarr_path, files_path, rows_axis, columns_axis, name)
            end
        end
    end

    FilesFormat.metadata_zip_rebuild!(destination)
    return nothing
end

function zarr_vector_to_files(
    source::DafReader,
    destination::FilesDaf,
    zarr_path::AbstractString,
    files_path::AbstractString,
    axis::AbstractString,
    name::AbstractString,
)::Nothing
    source_dir = "$(zarr_path)/vectors/$(axis)/$(name)"
    destination_base = "$(files_path)/vectors/$(axis)/$(name)"
    zarr_json = read_json_dict("$(source_dir)/zarr.json")

    if zarr_json["node_type"] == "array"
        if is_string_v3_dtype(zarr_json)
            set_vector!(destination, axis, name, get_vector(source, axis, name))
        else
            element_type = julia_type_from_v3_dtype(zarr_json["data_type"])
            FilesFormat.write_array_json("$(destination_base).json", "dense", element_type)
            hardlink("$(source_dir)/c/0", "$(destination_base).data")
        end
    else
        @assert zarr_json["node_type"] == "group" "unexpected node_type: $(zarr_json["node_type"])"
        zarr_sparse_vector_to_files(source_dir, destination_base)
    end
    return nothing
end

function zarr_sparse_vector_to_files(source_dir::AbstractString, destination_base::AbstractString)::Nothing
    nzind_json = read_json_dict("$(source_dir)/nzind/zarr.json")
    ind_type = julia_type_from_v3_dtype(nzind_json["data_type"])

    nzval_json_path = "$(source_dir)/nzval/zarr.json"
    if isfile(nzval_json_path)
        element_type = julia_type_from_v3_dtype(read_json_dict(nzval_json_path)["data_type"])
    else
        element_type = Bool
    end

    FilesFormat.write_array_json("$(destination_base).json", "sparse", element_type, ind_type)
    hardlink("$(source_dir)/nzind/c/0", "$(destination_base).nzind")
    if isfile("$(source_dir)/nzval/c/0")
        hardlink("$(source_dir)/nzval/c/0", "$(destination_base).nzval")
    end
    return nothing
end

function zarr_matrix_to_files(
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
    zarr_json = read_json_dict("$(source_dir)/zarr.json")

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
            element_type = julia_type_from_v3_dtype(zarr_json["data_type"])
            FilesFormat.write_array_json("$(destination_base).json", "dense", element_type)
            hardlink("$(source_dir)/c/0/0", "$(destination_base).data")
        end
    else
        @assert zarr_json["node_type"] == "group" "unexpected node_type: $(zarr_json["node_type"])"
        zarr_sparse_matrix_to_files(source_dir, destination_base)
    end
    return nothing
end

function zarr_sparse_matrix_to_files(source_dir::AbstractString, destination_base::AbstractString)::Nothing
    colptr_json = read_json_dict("$(source_dir)/colptr/zarr.json")
    ind_type = julia_type_from_v3_dtype(colptr_json["data_type"])

    nzval_json_path = "$(source_dir)/nzval/zarr.json"
    if isfile(nzval_json_path)
        element_type = julia_type_from_v3_dtype(read_json_dict(nzval_json_path)["data_type"])
    else
        element_type = Bool
    end

    FilesFormat.write_array_json("$(destination_base).json", "sparse", element_type, ind_type)
    hardlink("$(source_dir)/colptr/c/0", "$(destination_base).colptr")
    hardlink("$(source_dir)/rowval/c/0", "$(destination_base).rowval")
    if isfile("$(source_dir)/nzval/c/0")
        hardlink("$(source_dir)/nzval/c/0", "$(destination_base).nzval")
    end
    return nothing
end

function files_to_zarr_populate(files_path::AbstractString, zarr_path::AbstractString)::Nothing
    source = FilesDaf(files_path, "r")
    destination = ZarrDaf(zarr_path, "w+")::ZarrDaf  # NOJET

    for name in scalars_set(source)
        set_scalar!(destination, name, get_scalar(source, name))
    end

    for axis in axes_set(source)
        add_axis!(destination, axis, axis_vector(source, axis))
    end

    for axis in axes_set(source)
        for name in vectors_set(source, axis)
            files_vector_to_zarr(source, destination, files_path, zarr_path, axis, name)
        end
    end

    for rows_axis in axes_set(source)
        for columns_axis in axes_set(source)
            for name in matrices_set(source, rows_axis, columns_axis; relayout = false)
                files_matrix_to_zarr(source, destination, files_path, zarr_path, rows_axis, columns_axis, name)
            end
        end
    end

    ZarrFormat.refresh_consolidated_metadata!(destination)
    return nothing
end

function files_vector_to_zarr(
    source::DafReader,
    destination::ZarrDaf,
    files_path::AbstractString,
    zarr_path::AbstractString,
    axis::AbstractString,
    name::AbstractString,
)::Nothing
    json = read_json_dict("$(files_path)/vectors/$(axis)/$(name).json")
    format = json["format"]
    eltype_name = String(json["eltype"])

    if is_string_eltype(eltype_name)
        set_vector!(destination, axis, name, get_vector(source, axis, name))
        return nothing
    end

    parent_group = ZarrFormat.vectors_group(destination).groups[axis]
    element_type = julia_type_from_files_eltype(eltype_name)
    source_base = "$(files_path)/vectors/$(axis)/$(name)"
    destination_dir = "$(zarr_path)/vectors/$(axis)/$(name)"

    if format == "dense"
        n_elements = axis_length(source, axis)
        ZarrFormat.dense_zcreate(element_type, parent_group, name, false, (n_elements,))
        hardlink_v3_chunk("$(source_base).data", "$(destination_dir)/c/0")
    else
        @assert format == "sparse"
        ind_type = julia_type_from_files_eltype(String(json["indtype"]))
        nzind_path = "$(source_base).nzind"
        nnz_int = Int(div(filesize(nzind_path), sizeof(ind_type)))
        vector_group = Zarr.zgroup(parent_group, name)
        ZarrFormat.dense_zcreate(ind_type, vector_group, "nzind", false, (nnz_int,))
        hardlink_v3_chunk(nzind_path, "$(destination_dir)/nzind/c/0")
        nzval_path = "$(source_base).nzval"
        if isfile(nzval_path)
            ZarrFormat.dense_zcreate(element_type, vector_group, "nzval", false, (nnz_int,))
            hardlink_v3_chunk(nzval_path, "$(destination_dir)/nzval/c/0")
        end
    end
    return nothing
end

function files_matrix_to_zarr(
    source::DafReader,
    destination::ZarrDaf,
    files_path::AbstractString,
    zarr_path::AbstractString,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    json = read_json_dict("$(files_path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json")
    format = json["format"]
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

    parent_group = ZarrFormat.columns_axis_group(destination, rows_axis, columns_axis)
    element_type = julia_type_from_files_eltype(eltype_name)
    source_base = "$(files_path)/matrices/$(rows_axis)/$(columns_axis)/$(name)"
    destination_dir = "$(zarr_path)/matrices/$(rows_axis)/$(columns_axis)/$(name)"
    n_rows = axis_length(source, rows_axis)
    n_columns = axis_length(source, columns_axis)

    if format == "dense"
        ZarrFormat.dense_zcreate(element_type, parent_group, name, false, (n_rows, n_columns))
        hardlink_v3_chunk("$(source_base).data", "$(destination_dir)/c/0/0")
    else
        @assert format == "sparse"
        ind_type = julia_type_from_files_eltype(String(json["indtype"]))
        colptr_path = "$(source_base).colptr"
        rowval_path = "$(source_base).rowval"
        nnz_int = Int(div(filesize(rowval_path), sizeof(ind_type)))
        matrix_group = Zarr.zgroup(parent_group, name)
        ZarrFormat.dense_zcreate(ind_type, matrix_group, "colptr", false, (n_columns + 1,))
        hardlink_v3_chunk(colptr_path, "$(destination_dir)/colptr/c/0")
        ZarrFormat.dense_zcreate(ind_type, matrix_group, "rowval", false, (nnz_int,))
        hardlink_v3_chunk(rowval_path, "$(destination_dir)/rowval/c/0")
        nzval_path = "$(source_base).nzval"
        if isfile(nzval_path)
            ZarrFormat.dense_zcreate(element_type, matrix_group, "nzval", false, (nnz_int,))
            hardlink_v3_chunk(nzval_path, "$(destination_dir)/nzval/c/0")
        end
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
