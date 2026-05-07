"""
Read-only access to a [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) directory served over HTTP.

The server is assumed to expose the `FilesDaf` directory tree verbatim, with `GET {url}/{relative_path}` returning the
byte contents of the file. Any static web server (e.g. `python -m http.server`, `nginx`, S3 with HTTP access) pointed at
the root directory will do.

The client downloads `metadata.json` once at open time and keeps the parsed dict in memory for the lifetime of the
[`HttpDaf`](@ref). Every per-property descriptor (axes, scalars, vectors, matrices) is served from this in-memory
dict, so enumerating scalars/axes/vectors/matrices and reading all their JSON metadata completes without further HTTP
traffic.

Non-JSON payloads (axis entry `.txt` files, per-property `.data` / `.nzind` / `.nzval` / `.colptr` / `.rowval` /
`.nztxt`) are fetched lazily on first use via one `GET` each. Dense and sparse numeric vectors/matrices are returned
zero-copy by `unsafe_wrap`'ing the downloaded `Vector{UInt8}` buffer; the buffer is held alive by the same cache entry
that holds the returned array. Matching the Zarr HTTP backend, the server data is assumed stable while an
[`HttpDaf`](@ref) is open; reopening is the only way to pick up server-side changes.

!!! warning

    Because returned vectors/matrices alias the cache entry's `Vector{UInt8}` backing, calling
    [`empty_cache!`](@ref DataAxesFormats.Formats.empty_cache!) releases the memory that those arrays read from. Any
    vector/matrix reference previously returned by this format becomes dangling after the cache is emptied. Only call
    `empty_cache!` once you have dropped all references returned by prior queries.

Only a read-only interface is exposed; mutations are not supported.
"""
module HttpFormat

export HttpDaf

using ..Formats
using ..Readers
using ..StorageTypes
using Base.Threads
using HTTP
using JSON
using SparseArrays
using TanayLabUtilities

import ..FilesFormat.MAJOR_VERSION
import ..FilesFormat.MINOR_VERSION
import ..Formats.Internal
import ..Operations.DTYPE_BY_NAME
import ..PackedFormat.parse_sparse_descriptor

"""
    struct HttpDaf <: DafReader ... end

    HttpDaf(
        url::AbstractString;
        [name::Maybe{AbstractString} = nothing,
        packed::Bool = false]
    )::HttpDaf

Open a read-only view of a remote [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf) served over `http://` or
`https://`. `url` points at the root directory; the server must expose `metadata.json` and `daf.json` alongside the
usual `FilesDaf` tree.

If `name` is not specified and the remote data set defines a `name` scalar property, it is used as the name;
otherwise, the `url` itself is used.

The `packed` kwarg is accepted for API uniformity with the writable formats but has no effect — `HttpDaf` is
read-only, and `packed` controls only the per-daf write default.

!!! warning

    Vectors and matrices returned by this format alias `Vector{UInt8}` buffers that are held alive by the Daf cache.
    Calling [`empty_cache!`](@ref DataAxesFormats.Formats.empty_cache!) on this `HttpDaf` releases those buffers, so any
    previously returned array reference becomes dangling. Drop all such references before emptying the cache.

!!! note

    [`ZipDaf`](@ref DataAxesFormats.ZipFormat.ZipDaf) archives intentionally do not contain `metadata.json`, so an
    `unzip foo.daf.zip -d foo.daf/` produces a directory that lacks it. Before exposing such a directory over HTTP,
    open it once locally with `FilesDaf("foo.daf")` (any mode) so `FilesFormat.ensure_metadata_json!` builds it.
"""
struct HttpDaf <: DafReader
    name::AbstractString
    internal::Internal
    url::String
    metadata::Dict{String, Any}
end

function HttpDaf(
    url::AbstractString;
    name::Maybe{AbstractString} = nothing,
    packed::Bool = false,  # NOLINT
)::HttpDaf
    if !(startswith(url, "http://") || startswith(url, "https://"))
        error("not an HTTP(S) URL: $(url)")
    end
    url = String(rstrip(url, '/'))

    daf_json = JSON.parse(String(http_get("$(url)/daf.json")))
    @assert daf_json isa AbstractDict
    version = daf_json["version"]
    @assert version isa AbstractVector
    @assert length(version) == 2
    if Int(version[1]) != MAJOR_VERSION || Int(version[2]) > MINOR_VERSION
        error(chomp("""
              incompatible format version: $(version[1]).$(version[2])
              for the daf HTTP data set: $(url)
              the code supports version: $(MAJOR_VERSION).$(MINOR_VERSION)
              """))
    end

    metadata_parsed = JSON.parse(String(http_get("$(url)/metadata.json")))
    @assert metadata_parsed isa AbstractDict "$(url)/metadata.json must be a JSON object"
    metadata = Dict{String, Any}(String(key) => value for (key, value) in metadata_parsed)

    if name === nothing
        scalar_name_descriptor = get(metadata, "scalars/name", nothing)
        if scalar_name_descriptor isa AbstractDict && haskey(scalar_name_descriptor, "value")
            name = string(scalar_name_descriptor["value"])
        else
            name = url
        end
    end
    name = unique_name(name)

    http = HttpDaf(name, Internal(; is_frozen = true), url, metadata)
    @debug "Daf: $(brief(http)) url: $(url)" _group = :daf_repos
    return http
end

function http_get(url::AbstractString)::Vector{UInt8}
    response = try
        HTTP.get(url; retry = false, status_exception = false)  # NOJET
    catch exception
        error("HTTP GET failed for: $(url)\nunderlying error: $(exception)")
    end
    if response.status != 200
        error("HTTP GET returned status $(response.status) for: $(url)")
    end
    return response.body
end

function descriptor_to_scalar(descriptor::AbstractDict)::StorageScalar
    dtype_name = descriptor["type"]
    json_value = descriptor["value"]
    if dtype_name == "String" || dtype_name == "string"
        @assert json_value isa AbstractString
        return String(json_value)
    else
        type = get(DTYPE_BY_NAME, dtype_name, nothing)
        @assert type !== nothing
        return convert(type, json_value)
    end
end

function fetch_lines(http::HttpDaf, relative_path::AbstractString)::Vector{SubString{String}}
    bytes = http_get("$(http.url)/$(relative_path)")
    text = String(bytes)
    lines = split(text, '\n')
    @assert !isempty(lines)
    last_line = pop!(lines)
    @assert last_line == ""
    return lines
end

# Names of children under `parent_key` (one path segment beyond `parent_key/`) — i.e. keys whose form is
# `parent_key/<name>` with `<name>` containing no further `/`. Used to enumerate scalars / vectors under a single axis
# / matrices under a single (rows, columns) pair from the consolidated metadata dict.
function names_under(http::HttpDaf, parent_key::AbstractString)::Set{AbstractString}
    names_set = Set{AbstractString}()
    prefix = "$(parent_key)/"
    prefix_len = length(prefix)
    for key in keys(http.metadata)
        if startswith(key, prefix)
            tail = key[(prefix_len + 1):end]
            if !contains(tail, '/')
                push!(names_set, String(tail))
            end
        end
    end
    return names_set
end

function Readers.is_leaf(::HttpDaf)::Bool  # FLAKY TESTED
    return true
end

function Readers.is_leaf(::Type{HttpDaf})::Bool  # FLAKY TESTED
    return true
end

function Formats.format_has_scalar(http::HttpDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(http)
    return haskey(http.metadata, "scalars/$(name)")
end

function Formats.format_get_scalar(http::HttpDaf, name::AbstractString)::Tuple{StorageScalar, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(http)
    descriptor = http.metadata["scalars/$(name)"]
    @assert descriptor isa AbstractDict
    return (descriptor_to_scalar(descriptor), Formats.MemoryData)
end

function Formats.format_scalars_set(http::HttpDaf)::AbstractSet{<:AbstractString}  # FLAKY TESTED
    @assert Formats.has_data_read_lock(http)
    return names_under(http, "scalars")
end

function Formats.format_has_axis(http::HttpDaf, axis::AbstractString; for_change::Bool)::Bool  # NOLINT
    @assert Formats.has_data_read_lock(http)
    return axis in Formats.get_axes_set_through_cache(http)
end

function Formats.format_axes_set(http::HttpDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(http)
    return names_under(http, "axes")
end

function Formats.format_axis_vector(  # FLAKY TESTED
    http::HttpDaf,
    axis::AbstractString,
)::Tuple{AbstractVector{<:AbstractString}, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(http)
    return (fetch_lines(http, "axes/$(axis).txt"), Formats.MemoryData)
end

function Formats.format_axis_length(http::HttpDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(http)
    entries = Formats.get_axis_vector_through_cache(http, axis)
    return length(entries)
end

function Formats.format_has_vector(http::HttpDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(http)
    return haskey(http.metadata, "vectors/$(axis)/$(name)")
end

function Formats.format_vectors_set(http::HttpDaf, axis::AbstractString)::AbstractSet{<:AbstractString}  # FLAKY TESTED
    @assert Formats.has_data_read_lock(http)
    return names_under(http, "vectors/$(axis)")
end

function Formats.format_get_vector(
    http::HttpDaf,
    axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageVector, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(http)
    json = http.metadata["vectors/$(axis)/$(name)"]
    @assert json isa AbstractDict
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    size = Formats.format_axis_length(http, axis)
    if format == "dense"
        eltype_name = json["eltype"]
        if eltype_name == "String" || eltype_name == "string"
            vector = fetch_lines(http, "vectors/$(axis)/$(name).txt")
            @assert length(vector) == size
            return (vector, nothing, Formats.MemoryData)
        else
            element_type = DTYPE_BY_NAME[eltype_name]
            @assert element_type !== nothing
            data_bytes = http_get("$(http.url)/vectors/$(axis)/$(name).data")
            vector = bytes_to_vector(data_bytes, element_type, size)
            return (vector, data_bytes, Formats.MemoryData)
        end
    end

    @assert format == "sparse"
    eltype_name, indtype_name = parse_sparse_descriptor(json, "nzind")
    ind_type = DTYPE_BY_NAME[indtype_name]
    @assert ind_type !== nothing
    nzind_bytes = http_get("$(http.url)/vectors/$(axis)/$(name).nzind")
    nnz = div(length(nzind_bytes), sizeof(ind_type))
    nzind_vector = bytes_to_vector(nzind_bytes, ind_type, nnz)

    if eltype_name == "String" || eltype_name == "string"
        vector = Vector{AbstractString}(undef, size)
        fill!(vector, "")
        vector[nzind_vector] .= fetch_lines(http, "vectors/$(axis)/$(name).nztxt")  # NOJET
        return (vector, nothing, Formats.MemoryData)
    elseif eltype_name == "Bool"
        nzval_vector = fill(true, nnz)
        return (SparseVector(size, nzind_vector, nzval_vector), nzind_bytes, Formats.MemoryData)
    else
        element_type = DTYPE_BY_NAME[eltype_name]
        @assert element_type !== nothing
        nzval_bytes = http_get("$(http.url)/vectors/$(axis)/$(name).nzval")
        nzval_vector = bytes_to_vector(nzval_bytes, element_type, nnz)
        return (SparseVector(size, nzind_vector, nzval_vector), (nzind_bytes, nzval_bytes), Formats.MemoryData)
    end
end

function Formats.format_has_matrix(
    http::HttpDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(http)
    return haskey(http.metadata, "matrices/$(rows_axis)/$(columns_axis)/$(name)")
end

function Formats.format_matrices_set(  # FLAKY TESTED
    http::HttpDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(http)
    return names_under(http, "matrices/$(rows_axis)/$(columns_axis)")
end

function Formats.format_get_matrix(
    http::HttpDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Tuple{StorageMatrix, Any, Formats.CacheGroup}
    @assert Formats.has_data_read_lock(http)
    nrows = Formats.format_axis_length(http, rows_axis)
    ncols = Formats.format_axis_length(http, columns_axis)

    json = http.metadata["matrices/$(rows_axis)/$(columns_axis)/$(name)"]
    @assert json isa AbstractDict
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    base = "matrices/$(rows_axis)/$(columns_axis)/$(name)"
    if format == "dense"
        eltype_name = json["eltype"]
        if eltype_name == "String" || eltype_name == "string"
            flat = fetch_lines(http, "$(base).txt")
            @assert length(flat) == nrows * ncols
            return (reshape(flat, (nrows, ncols)), nothing, Formats.MemoryData)
        else
            element_type = DTYPE_BY_NAME[eltype_name]
            @assert element_type !== nothing
            data_bytes = http_get("$(http.url)/$(base).data")
            flat = bytes_to_vector(data_bytes, element_type, nrows * ncols)
            return (reshape(flat, (nrows, ncols)), data_bytes, Formats.MemoryData)
        end
    end

    @assert format == "sparse"
    eltype_name, indtype_name = parse_sparse_descriptor(json, "colptr")
    ind_type = DTYPE_BY_NAME[indtype_name]
    @assert ind_type !== nothing
    colptr_bytes = http_get("$(http.url)/$(base).colptr")
    colptr_vector = bytes_to_vector(colptr_bytes, ind_type, ncols + 1)
    rowval_bytes = http_get("$(http.url)/$(base).rowval")
    nnz = div(length(rowval_bytes), sizeof(ind_type))
    rowval_vector = bytes_to_vector(rowval_bytes, ind_type, nnz)

    if eltype_name == "String" || eltype_name == "string"
        matrix = Matrix{AbstractString}(undef, nrows, ncols)
        fill!(matrix, "")
        nztxt_vector = fetch_lines(http, "$(base).nztxt")
        position = 1
        for column_index in 1:ncols
            first_row_position = colptr_vector[column_index]
            last_row_position = colptr_vector[column_index + 1] - 1
            for row_index in rowval_vector[first_row_position:last_row_position]
                matrix[row_index, column_index] = nztxt_vector[position]
                position += 1
            end
        end
        return (matrix, nothing, Formats.MemoryData)
    elseif eltype_name == "Bool"
        nzval_vector = fill(true, nnz)
        return (
            SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector),
            (colptr_bytes, rowval_bytes),
            Formats.MemoryData,
        )
    else
        element_type = DTYPE_BY_NAME[eltype_name]
        @assert element_type !== nothing
        nzval_bytes = http_get("$(http.url)/$(base).nzval")
        nzval_vector = bytes_to_vector(nzval_bytes, element_type, nnz)
        return (
            SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector),
            (colptr_bytes, rowval_bytes, nzval_bytes),
            Formats.MemoryData,
        )
    end
end

function bytes_to_vector(bytes::Vector{UInt8}, ::Type{T}, size::Integer)::Vector{T} where {T}
    @assert length(bytes) == size * sizeof(T)
    return unsafe_wrap(Array, Ptr{T}(pointer(bytes)), size; own = false)
end

function Formats.format_description_header(
    http::HttpDaf,
    indent::AbstractString,
    lines::Vector{String},
    ::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(http)
    push!(lines, "$(indent)type: HttpDaf")
    push!(lines, "$(indent)url: $(http.url)")
    return nothing
end

function Readers.complete_path(http::HttpDaf)::Maybe{AbstractString}
    return http.url
end

end  # module
