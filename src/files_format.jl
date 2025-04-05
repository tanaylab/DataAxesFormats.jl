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

We use multiple files to store `Daf` data, under some root directory, as follows:

  - The directory will contain 4 sub-directories: `scalars`, `axes`, `vectors`, and `matrices`, and a file called
    `daf.json`.

  - The `daf.json` signifies that the directory contains `Daf` data. In this file, there should be a mapping with a
    `version` key whose value is an array of two integers. The first is the major version number and the second is the
    minor version number, using [semantic versioning](https://semver.org/). This makes it easy to test whether a
    directory does/n't contain `Daf` data, and which version of the internal structure it is using. Currently the only
    defined version is `[1,0]`.
  - The `scalars` directory contains scalar properties, each as in its own `name.json` file, containing a mapping with
    a `type` key whose value is the data type of the scalar (one of the `StorageScalar` types, with `String` for a
    string scalar) and a `value` key whose value is the actual scalar value.
  - The `axes` directory contains a `name.txt` file per axis, where each line contains a name of an axis entry.
  - The `vectors` directory contains a directory per axis, containing the vectors. For every vector, a `name.json` file
    will contain a mapping with an `eltype` key specifying the type of the vector element, and a `format` key specifying
    how the data is stored on disk, one of `dense` and `sparse`.

    If the `format` is `dense`, then there will be a file containing the vector entries, either `name.txt` for strings
    (with a value per line), or `name.data` for binary data (which we can memory-map for direct access).

    If the format is `sparse`, then there will also be an `indtype` key specifying the data type of the indices of the
    non-zero values, and two binary data files, `name.nzind` containing the indices of the non-zero entries, and
    `name.nzval` containing the values of the non-zero entries (which we can memory-map for direct access). See Julia's
    `SparseVector` implementation for details.
  - The `matrices` directly contains a directory per rows axis, which contains a directory per columns axis, which
    contains the matrices. For each matrix, a `name.json` file will contain a mapping with an `eltype` key specifying
    the type of the matrix element, and a `format` key specifying how the data is stored on disk, one of `dense` and
    `sparse`.

    If the `format` is `dense`, then there will be a `name.data` binary file in column-major layout (which we can
    memory-map for direct access).

    If the format is `sparse`, then there will also be an `indtype` key specifying the data type of the indices of the
    non-zero values, and three binary data files, `name.colptr`, `name.rowval` containing the indices of the non-zero
    values, and `name.nzval` containing the values of the non-zero entries (which we can memory-map for direct access).
    See Julia's `SparseMatrixCSC` implementation for details.

!!! note

    Since data is stored in files using the property names, we are sadly susceptible to the operating system vagaries
    when it comes to "what is a valid property name" (e.g., no `/` characters allowed) and whether property names
    are/not case sensitive. In theory, we could just encode the property names somehow but that would make the file
    names opaque which would lose out on a lot of the benefit of using files. It **always** pays to have "sane", simple,
    unique property names, using only alphanumeric characters, that would be a valid variable name in most programming
    languages.

Example directory structure:

    example-daf-dataset-root-directory/
    ├─ daf.json
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

    When creating an HDF5 file to contain `Daf` data, you should specify
    `;fapl=HDF5.FileAccessProperties(;alignment=(1,8))`. This ensures all the memory buffers are properly aligned for
    efficient access. Otherwise, memory mapping will be **much** less efficient. A warning is therefore generated
    whenever you try to access `Daf` data stored in an HDF5 file which does not enforce proper alignment.

That's all there is to it. The format is intentionally simple and transparent to maximize its accessibility by other
(standard) tools. Still, it is easiest to create the data using the Julia `Daf` package.

!!! note

    The code here assumes the files data obeys all the above conventions and restrictions. As long as you only create
    and access `Daf` data in files using [`FilesDaf`](@ref), then the code will work as expected (assuming no bugs).
    However, if you do this in some other way (e.g., directly using the filesystem and custom tools), and the result is
    invalid, then the code here may fails with "less than friendly" error messages.
"""
module FilesFormat

export FilesDaf

using ..Formats
using ..ReadOnly
using ..Readers
using ..StorageTypes
using ..Writers
using Base.Filesystem
using JSON
using Mmap
using SparseArrays
using StringViews
using TanayLabUtilities

import ..Formats
import ..Formats.Internal
import ..Operations.DTYPE_BY_NAME
import ..Readers.base_array
import SparseArrays.indtype

"""
The specific major version of the [`FilesDaf`](@ref) format that is supported by this code (`1`). The code will refuse
to access data that is stored in a different major format.
"""
MAJOR_VERSION::UInt8 = 1

"""
The maximal minor version of the [`FilesDaf`](@ref) format that is supported by this code (`0`). The code will refuse to
access data that is stored with the expected major version (`1`), but that uses a higher minor version.

!!! note

    Modifying data that is stored with a lower minor version number **may** increase its minor version number.
"""
MINOR_VERSION::UInt8 = 0

"""
    FilesDaf(
        path::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing]
    )

Storage in disk files in some directory.

When opening an existing data set, if `name` is not specified, and there exists a "name" scalar property, it is used as
the name. Otherwise, the `path` will be used as the name.

The valid `mode` values are as follows (the default mode is `r`):

| Mode | Allow modifications? | Create if does not exist? | Truncate if exists? | Returned type         |
|:---- |:-------------------- |:------------------------- |:------------------- |:--------------------- |
| `r`  | No                   | No                        | No                  | [`DafReadOnly`](@ref) |
| `r+` | Yes                  | No                        | No                  | [`FilesDaf`](@ref)    |
| `w+` | Yes                  | Yes                       | No                  | [`FilesDaf`](@ref)    |
| `w`  | Yes                  | Yes                       | Yes                 | [`FilesDaf`](@ref)    |
"""
struct FilesDaf <: DafWriter
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
)::Union{FilesDaf, DafReadOnly}
    is_read_only, create_if_missing, truncate_if_exists = Formats.parse_mode(mode)

    if isfile(path)
        error("not a directory: $(path)")
    end

    if truncate_if_exists && isdir(path)
        rm(path; force = true, recursive = true)
    end

    daf_file_path = "$(path)/daf.json"
    if create_if_missing
        if !isdir(path)
            mkpath(path)
        end

        if !ispath(daf_file_path)
            write("$(path)/daf.json", "{\"version\":[$(MAJOR_VERSION),$(MINOR_VERSION)]}\n")
            for directory in ("scalars", "axes", "vectors", "matrices")
                mkdir("$(path)/$(directory)")
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
        if ispath(name_path)
            name = string(read_scalar(name_path))
        else
            name = path
        end
    end
    name = unique_name(name)

    if is_read_only
        file = read_only(FilesDaf(name, Internal(; cache_group = MappedData, is_frozen = true), path, mode, "r"))
    else
        file = FilesDaf(name, Internal(; cache_group = MappedData, is_frozen = false), path, mode, "r+")
    end
    @debug "Daf: $(brief(file)) path: $(path)"
    return file
end

function Formats.format_has_scalar(files::FilesDaf, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(files)
    return ispath("$(files.path)/scalars/$(name).json")
end

function Formats.format_set_scalar!(files::FilesDaf, name::AbstractString, value::StorageScalar)::Nothing
    @assert Formats.has_data_write_lock(files)
    type = typeof(value)
    if type <: AbstractString
        type = String
    end

    open("$(files.path)/scalars/$(name).json", "w") do file
        JSON.Writer.print(file, Dict("type" => "$(type)", "value" => value))
        write(file, '\n')
        return nothing
    end

    return nothing
end

function Formats.format_delete_scalar!(files::FilesDaf, name::AbstractString; for_set::Bool)::Nothing  # NOLINT
    @assert Formats.has_data_write_lock(files)
    return rm("$(files.path)/scalars/$(name).json"; force = true)
end

function Formats.format_get_scalar(files::FilesDaf, name::AbstractString)::StorageScalar
    @assert Formats.has_data_read_lock(files)
    return read_scalar("$(files.path)/scalars/$(name).json")
end

function read_scalar(path::AbstractString)::StorageScalar
    json = JSON.parsefile(path)
    @assert json isa AbstractDict
    dtype_name = json["type"]
    json_value = json["value"]

    if dtype_name == "String"
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
    return ispath("$(files.path)/axes/$(axis).txt")
end

function Formats.format_add_axis!(
    files::FilesDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    open("$(files.path)/axes/$(axis).txt", "w") do file
        for entry in entries
            @assert !contains(entry, '\n')
            println(file, entry)
        end
    end

    mkdir("$(files.path)/vectors/$(axis)")
    mkdir("$(files.path)/matrices/$(axis)")

    axes_set = Formats.get_axes_set_through_cache(files)
    for other_axis in axes_set
        mkdir("$(files.path)/matrices/$(other_axis)/$(axis)")
        if other_axis != axis
            mkdir("$(files.path)/matrices/$(axis)/$(other_axis)")
        end
    end

    return nothing
end

function Formats.format_delete_axis!(files::FilesDaf, axis::AbstractString)::Nothing
    @assert Formats.has_data_write_lock(files)
    rm("$(files.path)/axes/$(axis).txt"; force = true)
    rm("$(files.path)/vectors/$(axis)"; force = true, recursive = true)
    rm("$(files.path)/matrices/$(axis)"; force = true, recursive = true)

    axes_set = Formats.get_axes_set_through_cache(files)
    for other_axis in axes_set
        rm("$(files.path)/matrices/$(other_axis)/$(axis)"; force = true, recursive = true)
    end
end

function Formats.format_axes_set(files::FilesDaf)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return get_names_set("$(files.path)/axes", ".txt")
end

function Formats.format_axis_vector(files::FilesDaf, axis::AbstractString)::AbstractVector{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return mmap_file_lines("$(files.path)/axes/$(axis).txt")
end

function Formats.format_axis_length(files::FilesDaf, axis::AbstractString)::Int64
    @assert Formats.has_data_read_lock(files)
    entries = Formats.get_axis_vector_through_cache(files, axis)
    return length(entries)
end

function Formats.format_has_vector(files::FilesDaf, axis::AbstractString, name::AbstractString)::Bool
    @assert Formats.has_data_read_lock(files)
    return ispath("$(files.path)/vectors/$(axis)/$(name).json")
end

function Formats.format_set_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    if vector == 0
        vector = spzeros(typeof(vector), Formats.format_axis_length(files, axis))
    end

    if vector isa AbstractString
        @assert !(contains(vector, '\n'))
        write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "dense", String)
        fill_file("$(files.path)/vectors/$(axis)/$(name).txt", vector, Formats.format_axis_length(files, axis))

    elseif vector isa StorageScalar
        @assert vector isa StorageReal
        write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "dense", typeof(vector))
        fill_file("$(files.path)/vectors/$(axis)/$(name).data", vector, Formats.format_axis_length(files, axis))

    elseif issparse(vector)
        write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "sparse", eltype(vector), indtype(vector))
        write("$(files.path)/vectors/$(axis)/$(name).nzind", nzind(vector))
        write("$(files.path)/vectors/$(axis)/$(name).nzval", nzval(vector))

    elseif eltype(vector) <: AbstractString
        write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "dense", String)
        open("$(files.path)/vectors/$(axis)/$(name).txt", "w") do file
            for value in vector
                @assert !(contains(value, '\n'))
                println(file, value)
            end
            return nothing
        end
    else
        write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "dense", eltype(vector))
        write("$(files.path)/vectors/$(axis)/$(name).data", vector)
    end
    return nothing
end

function Formats.format_get_empty_dense_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
)::AbstractVector{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(files)

    write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "dense", T)
    path = "$(files.path)/vectors/$(axis)/$(name).data"

    size = Formats.format_axis_length(files, axis)
    fill_file(path, T(0), size)

    return mmap_file_data(path, Vector{T}, size, "r+")
end

function Formats.format_get_empty_sparse_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(files)

    write_array_json("$(files.path)/vectors/$(axis)/$(name).json", "sparse", T, I)
    nzind_path = "$(files.path)/vectors/$(axis)/$(name).nzind"
    nzval_path = "$(files.path)/vectors/$(axis)/$(name).nzval"

    fill_file(nzind_path, I(0), nnz)
    fill_file(nzval_path, T(0), nnz)

    nzind_vector = mmap_file_data(nzind_path, Vector{I}, nnz, "r+")
    nzval_vector = mmap_file_data(nzval_path, Vector{T}, nnz, "r+")

    return (nzind_vector, nzval_vector)
end

function Formats.format_delete_vector!(
    files::FilesDaf,
    axis::AbstractString,
    name::AbstractString;
    for_set::Bool,  # NOLINT
)::Nothing
    @assert Formats.has_data_write_lock(files)
    for suffix in (".json", ".data", ".nzind", ".nzval")
        rm("$(files.path)/vectors/$(axis)/$(name)$(suffix)"; force = true)
    end
end

function Formats.format_vectors_set(files::FilesDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    @assert Formats.has_data_read_lock(files)
    return get_names_set("$(files.path)/vectors/$(axis)", ".json")
end

function Formats.format_get_vector(files::FilesDaf, axis::AbstractString, name::AbstractString)::StorageVector
    @assert Formats.has_data_read_lock(files)

    json = JSON.parsefile("$(files.path)/vectors/$(axis)/$(name).json")
    @assert json isa AbstractDict
    eltype_name = json["eltype"]
    format = json["format"]
    @assert format == "dense" || format == "sparse"

    size = Formats.format_axis_length(files, axis)
    if format == "dense"
        if eltype_name == "string" || eltype_name == "String"
            vector = mmap_file_lines("$(files.path)/vectors/$(axis)/$(name).txt")
            @assert length(vector) == size
        else
            eltype = DTYPE_BY_NAME[eltype_name]
            @assert eltype !== nothing
            vector =
                mmap_file_data("$(files.path)/vectors/$(axis)/$(name).data", Vector{eltype}, size, files.files_mode)
        end
    else
        @assert format == "sparse"
        indtype_name = json["indtype"]

        eltype = DTYPE_BY_NAME[eltype_name]
        @assert eltype !== nothing

        indtype = DTYPE_BY_NAME[indtype_name]
        @assert indtype !== nothing

        nzind_path = "$(files.path)/vectors/$(axis)/$(name).nzind"
        nzval_path = "$(files.path)/vectors/$(axis)/$(name).nzval"

        nnz = div(filesize(nzval_path), sizeof(eltype))

        nzind_vector = mmap_file_data(nzind_path, Vector{indtype}, nnz, files.files_mode)
        nzval_vector = mmap_file_data(nzval_path, Vector{eltype}, nnz, files.files_mode)

        vector = SparseVector(size, nzind_vector, nzval_vector)
    end

    return vector
end

function Formats.format_has_matrix(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Bool
    @assert Formats.has_data_read_lock(files)
    return ispath("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json")
end

function Formats.format_set_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageReal, StorageMatrix},
)::Nothing
    @assert Formats.has_data_write_lock(files)
    nrows = Formats.format_axis_length(files, rows_axis)
    ncols = Formats.format_axis_length(files, columns_axis)
    if matrix == 0
        matrix = spzeros(typeof(matrix), nrows, ncols)
    end

    if matrix isa StorageReal
        write_array_json("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json", "dense", typeof(matrix))
        fill_file("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).data", matrix, nrows * ncols)  # NOJET

    elseif issparse(matrix)
        @assert matrix isa AbstractMatrix
        write_array_json(  # NOJET
            "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json",
            "sparse",
            eltype(matrix),
            indtype(matrix),
        )
        write("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).colptr", colptr(matrix))
        write("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).rowval", rowval(matrix))
        write("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).nzval", nzval(matrix))

    else
        write_array_json("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json", "dense", eltype(matrix))
        write("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).data", matrix)
    end

    return nothing
end

function Formats.format_get_empty_dense_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
)::AbstractMatrix{T} where {T <: StorageReal}
    @assert Formats.has_data_write_lock(files)
    write_array_json("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json", "dense", T)
    path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).data"

    nrows = Formats.format_axis_length(files, rows_axis)
    ncols = Formats.format_axis_length(files, columns_axis)
    fill_file(path, T(0), nrows * ncols)

    return mmap_file_data(path, Matrix{T}, (nrows, ncols), "r+")
end

function Formats.format_get_empty_sparse_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    ::Type{T},
    nnz::StorageInteger,
    ::Type{I},
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    @assert Formats.has_data_write_lock(files)
    write_array_json("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json", "sparse", T, I)

    ncols = Formats.format_axis_length(files, columns_axis)

    colptr_path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).colptr"
    rowval_path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).rowval"
    nzval_path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).nzval"

    fill_file(colptr_path, I(0), ncols + 1)
    fill_file(rowval_path, I(0), nnz)
    fill_file(nzval_path, T(0), nnz)

    colptr_vector = mmap_file_data(colptr_path, Vector{I}, (ncols + 1), "r+")
    rowval_vector = mmap_file_data(rowval_path, Vector{I}, nnz, "r+")
    nzval_vector = mmap_file_data(nzval_path, Vector{T}, nnz, "r+")
    return (colptr_vector, rowval_vector, nzval_vector)
end

function Formats.format_relayout_matrix!(
    files::FilesDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::StorageMatrix,
)::StorageMatrix
    @assert Formats.has_data_write_lock(files)

    if issparse(matrix)
        colptr, rowval, nzval = Formats.format_get_empty_sparse_matrix!(
            files,
            columns_axis,
            rows_axis,
            name,
            eltype(matrix),
            nnz(matrix),
            eltype(matrix.colptr),
        )
        colptr[1] = 1
        colptr[2:end] .= length(nzval) + 1
        relayout_matrix =
            SparseMatrixCSC(axis_length(files, columns_axis), axis_length(files, rows_axis), colptr, rowval, nzval)
    else
        relayout_matrix = Formats.format_get_empty_dense_matrix!(files, columns_axis, rows_axis, name, eltype(matrix))
    end

    relayout!(transpose(relayout_matrix), matrix)
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
    for suffix in (".json", ".data", ".colptr", ".rowval", "nzval")
        rm("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name)$(suffix)"; force = true)
    end
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
)::StorageMatrix
    @assert Formats.has_data_read_lock(files)

    json = JSON.parsefile("$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).json")
    @assert json isa AbstractDict
    format = json["format"]
    @assert format == "dense" || format == "sparse"
    eltype_name = json["eltype"]
    eltype = DTYPE_BY_NAME[eltype_name]
    @assert eltype !== nothing

    nrows = Formats.format_axis_length(files, rows_axis)
    ncols = Formats.format_axis_length(files, columns_axis)

    if format == "dense"
        matrix = mmap_file_data(
            "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).data",
            Matrix{eltype},
            (nrows, ncols),
            files.files_mode,
        )
    else
        @assert format == "sparse"
        indtype_name = json["indtype"]

        eltype = DTYPE_BY_NAME[eltype_name]
        @assert eltype !== nothing

        indtype = DTYPE_BY_NAME[indtype_name]
        @assert indtype !== nothing

        colptr_path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).colptr"
        rowval_path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).rowval"
        nzval_path = "$(files.path)/matrices/$(rows_axis)/$(columns_axis)/$(name).nzval"

        nnz = div(filesize(nzval_path), sizeof(eltype))

        colptr_vector = mmap_file_data(colptr_path, Vector{indtype}, ncols + 1, files.files_mode)
        rowval_vector = mmap_file_data(rowval_path, Vector{indtype}, nnz, files.files_mode)
        nzval_vector = mmap_file_data(nzval_path, Vector{eltype}, nnz, files.files_mode)

        matrix = SparseMatrixCSC(nrows, ncols, colptr_vector, rowval_vector, nzval_vector)  # NOJET
    end

    return matrix
end

function get_names_set(path::AbstractString, suffix::AbstractString)::AbstractSet{<:AbstractString}
    names_set = Set{AbstractString}()
    suffix_length = length(suffix)

    for file_name in readdir(path)
        if endswith(file_name, suffix)
            push!(names_set, chop(file_name; tail = suffix_length))
        end
    end

    return names_set
end

function mmap_file_lines(path::AbstractString)::AbstractVector{<:AbstractString}
    size = filesize(path)
    text = StringView(mmap_file_data(path, Vector{UInt8}, size, "r"))
    lines = split(text, "\n")
    last_line = pop!(lines)
    @assert last_line == ""
    return lines
end

function mmap_file_data(
    path::AbstractString,
    ::Type{T},
    size::Union{Integer, Tuple{<:Integer, <:Integer}},
    mode::AbstractString,
)::T where {T <: Union{StorageVector, StorageMatrix}}
    return open(path, mode) do file
        return mmap(file, T, size)  # NOJET
    end
end

function fill_file(path::AbstractString, value::StorageScalar, size::Integer)::Nothing
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

function write_zeros_file(path::AbstractString, size::Integer)::Nothing
    open(path, "w") do file
        if size > 0
            seek(file, size - 1)
            write(file, UInt8(0))
        end
    end
    return nothing
end

function write_array_json(
    path::AbstractString,
    format::AbstractString,
    eltype::Type{<:StorageScalar},
    indtype::Maybe{Type{<:StorageInteger}} = nothing,
)::Nothing
    if format == "dense"
        @assert indtype === nothing
        write(path, "{\"format\":\"dense\",\"eltype\":\"$(eltype)\"}\n")
    else
        @assert format == "sparse"
        @assert indtype !== nothing
        write(path, "{\"format\":\"sparse\",\"eltype\":\"$(eltype)\",\"indtype\":\"$(indtype)\"}\n")
    end
    return nothing
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

end  # module
