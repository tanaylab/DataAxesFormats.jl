"""
Import/export `Daf` data from/to `AnnData`.

Due to the different data models, not all the content of `AnnData` can be represented as `Daf`, and vice-versa. However,
"most" of the data can be automatically converted from one form to the other. In both directions, conversion is
zero-copy; that is, we merely create a different view for the same vectors and matrices. We also use memory-mapping
whenever possible for increased performance.

!!! note

    As of Muon.jl 0.1.1, datasets created by `AnnData` are always written in chunked layout, which rules out
    memory-mapping them. In contrast, the Python `anndata` package, as of version 0.10.5, always writes datasets as
    contiguous, which does allow memory mapping them. That is, using `Daf` to access `AnnData` files written in Python
    will be more efficient than accessing data files written in Julia, at least until [this
    issue](https://github.com/scverse/Muon.jl/issues/24) is resolved. Sigh.

The following `Daf` data can't be naively stored in `AnnData`:

  - `AnnData` is restricted to storing data for only two axes, which `AnnData` always calls "obs" and "var".
    In contrast, `Daf` can store data for an arbitrary set of meaningfully named axes.
  - `Anndata` always contains a matrix property for these two axes called "X". Mercifully, the rest of the matrices are
    allowed to have meaningful names. In contrast, `Daf` allows storing an arbitrary set of meaningfully named matrices.
  - `AnnData` can only hold row-major matrices, while Julia defaults to column-major layout.

Therefore, when viewing `Daf` data as `AnnData`, we pick two specific axes and rename them to "obs" and "var", pick a
specific matrix property of these axes and rename it to "X", and [`relayout!`](@ref) it if needed so `AnnData` would be
happy. We store the discarded names of the axes and matrix in unstructured annotations called `obs_is`, `var_is` and
`X_is`. This allows us to reconstruct the original names when re-viewing the `AnnData` as `Daf` data.

The following `AnnData` can't be naively stored in `Daf`:

  - Unstructured data (`uns`) that isn't scalar (e.g., mappings).
  - Matrices whose axis is not explicitly declared (`obsm`, `varm`).

When viewing `AnnData` as `Daf`, we either ignore, warn, or treat as an error any such unsupported data.

!!! warning "DANGER, WILL ROBINSON"

    Square matrices accessed via `Daf` APIs will be the (column-major) **transpose** of the original `AnnData`
    (row-major) matrix.

    Due to limitations of the `Daf` data model, square matrices are stored only in column-major layout. In contrast,
    `AnnData` square matrices (`obsp`, `varp`), are stored in row-major layout. We have several bad options to address
    this:

      - We can break the `Daf` invariant that all accessed data is column-major, at least for square matrices. This is
        bad because the invariant greatly simplifies `Daf` client code. Forcing clients to check the data layout and
        calling `relayout!` would add a lot of error-prone boilerplate to our users.
      - We can `relayout!` the data when copying it between `AnnData` and `Daf`. This is bad because, it would
        force us to duplicate the data. More importantly, there is typically a good reason for the layout of the data.
        For example, assume a directed graph between cells. A common way to store is is to have a square matrix where
        each row contains the weights of the edges originating in one cell, connecting it to all other cells. This
        allows code to efficiently "loop on all cells; loop on all outgoing edges". If we `relayout!` the data, then
        such a loop would become extremely inefficient.
      - We can return the transposed matrix from `Daf`. This is bad because Julia code and Python code processing
        the "same" data would need to flip the indices (e.g., `outgoing_weight[from_cell, to_cell]` in Python vs.
        `outgoing_weight[to_cell, from_cell]` in Julia).

    Having to pick between these bad options, we chose the last one as the lesser evil. The assumption is that Julia
    code is written separately from the Python code anyway. If the same algorithm is implemented in both systems, it
    would work (efficiently!), as long as the developer read this warning and flipped the order of the indices, that is.

    We do **not** have this problem with non-square matrices (e.g., the per-cell-per-gene `UMIs` matrix), since `Daf`
    allows for storing and accessing both layouts of the same data in this case. We simply populate `Daf` with the
    row-major data from `AnnData` and if asked for the outher layout, will `relayout!` it (and store/cache the result).
"""
module AnnDataFormat

export anndata_as_daf
export daf_as_anndata
export ErrorUnsupported
export IgnoreUnsupported
export UnsupportedPolicy
export WarnUnsupported

using Daf.Formats
using Daf.Data
using Daf.MatrixLayouts
using Daf.MemoryFormat
using Daf.MemoryFormat
using Daf.StorageTypes
using Daf.Unions
using DataFrames
using HDF5
using Muon
import Daf.Data.require_matrix
import Daf.Formats
import Daf.Formats.Internal

"""
There are certain types of data held in `AnnData` which do not easily map into `Daf`, specifically:

  - Non-scalars (e.g., mappings) inside `uns` unstructured annotations. The `Daf` equivalent is storing JSON string
    blobs, which is awkward to use. TODO: provide better API to deal with such data.
  - Data using nullable entries (e.g. a matrix with nullable integer entries). In contrast, `Daf` supports the
    convention that zero values are special. This only works in some cases (e.g., it isn't a good solution for Boolean
    data). It is possible of course to explicitly store Boolean masks and apply them to the data, but this is
    inconvenient. TODO: Have `Daf` natively support nullable/masked arrays.
  - Matrix data that only uses one of the axes (that is, `obsm` and `varm` data). The problem here is, paradoxically,
    that `Daf` supports such data "too well", by allowing multiple axes to be defined, and storing matrices based on any
    pair of axes. However, this requires the other axes to be explicitly created, and their information just doesn't
    exist in the `AnnData` data set. TODO: Allow unstructured annotations to store the entries of the other axis.

The `UnsupportedPolicy` describes how to deal with such data:

  - `IgnoreUnsupported` will silently ignore such data.
  - `WarnUnsupported` will emit a warning for each `AnnData` annotation which doesn't map to `Daf`. This is the default.
  - `ErrorUnsupported` will emit an error instead, aborting the program.
"""
@enum UnsupportedPolicy IgnoreUnsupported WarnUnsupported ErrorUnsupported

"""
    anndata_as_daf(
        adata::Union{AnnData, AbstractString};
        [name::Maybe{AbstractString} = nothing,
        obs_is::Maybe{AbstractString} = nothing,
        var_is::Maybe{AbstractString} = nothing,
        X_is::Maybe{AbstractString} = nothing,
        unsupported_policy::UnsupportedPolicy = WarnUnsupported]
    )::MemoryDaf

View `AnnData` as a `Daf` data set, specifically using a [`MemoryDaf`](@ref). This doesn't duplicate matrices or
vectors, but acts as a view containing references to the same ones. Adding and/or deleting data in the view using the
`Daf` API will not affect the original `adata`.

If `adata` is a string, then it is the path of an `h5ad` file which is automatically loaded.

If not specified, the `name` will be the value of the "name" `uns` property, if it exists, otherwise, it will be
"anndata".

If not specified, `obs_is` (the name of the "obs" axis) will be the value of the "obs_is" `uns` property, if it exists,
otherwise, it will be "obs".

If not specified, `var_is` (the name of the "var" axis) will be the value of the "var_is" `uns` property, if it exists,
otherwise, it will be "var".

If not specified, `X_is` (the name of the "X" matrix) will be the value of the "X_is" `uns` property, if it exists,
otherwise, it will be "X".
"""
function anndata_as_daf(
    adata::Union{AnnData, AbstractString};
    name::Maybe{AbstractString} = nothing,
    obs_is::Maybe{AbstractString} = nothing,
    var_is::Maybe{AbstractString} = nothing,
    X_is::Maybe{AbstractString} = nothing,
    unsupported_policy::UnsupportedPolicy = WarnUnsupported,
)::MemoryDaf
    if adata isa AbstractString
        adata = readh5ad(adata; backed = true)  # NOJET
    end

    name = by_annotation(adata, name, "name", "anndata")
    obs_is = by_annotation(adata, obs_is, "obs_is", "obs")
    var_is = by_annotation(adata, var_is, "var_is", "var")
    X_is = by_annotation(adata, X_is, "X_is", "X")
    @assert obs_is != var_is

    verify_unsupported(adata, name, unsupported_policy)
    memory = MemoryDaf(; name = name)
    copy_supported(adata, memory, obs_is, var_is, X_is)
    return memory
end

function by_annotation(
    adata::AnnData,
    value::Maybe{AbstractString},
    name::AbstractString,
    default::AbstractString,
)::AbstractString
    if value == nothing
        value = get(adata.uns, name, default)
        @assert value != nothing
    end
    return value
end

function verify_unsupported(adata::AnnData, name::AbstractString, unsupported_policy::UnsupportedPolicy)::Nothing
    if unsupported_policy != IgnoreUnsupported
        verify_are_supported_type(adata.uns, "uns", StorageScalar, name, unsupported_policy)

        verify_are_supported_type(adata.obs, "obs", StorageVector, name, unsupported_policy)
        verify_are_supported_type(adata.var, "var", StorageVector, name, unsupported_policy)

        verify_is_supported_type(adata.X, StorageMatrix, name, "X", unsupported_policy)
        verify_are_supported_type(adata.layers, "layers", StorageMatrix, name, unsupported_policy)
        verify_are_supported_type(adata.obsp, "obsp", StorageMatrix, name, unsupported_policy)
        verify_are_supported_type(adata.varp, "varp", StorageMatrix, name, unsupported_policy)

        verify_are_empty(adata.obsm, "obsm", name, unsupported_policy)
        verify_are_empty(adata.varm, "varm", name, unsupported_policy)
    end
end

function verify_are_supported_type(
    dict::AbstractDict,
    member::AbstractString,
    supported_type::Type,
    name::AbstractString,
    unsupported_policy::UnsupportedPolicy,
)::Nothing
    for (key, value) in dict  # NOJET
        verify_is_supported_type(value, supported_type, name, "$(member)[$(key)]", unsupported_policy)
    end
    return nothing
end

function verify_are_supported_type(
    frame::DataFrame,
    member::AbstractString,
    supported_type::Type,
    name::AbstractString,
    unsupported_policy::UnsupportedPolicy,
)::Nothing
    for column in names(frame)
        verify_is_supported_type(frame[!, column], supported_type, name, "$(member)[$(column)]", unsupported_policy)
    end
    return nothing
end

function verify_is_supported_type(
    value::Any,
    supported_type::Type,
    name::AbstractString,
    property::AbstractString,
    unsupported_policy::UnsupportedPolicy,
)::Nothing
    if !(value isa supported_type)
        report_unsupported(
            name,
            unsupported_policy,
            "unsupported type for $(property): $(typeof(value))\nsupported type is: $(supported_type)\n",
        )
    end
    if value isa StorageMatrix && !(value isa Muon.TransposedDataset) && major_axis(value) == nothing
        report_unsupported(name, unsupported_policy, "type not in row/column-major layout: $(typeof(value))\n")  # untested
    end
    return nothing
end

function verify_are_empty(
    dict::AbstractDict,
    member::AbstractString,
    name::AbstractString,
    unsupported_policy::UnsupportedPolicy,
)::Nothing
    for key in keys(dict)
        report_unsupported(name, unsupported_policy, "unsupported annotation: $(member)[$(key)]\n")
    end
    return nothing
end

function report_unsupported(
    name::AbstractString,
    unsupported_policy::UnsupportedPolicy,
    message::AbstractString,
)::Nothing
    message *= "in AnnData for the daf data: $(name)"
    if unsupported_policy == ErrorUnsupported
        error(message)
    else
        @assert unsupported_policy == WarnUnsupported
        @warn message
    end
end

function copy_supported(
    adata::AnnData,
    memory::MemoryDaf,
    obs_is::AbstractString,
    var_is::AbstractString,
    X_is::AbstractString,
)::Nothing
    copy_supported_scalars(adata.uns, memory)

    add_axis!(memory, obs_is, adata.obs_names)
    add_axis!(memory, var_is, adata.var_names)

    copy_supported_vectors(adata.obs, memory, obs_is)
    copy_supported_vectors(adata.var, memory, var_is)

    copy_supported_square_matrices(adata.obsp, memory, obs_is)
    copy_supported_square_matrices(adata.varp, memory, var_is)

    copy_supported_matrices(adata.layers, memory, obs_is, var_is)
    copy_supported_matrix(adata.X, memory, obs_is, var_is, X_is)  # NOJET

    return nothing
end

function copy_supported_scalars(uns::AbstractDict, memory::MemoryDaf)::Nothing
    for (name, value) in uns
        if value isa StorageScalar
            set_scalar!(memory, name, value)
        end
    end
end

function copy_supported_vectors(frame::DataFrame, memory::MemoryDaf, axis::AbstractString)::Nothing
    for column in names(frame)
        vector = frame[!, column]
        if vector isa StorageVector
            set_vector!(memory, axis, column, vector)
        end
    end
end

function copy_supported_square_matrices(dict::AbstractDict, memory::MemoryDaf, axis::AbstractString)::Nothing
    for (name, matrix) in dict
        if matrix isa StorageMatrix
            set_matrix!(memory, axis, axis, name, transpose(access_matrix(matrix)); relayout = false)
        end
    end
end

function copy_supported_matrices(
    dict::AbstractDict,
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
)::Nothing
    for (name, matrix) in dict
        copy_supported_matrix(access_matrix(matrix), memory, rows_axis, columns_axis, name)
    end
end

function copy_supported_matrix(  # untested
    matrix::Any,
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    return nothing
end

function copy_supported_matrix(
    matrix::Muon.TransposedDataset,
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    copy_supported_matrix(access_matrix(matrix), memory, rows_axis, columns_axis, name)
    return nothing
end

function copy_supported_matrix(
    matrix::StorageMatrix,
    memory::MemoryDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    matrix_major_axis = major_axis(matrix)
    if matrix_major_axis == Rows
        matrix = transpose(matrix)
        rows_axis, columns_axis = columns_axis, rows_axis
    end
    if matrix_major_axis != nothing
        set_matrix!(memory, rows_axis, columns_axis, name, matrix; relayout = false)
    end
end

function access_matrix(matrix::Muon.TransposedDataset)::AbstractMatrix
    dataset = matrix.dset
    if HDF5.ismmappable(dataset) && HDF5.iscontiguous(dataset)
        return transpose(HDF5.readmmap(dataset))  # untested
    else
        return transpose(read(dataset))
    end
end

function access_matrix(matrix::Any)::Any
    return matrix
end

"""
    daf_as_anndata(
        daf::DafReader;
        [obs_is::Maybe{AbstractString} = nothing,
        var_is::Maybe{AbstractString} = nothing,
        X_is::Maybe{AbstractString} = nothing,
        h5ad::Maybe{AbstractString} = nothing]
    )::AnnData

View the `daf` data set as `AnnData`. This doesn't duplicate matrices or vectors, but acts as a view containing
references to the same ones. Adding and/or deleting data in the view using the `AnnData` API will not affect the
original `daf` data set.

If specified, the result is also written to an `h5ad` file.

If not specified, `obs_is` (the name of the "obs" axis) will be the value of the "obs_is" scalar property, if it exists,
otherwise, it will be "obs".

If not specified, `var_is` (the name of the "var" axis) will be the value of the "var_is" scalar property, if it exists,
otherwise, it will be "var".

If not specified, `X_is` (the name of the "X" matrix) will be the value of the "X_is" scalar property, if it exists,
otherwise, it will be "X".

Each of the final `obs_is`, `var_is`, `X_is` values is stored as unstructured annotations, unless the default value
("obs", "var", "X") is used.

All scalar properties, vector properties of the chosen "obs" and "var" axes, and matrix properties of these axes, are
stored in the returned new `AnnData` object.
"""
function daf_as_anndata(
    daf::DafReader;
    obs_is::Maybe{AbstractString} = nothing,
    var_is::Maybe{AbstractString} = nothing,
    X_is::Maybe{AbstractString} = nothing,
    h5ad::Maybe{AbstractString} = nothing,
)::AnnData
    obs_is = by_scalar(daf, obs_is, "obs_is", "obs")
    var_is = by_scalar(daf, var_is, "var_is", "var")
    X_is = by_scalar(daf, X_is, "X_is", "X")

    @assert obs_is != var_is
    require_matrix(daf, obs_is, var_is, X_is; relayout = true)

    matrix = transpose(get_matrix(daf, var_is, obs_is, X_is))
    adata = AnnData(; X = matrix, obs_names = get_axis(daf, obs_is), var_names = get_axis(daf, var_is))

    copy_scalars(daf, adata.uns)

    store_rename_scalar(adata.uns, obs_is, "obs_is", "obs")
    store_rename_scalar(adata.uns, var_is, "var_is", "var")
    store_rename_scalar(adata.uns, X_is, "X_is", "X")

    copy_square_matrices(daf, obs_is, adata.obsp)
    copy_square_matrices(daf, var_is, adata.varp)

    copy_vectors(daf, obs_is, adata.obs)
    copy_vectors(daf, var_is, adata.var)

    copy_matrices(daf, obs_is, var_is, X_is, adata.layers)

    if h5ad != nothing
        writeh5ad(h5ad, adata)  # NOJET
    end

    return adata
end

function by_scalar(
    daf::DafReader,
    value::Maybe{AbstractString},
    name::AbstractString,
    default::AbstractString,
)::AbstractString
    if value == nothing
        value = get_scalar(daf, name; default = default)
        @assert value != nothing
    end
    return value
end

function copy_scalars(daf::DafReader, dict::AbstractDict)::Nothing
    for name in scalar_names(daf)
        dict[name] = get_scalar(daf, name)
    end
end

function store_rename_scalar(
    dict::AbstractDict,
    value::Maybe{AbstractString},
    name::AbstractString,
    default::AbstractString,
)::Nothing
    if value == default
        delete!(dict, name)  # untested
    else
        dict[name] = value
    end
    return nothing
end

function copy_square_matrices(daf::DafReader, axis::AbstractString, dict::AbstractDict)::Nothing
    for name in matrix_names(daf, axis, axis)
        dict[name] = transpose(get_matrix(daf, axis, axis, name))
    end
end

function copy_vectors(daf::DafReader, axis::AbstractString, frame::DataFrame)::Nothing
    for name in vector_names(daf, axis)
        frame[!, name] = get_vector(daf, axis, name)
    end
end

function copy_matrices(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    skip_name::AbstractString,
    dict::AbstractDict,
)::Nothing
    for name in matrix_names(daf, rows_axis, columns_axis; relayout = true)
        if name != skip_name
            dict[name] = transpose(get_matrix(daf, columns_axis, rows_axis, name))
        end
    end
end

end  # module
