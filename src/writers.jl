"""
filled vector:
The [`DafWriter`](@ref) interface specify a high-level API for writing `Daf` data. This API is implemented here, on top
of the low-level [`FormatWriter`](@ref) API. This is an extension of the [`DafReader`](@ref) API and provides provides
thread safety for reading and writing to the same data set from multiple threads, so the low-level API can (mostly)
ignore this issue.
"""
module Writers

export add_axis!
export delete_axis!
export delete_matrix!
export delete_scalar!
export delete_vector!
export empty_dense_matrix!
export empty_dense_vector!
export empty_sparse_matrix!
export empty_sparse_vector!
export filled_empty_dense_matrix!
export filled_empty_dense_vector!
export filled_empty_sparse_matrix!
export filled_empty_sparse_vector!
export get_empty_dense_matrix!
export get_empty_dense_vector!
export get_empty_sparse_matrix!
export get_empty_sparse_vector!
export relayout_matrix!
export set_matrix!
export set_scalar!
export set_vector!

using ..Formats
using ..GenericTypes
using ..MatrixLayouts
using ..Messages
using ..Readers
using ..StorageTypes
using ConcurrentUtils
using NamedArrays
using SparseArrays

import ..Formats
import ..Formats.as_named_matrix
import ..Formats.as_named_vector
import ..Formats.as_read_only_array
import ..Formats.CacheEntry
import ..Formats.FormatReader
import ..Formats.FormatWriter
import ..Formats.upgrade_to_write_lock
import ..Formats.begin_write_lock
import ..Formats.with_read_lock
import ..Formats.with_write_lock
import ..Messages
import ..Readers.base_array
import ..Readers.require_axis
import ..Readers.require_axis_length
import ..Readers.require_axis_names
import ..Readers.require_column_major
import ..Readers.require_dim_name
import ..Readers.require_matrix
import ..Readers.require_scalar
import ..Readers.require_vector
import ..StorageTypes.indtype_for_size

"""
    set_scalar!(
        daf::DafWriter,
        name::AbstractString,
        value::StorageScalar;
        [overwrite::Bool = false]
    )::Nothing

Set the `value` of a scalar property with some `name` in `daf`.

If not `overwrite` (the default), this first verifies the `name` scalar property does not exist.
"""
function set_scalar!(daf::DafWriter, name::AbstractString, value::StorageScalar; overwrite::Bool = false)::Nothing
    @assert value isa AbstractString || isbits(value)
    return with_write_lock(daf) do
        @debug "set_scalar! daf: $(depict(daf)) name: $(name) value: $(depict(value)) overwrite: $(overwrite)"

        if !overwrite
            require_no_scalar(daf, name)
        else
            delete_scalar!(daf, name; must_exist = false, _for_set = true)
        end

        Formats.invalidate_cached!(daf, Formats.scalar_names_cache_key())
        Formats.format_set_scalar!(daf, name, value)

        return nothing
    end
end

"""
    delete_scalar!(
        daf::DafWriter,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a scalar property with some `name` from `daf`.

If `must_exist` (the default), this first verifies the `name` scalar property exists in `daf`.
"""
function delete_scalar!(daf::DafWriter, name::AbstractString; must_exist::Bool = true, _for_set = false)::Nothing
    return with_write_lock(daf) do
        @debug "delete_scalar! daf: $(depict(daf)) name: $(name) must exist: $(must_exist)"

        if must_exist
            require_scalar(daf, name)
        end

        if Formats.format_has_scalar(daf, name)
            Formats.invalidate_cached!(daf, Formats.scalar_cache_key(name))
            Formats.invalidate_cached!(daf, Formats.scalar_names_cache_key())
            Formats.format_delete_scalar!(daf, name; for_set = _for_set)
        end

        return nothing
    end
end

function require_no_scalar(daf::DafReader, name::AbstractString)::Nothing
    if Formats.format_has_scalar(daf, name)
        error("existing scalar: $(name)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    add_axis!(
        daf::DafWriter,
        axis::AbstractString,
        entries::AbstractStringVector
    )::Nothing

Add a new `axis` to `daf`.

This first verifies the `axis` does not exist and that the `entries` are unique.
"""
function add_axis!(daf::DafWriter, axis::AbstractString, entries::AbstractStringVector)::Nothing
    entries = base_array(entries)
    if entries isa SparseVector
        entries = Vector(entries)  # untested
    end
    return with_write_lock(daf) do
        @debug "add_axis! daf: $(depict(daf)) axis: $(axis) entries: $(depict(entries))"

        require_no_axis(daf, axis; for_change = true)

        if !allunique(entries)
            error("non-unique entries for new axis: $(axis)\nin the daf data: $(daf.name)")
        end

        Formats.invalidate_cached!(daf, Formats.axis_names_cache_key())
        Formats.format_add_axis!(daf, axis, entries)

        return nothing
    end
end

"""
    delete_axis!(
        daf::DafWriter,
        axis::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete an `axis` from the `daf`. This will also delete any vector or matrix properties that are based on this axis.

If `must_exist` (the default), this first verifies the `axis` exists in the `daf`.
"""
function delete_axis!(daf::DafWriter, axis::AbstractString; must_exist::Bool = true)::Nothing
    return with_write_lock(daf) do
        @debug "delete_axis! daf: $(depict(daf)) axis: $(axis) must exist: $(must_exist)"

        if must_exist
            require_axis(daf, axis; for_change = true)
        elseif !Formats.format_has_axis(daf, axis; for_change = true)
            return nothing
        end

        vector_names = Formats.get_vector_names_through_cache(daf, axis)
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))

        for name in vector_names
            Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
            Formats.format_delete_vector!(daf, axis, name; for_set = false)
        end

        axis_names = Formats.get_axis_names_through_cache(daf)
        for other_axis in axis_names
            matrix_names = Formats.get_matrix_names_through_cache(daf, axis, other_axis)
            Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(axis, other_axis))

            for name in matrix_names
                Formats.invalidate_cached!(daf, Formats.matrix_cache_key(axis, other_axis, name))
                Formats.format_delete_matrix!(daf, axis, other_axis, name; for_set = false)
            end

            if axis != other_axis
                matrix_names = Formats.get_matrix_names_through_cache(daf, other_axis, axis)
                Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(other_axis, axis))

                for name in matrix_names
                    Formats.invalidate_cached!(daf, Formats.matrix_cache_key(other_axis, axis, name))
                    Formats.format_delete_matrix!(daf, other_axis, axis, name; for_set = false)
                end
            end
        end

        Formats.invalidate_cached!(daf, Formats.axis_cache_key(axis))
        Formats.invalidate_cached!(daf, Formats.axis_names_cache_key())
        Formats.format_increment_version_counter(daf, axis)
        Formats.format_delete_axis!(daf, axis)
        delete!(daf.internal.axes, axis)

        return nothing
    end
end

function require_no_axis(daf::DafReader, axis::AbstractString; for_change::Bool = false)::Nothing
    if Formats.format_has_axis(daf, axis; for_change = for_change)
        error("existing axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    set_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        vector::Union{StorageScalar, StorageVector};
        [overwrite::Bool = false]
    )::Nothing

Set a vector property with some `name` for some `axis` in `daf`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This first verifies the `axis` exists in `daf`, that the property name isn't `name`, and that the `vector` has the
appropriate length. If not `overwrite` (the default), this also verifies the `name` vector does not exist for the
`axis`.
"""
function set_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector};
    overwrite::Bool = false,
)::Nothing
    @assert eltype(vector) <: AbstractString || isbitstype(eltype(vector))
    return with_write_lock(daf) do
        @debug "set_vector! daf: $(depict(daf)) axis: $(axis) name: $(name) vector: $(depict(vector)) overwrite: $(overwrite)"

        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        if vector isa StorageVector
            require_axis_length(daf, "vector length", length(vector), axis)
            if vector isa NamedVector
                require_dim_name(daf, axis, "vector dim name", dimnames(vector, 1))
                require_axis_names(daf, axis, "entry names of the: vector", names(vector, 1))
            end
            vector = base_array(vector)
        end

        if !overwrite
            require_no_vector(daf, axis, name)
        else
            delete_vector!(daf, axis, name; must_exist = false, _for_set = true)
        end

        update_caches_before_set_vector(daf, axis, name)
        Formats.format_set_vector!(daf, axis, name, vector)

        return nothing
    end
end

"""
    empty_dense_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T};
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber}

Create an empty dense vector property with some `name` for some `axis` in `daf`, pass it to `fill`, and return the
result.

The returned vector will be uninitialized; the caller is expected to `fill` it with values. This saves creating a copy
of the vector before setting it in the data, which makes a huge difference when creating vectors on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If not `overwrite` (the
default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_dense_vector!(
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber}
    @assert isbitstype(eltype)
    vector = get_empty_dense_vector!(daf, axis, name, eltype; overwrite = overwrite)
    try
        result = fill(vector)
        filled_empty_dense_vector!(daf, axis, name, vector)
        return result
    finally
        end_write_lock(daf)
    end
end

function get_empty_dense_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::AbstractVector{T} where {T <: StorageNumber}
    @assert isbitstype(eltype)
    return begin_write_lock(daf) do
        @debug "empty_dense_vector! daf: $(depict(daf)) axis: $(axis) name: $(name) eltype: $(eltype) overwrite: $(overwrite) {"
        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))
        Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))

        if !overwrite
            require_no_vector(daf, axis, name)
        else
            delete_vector!(daf, axis, name; must_exist = false, _for_set = true)
        end

        update_caches_before_set_vector(daf, axis, name)
        return Formats.format_get_empty_dense_vector!(daf, axis, name, eltype)
    end
end

function filled_empty_dense_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    filled_vector::AbstractVector{T},
)::Nothing where {T <: StorageNumber}
    Formats.format_filled_empty_dense_vector!(daf, axis, name, filled_vector)
    @debug "empty_dense_vector! filled vector: $(depict(filled_vector)) }"
    return nothing
end

"""
    empty_sparse_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        indtype::Maybe{Type{I}} = nothing;
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber, I <: StorageInteger}

Create an empty sparse vector property with some `name` for some `axis` in `daf`, pass its parts (`nzind` and `nzval`)
to `fill`, and return the result.

If `indtype` is not specified, it is chosen automatically to be the smallest unsigned integer type needed for the
vector.

The returned vector will be uninitialized; the caller is expected to `fill` its `nzind` and `nzval` vectors with values.
Specifying the `nnz` makes their sizes known in advance, to allow pre-allocating disk data. For this reason, this does
not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse vector is created by concatenating several smaller ones; this function
allows doing so directly into the data vector, avoiding a copy in case of memory-mapped disk formats.

!!! warning

    It is the caller's responsibility to fill the two vectors with valid data. Specifically, you must ensure:

      - `nzind[1] == 1`
      - `nzind[i] <= nzind[i + 1]`
      - `nzind[end] == nnz`

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If not `overwrite` (the
default), this also verifies the `name` vector does not exist for the `axis`.
"""
function empty_sparse_vector!(  # NOLINT
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Maybe{Type{I}} = nothing;
    overwrite::Bool = false,
)::Any where {T <: StorageNumber, I <: StorageInteger}
    if indtype === nothing
        indtype = indtype_for_size(axis_length(daf, axis))
    end
    @assert isbitstype(eltype)
    @assert isbitstype(indtype)
    nzind, nzval, extra = get_empty_sparse_vector!(daf, axis, name, eltype, nnz, indtype; overwrite = overwrite)
    try
        result = fill(nzind, nzval)
        filled_empty_sparse_vector!(daf, axis, name, nzind, nzval, extra)
        return result
    finally
        end_write_lock(daf)
    end
end

function get_empty_sparse_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I};
    overwrite::Bool = false,
)::Tuple{AbstractVector{I}, AbstractVector{T}, Any} where {T <: StorageNumber, I <: StorageInteger}
    return begin_write_lock(daf) do
        @debug "empty_sparse_vector! daf: $(depict(daf)) axis: $(axis) name: $(name) eltype: $(eltype) nnz: $(nnz) indtype: $(indtype) overwrite: $(overwrite) {"
        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))

        if !overwrite
            require_no_vector(daf, axis, name)
        else
            delete_vector!(daf, axis, name; must_exist = false, _for_set = true)
        end

        update_caches_before_set_vector(daf, axis, name)
        return Formats.format_get_empty_sparse_vector!(daf, axis, name, eltype, nnz, indtype)
    end
end

function filled_empty_sparse_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    nzind::AbstractVector{I},
    nzval::AbstractVector{T},
    extra::Any,
)::Nothing where {T <: StorageNumber, I <: StorageInteger}
    filled = SparseVector(axis_length(daf, axis), nzind, nzval)
    Formats.format_filled_empty_sparse_vector!(daf, axis, name, extra, filled)
    @debug "empty_sparse_vector! filled vector: $(depict(filled)) }"
    return nothing
end

function update_caches_before_set_vector(daf::DafWriter, axis::AbstractString, name::AbstractString)::Nothing
    Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
    if Formats.format_has_vector(daf, axis, name)
        Formats.format_delete_vector!(daf, axis, name; for_set = true)
    else
        Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))
    end
    Formats.format_increment_version_counter(daf, (axis, name))
    return nothing
end

"""
    delete_vector!(
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString;
        must_exist::Bool = true,
    )::Nothing

Delete a vector property with some `name` for some `axis` from `daf`.

This first verifies the `axis` exists in `daf` and that the property name isn't `name`. If `must_exist` (the default),
this also verifies the `name` vector exists for the `axis`.
"""
function delete_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
    _for_set::Bool = false,
)::Nothing
    return with_write_lock(daf) do
        @debug "delete_vector! $daf: $(depict(daf)) axis: $(axis) name: $(name) must exist: $(must_exist)"

        require_not_name(daf, axis, name)
        require_axis(daf, axis)

        if must_exist
            require_vector(daf, axis, name)
        end

        if Formats.format_has_vector(daf, axis, name)
            Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
            Formats.invalidate_cached!(daf, Formats.vector_names_cache_key(axis))
            Formats.format_delete_vector!(daf, axis, name; for_set = _for_set)
        end

        return nothing
    end
end

function require_no_vector(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if Formats.format_has_vector(daf, axis, name)
        error("existing vector: $(name)\nfor the axis: $(axis)\nin the daf data: $(daf.name)")
    end
    return nothing
end

"""
    set_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        matrix::Union{StorageNumber, StorageMatrix};
        [overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing

Set the matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. Since this is Julia, this
should be a column-major `matrix`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

If `relayout` (the default), this will also automatically [`relayout!`](@ref) the matrix and store the result, so the
data would also be stored in row-major layout (that is, with the axes flipped), similarly to calling
[`relayout_matrix!`](@ref).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function set_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageNumber, StorageMatrix};
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    @assert isbitstype(eltype(matrix))
    return with_write_lock(daf) do
        relayout = relayout && rows_axis != columns_axis
        @debug "set_matrix! daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) matrix: $(depict(matrix)) overwrite: $(overwrite) relayout: $(relayout)"

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if matrix isa StorageMatrix
            require_column_major(matrix)
            require_axis_length(daf, "matrix rows", size(matrix, Rows), rows_axis)
            require_axis_length(daf, "matrix columns", size(matrix, Columns), columns_axis)
            if matrix isa NamedMatrix
                require_dim_name(daf, rows_axis, "matrix rows dim name", dimnames(matrix, 1); prefix = "rows")
                require_dim_name(daf, columns_axis, "matrix columns dim name", dimnames(matrix, 2); prefix = "columns")
                require_axis_names(daf, rows_axis, "row names of the: matrix", names(matrix, 1))
                require_axis_names(daf, columns_axis, "column names of the: matrix", names(matrix, 2))
            end
            matrix = base_array(matrix)
        end

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
            if relayout
                require_no_matrix(daf, columns_axis, rows_axis, name; relayout = relayout)
            end
        else
            delete_matrix!(daf, columns_axis, rows_axis, name; relayout = relayout, must_exist = false, _for_set = true)
        end

        update_caches_before_set_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_set_matrix!(daf, rows_axis, columns_axis, name, matrix)

        if relayout
            update_caches_before_set_matrix(daf, columns_axis, rows_axis, name)
            Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name)
        end

        return nothing
    end
end

"""
    empty_dense_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T};
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber}

Create an empty dense matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`, pass it to
`fill`, and return the result. Since this is Julia, this will be a column-major `matrix`.

The returned matrix will be uninitialized; the caller is expected to `fill` it with values. This saves creating a copy
of the matrix before setting it in `daf`, which makes a huge difference when creating matrices on disk (using memory
mapping). For this reason, this does not work for strings, as they do not have a fixed size.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function empty_dense_matrix!(
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber}
    @assert isbitstype(eltype)
    matrix = get_empty_dense_matrix!(daf, rows_axis, columns_axis, name, eltype; overwrite = overwrite)
    try
        result = fill(matrix)
        filled_empty_dense_matrix!(daf, rows_axis, columns_axis, name, matrix)
        return result
    finally
        end_write_lock(daf)
    end
end

function get_empty_dense_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::Any where {T <: StorageNumber}
    return begin_write_lock(daf) do
        @debug "empty_dense_matrix! daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) eltype: $(eltype) overwrite: $(overwrite) {"
        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        else
            delete_matrix!(daf, rows_axis, columns_axis, name; relayout = false, must_exist = false, _for_set = true)
        end

        update_caches_before_set_matrix(daf, rows_axis, columns_axis, name)
        return Formats.format_get_empty_dense_matrix!(daf, rows_axis, columns_axis, name, eltype)
    end
end

function filled_empty_dense_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    filled_matrix::AbstractMatrix{T},
)::Nothing where {T <: StorageNumber}
    Formats.format_filled_empty_dense_matrix!(daf, rows_axis, columns_axis, name, filled_matrix)
    @debug "empty_dense_matrix! filled matrix: $(depict(filled_matrix)) }"
    return nothing
end

"""
    empty_sparse_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{T},
        nnz::StorageInteger,
        intdype::Maybe{Type{I}} = nothing;
        [overwrite::Bool = false]
    )::Any where {T <: StorageNumber, I <: StorageInteger}

Create an empty sparse matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`, pass its parts
(`colptr`, `rowval` and `nzval`) to `fill`, and return the result.

If `indtype` is not specified, it is chosen automatically to be the smallest unsigned integer type needed for the
matrix.

The returned matrix will be uninitialized; the caller is expected to `fill` its `colptr`, `rowval` and `nzval` vectors.
Specifying the `nnz` makes their sizes known in advance, to allow pre-allocating disk space. For this reason, this does
not work for strings, as they do not have a fixed size.

This severely restricts the usefulness of this function, because typically `nnz` is only know after fully computing the
matrix. Still, in some cases a large sparse matrix is created by concatenating several smaller ones; this function
allows doing so directly into the data, avoiding a copy in case of memory-mapped disk formats.

!!! warning


It is the caller's responsibility to fill the three vectors with valid data. Specifically, you must ensure:

  - `colptr[1] == 1`
  - `colptr[end] == nnz + 1`
  - `colptr[i] <= colptr[i + 1]`
  - for all `j`, for all `i` such that `colptr[j] <= i` and `i + 1 < colptr[j + 1]`, `1 <= rowptr[i] < rowptr[i + 1] <= nrows`

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If not `overwrite` (the default), this also
verifies the `name` matrix does not exist for the `rows_axis` and `columns_axis`.
"""
function empty_sparse_matrix!(  # NOLINT
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Maybe{Type{I}} = nothing;
    overwrite::Bool = false,
)::Any where {T <: StorageNumber, I <: StorageInteger}
    if indtype === nothing
        nrows = axis_length(daf, rows_axis)
        ncolumns = axis_length(daf, columns_axis)
        indtype = indtype_for_size(max(nrows, ncolumns, nnz))
    end
    @assert isbitstype(eltype)
    @assert isbitstype(indtype)
    colptr, rowval, nzval, extra =
        get_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, eltype, nnz, indtype; overwrite = overwrite)
    try
        result = fill(colptr, rowval, nzval)
        filled_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, colptr, rowval, nzval, extra)
        return result
    finally
        end_write_lock(daf)
    end
end

function get_empty_sparse_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I};
    overwrite::Bool = false,
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}, Any} where {T <: StorageNumber, I <: StorageInteger}
    return begin_write_lock(daf) do
        @debug "empty_sparse_matrix! daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) eltype: $(eltype) overwrite: $(overwrite) {"
        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        else
            delete_matrix!(daf, rows_axis, columns_axis, name; relayout = false, must_exist = false, _for_set = true)
        end

        update_caches_before_set_matrix(daf, rows_axis, columns_axis, name)
        return Formats.format_get_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, eltype, nnz, indtype)
    end
end

function filled_empty_sparse_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    colptr::AbstractVector{I},
    rowval::AbstractVector{I},
    nzval::AbstractVector{T},
    extra::Any,
)::Nothing where {T <: StorageNumber, I <: StorageInteger}
    filled = SparseMatrixCSC(axis_length(daf, rows_axis), axis_length(daf, columns_axis), colptr, rowval, nzval)
    Formats.format_filled_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, extra, filled)
    @debug "empty_sparse_matrix! filled matrix: $(depict(filled)) }"
    return nothing
end

"""
    relayout_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [overwrite::Bool = false]
    )::Nothing

Given a matrix property with some `name` exists (in column-major layout) in `daf` for the `rows_axis` and the
`columns_axis`, then [`relayout!`](@ref) it and store the row-major result as well (that is, with flipped axes).

This is useful following calling [`empty_dense_matrix!`](@ref) or [`empty_sparse_matrix!`](@ref) to ensure both layouts
of the matrix are stored in `def`. When calling [`set_matrix!`](@ref), it is simpler to just specify (the default)
`relayout = true`.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, and that there is a `name` (column-major) matrix
property for them. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
*flipped* `rows_axis` and `columns_axis`.

!!! note

    A restriction of the way `Daf` stores data is that square data is only stored in one (column-major) layout (e.g., to
    store a weighted directed graph between cells, you may store an outgoing_weights matrix where each cell's column
    holds the outgoing weights from the cell to the other cells. In this case you **can't** ask `Daf` to relayout the
    matrix to row-major order so that each cell's row would be the incoming weights from the other cells. Instead you
    would need to explicitly store a separate incoming_weights matrix where each cell's column holds the incoming
    weights).
"""
function relayout_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    overwrite::Bool = false,
)::Nothing
    return with_write_lock(daf) do
        @debug "relayout_matrix! daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) overwrite: $(overwrite) {"

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if rows_axis == columns_axis
            error(
                "can't relayout square matrix: $(name)\n" *
                "of the axis: $(rows_axis)\n" *
                "due to daf representation limitations\n" *
                "in the daf data: $(daf.name)",
            )
        end

        require_matrix(daf, rows_axis, columns_axis, name; relayout = false)

        if !overwrite
            require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
        else
            delete_matrix!(daf, columns_axis, rows_axis, name; relayout = false, must_exist = false, _for_set = true)
        end

        update_caches_before_set_matrix(daf, columns_axis, rows_axis, name)
        Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name)

        @debug "relayout_matrix! }"
        return nothing
    end
end

function update_caches_before_set_matrix(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
    if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
    else
        Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(rows_axis, columns_axis))
        Formats.invalidate_cached!(daf, Formats.matrix_relayout_names_cache_key(rows_axis, columns_axis))
        if rows_axis != columns_axis
            Formats.invalidate_cached!(daf, Formats.matrix_relayout_names_cache_key(columns_axis, rows_axis))
        end
    end
    Formats.format_increment_version_counter(daf, (rows_axis, columns_axis, name))
    return nothing
end

"""
    delete_matrix!(
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString;
        [must_exist::Bool = true,
        relayout::Bool = true]
    )::Nothing

Delete a matrix property with some `name` for some `rows_axis` and `columns_axis` from `daf`.

If `relayout` (the default), this will also delete the matrix in the other layout (that is, with flipped axes).

This first verifies the `rows_axis` and `columns_axis` exist in `daf`. If `must_exist` (the default), this also verifies
the `name` matrix exists for the `rows_axis` and `columns_axis`.
"""
function delete_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
    relayout::Bool = true,
    _for_set::Bool = false,
)::Nothing
    return with_write_lock(daf) do
        relayout = relayout && rows_axis != columns_axis
        @debug "delete_matrix! daf: $(depict(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) must exist: $(must_exist)"

        require_axis(daf, rows_axis)
        require_axis(daf, columns_axis)

        if must_exist
            require_matrix(daf, rows_axis, columns_axis, name; relayout = relayout)
        end

        if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            update_caches_and_delete_matrix(daf, rows_axis, columns_axis, name, _for_set)
        end

        if relayout && Formats.format_has_matrix(daf, columns_axis, rows_axis, name)
            update_caches_and_delete_matrix(daf, columns_axis, rows_axis, name, _for_set)
        end

        return nothing
    end
end

function update_caches_and_delete_matrix(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    for_set::Bool,
)::Nothing
    Formats.invalidate_cached!(daf, Formats.matrix_names_cache_key(rows_axis, columns_axis))
    Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
    return Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = for_set)
end

function require_no_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
        error(
            "existing matrix: $(name)\n" *
            "for the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "in the daf data: $(daf.name)",
        )
    end
    if relayout
        require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
    end
    return nothing
end

function require_not_name(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if name == "name"
        error("setting the reserved vector: name\n" * "for the axis: $(axis)\n" * "in the daf data: $(daf.name)")
    end
    return nothing
end

end # module
