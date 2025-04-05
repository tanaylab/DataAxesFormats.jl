"""
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
using ..Readers
using ..StorageTypes
using ConcurrentUtils
using NamedArrays
using SparseArrays
using TanayLabUtilities

import ..Formats
import ..Formats.FormatWriter  # For documentation.
import ..Readers.assert_valid_matrix
import ..Readers.base_array
import ..Readers.require_axis
import ..Readers.require_axis_length
import ..Readers.require_axis_names
import ..Readers.require_column_major
import ..Readers.require_dim_name
import ..Readers.require_matrix
import ..Readers.require_scalar
import ..Readers.require_vector

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
    return Formats.with_data_write_lock(daf, "set_scalar! of:", name) do
        # Formats.assert_valid_cache(daf)
        @debug "set_scalar! daf: $(brief(daf)) name: $(name) value: $(brief(value)) overwrite: $(overwrite)"

        if !overwrite
            require_no_scalar(daf, name)
        else
            delete_scalar!(daf, name; must_exist = false, _for_set = true)
        end

        Formats.invalidate_cached!(daf, Formats.scalars_set_cache_key())
        Formats.format_set_scalar!(daf, name, value)
        Formats.cache_scalar!(daf, name, value)

        # Formats.assert_valid_cache(daf)
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
    return Formats.with_data_write_lock(daf, "delete_scalar! of:", name) do
        # Formats.assert_valid_cache(daf)
        @debug "delete_scalar! daf: $(brief(daf)) name: $(name) must exist: $(must_exist)"

        if must_exist
            require_scalar(daf, name)
        end

        if Formats.format_has_scalar(daf, name)
            Formats.invalidate_cached!(daf, Formats.scalar_cache_key(name))
            Formats.invalidate_cached!(daf, Formats.scalars_set_cache_key())
            Formats.format_delete_scalar!(daf, name; for_set = _for_set)
        end

        # Formats.assert_valid_cache(daf)
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
        entries::AbstractVector{<:AbstractString};
        overwrite::Bool = false,
    )::Nothing

Add a new `axis` to `daf`.

This verifies the `entries` are unique. If `overwrite`, this will first delete an existing axis with the same name
(which will also delete any data associated with this axis!). Otherwise, this verifies the the `axis` does not exist.
"""
function add_axis!(
    daf::DafWriter,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString};
    overwrite::Bool = false,
)::Nothing
    if overwrite
        delete_axis!(daf, axis; must_exist = false)
    end

    entries = base_array(entries)
    if issparse(entries)
        entries = Vector(entries)  # UNTESTED
    end

    return Formats.with_data_write_lock(daf, "add_axis! of:", axis) do
        # Formats.assert_valid_cache(daf)
        @debug "add_axis! daf: $(brief(daf)) axis: $(axis) entries: $(brief(entries))"

        require_no_axis(daf, axis; for_change = true)

        if !allunique(entries)
            error("non-unique entries for new axis: $(axis)\nin the daf data: $(daf.name)")
        end

        Formats.invalidate_cached!(daf, Formats.axes_set_cache_key())
        Formats.format_add_axis!(daf, axis, entries)

        # Formats.assert_valid_cache(daf)
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
    return Formats.with_data_write_lock(daf, "delete_axis! of:", axis) do
        # Formats.assert_valid_cache(daf)
        @debug "delete_axis! daf: $(brief(daf)) axis: $(axis) must exist: $(must_exist)"

        if must_exist
            require_axis(daf, "for: delete_axis!", axis; for_change = true)
        elseif !Formats.format_has_axis(daf, axis; for_change = true)
            return nothing
        end

        vectors_set = Formats.get_vectors_set_through_cache(daf, axis)
        for name in vectors_set
            Formats.format_delete_vector!(daf, axis, name; for_set = false)
            Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
        end
        Formats.invalidate_cached!(daf, Formats.vectors_set_cache_key(axis))

        axes_set = Formats.get_axes_set_through_cache(daf)
        for other_axis in axes_set
            matrices_set = Formats.get_matrices_set_through_cache(daf, axis, other_axis)
            for name in matrices_set
                Formats.format_delete_matrix!(daf, axis, other_axis, name; for_set = false)
                Formats.invalidate_cached!(daf, Formats.matrix_cache_key(axis, other_axis, name))
                if axis != other_axis
                    Formats.invalidate_cached!(daf, Formats.matrix_cache_key(other_axis, axis, name))
                end
            end

            if axis != other_axis
                matrices_set = Formats.get_matrices_set_through_cache(daf, other_axis, axis)
                for name in matrices_set
                    Formats.format_delete_matrix!(daf, other_axis, axis, name; for_set = false)
                    Formats.invalidate_cached!(daf, Formats.matrix_cache_key(other_axis, axis, name))
                    if axis != other_axis
                        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(axis, other_axis, name))
                    end
                end
                Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(other_axis, axis; relayout = false))
            end

            Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(axis, other_axis; relayout = false))
            Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(axis, other_axis; relayout = true))
        end

        Formats.invalidate_cached!(daf, Formats.axis_dict_cache_key(axis))
        Formats.invalidate_cached!(daf, Formats.axis_vector_cache_key(axis))
        Formats.invalidate_cached!(daf, Formats.axes_set_cache_key())
        Formats.format_increment_version_counter(daf, axis)

        Formats.format_delete_axis!(daf, axis)
        # Formats.assert_valid_cache(daf)
        return nothing
    end
end

function require_no_axis(daf::DafReader, axis::AbstractString; for_change::Bool = false)::Nothing
    if Formats.format_has_axis(daf, axis; for_change)
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
        [eltype::Maybe{Type{<:StorageReal}} = nothing,
        overwrite::Bool = false]
    )::Nothing

Set a vector property with some `name` for some `axis` in `daf`.

If the `vector` specified is actually a [`StorageScalar`](@ref), the stored vector is filled with this value.

This first verifies the `axis` exists in `daf`, that the property name isn't `name`, and that the `vector` has the
appropriate length. If not `overwrite` (the default), this also verifies the `name` vector does not exist for the
`axis`.

If `eltype` is specified, and the data is of another type, then the data is converted to this data type before being
stored.
"""
function set_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector};
    eltype::Maybe{Type{<:StorageReal}} = nothing,
    overwrite::Bool = false,
)::Nothing
    @assert Base.eltype(vector) <: AbstractString || isbitstype(Base.eltype(vector))
    return Formats.with_data_write_lock(daf, "set_vector! of:", name, "of:", axis) do
        # Formats.assert_valid_cache(daf)
        @debug "set_vector! daf: $(brief(daf)) axis: $(axis) name: $(name) vector: $(brief(vector)) overwrite: $(overwrite)"

        require_not_reserved(daf, axis, name)
        require_axis(daf, "for the vector: $(name)", axis)

        if vector isa StorageVector
            require_axis_length(daf, length(vector), "vector: $(name)", axis)
            if vector isa NamedVector
                require_dim_name(daf, axis, "vector dim name", dimnames(vector, 1))
                require_axis_names(daf, axis, "entry names of the: vector", names(vector, 1))
            end
            vector = base_array(vector)
            if eltype === nothing
                eltype = Base.eltype(vector)
            end
            if vector isa BitVector || Base.eltype(vector) != eltype
                if issparse(vector)
                    vector = SparseVector{eltype}(vector)
                else
                    vector = Vector{eltype}(vector)
                end
            end
        else
            @assert vector isa StorageScalar
            if eltype !== nothing
                vector = eltype(vector)
            end
        end

        if !overwrite
            require_no_vector(daf, axis, name)
        end

        update_before_set_vector(daf, axis, name)
        Formats.format_set_vector!(daf, axis, name, vector)
        # Formats.assert_valid_cache(daf)
        return nothing
    end
end

"""
    empty_dense_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{<:StorageReal};
        [overwrite::Bool = false]
    )::Any

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
    eltype::Type{<:StorageReal};
    overwrite::Bool = false,
)::Any
    @assert isbitstype(eltype)
    vector = get_empty_dense_vector!(daf, axis, name, eltype; overwrite)
    try
        result = fill(vector)
        Formats.cache_vector!(daf, axis, name, Formats.as_named_vector(daf, axis, vector))
        @debug "empty_dense_vector! filled vector: $(brief(vector)) }"
        return result
    finally
        # Formats.assert_valid_cache(daf)
        Formats.end_data_write_lock(daf, "empty_dense_vector! of:", name, "of:", axis)
    end
end

function get_empty_dense_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::AbstractVector{T} where {T <: StorageReal}
    @assert isbitstype(eltype)
    Formats.begin_data_write_lock(daf, "empty_dense_vector! of:", name, "of:", axis)
    try
        # Formats.assert_valid_cache(daf)
        @debug "empty_dense_vector! daf: $(brief(daf)) axis: $(axis) name: $(name) eltype: $(eltype) overwrite: $(overwrite) {"
        require_not_reserved(daf, axis, name)
        require_axis(daf, "for the vector: $(name)", axis)

        if !overwrite
            require_no_vector(daf, axis, name)
        end

        update_before_set_vector(daf, axis, name)
        return Formats.format_get_empty_dense_vector!(daf, axis, name, eltype)
    catch
        Formats.end_data_write_lock(daf, "empty_dense_vector! of:", name, "of:", axis)
        rethrow()
    end
end

"""
    empty_sparse_vector!(
        fill::Function,
        daf::DafWriter,
        axis::AbstractString,
        name::AbstractString,
        eltype::Type{<:StorageReal},
        nnz::StorageInteger,
        indtype::Maybe{Type{<:StorageInteger}} = nothing;
        [overwrite::Bool = false]
    )::Any

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
function empty_sparse_vector!(
    fill::Function,
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal},
    nnz::StorageInteger,
    indtype::Maybe{Type{<:StorageInteger}} = nothing;
    overwrite::Bool = false,
)::Any
    if indtype === nothing
        indtype = indtype_for_size(axis_length(daf, axis))
    end
    @assert isbitstype(eltype)
    @assert isbitstype(indtype)
    nzind, nzval = get_empty_sparse_vector!(daf, axis, name, eltype, nnz, indtype; overwrite)
    try
        result = fill(nzind, nzval)
        filled_empty_sparse_vector!(daf, axis, name, nzind, nzval)
        return result
    finally
        # Formats.assert_valid_cache(daf)
        Formats.end_data_write_lock(daf, "empty_sparse_vector! of:", name, "of:", axis)
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
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    Formats.begin_data_write_lock(daf, "empty_sparse_vector! of:", name, "of:", axis)
    try
        # Formats.assert_valid_cache(daf)
        @debug "empty_sparse_vector! daf: $(brief(daf)) axis: $(axis) name: $(name) eltype: $(eltype) nnz: $(nnz) indtype: $(indtype) overwrite: $(overwrite) {"
        require_not_reserved(daf, axis, name)
        require_axis(daf, "for the vector: $(name)", axis)

        if !overwrite
            require_no_vector(daf, axis, name)
        end

        update_before_set_vector(daf, axis, name)
        return Formats.format_get_empty_sparse_vector!(daf, axis, name, eltype, nnz, indtype)
    catch
        Formats.end_data_write_lock(daf, "empty_sparse_vector! of:", name, "of:", axis)
        rethrow()
    end
end

function filled_empty_sparse_vector!(
    daf::DafWriter,
    axis::AbstractString,
    name::AbstractString,
    nzind::AbstractVector{<:StorageInteger},
    nzval::AbstractVector{<:StorageReal},
)::Nothing
    filled_vector = SparseVector(axis_length(daf, axis), nzind, nzval)
    Formats.format_filled_empty_sparse_vector!(daf, axis, name, filled_vector)
    Formats.cache_vector!(daf, axis, name, Formats.as_named_vector(daf, axis, filled_vector))
    @debug "empty_sparse_vector! filled vector: $(brief(filled_vector)) }"
    return nothing
end

function update_before_set_vector(daf::DafWriter, axis::AbstractString, name::AbstractString)::Nothing
    Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
    if Formats.format_has_vector(daf, axis, name)
        Formats.format_delete_vector!(daf, axis, name; for_set = true)
    else
        Formats.invalidate_cached!(daf, Formats.vectors_set_cache_key(axis))
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
function delete_vector!(daf::DafWriter, axis::AbstractString, name::AbstractString; must_exist::Bool = true)::Nothing
    return Formats.with_data_write_lock(daf, "delete_vector! of:", name, "of:", axis) do
        # Formats.assert_valid_cache(daf)
        @debug "delete_vector! $daf: $(brief(daf)) axis: $(axis) name: $(name) must exist: $(must_exist)"

        require_not_reserved(daf, axis, name)
        require_axis(daf, "for the vector: $(name)", axis)

        if must_exist
            require_vector(daf, axis, name)
        end

        if Formats.format_has_vector(daf, axis, name)
            Formats.invalidate_cached!(daf, Formats.vector_cache_key(axis, name))
            Formats.invalidate_cached!(daf, Formats.vectors_set_cache_key(axis))
            Formats.format_delete_vector!(daf, axis, name; for_set = false)
        end

        # Formats.assert_valid_cache(daf)
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
        matrix::Union{StorageReal, StorageMatrix};
        [eltype::Maybe{Type{<:StorageReal}} = nothing,
        overwrite::Bool = false,
        relayout::Bool = true]
    )::Nothing

Set the matrix property with some `name` for some `rows_axis` and `columns_axis` in `daf`. Since this is Julia, this
should be a column-major `matrix`.

If the `matrix` specified is actually a [`StorageScalar`](@ref), the stored matrix is filled with this value.

If `relayout` (the default), this will also automatically `relayout!` the matrix and store the result, so the
data would also be stored in row-major layout (that is, with the axes flipped), similarly to calling
`relayout!`.

This first verifies the `rows_axis` and `columns_axis` exist in `daf`, that the `matrix` is column-major of the
appropriate size. If not `overwrite` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.
"""
function set_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageReal, StorageMatrix};
    eltype::Maybe{Type{<:StorageReal}} = nothing,
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    @assert isbitstype(Base.eltype(matrix))
    Formats.with_data_write_lock(daf, "set_matrix! of:", name, "of:", rows_axis, "and:", columns_axis) do
        # Formats.assert_valid_cache(daf)
        relayout = relayout && rows_axis != columns_axis
        @debug "set_matrix! daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) matrix: $(brief(matrix)) overwrite: $(overwrite) relayout: $(relayout)"

        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        if matrix isa StorageMatrix
            require_column_major(matrix)
            require_axis_length(daf, size(matrix, Rows), "rows of the matrix: $(name)", rows_axis)
            require_axis_length(daf, size(matrix, Columns), "columns of the matrix: $(name)", columns_axis)
            if matrix isa NamedMatrix
                require_dim_name(daf, rows_axis, "matrix rows dim name", dimnames(matrix, 1); prefix = "rows")
                require_dim_name(daf, columns_axis, "matrix columns dim name", dimnames(matrix, 2); prefix = "columns")
                require_axis_names(daf, rows_axis, "row names of the: matrix", names(matrix, 1))
                require_axis_names(daf, columns_axis, "column names of the: matrix", names(matrix, 2))
            end
            matrix = base_array(matrix)
            if eltype === nothing
                eltype = Base.eltype(matrix)
            end
            if matrix isa BitMatrix || Base.eltype(matrix) != eltype
                if issparse(matrix)
                    matrix = SparseMatrixCSC{eltype}(matrix)
                else
                    matrix = Matrix{eltype}(matrix)
                end
            end
        else
            @assert matrix isa StorageReal
            if eltype !== nothing
                matrix = eltype(matrix)
            end
        end

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout)
        end

        update_before_set_matrix(daf, rows_axis, columns_axis, name)
        Formats.format_set_matrix!(daf, rows_axis, columns_axis, name, matrix)

        if relayout
            update_before_set_matrix(daf, columns_axis, rows_axis, name)
            Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name, matrix)
        end
        # Formats.assert_valid_cache(daf)
    end

    return nothing
end

"""
    empty_dense_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{<:StorageReal};
        [overwrite::Bool = false]
    )::Any

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
    eltype::Type{<:StorageReal};
    overwrite::Bool = false,
)::Any
    @assert isbitstype(eltype)
    matrix = get_empty_dense_matrix!(daf, rows_axis, columns_axis, name, eltype; overwrite)
    try
        result = fill(matrix)
        Formats.cache_matrix!(
            daf,
            rows_axis,
            columns_axis,
            name,
            Formats.as_named_matrix(daf, rows_axis, columns_axis, matrix),
        )
        @debug "empty_dense_matrix! filled matrix: $(brief(matrix)) }"
        return result
    finally
        # Formats.assert_valid_cache(daf)
        Formats.end_data_write_lock(daf, "empty_dense_matrix! of:", name, "of:", rows_axis, "and:", columns_axis)
    end
end

function get_empty_dense_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal};
    overwrite::Bool = false,
)::Any
    Formats.begin_data_write_lock(daf, "empty_dense_matrix! of:", name, "of:", rows_axis, "and:", columns_axis)
    try
        # Formats.assert_valid_cache(daf)
        @debug "empty_dense_matrix! daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) eltype: $(eltype) overwrite: $(overwrite) {"
        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        end

        update_before_set_matrix(daf, rows_axis, columns_axis, name)
        return Formats.format_get_empty_dense_matrix!(daf, rows_axis, columns_axis, name, eltype)
    catch
        Formats.end_data_write_lock(daf, "empty_dense_matrix! of:", name, "of:", rows_axis, "and:", columns_axis)
        rethrow()
    end
end

"""
    empty_sparse_matrix!(
        fill::Function,
        daf::DafWriter,
        rows_axis::AbstractString,
        columns_axis::AbstractString,
        name::AbstractString,
        eltype::Type{<:StorageReal},
        nnz::StorageInteger,
        intdype::Maybe{Type{<:StorageInteger}} = nothing;
        [overwrite::Bool = false]
    )::Any

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
function empty_sparse_matrix!(
    fill::Function,
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal},
    nnz::StorageInteger,
    indtype::Maybe{Type{<:StorageInteger}} = nothing;
    overwrite::Bool = false,
)::Any
    if indtype === nothing
        nrows = axis_length(daf, rows_axis)
        ncolumns = axis_length(daf, columns_axis)
        indtype = indtype_for_size(max(nrows, ncolumns, nnz))
    end
    @assert isbitstype(eltype)
    @assert isbitstype(indtype)
    colptr, rowval, nzval =
        get_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, eltype, nnz, indtype; overwrite)
    try
        result = fill(colptr, rowval, nzval)
        filled_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, colptr, rowval, nzval)
        return result
    finally
        # Formats.assert_valid_cache(daf)
        Formats.end_data_write_lock(daf, "empty_sparse_matrix! of:", name, "of:", rows_axis, "and:", columns_axis)
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
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    Formats.begin_data_write_lock(daf, "empty_sparse_matrix! of:", name, "of:", rows_axis, "and:", columns_axis)
    try
        # Formats.assert_valid_cache(daf)
        @debug "empty_sparse_matrix! daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) eltype: $(eltype) overwrite: $(overwrite) {"
        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        if !overwrite
            require_no_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        end

        update_before_set_matrix(daf, rows_axis, columns_axis, name)
        return Formats.format_get_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, eltype, nnz, indtype)
    catch
        Formats.end_data_write_lock(daf, "empty_sparse_matrix! of:", name, "of:", rows_axis, "and:", columns_axis)
        rethrow()
    end
end

function filled_empty_sparse_matrix!(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    colptr::AbstractVector{I},
    rowval::AbstractVector{I},
    nzval::AbstractVector{<:StorageReal},
)::Nothing where {I <: StorageInteger}
    filled_matrix = SparseMatrixCSC(axis_length(daf, rows_axis), axis_length(daf, columns_axis), colptr, rowval, nzval)
    Formats.format_filled_empty_sparse_matrix!(daf, rows_axis, columns_axis, name, filled_matrix)
    Formats.cache_matrix!(
        daf,
        rows_axis,
        columns_axis,
        name,
        Formats.as_named_matrix(daf, rows_axis, columns_axis, filled_matrix),
    )
    @debug "empty_sparse_matrix! filled matrix: $(brief(filled_matrix)) }"
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
`columns_axis`, then `relayout!` it and store the row-major result as well (that is, with flipped axes).

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
    Formats.with_data_write_lock(daf, "relayout_matrix! of:", name, "of:", rows_axis, "and:", columns_axis) do
        # Formats.assert_valid_cache(daf)
        @debug "relayout_matrix! daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) overwrite: $(overwrite) {"

        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        if rows_axis == columns_axis
            error("""
                can't relayout square matrix: $(name)
                of the axis: $(rows_axis)
                due to daf representation limitations
                in the daf data: $(daf.name)
                """)
        end

        require_matrix(daf, rows_axis, columns_axis, name; relayout = false)
        if !overwrite
            require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
        end

        matrix = Formats.get_matrix_through_cache(daf, rows_axis, columns_axis, name)
        assert_valid_matrix(daf, rows_axis, columns_axis, name, matrix)

        update_before_set_matrix(daf, columns_axis, rows_axis, name)
        Formats.format_relayout_matrix!(daf, rows_axis, columns_axis, name, matrix.array)

        @debug "relayout_matrix! }"
        # Formats.assert_valid_cache(daf)
    end
    return nothing
end

function update_before_set_matrix(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    if Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
        if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = true)
        end
    else
        Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(rows_axis, columns_axis; relayout = false))
        Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(rows_axis, columns_axis; relayout = true))
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
)::Nothing
    Formats.with_data_write_lock(daf, "delete_matrix! of:", name, "of:", rows_axis, "and:", columns_axis) do
        # Formats.assert_valid_cache(daf)
        relayout = relayout && rows_axis != columns_axis
        @debug "delete_matrix! daf: $(brief(daf)) rows_axis: $(rows_axis) columns_axis: $(columns_axis) name: $(name) must exist: $(must_exist)"

        require_axis(daf, "for the rows of the matrix: $(name)", rows_axis)
        require_axis(daf, "for the columns of the matrix: $(name)", columns_axis)

        if must_exist
            require_matrix(daf, rows_axis, columns_axis, name; relayout)
        end

        update_caches_and_delete_matrix(daf, rows_axis, columns_axis, name)
        if relayout
            update_caches_and_delete_matrix(daf, columns_axis, rows_axis, name)
        end
        # Formats.assert_valid_cache(daf)
    end
    return nothing
end

function update_caches_and_delete_matrix(
    daf::DafWriter,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::Nothing
    if Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
        Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(rows_axis, columns_axis; relayout = true))
        Formats.invalidate_cached!(daf, Formats.matrices_set_cache_key(rows_axis, columns_axis; relayout = false))
        Formats.invalidate_cached!(daf, Formats.matrix_cache_key(rows_axis, columns_axis, name))
        if Formats.format_has_matrix(daf, rows_axis, columns_axis, name)
            Formats.format_delete_matrix!(daf, rows_axis, columns_axis, name; for_set = false)
        end
    end
    return nothing
end

function require_no_matrix(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    @assert Formats.has_data_read_lock(daf)
    if Formats.format_has_cached_matrix(daf, rows_axis, columns_axis, name)
        error("""
            existing matrix: $(name)
            for the rows axis: $(rows_axis)
            and the columns axis: $(columns_axis)
            in the daf data: $(daf.name)
            """)
    end
    if relayout
        require_no_matrix(daf, columns_axis, rows_axis, name; relayout = false)
    end
    return nothing
end

function require_not_reserved(daf::DafReader, axis::AbstractString, name::AbstractString)::Nothing
    if name == "name" || name == "index"
        error("""
            setting the reserved vector: $(name)
            for the axis: $(axis)
            in the daf data: $(daf.name)
            """)
    end
    return nothing
end

end # module
