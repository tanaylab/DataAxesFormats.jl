"""
Storage objects provide low-level API for storing data in specific formats. To extend `Daf` to support an additional
format, create a new concrete implementation of [`AbstractStorage`](@ref) for that format.

A storage object contains some named scalar data, a set of axes (each with a unique name for each entry), and named
vector and matrix data based on these axes.

Data is identified by its unique name given the axes it is based on. That is, there is a separate namespace for scalar,
vectors for each specific axis, and matrices for each specific pair of axes.

For matrices, we keep careful track of their layout, that is, which axis is "major" and which is "minor". A storage will
only list matrix data under the axis order that describes its layout. In theory a storage may hold two copies of the
same matrix in both possible memory layouts, in which case it will be listed twice, under both axes orders.

In general, storage objects are as "dumb" as possible, to make it easier to create new storage format adapters. That is,
storage objects are just glorified key-value repositories, with the absolutely minimal necessary logic to deal with the
separate namespaces listed above. Most of this logic is common to all storage formats, and is contained in the base
functions so the concrete storage format adapters are even simpler.

The storage API is therefore very low-level, and is not suitable for use from outside the package, except for
constructing storage objects to be used by the higher-level `Container` API.
"""
module Storage

export AbstractStorage
export add_axis!
export axis_length
export axis_names
export delete_axis!
export delete_matrix!
export delete_scalar!
export delete_vector!
export freeze
export get_axis
export get_matrix
export get_scalar
export get_vector
export has_axis
export has_matrix
export has_scalar
export has_vector
export is_frozen
export matrix_names
export scalar_names
export set_matrix!
export set_scalar!
export set_vector!
export unfreeze
export unsafe_add_axis!
export unsafe_axis_length
export unsafe_delete_axis!
export unsafe_delete_matrix!
export unsafe_delete_scalar!
export unsafe_delete_vector!
export unsafe_get_axis
export unsafe_get_matrix
export unsafe_get_scalar
export unsafe_get_vector
export unsafe_has_matrix
export unsafe_has_vector
export unsafe_matrix_names
export unsafe_set_matrix!
export unsafe_set_scalar!
export unsafe_set_vector!
export unsafe_vector_names
export vector_names

using Daf.MatrixLayouts
using Daf.Messages

"""
An abstract interface for all `Daf` storage formats.
"""
abstract type AbstractStorage end

"""
    is_frozen(storage::AbstractStorage)::Bool

Whether the storage supports only read-only access.
"""
function is_frozen(storage::AbstractStorage)::Bool
    return error("missing method: is_frozen for storage type: $(typeof(storage))")  # untested
end

"""
    freeze(storage::AbstractStorage)::Nothing

Prevent modifications of the storage.
"""
function freeze(storage::AbstractStorage)::Nothing
    return error("missing method: freeze for storage type: $(typeof(storage))")  # untested
end

"""
    unfreeze(storage::AbstractStorage)::Nothing

Allow modifications of the storage.
"""
function unfreeze(storage::AbstractStorage)::Nothing
    return error("missing method: unfreeze for storage type: $(typeof(storage))")  # untested
end

function require_unfrozen(storage::AbstractStorage)::Nothing
    if is_frozen(storage)
        error("frozen storage: $(storage.name)")
    end
end

"""
    has_scalar(storage::AbstractStorage, name::String)::Bool

Check whether a scalar with some `name` exists in the `storage`.
"""
function has_scalar(storage::AbstractStorage, name::String)::Bool
    return error("missing method: has_scalar for storage type: $(typeof(storage))")  # untested
end

"""
    set_scalar!(
        storage::AbstractStorage,
        name::String,
        value::Any;
        overwrite::Bool = false,
    )::Nothing

Set the `value` of a scalar with some `name` in the `storage`.

If `overwrite` is `false` (the default), this first verifies the `name` does not exist.

This will cause an error if the `storage` [`is_frozen`](@ref).
"""
function set_scalar!(storage::AbstractStorage, name::String, value::Any; overwrite::Bool = false)::Nothing
    require_unfrozen(storage)
    if !overwrite
        require_no_scalar(storage, name)
    end
    unsafe_set_scalar!(storage, name, value)
    return nothing
end

"""
    unsafe_set_scalar!(
        storage::AbstractStorage,
        name::String,
        value::Any,
    )::Nothing

Implement setting the `value` of a scalar with some `name` in the `storage`.

This will silently overwrite an existing `value` for the same `name` scalar.
"""
function unsafe_set_scalar!(storage::AbstractStorage, name::String, value::Any)::Nothing
    return error("missing method: unsafe_set_scalar! for storage type: $(typeof(storage))")  # untested
end

"""
    delete_scalar!(
        storage::AbstractStorage,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing scalar with some `name` from the `storage`.

If `must_exist` is `true` (the default), this first verifies the `name` scalar exists in the `storage`.
"""
function delete_scalar!(storage::AbstractStorage, name::String; must_exist::Bool = true)::Nothing
    require_unfrozen(storage)
    if must_exist
        require_scalar(storage, name)
    elseif !has_scalar(storage, name)
        return nothing
    end
    unsafe_delete_scalar!(storage, name)
    return nothing
end

"""
    unsafe_delete_scalar!(storage::AbstractStorage, name::String)::Nothing

Implement deleting a scalar with some `name` from `storage`.

This trusts that the `name` scalar exists in the `storage`.
"""
function unsafe_delete_scalar!(storage::AbstractStorage, name::String)::Nothing
    return error("missing method: unsafe_delete_scalar! for storage type: $(typeof(storage))")  # untested
end

"""
    scalar_names(storage::AbstractStorage)::Set{String}

The names of the scalars in the `storage`.
"""
function scalar_names(storage::AbstractStorage)::AbstractSet{String}
    return error("missing method: scalar_names for storage type: $(typeof(storage))")  # untested
end

struct NoDefault end
NO_DEFAULT = NoDefault()

"""
    get_scalar(storage::AbstractStorage, name::String[; default::Any])::Any

The value of a scalar with some `name` in the `storage`.

If `default` is not specified, this first verifies the `name` scalar exists in the `storage`.
"""
function get_scalar(storage::AbstractStorage, name::String; default::Any = NO_DEFAULT)::Any
    if default === NO_DEFAULT
        require_scalar(storage, name)
    elseif !has_scalar(storage, name)
        return default
    end
    return unsafe_get_scalar(storage, name)
end

"""
    unsafe_get_scalar(storage::AbstractStorage, name::String)::Any

Implement fetching the value of a scalar with some `name` in the `storage`.

This trusts the `name` scalar exists in the `storage`.
"""
function unsafe_get_scalar(storage::AbstractStorage, name::String)::Any
    return error("missing method: unsafe_get_scalar for storage type: $(typeof(storage))")  # untested
end

function require_scalar(storage::AbstractStorage, name::String)::Nothing
    if !has_scalar(storage, name)
        error("missing scalar: $(name) in the storage: $(storage.name)")
    end
    return nothing
end

function require_no_scalar(storage::AbstractStorage, name::String)::Nothing
    if has_scalar(storage, name)
        error("existing scalar: $(name) in the storage: $(storage.name)")
    end
    return nothing
end

"""
    has_axis(storage::AbstractStorage, axis::String)::Bool

Check whether some `axis` exists in the `storage`.
"""
function has_axis(storage::AbstractStorage, axis::String)::Bool
    return error("missing method: has_axis for storage type: $(typeof(storage))")  # untested
end

"""
    add_axis!(
        storage::AbstractStorage,
        axis::String,
        entries::DenseVector{String}
    )::Nothing

Add a new `axis` to the `storage`.

This first verifies the `axis` does not exist and that the `entries` are unique.
"""
function add_axis!(storage::AbstractStorage, axis::String, entries::DenseVector{String})::Nothing
    require_unfrozen(storage)
    require_no_axis(storage, axis)

    if !allunique(entries)
        error("non-unique entries for new axis: $(axis) in the storage: $(storage.name)")
    end

    unsafe_add_axis!(storage, axis, entries)
    return nothing
end

"""
    unsafe_add_axis!(
        storage::AbstractStorage,
        axis::String,
        entries::DenseVector{String}
    )::Nothing

Implement adding a new `axis` to `storage`.

This trusts that the `axis` does not already exist in the `storage`, and that the names of the `entries` are unique.
"""
function unsafe_add_axis!(storage::AbstractStorage, axis::String, entries::DenseVector{String})::Nothing
    return error("missing method: unsafe_add_axis! for storage type: $(typeof(storage))")  # untested
end

"""
    delete_axis!(
        storage::AbstractStorage,
        axis::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing `axis` from the `storage`. This will also delete any vector or matrix data that is based on this
axis.

If `must_exist` is `true` (the default), this first verifies the `axis` exists in the `storage`.
"""
function delete_axis!(storage::AbstractStorage, axis::String; must_exist::Bool = true)::Nothing
    require_unfrozen(storage)
    if must_exist
        require_axis(storage, axis)
    elseif !has_axis(storage, axis)
        return nothing
    end
    for name in vector_names(storage, axis)
        unsafe_delete_vector!(storage, axis, name)
    end
    for other_axis in axis_names(storage)
        for name in matrix_names(storage, axis, other_axis)
            unsafe_delete_matrix!(storage, axis, other_axis, name)
        end
    end
    unsafe_delete_axis!(storage, axis)
    return nothing
end

"""
    unsafe_delete_axis!(
        storage::AbstractStorage,
        axis::String,
    )::Nothing

Implement deleting some `axis` from `storage`.

This trusts that the `axis` exists in the `storage`, and that all data that is based on this axis has already been
deleted.
"""
function unsafe_delete_axis!(storage::AbstractStorage, axis::String)::Nothing
    return error("missing method: unsafe_delete_axis! for storage type: $(typeof(storage))")  # untested
end

"""
    axis_names(storage::AbstractStorage)::AbstractSet{String}

The names of the axes of the `storage`.
"""
function axis_names(storage::AbstractStorage)::AbstractSet{String}
    return error("missing method: axis_names for storage type: $(typeof(storage))")  # untested
end

"""
    get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}

The unique names of the entries of some `axis` of the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}
    require_axis(storage, axis)
    return unsafe_get_axis(storage, axis)
end

"""
    unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}

Implement fetching the unique names of the entries of some `axis` of the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_get_axis(storage::AbstractStorage, axis::String)::DenseVector{String}
    return error("missing method: unsafe_get_axis for storage type: $(typeof(storage))")  # untested
end

"""
    axis_length(storage::AbstractStorage, axis::String)::Int64

The number of entries along the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function axis_length(storage::AbstractStorage, axis::String)::Int64
    require_axis(storage, axis)
    return unsafe_axis_length(storage, axis)
end

"""
    unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64

Implement fetching the number of entries along the `axis`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_axis_length(storage::AbstractStorage, axis::String)::Int64
    return error("missing method: unsafe_axis_length for storage type: $(typeof(storage))")  # untested
end

function require_axis(storage::AbstractStorage, axis::String)::Nothing
    if !has_axis(storage, axis)
        error("missing axis: $(axis) in the storage: $(storage.name)")
    end
    return nothing
end

function require_no_axis(storage::AbstractStorage, axis::String)::Nothing
    if has_axis(storage, axis)
        error("existing axis: $(axis) in the storage: $(storage.name)")
    end
    return nothing
end

"""
    has_vector(storage::AbstractStorage, axis::String, name::String)::Bool

Check whether a vector with some `name` exists for the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function has_vector(storage::AbstractStorage, axis::String, name::String)::Bool
    require_axis(storage, axis)
    return unsafe_has_vector(storage, axis, name)
end

"""
    has_vector(storage::AbstractStorage, axis::String, name::String)::Bool

Implement checking whether a vector with some `name` exists for the `axis` in the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_has_vector(storage::AbstractStorage, axis::String, name::String)::Bool
    return error("missing method: unsafe_has_vector for storage type: $(typeof(storage))")  # untested
end

"""
    set_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        vector::Union{Number, String, AbstractVector};
        overwrite::Bool = false,
    )::Nothing

Set a `vector` with some `name` for some `axis` in the `storage`.

If the `vector` specified is actually a number or a string, the stored vector is filled with this value.

This first verifies the `axis` exists in the `storage`, and that the `vector` has the appropriate length. If `overwrite`
is `false` (the default), this also verifies the `name` vector does not exist for the `axis`.

This will cause an error if the `storage` [`is_frozen`](@ref).
"""
function set_vector!(
    storage::AbstractStorage,
    axis::String,
    name::String,
    vector::Union{Number, String, AbstractVector};
    overwrite::Bool = false,
)::Nothing
    require_unfrozen(storage)
    require_axis(storage, axis)
    if vector isa AbstractVector && length(vector) != axis_length(storage, axis)
        error(
            "vector length: $(length(vector)) " *  # only seems untested
            "is different from axis: $(axis) " *  # only seems untested
            "length: $(axis_length(storage, axis)) " *  # only seems untested
            "in the storage: $(storage.name)",  # only seems untested
        )
    end
    if !overwrite
        require_no_vector(storage, axis, name)
    end
    unsafe_set_vector!(storage, axis, name, vector)
    return nothing
end

"""
    unsafe_set_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String,
        vector::Union{Number, String, AbstractVector},
    )::Nothing

Implement setting a `vector` with some `name` for some `axis` in the `storage`.

If the `vector` specified is actually a number or a string, the stored vector is filled with this value.

This trusts the `axis` exists in the `storage`, and that the `vector` has the appropriate length. It will silently
overwrite an existing vector for the same `name` for the `axis`.
"""
function unsafe_set_vector!(
    storage::AbstractStorage,
    axis::String,
    name::String,
    vector::Union{Number, String, AbstractVector},
)::Nothing
    return error("missing method: unsafe_set_vector! for storage type: $(typeof(storage))")  # untested
end

"""
    delete_vector!(
        storage::AbstractStorage,
        axis::String,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing vector with some `name` for some `axis` from the `storage`.

This first verifies the `axis` exists in the `storage`. If `must_exist` is `true` (the default), this also verifies the
`name` vector exists for the `axis`.
"""
function delete_vector!(storage::AbstractStorage, axis::String, name::String; must_exist::Bool = true)::Nothing
    require_unfrozen(storage)
    require_axis(storage, axis)
    if must_exist
        require_vector(storage, axis, name)
    elseif !has_vector(storage, axis, name)
        return nothing
    end
    unsafe_delete_vector!(storage, axis, name)
    return nothing
end

"""
    unsafe_delete_vector!(storage::AbstractStorage, axis::String, name::String)::Nothing

Implement deleting a vector with some `name` for some `axis` from `storage`.

This trusts the `axis` exists in the `storage`, and that the `name` vector exists for the `axis`.
"""
function unsafe_delete_vector!(storage::AbstractStorage, axis::String, name::String)::Nothing
    return error("missing method: unsafe_delete_vector! for storage type: $(typeof(storage))")  # untested
end

"""
    vector_names(storage::AbstractStorage, axis::String)::Set{String}

The names of the vectors for the `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
"""
function vector_names(storage::AbstractStorage, axis::String)::AbstractSet{String}
    require_axis(storage, axis)
    return unsafe_vector_names(storage, axis)
end

"""
    unsafe_vector_names(storage::AbstractStorage, axis::String)::Set{String}

Implement fetching the names of the vectors for the `axis` in the `storage`.

This trusts the `axis` exists in the `storage`.
"""
function unsafe_vector_names(storage::AbstractStorage, axis::String)::AbstractSet{String}
    return error("missing method: unsafe_vector_names for storage type: $(typeof(storage))")  # untested
end

"""
    get_vector(
        storage::AbstractStorage,
        axis::String,
        name::String
        [; default::Union{Number, String, AbstractVector}]
    )::AbstractVector

The vector with some `name` for some `axis` in the `storage`.

This first verifies the `axis` exists in the `storage`.
If `default` is not specified, this first verifies the `name` vector exists in the `storage`. Otherwise, if `default` is
an `AbstractVector`, it has to be of the same size as the `axis`, and is returned. Otherwise, a new `Vector` is created
of the correct size containing the `default`, and is returned.
"""
function get_vector(
    storage::AbstractStorage,
    axis::String,
    name::String;
    default::Union{Number, String, AbstractVector, NoDefault} = NO_DEFAULT,
)::AbstractVector
    require_axis(storage, axis)
    if default isa AbstractVector && length(default) != axis_length(storage, axis)
        error(
            "default length: $(length(default)) " *  # only seems untested
            "is different from axis: $(axis) " *  # only seems untested
            "length: $(axis_length(storage, axis)) " *  # only seems untested
            "in the storage: $(storage.name)",  # only seems untested
        )
    end
    if default === NO_DEFAULT
        require_vector(storage, axis, name)
    elseif !has_vector(storage, axis, name)
        if default isa AbstractVector
            return default
        else
            return fill(default, axis_length(storage, axis))
        end
    end
    vector = unsafe_get_vector(storage, axis, name)
    if length(vector) != axis_length(storage, axis)
        error(  # untested
            "unsafe_get_vector for: $(typeof(storage)) " *  # untested
            "returned vector length: $(length(vector)) " *  # untested
            "instead of axis: $(axis) " *  # untested
            "length: $(axis_length(storage, axis)) " *  # untested
            "in the storage: $(storage.name)",  # untested
        )
    end
    return vector
end

"""
    unsafe_get_vector(storage::AbstractStorage, axis::String, name::String)::AbstractVector

Implement fetching the vector with some `name` for some `axis` in the `storage`.

This trusts the `axis` exists in the `storage`, and the `name` vector exists for the `axis`.
"""
function unsafe_get_vector(storage::AbstractStorage, name::String)::AbstractVector
    return error("missing method: unsafe_get_vector for storage type: $(typeof(storage))")  # untested
end

function require_vector(storage::AbstractStorage, axis::String, name::String)::Nothing
    if !has_vector(storage, axis, name)
        error("missing vector: $(name) for the axis: $(axis) in the storage: $(storage.name)")
    end
    return nothing
end

function require_no_vector(storage::AbstractStorage, axis::String, name::String)::Nothing
    if has_vector(storage, axis, name)
        error("existing vector: $(name) for the axis: $(axis) in the storage: $(storage.name)")
    end
    return nothing
end

"""
    has_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
    )::Bool

Check whether a matrix with some `name` exists for the `rows_axis` and the `columns_axis` in the `storage`. Since this
is Julia, this means a column-major matrix. A storage may contain two copies of the same data, in which case it would
report the matrix under both axis orders.

This first verifies the `rows_axis` and `columns_axis` exists in the `storage`.
"""
function has_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Bool
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    return unsafe_has_matrix(storage, rows_axis, columns_axis, name)
end

"""
    has_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
    )::Bool

Implement checking whether a matrix with some `name` exists for the `rows_axis` and the `columns_axis` in the `storage`.

This trusts the `rows_axis` and the `columns_axis` exist in the `storage`.
"""
function unsafe_has_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Bool
    return error("missing method: unsafe_has_matrix for storage type: $(typeof(storage))")  # untested
end

"""
    set_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        matrix::AbstractMatrix;
        overwrite::Bool = false,
    )::Nothing

Set the `matrix` with some `name` for some `rows_axis` and `columns_axis` in the `storage`. Since this is Julia, this
should be a column-major `matrix`.

If the `matrix` specified is actually a number or a string, the stored matrix is filled with this value.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`, that the `matrix` is column-major of the
appropriate size. If `overwrite` is `false` (the default), this also verifies the `name` matrix does not exist for the
`rows_axis` and `columns_axis`.

This will cause an error if the `storage` [`is_frozen`](@ref).
"""
function set_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    matrix::Union{Number, String, AbstractMatrix};
    overwrite::Bool = false,
)::Nothing
    require_unfrozen(storage)
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    if matrix isa AbstractMatrix
        if major_axis(matrix) != Column
            error("matrix: $(typeof(matrix)) is not column-major")  # untested
        end
        if nrows(matrix) != axis_length(storage, rows_axis)
            error(
                "matrix rows: $(nrows(matrix)) " *  # only seems untested
                "is different from axis: $(rows_axis) " *  # only seems untested
                "length: $(axis_length(storage, rows_axis)) " *  # only seems untested
                "in the storage: $(storage.name)",  # only seems untested
            )
        end
        if ncolumns(matrix) != axis_length(storage, columns_axis)
            error(
                "matrix columns: $(ncolumns(matrix)) " *  # only seems untested
                "is different from axis: $(columns_axis) " *  # only seems untested
                "length: $(axis_length(storage, columns_axis)) " *  # only seems untested
                "in the storage: $(storage.name)",  # only seems untested
            )
        end
    end
    if !overwrite
        require_no_matrix(storage, rows_axis, columns_axis, name)
    end
    unsafe_set_matrix!(storage, rows_axis, columns_axis, name, matrix)
    return nothing
end

"""
    unsafe_set_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String,
        matrix::AbstractMatrix,
    )::Nothing

Implement setting the `matrix` with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

If the `matrix` specified is actually a number or a string, the stored matrix is filled with this value.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and that the `matrix` is column-major of the
appropriate size. It will silently overwrite an existing matrix for the same `name` for the `rows_axis` and
`columns_axis`.
"""
function unsafe_set_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
    matrix::Union{Number, String, AbstractMatrix},
)::Nothing
    return error("missing method: unsafe_set_matrix! for storage type: $(typeof(storage))")  # untested
end

"""
    delete_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String;
        must_exist::Bool = true,
    )::Nothing

Delete an existing matrix with some `name` for some `rows_axis` and `columns_axis` from the `storage`.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`. If `must_exist` is `true` (the default),
this also verifies the `name` matrix exists for the `rows_axis` and `columns_axis`.
"""
function delete_matrix!(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String;
    must_exist::Bool = true,
)::Nothing
    require_unfrozen(storage)
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    if must_exist
        require_matrix(storage, rows_axis, columns_axis, name)
    elseif !has_matrix(storage, rows_axis, columns_axis, name)
        return nothing
    end
    unsafe_delete_matrix!(storage, rows_axis, columns_axis, name)
    return nothing
end

"""
    unsafe_delete_matrix!(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String
    )::Nothing

Implement deleting a matrix with some `name` for some `rows_axis` and `columns_axis` from `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and that the `name` matrix exists for the
`rows_axis` and `columns_axis`.
"""
function unsafe_delete_matrix!(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Nothing
    return error("missing method: unsafe_delete_matrix! for storage type: $(typeof(storage))")  # untested
end

"""
    matrix_names(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
    )::Set{String}

The names of the matrices for the `rows_axis` and `columns_axis` in the `storage`.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`.
"""
function matrix_names(storage::AbstractStorage, rows_axis::String, columns_axis::String)::AbstractSet{String}
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    return unsafe_matrix_names(storage, rows_axis, columns_axis)
end

"""
    unsafe_matrix_names(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
    )::Set{String}

Implement fetching the names of the matrices for the `rows_axis` and `columns_axis` in the `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`.
"""
function unsafe_matrix_names(storage::AbstractStorage, rows_axis::String, columns_axis::String)::AbstractSet{String}
    return error("missing method: unsafe_matrix_names for storage type: $(typeof(storage))")  # untested
end

function require_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Nothing
    if !has_matrix(storage, rows_axis, columns_axis, name)
        error(
            "missing matrix: $(name) " *  # only seems untested
            "for the rows axis: $(rows_axis) " *  # only seems untested
            "and the columns axis: $(columns_axis) " *  # only seems untested
            "in the storage: $(storage.name)",  # only seems untested
        )
    end
    return nothing
end

function require_no_matrix(storage::AbstractStorage, rows_axis::String, columns_axis::String, name::String)::Nothing
    if has_matrix(storage, rows_axis, columns_axis, name)
        error(
            "existing matrix: $(name) " *  # only seems untested
            "for the rows axis: $(rows_axis) " *  # only seems untested
            "and the columns axis: $(columns_axis) " *  # only seems untested
            "in the storage: $(storage.name)",  # only seems untested
        )
    end
    return nothing
end

"""
    get_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String
        [; default::Union{Number, String, AbstractMatrix}]
    )::AbstractMatrix

The matrix with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This first verifies the `rows_axis` and `columns_axis` exist in the `storage`. If `default` is not specified, this first
verifies the `name` matrix exists in the `storage`. Otherwise, if `default` is an `AbstractMatrix`, it has to be of the
same size as the `rows_axis` and `columns_axis`, and is returned. Otherwise, a new `Matrix` is created of the correct
size containing the `default`, and is returned.
"""
function get_matrix(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String;
    default::Union{Number, String, NoDefault, AbstractMatrix} = NO_DEFAULT,
)::AbstractMatrix
    require_axis(storage, rows_axis)
    require_axis(storage, columns_axis)
    if default isa AbstractMatrix
        if major_axis(default) != Column
            error("default matrix: $(typeof(default)) is not column-major")  # untested
        end
        if nrows(default) != axis_length(storage, rows_axis)
            error(
                "default rows: $(nrows(default)) " *  # only seems untested
                "is different from axis: $(rows_axis) " *  # only seems untested
                "length: $(axis_length(storage, rows_axis)) " *  # only seems untested
                "in the storage: $(storage.name)",  # only seems untested
            )
        end
        if ncolumns(default) != axis_length(storage, columns_axis)
            error(
                "default columns: $(ncolumns(default)) " *  # only seems untested
                "is different from axis: $(columns_axis) " *  # only seems untested
                "length: $(axis_length(storage, columns_axis)) " *  # only seems untested
                "in the storage: $(storage.name)",  # only seems untested
            )
        end
    end
    if default === NO_DEFAULT
        require_matrix(storage, rows_axis, columns_axis, name)
    elseif !has_matrix(storage, rows_axis, columns_axis, name)
        if default isa AbstractMatrix
            return default
        else
            return fill(default, axis_length(storage, rows_axis), axis_length(storage, columns_axis))
        end
    end
    matrix = unsafe_get_matrix(storage, rows_axis, columns_axis, name)
    if nrows(matrix) != axis_length(storage, rows_axis)
        error(  # untested
            "unsafe_get_matrix for: $(typeof(storage)) " *  # untested
            "returned matrix rows: $(nrows(matrix)) " *  # untested
            "instead of axis: $(axis) " *  # untested
            "length: $(axis_length(storage, rows_axis)) " *  # untested
            "in the storage: $(storage.name)",  # untested
        )
    end
    if ncolumns(matrix) != axis_length(storage, columns_axis)
        error(  # untested
            "unsafe_get_matrix for: $(typeof(storage)) " *  # untested
            "returned matrix columns: $(ncolumns(matrix)) " *  # untested
            "instead of axis: $(axis) " *  # untested
            "length: $(axis_length(storage, columns_axis)) " *  # untested
            "in the storage: $(storage.name)",  # untested
        )
    end
    if major_axis(matrix) != Column
        error(  # untested
            "unsafe_get_matrix for: $(typeof(storage)) " *  # untested
            "returned non column-major matrix: $(typeof(matrix)) ",  # untested
        )
    end
    return matrix
end

"""
    unsafe_get_matrix(
        storage::AbstractStorage,
        rows_axis::String,
        columns_axis::String,
        name::String
    )::AbstractMatrix

Implement fetching the matrix with some `name` for some `rows_axis` and `columns_axis` in the `storage`.

This trusts the `rows_axis` and `columns_axis` exist in the `storage`, and the `name` matrix exists for the `rows_axis`
and `columns_axis`.
"""
function unsafe_get_matrix(
    storage::AbstractStorage,
    rows_axis::String,
    columns_axis::String,
    name::String,
)::AbstractMatrix
    return error("missing method: unsafe_get_matrix for storage type: $(typeof(storage))")  # untested
end

include("storage/memory.jl")

end # module
