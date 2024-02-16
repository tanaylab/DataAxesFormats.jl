"""
Functions for improving the quality of error and log messages.
"""
module Messages

export present
export present_percent
export unique_name

using Daf.MatrixLayouts
using Daf.StorageTypes
using Distributed
using LinearAlgebra
using NamedArrays
using SparseArrays

UNIQUE_NAME_PREFIXES = Dict{String, Int64}()

"""
    unique_name(prefix::AbstractString)::String

Using short, human-readable unique names for things is a great help when debugging. Normally one has to choose between
using a human-provided short non-unique name, and an opaque object identifier, or a combination thereof. This function
replaces the opaque object identifier with a short counter, which gives names that are both unique and short.

That is, this will return a unique name starting with the `prefix` and followed by `#`, the process index (if using
multiple processes), and an index (how many times this name was used in the process). For example, `unique_name("foo")`
will return `foo#1` for the first usage, `foo#2` for the 2nd, etc., and if using multiple processes, will return
`foo#1.1`, `foo#1.2`, etc.

To help with tests, if the `prefix` ends with `!`, we return it as-is, accepting it may not be unique.
"""
function unique_name(prefix::AbstractString)::String
    if prefix[end] == '!'
        return String(prefix)
    end

    global UNIQUE_NAME_PREFIXES

    if haskey(UNIQUE_NAME_PREFIXES, prefix)
        counter = UNIQUE_NAME_PREFIXES[prefix]
        counter += 1
    else
        counter = 1
    end

    UNIQUE_NAME_PREFIXES[prefix] = counter
    if nprocs() > 1
        return "$(prefix)#$(myid()).$(counter)"  # untested
    else
        return "$(prefix)#$(counter)"
    end
end

"""
    present(value::Any)::String

Present a `value` in an error message or a log entry. Unlike `"\$(value)"`, this focuses on producing a human-readable
indication of the type of the value, so it double-quotes strings, prefixes symbols with `:`, and reports the type and
sizes of arrays rather than showing their content.
"""
function present(value::Any)::String
    return "$(value)"
end

function present(value::UndefInitializer)::String
    return "undef"
end

function present(value::Missing)::String
    return "missing"
end

function present(value::AbstractString)::String
    return "\"$(value)\""
end

function present(value::Symbol)::String
    return ":$(value)"
end

function present(value::AbstractVector)::String
    return present_vector(value, "")
end

function present_vector(vector::SparseArrays.ReadOnly, prefix::AbstractString)::String
    return present_vector(parent(vector), concat_prefixes(prefix, "ReadOnly"))
end

function present_vector(vector::NamedArray, prefix::AbstractString)::String
    return present_vector(vector.array, concat_prefixes(prefix, "Named"))
end

function present_vector(vector::DenseVector, prefix::AbstractString)::String
    return present_vector_size(vector, concat_prefixes(prefix, "Dense"))
end

function present_vector(vector::SparseVector, prefix::AbstractString)::String
    nnz = present_percent(length(vector.nzval), length(vector))
    return present_vector_size(vector, concat_prefixes(prefix, "Sparse $(nnz)"))
end

function present_vector(vector::AbstractVector, prefix::AbstractString)::String  # untested
    try
        if strides(vector) == (1,)
            return present_vector_size(vector, "$(typeof(vector)) - Dense")
        else
            return present_vector_size(vector, "$(typeof(vector)) - Strided")
        end
    catch
        return present_vector_size(vector, "$(typeof(vector))")
    end
end

function present_vector_size(vector::AbstractVector, kind::AbstractString)::String
    return "$(length(vector)) x $(eltype(vector)) ($(kind))"
end

function present(matrix::AbstractMatrix)::String
    return present_matrix(matrix, ""; transposed = false)
end

function present_matrix(matrix::SparseArrays.ReadOnly, prefix::AbstractString; transposed::Bool = false)::String
    return present_matrix(parent(matrix), concat_prefixes(prefix, "ReadOnly"); transposed = transposed)
end

function present_matrix(matrix::NamedMatrix, prefix::AbstractString; transposed::Bool = false)::String
    return present_matrix(matrix.array, concat_prefixes(prefix, "Named"); transposed = transposed)
end

function present_matrix(matrix::Transpose, prefix::AbstractString; transposed::Bool = false)::String
    return present_matrix(transpose(matrix), prefix; transposed = !transposed)
end

function present_matrix(matrix::DenseMatrix, prefix::AbstractString; transposed::Bool = false)::String
    return present_matrix_size(matrix, concat_prefixes(prefix, "Dense"); transposed = transposed)
end

function present_matrix(matrix::SparseMatrixCSC, prefix::AbstractString; transposed::Bool = false)::String
    nnz = present_percent(length(matrix.nzval), length(matrix))
    return present_matrix_size(matrix, concat_prefixes(prefix, "Sparse $(nnz)"); transposed = transposed)
end

function present_matrix(matrix::AbstractMatrix, kind::AbstractString; transposed::Bool = false)::String  # untested
    try
        matrix_strides = strides(matrix)
        matrix_sizes = size(matrix)
        if matrix_strides == (1, matrix_sizes[1]) || matrix_strides == (matrix_sizes[2], 1)
            return present_matrix_size(matrix, "$(typeof(matrix)) - Dense"; transposed = transposed)
        else
            return present_matrix_size(matrix, "$(typeof(matrix)) - Strided"; transposed = transposed)
        end
    catch
        return present_matrix_size(matrix, "$(typeof(matrix))"; transposed = transposed)
    end
end

function present_matrix_size(matrix::AbstractMatrix, kind::AbstractString; transposed::Bool = false)::String
    layout = major_axis(matrix)
    if transposed
        layout = other_axis(layout)
    end

    if layout == nothing
        suffix = kind  # untested
    else
        suffix = "$(kind) in $(axis_name(layout))"
    end

    if transposed
        return "$(size(matrix, 2)) x $(size(matrix, 1)) x $(eltype(matrix)) ($(suffix))"
    else
        return "$(size(matrix, 1)) x $(size(matrix, 2)) x $(eltype(matrix)) ($(suffix))"
    end
end

function present(value::AbstractArray)::String
    text = ""
    for dim_size in size(value)
        if text == ""
            text = "$(dim_size)"
        else
            text = "$(text) x $(dim_size)"
        end
    end
    return "$(text) x $(eltype(value)) ($(typeof(value)))"
end

"""
    present_percent(used::Integer, out_of::Integer)::String

Present a fraction of `used` amount `out_of` some total as a percentage.
"""
function present_percent(used::Integer, out_of::Integer)::String
    float_percent = 100.0 * Float64(used) / Float64(out_of)
    int_percent = round(Int64, float_percent)

    if int_percent == 0 && float_percent > 0
        return "<1%"  # untested
    end

    if int_percent == 100 && float_percent < 100
        return ">99%"  # untested
    end

    return "$(int_percent)%"
end

function concat_prefixes(prefix::AbstractString, suffix::AbstractString)::String
    @assert suffix != ""
    if prefix == ""
        return suffix
    else
        return prefix * " " * suffix
    end
end

end # module
