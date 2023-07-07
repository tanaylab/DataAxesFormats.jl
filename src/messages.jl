"""
Functions for improving the quality of error and log messages.
"""
module Messages

export present
export present_percent
export unique_name

using Daf.AsDense
using Daf.DataTypes
using Daf.MatrixLayouts
using Distributed
using LinearAlgebra
using SparseArrays

UNIQUE_NAME_PREFIXES = Dict{String, Int64}()

"""
    unique_name(prefix::String)::String

Using short, human-readable unique names for things is a great help when debugging. Normally one has to choose between
using a human-provided short non-unique name, and an opaque object identifier, or a combination thereof. This function
replaces the opaque object identifier with a short counter, which gives names that are both unique and short.

That is, this will return a unique name starting with the `prefix` and followed by `#`, the process index (if using
multiple processes), and an index (how many times this name was used in the process). For example, `unique_name("foo")`
will return `foo#1` for the first usage, `foo#2` for the 2nd, etc., and if using multiple processes, will return
`foo#1.1`, `foo#1.2`, etc.
"""
function unique_name(prefix::String)::String
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

function present(value::Missing)::String
    return "missing"
end

function present(value::String)::String
    return "\"$(value)\""
end

function present(value::Symbol)::String
    return ":$(value)"
end

function present(value::AbstractVector)::String  # untested
    as_dense = as_dense_if_possible(value)
    if as_dense !== value
        return present(as_dense)
    end
    return "$(length(value)) x $(eltype(value)) ($(typeof(value)))"
end

function present(value::DenseVector)::String
    return "$(length(value)) x $(eltype(value)) (Dense)"
end

function present(value::SparseVector)::String
    nnz = present_percent(length(value.nzval), length(value))
    return "$(length(value)) x $(eltype(value)) (Sparse $(nnz))"
end

function present(value::AbstractMatrix; transposed::Bool = false)::String  # untested
    as_dense = as_dense_if_possible(value)
    if as_dense !== value
        return present(as_dense)
    end
    return present_matrix(value, "$(typeof(value))"; transposed = transposed)
end

function present(value::DenseMatrix; transposed::Bool = false)::String
    return present_matrix(value, "Dense"; transposed = transposed)
end

function present(value::SparseMatrixCSC; transposed::Bool = false)::String
    nnz = present_percent(length(value.nzval), length(value))
    return present_matrix(value, "Sparse $(nnz)"; transposed = transposed)
end

function present(value::Transpose; transposed::Bool = false)::String
    return present(transpose(value); transposed = !transposed)
end

function present_matrix(matrix::AbstractMatrix, kind::String; transposed::Bool = false)::String
    layout = major_axis(matrix)
    if transposed
        layout = other_axis(layout)
    end

    if layout != nothing
        suffix = "$(kind) in $(axis_name(layout))"
    else
        suffix = kind  # untested
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

end # module
