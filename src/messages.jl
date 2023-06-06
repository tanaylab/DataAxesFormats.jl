"""
Functions for improving the quality of error and log messages.
"""
module Messages

export present
export present_percent
export unique_name

using Daf.MatrixLayouts
using Distributed
using LinearAlgebra
using SparseArrays

unique_name_prefixes = Dict{String, Int64}()

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
    if haskey(unique_name_prefixes, prefix)
        counter = unique_name_prefixes[prefix]
        counter += 1
    else
        counter = 1
    end
    unique_name_prefixes[prefix] = counter
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
    if ismissing(value)
        return "missing"
    end

    if value isa String
        return "\"$(value)\""
    end

    if value isa Symbol
        return ":$(value)"
    end

    if value isa AbstractVector
        if value isa DenseVector
            return "$(length(value)) x $(eltype(value)) (dense)"
        end

        if value isa SparseVector
            nnz = present_percent(length(value.nzval), length(value))
            return "$(length(value)) x $(eltype(value)) (sparse $(nnz))"
        end

        return "$(length(value)) x $(eltype(value)) ($(typeof(value)))"  # untested
    end

    if value isa AbstractMatrix
        layout = major_axis(value)
        if layout == Row
            suffix = ", row-major"
        elseif layout == Column
            suffix = ", column-major"
        else
            suffix = ""  # untested
        end

        internal = value
        while internal isa Transpose
            internal = internal.parent
        end

        if internal isa DenseMatrix
            return "$(size(value, 1)) x $(size(value, 2)) x $(eltype(value)) (dense$(suffix))"
        end

        if internal isa SparseMatrixCSC
            nnz = present_percent(length(internal.nzval), length(value))
            return "$(size(value, 1)) x $(size(value, 2)) x $(eltype(value)) (sparse $(nnz)$(suffix))"
        end

        return "$(size(value, 1)) x $(size(value, 2)) x $(eltype(value)) ($(typeof(value))$(suffix))"  # untested
    end

    return "$(value)"
end

"""
    present_percent(used::Int64, out_of::Int64)::String

Present a fraction of `used` amount `out_of` some total as a percentage.
"""
function present_percent(used::Int64, out_of::Int64)::String
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
