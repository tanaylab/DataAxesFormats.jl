"""
Functions for improving the quality of error and log messages.
"""
module Messages

export depict
export depict_percent
export unique_name

using ..MatrixLayouts
using ..StorageTypes
using Distributed
using LinearAlgebra
using NamedArrays
using SparseArrays

import ..MatrixLayouts.depict
import ..MatrixLayouts.depict_matrix_size

UNIQUE_NAME_PREFIXES = Dict{AbstractString, Int64}()
UNIQUE_NAME_LOCK = ReentrantLock()

"""
    unique_name(prefix::AbstractString, separator::AbstractString = "#")::AbstractString

Using short, human-readable unique names for things is a great help when debugging. Normally one has to choose between
using a human-provided short non-unique name, and an opaque object identifier, or a combination thereof. This function
replaces the opaque object identifier with a short counter, which gives names that are both unique and short.

That is, this will return a unique name starting with the `prefix` and followed by the `separator` (by default, `#`),
the process index (if using multiple processes), and an index (how many times this name was used in the process). For
example, `unique_name("foo")` will return `foo` for the first usage, `foo#2` for the 2nd, etc. If using multiple
processes, it will return `foo`, `foo#1.2`, etc.

That is, for code where the names are unique (e.g., a simple script or Jupyter notebook), this doesn't mess up the
names. It only appends a suffix to the names if it is needed to disambiguate between multiple uses of the same name.

To help with tests, if the `prefix` contains `!`, we return it as-is, accepting it may not be unique.
"""
function unique_name(prefix::AbstractString, separator::AbstractString = "#")::AbstractString
    if contains(prefix, '!')
        return String(prefix)
    end

    counter = lock(UNIQUE_NAME_LOCK) do
        global UNIQUE_NAME_PREFIXES
        counter = get(UNIQUE_NAME_PREFIXES, prefix, 0)
        counter += 1
        UNIQUE_NAME_PREFIXES[prefix] = counter
        return counter
    end

    if counter == 1
        return prefix
    elseif nprocs() > 1
        return "$(prefix)$(separator)$(myid()).$(counter)"  # untested
    else
        return "$(prefix)$(separator)$(counter)"
    end
end

"""
    depict(value::Any)::String

Depict a `value` in an error message or a log entry. Unlike `"\$(value)"`, this focuses on producing a human-readable
indication of the type of the value, so it double-quotes strings, prefixes symbols with `:`, and reports the type and
sizes of arrays rather than showing their content, as well as having specializations for the various `Daf` data types.
"""
function depict(value::Any)::String
    try
        return "($(nameof(typeof(value))) size: $(size(value)))"  # NOJET
    catch
        try
            return "($(nameof(typeof(value))) length: $(length(value)))"  # NOJET
        catch
            return "($(typeof(value)))"
        end
    end
end

function depict(value::Real)::String
    return "$(value) ($(typeof(value)))"
end

function depict(value::Tuple)::String
    return "(" * join([depict(entry) for entry in value], ", ") * ")"
end

function depict(value::Union{Bool, Type, Nothing, Missing})::String
    return "$(value)"
end

function depict(::UndefInitializer)::String
    return "undef"
end

function depict(value::AbstractString)::String
    value = replace(value, "\\" => "\\\\", "\"" => "\\\"")
    return "\"$(value)\""
end

function depict(value::Symbol)::String
    return ":$(value)"
end

function depict(vector::AbstractVector)::String
    return depict_array(vector, depict_vector(vector, ""))
end

function depict_vector(vector::SparseArrays.ReadOnly, prefix::AbstractString)::String
    return depict_vector(parent(vector), concat_prefixes(prefix, "ReadOnly"))
end

function depict_vector(vector::NamedArray, prefix::AbstractString)::String
    return depict_vector(vector.array, concat_prefixes(prefix, "Named"))
end

function depict_vector(vector::DenseVector, prefix::AbstractString)::String
    return depict_vector_size(vector, concat_prefixes(prefix, "Dense"))
end

function depict_vector(vector::SparseVector, prefix::AbstractString)::String
    nnz = depict_percent(length(vector.nzval), length(vector))
    return depict_vector_size(vector, concat_prefixes(prefix, "Sparse $(SparseArrays.indtype(vector)) $(nnz)"))
end

function depict_vector(vector::AbstractVector, prefix::AbstractString)::String  # untested
    try
        if strides(vector) == (1,)
            return depict_vector_size(vector, concat_prefixes(prefix, "$(typeof(vector)) - Dense"))
        else
            return depict_vector_size(vector, concat_prefixes(prefix, "$(typeof(vector)) - Strided"))
        end
    catch
        return depict_vector_size(vector, concat_prefixes(prefix, "$(typeof(vector))"))
    end
end

function depict_vector_size(vector::AbstractVector, kind::AbstractString)::String
    return "$(length(vector)) x $(eltype(vector)) ($(kind))"
end

function depict(matrix::AbstractMatrix)::String
    return depict_array(matrix, depict_matrix(matrix, ""; transposed = false))
end

function depict_matrix(matrix::SparseArrays.ReadOnly, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(parent(matrix), concat_prefixes(prefix, "ReadOnly"); transposed = transposed)
end

function depict_matrix(matrix::NamedMatrix, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(matrix.array, concat_prefixes(prefix, "Named"); transposed = transposed)
end

function depict_matrix(matrix::Transpose, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(parent(matrix), concat_prefixes(prefix, "Transpose"); transposed = !transposed)
end

function depict_matrix(matrix::Adjoint, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix(parent(matrix), concat_prefixes(prefix, "Adjoint"); transposed = !transposed)
end

function depict_matrix(matrix::DenseMatrix, prefix::AbstractString; transposed::Bool = false)::String
    return depict_matrix_size(matrix, concat_prefixes(prefix, "Dense"); transposed = transposed)
end

function depict_matrix(matrix::SparseMatrixCSC, prefix::AbstractString; transposed::Bool = false)::String
    nnz = depict_percent(length(matrix.nzval), length(matrix))
    return depict_matrix_size(
        matrix,
        concat_prefixes(prefix, "Sparse $(SparseArrays.indtype(matrix)) $(nnz)");
        transposed = transposed,
    )
end

function depict_matrix(matrix::AbstractMatrix, ::AbstractString; transposed::Bool = false)::String  # untested
    try
        matrix_strides = strides(matrix)
        matrix_sizes = size(matrix)
        if matrix_strides == (1, matrix_sizes[1]) || matrix_strides == (matrix_sizes[2], 1)
            return depict_matrix_size(matrix, "$(nameof(typeof(matrix))) - Dense"; transposed = transposed)
        else
            return depict_matrix_size(matrix, "$(nameof(typeof(matrix))) - Strided"; transposed = transposed)
        end
    catch
        return depict_matrix_size(matrix, "$(nameof(typeof(matrix)))"; transposed = transposed)
    end
end

function depict(array::AbstractArray)::String
    text = ""
    for dim_size in size(array)
        if text == ""
            text = "$(dim_size)"
        else
            text = "$(text) x $(dim_size)"
        end
    end
    return depict_array(array, "$(text) x $(eltype(array)) ($(nameof(typeof(array))))")
end

function depict(set::AbstractSet)::String
    return "$(length(set)) x $(eltype(set)) ($(nameof(typeof(set))))"
end

function depict_array(array::AbstractArray, text::String)::String
    if eltype(array) == Bool
        trues = sum(array)  # NOJET
        text *= " ($(trues) true, $(depict_percent(trues, length(array))))"
    end
    return text
end

"""
    depict_percent(used::Integer, out_of::Integer)::String

Describe a fraction of `used` amount `out_of` some total as a percentage.
"""
function depict_percent(used::Integer, out_of::Integer)::String
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
