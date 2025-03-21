"""
Functions for improving the quality of error and log messages.
"""
module Messages

export depict
export depict_percent
export unique_name

using ..StorageTypes
using Distributed
using LinearAlgebra
using NamedArrays
using SparseArrays

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
        return "$(prefix)$(separator)$(myid()).$(counter)"  # UNTESTED
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
    @assert 0 <= used <= out_of

    if out_of == 0
        return "0%"  # UNTESTED
    end

    float_percent = 100.0 * Float64(used) / Float64(out_of)
    int_percent = round(Int64, float_percent)

    if int_percent == 0 && float_percent > 0
        return "<1%"  # UNTESTED
    end

    if int_percent == 100 && float_percent < 100
        return ">99%"  # UNTESTED
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
