"""
Query operations.
"""
module Operations

using Daf.Query
using Daf.Registry

import Base.MathConstants.e
import Daf.Query.error_in_context
import Daf.Query.parse_in_context
import Distributed.@everywhere

using Base.MathConstants

export Abs
export invalid_parameter_value
export Log
export Max
export parse_dtype_assignment
export parse_number_assignment
export parse_parameter
export Sum

# Map name of `dtype` in a query to the Julia.
#
# Valid names are `{B,b}ool`, `{UI,ui,I,i}nt{8,16,32,64}` and `{F,f}loat{32,64}`.
DTYPE_BY_NAME = Dict{String, Union{Type, Nothing}}(
    "bool" => Bool,
    "Bool" => Bool,
    "int8" => Int8,
    "Int8" => Int8,
    "int16" => Int16,
    "Int16" => Int16,
    "int32" => Int32,
    "Int32" => Int32,
    "int64" => Int64,
    "Int64" => Int64,
    "uint8" => UInt8,
    "UInt8" => UInt8,
    "uint16" => UInt16,
    "UInt16" => UInt16,
    "uint32" => UInt32,
    "UInt32" => UInt32,
    "uint64" => UInt64,
    "UInt64" => UInt64,
    "float32" => Float32,
    "Float32" => Float32,
    "float64" => Float64,
    "Float64" => Float64,
    "auto" => nothing,
)

# Map a data type to the matching size floating-point data type.
#
# This is used to compute a default data type for operations that return a floating point number (e.g., [`Log`](@ref)),
# when they get an input of an arbitrary data type (e.g., some integer).
FLOAT_DTYPE = Dict{Type, Type}(
    Bool => Float32,
    Int8 => Float32,
    Int8 => Float32,
    Int16 => Float32,
    Int32 => Float32,
    Int64 => Float64,
    UInt8 => Float32,
    UInt16 => Float32,
    UInt32 => Float32,
    UInt64 => Float64,
    Float32 => Float32,
    Float64 => Float64,
)

"""
Complain that an operation parameter is not valid.
"""
function invalid_parameter_value(context::QueryContext, parameter_assignment::QueryOperation, must_be::String)::Nothing
    return parse_in_context(context, parameter_assignment; name = "parameter assignment") do
        return parse_in_context(context, parameter_assignment.right; name = "parameter value") do
            return error_in_context(
                context,
                "invalid value: \"$(escape_string(parameter_assignment.right.string))\"\n" *
                "value must be: $(must_be)\n" *
                "for the parameter: $(parameter_assignment.left.string)",
            )
        end
    end
end

"""
Parse the `dtype` operation parameter.

Valid names are `{B,b}ool`, `{UI,ui,I,i}nt{8,16,32,64}` and `{F,f}loat{32,64}`, and `auto` which is parsed to `nothing`.
"""
function parse_dtype_assignment(context::QueryContext, parameter_assignment::QueryOperation)::Union{Type, Nothing}
    dtype_name = parameter_assignment.right.string
    global DTYPE_BY_NAME
    if !(dtype_name in keys(DTYPE_BY_NAME))
        invalid_parameter_value(context, parameter_assignment, "a number type")
    end
    return DTYPE_BY_NAME[dtype_name]
end

"""
Parse a numeric operation parameter.
"""
function parse_number_assignment(
    context::QueryContext,
    parameter_assignment::QueryOperation,
    type::Type{T},
)::T where {T <: Number}
    try
        if parameter_assignment.right.string == "e" || parameter_assignment.right.string == "E"
            return e
        else
            return parse(type, parameter_assignment.right.string)
        end
    catch
        invalid_parameter_value(context, parameter_assignment, "a valid $(type)")
    end
end

"""
Parse an operation parameter.
"""
function parse_parameter(
    parse_assignment::Function,
    context::QueryContext,
    parameters_assignments::Dict{String, QueryOperation},
    parameter_name::String,
    default::Any,
)::Any
    if parameter_name in keys(parameters_assignments)
        return parse_assignment(context, parameters_assignments[parameter_name])
    else
        return default
    end
end

"""
Element-wise operation that converts every element to its absolute value.

**Parameters**:

`dtype` - Force the result data type. By default the data type is identical to the input data type.
"""
struct Abs <: EltwiseOperation
    dtype::Union{Type, Nothing}
end
@query_operation Abs

function Abs(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Abs
    dtype = parse_parameter(parse_dtype_assignment, context, parameters_assignments, "dtype", nothing)
    return Abs(dtype)
end

"""
Element-wise operation that converts every element to its logarithm.

**Parameters**:

`dtype` - Force the result data type. By default the data type is `Float64` if the input is 64-bit data, `Float32`
otherwise.

`base` - The base of the logarithm. By default uses `e` (that is, computes the natural logarithm), which isn't
convenient, but is the standard.

`eps` - Added to the input before computing the logarithm, to handle zero input data. By default is zero.
"""
struct Log <: EltwiseOperation
    dtype::Union{Type, Nothing}
    base::Float64
    eps::Float64
end
@query_operation Log

function Log(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Log
    dtype = parse_parameter(parse_dtype_assignment, context, parameters_assignments, "dtype", nothing)

    base = parse_parameter(context, parameters_assignments, "base", Float64(e)) do context, parameter_assignment
        base = parse_number_assignment(context, parameter_assignment, Float64)
        if base <= 0
            invalid_parameter_value(context, parameter_assignment, "positive")
        end
        return base
    end

    eps = parse_parameter(context, parameters_assignments, "eps", 0.0) do context, parameter_assignment
        eps = parse_number_assignment(context, parameter_assignment, Float64)
        if eps < 0
            invalid_parameter_value(context, parameter_assignment, "non-negative")
        end
        return eps
    end

    return Log(dtype, base, eps)
end

"""
Reduction operation that sums elements.

**Parameters**:

`dtype` - Force the result data type. By default the data type is identical to the input data type.
"""
struct Sum <: ReductionOperation
    dtype::Union{Type, Nothing}
end
@query_operation Sum

function Sum(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Sum
    dtype = parse_parameter(parse_dtype_assignment, context, parameters_assignments, "dtype", nothing)
    return Sum(dtype)
end

"""
Reduction operation that returns the maximal element.

**Parameters**:

`dtype` - Force the result data type. By default the data type is identical to the input data type.
"""
struct Max <: ReductionOperation
    dtype::Union{Type, Nothing}
end
@query_operation Max

function Max(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Max
    dtype = parse_parameter(parse_dtype_assignment, context, parameters_assignments, "dtype", nothing)
    return Max(dtype)
end

end # module
