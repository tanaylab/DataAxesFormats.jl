"""
A `Daf` query can use operations to process the data: [`EltwiseOperation`](@ref)s that preserve the shape of the data,
and [`ReductionOperation`](@ref)s that reduce a matrix to a vector, or a vector to a scalar.
"""
module Operations

using Daf.ParseQueries
using Daf.Registry
using Daf.StorageTypes

import Base.MathConstants.e
import Daf.ParseQueries.error_in_context
import Daf.ParseQueries.parse_in_context
import Daf.Registry.compute_eltwise
import Daf.Registry.compute_reduction
import Distributed.@everywhere

using Base.MathConstants

export Abs
export float_dtype_for
export invalid_parameter_value
export Log
export Max
export parse_dtype_assignment
export parse_number_assignment
export parse_parameter
export Round
export Sum

FLOAT_DTYPE_FOR = Dict{Type, Type}(
    Bool => Float32,
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
    float_dtype_for(element_type::Type)::Type

Given an input `element_type`, return the data type to use for the result of an operation that always produces floating
point values (e.g., `Log`). If `dtype` isn't (the default) `nothing`, it is returned instead.
"""
function float_dtype_for(element_type::Type, dtype::Union{Type, Nothing} = nothing)::Type
    if dtype == nothing
        global FLOAT_DTYPE_FOR
        return FLOAT_DTYPE_FOR[element_type]
    else
        return dtype  # untested
    end
end

INT_DTYPE_FOR = Dict{Type, Type}(
    Bool => Bool,
    Int8 => Int8,
    Int16 => Int16,
    Int32 => Int32,
    Int64 => Int64,
    UInt8 => UInt8,
    UInt16 => UInt16,
    UInt32 => UInt32,
    UInt64 => UInt64,
    Float32 => Int32,
    Float64 => Int64,
)

"""
    int_dtype_for(element_type::Type[, dtype::Union{Type, Nothing} = nothing])::Type

Given an input `element_type`, return the data type to use for the result of an operation that always produces integer
values (e.g., `Round`). If `dtype` isn't (the default) `nothing`, it is returned instead.
"""
function int_dtype_for(element_type::Type, dtype::Union{Type, Nothing} = nothing)::Type
    if dtype == nothing
        global INT_DTYPE_FOR  # untested
        return INT_DTYPE_FOR[element_type]  # untested
    else
        return dtype
    end
end

"""
    invalid_parameter_value(
        context::QueryContext,
        parameter_assignment::QueryOperation,
        must_be::AbstractString
    )::Nothing

Complain that an operation parameter is not valid.
"""
function invalid_parameter_value(
    context::QueryContext,
    parameter_assignment::QueryOperation,
    must_be::AbstractString,
)::Nothing
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

"""
    parse_dtype_assignment(
        context::QueryContext,
        parameter_assignment::QueryOperation
    )::Union{Type, Nothing}

Parse the `dtype` operation parameter.

Valid names are `{B,b}ool`, `{UI,ui,I,i}nt{8,16,32,64}` and `{F,f}loat{32,64}`.
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
    function parse_number_assignment(
        context::QueryContext,
        parameter_assignment::QueryOperation,
        type::Type{T},
    )::T where {T <: Number}

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
    function parse_parameter(
        parse_assignment::Function,
        context::QueryContext,
        parameters_assignments::Dict{String, QueryOperation},
        parameter_name::AbstractString,
        default::Any,
    )::Any

Parse an operation parameter.
"""
function parse_parameter(
    parse_assignment::Function,
    context::QueryContext,
    parameters_assignments::Dict{String, QueryOperation},
    parameter_name::AbstractString,
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
"""
struct Abs <: EltwiseOperation end
@query_operation Abs

function Abs(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Abs
    return Abs()
end

function compute_eltwise(
    operation::Abs,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: Number}
    return abs.(input)
end

function compute_eltwise(operation::Abs, input::T)::T where {T <: Number}
    return abs(input)
end

"""
Element-wise operation that converts every element to the nearest integer value.
"""
struct Round <: EltwiseOperation
    dtype::Union{Type, Nothing}
end
@query_operation Round

function Round(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Round
    dtype = parse_parameter(parse_dtype_assignment, context, parameters_assignments, "dtype", nothing)
    return Round(dtype)
end

function compute_eltwise(
    operation::Round,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: Number}
    return round.(int_dtype_for(eltype(input), operation.dtype), input)
end

function compute_eltwise(operation::Round, input::Number)::Number  # untested
    return round(int_dtype_for(eltype(input), operation.dtype), input)
end

"""
Element-wise operation that converts every element to its logarithm.

**Parameters**:

`base` - The base of the logarithm. By default uses `e` (that is, computes the natural logarithm), which isn't
convenient, but is the standard.

`eps` - Added to the input before computing the logarithm, to handle zero input data. By default is zero.
"""
struct Log <: EltwiseOperation
    base::Float64
    eps::Float64
end
@query_operation Log

function Log(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Log
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

    return Log(base, eps)
end

function compute_eltwise(
    operation::Log,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: Number}
    dtype = float_dtype_for(eltype(input))
    output = similar(input, dtype)
    output .= input
    output .+= dtype(operation.eps)
    output .= log.(output)
    output ./= log(dtype(operation.base))
    return output
end

function compute_eltwise(operation::Log, input::T)::T where {T <: Number}  # untested
    dtype = float_dtype_for(eltype(input))
    return log(dtype(dtype(input) + dtype(operation.eps))) / log(dtype(operation.base))
end

"""
Reduction operation that sums elements.
"""
struct Sum <: ReductionOperation end
@query_operation Sum

function Sum(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Sum
    return Sum()
end

function compute_reduction(operation::Sum, input::StorageMatrix{T})::StorageVector{T} where {T <: Number}
    result = Vector{eltype(input)}(undef, size(input)[2])
    result .= transpose(sum(input; dims = 1))
    return result
end

function compute_reduction(operation::Sum, input::StorageVector{T})::T where {T <: Number}
    return sum(input)
end

"""
Reduction operation that returns the maximal element.
"""
struct Max <: ReductionOperation end
@query_operation Max

function Max(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::Max
    return Max()
end

end # module
