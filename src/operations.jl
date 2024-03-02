"""
A `Daf` query can use operations to process the data: [`EltwiseOperation`](@ref)s that preserve the shape of the data,
and [`ReductionOperation`](@ref)s that reduce a matrix to a vector, or a vector to a scalar.
"""
module Operations

using Daf.Registry
using Daf.StorageTypes
using Daf.Tokens

import Base.MathConstants.e
import Base.MathConstants.pi
import Daf.Registry.compute_eltwise
import Daf.Registry.compute_reduction
import Daf.Registry.reduction_result_type
import Daf.Tokens.error_at_token
import Daf.Tokens.Token
import Distributed.@everywhere

using Base.MathConstants
using Daf.Unions

export Abs
export float_dtype_for
export int_dtype_for
export error_invalid_parameter_value
export Log
export Max
export parse_dtype_value
export parse_float_dtype_value
export parse_int_dtype_value
export parse_number_value
export parse_parameter_value
export Round
export Sum
export sum_dtype_for
export unsigned_dtype_for

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
    float_dtype_for(
        element_type::Type{T},
        dtype::Maybe{Type{D}}
    )::Type where {T <: StorageNumber, D <: StorageNumber}

Given an input `element_type`, return the data type to use for the result of an operation that always produces floating
point values (e.g., [`Log`](@ref)). If `dtype` isn't  `nothing`, it is returned instead.
"""
function float_dtype_for(
    element_type::Type{T},
    dtype::Maybe{Type{D}},
)::Type where {T <: StorageNumber, D <: StorageNumber}
    if dtype == nothing
        global FLOAT_DTYPE_FOR
        return FLOAT_DTYPE_FOR[element_type]
    else
        @assert dtype <: AbstractFloat  # untested
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
    int_dtype_for(
        element_type::Type{T},
        dtype::Maybe{Type{D}}
    )::Type where {T <: StorageNumber, D <: StorageNumber}

Given an input `element_type`, return the data type to use for the result of an operation that always produces integer
values (e.g., [`Round`](@ref)). If `dtype` isn't `nothing`, it is returned instead.
"""
function int_dtype_for(  # untested
    element_type::Type{T},
    dtype::Maybe{Type{D}},
)::Type where {T <: StorageNumber, D <: StorageNumber}
    if dtype == nothing
        global INT_DTYPE_FOR
        return INT_DTYPE_FOR[element_type]
    else
        @assert dtype <: Integer
        return dtype
    end
end

SUM_DTYPE_FOR = Dict{Type, Type}(
    Bool => UInt32,
    Int8 => Int32,
    Int16 => Int32,
    Int32 => Int32,
    Int64 => Int64,
    UInt8 => UInt32,
    UInt16 => UInt32,
    UInt32 => UInt32,
    UInt64 => UInt64,
    Float32 => Int32,
    Float64 => Int64,
)

"""
    sum_dtype_for(
        element_type::Type{T},
        dtype::Maybe{Type{D}}
    )::Type where {T <: StorageNumber, D <: StorageNumber}

Given an input `element_type`, return the data type to use for the result of an operation that sums many such values
values (e.g., [`Sum`](@ref)). If `dtype` isn't `nothing`, it is returned instead.

This keeps floating point and 64-bit types as-is, but increases any small integer types to the matching 32 bit type
(e.g., an input type of `UInt8` will have a sum type of `UInt32`).
"""
function sum_dtype_for(
    element_type::Type{T},
    dtype::Maybe{Type{D}},
)::Type where {T <: StorageNumber, D <: StorageNumber}
    if dtype == nothing
        global SUM_DTYPE_FOR
        return SUM_DTYPE_FOR[element_type]
    elseif dtype <: Integer && element_type <: AbstractFloat  # untested
        error("summing float values: $(element_type)\ninto an integer value: $(dtype)")  # untested
    elseif dtype <: Unsigned && element_type <: Integer  # untested
        error("summing signed values: $(element_type)\ninto an unsigned value: $(dtype)")  # untested
    else
        return dtype  # untested
    end
end

UNSIGNED_DTYPE_FOR = Dict{Type, Type}(
    Bool => Bool,
    Int8 => UInt8,
    Int16 => UInt16,
    Int32 => UInt32,
    Int64 => UInt64,
    UInt8 => UInt8,
    UInt16 => UInt16,
    UInt32 => UInt32,
    UInt64 => UInt64,
    Float32 => Int32,
    Float64 => Int64,
)

"""
    unsigned_dtype_for(
        element_type::Type{T},
        dtype::Maybe{Type{D}}
    )::Type where {T <: StorageNumber, D <: StorageNumber}

Given an input `element_type`, return the data type to use for the result of an operation that discards the sign of the
value (e.g., [`Abs`](@ref)). If `dtype` isn't `nothing`, it is returned instead.
"""
function unsigned_dtype_for(
    element_type::Type{T},
    dtype::Maybe{Type{D}},
)::Type where {T <: StorageNumber, D <: StorageNumber}
    if dtype == nothing
        global UNSIGNED_DTYPE_FOR
        return UNSIGNED_DTYPE_FOR[element_type]
    else
        return dtype  # untested
    end
end

"""
    error_invalid_parameter_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
        must_be::AbstractString,
    )::Nothing

Complain that an operation parameter value is not valid.
"""
function error_invalid_parameter_value(  # untested
    operation_name::AbstractString,
    parameter_name::AbstractString,
    parameter_value::Token,
    must_be::AbstractString,
)::Union{}
    return error_at_token(
        parameter_value,
        "invalid value: \"$(escape_string(parameter_value.value))\"\n" *
        "value must be: $(must_be)\n" *
        "for the parameter: $(parameter_name)\n" *
        "for the operation: $(operation_name)",
    )
end

DTYPE_BY_NAME = Dict{String, Maybe{Type}}(
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
    parse_dtype_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Parse the `dtype` operation parameter.

Valid names are `{B,b}ool`, `{UI,ui,I,i}nt{8,16,32,64}` and `{F,f}loat{32,64}`.
"""
function parse_dtype_value(
    operation_name::AbstractString,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    dtype_name = parameter_value.value
    global DTYPE_BY_NAME
    dtype_type = get(DTYPE_BY_NAME, dtype_name, missing)
    if dtype_type === missing
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a number type")  # untested
    end
    return DTYPE_BY_NAME[dtype_name]
end

"""
    parse_int_dtype_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Similar to [`parse_dtype_value`](@ref), but only accept integer (signed or unsigned) types.
"""
function parse_int_dtype_value(  # untested
    operation_name::AbstractString,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    dtype = parse_dtype_value(operation_name, parameter_name, parameter_value)
    if dtype <: Integer
        return dtype
    else
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "an integer type")
    end
end

"""
    function parse_float_dtype_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Similar to [`parse_dtype_value`](@ref), but only accept floating point types.
"""
function parse_float_dtype_value(  # untested
    operation_name::AbstractString,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    dtype = parse_dtype_value(operation_name, parameter_name, parameter_value)
    if dtype <: AbstractFloat
        return dtype
    else
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a float type")
    end
end

"""
    function parse_number_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
        type::Type{T},
    )::T where {T <: StorageNumber}

Parse a numeric operation parameter.
"""
function parse_number_value(
    operation_name::AbstractString,
    parameter_name::AbstractString,
    parameter_value::Token,
    type::Type{T},
)::T where {T <: StorageNumber}
    try
        if parameter_value.value == "e" || parameter_value.value == "E"
            return Float64(e)  # untested
        elseif parameter_value.value == "pi" || parameter_value.value == "PI"
            return Float64(pi)  # untested
        else
            return parse(type, parameter_value.value)
        end
    catch
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a valid $(type)")  # untested
    end
end

"""
    function parse_parameter_value(
        parse_value::Function,
        parameters_values::Dict{String, Token},
        parameter_name::AbstractString,
        default::Any,
    )::Any

Parse an operation parameter.
"""
function parse_parameter_value(
    parse_value::Function,
    parameters_values::Dict{String, Token},
    parameter_name::AbstractString,
    default::Any,
)::Any
    parameter_value = get(parameters_values, parameter_name, missing)
    if parameter_value === missing
        return default
    else
        return parse_value(parameter_value)
    end
end

"""
    Abs([; dtype::Maybe{Type} = nothing])

Element-wise operation that converts every element to its absolute value.

**Parameters**

`dtype` - The default output data type is the [`unsigned_dtype_for`](@ref) the input data type.
"""
struct Abs <: EltwiseOperation
    dtype::Maybe{Type}
end
@query_operation Abs

function Abs(; dtype::Maybe{Type} = nothing)::Abs  # untested
    return Abs(dtype)
end

function Abs(parameters_values::Dict{String, Token})::Abs
    dtype = parse_parameter_value(parameters_values, "dtype", nothing) do parameters_value
        return parse_int_dtype_value("Abs", "dtype", parameters_value)  # untested
    end
    return Abs(dtype)
end

function compute_eltwise(
    operation::Abs,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: StorageNumber}
    dtype = unsigned_dtype_for(eltype(input), operation.dtype)
    output = similar(input, dtype)
    output .= abs.(input)  # NOJET
    return output
end

function compute_eltwise(operation::Abs, input::StorageNumber)::StorageNumber  # untested
    dtype = unsigned_dtype_for(eltype(input), operation.dtype)
    return dtype(abs(input))
end

"""
    Round([; dtype::Maybe{Type} = nothing])

Element-wise operation that converts every element to the nearest integer value.

**Parameters**

`dtype` - By default, uses the [`int_dtype_for`](@ref) the input data type.
"""
struct Round <: EltwiseOperation
    dtype::Maybe{Type}
end
@query_operation Round

function Round(; dtype::Maybe{Type} = nothing)::Round  # untested
    return Round(dtype)
end

function Round(parameters_values::Dict{String, Token})::Round  # untested
    dtype = parse_parameter_value(parameters_values, "dtype", nothing) do parameters_value
        return parse_int_dtype_value("Round", "dtype", parameters_value)
    end
    return Round(dtype)
end

function compute_eltwise(  # untested
    operation::Round,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: StorageNumber}
    return round.(int_dtype_for(eltype(input), operation.dtype), input)
end

function compute_eltwise(operation::Round, input::StorageNumber)::StorageNumber  # untested
    return round(int_dtype_for(eltype(input), operation.dtype), input)
end

"""
    Log(; dtype::Maybe{Type} = nothing, base::Float64 = e, eps::Float64 = 0.0)

Element-wise operation that converts every element to its logarithm.

**Parameters**:

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.

`base` - The base of the logarithm. By default uses `e` (that is, computes the natural logarithm), which isn't
convenient, but is the standard.

`eps` - Added to the input before computing the logarithm, to handle zero input data. By default is zero.
"""
struct Log <: EltwiseOperation
    dtype::Maybe{Type}
    base::Float64
    eps::Float64
end
@query_operation Log

function Log(; dtype::Maybe{Type} = nothing, base::Float64 = Float64(e), eps::Float64 = 0.0)  # untested
    return Log(dtype, base, eps)
end

function Log(parameters_values::Dict{String, Token})::Log
    dtype = parse_parameter_value(parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value("Log", "dtype", parameter_value)  # untested
    end

    base = parse_parameter_value(parameters_values, "base", Float64(e)) do parameter_value
        base = parse_number_value("Log", "base", parameter_value, Float64)
        if base <= 0
            error_invalid_parameter_value("Log", "base", parameter_value, "positive")  # untested
        end
        return base
    end

    eps = parse_parameter_value(parameters_values, "eps", 0.0) do parameter_value
        eps = parse_number_value("Log", "eps", parameter_value, Float64)
        if eps < 0
            error_invalid_parameter_value("Log", "eps", parameter_value, "positive")  # untested
        end
        return eps
    end

    return Log(dtype, base, eps)
end

function compute_eltwise(
    operation::Log,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: StorageNumber}
    dtype = float_dtype_for(eltype(input), operation.dtype)
    output = similar(input, dtype)
    output .= input
    output .+= dtype(operation.eps)
    output .= log.(output)
    output ./= log(dtype(operation.base))
    return output
end

function compute_eltwise(operation::Log, input::T)::T where {T <: StorageNumber}
    dtype = float_dtype_for(eltype(input), operation.dtype)
    return log(dtype(dtype(input) + dtype(operation.eps))) / log(dtype(operation.base))
end

"""
    Sum(; dtype::Maybe{Type} = nothing)

Reduction operation that sums elements.

**Parameters**

`dtype` - By default, uses the [`sum_dtype_for`](@ref) the input data type.
"""
struct Sum <: ReductionOperation
    dtype::Maybe{Type}
end
@query_operation Sum

function Sum(; dtype::Maybe{Type} = nothing)::Sum
    return Sum(dtype)
end

function Sum(parameters_values::Dict{String, Token})::Sum
    dtype = parse_parameter_value(parameters_values, "dtype", nothing) do parameters_value
        return parse_dtype_value("Sum", "dtype", parameters_value)  # untested
    end
    return Sum(dtype)
end

function compute_reduction(operation::Sum, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, T)
    result = Vector{dtype}(undef, size(input)[2])
    result .= transpose(sum(input; dims = 1))
    return result
end

function compute_reduction(operation::Sum, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, T)
    return dtype(sum(input))
end

function reduction_result_type(operation::Sum, eltype::Type)::Type
    return sum_dtype_for(eltype, operation.dtype)
end

"""
    Max()

Reduction operation that returns the maximal element.

**Parameters**

`dtype` - By default, the output data type is identical to the input data type.
"""
struct Max <: ReductionOperation end
@query_operation Max

function Max(parameters_values::Dict{String, Token})::Max
    return Max()
end

function compute_reduction(operation::Max, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}  # untested
    return transpose(maximum(input; dims = 1))  # NOJET
end

function compute_reduction(operation::Max, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    return maximum(input)
end

function reduction_result_type(operation::Max, eltype::Type)::Type
    return eltype
end

end # module
