"""
A `Daf` query can use operations to process the data: [`EltwiseOperation`](@ref)s that preserve the shape of the data,
and [`ReductionOperation`](@ref)s that reduce a matrix to a vector, or a vector to a scalar.
"""
module Operations

using Base.Threads
using Daf.Registry
using Daf.StorageTypes
using Daf.Tokens
using Statistics
using StatsBase
using SparseArrays

import Base.MathConstants.e
import Base.MathConstants.pi
import Daf.Registry.compute_eltwise
import Daf.Registry.compute_reduction
import Daf.Registry.reduction_result_type
import Daf.Tokens.error_at_token
import Daf.Tokens.Token
import Distributed.@everywhere

using Base.MathConstants
using Daf.GenericTypes

export Abs
export Clamp
export Convert
export Count
export Fraction
export Log
export Max
export Median
export Mean
export GeoMean
export Min
export Mode
export Quantile
export Round
export Significant
export Std
export StdN
export Sum
export Var
export VarN
export error_invalid_parameter_value
export float_dtype_for
export int_dtype_for
export parse_float_dtype_value
export parse_int_dtype_value
export parse_number_dtype_value
export parse_number_value
export parse_parameter_value
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
    if dtype === nothing
        global FLOAT_DTYPE_FOR
        return FLOAT_DTYPE_FOR[element_type]
    else
        @assert dtype <: AbstractFloat
        return dtype
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
function int_dtype_for(
    element_type::Type{T},
    dtype::Maybe{Type{D}},
)::Type where {T <: StorageNumber, D <: StorageNumber}
    if dtype === nothing
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
    Float32 => Float32,
    Float64 => Float64,
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
    if dtype === nothing
        global SUM_DTYPE_FOR
        return SUM_DTYPE_FOR[element_type]
    else
        return dtype
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
    Float32 => Float32,
    Float64 => Float64,
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
    if dtype === nothing
        global UNSIGNED_DTYPE_FOR
        return UNSIGNED_DTYPE_FOR[element_type]
    else
        return dtype
    end
end

"""
    error_invalid_parameter_value(
        operation_name::Token,
        parameter_name::AbstractString,
        parameter_value::Token,
        must_be::AbstractString,
    )::Nothing

Complain that an operation parameter value is not valid.
"""
function error_invalid_parameter_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
    must_be::AbstractString,
)::Union{}
    return error_at_token(
        parameter_value,
        "invalid value: \"$(escape_string(parameter_value.value))\"\n" *
        "value must be: $(must_be)\n" *
        "for the parameter: $(parameter_name)\n" *
        "for the operation: $(operation_name.value)",
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
    parse_number_dtype_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Parse the `dtype` operation parameter.

Valid names are `{B,b}ool`, `{UI,ui,I,i}nt{8,16,32,64}` and `{F,f}loat{32,64}`.
"""
function parse_number_dtype_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    dtype_name = parameter_value.value
    global DTYPE_BY_NAME
    dtype_type = get(DTYPE_BY_NAME, dtype_name, missing)
    if dtype_type === missing
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a number type")
    end
    return DTYPE_BY_NAME[dtype_name]
end

"""
    parse_int_dtype_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Similar to [`parse_number_dtype_value`](@ref), but only accept integer (signed or unsigned) types.
"""
function parse_int_dtype_value(  # untested
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    dtype = parse_number_dtype_value(operation_name, parameter_name, parameter_value)
    if dtype <: Integer
        return dtype
    else
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "an integer type")
    end
end

"""
    parse_float_dtype_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Similar to [`parse_number_dtype_value`](@ref), but only accept floating point types.
"""
function parse_float_dtype_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    dtype = parse_number_dtype_value(operation_name, parameter_name, parameter_value)
    if dtype <: AbstractFloat
        return dtype
    else
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a float type")
    end
end

"""
    parse_number_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
        type::Type{T},
    )::T where {T <: StorageNumber}

Parse a numeric operation parameter.
"""
function parse_number_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
    type::Type{T},
)::T where {T <: StorageNumber}
    if parameter_value.value == "e"
        return Float64(e)
    elseif parameter_value.value == "pi"
        return Float64(pi)
    end

    try
        return parse(type, parameter_value.value)
    catch
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a valid $(type)")
    end
end

"""
    parse_parameter_value(
        parse_value::Function,
        operation_name::Token,
        operation_kind::AbstractString,
        parameters_values::Dict{String, Token},
        parameter_name::AbstractString,
        default::Any,
    )::Any

Parse an operation parameter.
"""
function parse_parameter_value(
    parse_value::Function,
    operation_name::Token,
    operation_kind::AbstractString,
    parameters_values::Dict{String, Token},
    parameter_name::AbstractString,
    default::Any,
)::Any
    parameter_value = get(parameters_values, parameter_name, nothing)
    if parameter_value !== nothing
        parameter_value = parse_value(parameter_value)
    end

    if parameter_value !== nothing
        return parameter_value
    elseif default !== missing
        return default
    else
        error_at_token(
            operation_name,
            "missing required parameter: $(parameter_name)\n" *
            "for the $(operation_kind) operation: $(operation_name.value)",
        )
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

function Abs(; dtype::Maybe{Type} = nothing)::Abs
    return Abs(dtype)
end

function Abs(operation_name::Token, parameters_values::Dict{String, Token})::Abs
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_number_dtype_value(operation_name, "dtype", parameter_value)
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

function compute_eltwise(operation::Abs, input::StorageNumber)::StorageNumber
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

function Round(; dtype::Maybe{Type} = nothing)::Round
    return Round(dtype)
end

function Round(operation_name::Token, parameters_values::Dict{String, Token})::Round
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_number_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Round(dtype)
end

function compute_eltwise(
    operation::Round,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: StorageNumber}
    return round.(int_dtype_for(eltype(input), operation.dtype), input)
end

function compute_eltwise(operation::Round, input::StorageNumber)::StorageNumber
    return round(int_dtype_for(eltype(input), operation.dtype), input)
end

"""
    Clamp([; min::Maybe{StorageNumber} = nothing, max::Maybe{StorageNumber} = nothing])

Element-wise operation that converts every element to a value inside a range.

**Parameters**

`min` - If specified, values lower than this will be increased to this value.

`max` - If specified, values higher than this will be increased to this value.

!!! note

    At least one of `min` and `max` must be specified.
"""
struct Clamp <: EltwiseOperation
    min::Float64
    max::Float64
end
@query_operation Clamp

function Clamp(; min::StorageNumber = -Inf, max::StorageNumber = Inf)::Clamp
    @assert min < max
    return Clamp(Float64(min), Float64(max))
end

function Clamp(operation_name::Token, parameters_values::Dict{String, Token})::Clamp
    min = parse_parameter_value(operation_name, "eltwise", parameters_values, "min", -Inf) do parameter_value
        return parse_number_value(operation_name, "min", parameter_value, Float64)
    end
    max = parse_parameter_value(operation_name, "eltwise", parameters_values, "max", Inf) do parameter_value
        value = parse_number_value(operation_name, "max", parameter_value, Float64)
        if value <= min
            error_invalid_parameter_value(operation_name, "max", parameter_value, "larger than min ($(min))")
        end
        return value
    end
    return Clamp(min, max)
end

function compute_eltwise(
    operation::Clamp,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix{T}, StorageVector{T}} where {T <: StorageNumber}
    output = copy(input)
    clamp!(output, operation.min, operation.max)
    return output
end

function compute_eltwise(operation::Clamp, input::T)::T where {T <: StorageNumber}
    output = clamp(input, operation.min, operation.max)
    return T(output)
end

"""
    Convert([; dtype::Type])

Element-wise operation that converts every element to a given data type.

**Parameters**

`dtype` - The data type to convert to. There's no default.
"""
struct Convert <: EltwiseOperation
    dtype::Type
end
@query_operation Convert

function Convert(; dtype::Type{T})::Convert where {T <: StorageNumber}
    return Convert(dtype)
end

function Convert(operation_name::Token, parameters_values::Dict{String, Token})::Convert
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", missing) do parameter_value
        return parse_number_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Convert(dtype)
end

function compute_eltwise(
    operation::Convert,
    input::Union{StorageMatrix{T}, StorageVector{T}},
)::Union{StorageMatrix, StorageVector} where {T <: StorageNumber}
    return operation.dtype.(input)
end

function compute_eltwise(operation::Convert, input::StorageNumber)::StorageNumber
    return operation.dtype(input)
end

"""
    Fraction([; dtype::Type])

Element-wise operation that converts every element to its fraction out of the total. If the total is zero, all the
fractions are also set to zero. This implicitly assumes (but does not enforce) that all the entry value(s) are positive.

For matrices, each entry becomes its fraction out of the total of the column it belongs to. For vectors, each entry
becomes its fraction out of the total of the vector. For scalars, this operation makes no sense so fails with an error.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.
"""
struct Fraction <: EltwiseOperation
    dtype::Maybe{Type}
end
@query_operation Fraction

function Fraction(; dtype::Maybe{Type{T}} = nothing)::Fraction where {T <: StorageNumber}
    @assert dtype === nothing || dtype <: AbstractFloat
    return Fraction(dtype)
end

function Fraction(operation_name::Token, parameters_values::Dict{String, Token})::Fraction
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Fraction(dtype)
end

function compute_eltwise(operation::Fraction, input::StorageMatrix{T})::StorageMatrix where {T <: StorageNumber}
    dtype = float_dtype_for(eltype(input), operation.dtype)
    output = similar(input, dtype)
    output .= input
    columns_sums = sum(output; dims = 1)
    columns_sums[columns_sums .== 0] .= 1
    output ./= columns_sums
    output[output .== Inf] .= 0
    output[output .== -Inf] .= 0
    return output
end

function compute_eltwise(operation::Fraction, input::StorageVector{T})::StorageVector where {T <: StorageNumber}
    dtype = float_dtype_for(eltype(input), operation.dtype)
    vector_sum = sum(input)
    if vector_sum == 0
        output = zeros(dtype, length(input))
    else
        output = similar(input, dtype)
        output .= input
        output ./= vector_sum
    end
    return output
end

function compute_eltwise(::Fraction, ::StorageNumber)::StorageNumber
    return error("applying Fraction eltwise operation to a scalar")
end

"""
    Log(; dtype::Maybe{Type} = nothing, base::StorageNumber = e, eps::StorageNumber = 0)

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

function Log(; dtype::Maybe{Type} = nothing, base::StorageNumber = Float64(e), eps::StorageNumber = 0.0)::Log
    @assert dtype === nothing || dtype <: AbstractFloat
    @assert base > 0
    @assert eps >= 0
    return Log(dtype, Float64(base), Float64(eps))
end

function Log(operation_name::Token, parameters_values::Dict{String, Token})::Log
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end

    base = parse_parameter_value(operation_name, "eltwise", parameters_values, "base", Float64(e)) do parameter_value
        base = parse_number_value(operation_name, "base", parameter_value, Float64)
        if base <= 0
            error_invalid_parameter_value(operation_name, "base", parameter_value, "positive")
        end
        return base
    end

    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        eps = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if eps < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
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
    output .+= operation.eps
    map!(log, output, output)
    if operation.base != Float64(e)
        output ./= log(operation.base)
    end
    return output
end

function compute_eltwise(operation::Log, input::T)::StorageNumber where {T <: StorageNumber}
    dtype = float_dtype_for(eltype(input), operation.dtype)
    return dtype(log(Float64(input) + operation.eps) / log(operation.base))
end

"""
    Significant(; high::StorageNumber, low::Maybe{StorageNumber} = nothing)

Element-wise operation that zeros all "insignificant" values. Significant values have a high absolute value. This is
typically used to prune matrices of effect sizes (log of ratio between a baseline and some result) for heatmap display.
For example, log base 2 of gene expression ratio is typically considered significant if it is at least 3 (that is, a
ratio at least 8x or at most 1/8x); for genes that have a significant effect, we typically display all entries with a
log of at least 2 (that is, a ratio of at least 4x or at most 1/4x).

For scalars, this operation makes no sense so fails with an error.

**Parameters**:

`high` - A value is considered significant if its absolute value is higher than this. If all values in a vector (or a
matrix column) are less than this, then all the vector (or matrix column) entries are zeroed. There's no default.

`low` - If there is at least one significant value in a vector (or a matrix column), then zero all entries that are
lower than this. By default, this is the same as the `high` value. Setting it to a lower value will preserve more
entries, but only for vectors (or matrix columns) which contain at least some significant data.
"""
struct Significant <: EltwiseOperation
    high::Float64
    low::Float64
end
@query_operation Significant

function Significant(; high::StorageNumber, low::Maybe{StorageNumber} = nothing)::Significant
    if low === nothing
        low = high
    end
    @assert high > 0
    @assert low > 0  # NOJET
    @assert low <= high
    return Significant(Float64(high), Float64(low))  # NOJET
end

function Significant(operation_name::Token, parameters_values::Dict{String, Token})::Significant
    high = parse_parameter_value(operation_name, "eltwise", parameters_values, "high", missing) do parameter_value
        high = parse_number_value(operation_name, "high", parameter_value, Float64)
        if high <= 0
            error_invalid_parameter_value(operation_name, "high", parameter_value, "positive")
        end
        return high
    end

    low = parse_parameter_value(operation_name, "eltwise", parameters_values, "low", high) do parameter_value
        low = parse_number_value(operation_name, "low", parameter_value, Float64)
        if low < 0
            error_invalid_parameter_value(operation_name, "low", parameter_value, "not negative")
        end
        if low > high
            error_invalid_parameter_value(operation_name, "low", parameter_value, "at most high ($(high))")
        end
        return low
    end

    return Significant(high, low)
end

function compute_eltwise(operation::Significant, input::StorageMatrix{T})::StorageMatrix{T} where {T <: StorageNumber}
    output = copy(input)
    if output isa SparseMatrixCSC
        @threads for column_index in 1:size(output, 2)
            first = output.colptr[column_index]
            last = output.colptr[column_index + 1] - 1
            if first <= last
                column_vector = @view output.nzval[first:last]
                significant!(column_vector, operation.high, operation.low)
            end
        end
        dropzeros!(output)
    else
        n_columns = size(output, 2)
        @threads for column_index in 1:n_columns
            column_vector = @view output[:, column_index]
            significant!(column_vector, operation.high, operation.low)
        end
    end
    return output
end

function compute_eltwise(operation::Significant, input::StorageVector{T})::SparseVector{T} where {T <: StorageNumber}
    output = copy(input)
    significant!(output, operation.high, operation.low)
    return output
end

function significant!(
    vector::StorageVector{T},
    high::StorageNumber,
    low::StorageNumber,
)::Nothing where {T <: StorageNumber}
    high = eltype(vector)(high)
    low = eltype(vector)(low)
    not_high_mask = (-high .< vector) .& (vector .< high)
    if all(not_high_mask)
        vector .= 0
    else
        if low == high
            not_low_mask = not_high_mask
        else
            not_low_mask = (-low .< vector) .& (vector .< low)
        end
        vector[not_low_mask] .= 0
    end
    return nothing
end

function compute_eltwise(::Significant, ::T)::T where {T <: StorageNumber}
    return error("applying Significant eltwise operation to a scalar")
end

"""
    Count(; dtype::Maybe{Type} = nothing)

Reduction operation that counts elements. This is useful when using `GroupBy` queries to count the number of elements in
each group.

**Parameters**

`dtype` - By default, uses `UInt32`.
"""
struct Count <: ReductionOperation
    dtype::Maybe{Type}
end
@query_operation Count

function Count(; dtype::Maybe{Type} = nothing)::Count
    @assert dtype === nothing || dtype <: Real
    return Count(dtype)
end

function Count(operation_name::Token, parameters_values::Dict{String, Token})::Count
    dtype = parse_parameter_value(operation_name, "reduction", parameters_values, "dtype", nothing) do parameter_value
        return parse_number_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Count(dtype)
end

function compute_reduction(operation::Count, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, T)
    result = Vector{dtype}(undef, size(input, 2))
    result .= size(input, 1)
    return result
end

function compute_reduction(operation::Count, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, T)
    return dtype(length(input))
end

function reduction_result_type(operation::Count, ::Type)::Type
    return operation.dtype === nothing ? UInt32 : operation.dtype
end

"""
    Mode()

Reduction operation that returns the most frequent value in the input (the "mode").
"""
struct Mode <: ReductionOperation end
@query_operation Mode

function Mode(::Token, ::Dict{String, Token})::Mode
    return Mode()
end

function compute_reduction(operation::Mode, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    output = Vector{reduction_result_type(operation, eltype(input))}(undef, size(input, 2))
    @threads for column_index in 1:length(output)
        column_vector = @view input[:, column_index]
        output[column_index] = mode(column_vector)
    end
    return output
end

function compute_reduction(::Mode, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    return mode(input)
end

function reduction_result_type(::Mode, eltype::Type)::Type
    return eltype
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
    @assert dtype === nothing || dtype <: Real
    return Sum(dtype)
end

function Sum(operation_name::Token, parameters_values::Dict{String, Token})::Sum
    dtype = parse_parameter_value(operation_name, "reduction", parameters_values, "dtype", nothing) do parameter_value
        return parse_number_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Sum(dtype)
end

function compute_reduction(operation::Sum, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, T)
    result = Vector{dtype}(undef, size(input, 2))
    sum!(transpose(result), input)
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
"""
struct Max <: ReductionOperation end
@query_operation Max

function Max(::Token, ::Dict{String, Token})::Max
    return Max()
end

function compute_reduction(::Max, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    return vec(maximum(input; dims = 1))  # NOJET
end

function compute_reduction(::Max, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    return maximum(input)
end

function reduction_result_type(::Max, eltype::Type)::Type
    return eltype
end

"""
    Min()

Reduction operation that returns the minimal element.
"""
struct Min <: ReductionOperation end
@query_operation Min

function Min(::Token, ::Dict{String, Token})::Min
    return Min()
end

function compute_reduction(::Min, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    return vec(minimum(input; dims = 1))
end

function compute_reduction(::Min, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    return minimum(input)
end

function reduction_result_type(::Min, eltype::Type)::Type
    return eltype
end

"""
    Median(; dtype::Maybe{Type} = nothing)

Reduction operation that returns the median value.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.
"""
struct Median <: ReductionOperation
    dtype::Maybe{Type}
end
@query_operation Median

function Median(; dtype::Maybe{Type} = nothing)::Median
    @assert dtype === nothing || dtype <: AbstractFloat
    return Median(dtype)
end

function Median(operation_name::Token, parameters_values::Dict{String, Token})::Median
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Median(dtype)
end

function compute_reduction(operation::Median, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return convert(AbstractVector{dtype}, vec(median(input; dims = 1)))  # NOJET
end

function compute_reduction(operation::Median, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(median(input))
end

function reduction_result_type(operation::Median, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    Quantile(; dtype::Maybe{Type} = nothing, p::StorageNumber)

Reduction operation that returns the quantile value, that is, a value such that a certain fraction of the values is
lower.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.

`p` - The fraction of values below the result (e.g., the 0 computes the minimum, the 0.5 computes the median, and 1.0
computes the maximum). There's no default.
"""
struct Quantile <: ReductionOperation
    dtype::Maybe{Type}
    p::Float64
end
@query_operation Quantile

function Quantile(; dtype::Maybe{Type} = nothing, p::StorageNumber)::Quantile
    @assert dtype === nothing || dtype <: AbstractFloat
    @assert 0 <= p && p <= 1
    return Quantile(dtype, p)
end

function Quantile(operation_name::Token, parameters_values::Dict{String, Token})::Quantile
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    p = parse_parameter_value(operation_name, "eltwise", parameters_values, "p", missing) do parameter_value
        p = parse_number_value(operation_name, "p", parameter_value, Float64)
        if p < 0
            error_invalid_parameter_value(operation_name, "p", parameter_value, "at least 0")
        end
        if p > 1
            error_invalid_parameter_value(operation_name, "p", parameter_value, "at most 1")
        end
        return p
    end
    return Quantile(dtype, p)
end

function compute_reduction(operation::Quantile, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    output = Vector{dtype}(undef, size(input, 2))
    @threads for column_index in 1:length(output)
        column_vector = @view input[:, column_index]
        output[column_index] = quantile(column_vector, operation.p)  # NOJET
    end
    return output
end

function compute_reduction(operation::Quantile, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(quantile(input, operation.p))
end

function reduction_result_type(operation::Quantile, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    Mean(; dtype::Maybe{Type} = nothing)

Reduction operation that returns the mean value.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.
"""
struct Mean <: ReductionOperation
    dtype::Maybe{Type}
end
@query_operation Mean

function Mean(; dtype::Maybe{Type} = nothing)::Mean
    @assert dtype === nothing || dtype <: AbstractFloat
    return Mean(dtype)
end

function Mean(operation_name::Token, parameters_values::Dict{String, Token})::Mean
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Mean(dtype)
end

function compute_reduction(operation::Mean, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return convert(AbstractVector{dtype}, vec(mean(input; dims = 1)))  # NOJET
end

function compute_reduction(operation::Mean, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(mean(input))  # NOJET
end

function reduction_result_type(operation::Mean, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    GeoMean(; dtype::Maybe{Type} = nothing, eps::StorageNumber = 0.0)

Reduction operation that returns the geometric mean value.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.

`eps` - The regularization factor added to each value and subtracted from the raw geo-mean, to deal with zero values.
"""
struct GeoMean <: ReductionOperation
    dtype::Maybe{Type}
    eps::Float64
end
@query_operation GeoMean

function GeoMean(; dtype::Maybe{Type} = nothing, eps::StorageNumber = 0)::GeoMean
    @assert dtype === nothing || dtype <: AbstractFloat
    @assert eps >= 0
    return GeoMean(dtype, eps)
end

function GeoMean(operation_name::Token, parameters_values::Dict{String, Token})::GeoMean
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        eps = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if eps < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return eps
    end
    return GeoMean(dtype, eps)
end

function compute_reduction(operation::GeoMean, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    if operation.eps == 0
        return convert(AbstractVector{dtype}, geomean.(eachcol(input)))  # NOJET
    else
        return convert(AbstractVector{dtype}, geomean.(eachcol(input .+ operation.eps)) .- operation.eps)  # NOJET
    end
end

function compute_reduction(operation::GeoMean, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    if operation.eps == 0
        return dtype(geomean(input))  # NOJET
    else
        return dtype(geomean(input .+ operation.eps) - operation.eps)  # NOJET
    end
end

function reduction_result_type(operation::GeoMean, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    Var(; dtype::Maybe{Type} = nothing)

Reduction operation that returns the (uncorrected) variance of the values.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.
"""
struct Var <: ReductionOperation
    dtype::Maybe{Type}
end
@query_operation Var

function Var(; dtype::Maybe{Type} = nothing)::Var
    @assert dtype === nothing || dtype <: AbstractFloat
    return Var(dtype)
end

function Var(operation_name::Token, parameters_values::Dict{String, Token})::Var
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Var(dtype)
end

function compute_reduction(operation::Var, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return convert(AbstractVector{dtype}, vec(var(input; dims = 1, corrected = false)))
end

function compute_reduction(operation::Var, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(var(input; corrected = false))
end

function reduction_result_type(operation::Var, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    VarN(; dtype::Maybe{Type} = nothing, eps::StorageNumber = 0.0)

Reduction operation that returns the (uncorrected) variance of the values, normalized (divided) by the mean of the
values.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.

`eps` - Added to the mean before computing the division, to handle zero input data. By default is zero.
"""
struct VarN <: ReductionOperation
    dtype::Maybe{Type}
    eps::Float64
end
@query_operation VarN

function VarN(; dtype::Maybe{Type} = nothing, eps::StorageNumber = 0)::VarN
    @assert dtype === nothing || dtype <: AbstractFloat
    @assert eps >= 0
    return VarN(dtype, eps)
end

function VarN(operation_name::Token, parameters_values::Dict{String, Token})::VarN
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        eps = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if eps < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return eps
    end
    return VarN(dtype, eps)
end

function compute_reduction(operation::VarN, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    vars = convert(AbstractVector{dtype}, vec(var(input; dims = 1, corrected = false)))
    means = convert(AbstractVector{dtype}, vec(mean(input; dims = 1)))
    means .+= operation.eps
    vars ./= means
    return vars
end

function compute_reduction(operation::VarN, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(var(input; corrected = false)) / dtype((Float64(mean(input)) + operation.eps))
end

function reduction_result_type(operation::VarN, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    Std(; dtype::Maybe{Type} = nothing)

Reduction operation that returns the (uncorrected) standard deviation of the values.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.
"""
struct Std <: ReductionOperation
    dtype::Maybe{Type}
end
@query_operation Std

function Std(; dtype::Maybe{Type} = nothing)::Std
    @assert dtype === nothing || dtype <: AbstractFloat
    return Std(dtype)
end

function Std(operation_name::Token, parameters_values::Dict{String, Token})::Std
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    return Std(dtype)
end

function compute_reduction(operation::Std, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return convert(AbstractVector{dtype}, vec(std(input; dims = 1, corrected = false)))
end

function compute_reduction(operation::Std, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(std(input; corrected = false))
end

function reduction_result_type(operation::Std, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

"""
    StdN(; dtype::Maybe{Type} = nothing, eps::StorageNumber = 0)

Reduction operation that returns the (uncorrected) standard deviation of the values, normalized (divided) by the mean
value.

**Parameters**

`dtype` - The default output data type is the [`float_dtype_for`](@ref) of the input data type.

`eps` - Added to the mean before computing the division, to handle zero input data. By default is zero.
"""
struct StdN <: ReductionOperation
    dtype::Maybe{Type}
    eps::Float64
end
@query_operation StdN

function StdN(; dtype::Maybe{Type} = nothing, eps::StorageNumber = 0)::StdN
    @assert dtype === nothing || dtype <: AbstractFloat
    @assert eps >= 0
    return StdN(dtype, eps)
end

function StdN(operation_name::Token, parameters_values::Dict{String, Token})::StdN
    dtype = parse_parameter_value(operation_name, "eltwise", parameters_values, "dtype", nothing) do parameter_value
        return parse_float_dtype_value(operation_name, "dtype", parameter_value)
    end
    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        eps = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if eps < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return eps
    end
    return StdN(dtype, eps)
end

function compute_reduction(operation::StdN, input::StorageMatrix{T})::StorageVector where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    stds = convert(AbstractVector{dtype}, vec(std(input; dims = 1, corrected = false)))
    means = convert(AbstractVector{dtype}, vec(mean(input; dims = 1)))
    means .+= operation.eps
    stds ./= means
    return stds
end

function compute_reduction(operation::StdN, input::StorageVector{T})::StorageNumber where {T <: StorageNumber}
    dtype = reduction_result_type(operation, eltype(input))
    return dtype(std(input; corrected = false)) / dtype(mean(input) + operation.eps)
end

function reduction_result_type(operation::StdN, eltype::Type)::Type
    return float_dtype_for(eltype, operation.dtype)
end

end # module
