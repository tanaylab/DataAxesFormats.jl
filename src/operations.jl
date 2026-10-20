"""
A `Daf` query can use operations to process the data: [`EltwiseOperation`](@ref)s that preserve the shape of the data,
and [`ReductionOperation`](@ref)s that reduce a matrix to a vector, or a vector to a scalar.
"""
module Operations

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
export compute_grouped_reduction
export error_invalid_parameter_value
export float_type_for
export int_type_for
export parse_float_type_value
export parse_int_type_value
export parse_number_type_value
export parse_number_value
export parse_parameter_value
export sum_type_for
export unsigned_type_for

using ..Registry
using ..StorageTypes
using ..Tokens
using Base.MathConstants
using Base.Threads
using SparseArrays
using StatsBase
using LoopVectorization
using TanayLabUtilities

import ..Registry.compute_eltwise
import ..Registry.compute_reduction
import ..Registry.empty_reduction_state
import ..Registry.final_reduction_value
import ..Registry.reduction_result_type
import ..Registry.supports_grouped_reduction
import ..Registry.supports_strings
import ..Registry.update_reduction_state
import ..Tokens.error_at_token
import ..Tokens.Token
import Base.MathConstants.e
import Base.MathConstants.pi

const FLOAT_DTYPE_FOR = Dict{Type, Type{<:AbstractFloat}}(
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
    float_type_for(
        element_type::Type{<:StorageReal},
        type::Maybe{Type{<:StorageReal}}
    )::Type{<:AbstractFloat}

Given an input `element_type`, return the data type to use for the result of an operation that always produces floating
point values (e.g., [`Log`](@ref)). If `type` isn't  `nothing`, it is returned instead.
"""
function float_type_for(element_type::Type{<:StorageReal}, type::Maybe{Type{<:StorageReal}})::Type{<:AbstractFloat}
    if type === nothing
        return FLOAT_DTYPE_FOR[element_type]
    else
        return type
    end
end

const INT_DTYPE_FOR = Dict{Type, Type{<:Integer}}(
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
    int_type_for(
        element_type::Type{<:StorageReal},
        type::Maybe{Type{<:StorageReal}}
    )::Type{<:Integer}

Given an input `element_type`, return the data type to use for the result of an operation that always produces integer
values (e.g., [`Round`](@ref)). If `type` isn't `nothing`, it is returned instead.
"""
function int_type_for(element_type::Type{<:StorageReal}, type::Maybe{Type{<:StorageReal}})::Type{<:Integer}
    if type === nothing
        return INT_DTYPE_FOR[element_type]
    else
        return type
    end
end

const SUM_DTYPE_FOR = Dict{Type, Type{<:StorageReal}}(
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
    sum_type_for(
        element_type::Type{<:StorageReal},
        type::Maybe{Type{<:StorageReal}}
    )::Type{<:StorageReal}

Given an input `element_type`, return the data type to use for the result of an operation that sums many such values
values (e.g., [`Sum`](@ref)). If `type` isn't `nothing`, it is returned instead.

This keeps floating point and 64-bit types as-is, but increases any small integer types to the matching 32 bit type
(e.g., an input type of `UInt8` will have a sum type of `UInt32`).
"""
function sum_type_for(element_type::Type{<:StorageReal}, type::Maybe{Type{<:StorageReal}})::Type{<:StorageReal}
    if type === nothing
        return SUM_DTYPE_FOR[element_type]
    else
        return type
    end
end

const UNSIGNED_DTYPE_FOR = Dict{Type, Type}(
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
    unsigned_type_for(
        element_type::Type{<:StorageReal},
        type::Maybe{Type{<:StorageReal}}
    )::Type

Given an input `element_type`, return the data type to use for the result of an operation that discards the sign of the
value (e.g., [`Abs`](@ref)). If `type` isn't `nothing`, it is returned instead.
"""
function unsigned_type_for(element_type::Type{<:StorageReal}, type::Maybe{Type{<:StorageReal}})::Type
    if type === nothing
        return UNSIGNED_DTYPE_FOR[element_type]
    else
        return type
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
        """
        invalid value: "$(escape_string(parameter_value.value))"
        value must be: $(must_be)
        for the parameter: $(parameter_name)
        for the operation: $(operation_name.value)
        """,
    )
end

const DTYPE_BY_NAME = Dict{String, Maybe{Type}}(  # NOLINT
    "bool" => Bool,
    "Bool" => Bool,
    "int" => Int,
    "Int" => Int,
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
    parse_number_type_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Parse the `type` operation parameter.

Valid names are `{B,b}ool`, `{UI,ui,I,i}nt{8,16,32,64}` and `{F,f}loat{32,64}`.
"""
function parse_number_type_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    type_name = parameter_value.value
    type = get(DTYPE_BY_NAME, type_name, missing)
    if type === missing
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "a number type")
    end
    return type
end

"""
    parse_int_type_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Similar to [`parse_number_type_value`](@ref), but only accept integer (signed or unsigned) types.
"""
function parse_int_type_value(  # UNTESTED
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    type = parse_number_type_value(operation_name, parameter_name, parameter_value)
    if type <: Integer
        return type
    else
        error_invalid_parameter_value(operation_name, parameter_name, parameter_value, "an integer type")
    end
end

"""
    parse_float_type_value(
        operation_name::AbstractString,
        parameter_name::AbstractString,
        parameter_value::Token,
    )::Maybe{Type}

Similar to [`parse_number_type_value`](@ref), but only accept floating point types.
"""
function parse_float_type_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
)::Maybe{Type}
    type = parse_number_type_value(operation_name, parameter_name, parameter_value)
    if type <: AbstractFloat
        return type
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
    )::T where {T <: StorageReal}

Parse a numeric operation parameter.
"""
function parse_number_value(
    operation_name::Token,
    parameter_name::AbstractString,
    parameter_value::Token,
    type::Type{T},
)::T where {T <: StorageReal}
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
            """
            missing required parameter: $(parameter_name)
            for the $(operation_kind) operation: $(operation_name.value)
            """,
        )
    end
end

# An element-wise operation on a sparse matrix, computed on its stored values. If the `value_of` zero is zero, the
# result is sparse, with the same structure (sharing its indices with the input). Otherwise it is dense, filled with the
# `value_of` zero.
function compute_sparse_eltwise(
    value_of::Function,
    input::AbstractMatrix,
    type::Type,
    name::AbstractString,
)::StorageMatrix
    input = plain_array(input)
    if !(input isa SparseMatrixCSC)
        return transpose(compute_sparse_eltwise(value_of, parent(input), type, name))
    end

    zero_value = type(value_of(zero(eltype(input))))
    value_per_stored = nonzeros(input)
    if zero_value == 0
        output_value_per_stored = Vector{type}(undef, length(value_per_stored))
        map!(value -> type(value_of(value)), output_value_per_stored, value_per_stored)
        return SparseMatrixCSC(
            size(input, 1),
            size(input, 2),
            SparseArrays.getcolptr(input),
            rowvals(input),
            output_value_per_stored,
        )
    end

    output = fill(zero_value, size(input))
    row_index_per_stored = rowvals(input)
    n_columns = size(input, 2)
    parallel_loop_wo_rng(  # NOJET
        1:n_columns;
        name,
        progress = DebugProgress(n_columns; group = :daf_loops, desc = name),
    ) do column_index
        for stored_index in nzrange(input, column_index)
            output[row_index_per_stored[stored_index], column_index] = value_of(value_per_stored[stored_index])
        end
        return nothing
    end
    return output
end

"""
    Abs([; type::Maybe{Type} = nothing])

Element-wise operation that converts every element to its absolute value.

**Parameters**

`type` - The default output data type is the [`unsigned_type_for`](@ref) the input data type.
"""
struct Abs <: EltwiseOperation
    type::Maybe{Type}
end
@query_operation Abs

function Abs(; type::Maybe{Type} = nothing)::Abs
    return Abs(type)
end

function Abs(operation_name::Token, parameters_values::Dict{String, Token})::Abs
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_number_type_value(operation_name, "type", parameter_value)
    end
    return Abs(type)
end

function compute_eltwise(operation::Abs, input::StorageMatrix)::StorageMatrix
    type = unsigned_type_for(eltype(input), operation.type)
    if issparse(input)
        return compute_sparse_eltwise(abs, input, type, "Abs")
    end
    output = similar(input, type)
    axis = major_axis(input)
    if axis == Columns
        n_columns = size(input, 2)
        parallel_loop_wo_rng(  # NOJET
            1:n_columns;
            name = "Abs",
            progress = DebugProgress(n_columns; group = :daf_loops, desc = "Abs"),
        ) do column_index
            @views output[:, column_index] .= abs.(input[:, column_index])  # NOJET
            return nothing
        end
    elseif axis == Rows  # UNTESTED
        n_rows = size(input, 1)  # UNTESTED
        parallel_loop_wo_rng(  # UNTESTED
            1:n_rows;
            name = "Abs",
            progress = DebugProgress(n_rows; group = :daf_loops, desc = "Abs"),
        ) do row_index
            @views output[row_index, :] .= abs.(input[row_index, :])
            return nothing
        end
    else
        @. output = abs(input)  # UNTESTED
    end
    return output
end

function compute_eltwise(operation::Abs, input::StorageVector{<:StorageReal})::StorageVector{<:StorageReal}
    type = unsigned_type_for(eltype(input), operation.type)
    output = similar(input, type)
    @. output = abs(input)
    return output
end

function compute_eltwise(operation::Abs, input::StorageReal)::StorageReal
    type = unsigned_type_for(eltype(input), operation.type)
    return type(abs(input))
end

"""
    Round([; type::Maybe{Type} = nothing])

Element-wise operation that converts every element to the nearest integer value.

**Parameters**

`type` - By default, uses the [`int_type_for`](@ref) the input data type.
"""
struct Round <: EltwiseOperation
    type::Maybe{Type}
end
@query_operation Round

function Round(; type::Maybe{Type} = nothing)::Round
    return Round(type)
end

function Round(operation_name::Token, parameters_values::Dict{String, Token})::Round
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_number_type_value(operation_name, "type", parameter_value)
    end
    return Round(type)
end

function compute_eltwise(operation::Round, input::StorageMatrix)::StorageMatrix
    type = int_type_for(eltype(input), operation.type)
    if issparse(input)
        return compute_sparse_eltwise(value -> round(type, value), input, type, "Round")
    end
    output = similar(input, type)
    axis = major_axis(input)
    if axis == Columns
        n_columns = size(input, 2)
        parallel_loop_wo_rng(
            1:n_columns;
            name = "Round",
            progress = DebugProgress(n_columns; group = :daf_loops, desc = "Round"),
        ) do column_index
            @views output[:, column_index] .= round.(type, input[:, column_index])
            return nothing
        end
    elseif axis == Rows  # UNTESTED
        n_rows = size(input, 1)  # UNTESTED
        parallel_loop_wo_rng(  # UNTESTED
            1:n_rows;
            name = "Round",
            progress = DebugProgress(n_rows; group = :daf_loops, desc = "Round"),
        ) do row_index
            @views output[row_index, :] .= round.(type, input[row_index, :])
            return nothing
        end
    else
        output .= round.(type, input)  # UNTESTED
    end
    return output
end

function compute_eltwise(operation::Round, input::StorageVector{<:StorageReal})::StorageVector{<:StorageReal}
    return round.(int_type_for(eltype(input), operation.type), input)
end

function compute_eltwise(operation::Round, input::StorageReal)::StorageReal
    return round(int_type_for(eltype(input), operation.type), input)  # NOJET
end

"""
    Clamp([; min::Maybe{StorageReal} = nothing, max::Maybe{StorageReal} = nothing])

Element-wise operation that converts every element to a value inside a range.

**Parameters**

`min` - If specified, values lower than this will be increased to this value.

`max` - If specified, values higher than this will be increased to this value.

!!! note

    At least one of `min` and `max` must be specified.
"""
struct Clamp <: EltwiseOperation
    type::Maybe{Type}
    min::Float64
    max::Float64
end
@query_operation Clamp

function Clamp(; type::Maybe{Type} = nothing, min::StorageReal = -Inf, max::StorageReal = Inf)::Clamp
    @assert min < max
    return Clamp(type, Float64(min), Float64(max))
end

function Clamp(operation_name::Token, parameters_values::Dict{String, Token})::Clamp
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_number_type_value(operation_name, "type", parameter_value)
    end
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
    return Clamp(type, min, max)
end

function is_int(value::Float64)::Bool
    return value == -Inf || value == Inf || isinteger(value)
end

function type_for_clamp(operation::Clamp, input_type::Type)::Type
    if operation.type !== nothing
        return operation.type
    elseif input_type <: Integer && is_int(operation.min) && is_int(operation.max)
        return int_type_for(input_type, nothing)
    else
        return float_type_for(input_type, nothing)
    end
end

function compute_eltwise(operation::Clamp, input::StorageMatrix)::StorageMatrix
    type = type_for_clamp(operation, eltype(input))
    clamp_min = operation.min
    clamp_max = operation.max
    if issparse(input)
        return compute_sparse_eltwise(value -> clamp(value, clamp_min, clamp_max), input, type, "Clamp")
    end
    output = similar(input, type)
    axis = major_axis(input)
    if axis == Columns
        n_columns = size(input, 2)
        parallel_loop_wo_rng(
            1:n_columns;
            name = "Clamp",
            progress = DebugProgress(n_columns; group = :daf_loops, desc = "Clamp"),
        ) do column_index
            @views output[:, column_index] .= clamp.(input[:, column_index], clamp_min, clamp_max)
            return nothing
        end
    elseif axis == Rows  # UNTESTED
        n_rows = size(input, 1)  # UNTESTED
        parallel_loop_wo_rng(  # UNTESTED
            1:n_rows;
            name = "Clamp",
            progress = DebugProgress(n_rows; group = :daf_loops, desc = "Clamp"),
        ) do row_index
            @views output[row_index, :] .= clamp.(input[row_index, :], clamp_min, clamp_max)
            return nothing
        end
    else
        @. output = clamp(input, clamp_min, clamp_max)  # UNTESTED
    end
    return output
end

function compute_eltwise(operation::Clamp, input::StorageVector)::StorageVector
    type = type_for_clamp(operation, eltype(input))
    output = similar(input, type)
    @. output = clamp(input, operation.min, operation.max)
    return output
end

function compute_eltwise(operation::Clamp, input::StorageReal)::StorageReal
    type = type_for_clamp(operation, typeof(input))
    output = clamp(input, operation.min, operation.max)
    return type(output)
end

"""
    Convert([; type::Type])

Element-wise operation that converts every element to a given data type.

**Parameters**

`type` - The data type to convert to. There's no default.
"""
struct Convert <: EltwiseOperation
    type::Type
end
@query_operation Convert

function Convert(; type::Type{<:StorageReal})::Convert
    return Convert(type)
end

function Convert(operation_name::Token, parameters_values::Dict{String, Token})::Convert
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", missing) do parameter_value
        return parse_number_type_value(operation_name, "type", parameter_value)
    end
    return Convert(type)
end

function compute_eltwise(operation::Convert, input::StorageMatrix)::StorageMatrix
    type = operation.type
    if issparse(input)
        return compute_sparse_eltwise(identity, input, type, "Convert")
    end
    output = similar(input, type)
    axis = major_axis(input)
    if axis == Columns
        n_columns = size(input, 2)
        parallel_loop_wo_rng(
            1:n_columns;
            name = "Convert",
            progress = DebugProgress(n_columns; group = :daf_loops, desc = "Convert"),
        ) do column_index
            @views output[:, column_index] .= type.(input[:, column_index])
            return nothing
        end
    elseif axis == Rows  # UNTESTED
        n_rows = size(input, 1)  # UNTESTED
        parallel_loop_wo_rng(  # UNTESTED
            1:n_rows;
            name = "Convert",
            progress = DebugProgress(n_rows; group = :daf_loops, desc = "Convert"),
        ) do row_index
            @views output[row_index, :] .= type.(input[row_index, :])
            return nothing
        end
    else
        output .= type.(input)  # UNTESTED
    end
    return output
end

function compute_eltwise(operation::Convert, input::StorageVector{<:StorageReal})::StorageVector{<:StorageReal}
    return operation.type.(input)
end

function compute_eltwise(operation::Convert, input::StorageReal)::StorageReal
    return operation.type(input)
end

"""
    Fraction([; type::Type])

Element-wise operation that converts every element to its fraction out of the total. If the total is zero, all the
fractions are also set to zero. This implicitly assumes (but does not enforce) that all the entry value(s) are positive.

For matrices, each entry becomes its fraction out of the total of the column it belongs to. For vectors, each entry
becomes its fraction out of the total of the vector. For scalars, this operation makes no sense so fails with an error.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.
"""
struct Fraction <: EltwiseOperation
    type::Maybe{Type}
end
@query_operation Fraction

function Fraction(; type::Maybe{Type{<:AbstractFloat}} = nothing)::Fraction
    return Fraction(type)
end

function Fraction(operation_name::Token, parameters_values::Dict{String, Token})::Fraction
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    return Fraction(type)
end

function compute_eltwise(operation::Fraction, input::StorageMatrix)::StorageMatrix
    type = float_type_for(eltype(input), operation.type)
    output = similar(input, type)
    output .= input
    parallel_loop_wo_rng(
        1:size(output, 2);
        name = "Fraction",
        progress = DebugProgress(size(output, 2); group = :daf_loops, desc = "Fraction"),
    ) do column_index
        column_vector = @view output[:, column_index]
        column_sum = sum(column_vector)
        if column_sum != 0
            column_vector ./= column_sum
        end
        return nothing
    end
    return output
end

function compute_eltwise(operation::Fraction, input::StorageVector{<:StorageReal})::StorageVector{<:StorageReal}
    type = float_type_for(eltype(input), operation.type)
    vector_sum = sum(input)
    if vector_sum == 0
        output = zeros(type, length(input))
    else
        output = similar(input, type)
        output .= input
        output ./= vector_sum
    end
    return output
end

function compute_eltwise(::Fraction, ::StorageReal)::StorageReal
    return error("applying Fraction eltwise operation to a scalar")
end

"""
    Log(; type::Maybe{Type} = nothing, base::StorageReal = e, eps::StorageReal = 0)

Element-wise operation that converts every element to its logarithm.

**Parameters**:

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.

`base` - The base of the logarithm. By default uses `e` (that is, computes the natural logarithm), which isn't
convenient, but is the standard.

`eps` - Added to the input before computing the logarithm, to handle zero input data. By default is zero.
"""
struct Log <: EltwiseOperation
    type::Maybe{Type}
    base::Float64
    eps::Float64
end
@query_operation Log

function Log(;
    type::Maybe{Type{<:AbstractFloat}} = nothing,
    base::StorageReal = Float64(e),
    eps::StorageReal = 0.0,
)::Log
    @assert base > 0
    @assert eps >= 0
    return Log(type, Float64(base), Float64(eps))
end

function Log(operation_name::Token, parameters_values::Dict{String, Token})::Log
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end

    base = parse_parameter_value(operation_name, "eltwise", parameters_values, "base", Float64(e)) do parameter_value
        value = parse_number_value(operation_name, "base", parameter_value, Float64)
        if value <= 0
            error_invalid_parameter_value(operation_name, "base", parameter_value, "positive")
        end
        return value
    end

    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        value = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if value < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return value
    end

    return Log(type, base, eps)
end

function log_vector!(
    output::AbstractVector{T},
    input::AbstractVector,
    eps::T,
    base::Float64,
)::Nothing where {T <: AbstractFloat}
    n_elements = length(output)
    output = base_array(output)
    input = base_array(input)
    @check_turbo_vector(output)
    @check_turbo_vector(input)
    if base == 2.0
        @turbo for element_index in 1:n_elements
            output[element_index] = log2(T(input[element_index]) + eps)
        end
    elseif base == 10.0
        @turbo for element_index in 1:n_elements  # UNTESTED
            output[element_index] = log10(T(input[element_index]) + eps)  # UNTESTED
        end
    elseif base == Float64(e)
        @turbo for element_index in 1:n_elements
            output[element_index] = log(T(input[element_index]) + eps)
        end
    else
        inverse_log_base = T(1.0 / log(base))  # UNTESTED
        @turbo for element_index in 1:n_elements  # UNTESTED
            output[element_index] = log(T(input[element_index]) + eps) * inverse_log_base  # UNTESTED
        end
    end
    return nothing
end

function compute_eltwise(operation::Log, input::StorageMatrix)::StorageMatrix
    input = plain_array(densify(input))
    if major_axis(input) == Rows
        # The result has the same layout as the input, and is computed along its columns.
        return flip(compute_eltwise(operation, flip(input)::StorageMatrix))
    end
    @assert major_axis(input) == Columns
    type = float_type_for(eltype(input), operation.type)
    output = similar(input, type)
    eps = type(operation.eps)
    base = operation.base
    n_columns = size(input, 2)
    parallel_loop_wo_rng(
        1:n_columns;
        name = "Log",
        progress = DebugProgress(n_columns; group = :daf_loops, desc = "Log"),
    ) do column_index
        @views log_vector!(output[:, column_index], input[:, column_index], eps, base)
        return nothing
    end
    return output
end

function compute_eltwise(operation::Log, input::StorageVector{<:StorageReal})::StorageVector{<:StorageReal}
    input = plain_array(densify(input))
    type = float_type_for(eltype(input), operation.type)
    output = similar(input, type)
    log_vector!(output, input, type(operation.eps), operation.base)
    return output
end

function compute_eltwise(operation::Log, input::StorageReal)::StorageReal
    type = float_type_for(typeof(input), operation.type)
    return type(log(Float64(input) + operation.eps) / log(operation.base))
end

"""
    Significant(; high::StorageReal, low::Maybe{StorageReal} = nothing)

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

function Significant(; high::StorageReal, low::Maybe{StorageReal} = nothing)::Significant
    if low === nothing
        low = high
    end
    @assert high > 0
    @assert low > 0
    @assert low <= high
    return Significant(Float64(high), Float64(low))
end

function Significant(operation_name::Token, parameters_values::Dict{String, Token})::Significant
    high = parse_parameter_value(operation_name, "eltwise", parameters_values, "high", missing) do parameter_value
        value = parse_number_value(operation_name, "high", parameter_value, Float64)
        if value <= 0
            error_invalid_parameter_value(operation_name, "high", parameter_value, "positive")
        end
        return value
    end

    low = parse_parameter_value(operation_name, "eltwise", parameters_values, "low", high) do parameter_value
        value = parse_number_value(operation_name, "low", parameter_value, Float64)
        if value < 0
            error_invalid_parameter_value(operation_name, "low", parameter_value, "not negative")
        end
        if value > high
            error_invalid_parameter_value(operation_name, "low", parameter_value, "at most high ($(high))")
        end
        return value
    end

    return Significant(high, low)
end

function compute_eltwise(operation::Significant, input::StorageMatrix)::StorageMatrix
    output = copy_array(input)
    if issparse(output)
        parallel_loop_wo_rng(
            1:size(output, 2);
            name = "Significant",
            progress = DebugProgress(size(output, 2); group = :daf_loops, desc = "Significant"),
        ) do column_index
            first = colptr(output)[column_index]
            last = colptr(output)[column_index + 1] - 1
            if first <= last
                column_vector = @view nzval(output)[first:last]
                significant!(column_vector, operation.high, operation.low)
            end
            return nothing
        end
        dropzeros!(output)
        return output
    else
        n_columns = size(output, 2)
        is_dense_of_columns = zeros(Bool, n_columns)
        parallel_loop_wo_rng(
            1:size(output, 2);
            name = "Significant",
            progress = DebugProgress(size(output, 2); group = :daf_loops, desc = "Significant"),
        ) do column_index
            column_vector = @view output[:, column_index]
            significant!(column_vector, operation.high, operation.low)
            is_dense_of_columns[column_index] = all(column_vector .!= 0)
            return nothing
        end
        if all(is_dense_of_columns)
            return output  # UNTESTED
        else
            return SparseMatrixCSC(output)
        end
    end
end

function compute_eltwise(operation::Significant, input::StorageVector{T})::StorageVector{T} where {T <: StorageReal}
    output = copy_array(input)

    if issparse(output)
        significant!(nzval(output), operation.high, operation.low)
        dropzeros!(output)
        return output
    else
        significant!(output, operation.high, operation.low)
        if any(output .== 0)
            return SparseVector(output)
        else
            return output
        end
    end
end

function significant!(vector::StorageVector{<:StorageReal}, high::StorageReal, low::StorageReal)::Nothing
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

function compute_eltwise(::Significant, ::T)::T where {T <: StorageReal}
    return error("applying Significant eltwise operation to a scalar")
end

"""
    Count(; type::Maybe{Type} = nothing)

Reduction operation that counts elements. This is useful when using `GroupBy` queries to count the number of elements in
each group.

!!! note

    This operation supports strings; most operations do not.

**Parameters**

`type` - By default, uses `UInt32`.
"""
struct Count <: ReductionOperation
    type::Maybe{Type}
end
@query_operation Count

function Count(; type::Maybe{Type{<:StorageReal}} = nothing)::Count
    return Count(type)
end

function Count(operation_name::Token, parameters_values::Dict{String, Token})::Count
    type = parse_parameter_value(operation_name, "reduction", parameters_values, "type", nothing) do parameter_value
        return parse_number_type_value(operation_name, "type", parameter_value)
    end
    return Count(type)
end

function compute_reduction(operation::Count, input::StorageMatrix, axis::Integer)::StorageVector
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    result = Vector{type}(undef, size(input, other_axis(axis)))
    result .= size(input, axis)
    return result
end

function compute_reduction(operation::Count, input::Union{StorageVector, StorageMatrix})::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(length(input))
end

function reduction_result_type(operation::Count, ::Type)::Type
    return operation.type === nothing ? UInt32 : operation.type
end

function supports_strings(::Count)::Bool
    return true
end

function supports_grouped_reduction(::Count)::Bool
    return true
end

function empty_reduction_state(::Count, ::Type)::Nothing
    return nothing
end

function update_reduction_state(::Count, ::Nothing, ::StorageReal)::Nothing
    return nothing
end

function final_reduction_value(::Count, ::Nothing, ::Integer, n_values::Integer)::Integer
    return n_values
end

"""
    Mode()

Reduction operation that returns the most frequent value in the input (the "mode").

!!! note

    This operation supports strings; most operations do not.
"""
struct Mode <: ReductionOperation end
@query_operation Mode

function Mode(::Token, ::Dict{String, Token})::Mode
    return Mode()
end

function compute_reduction(operation::Mode, input::StorageMatrix, axis::Integer)::StorageVector
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    if axis == 1
        parallel_loop_wo_rng(
            1:length(output);
            name = "Mode",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Mode"),
        ) do column_index
            column_vector = @view input[:, column_index]
            output[column_index] = mode(column_vector)
            return nothing
        end
    else
        parallel_loop_wo_rng(
            1:length(output);
            name = "Mode",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Mode"),
        ) do row_index
            row_vector = @view input[row_index, :]
            output[row_index] = mode(row_vector)
            return nothing
        end
    end
    return output
end

function compute_reduction(::Mode, input::Union{StorageVector, StorageMatrix})::StorageScalar
    return mode(input)
end

function reduction_result_type(::Mode, eltype::Type)::Type
    if eltype <: AbstractString
        return AbstractString
    else
        return eltype
    end
end

function supports_strings(::Mode)::Bool
    return true
end

"""
    Sum(; type::Maybe{Type} = nothing)

Reduction operation that sums elements.

**Parameters**

`type` - By default, uses the [`sum_type_for`](@ref) the input data type.
"""
struct Sum <: ReductionOperation
    type::Maybe{Type}
end
@query_operation Sum

function Sum(; type::Maybe{Type{<:StorageReal}} = nothing)::Sum
    return Sum(type)
end

function Sum(operation_name::Token, parameters_values::Dict{String, Token})::Sum
    type = parse_parameter_value(operation_name, "reduction", parameters_values, "type", nothing) do parameter_value
        return parse_number_type_value(operation_name, "type", parameter_value)
    end
    return Sum(type)
end

function compute_reduction(operation::Sum, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    if axis == 1
        parallel_loop_wo_rng(
            1:length(output);
            name = "Sum",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Sum"),
        ) do column_index
            column_vector = @view input[:, column_index]
            output[column_index] = type(sum(column_vector))
            return nothing
        end
    else
        parallel_loop_wo_rng(
            1:length(output);
            name = "Sum",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Sum"),
        ) do row_index
            row_vector = @view input[row_index, :]
            output[row_index] = type(sum(row_vector))
            return nothing
        end
    end
    return output
end

function compute_reduction(
    operation::Sum,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sum(input))
end

function reduction_result_type(operation::Sum, eltype::Type)::Type
    return sum_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::Sum)::Bool
    return true
end

function empty_reduction_state(operation::Sum, eltype::Type)::StorageReal
    return zero(reduction_result_type(operation, eltype))
end

function update_reduction_state(::Sum, sum::StorageReal, value::StorageReal)::StorageReal
    return oftype(sum, sum + value)
end

function final_reduction_value(::Sum, sum::StorageReal, ::Integer, ::Integer)::StorageReal
    return sum
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

function compute_reduction(::Max, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    output = Vector{eltype(input)}(undef, size(input, other_axis(axis)))
    if axis == 1
        parallel_loop_wo_rng(
            1:length(output);
            name = "Max",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Max"),
        ) do column_index
            column_vector = @view input[:, column_index]
            output[column_index] = maximum(column_vector)
            return nothing
        end
    else
        parallel_loop_wo_rng(
            1:length(output);
            name = "Max",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Max"),
        ) do row_index
            row_vector = @view input[row_index, :]
            output[row_index] = maximum(row_vector)
            return nothing
        end
    end
    return output
end

function compute_reduction(::Max, input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}})::StorageReal
    return maximum(input)
end

function reduction_result_type(::Max, eltype::Type)::Type
    return eltype
end

function supports_grouped_reduction(::Max)::Bool
    return true
end

function empty_reduction_state(::Max, eltype::Type)::StorageReal
    return typemin(eltype)
end

function update_reduction_state(::Max, maximal::StorageReal, value::StorageReal)::StorageReal
    return max(maximal, value)
end

function final_reduction_value(::Max, maximal::StorageReal, n_seen::Integer, n_values::Integer)::StorageReal
    return n_seen < n_values ? max(maximal, zero(maximal)) : maximal
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

function compute_reduction(::Min, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    output = Vector{eltype(input)}(undef, size(input, other_axis(axis)))
    if axis == 1
        parallel_loop_wo_rng(
            1:length(output);
            name = "Min",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Min"),
        ) do column_index
            column_vector = @view input[:, column_index]
            output[column_index] = minimum(column_vector)
            return nothing
        end
    else
        parallel_loop_wo_rng(  # UNTESTED
            1:length(output);
            name = "Min",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Min"),
        ) do row_index
            row_vector = @view input[row_index, :]
            output[row_index] = minimum(row_vector)
            return nothing
        end
    end
    return output
end

function compute_reduction(::Min, input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}})::StorageReal
    return minimum(input)
end

function reduction_result_type(::Min, eltype::Type)::Type
    return eltype
end

function supports_grouped_reduction(::Min)::Bool
    return true
end

function empty_reduction_state(::Min, eltype::Type)::StorageReal
    return typemax(eltype)
end

function update_reduction_state(::Min, minimal::StorageReal, value::StorageReal)::StorageReal
    return min(minimal, value)
end

function final_reduction_value(::Min, minimal::StorageReal, n_seen::Integer, n_values::Integer)::StorageReal
    return n_seen < n_values ? min(minimal, zero(minimal)) : minimal
end

"""
    Median(; type::Maybe{Type} = nothing)

Reduction operation that returns the median value.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.
"""
struct Median <: ReductionOperation
    type::Maybe{Type}
end
@query_operation Median

function Median(; type::Maybe{Type{<:AbstractFloat}} = nothing)::Median
    return Median(type)
end

function Median(operation_name::Token, parameters_values::Dict{String, Token})::Median
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    return Median(type)
end

function compute_reduction(operation::Median, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    sparse_median(
        input;
        dims = axis,
        result = output,
        progress = DebugProgress(length(output); group = :daf_loops, desc = "Median"),
    )
    return output
end

function compute_reduction(
    operation::Median,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sparse_median(input))
end

function reduction_result_type(operation::Median, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

"""
    Quantile(; type::Maybe{Type} = nothing, p::StorageReal)

Reduction operation that returns the quantile value, that is, a value such that a certain fraction of the values is
lower.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.

`p` - The fraction of values below the result (e.g., the 0 computes the minimum, the 0.5 computes the median, and 1.0
computes the maximum). There's no default.
"""
struct Quantile <: ReductionOperation
    type::Maybe{Type}
    p::Float64
end
@query_operation Quantile

function Quantile(; type::Maybe{Type{<:AbstractFloat}} = nothing, p::StorageReal)::Quantile
    @assert 0 <= p && p <= 1
    return Quantile(type, p)
end

function Quantile(operation_name::Token, parameters_values::Dict{String, Token})::Quantile
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    p = parse_parameter_value(operation_name, "eltwise", parameters_values, "p", missing) do parameter_value
        value = parse_number_value(operation_name, "p", parameter_value, Float64)
        if value < 0
            error_invalid_parameter_value(operation_name, "p", parameter_value, "at least 0")
        end
        if value > 1
            error_invalid_parameter_value(operation_name, "p", parameter_value, "at most 1")
        end
        return value
    end
    return Quantile(type, p)
end

function compute_reduction(operation::Quantile, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    sparse_quantile(
        input,
        operation.p;
        dims = axis,
        result = output,
        progress = DebugProgress(length(output); group = :daf_loops, desc = "Quantile"),
    )
    return output
end

function compute_reduction(
    operation::Quantile,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sparse_quantile(input, operation.p))
end

function reduction_result_type(operation::Quantile, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

"""
    Mean(; type::Maybe{Type} = nothing)

Reduction operation that returns the mean value.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.
"""
struct Mean <: ReductionOperation
    type::Maybe{Type}
end
@query_operation Mean

function Mean(; type::Maybe{Type{<:AbstractFloat}} = nothing)::Mean
    return Mean(type)
end

function Mean(operation_name::Token, parameters_values::Dict{String, Token})::Mean
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    return Mean(type)
end

function compute_reduction(operation::Mean, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    if axis == 1
        parallel_loop_wo_rng(
            1:length(output);
            name = "Mean",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Mean"),
        ) do column_index
            column_vector = @view input[:, column_index]
            output[column_index] = type(mean(column_vector))  # NOLINT
            return nothing
        end
    else
        parallel_loop_wo_rng(
            1:length(output);
            name = "Mean",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "Mean"),
        ) do row_index
            row_vector = @view input[row_index, :]
            output[row_index] = type(mean(row_vector))  # NOLINT
            return nothing
        end
    end
    return output
end

function compute_reduction(
    operation::Mean,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(mean(input))  # NOLINT
end

function reduction_result_type(operation::Mean, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::Mean)::Bool
    return true
end

function empty_reduction_state(::Mean, ::Type)::Float64
    return 0.0
end

function update_reduction_state(::Mean, sum::Float64, value::StorageReal)::Float64
    return sum + value
end

function final_reduction_value(::Mean, sum::Float64, ::Integer, n_values::Integer)::Float64
    return sum / n_values
end

"""
    GeoMean(; type::Maybe{Type} = nothing, eps::StorageReal = 0.0)

Reduction operation that returns the geometric mean value.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.

`eps` - The regularization factor added to each value and subtracted from the raw geo-mean, to deal with zero values.
"""
struct GeoMean <: ReductionOperation
    type::Maybe{Type}
    eps::Float64
end
@query_operation GeoMean

function GeoMean(; type::Maybe{Type{<:AbstractFloat}} = nothing, eps::StorageReal = 0)::GeoMean
    @assert eps >= 0
    return GeoMean(type, eps)
end

function GeoMean(operation_name::Token, parameters_values::Dict{String, Token})::GeoMean
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        value = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if value < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return value
    end
    return GeoMean(type, eps)
end

function compute_reduction(operation::GeoMean, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    if axis == 1
        parallel_loop_wo_rng(
            1:length(output);
            name = "GeoMean",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "GeoMean"),
        ) do column_index
            column_vector = @view input[:, column_index]
            if operation.eps == 0
                output[column_index] = type(geomean(column_vector))
            else
                output[column_index] = type(geomean(column_vector .+ operation.eps) - operation.eps)
            end
            return nothing
        end
    else
        parallel_loop_wo_rng(
            1:length(output);
            name = "GeoMean",
            progress = DebugProgress(length(output); group = :daf_loops, desc = "GeoMean"),
        ) do row_index
            row_vector = @view input[row_index, :]
            if operation.eps == 0
                output[row_index] = type(geomean(row_vector))
            else
                output[row_index] = type(geomean(row_vector .+ operation.eps) - operation.eps)
            end
            return nothing
        end
    end
    return output
end

function compute_reduction(
    operation::GeoMean,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    if operation.eps == 0
        return type(geomean(input))
    else
        return type(geomean(input .+ operation.eps) - operation.eps)
    end
end

function reduction_result_type(operation::GeoMean, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::GeoMean)::Bool
    return true
end

function empty_reduction_state(::GeoMean, ::Type)::Float64
    return 0.0
end

function update_reduction_state(operation::GeoMean, sum_of_logs::Float64, value::StorageReal)::Float64
    return sum_of_logs + log(value + operation.eps)
end

function final_reduction_value(operation::GeoMean, sum_of_logs::Float64, n_seen::Integer, n_values::Integer)::Float64
    if n_seen < n_values
        sum_of_logs += (n_values - n_seen) * log(operation.eps)
    end
    return exp(sum_of_logs / n_values) - operation.eps
end

# The state of Welford's online algorithm, shared by the variance reductions: the number of values seen, their mean, and
# the sum of the squared differences from that mean.
const MomentsState = Tuple{Int, Float64, Float64}

function empty_moments_state()::MomentsState
    return (0, 0.0, 0.0)
end

function update_moments_state(state::MomentsState, value::StorageReal)::MomentsState
    n_seen, mean_of_seen, squared_differences = state
    n_seen += 1
    difference = value - mean_of_seen
    mean_of_seen += difference / n_seen
    squared_differences += difference * (value - mean_of_seen)
    return (n_seen, mean_of_seen, squared_differences)
end

# The mean and the (uncorrected) variance of `n_values` values: the ones seen by the `state`, and zeros for the rest.
function moments_with_zeros(state::MomentsState, n_values::Integer)::Tuple{Float64, Float64}
    n_seen, mean_of_seen, squared_differences = state
    n_zeros = n_values - n_seen
    mean = mean_of_seen * n_seen / n_values
    squared_differences += mean_of_seen^2 * n_seen * n_zeros / n_values
    return (mean, squared_differences / n_values)
end

"""
    Var(; type::Maybe{Type} = nothing)

Reduction operation that returns the (uncorrected) variance of the values.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.
"""
struct Var <: ReductionOperation
    type::Maybe{Type}
end
@query_operation Var

function Var(; type::Maybe{Type{<:AbstractFloat}} = nothing)::Var
    return Var(type)
end

function Var(operation_name::Token, parameters_values::Dict{String, Token})::Var
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    return Var(type)
end

function compute_reduction(operation::Var, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    sparse_var(
        input;
        dims = axis,
        corrected = false,
        result = output,
        progress = DebugProgress(length(output); group = :daf_loops, desc = "Var"),
    )
    return output
end

function compute_reduction(
    operation::Var,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sparse_var(input; corrected = false))
end

function reduction_result_type(operation::Var, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::Var)::Bool
    return true
end

function empty_reduction_state(::Var, ::Type)::MomentsState
    return empty_moments_state()
end

function update_reduction_state(::Var, state::MomentsState, value::StorageReal)::MomentsState
    return update_moments_state(state, value)
end

function final_reduction_value(::Var, state::MomentsState, ::Integer, n_values::Integer)::Float64
    _, variance = moments_with_zeros(state, n_values)
    return variance
end

"""
    VarN(; type::Maybe{Type} = nothing, eps::StorageReal = 0.0)

Reduction operation that returns the (uncorrected) variance of the values, normalized (divided) by the mean of the
values.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.

`eps` - Added to the mean before computing the division, to handle zero input data. By default is zero.
"""
struct VarN <: ReductionOperation
    type::Maybe{Type}
    eps::Float64
end
@query_operation VarN

function VarN(; type::Maybe{Type{<:AbstractFloat}} = nothing, eps::StorageReal = 0)::VarN
    @assert eps >= 0
    return VarN(type, eps)
end

function VarN(operation_name::Token, parameters_values::Dict{String, Token})::VarN
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        value = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if value < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return value
    end
    return VarN(type, eps)
end

function compute_reduction(operation::VarN, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    sparse_var(
        input;
        dims = axis,
        corrected = false,
        result = output,
        progress = DebugProgress(length(output); group = :daf_loops, desc = "VarN"),
    )
    means = vec(mean(input; dims = axis))  # NOLINT
    output ./= type.(means .+ operation.eps)
    return output
end

function compute_reduction(
    operation::VarN,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sparse_var(input; corrected = false) / (Float64(mean(input)) + operation.eps))  # NOLINT
end

function reduction_result_type(operation::VarN, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::VarN)::Bool
    return true
end

function empty_reduction_state(::VarN, ::Type)::MomentsState
    return empty_moments_state()
end

function update_reduction_state(::VarN, state::MomentsState, value::StorageReal)::MomentsState
    return update_moments_state(state, value)
end

function final_reduction_value(operation::VarN, state::MomentsState, ::Integer, n_values::Integer)::Float64
    mean, variance = moments_with_zeros(state, n_values)
    return variance / (mean + operation.eps)
end

"""
    Std(; type::Maybe{Type} = nothing)

Reduction operation that returns the (uncorrected) standard deviation of the values.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.
"""
struct Std <: ReductionOperation
    type::Maybe{Type}
end
@query_operation Std

function Std(; type::Maybe{Type{<:AbstractFloat}} = nothing)::Std
    return Std(type)
end

function Std(operation_name::Token, parameters_values::Dict{String, Token})::Std
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    return Std(type)
end

function compute_reduction(operation::Std, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    sparse_std(
        input;
        dims = axis,
        corrected = false,
        result = output,
        progress = DebugProgress(length(output); group = :daf_loops, desc = "Std"),
    )
    return output
end

function compute_reduction(
    operation::Std,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sparse_std(input; corrected = false))
end

function reduction_result_type(operation::Std, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::Std)::Bool
    return true
end

function empty_reduction_state(::Std, ::Type)::MomentsState
    return empty_moments_state()
end

function update_reduction_state(::Std, state::MomentsState, value::StorageReal)::MomentsState
    return update_moments_state(state, value)
end

function final_reduction_value(::Std, state::MomentsState, ::Integer, n_values::Integer)::Float64
    _, variance = moments_with_zeros(state, n_values)
    return sqrt(variance)
end

"""
    StdN(; type::Maybe{Type} = nothing, eps::StorageReal = 0)

Reduction operation that returns the (uncorrected) standard deviation of the values, normalized (divided) by the mean
value.

**Parameters**

`type` - The default output data type is the [`float_type_for`](@ref) of the input data type.

`eps` - Added to the mean before computing the division, to handle zero input data. By default is zero.
"""
struct StdN <: ReductionOperation
    type::Maybe{Type}
    eps::Float64
end
@query_operation StdN

function StdN(; type::Maybe{Type{<:AbstractFloat}} = nothing, eps::StorageReal = 0)::StdN
    @assert eps >= 0
    return StdN(type, eps)
end

function StdN(operation_name::Token, parameters_values::Dict{String, Token})::StdN
    type = parse_parameter_value(operation_name, "eltwise", parameters_values, "type", nothing) do parameter_value
        return parse_float_type_value(operation_name, "type", parameter_value)
    end
    eps = parse_parameter_value(operation_name, "eltwise", parameters_values, "eps", 0.0) do parameter_value
        value = parse_number_value(operation_name, "eps", parameter_value, Float64)
        if value < 0
            error_invalid_parameter_value(operation_name, "eps", parameter_value, "not negative")
        end
        return value
    end
    return StdN(type, eps)
end

function compute_reduction(operation::StdN, input::StorageMatrix, axis::Integer)::StorageVector{<:StorageReal}
    @assert 1 <= axis <= 2
    type = reduction_result_type(operation, eltype(input))
    output = Vector{type}(undef, size(input, other_axis(axis)))
    sparse_std(
        input;
        dims = axis,
        corrected = false,
        result = output,
        progress = DebugProgress(length(output); group = :daf_loops, desc = "StdN"),
    )
    means = vec(mean(input; dims = axis))  # NOLINT
    output ./= type.(means .+ operation.eps)
    return output
end

function compute_reduction(
    operation::StdN,
    input::Union{StorageVector{<:StorageReal}, StorageMatrix{<:StorageReal}},
)::StorageReal
    type = reduction_result_type(operation, eltype(input))
    return type(sparse_std(input; corrected = false) / (Float64(mean(input)) + operation.eps))  # NOLINT
end

function reduction_result_type(operation::StdN, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_grouped_reduction(::StdN)::Bool
    return true
end

function empty_reduction_state(::StdN, ::Type)::MomentsState
    return empty_moments_state()
end

function update_reduction_state(::StdN, state::MomentsState, value::StorageReal)::MomentsState
    return update_moments_state(state, value)
end

function final_reduction_value(operation::StdN, state::MomentsState, ::Integer, n_values::Integer)::Float64
    mean, variance = moments_with_zeros(state, n_values)
    return sqrt(variance) / (mean + operation.eps)
end

"""
    compute_grouped_reduction(
        operation::ReductionOperation,
        matrix::StorageMatrix{<:StorageReal},
        group_index_per_row::AbstractVector{<:Integer},
        n_groups::Integer,
    )::Matrix{<:StorageReal}

Reduce the rows of a column-major `matrix` per group. The result has one row per group, and one column per column of the
`matrix`. The group of each row is given by `group_index_per_row`, where 0 means the row is in no group. Each group must
have at least one row. The `operation` must [`supports_grouped_reduction`](@ref).

Each column is visited once, so the time is proportional to the number of values, plus the number of groups for each
column. For a sparse matrix only the stored values are visited.
"""
function compute_grouped_reduction(
    operation::ReductionOperation,
    matrix::StorageMatrix{<:StorageReal},
    group_index_per_row::AbstractVector{<:Integer},
    n_groups::Integer,
)::Matrix{<:StorageReal}
    @assert supports_grouped_reduction(operation)
    @assert length(group_index_per_row) == size(matrix, 1)
    @assert require_major_axis(matrix) == Columns

    n_rows_per_group = zeros(Int, n_groups)
    for group_index in group_index_per_row
        if group_index > 0
            n_rows_per_group[group_index] += 1
        end
    end
    @assert all(n_rows_per_group .> 0)

    plain_matrix = plain_array(matrix)
    element_type = eltype(plain_matrix)
    n_columns = size(plain_matrix, 2)
    output = Matrix{reduction_result_type(operation, element_type)}(undef, n_groups, n_columns)
    empty_state = empty_reduction_state(operation, element_type)
    buffers = ReusableStorage(() -> (Vector{typeof(empty_state)}(undef, n_groups), Vector{Int}(undef, n_groups)))

    parallel_loop_wo_rng(
        1:n_columns;
        name = "compute_grouped_reduction",
        progress = DebugProgress(n_columns; group = :daf_loops, desc = "compute_grouped_reduction"),
    ) do column_index
        with_reusable(buffers) do (state_per_group, n_seen_per_group)
            state_per_group .= Ref(empty_state)
            n_seen_per_group .= 0
            reduce_column_per_group!(
                operation,
                state_per_group,
                n_seen_per_group,
                plain_matrix,
                column_index,
                group_index_per_row,
            )
            for group_index in 1:n_groups
                output[group_index, column_index] = final_reduction_value(
                    operation,
                    state_per_group[group_index],
                    n_seen_per_group[group_index],
                    n_rows_per_group[group_index],
                )
            end
            return nothing
        end
        return nothing
    end

    return output
end

# Update the state of each group with the stored values of one column of a sparse matrix.
function reduce_column_per_group!(
    operation::ReductionOperation,
    state_per_group::AbstractVector,
    n_seen_per_group::AbstractVector{<:Integer},
    matrix::SparseMatrixCSC,
    column_index::Integer,
    group_index_per_row::AbstractVector{<:Integer},
)::Nothing
    row_index_per_stored = rowvals(matrix)
    value_per_stored = nonzeros(matrix)
    @inbounds for stored_index in nzrange(matrix, column_index)
        group_index = group_index_per_row[row_index_per_stored[stored_index]]
        if group_index > 0
            state_per_group[group_index] =
                update_reduction_state(operation, state_per_group[group_index], value_per_stored[stored_index])
            n_seen_per_group[group_index] += 1
        end
    end
    return nothing
end

# Update the state of each group with all the values of one column of a dense matrix.
function reduce_column_per_group!(
    operation::ReductionOperation,
    state_per_group::AbstractVector,
    n_seen_per_group::AbstractVector{<:Integer},
    matrix::AbstractMatrix,
    column_index::Integer,
    group_index_per_row::AbstractVector{<:Integer},
)::Nothing
    @inbounds for row_index in axes(matrix, 1)
        group_index = group_index_per_row[row_index]
        if group_index > 0
            state_per_group[group_index] =
                update_reduction_state(operation, state_per_group[group_index], matrix[row_index, column_index])
            n_seen_per_group[group_index] += 1
        end
    end
    return nothing
end

end # module
