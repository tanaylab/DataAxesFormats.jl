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

import ..Registry.buffer_reduction_eltype
import ..Registry.compute_eltwise
import ..Registry.compute_reduction
import ..Registry.empty_reduction_state
import ..Registry.final_reduction_value
import ..Registry.merge_reduction_states
import ..Registry.reduce_buffer!
import ..Registry.reduction_result_type
import ..Registry.supports_buffer_reduction
import ..Registry.supports_grouped_reduction
import ..Registry.supports_strings
import ..Registry.update_reduction_state
import ..Tokens.error_at_token
import ..Tokens.Token
import Base.Threads.maxthreadid
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

# An element-wise operation on a matrix, which `map_values!(output_values, input_values)` computes for some of its
# values, each on its own. The result has the same layout as the input. Since each value is computed on its own, a
# row-major input is computed as its column-major flip. Sparse input is computed on its stored values (see
# `compute_sparse_eltwise`), dense input on all of them (see `compute_dense_eltwise`).
function compute_blind_eltwise(
    map_values!::Function,
    input::StorageMatrix{<:StorageReal},
    ::Type{T},
    name::AbstractString,
)::StorageMatrix where {T <: StorageReal}
    if major_axis(input) == Rows
        flipped_input = flip(input)
        @assert flipped_input isa StorageMatrix{<:StorageReal}
        return flip(compute_blind_eltwise(map_values!, flipped_input, T, name))
    elseif issparse(input)
        return compute_sparse_eltwise(map_values!, input, T, name)
    else
        return compute_dense_eltwise(map_values!, input, T, name)
    end
end

# A matrix of the same layout as the `matrix`, computed by `compute(column_major_matrix)`. A row-major `matrix` is
# reported as inefficient, and is computed through copies to and from column-major.
function with_column_major(compute::Function, matrix::StorageMatrix)::StorageMatrix
    if major_axis(matrix) != Rows
        return compute(matrix)
    end
    @assert_matrix(matrix, Columns)
    column_major_matrix = relayout(matrix)
    @assert column_major_matrix isa StorageMatrix
    return relayout(compute(column_major_matrix))
end

# An element-wise operation on a column-major dense matrix, computed by `map_values!`. Plain storage is computed in
# parallel slices of its values, ignoring the columns. Any other matrix (a lazy one, or a view with gaps) is computed
# one column at a time.
function compute_dense_eltwise(
    map_values!::Function,
    input::StorageMatrix{<:StorageReal},
    ::Type{T},
    name::AbstractString,
)::StorageMatrix where {T <: StorageReal}
    output = Matrix{T}(undef, size(input))
    if column_scratch(base_array(input)) === nothing
        storage = plain_array(input)
        if storage isa DenseArray
            output_values = vec(output)
            input_values = vec(storage)
            parallel_foreach_block(length(input_values), name) do indices
                @views map_values!(output_values[indices], input_values[indices])
                return nothing
            end
            return output
        end
    end

    parallel_foreach_column(input; contiguous = true, name) do _, column_index, column
        @views map_values!(output[:, column_index], column)
        return nothing
    end
    return output
end

# The value `map_values!` computes for a zero of the `input_type`.
function mapped_zero(map_values!::Function, input_type::Type{<:StorageReal}, ::Type{T})::T where {T <: StorageReal}
    output_values = Vector{T}(undef, 1)
    map_values!(output_values, zeros(input_type, 1))
    return output_values[1]
end

# The values `map_values!` computes for the `input_values`, in parallel slices.
function mapped_values(
    map_values!::Function,
    input_values::AbstractVector,
    ::Type{T},
    name::AbstractString,
)::Vector{T} where {T <: StorageReal}
    output_values = Vector{T}(undef, length(input_values))
    parallel_foreach_block(length(input_values), name) do indices
        @views map_values!(output_values[indices], input_values[indices])
        return nothing
    end
    return output_values
end

# An element-wise operation on a column-major sparse matrix, computed by `map_values!` on its stored values. If the
# mapped zero is zero, the result is sparse (see `sparse_with_values`). Otherwise it is dense, filled with the mapped
# zero (see `compute_sparse_to_dense_eltwise`).
function compute_sparse_eltwise(
    map_values!::Function,
    input::StorageMatrix{<:StorageReal},
    ::Type{T},
    name::AbstractString,
)::StorageMatrix where {T <: StorageReal}
    zero_value = mapped_zero(map_values!, eltype(input), T)
    if zero_value != 0
        return compute_sparse_to_dense_eltwise(map_values!, input, zero_value, name)
    end

    sparse_input = plain_sparse_matrix(input)
    output_value_per_stored = mapped_values(map_values!, nonzeros(sparse_input), T, name)
    return sparse_with_values(sparse_input, output_value_per_stored)
end

# The plain storage of a column-major sparse matrix.
function plain_sparse_matrix(input::StorageMatrix)::SparseMatrixCSC
    plain_input = plain_array(input)
    return plain_input isa SparseMatrixCSC ? plain_input : SparseMatrixCSC(plain_input)
end

# A sparse matrix with the structure of the `sparse_input`, holding the `value_per_stored`. It shares the indices of the
# input, unless some of the values are zero, which are then dropped.
function sparse_with_values(
    sparse_input::SparseMatrixCSC,
    value_per_stored::Vector{T},
)::SparseMatrixCSC{T} where {T <: StorageReal}
    if any(iszero, value_per_stored)
        return dropzeros!(
            SparseMatrixCSC(
                size(sparse_input, 1),
                size(sparse_input, 2),
                copy(SparseArrays.getcolptr(sparse_input)),
                copy(rowvals(sparse_input)),
                value_per_stored,
            ),
        )
    end
    return SparseMatrixCSC(
        size(sparse_input, 1),
        size(sparse_input, 2),
        SparseArrays.getcolptr(sparse_input),
        rowvals(sparse_input),
        value_per_stored,
    )
end

# An operation on each column of a matrix, which `map_column!(output_values, input_values)` computes from the values of
# the column. The result has the same layout as the input (see `with_column_major`). The `map_column!` must map a zero
# to a zero. Zeros must also not change how it maps the other values of the column. So sparse input is computed on the
# stored values of each column only, and gives a sparse result (see `sparse_with_values`). Dense input is computed one
# column at a time.
function compute_columns_eltwise(
    map_column!::Function,
    input::StorageMatrix{<:StorageReal},
    ::Type{T},
    name::AbstractString,
)::StorageMatrix where {T <: StorageReal}
    return with_column_major(input) do column_major_input
        if issparse(column_major_input)
            sparse_input = plain_sparse_matrix(column_major_input)
            colptr = SparseArrays.getcolptr(sparse_input)
            output_value_per_stored = Vector{T}(undef, nnz(sparse_input))
            parallel_foreach_column(sparse_input; name) do _, column_index, column
                @views map_column!(
                    output_value_per_stored[colptr[column_index]:(colptr[column_index + 1] - 1)],
                    nonzeros(column),
                )
                return nothing
            end
            return sparse_with_values(sparse_input, output_value_per_stored)
        else
            output = Matrix{T}(undef, size(column_major_input))
            parallel_foreach_column(column_major_input; contiguous = true, name) do _, column_index, column
                @views map_column!(output[:, column_index], column)
                return nothing
            end
            return output
        end
    end
end

# An element-wise operation on a column-major sparse matrix whose `zero_value` isn't zero. The result is dense, filled
# with the `zero_value`, with the stored values mapped into it one column at a time.
function compute_sparse_to_dense_eltwise(
    map_values!::Function,
    input::StorageMatrix{<:StorageReal},
    zero_value::T,
    name::AbstractString,
)::Matrix{T} where {T <: StorageReal}
    output = fill(zero_value, size(input))
    mapped_values_per_thread = [Vector{T}() for _ in 1:maxthreadid()]
    parallel_foreach_column(input; name) do _, column_index, column  # NOJET
        mapped_column_values = mapped_values_per_thread[threadid()]
        resize!(mapped_column_values, length(nonzeros(column)))
        map_values!(mapped_column_values, nonzeros(column))
        for (position, row_index) in enumerate(SparseArrays.nonzeroinds(column))
            output[row_index, column_index] = mapped_column_values[position]
        end
        return nothing
    end
    return output
end

# An element-wise operation on a sparse vector, computed by `map_values!` on its stored values, as for a sparse matrix.
function compute_sparse_eltwise(
    map_values!::Function,
    input::AbstractVector,
    ::Type{T},
    name::AbstractString,
)::StorageVector where {T <: StorageReal}
    sparse_input = plain_array(input)
    @assert sparse_input isa SparseVector
    index_per_stored = SparseArrays.nonzeroinds(sparse_input)

    zero_value = mapped_zero(map_values!, eltype(sparse_input), T)
    output_value_per_stored = mapped_values(map_values!, nonzeros(sparse_input), T, name)
    if zero_value == 0
        if any(iszero, output_value_per_stored)
            return dropzeros!(SparseVector(length(sparse_input), copy(index_per_stored), output_value_per_stored))
        end
        return SparseVector(length(sparse_input), index_per_stored, output_value_per_stored)
    end

    output = fill(zero_value, length(sparse_input))
    for (stored_position, index) in enumerate(index_per_stored)
        output[index] = output_value_per_stored[stored_position]
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
    return compute_blind_eltwise(input, type, "Abs") do output_values, input_values
        output_values .= abs.(input_values)
        return nothing
    end
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
    return compute_blind_eltwise(input, type, "Round") do output_values, input_values
        output_values .= round.(eltype(output_values), input_values)
        return nothing
    end
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
    return compute_blind_eltwise(input, type, "Clamp") do output_values, input_values
        output_values .= clamp.(input_values, clamp_min, clamp_max)
        return nothing
    end
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
    return compute_blind_eltwise(input, operation.type, "Convert") do output_values, input_values
        output_values .= input_values
        return nothing
    end
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
    return compute_columns_eltwise(input, type, "Fraction") do output_values, input_values
        column_sum = sum(input_values)
        if column_sum == 0
            output_values .= 0
        else
            output_values .= input_values ./ column_sum
        end
        return nothing
    end
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

# The `map_values!` of a `Log` into values of the type `T`.
function log_values_mapper(operation::Log, ::Type{T})::Function where {T <: AbstractFloat}
    eps = T(operation.eps)
    base = operation.base
    return (output_values, input_values) -> log_vector!(output_values, input_values, eps, base)
end

function compute_eltwise(operation::Log, input::StorageMatrix)::StorageMatrix
    type = float_type_for(eltype(input), operation.type)
    return compute_blind_eltwise(log_values_mapper(operation, type), input, type, "Log")
end

function compute_eltwise(operation::Log, input::StorageVector{<:StorageReal})::StorageVector{<:StorageReal}
    if issparse(input)
        type = float_type_for(eltype(input), operation.type)
        return compute_sparse_eltwise(log_values_mapper(operation, type), input, type, "Log")
    end
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
    high = operation.high
    low = operation.low
    return with_column_major(input) do column_major_input
        output = compute_columns_eltwise(
            column_major_input,
            eltype(column_major_input),
            "Significant",
        ) do output_values, input_values
            significant!(output_values, input_values, high, low)
            return nothing
        end
        if issparse(output) || all(!iszero, output)
            return output
        else
            return SparseMatrixCSC(output)
        end
    end
end

function compute_eltwise(operation::Significant, input::StorageVector{T})::StorageVector{T} where {T <: StorageReal}
    output = copy_array(input)

    if issparse(output)
        significant!(nzval(output), nzval(output), operation.high, operation.low)
        dropzeros!(output)
        return output
    else
        significant!(output, output, operation.high, operation.low)
        if any(output .== 0)
            return SparseVector(output)
        else
            return output
        end
    end
end

# Write the significant values of the `input_values` into the `output_values`, and zeros instead of the others (see
# `Significant`). Both may be the same vector.
function significant!(
    output_values::AbstractVector{<:StorageReal},
    input_values::AbstractVector{<:StorageReal},
    high::StorageReal,
    low::StorageReal,
)::Nothing
    is_significant = false
    for value in input_values
        if !(-high < value < high)
            is_significant = true
            break
        end
    end
    if is_significant
        for index in eachindex(output_values, input_values)
            value = input_values[index]
            output_values[index] = -low < value < low ? zero(value) : value
        end
    else
        output_values .= 0
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

function merge_reduction_states(::Count, ::Nothing, ::Nothing)::Nothing
    return nothing
end

function final_reduction_value(::Count, ::Nothing, ::Integer, n_values::Integer)::Integer
    return n_values
end

"""
    Mode()

Reduction operation that returns the most frequent value in the input (the "mode"). If several values are the most
frequent, this returns the smallest of them, so the result doesn't depend on the order of the values.

!!! note

    This operation supports strings; most operations do not.
"""
struct Mode <: ReductionOperation end
@query_operation Mode

function Mode(::Token, ::Dict{String, Token})::Mode
    return Mode()
end

function supports_buffer_reduction(::Mode)::Bool
    return true
end

function buffer_reduction_eltype(::Mode, eltype::Type)::Type
    return eltype
end

function reduce_buffer!(::Mode, buffer::AbstractVector, n_implicit_zero::Integer)::StorageScalar
    return mode_of_buffer!(buffer, n_implicit_zero)
end

# The most frequent value of the values in the `buffer` and `n_implicit_zero` more zeros, the smallest of them if
# several are the most frequent. This sorts the `buffer`.
function mode_of_buffer!(buffer::AbstractVector, n_implicit_zero::Integer)::StorageScalar
    @assert !isempty(buffer) || n_implicit_zero > 0 "empty input"
    sort!(buffer)
    mode_value = nothing
    mode_count = 0
    has_explicit_zero = false
    run_start = 1
    while run_start <= length(buffer)
        run_value = buffer[run_start]
        run_end = run_start
        while run_end < length(buffer) && buffer[run_end + 1] == run_value
            run_end += 1
        end
        run_count = run_end - run_start + 1
        if n_implicit_zero > 0 && iszero(run_value)
            has_explicit_zero = true
            run_count += n_implicit_zero
        end
        if run_count > mode_count
            mode_value = run_value
            mode_count = run_count
        end
        run_start = run_end + 1
    end
    if n_implicit_zero > 0 && !has_explicit_zero
        if mode_value === nothing || n_implicit_zero > mode_count || (n_implicit_zero == mode_count && mode_value > 0)
            mode_value = zero(eltype(buffer))
        end
    end
    @assert mode_value !== nothing
    return mode_value
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

function merge_reduction_states(::Sum, sum::StorageReal, other_sum::StorageReal)::StorageReal
    return oftype(sum, sum + other_sum)
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

function merge_reduction_states(::Max, maximal::StorageReal, other_maximal::StorageReal)::StorageReal
    return max(maximal, other_maximal)
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

function merge_reduction_states(::Min, minimal::StorageReal, other_minimal::StorageReal)::StorageReal
    return min(minimal, other_minimal)
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

function reduction_result_type(operation::Median, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_buffer_reduction(::Median)::Bool
    return true
end

function buffer_reduction_eltype(::Median, ::Type)::Type
    return Float64
end

function reduce_buffer!(::Median, buffer::AbstractVector{Float64}, n_implicit_zero::Integer)::Float64
    return sparse_quantile!(buffer, 0.5; n_implicit_zero)
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

function reduction_result_type(operation::Quantile, eltype::Type)::Type
    return float_type_for(eltype, operation.type)
end

function supports_buffer_reduction(::Quantile)::Bool
    return true
end

function buffer_reduction_eltype(::Quantile, ::Type)::Type
    return Float64
end

function reduce_buffer!(operation::Quantile, buffer::AbstractVector{Float64}, n_implicit_zero::Integer)::Float64
    return sparse_quantile!(buffer, operation.p; n_implicit_zero)
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

function merge_reduction_states(::Mean, sum::Float64, other_sum::Float64)::Float64
    return sum + other_sum
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

function merge_reduction_states(::GeoMean, sum_of_logs::Float64, other_sum_of_logs::Float64)::Float64
    return sum_of_logs + other_sum_of_logs
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

# The moments of the values of both states (Chan's parallel algorithm).
function merge_moments_states(state::MomentsState, other_state::MomentsState)::MomentsState
    n_seen, mean_of_seen, squared_differences = state
    other_n_seen, other_mean_of_seen, other_squared_differences = other_state
    if other_n_seen == 0
        return state
    elseif n_seen == 0
        return other_state
    end
    merged_n_seen = n_seen + other_n_seen
    difference = other_mean_of_seen - mean_of_seen
    merged_mean_of_seen = mean_of_seen + difference * other_n_seen / merged_n_seen
    merged_squared_differences =
        squared_differences + other_squared_differences + difference^2 * n_seen * other_n_seen / merged_n_seen
    return (merged_n_seen, merged_mean_of_seen, merged_squared_differences)
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

function merge_reduction_states(::Var, state::MomentsState, other_state::MomentsState)::MomentsState
    return merge_moments_states(state, other_state)
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

function merge_reduction_states(::VarN, state::MomentsState, other_state::MomentsState)::MomentsState
    return merge_moments_states(state, other_state)
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

function merge_reduction_states(::Std, state::MomentsState, other_state::MomentsState)::MomentsState
    return merge_moments_states(state, other_state)
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

function merge_reduction_states(::StdN, state::MomentsState, other_state::MomentsState)::MomentsState
    return merge_moments_states(state, other_state)
end

function final_reduction_value(operation::StdN, state::MomentsState, ::Integer, n_values::Integer)::Float64
    mean, variance = moments_with_zeros(state, n_values)
    return sqrt(variance) / (mean + operation.eps)
end

# A reduction `operation` that implements the streaming or the buffer protocol is computed by the two methods below,
# unless it implements `compute_reduction` itself. A matrix is read one column at a time (through
# `parallel_foreach_column`), so a lazy matrix is never materialized. A row-major matrix is reduced as its column-major
# `flip`, along the other axis.
function compute_reduction(operation::ReductionOperation, input::StorageMatrix, axis::Integer)::StorageVector
    @assert 1 <= axis <= 2
    require_reduction_protocol(operation)
    if major_axis(input) == Rows
        return compute_reduction(operation, flip(input), other_axis(axis))
    elseif axis == 1
        # All the rows are in one group.
        return vec(compute_grouped_reduction(operation, input, StepRangeLen(1, 0, size(input, 1)), 1))
    else
        return compute_columns_reduction(operation, input)
    end
end

function compute_reduction(operation::ReductionOperation, input::Union{StorageVector, StorageMatrix})::StorageScalar
    require_reduction_protocol(operation)
    if input isa StorageMatrix && major_axis(input) == Rows
        return compute_reduction(operation, flip(input))
    end
    if input isa StorageVector
        value = compute_vector_reduction(operation, plain_array(input))
    elseif supports_grouped_reduction(operation)
        value = compute_streamed_matrix_reduction(operation, input)
    else
        value = compute_buffered_matrix_reduction(operation, input)
    end
    return convert(reduction_result_type(operation, eltype(input)), value)
end

function require_reduction_protocol(operation::ReductionOperation)::Nothing
    if !supports_grouped_reduction(operation) && !supports_buffer_reduction(operation)
        error(
            "the reduction operation: $(typeof(operation))\n" *
            "implements neither compute_reduction nor the streaming or buffer protocol",
        )
    end
    return nothing
end

# Reduce the values of a plain vector: the stored values of a sparse vector, or all of them. The implicit zeros of a
# sparse vector are accounted for by the protocol.
function compute_vector_reduction(operation::ReductionOperation, vector::AbstractVector)::StorageScalar
    value_per_stored = issparse(vector) ? nonzeros(vector) : vector
    if supports_grouped_reduction(operation)
        state = updated_reduction_state(operation, empty_reduction_state(operation, eltype(vector)), value_per_stored)
        return final_reduction_value(operation, state, length(value_per_stored), length(vector))
    else
        buffer = Vector{buffer_reduction_eltype(operation, eltype(vector))}(value_per_stored)
        return reduce_buffer!(operation, buffer, length(vector) - length(value_per_stored))
    end
end

# The `state` of the reduction `operation` after also seeing each of the `values`.
function updated_reduction_state(operation::ReductionOperation, state::S, values::AbstractVector)::S where {S}
    for value in values
        state = update_reduction_state(operation, state, value)
    end
    return state
end

"""
    compute_grouped_reduction(
        operation::ReductionOperation,
        matrix::StorageMatrix,
        group_index_per_row::AbstractVector{<:Integer},
        n_groups::Integer,
    )::StorageMatrix

Reduce the rows of a `matrix` per group. The result has one row per group, and one column per column of the `matrix`. It
has the same layout as the `matrix`. The group of each row is given by `group_index_per_row`, where 0 means the row is
in no group. Each group must have at least one row. The `operation` must [`supports_grouped_reduction`](@ref) or
[`supports_buffer_reduction`](@ref).

Each column is visited once, so the time is proportional to the number of values, plus the number of groups for each
column. For a sparse matrix only the stored values are visited. The columns are read using the TanayLabUtilities
`parallel_foreach_column`, so a lazy `matrix` is read one column at a time. A row-major `matrix` is reported to the
TanayLabUtilities `GLOBAL_INEFFICIENT_ACTION_HANDLER`, and is reduced through copies to and from column-major.
"""
function compute_grouped_reduction(
    operation::ReductionOperation,
    matrix::StorageMatrix,
    group_index_per_row::AbstractVector{<:Integer},
    n_groups::Integer,
)::StorageMatrix
    @assert supports_grouped_reduction(operation) || supports_buffer_reduction(operation)
    @assert length(group_index_per_row) == size(matrix, 1)

    n_rows_per_group = zeros(Int, n_groups)
    for group_index in group_index_per_row
        if group_index > 0
            n_rows_per_group[group_index] += 1
        end
    end
    @assert all(n_rows_per_group .> 0)

    return with_column_major(matrix) do column_major_matrix
        output = Matrix{reduction_result_type(operation, eltype(column_major_matrix))}(
            undef,
            n_groups,
            size(column_major_matrix, 2),
        )
        if supports_grouped_reduction(operation)
            compute_streamed_grouped_reduction!(
                output,
                operation,
                column_major_matrix,
                group_index_per_row,
                n_rows_per_group,
            )
        else
            compute_buffered_grouped_reduction!(
                output,
                operation,
                column_major_matrix,
                group_index_per_row,
                n_rows_per_group,
            )
        end
        return output
    end
end

# Reduce the rows of the `matrix` per group into the `output` by updating a state per group with each value of each
# column.
function compute_streamed_grouped_reduction!(
    output::AbstractMatrix,
    operation::ReductionOperation,
    matrix::StorageMatrix,
    group_index_per_row::AbstractVector{<:Integer},
    n_rows_per_group::AbstractVector{<:Integer},
)::Nothing
    n_groups = length(n_rows_per_group)
    empty_state = empty_reduction_state(operation, eltype(matrix))
    state_per_group_per_thread = [Vector{typeof(empty_state)}(undef, n_groups) for _ in 1:maxthreadid()]
    n_seen_per_group_per_thread = [Vector{Int}(undef, n_groups) for _ in 1:maxthreadid()]

    parallel_foreach_column(  # NOJET
        matrix;
        name = "compute_grouped_reduction",
        progress = DebugProgress(size(matrix, 2); group = :daf_loops, desc = "compute_grouped_reduction"),
    ) do _, column_index, column
        state_per_group = state_per_group_per_thread[threadid()]
        n_seen_per_group = n_seen_per_group_per_thread[threadid()]
        state_per_group .= Ref(empty_state)
        n_seen_per_group .= 0
        row_index_per_stored, value_per_stored = stored_entries_of_column(column)
        reduce_column_per_group!(
            operation,
            state_per_group,
            n_seen_per_group,
            row_index_per_stored,
            value_per_stored,
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

# The row index and the value of each stored entry of a column of `parallel_foreach_column`: the stored entries of a
# sparse column, or all the entries of a dense one.
function stored_entries_of_column(
    column::SparseArrays.SparseVectorUnion,
)::Tuple{AbstractVector{<:Integer}, AbstractVector}
    return (SparseArrays.nonzeroinds(column), nonzeros(column))
end

function stored_entries_of_column(column::AbstractVector)::Tuple{AbstractVector{<:Integer}, AbstractVector}
    return (eachindex(column), column)
end

# Reduce the rows of the `matrix` per group into the `output` by placing the values of each group of each column in a
# buffer, then reducing each group's slice of it, together with its implicit zeros.
function compute_buffered_grouped_reduction!(
    output::AbstractMatrix,
    operation::ReductionOperation,
    matrix::StorageMatrix,
    group_index_per_row::AbstractVector{<:Integer},
    n_rows_per_group::AbstractVector{<:Integer},
)::Nothing
    n_groups = length(n_rows_per_group)
    buffer_eltype = buffer_reduction_eltype(operation, eltype(matrix))
    n_grouped_rows = sum(n_rows_per_group)
    buffer_per_thread = [Vector{buffer_eltype}(undef, n_grouped_rows) for _ in 1:maxthreadid()]
    n_stored_per_group_per_thread = [Vector{Int}(undef, n_groups) for _ in 1:maxthreadid()]
    next_position_per_group_per_thread = [Vector{Int}(undef, n_groups) for _ in 1:maxthreadid()]

    parallel_foreach_column(
        matrix;
        name = "compute_grouped_reduction",
        progress = DebugProgress(size(matrix, 2); group = :daf_loops, desc = "compute_grouped_reduction"),
    ) do _, column_index, column
        buffer = buffer_per_thread[threadid()]
        n_stored_per_group = n_stored_per_group_per_thread[threadid()]
        next_position_per_group = next_position_per_group_per_thread[threadid()]
        row_index_per_stored, value_per_stored = stored_entries_of_column(column)
        place_values_per_group!(
            buffer,
            n_stored_per_group,
            next_position_per_group,
            (row_index_per_stored,),
            (value_per_stored,),
            group_index_per_row,
        )
        for group_index in 1:n_groups
            n_stored = n_stored_per_group[group_index]
            last_position = next_position_per_group[group_index] - 1
            output[group_index, column_index] = reduce_buffer!(
                operation,
                view(buffer, (last_position - n_stored + 1):last_position),
                n_rows_per_group[group_index] - n_stored,
            )
        end
        return nothing
    end

    return nothing
end

# Place the stored values of one or more parts (e.g. a column) in the `buffer`, the values of each group together, in
# order. Afterwards, the values of a group end just before its `next_position_per_group`.
function place_values_per_group!(
    buffer::AbstractVector,
    n_stored_per_group::AbstractVector{<:Integer},
    next_position_per_group::AbstractVector{<:Integer},
    row_index_per_stored_per_part::Union{Tuple, AbstractVector{<:AbstractVector{<:Integer}}},
    value_per_stored_per_part::Union{Tuple, AbstractVector{<:AbstractVector}},
    group_index_per_row::AbstractVector{<:Integer},
)::Nothing
    n_stored_per_group .= 0
    for row_index_per_stored in row_index_per_stored_per_part
        count_stored_per_group!(n_stored_per_group, row_index_per_stored, group_index_per_row)
    end

    position = 1
    @inbounds for group_index in eachindex(n_stored_per_group)
        next_position_per_group[group_index] = position
        position += n_stored_per_group[group_index]
    end

    for (row_index_per_stored, value_per_stored) in zip(row_index_per_stored_per_part, value_per_stored_per_part)
        place_stored_per_group!(
            buffer,
            next_position_per_group,
            row_index_per_stored,
            value_per_stored,
            group_index_per_row,
        )
    end

    return nothing
end

# Count the stored values of one part per group.
function count_stored_per_group!(
    n_stored_per_group::AbstractVector{<:Integer},
    row_index_per_stored::AbstractVector{<:Integer},
    group_index_per_row::AbstractVector{<:Integer},
)::Nothing
    @inbounds for row_index in row_index_per_stored
        group_index = group_index_per_row[row_index]
        if group_index > 0
            n_stored_per_group[group_index] += 1
        end
    end
    return nothing
end

# Place the stored values of one part at the next position of their group.
function place_stored_per_group!(
    buffer::AbstractVector,
    next_position_per_group::AbstractVector{<:Integer},
    row_index_per_stored::AbstractVector{<:Integer},
    value_per_stored::AbstractVector,
    group_index_per_row::AbstractVector{<:Integer},
)::Nothing
    @inbounds for (row_index, value) in zip(row_index_per_stored, value_per_stored)
        group_index = group_index_per_row[row_index]
        if group_index > 0
            buffer[next_position_per_group[group_index]] = value
            next_position_per_group[group_index] += 1
        end
    end
    return nothing
end

# Update the state of each group with the stored values of one column.
function reduce_column_per_group!(
    operation::ReductionOperation,
    state_per_group::AbstractVector,
    n_seen_per_group::AbstractVector{<:Integer},
    row_index_per_stored::AbstractVector{<:Integer},
    value_per_stored::AbstractVector,
    group_index_per_row::AbstractVector{<:Integer},
)::Nothing
    @inbounds for (row_index, value) in zip(row_index_per_stored, value_per_stored)
        group_index = group_index_per_row[row_index]
        if group_index > 0
            state_per_group[group_index] = update_reduction_state(operation, state_per_group[group_index], value)
            n_seen_per_group[group_index] += 1
        end
    end
    return nothing
end

# Reduce the columns of each group of a column-major `matrix` into the `output`, which has one row per row of the
# `matrix` and one column per group. Each group is a non-empty vector of column indices. The groups are reduced in
# parallel, and each one reads its columns once, keeping a state (or a buffer of values) per row.
function compute_grouped_columns_reduction!(
    output::AbstractMatrix,
    operation::ReductionOperation,
    matrix::StorageMatrix,
    column_indices_per_group::AbstractVector{<:AbstractVector{<:Integer}},
)::Nothing
    @assert supports_grouped_reduction(operation) || supports_buffer_reduction(operation)
    @assert require_major_axis(matrix) == Columns
    @assert size(output) == (size(matrix, 1), length(column_indices_per_group))
    @assert all(!isempty(column_indices) for column_indices in column_indices_per_group)

    weight_per_group = n_values_per_group_of_columns(matrix, column_indices_per_group)
    column_reader_per_thread = new_column_reader_per_thread(matrix)
    if supports_grouped_reduction(operation)
        compute_streamed_grouped_columns_reduction!(
            output,
            operation,
            eltype(matrix),
            column_reader_per_thread,
            column_indices_per_group,
            weight_per_group,
        )
    else
        compute_buffered_grouped_columns_reduction!(
            output,
            operation,
            buffer_reduction_eltype(operation, eltype(matrix)),
            column_reader_per_thread,
            column_indices_per_group,
            weight_per_group,
        )
    end
    return nothing
end

# The number of values the columns of each group hold: the stored ones for a sparse `matrix`, all of them otherwise.
function n_values_per_group_of_columns(
    matrix::StorageMatrix,
    column_indices_per_group::AbstractVector{<:AbstractVector{<:Integer}},
)::Vector{Int}
    base_matrix = base_array(matrix)
    if base_matrix isa SparseArrays.AbstractSparseMatrixCSC
        column_pointers = SparseArrays.getcolptr(base_matrix)
        return [
            sum(
                Int(column_pointers[column_index + 1] - column_pointers[column_index]) for
                column_index in column_indices
            ) for column_indices in column_indices_per_group
        ]
    else
        return [length(column_indices) * size(matrix, 1) for column_indices in column_indices_per_group]
    end
end

# Reduce the columns of each group into the `output` by updating a state per row with each value of each column.
function compute_streamed_grouped_columns_reduction!(
    output::AbstractMatrix,
    operation::ReductionOperation,
    input_eltype::Type{T},
    column_reader_per_thread::AbstractVector{<:ColumnReader},
    column_indices_per_group::AbstractVector{<:AbstractVector{<:Integer}},
    weight_per_group::AbstractVector{<:Integer},
)::Nothing where {T}
    n_rows = size(output, 1)
    n_groups = length(column_indices_per_group)
    empty_state = empty_reduction_state(operation, input_eltype)
    state_per_row_per_thread = [Vector{typeof(empty_state)}(undef, n_rows) for _ in 1:maxthreadid()]
    n_seen_per_row_per_thread = [Vector{Int}(undef, n_rows) for _ in 1:maxthreadid()]

    parallel_loop_wo_rng(
        1:n_groups;
        name = "compute_grouped_columns_reduction",
        weights = weight_per_group,
        progress = DebugProgress(n_groups; group = :daf_loops, desc = "compute_grouped_columns_reduction"),
    ) do group_index
        column_reader = column_reader_per_thread[threadid()]
        state_per_row = state_per_row_per_thread[threadid()]
        n_seen_per_row = n_seen_per_row_per_thread[threadid()]
        state_per_row .= Ref(empty_state)
        n_seen_per_row .= 0
        column_indices = column_indices_per_group[group_index]
        for column_index in column_indices
            row_index_per_stored, value_per_stored = stored_entries_of_column(read_column(column_reader, column_index))
            reduce_column_per_group!(
                operation,
                state_per_row,
                n_seen_per_row,
                row_index_per_stored,
                value_per_stored,
                1:n_rows,
            )
        end
        for row_index in 1:n_rows
            output[row_index, group_index] = final_reduction_value(
                operation,
                state_per_row[row_index],
                n_seen_per_row[row_index],
                length(column_indices),
            )
        end
        return nothing
    end

    return nothing
end

# Reduce the columns of each group into the `output` by collecting the stored values of its columns, placing the values
# of each row together in a buffer, then reducing each row's slice of it, together with its implicit zeros.
function compute_buffered_grouped_columns_reduction!(
    output::AbstractMatrix,
    operation::ReductionOperation,
    buffer_eltype::Type{T},
    column_reader_per_thread::AbstractVector{<:ColumnReader},
    column_indices_per_group::AbstractVector{<:AbstractVector{<:Integer}},
    weight_per_group::AbstractVector{<:Integer},
)::Nothing where {T}
    n_rows = size(output, 1)
    n_groups = length(column_indices_per_group)
    row_index_per_collected_per_thread = [Int[] for _ in 1:maxthreadid()]
    value_per_collected_per_thread = [Vector{buffer_eltype}() for _ in 1:maxthreadid()]
    buffer_per_thread = [Vector{buffer_eltype}() for _ in 1:maxthreadid()]
    n_stored_per_row_per_thread = [Vector{Int}(undef, n_rows) for _ in 1:maxthreadid()]
    next_position_per_row_per_thread = [Vector{Int}(undef, n_rows) for _ in 1:maxthreadid()]

    parallel_loop_wo_rng(
        1:n_groups;
        name = "compute_grouped_columns_reduction",
        weights = weight_per_group,
        progress = DebugProgress(n_groups; group = :daf_loops, desc = "compute_grouped_columns_reduction"),
    ) do group_index
        column_reader = column_reader_per_thread[threadid()]
        row_index_per_collected = row_index_per_collected_per_thread[threadid()]
        value_per_collected = value_per_collected_per_thread[threadid()]
        buffer = buffer_per_thread[threadid()]
        n_stored_per_row = n_stored_per_row_per_thread[threadid()]
        next_position_per_row = next_position_per_row_per_thread[threadid()]

        empty!(row_index_per_collected)
        empty!(value_per_collected)
        column_indices = column_indices_per_group[group_index]
        for column_index in column_indices
            row_index_per_stored, value_per_stored = stored_entries_of_column(read_column(column_reader, column_index))
            append!(row_index_per_collected, row_index_per_stored)
            append!(value_per_collected, value_per_stored)
        end

        resize!(buffer, length(value_per_collected))
        place_values_per_group!(
            buffer,
            n_stored_per_row,
            next_position_per_row,
            (row_index_per_collected,),
            (value_per_collected,),
            1:n_rows,
        )
        for row_index in 1:n_rows
            n_stored = n_stored_per_row[row_index]
            last_position = next_position_per_row[row_index] - 1
            output[row_index, group_index] = reduce_buffer!(
                operation,
                view(buffer, (last_position - n_stored + 1):last_position),
                length(column_indices) - n_stored,
            )
        end
        return nothing
    end

    return nothing
end

# Reduce all the columns of a column-major `matrix` into one value per row. Each column is read once, and its values are
# added to a state (or a list of values) per row of the thread that read it. The threads are then merged.
function compute_columns_reduction(operation::ReductionOperation, matrix::StorageMatrix)::Vector
    @assert supports_grouped_reduction(operation) || supports_buffer_reduction(operation)
    @assert require_major_axis(matrix) == Columns
    @assert size(matrix, 2) > 0

    output = Vector{reduction_result_type(operation, eltype(matrix))}(undef, size(matrix, 1))
    if supports_grouped_reduction(operation)
        compute_streamed_columns_reduction!(output, operation, matrix, eltype(matrix))
    else
        compute_buffered_columns_reduction!(
            output,
            operation,
            matrix,
            buffer_reduction_eltype(operation, eltype(matrix)),
        )
    end
    return output
end

# Reduce all the columns of the `matrix` into the `output` by updating a state per row per thread with each value of
# each column, then merging the states of the threads.
function compute_streamed_columns_reduction!(
    output::AbstractVector,
    operation::ReductionOperation,
    matrix::StorageMatrix,
    input_eltype::Type{T},
)::Nothing where {T}
    n_rows, n_columns = size(matrix)
    empty_state = empty_reduction_state(operation, input_eltype)
    state_per_row_per_thread = [fill(empty_state, n_rows) for _ in 1:maxthreadid()]
    n_seen_per_row_per_thread = [zeros(Int, n_rows) for _ in 1:maxthreadid()]

    parallel_foreach_column(
        matrix;
        name = "compute_columns_reduction",
        progress = DebugProgress(n_columns; group = :daf_loops, desc = "compute_columns_reduction"),
    ) do _, _, column
        row_index_per_stored, value_per_stored = stored_entries_of_column(column)
        reduce_column_per_group!(
            operation,
            state_per_row_per_thread[threadid()],
            n_seen_per_row_per_thread[threadid()],
            row_index_per_stored,
            value_per_stored,
            1:n_rows,
        )
        return nothing
    end

    parallel_foreach_block(n_rows, "compute_columns_reduction") do row_indices
        for row_index in row_indices
            state = state_per_row_per_thread[1][row_index]
            n_seen = n_seen_per_row_per_thread[1][row_index]
            for thread_index in 2:maxthreadid()
                state = merge_reduction_states(operation, state, state_per_row_per_thread[thread_index][row_index])
                n_seen += n_seen_per_row_per_thread[thread_index][row_index]
            end
            output[row_index] = final_reduction_value(operation, state, n_seen, n_columns)
        end
        return nothing
    end

    return nothing
end

# Reduce all the columns of the `matrix` into the `output` by collecting the stored values of the columns each thread
# read, placing the values of each row together in a buffer, then reducing each row's slice of it, together with its
# implicit zeros.
function compute_buffered_columns_reduction!(
    output::AbstractVector,
    operation::ReductionOperation,
    matrix::StorageMatrix,
    ::Type{T},
)::Nothing where {T}
    n_rows, n_columns = size(matrix)
    row_index_per_collected_per_thread = [Int[] for _ in 1:maxthreadid()]
    value_per_collected_per_thread = [Vector{T}() for _ in 1:maxthreadid()]

    parallel_foreach_column(
        matrix;
        name = "compute_columns_reduction",
        progress = DebugProgress(n_columns; group = :daf_loops, desc = "compute_columns_reduction"),
    ) do _, _, column
        row_index_per_stored, value_per_stored = stored_entries_of_column(column)
        append!(row_index_per_collected_per_thread[threadid()], row_index_per_stored)
        append!(value_per_collected_per_thread[threadid()], value_per_stored)
        return nothing
    end

    buffer = Vector{T}(undef, sum(length, value_per_collected_per_thread))
    n_stored_per_row = Vector{Int}(undef, n_rows)
    next_position_per_row = Vector{Int}(undef, n_rows)
    place_values_per_group!(
        buffer,
        n_stored_per_row,
        next_position_per_row,
        row_index_per_collected_per_thread,
        value_per_collected_per_thread,
        1:n_rows,
    )
    empty!(row_index_per_collected_per_thread)
    empty!(value_per_collected_per_thread)

    parallel_foreach_block(n_rows, "compute_columns_reduction") do row_indices
        for row_index in row_indices
            n_stored = n_stored_per_row[row_index]
            last_position = next_position_per_row[row_index] - 1
            output[row_index] = reduce_buffer!(
                operation,
                view(buffer, (last_position - n_stored + 1):last_position),
                n_columns - n_stored,
            )
        end
        return nothing
    end

    return nothing
end

# Run `visit(indices)` in parallel for blocks of `1:n`, a few per thread.
function parallel_foreach_block(visit::Function, n::Integer, name::AbstractString)::Nothing
    block_size = max(1, cld(n, 4 * nthreads()))
    indices_per_block = collect(Iterators.partition(1:n, block_size))
    parallel_loop_wo_rng(1:length(indices_per_block); name) do block_index
        visit(indices_per_block[block_index])
        return nothing
    end
    return nothing
end

# Reduce all the values of a column-major `matrix` into one value. Each column is read once, and its values are added to
# a state of the thread that read it. The states of the threads are then merged.
function compute_streamed_matrix_reduction(operation::ReductionOperation, matrix::StorageMatrix)::StorageReal
    @assert supports_grouped_reduction(operation)
    @assert require_major_axis(matrix) == Columns
    return compute_streamed_matrix_reduction(operation, matrix, eltype(matrix))
end

function compute_streamed_matrix_reduction(
    operation::ReductionOperation,
    matrix::StorageMatrix,
    input_eltype::Type{T},
)::StorageReal where {T}
    empty_state = empty_reduction_state(operation, input_eltype)
    state_per_thread = fill(empty_state, maxthreadid())
    n_seen_per_thread = zeros(Int, maxthreadid())

    parallel_foreach_column(
        matrix;
        name = "compute_matrix_reduction",
        progress = DebugProgress(size(matrix, 2); group = :daf_loops, desc = "compute_matrix_reduction"),
    ) do _, _, column
        _, value_per_stored = stored_entries_of_column(column)
        state_per_thread[threadid()] =
            updated_reduction_state(operation, state_per_thread[threadid()], value_per_stored)
        n_seen_per_thread[threadid()] += length(value_per_stored)
        return nothing
    end

    merged_state = reduce(
        (thread_state, other_thread_state) -> merge_reduction_states(operation, thread_state, other_thread_state),
        state_per_thread,
    )
    return final_reduction_value(operation, merged_state, sum(n_seen_per_thread), length(matrix))
end

# Reduce all the values of a column-major `matrix` into one value. Each column is read once, and its stored values are
# collected by the thread that read it. All the values are then reduced together, with the implicit zeros.
function compute_buffered_matrix_reduction(operation::ReductionOperation, matrix::StorageMatrix)::StorageScalar
    @assert supports_buffer_reduction(operation)
    @assert require_major_axis(matrix) == Columns
    return compute_buffered_matrix_reduction(operation, matrix, buffer_reduction_eltype(operation, eltype(matrix)))
end

function compute_buffered_matrix_reduction(
    operation::ReductionOperation,
    matrix::StorageMatrix,
    ::Type{T},
)::StorageScalar where {T}
    value_per_collected_per_thread = [Vector{T}() for _ in 1:maxthreadid()]

    parallel_foreach_column(
        matrix;
        name = "compute_matrix_reduction",
        progress = DebugProgress(size(matrix, 2); group = :daf_loops, desc = "compute_matrix_reduction"),
    ) do _, _, column
        _, value_per_stored = stored_entries_of_column(column)
        append!(value_per_collected_per_thread[threadid()], value_per_stored)
        return nothing
    end

    buffer = reduce(vcat, value_per_collected_per_thread)
    return reduce_buffer!(operation, buffer, length(matrix) - length(buffer))
end

end # module
