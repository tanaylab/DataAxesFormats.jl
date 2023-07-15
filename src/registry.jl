"""
Registering element-wise and reduction operations is required, to allow them to be used in a query.

!!! note

    We do not re-export everything from here to the main `Daf` namespace, as it is only of interest for implementers of
    new query operations. Most users of `Daf` just stick with the (fairly comprehensive) list of built-in query
    operations so there's no need to pollute their namespace with these detail.
"""
module Registry

export compute_eltwise
export compute_reduction
export EltwiseOperation
export float_dtype_for
export @query_operation
export ReductionOperation
export register_query_operation
export same_dtype_for

using Daf.DataTypes

FLOAT_DTYPE = Dict{Type, Type}(
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
    float_dtype_for(element_type::Type, dtype::Union{Type, Nothing})::Type

Given an input `element_type` and the value of the mandatory `dtype` operation parameter, return the data type to use
for the result of an operation that always produces floating point values (e.g., `Log`).
"""
function float_dtype_for(element_type::Type, dtype::Union{Type, Nothing})::Type
    if dtype == nothing
        return FLOAT_DTYPE[element_type]
    else
        return dtype
    end
end

"""
    same_dtype_for(element_type::Type, dtype::Union{Type, Nothing})::Type

Given an input `element_type` and the value of the mandatory `dtype` operation parameter, return the data type to use
for the result of an operation that does not modify the type of the data (e.g., `Min`).
"""
function same_dtype_for(element_type::Type, dtype::Union{Type, Nothing})::Type
    if dtype == nothing
        return element_type
    else
        return dtype
    end
end

# An operation in the global registry (used for parsing).
struct RegisteredOperation
    type::Type
    source_file::AbstractString
    source_line::Int
end

# Abstract interface for all query operations.
abstract type AbstractOperation end

"""
Abstract type for all element-wise operations.

An element-wise operation may be applied to matrix or vector data. It will preserve the shape of the data, but changes
the values, and possibly the data type of the elements. For example, `Abs` will compute the absolute value of each
element.

To implement a new such operation, the type is expected to be of the form:

    struct MyOperation <: EltwiseOperation
        dtype::Union{Type, Nothing}
        ... other parameters ...
    end
    @query_operation MyOperation

    MyOperation(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::MyOperation

The constructor should use `parse_parameter` for each of the parameters (using `parse_dtype_assignment` for the
mandatory `dtype` parameter, and typically `parse_number_assignment` for the rest). In addition you will need to invoke
[`@query_operation`](@ref) to register the operation so it can be used in a query, and implement the functions listed
below. See the query operations module for details and examples.
"""
abstract type EltwiseOperation <: AbstractOperation end

"""
    compute_eltwise(operation::EltwiseOperation, input::StorageMatrix)::StorageMatrix
    compute_eltwise(operation::EltwiseOperation, input::StorageVector)::StorageVector
    compute_eltwise(operation::EltwiseOperation, input_value::Number)::Number

Compute an [`EltwiseOperation`](@ref) `operation`.
"""
function compute_eltwise(operation::EltwiseOperation, input::Any)::Nothing  # untested
    return error("missing method: compute_eltwise ($(typeof(input)) for the operation: $(typeof(operation))")
end

"""
Abstract type for all reduction operations.

A reduction operation may be applied to matrix or vector data. It will reduce (eliminate) one dimension of the data, and
possibly the result will have a different data type than the input. When applied to a vector, the operation will return
a scalar. When applied to a matrix, it assumes the matrix is in column-major layout, and will return a vector with one
entry per column, containing the result of reducing the column to a scalar.

To implement a new such operation, the type is expected to be of the form:

    struct MyOperation <: ReductionOperation
        dtype::Union{Type, Nothing}
        ... other parameters ...
    end

    MyOperation(context::QueryContext, parameters_assignments::Dict{String, QueryOperation})::MyOperation

The constructor should use `parse_parameter` for each of the parameters (using `parse_dtype_assignment` for the
mandatory `dtype` parameter, and typically `parse_number_assignment` for the rest). In addition you will need to invoke
[`@query_operation`](@ref) to register the operation so it can be used in a query, and implement the functions listed
below. See the query operations module for details and examples.
"""
abstract type ReductionOperation <: AbstractOperation end

"""
    compute_reduction(operation::ReductionOperation, input::StorageMatrix)::StorageVector
    compute_reduction(operation::ReductionOperation, input::StorageVector)::Number

Compute an [`ReductionOperation`](@ref) `operation`.
"""
function compute_reduction(operation::ReductionOperation, input::Any)::StorageVector  # untested
    return error("missing method: compute_reduction ($(typeof(input)) for the operation: $(typeof(operation))")
end

# A global registry of all the known element-wise operations.
ELTWISE_REGISTERED_OPERATIONS = Dict{String, RegisteredOperation}()

# A global registry of all the known reduction operations.
REDUCTION_REGISTERED_OPERATIONS = Dict{String, RegisteredOperation}()

"""
    function register_query_operation(
        type::Type{T},
        source_file::AbstractString,
        source_line::Integer,
    )::Nothing where {T <: Union{EltwiseOperation, ReductionOperation}}

Register a specific operation so it would be available inside queries. This is required to be able to parse the
operation. This is idempotent (safe to invoke multiple times).

This isn't usually called directly. Instead, it is typically invoked by using the [`@query_operation`](@ref) macro.
"""
function register_query_operation(
    type::Type{T},
    source_file::AbstractString,
    source_line::Integer,
)::Nothing where {T <: Union{EltwiseOperation, ReductionOperation}}
    if T <: EltwiseOperation
        global ELTWISE_REGISTERED_OPERATIONS
        registered_operations = ELTWISE_REGISTERED_OPERATIONS
        kind = "eltwise"
    elseif T <: ReductionOperation
        global REDUCTION_REGISTERED_OPERATIONS
        registered_operations = REDUCTION_REGISTERED_OPERATIONS
        kind = "reduction"
    else
        @assert false  # untested
    end

    name = String(type.name.name)
    if name in keys(registered_operations)
        previous_registration = registered_operations[name]
        if previous_registration.type != type ||
           previous_registration.source_file != source_file ||
           previous_registration.source_line != source_line
            error(
                "conflicting registrations for the $(kind) operation: $(name)\n" *
                "1st in: $(previous_registration.source_file):$(previous_registration.source_line)\n" *
                "2nd in: $(source_file):$(source_line)",
            )
        end
    end

    if !(:dtype in fieldnames(T))
        error("missing field: dtype\n" * "for the $(kind) operation: $(name)\n" * "in: $(source_file):$(source_line)")
    end

    registered_operations[name] = RegisteredOperation(type, source_file, source_line)
    return nothing
end

"""
    struct MyOperation <: EltwiseOperation  # Or <: ReductionOperation
        ...
    end
    @query_operation MyOperation

Automatically call [`register_query_operation`](@ref) for `MyOperation`.

Note this will import `Daf.Registry.register_query_operation`, so it may only be called from the top level scope of a
module.
"""
macro query_operation(operation_type_name)
    name_string = String(operation_type_name)
    name_reference = esc(operation_type_name)

    source_line = __source__.line
    source_file = String(__source__.file)

    module_name = Symbol("Daf_$(name_string)_$(source_line)")

    return quote
        import Daf.Registry.register_query_operation
        register_query_operation($name_reference, $source_file, $source_line)
    end
end

end
