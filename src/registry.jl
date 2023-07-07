"""
Global registry of operations that can be used in a query.
"""
module Registry

export EltwiseOperation
export @query_operation
export ReductionOperation
export register_query_operation

# Abstract type for all query operations.
abstract type AbstractOperation end

# An operation in the global registry (used for parsing).
struct RegisteredOperation
    type::Type
    source_file::String
    source_line::Int
end

"""
Abstract type for all element-wise operations.

An element-wise operation may be applied to matrix or vector data. It will preserve the shape of the data, but changes
the values, and possibly the data type of the elements. For example, `Abs` will compute the absolute value of each
element.
"""
abstract type EltwiseOperation <: AbstractOperation end

"""
Abstract type for all reduction operations.

A reduction operation may be applied to matrix or vector data. It will reduce (eliminate) one dimension of the data, and
possibly the result will have a different data type than the input. When applied to a vector, the operation will return
a scalar. When applied to a matrix, the operation will produce a vector with an entry per each entry of the major axis
of the matrix. For example, `Sum` will compute the sum of the values in a vector. For a row-major matrix, it will
compute the sum of values in each row. For example, `cell , gene @ UMIs %> Sum` will compute the sum of the UMIs in each
cell. For a column-major matrix, it will compute the sum of values in each column. This means that changing the `,` to
`;`, that is, writing `cell ; gene @ UMIs %> Sum`, will compute the sum of the UMIs of each *gene*. It is preferable to
write `gene , cell @ UMIs %> Sum` to achieve the same result, that is, maintain the convention that the first axis of
the query will be used for the results.
"""
abstract type ReductionOperation <: AbstractOperation end

# A global registry of all the known element-wise operations.
ELTWISE_REGISTERED_OPERATIONS = Dict{String, RegisteredOperation}()

# A global registry of all the known reduction operations.
REDUCTION_REGISTERED_OPERATIONS = Dict{String, RegisteredOperation}()

"""
    function register_query_operation(
        type::Type{T},
        source_file::String,
        source_line::Integer,
    )::Nothing where {T <: Union{EltwiseOperation, ReductionOperation}}

Register a specific operation so it would be available inside queries. This is required to be able to parse the
operation. This is idempotent (safe to invoke multiple times).

This isn't usually called directly. Instead, it is typically invoked by using the [`@query_operation`](@ref) macro.
"""
function register_query_operation(
    type::Type{T},
    source_file::String,
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

Automatically call [`register_query_operation`](@ref) for `MyOpertion`.

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
