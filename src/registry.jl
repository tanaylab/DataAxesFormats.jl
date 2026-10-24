"""
Registering element-wise and reduction operations is required, to allow them to be used in a query.

!!! note

    We do not re-export everything from here to the main `Daf` namespace, as it is only of interest for implementers of
    new query operations. Most users of `Daf` just stick with the (fairly comprehensive) list of built-in query
    operations so there's no need to pollute their namespace with these detail.
"""
module Registry

export @query_operation
export EltwiseOperation
export QueryOperation
export ReductionOperation
export buffer_reduction_eltype
export compute_eltwise
export compute_reduction
export empty_reduction_state
export final_reduction_value
export merge_reduction_states
export reduce_buffer!
export reduction_result_type
export register_query_operation
export supports_buffer_reduction
export supports_grouped_reduction
export supports_strings
export update_reduction_state

using ..StorageTypes
using TanayLabUtilities

# An operation in the global registry (used for parsing).
struct RegisteredOperation  # NOLINT
    type::Type
    source_file::AbstractString
    source_line::Int
end

"""
Abstract interface for all query operations. An actual query is a series of these operations which, when applied to
`Daf` data, compute some result.
"""
abstract type QueryOperation end  # NOLINT

# Abstract interface for all query computation (element-wise and reduction) operations.
abstract type ComputationOperation <: QueryOperation end  # NOLINT

"""
    supports_strings(operation::ComputationOperation)::Bool

Most operations do not support string operands. The very few that do are explicitly marked in the documentation.
"""
function supports_strings(::ComputationOperation)::Bool
    return false
end

"""
Abstract type for all element-wise operations.

In a string query, this is specified using the `%` operator (e.g., `% Abs`, `% Log base 2`):

`EltwiseOperation` := `%` operation ( parameter value )*

Since each `EltwiseOperation` isa [`QueryOperation`](@ref), you can directly apply it to a query (e.g.,
`Axis("cell") |> LookupVector("age") |> Abs()`). For this there should be other constructor(s) tailored for this usage.

An element-wise operation may be applied to scalar, vector ot matrix data. It will preserve the shape of the data, but
changes the value(s), and possibly the data type of the elements. For example, `Abs` will compute the absolute value of
each value.

To implement a new such operation, the type is expected to be of the form:

    struct MyOperation <: EltwiseOperation
        ... optional parameters ...
    end
    @query_operation MyOperation

    MyOperation(operation_name::Token, parameter_values::Dict{String, Token})::MyOperation

The constructor should use `parse_parameter` for each of the parameters (for example, using `parse_number_assignment`).
In addition you will need to invoke [`@query_operation`](@ref) to register the operation so it can be used in a query,
and implement the functions listed below. See the query operations module for details and examples.
"""
abstract type EltwiseOperation <: ComputationOperation end

"""
    compute_eltwise(operation::EltwiseOperation, input::StorageMatrix)::StorageMatrix
    compute_eltwise(operation::EltwiseOperation, input::StorageVector)::StorageVector
    compute_eltwise(operation::EltwiseOperation, input_value::Number)::StorageReal

Compute an [`EltwiseOperation`](@ref) `operation`.
"""
function compute_eltwise end

"""
Abstract type for all reduction operations.

In a string query, this is specified using the `%>` operator (e.g., `%> Sum`, `%> Quantile fraction 0.05`):

`ReductionOperation` := `%>` operation ( parameter value )*

Since each `ReductionOperation` isa [`QueryOperation`](@ref), you can directly apply it to a query (e.g.,
`Axis("cell") |> Axis("gene") |> LookupMatrix("UMIs") |> Quantile(0.05)`). To allow for this, the reduction class should
provide constructor(s) tailored for this usage.

A reduction operation may be applied to matrix or vector data. It will reduce (eliminate) one dimension of the data, and
possibly the result will have a different data type than the input. When applied to a vector, the operation will return
a scalar. When applied to a matrix, you must specify the axis that is to be eliminated from the result (as per Julia's
conventions).

To implement a new such operation, the type is expected to be of the form:

    struct MyOperation <: ReductionOperation
        ... optional parameters ...
    end

    MyOperation(operation_name::Token, parameter_values::Dict{String, Token})::MyOperation

The constructor should use `parse_parameter` for each of the parameters (for example, using typically
`parse_number_assignment`). In addition you will need to invoke [`@query_operation`](@ref) to register the operation so
it can be used in a query, and implement the functions listed below. See the query operations module for details and
examples.
"""
abstract type ReductionOperation <: ComputationOperation end

"""
    compute_reduction(operation::ReductionOperation, input::StorageMatrix, axis::Integer)::StorageVector
    compute_reduction(operation::ReductionOperation, input::Union{StorageVector, StorageMatrix})::StorageScalar

Since each `ReductionOperation` isa [`QueryOperation`](@ref), you can directly apply it to a query (e.g.,
`Axis("cell") |> Axis("gene") |> LookupMatrix("UMIs") |> Sum()`). For this there should be other constructor(s) tailored
for this usage.

When reducing a matrix, if an axis is specified, that axis is the one being eliminated. For example, `axis = 1` (or
`Rows`) means the result will have one entry per column. If no axis is specified, the matrix (or vector) is reduced to a
scalar.

An operation which [`supports_grouped_reduction`](@ref) or [`supports_buffer_reduction`](@ref) doesn't implement these.
They are computed from its protocol, reading a matrix one column at a time, so a lazy matrix is never materialized. A
row-major matrix is reduced as its column-major transpose, along the other axis. An operation which supports neither
protocol implements both.
"""
function compute_reduction end

"""
    reduction_result_type(operation::ReductionOperation, eltype::Type)::Type

Return the data type of the result of the reduction `operation` if applied to a vector of the specified `eltype`.
"""
function reduction_result_type end

"""
    supports_grouped_reduction(operation::ReductionOperation)::Bool

Whether the reduction `operation` can be computed one value at a time, which allows reducing the rows of a matrix per
group in a single pass over each column (see `compute_grouped_reduction` in the query operations module). Such an
operation implements [`empty_reduction_state`](@ref), [`update_reduction_state`](@ref), [`merge_reduction_states`](@ref)
and [`final_reduction_value`](@ref). By default this is `false`. An operation which needs all the values at once may
instead [`supports_buffer_reduction`](@ref). Otherwise, the reduction is applied to each group's values.
"""
function supports_grouped_reduction(::ReductionOperation)::Bool
    return false
end

"""
    empty_reduction_state(operation::ReductionOperation, eltype::Type)::Any

The state of the reduction `operation` before seeing any value of the specified `eltype`. Only needed if
[`supports_grouped_reduction`](@ref).
"""
function empty_reduction_state end

"""
    update_reduction_state(operation::ReductionOperation, state::Any, value::StorageReal)::Any

The state of the reduction `operation` after also seeing the `value`. Only needed if
[`supports_grouped_reduction`](@ref).
"""
function update_reduction_state end

"""
    merge_reduction_states(operation::ReductionOperation, state::Any, other_state::Any)::Any

The state of the reduction `operation` after seeing the values of both the `state` and the `other_state`. This allows
reducing disjoint parts of the input in parallel. Only needed if [`supports_grouped_reduction`](@ref).
"""
function merge_reduction_states end

"""
    final_reduction_value(operation::ReductionOperation, state::Any, n_seen::Integer, n_values::Integer)::StorageReal

The result of the reduction `operation`, given the `state` after seeing `n_seen` values. The input has `n_values` values
in total. The values that weren't seen are zeros (the implicit zeros of a sparse matrix). The result is converted to the
[`reduction_result_type`](@ref). Only needed if [`supports_grouped_reduction`](@ref).
"""
function final_reduction_value end

"""
    supports_buffer_reduction(operation::ReductionOperation)::Bool

Whether the reduction `operation` can be computed from a buffer holding all the stored values, plus a count of the
implicit zeros of a sparse matrix that aren't in the buffer. This allows reducing the rows of a matrix per group in a
single pass over each column (see `compute_grouped_reduction` in the query operations module), for an operation which
needs all the values at once (e.g., a median). Such an operation implements [`buffer_reduction_eltype`](@ref) and
[`reduce_buffer!`](@ref). By default this is `false`.
"""
function supports_buffer_reduction(::ReductionOperation)::Bool
    return false
end

"""
    buffer_reduction_eltype(operation::ReductionOperation, eltype::Type)::Type

The element type of the buffer given to [`reduce_buffer!`](@ref), when reducing values of the specified `eltype`. Only
needed if [`supports_buffer_reduction`](@ref).
"""
function buffer_reduction_eltype end

"""
    reduce_buffer!(operation::ReductionOperation, buffer::AbstractVector, n_implicit_zero::Integer)::StorageScalar

The result of the reduction `operation` of the values in the `buffer`, together with `n_implicit_zero` zero values that
aren't in it. This may reorder the `buffer`. The result is converted to the [`reduction_result_type`](@ref). Only needed
if [`supports_buffer_reduction`](@ref).
"""
function reduce_buffer! end

# A global registry of all the known element-wise operations.
const ELTWISE_REGISTERED_OPERATIONS = Dict{String, RegisteredOperation}()  # NOLINT

# A global registry of all the known reduction operations.
const REDUCTION_REGISTERED_OPERATIONS = Dict{String, RegisteredOperation}()  # NOLINT

"""
    register_query_operation(
        type::Type{<:Union{EltwiseOperation, ReductionOperation}},
        source_file::AbstractString,
        source_line::Integer,
    )::Nothing

Register a specific operation so it would be available inside queries. This is required to be able to parse the
operation. This is idempotent (safe to invoke multiple times).

This isn't usually called directly. Instead, it is typically invoked by using the [`@query_operation`](@ref) macro.
"""
function register_query_operation(
    type::Type{<:Union{EltwiseOperation, ReductionOperation}},
    source_file::AbstractString,
    source_line::Integer,
)::Nothing
    if type <: EltwiseOperation
        registered_operations = ELTWISE_REGISTERED_OPERATIONS
        kind = "eltwise"
    elseif type <: ReductionOperation
        registered_operations = REDUCTION_REGISTERED_OPERATIONS
        kind = "reduction"
    else
        @assert false
    end

    name = String(type.name.name)
    if name in keys(registered_operations)
        previous_registration = registered_operations[name]
        if previous_registration.type != type ||
           previous_registration.source_file != source_file ||
           previous_registration.source_line != source_line
            error(chomp("""
                conflicting registrations for the $(kind) operation: $(name)
                first in: $(previous_registration.source_file):$(previous_registration.source_line)
                second in: $(source_file):$(source_line)
                """))
        end
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

Note this will import `DataAxesFormats.Registry.register_query_operation`, so it may only be called from the top level
scope of a module.
"""
macro query_operation(operation_type_name)
    name_reference = esc(operation_type_name)

    source_line = __source__.line
    file = __source__.file
    source_file = file === nothing ? "-" : String(file)

    return quote
        import DataAxesFormats.Registry.register_query_operation
        register_query_operation($name_reference, $source_file, $source_line)
    end
end

end  # module
