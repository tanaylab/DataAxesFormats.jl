"""
Enforce input and output contracts of computations using `Daf` data.
"""
module Contracts

export Contingent
export Contract
export Expectation
export Guaranteed
export Optional
export Required
export verify_input
export verify_output

using Daf.Data
using Daf.Formats
using Daf.Unions
using DocStringExtensions
using ExprTools

"""
The expectation from a specific entity for a computation on `Daf` data.

`Required` - data that must exist in the data when invoking the computation, will be used as input.

`Optional` - data that, if existing in the data when invoking the computation, will be used as an input.

`Guaranteed` - data that is guaranteed to exist when the computation is done.

`Contingent` - data that may exist when the computation is done, contingent on some condition, which may include the
existence of optional input and/or the value of parameters to the computation, and/or the content of the data.
"""
@enum Expectation Required Optional Guaranteed Contingent

"""
    function Contract([;
        scalars::Union{
            Vector{Pair{
                AbstractString,
                Tuple{Expectation, Type, AbstractString}
            }}, Nothing
        } = nothing,
        axes::Union{
            Vector{Pair{
                AbstractString,
                Tuple{Expectation, AbstractString}
            }}, Nothing
        } = nothing,
        vectors::Union{
            Vector{Pair{
                Tuple{AbstractString, AbstractString},
                Tuple{Expectation, Type, AbstractString}
            }},
            Nothing
        } = nothing,
        matrices::Union{
            Vector{Pair{
                Tuple{AbstractString, AbstractString, AbstractString},
                Tuple{Expectation, Type, AbstractString}
            }},
            Nothing
        } = nothing,
    ])::Contract

The contract of a computational tool. This consists of four separate parts:

`scalars` - a vector of pairs where the key is the scalar name and the value is a tuple of the [`Expectation`](@ref),
the data type of the scalar, and a description of the scalar (for documentation).

`axes` - a vector of pairs where the key is the axis name and the value is a tuple of the [`Expectation`](@ref) and a
description of the axis (for documentation). Axes are listed mainly for documentation; axes of required or guaranteed
vectors or matrices are automatically required or guaranteed to match. However it is considered polite to explicitly
list the axes with their descriptions so the documentation of the contract will be complete.

`vectors` - a vector of pairs where the key is a tuple of the axis and vector names, and the value is a tuple of the
[`Expectation`](@ref), the data type of the vector entries, and a description of the vector (for documentation).

`matrices` - a vector of pairs where the key is a tuple of the axes and matrix names, and the value is a tuple of the
[`Expectation`](@ref), the data type of the matrix entries, and a description of the matrix (for documentation).
"""
struct Contract{T1, T2, T3}
    scalars::Vector{Pair{String, Tuple{Expectation, T1, String}}}
    axes::Vector{Pair{String, Tuple{Expectation, String}}}
    vectors::Vector{Pair{Tuple{String, String}, Tuple{Expectation, T2, String}}}
    matrices::Vector{Pair{Tuple{String, String, String}, Tuple{Expectation, T3, String}}}
end

function Contract(;
    scalars::Maybe{Vector{Pair{String, Tuple{Expectation, T1, String}}}} = nothing,
    axes::Maybe{Vector{Pair{String, Tuple{Expectation, String}}}} = nothing,
    vectors::Maybe{Vector{Pair{Tuple{String, String}, Tuple{Expectation, T2, String}}}} = nothing,
    matrices::Maybe{Vector{Pair{Tuple{String, String, String}, Tuple{Expectation, T3, String}}}} = nothing,
)::Contract where {T1 <: Type, T2 <: Type, T3 <: Type}
    if scalars == nothing
        scalars = Vector{Pair{String, Tuple{Expectation, Type, String}}}()
    end
    if axes == nothing
        axes = Vector{Pair{String, Tuple{Expectation, String}}}()
    end
    if vectors == nothing
        vectors = Vector{Pair{Tuple{String, String}, Tuple{Expectation, Type, String}}}()
    end
    if matrices == nothing
        matrices = Vector{Pair{Tuple{String, String, String}, Tuple{Expectation, Type, String}}}()
    end
    return Contract(scalars, axes, vectors, matrices)
end

"""
    function verify_input(daf::DafReader, contract::Contract, computation::AbstractString)::Nothing

Verify the `Daf` data when a computation is invoked. This verifies that all the required data exists and is of the
appropriate type, and that if any of the optional data exists, it has the appropriate type.
"""
function verify_input(contract::Contract, computation::AbstractString, daf::DafReader)::Nothing
    return verify_contract(contract, computation, daf; is_output = false)
end

"""
    function verify_output(daf::DafReader, contract::Contract, computation::AbstractString)::Nothing

Verify the `Daf` data when a computation is complete. This verifies that all the guaranteed data exists and is of the
appropriate type, and that if any of the contingent data exists, it has the appropriate type.
"""
function verify_output(contract::Contract, computation::AbstractString, daf::DafReader)::Nothing
    return verify_contract(contract, computation, daf; is_output = true)
end

function verify_contract(contract::Contract, computation::AbstractString, daf::DafReader; is_output::Bool)::Nothing
    for (scalar_name, scalar_term) in contract.scalars
        verify_scalar_contract(computation, daf, scalar_name, scalar_term...; is_output = is_output)
    end
    for (axis_name, axis_term) in contract.axes
        verify_axis_contract(computation, daf, axis_name, axis_term...; is_output = is_output)
    end
    for (vector_names, vector_term) in contract.vectors
        verify_vector_contract(computation, daf, vector_names..., vector_term...; is_output = is_output)
    end
    for (matrix_names, matrix_term) in contract.matrices
        verify_matrix_contract(computation, daf, matrix_names..., matrix_term...; is_output = is_output)
    end
    return nothing
end

function verify_scalar_contract(
    computation::AbstractString,
    daf::DafReader,
    name::AbstractString,
    expectation::Expectation,
    data_type::T,
    description::AbstractString;
    is_output::Bool,
)::Nothing where {T <: Type}
    value = get_scalar(daf, name; default = nothing)
    if is_mandatory(expectation; is_output = is_output) && value == nothing
        error(
            "missing $(direction_name(is_output)) scalar: $(name)\n" *
            "with type: $(data_type)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
    if is_possible(expectation; is_output = is_output) && value != nothing && !(value isa data_type)
        error(
            "unexpected type: $(typeof(value))\n" *
            "instead of type: $(data_type)\n" *
            "for the $(direction_name(is_output)) scalar: $(name)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
end

function verify_axis_contract(
    computation::AbstractString,
    daf::DafReader,
    name::AbstractString,
    expectation::Expectation,
    description::AbstractString;
    is_output::Bool,
)::Bool
    axis_exists = has_axis(daf, name)
    if is_mandatory(expectation; is_output = is_output) && !axis_exists
        error(
            "missing $(direction_name(is_output)) axis: $(name)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
    return axis_exists
end

function verify_vector_contract(
    computation::AbstractString,
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString,
    expectation::Expectation,
    data_type::T,
    description::AbstractString;
    is_output::Bool,
)::Nothing where {T <: Type}
    value = nothing
    if verify_axis_contract(computation, daf, axis, expectation, ""; is_output = is_output)
        value = get_vector(daf, axis, name; default = nothing)
    end
    if is_mandatory(expectation; is_output = is_output) && value == nothing
        error(
            "missing $(direction_name(is_output)) vector: $(name)\n" *
            "of the axis: $(axis)\n" *
            "with element type: $(data_type)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
    if is_possible(expectation; is_output = is_output) && value != nothing && !(eltype(value) <: data_type)
        error(
            "unexpected type: $(eltype(value))\n" *
            "instead of type: $(data_type)\n" *
            "for the $(direction_name(is_output)) vector: $(name)\n" *
            "of the axis: $(axis)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
end

function verify_matrix_contract(
    computation::AbstractString,
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    expectation::Expectation,
    data_type::T,
    description::AbstractString;
    is_output::Bool,
)::Nothing where {T <: Type}
    has_rows_axis = verify_axis_contract(computation, daf, rows_axis, expectation, ""; is_output = is_output)
    has_columns_axis = verify_axis_contract(computation, daf, columns_axis, expectation, ""; is_output = is_output)
    value = nothing
    if has_rows_axis && has_columns_axis
        value = get_matrix(daf, rows_axis, columns_axis, name; default = nothing)
    end
    if is_mandatory(expectation; is_output = is_output) && value == nothing
        error(
            "missing $(direction_name(is_output)) matrix: $(name)\n" *
            "of the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "with element type: $(data_type)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
    if is_possible(expectation; is_output = is_output) && value != nothing && !(eltype(value) <: data_type)
        error(  # NOJET
            "unexpected type: $(eltype(value))\n" *
            "instead of type: $(data_type)\n" *
            "for the $(direction_name(is_output)) matrix: $(name)\n" *
            "of the rows axis: $(rows_axis)\n" *
            "and the columns axis: $(columns_axis)\n" *
            "for the computation: $(computation)\n" *
            "on the daf data: $(daf.name)",
        )
    end
end

function is_mandatory(expectation::Expectation; is_output::Bool)::Bool
    return (is_output && expectation == Guaranteed) || (!is_output && expectation == Required)
end

function is_possible(expectation::Expectation; is_output::Bool)::Bool
    return (is_output && (expectation == Guaranteed || expectation == Contingent)) ||
           (!is_output && (expectation == Required || expectation == Optional))
end

function direction_name(is_output::Bool)::String
    if is_output
        return "output"
    else
        return "input"
    end
end

function contract_documentation(contract::Contract, buffer::IOBuffer)::Nothing
    has_inputs = false
    has_inputs = scalar_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_inputs = axes_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_inputs = vectors_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_inputs = matrices_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_outputs = false
    has_outputs = scalar_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    has_outputs = axes_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    has_outputs = vectors_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    has_outputs = matrices_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    return nothing
end

function scalar_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    is_first = true
    for (name, (expectation, data_type, description)) in contract.scalars
        if (is_output && (expectation == Guaranteed || expectation == Contingent)) ||
           (!is_output && (expectation == Required || expectation == Optional))
            has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
            if is_first
                is_first = false
                println(buffer)
                println(buffer, "### Scalars")
            end
            println(buffer)
            println(buffer, "**$(name)**::$(data_type) ($(expectation)): $(description)")
        end
    end
    return has_any
end

function axes_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    is_first = true
    for (name, (expectation, description)) in contract.axes
        if (is_output && (expectation == Guaranteed || expectation == Contingent)) ||
           (!is_output && (expectation == Required || expectation == Optional))
            has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
            if is_first
                is_first = false
                println(buffer)
                println(buffer, "### Axes")
            end
            println(buffer)
            println(buffer, "**$(name)** ($(expectation)): $(description)")
        end
    end
    return has_any
end

function vectors_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    is_first = true
    for ((axis_name, name), (expectation, data_type, description)) in contract.vectors
        if (is_output && (expectation == Guaranteed || expectation == Contingent)) ||
           (!is_output && (expectation == Required || expectation == Optional))
            has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
            if is_first
                is_first = false
                println(buffer)
                println(buffer, "### Vectors")
            end
            println(buffer)
            println(buffer, "**$(axis_name) @ $(name)**::$(data_type) ($(expectation)): $(description)")
        end
    end
    return has_any
end

function matrices_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    is_first = true
    for ((rows_axis_name, columns_axis_name, name), (expectation, data_type, description)) in contract.matrices
        if (is_output && (expectation == Guaranteed || expectation == Contingent)) ||
           (!is_output && (expectation == Required || expectation == Optional))
            has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
            if is_first
                is_first = false
                println(buffer)
                println(buffer, "### Matrices")
            end
            println(buffer)
            println(
                buffer,
                "**$(rows_axis_name), $(columns_axis_name) @ $(name)**::$(data_type) ($(expectation)): $(description)",
            )
        end
    end
    return has_any
end

function direction_header(buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    if !has_any
        if is_output
            println(buffer)
            println(buffer, "## Outputs")
        else
            println(buffer, "## Inputs")
        end
    end
    return true
end

end # module
