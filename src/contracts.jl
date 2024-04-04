"""
Enforce input and output contracts of computations using `Daf` data.
"""
module Contracts

export Contract
export ContractAxes
export ContractData
export ContractExpectation
export dedent
export GuaranteedOutput
export OptionalInput
export OptionalOutput
export RequiredInput
export verify_input
export verify_output

using Daf.Formats
using Daf.GenericFunctions
using Daf.GenericTypes
using Daf.Readers
using DocStringExtensions
using ExprTools

"""
The expectation from a specific property for a computation on `Daf` data.

Input data:

`RequiredInput` - data that must exist in the data when invoking the computation, will be used as input.

`OptionalInput` - data that, if existing in the data when invoking the computation, will be used as an input.

Output data:

`GuaranteedOutput` - data that is guaranteed to exist when the computation is done.

`OptionalOutput` - data that may exist when the computation is done, depending on some condition, which may include the
existence of optional input and/or the value of parameters to the computation, and/or the content of the data.
"""
@enum ContractExpectation RequiredInput OptionalInput GuaranteedOutput OptionalOutput

"""
A vector of pairs where the key is the axis name and the value is a tuple of the [`ContractExpectation`](@ref) and a
description of the axis (for documentation). Axes are listed mainly for documentation; axes of required or guaranteed
vectors or matrices are automatically required or guaranteed to match. However it is considered polite to explicitly
list the axes with their descriptions so the documentation of the contract will be complete.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        ContractAxes = AbstractVector{Pair{AbstractString, Tuple{ContractExpectation, AbstractString}}}

    But what we are **forced** to say is:

        ContractAxes = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
ContractAxes = AbstractVector{<:Pair}

"""
A vector of pairs where the key is a [`DataKey`](@ref) identifying some data property, and the value is a tuple of the
[`ContractExpectation`](@ref), the expected data type, and a description (for documentation).

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        ContractData = AbstractVector{Pair{DataKey, Tuple{ContractExpectation, Type, AbstractString}}}

    But what we are **forced** to say is:

        ContractData = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
ContractData = AbstractVector{<:Pair}

"""
    function Contract(;
        [axes::Maybe{ContractAxes} = nothing,
        data::Maybe{ContractData} = nothing]
    )::Contract

The contract of a computational tool, specifing the [`ContractAxes`](@ref) and [`ContractData`](@ref).
"""
struct Contract
    axes::Maybe{ContractAxes}
    data::Maybe{ContractData}
end

function Contract(; axes::Maybe{ContractAxes} = nothing, data::Maybe{ContractData} = nothing)::Contract
    return Contract(axes, data)
end

"""
    function verify_input(daf::DafReader, contract::Contract, computation::AbstractString)::Nothing

Verify the `daf` data when a computation is invoked. This verifies that all the required data exists and is of the
appropriate type, and that if any of the optional data exists, it has the appropriate type.
"""
function verify_input(contract::Contract, computation::AbstractString, daf::DafReader)::Nothing
    return verify_contract(contract, computation, daf; is_output = false)
end

"""
    function verify_output(daf::DafReader, contract::Contract, computation::AbstractString)::Nothing

Verify the `daf` data when a computation is complete. This verifies that all the guaranteed output data exists and is of
the appropriate type, and that if any of the optional output data exists, it has the appropriate type.
"""
function verify_output(contract::Contract, computation::AbstractString, daf::DafReader)::Nothing
    return verify_contract(contract, computation, daf; is_output = true)
end

function verify_contract(contract::Contract, computation::AbstractString, daf::DafReader; is_output::Bool)::Nothing
    if contract.axes != nothing
        for (axis_name, axis_term) in contract.axes
            @assert axis_name isa AbstractString
            @assert axis_term isa Tuple{ContractExpectation, AbstractString}
            verify_axis_contract(computation, daf, axis_name, axis_term...; is_output = is_output)
        end
    end

    if contract.data != nothing
        for (data_key, data_term) in contract.data
            @assert data_key isa DataKey
            @assert data_term isa Tuple{ContractExpectation, Type, AbstractString}
            if data_key isa AbstractString
                verify_scalar_contract(computation, daf, data_key, data_term...; is_output = is_output)
            elseif data_key isa Tuple{AbstractString, AbstractString}
                verify_vector_contract(computation, daf, data_key..., data_term...; is_output = is_output)
            else
                @assert data_key isa Tuple{AbstractString, AbstractString, AbstractString}
                verify_matrix_contract(computation, daf, data_key..., data_term...; is_output = is_output)
            end
        end
    end

    return nothing
end

function verify_scalar_contract(
    computation::AbstractString,
    daf::DafReader,
    name::AbstractString,
    expectation::ContractExpectation,
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
    expectation::ContractExpectation,
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
    expectation::ContractExpectation,
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
    expectation::ContractExpectation,
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

function is_mandatory(expectation::ContractExpectation; is_output::Bool)::Bool
    return (is_output && expectation == GuaranteedOutput) || (!is_output && expectation == RequiredInput)
end

function is_possible(expectation::ContractExpectation; is_output::Bool)::Bool
    return (is_output && (expectation == GuaranteedOutput || expectation == OptionalOutput)) ||
           (!is_output && (expectation == RequiredInput || expectation == OptionalInput))
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
    if contract.data != nothing
        is_first = true
        for (name, (expectation, data_type, description)) in contract.data
            if name isa AbstractString && (
                (is_output && (expectation == GuaranteedOutput || expectation == OptionalOutput)) ||
                (!is_output && (expectation == RequiredInput || expectation == OptionalInput))
            )
                has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
                if is_first
                    is_first = false
                    println(buffer)
                    println(buffer, "### Scalars")
                end
                println(buffer)
                println(buffer, "**$(name)**::$(data_type) ($(short(expectation))): $(dedent(description))")
            end
        end
    end

    return has_any
end

function axes_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    if contract.axes != nothing
        is_first = true
        for (name, (expectation, description)) in contract.axes
            if (is_output && (expectation == GuaranteedOutput || expectation == OptionalOutput)) ||
               (!is_output && (expectation == RequiredInput || expectation == OptionalInput))
                has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
                if is_first
                    is_first = false
                    println(buffer)
                    println(buffer, "### Axes")
                end
                println(buffer)
                println(buffer, "**$(name)** ($(short(expectation))): $(dedent(description))")
            end
        end
    end

    return has_any
end

function vectors_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    if contract.data != nothing
        is_first = true
        for (key, (expectation, data_type, description)) in contract.data
            if key isa Tuple{AbstractString, AbstractString}
                axis_name, name = key
                if (is_output && (expectation == GuaranteedOutput || expectation == OptionalOutput)) ||
                   (!is_output && (expectation == RequiredInput || expectation == OptionalInput))
                    has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
                    if is_first
                        is_first = false
                        println(buffer)
                        println(buffer, "### Vectors")
                    end
                    println(buffer)
                    println(
                        buffer,
                        "**$(axis_name) @ $(name)**::$(data_type) ($(short(expectation))): $(dedent(description))",
                    )
                end
            end
        end
    end

    return has_any
end

function matrices_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    if contract.data != nothing
        is_first = true
        for (key, (expectation, data_type, description)) in contract.data
            if key isa Tuple{AbstractString, AbstractString, AbstractString}
                rows_axis_name, columns_axis_name, name = key
                if (is_output && (expectation == GuaranteedOutput || expectation == OptionalOutput)) ||
                   (!is_output && (expectation == RequiredInput || expectation == OptionalInput))
                    has_any = direction_header(buffer; is_output = is_output, has_any = has_any)
                    if is_first
                        is_first = false
                        println(buffer)
                        println(buffer, "### Matrices")
                    end
                    println(buffer)
                    println(
                        buffer,
                        "**$(rows_axis_name), $(columns_axis_name) @ $(name)**::$(data_type) ($(short(expectation))): $(dedent(description))",
                    )
                end
            end
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

function short(expectation::ContractExpectation)::String
    if expectation == RequiredInput
        return "required"
    elseif expectation == GuaranteedOutput
        return "guaranteed"
    elseif expectation == OptionalInput || expectation == OptionalOutput
        return "optional"
    else
        @assert false
    end
end

end # module
