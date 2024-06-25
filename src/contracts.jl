"""
Enforce input and output contracts of computations using `Daf` data.
"""
module Contracts

export AxisSpecification
export Contract
export ContractAxes
export ContractData
export ContractExpectation
export DataSpecification
export GuaranteedOutput
export OptionalInput
export OptionalOutput
export RequiredInput
export contractor
export verify_input
export verify_output

using ..Formats
using ..GenericFunctions
using ..GenericTypes
using ..Keys
using ..Messages
using ..Queries
using ..Readers
using ..StorageTypes
using ..Views
using ..Writers
using DocStringExtensions
using ExprTools
using NamedArrays

import ..Formats.CacheKey
import ..Formats.CachedAxis
import ..Formats.CachedData
import ..Formats.CachedNames
import ..Formats.CachedQuery

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
The specification of an axis in a [`Contract`](@ref), which is the [`ContractExpectation`](@ref) for enforcement and a
string description for the generated documentation.
"""
AxisSpecification = Tuple{ContractExpectation, AbstractString}

"""
A vector of pairs where the key is the axis name and the value is a tuple of the [`ContractExpectation`](@ref) and a
description of the axis (for documentation). Axes are listed mainly for documentation; axes of required or guaranteed
vectors or matrices are automatically required or guaranteed to match. However it is considered polite to explicitly
list the axes with their descriptions so the documentation of the contract will be complete.

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        ContractAxes = AbstractVector{Pair{AxisKey, AxisSpecification}}

    But what we are **forced** to say is:

        ContractAxes = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
ContractAxes = AbstractVector{<:Pair}

"""
The specification of some property in a [`Contract`](@ref), which is the [`ContractExpectation`](@ref) and the type for
enforcement, and a string description for the generated documentation.
"""
DataSpecification = Tuple{ContractExpectation, Type, AbstractString}

"""
A vector of pairs where the key is a [`DataKey`](@ref) identifying some data property, and the value is a tuple of the
[`ContractExpectation`](@ref), the expected data type, and a description (for documentation).

!!! note

    Due to Julia's type system limitations, there's just no way for the system to enforce the type of the pairs
    in this vector. That is, what we'd **like** to say is:

        ContractData = AbstractVector{Pair{DataKey, DataSpecification}

    But what we are **forced** to say is:

        ContractData = AbstractVector{<:Pair}

    Glory to anyone who figures out an incantation that would force the system to perform more meaningful type inference
    here.
"""
ContractData = AbstractVector{<:Pair}

"""
    @kwdef struct Contract
        is_relaxed::Bool = false
        axes::Maybe{ContractAxes} = nothing
        data::Maybe{ContractData} = nothing
    end

The contract of a computational tool, specifing the `axes` and and `data`. If `is_relaxed`, this allows for additional
unlisted inputs; this is typically used when the computation has query parameters, which may need to access such
additional data.

!!! note

    When a function calls several functions in a row, you can compute its contract by using `function_contract` on them
    and then adding the results in their invocation order using `+`.
"""
@kwdef struct Contract
    is_relaxed::Bool = false
    axes::Maybe{ContractAxes} = nothing
    data::Maybe{ContractData} = nothing
end

function contract_documentation(contract::Contract, buffer::IOBuffer)::Nothing
    has_inputs = false
    has_inputs = scalar_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_inputs = axes_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_inputs = vectors_documentation(contract, buffer; is_output = false, has_any = has_inputs)
    has_inputs = matrices_documentation(contract, buffer; is_output = false, has_any = has_inputs)

    if contract.is_relaxed
        direction_header(buffer; is_output = false, has_any = has_inputs)
        println(buffer)
        println(buffer, "Additional inputs may be used depending to the query parameter(s).")
    end

    has_outputs = false
    has_outputs = scalar_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    has_outputs = axes_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    has_outputs = vectors_documentation(contract, buffer; is_output = true, has_any = has_outputs)
    has_outputs = matrices_documentation(contract, buffer; is_output = true, has_any = has_outputs)

    return nothing
end

function scalar_documentation(contract::Contract, buffer::IOBuffer; is_output::Bool, has_any::Bool)::Bool
    if contract.data !== nothing
        is_first = true
        for (name, (expectation, data_type, description)) in contract.data
            if name isa ScalarKey && (
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
    if contract.axes !== nothing
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
    if contract.data !== nothing
        is_first = true
        for (key, (expectation, data_type, description)) in contract.data
            if key isa VectorKey
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
    if contract.data !== nothing
        is_first = true
        for (key, (expectation, data_type, description)) in contract.data
            if key isa MatrixKey
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

mutable struct Tracker
    expectation::ContractExpectation
    type::Maybe{Type{<:StorageScalarBase}}
    accessed::Bool
end

"""
    struct ContractDaf <: DafWriter ... end

A [`DafWriter`](@ref) wrapper which restricts access only to the properties listed in some [`Contract`](@ref). This also
tracks which properties are accessed, so when a computation is done, we can verify that all required inputs were
actually accessed. If they weren't, then they weren't really required (should have been marked as optional instead).

This isn't exported and isn't created manually; instead call [`contractor`](@ref), or, better yet, use the `@computation` macro.

!!! note

    If the [`Contract`](@ref) specifies no outputs, then this becomes effectively a read-only `Daf` data set; however,
    to avoid code duplication, it is still a [`DafWriter`](@ref) rather than a [`DafReader`](@ref).
"""
struct ContractDaf <: DafWriter
    name::AbstractString
    internal::Formats.Internal
    computation::AbstractString
    is_relaxed::Bool
    axes::Dict{AbstractString, Tracker}
    data::Dict{DataKey, Tracker}
    daf::DafReader
    overwrite::Bool
end

"""
    function contractor(
        computation::AbstractString,
        contract::Contract,
        daf::DafReader;
        overwrite::Bool,
    )::ContractDaf

Wrap a `daf` data set to enforce a `contract` for some `computation`, possibly allowing for `overwrite` of existing
outputs.

!!! note

    If the `contract` specifies any outputs, the `daf` needs to be a `DafWriter`.
"""
function contractor(
    computation::AbstractString,
    contract::Contract,
    daf::DafReader;
    overwrite::Bool = false,
)::ContractDaf
    axes = collect_axes(contract)
    data = collect_data(contract, axes)
    name = unique_name("$(daf.name).for.$(split(computation, '.')[end])")
    return ContractDaf(name, daf.internal, computation, contract.is_relaxed, axes, data, daf, overwrite)
end

function collect_axes(contract::Contract)::Dict{AbstractString, Tracker}
    axes = Dict{AbstractString, Tracker}()
    if contract.axes !== nothing
        for (axis_name, axis_specification) in contract.axes
            @assert axis_name isa AxisKey
            @assert axis_specification isa AxisSpecification
            collect_axis(axis_name, axis_specification[1], axes)
        end
    end
    return axes
end

function collect_data(contract::Contract, axes::Dict{AbstractString, Tracker})::Dict{DataKey, Tracker}
    data = Dict{DataKey, Tracker}()
    if contract.data !== nothing
        for (data_key, data_specification) in contract.data
            @assert data_key isa DataKey
            @assert data_specification isa DataSpecification
            expectation = data_specification[1]
            type = data_specification[2]
            data[data_key] = Tracker(expectation, type, false)
            if data_key isa VectorKey
                collect_axis(data_key[1], implicit_axis_expectation(expectation), axes)
            elseif data_key isa MatrixKey
                collect_axis(data_key[1], implicit_axis_expectation(expectation), axes)
                collect_axis(data_key[2], implicit_axis_expectation(expectation), axes)
            end
        end
    end
    return data
end

function implicit_axis_expectation(expectation::ContractExpectation)::ContractExpectation
    if expectation == GuaranteedOutput || expectation == OptionalOutput
        return OptionalInput
    else
        return expectation
    end
end

function collect_axis(
    name::AbstractString,
    expectation::ContractExpectation,
    axes::Dict{AbstractString, Tracker},
)::Nothing
    tracker = get(axes, name, nothing)
    if tracker === nothing
        axes[name] = Tracker(expectation, nothing, false)
    elseif expectation == RequiredInput || tracker.expectation == RequiredInput
        tracker.expectation = RequiredInput
    elseif expectation == GuaranteedOutput || tracker.expectation == GuaranteedOutput
        tracker.expectation = GuaranteedOutput  # untested
    elseif expectation == OptionalOutput || tracker.expectation == OptionalOutput
        tracker.expectation = OptionalOutput
    elseif expectation == OptionalInput || tracker.expectation == OptionalInput
        tracker.expectation = OptionalInput
    else
        @assert false
    end
    return nothing
end

"""
    verify_input(contract_daf::ContractDaf)::Nothing

Verify the `contract_daf` data before a computation is invoked. This verifies that all the required data exists and is
of the appropriate type, and that if any of the optional data exists, it has the appropriate type.
"""
function verify_input(contract_daf::ContractDaf)::Nothing
    return verify_contract(contract_daf; is_output = false)
end

"""
    verify_output(contract_daf::ContractDaf)::Nothing

Verify the `contract_daf` data when a computation is complete. This verifies that all the guaranteed output data exists
and is of the appropriate type, and that if any of the optional output data exists, it has the appropriate type. It also
verifies that all the required inputs were accessed by the computation.
"""
function verify_output(contract_daf::ContractDaf)::Nothing
    return verify_contract(contract_daf; is_output = true)
end

function verify_contract(contract_daf::ContractDaf; is_output::Bool)::Nothing
    for (axis, tracker) in contract_daf.axes
        verify_axis(contract_daf, axis, tracker; is_output = is_output)
    end

    for (data_key, tracker) in contract_daf.data
        if data_key isa ScalarKey
            verify_scalar(contract_daf, data_key, tracker; is_output = is_output)  # NOJET
        elseif data_key isa VectorKey
            verify_vector(contract_daf, data_key..., tracker; is_output = is_output)
        elseif data_key isa MatrixKey
            verify_matrix(contract_daf, data_key..., tracker; is_output = is_output)
        else
            @assert false
        end
    end
end

function verify_axis(contract_daf::ContractDaf, axis::AbstractString, tracker::Tracker; is_output::Bool)::Nothing
    if has_axis(contract_daf.daf, axis)
        if is_forbidden(tracker.expectation; is_output = is_output, overwrite = contract_daf.overwrite)
            error(dedent("""
                pre-existing $(tracker.expectation) axis: $(axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        if is_output && !tracker.accessed && tracker.expectation == RequiredInput
            error(dedent("""
                unused RequiredInput axis: $(axis)
                of the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    else
        if is_mandatory(tracker.expectation; is_output = is_output)
            error(dedent("""
                missing $(direction_name(is_output)) axis: $(axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    end
end

function verify_scalar(contract_daf::ContractDaf, name::AbstractString, tracker::Tracker; is_output::Bool)::Nothing
    value = get_scalar(contract_daf.daf, name; default = nothing)
    if value === nothing
        if is_mandatory(tracker.expectation; is_output = is_output) && value === nothing
            error(dedent("""
                missing $(direction_name(is_output)) scalar: $(name)
                with type: $(tracker.type)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    else
        if is_forbidden(tracker.expectation; is_output = is_output, overwrite = contract_daf.overwrite)
            error(dedent("""
                pre-existing $(tracker.expectation) scalar: $(name)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        type = tracker.type
        @assert type !== nothing
        if !(value isa type)
            error(dedent("""
                unexpected type: $(typeof(value))
                instead of type: $(type)
                for the $(direction_name(is_output)) scalar: $(name)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        if is_output && !tracker.accessed && tracker.expectation == RequiredInput
            error(dedent("""
                unused RequiredInput scalar: $(name)
                of the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    end
end

function verify_vector(
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString,
    tracker::Tracker;
    is_output::Bool,
)::Nothing
    if has_axis(contract_daf.daf, axis)
        value = get_vector(contract_daf.daf, axis, name; default = nothing)
    else
        value = nothing  # untested
    end
    if value === nothing
        if is_mandatory(tracker.expectation; is_output = is_output)
            error(dedent("""
                missing $(direction_name(is_output)) vector: $(name)
                of the axis: $(axis)
                with element type: $(tracker.type)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    else
        if is_forbidden(tracker.expectation; is_output = is_output, overwrite = contract_daf.overwrite)
            error(dedent("""
                pre-existing $(tracker.expectation) vector: $(name)
                of the axis: $(axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        type = tracker.type
        @assert type !== nothing
        if !(eltype(value) <: type)
            error(dedent("""
                unexpected type: $(eltype(value))
                instead of type: $(type)
                for the $(direction_name(is_output)) vector: $(name)
                of the axis: $(axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        if is_output && !tracker.accessed && tracker.expectation == RequiredInput
            error(dedent("""
                unused RequiredInput vector: $(name)
                of the axis: $(axis)
                of the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    end
end

function verify_matrix(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    tracker::Tracker;
    is_output::Bool,
)::Nothing
    if has_axis(contract_daf.daf, rows_axis) && has_axis(contract_daf.daf, columns_axis)
        value = get_matrix(contract_daf.daf, rows_axis, columns_axis, name; default = nothing)
    else
        value = nothing
    end
    if value === nothing
        if is_mandatory(tracker.expectation; is_output = is_output) && value === nothing
            error(dedent("""
                missing $(direction_name(is_output)) matrix: $(name)
                of the rows axis: $(rows_axis)
                and the columns axis: $(columns_axis)
                with element type: $(tracker.type)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    else
        if is_forbidden(tracker.expectation; is_output = is_output, overwrite = contract_daf.overwrite)
            error(dedent("""
                pre-existing $(tracker.expectation) matrix: $(name)
                of the rows axis: $(rows_axis)
                and the columns axis: $(columns_axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        type = tracker.type
        @assert type !== nothing
        if !(eltype(value) <: type)
            error(dedent("""
                unexpected type: $(eltype(value))
                instead of type: $(type)
                for the $(direction_name(is_output)) matrix: $(name)
                of the rows axis: $(rows_axis)
                and the columns axis: $(columns_axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
        if is_output && !tracker.accessed && tracker.expectation == RequiredInput
            error(dedent("""
                unused RequiredInput matrix: $(name)
                of the rows axis: $(rows_axis)
                and the columns axis: $(columns_axis)
                of the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    end
end

function Messages.depict(contract_daf::ContractDaf; name::Maybe{AbstractString} = nothing)::AbstractString
    if name === nothing
        name = contract_daf.name
    end
    return "Contract $(depict(contract_daf.daf; name = name))"
end

function Readers.axes_set(contract_daf::ContractDaf)::AbstractSet{<:AbstractString}
    return axes_set(contract_daf.daf)
end

function Readers.axis_array(
    contract_daf::ContractDaf,
    axis::AbstractString;
    default::Union{Nothing, UndefInitializer} = undef,
)::Maybe{AbstractVector{<:AbstractString}}
    access_axis(contract_daf, axis; is_modify = false)
    return axis_array(contract_daf.daf, axis; default = default)
end

function Readers.axis_dict(contract_daf::ContractDaf, axis::AbstractString)::AbstractDict{<:AbstractString, <:Integer}
    access_axis(contract_daf, axis; is_modify = false)
    return axis_dict(contract_daf.daf, axis)
end

function Readers.axis_indices(
    contract_daf::ContractDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::AbstractVector{<:Integer}
    access_axis(contract_daf, axis; is_modify = false)
    return axis_indices(contract_daf.daf, axis, entries)
end

function Readers.axis_length(contract_daf::ContractDaf, axis::AbstractString)::Int64
    access_axis(contract_daf, axis; is_modify = false)
    return axis_length(contract_daf.daf, axis)
end

function Readers.axis_version_counter(contract_daf::ContractDaf, axis::AbstractString)::UInt32
    access_axis(contract_daf, axis; is_modify = false)
    return axis_version_counter(contract_daf.daf, axis)
end

function Readers.description(contract_daf::ContractDaf; cache::Bool = false, deep::Bool = false)::String
    return description(contract_daf.daf; cache = cache, deep = deep)
end

function Readers.empty_cache!(
    contract_daf::ContractDaf;
    clear::Maybe{CacheGroup} = nothing,
    keep::Maybe{CacheGroup} = nothing,
)::Nothing
    empty_cache!(contract_daf.daf; clear = clear, keep = keep)
    return nothing
end

function Readers.get_matrix(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    default::Union{StorageReal, StorageMatrix, Nothing, UndefInitializer} = undef,
    relayout::Bool = true,
)::Maybe{NamedArray}
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = false)
    return get_matrix(contract_daf.daf, rows_axis, columns_axis, name; default = default, relayout = relayout)
end

function Readers.Readers.get_scalar(
    contract_daf::ContractDaf,
    name::AbstractString;
    default::Union{StorageScalar, Nothing, UndefInitializer} = undef,
)::Maybe{StorageScalar}
    access_scalar(contract_daf, name; is_modify = false)
    return get_scalar(contract_daf.daf, name; default = default)
end

function Readers.get_vector(
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString;
    default::Union{StorageScalar, StorageVector, Nothing, UndefInitializer} = undef,
)::Maybe{NamedArray}
    access_vector(contract_daf, axis, name; is_modify = false)
    return get_vector(contract_daf.daf, axis, name; default = default)
end

function Readers.has_axis(contract_daf::ContractDaf, axis::AbstractString)::Bool
    access_axis(contract_daf, axis; is_modify = false)
    return has_axis(contract_daf.daf, axis)
end

function Readers.has_matrix(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool = true,
)::Bool
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = false)
    return has_matrix(contract_daf.daf, rows_axis, columns_axis, name; relayout = relayout)
end

function Readers.has_scalar(contract_daf::ContractDaf, name::AbstractString)::Bool
    access_scalar(contract_daf, name; is_modify = false)
    return has_scalar(contract_daf.daf, name)
end

function Readers.has_vector(contract_daf::ContractDaf, axis::AbstractString, name::AbstractString)::Bool
    access_vector(contract_daf, axis, name; is_modify = false)
    return has_vector(contract_daf.daf, axis, name)
end

function Readers.matrices_set(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString;
    relayout::Bool = true,
)::AbstractSet{<:AbstractString}
    access_axis(contract_daf, rows_axis; is_modify = false)
    access_axis(contract_daf, columns_axis; is_modify = false)
    return matrices_set(contract_daf.daf, rows_axis, columns_axis; relayout = relayout)
end

function Readers.matrix_version_counter(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
)::UInt32
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = false)
    return matrix_version_counter(contract_daf.daf, rows_axis, columns_axis, name)
end

function Readers.scalars_set(contract_daf::ContractDaf)::AbstractSet{<:AbstractString}
    return scalars_set(contract_daf.daf)
end

function Readers.vector_version_counter(contract_daf::ContractDaf, axis::AbstractString, name::AbstractString)::UInt32
    access_vector(contract_daf, axis, name; is_modify = false)
    return vector_version_counter(contract_daf.daf, axis, name)
end

function Readers.vectors_set(contract_daf::ContractDaf, axis::AbstractString)::AbstractSet{<:AbstractString}
    access_axis(contract_daf, axis; is_modify = false)
    return vectors_set(contract_daf.daf, axis)
end

function Writers.add_axis!(
    contract_daf::ContractDaf,
    axis::AbstractString,
    entries::AbstractVector{<:AbstractString},
)::Nothing
    access_axis(contract_daf, axis; is_modify = true)
    add_axis!(contract_daf.daf, axis, entries)
    return nothing
end

function Writers.delete_axis!(contract_daf::ContractDaf, axis::AbstractString; must_exist::Bool = true)::Nothing
    access_axis(contract_daf, axis; is_modify = true)
    delete_axis!(contract_daf.daf, axis; must_exist = must_exist)
    return nothing
end

function Writers.delete_matrix!(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
    relayout::Bool = true,
)::Nothing
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    delete_matrix!(contract_daf.daf, rows_axis, columns_axis, name; must_exist = must_exist, relayout = relayout)
    return nothing
end

function Writers.delete_scalar!(contract_daf::ContractDaf, name::AbstractString; must_exist::Bool = true)::Nothing
    access_scalar(contract_daf, name; is_modify = true)
    delete_scalar!(contract_daf.daf, name; must_exist = must_exist)
    return nothing
end

function Writers.delete_vector!(
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString;
    must_exist::Bool = true,
)::Nothing
    access_vector(contract_daf, axis, name; is_modify = true)
    delete_vector!(contract_daf.daf, axis, name; must_exist = must_exist)
    return nothing
end

function Writers.empty_dense_matrix!(
    fill::Function,
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal};
    overwrite::Bool = false,
)::Any
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    return empty_dense_matrix!(fill, contract_daf.daf, rows_axis, columns_axis, name, eltype; overwrite = overwrite)
end

function Writers.empty_dense_vector!(
    fill::Function,
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal};
    overwrite::Bool = false,
)::Any
    access_vector(contract_daf, axis, name; is_modify = true)
    return empty_dense_vector!(fill, contract_daf.daf, axis, name, eltype; overwrite = overwrite)
end

function Writers.empty_sparse_matrix!(
    fill::Function,
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal},
    nnz::StorageInteger,
    indtype::Maybe{Type{<:StorageInteger}} = nothing;
    overwrite::Bool = false,
)::Any
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    return empty_sparse_matrix!(
        fill,
        contract_daf.daf,
        rows_axis,
        columns_axis,
        name,
        eltype,
        nnz,
        indtype;
        overwrite = overwrite,
    )
end

function Writers.empty_sparse_vector!(
    fill::Function,
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal},
    nnz::StorageInteger,
    indtype::Maybe{Type{<:StorageInteger}} = nothing;
    overwrite::Bool = false,
)::Any
    access_vector(contract_daf, axis, name; is_modify = true)
    return empty_sparse_vector!(fill, contract_daf.daf, axis, name, eltype, nnz, indtype; overwrite = overwrite)
end

function Writers.get_empty_dense_matrix!(  # untested
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{<:StorageReal};
    overwrite::Bool = false,
)::Any
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    get_empty_dense_matrix!(contract_daf.daf, rows_axis, columns_axis, name, eltype; overwrite = overwrite)
    return nothing
end

function Writers.get_empty_dense_vector!(  # untested
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T};
    overwrite::Bool = false,
)::AbstractVector{T} where {T <: StorageReal}
    access_vector(contract_daf, axis, name; is_modify = true)
    return get_empty_dense_vector!(contract_daf.daf, axis, name, eltype; overwrite = overwrite)
end

function Writers.get_empty_sparse_matrix!(  # untested
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I};
    overwrite::Bool = false,
)::Tuple{AbstractVector{I}, AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    return get_empty_sparse_matrix!(
        contract_daf.daf,
        rows_axis,
        columns_axis,
        name,
        eltype,
        nnz,
        indtype;
        overwrite = overwrite,
    )
end

function Writers.get_empty_sparse_vector!(  # untested
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString,
    eltype::Type{T},
    nnz::StorageInteger,
    indtype::Type{I};
    overwrite::Bool = false,
)::Tuple{AbstractVector{I}, AbstractVector{T}} where {T <: StorageReal, I <: StorageInteger}
    access_vector(contract_daf, axis, name; is_modify = true)
    return get_empty_sparse_vector!(contract_daf.daf, axis, name, eltype, nnz, indtype; overwrite = overwrite)
end

function Writers.relayout_matrix!(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    overwrite::Bool = false,
)::Nothing
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    relayout_matrix!(contract_daf.daf, rows_axis, columns_axis, name; overwrite = overwrite)
    return nothing
end

function Writers.set_matrix!(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString,
    matrix::Union{StorageReal, StorageMatrix};
    overwrite::Bool = false,
    relayout::Bool = true,
)::Nothing
    access_matrix(contract_daf, rows_axis, columns_axis, name; is_modify = true)
    set_matrix!(contract_daf.daf, rows_axis, columns_axis, name, matrix; overwrite = overwrite, relayout = relayout)
    return nothing
end

function Writers.set_scalar!(
    contract_daf::ContractDaf,
    name::AbstractString,
    value::StorageScalar;
    overwrite::Bool = false,
)::Nothing
    access_scalar(contract_daf, name; is_modify = true)
    set_scalar!(contract_daf.daf, name, value; overwrite = overwrite)
    return nothing
end

function Writers.set_vector!(
    contract_daf::ContractDaf,
    axis::AbstractString,
    name::AbstractString,
    vector::Union{StorageScalar, StorageVector};
    overwrite::Bool = false,
)::Nothing
    access_vector(contract_daf, axis, name; is_modify = true)
    return set_vector!(contract_daf.daf, axis, name, vector; overwrite = overwrite)
end

function access_scalar(contract_daf::ContractDaf, name::AbstractString; is_modify::Bool)::Nothing
    tracker = get(contract_daf.data, name, nothing)
    if tracker === nothing
        if contract_daf.is_relaxed
            return nothing
        end
        error(dedent("""
            accessing non-contract scalar: $(name)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    if is_immutable(tracker.expectation; is_modify = is_modify)
        error(dedent("""
            modifying $(tracker.expectation) scalar: $(name)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    tracker.accessed = true
    return nothing
end

function access_axis(contract_daf::ContractDaf, axis::AbstractString; is_modify::Bool)::Nothing
    tracker = get(contract_daf.axes, axis, nothing)
    if tracker === nothing
        if contract_daf.is_relaxed
            return nothing
        end
        error(dedent("""
            accessing non-contract axis: $(axis)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    if is_immutable(tracker.expectation; is_modify = is_modify)
        error(dedent("""
            modifying $(tracker.expectation) axis: $(axis)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    tracker.accessed = true
    return nothing
end

function access_vector(contract_daf::ContractDaf, axis::AbstractString, name::AbstractString; is_modify::Bool)::Nothing
    access_axis(contract_daf, axis; is_modify = false)

    tracker = get(contract_daf.data, (axis, name), nothing)
    if tracker === nothing
        if contract_daf.is_relaxed || name == "name"
            return nothing
        end
        error(dedent("""
            accessing non-contract vector: $(name)
            of the axis: $(axis)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    if is_immutable(tracker.expectation; is_modify = is_modify)
        error(dedent("""
            modifying $(tracker.expectation) vector: $(name)
            of the axis: $(axis)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    tracker.accessed = true
    return nothing
end

function access_matrix(
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    is_modify::Bool,
)::Nothing
    access_axis(contract_daf, rows_axis; is_modify = false)
    access_axis(contract_daf, columns_axis; is_modify = false)

    tracker = get(contract_daf.data, (rows_axis, columns_axis, name), nothing)
    if tracker === nothing
        if contract_daf.is_relaxed
            return nothing
        end
        tracker = get(contract_daf.data, (columns_axis, rows_axis, name), nothing)
        if tracker === nothing
            error(dedent("""
                accessing non-contract matrix: $(name)
                of the rows axis: $(rows_axis)
                and the columns axis: $(columns_axis)
                for the computation: $(contract_daf.computation)
                on the daf data: $(contract_daf.name)
            """))
        end
    end

    if is_immutable(tracker.expectation; is_modify = is_modify)
        error(dedent("""
            modifying $(tracker.expectation) matrix: $(name)
            of the rows_axis: $(rows_axis)
            and the columns_axis: $(columns_axis)
            for the computation: $(contract_daf.computation)
            on the daf data: $(contract_daf.name)
        """))
    end

    tracker.accessed = true
    return nothing
end

function is_mandatory(expectation::ContractExpectation; is_output::Bool)::Bool
    return (is_output && expectation == GuaranteedOutput) || (!is_output && expectation == RequiredInput)
end

function is_forbidden(expectation::ContractExpectation; is_output::Bool, overwrite::Bool)::Bool
    return !is_output && expectation in (GuaranteedOutput, OptionalOutput) && !overwrite
end

function is_immutable(expectation::ContractExpectation; is_modify::Bool)::Bool
    return is_modify && expectation in (RequiredInput, OptionalInput)
end

function direction_name(is_output::Bool)::String
    if is_output
        return "output"
    else
        return "input"
    end
end

function Readers.require_scalar(contract_daf::ContractDaf, name::AbstractString)::Nothing  # untested
    Readers.require_scalar(contract_daf.daf, name)
    return nothing
end

function Readers.require_axis(  # untested
    contract_daf::ContractDaf,
    what_for::AbstractString,
    axis::AbstractString;
    for_change::Bool = false,
)::Nothing
    Readers.require_axis(contract_daf.daf, what_for, axis; for_change = for_change)
    return nothing
end

function Readers.require_vector(contract_daf::ContractDaf, axis::AbstractString, name::AbstractString)::Nothing  # untested
    Readers.require_vector(contract_daf, axis, name)
    return nothing
end

function Readers.require_matrix(  # untested
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    Readers.require_matrix(contract_daf.daf, rows_axis, columns_axis, name; relayout = relayout)
    return nothing
end

function Readers.require_axis_length(  # untested
    contract_daf::ContractDaf,
    what_length::StorageInteger,
    vector_name::AbstractString,
    axis::AbstractString,
)::Nothing
    Readers.require_axis_length(contract_daf.daf, what_length, vector_name, axis)
    return nothing
end

function Readers.require_dim_name(  # untested
    contract_daf::ContractDaf,
    axis::AbstractString,
    what::AbstractString,
    name::Union{Symbol, AbstractString};
    prefix::AbstractString = "",
)::Nothing
    Writers.require_dim_name(contract_daf.daf, axis, what, name; prefix = prefix)
    return nothing
end

function Writers.require_no_scalar(contract_daf::ContractDaf, name::AbstractString)::Nothing  # untested
    Writers.require_no_scalar(contract_daf.daf, name)
    return nothing
end

function Writers.require_no_axis(contract_daf::ContractDaf, axis::AbstractString; for_change::Bool = false)::Nothing  # untested
    Writers.require_no_axis(contract_daf.daf, axis; for_change = for_change)
    return nothing
end

function Writers.require_no_vector(contract_daf::ContractDaf, axis::AbstractString, name::AbstractString)::Nothing  # untested
    Writers.require_no_vector(contract_daf, axis, name)
    return nothing
end

function Writers.require_no_matrix(  # untested
    contract_daf::ContractDaf,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    relayout::Bool,
)::Nothing
    Writers.require_no_matrix(contract_daf, rows_axis, columns_axis, name; relayout = relayout)
    return nothing
end

function Writers.require_not_name(contract_daf::ContractDaf, axis::AbstractString, name::AbstractString)::Nothing  # untested
    Writers.require_not_name(contract_daf.daf, axis, name)
    return nothing
end

function Base.getindex(
    contract_daf::ContractDaf,
    query::QueryString,
)::Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray}
    return get_query(contract_daf, query)
end

function Formats.with_data_read_lock(action::Function, contract_daf::ContractDaf, what::AbstractString...)::Any  # untested
    return Formats.with_data_read_lock(action, contract_daf.daf, what...)
end

function Formats.with_data_write_lock(action::Function, contract_daf::ContractDaf, what::AbstractString...)::Any  # untested
    return Formats.with_data_write_lock(action, contract_daf.daf, what...)
end

function Queries.verify_contract_query(contract_daf::ContractDaf, cache_key::CacheKey)::Nothing
    dependecies_keys = contract_daf.internal.dependecies_of_query_keys[cache_key]
    for dependency_key in dependecies_keys
        type, key = dependency_key
        if type == CachedAxis
            access_axis(contract_daf, key[1]; is_modify = false)
        elseif type == CachedQuery
            @assert false
        elseif type == CachedData
            if key isa AbstractString
                access_scalar(contract_daf, key; is_modify = false)
            elseif key isa Tuple{AbstractString, AbstractString}
                access_vector(contract_daf, key...; is_modify = false)
            elseif key isa Tuple{AbstractString, AbstractString, AbstractString}
                access_matrix(contract_daf, key...; is_modify = false)
            else
                @assert false
            end
        elseif type == CachedNames
            if key isa AbstractString

            elseif key isa Tuple{AbstractString}  # untested
                access_axis(contract_daf, key[1]; is_modify = false)  # untested
            elseif key isa Tuple{AbstractString, AbstractString, Bool}  # untested
                access_axis(contract_daf, key[1]; is_modify = false)  # untested
                access_axis(contract_daf, key[2]; is_modify = false)  # untested
            end
        else
            @assert false
        end
    end
    return nothing
end

function Base.:+(left::Contract, right::Contract)::Contract
    return Contract(
        left.is_relaxed || right.is_relaxed,
        add_pairs(left.axes, right.axes),
        add_pairs(left.data, right.data),
    )
end

function add_pairs(::Nothing, ::Nothing)::Nothing
    return nothing
end

function add_pairs(::Nothing, right::T)::T where {T <: AbstractVector{<:Pair}}
    return right
end

function add_pairs(left::T, ::Nothing)::T where {T <: AbstractVector{<:Pair}}
    return left
end

function add_pairs(
    left::L,
    right::R,
)::AbstractVector{<:Pair} where {L <: AbstractVector{<:Pair}, R <: AbstractVector{<:Pair}}
    merged = Dict(left)
    for (right_key, right_specification) in right
        left_specification = get(merged, right_key, nothing)
        merged[right_key] = merge_specifications(right_key, left_specification, right_specification)
    end
    return collect(merged)
end

function merge_specifications(
    ::Any,
    ::Nothing,
    right_specification::T,
)::T where {T <: Union{AxisSpecification, DataSpecification}}
    return right_specification
end

function merge_specifications(
    axis_key::AxisKey,
    left_specification::AxisSpecification,
    right_specification::AxisSpecification,
)::AxisSpecification
    left_expectation, left_description = left_specification
    right_expectation, right_description = right_specification
    @assert left_description == right_description "different description for the axis: $(axis_key)"
    return (merge_expectations("axis", axis_key, left_expectation, right_expectation), left_description)
end

function merge_specifications(
    data_key::DataKey,
    left_specification::DataSpecification,
    right_specification::DataSpecification,
)::DataSpecification
    left_expectation, left_type, left_description = left_specification
    right_expectation, right_type, right_description = right_specification
    @assert left_description == right_description "different description for the data: $(data_key)"
    return (
        merge_expectations("data", data_key, left_expectation, right_expectation),
        merge_types(data_key, left_type, right_type),
        left_description,
    )
end

function merge_types(data_key::DataKey, left_type::Type, right_type::Type)::Type
    if left_type == right_type || left_type <: right_type
        return left_type
    elseif right_type <: left_type
        return right_type
    else
        error(dedent("""
            incompatible type: $(left_type)
            and type: $(right_type)
            for the contracts data: $(data_key)
        """))
    end
end

function merge_expectations(
    what::AbstractString,
    key::K,
    left_expectation::ContractExpectation,
    right_expectation::ContractExpectation,
)::ContractExpectation where {K <: Union{AxisKey, DataKey}}
    if left_expectation == RequiredInput && right_expectation in (RequiredInput, OptionalInput)
        return RequiredInput
    elseif left_expectation == OptionalInput && right_expectation in (RequiredInput, OptionalInput)
        return right_expectation
    elseif left_expectation == GuaranteedOutput && right_expectation in (RequiredInput, OptionalInput)
        return GuaranteedOutput
    elseif left_expectation == OptionalOutput && right_expectation == OptionalInput
        return OptionalOutput
    else
        error(dedent("""
            incompatible expectation: $(left_expectation)
            and expectation: $(right_expectation)
            for the contracts $(what): $(key)
        """))
    end
end

end # module
