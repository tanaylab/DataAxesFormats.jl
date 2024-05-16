"""
Adapt `Daf` data to a [`@computation`](@ref).
"""
module Adapters

export adapter

using ..Chains
using ..Computations
using ..Contracts
using ..Copies
using ..Copies
using ..GenericLogging
using ..GenericTypes
using ..Formats
using ..MemoryFormat
using ..StorageTypes
using ..Queries
using ..ReadOnly
using ..Views

"""
    adapter(
        computation::Function,
        view::Union{DafWriter, DafReadOnly},
        [name::Maybe{AbstractString} = nothing,
        capture=MemoryDaf,
        axes::Maybe{ViewAxes} = nothing,
        data::Maybe{ViewData} = nothing,
        empty::Maybe{EmptyData} = nothing,
        relayout::Bool = true,
        overwrite::Bool = false]
    )::Any

Invoke a `computation` on a `view` data set and return the result; copy a [`viewer`](@ref) of the updated data set into
the base `Daf` data of the view. If specified, the `name` is used as a prefix for all the names; otherwise, the `view`
name is used as the prefix.

If you have some `Daf` data you wish to run a `computation` on, you need to deal with name mismatches. That is, the names
of the input and output data properties of the `computation` may be different from these used in your data. In addition,
you might be interested only in a subset of the computed data properties, to avoiding polluting your data set with
irrelevant properties.

To address these issues, the common idiom for applying computations to `Daf` data is to use the `adapter` as
follows:

  - Create a (read-only) `view` of your data which presents the data properties under the names expected by the
    `computation`, using [`viewer`](@ref). If the `computation` was annotated by [`@computation`](@ref), then its
    [`Contract`](@ref) will be explicitly documented so you will know exactly what to provide.
  - Pass this `view` to `adapter`, which will invoke the `computation` with a (writable) `adapted` version of the
    data (created using [`chain_writer`](@ref) and a new [`DafWriter`](@ref) to `capture` the output; by default, this
    will be a [`MemoryDaf`](@ref)), but it can be any function that takes a `name` (named) parameter and returns a
    `DafWriter`.
  - Once the `computation` is done, create a new view of the output, which presents the subset of the output data
    properties you are interested in, with the names you would like to store them as. Again, if the computation was
    annotated by [`@computation`](@ref), then its [`Contract`](@ref) will be explicitly documented so you will know
    exactly what to expect.
  - Copy this output view data into the base `Daf` data of the `view` (using [`copy_all!`](@ref), `empty`, `relayout`
    (default: `true`) and `overwrite` (default: `false`).

!!! note

    If the names of the properties in the input already match the contract of the `computation`, you can pass the data
    set directly as the `view`. The call to `adapter` may still be needed to filter or rename the `computation`'s
    output. If the outputs can also be used as-is, then there's no need to invoke `adapter`; directly apply the
    `computation` to the data and be done.

Typically the code would look something like this:

```
daf = ... # Some input `Daf` data we wish to compute on.

# Here `daf` contains the inputs for the computation, but possibly
# under a different name.

result = adapter(
    viewer(daf; ...),          # How to view the input in the way expected by the computation.
    name = "example",          # A name to use to generate the temporary `Daf` data names.
    axes = ..., data = ...,    # How and what to view from the output for copying back into `daf`.
    empty = ...,               # If the input view specifies a subset of some axes.
) do adapted                   # The writable adapted data we can pass to the computation.
    computation(adapted, ...)  # Actually do the computation.
    return ...                 # An additional result outside `daf`.
end

# Here `daf` will contain the specific renamed outputs specified in `adapter`,
# and you can also access the additional non-`daf` data `result`.
```

This idiom allows [`@computation`](@ref) functions to use clear generic names for their inputs and outputs, and still
apply them to arbitrary data sets using more specific names. One can even invoke the same computation with different
parameter values, and store the different results in the same data set under different names.
"""
@logged function adapter(
    computation::Function,
    view::Union{DafWriter, DafReadOnly};
    name::Maybe{AbstractString} = nothing,
    capture = MemoryDaf,
    axes::Maybe{ViewAxes} = nothing,
    data::Maybe{ViewData} = nothing,
    empty::Maybe{EmptyData} = nothing,
    relayout::Bool = true,
    overwrite::Bool = false,
)::Any
    writer = capture(; name = get_adapter_capture_name(view; name = name))
    adapted = get_adapter_input(view; name = name, writer = writer)
    result = computation(adapted)
    copy_adapter_output(
        view,
        adapted;
        name = name,
        axes = axes,
        data = data,
        empty = empty,
        relayout = relayout,
        overwrite = overwrite,
    )
    return result
end

function get_adapter_capture_name(view::Union{DafWriter, DafReadOnly}; name::Maybe{AbstractString})::AbstractString
    _, prefix = get_base(view, name)
    return prefix * ".capture"
end

@logged function get_adapter_input(
    view::Union{DafWriter, DafReadOnly};
    name::Maybe{AbstractString},
    writer::DafWriter,
)::DafWriter
    _, prefix = get_base(view, name)
    return chain_writer([view, writer]; name = "$(prefix).adapted")
end

@logged function copy_adapter_output(
    view::Union{DafWriter, DafReadOnly},
    adapted::DafWriter;
    name::Maybe{AbstractString},
    axes::Maybe{ViewAxes},
    data::Maybe{ViewData},
    empty::Maybe{EmptyData},
    relayout::Bool,
    overwrite::Bool,
)::Nothing
    destination, prefix = get_base(view, name)
    output = viewer(adapted; name = "$(prefix).output", axes = axes, data = data)
    copy_all!(; source = output, destination = destination, empty = empty, relayout = relayout, overwrite = overwrite)
    return nothing
end

function get_base(view::Union{DafWriter, DafReadOnly}, name::Maybe{AbstractString})::Tuple{DafWriter, AbstractString}
    base = view
    if base isa DafReadOnly
        base = view.daf
        @assert base isa DafWriter && !base.internal.is_frozen
    end

    if name === nothing
        prefix = base.name
    else
        prefix = name  # untested
    end

    return (base, prefix)
end

end # module
