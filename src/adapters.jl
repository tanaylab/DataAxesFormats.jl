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
using ..Formats
using ..MemoryFormat
using ..StorageTypes
using ..Queries
using ..ReadOnly
using ..Views
using TanayLabUtilities

"""
    adapter(
        computation::Function,
        daf::DafWriter;
        name::AbstractString = ".adapter",
        input_axes::Maybe{ViewAxes} = nothing,
        input_data::Maybe{ViewData} = nothing,
        capture = MemoryDaf,
        output_axes::Maybe{ViewAxes} = nothing,
        output_data::Maybe{ViewData} = nothing,
        empty::Maybe{EmptyData} = nothing,
        relayout::Bool = true,
        overwrite::Bool = false,
    )::Any

Invoke a `computation` on a view of some `daf` data and return the result; copy a view of the results into the base
`daf` data.

If you have some `Daf` data you wish to run a `computation` on, you need to deal with name mismatches. That is, the names
of the input and output data properties of the `computation` may be different from these used in your data. In addition,
you might be interested only in a subset of the computed data properties, to avoiding polluting your data set with
irrelevant properties.

To address these issues, the common idiom for applying computations to `Daf` data is to use the `adapter` as
follows:

  - Create a (read-only) view of your data which presents the data properties under the names expected by the
    `computation`, using `input_axes` and `input_data`. If the `computation` was annotated by [`@computation`](@ref),
    then its [`Contract`](@ref) will be explicitly documented so you will know exactly what to provide.
  - Chain this read-only view with an empty `capture` writable data set (by default, [`MemoryDaf`](@ref)) and pass the
    result to the `computation` as the "adapted" data set.
  - Once the `computation` is done, use the `output_axes` and `output_data` to create a view of the
    output, and copy this subset to the original `daf` data set, using (using [`copy_all!`](@ref), `empty`, `relayout`
    (default: `true`) and `overwrite` (default: `false`).

Typically the code would look something like this:

```
daf = ... # Some input `Daf` data we wish to compute on.

# Here `daf` contains the inputs for the computation, but possibly
# under a different name.

result = adapter(
    daf;                                   # The Daf data set we want to apply the computation to.
    input_axes = ..., input_data = ...,    # How and what to provide as input to the computation.
    output_axes = ..., output_data = ...,  # How and what to copy back as output of the computation.
    empty = ...,                           # If the input view specifies a subset of some axes.
) do adapted                   # The writable adapted data we can pass to the computation.
    computation(adapted, ...)  # Actually do the computation.
    return ...                 # An additional result outside `daf`.
end

The `name` parameter is used for [`flame_timed`](@ref).

# Here `daf` will contain the specific renamed outputs specified in `adapter`,
# and you can also access the additional non-`daf` data `result`.
```

This idiom allows [`@computation`](@ref) functions to use clear generic names for their inputs and outputs, and still
apply them to arbitrary data sets that use more specific names. One can even invoke the same computation with different
parameter values, and store the different results in the same data set under different names.
"""
@logged function adapter(  # NOLINT
    computation::Function,
    daf::DafWriter;
    name::AbstractString = ".adapter",
    input_axes::Maybe{ViewAxes} = nothing,
    input_data::Maybe{ViewData} = nothing,
    capture = MemoryDaf,
    output_axes::Maybe{ViewAxes} = nothing,
    output_data::Maybe{ViewData} = nothing,
    empty::Maybe{EmptyData} = nothing,
    relayout::Bool = true,
    overwrite::Bool = false,
)::Any
    flame_timed(name) do
        local adapted
        local base_name
        flame_timed("input") do
            base_name = daf.name
            @assert input_axes !== nothing ||
                    input_data !== nothing ||
                    output_axes !== nothing ||
                    output_data !== nothing "no-op adapter"
            input = viewer(daf; axes = input_axes, data = input_data, name = base_name * ".input")
            captured = capture(; name = base_name * ".capture")
            adapted = chain_writer([input, captured]; name = base_name * ".adapted")  # NOLINT
            return nothing
        end
        result = computation(adapted)
        flame_timed("output") do
            output = viewer(adapted; axes = output_axes, data = output_data, name = base_name * ".output")
            return copy_all!(; source = output, destination = daf, empty, relayout, overwrite)
        end
        return result
    end
end

end # module
