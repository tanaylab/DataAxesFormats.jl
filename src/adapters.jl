"""
Adapt `daf` data to a [`@computation`](@ref).
"""
module Adapters

export adapter

using Daf.Chains
using Daf.Computations
using Daf.Contracts
using Daf.Copies
using Daf.MemoryFormat
using Daf.ReadOnly
using Daf.Views

"""
    adapter(
        computation::Function,
        name::AbstractString,
        view::Union{DafView, ReadOnlyView},
        [capture=MemoryDaf,
        scalars::AbstractVector{Pair{String, Union{String, Nothing}}} = [],
        axes::AbstractVector{Pair{String, Union{String, Nothing}}} = [],
        vectors::AbstractVector{Pair{Tuple{String, String}, Union{String, Nothing}}} = []],
        matrices::AbstractVector{Pair{Tuple{String, String, String}, Union{String, Nothing}}} = [],
        relayout::Bool = true,
        overwrite::Bool = false]
    )::Any

Invoke a computation on a `view` data set and return the result; copy a [`viewer`](@ref) of the updated data set into
the base `daf` data of the view.

If you have some `daf` data you wish to run a computation on, you need to deal with name mismatches. That is, the names
of the input and output data properties of the computation may be different from these used in your data. In addition,
you might be interested only in a subset of the computed data properties, to avoiding polluting your data set with
irrelevant properties.

To address these issues, the common idiom for applying computations to `daf` data is to use the `adapter` as follows:

  - Create a (read-only) `view` of your data which presents the data properties under the names expected by the
    computation, using [`viewer`](@ref). If the computation was annotated by `@computation`, then its [`Contract`](@ref)
    will be explicitly documented so you will know exactly what to provide.

  - Pass this `view` to `adapter`, which will invoke the `computation` with a (writable) `adapted` version of the data
    (created using [`chain_writer`](@ref) and a new `DafWriter` to `capture` the output, by default,
    [`MemoryDaf`]@(ref)).
  - Once the computation is done, create a new view of the output, which presents the subset of the output data
    properties you are interested in, with the names you would like to store them as. Again, if the computation was
    annotated by [`@computation`](@ref), then its [`Contract`](@ref) will be explicitly documented so you will know
    exactly what to expect.
  - Copy this output view data into the base `daf` data of the `view` (using [`copy_all!`](@ref), `relayout` (default:
    `true`) and `overwrite` (default: `false`).

That is, the code would look something like this:

```
daf = ... # Some input `daf` data we wish to compute on.

# Here `daf` contains the inputs for the computation, but possibly
# under a different name.

result = adapter(
    "example",      # A name to use to generate the temporary `daf` data names.
    view(daf; ...), # How to view the input in the way expected by the computation.
    ...,            # How and what to view the output for copying back into `daf`.
) do adapted        # The writable adapted data we can pass to the computation.
    computation(adapted, ...)  # Actually do the computation.
    return ...                 # An additional result outside `daf`.
end

# Here `daf` will contain the specific renamed outputs specified in `adapter`,
# and you can also access the additional non-`daf` data `result`.
```

This idiom allows [`@computation`](@ref) functions to use clear generic names for their inputs and outputs, and still
apply them to arbitrary data sets using more specific names. For example, one can invoke the same computation with
different parameter values, and store the different results in the same data set under different names. Or, one can
pre-process the inputs of the computation, storing the result under a different name, and still be able to apply the
computation to these modified inputs.
"""
function adapter(
    computation::Function,
    name::AbstractString,
    view::Union{DafView, ReadOnlyView};
    capture = MemoryDaf,
    scalars::AbstractVector{Pair{String, S}} = Vector{Pair{String, Union{String, Nothing}}}(),
    axes::AbstractVector{Pair{String, A}} = Vector{Pair{String, Union{String, Nothing}}}(),
    vectors::AbstractVector{Pair{Tuple{String, String}, V}} = Vector{
        Pair{Tuple{String, String}, Union{String, Nothing}},
    }(),
    matrices::AbstractVector{Pair{Tuple{String, String, String}, M}} = Vector{
        Pair{Tuple{String, String, String}, Union{String, Nothing}},
    }(),
    relayout::Bool = true,
    overwrite::Bool = false,
)::Any where {
    S <: Union{String, Nothing},
    A <: Union{String, Nothing},
    V <: Union{String, Nothing},
    M <: Union{String, Nothing},
}
    input_chain = chain_writer("$(view.daf.name).$(name).input", [view, capture("$(view.daf.name).$(name).capture")])
    result = computation(input_chain)
    output_chain = viewer(
        "$(view.daf.name).$(name).output",
        input_chain;
        scalars = scalars,
        axes = axes,
        vectors = vectors,
        matrices = matrices,
    )
    copy_all!(; from = output_chain, into = view.daf, relayout = relayout, overwrite = overwrite)
    return result
end

end # module
