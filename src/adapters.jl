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
using Daf.Queries
using Daf.ReadOnly
using Daf.Unions
using Daf.Views

"""
    adapter(
        computation::Function,
        view::Union{DafView, ReadOnlyView};
        [name::Maybe{AbstractString} = nothing,
        capture=MemoryDaf,
        axes::AbstractVector{Pair{String, AxesValue}} = Vector{Pair{String, String}}(),
        data::AbstractVector{Pair{DataKey, DataValue}} = Vector{Pair{String, String}}(),
        empty::Maybe{Dict{EmptyKey, EmptyValue}} = nothing,
        relayout::Bool = true,
        overwrite::Bool = false]
    )::Any where {
        DataKey <: Union{
            String,                        # Scalar name
            Tuple{String, String},         # Axis, vector name
            Tuple{String, String, String}  # Rows axis, columns axis, matrix name
        },
        DataValue <: Maybe{Union{AbstractString, Query}},
        AxesValue <: Maybe{Union{AbstractString, Query}},
        EmptyKey <: Union{
            Tuple{AbstractString, AbstractString},                  # Key for empty value for vectors.
            Tuple{AbstractString, AbstractString, AbstractString},  # Key for empty value for matrices.
        },
        EmptyValue <: StorageScalarBase
    }

Invoke a computation on a `view` data set and return the result; copy a [`viewer`](@ref) of the updated data set into
the base `daf` data of the view. If specified, the `name` is used as a prefix for all the names; otherwise, the `view`
name is used as the prefix.

If you have some `daf` data you wish to run a computation on, you need to deal with name mismatches. That is, the names
of the input and output data properties of the computation may be different from these used in your data. In addition,
you might be interested only in a subset of the computed data properties, to avoiding polluting your data set with
irrelevant properties.

To address these issues, the common idiom for applying computations to `daf` data is to use the `adapter` as follows:

  - Create a (read-only) `view` of your data which presents the data properties under the names expected by the
    computation, using [`viewer`](@ref). If the computation was annotated by `@computation`, then its [`Contract`](@ref)
    will be explicitly documented so you will know exactly what to provide.
  - Pass this `view` to `adapter`, which will invoke the `computation` with a (writable) `adapted` version of the data
    (created using [`chain_writer`](@ref) and a new `DafWriter` to `capture` the output; by default, this will be a
    [`MemoryDaf`]@(ref)).
  - Once the computation is done, create a new view of the output, which presents the subset of the output data
    properties you are interested in, with the names you would like to store them as. Again, if the computation was
    annotated by [`@computation`](@ref), then its [`Contract`](@ref) will be explicitly documented so you will know
    exactly what to expect.
  - Copy this output view data into the base `daf` data of the `view` (using [`copy_all!`](@ref), `empty`, `relayout`
    (default: `true`) and `overwrite` (default: `false`).

That is, the code would look something like this:

```
daf = ... # Some input `daf` data we wish to compute on.

# Here `daf` contains the inputs for the computation, but possibly
# under a different name.

result = adapter(
    "example",              # A name to use to generate the temporary `daf` data names.
    view(daf; ...),         # How to view the input in the way expected by the computation.
    axes = ..., data = ..., # How and what to view the output for copying back into `daf`.
    empty = ...,            # If the view specifies a subset of some axes.
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
function adapter(
    computation::Function,
    view::Union{DafView, ReadOnlyView};
    name::Maybe{AbstractString} = nothing,
    capture = MemoryDaf,
    axes::AbstractVector{Pair{String, AxesValue}} = Vector{Pair{String, String}}(),
    data::AbstractVector{Pair{DataKey, DataValue}} = Vector{Pair{String, String}}(),
    empty::Maybe{Dict} = nothing,
    relayout::Bool = true,
    overwrite::Bool = false,
)::Any where {
    DataKey <: Union{String, Tuple{String, String}, Tuple{String, String, String}},
    DataValue <: Maybe{Union{AbstractString, Query}},
    AxesValue <: Maybe{Union{AbstractString, Query}},
}
    if name == nothing
        prefix = view.name
    else
        prefix = name  # untested
    end
    input_chain = chain_writer([view, capture(; name = "$(prefix).capture")]; name = "$(prefix).input")
    result = computation(input_chain)
    output_chain = viewer(input_chain; name = "$(prefix).output", axes = axes, data = data)
    copy_all!(; from = output_chain, into = view.daf, empty = empty, relayout = relayout, overwrite = overwrite)
    return result
end

end # module
