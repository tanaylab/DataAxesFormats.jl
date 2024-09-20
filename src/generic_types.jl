"""
Generic types that arguably should belong in a more general-purpose package.

!!! note

    We do not re-export the types and functions defined here from the top-level `DataAxesFormats` namespace. That is,
    even if `using DataAxesFormats`, you will **not** have these generic names polluting your namespace. If you do want
    to reuse them in your code, explicitly write `using DataAxesFormats.GenericTypes`.

We got sick and tired of writing `Union{..., Nothing}` everywhere. We therefore created this shorthand unions listed
below and used them throughout the code. We're well aware there was a religious war of whether there should be a
standard shorthand for this, vs. a standard shorthand for `Union{..., Missing},` with everyone losing, that is, having
to use the explicit `Union` notation everywhere.

Looking at the answers
[here](https://stackoverflow.com/questions/61936371/usage-and-convention-differences-between-missing-nothing-undef-and-nan-in-jul)
then `Nothing` means "there is no value" and `Missing` means "there is a value, but we don't know what it is" (`Unknown`
might have been a better name).

Under this interpretation, `Union{..., Nothing}` has (almost) the same semantics as Haskell's `Maybe`, so that's what we
called it (other languages call this `Optional` or `Opt`). It is used heavily in our (and a lot of other) Julia code. We
also added `Unsure` as a shorthand for `Union{..., Missing}` for completeness, but we do not actually use it anywhere.
We assume it is useful for Julia code dealing specifically with statistical analysis.
"""
module GenericTypes

export Maybe
export Unsure

"""
    Maybe{T} = Union{T, Nothing}

The type to use when maybe there is a value, maybe there isn't. This is exactly as if writing the explicit `Union`
with `Nothing` but is shorter and more readable. This is extremely common.
"""
Maybe = Union{T, Nothing} where {T}

"""
    Unsure{T} = Union{T, Missing}

The type to use when maybe there always is a value, but sometimes we are not sure what it is. This is exactly as if
writing the explicit `Union` with `Missing` but is shorter and more readable. This is only used in code dealing with
statistics to represent missing (that is, unknown) data. It is only provided here for completeness.
"""
Unsure = Union{T, Missing} where {T}

end  # module
