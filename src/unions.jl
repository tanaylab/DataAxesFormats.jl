"""
In the `Daf` documentation, you will see [`Maybe`](@ref)`{...}`, but it acts exactly as if I used the explicit
`Union{..., Nothing}` notation.

I got sick and tired of writing `Union{..., Nothing}` everywhere. I therefore created this shorthand unions listed below
and used them throughout the code. I'm well aware there was a religious war of whether there should be a shorthand for
this vs. `Union{..., Missing}` with everyone losing - having to use the explicit `Union` notation everywhere.

Looking at the answers
[here](https://stackoverflow.com/questions/61936371/usage-and-convention-differences-between-missing-nothing-undef-and-nan-in-jul)
then `Nothing` means "there is no value" and `Missing` means "there is a value, but we don't know what it is" (`Unknown`
might have been a better name).

Under this interpretation, `Union{..., Nothing}` has (almost) the same semantics as Haskell's `Maybe`, so that's what I
called it (other languages call this `Optional` or `Opt` but I think Haskell came first, and at any rate I like `Maybe`
better). It is used heavily in my, and most other, Julia code. I also added `Unsure` as a shorthand for `Union{..., Missing}` for completeness, but I do not actually use it anywhere. I assume it is useful for Julia code dealing
specifically with statistical analysis.

I do not re-export the shorthand unions from the top-level `Daf` namespace. That is, even if `using Daf`, you will
**not** have my `Maybe` and `Unsure` shorthands pollute your namespace. If you do want to reuse them in your code,
explicitly write `using Daf.Unions`.
"""
module Unions

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

end # module
