# Generic Types

```@docs
DafJL.GenericTypes
```

## Unions

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

```@docs
DafJL.GenericTypes.Maybe
DafJL.GenericTypes.Unsure
```

## Index

```@index
Pages = ["generic_types.md"]
```
