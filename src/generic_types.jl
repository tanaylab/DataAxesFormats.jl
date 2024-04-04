"""
Generic types that arguably should belong in a more general-purpose package.

We do not re-export the types and functions defined here from the top-level `Daf` namespace. That is, even if
`using Daf`, you will **not** have these generic names polluting your namespace. If you do want to reuse them in your
code, explicitly write `using Daf.GenericTypes`.
"""
module GenericTypes

export AbstractStringSet
export AbstractStringVector
export Maybe
export Unsure

"""
    AbstractStringSet = AbstractSet{S} where {S <: AbstractString}

A set of strings, without commitment to the concrete implementation of either the set or the strings contained in it.
"""
AbstractStringSet = AbstractSet{S} where {S <: AbstractString}

"""
    AbstractStringVector = AbstractVector{S} where {S <: AbstractString}

A vector of strings, without commitment to the concrete implementation of either the vector or the strings contained in
it.
"""
AbstractStringVector = AbstractVector{S} where {S <: AbstractString}

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
