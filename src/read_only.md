# Read-only

```@docs
DafJL.ReadOnly
```

## Arrays

`Daf` access operations return a read-only result; this allows `Daf` to cache results for efficiency, which is important
when getting the data is slow (e.g., accessing disk data or aggregating data). If you want to modify such results, you
need to explicitly create a copy. TODO: Explicitly support the concept of in-place modifications of data in `Daf`
(building on the memory-mapped implementation).

!!! note
    
    The read-only array functions below are restricted to dealing with normal (dense) arrays, `SparseArrays`,
    `NamedArrays`, and `LinearAlgebra` arrays (specifically, `Transpose` and `Adjoint`), as these are the types actually
    used in `Daf` storage. YMMV if using more exotic matrix types. In theory you could extend the implementation to
    cover such types as well.

```@docs
DafJL.ReadOnly.read_only_array
DafJL.ReadOnly.is_read_only_array
```

## Data

```@docs
DafJL.ReadOnly.DafReadOnly
DafJL.ReadOnly.read_only
DafJL.ReadOnly.DafReadOnlyWrapper
```

## Index

```@index
Pages = ["read_only.md"]
```
