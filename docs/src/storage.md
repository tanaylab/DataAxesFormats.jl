# Storage

```@docs
Daf.Storage
```

## Abstract storage

```@docs
Daf.Storage.AbstractStorage
```

We require each storage to have a (ideally, unique) human-readable name for error messages and the like.

```@docs
Daf.Storage.storage_name
```

A storage may be frozen, preventing it against changes. This prevents calling any of the `add_...!`, `set_...!` and
`delete_...!` functions. It also tries to prevent, as much as Julia allows, modifications to the vectors and matrices
returned from the storage. Normally, these modified in-place - this applies even to persistent storage formats that use
memory-mapping (which is all of them).

```@docs
Daf.Storage.is_frozen
Daf.Storage.freeze
Daf.Storage.unfreeze
```

We can store arbitrary named values in the storage, that is, treat it as a simple key-value container. This is useful
for keep metadata for the whole data set (e.g. provenance and versioning information). The code is not optimized for
usage as a "heavy duty" key-value container, though; there are plenty of other options for that.

```@docs
Daf.Storage.has_scalar
Daf.Storage.set_scalar!
Daf.Storage.delete_scalar!
Daf.Storage.scalar_names
Daf.Storage.get_scalar
```

The focus of the storage is on data along some axes:

```@docs
Daf.Storage.has_axis
Daf.Storage.add_axis!
Daf.Storage.delete_axis!
Daf.Storage.axis_names
Daf.Storage.get_axis
Daf.Storage.axis_length
```

We can store named vector (1D) data along an axis:

```@docs
Daf.Storage.has_vector
Daf.Storage.set_vector!
Daf.Storage.delete_vector!
Daf.Storage.vector_names
Daf.Storage.get_vector
```

And named matrix (2D) data along a pair of axes:

```@docs
Daf.Storage.has_matrix
Daf.Storage.set_matrix!
Daf.Storage.delete_matrix!
Daf.Storage.matrix_names
Daf.Storage.get_matrix
```

## Concrete storage

To implement a new storage format adapter, you will need to implement the `storage_name`, `is_frozen`, `freeze`,
`unfreeze`, `has_scalar` and `has_axis` functions listed above. In addition, you will need to implement the "unsafe"
variant of the rest of the functions. This implementation can ignore most error conditions because the "safe" version of
the functions performs most validations first, before calling the "unsafe" variant.

For scalars:

```@docs
Daf.Storage.unsafe_set_scalar!
Daf.Storage.unsafe_delete_scalar!
Daf.Storage.unsafe_get_scalar
```

For axes:

```@docs
Daf.Storage.unsafe_add_axis!
Daf.Storage.unsafe_axis_length
Daf.Storage.unsafe_delete_axis!
Daf.Storage.unsafe_get_axis
```

For vectors:

```@docs
Daf.Storage.unsafe_has_vector
Daf.Storage.unsafe_set_vector!
Daf.Storage.unsafe_delete_vector!
Daf.Storage.unsafe_vector_names
Daf.Storage.unsafe_get_vector
```

For matrices:

```@docs
Daf.Storage.unsafe_has_matrix
Daf.Storage.unsafe_set_matrix!
Daf.Storage.unsafe_delete_matrix!
Daf.Storage.unsafe_matrix_names
Daf.Storage.unsafe_get_matrix
```

## Memory storage

```@docs
Daf.Storage.MemoryStorage
```

## Index

```@index
Pages = ["storage.md"]
```
