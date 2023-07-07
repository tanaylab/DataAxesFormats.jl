# Storage

```@docs
Daf.Storage
```

## Abstract storage

```@docs
Daf.Storage.AbstractStorage
```

We require each storage to have a human-readable `.name::String` property for error messages and the like. This name
should be unique, using [`unique_name`](@ref Daf.Messages.unique_name).

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
for keep metadata for the whole data set (e.g., provenance and version information). The code is not optimized for
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

`Daf` disk storage formats rely heavily on memory-mapping. This allows efficient access to large amount of data, in
theory even larger than the available memory. While reading such data is straightforward enough, writing it is tricky,
because using [`set_vector!`](@ref) or [`set_matrix!`](@ref) would require one to create a full in-memory version of the
data, which would then be written to disk; this is not only inefficient, but also limits one to data that fits in
memory. The following functions allow creating and memory-mapping data directly in the storage, allowing for more
efficient data storage creation, and storing arbitrary large data regardless of the available memory size.

```@docs
Daf.Storage.empty_dense_vector!
Daf.Storage.empty_sparse_vector!
Daf.Storage.empty_dense_matrix!
Daf.Storage.empty_sparse_matrix!
```

## Concrete storage

To implement a new storage format adapter, you will need to provide a `.name::String` property, and the
[`is_frozen`](@ref), [`freeze`](@ref), [`unfreeze`](@ref), [`has_scalar`](@ref) and [`has_axis`](@ref) functions listed
above. In addition, you will need to implement the "unsafe" variant of the rest of the functions. This implementation
can ignore most error conditions because the "safe" version of the functions performs most validations first, before
calling the "unsafe" variant.

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
Daf.Storage.unsafe_empty_dense_vector!
Daf.Storage.unsafe_empty_sparse_vector!
Daf.Storage.unsafe_delete_vector!
Daf.Storage.unsafe_vector_names
Daf.Storage.unsafe_get_vector
```

For matrices:

```@docs
Daf.Storage.unsafe_has_matrix
Daf.Storage.unsafe_set_matrix!
Daf.Storage.unsafe_empty_dense_matrix!
Daf.Storage.unsafe_empty_sparse_matrix!
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
