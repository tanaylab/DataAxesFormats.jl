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

## Concrete storage

To implement a new storage format adapter, you will need to implement the `name`, `has_scalar` and `has_axis` functions
listed above. In addition, you will need to implement the "unsafe" variant of the rest of the functions. This
implementation can ignore most error conditions because the "safe" version of the functions performs most validations
first, before calling the "unsafe" variant.

```@docs
Daf.Storage.unsafe_set_scalar!
Daf.Storage.unsafe_delete_scalar!
Daf.Storage.unsafe_get_scalar
Daf.Storage.unsafe_add_axis!
Daf.Storage.unsafe_delete_axis!
Daf.Storage.unsafe_get_axis
Daf.Storage.unsafe_axis_length
Daf.Storage.unsafe_has_vector
Daf.Storage.unsafe_set_vector!
Daf.Storage.unsafe_delete_vector!
Daf.Storage.unsafe_get_vector
Daf.Storage.unsafe_vector_names
```

## Memory storage

```@docs
Daf.Storage.MemoryStorage
```

## Index

```@index
Pages = ["storage.md"]
```
