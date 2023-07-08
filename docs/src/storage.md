# Storage

```@docs
Daf.Storage
```

## Abstract storage

```@docs
Daf.Storage.AbstractStorage
```

## Freezing storage

```@docs
Daf.Storage.is_frozen
Daf.Storage.freeze
Daf.Storage.unfreeze
```

## Scalar properties

```@docs
Daf.Storage.has_scalar
Daf.Storage.set_scalar!
Daf.Storage.delete_scalar!
Daf.Storage.scalar_names
Daf.Storage.get_scalar
```

## Data axes

```@docs
Daf.Storage.has_axis
Daf.Storage.add_axis!
Daf.Storage.delete_axis!
Daf.Storage.axis_names
Daf.Storage.get_axis
Daf.Storage.axis_length
```

## Vector properties

```@docs
Daf.Storage.has_vector
Daf.Storage.set_vector!
Daf.Storage.delete_vector!
Daf.Storage.vector_names
Daf.Storage.get_vector
```

## Matrix properties

```@docs
Daf.Storage.has_matrix
Daf.Storage.set_matrix!
Daf.Storage.delete_matrix!
Daf.Storage.matrix_names
Daf.Storage.get_matrix
```

## Creating properties

```@docs
Daf.Storage.empty_dense_vector!
Daf.Storage.empty_sparse_vector!
Daf.Storage.empty_dense_matrix!
Daf.Storage.empty_sparse_matrix!
```

## Concrete storage

### For scalars:

```@docs
Daf.Storage.unsafe_set_scalar!
Daf.Storage.unsafe_delete_scalar!
Daf.Storage.unsafe_get_scalar
```

### For axes:

```@docs
Daf.Storage.unsafe_add_axis!
Daf.Storage.unsafe_axis_length
Daf.Storage.unsafe_delete_axis!
Daf.Storage.unsafe_get_axis
```

### For vectors:

```@docs
Daf.Storage.unsafe_has_vector
Daf.Storage.unsafe_set_vector!
Daf.Storage.unsafe_empty_dense_vector!
Daf.Storage.unsafe_empty_sparse_vector!
Daf.Storage.unsafe_delete_vector!
Daf.Storage.unsafe_vector_names
Daf.Storage.unsafe_get_vector
```

### For matrices:

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
