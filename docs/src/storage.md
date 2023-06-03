# Storage

```@docs
Daf.Storage
```

## Abstract storage

```@docs
Daf.Storage.AbstractStorage
Daf.Storage.name
Daf.Storage.has_axis
Daf.Storage.add_axis!
Daf.Storage.delete_axis!
Daf.Storage.axis_entries
Daf.Storage.axis_length
```

## Concrete storage

To implement a new storage format adapter, you will need to implement the `name` and `has_axis` functions listed above.
In addition, you will need to implement the "unsafe" variant of the rest of the functions. This implementation can
ignore most error conditions because the "safe" version of the functions performs most validations first, before calling
the "unsafe" variant.

```@docs
Daf.Storage.unsafe_add_axis!
Daf.Storage.unsafe_delete_axis!
Daf.Storage.unsafe_axis_entries
Daf.Storage.unsafe_axis_length
```

## Memory storage

```@docs
Daf.Storage.MemoryStorage
```

## Index

```@index
Pages = ["storage.md"]
```
