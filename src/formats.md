# Formats

```@docs
DafJL.Formats
```

## Read API

```@docs
DafJL.Formats.DafReader
DafJL.Formats.FormatReader
DafJL.Formats.Internal
```

### Caching

```@docs
DafJL.Formats.CacheGroup
DafJL.Formats.empty_cache!
```

### Description

```@docs
DafJL.Formats.format_description_header
DafJL.Formats.format_description_footer
```

### Scalar properties

```@docs
DafJL.Formats.format_has_scalar
DafJL.Formats.format_scalars_set
DafJL.Formats.format_get_scalar
```

### Data axes

```@docs
DafJL.Formats.format_has_axis
DafJL.Formats.format_axes_set
DafJL.Formats.format_axis_array
DafJL.Formats.format_axis_length
```

### Vector properties

```@docs
DafJL.Formats.format_has_vector
DafJL.Formats.format_vectors_set
DafJL.Formats.format_get_vector
```

### Matrix properties

```@docs
DafJL.Formats.format_has_matrix
DafJL.Formats.format_matrices_set
DafJL.Formats.format_get_matrix
```

## Write API

```@docs
DafJL.Formats.DafWriter
DafJL.Formats.FormatWriter
```

### Scalar properties

```@docs
DafJL.Formats.format_set_scalar!
DafJL.Formats.format_delete_scalar!
```

### Data axes

```@docs
DafJL.Formats.format_add_axis!
DafJL.Formats.format_delete_axis!
```

### Vector properties

```@docs
DafJL.Formats.format_set_vector!
DafJL.Formats.format_delete_vector!
```

### Matrix properties

```@docs
DafJL.Formats.format_set_matrix!
DafJL.Formats.format_relayout_matrix!
DafJL.Formats.format_delete_matrix!
```

### Creating properties

```@docs
DafJL.Formats.format_get_empty_dense_vector!
DafJL.Formats.format_get_empty_sparse_vector!
DafJL.Formats.format_filled_empty_sparse_vector!
DafJL.Formats.format_get_empty_dense_matrix!
DafJL.Formats.format_get_empty_sparse_matrix!
DafJL.Formats.format_filled_empty_sparse_matrix!
```

## Index

```@index
Pages = ["formats.md"]
```
