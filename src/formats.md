# Formats

```@docs
Daf.Formats
```

## Read API

```@docs
Daf.Formats.DafReader
Daf.Formats.FormatReader
Daf.Formats.Internal
```

### Caching

```@docs
Daf.Formats.CacheGroup
Daf.Formats.empty_cache!
```

### Description

```@docs
Daf.Formats.format_description_header
Daf.Formats.format_description_footer
```

### Scalar properties

```@docs
Daf.Formats.format_has_scalar
Daf.Formats.format_scalars_set
Daf.Formats.format_get_scalar
```

### Data axes

```@docs
Daf.Formats.format_has_axis
Daf.Formats.format_axes_set
Daf.Formats.format_axis_array
Daf.Formats.format_axis_length
```

### Vector properties

```@docs
Daf.Formats.format_has_vector
Daf.Formats.format_vectors_set
Daf.Formats.format_get_vector
```

### Matrix properties

```@docs
Daf.Formats.format_has_matrix
Daf.Formats.format_matrices_set
Daf.Formats.format_get_matrix
```

## Write API

```@docs
Daf.Formats.DafWriter
Daf.Formats.FormatWriter
```

### Scalar properties

```@docs
Daf.Formats.format_set_scalar!
Daf.Formats.format_delete_scalar!
```

### Data axes

```@docs
Daf.Formats.format_add_axis!
Daf.Formats.format_delete_axis!
```

### Vector properties

```@docs
Daf.Formats.format_set_vector!
Daf.Formats.format_delete_vector!
```

### Matrix properties

```@docs
Daf.Formats.format_set_matrix!
Daf.Formats.format_relayout_matrix!
Daf.Formats.format_delete_matrix!
```

### Creating properties

```@docs
Daf.Formats.format_get_empty_dense_vector!
Daf.Formats.format_get_empty_sparse_vector!
Daf.Formats.format_filled_empty_sparse_vector!
Daf.Formats.format_get_empty_dense_matrix!
Daf.Formats.format_get_empty_sparse_matrix!
Daf.Formats.format_filled_empty_sparse_matrix!
```

## Index

```@index
Pages = ["formats.md"]
```
