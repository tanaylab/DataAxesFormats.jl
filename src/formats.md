# Formats

```@docs
DataAxesFormats.Formats
```

## Read API

```@docs
DataAxesFormats.Formats.DafReader
DataAxesFormats.Formats.FormatReader
DataAxesFormats.Formats.Internal
```

### Caching

```@docs
DataAxesFormats.Formats.CacheGroup
DataAxesFormats.Formats.empty_cache!
```

### Description

```@docs
DataAxesFormats.Formats.format_description_header
DataAxesFormats.Formats.format_description_footer
```

### Scalar properties

```@docs
DataAxesFormats.Formats.format_has_scalar
DataAxesFormats.Formats.format_scalars_set
DataAxesFormats.Formats.format_get_scalar
```

### Data axes

```@docs
DataAxesFormats.Formats.format_has_axis
DataAxesFormats.Formats.format_axes_set
DataAxesFormats.Formats.format_axis_array
DataAxesFormats.Formats.format_axis_length
```

### Vector properties

```@docs
DataAxesFormats.Formats.format_has_vector
DataAxesFormats.Formats.format_vectors_set
DataAxesFormats.Formats.format_get_vector
```

### Matrix properties

```@docs
DataAxesFormats.Formats.format_has_matrix
DataAxesFormats.Formats.format_matrices_set
DataAxesFormats.Formats.format_get_matrix
```

## Write API

```@docs
DataAxesFormats.Formats.DafWriter
DataAxesFormats.Formats.FormatWriter
```

### Scalar properties

```@docs
DataAxesFormats.Formats.format_set_scalar!
DataAxesFormats.Formats.format_delete_scalar!
```

### Data axes

```@docs
DataAxesFormats.Formats.format_add_axis!
DataAxesFormats.Formats.format_delete_axis!
```

### Vector properties

```@docs
DataAxesFormats.Formats.format_set_vector!
DataAxesFormats.Formats.format_delete_vector!
```

### Matrix properties

```@docs
DataAxesFormats.Formats.format_set_matrix!
DataAxesFormats.Formats.format_relayout_matrix!
DataAxesFormats.Formats.format_delete_matrix!
```

### Creating properties

```@docs
DataAxesFormats.Formats.format_get_empty_dense_vector!
DataAxesFormats.Formats.format_get_empty_sparse_vector!
DataAxesFormats.Formats.format_filled_empty_sparse_vector!
DataAxesFormats.Formats.format_get_empty_dense_matrix!
DataAxesFormats.Formats.format_get_empty_sparse_matrix!
DataAxesFormats.Formats.format_filled_empty_sparse_matrix!
```

## Index

```@index
Pages = ["formats.md"]
```
