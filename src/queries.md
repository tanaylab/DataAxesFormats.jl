# Queries

```@docs
DataAxesFormats.Queries
```

## Construction

```@docs
DataAxesFormats.Queries.Query
DataAxesFormats.Queries.@q_str
DataAxesFormats.Queries.QueryString
```

## Functions

```@docs
DataAxesFormats.Queries.get_query
DataAxesFormats.Queries.get_frame
DataAxesFormats.Queries.FrameColumn
DataAxesFormats.Queries.FrameColumns
DataAxesFormats.Queries.full_vector_query
DataAxesFormats.Queries.query_result_dimensions
DataAxesFormats.Queries.query_requires_relayout
DataAxesFormats.Queries.is_axis_query
```

## Syntax

```@docs
DataAxesFormats.Queries.QUERY_OPERATORS
DataAxesFormats.Queries.NAMES_QUERY
DataAxesFormats.Queries.SCALAR_QUERY
DataAxesFormats.Queries.LOOKUP_PROPERTY
DataAxesFormats.Queries.VECTOR_ENTRY
DataAxesFormats.Queries.MATRIX_ENTRY
DataAxesFormats.Queries.REDUCE_VECTOR
DataAxesFormats.Queries.VECTOR_QUERY
DataAxesFormats.Queries.VECTOR_PROPERTY
DataAxesFormats.Queries.VECTOR_LOOKUP
DataAxesFormats.Queries.MATRIX_ROW
DataAxesFormats.Queries.MATRIX_COLUMN
DataAxesFormats.Queries.REDUCE_MATRIX
DataAxesFormats.Queries.MATRIX_QUERY
DataAxesFormats.Queries.MATRIX_LOOKUP
DataAxesFormats.Queries.COUNTS_MATRIX
DataAxesFormats.Queries.POST_PROCESS
DataAxesFormats.Queries.GROUP_BY
DataAxesFormats.Queries.AXIS_MASK
DataAxesFormats.Queries.MASK_OPERATION
DataAxesFormats.Queries.MASK_SLICE
DataAxesFormats.Queries.VECTOR_FETCH
DataAxesFormats.Queries.guess_typed_value
```

## Query Operators

```@docs
DataAxesFormats.Queries.QuerySequence
```

### Data Operators

```@docs
DataAxesFormats.Queries.AsAxis
DataAxesFormats.Queries.Axis
DataAxesFormats.Queries.CountBy
DataAxesFormats.Queries.Fetch
DataAxesFormats.Queries.GroupBy
DataAxesFormats.Queries.IfMissing
DataAxesFormats.Queries.IfNot
DataAxesFormats.Queries.Lookup
DataAxesFormats.Queries.Names
```

### Comparison Operators

```@docs
DataAxesFormats.Queries.ComparisonOperation
DataAxesFormats.Queries.IsEqual
DataAxesFormats.Queries.IsGreater
DataAxesFormats.Queries.IsGreaterEqual
DataAxesFormats.Queries.IsLess
DataAxesFormats.Queries.IsLessEqual
DataAxesFormats.Queries.IsMatch
DataAxesFormats.Queries.IsNotEqual
DataAxesFormats.Queries.IsNotMatch
```

### Mask Operators

```@docs
DataAxesFormats.Queries.And
DataAxesFormats.Queries.AndNot
DataAxesFormats.Queries.Or
DataAxesFormats.Queries.OrNot
DataAxesFormats.Queries.Xor
DataAxesFormats.Queries.XorNot
DataAxesFormats.Queries.MaskSlice
DataAxesFormats.Queries.SquareMaskColumn
DataAxesFormats.Queries.SquareMaskRow
```

## Index

```@index
Pages = ["queries.md"]
```
