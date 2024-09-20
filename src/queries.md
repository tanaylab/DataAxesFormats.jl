# Queries

```@docs
Daf.Queries
```

## Construction

```@docs
Daf.Queries.Query
Daf.Queries.@q_str
Daf.Queries.QueryString
```

## Functions

```@docs
Daf.Queries.get_query
Daf.Queries.get_frame
Daf.Queries.FrameColumn
Daf.Queries.FrameColumns
Daf.Queries.full_vector_query
Daf.Queries.query_result_dimensions
Daf.Queries.query_requires_relayout
Daf.Queries.is_axis_query
```

## Syntax

```@docs
Daf.Queries.QUERY_OPERATORS
Daf.Queries.NAMES_QUERY
Daf.Queries.SCALAR_QUERY
Daf.Queries.LOOKUP_PROPERTY
Daf.Queries.VECTOR_ENTRY
Daf.Queries.MATRIX_ENTRY
Daf.Queries.REDUCE_VECTOR
Daf.Queries.VECTOR_QUERY
Daf.Queries.VECTOR_PROPERTY
Daf.Queries.VECTOR_LOOKUP
Daf.Queries.MATRIX_ROW
Daf.Queries.MATRIX_COLUMN
Daf.Queries.REDUCE_MATRIX
Daf.Queries.MATRIX_QUERY
Daf.Queries.MATRIX_LOOKUP
Daf.Queries.COUNTS_MATRIX
Daf.Queries.POST_PROCESS
Daf.Queries.GROUP_BY
Daf.Queries.AXIS_MASK
Daf.Queries.MASK_OPERATION
Daf.Queries.MASK_SLICE
Daf.Queries.VECTOR_FETCH
Daf.Queries.guess_typed_value
```

## Query Operators

```@docs
Daf.Queries.QuerySequence
```

### Data Operators

```@docs
Daf.Queries.AsAxis
Daf.Queries.Axis
Daf.Queries.CountBy
Daf.Queries.Fetch
Daf.Queries.GroupBy
Daf.Queries.IfMissing
Daf.Queries.IfNot
Daf.Queries.Lookup
Daf.Queries.Names
```

### Comparison Operators

```@docs
Daf.Queries.ComparisonOperation
Daf.Queries.IsEqual
Daf.Queries.IsGreater
Daf.Queries.IsGreaterEqual
Daf.Queries.IsLess
Daf.Queries.IsLessEqual
Daf.Queries.IsMatch
Daf.Queries.IsNotEqual
Daf.Queries.IsNotMatch
```

### Mask Operators

```@docs
Daf.Queries.And
Daf.Queries.AndNot
Daf.Queries.Or
Daf.Queries.OrNot
Daf.Queries.Xor
Daf.Queries.XorNot
Daf.Queries.MaskSlice
Daf.Queries.SquareMaskColumn
Daf.Queries.SquareMaskRow
```

## Index

```@index
Pages = ["queries.md"]
```
