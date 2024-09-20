# Queries

```@docs
DafJL.Queries
```

## Construction

```@docs
DafJL.Queries.Query
DafJL.Queries.@q_str
DafJL.Queries.QueryString
```

## Functions

```@docs
DafJL.Queries.get_query
DafJL.Queries.get_frame
DafJL.Queries.FrameColumn
DafJL.Queries.FrameColumns
DafJL.Queries.full_vector_query
DafJL.Queries.query_result_dimensions
DafJL.Queries.query_requires_relayout
DafJL.Queries.is_axis_query
```

## Syntax

```@docs
DafJL.Queries.QUERY_OPERATORS
DafJL.Queries.NAMES_QUERY
DafJL.Queries.SCALAR_QUERY
DafJL.Queries.LOOKUP_PROPERTY
DafJL.Queries.VECTOR_ENTRY
DafJL.Queries.MATRIX_ENTRY
DafJL.Queries.REDUCE_VECTOR
DafJL.Queries.VECTOR_QUERY
DafJL.Queries.VECTOR_PROPERTY
DafJL.Queries.VECTOR_LOOKUP
DafJL.Queries.MATRIX_ROW
DafJL.Queries.MATRIX_COLUMN
DafJL.Queries.REDUCE_MATRIX
DafJL.Queries.MATRIX_QUERY
DafJL.Queries.MATRIX_LOOKUP
DafJL.Queries.COUNTS_MATRIX
DafJL.Queries.POST_PROCESS
DafJL.Queries.GROUP_BY
DafJL.Queries.AXIS_MASK
DafJL.Queries.MASK_OPERATION
DafJL.Queries.MASK_SLICE
DafJL.Queries.VECTOR_FETCH
DafJL.Queries.guess_typed_value
```

## Query Operators

```@docs
DafJL.Queries.QuerySequence
```

### Data Operators

```@docs
DafJL.Queries.AsAxis
DafJL.Queries.Axis
DafJL.Queries.CountBy
DafJL.Queries.Fetch
DafJL.Queries.GroupBy
DafJL.Queries.IfMissing
DafJL.Queries.IfNot
DafJL.Queries.Lookup
DafJL.Queries.Names
```

### Comparison Operators

```@docs
DafJL.Queries.ComparisonOperation
DafJL.Queries.IsEqual
DafJL.Queries.IsGreater
DafJL.Queries.IsGreaterEqual
DafJL.Queries.IsLess
DafJL.Queries.IsLessEqual
DafJL.Queries.IsMatch
DafJL.Queries.IsNotEqual
DafJL.Queries.IsNotMatch
```

### Mask Operators

```@docs
DafJL.Queries.And
DafJL.Queries.AndNot
DafJL.Queries.Or
DafJL.Queries.OrNot
DafJL.Queries.Xor
DafJL.Queries.XorNot
DafJL.Queries.MaskSlice
DafJL.Queries.SquareMaskColumn
DafJL.Queries.SquareMaskRow
```

## Index

```@index
Pages = ["queries.md"]
```
