# Queries

```@docs
Daf.Query
```

## Escaping special characters

```@docs
Daf.Query.escape_query
Daf.Query.unescape_query
Daf.Query.is_safe_query_char
```

## Query syntax

```@docs
Daf.Query.QueryOperators
Daf.Query.QueryOperator
Daf.Query.QueryOperation
Daf.Query.QueryToken
Daf.Query.QueryExpression
Daf.Query.QueryContext
```

## Matrix queries

```@docs
Daf.Query.parse_matrix_query
Daf.Query.MatrixQuery
Daf.Query.MatrixPropertyLookup
Daf.Query.MatrixAxes
Daf.Query.MatrixLayout
```

## Vector queries

```@docs
Daf.Query.parse_vector_query
Daf.Query.VectorQuery
Daf.Query.VectorDataLookup
Daf.Query.VectorPropertyLookup
Daf.Query.MatrixSliceLookup
Daf.Query.MatrixSliceAxes
Daf.Query.ReduceMatrixQuery
```

## Scalar queries

```@docs
Daf.Query.parse_scalar_query
Daf.Query.ScalarQuery
Daf.Query.ScalarDataLookup
Daf.Query.ScalarPropertyLookup
Daf.Query.ReduceVectorQuery
Daf.Query.VectorEntryLookup
Daf.Query.MatrixEntryLookup
Daf.Query.MatrixEntryAxes
```

## Filtering axes

```@docs
Daf.Query.FilteredAxis
Daf.Query.AxisFilter
Daf.Query.FilterOperator
Daf.Query.AxisLookup
```

## Slicing axes

```@docs
Daf.Query.AxisEntry
```

## Looking up properties

```@docs
Daf.Query.PropertyLookup
Daf.Query.PropertyComparison
Daf.Query.ComparisonOperator
```

## Query operations

```@docs
Daf.Query.parse_eltwise_operation
Daf.Query.parse_reduction_operation
Daf.Query.ParameterAssignment
```

## Canonical format

```@docs
Daf.Query.canonical
```

## Index

```@index
Pages = ["query.md"]
```
