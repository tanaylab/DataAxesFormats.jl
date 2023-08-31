# Queries

```@docs
Daf.ParseQueries
```

## Queries syntax

```@docs
Daf.ParseQueries.QueryOperators
Daf.ParseQueries.QueryOperator
Daf.ParseQueries.QueryOperation
Daf.ParseQueries.QueryToken
Daf.ParseQueries.QueryExpression
Daf.ParseQueries.QueryContext
```

## Matrix queries

```@docs
Daf.ParseQueries.parse_matrix_query
Daf.ParseQueries.MatrixQuery
Daf.ParseQueries.MatrixDataLookup
Daf.ParseQueries.MatrixPropertyLookup
Daf.ParseQueries.MatrixAxes
Daf.ParseQueries.MatrixCounts
Daf.ParseQueries.CountedAxes
Daf.ParseQueries.CountedAxis
```

## Vector queries

```@docs
Daf.ParseQueries.parse_vector_query
Daf.ParseQueries.VectorQuery
Daf.ParseQueries.VectorDataLookup
Daf.ParseQueries.VectorPropertyLookup
Daf.ParseQueries.MatrixSliceLookup
Daf.ParseQueries.MatrixSliceAxes
Daf.ParseQueries.ReduceMatrixQuery
```

## Scalar queries

```@docs
Daf.ParseQueries.parse_scalar_query
Daf.ParseQueries.ScalarQuery
Daf.ParseQueries.ScalarDataLookup
Daf.ParseQueries.ScalarPropertyLookup
Daf.ParseQueries.ReduceVectorQuery
Daf.ParseQueries.VectorEntryLookup
Daf.ParseQueries.MatrixEntryLookup
Daf.ParseQueries.MatrixEntryAxes
```

## Filtering axes

```@docs
Daf.ParseQueries.FilteredAxis
Daf.ParseQueries.AxisFilter
Daf.ParseQueries.FilterOperator
Daf.ParseQueries.AxisLookup
```

## Slicing axes

```@docs
Daf.ParseQueries.AxisEntry
```

## Looking up properties

```@docs
Daf.ParseQueries.PropertyLookup
Daf.ParseQueries.PropertyComparison
Daf.ParseQueries.ComparisonOperator
```

## Query operations

```@docs
Daf.ParseQueries.parse_eltwise_operation
Daf.ParseQueries.parse_reduction_operation
Daf.ParseQueries.ParameterAssignment
```

## Canonical format

```@docs
Daf.ParseQueries.canonical
```

## Index

```@index
Pages = ["parse_queries.md"]
```
