# TODO

The following issues are not addressed (yet):

## Composable computations

  - Adapters based on views.

## Basic functionality

  - Aggregation functions.
  - Aggregation in queries.

## Disk storage formats

  - `FilesStorage`
  - `Hdf5Storage`
  - `AnnDataStorage`

## Queries

  - Test the operations.
  - Implement more operations.

## Functionality

  - `concatenate_sparse_vectors!`
  - `concatenate_sparse_matrices!`
  - Concatenate along axis

## Tracking

  - Log non-computation functions.
  - Collect computations invocations into "computations" scalar (JSON blob)?
  - Repository: DAG of data sets

## Documentation

  - Add examples and doctests (especially for queries)
  - Link to `Expression` in documentation of `Context`?
  - Link to `encode_expression` in documentation of `Token`?
  - Rename links to `QueryToken` in documentation of queries?

## Misc

  - Make containers thread-safe!!!
  - @everywhere for registries

## Performance

  - More efficient lookup of chained properties?
  - More efficient copy of `:from_is_subset` sparse matrices.
    
      + `expand_sparse_vector`
      + `expand_sparse_matrix`
