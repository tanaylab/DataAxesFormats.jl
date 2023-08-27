# TODO

The following issues are not addressed:

## Queries

  - Implement more operations.
  - Test the operations.
  - Aggregation in queries.

## Composable computations

  - Copy functions.
  - Aggregation functions.
  - Adapters based on views.

## Disk storage formats

  - `ChainStorage`
  - `FilesStorage`
  - `Hdf5Storage`
  - `AnnDataStorage`

## Convenience functions

  - `concatenate_sparse_vectors!`
  - `concatenate_sparse_matrices!`
  - `embed_sparse_vector!`
  - `embed_sparse_matrix!`

## Documentation

  - Add examples and doctests (especially for queries)
  - Link to `Expression` in documentation of `Context`?
  - Link to `encode_expression` in documentation of `Token`?
  - Rename links to `QueryToken` in documentation of queries?

## Misc

  - Make containers thread-safe!!!
  - @everywhere for registries
  - More efficient lookup of chained properties?
