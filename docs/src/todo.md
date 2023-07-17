# TODO

The following issues are not addressed:

## Queries

  - Return named arrays.
  - Implement more operations.
  - Relayout?
  - Generic input types?

## Composable computations

  - Generic two-way `Adapter` functionality based on the `View`
  - Composable self-documenting `Computation` functions

## Disk storage formats

  - Make `name` a reserved property (alias `name` member of the storage?)
  - `FilesStorage`
  - `Hdf5Storage`
  - `AnnDataStorage`
  - `ChainStorage`
  - `ViewStorage`

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

  - @everywhere for registries
  - More efficient lookup of chained properties?
