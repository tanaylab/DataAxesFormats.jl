# TODO

The following issues are not addressed:

## Containers

  - The actual `Daf` container (as opposed to the low-level `Storage`)
  - Which should provide a `Query` based API
  - Which would allow for `ElementWise` and `Reduction` operations
  - Which would allow for a powerful `View` mechanism

## Composable computations

  - Generic two-way `Adapter` functionality based on the `View`
  - Composable self-documenting `Computation` functions

## Disk storage formats

  - `FilesStorage`
  - `Hdf5Storage`
  - `AnnDataStorage`

## Convenience functions

  - `concatenate_sparse_vectors!`
  - `concatenate_sparse_matrices!`
  - `embed_sparse_vector!`
  - `embed_sparse_matrix!`

## Read-only matrix/vector types

The Julia standard library has *internally* `ReadOnly` arrays and even `FixedSparseCSC` and `FixedSparseVector` that
make use for it. However, "what is allowed to Jupiter is forbidden for the ox", so these are not exported and can't be
used by us mere mortals (unless we duplicate most of that code). This is a real pity because it would be "very nice
indeed" if "frozen" storage returned `ReadOnly` arrays, protected against accidental change. The situation isn't *that*
bad because all the disk storage formats (will) use memory-mapping, allowing us to protect their data using the
operating system instead. This however does not work for data stored in-memory.

  - `ReadOnly` as guide for `DenseView`
