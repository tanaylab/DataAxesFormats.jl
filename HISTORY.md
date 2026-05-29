# v0.3.0

  - Support packing the data (chunked + compressed), works for both dense and sparse data.
  - Support serving `Daf` data over HTTP/S.
  - Support ZIP files format.
  - Full support for Zarr backend (including ZIP and HTTP/S support).

# v0.2.0

  - Improve query language.
  - Make views lazy.
  - Improve contracts.
  - Track performance (flame graphs).
  - Improve performance (control over contracts and other expensive verifications).

# v0.1.2

  - Add tensors to documentation.
  - Rename `axis_array` to `axis_vector` (breaking change).
  - Adapt to Julia 1.11.
  - Minor bug fixes.

# v0.1.1

  - Add tensors to APIs (a 3D tensor is just a collection of 2D matrices with a naming convention).
  - Minor bug fixes.

# v0.1.0

First (alpha) release.
