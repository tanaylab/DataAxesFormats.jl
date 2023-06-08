"""
The `Daf.jl` package provides a uniform generic interface for accessing 1D and 2D data arranged along some set of axes.
This is a much-needed generalization of the [AnnData](https://pypi.org/project/anndata) functionality. The key
features are:

  - Support both in-memory and persistent data storage of "any" format (given an adapter implementation).

  - Out of the box, allow storing the data in memory, in `AnnData` objects (e.g., using `h5ad` files), directly inside
    [H5FS](https://hdfgroup.org/) files, or as a collection of simple memory-mapped files in a directory.
  - The data model is based on (1) some axes with named entries, (2) vector data indexed by a single axis, (3) matrix data
    indexed by a pair of axes, and also (4) scalar data items (anything not tied to some axis).
  - There is explicit control over 2D data layout (row or column major), and support for both dense and sparse matrices,
    both of which are crucial for performance.

The top-level `Daf` module re-exports everything all the sub-modules, so you can directly access any exported symbol by
`using Daf` (or `import Daf: MemoryStorage`), instead of having to import or use qualified names (such as
`Daf.Storage.MemoryStorage`).
"""
module Daf

using Reexport

include("data_types.jl")
@reexport using Daf.DataTypes

include("as_dense.jl")
@reexport using Daf.AsDense

include("messages.jl")
@reexport using Daf.Messages

include("storage.jl")
@reexport using Daf.Storage

end # module
