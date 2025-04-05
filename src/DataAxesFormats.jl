"""
The `DataAxesFormats` package provides a uniform generic interface for accessing 1D and 2D data arranged along some set
of axes. This is a much-needed generalization of the [AnnData](https://pypi.org/project/anndata/) functionality. The key
features are:

  - The data model [`StorageTypes`](@ref) include (1) some axes with named entries, (2) vector data indexed by a single
    axis, (3) matrix data indexed by a pair of axes, and also (4) scalar data (anything not tied to some axis).
  - Explicit control over 2D data (row or column major), with support for both dense and sparse matrices, both of which
    are crucial for performance.
  - Out of the box, allow storing the data in memory (using [`MemoryDaf`](@ref)), directly inside
    [HDF5](https://www.hdfgroup.org/solutions/hdf5/) files (using [`H5df`](@ref)), or as a collection of simple files in
    a directory (using [`FilesDaf`](@ref)), which works nicely with tools like `make` for automating computation
    pipelines.
  - Import and export to/from [`AnnDataFormat`](@ref) for interoperability with non-`Daf` tools.
  - Implementation with a focus on memory-mapping to allow for efficient processing of large data sets (in theory,
    larger than the system's memory). In particular, merely opening a data set is a fast operation (almost) regardless
    of its size.
  - Well-defined interfaces for implementing additional storage [`Formats`](@ref).
  - Creating [`Chains`](@ref) of data sets, allowing zero-copy reuse of common data between multiple computation
    pipelines.
  - [`Concat`](@ref) multiple data sets into a single data set along one or more axes.
  - A [`Query`](@ref) language for accessing the data, providing features such as slicing, aggregation and filtering,
    and making [`Views`](@ref) and [`Copies`](@ref) based on these queries.
  - Self documenting [`Computations`](@ref) with an explicit [`Contracts`](@ref) describing and enforcing the inputs and
    outputs, and [`Adapters`](@ref) for applying the computation to data of a different format.

!!! note

    The top-level `DataAxesFormats` module re-exports all(most) everything from the sub-modules, so you can directly
    access any exported symbol by `using DataAxesFormats` (or, say, `import DataAxesFormats: MemoryDaf`), instead of
    having to import or use qualified names (such as `DataAxesFormats.MemoryFormat.MemoryDaf`).

The `Daf` datasets type hierarchy looks like this:

  - [`DafReader`](@ref DataAxesFormats.Formats.DafReader)

      + [`DafReadOnly`](@ref DataAxesFormats.ReadOnly.DafReadOnly) (abstract type)

          * [`DafReadOnlyWrapper`](@ref DataAxesFormats.ReadOnly.DafReadOnly) (created by [`read_only`](@ref DataAxesFormats.ReadOnly.read_only))
          * [`DafView`](@ref DataAxesFormats.Views.DafView) (created by [`viewer`](@ref DataAxesFormats.Views.viewer))
          * [`ReadOnlyChain`](@ref DataAxesFormats.Chains.ReadOnlyChain) (created by [`chain_reader`](@ref DataAxesFormats.Chains.chain_reader))

      + [`DafWriter`](@ref DataAxesFormats.Formats.DafWriter) (abstract type)

          * [`WriteChain`](@ref DataAxesFormats.Chains.WriteChain) (created by [`chain_writer`](@ref DataAxesFormats.Chains.chain_writer))
          * [`MemoryDaf`](@ref DataAxesFormats.MemoryFormat.MemoryDaf)
          * [`FilesDaf`](@ref DataAxesFormats.FilesFormat.FilesDaf)
          * [`H5df`](@ref DataAxesFormats.H5dfFormat.H5df)

Here are all the internal modules implementing this package and the relationship between them (linking to their
documentation). They are also listed in the quick access bar on the left.

![](assets/modules.svg)
"""
module DataAxesFormats

using Reexport

include("storage_types.jl")
@reexport using .StorageTypes

include("registry.jl")
@reexport using .Registry: EltwiseOperation, ReductionOperation

include("tokens.jl")
@reexport using .Tokens

include("keys.jl")
@reexport using .Keys

include("operations.jl")
@reexport using .Operations

include("formats.jl")
@reexport using .Formats

include("readers.jl")
@reexport using .Readers

include("read_only.jl")
@reexport using .ReadOnly

include("groups.jl")
@reexport using .Groups

include("writers.jl")
@reexport using .Writers

include("queries.jl")
@reexport using .Queries

include("files_format.jl")
@reexport using .FilesFormat

include("chains.jl")
@reexport using .Chains

include("h5df_format.jl")
@reexport using .H5dfFormat

include("memory_format.jl")
@reexport using .MemoryFormat

include("views.jl")
@reexport using .Views

include("reconstruction.jl")
@reexport using .Reconstruction

include("anndata_format.jl")
@reexport using .AnnDataFormat

include("contracts.jl")
@reexport using .Contracts

include("computations.jl")
@reexport using .Computations

include("copies.jl")
@reexport using .Copies

include("concat.jl")
@reexport using .Concat

include("adapters.jl")
@reexport using .Adapters

include("example_data.jl")
@reexport using .ExampleData

end  # module
