# DAF - Data in Axes in Formats

The `DataAxesFormats` package provides a uniform generic interface for accessing 1D and 2D data arranged along some set
of axes. This is a much-needed generalization of the [AnnData](https://github.com/scverse/anndata) functionality. Unlike
other generalizations (e.g., [Muon](https://github.com/scverse/mudata)), `Daf` attempts to provide a simple generic
unified 1D and 2D data storage, which can be used for "any" purpose, not necessarily scRNA and/or ATAC data, though such
use cases were the driving force for the development of `Daf`.

The key features of `Daf` are:

  - Support both in-memory and persistent data storage of "any" format (given an adapter implementation).

  - The implementation is thread-safe, using read/write locks, to allow safe and efficient parallel processing.
  - Out of the box, allow importing and exporting `AnnData` objects (e.g., using `h5ad` files), storing the data in
    memory, directly inside [H5FS](https://hdfgroup.org/) files, or as a collection of simple memory-mapped files in a
    directory.
  - Support views and adapters for applying generic algorithms to your specific data while using your own specific names
    for the data properties.
  - Support chaining repositories for reusing a large base repository with one or more smaller computed results
    repositories, possibly computed in several ways for comparison of the results.
  - The data model is based on (1) some axes with named entries, (2) vector data indexed by a single axis, (3) matrix
    data indexed by a pair of axes, and also (4) scalar data (anything not tied to some axis).
  - The common case where one axis is a group of another is explicitly supported (e.g., storing a type for each cell,
    and having a type axis).
  - A simple query language allows for common operations such as accessing subsets of the data ("age of all cells which
    aren't doublets") or group data ("color of type of cell") or aggregate data ("mean age of cells of batch").
  - There is explicit control over 2D data layout (row or column major), and support for both dense and sparse matrices,
    both of which are crucial for performance.
  - This is implemented in Julia, as a seed for efficient computation pipelines (which are hard to implement in Python
    without resorting to using C/C++ code). WIP: a [DafPY](https://pypi.org/project/dafpy/) Python package, which is a
    thin wrapper around `DataAxesFormats` allowing efficient (zero-copy) access to the data using `numpy`, `scipy` and
    `pandas` vector and matrix types. WIP: Implement a similar R package using
    [JuliaCall](https://libraries.io/cran/JuliaCall) to allow direct access to `DataAxesFormats` from R code.

See the [v0.2.0 documentation](https://tanaylab.github.io/DataAxesFormats.jl/v0.2.0) for details.

## Status

Version 0.2.0 is an alpha release. We hope it is feature complete and have started using it for internal projects.
However, everything is subject to change based on user feedback (so don't be shy). Comments, bug reports and PRs are
welcome!

## Motivation

The `Daf` package was created to overcome the limitations of the `AnnData` package. Like `AnnData`, `Daf` was created to
support code analyzing single-cell RNA sequencing data ("scRNA-seq"). Unlike `AnnData`, the `Daf` data model was
designed from the start to naturally cover additional modalities (such as ATAC), as well as any other kind of 1D and 2D
data arranged in common axes, without requiring further extensions.

The main issue we had with `AnnData` is that it restricts all the stored data to be described by two axes
("observations" and "variables"). E.g., in single-cell data, every cell would be an "observation" and every gene would
be a "variable". As a secondary annoyance, `AnnData` gives one special "default" per-observation-per-variable data layer
the uninformative name `X`, and only allows meaningful names for any additional data layers, which use a completely
different access method.

This works pretty well until one starts to perform higher level operations:

  - (Almost) everyone groups cells into "type" clusters. This requires storing per-cluster data (e.g., its name and its
    color).
  - Since "type" clusters (hopefully) correspond to biological states, which map to gene expression levels, this
    requires also storing per-cluster-per-gene data.
  - Often such clusters form at least a two-level hierarchy, so we need per-sub-cluster data and
    per-sub-cluster-per-gene data as well.
  - Lately the use of multi-modal data (e.g. ATAC) also requires storing data per-cell-per-genome-location.

Sure, it is possible to use a set of `AnnData` objects, each with its own distinct set of "observations" (for cell,
clusters, and sub-clusters), and/or "variables" (genes, genome locations). We can establish conventions about what `X`
is in each data set, which will have to depend on the type of variables (UMIs for genes, accessibility for genome
locations). We'll also need to replicate simple per-cell and per-gene data across the different `AnnData` objects, and
keep it in sync, or just store each such data in one (or some) of the objects, and remember in which.

In short, we'd end up writing some problem-specific code to manage the multiple `AnnData` objects for us. An example of
such an approach is `Muon`, which addresses part of the issue by introducing the concept of multiple "assays". This
narrow approach doesn't address the issue of clustering, for example.

Instead, we have chosen to create `Daf`, which is a general-purpose solution that embraces the existence of arbitrary
multiple axes in the same data set, and enforces no opaque default names, to make it easy for us store explicitly named
data per-whatever-we-damn-please all in a single place.

When it comes to storage, `Daf` makes it as easy as possible to write adapters to allow storing the data in your
favorite format. In particular, `Daf` allows storing a single (or multiple) data sets inside an
[HDF5](https://www.hdfgroup.org/solutions/hdf5/) file in `h5df`/`h5dfs` files, similar to `AnnData` allowing storage in
`.h5ad` files.

That said, we find that, for our use cases, the use of complex single-file formats such as HDF5 to be sub-optimal. In
effect they function as a file system, but offer only some of its functionality. For example, you need special APIs to
list the content of the data, copy or delete just parts of it, find out which parts have been changed when. Also, most
implementations do not support memory-mapping the data, which causes a large performance hit for large data sets.

Therefore `Daf` also supports a simple files storage format where every property is stored in separate file(s) (in a
trivial format) under some root directory. This allows for efficient memory-mapping of files, using standard file system
tools to list, copy and/or delete data, and using tools like `make` to automate incremental computations. The main
downside is that to send a data set across the network, one has to first collect it into a `tar` or `zip` or some other
archive file format. This may actually end up being faster as this allows compressing the data for more efficient
transmission or archiving. Besides, due to the limitations of `AnnData`, one often has to send multiple files for a
complete data set anyway.

`Daf` also provides a simple in-memory storage format, which is a very efficient and lightweight container (similar to
an in-memory `AnnData` object).

Since `AnnData` is used by many existing tools, `Daf` allows exporting (a subset of) a data set into `AnnData`, and
present an `AnnData` data set as a (restricted) `Daf` data set.

It is possible to create zero-copy views of `Daf` data (slicing, renaming and hiding axes and/or specific properties),
and to copy `Daf` data from one data set to another. `Daf` also allows chaining data sets, for example storing
alternative clustering options in separates (small) data sets, next to the clustered data, without having to copy the
(large) clustered data into each data set. The combination of views and chains avoids needlessly duplicating large
amounts of data just to mold it to the form needed by some computational tool.

It is assumed that `Daf` data will be processed in a single machine, that is, `Daf` does not try to address the issues
of a distributed cluster of servers working on a shared data set. Today's servers (as of 2023) can get very big (~100
cores and ~1TB of RAM is practical), which means that all/most data sets would fit comfortably in one machine (and
memory mapped files are a great help here). In addition, if using the "files" storage, it is possible to have different
servers access the same `Daf` directory, each computing a different independent additional annotation (e.g., one server
searching for doublets while another is searching for gene modules), and as long as only one server writes each new data
property, this should work fine (one can do even better by writing more complex code). This is another example of how
simple files make it easy to provide functionality which is very difficult to achieve using a complex single-file format
such as HDF5.

The bottom line is that `Daf` provides a "universal" (well, generic) useful abstraction layer for 1D and 2D data
storage, which can be persisted in any "reasonable" storage format, allowing efficient computation and/or visualization
code to naturally access and/or write only the data it needs, even for higher-level analysis pipeline, for small to
"very large" (but not for "ludicrously large") data sets. `Daf` can import and export `AnnData` data sets for
interoperability with legacy computation pipelines.

## Installation

Just `Pkg.add("DataAxesFormats")`, like installing any other Julia package.

To install the Python `Daf` [package](https://github.com/tanaylab/dafpy), just `pip install dafpy`, like installing any
other Python package.

TODO: To install the R wrappers...

## License (MIT)

Copyright © 2023-2025 Weizmann Institute of Science

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
