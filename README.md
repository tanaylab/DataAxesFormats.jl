# DAF - Data in Axes in Formats

![Documentation Status](https://readthedocs.org/projects/daf.jl/badge/?version=latest)

The `Daf.jl` package provides a uniform generic interface for accessing 1D and 2D data arranged along some set of axes.
This is a much-needed generalization of the [AnnData](https://pypi.org/project/anndata) functionality. The key
features are:

  - Support both in-memory and persistent data storage of "any" format (given an adapter implementation).

  - Out of the box, allow storing the data in memory, in `AnnData` objects (e.g., using `h5ad` files), directly inside
    [H5FS](https://hdfgroup.org/) files, or as a collection of simple memory-mapped files in a directory.
  - The data model is based on (1) some axes with named entries, (2) vector data indexed by a single axis, (3) matrix
    data indexed by a pair of axes, and also (4) scalar data (anything not tied to some axis).
  - There is explicit control over 2D data layout (row or column major), and support for both dense and sparse matrices,
    both of which are crucial for performance.
  - This is implemented in Julia, as a seed for efficient computation pipelines (which are hard to implement in Python
    without resorting to using C/C++ code). TODO: provide a `daf <https://pypi.org/project/daf/>`_ Python package, which
    is a thin wrapper around `Daf.jl` allowing efficient (zero-copy) access to the data using `numpy`, `scipy` and
    `pandas` vector and matrix types. TODO: Implement a similar R package using
    [https://libraries.io/cran/JuliaCall](https://libraries.io/cran/JuliaCall) to allow direct access to `Daf.jl` from R
    code.

See the [documentation](https://daf.jl.readthedocs.io/en/latest/) for details.

## Motivation

The `Daf` package was created to overcome the limitations of the `AnnData` package. Like `AnnData`, `Daf` was created to
support code analyzing single-cell RNA sequencing data ("scRNA-seq"), but should be useful for other problem domains.

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
  - We'd like to keep both the UMIs count and the normalized gene fractions (and possibly their log2 values for quick
    fold factor computations).

Sure, it is possible to use a set of `AnnData` objects, each with its own distinct set of "observations" (for cell,
clusters, and sub-clusters). We can reduce confusion about what `X` is in each data set by always using it for UMIs,
though that may not make much sense for some of the data sets. We'll also need to replicate simple per-gene data across
the data sets, and keep it in sync, or just store each such data in one of the data sets, and remember in which.

In short, we'd end up writing some problem-specific code to manage the multiple `AnnData` objects for us, which kind of
defeats the purpose of using `AnnData` in the first place. Instead, we have chosen to create `Daf`, which is a
general-purpose solution that embraces the existence of arbitrary multiple axes in the same data set, and enforces no
opaque default names, to make it easy for us store explicitly named data per-whatever-we-damn-please all in a single
place.

When it comes to storage, `Daf` makes it as easy as possible to write adapters to allow storing the data in your
favorite format; in particular, `Daf.jl` supports `AnnData` as a storage format using an `h5ad` file. Since `h5ad` is
restricted by the `AnnData` data model, we also allow directly storing `Daf.jl` data in an `h5fs` file in a more
efficient way (which we call an `h5df` file).

That said, we find that, for our use cases, the use of complex single-file formats such as `h5fs` to be sub-optimal. In
effect they function as a file system, but offer only some of its functionality. For example, you need special APIs to
list the content of the data, copy or delete just parts of it, find out which parts have been changed when, and most
implementations do not support memory-mapping the data, which causes a large performance hit for large data sets.

Therefore, as an option, `Daf` also supports a simple "files" storage format where every "annotation" is a separate file
(in a trivial format) inside a single directory. This allows for efficient memory-mapping of files, using standard file
system tools to list, copy and/or delete data, and using tools like `make` to automate incremental computations. The
main downside is that to send a data set across the network, one has to first collect it into a `tar` or `zip` archive.
This may actually end up being faster as this allows compressing the data for more efficient transmission or archiving.
Besides, due to the limitations of `AnnData`, one has to send multiple files for a complete data set anyway.

Finally, `Daf` also provides a simple in-memory storage format, which is a very lightweight container (similar to an
in-memory `AnnData` object).

It is possible to create views of `Daf` data (slicing, renaming and hiding axes and/or specific annotations), and to
copy `Daf` data from one data set to another (e.g., from a view of an in-memory data set into an `AnnData` data set for
exporting it into an `h5ad` file).

Finally, the `Daf` package also provides some convenience functionality out of the box, such as caching derived data
(different layouts of the same data, sums or other computations along axes, etc.).

It is assumed that `Daf` data will be processed in a single machine, that is, `Daf` does not try to address the issues
of a distributed cluster of servers working on a shared data set. Today's servers (as of 2023) can get very big (~100
cores and ~1TB of RAM is practical), which means that all/most data sets would fit comfortably in one machine (and
memory mapped files are a great help here). In addition, if using the "files" storage, it is possible to have different
servers access the same `Daf` directory, each computing a different independent additional annotation (e.g., one server
searching for doublets while another is searching for gene modules), and as long as only one server writes each new
"annotation", this should work fine (one can do even better by writing more complex code). This is another example of
how simple files make it easy to provide functionality which is very difficult to achieve using a complex single-file
format such as `h5fs`.

The bottom line is that `Daf` provides a convenient abstraction layer above any "reasonable" storage format, allowing
efficient computation and/or visualization code to naturally access and/or write the data it needs, even for
higher-level analysis pipeline, for small to "very large" (but not for "ludicrously large") data sets.

## Installation

Just `Pkg.add("Daf")`, like installing any other Julia package.

TODO: To install the Python wrappers...

TODO: To install the R wrappers...

## License (MIT)

Copyright © 2023 Weizmann Institute of Science

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
