# Utilities

```@docs
Daf.Utilities
```

## Matrices

Matrices typically have a well-defined layout which dictates which types of processing is (much!)
more efficient. In general, we re-layout matrices to the efficient layout before processing. This
allows us to optimize the re-layout computation, and the following computational steps become much
faster due to using the more efficient matrix layout.

```@docs
Daf.Utilities.MatrixAxis
Daf.Utilities.major_axis
Daf.Utilities.minor_axis
Daf.Utilities.other_axis
```

When performing computations on a matrix, it is important to, as much as possible, use the correct
layout and iterate on the matrix accordingly. The following allows us to detect and report code
where this isn't the case:

```@docs
Daf.Utilities.InefficientPolicy
Daf.Utilities.inefficient_policy
Daf.Utilities.inefficient_action
```

To ensure we are looping on the proper memory layout, it is sometimes necessary to re-layout a
matrix:

```@docs
Daf.Utilities.relayout
```

We also use the symbolic axis names in other utility functions which make operations more readable,
for example `nrows(matrix)` is clearer than `matrix.size(1)`,`count_nnz(matrix, per=Row)` is clearer
than `count_nnz(matrix, axis=2)` and `view_column(matrix, 2)` or even `view_axis(matrix, Column, 2)`
are clearer than `selectdim(matrix, 2, 2)`.

```@docs
Daf.Utilities.nrows
Daf.Utilities.ncolumns
Daf.Utilities.naxis
Daf.Utilities.count_nnz
Daf.Utilities.view_axis
Daf.Utilities.view_column
Daf.Utilities.view_row
```

## Messages

When generating error and log messages, it is useful to have unique human-readable names for objects.

```@docs
Daf.Utilities.unique_name
```

When generating error and log messages, presenting values to the user often requires a different
approach than the simple `"$(value)"` string interpolation.

```@docs
Daf.Utilities.present
Daf.Utilities.present_percent
```

## Index

```@index
Pages = ["utilities.md"]
```
