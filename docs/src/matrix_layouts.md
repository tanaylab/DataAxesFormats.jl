# Matrix Layouts

```@docs
Daf.MatrixLayouts
```

```@docs
Daf.MatrixLayouts.MatrixAxis
Daf.MatrixLayouts.major_axis
Daf.MatrixLayouts.minor_axis
Daf.MatrixLayouts.other_axis
```

When performing computations on a matrix, it is important to, as much as possible, use the correct
layout and iterate on the matrix accordingly. The following allows us to detect and report code
where this isn't the case:

```@docs
Daf.MatrixLayouts.InefficientPolicy
Daf.MatrixLayouts.inefficient_policy
Daf.MatrixLayouts.inefficient_action
```

To ensure we are looping on the proper memory layout, it is sometimes necessary to re-layout a
matrix:

```@docs
Daf.MatrixLayouts.relayout
```

We also use the symbolic axis names in other utility functions which make operations more readable,
for example `nrows(matrix)` is clearer than `matrix.size(1)`,`count_nnz(matrix, per=Row)` is clearer
than `count_nnz(matrix, axis=2)` and `view_column(matrix, 2)` or even `view_axis(matrix, Column, 2)`
are clearer than `selectdim(matrix, 2, 2)`.

```@docs
Daf.MatrixLayouts.nrows
Daf.MatrixLayouts.ncolumns
Daf.MatrixLayouts.naxis
Daf.MatrixLayouts.count_nnz
Daf.MatrixLayouts.view_axis
Daf.MatrixLayouts.view_column
Daf.MatrixLayouts.view_row
```

## Index

```@index
Pages = ["matrix_layouts.md"]
```
