# Matrix layouts

```@docs
DafJL.MatrixLayouts
```

## Symbolic names for axes

```@docs
DafJL.MatrixLayouts.Rows
DafJL.MatrixLayouts.Columns
DafJL.MatrixLayouts.axis_name
```

## Checking layout

```@docs
DafJL.MatrixLayouts.major_axis
DafJL.MatrixLayouts.require_major_axis
DafJL.MatrixLayouts.minor_axis
DafJL.MatrixLayouts.require_minor_axis
DafJL.MatrixLayouts.other_axis
```

## Changing layout

```@docs
DafJL.MatrixLayouts.relayout!
DafJL.MatrixLayouts.relayout
DafJL.MatrixLayouts.transposer
DafJL.MatrixLayouts.copy_array
```

## Changing format

```@docs
DafJL.MatrixLayouts.bestify
DafJL.MatrixLayouts.densify
DafJL.MatrixLayouts.sparsify
```

## Assertions

```@docs
DafJL.MatrixLayouts.@assert_vector
DafJL.MatrixLayouts.@assert_matrix
DafJL.MatrixLayouts.check_efficient_action
DafJL.MatrixLayouts.inefficient_action_handler
```

## Index

```@index
Pages = ["matrix_layouts.md"]
```
