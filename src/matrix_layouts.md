# Matrix layouts

```@docs
DataAxesFormats.MatrixLayouts
```

## Symbolic names for axes

```@docs
DataAxesFormats.MatrixLayouts.Rows
DataAxesFormats.MatrixLayouts.Columns
DataAxesFormats.MatrixLayouts.axis_name
```

## Checking layout

```@docs
DataAxesFormats.MatrixLayouts.major_axis
DataAxesFormats.MatrixLayouts.require_major_axis
DataAxesFormats.MatrixLayouts.minor_axis
DataAxesFormats.MatrixLayouts.require_minor_axis
DataAxesFormats.MatrixLayouts.other_axis
```

## Changing layout

```@docs
DataAxesFormats.MatrixLayouts.relayout!
DataAxesFormats.MatrixLayouts.relayout
DataAxesFormats.MatrixLayouts.transposer
DataAxesFormats.MatrixLayouts.copy_array
```

## Changing format

```@docs
DataAxesFormats.MatrixLayouts.bestify
DataAxesFormats.MatrixLayouts.densify
DataAxesFormats.MatrixLayouts.sparsify
```

## Assertions

```@docs
DataAxesFormats.MatrixLayouts.@assert_vector
DataAxesFormats.MatrixLayouts.@assert_matrix
DataAxesFormats.MatrixLayouts.check_efficient_action
DataAxesFormats.MatrixLayouts.inefficient_action_handler
```

## Index

```@index
Pages = ["matrix_layouts.md"]
```
