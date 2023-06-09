# Data Types

```@docs
Daf.DataTypes
```

## Supported storage types

Scalars (data which is not associated with any axis):

```@docs
Daf.DataTypes.StorageScalar
```

Vectors (data which is associated with a single axis):

```@docs
Daf.DataTypes.StorageVector
Daf.DataTypes.require_storage_vector
```

Matrices (data which is associated with a pair of axes):

```@docs
Daf.DataTypes.StorageMatrix
Daf.DataTypes.is_storage_matrix
Daf.DataTypes.require_storage_matrix
```

## Symbolic names for axes

```@docs
Daf.DataTypes.Rows
Daf.DataTypes.Columns
Daf.DataTypes.axis_name
```

## Matrix layout

```@docs
Daf.DataTypes.major_axis
Daf.DataTypes.minor_axis
Daf.DataTypes.other_axis
Daf.DataTypes.relayout!
```

## Ensuring code efficiency

```@docs
Daf.DataTypes.InefficientActionPolicy
Daf.DataTypes.inefficient_action_policy
Daf.DataTypes.verify_efficient_action
```

## Index

```@index
Pages = ["data_types.md"]
```
