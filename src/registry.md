# Operations registry

```@docs
DataAxesFormats.Registry
```

## Element-wise operations

```@docs
DataAxesFormats.Registry.EltwiseOperation
DataAxesFormats.Registry.compute_eltwise
```

## Reduction operations

```@docs
DataAxesFormats.Registry.ReductionOperation
DataAxesFormats.Registry.compute_reduction
DataAxesFormats.Registry.reduction_result_type
DataAxesFormats.Registry.supports_strings
DataAxesFormats.Registry.supports_grouped_reduction
DataAxesFormats.Registry.empty_reduction_state
DataAxesFormats.Registry.update_reduction_state
DataAxesFormats.Registry.merge_reduction_states
DataAxesFormats.Registry.final_reduction_value
DataAxesFormats.Registry.supports_buffer_reduction
DataAxesFormats.Registry.buffer_reduction_eltype
DataAxesFormats.Registry.reduce_buffer!
```

## Registering operations

```@docs
DataAxesFormats.Registry.QueryOperation
DataAxesFormats.Registry.@query_operation
DataAxesFormats.Registry.register_query_operation
```

## Index

```@index
Pages = ["registry.md"]
```
