# Operator precedence parser

```@docs
Daf.Oprec
```

## Encoding / decoding

```@docs
Daf.Oprec.encode_expression
Daf.Oprec.decode_expression
```

## Syntax

```@docs
Daf.Oprec.Syntax
Daf.Oprec.Operator
Daf.Oprec.Associativity
```

## Expression tree

```@docs
Daf.Oprec.Token
Daf.Oprec.Operation
Daf.Oprec.Expression
Daf.Oprec.build_encoded_expression
```

## Context and errors

```@docs
Daf.Oprec.Context
Daf.Oprec.error_in_context
```

## Parsing trees

```@docs
Daf.Oprec.parse_in_context
Daf.Oprec.parse_string_in_context
Daf.Oprec.parse_operand_in_context
Daf.Oprec.parse_operation_in_context
Daf.Oprec.parse_list_in_context
Daf.Oprec.parse_with_list_in_context
Daf.Oprec.check_operation
```

## Index

```@index
Pages = ["oprec.md"]
```
