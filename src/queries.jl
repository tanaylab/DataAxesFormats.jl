"""
Extract data from a [`DafReader`](@ref).
"""
module Queries

export @q_str
export And
export AndNot
export AsAxis
export Axis
export CountBy
export Fetch
export FrameColumn
export FrameColumns
export GroupBy
export IfMissing
export IfNot
export IsEqual
export IsGreater
export IsGreaterEqual
export IsLess
export IsLessEqual
export IsMatch
export IsNotEqual
export IsNotMatch
export Lookup
export Names
export Or
export OrNot
export Query
export QuerySequence
export QueryString
export Xor
export XorNot
export full_vector_query
export get_frame
export get_query
export is_axis_query
export query_axis_name
export query_result_dimensions
export query_requires_relayout

using ..Readers
using ..Formats
using ..GenericFunctions
using ..GenericTypes
using ..MatrixLayouts
using ..Messages
using ..Operations
using ..ReadOnly
using ..Registry
using ..StorageTypes
using ..Tokens
using Base.Threads
using DataFrames
using NamedArrays

import ..Formats.CacheEntry
import ..Formats.CacheKey
import ..Formats.CachedQuery
import ..Readers.require_axis
import ..Readers.require_matrix
import ..Readers.require_scalar
import ..Readers.require_vector
import ..Registry.ComputationOperation
import ..Registry.ELTWISE_REGISTERED_OPERATIONS
import ..Registry.QueryOperation
import ..Registry.REDUCTION_REGISTERED_OPERATIONS
import ..Registry.reduction_result_type
import ..Registry.RegisteredOperation
import ..Tokens.error_at_token
import ..Tokens.Token
import ..Tokens.tokenize
import Base.MathConstants.e
import Base.MathConstants.pi

"""
    Query(
        query::QueryString,
        operand_only::Maybe{Type{QueryOperation}} = nothing,
    ) <: QueryOperation

A query is a description of a (sub-)process for extracting some data from a [`DafReader`](@ref). A full query is a
sequence of [`QueryOperation`](@ref), that when applied one at a time on some [`DafReader`](@ref), result in a scalar,
vector or matrix result.

To apply a query, invoke [`get_query`](@ref) to apply a query to some [`DafReader`](@ref) data (you can also use the
shorthand ``daf[query]`` instead of ``get_query(daf, query)``). By default, query operations will cache their results in
memory as [`QueryData`](@ref CacheGroup), to speed up repeated queries. This may lock up large amounts of memory; you can
[`empty_cache!`](@ref) to release it.

Queries can be constructed in two ways. In code, a query can be built by chaining query operations (e.g., the expression
`Axis("gene") |> Lookup("is_marker")` looks up the `is_marker` vector property of the `gene` axis).

Alternatively, a query can be parsed from a string, which needs to be parsed into a [`Query`](@ref) object (e.g., the
above can be written as `Query("/gene:is_marker")`). See the [`QUERY_OPERATORS`](@ref) for a table of supported
operators. Spaces (and comments) around the operators are optional; see [`tokenize`](@ref) for details. You can also
convert a [`Query`](@ref) to a `string` (or `print` it, etc.) to see its representation. This is used for `error`
messages and as a key when caching query results.

Since query strings use `\\` as an escape character, it is easier to use `raw` string literals for queries (e.g.,
`Query(raw"cell = ATGC\\:B1 : age")` vs. `Query("cell = ATGC\\\\:B1 : age")`). To make this even easier we provide the
[`q`](@ref @q_str) macro (e.g., `q"cell = ATGC\\:B1 : batch"`) which works similarly to Julia's standard `r` macro for
literal `Regex` strings.

If the provided query string contains only an operand, and `operand_only` is specified, it is used as the operator
(i.e., `Query("metacell")` is an error, but `Query("metacell", Axis)` is the same as `Axis("metacell")`). This is
useful when providing suffix queries (e.g., for [`get_frame`](@ref)).

Being able to represent queries as strings allows for reading them from configuration files and letting the user input
them in an application UI (e.g., allowing the user to specify the X, Y and/or colors of a scatter plot using queries).
At the same time, being able to incrementally build queries using code allows for convenient reuse (e.g., reusing axis
sub-queries in `Daf` views), without having to go through the string representation.

`Daf` provides a comprehensive set of [`QueryOperation`](@ref)s that can be used to construct queries. The
[`QUERY_OPERATORS`](@ref) listed below provide the basic functionality (e.g., specifying an [`Axis`](@ref) or a property
[`Lookup`](@ref)). In addition, `Daf` provides computation operations ([`EltwiseOperation`](@ref) and
[`ReductionOperation`](@ref)), allowing for additional operations to be provided by external packages.

Obviously not all possible combinations of operations make sense (e.g., `Lookup("is_marker") |> Axis("cell")` will not
work). For the full list of valid combinations, see [`NAMES_QUERY`](@ref), [`SCALAR_QUERY`](@ref),
[`VECTOR_QUERY`](@ref) and [`MATRIX_QUERY`](@ref) below.
"""
abstract type Query <: QueryOperation end

"""
Most operations that take a query allow passing a string to be parsed into a query, or an actual [`Query`](@ref) object.
This type is used as a convenient notation for such query parameters.
"""
QueryString = Union{Query, AbstractString}

"""
`NAMES_QUERY` :=
( [`Names`](@ref) `scalars`
| [`Names`](@ref) `axes`
| [`Axis`](@ref) [`Names`](@ref)
| [`Axis`](@ref) [`Axis`](@ref) [`Names`](@ref)
)

A query returning a set of names:

  - Looking up the set of names of the scalar properties (`? scalars`).
  - Looking up the set of names of the axes (`? axes`).
  - Looking up the set of names of the vector properties of an axis (e.g., `/ cell ?`).
  - Looking up the set of names of the matrix properties of a pair of axes (e.g., `/ cell / gene ?`).
"""
NAMES_QUERY = nothing

"""
`SCALAR_QUERY` :=
( `LOOKUP_PROPERTY`](@ref)
| [`VECTOR_ENTRY`](@ref)
| [`MATRIX_ENTRY`](@ref)
| [`REDUCE_VECTOR`](@ref)
) [`EltwiseOperation`](@ref)*

A query returning a scalar can be one of:

  - Looking up the value of a scalar property (e.g., `: version` will return the value of the version scalar property).
  - Picking a single entry of a vector property (e.g., `/ gene = FOX1 : is_marker` will return whether the gene named
    FOX1 is a marker gene).
  - Picking a single entry of a matrix property (e.g., `/ gene = FOX1 / cell = ATGC : UMIs` will return the number of
    UMIs of the FOX1 gene of the ATGC cell).
  - Reducing some vector into a single value (e.g., `/ donor : age %> Mean` will compute the mean age of all the
    donors).

Either way, this can be followed by a series of [`EltwiseOperation`](@ref) to modify the scalar result (e.g.,
`/ donor : age %> Mean % Log base 2 % Abs` will compute the absolute value of the log base 2 of the mean age of all the
donors).
"""
SCALAR_QUERY = nothing

"""
`LOOKUP_PROPERTY` := [`Lookup`](@ref) [`IfMissing`](@ref)?

Lookup the value of a scalar or matrix property. This is used on its own to access a scalar property (e.g., `: version`)
or combined with two axes to access a matrix property (e.g., `/ cell / gene : UMIs`).

By default, it is an error if the property does not exist. However, if an [`IfMissing`](@ref) is provided, then this
value is used instead (e.g., `: version || Unknown` will return a `Unknown` if there is no `version` scalar property,
and `/ cell / gene : UMIs || 0` will return an all-zero matrix if there is no `UMIs` matrix property).

Accessing a [`VECTOR_PROPERTY`](@ref) allows for more complex operations.
"""
LOOKUP_PROPERTY = nothing

"""
`VECTOR_ENTRY` := [`Axis`](@ref) [`IsEqual`](@ref) [`VECTOR_LOOKUP`](@ref)

Lookup the scalar value of some entry of a vector property of some axis (e.g., `/ gene = FOX1 : is_marker` will return
whether the FOX1 gene is a marker gene).
"""
VECTOR_ENTRY = nothing

"""
`MATRIX_ENTRY` := [`Axis`](@ref) [`IsEqual`](@ref) [`Axis`](@ref) [`IsEqual`](@ref) [`LOOKUP_PROPERTY`](@ref)

Lookup the scalar value of the named entry of a matrix property (e.g., `/ gene = FOX1 / cell = ATGC : UMIs` will return
the number of UMIs of the FOX1 gene of the ATGC cell).
"""
MATRIX_ENTRY = nothing

"""
REDUCE_VECTOR := [`VECTOR_QUERY`](@ref) [`ReductionOperation`](@ref) [`IfMissing`](@ref)?

Perform an arbitrary vector query, and reduce the result into a single scalar value (e.g., `/ donor : age %> Mean` will
compute the mean age of the ages of the donors).

By default, it is an error if the vector query results in an empty vector. However, if an [`IfMissing`](@ref) suffix is
provided, then this value is used instead (e.g., `/ cell & type = LMPP : age %> Mean || 0` will return zero if there are
no cells whose type is LMPP).
"""
REDUCE_VECTOR = nothing

"""
`VECTOR_QUERY` :=
( [`VECTOR_PROPERTY`](@ref)
| [`MATRIX_ROW`](@ref)
| [`MATRIX_COLUMN`](@ref)
| [`REDUCE_MATRIX`](@ref)
) [`POST_PROCESS`](@ref)*

A query returning a vector can be one of:

  - Looking up the value of a vector property (e.g., `/ gene : is_marker` will return a mask of the marker genes).
  - Picking a single row or column of a matrix property (e.g., `/ gene = FOX1 / cell : UMIs` will return a vector of the
    UMIs of the FOX1 gene of all the cells).
  - Reducing each column of some matrix into a scalar, resulting in a vector (e.g., `/ gene / cell : UMIs %> Sum` will
    compute the sum of the UMIs of all the genes in each cell).

Either way, this can be followed by further processing of the vector (e.g., `/ gene / cell : UMIs % Log base 2 eps 1`
will compute the log base 2 of one plus the of the UMIs of each gene in each cell).
"""
VECTOR_QUERY = nothing

"""
`VECTOR_PROPERTY` := [`Axis`](@ref) [`AXIS_MASK`](@ref)* [`VECTOR_LOOKUP`](@ref) [`VECTOR_FETCH`](@ref)*

Lookup the values of some vector property (e.g., `/ gene : is_marker` will return a mask of the marker genes). This can
be restricted to a subset of the vector using masks (e.g., `/ gene & is_marker : is_noisy` will return a mask of the
noisy genes out of the marker genes), and/or fetch the property value from indirect axes (e.g.,
`/ cell : batch => donor => age` will return the age of the donor of the batch of each cell).
"""
VECTOR_PROPERTY = nothing

"""
`VECTOR_LOOKUP` := [`Lookup`](@ref) [`IfMissing`](@ref)? ( [`IfNot`](@ref) | [`AsAxis`](@ref) )?

A [`Lookup`](@ref) of a vector property (e.g., `/ cell : type` will return the type of each cell).

By default, it is an error if the property does not exist. However, if an [`IfMissing`](@ref) is provided,
then this value is used instead (e.g., `/ cell : type || Unknown` will return a vector of `Unknown` types if
there is no `type` property for the `cell` axis).

If the [`IfNot`](@ref) suffix is provided, it controls how to modify "false-ish" (empty string, zero numeric value, or
false Boolean value) entries (e.g., `/ cell : type ?` will return a vector of the type of each cell that has a non-empty
type, while `/ cell : type ? Outlier` will return a vector of the type of each cell, where cells with an empty type are
given  the type `Outlier`).

Only when the vector property is used for [`CountBy`](@ref) or for [`GroupBy`](@ref), providing the [`AsAxis`](@ref)
suffix indicates that the property is associated with an axis (similar to an indirect axis in [`Fetch`](@ref)), and the
set of groups is forced to be the values of that axis; in this case, empty string values are always ignored (e.g.,
`/ cell : age @ type ! %> Mean || 0` will return a vector of the mean age of the cells of each type, with a value of
zero for types which have no cells, and ignoring cells which have an empty type; similarly,
`/ cell : batch => donor ! * type !` will return a matrix whose rows are donors and columns are types, counting the
number of cells of each type that were sampled from each donor, ignoring cells which have an empty type or whose batch
has an empty donor).
"""
VECTOR_LOOKUP = nothing

"""
`MATRIX_ROW` := [`Axis`](@ref) [`IsEqual`](@ref) [`Axis`](@ref) [`AXIS_MASK`](@ref)* [`Lookup`](@ref)

Lookup the values of a single row of a matrix property, eliminating the rows axis (e.g., `/ gene = FOX1 / cell : UMIs`
will evaluate to a vector of the UMIs of the FOX1 gene of all the cells).
"""
MATRIX_ROW = nothing

"""
`MATRIX_COLUMN` := [`Axis`](@ref) [`AXIS_MASK`](@ref)* [`Axis`](@ref) [`IsEqual`](@ref) [`Lookup`](@ref)

Lookup the values of a single column of a matrix property, eliminating the columns axis (e.g.,
`/ gene / cell = ATGC : UMIs` will evaluate to a vector of the UMIs of all the genes of the ATGC cell).
"""
MATRIX_COLUMN = nothing

"""
`REDUCE_MATRIX` := [`MATRIX_QUERY`](@ref) [`ReductionOperation`](@ref)

Perform an arbitrary matrix query, and reduce the result into a vector by converting each column into a single value,
eliminating the rows axis (e.g., `/ gene / cell : UMIs %> Sum` will evaluate to a vector of the total UMIs of each
cell).
"""
REDUCE_MATRIX = nothing

"""
`MATRIX_QUERY` := ( [`MATRIX_LOOKUP`](@ref) | [`COUNTS_MATRIX`](@ref) ) [`POST_PROCESS`](@ref)*

A query returning a matrix can be one of:

  - Looking up the value of a matrix property (e.g., `/ gene / cell : UMIs` will return the matrix of UMIs for each gene
    and cell).
  - Counting the number of times each combination of two vector properties occurs in the data (e.g.,
    `/ cell : batch => donor => age * type` will return a matrix whose rows are ages and columns are types,
    where each entry contains the number of cells which have the specific type and age).

Either way, this can be followed by a series of [`EltwiseOperation`](@ref) to modify the results (e.g.,
`/ gene / cell : UMIs % Log base 2 eps 1` will compute the log base 2 of 1 plus the UMIs of each gene in each cell).
"""
MATRIX_QUERY = nothing

"""
`MATRIX_LOOKUP` := [`Axis`](@ref) [`AXIS_MASK`](@ref)* [`Axis`](@ref) [`AXIS_MASK`](@ref)* [`Lookup`](@ref)

Lookup the values of some matrix property (e.g., `/ gene / cell : UMIs` will return the matrix of UMIs of each gene in
each cell). This can be restricted to a subset of the vector using masks (e.g.,
`/ gene & is_marker / cell & type = LMPP : UMIs` will return a matrix of the UMIs of each marker gene in cells whose
type is LMPP).
"""
MATRIX_LOOKUP = nothing

"""
`COUNTS_MATRIX` := [`VECTOR_QUERY`](@ref) [`CountBy`](@ref) [`VECTOR_FETCH`](@ref)*

Compute a matrix of counts of each combination of values given two vectors (e.g.,
`/ cell : batch => donor => age * batch => donor => sex` will return a matrix whose rows are ages and columns are sexes,
where each entry contains the number of cells which have the specific age and sex).
"""
COUNTS_MATRIX = nothing

"""
`POST_PROCESS` := [`EltwiseOperation`](@ref) | [`GROUP_BY`](@ref)

A vector or a matrix result may be processed by one of:

  - Applying an [`EltwiseOperation`](@ref) operation to each value (e.g., `/ donor : age % Log base 2` will compute the
    log base 2 of the ages of all donors, and `/ gene / cell : UMIs % Log base 2 eps 1` will compute the log base 2 of 1
    plus the UMIs count of each gene in each cell).
  - Reducing each group of vector entries or matrix rows into a single value (e.g.,
    `/ cell : batch => donor => age @ type %> Mean` will compute a vector of the mean age of the cells of each type,
    and `/ cell / gene : UMIs @ type %> Mean` will compute a matrix of the mean UMIs of each gene for the cells of each
    type).
"""
POST_PROCESS = nothing

"""
`GROUP_BY` := [`GroupBy`](@ref) [`VECTOR_FETCH`](@ref)* [`ReductionOperation`](@ref) [`IfMissing`](@ref)

The entries of a vector or the rows of a matrix result may be grouped, where all the values that have the same group
value are reduced to a single value using a [`ReductionOperation`](@ref) (e.g.,
`/ cell : batch => donor => age @ type %> Mean` will compute the mean age of all the cells of each type,
and `/ cell / gene : UMIs @ type %> Mean` will compute a matrix of the mean UMIs of each gene for the cells of each
type).

If the group property is suffixed by [`AsAxis`](@ref), then the result will have a value for each entry of the axis
(e.g., `/ cell : age @ type ! %> Mean` will compute the mean age of the cells of each type). In this case, some groups
may have no values at all, which by default, is an error. Providing an [`IfMissing`](@ref) suffix will use the specified
value for such empty groups instead (e.g., `/ cell : age @ type ! %> Mean || 0` will compute the mean age for the cells
of each type, with a zero value for types for which there are no cells).
"""
GROUP_BY = nothing

"""
`AXIS_MASK` := [`MASK_OPERATION`](@ref) ( [`VECTOR_FETCH`](@ref) )* ( [`ComparisonOperation`](@ref) )?

Restrict the set of entries of an axis to lookup results for (e.g., `/ gene & is_marker`). If the mask is based on a
non-`Bool` property, it is converted to a Boolean by comparing with the empty string or a zero value (depending on its
data type); alternatively, you can explicitly compare it with a value (e.g.,
`/ cell & batch => donor => age > 1`).
"""
AXIS_MASK = nothing

"""
`MASK_OPERATION` := [`And`](@ref) | [`AndNot`](@ref) | [`Or`](@ref) | [`OrNot`](@ref) | [`Xor`](@ref) | [`XorNot`](@ref)

A query operation for restricting the set of entries of an [`Axis`](@ref). The mask operations are applied to the
current mask, so if several operations are applied, they are applied in order from left to right (e.g.,
`/ gene & is_marker | is_noisy &! is_lateral` will first restrict the set of genes to marker genes, then expand it to
include noisy genes as well, then remove all the lateral genes; this would be different from
`/ gene & is_marker &! is_lateral | is_noisy`, which will include all noisy genes even if they are lateral).
"""
MASK_OPERATION = nothing

"""
`VECTOR_FETCH` := [`AsAxis`](@ref)? [`Fetch`](@ref) [`IfMissing`](@ref)? ( [`IfNot`](@ref) | [`AsAxis`](@ref) )?

Fetch the value of a property of an indirect axis. That is, there is a common pattern where one axis (e.g., cell) has a
property (e.g., type) which has the same name as an axis, and whose values are (string) entry names of that axis.
In this case, we often want to lookup a property of the other axis (e.g., `/ cell : type => color` will evaluate to a
vector of the color of the type of each cell). Sometimes one walks a chain of such properties (e.g.,
`/ cell : batch => donor => age`).

Sometimes it is needed to store several alternate properties that refer to the same indirect axis. In this case, the
name of the property can begin with the axis name, followed by `.` and a suffix (e.g., `/ cell : type.manual => color`
will fetch the color of the manual type of each cell, still using the type axis).

If the property does not follow this convention, it is possible to manually specify the name of the axis using an
[`AsAxis`](@ref) prefix (e.g., `/ cell : manual ! type => color` will assume the value of the `manual` property
is a vector of names of entries of the `type` axis).

As usual, if the property does not exist, this is an error, unless an [`IfMissing`](@ref) suffix is provided
(e.g., `/ cell : type || red => color` will assign all cells the color `red` if the `type` property does not exist).

If the value of the property is the empty string for some vector entries, by default this is again an error (as the
empty string is not one of the values of the indirect axis). If an [`IfNot`](@ref) suffix is provided, such entries can
be removed from the result (e.g., `/ cell : type ? => color` will return a vector of the colors of the cells which
have a non-empty type), or can be given an specific value (e.g., `/ cell : type ? red => color` will return a vector
of a color for each cell, giving the `red` color to cells with an empty type).

When using [`IfMissing`](@ref) and/or [`IfNot`](@ref), the default value provided is always of the final value (e.g.,
`/ cell : batch || -1 ? -2 => donor || -3 ? -4 => age || -5 ? -6` will compute a vector if age per cell; if there's no
`batch` property, all cells will get the age `-1`). If there is such property, then cells with an empty batch will get
the age `-2`. For cells with a non-empty batch, if there's no `donor` property, they will get the value `-3`. If there
is such a property, cells with an empty donor will get the value `-4`. Finally, for cells with a batch and donor, if
there is no `age` property, they will be given an age of `-5`. Otherwise, if their age is zero, it will be changed to
`-6`.
"""
VECTOR_FETCH = nothing

"""
Operators used to represent a [`Query`](@ref) as a string.

| Operator | Implementation               | Description                                                                              |
|:-------- |:----------------------------:|:---------------------------------------------------------------------------------------- |
| `/`      | [`Axis`](@ref)               | Specify a vector or matrix axis (e.g., `/ cell : batch` or `/ cell / gene : UMIs`).      |
| `?`      | [`Names`](@ref)              | 1. Names of scalars or axes (`? axes`, `? scalars`).                                     |
|          |                              | 2. Names of vectors of axis (e.g., `/ cell ?`).                                          |
|          |                              | 3. Names of matrices of axes (e.g., `/ cell / gene ?`).                                  |
| `:`      | [`Lookup`](@ref)             | Lookup a property (e.g., `: version`, `/ cell : batch` or `/ cell / gene : UMIs`).       |
| `=>`     | [`Fetch`](@ref)              | Fetch a property from another axis (e.g., `/ cell : batch => age`).                      |
| `!`      | [`AsAxis`](@ref)             | 1. Specify axis name when fetching a property (e.g., `/ cell : manual ! type => color`). |
|          |                              | 2. Force all axis values when counting (e.g., `/ cell : batch ! * manual ! type`).       |
|          |                              | 3. Force all axis values when grouping (e.g., `/ cell : age @ batch ! %> Mean`).         |
| `??`     | [`IfNot`](@ref)              | 1. Mask excluding false-ish values (e.g., `/ cell : batch ?? => age`).                   |
|          |                              | 2. Default for false-ish lookup values (e.g., `/ cell : type ?? Outlier`).               |
|          |                              | 3. Default for false-ish fetched values (e.g., `/ cell : batch ?? 1 => age`).            |
| `││`     | [`IfMissing`](@ref)          | 1. Value for missing lookup properties (e.g., `/ gene : is_marker ││ false`).            |
|          |                              | 2. Value for missing fetched properties (e.g., `/ cell : type || red => color`).         |
|          |                              | 3. Value for empty reduced vectors (e.g., `/ cell : type = LMPP => age %> Max || 0`).    |
| `%`      | [`EltwiseOperation`](@ref)   | Apply an element-wise operation (e.g., `/ cell / gene : UMIs % Log base 2 eps 1`).       |
| `%>`     | [`ReductionOperation`](@ref) | Apply a reduction operation (e.g., `/ cell / gene : UMIs %> Sum`).                       |
| `*`      | [`CountBy`](@ref)            | Compute counts matrix (e.g., `/ cell : age * type`).                                     |
| `@`      | [`GroupBy`](@ref)            | 1. Aggregate vector entries by a group (e.g., `/ cell : age @ type %> Mean`).            |
|          |                              | 2. Aggregate matrix row entries by a group (e.g.,`/ cell / gene : UMIs @ type %> Max`).  |
| `&`      | [`And`](@ref)                | Restrict axis entries (e.g., `/ gene & is_marker`).                                      |
| `&!`     | [`AndNot`](@ref)             | Restrict axis entries (e.g., `/ gene &! is_marker`).                                     |
| `│`      | [`Or`](@ref)                 | Expand axis entries (e.g., `/ gene & is_marker │ is_noisy`).                             |
| `│!`     | [`OrNot`](@ref)              | Expand axis entries (e.g., `/ gene & is_marker │! is_noisy`).                            |
| `^`      | [`Xor`](@ref)                | Flip axis entries (e.g., `/ gene & is_marker ^ is_noisy`).                               |
| `^!`     | [`XorNot`](@ref)             | Flip axis entries (e.g., `/ gene & is_marker ^! is_noisy`).                              |
| `=`      | [`IsEqual`](@ref)            | 1. Select an entry from an axis (e.g., `/ cell / gene = FOX1 : UMIs`).                   |
|          |                              | 2. Compare equal (e.g., `/ cell & age = 1`).                                             |
| `!=`     | [`IsNotEqual`](@ref)         | Compare not equal (e.g., `/ cell & age != 1`).                                           |
| `<`      | [`IsLess`](@ref)             | Compare less than (e.g., `/ cell & age < 1`).                                            |
| `<=`     | [`IsLessEqual`](@ref)        | Compare less or equal (e.g., `/ cell & age <= 1`).                                       |
| `>`      | [`IsGreater`](@ref)          | Compare greater than (e.g., `/ cell & age > 1`).                                         |
| `>=`     | [`IsGreaterEqual`](@ref)     | Compare greater or equal (e.g., `/ cell & age >= 1`).                                    |
| `~`      | [`IsMatch`](@ref)            | Compare match (e.g., `/ gene & name ~ RP\\[SL\\]`).                                      |
| `!~`     | [`IsNotMatch`](@ref)         | Compare not match (e.g., `/ gene & name !~ RP\\[SL\\]`).                                 |

!!! note

    Due to Julia's Documenter limitations, the ASCII `|` character (`&#124;`) is replaced by the Unicode `│` character
    (`&#9474;`) in the above table. Sigh.
"""
QUERY_OPERATORS = r"^(?:=>|\|\||\?\?|%>|&!|\|!|\^!|!=|<=|>=|!~|/|:|!|%|\*|@|&|\||\?|\^|=|<|>|~)"

function next_query_operation(tokens::Vector{Token}, next_token_index::Int)::Tuple{QueryOperation, Int}
    token = next_operator_token(tokens, next_token_index)
    next_token_index += 1

    for (operator, operation_type) in (
        ("/", Axis),
        (":", Lookup),
        ("=>", Fetch),
        ("*", CountBy),
        ("@", GroupBy),
        ("&", And),
        ("&!", AndNot),
        ("|", Or),
        ("|!", OrNot),
        ("^", Xor),
        ("^!", XorNot),
        ("=", IsEqual),
        ("!=", IsNotEqual),
        ("<", IsLess),
        ("<=", IsLessEqual),
        (">", IsGreater),
        (">=", IsGreaterEqual),
        ("~", IsMatch),
        ("!~", IsNotMatch),
    )
        if token.value == operator
            token = next_value_token(tokens, next_token_index)
            return (operation_type(token.value), next_token_index + 1)
        end
    end

    for (operator, operation_type) in (("??", IfNot), ("!", AsAxis), ("?", Names))
        if token.value == operator
            token = maybe_next_value_token(tokens, next_token_index)
            if token === nothing
                return (operation_type(), next_token_index)
            else
                return (operation_type(token.value), next_token_index + 1)
            end
        end
    end

    for (operator, kind, registered_operations) in
        (("%", "eltwise", ELTWISE_REGISTERED_OPERATIONS), ("%>", "reduce", REDUCTION_REGISTERED_OPERATIONS))
        if token.value == operator
            computation_operation, next_token_index =
                parse_registered_operation(tokens, next_token_index, kind, registered_operations)
            return (computation_operation, next_token_index)
        end
    end

    if token.value == "||"
        value_token = next_value_token(tokens, next_token_index)
        next_token_index += 1
        value = value_token.value

        if next_token_index <= length(tokens) && !tokens[next_token_index].is_operator
            type_token = tokens[next_token_index]
            next_token_index += 1
            if type_token.value == "String"
                dtype = String
            else
                dtype = parse_number_dtype_value(token, "dtype", type_token)
                if dtype !== nothing
                    value = parse_number_value(token, "value", value_token, dtype)
                end
            end
        else
            dtype = nothing
        end

        return (IfMissing(value; dtype = dtype), next_token_index)
    end

    return error_at_token(tokens[next_token_index - 1], "bug when parsing query"; at_end = true)  # untested
end

function next_operator_token(tokens::Vector{Token}, next_token_index::Int)::Token
    if next_token_index > length(tokens)
        error_at_token(tokens[next_token_index - 1], "expected: operator"; at_end = true)  # untested
    elseif !tokens[next_token_index].is_operator
        error_at_token(tokens[next_token_index], "expected: operator")
    end
    return tokens[next_token_index]
end

function next_value_token(tokens::Vector{Token}, next_token_index::Int)::Token
    if next_token_index > length(tokens)
        error_at_token(tokens[next_token_index - 1], "expected: value"; at_end = true)  # untested
    elseif tokens[next_token_index].is_operator
        error_at_token(tokens[next_token_index], "expected: value")
    end
    return tokens[next_token_index]
end

function maybe_next_value_token(tokens::Vector{Token}, next_token_index::Int)::Maybe{Token}
    if next_token_index <= length(tokens) && !tokens[next_token_index].is_operator
        return tokens[next_token_index]
    else
        return nothing
    end
end

function parse_registered_operation(
    tokens::Vector{Token},
    next_token_index::Int,
    kind::AbstractString,
    registered_operations::Dict{String, RegisteredOperation},
)::Tuple{QueryOperation, Int}
    operation_name = next_value_token(tokens, next_token_index)
    registered_operation = get(registered_operations, operation_name.value, nothing)
    if registered_operation === nothing
        error_at_token(operation_name, "unknown $(kind) operation: $(operation_name.value)")
    end
    next_token_index += 1
    operation_type = registered_operation.type

    parameters_values, next_token_index = parse_operation_parameters(tokens, next_token_index)
    parameters_dict = Dict{String, Token}()
    parameter_symbols = fieldnames(operation_type)
    for (name_token, value_token) in parameters_values
        if !(Symbol(name_token.value) in parameter_symbols)
            error_at_token(name_token, dedent("""
                                          the parameter: $(name_token.value)
                                          does not exist for the operation: $(operation_name.value)
                                       """))
        end
        if haskey(parameters_dict, name_token.value)
            error_at_token(name_token, dedent("""
                                           repeated parameter: $(name_token.value)
                                           for the operation: $(operation_name.value)
                                       """))
        end
        parameters_dict[name_token.value] = value_token
    end

    operation = operation_type(operation_name, parameters_dict)
    return (operation, next_token_index)
end

function parse_operation_parameters(
    tokens::Vector{Token},
    next_token_index::Int,
)::Tuple{Vector{Tuple{Token, Token}}, Int}
    parameters_values = Vector{Tuple{Token, Token}}()

    while next_token_index < length(tokens)
        if tokens[next_token_index].is_operator
            break
        end

        name_token = tokens[next_token_index]
        next_token_index += 1
        value_token = next_value_token(tokens, next_token_index)
        next_token_index += 1
        push!(parameters_values, (name_token, value_token))
    end

    return (parameters_values, next_token_index)
end

"""
    q"..."

Shorthand for parsing a literal string as a [`Query`](@ref). This is equivalent to [`Query`](@ref)`(raw"...")`, that is,
a `\\` can be placed in the string without escaping it (except for before a `"`). This is very convenient for literal
queries (e.g., `q"/ cell = ATGC\\:B1 : batch"` == `Query(raw"/ cell = ATGC\\:B1 : batch")` ==
`Query("/ cell = ATGC\\\\:B1 : batch")` == `Axis("cell") |> IsEqual("ATGC:B1") |> Lookup("batch")).
"""
macro q_str(query_string::AbstractString)
    return Query(query_string)
end

"""
    struct QuerySequence{N} <: Query where {N<:Integer}

A sequence of `N` [`QueryOperation`](@ref)s.
"""
struct QuerySequence{N} <: Query where {N}
    query_operations::NTuple{N, QueryOperation}
end

function Base.show(io::IO, query_sequence::QuerySequence)::Nothing
    if !isempty(query_sequence.query_operations)
        show(io, query_sequence.query_operations[1])
        for index in 2:length(query_sequence.query_operations)
            print(io, " ")
            show(io, query_sequence.query_operations[index])
        end
    end
end

# For avoiding Julia operators when calling Julia from another language.
function QuerySequence(
    first::Union{QuerySequence, QueryOperation},
    second::Union{QuerySequence, QueryOperation},
)::QuerySequence
    return first |> second
end

function Base.:(|>)(first_sequence::QuerySequence, second_sequence::QuerySequence)::QuerySequence
    return QuerySequence((first_sequence.query_operations..., second_sequence.query_operations...))
end

function Base.:(|>)(first_operation::QueryOperation, second_sequence::QuerySequence)::QuerySequence
    return QuerySequence((first_operation, second_sequence.query_operations...))
end

function Base.:(|>)(first_sequence::QuerySequence, second_operation::QueryOperation)::QuerySequence
    return QuerySequence((first_sequence.query_operations..., second_operation))
end

function Base.:(|>)(first_operation::QueryOperation, second_operation::QueryOperation)::QuerySequence
    return QuerySequence((first_operation, second_operation))
end

function Base.:(|>)(first::Union{QuerySequence, QueryOperation}, second::AbstractString)::QuerySequence
    return first |> Query(second)
end

function Base.:(|>)(first::AbstractString, second::Union{QuerySequence, QueryOperation})::QuerySequence
    return Query(first) |> second
end

"""
    Names(kind::Maybe{AbstractString} = nothing) <: Query

A query operation for looking up a set of names. In a string [`Query`](@ref), this is specified using the `?`
operator, optionally followed by the kind of objects to name.

  - If the query state is empty, a `kind` must be specified, one of `scalars` or `axes`, and the result is the set of
    their names (`? scalars`, `? axes`).
  - If the query state contains a single axis (without any masks), the `kind` must not be specified, and the result is
    the set of names of vector properties of the axis (e.g., `/ cell ?`).
  - If the query state contains two axes (without any masks), the `kind` must not be specified, and the result is
    the set of names of matrix properties of the axes (e.g., `/ cell / gene ?`).

!!! note

    This, [`Lookup`](@ref) and [`Axis`](@ref) are the only [`QueryOperation`](@ref)s that also works as a complete
    [`Query`](@ref).
"""
struct Names <: Query
    kind::Maybe{AbstractString}
    function Names(kind::Maybe{AbstractString} = nothing)::Names
        return new(kind)
    end
end

function get_query(daf::DafReader, names::Names; cache::Bool = true)::AbstractSet{<:AbstractString}
    return get_query(daf, QuerySequence((names,)); cache = cache)
end

function is_axis_query(names::Names)::Bool
    return is_axis_query(QuerySequence((names,)))
end

function query_result_dimensions(names::Names)::Int
    return query_result_dimensions(QuerySequence((names,)))
end

function query_requires_relayout(daf::DafReader, names::Names)::Bool
    return query_requires_relayout(daf, QuerySequence((names,)))
end

function Base.show(io::IO, names::Names)::Nothing
    kind = names.kind
    if kind === nothing
        print(io, "?")
    else
        print(io, "? $(kind)")
    end
    return nothing
end

"""
    Lookup(property::AbstractString) <: Query

A query operation for looking up the value of a property with some name. In a string [`Query`](@ref), this is specified
using the `:` operator, followed by the property name to look up.

  - If the query state is empty, this looks up the value of a scalar property (e.g., `: version`).
  - If the query state contains a single axis, this looks up the value of a vector property (e.g., `/ cell : batch`).
  - If the query state contains two axes, this looks up the value of a matrix property (e.g., `/ cell / gene : UMIs`).

If the property does not exist, this is an error, unless this is followed by [`IfMissing`](@ref) (e.g.,
`: version || 1.0`).

If any of the axes has a single entry selected using [`IsEqual`](@ref), this will reduce the dimension of the result
(e.g., `/ cell / gene = FOX1 : UMIs` is a vector, and both `/ cell = C1 / gene = FOX1 : UMI` and
`/ gene = FOX1 : is_marker` are scalars).

!!! note

    This, [`Names`](@ref) and [`Axis`](@ref) are the only [`QueryOperation`](@ref)s that also works as a complete
    [`Query`](@ref).
"""
struct Lookup <: Query
    property_name::AbstractString
end

function get_query(daf::DafReader, lookup::Lookup; cache::Bool = true)::StorageScalar
    return get_query(daf, QuerySequence((lookup,)); cache = cache)
end

function is_axis_query(lookup::Lookup)::Bool
    return is_axis_query(QuerySequence((lookup,)))
end

function query_result_dimensions(lookup::Lookup)::Int
    return query_result_dimensions(QuerySequence((lookup,)))
end

function query_requires_relayout(daf::DafReader, lookup::Lookup)::Bool
    return query_requires_relayout(daf, QuerySequence((lookup,)))
end

function Base.show(io::IO, lookup::Lookup)::Nothing
    print(io, ": $(escape_value(lookup.property_name))")
    return nothing
end

abstract type ModifierQueryOperation <: QueryOperation end

"""
    Fetch(property::AbstractString) <: QueryOperation

A query operation for fetching the value of a property from another axis, based on a vector property whose values are
entry names of the axis. In a string [`Query`](@ref), this is specified using the `=>` operator, followed by the name to
look up.

That is, if you query for the values of a vector property (e.g., `batch` for each `cell`), and the name of this property
is identical to some axis name, then we assume each value is the name of an entry of this axis. We use this to fetch the
value of some other property (e.g., `age`) of that axis (e.g., `/ cell : batch => age`).

It is useful to be able to store several vector properties which all map to the same axis. To support this, we support a
naming convention where the property name begins with the axis name followed by a `.suffix`. (e.g., both
`/ cell : type => color` and `/ cell : type.manual => color` will look up the `color` of the `type` of some property of
the `cell` axis - either "the" `type` of each `cell`, or the alternate `type.manual` of each cell).

Fetching can be chained (e.g., `/ cell : batch => donor => age` will fetch the `age` of the `donor` of the `batch` of
each `cell`).

If the property does not exist, this is an error, unless this is followed by [`IfMissing`](@ref) (e.g.,
`/ cell : type => color || red`). If the property contains an empty value, this is also an error, unless it is followed
by an [`IfNot`](@ref) (e.g., `/ cell : type ? => color` will compute a vector of the colors of the type of the cells
that have a non-empty type, and `/ cell : batch ? 0 => donor => age` will assign a zero age for cells which have an
empty batch).
"""
struct Fetch <: ModifierQueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, fetch::Fetch)::Nothing
    print(io, "=> $(escape_value(fetch.property_name))")
    return nothing
end

"""
    IfMissing(value::StorageScalar; dtype::Maybe{Type} = nothing) <: QueryOperation

A query operation providing a value to use if the data is missing some property. In a string [`Query`](@ref), this is
specified using the `||` operator, followed by the value to use, and optionally followed by the data type of the value
(e.g., `: score || 1 Float32`).

If the data type is not specified, and the `value` isa `AbstractString`, then the data type is deduced using
[`guess_typed_value`](@ref) of the `value`.
"""
struct IfMissing <: ModifierQueryOperation
    missing_value::StorageScalar
    dtype::Maybe{Type}
end

function IfMissing(value::StorageScalar; dtype::Maybe{Type} = nothing)::IfMissing
    if dtype !== nothing
        @assert value isa dtype
    elseif !(value isa AbstractString)
        dtype = typeof(value)  # untested
    end
    return IfMissing(value, dtype)
end

function Base.show(io::IO, if_missing::IfMissing)::Nothing
    print(io, "|| $(escape_value(string(if_missing.missing_value)))")
    if if_missing.dtype !== nothing
        print(io, " $(if_missing.dtype)")
    end
    return nothing
end

"""
    IfNot(value::Maybe{StorageScalar} = nothing) <: QueryOperation

A query operation providing a value to use for "false-ish" values in a vector (empty strings, zero numeric values, or
false Boolean values). In a string [`Query`](@ref), this is indicated using the `??` operator, optionally followed by a
value to use.

If the value is `nothing` (the default), then these entries are dropped (masked out) of the result (e.g.,
`/ cell : type ?` behaves the same as `/ cell & type : type`, that is, returns the type of the cells which have a
non-empty type). Otherwise, this value is used instead of the "false-ish" value (e.g., `/ cell : type ? Outlier` will
return a vector of the type of each cell, with the value `Outlier` for cells with an empty type). When fetching
properties, this is the final value (e.g., `/ cell : type ? red => color` will return a vector of the color of the type
of each cell, with a `red` color for the cells with an empty type).

If the `value` isa `AbstractString`, then it is automatically converted to the data type of the elements of the results
vector.
"""
struct IfNot <: ModifierQueryOperation
    not_value::Maybe{StorageScalar}
end

function IfNot()
    return IfNot(nothing)
end

function Base.show(io::IO, if_not::IfNot)::Nothing
    not_value = if_not.not_value
    if not_value === nothing
        print(io, "??")
    else
        print(io, "?? $(escape_value(string(not_value)))")
    end
    return nothing
end

"""
    AsAxis([axis::AbstractString = nothing]) <: QueryOperation

There are three cases where we may want to take a vector property and consider each value to be the name of an entry of
some axis: [`Fetch`](@ref), [`CountBy`](@ref) and [`GroupBy`](@ref). In a string [`Query`](@ref), this is indicated by
the `!` operators, optionally followed by the name of the axis to use.

When using [`Fetch`](@ref), we always lookup in some axis, so [`AsAxis`](@ref) is implied (e.g.,
`/ cell : type => color` is identical to `/ cell : type ! => color`). In contrast, when using [`CountBy`](@ref) and
[`GroupBy`](@ref), one has to explicitly specify `AsAxis` to force using all the entries of the axis for the counting or
grouping (e.g., `/ cell : age @ type %> Mean` will return a vector of the mean age of every type that has cells
associated with it, while `/ cell : age @ type ! %> Mean` will return a vector of the mean age of each and every value
of the type axis; similarly, `/ cell : type * age` will generate a counts matrix whose rows are types that have cells
associated with them, while `/ cell : type ! * age` will generate a counts matrix whose rows are exactly the entries of
the type axis).

Since the set of values is fixed by the axis matching the vector property, it is possible that, when using this for
[`GroupBy`](@ref), some groups would have no values, causing an error. This can be avoided by providing an
[`IfMissing`](@ref) suffix to the reduction (e.g., `/ cell : age @ type ! %> Mean` will fail if some type has no cells
associated with it, while `/ cell : age @ type ! %> Mean || 0` will give such types a zero mean age).

Typically, the name of the base property is identical to the name of the axis. In this case, there is no need to specify
the name of the axis (as in the examples above). Sometimes it is useful to be able to store several vector properties
which all map to the same axis. To support this, we support a naming convention where the property name begins with the
axis name followed by a `.suffix`. (e.g., both `/ cell : type => color` and `/ cell : type.manual => color` will look up
the `color` of the `type` of some property of the `cell` axis - either "the" `type` of each `cell`, or the alternate
`type.manual` of each cell).

If the property name does not follow the above conventions, then it is possible to explicitly specify the name of the
axis (e.g., `/ cell : manual ! type => color` will consider each value of the `manual` property as the name of an entry
of the `type` axis and look up the matching `color` property value of this axis).
"""
struct AsAxis <: QueryOperation
    axis_name::Maybe{AbstractString}
end

function AsAxis()::AsAxis
    return AsAxis(nothing)
end

function Base.show(io::IO, as_axis::AsAxis)::Nothing
    if as_axis.axis_name === nothing
        print(io, "!")
    else
        print(io, "! $(escape_value(as_axis.axis_name))")
    end
    return nothing
end

"""
    Axis(axis::AbstractString) <: QueryOperation

A query operation for specifying a result axis. In a string [`Query`](@ref), this is specified using the `/` operator
followed by the axis name.

This needs to be specified at least once for a vector query (e.g., `/ cell : batch`), and twice for a matrix (e.g.,
`/ cell / gene : UMIs`). Axes can be filtered using Boolean masks using [`And`](@ref), [`AndNot`](@ref), [`Or`](@ref),
[`OrNot`](@ref), [`Xor`](@ref) and [`XorNot`](@ref) (e.g., `/ gene & is_marker : is_noisy`). Alternatively, a single
entry can be selected from the axis using [`IsEqual`](@ref) (e.g., `/ gene = FOX1 : is_noisy`,
`/ cell / gene = FOX1 : UMIs`, `/ cell = C1 / gene = FOX1 : UMIs`). Finally, a matrix can be reduced into a vector, and
a vector to a scalar, using [`ReductionOperation`](@ref) (e.g., `/ gene / cell : UMIs %> Sum %> Mean`).

!!! note

    This, [`Names`](@ref) and [`Lookup`](@ref) are the only [`QueryOperation`](@ref)s that also works as a complete
    [`Query`](@ref).
"""
struct Axis <: Query
    axis_name::AbstractString
end

function get_query(daf::DafReader, axis::Axis; cache::Bool = true)::AbstractVector{<:AbstractString}
    return get_query(daf, QuerySequence((axis,)); cache = cache)
end

function is_axis_query(axis::Axis)::Bool
    return is_axis_query(QuerySequence((axis,)))
end

function query_result_dimensions(axis::Axis)::Int
    return query_result_dimensions(QuerySequence((axis,)))
end

function query_requires_relayout(daf::DafReader, axis::Axis)::Bool
    return query_requires_relayout(daf, QuerySequence((axis,)))
end

function Base.show(io::IO, axis::Axis)::Nothing
    print(io, "/ $(escape_value(axis.axis_name))")
    return nothing
end

abstract type MaskOperation <: QueryOperation end

function Base.show(io::IO, mask_operation::MaskOperation)::Nothing
    print(io, "$(mask_operator(mask_operation)) $(escape_value(mask_operation.property_name))")
    return nothing
end

"""
    And(property::AbstractString) <: QueryOperation

A query operation for restricting the set of entries of an [`Axis`](@ref). In a string [`Query`](@ref), this is
specified using the `&` operator, followed by the name of an axis property to look up to compute the mask.

The mask may be just the fetched property (e.g., `/ gene & is_marker` will restrict the result vector to only marker
genes). If the value of the property is not Boolean, it is automatically compared to `0` or the empty string, depending
on its type (e.g., `/ cell & type` will restrict the result vector to only cells which were given a non-empty-string
type annotation). It is also possible to fetch properties from other axes, and use an explicit
[`ComparisonOperation`](@ref) to compute the Boolean mask (e.g., `/ cell & batch => age > 1` will restrict the result
vector to cells whose batch has an age larger than 1).
"""
struct And <: MaskOperation
    property_name::AbstractString
end

function mask_operator(::And)::String
    return "&"
end

function update_axis_mask(
    axis_mask::AbstractVector{Bool},
    mask_vector::Union{AbstractVector{Bool}, BitVector},
    ::And,
)::Nothing
    axis_mask .&= mask_vector
    return nothing
end

"""
    AndNot(property::AbstractString) <: QueryOperation

Same as [`And`](@ref) but use the inverse of the mask. In a string [`Query`](@ref), this is specified using the `&!`
operator, followed by the name of an axis property to look up to compute the mask.
"""
struct AndNot <: MaskOperation
    property_name::AbstractString
end

function mask_operator(::AndNot)::String
    return "&!"
end

function update_axis_mask(
    axis_mask::AbstractVector{Bool},
    mask_vector::Union{AbstractVector{Bool}, BitVector},
    ::AndNot,
)::Nothing
    axis_mask .&= .!mask_vector
    return nothing
end

"""
    Or(property::AbstractString) <: QueryOperation

A query operation for expanding the set of entries of an [`Axis`](@ref). In a string [`Query`](@ref), this is specified
using the `|` operator, followed by the name of an axis property to look up to compute the mask.

This works similarly to [`And`](@ref), except that it adds to the mask (e.g., `/ gene & is_marker | is_noisy` will
restrict the result vector to either marker or noisy genes).
"""
struct Or <: MaskOperation
    property_name::AbstractString
end

function mask_operator(::Or)::String
    return "|"
end

function update_axis_mask(
    axis_mask::AbstractVector{Bool},
    mask_vector::Union{AbstractVector{Bool}, BitVector},
    ::Or,
)::Nothing
    axis_mask .|= mask_vector
    return nothing
end

"""
    OrNot(property::AbstractString) <: QueryOperation

Same as [`Or`](@ref) but use the inverse of the mask. In a string [`Query`](@ref), this is specified using the `|!`
operator, followed by the name of an axis property to look up to compute the mask.
"""
struct OrNot <: MaskOperation
    property_name::AbstractString
end

function mask_operator(::OrNot)::String
    return "|!"
end

function update_axis_mask(
    axis_mask::AbstractVector{Bool},
    mask_vector::Union{AbstractVector{Bool}, BitVector},
    ::OrNot,
)::Nothing
    axis_mask .|= .!mask_vector
    return nothing
end

"""
    Xor(property::AbstractString) <: QueryOperation

A query operation for flipping the set of entries of an [`Axis`](@ref). In a string [`Query`](@ref), this is specified
using the `^` operator, followed by the name of an axis property to look up to compute the mask.

This works similarly to [`Or`](@ref), except that it flips entries in the mask (e.g., `/ gene & is_marker ^ is_noisy`
will restrict the result vector to either marker or noisy genes, but not both).
"""
struct Xor <: MaskOperation
    property_name::AbstractString
end

function mask_operator(::Xor)::String
    return "^"
end

function update_axis_mask(
    axis_mask::AbstractVector{Bool},
    mask_vector::Union{AbstractVector{Bool}, BitVector},
    ::Xor,
)::Nothing
    axis_mask .= @. xor(axis_mask, mask_vector)
    return nothing
end

"""
    XorNot(property::AbstractString) <: QueryOperation

Same as [`Xor`](@ref) but use the inverse of the mask. In a string [`Query`](@ref), this is specified using the `^!`
operator, followed by the name of an axis property to look up to compute the mask.
"""
struct XorNot <: MaskOperation
    property_name::AbstractString
end

function mask_operator(::XorNot)::String
    return "^!"
end

function update_axis_mask(
    axis_mask::AbstractVector{Bool},
    mask_vector::Union{AbstractVector{Bool}, BitVector},
    ::XorNot,
)::Nothing
    axis_mask .= @. xor(axis_mask, .!mask_vector)
    return nothing
end

"""
`ComparisonOperation` :=
( [`IsLess`](@ref)
| [`IsLessEqual`](@ref)
| [`IsEqual`](@ref)
| [`IsNotEqual`](@ref)
| [`IsGreater`](@ref)
| [`IsGreaterEqual`](@ref)
| [`IsMatch`](@ref)
| [`IsNotMatch`](@ref)
)

A query operation computing a mask by comparing the values of a vector with some constant (e.g., `/ cell & age > 0`).
In addition, the [`IsEqual`](@ref) operation can be used to slice an entry from a vector (e.g.,
`/ gene = FOX1 : is_marker`) or a matrix (e.g., `/ cell / gene = FOX1 & UMIs`, `/ cell = ATGC / gene = FOX1 : UMIs`).
"""
abstract type ComparisonOperation <: ModifierQueryOperation end

function Base.show(io::IO, comparison_operation::ComparisonOperation)::Nothing
    print(
        io,
        "$(comparison_operator(comparison_operation)) $(escape_value(string(comparison_operation.comparison_value)))",
    )
    return nothing
end

"""
    IsLess(value::StorageScalar) <: QueryOperation

A query operation for converting a vector value to a Boolean mask by comparing it some value. In a string
[`Query`](@ref), this is specified using the `<` operator, followed by the value to compare with.

A string value is automatically converted into the same type as the vector values (e.g., `/ cell & probability < 0.5`
will restrict the result vector only to cells whose probability is less than half).
"""
struct IsLess <: ComparisonOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsLess)::String
    return "<"
end

function compute_comparison(compared_value::StorageScalar, ::IsLess, comparison_value::StorageScalar)::Bool
    return compared_value < comparison_value
end

"""
    IsLessEqual(value::StorageScalar) <: QueryOperation

Similar to [`IsLess`](@ref) except that uses `<=` instead of `<` for the comparison.
"""
struct IsLessEqual <: ComparisonOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsLessEqual)::String
    return "<="
end

function compute_comparison(compared_value::StorageScalar, ::IsLessEqual, comparison_value::StorageScalar)::Bool
    return compared_value <= comparison_value
end

"""
    IsEqual(value::StorageScalar) <: QueryOperation

Equality is used for two purposes:

  - As a comparison operator, similar to [`IsLess`](@ref) except that uses `=` instead of `<` for the comparison.
  - To select a single entry from a vector. This allows a query to select a single scalar from a vector (e.g.,
    `/ gene = FOX1 : is_marker`) or from a matrix (e.g., `/ cell = ATGC / gene = FOX1 : UMIs`); or to slice a single
    vector from a matrix (e.g., `/ cell = ATGC / gene : UMIs` or `/ cell / gene = FOX1 : UMIs`).
"""
struct IsEqual <: ComparisonOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsEqual)::String
    return "="
end

function compute_comparison(compared_value::StorageScalar, ::IsEqual, comparison_value::StorageScalar)::Bool
    return compared_value == comparison_value
end

"""
    IsNotEqual(value::StorageScalar) <: QueryOperation

Similar to [`IsLess`](@ref) except that uses `!=` instead of `<` for the comparison.
"""
struct IsNotEqual <: ComparisonOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsNotEqual)::String
    return "!="
end

function compute_comparison(compared_value::StorageScalar, ::IsNotEqual, comparison_value::StorageScalar)::Bool
    return compared_value != comparison_value
end

"""
    IsGreater(value::StorageScalar) <: QueryOperation

Similar to [`IsLess`](@ref) except that uses `>` instead of `<` for the comparison.
"""
struct IsGreater <: ComparisonOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsGreater)::String
    return ">"
end

function compute_comparison(compared_value::StorageScalar, ::IsGreater, comparison_value::StorageScalar)::Bool
    return compared_value > comparison_value
end

"""
    IsGreaterEqual(value::StorageScalar) <: QueryOperation

Similar to [`IsLess`](@ref) except that uses `>=` instead of `<` for the comparison.
"""
struct IsGreaterEqual <: ComparisonOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsGreaterEqual)::String
    return ">="
end

function compute_comparison(compared_value::StorageScalar, ::IsGreaterEqual, comparison_value::StorageScalar)::Bool
    return compared_value >= comparison_value
end

abstract type MatchOperation <: ComparisonOperation end

"""
    IsMatch(value::Union{AbstractString, Regex}) <: QueryOperation

Similar to [`IsLess`](@ref) except that the compared values must be strings, and the mask
is of the values that match the given regular expression.
"""
struct IsMatch <: MatchOperation
    comparison_value::Union{AbstractString, Regex}
end

function comparison_operator(::IsMatch)::String
    return "~"
end

function compute_comparison(compared_value::AbstractString, ::IsMatch, comparison_regex::Regex)::Bool
    return occursin(comparison_regex, compared_value)
end

"""
    IsNotMatch(value::Union{AbstractString, Regex}) <: QueryOperation

Similar to [`IsMatch`](@ref) except that looks for entries that do not match the pattern.
"""
struct IsNotMatch <: MatchOperation
    comparison_value::Union{AbstractString, Regex}
end

function comparison_operator(::IsNotMatch)::String
    return "!~"
end

function compute_comparison(compared_value::AbstractString, ::IsNotMatch, comparison_regex::Regex)::Bool
    return !occursin(comparison_regex, compared_value)
end

"""
    CountBy(property::AbstractString) <: QueryOperation

A query operation that generates a matrix of counts of combinations of pairs of values for the same entries of an axis.
That is, it follows fetching some vector property, and is followed by fetching a second vector property of the same
axis. The result is a matrix whose rows are the values of the 1st property and the columns are the values of the
2nd property, and the values are the number of times the combination of values appears. In a string [`Query`](@ref),
this is specified using the `*` operator, followed by the property name to look up (e.g., `/ cell : type * batch`
will generate a matrix whose rows correspond to cell types, whose columns correspond to cell batches, and whose
values are the number of cells of each combination of batch and type).

By default, the rows and/or columns only contain actually seen values and are ordered alphabetically. However, it is
common that one or both of the properties correspond to an axis. In this case, you can use an [`AsAxis`](@ref) suffix to
force the rows and/or columns of the matrix to be exactly the entries of the specific axis (e.g.,
` / cell : type ! * batch`
will generate a matrix whose rows are exactly the entries of the `type` axis, even if there is a type without any
cells). This is especially useful when both properties are axes, as the result can be stored as a matrix property (e.g.,
`/ cell : type ! * batch !` will generate a matrix whose rows are the entries of the type axis, and whose columns are
the entries of the batch axis, so it can be given to `set_matrix!(daf, "type", "batch", ...)`).

The raw counts matrix can be post-processed like any other matrix (using [`ReductionOperation`](@ref) or an
[`EltwiseOperation`](@ref)). This allows computing useful aggregate properties (e.g.,
`/ cell : type * batch % Fractions` will generate a matrix whose columns correspond to batches and whose rows are the
fraction of the cells from each type within each batch).
"""
struct CountBy <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, count_by::CountBy)::Nothing
    print(io, "* $(escape_value(count_by.property_name))")
    return nothing
end

"""
    GroupBy(property::AbstractString) <: QueryOperation

A query operation that uses a (following) [`ReductionOperation`](@ref) to aggregate the values of each group of values.
Will fetch the specified `property_name` (possibly followed by additional [`Fetch`](@ref) operations) and use the
resulting vector for the name of the group of each value.

If applied to a vector, the result is a vector with one entry per group (e.g., `/ cell : age @ type %> Mean` will
generate a vector with an entry per cell type and whose values are the mean age of the cells of each type). If applied
to a matrix, the result is a matrix with one row per group (e.g., `/ cell / gene : UMIs @ type %> Max` will generate
a matrix with one row per type and one column per gene, whose values are the maximal UMIs count of the gene in the cells
of each type).

By default, the result uses only group values we actually observe, in sorted order. However, if the operation is
followed by an [`AsAxis`](@ref) suffix, then the fetched property must correspond to an existing axis (similar to when
using [`Fetch`](@ref)), and the result will use the entries of the axis, even if we do not observe them in the data (and
will ignore vector entries with an empty value). In this case, the reduction operation will fail if there are no values
for some group, unless it is followed by an [`IfMissing`](@ref) suffix (e.g., `/ cell : age @ type ! %> Mean` will
generate a vector whose entries are all the entries of the `type` axis, and will ignore cells with an empty type; this
will fail if there are types which are not associated with any cell. In contrast, `/ cell : age @ type ! %> Mean || 0`
will succeed, assigning a value of zero for types which have no cells associated with them).
"""
struct GroupBy <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, group_by::GroupBy)::Nothing
    print(io, "@ $(escape_value(group_by.property_name))")
    return nothing
end

function Base.show(io::IO, eltwise_operation::EltwiseOperation)::Nothing
    show_computation_operation(io, "%", eltwise_operation)
    return nothing
end

function Base.show(io::IO, reduction_operation::ReductionOperation)::Nothing
    show_computation_operation(io, "%>", reduction_operation)
    return nothing
end

function show_computation_operation(
    io::IO,
    operator::AbstractString,
    computation_operation::ComputationOperation,
)::Nothing
    print(io, operator)
    print(io, " ")

    operation_type = typeof(computation_operation)
    print(io, operation_type)

    for field_name in fieldnames(operation_type)
        if field_name != :dtype || getfield(computation_operation, :dtype) !== nothing
            print(io, " ")
            print(io, escape_value(string(field_name)))
            print(io, " ")
            field_value = getfield(computation_operation, field_name)
            if field_value == Float64(e)
                print(io, "e")
            elseif field_value == Float64(pi)
                print(io, "pi")
            elseif field_value isa AbstractString
                print(io, escape_value(field_value))  # untested
            else
                print(io, field_value)
            end
        end
    end

    return nothing
end

mutable struct ScalarState
    query_sequence::QuerySequence
    dependency_keys::Set{CacheKey}
    scalar_value::StorageScalar
end

struct FakeScalarState end

mutable struct AxisState
    query_sequence::QuerySequence
    dependency_keys::Set{CacheKey}
    axis_name::AbstractString
    axis_modifier::Maybe{Union{AbstractVector{Bool}, Int}}
end

struct FakeAxisState
    axis_name::Maybe{AbstractString}
    is_entry::Bool
    is_slice::Bool
end

mutable struct VectorState
    query_sequence::QuerySequence
    dependency_keys::Set{CacheKey}
    named_vector::NamedArray
    property_name::AbstractString
    axis_state::Maybe{AxisState}
    is_processed::Bool
end

struct FakeVectorState
    is_processed::Bool
end

mutable struct MatrixState
    query_sequence::QuerySequence
    dependency_keys::Set{CacheKey}
    named_matrix::NamedArray
    rows_property_name::AbstractString
    columns_property_name::AbstractString
    rows_axis_state::Maybe{AxisState}
    columns_axis_state::AxisState
end

mutable struct NamesState
    dependency_keys::Set{CacheKey}
    names::AbstractSet{<:AbstractString}
end

struct FakeMatrixState end

QueryValue = Union{NamesState, ScalarState, AxisState, VectorState, MatrixState, AsAxis, GroupBy}

mutable struct QueryState
    daf::DafReader
    query_sequence::QuerySequence
    next_operation_index::Int
    stack::Vector{QueryValue}
end

function debug_query(object::Any; name::Maybe{AbstractString} = nothing, indent::AbstractString = "")::Nothing  # untested
    if name === nothing
        @debug "$(indent)- $(depict(object))"
    else
        @debug "$(indent)- $(name): $(depict(object))"
    end
    return nothing
end

function debug_query(  # untested
    query_state::QueryState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- QueryState:"
    else
        @debug "$(indent)- $(name): QueryState:"
    end
    @debug "$(indent)  daf: $(query_state.daf.name)"
    @debug "$(indent)  sequence: $(query_state.query_sequence)"
    @debug "$(indent)  next_operation_index: $(query_state.next_operation_index)"
    @debug "$(indent)  stack:"
    for (index, query_value) in enumerate(query_state.stack)
        debug_query(query_value; name = "#$(index)", indent = indent * "  ")
    end
    return nothing
end

function debug_query(  # untested
    scalar_state::ScalarState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- ScalarState:"
    else
        @debug "$(indent)- $(name): ScalarState:"
    end
    @debug "$(indent)  query_sequence: $(scalar_state.query_sequence)"
    @debug "$(indent)  scalar_value: $(scalar_state.scalar_value)"
    return nothing
end

function debug_query(axis_state::AxisState; name::Maybe{AbstractString} = nothing, indent::AbstractString = "")::Nothing  # untested
    if name === nothing
        @debug "$(indent)- AxisState:"
    else
        @debug "$(indent)- $(name): AxisState:"
    end
    @debug "$(indent)  query_sequence: $(axis_state.query_sequence)"
    @debug "$(indent)  axis_name: $(axis_state.axis_name)"
    @debug "$(indent)  axis_modifier: $(depict(axis_state.axis_modifier))"
    return nothing
end

function debug_query(  # untested
    vector_state::VectorState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- VectorState:"
    else
        @debug "$(indent)- $(name): VectorState:"
    end
    @debug "$(indent)   query_sequence: $(vector_state.query_sequence)"
    @debug "$(indent)   named_vector: $(depict(vector_state.named_vector))"
    @debug "$(indent)   property_name: $(vector_state.property_name)"
    @debug "$(indent)  is_processed: $(vector_state.is_processed)"
    if vector_state.axis_state === nothing
        @debug "$(indent)  axis_state: nothing"
    else
        debug_query(vector_state.axis_state; name = "axis_state", indent = indent * "  ")
    end
    return nothing
end

function debug_query(  # untested
    matrix_state::MatrixState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- MatrixState:"
    else
        @debug "$(indent)- $(name): MatrixState:"
    end
    @debug "$(indent)  query_sequence: $(matrix_state.query_sequence)"
    @debug "$(indent)  named_matrix: $(depict(matrix_state.named_matrix))"
    @debug "$(indent)  rows_property_name: $(matrix_state.rows_property_name)"
    @debug "$(indent)  columns_property_name: $(matrix_state.columns_property_name)"
    if matrix_state.rows_axis_state === nothing
        @debug "$(indent)  rows_axis_state: nothing"
    else
        debug_query(matrix_state.rows_axis_state; name = "rows_axis_state", indent = indent * "  ")
    end
    debug_query(matrix_state.columns_axis_state; name = "columns_axis_state", indent = indent * "  ")
    return nothing
end

struct FakeAsAxis end

struct FakeGroupBy end

FakeQueryValue = Union{
    Set{AbstractString},
    FakeScalarState,
    FakeAxisState,
    FakeVectorState,
    FakeMatrixState,
    FakeAsAxis,
    FakeGroupBy,
}

mutable struct FakeQueryState
    daf::Maybe{DafReader}
    query_sequence::QuerySequence
    next_operation_index::Int
    stack::Vector{FakeQueryValue}
    requires_relayout::Bool
end

function query_state_sequence(query_state::Union{QueryState, FakeQueryState}; trim::Int = 0)::QuerySequence
    last_index = query_state.next_operation_index - 1 - trim
    return QuerySequence(query_state.query_sequence.query_operations[1:last_index])
end

function error_at_state(query_state::Union{QueryState, FakeQueryState}, message::AbstractString)::Union{}
    query_state_first_offset = length(string(query_state_sequence(query_state; trim = 1)))
    query_state_last_offset = length(string(query_state_sequence(query_state)))
    if query_state_first_offset > 0
        query_state_first_offset += 1
    end
    @assert query_state_last_offset > query_state_first_offset

    indent = repeat(" ", query_state_first_offset)
    marker = repeat("▲", query_state_last_offset - query_state_first_offset)

    message *= "\nin the query: $(query_state.query_sequence)\nat operation: $(indent)$(marker)"
    if query_state isa QueryState
        message *= "\nfor the daf data: $(query_state.daf.name)"
    end

    return error(message)
end

function error_unexpected_operation(query_state::Union{QueryState, FakeQueryState})::Union{}
    query_operation = query_state.query_sequence.query_operations[query_state.next_operation_index - 1]
    if query_operation isa EltwiseOperation
        query_operation_type = EltwiseOperation
    elseif query_operation isa ReductionOperation
        query_operation_type = ReductionOperation
    else
        query_operation_type = typeof(query_operation)
    end
    return error_at_state(query_state, "unexpected operation: $(query_operation_type)")
end

function Base.getindex(
    daf::DafReader,
    query::QueryString,
)::Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray}
    return get_query(daf, query)
end

"""
    get_query(
        daf::DafReader,
        query::QueryString;
        [cache::Bool = true]
    )::Union{StorageScalar, NamedVector, NamedMatrix}

Apply the full `query` to the `Daf` data and return the result. By default, this will cache results, so repeated queries
will be accelerated. This may consume a large amount of memory. You can disable it by specifying `cache = false`, or
release the cached data using [`empty_cache!`](@ref).

As a shorthand syntax you can also invoke this using `getindex`, that is, using the `[]` operator (e.g.,
`daf[q"/ cell"]` is equivalent to `get_query(daf, q"/ cell")`).
"""
function get_query(
    daf::DafReader,
    query_string::AbstractString;
    cache::Bool = true,
)::Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray}
    return get_query(daf, Query(query_string); cache = cache)
end

function get_query(
    daf::DafReader,
    query_sequence::QuerySequence;
    cache::Bool = true,
)::Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray}
    cache_key = (CachedQuery, "$(query_sequence)")
    return Formats.with_data_read_lock(daf, "for get_query of:", cache_key) do
        did_compute = [false]
        if cache
            result = Formats.get_through_cache(
                daf,
                cache_key,
                Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray},
                QueryData;
                is_slow = true,
            ) do
                did_compute[1] = true
                return do_get_query(daf, query_sequence)
            end
        else
            result = Formats.with_cache_read_lock(daf, "for get_query of:", cache_key) do
                return get(daf.internal.cache, cache_key, nothing)
            end
            if result === nothing
                did_compute[1] = true
                result, _ = do_get_query(daf, query_sequence)
            else
                result = result.data
            end
        end
        if !did_compute[1]
            verify_contract_query(daf, cache_key)
        end
        @debug "get_query daf: $(depict(daf)) query_sequence: $(query_sequence) cache: $(cache) result: $(depict(result))"
        return result
    end
end

function verify_contract_query(::DafReader, ::CacheKey)::Nothing
    return nothing
end

function do_get_query(
    daf::DafReader,
    query_sequence::QuerySequence,
)::Tuple{
    Union{AbstractSet{<:AbstractString}, AbstractVector{<:AbstractString}, StorageScalar, NamedArray},
    Set{CacheKey},
}
    query_state = QueryState(daf, query_sequence, 1, Vector{QueryValue}())
    while query_state.next_operation_index <= length(query_state.query_sequence.query_operations)
        query_operation = query_sequence.query_operations[query_state.next_operation_index]
        query_state.next_operation_index += 1
        apply_query_operation!(query_state, query_operation)
    end

    if is_all(query_state, (NamesState,))
        return get_names_result(query_state)
    elseif is_all(query_state, (ScalarState,))
        return get_scalar_result(query_state)
    elseif is_all(query_state, (AxisState,))
        return axis_array_result(query_state)
    elseif is_all(query_state, (VectorState,))
        return get_vector_result(query_state)
    elseif is_all(query_state, (MatrixState,))
        return get_matrix_result(query_state)
    else
        return error(dedent("""
            partial query: $(query_state.query_sequence)
            for the daf data: $(query_state.daf.name)
        """))
    end
end

function get_names_result(query_state::QueryState)::Tuple{AbstractSet{<:AbstractString}, Set{CacheKey}}
    names_state = pop!(query_state.stack)
    @assert names_state isa NamesState
    return (names_state.names, names_state.dependency_keys)
end

function get_scalar_result(query_state::QueryState)::Tuple{StorageScalar, Set{CacheKey}}
    scalar_state = pop!(query_state.stack)
    @assert scalar_state isa ScalarState
    return (scalar_state.scalar_value, scalar_state.dependency_keys)
end

function axis_array_result(
    query_state::QueryState,
)::Tuple{Union{AbstractString, AbstractVector{<:AbstractString}}, Set{CacheKey}}
    axis_state = pop!(query_state.stack)
    @assert axis_state isa AxisState

    axis_modifier = axis_state.axis_modifier
    axis_entries = axis_array(query_state.daf, axis_state.axis_name)
    if axis_modifier isa Int
        return (axis_entries[axis_modifier], axis_state.dependency_keys)
    else
        if axis_modifier isa AbstractVector{Bool}
            axis_entries = axis_entries[axis_modifier]
        end
        return (Formats.read_only_array(axis_entries), axis_state.dependency_keys)
    end
end

function get_vector_result(query_state::QueryState)::Tuple{NamedArray, Set{CacheKey}}
    vector_state = pop!(query_state.stack)
    @assert vector_state isa VectorState
    return (Formats.read_only_array(vector_state.named_vector), vector_state.dependency_keys)
end

function get_matrix_result(query_state::QueryState)::Tuple{NamedArray, Set{CacheKey}}
    matrix_state = pop!(query_state.stack)
    @assert matrix_state isa MatrixState
    return (Formats.read_only_array(matrix_state.named_matrix), matrix_state.dependency_keys)
end

function is_axis_query(query_string::AbstractString)::Bool
    return is_axis_query(Query(query_string))
end

function query_result_dimensions(query_string::AbstractString)::Int
    return query_result_dimensions(Query(query_string))
end

function query_requires_relayout(daf::DafReader, query_string::AbstractString)::Bool
    return query_requires_relayout(daf, Query(query_string))
end

"""
    is_axis_query(query::QueryString)::Bool

Returns whether the `query` specifies a (possibly masked) axis. This also verifies the query is syntactically valid,
though it may still fail if applied to specific data due to invalid data values or types.
"""
function is_axis_query(query_sequence::QuerySequence)::Bool
    return get_is_axis_query(get_fake_query_result(query_sequence))
end

function get_is_axis_query(fake_query_state::FakeQueryState)::Bool
    if is_all(fake_query_state, (FakeAxisState,))
        fake_axis_state = fake_query_state.stack[1]
        @assert fake_axis_state isa FakeAxisState
        if fake_axis_state.axis_name === nothing || !fake_axis_state.is_entry
            return true
        end
    end

    return false
end

"""
    query_result_dimensions(query::QueryString)::Int

Return the number of dimensions (-1 - names, 0 - scalar, 1 - vector, 2 - matrix) of the results of a `query`. This also
verifies the query is syntactically valid, though it may still fail if applied to specific data due to invalid data
values or types.
"""
function query_result_dimensions(query_sequence::QuerySequence)::Int
    return get_query_result_dimensions(get_fake_query_result(query_sequence))
end

"""
    query_requires_relayout(daf::DafReader, query::QueryString)::Bool

Whether computing the `query` for the `daf` data requires [`relayout!`](@ref) of some matrix. This also verifies the
query is syntactically valid and that the query can be computed, though it may still fail if applied to specific data
due to invalid values or types.
"""
function query_requires_relayout(daf::DafReader, query_sequence::QuerySequence)::Bool
    return Formats.with_data_read_lock(daf, "for query_requires_relayout:", query_sequence) do
        return get_fake_query_result(query_sequence; daf = daf).requires_relayout
    end
end

function get_fake_query_result(query_sequence::QuerySequence; daf::Maybe{DafReader} = nothing)::FakeQueryState
    fake_query_state = FakeQueryState(daf, query_sequence, 1, Vector{FakeQueryValue}(), false)
    while fake_query_state.next_operation_index <= length(fake_query_state.query_sequence.query_operations)
        query_operation = query_sequence.query_operations[fake_query_state.next_operation_index]
        fake_query_state.next_operation_index += 1
        fake_query_operation!(fake_query_state, query_operation)
    end
    return fake_query_state
end

function get_next_operation(
    query_state::Union{QueryState, FakeQueryState},
    query_operation_type::Type,
)::Maybe{QueryOperation}
    query_operation = peek_next_operation(query_state, query_operation_type)
    if query_operation !== nothing
        query_state.next_operation_index += 1
    end
    return query_operation
end

function peek_next_operation(
    query_state::Union{QueryState, FakeQueryState},
    query_operation_type::Type;
    skip::Int = 0,
)::Maybe{QueryOperation}
    if query_state.next_operation_index + skip <= length(query_state.query_sequence.query_operations)
        query_operation = query_state.query_sequence.query_operations[query_state.next_operation_index + skip]
        if query_operation isa query_operation_type
            return query_operation
        end
    end
    return nothing
end

function get_query_result_dimensions(fake_query_state::FakeQueryState)::Int
    if is_all(fake_query_state, (AbstractSet{<:AbstractString},))
        return -1
    elseif is_all(fake_query_state, (FakeScalarState,))
        return 0
    elseif is_all(fake_query_state, (FakeAxisState,))
        fake_axis_state = fake_query_state.stack[1]
        @assert fake_axis_state isa FakeAxisState
        if fake_axis_state.is_entry
            return 0
        else
            return 1
        end
    elseif is_all(fake_query_state, (FakeVectorState,))
        return 1
    elseif is_all(fake_query_state, (FakeMatrixState,))
        return 2
    else
        return error("partial query: $(fake_query_state.query_sequence)")
    end
end

function apply_query_operation!(query_state::QueryState, ::ModifierQueryOperation)::Nothing
    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, ::ModifierQueryOperation)::Nothing
    return error_unexpected_operation(fake_query_state)
end

function apply_query_operation!(query_state::QueryState, axis::Axis)::Nothing
    if isempty(query_state.stack) || is_all(query_state, (AxisState,))
        is_equal = get_next_operation(query_state, IsEqual)
        push_axis(query_state, axis, is_equal)
        return nothing
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, axis::Axis)::Nothing
    if isempty(fake_query_state.stack) || is_all(fake_query_state, (FakeAxisState,))
        daf = fake_query_state.daf
        if daf !== nothing
            require_axis(daf, "for the query: $(fake_query_state.query_sequence)", axis.axis_name)
        end
        is_entry = get_next_operation(fake_query_state, IsEqual) !== nothing
        is_slice = peek_next_operation(fake_query_state, MaskOperation) !== nothing
        push!(fake_query_state.stack, FakeAxisState(axis.axis_name, is_entry, is_slice))
        return nothing
    end

    return error_unexpected_operation(fake_query_state)
end

function push_axis(query_state::QueryState, axis::Axis, ::Nothing)::Nothing
    require_axis(query_state.daf, "for the query: $(query_state.query_sequence)", axis.axis_name)
    query_sequence = QuerySequence((axis,))
    dependency_keys = Set((Formats.axis_array_cache_key(axis.axis_name),))
    axis_state = AxisState(query_sequence, dependency_keys, axis.axis_name, nothing)
    push!(query_state.stack, axis_state)
    return nothing
end

function push_axis(query_state::QueryState, axis::Axis, is_equal::IsEqual)::Nothing
    axis_entries = get_vector(query_state.daf, axis.axis_name, "name")

    query_sequence = QuerySequence((axis, is_equal))
    dependency_keys = Set((Formats.axis_array_cache_key(axis.axis_name),))

    comparison_value = is_equal.comparison_value
    if !(comparison_value isa AbstractString)
        error_at_state(query_state, dedent("""
                                        comparing a non-String ($(typeof(comparison_value))): $(comparison_value)
                                        with entries of the axis: $(axis.axis_name)
                                    """))
    end

    axis_entry_index = get(axis_entries.dicts[1], comparison_value, nothing)
    if axis_entry_index === nothing
        error_at_state(query_state, dedent("""
                                        the entry: $(comparison_value)
                                        does not exist in the axis: $(axis.axis_name)
                                    """))
    end

    axis_state = AxisState(query_sequence, dependency_keys, axis.axis_name, axis_entry_index)
    push!(query_state.stack, axis_state)
    return nothing
end

function apply_query_operation!(query_state::QueryState, names::Names)::Nothing
    if isempty(query_state.stack)
        return get_kind_names(query_state, names)
    elseif is_all(query_state, (AxisState,))
        return get_vectors_set(query_state, names)
    elseif is_all(query_state, (AxisState, AxisState))
        return get_matrices_set(query_state, names)
    end

    return error_unexpected_operation(query_state)
end

function get_kind_names(query_state::QueryState, names::Names)::Nothing
    if names.kind === nothing
        error_at_state(query_state, "no kind specified for names")
    end

    if names.kind == "scalars"
        push!(query_state.stack, NamesState(Set([Formats.scalars_set_cache_key()]), scalars_set(query_state.daf)))
    elseif names.kind == "axes"
        push!(query_state.stack, NamesState(Set([Formats.axes_set_cache_key()]), axes_set(query_state.daf)))
    else
        error_at_state(query_state, "invalid kind: $(names.kind)")
    end

    return nothing
end

function get_vectors_set(query_state::QueryState, names::Names)::Nothing
    if names.kind !== nothing
        error_at_state(query_state, dedent("""
            unexpected kind: $(names.kind)
            specified for vector names
        """))
    end
    axis_state = pop!(query_state.stack)
    @assert axis_state isa AxisState
    if axis_state.axis_modifier !== nothing
        error_at_state(query_state, "sliced/masked axis for vector names")
    end

    push!(
        query_state.stack,
        NamesState(
            Set([Formats.vectors_set_cache_key(axis_state.axis_name)]),
            vectors_set(query_state.daf, axis_state.axis_name),
        ),
    )
    return nothing
end

function get_matrices_set(query_state::QueryState, names::Names)::Nothing
    if names.kind !== nothing
        error_at_state(query_state, dedent("""
            unexpected kind: $(names.kind)
            specified for matrix names
        """))
    end

    rows_axis_state = pop!(query_state.stack)
    @assert rows_axis_state isa AxisState
    columns_axis_state = pop!(query_state.stack)
    @assert columns_axis_state isa AxisState
    if rows_axis_state.axis_modifier !== nothing || columns_axis_state.axis_modifier !== nothing
        error_at_state(query_state, "sliced/masked axis for matrix names")
    end

    push!(
        query_state.stack,
        NamesState(
            Set([
                Formats.matrices_set_cache_key(
                    rows_axis_state.axis_name,
                    columns_axis_state.axis_name;
                    relayout = true,
                ),
            ]),
            matrices_set(query_state.daf, rows_axis_state.axis_name, columns_axis_state.axis_name),
        ),
    )
    return nothing
end

function fake_query_operation!(fake_query_state::FakeQueryState, names::Names)::Nothing
    if isempty(fake_query_state.stack)
        return fake_kind_names(fake_query_state, names)
    elseif is_all(fake_query_state, (FakeAxisState,))
        return fake_vectors_set(fake_query_state, names)
    elseif is_all(fake_query_state, (FakeAxisState, FakeAxisState))
        return fake_matrices_set(fake_query_state, names)
    end

    return error_unexpected_operation(fake_query_state)
end

function fake_kind_names(fake_query_state::FakeQueryState, names::Names)::Nothing
    if names.kind === nothing
        error_at_state(fake_query_state, "no kind specified for names")
    elseif names.kind != "scalars" && names.kind != "axes"
        error_at_state(fake_query_state, "invalid kind: $(names.kind)")
    end

    push!(fake_query_state.stack, Set{AbstractString}())
    return nothing
end

function fake_vectors_set(fake_query_state::FakeQueryState, names::Names)::Nothing
    if names.kind !== nothing
        error_at_state(fake_query_state, dedent("""
            unexpected kind: $(names.kind)
            specified for vector names
        """))
    end

    fake_axis_state = pop!(fake_query_state.stack)
    @assert fake_axis_state isa FakeAxisState

    if fake_axis_state.is_entry || fake_axis_state.is_slice
        error_at_state(fake_query_state, "sliced/masked axis for vector names")
    end

    push!(fake_query_state.stack, Set{AbstractString}())
    return nothing
end

function fake_matrices_set(fake_query_state::FakeQueryState, names::Names)::Nothing
    if names.kind !== nothing
        error_at_state(fake_query_state, dedent("""
            unexpected kind: $(names.kind)
            specified for matrix names
        """))
    end

    fake_rows_axis_state = pop!(fake_query_state.stack)
    @assert fake_rows_axis_state isa FakeAxisState
    fake_columns_axis_state = pop!(fake_query_state.stack)
    @assert fake_columns_axis_state isa FakeAxisState

    if fake_rows_axis_state.is_entry ||
       fake_rows_axis_state.is_slice ||
       fake_columns_axis_state.is_entry ||
       fake_columns_axis_state.is_slice
        error_at_state(fake_query_state, "sliced/masked axis for matrix names")
    end

    push!(fake_query_state.stack, Set{AbstractString}())
    return nothing
end

function apply_query_operation!(query_state::QueryState, lookup::Lookup)::Nothing
    if isempty(query_state.stack)
        return lookup_scalar(query_state, lookup)
    elseif is_all(query_state, (AxisState,))
        return lookup_axis(query_state, lookup)
    elseif is_all(query_state, (AxisState, AxisState))
        return lookup_axes(query_state, lookup)
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, lookup::Lookup)::Nothing
    if isempty(fake_query_state.stack)
        return fake_lookup_scalar(fake_query_state)
    elseif is_all(fake_query_state, (FakeAxisState,))
        return fake_lookup_axis(fake_query_state)
    elseif is_all(fake_query_state, (FakeAxisState, FakeAxisState))
        return fake_lookup_axes(fake_query_state, lookup)
    end

    return error_unexpected_operation(fake_query_state)
end

function lookup_scalar(query_state::QueryState, lookup::Lookup)::Nothing
    if_missing_value = parse_if_missing_value(query_state)
    scalar_value = get_scalar(query_state.daf, lookup.property_name; default = if_missing_value)
    dependency_keys = Set((Formats.scalar_cache_key(lookup.property_name),))
    scalar_state = ScalarState(query_state_sequence(query_state), dependency_keys, scalar_value)
    push!(query_state.stack, scalar_state)
    return nothing
end

function fake_lookup_scalar(fake_query_state::FakeQueryState)::Nothing
    get_next_operation(fake_query_state, IfMissing)
    push!(fake_query_state.stack, FakeScalarState())
    return nothing
end

function lookup_axes(query_state::QueryState, lookup::Lookup)::Nothing
    columns_axis_state = pop!(query_state.stack)
    @assert columns_axis_state isa AxisState
    rows_axis_state = pop!(query_state.stack)
    @assert rows_axis_state isa AxisState

    if_missing_value = parse_if_missing_value(query_state)
    named_matrix = get_matrix(
        query_state.daf,
        rows_axis_state.axis_name,
        columns_axis_state.axis_name,
        lookup.property_name;
        default = if_missing_value,
    )

    dependency_keys = union(rows_axis_state.dependency_keys, columns_axis_state.dependency_keys)
    push!(
        dependency_keys,
        Formats.matrix_cache_key(rows_axis_state.axis_name, columns_axis_state.axis_name, lookup.property_name),
    )

    rows_axis_modifier = rows_axis_state.axis_modifier
    columns_axis_modifier = columns_axis_state.axis_modifier

    if rows_axis_modifier === nothing
        if columns_axis_modifier === nothing
            return lookup_matrix(query_state, named_matrix, rows_axis_state, columns_axis_state, dependency_keys)
        elseif columns_axis_modifier isa Int
            return lookup_matrix_slice(
                query_state,
                named_matrix[:, columns_axis_modifier],
                rows_axis_state,
                dependency_keys,
            )
        elseif columns_axis_modifier isa AbstractVector{Bool}
            return lookup_matrix(
                query_state,
                named_matrix[:, columns_axis_modifier],
                rows_axis_state,
                columns_axis_state,
                dependency_keys,
            )
        end

    elseif rows_axis_modifier isa Int
        if columns_axis_modifier === nothing
            return lookup_matrix_slice(
                query_state,
                named_matrix[rows_axis_modifier, :],
                columns_axis_state,
                dependency_keys,
            )
        elseif columns_axis_modifier isa Int
            return lookup_matrix_entry(
                query_state,
                named_matrix[rows_axis_modifier, columns_axis_modifier],
                dependency_keys,
            )
        elseif columns_axis_modifier isa AbstractVector{Bool}
            return lookup_matrix_slice(
                query_state,
                named_matrix[rows_axis_modifier, columns_axis_modifier],
                columns_axis_state,
                dependency_keys,
            )
        end

    elseif rows_axis_modifier isa AbstractVector{Bool}
        if columns_axis_modifier === nothing
            return lookup_matrix(
                query_state,
                named_matrix[rows_axis_modifier, :],
                rows_axis_state,
                columns_axis_state,
                dependency_keys,
            )
        elseif columns_axis_modifier isa Int
            return lookup_matrix_slice(
                query_state,
                named_matrix[rows_axis_modifier, columns_axis_modifier],
                rows_axis_state,
                dependency_keys,
            )
        elseif columns_axis_modifier isa AbstractVector{Bool}
            return lookup_matrix(
                query_state,
                named_matrix[rows_axis_modifier, columns_axis_modifier],
                rows_axis_state,
                columns_axis_state,
                dependency_keys,
            )
        end
    end

    @assert false
end

function fake_lookup_axes(fake_query_state::FakeQueryState, lookup::Lookup)::Nothing
    columns_axis_state = pop!(fake_query_state.stack)
    @assert columns_axis_state isa FakeAxisState
    rows_axis_state = pop!(fake_query_state.stack)
    @assert rows_axis_state isa FakeAxisState

    daf = fake_query_state.daf
    if daf !== nothing
        rows_axis_name = rows_axis_state.axis_name
        columns_axis_name = columns_axis_state.axis_name
        @assert rows_axis_name !== nothing
        @assert columns_axis_name !== nothing
        if rows_axis_name != columns_axis_name &&
           !has_matrix(daf, rows_axis_name, columns_axis_name, lookup.property_name; relayout = false) &&
           has_matrix(daf, columns_axis_name, rows_axis_name, lookup.property_name; relayout = false)
            fake_query_state.requires_relayout = true
        end
    end

    get_next_operation(fake_query_state, IfMissing)

    if rows_axis_state.is_entry
        if columns_axis_state.is_entry
            push!(fake_query_state.stack, FakeScalarState())
        else
            push!(fake_query_state.stack, FakeVectorState(false))
        end

    else
        if columns_axis_state.is_entry
            push!(fake_query_state.stack, FakeVectorState(false))
        else
            push!(fake_query_state.stack, FakeMatrixState())
        end
    end

    return nothing
end

function lookup_matrix(
    query_state::QueryState,
    named_matrix::Maybe{NamedMatrix},
    rows_axis_state::AxisState,
    columns_axis_state::AxisState,
    dependency_keys::Set{CacheKey},
)::Nothing
    matrix_state = MatrixState(
        query_state_sequence(query_state),
        dependency_keys,
        named_matrix,
        rows_axis_state.axis_name,
        columns_axis_state.axis_name,
        rows_axis_state,
        columns_axis_state,
    )
    push!(query_state.stack, matrix_state)
    return nothing
end

function lookup_matrix_slice(
    query_state::QueryState,
    named_vector::NamedVector,
    axis_state::AxisState,
    dependency_keys::Set{CacheKey},
)::Nothing
    vector_state = VectorState(
        query_state_sequence(query_state),
        dependency_keys,
        named_vector,
        axis_state.axis_name,
        axis_state,
        false,
    )
    push!(query_state.stack, vector_state)
    return nothing
end

function lookup_matrix_entry(
    query_state::QueryState,
    scalar_value::StorageScalar,
    dependency_keys::Set{CacheKey},
)::Nothing
    scalar_state = ScalarState(query_state_sequence(query_state), dependency_keys, scalar_value)
    push!(query_state.stack, scalar_state)
    return nothing
end

function parse_if_missing_value(query_state::QueryState)::Union{UndefInitializer, StorageScalar}
    if_missing = get_next_operation(query_state, IfMissing)
    if if_missing === nothing
        if_missing_value = undef
    else
        @assert if_missing isa IfMissing
        if_missing_value = value_for_if_missing(query_state, if_missing)
    end
    return if_missing_value
end

function lookup_axis(query_state::QueryState, lookup::Lookup)::Nothing
    axis_state = pop!(query_state.stack)
    @assert axis_state isa AxisState
    fetch_property(query_state, axis_state, lookup)
    return nothing
end

function fake_lookup_axis(fake_query_state::FakeQueryState)::Nothing
    fake_axis_state = pop!(fake_query_state.stack)
    @assert fake_axis_state isa FakeAxisState

    fake_fetch_property(fake_query_state, fake_axis_state)
    return nothing
end

FetchBaseOperation = Union{Lookup, MaskOperation, CountBy, GroupBy}

mutable struct CommonFetchState
    base_query_sequence::QuerySequence
    first_operation_index::Int
    axis_state::AxisState
    axis_name::AbstractString
    property_name::AbstractString
    dependency_keys::Set{CacheKey}
end

mutable struct EntryFetchState
    common::CommonFetchState
    axis_entry_index::Int
    scalar_value::Maybe{StorageScalar}
    if_not_value::Maybe{StorageScalar}
end

mutable struct VectorFetchState
    common::CommonFetchState
    may_modify_axis_mask::Bool
    named_vector::Maybe{NamedArray}
    may_modify_named_vector::Bool
    if_not_values::Maybe{Vector{Maybe{IfNot}}}
end

function debug_query(  # untested
    common_fetch_state::CommonFetchState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- CommonFetchState:"
    else
        @debug "$(indent)- $(name): CommonFetchState:"
    end
    @debug "$(indent)  base_query_sequence: $(common_fetch_state.base_query_sequence)"
    @debug "$(indent)  first_operation_index: $(common_fetch_state.first_operation_index)"
    debug_query(common_fetch_state.axis_state; name = "axis_state", indent = indent * "  ")
    @debug "$(indent)  axis_name: $(common_fetch_state.axis_name)"
    @debug "$(indent)  property_name: $(depict(common_fetch_state.property_name))"
    @debug "$(indent)  dependency_keys: $(depict(common_fetch_state.dependency_keys))"
    return nothing
end

function debug_query(  # untested
    entry_fetch_state::EntryFetchState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- EntryFetchState:"
    else
        @debug "$(indent)- $(name): EntryFetchState:"
    end
    debug_query(entry_fetch_state.common; name = "common", indent = indent * "  ")
    @debug "$(indent)  axis_entry_index: $(entry_fetch_state.axis_entry_index)"
    @debug "$(indent)  scalar_value: $(entry_fetch_state.scalar_value)"
    @debug "$(indent)  if_not_value: $(entry_fetch_state.if_not_value)"
    return nothing
end

function debug_query(  # untested
    vector_fetch_state::VectorFetchState;
    name::Maybe{AbstractString} = nothing,
    indent::AbstractString = "",
)::Nothing
    if name === nothing
        @debug "$(indent)- VectorFetchState:"
    else
        @debug "$(indent)- $(name): VectorFetchState:"
    end
    debug_query(vector_fetch_state.common; name = "common", indent = indent * "  ")
    @debug "$(indent)  may_modify_axis_mask: $(vector_fetch_state.may_modify_axis_mask)"
    @debug "$(indent)  named_vector: $(depict(vector_fetch_state.named_vector))"
    @debug "$(indent)  may_modify_named_vector: $(vector_fetch_state.may_modify_named_vector)"
    @debug "$(indent)  if_not_values: $(depict(vector_fetch_state.if_not_values))"
    return nothing
end

function fetch_property(query_state::QueryState, axis_state::AxisState, fetch_operation::FetchBaseOperation)::Nothing
    base_query_sequence = QuerySequence((axis_state.query_sequence.query_operations..., fetch_operation))
    common_fetch_state = CommonFetchState(
        base_query_sequence,
        query_state.next_operation_index - 1,
        axis_state,
        axis_state.axis_name,
        "",
        copy(axis_state.dependency_keys),
    )

    axis_modifier = axis_state.axis_modifier
    if axis_modifier isa Int
        fetch_state = EntryFetchState(common_fetch_state, axis_modifier, nothing, nothing)
    else
        fetch_state = VectorFetchState(common_fetch_state, false, nothing, false, nothing)
    end

    fetch_axis_name = axis_state.axis_name
    fetch_property_name = fetch_operation.property_name

    while true
        push!(fetch_state.common.dependency_keys, Formats.vector_cache_key(fetch_axis_name, fetch_property_name))

        if_missing = get_next_operation(query_state, IfMissing)
        if_not = get_next_operation(query_state, IfNot)

        if peek_next_operation(query_state, AsAxis) !== nothing &&
           peek_next_operation(query_state, Fetch; skip = 1) !== nothing
            as_axis = get_next_operation(query_state, AsAxis)
            @assert as_axis !== nothing
        else
            as_axis = nothing
        end

        next_fetch_operation = peek_next_operation(query_state, Fetch)
        is_final = next_fetch_operation === nothing
        if is_final && if_not !== nothing
            error_unexpected_operation(query_state)
        end

        if if_missing === nothing
            if_missing_value = undef
            default_value = undef
        else
            @assert if_missing isa IfMissing
            if_missing_value = value_for_if_missing(query_state, if_missing)
            default_value = nothing
        end
        next_named_vector = get_vector(query_state.daf, fetch_axis_name, fetch_property_name; default = default_value)

        if !is_final && next_named_vector !== nothing && !(eltype(next_named_vector) <: AbstractString)
            query_state.next_operation_index += 1
            error_at_state(query_state, dedent("""
                                            fetching with a non-String vector of: $(eltype(next_named_vector))
                                            of the vector: $(fetch_property_name)
                                            of the axis: $(fetch_axis_name)
                                        """))
        end

        next_fetch_state(
            query_state,
            fetch_state,
            fetch_axis_name,
            fetch_property_name,
            next_named_vector,
            if_missing_value,
            if_not,
            as_axis,
            is_final,
        )
        fetch_state.common.axis_name = fetch_axis_name
        fetch_state.common.property_name = fetch_property_name

        if next_fetch_operation === nothing
            base_query_operations = fetch_state.common.base_query_sequence.query_operations
            fetch_query_operations =
                query_state.query_sequence.query_operations[(fetch_state.common.first_operation_index):(query_state.next_operation_index - 1)]
            fetch_query_sequence = QuerySequence((base_query_operations..., fetch_query_operations...))
            fetch_result(query_state, fetch_state, fetch_query_sequence)
            return nothing
        end

        fetch_operation = next_fetch_operation
        query_state.next_operation_index += 1
        fetch_axis_name = axis_of_property(query_state.daf, fetch_property_name, as_axis)
        fetch_property_name = fetch_operation.property_name
    end
end

function fake_fetch_property(fake_query_state::FakeQueryState, fake_axis_state::FakeAxisState)::Nothing
    while true
        get_next_operation(fake_query_state, IfMissing)
        if_not = get_next_operation(fake_query_state, IfNot)

        if peek_next_operation(fake_query_state, AsAxis) !== nothing &&
           peek_next_operation(fake_query_state, Fetch; skip = 1) !== nothing
            as_axis = get_next_operation(fake_query_state, AsAxis)
            @assert as_axis !== nothing
        else
            as_axis = nothing
        end

        next_fetch_operation = peek_next_operation(fake_query_state, Fetch)
        is_final = next_fetch_operation === nothing
        if is_final && if_not !== nothing
            error_unexpected_operation(fake_query_state)
        end

        if next_fetch_operation === nothing
            if fake_axis_state.is_entry
                push!(fake_query_state.stack, FakeScalarState())
            else
                push!(fake_query_state.stack, FakeVectorState(false))
            end
            return nothing
        end

        fake_query_state.next_operation_index += 1
    end
end

function axis_of_property(daf::DafReader, property_name::AbstractString, as_axis::Maybe{AsAxis})::AbstractString
    if as_axis !== nothing
        axis_name = as_axis.axis_name
        if axis_name !== nothing
            return axis_name
        end
    end

    if has_axis(daf, property_name)
        return property_name
    end

    return split(property_name, "."; limit = 2)[1]
end

function next_fetch_state(
    query_state::QueryState,
    entry_fetch_state::EntryFetchState,
    fetch_axis_name::AbstractString,
    fetch_property_name::AbstractString,
    next_named_vector::Maybe{NamedVector},
    if_missing_value::Union{UndefInitializer, StorageScalar},
    if_not::Maybe{IfNot},
    as_axis::Maybe{AsAxis},
    is_final::Bool,
)::Nothing
    if entry_fetch_state.if_not_value !== nothing
        scalar_value =
            if_not_scalar_value(query_state, entry_fetch_state, next_named_vector, if_missing_value, is_final)

    elseif next_named_vector === nothing
        scalar_value = missing_scalar_value(entry_fetch_state, if_missing_value, is_final)

    else
        scalar_value = entry_scalar_value(
            query_state,
            entry_fetch_state,
            fetch_axis_name,
            fetch_property_name,
            next_named_vector,
            if_not,
            as_axis,
            is_final,
        )
    end

    entry_fetch_state.scalar_value = scalar_value
    return nothing
end

function if_not_scalar_value(
    query_state::QueryState,
    entry_fetch_state::EntryFetchState,
    next_named_vector::Maybe{NamedVector},
    if_missing_value::Union{UndefInitializer, StorageScalar},
    is_final::Bool,
)::Maybe{StorageScalar}
    if !is_final
        return nothing
    else
        if next_named_vector === nothing
            @assert if_missing_value != undef
            dtype = typeof(if_missing_value)
        else
            dtype = eltype(next_named_vector)
        end
        if_not_value = entry_fetch_state.if_not_value
        @assert if_not_value !== nothing
        return value_for(query_state, dtype, if_not_value)  # NOJET
    end
end

function missing_scalar_value(
    entry_fetch_state::EntryFetchState,
    if_missing_value::Union{UndefInitializer, StorageScalar},
    is_final::Bool,
)::Maybe{StorageScalar}
    @assert if_missing_value != undef
    if is_final
        return if_missing_value
    else
        @assert entry_fetch_state.if_not_value === nothing
        entry_fetch_state.if_not_value = if_missing_value  # NOJET
        return nothing
    end
end

function entry_scalar_value(
    query_state::QueryState,
    entry_fetch_state::EntryFetchState,
    fetch_axis_name::AbstractString,
    fetch_property_name::AbstractString,
    next_named_vector::NamedVector,
    if_not::Maybe{IfNot},
    as_axis::Maybe{AsAxis},
    is_final::Bool,
)::Maybe{StorageScalar}
    previous_scalar_value = entry_fetch_state.scalar_value
    if previous_scalar_value === nothing
        scalar_value = next_named_vector.array[entry_fetch_state.axis_entry_index]

    else
        @assert previous_scalar_value isa AbstractString
        @assert previous_scalar_value != ""

        index_in_fetched = get(next_named_vector.dicts[1], previous_scalar_value, nothing)
        if index_in_fetched === nothing
            error_at_state(query_state, dedent("""
                                            invalid value: $(previous_scalar_value)
                                            of the vector: $(entry_fetch_state.common.property_name)
                                            of the axis: $(entry_fetch_state.common.axis_name)
                                            is missing from the fetched axis: $(fetch_axis_name)
                                        """))
        end
        scalar_value = next_named_vector[index_in_fetched]
    end

    if if_not !== nothing && (scalar_value == "" || scalar_value == 0 || scalar_value == false)
        @assert !is_final
        entry_fetch_state.if_not_value = if_not.not_value
        scalar_value = nothing
    end

    if !is_final && scalar_value == ""
        fetch = get_next_operation(query_state, Fetch)
        @assert fetch !== nothing
        next_axis_name = axis_of_property(query_state.daf, fetch_property_name, as_axis)
        error_at_state(query_state, dedent("""
                                        empty value of the vector: $(fetch_property_name)
                                        of the axis: $(fetch_axis_name)
                                        used for the fetched axis: $(next_axis_name)
                                    """))
    end

    return scalar_value
end

function next_fetch_state(
    query_state::QueryState,
    vector_fetch_state::VectorFetchState,
    fetch_axis_name::AbstractString,
    fetch_property_name::AbstractString,
    next_named_vector::Maybe{NamedVector},
    if_missing_value::Union{UndefInitializer, StorageScalar},
    if_not::Maybe{IfNot},
    as_axis::Maybe{AsAxis},
    is_final::Bool,
)::Nothing
    previous_named_vector = vector_fetch_state.named_vector
    if previous_named_vector === nothing
        base_named_vector, fetched_values =
            fetch_first_named_vector(query_state, vector_fetch_state, next_named_vector, if_missing_value)
    else
        base_named_vector = previous_named_vector
        fetched_values = fetch_second_named_vector(
            query_state,
            vector_fetch_state,
            fetch_axis_name,
            previous_named_vector,
            next_named_vector,
            if_missing_value,
        )
    end

    if_not_values = vector_fetch_state.if_not_values

    if if_not !== nothing
        base_named_vector, fetched_values =
            patch_fetched_values(vector_fetch_state, base_named_vector, fetched_values, if_not_values, if_not)
    elseif !is_final
        verify_fetched_values(query_state, fetch_property_name, fetch_axis_name, fetched_values, if_not_values, as_axis)
    end

    vector_fetch_state.named_vector =
        NamedArray(fetched_values, base_named_vector.dicts, (vector_fetch_state.common.axis_state.axis_name,))

    return nothing
end

function fetch_first_named_vector(
    query_state::QueryState,
    vector_fetch_state::VectorFetchState,
    next_named_vector::Maybe{NamedVector},
    if_missing_value::Union{UndefInitializer, StorageScalar},
)::Tuple{NamedVector, StorageVector}
    @assert vector_fetch_state.if_not_values === nothing
    axis_mask = vector_fetch_state.common.axis_state.axis_modifier

    if next_named_vector === nothing
        base_named_vector = get_vector(query_state.daf, vector_fetch_state.common.axis_state.axis_name, "name")
        @assert if_missing_value != undef
        if axis_mask === nothing
            size = axis_length(query_state.daf, vector_fetch_state.common.axis_state.axis_name)
        else
            @assert axis_mask isa AbstractVector{Bool}
            size = sum(axis_mask)
            base_named_vector = base_named_vector[axis_mask]
        end
        fetched_values = Vector{typeof(if_missing_value)}(undef, size)
        vector_fetch_state.may_modify_named_vector = true

        if_not_values = Vector{Maybe{IfNot}}(undef, length(fetched_values))
        fill!(if_not_values, IfNot(if_missing_value))
        vector_fetch_state.if_not_values = if_not_values

    else
        base_named_vector = next_named_vector
        if axis_mask === nothing
            vector_fetch_state.may_modify_named_vector = false
        else
            @assert axis_mask isa AbstractVector{Bool}
            base_named_vector = base_named_vector[axis_mask]  # NOJET
            vector_fetch_state.may_modify_named_vector = true
        end
        fetched_values = base_named_vector.array
    end

    return (base_named_vector, fetched_values)
end

function fetch_second_named_vector(
    query_state::QueryState,
    vector_fetch_state::VectorFetchState,
    fetch_axis_name::AbstractString,
    previous_named_vector::NamedVector,
    next_named_vector::Maybe{NamedVector},
    if_missing_value::Union{UndefInitializer, StorageScalar},
)::StorageVector
    @assert eltype(previous_named_vector) <: AbstractString

    vector_fetch_state.may_modify_named_vector = true

    if next_named_vector === nothing
        @assert if_missing_value != undef
        fetched_values = Vector{typeof(if_missing_value)}(undef, length(previous_named_vector))
        if_not_values = ensure_if_not_values(vector_fetch_state, length(previous_named_vector))
        if_not = IfNot(if_missing_value)
        for (index, if_not_value) in enumerate(if_not_values)
            if if_not_value === nothing
                if_not_values[index] = if_not
            end
        end
    else
        fetched_values = Vector{eltype(next_named_vector)}(undef, length(previous_named_vector))
        if_not_values = vector_fetch_state.if_not_values

        n_values = length(previous_named_vector)
        for index in 1:n_values
            if if_not_values === nothing || if_not_values[index] === nothing
                previous_value = previous_named_vector[index]
                @assert previous_value != ""

                index_in_fetch = get(next_named_vector.dicts[1], previous_value, nothing)
                if index_in_fetch === nothing
                    error_at_state(query_state, dedent("""
                                                    invalid value: $(previous_value)
                                                    of the vector: $(vector_fetch_state.common.property_name)
                                                    of the axis: $(vector_fetch_state.common.axis_name)
                                                    is missing from the fetched axis: $(fetch_axis_name)
                                                """))
                end

                fetched_values[index] = next_named_vector.array[index_in_fetch]
            end
        end
    end

    return fetched_values
end

function patch_fetched_values(
    vector_fetch_state::VectorFetchState,
    base_named_vector::NamedVector,
    fetched_values::StorageVector,
    if_not_values::Maybe{Vector{Maybe{IfNot}}},
    if_not::IfNot,
)::Tuple{NamedVector, StorageVector}
    fetched_mask = nothing
    n_values = length(fetched_values)
    for index in 1:n_values
        if if_not_values === nothing || if_not_values[index] === nothing
            if fetched_values[index] in ("", 0, false)
                if if_not.not_value === nothing
                    fetched_mask = ensure_fetched_mask(fetched_mask, length(fetched_values))
                    fetched_mask[index] = false
                else
                    if_not_values = ensure_if_not_values(vector_fetch_state, length(fetched_values))
                    if_not_values[index] = if_not
                end
            end
        end
    end

    if fetched_mask !== nothing
        axis_mask = vector_fetch_state.common.axis_state.axis_modifier
        if axis_mask === nothing
            axis_mask = fetched_mask
        else
            @assert axis_mask isa AbstractVector{Bool}
            if !vector_fetch_state.may_modify_axis_mask
                axis_mask = copy_array(axis_mask)
            end
            axis_mask[axis_mask] = fetched_mask  # NOJET
        end
        vector_fetch_state.may_modify_axis_mask = true

        axis_state = vector_fetch_state.common.axis_state
        vector_fetch_state.common.axis_state =
            AxisState(axis_state.query_sequence, axis_state.dependency_keys, axis_state.axis_name, axis_mask)

        base_named_vector = base_named_vector[fetched_mask]
        if_not_values = vector_fetch_state.if_not_values
        if if_not_values === nothing
            fetched_values = fetched_values[fetched_mask]
        else
            masked_fetched_values = Vector{eltype(fetched_values)}(undef, sum(fetched_mask))
            masked_index = 0
            for (unmasked_index, is_fetched) in enumerate(fetched_mask)
                if is_fetched
                    masked_index += 1
                    if if_not_values[unmasked_index] === nothing
                        masked_fetched_values[masked_index] = fetched_values[unmasked_index]  # untested
                    end
                end
            end
            @assert masked_index == length(masked_fetched_values)
            vector_fetch_state.if_not_values = if_not_values[fetched_mask]  # NOJET
            fetched_values = masked_fetched_values
        end
    end

    return (base_named_vector, fetched_values)
end

function verify_fetched_values(
    query_state::QueryState,
    fetch_property_name::AbstractString,
    fetch_axis_name::AbstractString,
    fetched_values::StorageVector,
    if_not_values::Maybe{Vector{Maybe{IfNot}}},
    as_axis::Maybe{AsAxis},
)::Nothing
    @assert eltype(fetched_values) <: AbstractString
    n_values = length(fetched_values)
    for index in 1:n_values
        if (if_not_values === nothing || if_not_values[index] === nothing) && fetched_values[index] == ""
            fetch = get_next_operation(query_state, Fetch)
            @assert fetch !== nothing
            next_axis_name = axis_of_property(query_state.daf, fetch_property_name, as_axis)
            error_at_state(query_state, dedent("""
                                            empty value of the vector: $(fetch_property_name)
                                            of the axis: $(fetch_axis_name)
                                            used for the fetched axis: $(next_axis_name)
                                        """))
        end
    end
    return nothing
end

function ensure_if_not_values(vector_fetch_state::VectorFetchState, size::Int)::Vector{Maybe{IfNot}}
    if_not_values = vector_fetch_state.if_not_values
    if if_not_values === nothing
        if_not_values = Vector{Maybe{IfNot}}(undef, size)
        fill!(if_not_values, nothing)
        vector_fetch_state.if_not_values = if_not_values
    end
    return if_not_values
end

function ensure_fetched_mask(fetched_mask::Maybe{AbstractVector{Bool}}, size::Int)::AbstractVector{Bool}
    if fetched_mask === nothing
        fetched_mask = ones(Bool, size)
    end
    return fetched_mask
end

function fetch_result(
    query_state::QueryState,
    entry_fetch_state::EntryFetchState,
    fetch_query_sequence::QuerySequence,
)::Nothing
    scalar_value = entry_fetch_state.scalar_value
    @assert scalar_value !== nothing
    push!(query_state.stack, ScalarState(fetch_query_sequence, entry_fetch_state.common.dependency_keys, scalar_value))
    return nothing
end

function fetch_result(
    query_state::QueryState,
    fetch_state::VectorFetchState,
    fetch_query_sequence::QuerySequence,
)::Nothing
    named_vector = fetch_state.named_vector
    @assert named_vector !== nothing

    if_not_values = fetch_state.if_not_values
    if if_not_values !== nothing
        if !fetch_state.may_modify_named_vector ||
           (eltype(named_vector) <: AbstractString && !(eltype(named_vector) in (String, AbstractString)))
            named_vector = copy_array(named_vector)  # untested
        end

        for index in 1:length(named_vector)
            if_not = if_not_values[index]
            if if_not !== nothing
                if_not_value = if_not.not_value
                @assert if_not_value !== nothing
                named_vector.array[index] = value_for(query_state, eltype(named_vector), if_not_value)
            end
        end
    end

    push!(
        query_state.stack,
        VectorState(
            fetch_query_sequence,
            fetch_state.common.dependency_keys,
            named_vector,
            fetch_state.common.property_name,
            fetch_state.common.axis_state,
            false,
        ),
    )
    return nothing
end

function apply_query_operation!(query_state::QueryState, mask_operation::MaskOperation)::Nothing
    if has_top(query_state, (AxisState,))
        axis_state = pop!(query_state.stack)
        @assert axis_state isa AxisState
        if !(axis_state.axis_modifier isa Int)
            full_axis_state =
                AxisState(axis_state.query_sequence, axis_state.dependency_keys, axis_state.axis_name, nothing)
            fetch_property(query_state, full_axis_state, mask_operation)
            apply_comparison(query_state)
            mask_state = pop!(query_state.stack)
            @assert mask_state isa VectorState
            apply_mask_to_axis_state(query_state, axis_state, mask_state, mask_operation)  # NOJET
            push!(query_state.stack, axis_state)
            return nothing
        end
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, ::MaskOperation)::Nothing
    if has_top(fake_query_state, (FakeAxisState,))
        fake_axis_state = pop!(fake_query_state.stack)
        @assert fake_axis_state isa FakeAxisState
        if !fake_axis_state.is_entry
            fake_fetch_property(fake_query_state, fake_axis_state)
            get_next_operation(fake_query_state, ComparisonOperation)
            fake_mask_state = pop!(fake_query_state.stack)
            @assert fake_mask_state isa FakeVectorState
            push!(fake_query_state.stack, fake_axis_state)
            return nothing
        end
    end

    return error_unexpected_operation(fake_query_state)
end

function apply_comparison(query_state::QueryState)::Nothing
    comparison_operation = get_next_operation(query_state, ComparisonOperation)
    if comparison_operation === nothing
        return nothing
    end

    @assert comparison_operation isa ComparisonOperation
    vector_state = pop!(query_state.stack)
    @assert vector_state isa VectorState

    if comparison_operation isa MatchOperation
        if !(eltype(vector_state.named_vector) <: AbstractString)
            axis_state = vector_state.axis_state
            @assert axis_state !== nothing
            error_at_state(query_state, dedent("""
                                            matching non-string vector: $(eltype(vector_state.named_vector))
                                            of the vector: $(vector_state.property_name)
                                            of the axis: $(axis_state.axis_name)
                                        """))
        end
        compare_with = regex_for(query_state, comparison_operation.comparison_value)

    else
        compare_with = value_for(query_state, eltype(vector_state.named_vector), comparison_operation.comparison_value)
    end

    comparison_values =
        [compute_comparison(value, comparison_operation, compare_with) for value in vector_state.named_vector]
    vector_state.named_vector =
        NamedArray(comparison_values, vector_state.named_vector.dicts, vector_state.named_vector.dimnames)

    push!(query_state.stack, vector_state)
    return nothing
end

function apply_mask_to_axis_state(
    query_state::QueryState,
    axis_state::AxisState,
    mask_state::VectorState,
    mask_operation::MaskOperation,
)::Nothing
    mask_vector = mask_state.named_vector.array
    if eltype(mask_vector) <: AbstractString
        mask_vector = mask_vector .!= ""
    elseif eltype(mask_vector) != Bool
        mask_vector = mask_vector .!= 0
    end
    @assert eltype(mask_vector) <: Bool

    axis_mask = axis_state.axis_modifier
    if axis_mask === nothing
        axis_mask = ones(Bool, axis_length(query_state.daf, axis_state.axis_name))
        axis_state.axis_modifier = axis_mask
    end
    @assert axis_mask isa AbstractVector{Bool}

    mask_mask = mask_state.axis_state.axis_modifier

    if mask_mask === nothing
        update_axis_mask(axis_mask, mask_vector, mask_operation)
    else
        axis_mask .&= mask_mask
        @views update_axis_mask(axis_mask[mask_mask], mask_vector, mask_operation)
    end

    axis_state.axis_modifier = axis_mask
    union!(axis_state.dependency_keys, mask_state.dependency_keys)
    return nothing
end

function apply_query_operation!(query_state::QueryState, as_axis::AsAxis)::Nothing
    if is_all(query_state, (VectorState,))
        vector_state = query_state.stack[1]
        @assert vector_state isa VectorState
        if !vector_state.is_processed && peek_next_operation(query_state, CountBy) !== nothing
            push!(query_state.stack, as_axis)
            return nothing
        end
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, ::AsAxis)::Nothing
    if is_all(fake_query_state, (FakeVectorState,))
        fake_vector_state = fake_query_state.stack[1]
        @assert fake_vector_state isa FakeVectorState
        if !fake_vector_state.is_processed && peek_next_operation(fake_query_state, CountBy) !== nothing
            return nothing
        end
    end

    return error_unexpected_operation(fake_query_state)
end

function apply_query_operation!(query_state::QueryState, count_by::CountBy)::Nothing
    if is_all(query_state, (VectorState, AsAxis))
        as_axis = pop!(query_state.stack)
        @assert as_axis isa AsAxis
        return fetch_count_by(query_state, count_by, as_axis)
    elseif is_all(query_state, (VectorState,))
        return fetch_count_by(query_state, count_by, nothing)
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, count_by::CountBy)::Nothing
    if is_all(fake_query_state, (FakeVectorState,))
        return fake_fetch_count_by(fake_query_state, count_by)
    end

    return error_unexpected_operation(fake_query_state)
end

function fetch_count_by(query_state::QueryState, count_by::CountBy, rows_as_axis::Maybe{AsAxis})::Nothing
    rows_vector_state = pop!(query_state.stack)
    @assert rows_vector_state isa VectorState
    rows_axis_state = rows_vector_state.axis_state
    @assert rows_axis_state !== nothing

    fetch_property(query_state, rows_axis_state, count_by)
    columns_vector_state = pop!(query_state.stack)
    @assert columns_vector_state isa VectorState
    columns_axis_state = columns_vector_state.axis_state
    @assert columns_axis_state !== nothing

    columns_as_axis = get_next_operation(query_state, AsAxis)

    apply_mask_to_base_vector_state(rows_vector_state, columns_vector_state)
    rows_name, rows_values, rows_index_of_value = unique_values(query_state, rows_vector_state, rows_as_axis, true)
    columns_name, columns_values, columns_index_of_value =
        unique_values(query_state, columns_vector_state, columns_as_axis, true)
    @assert rows_index_of_value !== nothing
    @assert columns_index_of_value !== nothing

    rows_names = values_to_names(rows_values)
    columns_names = values_to_names(columns_values)

    count_by_matrix = compute_count_by(
        rows_vector_state.named_vector.array,
        rows_values,
        rows_index_of_value,
        columns_vector_state.named_vector.array,
        columns_values,
        columns_index_of_value,
    )
    named_matrix = NamedArray(count_by_matrix, (rows_names, columns_names), (rows_name, columns_name))

    dependency_keys = union(rows_vector_state.dependency_keys, columns_vector_state.dependency_keys)

    matrix_state = MatrixState(
        query_state_sequence(query_state),
        dependency_keys,
        named_matrix,
        rows_vector_state.property_name,
        columns_vector_state.property_name,
        rows_axis_state,
        columns_axis_state,
    )
    push!(query_state.stack, matrix_state)
    return nothing
end

function fake_fetch_count_by(fake_query_state::FakeQueryState, ::CountBy)::Nothing
    rows_vector_state = pop!(fake_query_state.stack)
    @assert rows_vector_state isa FakeVectorState

    fake_fetch_property(fake_query_state, FakeAxisState(nothing, false, false))
    columns_vector_state = pop!(fake_query_state.stack)
    @assert columns_vector_state isa FakeVectorState

    get_next_operation(fake_query_state, AsAxis)

    push!(fake_query_state.stack, FakeMatrixState())
    return nothing
end

function apply_mask_to_base_vector_state(base_vector_state::VectorState, masked_vector_state::VectorState)::Nothing
    base_axis_state = base_vector_state.axis_state
    @assert base_axis_state !== nothing
    base_axis_mask = base_axis_state.axis_modifier

    masked_axis_state = masked_vector_state.axis_state
    @assert masked_axis_state !== nothing
    masked_axis_mask = masked_axis_state.axis_modifier

    if base_axis_mask != masked_axis_mask
        @assert masked_axis_mask isa AbstractVector{Bool}
        @assert base_axis_mask === nothing || !any(masked_axis_mask .& .!base_axis_mask)  # NOJET
        apply_mask_to_vector_state(base_vector_state, masked_axis_mask)
    end
end

function apply_mask_to_base_matrix_state(base_matrix_state::MatrixState, masked_vector_state::VectorState)::Nothing
    base_axis_state = base_matrix_state.rows_axis_state
    @assert base_axis_state !== nothing
    base_axis_mask = base_axis_state.axis_modifier

    masked_axis_state = masked_vector_state.axis_state
    @assert masked_axis_state !== nothing
    masked_axis_mask = masked_axis_state.axis_modifier

    if base_axis_mask != masked_axis_mask
        @assert masked_axis_mask isa AbstractVector{Bool}
        @assert base_axis_mask === nothing || !any(masked_axis_mask .& .!base_axis_mask)
        apply_mask_to_matrix_state_rows(base_matrix_state, masked_axis_mask)
    end
end

function unique_values(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    need_index_of_values::Bool,
)::Tuple{AbstractString, StorageVector, Maybe{Dict}}
    property_name = vector_state.property_name

    if as_axis === nothing
        values = unique(vector_state.named_vector)
        sort!(values)
        if !need_index_of_values
            index_of_value = nothing
        else
            index_of_value = Dict{eltype(values), Int32}()
            for (index, value) in enumerate(values)
                index_of_value[value] = index
            end
        end
        return (property_name, values, index_of_value)

    else
        axis_name = axis_of_property(query_state.daf, property_name, as_axis)
        entry_names = get_vector(query_state.daf, axis_name, "name")
        return (axis_name, entry_names.array, entry_names.dicts[1])
    end
end

function values_to_names(values::StorageVector)::Vector{String}
    return [string(value) for value in values]
end

function compute_count_by(
    rows_vector::StorageVector,
    rows_values::StorageVector,
    rows_index_of_value::Dict,
    columns_vector::StorageVector,
    columns_values::StorageVector,
    columns_index_of_value::Dict,
)::Matrix
    @assert length(rows_vector) == length(columns_vector)

    matrix_type = UInt64
    for type in (UInt32, UInt16, UInt8)
        if length(rows_vector) <= typemax(type)
            matrix_type = type
        end
    end

    counts_matrix = zeros(matrix_type, length(rows_values), length(columns_values))

    for (row_value, column_value) in zip(rows_vector, columns_vector)
        row_index = get(rows_index_of_value, row_value, nothing)
        column_index = get(columns_index_of_value, column_value, nothing)
        if row_index !== nothing && column_index !== nothing
            @inbounds counts_matrix[row_index, column_index] += 1
        end
    end

    return counts_matrix
end

function apply_mask_to_vector_state(vector_state::VectorState, new_axis_mask::AbstractVector{Bool})::Nothing
    axis_state = vector_state.axis_state
    @assert axis_state !== nothing
    old_axis_mask = axis_state.axis_modifier
    if old_axis_mask === nothing
        vector_state.named_vector = vector_state.named_vector[new_axis_mask]  # NOJET
        axis_state.axis_modifier = new_axis_mask
    else
        sub_axis_mask = new_axis_mask[old_axis_mask]
        vector_state.named_vector = vector_state.named_vector[sub_axis_mask]
        axis_state.axis_modifier = new_axis_mask
    end
    return nothing
end

function apply_mask_to_matrix_state_rows(matrix_state::MatrixState, new_rows_mask::AbstractVector{Bool})::Nothing
    rows_axis_state = matrix_state.rows_axis_state
    @assert rows_axis_state !== nothing
    old_rows_mask = rows_axis_state.axis_modifier
    if old_rows_mask === nothing
        matrix_state.named_matrix = matrix_state.named_matrix[new_rows_mask, :]  # NOJET
        rows_axis_state.axis_modifier = new_rows_mask
    else
        sub_rows_mask = new_rows_mask[old_rows_mask]
        matrix_state.named_matrix = matrix_state.named_matrix[sub_rows_mask, :]
        rows_axis_state.axis_modifier = new_rows_mask
    end
    return nothing
end

function apply_query_operation!(query_state::QueryState, group_by::GroupBy)::Nothing
    if is_all(query_state, (AxisState,))
        apply_query_operation!(query_state, Lookup("name"))
    end
    if is_all(query_state, (VectorState,))
        return fetch_group_by_vector(query_state, group_by)
    elseif is_all(query_state, (MatrixState,))
        return fetch_group_by_matrix(query_state, group_by)
    end

    return error_unexpected_operation(query_state)  # untested
end

function fake_query_operation!(fake_query_state::FakeQueryState, ::GroupBy)::Nothing
    if is_all(fake_query_state, (FakeAxisState,))
        fake_query_operation!(fake_query_state, Lookup("name"))
    end
    if is_all(fake_query_state, (FakeVectorState,))
        return fake_fetch_group_by_vector(fake_query_state)
    elseif is_all(fake_query_state, (FakeMatrixState,))
        return fake_fetch_group_by_matrix(fake_query_state)
    end

    return error_unexpected_operation(fake_query_state)  # untested
end

function fetch_group_by_vector(query_state::QueryState, group_by::GroupBy)::Nothing
    values_vector_state = pop!(query_state.stack)
    @assert values_vector_state isa VectorState
    axis_state = values_vector_state.axis_state
    @assert axis_state !== nothing

    parsed_group_by = parse_group_by(query_state, axis_state, group_by)
    if parsed_group_by === nothing
        return nothing
    end
    groups_vector_state, groups_values, groups_names, groups_name, reduction_operation, if_missing = parsed_group_by

    apply_mask_to_base_vector_state(values_vector_state, groups_vector_state)

    group_by_values = compute_vector_group_by(
        query_state,
        values_vector_state.named_vector.array,
        groups_vector_state.named_vector.array,
        groups_values,
        reduction_operation,
        if_missing,
    )
    named_vector = NamedArray(group_by_values, (groups_names,), (groups_name,))

    dependency_keys = union(values_vector_state.dependency_keys, groups_vector_state.dependency_keys)

    vector_state = VectorState(
        query_state.query_sequence,
        dependency_keys,
        named_vector,
        values_vector_state.property_name,
        nothing,
        true,
    )
    push!(query_state.stack, vector_state)
    return nothing
end

function fake_fetch_group_by_vector(fake_query_state::FakeQueryState)::Nothing
    fake_values_vector_state = pop!(fake_query_state.stack)
    @assert fake_values_vector_state isa FakeVectorState
    if fake_parse_group_by(fake_query_state)
        push!(fake_query_state.stack, FakeVectorState(true))
    end
    return nothing
end

function fetch_group_by_matrix(query_state::QueryState, group_by::GroupBy)::Nothing
    values_matrix_state = pop!(query_state.stack)
    @assert values_matrix_state isa MatrixState
    axis_state = values_matrix_state.rows_axis_state
    @assert axis_state !== nothing

    parsed_group_by = parse_group_by(query_state, axis_state, group_by)
    if parsed_group_by === nothing
        return nothing
    end
    groups_vector_state, groups_values, groups_names, groups_name, reduction_operation, if_missing = parsed_group_by

    columns_axis_state = values_matrix_state.columns_axis_state
    @assert columns_axis_state !== nothing

    columns_names = axis_array(query_state.daf, columns_axis_state.axis_name)
    axis_mask = columns_axis_state.axis_modifier
    if axis_mask !== nothing
        @assert axis_mask isa AbstractVector{Bool}
        columns_names = columns_names[axis_mask]
    end

    apply_mask_to_base_matrix_state(values_matrix_state, groups_vector_state)

    group_by_values = compute_matrix_group_by(
        query_state,
        values_matrix_state.named_matrix.array,
        groups_vector_state.named_vector.array,
        groups_values,
        reduction_operation,
        if_missing,
    )
    named_matrix =
        NamedArray(group_by_values, (groups_names, columns_names), (groups_name, columns_axis_state.axis_name))

    dependency_keys = union(values_matrix_state.dependency_keys, groups_vector_state.dependency_keys)

    matrix_state = MatrixState(
        query_state_sequence(query_state),
        dependency_keys,
        named_matrix,
        values_matrix_state.rows_property_name,
        values_matrix_state.columns_property_name,
        nothing,
        columns_axis_state,
    )
    push!(query_state.stack, matrix_state)
    return nothing
end

function fake_fetch_group_by_matrix(fake_query_state::FakeQueryState)::Nothing
    fake_values_matrix_state = pop!(fake_query_state.stack)
    @assert fake_values_matrix_state isa FakeMatrixState
    if fake_parse_group_by(fake_query_state)
        push!(fake_query_state.stack, FakeMatrixState())
    end
    return nothing
end

function parse_group_by(
    query_state::QueryState,
    axis_state::AxisState,
    group_by::GroupBy,
)::Maybe{
    Tuple{
        VectorState,
        StorageVector,
        AbstractVector{<:AbstractString},
        AbstractString,
        ReductionOperation,
        Maybe{IfMissing},
    },
}
    fetch_property(query_state, axis_state, group_by)
    groups_vector_state = pop!(query_state.stack)
    @assert groups_vector_state isa VectorState
    groups_as_axis = get_next_operation(query_state, AsAxis)

    reduction_operation = get_next_operation(query_state, ReductionOperation)
    if reduction_operation === nothing
        push!(query_state.stack, group_by)
        return nothing
    end

    if groups_as_axis === nothing
        if_missing = nothing
    else
        if_missing = get_next_operation(query_state, IfMissing)
    end

    groups_name, groups_values, _ = unique_values(query_state, groups_vector_state, groups_as_axis, false)
    groups_names = values_to_names(groups_values)

    return (groups_vector_state, groups_values, groups_names, groups_name, reduction_operation, if_missing)
end

function fake_parse_group_by(fake_query_state::FakeQueryState)::Bool
    fake_fetch_property(fake_query_state, FakeAxisState(nothing, false, false))
    groups_vector_state = pop!(fake_query_state.stack)
    @assert groups_vector_state isa FakeVectorState
    groups_as_axis = get_next_operation(fake_query_state, AsAxis)

    reduction_operation = get_next_operation(fake_query_state, ReductionOperation)
    if reduction_operation === nothing
        push!(fake_query_state.stack, FakeGroupBy())
        return false
    end

    if groups_as_axis !== nothing
        get_next_operation(fake_query_state, IfMissing)
    end

    return true
end

function compute_vector_group_by(
    query_state::QueryState,
    values_vector::StorageVector,
    groups_vector::StorageVector,
    groups_values::StorageVector,
    reduction_operation::ReductionOperation,
    if_missing::Maybe{IfMissing},
)::StorageVector
    dtype = reduction_result_type(reduction_operation, eltype(values_vector))
    results_vector = Vector{dtype}(undef, length(groups_values))

    if if_missing === nothing
        empty_group_value = nothing
    else
        empty_group_value = value_for_if_missing(query_state, if_missing; dtype = dtype)
    end

    collect_vector_group_by(
        query_state,
        results_vector,
        values_vector,
        groups_vector,
        groups_values,
        empty_group_value,
        reduction_operation,
    )

    return results_vector
end

function compute_matrix_group_by(
    query_state::QueryState,
    values_matrix::StorageMatrix,
    groups_vector::StorageVector,
    groups_values::StorageVector,
    reduction_operation::ReductionOperation,
    if_missing::Maybe{IfMissing},
)::StorageMatrix
    dtype = reduction_result_type(reduction_operation, eltype(values_matrix))
    results_matrix = Matrix{dtype}(undef, length(groups_values), size(values_matrix)[2])

    if if_missing === nothing
        empty_group_value = nothing
    else
        empty_group_value = value_for_if_missing(query_state, if_missing; dtype = dtype)
    end

    @threads for column_index in 1:size(values_matrix)[2]
        values_column = @views values_matrix[:, column_index]
        results_column = @views results_matrix[:, column_index]
        collect_vector_group_by(
            query_state,
            results_column,
            values_column,
            groups_vector,
            groups_values,
            empty_group_value,
            reduction_operation,
        )
    end  # untested

    return results_matrix
end

function collect_vector_group_by(
    query_state::QueryState,
    results_vector::StorageVector,
    values_vector::StorageVector,
    groups_vector::StorageVector,
    groups_values::StorageVector,
    empty_group_value::Maybe{StorageScalar},
    reduction_operation::ReductionOperation,
)::Nothing
    n_groups = length(groups_values)
    @threads for group_index in 1:n_groups
        group_value = groups_values[group_index]
        group_mask = groups_vector .== group_value
        values_of_group = values_vector[group_mask]
        if length(values_of_group) > 0
            results_vector[group_index] = compute_reduction(reduction_operation, read_only_array(values_of_group))  # NOLINT
        elseif empty_group_value !== nothing
            results_vector[group_index] = empty_group_value
        else
            error_at_state(query_state, dedent("""
                                            no values for the group: $(group_value)
                                            and no IfMissing value was specified: || value_for_empty_groups
                                        """))
        end
    end  # untested
end

function apply_query_operation!(query_state::QueryState, eltwise_operation::EltwiseOperation)::Nothing
    if is_all(query_state, (ScalarState,))
        eltwise_scalar(query_state, eltwise_operation)
        return nothing
    end

    if is_all(query_state, (VectorState,))
        eltwise_vector(query_state, eltwise_operation)
        return nothing
    end

    if is_all(query_state, (MatrixState,))
        eltwise_matrix(query_state, eltwise_operation)
        return nothing
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, ::EltwiseOperation)::Nothing
    if is_all(fake_query_state, (FakeScalarState,)) ||
       is_all(fake_query_state, (FakeVectorState,)) ||
       is_all(fake_query_state, (FakeMatrixState,))
        return nothing
    end

    return error_unexpected_operation(fake_query_state)
end

function eltwise_scalar(query_state::QueryState, eltwise_operation::EltwiseOperation)::Nothing
    scalar_state = pop!(query_state.stack)
    @assert scalar_state isa ScalarState

    scalar_value = scalar_state.scalar_value
    if scalar_value isa AbstractString
        error_at_state(query_state, dedent("""
                                        unsupported input type: String
                                        for the eltwise operation: $(typeof(eltwise_operation))
                                    """))
    end

    scalar_state.scalar_value = compute_eltwise(eltwise_operation, scalar_value)  # NOLINT

    push!(query_state.stack, scalar_state)
    return nothing
end

function eltwise_vector(query_state::QueryState, eltwise_operation::EltwiseOperation)::Nothing
    vector_state = pop!(query_state.stack)
    @assert vector_state isa VectorState

    if eltype(vector_state.named_vector) <: AbstractString
        error_at_state(query_state, dedent("""
                                        unsupported input type: $(eltype(vector_state.named_vector))
                                        for the eltwise operation: $(typeof(eltwise_operation))
                                    """))
    end

    vector_value = compute_eltwise(eltwise_operation, read_only_array(vector_state.named_vector.array))  # NOLINT
    vector_state.named_vector =
        NamedArray(vector_value, vector_state.named_vector.dicts, vector_state.named_vector.dimnames)
    vector_state.is_processed = true

    push!(query_state.stack, vector_state)
    return nothing
end

function eltwise_matrix(query_state::QueryState, eltwise_operation::EltwiseOperation)::Nothing
    matrix_state = pop!(query_state.stack)
    @assert matrix_state isa MatrixState

    matrix_value = compute_eltwise(eltwise_operation, read_only_array(matrix_state.named_matrix.array))  # NOLINT
    matrix_state.named_matrix =
        NamedArray(matrix_value, matrix_state.named_matrix.dicts, matrix_state.named_matrix.dimnames)

    push!(query_state.stack, matrix_state)
    return nothing
end

function apply_query_operation!(query_state::QueryState, reduction_operation::ReductionOperation)::Nothing
    if is_all(query_state, (VectorState,))
        reduce_vector(query_state, reduction_operation)
        return nothing
    end

    if is_all(query_state, (MatrixState,))
        reduce_matrix(query_state, reduction_operation)
        return nothing
    end

    return error_unexpected_operation(query_state)
end

function fake_query_operation!(fake_query_state::FakeQueryState, reduction_operation::ReductionOperation)::Nothing  # NOLINT
    if is_all(fake_query_state, (FakeVectorState,))
        fake_reduce_vector(fake_query_state)
        return nothing
    elseif is_all(fake_query_state, (FakeMatrixState,))
        fake_reduce_matrix(fake_query_state)
        return nothing
    end

    return error_unexpected_operation(fake_query_state)
end

function reduce_vector(query_state::QueryState, reduction_operation::ReductionOperation)::Nothing
    vector_state = pop!(query_state.stack)
    @assert vector_state isa VectorState

    if eltype(vector_state.named_vector) <: AbstractString
        error_at_state(query_state, dedent("""
                                        unsupported input type: $(eltype(vector_state.named_vector))
                                        for the reduction operation: $(typeof(reduction_operation))
                                    """))
    end

    scalar_value = compute_reduction(reduction_operation, read_only_array(vector_state.named_vector.array))  # NOLINT

    scalar_state = ScalarState(query_state.query_sequence, vector_state.dependency_keys, scalar_value)
    push!(query_state.stack, scalar_state)
    return nothing
end

function fake_reduce_vector(fake_query_state::FakeQueryState)::Nothing
    fake_vector_state = pop!(fake_query_state.stack)
    @assert fake_vector_state isa FakeVectorState
    push!(fake_query_state.stack, FakeScalarState())
    return nothing
end

function reduce_matrix(query_state::QueryState, reduction_operation::ReductionOperation)::Nothing
    matrix_state = pop!(query_state.stack)
    @assert matrix_state isa MatrixState

    named_matrix = matrix_state.named_matrix
    vector_value = compute_reduction(reduction_operation, read_only_array(named_matrix.array))  # NOLINT
    named_vector = NamedArray(vector_value, named_matrix.dicts[2:2], named_matrix.dimnames[2:2])

    vector_state = VectorState(
        query_state.query_sequence,
        matrix_state.dependency_keys,
        named_vector,
        matrix_state.columns_property_name,
        matrix_state.columns_axis_state,
        true,
    )

    push!(query_state.stack, vector_state)
    return nothing
end

function fake_reduce_matrix(fake_query_state::FakeQueryState)::Nothing
    fake_matrix_state = pop!(fake_query_state.stack)
    @assert fake_matrix_state isa FakeMatrixState
    push!(fake_query_state.stack, FakeVectorState(true))
    return nothing
end

function is_all(query_state::Union{QueryState, FakeQueryState}, types::NTuple{N, Type})::Bool where {N}
    return length(query_state.stack) == length(types) && has_top(query_state, types)
end

function has_top(query_state::Union{QueryState, FakeQueryState}, types::NTuple{N, Type})::Bool where {N}
    if length(query_state.stack) < length(types)
        return false  # untested
    end

    for (query_operation, type) in zip(query_state.stack, types)
        if !(query_operation isa type)
            return false
        end
    end

    return true
end

"""
    guess_typed_value(value::AbstractString)::StorageScalar

Given a string value, guess the typed value it represents:

  - `true` and `false` are assumed to be `Bool`.
  - Integers are assumed to be `Int64`.
  - Floating point numbers are assumed to be `Float64`, as are `e` and `pi`.
  - Anything else is assumed to be a string.

This doesn't have to be 100% accurate; it is intended to allow omitting the data type in most cases when specifying an
[`IfMissing`](@ref) value. If it guesses wrong, just specify an explicit type (e.g., `@ version || 1.0 String`).
"""
function guess_typed_value(value::AbstractString)::StorageScalar
    for (string_value, typed_value) in (("true", true), ("false", false), ("e", Float64(e)), ("pi", Float64(pi)))
        if value == string_value
            return typed_value
        end
    end

    try
        return parse(Int64, value)
    catch
    end

    try
        return parse(Float64, value)
    catch
    end

    return string(value)
end

function value_for_if_missing(
    query_state::QueryState,
    if_missing::IfMissing;
    dtype::Maybe{Type} = nothing,
)::StorageScalar
    if if_missing.dtype !== nothing
        @assert if_missing.missing_value isa if_missing.dtype
        return if_missing.missing_value
    end

    if dtype === nothing
        return guess_typed_value(if_missing.missing_value)
    end

    if if_missing.missing_value isa dtype
        return if_missing.missing_value  # untested
    end

    return value_for(query_state, dtype, if_missing.missing_value)
end

function regex_for(query_state::QueryState, value::StorageScalar)::Regex
    comparison_value = value_for(query_state, String, value)  # NOJET
    comparison_value = "^(:?" * comparison_value * ")\$"  # NOJET
    try
        return Regex(comparison_value)  # NOJET
    catch exception
        error_at_state(query_state, dedent("""
                                        $(typeof(exception)): $(exception.msg)
                                        in the regular expression: $(comparison_value)
                                    """))
    end
end

function value_for(
    query_state::QueryState,
    ::Type{T},
    value::StorageScalar,
)::StorageScalar where {T <: StorageScalarBase}
    if value isa T
        return value
    elseif T <: AbstractString
        return String(value)  # untested
    elseif value isa AbstractString
        try
            return parse(T, value)
        catch exception
            error_at_state(query_state, "$(typeof(exception)): $(exception.msg)")
        end
    else
        try  # untested
            return T(value)  # untested
        catch exception
            error_at_state(query_state, "$(typeof(exception)): $(exception.msg)")  # untested
        end
    end
end

"""
Specify a column for [`get_frame`](@ref) for some axis. The most generic form is a pair `"column_name" => query`. Two
shorthands apply: the pair `"column_name" => "="` is a shorthand for the pair `"column_name" => ": column_name"`, and
so is the shorthand `"column_name"` (simple string).

We also allow specifying tuples instead of pairs to make it easy to invoke the API from other languages such as Python
which do not have the concept of a `Pair`.

The query is combined with the axis query as follows (using [`full_vector_query`](@ref):

  - If the query contains [`GroupBy`](@ref), then the query must repeat any mask specified for the axis query.
    That is, if the axis query is `metacell & type = B`, then the column query must be
    `/ cell & metacell => type = B @ metacell : age %> Mean`. Sorry for the inconvenience. TODO: Automatically inject the
    mask into [`GroupBy`](@ref) column queries.
  - Otherwise, if the query starts with a (single) axis, then it should only contain a reduction; the axis query is
    automatically injected following it. That is, if the axis query is `gene & is_marker`, then the full query for the
    column query `/ metacell : fraction %> Mean` will be `/ metacell / gene : fraction %> Mean` (the mean gene expression
    in all metacells). We can't just concatenate the axis query and the columns query here, is because Julia, in its
    infinite wisdom, uses column-major matrices, like R and matlab; so reduction eliminates the rows instead of the
    columns of the matrix.
  - Otherwise (the typical case), we simply concatenate the axis query and the column query. That is, of the axis query
    is `cell & batch = B1` and the column query is `: age`, then the full query will be `cell & batch = B1 : age`. This
    is the simplest and most common case.

In all cases the (full) query must return a value for each entry of the axis.
"""
FrameColumn = Union{AbstractString, Tuple{AbstractString, QueryString}, Pair{<:AbstractString, <:QueryString}}

"""
Specify all the columns to collect for a frame. We would have liked to specify this as `AbstractVector{<:FrameColumn}`
but Julia in its infinite wisdom considers `["a", "b" => "c"]` to be a `Vector{Any}`, which would require literals
to be annotated with the type.
"""
FrameColumns = AbstractVector

"""
    get_frame(
        daf::DafReader,
        axis::QueryString,
        [columns::Maybe{FrameColumns} = nothing;
        cache::Bool = true]
    )::DataFrame end

Return a `DataFrame` containing multiple vectors of the same `axis`.

The `axis` can be either just the name of an axis (e.g., `"cell"`), or a query for the axis (e.g., `q"/ cell"`),
possibly using a mask (e.g., `q"/ cell & age > 1"`). The result of the query must be a vector of unique axis entry
names.

If `columns` is not specified, the data frame will contain all the vector properties of the axis, in alphabetical order
(since `DataFrame` has no concept of named rows, the 1st column will contain the name of the axis entry).

By default, this will cache results of all queries. This may consume a large amount of memory. You can disable it by
specifying `cache = false`, or release the cached data using [`empty_cache!`](@ref).
"""
function get_frame(
    daf::DafReader,
    axis::QueryString,
    columns::Maybe{FrameColumns} = nothing;
    cache::Bool = true,
)::DataFrame
    if columns !== nothing
        for column in columns
            @assert column isa FrameColumn "invalid FrameColumn: $(column)"
        end
    end

    if axis isa Query
        axis_query = axis
        axis_name = query_axis_name(axis)
    else
        axis_query = Query(axis, Axis)
        axis_name = query_axis_name(axis_query)
    end

    names_of_rows = get_query(daf, axis_query; cache = cache)
    @assert names_of_rows isa AbstractVector{<:AbstractString}

    if columns === nothing
        columns = sort!(collect(vectors_set(daf, axis_name)))
        insert!(columns, 1, "name")
    end

    if eltype(columns) <: AbstractString
        columns = [column => Lookup(column) for column in columns]
    end

    data = Vector{Pair{AbstractString, StorageVector}}()
    for frame_column in columns
        @assert frame_column isa FrameColumn "invalid FrameColumn: $(frame_column)"
        if frame_column isa AbstractString
            column_name = frame_column
            column_query = "="
        else
            column_name, column_query = frame_column
        end
        if column_query == "="
            column_query = ": " * column_name
        end
        column_query = full_vector_query(axis_query, column_query, column_name)
        vector = get_query(daf, column_query; cache = cache)
        if !(vector isa StorageVector) || !(vector isa NamedArray) || names(vector, 1) != names_of_rows
            error(dedent("""
                invalid column query: $(column_query)
                for the axis query: $(axis_query)
                of the daf data: $(daf.name)
            """))
        end
        push!(data, column_name => vector.array)
    end

    result = DataFrame(data)
    @debug "get_frame daf: $(depict(daf)) axis: $(axis) columns: $(columns) result: $(depict(result))"
    return result
end

function query_axis_name(query::Query)::AbstractString
    return query_axis_name(QuerySequence((query,)))
end

function query_axis_name(query_sequence::QuerySequence)::AbstractString
    return get_query_axis_name(get_fake_query_result(query_sequence))
end

function get_query_axis_name(fake_query_state::FakeQueryState)::AbstractString
    if is_all(fake_query_state, (FakeAxisState,))
        fake_axis_state = fake_query_state.stack[1]
        @assert fake_axis_state isa FakeAxisState
        if fake_axis_state.axis_name !== nothing && !fake_axis_state.is_entry
            return fake_axis_state.axis_name
        end
    end

    return error("invalid axis query: $(fake_query_state.query_sequence)")
end

"""
    full_vector_query(
        axis_query::Query,
        vector_query::QueryString,
        vector_name::Maybe{AbstractString} = nothing,
    )::Query

Given a query for an axis, and some suffix query for a vector property, combine them into a full query for the vector
values for the axis. This is used by [`FrameColumn`](@ref) for [`get_frame`](@ref) and also for queries of vector data
in views.
"""
function full_vector_query(
    axis_query::Query,
    vector_query::QueryString,
    vector_name::Maybe{AbstractString} = nothing,
)::Query
    if vector_name !== nothing && vector_query == "="
        vector_query = Lookup(vector_name)
    else
        vector_query = Query(vector_query, Lookup)
    end
    if vector_query isa QuerySequence && vector_query.query_operations[1] isa Axis
        if !any([query_operation isa GroupBy for query_operation in vector_query.query_operations])
            query_prefix, query_suffix = split_vector_query(vector_query)
            vector_query = query_prefix |> axis_query |> query_suffix
        end
    else
        vector_query = axis_query |> vector_query
    end

    return vector_query
end

function split_vector_query(query_sequence::QuerySequence)::Tuple{QuerySequence, QuerySequence}
    index = findfirst(query_sequence.query_operations) do query_operation
        return query_operation isa Lookup
    end
    if index === nothing
        return (query_sequence, QuerySequence(()))  # untested
    else
        return (
            QuerySequence(query_sequence.query_operations[1:(index - 1)]),
            QuerySequence(query_sequence.query_operations[index:end]),
        )
    end
end

function Query(query::Query, ::Maybe{Union{Type{Lookup}, Type{Axis}}} = nothing)::Query
    return query
end

function Query(query_string::AbstractString, operand_only::Maybe{Union{Type{Lookup}, Type{Axis}}} = nothing)::Query
    tokens = tokenize(query_string, QUERY_OPERATORS)
    if operand_only !== nothing && length(tokens) == 1 && !tokens[1].is_operator
        return operand_only(query_string)  # NOJET
    end

    next_token_index = 1
    query_operations = Vector{QueryOperation}()
    while next_token_index <= length(tokens)
        query_operation, next_token_index = next_query_operation(tokens, next_token_index)
        push!(query_operations, query_operation)
    end

    return QuerySequence(Tuple(query_operations))
end

end  # module
