"""
Extract data from a [`DafReader`](@ref).
"""
module Queries

export @q_str
export AndMask
export AndNegatedMask
export AsAxis
export Axis
export BeginMask
export BeginNegatedMask
export CountBy
export EndMask
export FrameColumn
export full_vector_query
export get_frame
export get_query
export GroupBy
export GroupColumnsBy
export GroupRowsBy
export has_query
export IfMissing
export IfNot
export is_axis_query
export IsEqual
export IsGreater
export IsGreaterEqual
export IsLess
export IsLessEqual
export IsMatch
export IsNotEqual
export IsNotMatch
export LookupMatrix
export LookupScalar
export LookupVector
export Names
export OrMask
export OrNegatedMask
export parse_query
export Query
export query_axis_name
export query_requires_relayout
export query_result_dimensions
export QuerySequence
export QueryString
export ReduceToColumn
export ReduceToRow
export SquareColumnIs
export SquareRowIs
export XorMask
export XorNegatedMask

using ..Formats
using ..Operations
using ..Readers
using ..Registry
using ..StorageTypes
using ..Tokens
using Base.Threads
using DataFrames
using NamedArrays
using SparseArrays
using TanayLabUtilities

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
A query returning a set of names. Valid phrases are:

  - Looking up the set of names of the scalar properties (`?`). Example:

```jldoctest
cells = example_cells_daf()
cells["?"]

# output

KeySet for a Dict{AbstractString, Union{Bool, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8, AbstractString}} with 2 entries. Keys:
  "organism"
  "reference"
```

  - Looking up the set of names of the axes (`@ ?`). Example:

```jldoctest
cells = example_cells_daf()
cells["@ ?"]

# output

KeySet for a Dict{AbstractString, AbstractVector{<:AbstractString}} with 4 entries. Keys:
  "gene"
  "experiment"
  "donor"
  "cell"
```

  - Looking up the set of names of the vector properties of an axis (e.g., `@ cell ?`).

```jldoctest
cells = example_cells_daf()
cells["@ gene ?"]

# output

KeySet for a Dict{AbstractString, AbstractVector{T} where T<:(Union{Bool, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8, S} where S<:AbstractString)} with 1 entry. Keys:
  "is_lateral"
```

  - Looking up the set of names of the matrix properties of a pair of axes (e.g., `@ cell @ gene ?`).

```jldoctest
cells = example_cells_daf()
cells["@ cell @ gene ?"]

# output

Set{AbstractString} with 1 element:
  "UMIs"
```

[**Syntax diagram:**](assets/names.svg)

![](assets/names.svg)
"""
const NAMES_QUERY = nothing

"""
A query returning a scalar result. Valid phrases are:

  - Looking up a scalar property (`. scalar-property`, `. scalar-property || default-value`).
  - Looking up a vector, and picking a specific entry in it (`: vector-property @ axis = entry`, `: vector-property || default-value @ axis = entry`).
  - Looking up a matrix, and picking a specific entry in it (`:: matrix-property @ rows-axis = row-entry @ columns-axis = column-entry`, `:: matrix-property || default-value @ rows-axis = row-entry @ columns-axis = column-entry`).

In addition, you can use [`EltwiseOperation`](@ref) and [`ReductionOperation`](@ref):

  - Transform any scalar (...scalar... `% Eltwise operation...`). Actually, we don't currently have any element-wise
    operations that apply to strings, but we can add some if useful.

  - Reduce any vector to a scalar (...vector... `>> Reduction operation...`) - see [`VECTOR_QUERY`](@ref). Example:

```jldoctest
cells = example_cells_daf()
# Number of genes which are marked as lateral.
cells["@ gene : is_lateral >> Sum type Int64"]

# output

438
```

  - Reduce any matrix to a scalar (...matrix... `>> Reduction operation...`) - see [`MATRIX_QUERY`](@ref). Example:

```jldoctest
cells = example_cells_daf()
# Total number of measured UMIs in the data.
cells["@ cell @ gene :: UMIs >> Sum type Int64"]

# output

1171936
```

[**Syntax diagram:**](assets/scalar.svg)

![](assets/scalar.svg)
"""
const SCALAR_QUERY = nothing

"""
A query returning a vector result. Valid phrases are:

  - Looking up a vector axis (`@ axis`). This gives us a vector of the axis entries. Example:

```jldoctest
cells = example_cells_daf()
cells["@ experiment"]

# output

23-element Named SparseArrays.ReadOnly{SubString{StringViews.StringView{Vector{UInt8}}}, 1, Vector{SubString{StringViews.StringView{Vector{UInt8}}}}}
experiment       │
─────────────────┼───────────────────
demux_01_02_21_1 │ "demux_01_02_21_1"
demux_01_02_21_2 │ "demux_01_02_21_2"
demux_01_03_21_1 │ "demux_01_03_21_1"
demux_04_01_21_1 │ "demux_04_01_21_1"
demux_04_01_21_2 │ "demux_04_01_21_2"
demux_07_03_21_1 │ "demux_07_03_21_1"
demux_07_03_21_2 │ "demux_07_03_21_2"
demux_07_12_20_1 │ "demux_07_12_20_1"
⋮                                   ⋮
demux_21_02_21_1 │ "demux_21_02_21_1"
demux_21_02_21_2 │ "demux_21_02_21_2"
demux_21_12_20_1 │ "demux_21_12_20_1"
demux_21_12_20_2 │ "demux_21_12_20_2"
demux_22_02_21_1 │ "demux_22_02_21_1"
demux_22_02_21_2 │ "demux_22_02_21_2"
demux_28_12_20_1 │ "demux_28_12_20_1"
demux_28_12_20_2 │ "demux_28_12_20_2"
```

  - Applying a mask to an axis (...axis... `[` ...mask... `]`) - see [`VECTOR_MASK`](@ref).
  - Looking up the values of a property based on a (possibly masked) axis (...axis... `:` ...lookup...) - see [`VECTOR_LOOKUP`](@ref).
  - Applying some operation to a vector we looked up (...vector... `% Eltwise operation...`) - see [`VECTOR_OPERATION`](@ref).
  - Taking any matrix query and reducing it to a column or a row vector (...matrix... `>| Reduction operation...`,
    ...matrix... `>- Reduction operation...`) - see [`VECTOR_FROM_MATRIX`](@ref).

[**Syntax diagram:**](assets/vector.svg)

![](assets/vector.svg)
"""
const VECTOR_QUERY = nothing

"""
A query fragment specifying a mask to apply to an axis. Valid phrases are:

  - Beginning a mask by looking up some vector property for each entry (...axis... `[ vector-property`, ...axis... `[ ! vector-property`) - see [`VECTOR_MASK_LOOKUP`](@ref).
  - Applying some operation to a vector we looked up (...mask... `> value`) - see [`VECTOR_OPERATION`](@ref).
  - Combining the mask with another one (...mask... `&` ...mask..., ...mask... `& !` ...mask...) - see
    [`VECTOR_MASK_OPERATION`](@ref).
  - Ending the mask (...mask... `]`).

[**Syntax diagram:**](assets/vector_mask.svg)

![](assets/vector_mask.svg)
"""
const VECTOR_MASK = nothing

"""
A query fragment specifying looking up a vector for a mask to apply to an axis. Valid phrases are similar to [`VECTOR_LOOKUP`](@ref),
except that they start with `[` instead of `:` (starting with `[ !` reverses the mask). Example:

```jldoctest
cells = example_cells_daf()
cells["@ gene [ ! is_lateral ]"]

# output

245-element Named Vector{SubString{StringViews.StringView{Vector{UInt8}}}}
gene       │
───────────┼────────────
ENO1       │      "ENO1"
PRDM2      │     "PRDM2"
HP1BP3     │    "HP1BP3"
HNRNPR     │    "HNRNPR"
RSRP1      │     "RSRP1"
KHDRBS1    │   "KHDRBS1"
THRAP3     │    "THRAP3"
SMAP2      │     "SMAP2"
⋮                      ⋮
MYADM      │     "MYADM"
DDT        │       "DDT"
UQCR10     │    "UQCR10"
EIF3L      │     "EIF3L"
TNRC6B     │    "TNRC6B"
TNFRSF13C  │ "TNFRSF13C"
SOD1       │      "SOD1"
ATP5PO     │    "ATP5PO"
```

[**Syntax diagram:**](assets/vector_mask_lookup.svg)

![](assets/vector_mask_lookup.svg)
"""
const VECTOR_MASK_LOOKUP = nothing

"""
A query fragment specifying combining a mask with a second mask. Valid phrases are similar to [`VECTOR_MASK_LOOKUP`](@ref),
except that they start with the logical combination operator (`&`, `|`, `^`), with an optional `!` suffix for negating
the second mask. Operations are evaluated in order (left to right). Example:

```jldoctest
cells = example_cells_daf()
cells["@ donor [ age > 60 & sex = male ]"]

# output

29-element Named Vector{SubString{StringViews.StringView{Vector{UInt8}}}}
donor  │
───────┼───────
N16    │  "N16"
N17    │  "N17"
N59    │  "N59"
N86    │  "N86"
N88    │  "N88"
N91    │  "N91"
N92    │  "N92"
N95    │  "N95"
⋮             ⋮
N163   │ "N163"
N164   │ "N164"
N169   │ "N169"
N172   │ "N172"
N174   │ "N174"
N175   │ "N175"
N179   │ "N179"
N181   │ "N181"
```

[**Syntax diagram:**](assets/vector_mask_operation.svg)

![](assets/vector_mask_operation.svg)
"""
const VECTOR_MASK_OPERATION = nothing

"""
A query fragment specifying looking up vector properties. Valid phrases are:

  - Looking up a vector property based on an axis (...axis... `: vector-property`). Example:

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell : type"]

# output

7-element Named SparseArrays.ReadOnly{String, 1, Vector{String}}
metacell  │
──────────┼───────────
M1671.28  │      "MPP"
M2357.20  │      "MPP"
M2169.56  │ "MEBEMP-L"
M2576.86  │ "MEBEMP-E"
M1440.15  │      "MPP"
M756.63   │ "MEBEMP-E"
M412.08   │ "memory-B"
```

This can be further embellished:

  - Looking up a matrix property based on an axis, and slicing a column based on an explicit entry of the other axis of
    the matrix (...axis... `:: matrix-property @ columns-axis = columns-axis-entry`). Example:

```jldoctest
metacells = example_metacells_daf()
metacells["@ gene :: fraction @ metacell = M412.08"]

# output

683-element Named Vector{Float32}
gene         │
─────────────┼────────────
RPL22        │  0.00373581
PARK7        │  6.50531f-5
ENO1         │  4.22228f-5
PRDM2        │ 0.000151486
HP1BP3       │  0.00012099
CDC42        │ 0.000176377
HNRNPR       │   6.7083f-5
RPL11        │   0.0124251
⋮                        ⋮
NRIP1        │  2.79487f-5
ATP5PF       │  8.22312f-5
CCT8         │  4.13243f-5
SOD1         │ 0.000103708
SON          │  0.00032361
ATP5PO       │  9.73498f-5
TTC3         │ 0.000122469
HMGN1        │ 0.000160654
```

  - Looking up a square matrix property, and slicing a column based on an explicit entry of the (column) axis of the
    matrix (...axis... `:: square-matrix-property @| column-axis-entry`).

```jldoctest
metacells = example_metacells_daf()
# Outgoing weights from the M412.08 metacell.
metacells["@ metacell :: edge_weight @| M412.08"]

# output

7-element Named Vector{Float32}
metacell  │
──────────┼────
M1671.28  │ 0.0
M2357.20  │ 0.0
M2169.56  │ 0.0
M2576.86  │ 0.0
M1440.15  │ 0.5
M756.63   │ 0.1
M412.08   │ 0.0
```

  - Looking up a square matrix property, and slicing a row based on an explicit entry of the (column) axis of the
    matrix (...vector... `:: square-matrix-property @- row-axis-entry`).

```jldoctest
metacells = example_metacells_daf()
# Incoming weights into the M412.08 metacell.
metacells["@ metacell :: edge_weight @- M412.08"]

# output

7-element Named Vector{Float32}
metacell  │
──────────┼────
M1671.28  │ 0.0
M2357.20  │ 0.0
M2169.56  │ 0.1
M2576.86  │ 0.0
M1440.15  │ 0.0
M756.63   │ 0.9
M412.08   │ 0.0
```

In all of these, the lookup operation (`:`, `::`) can be followed by `|| default-value` to specify a value to use if the
property we look up doesn't exist (...vector... `: vector-property || default-value`, ...vector... `:: square-matrix-property || default-value @| column-entry`).

If the base axis is the result of looking up some property, then some of the entries may have an empty string value.
Looking up the vector property based on this will cause an error. To overcome this, you can request that these entries
will be masked out of the result by prefixing the query with `??` (...vector... `?? : vector-property`, ...vector... `?? :: matrix-property ...`), or specify the *final* value of these entries (...vector... `?? final-value : vector-property`, ...vector... `?? final-value :: matrix-property ...`). Since it is possible to chain lookup operations
(see [`VECTOR_OPERATION`](@ref)), the final value is applied at the end of the lookup chain (`?? final-value : vector-property-which-holds-axis-entries : vector-property-of-that-axis-which-holds-another-axis-entries : vector-property-of-the-other-axis`).

[**Syntax diagram:**](assets/vector_lookup.svg)

![](assets/vector_lookup.svg)
"""
const VECTOR_LOOKUP = nothing

"""
A query fragment specifying some operation on a vector of values. Valid phrases are:

  - Treating the vector values as names of some axis entries and looking up some property of that axis
    (...vector... `@ axis-values-are-entries-of : vector-property-of-that-axis || default-value`) - see
    [`VECTOR_AS_AXIS`](@ref) and [`VECTOR_LOOKUP`](@ref)).

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell : type : color"]

# output

7-element Named Vector{String}
metacell  │
──────────┼────────────
M1671.28  │      "gold"
M2357.20  │      "gold"
M2169.56  │      "plum"
M2576.86  │   "#eebb6e"
M1440.15  │      "gold"
M756.63   │   "#eebb6e"
M412.08   │ "steelblue"
```

  - Applying some operation to a vector we looked up (...vector... `% Eltwise ...`).

```jldoctest
cells = example_cells_daf()
cells["@ donor : age % Clamp min 40 max 60 type Int64"]

# output

95-element Named Vector{Int64}
donor  │
───────┼───
N16    │ 60
N17    │ 60
N18    │ 60
N59    │ 60
N79    │ 60
N83    │ 42
N84    │ 60
N85    │ 60
⋮         ⋮
N176   │ 60
N177   │ 58
N178   │ 40
N179   │ 60
N181   │ 60
N182   │ 60
N183   │ 60
N184   │ 60
```

  - Comparing the values in the vector with some constant (...vector... `> value`).

```jldoctest
cells = example_cells_daf()
cells["@ donor : age > 60"]

# output

95-element Named Vector{Bool}
donor  │
───────┼──────
N16    │  true
N17    │  true
N18    │  true
N59    │  true
N79    │  true
N83    │ false
N84    │  true
N85    │  true
⋮            ⋮
N176   │  true
N177   │ false
N178   │ false
N179   │  true
N181   │  true
N182   │  true
N183   │  true
N184   │  true
```

  - Grouping the vector values by something and reducing each group to a single value
    (...vector... `/ vector-property >> Sum`) - see [`VECTOR_GROUP`](@ref).

[**Syntax diagram:**](assets/vector_operation.svg)

![](assets/vector_operation.svg)
"""
const VECTOR_OPERATION = nothing

"""
A query fragment for explicitly specifying that the values or a vector are entries of an axis. Valid phrases are:

  - Using the name of the property of the vector as the axis name (...vector... `@`). The convention is that the
    property name is the name of the axis, or starts with the name of the axis followed by `.` and some suffix
  - Specifying an explicit axis name (...vector... `@ axis`) ignoring the vector property name.

When the values of a vector are entries in some axis, we can use it to look up some property based on it. For simple
lookups the `@` can be omitted (e.g. `@ cell : metacell`). This can be chained (`@ cell : metacell : type : color`).
When grouping a vector or matrix rows or columns, explicitly associating an axis with the values causes creating a group
for each axis entry in the right order so that the result is a proper values vector for the
axis (`@ metacell / type @ >> Count`).

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell : type =@ : color"]

# output

7-element Named Vector{String}
metacell  │
──────────┼────────────
M1671.28  │      "gold"
M2357.20  │      "gold"
M2169.56  │      "plum"
M2576.86  │   "#eebb6e"
M1440.15  │      "gold"
M756.63   │   "#eebb6e"
M412.08   │ "steelblue"
```

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell : type =@ type : color"]

# output

7-element Named Vector{String}
metacell  │
──────────┼────────────
M1671.28  │      "gold"
M2357.20  │      "gold"
M2169.56  │      "plum"
M2576.86  │   "#eebb6e"
M1440.15  │      "gold"
M756.63   │   "#eebb6e"
M412.08   │ "steelblue"
```

[**Syntax diagram:**](assets/vector_as_axis.svg)

![](assets/vector_as_axis.svg)
"""
const VECTOR_AS_AXIS = nothing

"""
A query fragment for grouping vector values by some property and computing a single value per group. Valid phrases
for fetching the group values are similar to [`VECTOR_LOOKUP`](@ref) but start with a `/` instead of `:`. This
can be followed by any [`VECTOR_OPERATION`](@ref) (in particular, additional lookups). Once the final group value
is established for each vector entry, the values of all entries with the same group value are reduced using a
[`ReductionOperation`](@ref) to a single value. The result vector has this reduced value per group. E.g.,
`@ cell : age / type >> Mean`.

```jldoctest
chain = example_chain_daf()
chain["@ cell : donor : age / metacell ?? : type >> Mean"]

# output

4-element Named Vector{Float32}
A        │
─────────┼────────
MEBEMP-E │ 63.9767
MEBEMP-L │ 63.9524
MPP      │  64.238
memory-B │ 62.3077
```

By default the result vector is sorted by the group value (this is also used as the name in the result `NamedArray`).
Specifying an [`VECTOR_AS_AXIS`](@ref) before the reduction operation changes this to require that the group values be
entries in some axis. In this case the result vector will have one entry for each entry of the axis, in the axis order.
If some axis entries do not have any vector values associated with them, then the reduction will fail (e.g. "mean of an
empty vector"). In this case, you should specify a default value for the reduction.
E.g., `@ cell : age / type @ >> Mean || 0`. Example:

```jldoctest
chain = example_chain_daf()
chain["@ cell [ metacell ?? : type != memory-B ] : donor : age / metacell : type =@ >> Mean || 0"]

# output

4-element Named Vector{Float32}
type     │
─────────┼────────
memory-B │     0.0
MEBEMP-E │ 63.9767
MEBEMP-L │ 63.9524
MPP      │  64.238
```

[**Syntax diagram:**](assets/vector_group.svg)

![](assets/vector_group.svg)
"""
const VECTOR_GROUP = nothing

"""
A query fragment for reducing a matrix to a vector. Valid phrases are:

  - Reduce each row into a single value, resulting in an entry for each column of the matrix
    (...matrix... `>| ReductionOperation ...`). Example:

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell @ gene :: fraction >| Max"]

# output

7-element Named Vector{Float32}
metacell  │
──────────┼──────────
M1671.28  │  0.023321
M2357.20  │ 0.0233425
M2169.56  │ 0.0219235
M2576.86  │ 0.0236719
M1440.15  │ 0.0227677
M756.63   │ 0.0249121
M412.08   │ 0.0284936
```

  - Reduce each column into a single value, resulting in an entry for each row of the matrix
    (...matrix... `>- ReductionOperation ...`).

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell @ gene :: fraction >- Max"]

# output

683-element Named Vector{Float32}
gene         │
─────────────┼────────────
RPL22        │  0.00474096
PARK7        │ 0.000154199
ENO1         │ 0.000533887
PRDM2        │ 0.000151486
HP1BP3       │ 0.000248206
CDC42        │ 0.000207847
HNRNPR       │ 0.000129013
RPL11        │   0.0124251
⋮                        ⋮
NRIP1        │ 0.000361428
ATP5PF       │ 0.000170554
CCT8         │ 0.000142851
SOD1         │ 0.000177344
SON          │  0.00032361
ATP5PO       │  0.00018833
TTC3         │ 0.000144736
HMGN1        │ 0.000415481
```

[**Syntax diagram:**](assets/vector_from_matrix.svg)

![](assets/vector_from_matrix.svg)
"""
const VECTOR_FROM_MATRIX = nothing

"""
A query returning a matrix result. Valid phrases are:

  - Lookup a matrix property after specifying its rows and columns axes
    (...rows_axis... ...columns_axis... `:: matrix-property`,
    ...rows_axis... ...columns_axis... `:: matrix-property || default-value`). Example:

```jldoctest
cells = example_cells_daf()
cells["@ cell @ gene :: UMIs"]

# output

856×683 Named Matrix{UInt8}
                        cell ╲ gene │        RPL22  …         HMGN1
────────────────────────────────────┼──────────────────────────────
demux_07_12_20_1_AACAAGATCCATTTCA-1 │         0x0c  …          0x02
demux_07_12_20_1_AACGAAAGTCCAATCA-1 │         0x08             0x01
demux_07_12_20_1_AAGACAAAGTTCCGTA-1 │         0x03             0x03
demux_07_12_20_1_AGACTCATCTATTGTC-1 │         0x08             0x01
demux_07_12_20_1_AGATAGACATTCCTCG-1 │         0x08             0x00
demux_07_12_20_1_ATCGTAGTCCAGTGCG-1 │         0x0e             0x02
demux_07_12_20_1_CACAGGCGTCCTACAA-1 │         0x0b             0x03
demux_07_12_20_1_CCTACGTAGCCAACCC-1 │         0x03             0x01
⋮                                                ⋮  ⋱             ⋮
demux_11_04_21_2_GGGTCACCACCACATA-1 │         0x05             0x03
demux_11_04_21_2_TACAACGGTTACACAC-1 │         0x01             0x00
demux_11_04_21_2_TAGAGTCAGAACGCGT-1 │         0x09             0x00
demux_11_04_21_2_TGATGCAAGGCCTGCT-1 │         0x07             0x00
demux_11_04_21_2_TGCCGAGAGTCGCGAA-1 │         0x01             0x00
demux_11_04_21_2_TGCTGAAAGCCGCACT-1 │         0x01             0x03
demux_11_04_21_2_TTCAGGACAGGAATAT-1 │         0x06             0x00
demux_11_04_21_2_TTTAGTCGTCTAGTGT-1 │         0x06  …          0x00
```

  - Given a vector of values, lookup another vector of the same size and generate a matrix of the number of times each
    combination of values appears (...vector... `* vector-property ...`) - see [`MATRIX_COUNT`](@ref). Example:

Matrices can then be modified by applying any [`MATRIX_OPERATION`](@ref) to it.

[**Syntax diagram:**](assets/matrix.svg)

![](assets/matrix.svg)
"""
const MATRIX_QUERY = nothing

"""
A query fragment for computing a matrix of the number of times a combination of values appears in the same index in the
first and second vectors. Valid phrases are similar to [`VECTOR_LOOKUP`](@ref) except they start with `*` instead of
`:`. This can be followed by any [`VECTOR_OPERATION`](@ref) for computing the final second vector. E.g.,
`@ cell : age * metacell : type`. Example:

```jldoctest
cells = example_cells_daf()
cells["@ cell : experiment * donor : sex"]

# output

23×2 Named Matrix{UInt16}
           A ╲ B │ female    male
─────────────────┼───────────────
demux_01_02_21_1 │ 0x0017  0x000e
demux_01_02_21_2 │ 0x000a  0x001a
demux_01_03_21_1 │ 0x0012  0x001b
demux_04_01_21_1 │ 0x0013  0x0016
demux_04_01_21_2 │ 0x0006  0x0012
demux_07_03_21_1 │ 0x000a  0x0016
demux_07_03_21_2 │ 0x000d  0x001b
demux_07_12_20_1 │ 0x0006  0x0011
⋮                       ⋮       ⋮
demux_21_02_21_1 │ 0x0012  0x0005
demux_21_02_21_2 │ 0x0009  0x002a
demux_21_12_20_1 │ 0x001e  0x0005
demux_21_12_20_2 │ 0x0000  0x0026
demux_22_02_21_1 │ 0x0012  0x0009
demux_22_02_21_2 │ 0x001c  0x0013
demux_28_12_20_1 │ 0x0018  0x0022
demux_28_12_20_2 │ 0x003f  0x0009
```

By default, the matrix rows and columns are sorted by the unique values. Explicitly specifying [`VECTOR_AS_AXIS`](@ref)
for either the first or second vector will change the rows or columns to the axis entries in the right (axis) order.
This may create rows or columns with all-zero values. E.g., `@ cell : batch @ * metacell : type @`. Example:

```jldoctest
cells = example_cells_daf()
cells["@ cell : experiment =@ * donor : sex"]

# output

23×2 Named Matrix{UInt16}
  experiment ╲ B │ female    male
─────────────────┼───────────────
demux_01_02_21_1 │ 0x0017  0x000e
demux_01_02_21_2 │ 0x000a  0x001a
demux_01_03_21_1 │ 0x0012  0x001b
demux_04_01_21_1 │ 0x0013  0x0016
demux_04_01_21_2 │ 0x0006  0x0012
demux_07_03_21_1 │ 0x000a  0x0016
demux_07_03_21_2 │ 0x000d  0x001b
demux_07_12_20_1 │ 0x0006  0x0011
⋮                       ⋮       ⋮
demux_21_02_21_1 │ 0x0012  0x0005
demux_21_02_21_2 │ 0x0009  0x002a
demux_21_12_20_1 │ 0x001e  0x0005
demux_21_12_20_2 │ 0x0000  0x0026
demux_22_02_21_1 │ 0x0012  0x0009
demux_22_02_21_2 │ 0x001c  0x0013
demux_28_12_20_1 │ 0x0018  0x0022
demux_28_12_20_2 │ 0x003f  0x0009
```

[**Syntax diagram:**](assets/matrix_count.svg)

![](assets/matrix_count.svg)
"""
const MATRIX_COUNT = nothing

"""
A query fragment specifying some operation on a matrix of values. Valid phrases are:

  - Treating the matrix values as names of some axis entries and looking up some property of that axis
    (...matrix... `@ axis-values-are-entries-of : vector-property-of-that-axis || default-value`) - see
    [`VECTOR_AS_AXIS`](@ref) and [`VECTOR_LOOKUP`](@ref)) (while the matrix retains its shape, this shape does not
    effect the result so we treat it as a long vector for the purpose of the lookup).

  - Applying some operation to a vector we looked up (...matrix... `% Eltwise ...`). Example:

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell @ gene :: fraction % Log base 2 eps 1e-5"]

# output

7×683 Named Matrix{Float32}
metacell ╲ gene │        RPL22         PARK7  …          TTC3         HMGN1
────────────────┼──────────────────────────────────────────────────────────
M1671.28        │     -7.80014      -13.3582  …      -13.0011      -11.4571
M2357.20        │     -7.91664      -12.5723          -13.009      -11.7136
M2169.56        │     -7.71757      -13.0192         -13.0406      -11.1986
M2576.86        │      -7.8198      -12.8843         -12.6579      -11.5767
M1440.15        │     -7.77472      -12.9433         -13.3506      -11.5629
M756.63         │     -7.84368      -13.0487          -13.148      -11.8308
M412.08         │     -8.06051      -13.7017  …      -12.8821      -12.5166
```

  - Grouping the matrix rows or columns by something and reducing each group to a single one
    (...matrix... `-/ vector-property >- Sum`, ...matrix... `|/ vector-property >| Sum`) - see [`MATRIX_GROUP`](@ref).

[**Syntax diagram:**](assets/matrix_operation.svg)

![](assets/matrix_operation.svg)
"""
const MATRIX_OPERATION = nothing

"""
A query fragment for grouping rows or columns by some property and computing a single one per group. Valid phrases
for fetching the group values are similar to [`VECTOR_LOOKUP`](@ref) but start with a `-/` or `|/` instead of `:`. This
can be followed by any [`VECTOR_OPERATION`](@ref) (in particular, additional lookups). Once the final group value
is established for each row or column entry, the values of all entries with the same group value are reduced using a
[`ReductionOperation`](@ref) to a single value. The result matrix has this reduced value per group.
E.g., `@ cell @ gene :: UMIs -/ metacell : type >- Sum`. The reduction operation must match the group
operation (`-/ ... >-`, `|/ ... >|`). Example:

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell @ gene :: fraction -/ type >- Mean"]

# output

4×683 Named Matrix{Float32}
A ╲ gene │        RPL22         PARK7  …          TTC3         HMGN1
─────────┼──────────────────────────────────────────────────────────
MEBEMP-E │   0.00437961   0.000115139  …   0.000122451   0.000290955
MEBEMP-L │   0.00474096   0.000110458      0.000108683   0.000415481
MPP      │   0.00438723   0.000118797      0.000103007   0.000317991
memory-B │   0.00373581    6.50531f-5  …   0.000122469   0.000160654
```

By default groups are sorted by their unique values. Explicitly specifying [`VECTOR_AS_AXIS`](@ref) for the group
will change the rows or columns to the axis entries in the right (axis) order. T
This may create rows or columns with all-zero values.

By default the result is sorted by the group value (this is also used as the name in the result `NamedArray`).
Specifying an [`VECTOR_AS_AXIS`](@ref) before the reduction operation changes this to require that the group values be
entries in some axis. In this case the result will have one entry for each entry of the axis, in the axis order.

```jldoctest
metacells = example_metacells_daf()
metacells["@ metacell @ gene :: fraction -/ type =@ >- Mean"]

# output

4×683 Named Matrix{Float32}
type ╲ gene │        RPL22         PARK7  …          TTC3         HMGN1
────────────┼──────────────────────────────────────────────────────────
memory-B    │   0.00373581    6.50531f-5  …   0.000122469   0.000160654
MEBEMP-E    │   0.00437961   0.000115139      0.000122451   0.000290955
MEBEMP-L    │   0.00474096   0.000110458      0.000108683   0.000415481
MPP         │   0.00438723   0.000118797  …   0.000103007   0.000317991
```

If some axis entries do not have any values associated with them, then the reduction will fail (e.g. "mean of an
empty row/column vector"). In this case, you should specify a default value for the reduction.
E.g., `@ cell @ gene :: UMIs -/ metacell : type =@ >- Sum || 0`. Example:

[**Syntax diagram:**](assets/matrix_group.svg)

![](assets/matrix_group.svg)
"""
const MATRIX_GROUP = nothing

"""
A query is a description of a (subset of a) procedure for extracting some data from a [`DafReader`](@ref). A full query
is a sequence of [`QueryOperation`](@ref), that when applied to some [`DafReader`](@ref), result in a set of names, or
scalar, vector or matrix result.

Queries can be constructed in two ways. In code, a query can be built by chaining query operations (e.g., the query
`Axis("gene") |> LookupVector("is_marker")` looks up the `is_marker` vector property of the `gene` axis).
Alternatively, a query can be parsed from a string, which needs to be parsed into a [`Query`](@ref) object (e.g., the
above can be written as `parse_query("@gene:is_marker")` or using the [`@q_str`](@ref) macro as `q"gene:is_marker"`).

Being able to represent queries as strings allows for reading them from configuration files and letting the user input
them in an application UI (e.g., allowing the user to specify the X, Y and/or colors of a scatter plot using queries).
At the same time, being able to incrementally build queries using code allows for convenient reuse (e.g., reusing axis
sub-queries in `Daf` views), without having to go through the string representation.

If the provided query string contains only an operand, and `operand_only` is specified, it is used as the operator
(i.e., `parse_query("metacell")` is an error, but `parse_query("metacell", Axis)` is the same as `Axis("metacell")`).
This is useful when providing suffix queries (e.g., for [`get_frame`](@ref)).

To apply a query, invoke [`get_query`](@ref) to apply a query to some [`DafReader`](@ref) data (you can also use the
shorthand `daf[query]` instead of `get_query(daf, query)`. Tou can also write `query |> get_query(daf)` which is useful
when constructing a query from parts using `|>`). By default, [`get_query`](@ref) will cache their results in memory as
[`QueryData`](@ref CacheGroup), to speed up repeated queries. This may lock up large amounts of memory. Using
`daf[query]` does not cache the results; you can also use [`empty_cache!`](@ref) to release the memory.

!!! note

    This has started as a very simple query language (which it still is, for the simple cases) but became complex to
    allow for useful but complex scenarios. In particular, the approach here of using a concatenative language (similar
    to `ggplot`) makes simple things simpler, but became less natural for some of the more advanced operations. However,
    using an RPN or a LISP notation to better support such cases would have ended up with a much less nice syntax for
    the simple cases.

    Hopefully we have covered sufficient ground so that we won't need to add further operations (except for more
    element-wise and reduction operations). In most cases, you can write code that accesses the vectors/matrix data and
    performs whatever computation you want instead of writing a complex query; however, this isn't an option when
    defining views or adapters, which rely on the query mechanism for specifying the data.

## Execution Model

Queries consist of a combination of one or more of the operators listed below. However, the execution of the query is
not one operator at a time. Instead, at each point, a phrase consisting of several operators is executed as a single
operation. Each such step modifies the state of the query (starting with an empty state). When the query is done, the
result is extracted from the final query state.

The query state is a stack which starts empty. Each phrase only applies if the top of the stack matches some pattern
(e.g., looking up a vector property requires the top of the stack contains an axis specification). The execution of the
phrase pops out the matching top stack elements, performs some operations on them, and then pushes some elements to the
stack.

This approach simplifies both the code and the mental model for the query language. For example, when looking up a
scalar property using the [`LookupScalar`](@ref) operator, e.g. `". version"`, and we want to provide a default value to
return if the property doesn't exist by following it with the [`IfMissing`](@ref) operator, e.g. `" || 0.0.0", the phrase `LookupScalar("version") |> IfMissing("0.0.0")`is executed as a single operation, invoking `get_scalar(daf,
"version"; default = "0.0.0")`and pushing a scalar into the query state stack. This eliminates the issue of "what is the state of the query after executing a`LookupScalar`of a missing scalar property, before executing`IfMissing`".

A disadvantage of this approach is that the semantics of an operator depends on the phrase it is used in. However, we
defined the operators such that they would "make sense" in the context of the different phrases they participate in.
This allows us to provide a list of operators with a coherent function for each:

## Query Operators

| Operator | Implementation               | Description                                                                                     |
|:-------- |:----------------------------:|:----------------------------------------------------------------------------------------------- |
| `@`      | [`Axis`](@ref)               | Specify an axis, e.g. for looking up a vector or matrix property.                               |
| `=@`     | [`AsAxis`](@ref)             | Specify that values are axis entries, e.g. for looking up another vector or matrix property.    |
| `@❘`     | [`SquareColumnIs`](@ref)     | Specify which column to slice from a square matrix.                                             |
| `@-`     | [`SquareRowIs`](@ref)        | Specify which row to slice from a square matrix.                                                |
| `/`      | [`GroupBy`](@ref)            | Group elements of a vector by values of another vector of the same length.                      |
| `❘/`     | [`GroupColumnsBy`](@ref)     | Group columns of a matrix by values of a vector with one value per row.                         |
| `-/`     | [`GroupRowsBy`](@ref)        | Group rows of a matrix by values of a vector with one value per row.                            |
| `%`      | [`EltwiseOperation`](@ref)   | Specify an element-wise operation to apply to scalar, vector or matrix data.                    |
| `>>`     | [`ReductionOperation`](@ref) | Specify a reduction operation to convert vector or matrix data to a single scalar value.        |
| `>❘`     | [`ReduceToColumn`](@ref)     | Specify a reduction operation to convert matrix data to a single column.                        |
| `>-`     | [`ReduceToRow`](@ref)        | Specify a reduction operation to convert matrix data to a single row.                           |
| `❘❘`     | [`IfMissing`](@ref)          | Specify a default value to use when looking up a property that doesn't exist,                   |
|          |                              | or when reducing an empty vector or matrix into a single scalar value.                          |
| `*`      | [`CountBy`](@ref)            | Count in a matrix the number of times each combination of values from two vectors coincide.     |
| `?`      | [`Names`](@ref)              | Ask for a set of names of axes or properties that can be used to look up data.                  |
| `??`     | [`IfNot`](@ref)              | Specify a final value to use when performing chained lookup operations based on an empty value. |
| `.`      | [`LookupScalar`](@ref)       | Lookup a scalar property.                                                                       |
| `:`      | [`LookupVector`](@ref)       | Lookup a vector property based on some axis.                                                    |
| `::`     | [`LookupMatrix`](@ref)       | Lookup a matrix property based on a pair of axes (rows and columns).                            |
| `<`      | [`IsLess`](@ref)             | Compare less than a value.                                                                      |
| `<=`     | [`IsLess`](@ref)             | Compare less than or equal to a value.                                                          |
| `=`      | [`IsEqual`](@ref)            | Compare equal to a value.                                                                       |
| `!=`     | [`IsNotEqual`](@ref)         | Compare not equal to a value.                                                                   |
| `>=`     | [`IsLess`](@ref)             | Compare greater than or equal to a value.                                                       |
| `>`      | [`IsLess`](@ref)             | Compare greater than a value.                                                                   |
| `~`      | [`IsMatch`](@ref)            | Compare by matching to a regular expression.                                                    |
| `!~`     | [`IsNotMatch`](@ref)         | Compare by not matching to a regular expression.                                                |
| `[`      | [`BeginMask`](@ref)          | Begin computing a mask on an axis.                                                              |
| `[ !`    | [`BeginNegatedMask`](@ref)   | Begin computing a mask on an axis, negating it.                                                 |
| `]`      | [`EndMask`](@ref)            | Complete computing a mask on an axis.                                                           |
| `&`      | [`AndMask`](@ref)            | Merge masks by AND Boolean operation.                                                           |
| `& !`    | [`AndNegatedMask`](@ref)     | Merge masks by AND NOT Boolean operation.                                                       |
| `❘`      | [`OrMask`](@ref)             | Merge masks by OR Boolean operation.                                                            |
| `❘ !`    | [`OrNegatedMask`](@ref)      | Merge masks by OR NOT Boolean operation.                                                        |
| `^`      | [`XorMask`](@ref)            | Merge masks by XOR Boolean operation.                                                           |
| `^ !`    | [`XorNegatedMask`](@ref)     | Merge masks by XOR NOT Boolean operation.                                                       |

!!! note

    Due to Julia's Documenter limitations, the ASCII `|` character (`&#124;`, vertical bar) is replaced by the Unicode
    `❘` character (`&#x2758;`, light vertical bar) in the above table. Sigh.

## Query Syntax

Obviously not all possible combinations of operators make sense (e.g., `LookupScalar("is_marker") |> Axis("cell")` will
not work). Valid queries are built out of supported phrases (each including one or more operators), combined into a
coherent query. For the full list of valid phrases and queries, see [`NAMES_QUERY`](@ref), [`SCALAR_QUERY`](@ref),
[`VECTOR_QUERY`](@ref) and [`MATRIX_QUERY`](@ref) below.
"""
abstract type Query <: QueryOperation end

"""
Most operations that take a query allow passing a string to be parsed into a query, or an actual [`Query`](@ref) object.
This type is used as a convenient notation for such query parameters.
"""
QueryString = Union{Query, AbstractString}

"""
    struct QuerySequence <: Query

A sequence of `N` [`QueryOperation`](@ref)s. This is the internal representation of the query as of itself (without applying it).
"""
struct QuerySequence <: Query
    query_operations::AbstractVector{<:QueryOperation}
end

function next_query_operation(tokens::Vector{Token}, next_token_index::Int)::Tuple{QueryOperation, Int}
    token = next_operator_token(tokens, next_token_index)
    next_token_index += 1

    if token.value == "||"
        value_token = next_value_token(tokens, next_token_index)
        next_token_index += 1
        value = value_token.value

        type_token = maybe_next_value_token(tokens, next_token_index)
        if type_token === nothing
            value = guess_typed_value(value)
        else
            next_token_index += 1
            if type_token.value == "String"
                type = String
            else
                type = parse_number_type_value(token, "type", type_token)
                if type !== nothing
                    value = parse_number_value(token, "value", value_token, type)
                end
            end
        end

        return (IfMissing(value), next_token_index)
    end

    if token.value == "%"
        computation_operation, next_token_index =
            parse_registered_operation(tokens, next_token_index, "eltwise", ELTWISE_REGISTERED_OPERATIONS)
        return (computation_operation, next_token_index)
    end

    if token.value in (">>", ">-", ">|")
        computation_operation, next_token_index =
            parse_registered_operation(tokens, next_token_index, "reduce", REDUCTION_REGISTERED_OPERATIONS)
        if token.value == ">-"
            computation_operation = ReduceToRow(computation_operation)
        elseif token.value == ">|"
            computation_operation = ReduceToColumn(computation_operation)
        end
        return (computation_operation, next_token_index)
    end

    if token.value in ("[", "&", "|", "^") &&
       next_token_index <= length(tokens) &&
       tokens[next_token_index].is_operator &&
       tokens[next_token_index].value == "!"
        next_token = tokens[next_token_index]
        next_token_index += 1
        token = Token(
            true,
            token.value * " !",
            token.token_index,
            token.first_index,
            next_token.last_index,
            token.encoded_string * " " * next_token.encoded_string,
        )
    end

    operation = get(QUERY_OPERATIONS_DICT, token.value, nothing)
    if operation !== nothing
        operation_type, requires_operand = operation
        if requires_operand === nothing
            token = nothing
        elseif requires_operand
            token = next_value_token(tokens, next_token_index)
        else
            token = maybe_next_value_token(tokens, next_token_index)
        end
        if token === nothing
            return (operation_type(), next_token_index)
        else
            return (operation_type(token.value), next_token_index + 1)
        end
    end

    error_at_token(tokens[next_token_index - 1], "regex bug when parsing query"; at_end = true)  # UNTESTED
    @assert false
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
            error_at_token(
                name_token,
                """
                the parameter: $(name_token.value)
                does not exist for the operation: $(operation_name.value)
                """,
            )
        end
        if haskey(parameters_dict, name_token.value)
            error_at_token(
                name_token,
                """
                repeated parameter: $(name_token.value)
                for the operation: $(operation_name.value)
                """,
            )
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

function next_operator_token(tokens::Vector{Token}, next_token_index::Int)::Token
    if !tokens[next_token_index].is_operator
        error_at_token(tokens[next_token_index], "expected: operator")
    end
    return tokens[next_token_index]
end

function next_value_token(tokens::Vector{Token}, next_token_index::Int)::Token
    if next_token_index > length(tokens)
        error_at_token(tokens[next_token_index - 1], "expected: value"; at_end = true)
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
    return QuerySequence([first_sequence.query_operations..., second_sequence.query_operations...])
end

function Base.:(|>)(first_operation::QueryOperation, second_sequence::QuerySequence)::QuerySequence
    return QuerySequence([first_operation, second_sequence.query_operations...])
end

function Base.:(|>)(first_sequence::QuerySequence, second_operation::QueryOperation)::QuerySequence
    return QuerySequence([first_sequence.query_operations..., second_operation])
end

function Base.:(|>)(first_operation::QueryOperation, second_operation::QueryOperation)::QuerySequence
    return QuerySequence([first_operation, second_operation])
end

function Base.:(|>)(first::Union{QuerySequence, QueryOperation}, second::AbstractString)::QuerySequence
    return first |> as_query_sequence(second)
end

function Base.:(|>)(first::AbstractString, second::Union{QuerySequence, QueryOperation})::QuerySequence
    return as_query_sequence(first) |> second
end

function as_query_sequence(query_string::AbstractString)::QuerySequence
    return as_query_sequence(parse_query(query_string))
end

function as_query_sequence(query_operation::QueryOperation)::QuerySequence
    return QuerySequence([query_operation])
end

function as_query_sequence(query::Query)::QuerySequence
    return QuerySequence([query])
end

function as_query_sequence(query_sequence::QuerySequence)::QuerySequence
    return query_sequence
end

"""
    q"..."

Shorthand for parsing a literal string as a [`Query`](@ref). This is equivalent to [`Query`](@ref)`(raw"...")`, that is,
a `\\` can be placed in the string without escaping it (except for before a `"`). This is very convenient for literal
queries (e.g., `q"@ cell = ATCG\\:B1 : batch"` == `parse_query(raw"@ cell = ATCG\\:B1 : batch")` ==
`parse_query("@ cell = ATCG\\\\:B1 : batch")` == `Axis("cell") |> IsEqual("ATCG:B1") |> LookupVector("batch"))`.

```jldoctest
println("@ cell = ATCG\\\\:B1 : batch")
println(q"@ cell = ATCG\\:B1 : batch")

# output

@ cell = ATCG\\:B1 : batch
@ cell = ATCG\\:B1 : batch
```
"""
macro q_str(query_string::AbstractString)
    return parse_query(query_string)
end

"""
    get_query(
        daf::DafReader,
        query::QueryString;
        [cache::Bool = true]
    )::Union{AbstractSet{<:AbstractString}, StorageScalar, NamedVector, NamedMatrix}

    query |> get_query(
        daf::DafReader;
        cache::Bool = true,
    )

Apply the full `query` to the `Daf` data and return the result. By default, this will cache the final query result, so
repeated identical queries will be accelerated. This may consume a large amount of memory. You can disable it by
specifying `cache = false`, or release the cached data using [`empty_cache!`](@ref).

As a shorthand syntax you can also invoke this using `getindex`, that is, using the `[]` operator (e.g.,
`daf["@ cell"]` is equivalent to `get_query(daf, "@ cell"; cache = false)`). Finally, you can use `|>` to
invoke the query, which is especially useful when constructing it from the operations `Axis("cell") |> get_query(daf)`
or even `"@ cell" |> get_query(daf)`.

!!! note

    Using `get_query`, the query *is* cached (by default). Using `[...]`, the query is *not* cached. That is, `[...]` is
    mostly used for one-off queries (and in interactive sessions, etc.) while `get_query` is used for more "fundamental"
    queries that are expected to be re-used.
"""
function get_query(daf::DafReader; cache::Bool = true)::Tuple{DafReader, Bool}
    return (daf, cache)
end

function Base.:(|>)(
    query::QueryString,
    daf_cache::Tuple{DafReader, Bool},
)::Union{AbstractSet{<:AbstractString}, StorageScalar, NamedArray}
    daf, cache = daf_cache
    return get_query(daf, query; cache)
end

function Base.getindex(
    daf::DafReader,
    query::QueryString,
)::Union{AbstractSet{<:AbstractString}, StorageScalar, NamedArray}
    return get_query(daf, query; cache = false)
end

function get_query(
    daf::DafReader,
    query_string::QueryString;
    cache::Bool = true,
)::Union{AbstractSet{<:AbstractString}, StorageScalar, NamedArray}
    query_sequence = as_query_sequence(query_string)
    cache_key = (CachedQuery, "$(query_sequence)")
    verify_contract_query(daf, cache_key)
    return Formats.with_data_read_lock(daf, "for get_query of:", cache_key) do
        if cache
            result = Formats.get_through_cache(
                daf,
                cache_key,
                Union{AbstractSet{<:AbstractString}, StorageScalar, NamedArray},
                QueryData;
                is_slow = true,
            ) do
                return get_query_result(daf, query_sequence)
            end
        else
            result = Formats.with_cache_read_lock(daf, "for get_query of:", cache_key) do
                return get(daf.internal.cache, cache_key, nothing)
            end
            if result === nothing
                result, _ = get_query_result(daf, query_sequence)
            else
                result = result.data
            end
        end
        @debug "get_query daf: $(brief(daf)) query_sequence: $(query_sequence) cache: $(cache) result: $(brief(result))"
        return result
    end
end

function verify_contract_query(::DafReader, ::CacheKey)::Nothing
    return nothing
end

function assert_is_valid(::Any)::Nothing
    return nothing
end

mutable struct NamesState
    names_set::Maybe{AbstractSet{<:AbstractString}}
end

function print_query_stack_entry(query_operation::QueryOperation)::Nothing  # UNTESTED
    println("   query_operation: $(query_operation)")
    return nothing
end

function print_query_stack_entry(names_state::NamesState)::Nothing  # UNTESTED
    println("   names_set: $(brief(names_state.names_set))")
    return nothing
end

mutable struct ScalarState
    scalar_value::Maybe{StorageScalar}
end

function print_query_stack_entry(scalar_state::ScalarState)::Nothing  # UNTESTED
    println("   scalar_value: $(scalar_state.scalar_value)")
    return nothing
end

mutable struct VectorState
    entries_axis_name::Maybe{AbstractString}
    vector_entries::Maybe{AbstractVector{<:AbstractString}}
    property_name::Maybe{AbstractString}
    property_axis_name::Maybe{AbstractString}
    is_complete_property_axis::Bool
    vector_values::Maybe{StorageVector}
    pending_final_values::Maybe{AbstractVector{Any}}
end

function assert_is_valid(vector_state::VectorState)::Nothing
    if vector_state.vector_entries !== nothing
        @assert vector_state.vector_values !== nothing
        @assert length(vector_state.vector_entries) == length(vector_state.vector_values)
    end
    if vector_state.pending_final_values !== nothing
        @assert vector_state.vector_values !== nothing
        @assert length(vector_state.pending_final_values) == length(vector_state.vector_values)
    end
    if vector_state.is_complete_property_axis
        @assert vector_state.pending_final_values === nothing
    end
    return nothing
end

function VectorState()::VectorState
    return VectorState(nothing, nothing, nothing, nothing, false, nothing, nothing)
end

function Base.copy(vector_state::VectorState)::VectorState
    copy_state = VectorState()
    copy_state.entries_axis_name = vector_state.entries_axis_name
    copy_state.vector_entries = vector_state.vector_entries
    copy_state.property_name = vector_state.property_name
    copy_state.property_axis_name = vector_state.property_axis_name
    copy_state.is_complete_property_axis = vector_state.is_complete_property_axis
    copy_state.vector_values = vector_state.vector_values
    copy_state.pending_final_values = vector_state.pending_final_values
    return copy_state
end

function print_query_stack_entry(vector_state::VectorState)::Nothing  # UNTESTED
    return print_vector_state(vector_state, "")
end

function print_vector_state(vector_state::VectorState, indent::AbstractString)::Nothing  # UNTESTED
    println("   $(indent)entries_axis_name: $(vector_state.entries_axis_name)")
    println("   $(indent)vector_entries: $(brief(vector_state.vector_entries)) = $(vector_state.vector_entries)")
    println("   $(indent)property_name: $(vector_state.property_name)")
    println("   $(indent)property_axis_name: $(vector_state.property_axis_name)")
    println("   $(indent)is_complete_property_axis: $(brief(vector_state.is_complete_property_axis))")
    println("   $(indent)vector_values: $(brief(vector_state.vector_values)) = $(vector_state.vector_values)")
    println(
        "   $(indent)pending_final_values: $(brief(vector_state.pending_final_values)) = $(vector_state.pending_final_values)",
    )
    return nothing
end

mutable struct MatrixState
    rows_state::Maybe{VectorState}
    columns_state::Maybe{VectorState}
    property_name::Maybe{AbstractString}
    property_axis_name::Maybe{AbstractString}
    matrix_values::Maybe{AbstractMatrix}
    pending_final_values::Maybe{AbstractMatrix{Any}}
end

function assert_is_valid(matrix_state::MatrixState)::Nothing
    assert_is_valid(matrix_state.rows_state)
    assert_is_valid(matrix_state.columns_state)
    if matrix_state.pending_final_values !== nothing
        @assert matrix_state.matrix_values !== nothing
        @assert size(matrix_state.pending_final_values) == size(matrix_state.matrix_values)
    end
    if matrix_state.matrix_values !== nothing
        @assert matrix_state.rows_state !== nothing
        @assert matrix_state.rows_state.vector_values !== nothing
        @assert matrix_state.columns_state !== nothing
        @assert matrix_state.columns_state.vector_values !== nothing
        @assert size(matrix_state.matrix_values) ==
                (length(matrix_state.rows_state.vector_values), length(matrix_state.columns_state.vector_values))
    end
    return nothing
end

function MatrixState()::MatrixState
    return MatrixState(nothing, nothing, nothing, nothing, nothing, nothing)
end

function print_query_stack_entry(matrix_state::MatrixState)::Nothing  # UNTESTED
    println("   rows_state:")
    print_vector_state(matrix_state.rows_state, "  ")  # NOJET
    println("   columns_state:")
    print_vector_state(matrix_state.columns_state, "  ")  # NOJET
    println("   property_name: $(matrix_state.property_name)")
    println("   property_axis_name: $(matrix_state.property_axis_name)")
    println("   matrix_values: $(brief(matrix_state.matrix_values)) = $(matrix_state.matrix_values)")
    println(
        "   pending_final_values: $(brief(matrix_state.pending_final_values)) = $(matrix_state.pending_final_values)",
    )
    return nothing
end

QueryStackElement = Union{NamesState, ScalarState, VectorState, MatrixState, QueryOperation}

mutable struct QueryState
    daf::Maybe{DafReader}
    query_sequence::QuerySequence
    first_operation_index::Integer
    next_operation_index::Integer
    what_for::Symbol
    dependency_keys::Maybe{Set{CacheKey}}
    requires_relayout::Bool
    stack::Vector{QueryStackElement}
end

function assert_is_valid(query_state::QueryState)::Nothing
    for element in query_state.stack
        assert_is_valid(element)
    end
end

function print_query_state(query_state::QueryState, where::AbstractString)::Nothing  # UNTESTED
    println("AT: $(where) FOR: $(query_state.what_for)")
    println("FULL: $(query_state.query_sequence)")
    if query_state.first_operation_index > 1
        println(
            "DONE: $(QuerySequence(query_state.query_sequence.query_operations[1:query_state.first_operation_index - 1]))",
        )
    end
    if query_state.next_operation_index > query_state.first_operation_index
        println(
            "CURR: $(QuerySequence(query_state.query_sequence.query_operations[query_state.first_operation_index:query_state.next_operation_index - 1]))",
        )
    end
    if query_state.next_operation_index <= length(query_state.query_sequence.query_operations)
        println(
            "NEXT: $(QuerySequence(query_state.query_sequence.query_operations[query_state.next_operation_index:end]))",
        )
    end

    println("KEYS: $(query_state.dependency_keys === nothing ? nothing : sort(collect(query_state.dependency_keys)))")  # NOJET
    for index in 1:length(query_state.stack)
        println("- $(index): $(typeof(query_state.stack[index]))")
        print_query_stack_entry(query_state.stack[index])
    end
    return nothing
end

function query_state_offset(query_state::QueryState, index::Integer)::Integer
    if index <= 1
        return 0  # UNTESTED
    else
        return length(string(QuerySequence(query_state.query_sequence.query_operations[1:(index - 1)]))) + 1
    end
end

function error_at_state(query_state::QueryState, message::AbstractString)::Nothing
    if query_state.next_operation_index == query_state.first_operation_index
        if query_state.first_operation_index <= length(query_state.query_sequence.query_operations)
            query_state_first_offset = query_state_offset(query_state, query_state.first_operation_index) - 1
        else
            query_state_first_offset = length(string(query_state.query_sequence))  # UNTESTED
        end
        query_state_last_offset = query_state_first_offset + 1
    else
        query_state_first_offset = query_state_offset(query_state, query_state.first_operation_index)
        if query_state.next_operation_index <= length(query_state.query_sequence.query_operations)
            query_state_last_offset = query_state_offset(query_state, query_state.next_operation_index) - 1  # UNTESTED
        else
            query_state_last_offset = length(string(query_state.query_sequence))
        end
    end
    @assert query_state_last_offset > query_state_first_offset

    indent = repeat(" ", max(0, query_state_first_offset))
    marker = repeat("▲", max(1, query_state_last_offset - query_state_first_offset))

    message = chomp(message)
    message *= "\nin the query: $(query_state.query_sequence)\nat location:  $(indent)$(marker)"

    if query_state.daf !== nothing
        message *= "\nfor the daf data: $(query_state.daf.name)"
    end

    error(message)
    @assert false
end

function error_invalid_operation(query_state::QueryState)::Nothing
    query_state.first_operation_index = query_state.next_operation_index
    error_at_state(query_state, "invalid operation(s)")
    @assert false
end

function is_complete(query_state::QueryState)::Bool
    return (
        is_all_stack(query_state, (NamesState,)) ||
        is_all_stack(query_state, (ScalarState,)) ||
        (
            is_all_stack(query_state, (VectorState,)) &&
            query_state.stack[1].vector_entries !== nothing &&
            query_state.stack[1].vector_values !== nothing
        ) ||
        (
            is_all_stack(query_state, (MatrixState,)) &&
            query_state.stack[1].rows_state !== nothing &&
            query_state.stack[1].rows_state.vector_entries !== nothing &&
            query_state.stack[1].rows_state.pending_final_values === nothing &&
            query_state.stack[1].columns_state !== nothing &&
            query_state.stack[1].columns_state.vector_entries !== nothing &&
            query_state.stack[1].columns_state.pending_final_values === nothing &&
            query_state.stack[1].matrix_values !== nothing
        )
    )
end

function is_all_stack(query_state::QueryState, expected::NTuple{N, Union{Type, Function}})::Bool where {N}
    return length(query_state.stack) == length(expected) && stack_has_top(query_state, expected)
end

function stack_has_top(query_state::QueryState, expected::NTuple{N, Union{Type, Function}})::Bool where {N}
    if length(query_state.stack) < length(expected)
        return false
    end

    @views top_of_stack = query_state.stack[(end - length(expected) + 1):end]
    for (query_value, expectation) in zip(top_of_stack, expected)
        if expectation isa Type
            if !(query_value isa expectation)
                return false
            end
        else
            if !expectation(query_state, query_value)
                return false
            end
        end
    end

    return true
end

struct Optional
    condition::Maybe{Union{Type, Function}}
end

function next_matching_operations(
    query_state::QueryState,
    expected::NTuple{N, Union{Optional, Type, Function}},
)::Maybe{AbstractVector{Maybe{QueryOperation}}} where {N}
    matching_operations = Maybe{QueryOperation}[]

    next_operation_index = query_state.next_operation_index
    for expectation in expected
        if next_operation_index > length(query_state.query_sequence.query_operations)
            is_match = false
        else
            if expectation isa Optional
                condition = expectation.condition
            else
                condition = expectation
            end

            query_operation = query_state.query_sequence.query_operations[next_operation_index]
            if condition isa Type
                is_match = query_operation isa condition
            else
                is_match = condition(query_operation)
            end
        end

        if is_match
            push!(matching_operations, query_operation)
            next_operation_index += 1
        elseif expectation isa Optional
            push!(matching_operations, nothing)
        else
            return nothing
        end
    end

    query_state.next_operation_index = next_operation_index
    return matching_operations
end

function get_query_result(
    daf::DafReader,
    query_sequence::QuerySequence,
)::Tuple{Union{AbstractSet{<:AbstractString}, StorageScalar, NamedArray}, Set{CacheKey}}
    query_state = Formats.with_data_read_lock(daf, "for get_query:", query_sequence) do
        @debug "Query: $(query_sequence)"
        return get_query_final_state(daf, query_sequence, :compute)
    end

    if is_all_stack(query_state, (NamesState,))
        return (pop!(query_state.stack).names_set, query_state.dependency_keys)
    end

    if is_all_stack(query_state, (ScalarState,))
        scalar_state = pop!(query_state.stack)
        @assert scalar_state isa ScalarState
        if scalar_state.scalar_value !== nothing
            return (scalar_state.scalar_value, query_state.dependency_keys)
        end
    end

    if is_all_stack(query_state, (VectorState,))
        vector_state = pop!(query_state.stack)
        @assert vector_state isa VectorState
        @assert vector_state.vector_entries !== nothing
        @assert vector_state.vector_values !== nothing
        finalize_vector_values!(query_state, vector_state)
        if vector_state.entries_axis_name === nothing
            named_vector = NamedArray(vector_state.vector_values; names = (vector_state.vector_entries,))  # NOJET
        else
            named_vector = NamedArray(  # NOJET
                vector_state.vector_values;
                names = (vector_state.vector_entries,),
                dimnames = (vector_state.entries_axis_name,),
            )
        end
        return (named_vector, query_state.dependency_keys)
    end

    if is_all_stack(query_state, (MatrixState,))
        matrix_state = pop!(query_state.stack)
        @assert matrix_state isa MatrixState
        @assert matrix_state.rows_state !== nothing
        @assert matrix_state.rows_state.vector_entries !== nothing
        @assert matrix_state.rows_state.pending_final_values === nothing
        @assert matrix_state.columns_state !== nothing
        @assert matrix_state.columns_state.vector_entries !== nothing
        @assert matrix_state.columns_state.pending_final_values === nothing
        @assert matrix_state.matrix_values !== nothing
        finalize_matrix_values!(query_state, matrix_state)

        if matrix_state.matrix_values !== nothing
            if matrix_state.rows_state.entries_axis_name === nothing
                row_axis_name = :A
            else
                row_axis_name = matrix_state.rows_state.entries_axis_name
            end

            if matrix_state.columns_state.entries_axis_name === nothing
                column_axis_name = :B
            else
                column_axis_name = matrix_state.columns_state.entries_axis_name
            end

            named_matrix = NamedArray(  # NOJET
                matrix_state.matrix_values;
                names = (matrix_state.rows_state.vector_entries, matrix_state.columns_state.vector_entries),
                dimnames = (row_axis_name, column_axis_name),
            )

            return (named_matrix, query_state.dependency_keys)
        end
    end

    @assert false
end

function get_query_final_state(daf::Maybe{DafReader}, query_sequence::QuerySequence, what_for::Symbol)::QueryState
    if what_for == :compute
        dependency_keys = Set{CacheKey}()
    else
        dependency_keys = nothing
    end
    query_state = QueryState(daf, query_sequence, 1, 1, what_for, dependency_keys, false, Vector{QueryStackElement}())

    while true
        # print_query_state(query_state, "STATE")
        assert_is_valid(query_state)

        if do_query_phrase(query_state)
            continue
        end

        if query_state.next_operation_index <= length(query_state.query_sequence.query_operations)
            error_invalid_operation(query_state)
            @assert false
        end

        break
    end
    # print_query_state(query_state, "IS DONE")
    assert_is_valid(query_state)

    if !is_complete(query_state)
        text = "invalid query: $(query_state.query_sequence)"
        if daf !== nothing
            text *= "\nfor the daf data: $(query_state.daf.name)"
        end
        error(text)
    end

    return query_state
end

"""
    has_query(daf::DafReader, query::QueryString)::Bool

Return whether the `query` can be successfully applied to the `Daf` data.
"""
function has_query(daf::DafReader, query_string::QueryString)::Bool
    return has_query(daf, as_query_sequence(query_string))
end

function has_query(daf::DafReader, query_sequence::QuerySequence)::Bool
    return Formats.with_data_read_lock(daf, "for has_query:", query_sequence) do
        try
            get_query_final_state(daf, query_sequence, :has_query)
            return true
        catch error
            if error isa AssertionError
                throw(error)  # UNTESTED
            end
            return false
        end
    end
end

"""
    is_axis_query(query::QueryString)::Bool

Returns whether the `query` specifies a (possibly masked) axis. This also verifies the query is syntactically valid,
though it may still fail if applied to specific data due to invalid data values or types.
"""
function is_axis_query(query_sequence::QueryString)::Bool
    query_state = get_query_final_state(nothing, as_query_sequence(query_sequence), :is_axis)
    return is_all_stack(query_state, (VectorState,)) &&
           query_state.stack[end].entries_axis_name !== nothing &&  # NOLINT
           query_state.stack[end].property_axis_name == query_state.stack[end].entries_axis_name &&  # NOLINT
           query_state.stack[end].property_name == "name"  # NOLINT
end

"""
    query_axis_name(query::QueryString)::AbstractString

Return the axis name of a query. This must only be applied to queries that have a vector result that specify an axis,
that is, if [`is_axis_query`](@ref).
"""
function query_axis_name(query_string::QueryString)::AbstractString
    query_sequence = as_query_sequence(query_string)
    query_state = get_query_final_state(nothing, as_query_sequence(query_sequence), :axis_name)
    @assert is_all_stack(query_state, (VectorState,))
    @assert query_state.stack[end].entries_axis_name !== nothing  # NOLINT
    @assert query_state.stack[end].property_name == "name"  # NOLINT
    @assert query_state.stack[end].entries_axis_name == query_state.stack[end].property_axis_name  # NOLINT
    return query_state.stack[end].entries_axis_name  # NOLINT # NOJET
end

"""
    query_result_dimensions(query::QueryString)::Int

Return the number of dimensions (-1 - names, 0 - scalar, 1 - vector, 2 - matrix) of the results of a `query`. This also
verifies the query is syntactically valid, though it may still fail if applied to specific data due to invalid data
values or types.
"""
function query_result_dimensions(query_string::QueryString)::Int
    query_sequence = as_query_sequence(query_string)
    query_state = get_query_final_state(nothing, query_sequence, :result_dimensions)

    if is_all_stack(query_state, (NamesState,))
        return -1
    end

    if is_all_stack(query_state, (ScalarState,))
        return 0
    end

    if is_all_stack(query_state, (VectorState,))
        return 1
    end

    if is_all_stack(query_state, (MatrixState,))
        return 2
    end

    @assert false
end

"""
    query_requires_relayout(daf::DafReader, query::QueryString)::Bool

Whether computing the `query` for the `daf` data requires `relayout` of some matrix. This also verifies the query is
syntactically valid and that the query can be computed, though it may still fail if applied to specific data due to
invalid values or types.
"""
function query_requires_relayout(daf::DafReader, query_string::QueryString)::Bool
    query_sequence = as_query_sequence(query_string)
    query_state = Formats.with_data_read_lock(daf, "for requires_relayout:", query_sequence) do
        return get_query_final_state(daf, query_sequence, :requires_relayout)
    end

    return query_state.requires_relayout
end

"""
    guess_typed_value(value::AbstractString)::StorageScalar

Given a string value, guess the typed value it represents:

  - `true` and `false` are assumed to be `Bool`.
  - Integers are assumed to be `Int64`.
  - Floating point numbers are assumed to be `Float64`, as are `e` and `pi`.
  - Anything else is assumed to be a string.

This doesn't have to be 100% accurate; it is intended to allow omitting the data type in most cases when specifying an
[`IfMissing`](@ref) value. If it guesses wrong, just specify an explicit type (e.g., `. version || 1.0 String`).
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
        if field_name != :type || getfield(computation_operation, :type) !== nothing
            print(io, " ")
            print(io, escape_value(string(field_name)))
            print(io, " ")
            field_value = getfield(computation_operation, field_name)
            if field_value == Float64(e)
                print(io, "e")
            elseif field_value == Float64(pi)
                print(io, "pi")
            elseif field_value isa AbstractString
                print(io, escape_value(field_value))  # UNTESTED
            else
                print(io, field_value)
            end
        end
    end

    return nothing
end

function Base.show(io::IO, eltwise_operation::EltwiseOperation)::Nothing
    show_computation_operation(io, "%", eltwise_operation)
    return nothing
end

function Base.show(io::IO, reduction_operation::ReductionOperation)::Nothing
    show_computation_operation(io, ">>", reduction_operation)
    return nothing
end

"""
    struct Names <: Query end

A query operation for looking up a set of names. In a string [`Query`](@ref), this is specified using the `?` operator.
This is only used in [`NAMES_QUERY`](@ref).
"""
struct Names <: Query end

function Base.show(io::IO, ::Names)::Nothing
    print(io, "?")
    return nothing
end

"""
    struct Axis <: Query
        axis_name::Maybe{AbstractString}
    end

A query operator for specifying an axis. This is used extensively in [`VECTOR_QUERY`](@ref) and [`MATRIX_QUERY`](@ref).
In addition, this is also used to ask for the names of axes (see [`NAMES_QUERY`](@ref)).
"""
struct Axis <: Query
    axis_name::Maybe{AbstractString}
end

function Axis()::Axis
    return Axis(nothing)
end

function Base.show(io::IO, axis::Axis)::Nothing
    if axis.axis_name === nothing
        print(io, "@")
    else
        print(io, "@ $(escape_value(axis.axis_name))")
    end
    return nothing
end

"""
    struct AsAxis <: Query
        axis_name::Maybe{AbstractString}
    end

A query operator for specifying that the values of a property we looked up are the names of entries in some axis. This
is used extensively in [`VECTOR_AS_AXIS`](@ref).
"""
struct AsAxis <: Query
    axis_name::Maybe{AbstractString}
end

function AsAxis()::AsAxis
    return AsAxis(nothing)
end

function Base.show(io::IO, as_axis::AsAxis)::Nothing
    if as_axis.axis_name === nothing
        print(io, "=@")
    else
        print(io, "=@ $(escape_value(as_axis.axis_name))")
    end
    return nothing
end

"""
    struct IfMissing <: QueryOperation
        default_value::StorageScalar
    end

A query operator for specifying a value to use for a property that is missing from the data. This is used anywhere we
look up a property (see [`SCALAR_QUERY`](@ref), [`VECTOR_QUERY`](@ref), [`MATRIX_QUERY`](@ref)).
"""
struct IfMissing <: QueryOperation
    default_value::StorageScalar
end

function Base.show(io::IO, if_missing::IfMissing)::Nothing
    string_default_value = string(if_missing.default_value)
    print(io, "|| $(escape_value(string_default_value))")
    if if_missing.default_value != guess_typed_value(string_default_value)
        print(io, " $(typeof(if_missing.default_value))")
    end
    return nothing
end

function default_value(::Nothing)::UndefInitializer
    return undef
end

function default_value(if_missing::IfMissing)::StorageScalar
    return if_missing.default_value
end

"""
    struct LookupScalar <: Query
        property_name::AbstractString
    end

Lookup the value of a scalar property (see [`SCALAR_QUERY`](@ref)).
"""
struct LookupScalar <: Query
    property_name::AbstractString
end

function Base.show(io::IO, lookup_scalar::LookupScalar)::Nothing
    print(io, ". $(escape_value(lookup_scalar.property_name))")
    return nothing
end

"""
    parse_query(
        query_string::AbstractString,
        operand_only::Maybe{Type{<:QueryOperation}} = nothing
    )::QueryOperation

Parse a query (or a fragment of a query). If the `query_string` contains just a name, and `operand_only` was specified,
then it is assumed this is the type of query operation.
"""
function parse_query(
    query_string::AbstractString,
    operand_only::Maybe{Type{<:QueryOperation}} = nothing,
)::QueryOperation
    tokens = tokenize(query_string, QUERY_OPERATIONS_REGEX)
    if operand_only !== nothing && length(tokens) == 1 && !tokens[1].is_operator
        return operand_only(query_string)  # NOJET
    end

    next_token_index = 1
    query_operations = Vector{QueryOperation}()
    while next_token_index <= length(tokens)
        query_operation, next_token_index = next_query_operation(tokens, next_token_index)
        push!(query_operations, query_operation)
    end

    return QuerySequence(query_operations)
end

"""
    struct LookupVector <: QueryOperation
        property_name::AbstractString
    end

Lookup the value of a vector property (see [`VECTOR_QUERY`](@ref)).
"""
struct LookupVector <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, lookup_vector::LookupVector)::Nothing
    print(io, ": $(escape_value(lookup_vector.property_name))")
    return nothing
end

"""
    struct LookupMatrix <: QueryOperation
        property_name::AbstractString
    end

Lookup the value of a matrix property, even if we immediately slice just a vector (row or column) or even a single
scalar entry out of the matrix (see [`SCALAR_QUERY`](@ref), [`VECTOR_LOOKUP`](@ref) and [`MATRIX_QUERY`](@ref)).
"""
struct LookupMatrix <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, lookup_vector::LookupMatrix)::Nothing
    print(io, ":: $(escape_value(lookup_vector.property_name))")
    return nothing
end

"""
    struct IfNot <: QueryOperation
        final_value::Maybe{StorageScalar}
    end

Specify a final value to use when, having looked up some base property values, we use them as axis entry names to lookup
another property of that axis. If the base property value is empty, then this is an error. Specifying `IfNot` without
a `final_value` allows us to mask out that entry from the result instead. Specifying a `final_value` will use it for
the final property value (since there may be an arbitrarily long chain of lookup operations).
"""
struct IfNot <: QueryOperation
    final_value::Maybe{StorageScalar}
end

function IfNot()
    return IfNot(nothing)
end

function Base.show(io::IO, if_not::IfNot)::Nothing
    final_value = if_not.final_value
    if final_value === nothing
        print(io, "??")
    else
        print(io, "?? $(escape_value(string(final_value)))")
    end
    return nothing
end

"""
    struct BeginMask <: QueryOperation
        property_name::AbstractString
    end

Start specifying a mask to apply to an axis of the result. Must be accompanied by an [`EndMask`](@ref) (see
[`VECTOR_MASK`](@ref)).
"""
struct BeginMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, begin_mask::BeginMask)::Nothing
    return print(io, "[ $(escape_value(begin_mask.property_name))")
end

"""
    struct BeginNegatedMask <: QueryOperation
        property_name::AbstractString
    end

Start specifying a mask to apply to an axis of the result, negating the first mask. Must be accompanied by an
[`EndMask`](@ref) (see [`VECTOR_MASK`](@ref)).
"""
struct BeginNegatedMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, begin_negated_mask::BeginNegatedMask)::Nothing
    return print(io, "[ ! $(escape_value(begin_negated_mask.property_name))")
end

BeginAnyMask = Union{BeginMask, BeginNegatedMask}

"""
    struct EndMask <: QueryOperation end

Finish specifying a mask to apply to an axis of the result, following [`BeginMask`](@ref) or [`BeginNegatedMask`](@ref)
(see [`VECTOR_MASK`](@ref)).
"""
struct EndMask <: QueryOperation end

function Base.show(io::IO, ::EndMask)::Nothing
    return print(io, "]")
end

"""
    struct IsLess <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for entries that are less than the `comparison_value`
(see [`VECTOR_OPERATION`](@ref)).
"""
struct IsLess <: QueryOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsLess)::String
    return "<"
end

function compute_comparison(compared_value::StorageScalar, ::IsLess, comparison_value::StorageScalar)::Bool
    return compared_value < comparison_value
end

"""
    struct IsLessEqual <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for entries that are less than or equal to the
`comparison_value` (see [`VECTOR_OPERATION`](@ref)).
"""
struct IsLessEqual <: QueryOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsLessEqual)::String
    return "<="
end

function compute_comparison(compared_value::StorageScalar, ::IsLessEqual, comparison_value::StorageScalar)::Bool
    return compared_value <= comparison_value
end

"""
    struct IsEqual <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for entries that are equal to the `comparison_value` (see
[`VECTOR_OPERATION`](@ref)).
"""
struct IsEqual <: QueryOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsEqual)::String
    return "="
end

function compute_comparison(compared_value::StorageScalar, ::IsEqual, comparison_value::StorageScalar)::Bool
    return compared_value == comparison_value
end

"""
    struct IsNotEqual <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for entries that are not equal to the `comparison_value`
(see [`VECTOR_OPERATION`](@ref)).
"""
struct IsNotEqual <: QueryOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsNotEqual)::String
    return "!="
end

function compute_comparison(compared_value::StorageScalar, ::IsNotEqual, comparison_value::StorageScalar)::Bool
    return compared_value != comparison_value
end

"""
    struct IsGreaterEqual <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for entries that are greater than or equal to the
`comparison_value` (see [`VECTOR_OPERATION`](@ref)).
"""
struct IsGreaterEqual <: QueryOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsGreaterEqual)::String
    return ">="
end

function compute_comparison(compared_value::StorageScalar, ::IsGreaterEqual, comparison_value::StorageScalar)::Bool
    return compared_value >= comparison_value
end

"""
    struct IsGreater <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for entries that are greater than the `comparison_value`
(see [`VECTOR_OPERATION`](@ref)).
"""
struct IsGreater <: QueryOperation
    comparison_value::StorageScalar
end

function comparison_operator(::IsGreater)::String
    return ">"
end

function compute_comparison(compared_value::StorageScalar, ::IsGreater, comparison_value::StorageScalar)::Bool
    return compared_value > comparison_value
end

"""
    struct IsMatch <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for (string!) entries that are a (complete!) match to the
`comparison_value` regular expression (see [`VECTOR_OPERATION`](@ref)).
"""
struct IsMatch <: QueryOperation
    comparison_value::Union{AbstractString, Regex}
end

function comparison_operator(::IsMatch)::String
    return "~"
end

function compute_comparison(compared_value::AbstractString, ::IsMatch, comparison_regex::Regex)::Bool
    return occursin(comparison_regex, compared_value)
end

"""
    struct IsNotMatch <: QueryOperation
        comparison_value::StorageScalar
    end

Convert a vector of values to a vector of Booleans, is true for (string!) entries that are not a (complete!) match to
the `comparison_value` regular expression (see [`VECTOR_OPERATION`](@ref)).
"""
struct IsNotMatch <: QueryOperation
    comparison_value::Union{AbstractString, Regex}
end

function comparison_operator(::IsNotMatch)::String
    return "!~"
end

function compute_comparison(compared_value::AbstractString, ::IsNotMatch, comparison_regex::Regex)::Bool
    return !occursin(comparison_regex, compared_value)
end

VectorComparisonOperation =
    Union{IsLess, IsLessEqual, IsEqual, IsNotEqual, IsGreaterEqual, IsGreater, IsMatch, IsNotMatch}

function Base.show(io::IO, comparison_operation::VectorComparisonOperation)::Nothing
    print(
        io,
        "$(comparison_operator(comparison_operation)) $(escape_value(string(comparison_operation.comparison_value)))",
    )
    return nothing
end

"""
    struct SquareColumnIs <: QueryOperation
        comparison_value::AbstractString
    end

Whenever extracting a vector from a square matrix, specify the axis entry that identifies the column to extract. This is
used in any phrase that looks up a vector out of a matrix (see [`VECTOR_QUERY`](@ref) and [`MATRIX_QUERY`](@ref)).

!!! note

    Julia and `Daf` use column-major layout as their default, so this is typically the natural way to extract a vector
    from a square matrix (e.g., for a square `is_in_neighborhood` matrix per block per block, the column is the base
    block and the rows are the other block, so the column vector contains a mask of all the blocks in the neighborhood
    of the base block).
"""
struct SquareColumnIs <: QueryOperation
    comparison_value::AbstractString
end

function Base.show(io::IO, columns_is::SquareColumnIs)::Nothing
    print(io, "@| $(escape_value(columns_is.comparison_value))")
    return nothing
end

"""
    struct SquareRowIs <: QueryOperation
        comparison_value::AbstractString
    end

Whenever extracting a vector from a square matrix, specify the axis entry that identifies the row to extract. This is
used in any phrase that looks up a vector out of a matrix (see [`VECTOR_QUERY`](@ref) and [`MATRIX_QUERY`](@ref)).

!!! note

    Julia and `Daf` use column-major layout as their default, so this typically cuts across the natural way to extract a
    vector from a square matrix (e.g., for a square `is_in_neighborhood` matrix per block per block, the column is the
    base block and the rows are the other block, so the row vector contains a mask of all the base blocks that a given
    block is in the neighborhood of).
"""
struct SquareRowIs <: QueryOperation
    comparison_value::AbstractString
end

function Base.show(io::IO, square_row_is::SquareRowIs)::Nothing
    print(io, "@| $(escape_value(square_row_is.comparison_value))")
    return nothing
end

"""
    struct AndMask <: QueryOperation
        property_name::AbstractString
    end

Combine a mask with another, using the bitwise AND operator (see [`VECTOR_MASK`](@ref)).
"""
struct AndMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, and_mask::AndMask)::Nothing
    print(io, "& $(escape_value(and_mask.property_name))")
    return nothing
end

function combine_masks(
    first_mask::AbstractVector{Bool},
    ::AndMask,
    second_mask::AbstractVector{Bool},
)::AbstractVector{Bool}
    return Vector{Bool}(first_mask .& second_mask)
end

"""
    struct AndNegatedMask <: QueryOperation
        property_name::AbstractString
    end

Combine a mask with another, using the bitwise AND-NOT operator (see [`VECTOR_MASK`](@ref)).
"""
struct AndNegatedMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, and_negated_mask::AndNegatedMask)::Nothing
    print(io, "& ! $(escape_value(and_negated_mask.property_name))")
    return nothing
end

function combine_masks(
    first_mask::AbstractVector{Bool},
    ::AndNegatedMask,
    second_mask::AbstractVector{Bool},
)::AbstractVector{Bool}
    return Vector{Bool}(first_mask .& .!second_mask)
end

"""
    struct OrMask <: QueryOperation
        property_name::AbstractString
    end

Combine a mask with another, using the bitwise OR operator (see [`VECTOR_MASK`](@ref)).
"""
struct OrMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, or_mask::OrMask)::Nothing
    print(io, "| $(escape_value(or_mask.property_name))")
    return nothing
end

function combine_masks(
    first_mask::AbstractVector{Bool},
    ::OrMask,
    second_mask::AbstractVector{Bool},
)::AbstractVector{Bool}
    return Vector{Bool}(first_mask .| second_mask)
end

"""
    struct OrNegatedMask <: QueryOperation
        property_name::AbstractString
    end

Combine a mask with another, using the bitwise OR-NOT operator (see [`VECTOR_MASK`](@ref)).
"""
struct OrNegatedMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, or_negated_mask::OrNegatedMask)::Nothing
    print(io, "| ! $(escape_value(or_negated_mask.property_name))")
    return nothing
end

function combine_masks(
    first_mask::AbstractVector{Bool},
    ::OrNegatedMask,
    second_mask::AbstractVector{Bool},
)::AbstractVector{Bool}
    return Vector{Bool}(first_mask .| .!second_mask)
end

"""
    struct XorMask <: QueryOperation
        property_name::AbstractString
    end

Combine a mask with another, using the bitwise XOR operator (see [`VECTOR_MASK`](@ref)).
"""
struct XorMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, xor_mask::XorMask)::Nothing
    print(io, "^ $(escape_value(xor_mask.property_name))")
    return nothing
end

function combine_masks(
    first_mask::AbstractVector{Bool},
    ::XorMask,
    second_mask::AbstractVector{Bool},
)::AbstractVector{Bool}
    return Vector{Bool}(@. xor(first_mask, second_mask))
end

"""
    struct XorNegatedMask <: QueryOperation
        property_name::AbstractString
    end

Combine a mask with another, using the bitwise XOR-NOT operator (see [`VECTOR_MASK`](@ref)).
"""
struct XorNegatedMask <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, xor_negated_mask::XorNegatedMask)::Nothing
    print(io, "^ ! $(escape_value(xor_negated_mask.property_name))")
    return nothing
end

function combine_masks(
    first_mask::AbstractVector{Bool},
    ::XorNegatedMask,
    second_mask::AbstractVector{Bool},
)::AbstractVector{Bool}
    return Vector{Bool}(@. xor(first_mask, .!second_mask))
end

MaskOperation = Union{AndMask, AndNegatedMask, OrMask, OrNegatedMask, XorMask, XorNegatedMask}

"""
    struct GroupBy <: QueryOperation
        property_name::AbstractString
    end

Specify value per vector entry to group vector values by, must be followed by a [`ReductionOperation`](@ref) to reduce
each group of values to a single value (see [`VECTOR_GROUP`](@ref)).
"""
struct GroupBy <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, group_by::GroupBy)::Nothing
    print(io, "/ $(escape_value(group_by.property_name))")
    return nothing
end

"""
    struct GroupColumnsBy <: QueryOperation
        property_name::AbstractString
    end

Specify value per matrix column to group the columns by, must be followed by a [`ReduceToColumn`](@ref) to reduce each
group of columns to a single column (see [`MATRIX_GROUP`](@ref)).
"""
struct GroupColumnsBy <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, group_columns_by::GroupColumnsBy)::Nothing
    print(io, "|/ $(escape_value(group_columns_by.property_name))")
    return nothing
end

"""
    struct GroupRowsBy <: QueryOperation
        property_name::AbstractString
    end

Specify value per matrix row to group the rows by, must be followed by a [`ReduceToRow`](@ref) to reduce each group of
rows to a single row (see [`MATRIX_GROUP`](@ref)).
"""
struct GroupRowsBy <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, group_rows_by::GroupRowsBy)::Nothing
    print(io, "-/ $(escape_value(group_rows_by.property_name))")
    return nothing
end

GroupAnyBy = Union{GroupColumnsBy, GroupRowsBy}

"""
    struct ReduceToRow <: QueryOperation
        reduction_operation::ReductionOperation
    end

Specify a [`ReductionOperation`](@ref) to convert each column of a matrix to a single value, reducing the matrix to a
single row (see [`VECTOR_FROM_MATRIX`](@ref) and [`MATRIX_GROUP`](@ref)).
"""
struct ReduceToRow <: QueryOperation
    reduction_operation::ReductionOperation
end

function Base.show(io::IO, reduce_to_row::ReduceToRow)::Nothing
    show_computation_operation(io, ">-", reduce_to_row.reduction_operation)
    return nothing
end

"""
    struct ReduceToColumn <: QueryOperation
        reduction_operation::ReductionOperation
    end

Specify a [`ReductionOperation`](@ref) to convert each row of a matrix to a single value, reducing the matrix to a
single column (see [`VECTOR_FROM_MATRIX`](@ref) and [`MATRIX_GROUP`](@ref)).
"""
struct ReduceToColumn <: QueryOperation
    reduction_operation::ReductionOperation
end

function Base.show(io::IO, reduce_to_column::ReduceToColumn)::Nothing
    show_computation_operation(io, ">|", reduce_to_column.reduction_operation)
    return nothing
end

"""
    struct CountBy <: QueryOperation
        property_name::AbstractString
    end

Specify a second property for each vector entry, to compute a matrix of counts of the entries with each combination of
values (see [`MATRIX_COUNT`](@ref).
"""
struct CountBy <: QueryOperation
    property_name::AbstractString
end

function Base.show(io::IO, count_by::CountBy)::Nothing
    print(io, "* $(escape_value(count_by.property_name))")
    return nothing
end

function reduce_vector_to_scalar(
    query_state::QueryState,
    vector_state::VectorState,
    reduction_operation::ReductionOperation,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    if query_state.what_for !== :compute
        push!(query_state.stack, ScalarState(nothing))
    elseif eltype(vector_state.vector_values) <: AbstractString && !supports_strings(reduction_operation)
        error_at_state(
            query_state,
            """
            unsupported input type: String
            for the reduction operation: $(typeof(reduction_operation))
            """,
        )
    elseif length(vector_state.vector_values) > 0
        push!(query_state.stack, ScalarState(compute_reduction(reduction_operation, vector_state.vector_values)))  # NOLINT
    elseif if_missing !== nothing
        push!(query_state.stack, ScalarState(if_missing.default_value))
    else
        error_at_state(query_state, "no IfMissing value specified for reducing an empty vector")
    end

    return nothing
end

function reduce_matrix_to_scalar(
    query_state::QueryState,
    matrix_state::MatrixState,
    reduction_operation::ReductionOperation,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    if query_state.what_for !== :compute
        push!(query_state.stack, ScalarState(nothing))
    elseif eltype(matrix_state.matrix_values) <: AbstractString && !supports_strings(reduction_operation)
        error_at_state(
            query_state,
            """
            unsupported input type: String
            for the reduction operation: $(typeof(reduction_operation))
            """,
        )
    elseif length(matrix_state.matrix_values) > 0
        push!(query_state.stack, ScalarState(compute_reduction(reduction_operation, matrix_state.matrix_values)))  # NOLINT
    elseif if_missing !== nothing
        push!(query_state.stack, ScalarState(if_missing.default_value))
    else
        error_at_state(query_state, "no IfMissing value specified for reducing an empty matrix")
    end

    return nothing
end

QUERY_OPERATIONS_REGEX = r"^(?:[!<>]=|!~|[\|-]/|[&^\|]!|\?\?|@[-\|]|=@|::|>[->\|]|\|\||[!&*%./:<=>?@\[\]^\|~])"

struct NotModifier end

QUERY_OPERATIONS_DICT = Dict(
    "!" => (NotModifier, nothing),
    "!=" => (IsNotEqual, true),
    "!~" => (IsNotMatch, true),
    "& !" => (AndNegatedMask, true),
    "&" => (AndMask, true),
    "*" => (CountBy, true),
    "." => (LookupScalar, true),
    "/" => (GroupBy, true),
    "-/" => (GroupRowsBy, true),
    "|/" => (GroupColumnsBy, true),
    ":" => (LookupVector, true),
    "::" => (LookupMatrix, true),
    "<" => (IsLess, true),
    "<=" => (IsLessEqual, true),
    "=" => (IsEqual, true),
    ">" => (IsGreater, true),
    ">-" => (ReduceToRow, true),
    ">=" => (IsGreaterEqual, true),
    ">|" => (ReduceToColumn, true),
    "?" => (Names, nothing),
    "??" => (IfNot, false),
    "@" => (Axis, false),
    "=@" => (AsAxis, false),
    "@-" => (SquareRowIs, true),
    "@|" => (SquareColumnIs, true),
    "[ !" => (BeginNegatedMask, true),
    "[" => (BeginMask, true),
    "]" => (EndMask, nothing),
    "^ !" => (XorNegatedMask, true),
    "^" => (XorMask, true),
    "| !" => (OrNegatedMask, true),
    "|" => (OrMask, true),
    "~" => (IsMatch, true),
)

function axis_with_name(query_operation::QueryOperation)::Bool
    return query_operation isa Axis && query_operation.axis_name !== nothing
end

function axis_without_name(query_operation::QueryOperation)::Bool
    return query_operation isa Axis && query_operation.axis_name === nothing
end

function vector_axis(::QueryState, query_value::QueryStackElement)::Bool
    return query_value isa VectorState && query_value.property_axis_name !== nothing
end

function vector_maybe_axis(query_state::QueryState, query_value::QueryStackElement)::Bool
    return query_value isa VectorState && (
        (query_state.what_for != :compute || eltype(query_value.vector_values) <: AbstractString) &&
        (query_value.property_axis_name !== nothing || query_value.property_name !== nothing)
    )
end

function matrix_maybe_axis(query_state::QueryState, query_value::QueryStackElement)::Bool
    return query_value isa MatrixState &&
           query_value.property_name !== nothing &&
           query_value.property_axis_name === nothing &&
           (query_state.what_for != :compute || eltype(query_value.matrix_values) <: AbstractString)
end

function names_of_matrices(query_state::QueryState, rows_axis::Axis, columns_axis::Axis, ::Names)::Nothing
    @assert rows_axis.axis_name !== nothing
    @assert columns_axis.axis_name !== nothing

    if query_state.what_for != :compute
        push!(query_state.stack, NamesState(nothing))

    else
        push!(query_state.stack, NamesState(matrices_set(query_state.daf, rows_axis.axis_name, columns_axis.axis_name)))  # NOJET
        push!(  # NOJET
            query_state.dependency_keys,
            Formats.matrices_set_cache_key(rows_axis.axis_name, columns_axis.axis_name; relayout = true),
        )
    end

    return nothing
end

function names_of_vectors(query_state::QueryState, axis::Axis, ::Names)::Nothing
    @assert axis.axis_name !== nothing

    if query_state.what_for != :compute
        push!(query_state.stack, NamesState(nothing))

    else
        push!(query_state.stack, NamesState(vectors_set(query_state.daf, axis.axis_name)))  # NOJET
        push!(query_state.dependency_keys, Formats.vectors_set_cache_key(axis.axis_name))  # NOJET
    end

    return nothing
end

function names_of_axes(query_state::QueryState, axis::Axis, ::Names)::Nothing
    @assert axis.axis_name === nothing

    if query_state.what_for != :compute
        push!(query_state.stack, NamesState(nothing))

    else
        push!(query_state.stack, NamesState(axes_set(query_state.daf)))  # NOJET
        push!(query_state.dependency_keys, Formats.axes_set_cache_key())  # NOJET
    end

    return nothing
end

function names_of_scalars(query_state::QueryState, ::Names)::Nothing
    if query_state.what_for != :compute
        push!(query_state.stack, NamesState(nothing))

    else
        @assert query_state.daf !== nothing
        push!(query_state.stack, NamesState(scalars_set(query_state.daf)))  # NOJET
        push!(query_state.dependency_keys, Formats.scalars_set_cache_key())  # NOJET
    end

    return nothing
end

function scalar_lookup(query_state::QueryState, lookup_scalar::LookupScalar, if_missing::Maybe{IfMissing})::Nothing
    if query_state.what_for != :compute
        push!(query_state.stack, ScalarState(nothing))

    else
        @assert query_state.daf !== nothing
        default = default_value(if_missing)
        push!(query_state.stack, ScalarState(get_scalar(query_state.daf, lookup_scalar.property_name; default)))  # NOJET
        push!(query_state.dependency_keys, Formats.scalar_cache_key(lookup_scalar.property_name))  # NOJET
    end

    return nothing
end

function lookup_vector_entry(
    query_state::QueryState,
    lookup_vector::LookupVector,
    if_missing::Maybe{IfMissing},
    axis::Axis,
    is_equal::IsEqual,
)::Nothing
    @assert axis.axis_name !== nothing

    if query_state.what_for != :compute
        push!(query_state.stack, ScalarState(nothing))
        return nothing
    end

    @assert query_state.daf !== nothing
    default = default_value(if_missing)
    named_vector = get_vector(query_state.daf, axis.axis_name, lookup_vector.property_name; default)  # NOJET
    scalar_value = named_vector[is_equal.comparison_value]
    push!(query_state.stack, ScalarState(scalar_value))
    push!(query_state.dependency_keys, Formats.vector_cache_key(axis.axis_name, lookup_vector.property_name))  # NOJET

    return nothing
end

function lookup_matrix_entry(
    query_state::QueryState,
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    rows_axis::Axis,
    square_row_is_equal::IsEqual,
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    @assert rows_axis.axis_name !== nothing
    @assert columns_axis.axis_name !== nothing

    if query_state.what_for != :compute
        push!(query_state.stack, ScalarState(nothing))
        return nothing
    end

    @assert query_state.daf !== nothing
    default = default_value(if_missing)
    named_matrix =  # NOJET
        get_matrix(query_state.daf, rows_axis.axis_name, columns_axis.axis_name, lookup_matrix.property_name; default)
    scalar_value = named_matrix[square_row_is_equal.comparison_value, square_column_is_equal.comparison_value]
    push!(query_state.stack, ScalarState(scalar_value))
    push!(  # NOJET
        query_state.dependency_keys,
        Formats.matrix_cache_key(rows_axis.axis_name, columns_axis.axis_name, lookup_matrix.property_name),
    )

    return nothing
end

function eltwise_scalar(
    query_state::QueryState,
    scalar_state::ScalarState,
    eltwise_operation::EltwiseOperation,
)::Nothing
    if query_state.what_for == :compute
        if scalar_state.scalar_value isa AbstractString && !supports_strings(eltwise_operation)
            error_at_state(
                query_state,
                """
                unsupported input type: String
                for the eltwise operation: $(typeof(eltwise_operation))
                """,
            )
        end

        scalar_state.scalar_value = compute_eltwise(eltwise_operation, scalar_state.scalar_value)  # NOLINT
    end

    push!(query_state.stack, scalar_state)
    return nothing
end

function axis_lookup(query_state::QueryState, axis::Axis)::Nothing
    @assert axis.axis_name !== nothing

    vector_state = VectorState()

    if query_state.what_for != :compute
        vector_state.vector_entries = String[]
    else
        @assert query_state.daf !== nothing
        vector_state.vector_entries = axis_vector(query_state.daf, axis.axis_name)  # NOJET
        push!(query_state.dependency_keys, Formats.axis_vector_cache_key(axis.axis_name))  # NOJET
    end

    vector_state.entries_axis_name = axis.axis_name
    vector_state.property_name = "name"
    vector_state.property_axis_name = axis.axis_name
    vector_state.is_complete_property_axis = true
    vector_state.vector_values = vector_state.vector_entries

    push!(query_state.stack, vector_state)
    return nothing
end

function ensure_vector_is_axis(query_state::QueryState, vector_state::VectorState, as_axis::Maybe{AsAxis})::Nothing
    if vector_state.property_axis_name === nothing || as_axis !== nothing
        vector_property_is_axis(query_state, vector_state, as_axis)
        @assert pop!(query_state.stack) === vector_state
    end
    @assert vector_state.property_axis_name !== nothing
    return nothing
end

function lookup_vector_by_vector(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_vector::LookupVector,
    if_missing::Maybe{IfMissing},
)::Nothing
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    if query_state.what_for != :compute
        if vector_state.property_axis_name === nothing
            vector_state.property_axis_name = vector_state.property_name  # UNTESTED
        end
    else
        @assert query_state.daf !== nothing

        if vector_state.property_axis_name === nothing
            vector_state.property_axis_name = axis_of_property(query_state.daf, vector_state.property_name)  # NOJET # UNTESTED
        end

        add_final_values!(vector_state, if_not)

        default = default_value(if_missing)
        named_vector_values =  # NOJET
            get_vector(query_state.daf, vector_state.property_axis_name, lookup_vector.property_name; default)
        push!(  # NOJET
            query_state.dependency_keys,
            Formats.vector_cache_key(vector_state.property_axis_name, lookup_vector.property_name),
        )

        if vector_state.pending_final_values !== nothing
            type = eltype(named_vector_values)
            if type <: AbstractString
                type = AbstractString
            end
            lookup_vector_values = Vector{type}(undef, length(vector_state.vector_values))
            for index in 1:length(vector_state.vector_values)
                if vector_state.pending_final_values[index] === nothing
                    lookup_vector_values[index] = named_vector_values[string(vector_state.vector_values[index])]
                elseif type == AbstractString
                    lookup_vector_values[index] = ""
                else
                    lookup_vector_values[index] = zero(type)
                end
            end

        elseif vector_state.is_complete_property_axis
            @assert axis_vector(query_state.daf, vector_state.property_axis_name) == vector_state.vector_values  # NOJET
            lookup_vector_values = named_vector_values.array

        else
            lookup_vector_values = [named_vector_values[string.(value)] for value in vector_state.vector_values]
        end

        vector_state.vector_values = lookup_vector_values
    end

    vector_state.property_name = lookup_vector.property_name
    vector_state.property_axis_name = nothing
    vector_state.is_complete_property_axis = false

    push!(query_state.stack, vector_state)

    return nothing
end

function lookup_matrix_column_by_vector(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    @assert columns_axis.axis_name !== nothing

    ensure_vector_is_axis(query_state, vector_state, as_axis)

    if query_state.what_for == :compute
        fill_lookup_vector_values(
            query_state,
            vector_state,
            if_not,
            lookup_matrix,
            if_missing,
            columns_axis.axis_name,
            square_column_is_equal.comparison_value;
            is_column = true,
        )
    end

    vector_state.property_name = lookup_matrix.property_name
    vector_state.property_axis_name = nothing
    vector_state.is_complete_property_axis = false

    push!(query_state.stack, vector_state)

    return nothing
end

function lookup_square_matrix_column_by_vector(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    if query_state.what_for == :compute
        fill_lookup_vector_values(  # NOJET
            query_state,
            vector_state,
            if_not,
            lookup_matrix,
            if_missing,
            vector_state.property_axis_name,
            square_column_is.comparison_value;
            is_column = true,
        )
    end

    vector_state.property_name = lookup_matrix.property_name
    vector_state.property_axis_name = nothing
    vector_state.is_complete_property_axis = false

    push!(query_state.stack, vector_state)

    return nothing
end

function lookup_square_matrix_row_by_vector(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    if query_state.what_for == :compute
        fill_lookup_vector_values(  # NOJET
            query_state,
            vector_state,
            if_not,
            lookup_matrix,
            if_missing,
            vector_state.property_axis_name,
            square_row_is.comparison_value;
            is_column = false,
        )
    end

    vector_state.property_name = lookup_matrix.property_name
    vector_state.property_axis_name = nothing
    vector_state.is_complete_property_axis = false

    push!(query_state.stack, vector_state)

    return nothing
end

function fill_lookup_vector_values(
    query_state::QueryState,
    vector_state::VectorState,
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    columns_axis_name::AbstractString,
    lookup_value::StorageScalar;
    is_column::Bool,
)::Nothing
    add_final_values!(vector_state, if_not)

    @assert query_state.daf !== nothing
    default = default_value(if_missing)
    named_matrix_values = get_matrix(  # NOJET
        query_state.daf,
        vector_state.property_axis_name,
        columns_axis_name,
        lookup_matrix.property_name;
        default,
    )
    push!(  # NOJET
        query_state.dependency_keys,
        Formats.matrix_cache_key(vector_state.property_axis_name, columns_axis_name, lookup_matrix.property_name),
    )

    if vector_state.pending_final_values !== nothing
        type = eltype(named_matrix)  # NOLINT # NOJET # UNTESTED
        if type <: AbstractString  # UNTESTED
            type = AbstractString  # UNTESTED
        end
        lookup_vector_values = Vector{type}(undef, length(vector_state.vector_values))  # UNTESTED
        for index in 1:length(vector_state.vector_values)  # UNTESTED
            if vector_state.pending_final_values[index] === nothing  # UNTESTED
                if is_column  # UNTESTED
                    lookup_vector_values[index] =  # UNTESTED
                        named_matrix_values[string(vector_state.vector_values[index]), string(lookup_value)]
                else
                    lookup_vector_values[index] =  # UNTESTED
                        named_matrix_values[string(lookup_value), string(vector_state.vector_values[index])]
                end
            elseif type == AbstractString  # UNTESTED
                lookup_vector_values[index] = ""  # UNTESTED
            else
                lookup_vector_values[index] = zero(type)  # UNTESTED
            end
        end

    elseif vector_state.is_complete_property_axis
        @assert axis_vector(query_state.daf, vector_state.property_axis_name) == vector_state.vector_values  # NOJET
        if is_column
            lookup_vector_values = named_matrix_values[:, string(lookup_value)].array
        else
            lookup_vector_values = named_matrix_values[string(lookup_value), :].array
        end

    else
        if is_column
            lookup_vector_values = named_matrix_values[string.(vector_state.vector_values), string(lookup_value)].array
        else
            lookup_vector_values = named_matrix_values[string(lookup_value), string.(vector_state.vector_values)].array
        end
    end

    vector_state.vector_values = lookup_vector_values
    return nothing
end

function add_final_values!(vector_state::VectorState, if_not::Maybe{IfNot})::Nothing
    if if_not !== nothing
        mask = as_booleans(vector_state.vector_values)  # NOJET
        if if_not.final_value === nothing
            vector_state.vector_entries = vector_state.vector_entries[mask]
            vector_state.vector_values = vector_state.vector_values[mask]
            if vector_state.pending_final_values !== nothing
                vector_state.pending_final_values = vector_state.pending_final_values[mask]  # UNTESTED
            end
            vector_state.is_complete_property_axis = false
        else
            if vector_state.pending_final_values === nothing
                vector_state.pending_final_values = Vector{Any}(undef, length(vector_state.vector_values))
                vector_state.pending_final_values .= nothing  # NOJET
            end
            vector_state.pending_final_values[.!mask] .= if_not.final_value
        end
    end
    return nothing
end

function finalize_vector_values!(query_state::QueryState, vector_state::VectorState)::Nothing
    if query_state.what_for === :compute && vector_state.pending_final_values !== nothing
        vector_values = densify(vector_state.vector_values; copy = is_read_only_array(vector_state.vector_values))  # NOLINT
        @assert !is_read_only_array(vector_values)  # NOLINT
        for index in eachindex(vector_values)
            final_value = vector_state.pending_final_values[index]
            if final_value !== nothing
                vector_values[index] = cast_value(query_state, "final", final_value, eltype(vector_values))
            end
        end
        vector_state.vector_values = vector_values
        vector_state.pending_final_values = nothing
    end
    return nothing
end

function lookup_vector_mask(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    begin_mask::BeginAnyMask,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_vector_values!(query_state, vector_state)
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, begin_mask)

    lookup_vector_by_vector(
        query_state,
        copy(vector_state),
        nothing,
        nothing,
        LookupVector(begin_mask.property_name),
        if_missing,
    )
    return nothing
end

function lookup_matrix_column_mask(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    begin_mask::BeginAnyMask,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    @assert columns_axis.axis_name !== nothing
    finalize_vector_values!(query_state, vector_state)
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, begin_mask)

    lookup_matrix_column_by_vector(
        query_state,
        copy(vector_state),
        nothing,
        nothing,
        LookupMatrix(begin_mask.property_name),
        if_missing,
        columns_axis,
        square_column_is_equal,
    )
    return nothing
end

function lookup_square_matrix_column_mask(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    begin_mask::BeginAnyMask,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    @assert vector_state.property_axis_name !== nothing
    finalize_vector_values!(query_state, vector_state)
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, begin_mask)

    lookup_square_matrix_column_by_vector(
        query_state,
        copy(vector_state),
        nothing,
        nothing,
        LookupMatrix(begin_mask.property_name),
        if_missing,
        square_column_is,
    )
    return nothing
end

function lookup_square_matrix_row_mask(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    begin_mask::BeginAnyMask,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    finalize_vector_values!(query_state, vector_state)
    ensure_vector_is_axis(query_state, vector_state, as_axis)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, begin_mask)
    lookup_square_matrix_row_by_vector(
        query_state,
        copy(vector_state),
        nothing,
        nothing,
        LookupMatrix(begin_mask.property_name),
        if_missing,
        square_row_is,
    )
    return nothing
end

function apply_mask(
    query_state::QueryState,
    base_state::VectorState,
    begin_mask::BeginAnyMask,
    mask_state::VectorState,
    ::EndMask,
)::Nothing
    if query_state.what_for != :compute
        base_state.is_complete_property_axis = false
        push!(query_state.stack, base_state)
        return nothing
    end

    finalize_vector_values!(query_state, mask_state)

    named_mask = NamedArray(zeros(Bool, length(base_state.vector_values)); names = (base_state.vector_entries,))
    named_mask[mask_state.vector_entries] .= as_booleans(mask_state.vector_values)  # NOJET

    if begin_mask isa BeginMask
        mask = named_mask.array
    elseif begin_mask isa BeginNegatedMask
        mask = .!named_mask.array
    else
        @assert false
    end

    base_state.vector_values = base_state.vector_values[mask]
    base_state.vector_entries = base_state.vector_entries[mask]
    base_state.is_complete_property_axis = false

    push!(query_state.stack, base_state)
    return nothing
end

function lookup_vector_other_mask(
    query_state::QueryState,
    base_state::VectorState,
    begin_mask::BeginAnyMask,
    first_mask_state::VectorState,
    mask_operation::MaskOperation,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_vector_values!(query_state, first_mask_state)

    push!(query_state.stack, base_state)
    push!(query_state.stack, begin_mask)
    push!(query_state.stack, first_mask_state)
    push!(query_state.stack, mask_operation)

    lookup_vector_by_vector(
        query_state,
        copy(base_state),
        nothing,
        nothing,
        LookupVector(mask_operation.property_name),
        if_missing,
    )
    return nothing
end

function lookup_matrix_column_other_mask(
    query_state::QueryState,
    base_state::VectorState,
    begin_mask::BeginAnyMask,
    first_mask_state::VectorState,
    mask_operation::MaskOperation,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    finalize_vector_values!(query_state, first_mask_state)

    push!(query_state.stack, base_state)
    push!(query_state.stack, begin_mask)
    push!(query_state.stack, first_mask_state)
    push!(query_state.stack, mask_operation)

    lookup_matrix_column_by_vector(
        query_state,
        copy(base_state),
        nothing,
        nothing,
        LookupMatrix(mask_operation.property_name),
        if_missing,
        columns_axis,
        square_column_is_equal,
    )
    return nothing
end

function lookup_square_matrix_column_other_mask(
    query_state::QueryState,
    base_state::VectorState,
    begin_mask::BeginAnyMask,
    first_mask_state::VectorState,
    mask_operation::MaskOperation,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    finalize_vector_values!(query_state, first_mask_state)

    push!(query_state.stack, base_state)
    push!(query_state.stack, begin_mask)
    push!(query_state.stack, first_mask_state)
    push!(query_state.stack, mask_operation)

    lookup_square_matrix_column_by_vector(
        query_state,
        copy(base_state),
        nothing,
        nothing,
        LookupMatrix(mask_operation.property_name),
        if_missing,
        square_column_is,
    )
    return nothing
end

function lookup_square_matrix_row_other_mask(
    query_state::QueryState,
    base_state::VectorState,
    begin_mask::BeginAnyMask,
    first_mask_state::VectorState,
    mask_operation::MaskOperation,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    finalize_vector_values!(query_state, first_mask_state)

    push!(query_state.stack, base_state)
    push!(query_state.stack, begin_mask)
    push!(query_state.stack, first_mask_state)
    push!(query_state.stack, mask_operation)

    lookup_square_matrix_row_by_vector(
        query_state,
        copy(base_state),
        nothing,
        nothing,
        LookupMatrix(mask_operation.property_name),
        if_missing,
        square_row_is,
    )
    return nothing
end

function compute_mask_operation(
    query_state::QueryState,
    base_state::VectorState,
    begin_mask::BeginAnyMask,
    first_mask_state::VectorState,
    mask_operation::MaskOperation,
    second_mask_state::VectorState,
)::Nothing
    @assert first_mask_state.pending_final_values === nothing
    finalize_vector_values!(query_state, second_mask_state)

    if query_state.what_for != :compute
        push!(query_state.stack, base_state)
        push!(query_state.stack, begin_mask)
        push!(query_state.stack, first_mask_state)
        return nothing
    end

    first_named_mask = NamedArray(zeros(Bool, length(base_state.vector_values)); names = (base_state.vector_entries,))
    first_named_mask[first_mask_state.vector_entries] .= as_booleans(first_mask_state.vector_values)  # NOJET

    if begin_mask isa BeginMask
        first_mask = first_named_mask.array
    elseif begin_mask isa BeginNegatedMask
        first_mask = .!first_named_mask.array
        begin_mask = BeginMask(begin_mask.property_name)
    else
        @assert false
    end

    second_named_mask = NamedArray(zeros(Bool, length(base_state.vector_values)); names = (base_state.vector_entries,))
    second_named_mask[second_mask_state.vector_entries] .= as_booleans(second_mask_state.vector_values)  # NOJET
    second_mask = second_named_mask.array

    combined_mask = combine_masks(first_mask, mask_operation, second_mask)

    combined_mask_state = VectorState()
    combined_mask_state.entries_axis_name = base_state.entries_axis_name
    combined_mask_state.vector_entries = base_state.vector_entries
    combined_mask_state.property_name = base_state.property_name
    combined_mask_state.property_axis_name = base_state.property_axis_name
    combined_mask_state.is_complete_property_axis = base_state.is_complete_property_axis
    combined_mask_state.vector_values = combined_mask
    combined_mask_state.pending_final_values = base_state.pending_final_values

    push!(query_state.stack, base_state)
    push!(query_state.stack, begin_mask)
    push!(query_state.stack, combined_mask_state)

    return nothing
end

function lookup_vector_group_by_vector(
    query_state::QueryState,
    vector_state::VectorState,
    group_by::GroupBy,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, group_by)

    lookup_vector_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupVector(group_by.property_name),
        if_missing,
    )
    return nothing
end

function lookup_vector_group_by_matrix_column(
    query_state::QueryState,
    vector_state::VectorState,
    group_by::GroupBy,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, group_by)

    lookup_matrix_column_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupMatrix(group_by.property_name),
        if_missing,
        columns_axis,
        square_column_is_equal,
    )
    return nothing
end

function lookup_vector_group_by_square_matrix_column(
    query_state::QueryState,
    vector_state::VectorState,
    group_by::GroupBy,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, group_by)

    lookup_square_matrix_column_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupMatrix(group_by.property_name),
        if_missing,
        square_column_is,
    )
    return nothing
end

function lookup_vector_group_by_square_matrix_row(
    query_state::QueryState,
    vector_state::VectorState,
    group_by::GroupBy,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, group_by)

    lookup_square_matrix_row_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupMatrix(group_by.property_name),
        if_missing,
        square_row_is,
    )
    return nothing
end

function extract_vector_axis(vector_state::VectorState)::VectorState
    vector_axis = copy(vector_state)
    vector_axis.property_axis_name = vector_axis.entries_axis_name
    vector_axis.is_complete_property_axis = false
    vector_axis.vector_values = vector_axis.vector_entries
    @assert vector_axis.pending_final_values === nothing
    return vector_axis
end

function extract_vector_axis(matrix_state::MatrixState, ::GroupColumnsBy)::VectorState
    @assert matrix_state.columns_state !== nothing
    return extract_vector_axis(matrix_state.columns_state)  # NOJET
end

function extract_vector_axis(matrix_state::MatrixState, ::GroupRowsBy)::VectorState
    @assert matrix_state.rows_state !== nothing
    return extract_vector_axis(matrix_state.rows_state)  # NOJET
end

function reduce_grouped_vector(
    query_state::QueryState,
    base_state::VectorState,
    ::GroupBy,
    group_state::VectorState,
    reduction_operation::ReductionOperation,
    if_missing::Maybe{IfMissing},
)::Nothing
    @assert base_state.pending_final_values === nothing
    finalize_vector_values!(query_state, group_state)

    if query_state.what_for != :compute
        base_state.property_name = nothing
        base_state.property_axis_name = nothing
        base_state.is_complete_property_axis = false

        push!(query_state.stack, base_state)

        return nothing
    end

    named_values = NamedArray(base_state.vector_values; names = (base_state.vector_entries,))  # NOJET
    vector_values = named_values[group_state.vector_entries].array

    if group_state.property_axis_name === nothing
        unique_group_values = sort!(unique(group_state.vector_values))
    else
        @assert query_state.daf !== nothing
        unique_group_values = axis_vector(query_state.daf, group_state.property_axis_name)  # NOJET
    end

    if eltype(vector_values) <: AbstractString && !supports_strings(reduction_operation)
        error_at_state(
            query_state,
            """
            unsupported input type: String
            for the reduction operation: $(typeof(reduction_operation))
            """,
        )
    end

    result_type = reduction_result_type(reduction_operation, eltype(vector_values))
    reduced_values = Vector{result_type}(undef, length(unique_group_values))
    for (group_index, group_value) in enumerate(unique_group_values)
        group_mask = group_state.vector_values .== group_value
        if any(group_mask)
            @views group_vector_values = vector_values[group_mask]
            reduced_values[group_index] = compute_reduction(reduction_operation, group_vector_values)  # NOLINT
        elseif if_missing !== nothing
            reduced_values[group_index] = cast_value(query_state, "missing", if_missing.default_value, result_type)
        else
            error_at_state(
                query_state,
                """
                no IfMissing value specified for the unused entry: $(group_value)
                of the axis: $(group_state.property_axis_name)
                """,
            )
        end
    end

    reduced_state = VectorState()
    reduced_state.entries_axis_name = group_state.property_axis_name
    reduced_state.vector_entries = string.(unique_group_values)
    reduced_state.is_complete_property_axis = base_state.property_axis_name !== nothing
    reduced_state.vector_values = reduced_values

    push!(query_state.stack, reduced_state)

    return nothing
end

function reduce_matrix_to_column(
    query_state::QueryState,
    matrix_state::MatrixState,
    reduce_to_column::ReduceToColumn,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    vector_state = matrix_state.rows_state
    vector_state.property_name = nothing  # NOJET
    vector_state.property_axis_name = nothing

    if query_state.what_for == :compute
        if eltype(matrix_state.matrix_values) <: AbstractString &&
           !supports_strings(reduce_to_column.reduction_operation)
            error_at_state(
                query_state,
                """
                unsupported input type: String
                for the reduction operation: $(typeof(reduce_to_column.reduction_operation))
                """,
            )
        elseif length(matrix_state.matrix_values) > 0
            vector_state.vector_values =
                compute_reduction(reduce_to_column.reduction_operation, matrix_state.matrix_values, Columns)  # NOLINT
        elseif if_missing !== nothing
            vector_state.vector_values = fill(if_missing.default_value, length(vector_state.vector_values))
        else
            error_at_state(query_state, "no IfMissing value specified for reducing an empty matrix")
        end
    end

    push!(query_state.stack, vector_state)  # NOJET
    return nothing
end

function reduce_matrix_to_row(
    query_state::QueryState,
    matrix_state::MatrixState,
    reduce_to_row::ReduceToRow,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    vector_state = matrix_state.columns_state
    vector_state.property_name = nothing
    vector_state.property_axis_name = nothing

    if query_state.what_for == :compute
        if eltype(matrix_state.matrix_values) <: AbstractString && !supports_strings(reduce_to_row.reduction_operation)
            error_at_state(
                query_state,
                """
                unsupported input type: String
                for the reduction operation: $(typeof(reduce_to_row.reduction_operation))
                """,
            )
        elseif length(matrix_state.matrix_values) > 0
            vector_state.vector_values =
                compute_reduction(reduce_to_row.reduction_operation, matrix_state.matrix_values, Rows)  # NOLINT
        elseif if_missing !== nothing
            vector_state.vector_values = fill(if_missing.default_value, length(vector_state.vector_values))
        else
            error_at_state(query_state, "no IfMissing value specified for reducing an empty matrix")
        end
    end

    push!(query_state.stack, vector_state)
    return nothing
end

function eltwise_vector(
    query_state::QueryState,
    vector_state::VectorState,
    eltwise_operation::EltwiseOperation,
)::Nothing
    if query_state.what_for == :compute
        if eltype(vector_state.vector_values) <: AbstractString && !supports_strings(eltwise_operation)
            error_at_state(
                query_state,
                """
                unsupported input type: String
                for the eltwise operation: $(typeof(eltwise_operation))
                """,
            )
        end

        vector_state.vector_values = compute_eltwise(eltwise_operation, vector_state.vector_values)  # NOLINT
    end

    push!(query_state.stack, vector_state)
    return nothing
end

function eltwise_matrix(
    query_state::QueryState,
    matrix_state::MatrixState,
    eltwise_operation::EltwiseOperation,
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    if query_state.what_for == :compute
        if eltype(matrix_state.matrix_values) <: AbstractString && !supports_strings(eltwise_operation)
            error_at_state(
                query_state,
                """
                unsupported input type: String
                for the eltwise operation: $(typeof(eltwise_operation))
                """,
            )
        end

        matrix_state.matrix_values = compute_eltwise(eltwise_operation, matrix_state.matrix_values)  # NOLINT
    end

    push!(query_state.stack, matrix_state)
    return nothing
end

function compare_vector(
    query_state::QueryState,
    vector_state::VectorState,
    comparison_operation::VectorComparisonOperation,
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    if query_state.what_for == :compute
        comparison_value = comparison_operation.comparison_value
        is_string_value = comparison_value isa AbstractString || comparison_value isa Regex
        is_string_vector = eltype(vector_state.vector_values) <: AbstractString

        if comparison_operation isa IsMatch || comparison_operation isa IsNotMatch
            if !is_string_vector
                error_at_state(
                    query_state,
                    """
                    unsupported vector element type: $(eltype(vector_state.vector_values))
                    for the comparison operation: $(typeof(comparison_operation))
                    """,
                )
            end

            if comparison_value isa AbstractString
                try
                    comparison_value = Regex(comparison_value)
                catch exception
                    error_at_state(
                        query_state,
                        """
                        invalid regular expression: $(comparison_value)
                        for the comparison operation: $(typeof(comparison_operation))
                        $(exception)
                        """,
                    )
                end
            end

            @assert comparison_value isa Regex

        elseif !is_string_vector && is_string_value
            try
                comparison_value = parse(Float64, comparison_value)
            catch exception
                error_at_state(
                    query_state,
                    """
                    error parsing number comparison value: $(comparison_value)
                    for comparison with a vector of type: $(eltype(vector_state.vector_values))
                    $(exception)
                    """,
                )
            end
        end

        vector_state.vector_values =
            [compute_comparison(value, comparison_operation, comparison_value) for value in vector_state.vector_values]
    end

    vector_state.property_name = nothing
    vector_state.property_axis_name = nothing
    vector_state.is_complete_property_axis = false

    push!(query_state.stack, vector_state)
    return nothing
end

function vector_property_is_axis(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis} = nothing,
)::Nothing
    @assert vector_state.property_name !== nothing
    @assert vector_state.property_axis_name === nothing
    if query_state.what_for == :compute
        @assert eltype(vector_state.vector_values) <: AbstractString
    end

    finalize_vector_values!(query_state, vector_state)

    if as_axis !== nothing && as_axis.axis_name !== nothing
        axis_name = as_axis.axis_name

    else
        if query_state.what_for != :compute
            axis_name = vector_state.property_name
        else
            @assert query_state.daf !== nothing
            axis_name = axis_of_property(query_state.daf, vector_state.property_name)  # NOJET
        end
    end

    vector_state.property_axis_name = axis_name
    push!(query_state.stack, vector_state)

    return nothing
end

function matrix_lookup(
    query_state::QueryState,
    rows_state::VectorState,
    columns_state::VectorState,
    as_axis::Maybe{AsAxis},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
)::Nothing
    @assert rows_state.pending_final_values === nothing
    @assert rows_state.property_axis_name !== nothing

    finalize_vector_values!(query_state, columns_state)
    ensure_vector_is_axis(query_state, columns_state, as_axis)

    matrix_state = MatrixState()

    matrix_state.rows_state = rows_state
    matrix_state.columns_state = columns_state
    matrix_state.property_name = lookup_matrix.property_name

    if query_state.what_for == :requires_relayout
        @assert query_state.daf !== nothing
        if !has_matrix(
            query_state.daf,
            rows_state.property_axis_name,
            columns_state.property_axis_name,
            lookup_matrix.property_name;
            relayout = false,
        )
            query_state.requires_relayout = has_matrix(
                query_state.daf,
                columns_state.property_axis_name,
                rows_state.property_axis_name,
                lookup_matrix.property_name;
                relayout = false,
            )
        end
        matrix_state.matrix_values = String[;;]

    elseif query_state.what_for == :compute
        @assert query_state.daf !== nothing
        default = default_value(if_missing)
        named_matrix_values = get_matrix(
            query_state.daf,
            rows_state.property_axis_name,
            columns_state.property_axis_name,
            lookup_matrix.property_name;
            default,
        )
        push!(  # NOJET
            query_state.dependency_keys,
            Formats.matrix_cache_key(
                rows_state.property_axis_name,
                columns_state.property_axis_name,
                lookup_matrix.property_name,
            ),
        )

        if rows_state.is_complete_property_axis && columns_state.is_complete_property_axis
            matrix_values = named_matrix_values.array
        elseif rows_state.is_complete_property_axis
            matrix_values = named_matrix_values[:, columns_state.vector_values].array
        elseif columns_state.is_complete_property_axis
            matrix_values = named_matrix_values[rows_state.vector_values, :].array
        else
            matrix_values = named_matrix_values[rows_state.vector_values, columns_state.vector_values].array  # UNTESTED
        end

        matrix_state.matrix_values = matrix_values
    else
        matrix_state.matrix_values = String[;;]
    end

    push!(query_state.stack, matrix_state)

    return nothing
end

function lookup_vector_count(
    query_state::QueryState,
    vector_state::VectorState,
    as_axis::Maybe{AsAxis},
    count_by::CountBy,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    if as_axis !== nothing
        vector_property_is_axis(query_state, vector_state, as_axis)
        @assert pop!(query_state.stack) === vector_state
    end

    push!(query_state.stack, vector_state)
    push!(query_state.stack, count_by)

    lookup_vector_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupVector(count_by.property_name),
        if_missing,
    )
    return nothing
end

function lookup_matrix_column_count(
    query_state::QueryState,
    vector_state::VectorState,
    count_by::CountBy,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    @assert columns_axis.axis_name !== nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, count_by)

    lookup_matrix_column_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupMatrix(count_by.property_name),
        if_missing,
        columns_axis,
        square_column_is_equal,
    )
    return nothing
end

function lookup_square_matrix_column_count(
    query_state::QueryState,
    vector_state::VectorState,
    count_by::CountBy,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, count_by)

    lookup_square_matrix_column_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupMatrix(count_by.property_name),
        if_missing,
        square_column_is,
    )
    return nothing
end

function lookup_square_matrix_row_count(
    query_state::QueryState,
    vector_state::VectorState,
    count_by::CountBy,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    finalize_vector_values!(query_state, vector_state)

    push!(query_state.stack, vector_state)
    push!(query_state.stack, count_by)

    lookup_square_matrix_row_by_vector(
        query_state,
        extract_vector_axis(vector_state),
        nothing,
        nothing,
        LookupMatrix(count_by.property_name),
        if_missing,
        square_row_is,
    )
    return nothing
end

function compute_count_matrix(
    query_state::QueryState,
    rows_state::VectorState,
    ::CountBy,
    columns_state::VectorState,
)::Nothing
    @assert rows_state.pending_final_values === nothing
    finalize_vector_values!(query_state, columns_state)

    if length(rows_state.vector_entries) != length(columns_state.vector_entries)
        error_at_state(  # UNTESTED
            query_state,
            "different CountBy vector lengths: $(length(rows_state.vector_entries)) * $(length(columns_state.vector_entries))",
        )
    end

    matrix_state = MatrixState()

    matrix_state.rows_state, rows_index_of_value = count_by_axis_state(query_state, rows_state)
    matrix_state.columns_state, columns_index_of_value = count_by_axis_state(query_state, columns_state)
    matrix_state.property_name = nothing
    matrix_state.matrix_values = count_by_values(
        rows_state,
        matrix_state.rows_state,
        rows_index_of_value,
        columns_state,
        matrix_state.columns_state,
        columns_index_of_value,
    )

    push!(query_state.stack, matrix_state)

    return nothing
end

function count_by_axis_state(query_state::QueryState, vector_state::VectorState)::Tuple{VectorState, AbstractDict}
    if query_state.what_for !== :compute
        unique_vector_values = vector_state.vector_values
        index_of_value = Dict()
    elseif vector_state.property_axis_name === nothing
        unique_vector_values = sort!(unique(vector_state.vector_values))
        index_of_value = Dict{eltype(unique_vector_values), Int32}()
        for (index, value) in enumerate(unique_vector_values)
            index_of_value[value] = index
        end
    else
        @assert query_state.daf !== nothing
        unique_vector_values = axis_vector(query_state.daf, vector_state.property_axis_name)  # NOJET
        index_of_value = axis_dict(query_state.daf, vector_state.property_axis_name)  # NOJET
    end

    count_state = VectorState()
    count_state.entries_axis_name = vector_state.property_axis_name
    count_state.vector_entries = string.(unique_vector_values)
    count_state.property_axis_name = vector_state.property_axis_name
    count_state.is_complete_property_axis = vector_state.property_axis_name !== nothing
    count_state.vector_values = unique_vector_values

    return (count_state, index_of_value)
end

function count_by_values(
    rows_values::VectorState,
    rows_axis::VectorState,
    rows_index_of_value::AbstractDict,
    columns_values::VectorState,
    columns_axis::VectorState,
    columns_index_of_value::AbstractDict,
)::AbstractMatrix
    @assert length(rows_values.vector_values) == length(columns_values.vector_values)
    n_values = length(rows_values.vector_values)

    matrix_type = UInt64
    for type in (UInt32, UInt16, UInt8)
        if n_values <= typemax(type)
            matrix_type = type
        end
    end

    n_rows = length(rows_axis.vector_values)
    n_columns = length(columns_axis.vector_values)
    counts_matrix = zeros(matrix_type, n_rows, n_columns)

    for (row_value, column_value) in zip(rows_values.vector_values, columns_values.vector_values)  # NOJET
        row_index = get(rows_index_of_value, row_value, nothing)
        column_index = get(columns_index_of_value, column_value, nothing)
        if row_index !== nothing && column_index !== nothing
            @inbounds counts_matrix[row_index, column_index] += 1
        end
    end

    return counts_matrix
end

function lookup_matrix_group_by_vector(
    query_state::QueryState,
    matrix_state::MatrixState,
    group_by::GroupAnyBy,
    if_missing::Maybe{IfMissing},
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    push!(query_state.stack, matrix_state)
    push!(query_state.stack, group_by)

    lookup_vector_by_vector(
        query_state,
        extract_vector_axis(matrix_state, group_by),
        nothing,
        nothing,
        LookupVector(group_by.property_name),
        if_missing,
    )
    return nothing
end

function lookup_matrix_group_by_matrix_column(
    query_state::QueryState,
    matrix_state::MatrixState,
    group_by::GroupAnyBy,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    column_is_equal::IsEqual,
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    @assert columns_axis.axis_name !== nothing
    push!(query_state.stack, matrix_state)
    push!(query_state.stack, group_by)
    lookup_matrix_column_by_vector(
        query_state,
        extract_vector_axis(matrix_state, group_by),
        nothing,
        nothing,
        LookupMatrix(group_by.property_name),
        if_missing,
        columns_axis,
        column_is_equal,
    )
    return nothing
end

function lookup_matrix_group_by_square_matrix_column(
    query_state::QueryState,
    matrix_state::MatrixState,
    group_by::GroupAnyBy,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    push!(query_state.stack, matrix_state)
    push!(query_state.stack, group_by)

    lookup_square_matrix_column_by_vector(
        query_state,
        extract_vector_axis(matrix_state, group_by),
        nothing,
        nothing,
        LookupMatrix(group_by.property_name),
        if_missing,
        square_column_is,
    )
    return nothing
end

function lookup_matrix_group_by_square_matrix_row(
    query_state::QueryState,
    matrix_state::MatrixState,
    group_by::GroupAnyBy,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    finalize_matrix_values!(query_state, matrix_state)

    push!(query_state.stack, matrix_state)
    push!(query_state.stack, group_by)

    lookup_square_matrix_row_by_vector(
        query_state,
        extract_vector_axis(matrix_state, group_by),
        nothing,
        nothing,
        LookupMatrix(group_by.property_name),
        if_missing,
        square_row_is,
    )
    return nothing
end

function compute_grouped_matrix(
    query_state::QueryState,
    base_state::MatrixState,
    ::GroupColumnsBy,
    group_state::VectorState,
    reduce_to_column::ReduceToColumn,
    if_missing::Maybe{IfMissing} = nothing,
)::Nothing
    finalize_vector_values!(query_state, group_state)

    if query_state.what_for != :compute
        base_state.property_name = nothing
        base_state.columns_state.property_name = nothing
        base_state.columns_state.property_axis_name = nothing
        base_state.columns_state.is_complete_property_axis = false

        push!(query_state.stack, base_state)

        return nothing
    end

    named_values = NamedArray(  # NOJET
        base_state.matrix_values;
        names = (base_state.rows_state.vector_entries, base_state.columns_state.vector_entries),
    )
    matrix_values = named_values[:, group_state.vector_entries].array

    if group_state.property_axis_name === nothing
        unique_group_values = sort!(unique(group_state.vector_values))
    else
        @assert query_state.daf !== nothing
        unique_group_values = axis_vector(query_state.daf, group_state.property_axis_name)  # NOJET
    end

    if eltype(matrix_values) <: AbstractString && !supports_strings(reduce_to_column.reduction_operation)
        error_at_state(
            query_state,
            """
            unsupported input type: String
            for the reduction operation: $(typeof(reduce_to_column.reduction_operation))
            """,
        )
    end

    result_type = reduction_result_type(reduce_to_column.reduction_operation, eltype(matrix_values))
    reduced_values =
        Matrix{result_type}(undef, length(base_state.rows_state.vector_entries), length(unique_group_values))
    for (group_index, group_value) in enumerate(unique_group_values)
        group_mask = group_state.vector_values .== group_value
        if any(group_mask)
            @views group_matrix_values = matrix_values[:, group_mask]
            reduced_values[:, group_index] =
                vec(compute_reduction(reduce_to_column.reduction_operation, group_matrix_values, 2))  # NOLINT
        elseif if_missing !== nothing
            reduced_values[:, group_index] .= cast_value(query_state, "missing", if_missing.default_value, result_type)
        else
            error_at_state(
                query_state,
                """
                no IfMissing value specified for the unused entry: $(group_value)
                of the axis: $(group_state.property_axis_name)
                """,
            )
        end
    end

    reduced_state = MatrixState()
    reduced_state.rows_state = base_state.rows_state
    reduced_state.columns_state = VectorState()
    reduced_state.columns_state.entries_axis_name = group_state.property_axis_name
    reduced_state.columns_state.vector_entries = string.(unique_group_values)
    reduced_state.columns_state.is_complete_property_axis = group_state.property_axis_name !== nothing
    reduced_state.columns_state.vector_values = unique_group_values
    reduced_state.matrix_values = reduced_values

    push!(query_state.stack, reduced_state)

    return nothing
end

function compute_grouped_matrix(
    query_state::QueryState,
    base_state::MatrixState,
    ::GroupRowsBy,
    group_state::VectorState,
    reduce_to_row::ReduceToRow,
    if_missing::Maybe{IfMissing} = nothing,
)::Nothing
    finalize_vector_values!(query_state, group_state)

    if query_state.what_for != :compute
        base_state.property_name = nothing
        base_state.rows_state.property_name = nothing
        base_state.rows_state.property_axis_name = nothing
        base_state.rows_state.is_complete_property_axis = false

        push!(query_state.stack, base_state)

        return nothing
    end

    named_values = NamedArray(  # NOJET
        base_state.matrix_values;
        names = (base_state.rows_state.vector_entries, base_state.columns_state.vector_entries),
    )
    matrix_values = named_values[group_state.vector_entries, :].array

    if group_state.property_axis_name === nothing
        unique_group_values = sort!(unique(group_state.vector_values))
    else
        @assert query_state.daf !== nothing
        unique_group_values = axis_vector(query_state.daf, group_state.property_axis_name)  # NOJET
    end

    if eltype(matrix_values) <: AbstractString && !supports_strings(reduce_to_row.reduction_operation)
        error_at_state(
            query_state,
            """
            unsupported input type: String
            for the reduction operation: $(typeof(reduce_to_row.reduction_operation))
            """,
        )
    end

    result_type = reduction_result_type(reduce_to_row.reduction_operation, eltype(matrix_values))
    reduced_values =
        Matrix{result_type}(undef, length(unique_group_values), length(base_state.columns_state.vector_entries))
    for (group_index, group_value) in enumerate(unique_group_values)
        group_mask = group_state.vector_values .== group_value
        if any(group_mask)
            @views group_matrix_values = matrix_values[group_mask, :]
            reduced_values[group_index, :] =
                vec(compute_reduction(reduce_to_row.reduction_operation, group_matrix_values, 1))  # NOLINT
        elseif if_missing !== nothing
            reduced_values[group_index, :] .= cast_value(query_state, "missing", if_missing.default_value, result_type)
        else
            error_at_state(
                query_state,
                """
                no IfMissing value specified for the unused entry: $(group_value)
                of the axis: $(group_state.property_axis_name)
                """,
            )
        end
    end

    reduced_state = MatrixState()
    reduced_state.rows_state = VectorState()
    reduced_state.rows_state.entries_axis_name = group_state.property_axis_name
    reduced_state.rows_state.vector_entries = string.(unique_group_values)
    reduced_state.rows_state.is_complete_property_axis = group_state.property_axis_name !== nothing
    reduced_state.rows_state.vector_values = unique_group_values
    reduced_state.columns_state = base_state.columns_state
    reduced_state.matrix_values = reduced_values

    push!(query_state.stack, reduced_state)

    return nothing
end

function ensure_matrix_is_axis(query_state::QueryState, matrix_state::MatrixState, as_axis::Maybe{AsAxis})::Nothing
    if matrix_state.property_axis_name === nothing || as_axis !== nothing
        matrix_property_is_axis(query_state, matrix_state, as_axis)
        @assert pop!(query_state.stack) === matrix_state
    end
    @assert matrix_state.property_axis_name !== nothing
    return nothing
end

function lookup_vector_by_matrix(
    query_state::QueryState,
    matrix_state::MatrixState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_vector::LookupVector,
    if_missing::Maybe{IfMissing},
)::Nothing
    ensure_matrix_is_axis(query_state, matrix_state, as_axis)

    if query_state.what_for == :compute
        add_final_values!(matrix_state, if_not)

        @assert query_state.daf !== nothing
        default = default_value(if_missing)
        named_vector_values =  # NOJET
            get_vector(query_state.daf, matrix_state.property_axis_name, lookup_vector.property_name; default)
        push!(  # NOJET
            query_state.dependency_keys,
            Formats.vector_cache_key(matrix_state.property_axis_name, lookup_vector.property_name),
        )

        type = eltype(named_vector_values)
        if type <: AbstractString
            type = AbstractString
        end
        n_rows, n_columns = size(matrix_state.matrix_values)
        lookup_matrix_values = Matrix{type}(undef, n_rows, n_columns)
        for column_index in 1:n_columns
            for row_index in 1:n_rows
                if matrix_state.pending_final_values === nothing ||
                   matrix_state.pending_final_values[row_index, column_index] === nothing
                    lookup_matrix_values[row_index, column_index] =
                        named_vector_values[string(matrix_state.matrix_values[row_index, column_index])]
                elseif type == AbstractString
                    lookup_matrix_values[row_index, column_index] = ""
                else
                    lookup_matrix_values[row_index, column_index] = zero(type)  # UNTESTED
                end
            end
        end

        matrix_state.matrix_values = lookup_matrix_values
    end

    matrix_state.property_name = lookup_vector.property_name
    matrix_state.property_axis_name = nothing

    push!(query_state.stack, matrix_state)

    return nothing
end

function lookup_matrix_column_by_matrix(
    query_state::QueryState,
    matrix_state::MatrixState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    columns_axis::Axis,
    square_column_is_equal::IsEqual,
)::Nothing
    @assert columns_axis.axis_name !== nothing

    ensure_matrix_is_axis(query_state, matrix_state, as_axis)

    if query_state.what_for == :compute
        fill_lookup_matrix_values(
            query_state,
            matrix_state,
            if_not,
            lookup_matrix,
            if_missing,
            columns_axis.axis_name,
            square_column_is_equal.comparison_value;
            is_column = true,
        )
    end

    matrix_state.property_name = lookup_matrix.property_name
    matrix_state.property_axis_name = nothing

    push!(query_state.stack, matrix_state)

    return nothing
end

function lookup_square_matrix_column_by_matrix(
    query_state::QueryState,
    matrix_state::MatrixState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    square_column_is::SquareColumnIs,
)::Nothing
    ensure_matrix_is_axis(query_state, matrix_state, as_axis)

    if query_state.what_for == :compute
        fill_lookup_matrix_values(
            query_state,
            matrix_state,
            if_not,
            lookup_matrix,
            if_missing,
            matrix_state.property_axis_name,
            square_column_is.comparison_value;
            is_column = true,
        )
    end

    matrix_state.property_name = lookup_matrix.property_name
    matrix_state.property_axis_name = nothing

    push!(query_state.stack, matrix_state)

    return nothing
end

function lookup_square_matrix_row_by_matrix(
    query_state::QueryState,
    matrix_state::MatrixState,
    as_axis::Maybe{AsAxis},
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    square_row_is::SquareRowIs,
)::Nothing
    ensure_matrix_is_axis(query_state, matrix_state, as_axis)

    if query_state.what_for == :compute
        fill_lookup_matrix_values(
            query_state,
            matrix_state,
            if_not,
            lookup_matrix,
            if_missing,
            matrix_state.property_axis_name,
            square_row_is.comparison_value;
            is_column = false,
        )
    end

    matrix_state.property_name = lookup_matrix.property_name
    matrix_state.property_axis_name = nothing

    push!(query_state.stack, matrix_state)

    return nothing
end

function fill_lookup_matrix_values(
    query_state::QueryState,
    matrix_state::MatrixState,
    if_not::Maybe{IfNot},
    lookup_matrix::LookupMatrix,
    if_missing::Maybe{IfMissing},
    columns_axis_name::AbstractString,
    lookup_value::StorageScalar;
    is_column::Bool,
)::Nothing
    add_final_values!(matrix_state, if_not)

    @assert query_state.daf !== nothing
    default = default_value(if_missing)
    named_matrix_values = get_matrix(  # NOJET
        query_state.daf,
        matrix_state.property_axis_name,
        columns_axis_name,
        lookup_matrix.property_name;
        default,
    )
    push!(  # NOJET
        query_state.dependency_keys,
        Formats.matrix_cache_key(matrix_state.property_axis_name, columns_axis_name, lookup_matrix.property_name),
    )

    type = eltype(named_matrix_values)
    if type <: AbstractString
        type = AbstractString
    end
    n_rows, n_columns = size(matrix_state.matrix_values)
    lookup_matrix_values = Matrix{type}(undef, n_rows, n_columns)
    for column_index in 1:n_columns
        for row_index in 1:n_rows
            if matrix_state.pending_final_values === nothing ||
               matrix_state.pending_final_values[row_index, column_index] === nothing
                if is_column
                    lookup_matrix_values[row_index, column_index] = named_matrix_values[
                        string(matrix_state.matrix_values[row_index, column_index]),
                        string(lookup_value),
                    ]
                else
                    lookup_matrix_values[row_index, column_index] = named_matrix_values[
                        string(lookup_value),
                        string(matrix_state.matrix_values[row_index, column_index]),
                    ]
                end
            elseif type == AbstractString  # UNTESTED
                lookup_matrix_values[row_index, column_index] = ""  # UNTESTED
            else
                lookup_matrix_values[row_index, column_index] = zero(type)  # UNTESTED
            end
        end
    end
    matrix_state.matrix_values = lookup_matrix_values
    return nothing
end

function add_final_values!(matrix_state::MatrixState, if_not::Maybe{IfNot})::Nothing
    if if_not !== nothing
        @assert if_not.final_value !== nothing
        mask = as_booleans(matrix_state.matrix_values)  # NOJET
        if matrix_state.pending_final_values === nothing
            matrix_state.pending_final_values = Matrix{Any}(undef, size(matrix_state.matrix_values)...)
            matrix_state.pending_final_values .= nothing
        end
        matrix_state.pending_final_values[.!mask] .= if_not.final_value
    end
    return nothing
end

function finalize_matrix_values!(query_state::QueryState, matrix_state::MatrixState)::Nothing
    if query_state.what_for === :compute && matrix_state.pending_final_values !== nothing
        matrix_values = densify(matrix_state.matrix_values; copy = is_read_only_array(matrix_state.matrix_values))  # NOLINT
        @assert !is_read_only_array(matrix_values)  # NOLINT
        n_rows, n_columns = size(matrix_values)
        for column_index in 1:n_columns
            for row_index in 1:n_rows
                final_value = matrix_state.pending_final_values[row_index, column_index]
                if final_value !== nothing
                    matrix_values[row_index, column_index] =
                        cast_value(query_state, "final", final_value, eltype(matrix_values))
                end
            end
        end
        matrix_state.matrix_values = matrix_values
        matrix_state.pending_final_values = nothing
    end
    return nothing
end

function matrix_property_is_axis(
    query_state::QueryState,
    matrix_state::MatrixState,
    as_axis::Maybe{AsAxis} = nothing,
)::Nothing
    @assert matrix_state.property_name !== nothing
    @assert matrix_state.property_axis_name === nothing
    if query_state.what_for == :compute
        @assert eltype(matrix_state.matrix_values) <: AbstractString
    end

    finalize_matrix_values!(query_state, matrix_state)

    if as_axis !== nothing && as_axis.axis_name !== nothing
        axis_name = as_axis.axis_name

    else
        if query_state.what_for != :compute
            axis_name = matrix_state.property_name
        else
            @assert query_state.daf !== nothing
            axis_name = axis_of_property(query_state.daf, matrix_state.property_name)  # NOJET
        end
    end

    matrix_state.property_axis_name = axis_name
    push!(query_state.stack, matrix_state)

    return nothing
end

function axis_of_property(daf::DafReader, property_name::AbstractString)::AbstractString
    if has_axis(daf, property_name)
        return property_name
    end

    return split(property_name, "."; limit = 2)[1]
end

function as_booleans(vector::Union{AbstractVector{Bool}, BitVector})::Union{AbstractVector{Bool}, BitVector}
    return vector
end

function as_booleans(vector::AbstractVector{<:AbstractString})::Union{AbstractVector{Bool}, BitVector}
    return vector .!= ""
end

function as_booleans(vector::AbstractVector{<:Real})::Union{AbstractVector{Bool}, BitVector}
    return vector .!= 0
end

function as_booleans(matrix::Union{AbstractMatrix{Bool}, BitMatrix})::Union{AbstractMatrix{Bool}, BitMatrix}  # UNTESTED
    return matrix
end

function as_booleans(matrix::AbstractMatrix{<:AbstractString})::Union{AbstractMatrix{Bool}, BitMatrix}
    return matrix .!= ""
end

function as_booleans(matrix::AbstractMatrix{<:Real})::Union{AbstractMatrix{Bool}, BitMatrix}  # UNTESTED
    return matrix .!= 0
end

function cast_value(::QueryState, ::AbstractString, value::AbstractString, ::Type{T})::T where {T <: AbstractString}
    return value
end

function cast_value(query_state::QueryState, what::AbstractString, value::AbstractString, ::Type{T})::T where {T}
    try
        return parse(T, value)
    catch exception
        error_at_state(
            query_state,
            """
            error parsing $(what) value: $(value)
            as type: $(T)
            $(exception)
            """,
        )
    end
end

function cast_value(query_state::QueryState, what::AbstractString, value::Real, ::Type{T})::T where {T <: Real}
    try
        return T(value)
    catch exception
        error_at_state(
            query_state,
            """
            error converting: $(typeof(value))
            $(what) value: $(value)
            to type: $(T)
            $(exception)
            """,
        )
    end
end

struct Phrase
    input::Any  # Maybe{NTuple{N, Union{Type, Function}}}
    operations::Any  # Maybe{NTuple{M, Union{Optional, Type, Function}}}
    implementation::Any  # Function
    output::Any  # NTuple{O, Union{Type, Function}}
end

PHRASES = [  # Order matters - first one wins, longer matches should win.
    # Names

    Phrase(nothing, (axis_with_name, axis_with_name, Names), names_of_matrices, (NamesState,)),
    Phrase(nothing, (axis_with_name, Names), names_of_vectors, (NamesState,)),
    Phrase(nothing, (axis_without_name, Names), names_of_axes, (NamesState,)),
    Phrase(nothing, (Names,), names_of_scalars, (NamesState,)),

    # Matrix

    Phrase(
        (vector_axis, vector_maybe_axis),
        (Optional(AsAxis), LookupMatrix, Optional(IfMissing)),
        matrix_lookup,
        (MatrixState,),
    ),
    Phrase(
        (VectorState,),
        (CountBy, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_matrix_column_count,
        (VectorState, CountBy, VectorState),
    ),
    Phrase(
        (VectorState,),
        (CountBy, Optional(IfMissing), SquareColumnIs),
        lookup_square_matrix_column_count,
        (VectorState, CountBy, VectorState),
    ),
    Phrase(
        (VectorState,),
        (CountBy, Optional(IfMissing), SquareRowIs),
        lookup_square_matrix_row_count,
        (VectorState, CountBy, VectorState),
    ),
    Phrase(
        (VectorState,),
        (Optional(AsAxis), CountBy, Optional(IfMissing)),
        lookup_vector_count,
        (VectorState, CountBy, VectorState),
    ),
    Phrase(
        (matrix_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupMatrix, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_matrix_column_by_matrix,
        (MatrixState,),
    ),
    Phrase(
        (matrix_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupMatrix, Optional(IfMissing), SquareColumnIs),
        lookup_square_matrix_column_by_matrix,
        (MatrixState,),
    ),
    Phrase(
        (matrix_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupMatrix, Optional(IfMissing), SquareRowIs),
        lookup_square_matrix_row_by_matrix,
        (MatrixState,),
    ),
    Phrase(
        (matrix_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupVector, Optional(IfMissing)),
        lookup_vector_by_matrix,
        (MatrixState,),
    ),
    Phrase(
        (MatrixState,),
        (GroupAnyBy, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_matrix_group_by_matrix_column,
        (MatrixState, GroupAnyBy, VectorState),
    ),
    Phrase(
        (MatrixState,),
        (GroupAnyBy, Optional(IfMissing), SquareColumnIs),
        lookup_matrix_group_by_square_matrix_column,
        (MatrixState, GroupAnyBy, VectorState),
    ),
    Phrase(
        (MatrixState,),
        (GroupAnyBy, Optional(IfMissing), SquareRowIs),
        lookup_matrix_group_by_square_matrix_row,
        (MatrixState, GroupAnyBy, VectorState),
    ),
    Phrase(
        (MatrixState,),
        (GroupAnyBy, Optional(IfMissing)),
        lookup_matrix_group_by_vector,
        (MatrixState, GroupAnyBy, VectorState),
    ),
    Phrase(
        (MatrixState, GroupColumnsBy, VectorState),
        (ReduceToColumn, Optional(IfMissing)),
        compute_grouped_matrix,
        (MatrixState,),
    ),
    Phrase(
        (MatrixState, GroupRowsBy, VectorState),
        (ReduceToRow, Optional(IfMissing)),
        compute_grouped_matrix,
        (MatrixState,),
    ),
    Phrase((MatrixState,), (EltwiseOperation,), eltwise_matrix, (MatrixState,)),

    # Vector

    Phrase(nothing, (axis_with_name,), axis_lookup, (vector_axis,)),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), BeginAnyMask, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_matrix_column_mask,
        (vector_axis, BeginAnyMask, VectorState),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), BeginAnyMask, Optional(IfMissing), SquareColumnIs),
        lookup_square_matrix_column_mask,
        (vector_axis, BeginAnyMask, VectorState),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), BeginAnyMask, Optional(IfMissing), SquareRowIs),
        lookup_square_matrix_row_mask,
        (vector_axis, BeginAnyMask, VectorState),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), BeginAnyMask, Optional(IfMissing)),
        lookup_vector_mask,
        (vector_axis, BeginAnyMask, VectorState),
    ),
    Phrase((vector_axis, BeginAnyMask, VectorState), (EndMask,), apply_mask, (vector_axis,)),
    Phrase(
        (vector_axis, BeginAnyMask, VectorState),
        (MaskOperation, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_matrix_column_other_mask,
        (vector_axis, BeginAnyMask, VectorState, MaskOperation, VectorState),
    ),
    Phrase(
        (vector_axis, BeginAnyMask, VectorState),
        (MaskOperation, Optional(IfMissing), SquareColumnIs),
        lookup_square_matrix_column_other_mask,
        (vector_axis, BeginAnyMask, VectorState, MaskOperation, VectorState),
    ),
    Phrase(
        (vector_axis, BeginAnyMask, VectorState),
        (MaskOperation, Optional(IfMissing), SquareRowIs),
        lookup_square_matrix_row_other_mask,
        (vector_axis, BeginAnyMask, VectorState, MaskOperation, VectorState),
    ),
    Phrase(
        (vector_axis, BeginAnyMask, VectorState),
        (MaskOperation, Optional(IfMissing)),
        lookup_vector_other_mask,
        (vector_axis, BeginAnyMask, VectorState, MaskOperation, VectorState),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupMatrix, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_matrix_column_by_vector,
        (VectorState,),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupMatrix, Optional(IfMissing), SquareColumnIs),
        lookup_square_matrix_column_by_vector,
        (VectorState,),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupMatrix, Optional(IfMissing), SquareRowIs),
        lookup_square_matrix_row_by_vector,
        (VectorState,),
    ),
    Phrase(
        (vector_maybe_axis,),
        (Optional(AsAxis), Optional(IfNot), LookupVector, Optional(IfMissing)),
        lookup_vector_by_vector,
        (VectorState,),
    ),
    Phrase(
        (VectorState,),
        (GroupBy, Optional(IfMissing), axis_with_name, IsEqual),
        lookup_vector_group_by_matrix_column,
        (VectorState, GroupBy, VectorState),
    ),
    Phrase(
        (VectorState,),
        (GroupBy, Optional(IfMissing), SquareColumnIs),
        lookup_vector_group_by_square_matrix_column,
        (VectorState, GroupBy, VectorState),
    ),
    Phrase(
        (VectorState,),
        (GroupBy, Optional(IfMissing), SquareRowIs),
        lookup_vector_group_by_square_matrix_row,
        (VectorState, GroupBy, VectorState),
    ),
    Phrase(
        (VectorState,),
        (GroupBy, Optional(IfMissing)),
        lookup_vector_group_by_vector,
        (VectorState, GroupBy, VectorState),
    ),
    Phrase(
        (VectorState, GroupBy, VectorState),
        (ReductionOperation, Optional(IfMissing)),
        reduce_grouped_vector,
        (VectorState,),
    ),
    Phrase((MatrixState,), (ReduceToColumn, Optional(IfMissing)), reduce_matrix_to_column, (VectorState,)),
    Phrase((MatrixState,), (ReduceToRow, Optional(IfMissing)), reduce_matrix_to_row, (VectorState,)),
    Phrase((VectorState,), (EltwiseOperation,), eltwise_vector, (VectorState,)),
    Phrase((VectorState,), (VectorComparisonOperation,), compare_vector, (VectorState,)),

    # Scalar

    Phrase(nothing, (LookupScalar, Optional(IfMissing)), scalar_lookup, (ScalarState,)),
    Phrase(nothing, (LookupVector, Optional(IfMissing), axis_with_name, IsEqual), lookup_vector_entry, (ScalarState,)),
    Phrase(
        nothing,
        (LookupMatrix, Optional(IfMissing), axis_with_name, IsEqual, axis_with_name, IsEqual),
        lookup_matrix_entry,
        (ScalarState,),
    ),
    Phrase((MatrixState,), (ReductionOperation, Optional(IfMissing)), reduce_matrix_to_scalar, (ScalarState,)),
    Phrase((VectorState,), (ReductionOperation, Optional(IfMissing)), reduce_vector_to_scalar, (ScalarState,)),
    Phrase((ScalarState,), (EltwiseOperation,), eltwise_scalar, (ScalarState,)),

    # Implicit

    Phrase((vector_maybe_axis,), (AsAxis,), vector_property_is_axis, (vector_axis,)),
    Phrase((VectorState, CountBy, VectorState), nothing, compute_count_matrix, (MatrixState,)),
    Phrase(
        (vector_axis, BeginAnyMask, VectorState, MaskOperation, VectorState),
        nothing,
        compute_mask_operation,
        (vector_axis, BeginAnyMask, VectorState),
    ),
]

function do_query_phrase(query_state::QueryState)::Bool
    original_first_operation_index = query_state.first_operation_index
    original_next_operation_index = query_state.next_operation_index
    original_stack = query_state.stack

    for phrase in PHRASES
        query_state.stack = original_stack

        if phrase.input === nothing
            match_stack = QueryStackElement[]

        else
            if !stack_has_top(query_state, phrase.input)
                continue
            end
            match_stack = query_state.stack[(end - length(phrase.input) + 1):end]
        end

        query_state.first_operation_index = original_first_operation_index
        query_state.next_operation_index = original_next_operation_index

        query_state.first_operation_index = query_state.next_operation_index
        if phrase.operations === nothing
            next_operations = Maybe{QueryOperation}[]
        else
            next_operations = next_matching_operations(query_state, phrase.operations)
            if next_operations === nothing
                continue
            end
        end

        real_operations = QueryOperation[operation for operation in next_operations if operation !== nothing]

        if query_state.what_for == :compute
            @debug "- $(phrase.implementation): $(QuerySequence(real_operations))"
        end

        @views query_state.stack = original_stack[1:(end - length(match_stack))]
        phrase.implementation(query_state, match_stack..., next_operations...)
        @assert stack_has_top(query_state, phrase.output)

        return true
    end

    query_state.stack = original_stack
    query_state.first_operation_index = original_first_operation_index
    query_state.next_operation_index = original_next_operation_index

    return false
end

"""
Specify a column for [`get_frame`](@ref) for some axis. The most generic form is a pair `"column_name" => query`. Two
shorthands apply: the pair `"column_name" => "="` is a shorthand for the pair `"column_name" => ": column_name"`, and
so is the shorthand `"column_name"` (simple string).

We also allow specifying tuples instead of pairs to make it easy to invoke the API from other languages such as Python
which do not have the concept of a `Pair`.

The query is combined with the axis query as follows (using [`full_vector_query`](@ref)). The (full) query result should
be a vector with one value for each entry of the axis query result.
"""
FrameColumn = Union{
    AbstractString,
    Tuple{AbstractString, Union{QueryString, QueryOperation}},
    Pair{<:AbstractString, <:Union{QueryString, QueryOperation}},
}

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

The `axis` can be either just the name of an axis (e.g., `"cell"`), or a query for the axis (e.g., `q"@ cell"`),
possibly using a mask (e.g., `q"@ cell [ age > 1 ]"`). The result of the query must be a vector of unique axis entry
names.

If `columns` is not specified, the data frame will contain all the vector properties of the axis, in alphabetical order
(since `DataFrame` has no concept of named rows, the 1st column will contain the name of the axis entry).

By default, this will not `cache` the results of the queries.
"""
function get_frame(
    daf::DafReader,
    axis::QueryString,
    columns::Maybe{FrameColumns} = nothing;
    cache::Bool = false,
)::DataFrame
    if columns !== nothing
        for column in columns
            @assert column isa FrameColumn "invalid FrameColumn: $(column)"
        end
    end

    if axis isa Query
        axis_query = axis
    else
        axis_query = parse_query(axis, Axis)
    end

    @assert is_axis_query(axis_query) "invalid axis query: $(axis_query)"
    axis_name = query_axis_name(axis_query)

    names_of_rows = get_query(daf, axis_query; cache)
    @assert names_of_rows isa AbstractVector{<:AbstractString}

    if columns === nothing
        columns = sort!(collect(vectors_set(daf, axis_name)))
        insert!(columns, 1, "name")
    end

    if eltype(columns) <: AbstractString
        columns = [column => LookupVector(column) for column in columns]
    end

    data = Vector{Pair{AbstractString, StorageVector}}()
    for frame_column in columns
        @assert frame_column isa FrameColumn "invalid FrameColumn: $(frame_column) :: $(typeof(frame_column))"
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
        vector = get_query(daf, column_query; cache)
        if !(vector isa StorageVector) || !(vector isa NamedArray) || names(vector, 1) != names_of_rows
            error(chomp("""
                  invalid column query: $(column_query)
                  for the axis query: $(axis_query)
                  of the daf data: $(daf.name)
                  """))
        end
        push!(data, column_name => vector.array)
    end

    result = DataFrame(data)  # NOJET
    @debug "get_frame daf: $(brief(daf)) axis: $(axis) columns: $(columns) result: $(brief(result))"
    return result
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

Normally we just concatenate the axis query and the vector query.

  - If the vector query contains [`GroupBy`](@ref), then the query must repeat any mask specified for the axis query.
    That is, if the axis query is `metacell [ type = B ]` (the frame has a row for each metacells of B cells), and we
    want the mean age of the cells (`@ cell : age`) in each such metacell (`/ metacell >> Mean`), then the vector query
    must be the full `@ cell [ metacell : type = B ] : age / metacell %> Mean`. TODO: Find a way to inject the mask in
    the right place in such a query (that is, allow saying just `@ cell : age / metacell >> Mean`) - this is difficult
    in the general case.

  - Otherwise (the common case) we simply concatenate the axis query and the vector query. That is, of the axis query is
    `@ cell [ batch = B1 ]` and the vector query is `: age`, then the full query will be `@ cell [ batch = B1 ] : age`.
    Or, if the axis query is `@ gene [ is_marker ]` and the vector query is `@ metacell :: fraction >| Mean`, then the
    full query would be `@ cell [ batch = B1 ] @ metacell :: fraction >| Mean`.
"""
function full_vector_query(
    axis_query::Query,
    vector_query::Union{QueryString, QueryOperation},
    vector_name::Maybe{AbstractString} = nothing,
)::Query
    if vector_name !== nothing && vector_query == "="
        vector_query = LookupVector(vector_name)
    elseif vector_query isa AbstractString
        vector_query = parse_query(vector_query, LookupVector)
    else
        vector_query = as_query_sequence(vector_query)
    end

    if vector_query isa QuerySequence &&
       any([query_operation isa GroupBy for query_operation in vector_query.query_operations])
        return vector_query
    else
        return axis_query |> vector_query
    end
end

end  # module
