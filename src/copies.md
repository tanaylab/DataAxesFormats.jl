# Copies

```@docs
DataAxesFormats.Copies
DataAxesFormats.Copies.copy_scalar!
DataAxesFormats.Copies.copy_axis!
DataAxesFormats.Copies.copy_vector!
DataAxesFormats.Copies.copy_matrix!
DataAxesFormats.Copies.copy_tensor!
DataAxesFormats.Copies.copy_all!
DataAxesFormats.Copies.EmptyData
DataAxesFormats.Copies.DataTypes
```

## Picking the right format for the workload

Packed and unpacked-on-local-SSD layouts optimise for different bottlenecks; which one wins for a given
workload depends on where the data actually lives and the storage tier's bandwidth relative to a single core's
decompression speed. **The single rule: pick the format that matches where the data actually lives.** Packed
for slow tiers, unpacked on fast local SSD if you can stage it. Neither side is the default.

### When packed wins for compute

Packed format isn't only for transport and archival — for compute against data living on a slow tier, packed
often beats the equivalent unpacked layout because the bandwidth saving outweighs the decompression CPU and
the chunk-cache lock overhead:

  - **Data on an NFS mount.** NFS mmap is page-by-page; random scattered scalar access can produce one
    round-trip per page, and aggregate bandwidth from a network file system is typically tens of MB/s. Packed
    format compresses the wire transfer 2–10× and serves chunks via `DiskArrays.cache`, so a per-block
    iteration touches only the chunks it needs. Net throughput is usually higher than reading the equivalent
    unpacked layout.
  - **Data accessed via HTTP** ([`HttpDaf`](@ref DataAxesFormats.HttpFormat.HttpDaf),
    [`ZarrDaf`](@ref DataAxesFormats.ZarrFormat.ZarrDaf) over HTTP). Same story more so: round-trip latency
    is the dominant cost; striped / chunked access amortises it across the data the user actually touches,
    and compression cuts wire bytes. Reading an unpacked HTTP-served property doesn't even avoid the
    chunk-cache machinery — the unpacked HTTP striped path also wraps through `DiskArrays.cache` — so the
    only thing un-packing the source buys you is wire bandwidth, which is precisely the thing you don't want
    to give up here.
  - **Data on a slow local disk** (mechanical HDD, slow USB storage, some cloud-attached block volumes).
    Disk bandwidth is on the same order as a single core's decompression speed (~150 MB/s at the low end),
    so the trade is roughly even on raw bytes — but packed reads fewer bytes, so the disk-bound side wins.

### When staging to local SSD wins

For data that's already on (or can fit on) a fast local SSD, an unpacked staged copy is the
throughput-optimal choice for compute-intensive work:

  - mmap-Strided fast path: zero per-access overhead, full `@turbo` / `LoopVectorization` / BLAS support.
  - No `DiskArrays.cache` `SpinLock` in the access path; on many-core boxes (32+ threads) the cache-line
    bouncing on the lock atomic is meaningful, and the unpacked path avoids it entirely.
  - NVMe SSD bandwidth (~3–7 GB/s for modern drives) exceeds zstd decompression speed (~500 MB/s/core), so
    even when bytes-on-disk are similar, the unpacked side reads them faster.

### Staging idiom

Open the source packed, copy once to a local unpacked daf via [`copy_all!`](@ref), then run compute against
the staged copy:

```julia
using DataAxesFormats

remote = open_daf("https://example.com/dataset")           # packed remote source
local_dir = tempname() * ".daf"
staged = open_daf(local_dir, "w+"; packed = false)
copy_all!(; destination = staged, source = remote)

# ... compute against `staged` here, with full mmap-Strided fast path,
# no SpinLock contention, full @turbo / BLAS support ...
```

The same pattern applies for compressed local archives you want to run heavy compute against: open packed,
`copy_all!` to a fresh unpacked daf on local SSD, compute against that. [`copy_all!`](@ref) resolves its
`packed` kwarg against the destination's per-daf default and routes every property through the same writer
the user would call directly — so the staged copy ends up byte-identical to one written from scratch.

## Index

```@index
Pages = ["copies.md"]
```
