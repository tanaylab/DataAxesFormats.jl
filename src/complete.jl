"""
The common idiom in `Daf` is to have multiple repositories in a chain; actually, they typically form a tree where
different leaf repositories are based on common ancestor repositories. For example, a base cells repository can be used
by multiple alternative metacells repositories.

Tracking this tree manually is possible, by using naming conventions (which is always a good idea). However, this gets
tedious. The code here automates this by using an additional convention - each repository contains a scalar property
called `base_daf_repository` which identifies the repositories it is immediately based on (if any). Each path is
relative to the directory containing the child repository. See [`open_daf`](@ref) for details.

The property holds only the immediate bases; what each of them in turn is based on is recorded in it, so the shape of
the whole is found by following the records. It is one of:

  - A path, for the common case of resting on the whole of a single repository.
  - A JSON object `{"path": ..., "axes": ..., "data": ...}`, where the optional `axes` and `data` are the parameters of
    a `DafView` to apply, so that the child rests on a subset of the base data, and/or on renamed base data.
  - A JSON array of either of the above, for a repository resting on several. Later bases override earlier ones, as in
    any chain.

Since the same base repository can be reached through more than one of these, it is opened once (using the
`GlobalWeakCache`) and appears once in the chain, at its earliest position - a base must not override what rests on it.

Since the same base repository can be used by multiple other repositories, we use the `GlobalWeakCache` to avoid
needlessly re-opening the same repository more than once.
"""
module CompleteDaf

export complete_daf
export open_daf

using ..Chains
using ..FilesFormat
using ..Formats
using ..H5dfFormat
using ..HttpFormat
using ..Readers
using ..Views
using ..Writers
using ..ZarrFormat
using ..ZipFormat

using TanayLabUtilities

import ..Chains.RecordedBase
import ..Chains.recorded_bases

"""
    complete_daf(
        leaf::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing,
        packed::Bool = false]
    )::Union{DafReader, DafWriter}

Open a complete chain of `Daf` repositories by tracing back through the `base_daf_repository` of each. Valid modes are
only "r" and "r+"; if the latter, only the `leaf` repository is opened in write mode.

If `packed` is `true`, the leaf repository (the only one opened in write mode under "r+") gets `packed = true` as its
per-daf default. Base repositories are always opened in "r" mode and the `packed` value is irrelevant for them.

A convenient way to create persistent complete chains is using [`complete_chain!`](@ref).

TODO: Properly indent the log messages of the created leaf repositories. Generic mechanism for indenting all hierarchical log messages?
"""
function complete_daf(
    leaf::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
    packed::Bool = false,
)::Union{DafReader, DafWriter}
    return flame_timed("complete_daf") do
        if name === nothing
            name = leaf
        end
        @assert mode in ("r", "r+")
        @debug "Open complete $(name):" _group = :daf_repose
        dafs = flame_timed("complete_daf.collect_dafs") do
            return collect_dafs(; name, path = leaf, mode, is_packed = packed, indent = "", index = 0)
        end
        return flame_timed("complete_daf.chain") do
            if mode == "r+"
                return chain_writer(dafs; name = name * ".complete")
            else
                return chain_reader(dafs; name = name * ".complete")
            end
        end
    end
end

# The repositories of the complete chain of a repository, in chain order - a repository comes after everything it is
# based on. A repository names only its own immediate bases, so the shape of the whole is found by following them, and
# a repository reached through more than one of them is opened once, by the `GlobalWeakCache`, and appears once, at
# its earliest position.
function collect_dafs(;
    name::AbstractString,
    path::AbstractString,
    mode::AbstractString,
    is_packed::Bool,
    indent::AbstractString,
    index::Integer,
)::Vector{DafReader}
    @debug "$(indent)- Open $(path) $(mode)" _group = :daf_repose
    daf = open_daf(path, mode; packed = is_packed)  # NOJET

    specification = get_scalar(daf, "base_daf_repository"; default = nothing)
    if specification === nothing
        return DafReader[daf]
    end

    dafs = DafReader[]
    for base in recorded_bases(specification)
        append!(dafs, collect_base(; name, base, base_directory = dirname(path), indent, index))
    end
    push!(dafs, daf)
    return dafs
end

function collect_base(;
    name::AbstractString,
    base::RecordedBase,
    base_directory::AbstractString,
    indent::AbstractString,
    index::Integer,
)::Vector{DafReader}
    base_dafs = collect_dafs(;
        name,
        path = joinpath(base_directory, base.path),
        mode = "r",
        is_packed = false,
        indent = indent * "  ",
        index = index + 1,
    )

    if base.axes === nothing && base.data === nothing
        return base_dafs
    end

    # A view is of the base's own complete chain, so what it exposes is decided before anything is chained on top of it.
    @debug "$(indent)  View" _group = :daf_repose
    chain = chain_reader(base_dafs; name = "$(name).chain_$(index)")
    view = viewer(chain; name = "$(name).view_$(index)", axes = base.axes, data = base.data)  # NOJET
    push!(view.path, complete_path(base_dafs[end]))  # NOJET
    return DafReader[view]
end

"""
    open_daf(
        path::AbstractString,
        mode::AbstractString = "r";
        [name::Maybe{AbstractString} = nothing,
        packed::Bool = false]
    )::Union{DafReader, DafWriter}

Open a `Daf` data set, dispatching to the appropriate backend based on `path`:

  - If `path` ends with `.daf.zarr`, ends with `.daf.zarr.zip`, or contains `.dafs.zarr.zip#` (followed by a sub-daf
    group path), open a [`ZarrDaf`](@ref).
  - Otherwise, if `path` ends with `.daf.zip` or contains `.dafs.zip#` (followed by a sub-daf group path), open a
    [`ZipDaf`](@ref).
  - Otherwise, if `path` starts with `http://` or `https://`, open an [`HttpDaf`](@ref). Only `mode = "r"` is supported
    for the HTTP backend; any other mode raises an error.
  - Otherwise, if `path` ends with `.h5df` or contains `.h5dfs#` (followed by a group path), open an [`H5df`](@ref)
    file (or a group in one).
  - Otherwise, open a [`FilesDaf`](@ref).

The `packed` kwarg is forwarded to the chosen backend; see each backend's constructor for what it controls.
"""
function open_daf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
    packed::Bool = false,
)::Union{DafReader, DafWriter}
    if endswith(path, ".daf.zarr") || endswith(path, ".daf.zarr.zip") || occursin(".dafs.zarr.zip#", path)
        return ZarrDaf(path, mode; name, packed)
    elseif endswith(path, ".daf.zip") || occursin(".dafs.zip#", path)
        return ZipDaf(path, mode; name, packed)
    elseif startswith(path, "http://") || startswith(path, "https://")
        if mode != "r"
            error("can't open an http(s)://... HttpDaf in mode: $(mode); the HTTP backend is read-only: $(path)")
        end
        return HttpDaf(path; name, packed)
    elseif endswith(path, ".h5df") || occursin(".h5dfs#", path)
        return H5df(path, mode; name, packed)
    else
        return FilesDaf(path, mode; name, packed)
    end
end

end
