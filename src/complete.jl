"""
The common idiom in `Daf` is to have multiple repositories in a chain; actually, they typically form a tree where
different leaf repositories are based on common ancestor repositories. For example, a base cells repository can be used
by multiple alternative metacells repositories.

Tracking this tree manually is possible, by using naming conventions (which is always a good idea). However, this gets
tedious. The code here automates this by using an additional convention - each repository contains a scalar property
called `base_daf_repository` which identifies its parent repository (if any). This path is relative to the directory
containing the child repository. See [`open_daf`](@ref) for details.

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
using ..Readers
using ..Writers

using TanayLabUtilities

"""
    complete_daf(leaf::AbstractString, mode::AbstractString = "r"; name::Maybe{AbstractString} = nothing)::Union{DafReader, DafWriter}

Open a complete chain of `Daf` repositories by tracing back through the `base_daf_repository`. Valid modes are only "r"
and "r+"; if the latter, only the first (leaf) repository is opened in write mode.
"""
function complete_daf(
    leaf::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{DafReader, DafWriter}
    @assert mode in ("r", "r+")
    is_writer = mode == "r+"
    base_daf_repository = leaf
    dafs = DafReader[]
    @info "Open complete $(name === nothing ? leaf : name):"
    while true
        @info "- Open $(base_daf_repository)"
        daf = open_daf(base_daf_repository, mode)
        push!(dafs, daf)
        mode = "r"

        base_directory = dirname(base_daf_repository)
        base_daf_repository = get_scalar(daf, "base_daf_repository"; default = nothing)
        if base_daf_repository !== nothing
            base_daf_repository = joinpath(base_directory, base_daf_repository)
            continue
        end

        reverse!(dafs)
        if is_writer
            return chain_writer(dafs; name)
        else
            return chain_reader(dafs; name)
        end
    end
end

"""
    open_daf(
        path::AbstractString,
        mode::AbstractString = "r";
        name::Maybe{AbstractString} = nothing
    )::Union{DafReader, DafWriter}

Open either a [`FilesDaf`](@ref) or an [`H5df`](@ref). If the `path` ends with `.h5df` or contains `.h5dfs#` (followed
by a group path), then it opens an [`H5dfFormat`](@ref) file (or a group in one). Otherwise, it opens a
[`FilesFormat`](@ref) `Daf`.
"""
function open_daf(
    path::AbstractString,
    mode::AbstractString = "r";
    name::Maybe{AbstractString} = nothing,
)::Union{DafReader, DafWriter}
    if endswith(path, ".h5df") || occursin(".h5dfs#", path)
        return H5df(path, mode; name)
    else
        return FilesDaf(path, mode; name)
    end
end

end
