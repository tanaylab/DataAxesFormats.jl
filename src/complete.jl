"""
The common idiom in `Daf` is to have multiple repositories in a chain; actually, they typically form a tree where
different leaf repositories are based on common ancestor repositories. For example, a base cells repository can be used
by multiple alternative metacells repositories.

Tracking this tree manually is possible, by using naming conventions (which is always a good idea). However, this gets
tedious. The code here automates this by using an additional convention - each repository contains a scalar property
called `base_daf_repository` which identifies its parent repository (if any). This path is relative to the directory
containing the child repository. See [`open_daf`](@ref) for details.

In addition, it is possible to have another scalar property, `base_daf_view`. If specified, this should contain JSON
serialization of the parameters of a `DafView` to apply to the base repository. This allows the child repository to be
based on a subset of the base data, and/or rename the base data.

Since the same base repository can be used by multiple other repositories, we use the `GlobalWeakCache` to avoid
needlessly re-opening the same repository more than once.
"""
module CompleteDaf

export complete_daf
export open_daf

using JSON

using ..Chains
using ..FilesFormat
using ..Formats
using ..H5dfFormat
using ..Readers
using ..Views
using ..Writers

using TanayLabUtilities

"""
    complete_daf(leaf::AbstractString, mode::AbstractString = "r"; name::Maybe{AbstractString} = nothing)::Union{DafReader, DafWriter}

Open a complete chain of `Daf` repositories by tracing back through the `base_daf_repository` and the optional
`base_daf_view`. Valid modes are only "r" and "r+"; if the latter, only the first (leaf) repository is opened in write
mode.
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
    real_paths = AbstractString[]
    leaf_name = name === nothing ? leaf : name
    @info "Open complete $(leaf_name):"
    while true
        @info "- Open $(base_daf_repository)"
        real_path = realpath(base_daf_repository)
        is_in = real_path in real_paths
        if is_in
            @error "Loop in base repositories: $(join(real_paths, " -> "))"
            @assert false
        end
        push!(real_paths, real_path)

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
    @assert mode in ("r", "r+")
    @info "Open complete $(leaf_name):"
    dafs = reverse!(collect_dafs(; name, base_daf_repository = leaf, mode, indent = "", index = 0))
    if mode == "r+"
        return chain_writer(dafs; name = leaf_name * ".complete")
    else
        return chain_reader(dafs; name = leaf_name * ".complete")
    end
end

function collect_dafs(;
    name::AbstractString,
    base_daf_repository::Union{AbstractString, DafReader},
    mode::AbstractString,
    indent::AbstractString,
    index::Integer,
)::AbstractVector{<:DafReader}
    dafs = DafReader[]
    while true
        @info "$(indent)- Open $(base_daf_repository) $(mode)"
        daf = open_daf(base_daf_repository, mode)  # NOJET
        base_directory = dirname(base_daf_repository)  # NOJET

        push!(dafs, daf)
        base_daf_repository = get_scalar(daf, "base_daf_repository"; default = nothing)
        if base_daf_repository === nothing
            return dafs
        end
        base_daf_repository = joinpath(base_directory, base_daf_repository)

        base_daf_view = parse_view_parameters(get_scalar(daf, "base_daf_view"; default = nothing))
        if base_daf_view !== nothing
            @debug "$(indent)  View"
            base_daf_view = parse_view_parameters(get_scalar(daf, "base_daf_view"; default = nothing))
            base_dafs = reverse!(
                collect_dafs(; name, base_daf_repository, mode = "r", indent = indent * "  ", index = index + 1),
            )
            chain = chain_reader(base_dafs; name = "$(name).chain_$(index)")
            daf = viewer(chain; name = "$(name).view_$(index)", base_daf_view...)  # NOJET
            return push!(dafs, daf)
        end

        mode = "r"
    end
    return dafs
end

function parse_view_parameters(::Nothing)::Nothing
    return nothing
end

function parse_view_parameters(json::AbstractString)::AbstractDict
    parameters = Dict{Symbol, Any}()
    json_parameters = JSON.parse(json)  # NOLINT
    @assert json_parameters isa AbstractDict
    for (key, value) in json_parameters
        pairs = Pair[]
        for pair in value
            for (pattern, value) in pair
                if contains(pattern, "(")
                    pattern = replace(pattern, "(" => "[", ")" => "]")
                    pattern = JSON.parse(pattern)  # NOLINT
                    pattern = Tuple(pattern)
                end
                push!(pairs, pattern => value)
            end
        end
        parameters[Symbol(key)] = pairs
    end
    return parameters
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
