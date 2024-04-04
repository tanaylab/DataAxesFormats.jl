# Build the documentations locally into `docs` so they will appear in the github pages. This way, in github we have the
# head version documentation, while in the standard Julia packages documentation we have the documentation of the last
# published version.

using Documenter
using Logging
using LoggingExtras

seen_problems = false

detect_problems = EarlyFilteredLogger(global_logger()) do log_args
    if log_args.level >= Logging.Warn
        global seen_problems
        seen_problems = true
    end
    return true
end

global_logger(detect_problems)

push!(LOAD_PATH, ".")

using Daf
using Pkg

PROJECT_TOML = Pkg.TOML.parsefile(joinpath(@__DIR__, "..", "Project.toml"))
VERSION = PROJECT_TOML["version"]
NAME = PROJECT_TOML["name"]
AUTHORS = PROJECT_TOML["authors"]
REPO = "https://github.com/tanaylab/$(NAME).jl"

makedocs(;
    authors = join(" ", AUTHORS),
    build = "../docs/v$(VERSION)",
    remotes = nothing,
    source = "../src",
    clean = true,
    doctest = true,
    modules = [Daf],
    highlightsig = true,
    sitename = "$(NAME).jl v$(VERSION)",
    draft = false,
    linkcheck = true,
    format = Documenter.HTML(;
        repolink = "$(REPO)/blob/main{path}?plain=1#L{line}",
        prettyurls = false,
        size_threshold_warn = 200 * 2^10,
    ),
    pages = [
        "index.md",
        "readers.md",
        "writers.md",
        "read_only.md",
        "queries.md",
        "tokens.md",
        "views.md",
        "chains.md",
        "computations.md",
        "copies.md",
        "concat.md",
        "adapters.md",
        "contracts.md",
        "formats.md",
        "memory_format.md",
        "h5df_format.md",
        "files_format.md",
        "anndata_format.md",
        "reconstruction.md",
        "registry.md",
        "operations.md",
        "storage_types.md",
        "matrix_layouts.md",
        "groups.md",
        "messages.md",
        "generic_logging.md",
        "generic_types.md",
        "generic_functions.md",
        "example_data.md",
    ],
)

if seen_problems
    exit(1)
end
