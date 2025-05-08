# This file is copied into `docs` so that when the package is published its documentation will be built automatically,
# that is, pretend the package uses the usual "docs/make.jl" idiom. Normally this isn't used because we build the
# documentation locally and publish them in github pages. This way, in github we have the head version documentation,
# while in the standard Julia packages documentation we have the documentation of the last published version.

using Documenter

push!(LOAD_PATH, "..")

using NestedTests
using Pkg
using DataAxesFormats

for file in readdir("docs"; join = true)
    if !endswith(file, "make.jl")
        rm(file; force = true, recursive = true)
    end
end

PROJECT_TOML = Pkg.TOML.parsefile("Project.toml")
NAME = PROJECT_TOML["name"]
AUTHORS = PROJECT_TOML["authors"]

makedocs(;
    authors = join(" ", AUTHORS),
    source = "../src",
    clean = true,
    doctest = true,
    modules = [DataAxesFormats],
    highlightsig = true,
    sitename = "$(NAME).jl",
    draft = false,
    strict = true,
    linkcheck = true,
    format = Documenter.HTML(; prettyurls = false, size_threshold_warn = 200 * 2^10),
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
        "keys.md",
        "memory_format.md",
        "h5df_format.md",
        "files_format.md",
        "complete.md",
        "anndata_format.md",
        "reconstruction.md",
        "registry.md",
        "operations.md",
        "storage_types.md",
        "groups.md",
        "example_data.md",
    ],
)

for file in readdir("docs/build"; join = true)
    if endswith(file, ".cov")
        rm(file)
    end
end
