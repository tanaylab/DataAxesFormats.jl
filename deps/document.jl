# Build the documentation of the package. Run as a script, this builds it locally into `docs/v<version>` so it will
# appear in the github pages. This way, in github we have the head version documentation, while in the standard Julia
# packages documentation we have the documentation of the last published version, built by `docs/make.jl`.
#
# The package-specific settings are in `docs/metadata.toml`:
#
#   - `pages` - the documentation pages, in navigation order. This must list every `src/*.md` file.
#   - `interlinks` - optional, a table of package names to `objects.inv` URLs for `DocumenterInterLinks`.
#   - `linkcheck_ignore` - optional, the URLs the link check should skip (e.g. sites which refuse automated requests).
#
# Every `src/assets/*.css` file is added to the HTML pages.

using Documenter
using Logging
using LoggingExtras
using TOML

PACKAGE_ROOT = abspath(joinpath(@__DIR__, ".."))
push!(LOAD_PATH, PACKAGE_ROOT)

PROJECT_TOML = TOML.parsefile(joinpath(PACKAGE_ROOT, "Project.toml"))
NAME = PROJECT_TOML["name"]
VERSION = PROJECT_TOML["version"]
AUTHORS = PROJECT_TOML["authors"]
REPO = "https://github.com/tanaylab/$(NAME).jl"

METADATA = TOML.parsefile(joinpath(PACKAGE_ROOT, "docs", "metadata.toml"))
PAGES = METADATA["pages"]
INTERLINKS = get(METADATA, "interlinks", Dict{String, Any}())
LINKCHECK_IGNORE = get(METADATA, "linkcheck_ignore", String[])

ASSETS_DIRECTORY = joinpath(PACKAGE_ROOT, "src", "assets")
ASSETS = String[]
if isdir(ASSETS_DIRECTORY)
    ASSETS = ["assets/$(file)" for file in readdir(ASSETS_DIRECTORY) if endswith(file, ".css")]
end

@eval using $(Symbol(NAME))
PACKAGE = getfield(Main, Symbol(NAME))

if !isempty(INTERLINKS)
    @eval using DocumenterInterLinks
end

# Fail if the pages do not match the `src/*.md` files, so the list can't drift away from them.
function verify_pages()::Nothing
    source_pages = sort([file for file in readdir(joinpath(PACKAGE_ROOT, "src")) if endswith(file, ".md")])
    missing_pages = setdiff(source_pages, PAGES)
    unknown_pages = setdiff(PAGES, source_pages)
    if !isempty(missing_pages) || !isempty(unknown_pages)
        error(
            "the pages in docs/metadata.toml do not match the src/*.md files\n" *
            "missing pages: $(join(missing_pages, ", "))\n" *
            "unknown pages: $(join(unknown_pages, ", "))",
        )
    end
    return nothing
end

"""
    make_documentation(; is_local::Bool)::Nothing

Build the documentation of the package. If `is_local`, build it into `docs/v<version>` for the github pages, linking
the source code into the github repository, and fail on any warning. Otherwise, build it into `docs/build`, as the
standard `docs/make.jl` idiom does.
"""
function make_documentation(; is_local::Bool)::Nothing
    verify_pages()

    if is_local
        seen_problems = false
        detect_problems = EarlyFilteredLogger(global_logger()) do log_args
            if log_args.level >= Logging.Warn
                seen_problems = true
            end
            return true
        end
        global_logger(detect_problems)
    else
        for file in readdir(joinpath(PACKAGE_ROOT, "docs"); join = true)
            if !(basename(file) in ("make.jl", "metadata.toml"))
                rm(file; force = true, recursive = true)
            end
        end
    end

    DocMeta.setdocmeta!(PACKAGE, :DocTestSetup, :(using $(Symbol(NAME))); recursive = true)

    if is_local
        build = joinpath(PACKAGE_ROOT, "docs", "v$(VERSION)")
        sitename = "$(NAME).jl v$(VERSION)"
        format = Documenter.HTML(;
            repolink = "$(REPO)/blob/main{path}?plain=1#L{line}",
            prettyurls = false,
            size_threshold_warn = 200 * 2^10,
            assets = ASSETS,
        )
    else
        build = joinpath(PACKAGE_ROOT, "docs", "build")
        sitename = "$(NAME).jl"
        format = Documenter.HTML(; prettyurls = false, size_threshold_warn = 200 * 2^10, assets = ASSETS)
    end

    plugins = isempty(INTERLINKS) ? Documenter.Plugin[] : [DocumenterInterLinks.InterLinks(INTERLINKS...)]
    # Locally the source is linked by `repolink`; otherwise Documenter finds the remote repository itself.
    remotes = is_local ? (; remotes = nothing) : (;)

    makedocs(;
        root = @__DIR__,
        authors = join(AUTHORS, " "),
        build,
        remotes...,
        source = joinpath(PACKAGE_ROOT, "src"),
        clean = true,
        doctest = true,
        modules = [PACKAGE],
        highlightsig = true,
        sitename,
        draft = false,
        linkcheck = true,
        linkcheck_ignore = LINKCHECK_IGNORE,
        # A slow response from a working site should not fail the build; Documenter's default is 10 seconds.
        linkcheck_timeout = 30,
        format,
        pages = PAGES,
        plugins,
    )

    for file in readdir(build; join = true)
        if endswith(file, ".cov")
            rm(file)
        end
    end

    if is_local && seen_problems
        exit(1)
    end
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    make_documentation(; is_local = true)
end
