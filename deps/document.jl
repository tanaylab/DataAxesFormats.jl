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

makedocs(;
    authors = "Oren Ben-Kiki",
    repo = "https://github.com/tanaylab/Daf.jl/blob/main{path}?plain=1#L{line}",
    build = "../docs",
    source = "../src",
    clean = true,
    doctest = true,
    modules = [Daf],
    highlightsig = true,
    sitename = "Daf.jl",
    draft = false,
    strict = true,
    linkcheck = true,
    format = Documenter.HTML(; prettyurls = false),
    pages = [
        "index.md",
        "data.md",
        "read_only.md",
        "formats.md",
        "memory_format.md",
        "queries.md",
        "registry.md",
        "operations.md",
        "storage_types.md",
        "matrix_layouts.md",
        "oprec.md",
        "messages.md",
        "example_data.md",
        "todo.md",
    ],
)

if seen_problems
    exit(1)
end
