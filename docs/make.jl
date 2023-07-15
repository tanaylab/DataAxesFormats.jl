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

push!(LOAD_PATH, "../src/")

using Daf

makedocs(;
    sitename = "Daf.jl",
    modules = [Daf],
    authors = "Oren Ben-Kiki",
    clean = true,
    format = Documenter.HTML(; prettyurls = get(ENV, "CI", nothing) == "true"),
    pages = [
        "index.md",
        "storage.md",
        "query.md",
        "registry.md",
        "operations.md",
        "data_types.md",
        "matrix_layouts.md",
        "as_dense.md",
        "oprec.md",
        "messages.md",
        "example_data.md",
        "todo.md",
    ],
)

if seen_problems
    exit(1)
end
