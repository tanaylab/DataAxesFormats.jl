using Pkg
using TOML

project = TOML.parse(read("Project.toml", String))

for pkg in keys(project["deps"])
    println("Adding $(pkg):")
    Pkg.add(pkg)
end

for pkg in (
    "Aqua",
    "Coverage",
    "Documenter",
    "JET",
    "JuliaFormatter",
    "LanguageServer",
    "Logging",
    "LoggingExtras",
    "StaticLint",
    "SymbolServer",
)
    println("Adding $(pkg):")
    Pkg.add(pkg)
end
