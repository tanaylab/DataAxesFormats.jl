using Pkg

for pkg in ("Coverage", "Documenter", "JET", "JuliaFormatter", "Logging", "LoggingExtras", "SnoopCompile")
    println("Adding $(pkg):")
    Pkg.add(pkg)
end
