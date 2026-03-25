using Pkg
using TOML

project = TOML.parse(read("Project.toml", String))

for pkg in keys(project["deps"])
    println("Adding $(pkg):")
    Pkg.add(pkg)
end

for env in ("aqua_env", "coverage_env", "document_env", "format_env", "jet_env", "lint_env")
    env_path = joinpath("deps", env)
    println("Instantiating $(env):")
    Pkg.activate(env_path)
    Pkg.resolve();
    Pkg.instantiate()
end
