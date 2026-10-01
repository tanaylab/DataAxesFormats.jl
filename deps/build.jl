using TOML

println("Building $(TOML.parsefile(joinpath(@__DIR__, "..", "Project.toml"))["name"])...")
