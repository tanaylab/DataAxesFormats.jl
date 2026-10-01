using JET
using TOML

push!(LOAD_PATH, ".")

PACKAGE_NAME = TOML.parsefile("Project.toml")["name"]
@eval using $(Symbol(PACKAGE_NAME))

println(report_package(PACKAGE_NAME))
