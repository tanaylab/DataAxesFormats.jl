# The standard `docs/make.jl` idiom, which builds the documentation of the published version of the package.
include(joinpath(@__DIR__, "..", "deps", "document.jl"))
make_documentation(; is_local = false)
