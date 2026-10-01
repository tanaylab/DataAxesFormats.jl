push!(LOAD_PATH, ".")

using Aqua
using Test
using TOML

PACKAGE_NAME = Symbol(TOML.parsefile("Project.toml")["name"])
@eval using $(PACKAGE_NAME)
PACKAGE = getfield(Main, PACKAGE_NAME)

Aqua.test_ambiguities([PACKAGE])
Aqua.test_all(PACKAGE; ambiguities = false, unbound_args = false, deps_compat = false, persistent_tasks = false)

# Aqua's own default of 10 seconds is not the time the package spends starting tasks; it is the time the whole
# precompilation subprocess takes to exit after loading, which on a loaded machine writing its cache over the
# network is regularly longer than that. A larger budget tests the same thing without failing at random.
@testset "Persistent tasks" begin
    Aqua.test_persistent_tasks(PACKAGE; tmax = 60)
end
