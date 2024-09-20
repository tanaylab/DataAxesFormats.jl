push!(LOAD_PATH, ".")

using Aqua
using DafJL
Aqua.test_ambiguities([DafJL])
Aqua.test_all(DafJL; ambiguities = false, unbound_args = false, deps_compat = false)
