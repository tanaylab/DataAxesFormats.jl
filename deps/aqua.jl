push!(LOAD_PATH, ".")

using Aqua
using Daf
Aqua.test_ambiguities([Daf])
Aqua.test_all(Daf; ambiguities = false, unbound_args = false, deps_compat = false)
