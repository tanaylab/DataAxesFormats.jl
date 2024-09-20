push!(LOAD_PATH, ".")

using Aqua
using DataAxesFormats
Aqua.test_ambiguities([DataAxesFormats])
Aqua.test_all(DataAxesFormats; ambiguities = false, unbound_args = false, deps_compat = false)
