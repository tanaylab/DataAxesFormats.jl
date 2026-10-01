#!/bin/bash
set -e -o pipefail
julia --project=deps/aqua_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
# Right after the package changes, the persistent tasks test races two compilations of the package and may fail.
# A second run finds the compiled package and does not race.
julia --project=deps/aqua_env deps/aqua.jl \
|| (echo "Retrying Aqua once" && julia --project=deps/aqua_env deps/aqua.jl)
