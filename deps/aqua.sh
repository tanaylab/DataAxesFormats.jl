#!/bin/bash
set -e -o pipefail
julia --project=deps/aqua_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
julia --project=deps/aqua_env deps/aqua.jl
