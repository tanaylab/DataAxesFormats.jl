#!/bin/bash
set -e -o pipefail
julia --project=deps/jet_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
JULIA_DEBUG="" julia --project=deps/jet_env --color=no deps/jet.jl 2>&1 | python3 deps/jet.py
