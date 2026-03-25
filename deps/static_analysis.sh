#!/bin/bash
set -e -o pipefail
julia --project=deps/lint_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
julia --project=deps/lint_env deps/static_analysis.jl
