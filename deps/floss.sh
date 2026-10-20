#!/bin/bash
set -e -o pipefail
julia --project=. -e 'using Pkg; Pkg.resolve(); Pkg.instantiate(); Pkg.precompile()'
julia --project=. deps/floss.jl
