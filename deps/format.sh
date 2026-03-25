#!/bin/bash
set -e -o pipefail
julia --project=deps/format_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
julia --project=deps/format_env --color=no deps/format.jl
sed -i 's/do  [ ]*#/do  #/' src/*jl
