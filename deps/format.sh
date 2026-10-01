#!/bin/bash
set -e -o pipefail
julia --project=deps/format_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
python3 deps/reflow.py `git ls-files '*.jl'`
julia --project=deps/format_env --color=no deps/format.jl
sed -i 's/do  [ ]*#/do  #/' src/*jl
sed -i 's/^ [ ]*$//' README.md
python3 deps/reflow.py --check `git ls-files '*.jl'`
