#!/bin/bash
set -e -o pipefail
JULIA_DEBUG="" julia --color=no deps/jet.jl 2>&1 | tee junk.todox.jet | python3 deps/jet.py
