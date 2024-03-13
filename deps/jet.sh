#!/bin/bash
set -e -o pipefail
julia --color=no deps/jet.jl 2>&1 | python3 deps/jet.py
