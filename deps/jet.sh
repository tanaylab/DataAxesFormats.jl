#!/bin/bash
set -e -o pipefail
JULIA_DEBUG="" julia --color=no deps/jet.jl 2>&1 | python3 deps/jet.py
