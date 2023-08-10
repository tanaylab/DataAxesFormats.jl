#!/bin/bash
set -e -o pipefail
julia --color=no deps/format.jl
