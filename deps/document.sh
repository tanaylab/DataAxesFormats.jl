#!/bin/bash
set -e -o pipefail
rm -f docs/*.*
julia --color=no deps/document.jl
rm -f docs/*.jl docs/*.cov
