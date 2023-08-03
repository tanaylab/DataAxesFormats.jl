#!/bin/bash
set -e -o pipefail
rm -f docs/*.*
julia --color=yes deps/document.jl
rm -f docs/*.jl docs/*.cov
