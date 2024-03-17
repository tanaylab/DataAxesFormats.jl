#!/bin/bash
set -e -o pipefail
julia --color=no deps/document.jl
sed -i 's: on <span class="colophon-date" title="[^"]*">[^<]*</span>::;s:<:\n<:g' docs/v0.1.0/*html
rm -rf docs/*/*.{cov,jl}
