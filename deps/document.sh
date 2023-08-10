#!/bin/bash
set -e -o pipefail
julia --color=no deps/document.jl
sed -i 's: on <span class="colophon-date" title="[^"]*">[^<]*</span>::' docs/*/*html
rm -rf docs/*/*.{cov,jl}
