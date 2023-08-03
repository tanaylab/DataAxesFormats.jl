#!/bin/bash
set -e -o pipefail
rm -f docs/*.*
julia --color=yes deps/document.jl
