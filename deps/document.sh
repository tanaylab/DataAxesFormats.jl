#!/bin/bash
set -e -o pipefail
cd docs
rm -rf build
julia --color=yes make.jl
