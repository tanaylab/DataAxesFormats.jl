#!/bin/sh
set -e
cd docs
rm -rf build
julia --color=yes make.jl
