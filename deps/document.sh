#!/bin/sh
cd docs
rm -rf build
julia --color=yes make.jl
