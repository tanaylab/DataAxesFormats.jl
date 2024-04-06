#!/bin/bash
set -e -o pipefail
julia deps/static_analysis.jl
