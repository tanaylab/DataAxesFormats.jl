#!/bin/bash
set -e -o pipefail
julia deps/line_coverage.jl 2>&1 | grep -v 'Info:'
