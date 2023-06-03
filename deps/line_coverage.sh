#!/bin/sh
set -e
julia deps/line_coverage.jl 2>&1 | grep -v 'Info:'
