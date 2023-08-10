#!/bin/bash
set -e -o pipefail
deps/clean.sh
JULIA_NUM_THREADS=4 julia --color=no --code-coverage=tracefile.info deps/test.jl "$@" \
|| (deps/clean.sh && false)
