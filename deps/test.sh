#!/bin/sh
deps/clean.sh
JULIA_NUM_THREADS=4 julia --code-coverage=tracefile.info deps/test.jl "$@" \
|| deps/clean.sh
