#!/bin/bash
set -e -o pipefail
rm -rf tracefile.info src/*.cov src/*/*.cov test/*.cov
DAF_ENFORCE_CONTRACTS=true JULIA_DEBUG="" JULIA_NUM_THREADS=4 julia --color=no --code-coverage=tracefile.info deps/test.jl "$@" \
|| (deps/clean.sh && false)
