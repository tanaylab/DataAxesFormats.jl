#!/bin/bash
set -e -o pipefail
julia deps/line_coverage.jl 2>&1 \
| grep -v '\(Info: .*\(Detecting\|Searching\|processing\)\)\|Assuming file has no coverage' \
| sed 's/.*process_cov: //'
