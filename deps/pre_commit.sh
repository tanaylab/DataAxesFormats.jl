#!/bin/bash
set -e -o pipefail
deps/unindexed_files.sh
if grep -i 'TODO'"X" `git ls-files`
then
    exit 1
fi
deps/format.sh
deps/unindexed_files.sh
deps/test.sh
deps/untested_lines.sh
deps/line_coverage.sh
deps/unindexed_files.sh
