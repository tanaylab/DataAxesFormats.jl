#!/bin/bash
set -e -o pipefail
# Verify the shared tools in `deps/` are the ones `deps/fetch_tools.sh` recorded in `deps/tools_version`.
cd deps
if ! tail -n +2 tools_version | sha256sum --check --quiet
then
    echo 'The shared tools in deps/ differ from deps/tools_version (push the change, then run `make fetch_tools`).'
    false
fi
