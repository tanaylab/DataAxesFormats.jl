#!/bin/bash
set -e -o pipefail
python3 deps/modules_to_dots.py | dot -Tsvg > src/assets/modules.svg
