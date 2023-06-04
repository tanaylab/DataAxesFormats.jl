#!/bin/bash
set -e -o pipefail
rm -rf tracefile.info src/*.cov src/*/*.cov test/*.cov docs/build deps/.format deps/.untested
