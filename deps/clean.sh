#!/bin/bash
set -e -o pipefail
rm -rf tracefile.info src/*.cov src/*/*.cov test/*.cov docs/build docs/assets docs/*.{html,js,cov} deps/.format deps/.untested
