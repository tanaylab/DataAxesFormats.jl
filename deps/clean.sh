#!/bin/bash
set -e -o pipefail
VERSION=`sed -n 's/^version = "\(.*\)"$/\1/p' Project.toml`
rm -rf tracefile.info src/*.cov src/*/*.cov test/*.cov deps/.did.*
rm -rf docs/build docs/assets docs/*.{html,js,cov} docs/v$VERSION
