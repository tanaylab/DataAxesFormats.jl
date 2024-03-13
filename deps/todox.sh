#!/bin/bash
set -e -o pipefail
if grep -i -n todo""x $(git ls-files | grep -v 'docs/v')
then
    exit 1
else
    true
fi
