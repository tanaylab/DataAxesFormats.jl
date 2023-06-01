#!/bin/sh
if grep -i -n todo""x $(git ls-files)
then
    exit 1
else
    true
fi
