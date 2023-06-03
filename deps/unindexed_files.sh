#!/bin/bash
set -e -o pipefail
UNTRACKED=`git ls-files --others --exclude-standard`
UNADDED=`git diff`
if [ "$UNTRACKED$UNADDED" != "" ]
then
    git status
    false
fi
