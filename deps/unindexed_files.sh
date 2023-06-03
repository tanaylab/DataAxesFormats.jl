#!/bin/sh
set -e
UNTRACKED=`git ls-files --others --exclude-standard`
UNADDED=`git diff`
if [ "Q$UNTRACKED$UNADDED" != "Q" ]
then
    git status
    false
fi
