#!/bin/bash
set -e -o pipefail
VERSION=`sed -n 's/^version = "\(.*\)"$/\1/p' Project.toml`
if grep -q 'assets/modules.svg' src/*.jl src/*.md
then
    mkdir -p src/assets
    python3 deps/modules_to_dot.py | dot -Tsvg > src/assets/modules.svg
fi
if [ -d src/dots ]
then
    mkdir -p src/assets
    for F in src/dots/*.dot
    do
        dot -Tsvg $F > src/assets/`basename $F .dot`.svg
    done
fi
julia --project=deps/document_env -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
JULIA_DEBUG="" julia --project=deps/document_env --color=no deps/document.jl
python3 deps/document.py docs/v$VERSION
sed -i 's: on <span class="colophon-date" title="[^"]*">[^<]*</span>::;s:<:\n<:g' docs/v$VERSION/*html
sed -i -E ':a ; $!N ; s/\n<sub>/<sub>/ ; s/\n<\/sub>/<\/sub>/ ; ta ; P ; D' docs/v$VERSION/*html
rm -rf docs/*/*.{cov,jl}
