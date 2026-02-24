#!/bin/bash
set -e -o pipefail
python3 deps/modules_to_dot.py | dot -Tsvg > src/assets/modules.svg
cd src/dots
for F in *.dot
do
    dot -Tsvg $F > ../assets/`echo $F | sed 's:dot$:svg:'`
done
cd ../..
JULIA_DEBUG="" julia --color=no deps/document.jl
python3 deps/document.py docs/v0.2.0
sed -i 's:<img src="assets/\([^ ]*\).svg":<embed src="assets/\1.svg":;s: on <span class="colophon-date" title="[^"]*">[^<]*</span>::;s:<:\n<:g' docs/v0.2.0/*html
rm -rf docs/*/*.{cov,jl}
