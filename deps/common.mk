# The common Makefile rules of a Julia package. A package Makefile includes it, then adds any package-only rules.

DOCS_VERSION := v$(shell sed -n 's/^version = "\(.*\)"$$/\1/p' Project.toml)

TODO = todo
TODO_X = $(TODO)x

.PHONY: ci
ci: format check coverage docs $(TODO_X) unindexed_files check_tools

$(TODO_X): deps/.did.$(TODO_X)

deps/.did.$(TODO_X): $(shell git ls-files | grep -v docs)
	deps/$(TODO_X).sh
	@touch deps/.did.$(TODO_X)

.PHONY: unindexed_files
unindexed_files:
	@deps/unindexed_files.sh

.PHONY: check_tools
check_tools:
	@deps/check_tools.sh

# Use `make fetch_tools TOOLS=../JuliaPackageTools` to fetch from a local clone instead of GitHub.
.PHONY: fetch_tools
fetch_tools:
	deps/fetch_tools.sh $(TOOLS)

.PHONY: format
format: deps/.did.format
deps/.did.format: *.md */*.jl deps/format.sh deps/format.jl deps/reflow.py
	deps/format.sh
	@touch deps/.did.format

.PHONY: check
check: static_analysis jet aqua untested_lines

.PHONY: static_analysis
static_analysis: deps/.did.static_analysis

deps/.did.static_analysis: *.toml src/*.jl test/*.toml test/*.jl deps/static_analysis.sh deps/static_analysis.jl
	deps/static_analysis.sh
	@touch deps/.did.static_analysis

.PHONY: jet
jet: deps/.did.jet

deps/.did.jet: *.toml src/*.jl test/*.toml test/*.jl deps/jet.sh deps/jet.jl deps/jet.py
	deps/jet.sh
	@touch deps/.did.jet

.PHONY: aqua
aqua: deps/.did.aqua

deps/.did.aqua: *.toml src/*.jl test/*.toml test/*.jl deps/aqua.sh deps/aqua.jl
	deps/aqua.sh
	@touch deps/.did.aqua

.PHONY: test
test: tracefile.info

tracefile.info: *.toml src/*.jl test/*.toml test/*.jl deps/test.sh deps/test.jl deps/clean.sh
	deps/test.sh

.PHONY: line_coverage
line_coverage: deps/.did.coverage

deps/.did.coverage: tracefile.info deps/line_coverage.sh deps/line_coverage.jl
	deps/line_coverage.sh
	@touch deps/.did.coverage

.PHONY: untested_lines
untested_lines: deps/.did.untested

deps/.did.untested: deps/.did.coverage deps/untested_lines.sh
	deps/untested_lines.sh
	@touch deps/.did.untested

.PHONY: coverage
coverage: untested_lines line_coverage

.PHONY: docs
docs: docs/$(DOCS_VERSION)/index.html

docs/$(DOCS_VERSION)/index.html: src/*.jl src/*.md deps/document.sh deps/document.jl deps/document.py \
    docs/metadata.toml docs/make.jl deps/modules_to_dot.py
	deps/document.sh
	@# The documentation is generated into the repository, so the check that nothing is left unstaged would
	@# otherwise trip over the files this very rule just wrote.
	@git add -A docs $(wildcard src/assets)

.PHONY: clean
clean:
	deps/clean.sh

.PHONY: add_pkgs
add_pkgs:
	deps/add_pkgs.sh

tags: */*.jl
	ctags */*.jl
	sed -i 's/!\t/\t/' tags
