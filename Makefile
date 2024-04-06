TODO = todo
TODO_X = $(TODO)x

.PHONY: ci
ci: format check coverage docs $(TODO_X) unindexed_files

$(TODO_X): deps/.$(TODO_X)

deps/.$(TODO_X): $(shell git ls-files | grep -v docs)
	deps/$(TODO_X).sh
	@touch deps/.$(TODO_X)

.PHONY: unindexed_files
unindexed_files:
	@deps/unindexed_files.sh

.PHONY: format
format: deps/.format
deps/.format: */*.jl deps/format.sh deps/format.jl
	deps/format.sh
	@touch deps/.format

.PHONY: check
check: static_analysis jet aqua untested_lines

.PHONY: static_analysis
static_analysis: deps/.static_analysis

deps/.static_analysis: *.toml src/*.jl test/*.toml test/*.jl deps/static_analysis.sh deps/static_analysis.jl
	deps/static_analysis.sh
	@touch deps/.static_analysis

.PHONY: jet
jet: deps/.jet

deps/.jet: *.toml src/*.jl test/*.toml test/*.jl deps/jet.sh deps/jet.jl deps/jet.py
	deps/jet.sh
	@touch deps/.jet

.PHONY: aqua
aqua: deps/.aqua

deps/.aqua: *.toml src/*.jl test/*.toml test/*.jl deps/aqua.sh deps/aqua.jl
	deps/aqua.sh
	@touch deps/.aqua

.PHONY: test
test: tracefile.info

tracefile.info: *.toml src/*.jl test/*.toml test/*.jl deps/test.sh deps/test.jl deps/clean.sh
	deps/test.sh

.PHONY: line_coverage
line_coverage: deps/.coverage

deps/.coverage: tracefile.info deps/line_coverage.sh deps/line_coverage.jl
	deps/line_coverage.sh
	@touch deps/.coverage

.PHONY: untested_lines
untested_lines: deps/.untested

deps/.untested: deps/.coverage deps/untested_lines.sh
	deps/untested_lines.sh
	@touch deps/.untested

.PHONY: coverage
coverage: untested_lines line_coverage

.PHONY: docs
docs: docs/v0.1.0/index.html

docs/v0.1.0/index.html: src/*.jl src/*.md deps/document.sh deps/document.jl
	deps/document.sh

.PHONY: clean
clean:
	deps/clean.sh

.PHONY: add_pkgs
add_pkgs:
	deps/add_pkgs.sh
