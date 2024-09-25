TODO = todo
TODO_X = $(TODO)x

.PHONY: ci
ci: format check coverage docs $(TODO_X) unindexed_files

$(TODO_X): deps/.did.$(TODO_X)

deps/.did.$(TODO_X): $(shell git ls-files | grep -v docs)
	deps/$(TODO_X).sh
	@touch deps/.did.$(TODO_X)

.PHONY: unindexed_files
unindexed_files:
	@deps/unindexed_files.sh

.PHONY: format
format: deps/.did.format
deps/.did.format: */*.jl deps/format.sh deps/format.jl
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
docs: docs/v0.1.1/index.html

docs/v0.1.1/index.html: src/*.jl src/*.md deps/document.sh deps/document.jl docs/make.jl
	deps/document.sh

.PHONY: clean
clean:
	deps/clean.sh

.PHONY: add_pkgs
add_pkgs:
	deps/add_pkgs.sh
