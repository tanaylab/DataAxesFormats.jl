TODO = todo
TODO_X = $(TODO)x

.PHONY: ci
ci: format check coverage docs $(TODO_X) unindexed_files

$(TODO_X): deps/.$(TODO_X)

deps/.$(TODO_X): $(shell git ls-files)
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
check: untested_lines

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
