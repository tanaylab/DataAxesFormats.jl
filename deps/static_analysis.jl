using LanguageServer
using StaticLint
using SymbolServer

language_server = LanguageServerInstance(Pipe(), stdout, ".")
_, symbols = SymbolServer.getstore(language_server.symbol_server, ".")
language_server.global_env.symbols = symbols
language_server.global_env.extended_methods = SymbolServer.collect_extended_methods(language_server.global_env.symbols)
language_server.global_env.project_deps = collect(keys(language_server.global_env.symbols))

file = StaticLint.loadfile(language_server, abspath("src/Daf.jl"))
StaticLint.semantic_pass(LanguageServer.getroot(file))

global errors = 0
global skipped = 0
global unused = 0
for doc in LanguageServer.getdocuments_value(language_server)
    StaticLint.check_all(
        LanguageServer.getcst(doc),
        language_server.lint_options,
        LanguageServer.getenv(doc, language_server),
    )
    LanguageServer.mark_errors(doc, doc.diagnostics)
    no_lint_lines = Set{Int}()
    unused_no_lint_lines = Set{Int}()
    for (line_number, line_text) in enumerate(readlines(doc._path))
        if contains(line_text, "NOLINT")
            push!(no_lint_lines, line_number)
            push!(unused_no_lint_lines, line_number)
        end
    end
    for diagnostic in doc.diagnostics
        line_number = diagnostic.range.start.line + 1
        character_number = diagnostic.range.start.character + 1
        if line_number in no_lint_lines
            delete!(unused_no_lint_lines, line_number)
            global skipped
            skipped += 1
        else
            println("$(doc._path):$(line_number):$(character_number): $(diagnostic.message)")
            global errors
            errors += 1
        end
    end
    for line_number in sort(collect(unused_no_lint_lines))
        global unused
        unused += 1
        println("$(doc._path):$(line_number): unused NOLINT directive)")
    end
end

message = "StaticLint:"
separator = ""
if errors > 0
    message *= " $(errors) errors"
    separator = ","
end
if skipped > 0
    message *= "$(separator) $(skipped) skipped"
    separator = ","
end
if unused > 0
    message *= "$(separator) $(unused) unused"
end

if errors + skipped + unused > 0
    println(message)
else
    println("StaticLint: clean!")
end

if errors + unused > 0
    exit(1)
end
