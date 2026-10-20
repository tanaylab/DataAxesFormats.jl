using TOML

using Base.JuliaSyntax: @K_str, children, kind, parseall, source_line, sourcetext, SyntaxNode

PACKAGE_NAME = TOML.parsefile("Project.toml")["name"]

# The module-level assignments of a file which are neither `const` nor typed (`name::Type = value`), as `(line_number,
# name)`. A function which uses such a variable looks it up at run time, and can't infer its type. So a type alias must
# be `const`, and a setting must be typed.
function untyped_module_variables(path::AbstractString)::Vector{Tuple{Int, String}}
    untyped = Tuple{Int, String}[]
    collect_untyped_module_variables!(untyped, parseall(SyntaxNode, read(path, String); filename = path))
    return untyped
end

function collect_untyped_module_variables!(untyped::Vector{Tuple{Int, String}}, node::SyntaxNode)::Nothing
    node_kind = kind(node)
    if node_kind in (K"toplevel", K"module", K"block")
        for child in children(node)
            collect_untyped_module_variables!(untyped, child)
        end
    elseif node_kind == K"doc"
        collect_untyped_module_variables!(untyped, children(node)[end])
    elseif node_kind == K"=" && kind(children(node)[1]) in (K"Identifier", K"curly")
        push!(untyped, (source_line(node), sourcetext(children(node)[1])))
    end
    return nothing
end

# The local variables which closures capture in a `Core.Box`, in the methods of the `modules` defined in the `paths`, as
# `(path, line_number, name)`. A variable is boxed when a closure captures it and it is assigned more than once. Each
# access of a boxed variable is untyped. The line is that of its first assignment in the method's body. A variable which
# is only assigned in closures is reported at the start of its scope.
function boxed_variables(modules::Set{Module}, paths::Set{String})::Vector{Tuple{String, Int, String}}
    boxed = Set{Tuple{String, Int, String}}()
    path_per_file = Dict{Symbol, String}()
    Base.visit(Core.methodtable) do method
        if method.module in modules && method.source !== nothing
            path = get!(path_per_file, method.file) do
                return isfile(String(method.file)) ? realpath(String(method.file)) : ""
            end
            if path in paths
                collect_boxed_variables!(boxed, method, path)
            end
        end
        return nothing
    end
    return sort!(collect(boxed))
end

# A boxed variable is created by `slot = Core.Box()` (or `Core.Box(argument)`) at the start of its scope. Each
# assignment to it is a `Core.setfield!(box, :contents, value)`, where the `box` is the slot or an SSA value loading it.
function collect_boxed_variables!(boxed::Set{Tuple{String, Int, String}}, method::Method, path::String)::Nothing
    code = Base.uncompressed_ir(method)
    line_per_slot = Dict{Int, Int}()
    for (position, statement) in enumerate(code.code)
        if statement isa Expr && statement.head == :(=) && is_call_of(statement.args[2], :Box)
            line_per_slot[statement.args[1].id] = statement_line(code, method, position, method.line)
        end
    end
    first_assignment_line_per_slot = Dict{Int, Int}()
    for (position, statement) in enumerate(code.code)
        if is_call_of(statement, :setfield!) && statement.args[3] == QuoteNode(:contents)
            slot = statement.args[2]
            if slot isa Core.SSAValue
                slot = code.code[slot.id]
            end
            if slot isa Core.SlotNumber && haskey(line_per_slot, slot.id)
                line_number = statement_line(code, method, position, line_per_slot[slot.id])
                first_assignment_line_per_slot[slot.id] =
                    min(line_number, get(first_assignment_line_per_slot, slot.id, line_number))
            end
        end
    end
    for (slot, line_number) in line_per_slot
        push!(boxed, (path, get(first_assignment_line_per_slot, slot, line_number), String(code.slotnames[slot])))
    end
    return nothing
end

function is_call_of(statement::Any, name::Symbol)::Bool
    return statement isa Expr && statement.head == :call && statement.args[1] == GlobalRef(Core, name)
end

function statement_line(code::Core.CodeInfo, method::Method, position::Integer, default_line::Integer)::Int
    for line_info in Base.IRShow.buildLineInfoNode(code.debuginfo, method, position)
        if line_info.file == method.file
            return Int(line_info.line)
        end
    end
    return Int(default_line)
end

# Known cases for the self-test. Each boxed variable is marked on the line of its first assignment, which is never the
# first statement of its scope (where the box is created), except for a variable which is only assigned in a closure.
module FlossFixtures

struct Shown end

function boxed_in_function(values::Vector{Int})::Vector{Int}
    n_values = length(values)
    if n_values > 1
        order = sortperm(values)  # BOXED order
        order = order[2:end]
    else
        order = [1]
    end
    return map(index -> values[order[index]], 1:length(order))
end

function boxed_argument(values::Vector{Int})::Vector{Int}
    n_values = length(values)
    if n_values > 1
        values = sort(values)  # BOXED values
    end
    return map(index -> values[index], 1:n_values)
end

function boxed_in_closure(values::Vector{Int})::Vector{Int}
    return map(1:2) do offset
        n_values = length(values)
        if offset > 1
            shifted = values .+ offset  # BOXED shifted
        else
            shifted = values
        end
        return sum(map(index -> shifted[index], 1:n_values))
    end
end

function assigned_only_in_closure(values::Vector{Int})::Int
    n_values = length(values)  # BOXED total
    local total
    foreach(values) do value
        total = value
        return nothing
    end
    return total + n_values
end

function Base.show(io::IO, ::Shown)::Nothing
    n_marks = 1
    if n_marks > 0
        text = "shown"  # BOXED text
    else
        text = ""
    end
    foreach(_ -> print(io, text), 1:n_marks)
    return nothing
end

function not_boxed(values::Vector{Int})::Vector{Int}
    sorted = sortperm(values)
    order = sorted[2:end]
    return map(index -> values[order[index]], 1:length(order))
end

end  # module

# Verify the rules find exactly the known cases. The boxed variables are found through Julia internals, which a new
# Julia version may change. Without this, such a change would make the rule silently find nothing.
function self_test()::Nothing
    path = realpath(@__FILE__)
    expected_boxed = Tuple{Int, String}[]
    for (line_number, line_text) in enumerate(readlines(path))
        boxed_match = match(r"# BOXED (\w+)$", line_text)
        if boxed_match !== nothing
            push!(expected_boxed, (line_number, boxed_match.captures[1]))
        end
    end
    found_boxed = [
        (line_number, name) for
        (_, line_number, name) in boxed_variables(Set{Module}([FlossFixtures]), Set{String}([path]))
    ]
    if found_boxed != expected_boxed || length(expected_boxed) != 5
        error("floss self-test failed: boxed variables: $(found_boxed) expected: $(expected_boxed)")
    end

    found_untyped = mktemp() do module_path, io
        write(io, "module M\nUNTYPED = 1\nconst ALIAS = Int\nTYPED::Int = 1\nend\n")
        close(io)
        return untyped_module_variables(module_path)
    end
    if found_untyped != [(2, "UNTYPED")]
        error("floss self-test failed: module-level variables: $(found_untyped) expected: [(2, \"UNTYPED\")]")
    end

    return nothing
end

# The modules of the package: its own and all its submodules.
function package_modules(root_module::Module)::Set{Module}
    modules = Set{Module}([root_module])
    pending = [root_module]
    while !isempty(pending)
        parent_module = pop!(pending)
        for name in names(parent_module; all = true)
            if isdefined(parent_module, name)
                value = getfield(parent_module, name)
                if value isa Module && value !== parent_module && parentmodule(value) === parent_module
                    push!(modules, value)
                    push!(pending, value)
                end
            end
        end
    end
    return modules
end

function floss(root_module::Module)::Nothing
    relative_path_per_path = Dict{String, String}()
    for file_name in readdir("src")
        if endswith(file_name, ".jl")
            relative_path_per_path[realpath("src/$(file_name)")] = "src/$(file_name)"
        end
    end

    problems_per_path = Dict(path => Tuple{Int, String}[] for path in keys(relative_path_per_path))
    for (path, problems) in problems_per_path
        for (line_number, name) in untyped_module_variables(path)
            push!(problems, (line_number, "Module-level variable is neither const nor typed: $(name)"))
        end
    end
    for (path, line_number, name) in
        boxed_variables(package_modules(root_module), Set{String}(keys(relative_path_per_path)))
        push!(problems_per_path[path], (line_number, "Captured variable is boxed: $(name)"))
    end

    errors = 0
    skipped = 0
    unused = 0
    for path in sort!(collect(keys(problems_per_path)); by = path -> relative_path_per_path[path])
        relative_path = relative_path_per_path[path]
        no_floss_lines = Set{Int}()
        for (line_number, line_text) in enumerate(readlines(path))
            if contains(line_text, "NOFLOSS")
                push!(no_floss_lines, line_number)
            end
        end
        unused_no_floss_lines = copy(no_floss_lines)
        for (line_number, problem) in sort!(problems_per_path[path])
            if line_number in no_floss_lines
                delete!(unused_no_floss_lines, line_number)
                skipped += 1
            else
                println("$(relative_path):$(line_number): $(problem)")
                errors += 1
            end
        end
        for line_number in sort!(collect(unused_no_floss_lines))
            println("$(relative_path):$(line_number): unused NOFLOSS directive")
            unused += 1
        end
    end

    counts = String[]
    if errors > 0
        push!(counts, "$(errors) errors")
    end
    if skipped > 0
        push!(counts, "$(skipped) skipped")
    end
    if unused > 0
        push!(counts, "$(unused) unused")
    end
    if isempty(counts)
        println("Floss: clean!")
    else
        println("Floss: $(join(counts, ", "))")
    end

    if errors + unused > 0
        exit(1)
    end
    return nothing
end

self_test()
@eval using $(Symbol(PACKAGE_NAME))
floss(getfield(Main, Symbol(PACKAGE_NAME)))
