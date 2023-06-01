using Logging
using LoggingExtras
seen_problems = false
detect_problems = EarlyFilteredLogger(global_logger()) do log_args
    if log_args.level >= Logging.Warn
        global seen_problems
        seen_problems = true
    end
    return true
end
global_logger(detect_problems)

using JuliaFormatter
format(
    ".";
    indent = 4,
    margin = 120,
    always_for_in = true,
    for_in_replacement = "in",
    whitespace_typedefs = true,
    whitespace_ops_in_indices = true,
    remove_extra_newlines = true,
    import_to_using = false,
    pipe_to_function_call = false,
    short_to_long_function_def = true,
    long_to_short_function_def = false,
    always_use_return = true,
    whitespace_in_kwargs = true,
    annotate_untyped_fields_with_any = true,
    format_docstrings = true,
    conditional_to_if = true,
    normalize_line_endings = "unix",
    trailing_comma = true,
    trailing_zero = true,
    join_lines_based_on_source = false,
    indent_submodule = true,
    separate_kwargs_with_semicolon = true,
    surround_whereop_typeparameters = true,
    overwrite = true,
    format_markdown = true,
    align_assignment = false,
    align_struct_field = false,
    align_conditional = false,
    align_pair_arrow = false,
    align_matrix = true,
)

if seen_problems
    exit(1)
end
