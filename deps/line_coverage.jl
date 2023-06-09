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

using Coverage

# Process '*.cov' files
coverage = process_folder("src")
coverage = append!(coverage, process_folder("test"))

# Process '*.info' files
coverage = merge_coverage_counts(
    coverage,
    filter!(let prefixes = (joinpath(pwd(), "src", ""), joinpath(pwd(), "test", ""))
        c -> any(p -> startswith(c.filename, p), prefixes)
    end, LCOV.readfolder("test")),
)

# Get total coverage for all Julia files
covered_lines, total_lines = get_summary(coverage)

percent = round(Int8, 100 * covered_lines / total_lines)
if percent == 0 && covered_lines > 0
    percent = "<1%"
elseif percent == 100 && covered_lines < total_lines
    percent = ">99%"
else
    percent = "$(percent)%"
end

println("Line coverage: $(percent) ($(covered_lines) covered out of $(total_lines) total lines)")

if seen_problems
    exit(1)
end
