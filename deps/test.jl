import Pkg
Pkg.activate(".")

using Logging
using LoggingExtras

module CountWarnings
seen_problems = 0
end

detect_problems = EarlyFilteredLogger(global_logger()) do log_args
    if log_args.level >= Logging.Warn
        CountWarnings.seen_problems += 1
    end
    return true
end
global_logger(detect_problems)

Pkg.test(; coverage = true, test_args = Base.ARGS)

if CountWarnings.seen_problems > 1
    exit(1)
end
