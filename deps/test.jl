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

import (Pkg)
Pkg.activate(".")
Pkg.test(; coverage = true, test_args = Base.ARGS)

if seen_problems
    exit(1)
end
