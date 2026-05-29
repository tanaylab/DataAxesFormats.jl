#!/bin/bash
set -e -o pipefail
grep -H -n '.' */*.cov \
| sed 's/\.[0-9][0-9]*\.cov:\([0-9][0-9]*\): [ ]*\([^ ]*\) /`\1`\2`/' \
| sort -t '`' -k '1,1' -k '2n,2' \
| awk -F '`' '
    BEGIN {
        OFS = "`"
        prev_file = ""
        prev_line = -1
        prev_count = "-"
        prev_text = ""
    }
    {
        next_file = $1
        next_line = $2
        next_count = $3
        next_text = $4
        if (next_file == prev_file && next_line == prev_line) {
            if (next_count != "-") {
                if (prev_count == "-") {
                    prev_count = next_count
                } else {
                    prev_count += next_count
                }
            }
        } else {
            if (prev_file != "") {
                print prev_file, prev_line, prev_count, prev_text
            }
            prev_file = next_file
            prev_line = next_line
            prev_count = next_count
            prev_text = next_text
        }
    }
' \
| awk -F '`' '
    BEGIN {
        state = 0
        # state 0: Outside a function
        # state 1: Function declaration
        # state 2: Function body
        # state 3: Comment
        # `func_directive` carries the function-level marker (if any) from the signature line to body
        # lines: "" = no directive, "untested" = body $3==0 suppressed, "flaky" = body lines get `# FLAKY
        # TESTED` appended, "seems" = body lines get `# ONLY SEEMS UNTESTED` appended.
        func_directive = ""
        OFS = "`"
        # Buffer of lines from the `function` keyword through the first body line with a determinate count.
        # `buf_count > 0` means we are pending a body-coverage verdict; the buffered signature line is
        # `buf[1]` and its `$3` will be patched to `-` (covered, no-directive) or kept at "0" (uncovered)
        # when the first executable body line arrives.
        buf_count = 0
    }
    function detect_directive(text) {
        if (tolower(text) ~ /# flaky tested/) return "flaky"
        if (tolower(text) ~ /# only seems untested/) return "seems"
        if (tolower(text) ~ /# untested/) return "untested"
        return ""
    }
    function flush_buf(   i) {
        for (i = 1; i <= buf_count; i++) {
            print buf[i]
        }
        buf_count = 0
    }
    function set_buf_signature_count(count,   parts) {
        split(buf[1], parts, "`")
        buf[1] = parts[1] OFS parts[2] OFS count OFS parts[4]
    }
    (state == 0 || state == 3) && $4 ~ /"""/ { state = 3 - state }
    state == 0 && $4 ~ /^[@A-Z][A-Za-z0-9:{}, ()]* =/ && $3 == "-" { $3 = "0" }
    state == 2 && $4 ~ /^end/ { state = 0; if (buf_count > 0) { flush_buf() }; func_directive = "" }
    state != 3 && ($3 != "-" && $3 != "0") && $4 ~ /^(@.* )?[ ]*function / && $4 !~ /^function.*end/ {
        if (buf_count > 0) { flush_buf() }
        state = 1; func_directive = detect_directive($0)
    }
    state != 3 && ($3 == "-" || $3 == "0") && $4 ~ /^(@.* )?[ ]*function / && $4 !~ /^function.*end/ {
        if (buf_count > 0) { flush_buf() }
        # Tentatively treat as uncovered (forces `$3="0"` so an unmarked uncov function gets reported
        # at the signature line). The buffer decision below revises this if the body turns out to be
        # covered or the signature carries a FLAKY/SEEMS marker.
        state = 1; func_directive = "untested"; $3 = "0"
        if ($4 !~ /)::|)$/) {
            # Multi-line signature — buffer; the first executable body line will decide whether the
            # signature line is reclassified to `-` (covered, no directive) or kept at "0" (uncovered).
            buf_count = 1
            buf[1] = $0
            next
        }
        # Single-line signature — fall through to the transition rule below, which sets $3="-".
    }
    state == 1 && $4 ~ /)::|)$/ {
        state = 2
        # Single-line `function foo(args)::T`: the signature itself is non-executable in Julia
        # coverage (the count goes to body lines). Mark the signature `-` to avoid a
        # false-positive untested report; the body lines determine actual coverage status.
        if ($4 ~ /^(@.* )?[ ]*function /) {
            $3 = "-"
        }
    }
    # While pending a body-coverage verdict, buffer signature continuation + non-executable body lines.
    # On the first executable body line, patch the buffered signature accordingly and flush. This runs
    # BEFORE the `func_directive == "untested"` suppression rule so the buffer decision sees the
    # original `$3` of the line.
    buf_count > 0 {
        if (state == 1 || $3 == "-") {
            buf_count += 1
            buf[buf_count] = $0
            next
        }
        # First executable body line. Resolve the function-level directive: a signature marker
        # (`# FLAKY TESTED` / `# ONLY SEEMS UNTESTED` / `# UNTESTED`) wins; otherwise body coverage
        # decides.
        sig_directive = detect_directive(buf[1])
        if (sig_directive != "") {
            func_directive = sig_directive
        } else if (state == 2 && $3 != "0") {
            # Body line has a positive count → function is covered; signature was a multi-line form
            # that Julia coverage attributed counts to the body. Reclassify the signature as
            # non-executable so the script does not mis-report it as untested.
            set_buf_signature_count("-")
            func_directive = ""
        }
        # else: state == 2 && $3 == "0" and no signature marker — keep `func_directive = "untested"`.
        flush_buf()
    }
    # Suppress executable-but-uncovered body lines inside an uncovered function. Lines with a
    # positive count (covered) are NOT suppressed - this matters for single-line `function ... )::T`
    # declarations, where Julia coverage attributes the count to body lines, not the signature.
    state > 0 && func_directive == "untested" && $3 == "0" { $3 = "-" }
    # Function-level FLAKY / ONLY SEEMS UNTESTED propagate to body lines that lack their own marker so
    # the final pass classifies them the same way as the signature.
    state == 2 && func_directive == "flaky" && tolower($4) !~ /# (flaky tested|untested|only seems untested)/ {
        $4 = $4 "  # FLAKY TESTED"
    }
    state == 2 && func_directive == "seems" && tolower($4) !~ /# (flaky tested|untested|only seems untested)/ {
        $4 = $4 "  # ONLY SEEMS UNTESTED"
    }
    state != 2 && state != 1 && $3 == "0" { $3 = "-" }
    { print }
' \
| awk -F '`' '
BEGIN {
    OFS = ":"
    untested = 0
    tested = 0
    ok_untested = 0
    ok_tested = 0
    useless_untested = 0
    useless_seems_untested = 0
    flaky_tested = 0
}
tolower($4) ~ /# flaky tested/ { flaky_tested += 1; next }
$3 > 0 { ok_tested += 1 }
$3 == "-" && tolower($4) ~ /# untested|# only seems untested/ {
    print $1, $2, " ! " $4
    useless_untested += 1;
}
$3 == 0 && tolower($4) ~ /@assert false([ ]*#.*)?$|^[ ]*end([ ]*#.*)?$|# untested|# only seems untested/ { ok_untested += 1; next }
$3 == 0 {
    print $1, $2, " - " $4
    untested += 1
}
$3 > 0 && tolower($4) ~ /# untested/ {
    print $1, $2, " + " $4
    tested += 1
}
$3 > 0 && tolower($4) ~ /# only seems untested/ {
    print $1, $2, " ! " $4
    useless_seems_untested += 1
}
END {
    ok_total = ok_untested + ok_tested + flaky_tested
    printf("%s (%.1f%%) tested, %s (%.1f%%) untested, %s (%.1f%%) flaky\n",
           ok_tested, ok_tested * 100.0 / ok_total,
           ok_untested, ok_untested * 100.0 / ok_total,
           flaky_tested, flaky_tested * 100.0 / ok_total)

    if (useless_untested > 0) {
        printf("ERROR: %s lines with useless_untested \"untested\" annotation\n", useless_untested)
    }

    if (useless_seems_untested > 0) {
        printf("ERROR: %s lines with useless_untested \"seems untested\" annotation\n", useless_seems_untested)
    }

    if (untested > 0) {
        printf("ERROR: %s untested lines without an \"untested\" annotation\n", untested)
    }
    if (tested > 0) {
        printf("ERROR: %s tested lines with an \"untested\" annotation\n", tested)
    }

    if (untested > 0 || tested > 0) {
        exit(1)
    }
}
'
