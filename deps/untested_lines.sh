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
        uncov_func = 0
        OFS = "`"
    }
    (state == 0 || state == 3) && $4 ~ /"""/ { state = 3 - state }
    state > 0 && uncov_func { $3 = "-" }
    state == 0 && $4 ~ /^[@A-Z][A-Za-z0-9:{}, ()]* =/ && $3 == "-" { $3 = "0" }
    state == 2 && $4 ~ /^end/ { state = 0 }
    state != 3 && ($3 != "-" && $3 != "0") && $4 ~ /^(@.* )?[ ]*function / && $4 !~ /^function.*end/ { state = 1; uncov_func = 0 }
    state != 3 && ($3 == "-" || $3 == "0") && $4 ~ /^(@.* )?[ ]*function / && $4 !~ /^function.*end/ { state = 1; uncov_func = 1; $3 = "0"; }
    state == 1 && $4 ~ /)::|)$/ { state = 2 }
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
$3 == 0 && tolower($4) ~ /@assert false([ ]*#.*)?$|^[ ]*end([ ]*#.*)?$|^[ ]*local [^ =]*$|# untested|# only seems untested/ { ok_untested += 1; next }
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
