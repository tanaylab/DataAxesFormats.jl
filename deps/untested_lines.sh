#!/bin/bash
set -e -o pipefail
grep -H -n '.' */*.cov \
| sed 's/\.[0-9][0-9]*\.cov:\([0-9][0-9]*\): [ ]*\(\S*\) /`\1`\2`/' \
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
        OFS = "`"
    }
    state == 0 && $4 ~ /^[@A-Z][A-Za-z0-9:{}, ()]* =/ && $3 == "-" { $3 = "0" }
    state == 2 && $3 == "-" && $4 !~ /^\s*([)]|begin|end|else|try|finally)?\s*(#.*)$/ { $3 = "0" }
    state == 2 && $3 == "0" && $4 ~ /^\s*([)]|begin|end|else|try|finally)?\s*(#.*)?$/ { $3 = "-" }
    state == 2 && $4 ~ /^end/ { state = 0 }
    $4 ~ /^\s*function / { state = 1 }
    state == 1 && $3 == "0" { $3 = "-" }
    state == 1 && $4 ~ /)::/ { state = 2 }
    state != 2 && $3 == "0" { $3 = "-" }
    { print }
' \
| awk -F '`' '
BEGIN {
    OFS = "`"
    untested = 0
    tested = 0
}
tolower($4) ~ /only seems untested/ { next }
$3 == 0 && tolower($4) ~ /untested/ { next }
$3 == 0 {
    print $1, $2, " - " $4
    untested += 1
}
$3 > 0 && tolower($4) ~ /untested/ {
    print $1, $2, " + " $4
    tested += 1
}
END {
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
