import fileinput
import re
import sys

location_pattern = re.compile(r'(\S+):(\d+)$')

read_path = None
read_lines = None
bad_paths = set()

def is_disabled(path, line):
    if path in bad_paths:
        return False

    global read_path
    global read_lines

    if path != read_path:
        try:
            with open(path) as file:
                read_lines = list(file.readlines())
            read_path = path
        except:
            bad_paths.add(path)
            return False

    return "NOJET" in read_lines[int(line) - 1]

context_lines = []
context_disabled = []
context_changed = False

errors = 0
skipped = 0

for line in fileinput.input():
    if line.startswith("[toplevel-info]"):
        print(line[:-1])
        sys.stdout.flush()
        continue

    if line == "\n" or "[toplevel-info]" in line or "possible errors" in line:
        continue

    match = location_pattern.search(line)
    if match:
        context_changed = True
        depth = len(line.split(' ')[0])

        while len(context_lines) >= depth:
            context_lines.pop()
            context_disabled.pop()

        context_lines.append(line)
        context_disabled.append(is_disabled(*match.groups()))
        continue

    if any(context_disabled):
        if context_changed:
            skipped += 1
            context_changed = False
    else:
        if context_changed:
            context_changed = False
            errors += 1
            print("")
            for context_line in context_lines:
                print(context_line[:-1])
        print(line[:-1])

if errors > 0:
    if skipped > 0:
        print(f"\nJET: {errors} errors found, {skipped} errors skipped")
    else:
        print(f"\nJET: {errors} errors found")
    sys.exit(1)

if skipped > 0:
    print(f"\nJET: {skipped} errors skipped")
else:
    print(f"\nJET: clean!")
