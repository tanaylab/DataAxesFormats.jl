from glob import glob
import fileinput
import os
import re
import sys

location_pattern = re.compile(r'(\S+):(\d+)\s*$')

read_path = None
read_lines = {}
unused_lines = {}
bad_paths = set()
# The files of this package. Only their NOJET directives disable reports, so each package disables the reports of its own
# analysis in its own files, and only these are checked for unused NOJET directives.
local_paths = set()

def normalized(path):
    # The same file may be reached through several paths (e.g. through a symbolic link), so it is keyed by its real path.
    return os.path.relpath(os.path.realpath(path))

def load_file(path):
    global read_lines
    global unused_lines
    global bad_paths

    if path in bad_paths:
        return False

    if path in read_lines:
        return True

    try:
        with open(path) as file:
            file_lines = list(file.readlines())

        read_lines[path] = file_lines
        unused_lines[path] = file_lines.copy()
        return True

    except:
        bad_paths.add(path)
        return False

def is_local(line):
    return os.path.basename(os.getcwd()) in line

def is_disabled(path, line):
    path = normalized(path)
    if not load_file(path):
        return False

    line = int(line) - 1
    if line >= len(read_lines[path]):
        return True  # macro-expanded line beyond file end; skip
    unused_lines[path][line] = ""
    return path in local_paths and "NOJET" in read_lines[path][line]

for path in glob("src/*.jl") + glob("test/*.jl"):
    path = normalized(path)
    local_paths.add(path)
    load_file(path)

context_lines = []
context_disabled = []
context_is_local = []
context_changed = False

errors = 0
non_local = 0
skipped = 0
undefined = 0

is_undefined = False
for line in fileinput.input():
    if line.startswith("[toplevel-info]"):
        print(line[:-1])
        sys.stdout.flush()
        continue

    if line == "\n" or "[toplevel-info]" in line or "possible errors" in line:
        continue

    match = location_pattern.search(line)
    if match:
        is_undefined = False
        context_changed = True
        depth = len(line.split(' ')[0])

        while len(context_lines) >= depth:
            context_lines.pop()
            context_disabled.pop()
            context_is_local.pop()

        context_lines.append(line)
        context_disabled.append(is_disabled(*match.groups()))
        context_is_local.append(is_local(line))
        continue

    if 'UndefVarError' in line and 'not defined' in line:
        print(line)
        is_undefined = True
        undefined += 1

    if not is_undefined:
        if any(context_disabled):
            if context_changed:
                skipped += 1

        elif any(context_is_local):
            if context_changed:
                errors += 1
                print("")
                for context_line in context_lines:
                    print(context_line[:-1])
            print(line[:-1])

        else:
            if context_changed:
                non_local += 1

    context_changed = False

unused = 0
for path in sorted(local_paths):
    for line_index, line_text in enumerate(unused_lines.get(path, [])):
        if "NOJET" in line_text:
            if unused == 0:
                print("")
            unused += 1
            print(f"{path}:{line_index + 1}: Unused NOJET directive")

print("")

message = "JET:"
separator = ""
if errors > 0:
    message += f" {errors} errors"
    separator = ","
if skipped > 0:
    message += f"{separator} {skipped} skipped"
    separator = ","
if undefined > 0:
    message += f"{separator} {undefined} undefined"
if non_local > 0:
    message += f"{separator} {non_local} non_local"
if unused > 0:
    message += f"{separator} {unused} unused"

if errors + skipped + non_local + unused + undefined > 0:
    print(message)
else:
    print("JET: clean!")

if errors + unused + undefined > 0:
    sys.exit(1)
