from glob import glob
import fileinput
import os
import re
import sys

location_pattern = re.compile(r'(\S+):(\d+)$')

read_path = None
read_lines = {}
unused_lines = {}
bad_paths = set()

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
    path = os.path.relpath(path)
    if not load_file(path):
        return False

    line = int(line) - 1
    unused_lines[path][line] = ""
    return "NOJET" in read_lines[path][line]

for path in glob("src/*.jl"):
    path = os.path.relpath(path)
    load_file(path)

for path in glob("test/*.jl"):
    path = os.path.relpath(path)
    load_file(path)

context_lines = []
context_disabled = []
context_is_local = []
context_changed = False

errors = 0
non_local = 0
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
            context_is_local.pop()

        context_lines.append(line)
        context_disabled.append(is_disabled(*match.groups()))
        context_is_local.append(is_local(line))
        continue

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
for path, lines in unused_lines.items():
    for line_index, line_text in enumerate(lines):
        if "NOJET" in line_text and ".jl/" in line_text:
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
if non_local > 0:
    message += f"{separator} {non_local} non_local"
if unused > 0:
    message += f"{separator} {unused} unused"

if errors + skipped + non_local + unused > 0:
    print(message)
else:
    print("JET: clean!")

if errors + unused > 0:
    sys.exit(1)
