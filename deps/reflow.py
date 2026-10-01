import re
import sys

MAX_LINE_LENGTH = 120

# A line with a run of this many non-space characters is allowed to be too long.
LONG_NAME_LENGTH = 80

BULLET_PATTERN = re.compile(r"^( *)([-*+]|\d+\.) +\S")
LONG_NAME_PATTERN = re.compile(r"\S{%d}" % LONG_NAME_LENGTH)

# Trailing comments which tools read from the line they annotate. A line is allowed to be too long because of them.
MARKERS_PATTERN = re.compile(r"( +# (UNTESTED|untested|ONLY SEEMS UNTESTED|FLAKY TESTED|NOJET|NOLINT))+$")


# Classify each line as code, a line inside a (non-doc) string, a comment, a docstring delimiter, or docstring content.
# A docstring is opened by a line which contains only `"""`.
def line_kinds(lines):
    kinds = []
    is_in_docstring = False
    is_in_string = False
    for line in lines:
        stripped = line.strip()
        if is_in_docstring:
            if '"""' in line:
                kinds.append("docstring_close")
                is_in_docstring = False
            else:
                kinds.append("docstring")
        elif is_in_string:
            kinds.append("string")
            if line.count('"""') % 2 == 1:
                is_in_string = False
        elif stripped == '"""':
            kinds.append("docstring_open")
            is_in_docstring = True
        elif line.count('"""') % 2 == 1:
            kinds.append("code")
            is_in_string = True
        elif stripped == "#" or stripped.startswith("# "):
            kinds.append("comment")
        else:
            kinds.append("code")
    return kinds


def indentation(line):
    return len(line) - len(line.lstrip(" "))


# Lines which are kept as they are, and which end a paragraph or a bullet item.
def is_verbatim(stripped):
    return (
        stripped.startswith(("```", "|", "#", "!!!", "<"))
        or (stripped.startswith("$(") and stripped.endswith(")"))
        or BULLET_PATTERN.match(stripped) is not None
    )


# Split text into words. Code spans, `$(...)` interpolations and `[...](...)` links are never split. JuliaFormatter
# would join a code span which is split across lines.
def split_words(text):
    words = []
    word = ""
    index = 0
    while index < len(text):
        character = text[index]
        if character == " ":
            if word:
                words.append(word)
                word = ""
            index += 1
            continue
        end = atom_end(text, index)
        word += text[index:end]
        index = end
    if word:
        words.append(word)
    return words


# The index just past the atom that starts at the index.
def atom_end(text, index):
    character = text[index]
    if character == "`":
        ticks = len(text[index:]) - len(text[index:].lstrip("`"))
        close = text.find("`" * ticks, index + ticks)
        if close >= 0:
            return close + ticks
    elif character == "$" and text.startswith("$(", index):
        close = matching_close(text, index + 1, "(", ")")
        if close >= 0:
            return close + 1
    elif character == "[":
        close = matching_close(text, index, "[", "]")
        if close >= 0:
            if text.startswith("(", close + 1):
                target_close = matching_close(text, close + 1, "(", ")")
                if target_close >= 0:
                    return target_close + 1
            return close + 1
    return index + 1


# The index of the bracket closing the one at the index, skipping code spans, or -1 if there is none.
def matching_close(text, index, open_bracket, close_bracket):
    depth = 0
    while index < len(text):
        character = text[index]
        if character == "`":
            end = atom_end(text, index)
            if end > index + 1:
                index = end
                continue
        if character == open_bracket:
            depth += 1
        elif character == close_bracket:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return -1


def fill(first_prefix, rest_prefix, text, width):
    filled = []
    current = first_prefix
    has_word = False
    for word in split_words(text):
        if has_word and len(current) + 1 + len(word) > width:
            filled.append(current)
            current = rest_prefix + word
        elif has_word:
            current += " " + word
        else:
            current += word
        has_word = True
    filled.append(current)
    return filled


# Rewrap each markdown paragraph or bullet item which has a line longer than the width, unless the line is allowed to
# be long. Everything else is kept as is.
# Prose is only at the base indentation, or at the indentation of the text of an enclosing bullet item or admonition.
# Anything indented deeper (e.g. a signature block) is code.
def reflow_markdown(lines, base_indentation, width):
    reflowed = []
    contexts = [base_indentation]
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if not stripped:
            reflowed.append(line)
            index += 1
            continue

        line_indentation = indentation(line)
        while len(contexts) > 1 and line_indentation < contexts[-1]:
            contexts.pop()

        if stripped.startswith("```"):
            fence = stripped[: len(stripped) - len(stripped.lstrip("`"))]
            reflowed.append(line)
            index += 1
            while index < len(lines):
                reflowed.append(lines[index])
                index += 1
                if lines[index - 1].strip().startswith(fence):
                    break
            continue

        if stripped.startswith("!!!"):
            reflowed.append(line)
            contexts.append(line_indentation + 4)
            index += 1
            continue

        bullet = BULLET_PATTERN.match(line)
        if bullet is not None:
            text_column = bullet.end() - 1
            item_end = index + 1
            while (
                item_end < len(lines)
                and lines[item_end].strip()
                and indentation(lines[item_end]) == text_column
                and not is_verbatim(lines[item_end].strip())
            ):
                item_end += 1
            item_lines = lines[index:item_end]
            if needs_reflow(item_lines, width):
                text = " ".join(item_line.strip() for item_line in item_lines)[text_column - line_indentation :]
                reflowed += fill(line[:text_column], " " * text_column, text, width)
            else:
                reflowed += item_lines
            contexts.append(text_column)
            index = item_end
            continue

        if is_verbatim(stripped) or line_indentation != contexts[-1]:
            reflowed.append(line)
            index += 1
            continue

        paragraph_end = index + 1
        while (
            paragraph_end < len(lines)
            and lines[paragraph_end].strip()
            and indentation(lines[paragraph_end]) == line_indentation
            and not is_verbatim(lines[paragraph_end].strip())
        ):
            paragraph_end += 1
        paragraph_lines = lines[index:paragraph_end]
        if needs_reflow(paragraph_lines, width):
            text = " ".join(paragraph_line.strip() for paragraph_line in paragraph_lines)
            reflowed += fill(" " * line_indentation, " " * line_indentation, text, width)
        else:
            reflowed += paragraph_lines
        index = paragraph_end

    return reflowed


def needs_reflow(lines, width):
    return any(len(line) > width and LONG_NAME_PATTERN.search(line) is None for line in lines)


# Rewrap a run of whole-line comments which share the same indentation. The text after the `# ` is markdown.
def reflow_comments(lines):
    prefix = " " * indentation(lines[0]) + "#"
    texts = [line[len(prefix) + 1 :] for line in lines]
    reflowed = reflow_markdown(texts, 0, MAX_LINE_LENGTH - len(prefix) - 1)
    return [prefix + " " + text if text else prefix for text in reflowed]


def reflow_lines(lines):
    kinds = line_kinds(lines)
    reflowed = []
    index = 0
    while index < len(lines):
        kind = kinds[index]
        if kind == "docstring_open":
            reflowed.append(lines[index])
            end = index + 1
            while end < len(lines) and kinds[end] == "docstring":
                end += 1
            reflowed += reflow_markdown(lines[index + 1 : end], indentation(lines[index]), MAX_LINE_LENGTH)
            index = end
        elif kind == "comment":
            end = index + 1
            while end < len(lines) and kinds[end] == "comment" and indentation(lines[end]) == indentation(lines[index]):
                end += 1
            reflowed += reflow_comments(lines[index:end])
            index = end
        else:
            reflowed.append(lines[index])
            index += 1
    return reflowed


# Whether each line is Julia output inside a `jldoctest` block of a docstring.
def doctest_lines(lines, kinds):
    is_doctest = []
    is_in_doctest = False
    for line, kind in zip(lines, kinds):
        stripped = line.strip()
        if kind != "docstring":
            is_in_doctest = False
            is_doctest.append(False)
        elif stripped.startswith("```"):
            is_in_doctest = stripped.startswith("```jldoctest")
            is_doctest.append(False)
        else:
            is_doctest.append(is_in_doctest)
    return is_doctest


def is_allowed(line, is_doctest):
    return (
        len(MARKERS_PATTERN.sub("", line)) <= MAX_LINE_LENGTH
        or is_doctest
        or line.strip().startswith("|")
        or LONG_NAME_PATTERN.search(line) is not None
    )


def read_lines(path):
    with open(path, "r") as file:
        return file.read().split("\n")


def reflow_file(path):
    lines = read_lines(path)
    reflowed = reflow_lines(lines)
    if reflowed != lines:
        with open(path, "w") as file:
            file.write("\n".join(reflowed))


def check_file(path):
    lines = read_lines(path)
    is_doctest = doctest_lines(lines, line_kinds(lines))
    is_ok = True
    for line_number, (line, is_line_doctest) in enumerate(zip(lines, is_doctest), 1):
        if not is_allowed(line, is_line_doctest):
            print(f"{path}:{line_number}: {len(line)}")
            is_ok = False
    return is_ok


if sys.argv[1:2] == ["--check"]:
    are_all_ok = True
    for path in sys.argv[2:]:
        are_all_ok = check_file(path) and are_all_ok
    if not are_all_ok:
        sys.exit(1)
else:
    for path in sys.argv[1:]:
        reflow_file(path)
