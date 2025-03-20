from glob import glob

deps_of = {}
paths = {}

for module_path in glob("src/*.jl"):
    with open(module_path, "r") as module_file:
        for line in module_file.readlines():
            line = line[:-1]

            if "..." in line:
                continue

            if line.startswith("module "):
                module_name = line[7:]
                deps_of[module_name] = set()
                paths[module_name] = module_path.split("/")[1][:-3]
                continue

            parts = line.split(" ..")
            if len(parts) == 2:
                dependency_name = parts[1].split(".")[0]
                if " " not in dependency_name:
                    deps_of[module_name].add(dependency_name)

level = 0

def is_reachable(dep, mod, path):
    global level

    #print(f"{level * ' '}is_reachable {dep} -> {mod} path: {path}")
    level += 1

    if mod in path:
        level -= 1
        return False

    for other_dep in deps_of[mod]:
        #print(f"{level * ' '}{other_dep} -> {mod}...")
        if other_dep == dep:
            if len(path) == 0:
                #print(f"{level * ' '}...is direct")
                continue
            level -= 1
            #print(f"{level * ' '}...is indirect!")
            return True
        path.append(mod)
        result = is_reachable(dep, other_dep, path)
        if result:
            level -= 1
            return True
        path.pop()
    level -= 1
    return False

for mod, deps in deps_of.items():
    #for dep in deps:
        #path = []
        #if is_reachable(dep, mod, path):
            #print(f"{dep} ---> {mod} !!! {list(reversed(path))}")
        #else:
            #print(f"{dep} ===> {mod} !!! {list(reversed(path))}")

    deps_of[mod] = {dep for dep in deps if not is_reachable(dep, mod, [])}

print("digraph {")
print("node [ fontname = \"Sans-Serif\" ];")

for mod, deps in deps_of.items():
    if mod != "DataAxesFormats":
        print(f"{mod} [ shape = box, color = white, margin = 0.03, width = 0, height = 0, URL = \"../{paths[mod]}.html\" target = _top ];")
    for dep in deps:
        prefix = mod[:-3]
        print(f"{dep} -> {mod};")

print("}")
