import yaml
import json
import sys

def print_graph():
    with open("deps/query_syntax.bnf.yaml", "r") as file:
        data = yaml.safe_load(file)

    print('digraph {')
    print('fontname = "Sans-Serif";')
    print('node [ fontname = "Sans-Serif" ];')
    print('edge [ fontname = "Sans-Serif" ];')
    print('concentrate = true;')

    types = data["types"]

    nodes = {}
    for name, rule in data["rules"].items():
        print_rule(types, nodes, name, rule)

    for name, rule in data["rules"].items():
        connect_node(name, rule.get("next", []))

    print('}')


NODES_BY_NAME = {}


def print_rule(types, nodes, name, rule):
    from_node = print_stack_node(nodes, rule["from"])
    to_node = print_stack_node(nodes, rule["to"])
    NODES_BY_NAME[name] = (from_node, to_node)
    operations_node = print_operations_node(rule["by"])
    if operations_node is None:
        print_edge(from_node, to_node)
    else:
        print_edge(from_node, operations_node, name)
        print_edge(operations_node, to_node, "")


def connect_node(name, nexts):
    node_from, node_to = NODES_BY_NAME[name]
    for next in nexts:
        next_from, next_to = NODES_BY_NAME[next]
        print_edge(node_to, next_from)


NODES = 0


def print_stack_node(nodes, stack):
    key = json.dumps(stack, sort_keys=True)
    if key in nodes:
        return nodes[key]
    global NODES
    NODES += 1
    nodes[key] = NODES

    if stack is None:
        print(f"N{NODES} [ shape = point ]")
    else:
        print(f"N{NODES} [ shape = plain, label=<")
        print("<TABLE BORDER=\"0\">")
        for state in stack:
            print("<TR><TD><TABLE CELLSPACING=\"0\">")
            print("<TR>")
            name = state.get('name', 'new')
            if name == "rest":
                what = "..."
            else:
                what = state.get('what', ' ')
                if not isinstance(what, str):
                    what = ' or '.join(what)
            print(f"<TD ALIGN=\"LEFT\"><B>{name}</B></TD>")
            print(f"<TD ALIGN=\"LEFT\"><B>{what}</B></TD>")
            print("</TR>")
            for field, value in state.items():
                if value is None:
                    value = "~"
                if field not in ("name", "what"):
                    parts = value.split("<br/>")
                    parts = [ part + ((len(part) // 12) * " ") for part in parts ]
                    value = "<br/>".join(parts)
                    print("<TR>")
                    print(f"<TD ALIGN=\"LEFT\">{field}</TD>")
                    print(f"<TD ALIGN=\"LEFT\">{value}</TD>")
                    print("</TR>")
            print("</TABLE></TD></TR>")
        print("</TABLE>")
        print("> ];")
    return NODES

def is_compatible_stack(from_stack, to_stack):
    if from_stack is None or to_stack is None:
        return from_stack is None and to_stack is None
    if len(from_stack) > 0 and 'name' in from_stack[0] and from_stack[0]['name'] == 'rest':
        assert len(from_stack) == 1
        return True
    if len(to_stack) > 0 and 'name' in to_stack[0] and to_stack[0]['name'] == 'rest':
        assert len(to_stack) == 1
        return True
    if len(from_stack) == 0 and len(to_stack) == 0:
        return True
    if len(from_stack) == 0 or len(to_stack) == 0:
        return False
    return is_compatible_state(from_stack[0], to_stack[0]) and is_compatible_stack(from_stack[1:], to_stack[1:])

def is_compatible_state(from_state, to_state):
    for key in to_state.keys():
        if key != 'name' and (key not in from_state or not is_compatible_value(from_state[key], to_state[key])):
            return False
    return True

def is_compatible_value(from_value, to_value):
    if to_value is None:
        return from_value is None
    if to_value == "+":
        return from_value is not None
    if isinstance(from_value, str):
        return to_value == from_value
    return to_value in from_value

def print_operations_node(operations):
    if operations is None:
        return None
    global NODES
    NODES += 1
    print(f"N{NODES} [ shape = plain, label=<", end="")
    print(f"<FONT FACE=\"Courier\">")
    for operation in operations:
        print(operation[0], end=" ")
    print("<br/>", end=" ")
    for operation in operations:
        print("<br/>" + operation[1], end="")
    print("</FONT>")
    print("> ];")
    return NODES


def print_edge(from_node, to_node, name = None):
    if name is None:
        print(f'N{from_node} -> N{to_node} [ style = dashed ];')
    elif name == "":
        print(f'N{from_node} -> N{to_node};')
    else:
        print(f'N{from_node} -> N{to_node} [ label = \"  {name}\" ];')

print_graph()
