def eval_op(a, op, b):
    if op == "==": return a == b
    if op == "!=": return a != b
    if op == ">": return a > b
    if op == ">=": return a >= b
    if op == "<": return a < b
    if op == "<=": return a <= b
    if op == "in": return a in b
    raise ValueError(f"Unsupported op: {op}")

def match_conditions(tx, conditions):
    for cond in conditions:
        if not eval_op(tx.get(cond["field"]), cond["op"], cond["value"]):
            return False
    return True