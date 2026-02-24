from __future__ import annotations

import ast
import re
from typing import Any, Dict, Tuple


class RuleEvalError(ValueError):
    """Raised when a rule expression is invalid or unsafe."""
    pass


# ============================================================
# NORMALIZATION
# ============================================================

def _normalize_expr(expr: str) -> str:
    """
    Convert policy DSL to restricted Python boolean expression.

    Supported DSL features:
      - AND / OR / NOT (case insensitive)
      - true / false
      - customer_risk enum values: Low / Medium / High
      - comparison operators: ==, >=, <=, >, <
      - parentheses
    """

    if not isinstance(expr, str) or not expr.strip():
        raise RuleEvalError("Rule must be a non-empty string")

    s = expr.strip()

    # --- Logical operators ---
    s = re.sub(r"\bAND\b", "and", s, flags=re.IGNORECASE)
    s = re.sub(r"\bOR\b", "or", s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\b", "not", s, flags=re.IGNORECASE)

    # --- Boolean literals ---
    s = re.sub(r"\btrue\b", "True", s, flags=re.IGNORECASE)
    s = re.sub(r"\bfalse\b", "False", s, flags=re.IGNORECASE)

    # --- Quote enum values for customer_risk ---
    # Example:
    #   customer_risk == High
    # becomes:
    #   customer_risk == "High"
    s = re.sub(
        r'customer_risk\s*==\s*(Low|Medium|High)\b',
        r'customer_risk == "\1"',
        s
    )

    return s


# ============================================================
# AST SAFETY VALIDATION
# ============================================================

_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.UnaryOp,
    ast.Compare,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Eq,
    ast.NotEq,
    ast.Gt,
    ast.GtE,
    ast.Lt,
    ast.LtE,
)


def _assert_safe_ast(node: ast.AST) -> None:
    """
    Walk AST and ensure only allowed safe nodes exist.
    Blocks:
      - function calls
      - attribute access
      - subscripting
      - lambda
      - comprehensions
      - etc.
    """

    for child in ast.walk(node):
        if not isinstance(child, _ALLOWED_NODES):
            raise RuleEvalError(f"Disallowed expression node: {type(child).__name__}")

        # Explicitly block dangerous constructs
        if isinstance(child, (ast.Call, ast.Attribute, ast.Subscript, ast.Lambda)):
            raise RuleEvalError(f"Disallowed operation in rule: {type(child).__name__}")


# ============================================================
# RULE EVALUATION
# ============================================================

def evaluate_rule(expr: str, variables: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Safely evaluate a single rule expression.

    Returns:
        (matched: bool, normalized_expression: str)

    Security:
        - No builtins
        - No function calls
        - No attribute access
        - Strict variable validation
    """

    normalized = _normalize_expr(expr)

    try:
        tree = ast.parse(normalized, mode="eval")
    except SyntaxError as e:
        raise RuleEvalError(f"Rule syntax error: {expr}") from e

    _assert_safe_ast(tree)

    # Validate all variable names used in expression
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    missing = [n for n in sorted(names) if n not in variables]

    if missing:
        raise RuleEvalError(
            f"Rule references unknown variables: {missing} | rule={expr}"
        )

    compiled = compile(tree, "<policy_rule>", "eval")

    try:
        result = eval(compiled, {"__builtins__": {}}, variables)
    except Exception as e:
        raise RuleEvalError(f"Rule evaluation failed: {expr}") from e

    if not isinstance(result, bool):
        raise RuleEvalError(f"Rule did not evaluate to bool: {expr}")

    return result, normalized