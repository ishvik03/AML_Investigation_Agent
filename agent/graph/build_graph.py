from __future__ import annotations
from typing import Any, Dict
from langgraph.graph import StateGraph, END
from agent.graph.state import AgentState

from agent.graph.node import (
    node_calculate_risk_signals,
    node_extract_behavior_signals,
    node_validate_output,
    make_node_policy_engine,
)
from agent.graph.node_llm_justification import node_llm_justification

from agent.policy_engine.policy_engine import PolicyEngine

def build_aml_graph(policy_engine: PolicyEngine):

    g = StateGraph(AgentState)

    # Nodes
    g.add_node("tool1_risk", node_calculate_risk_signals)
    g.add_node("tool2_behavior", node_extract_behavior_signals)
    g.add_node("policy_engine", make_node_policy_engine(policy_engine))
    g.add_node("llm_justify", node_llm_justification)
    g.add_node("validate", node_validate_output)

    g.set_entry_point("tool1_risk")
    g.add_edge("tool1_risk", "tool2_behavior")
    g.add_edge("tool2_behavior", "policy_engine")
    g.add_edge("policy_engine", "llm_justify")
    g.add_edge("llm_justify", "validate")
    g.add_edge("validate", END)

    return g.compile()
