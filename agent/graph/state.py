from __future__ import annotations
from typing import Any, Dict, List, Optional, TypedDict, Literal

Decision = Literal["CLOSE_NO_ACTION", "L1_REVIEW", "ESCALATE_L2", "SAR_REVIEW_L2"]

class AgentState(TypedDict, total=False):


    # Input
    enriched_case: Dict[str, Any]

    # Tool outputs
    risk_signals: Dict[str, Any]
    behavior_signals: Dict[str, Any]

    # Policy decision output (deterministic)
    policy_decision: Dict[str, Any]  # store as dict to keep JSON-serializable

    # LLM narrative (optional today)
    llm_justification: Optional[str]

    # Validation
    validation_ok: bool
    validation_errors: List[str]

    # Bookkeeping
    audit_rows: List[Dict[str, Any]]