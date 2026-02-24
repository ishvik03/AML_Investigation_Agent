"""
FastAPI backend for AML LangGraph Agent.
Serves case options and runs the agent on selected cases.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Project root (parent of api/)
BASE_DIR = Path(__file__).resolve().parents[1]
ENRICHED_CASES_PATH = BASE_DIR / "Generate_Data" / "enriched_cases" / "enriched_cases.jsonl"
POLICY_PATH = BASE_DIR / "policies" / "policy_v1.json"
OUT_DIR = BASE_DIR / "agent" / "eval_results"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_cases(limit: int = 5) -> List[Dict[str, Any]]:
    """Load first N cases from enriched_cases.jsonl."""
    cases = []
    with ENRICHED_CASES_PATH.open("r") as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            line = line.strip()
            if line:
                cases.append(json.loads(line))
    return cases


def run_agent_on_case(enriched_case: Dict[str, Any]) -> Dict[str, Any]:
    """Run the LangGraph agent on a single enriched case. Returns final state."""
    from agent.graph.build_graph import build_aml_graph
    from agent.policy_engine.policy_engine import PolicyEngine

    policy_engine = PolicyEngine(
        policy_path=POLICY_PATH,
        audit_log_path=OUT_DIR / "policy_audit_api.jsonl",
        decision_output_path=OUT_DIR / "policy_decisions_api.jsonl",
        append_outputs=True,
    )
    graph = build_aml_graph(policy_engine)

    state: Dict[str, Any] = {"enriched_case": enriched_case}
    final_state = graph.invoke(state)
    return dict(final_state)


app = FastAPI(title="AML Investigation Agent API")


@app.get("/")
def root():
    return {
        "message": "AML Investigation Agent API",
        "docs": "/docs",
        "cases": "/api/cases",
        "run": "POST /api/run",
        "frontend": "Open http://localhost:5173 for the UI",
    }
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RunRequest(BaseModel):
    enriched_case: Dict[str, Any]


@app.get("/api/cases")
def get_cases(limit: int = 5) -> Dict[str, Any]:
    """Return up to 5 case options for the frontend dropdown."""
    if not ENRICHED_CASES_PATH.exists():
        raise HTTPException(status_code=500, detail="enriched_cases.jsonl not found")
    cases = load_cases(limit=limit)
    # Return full cases so frontend can send selected one to /api/run
    return {"cases": cases}


@app.post("/api/run")
def run_case(req: RunRequest) -> Dict[str, Any]:
    """Run the LangGraph agent on the selected case. Returns the final output."""
    try:
        final_state = run_agent_on_case(req.enriched_case)
        return {
            "case_id": req.enriched_case.get("case_id"),
            "customer_id": req.enriched_case.get("customer_id"),
            "validation_ok": final_state.get("validation_ok", False),
            "validation_errors": final_state.get("validation_errors", []),
            "policy_decision": final_state.get("policy_decision"),
            "llm_justification": final_state.get("llm_justification"),
            "risk_signals": final_state.get("risk_signals"),
            "behavior_signals": final_state.get("behavior_signals"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
