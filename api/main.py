"""
FastAPI backend for AML LangGraph Agent.
Serves case options and runs the agent on selected cases.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List

# How long (seconds) to show each progress step so the user can read it (deterministic steps finish very fast)
PROGRESS_STEP_DELAY_SECONDS = 0.9

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Project root (parent of api/)
BASE_DIR = Path(__file__).resolve().parents[1]
ENRICHED_CASES_PATH = BASE_DIR / "Generate_Data" / "enriched_cases" / "enriched_cases.jsonl"
CUSTOMER_PROFILES_PATH = BASE_DIR / "Generate_Data" / "customer_engine" / "customer_profiles.jsonl"
POLICY_PATH = BASE_DIR / "policies" / "policy_v1.json"
OUT_DIR = BASE_DIR / "agent" / "eval_results"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_customer_names() -> Dict[str, str]:
    """Build customer_id -> display name from customer_profiles.jsonl (first_name last_name)."""
    names: Dict[str, str] = {}
    if not CUSTOMER_PROFILES_PATH.exists():
        return names
    try:
        with CUSTOMER_PROFILES_PATH.open("r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                cid = row.get("customer_id")
                first = (row.get("first_name") or "").strip()
                last = (row.get("last_name") or "").strip()
                if cid:
                    names[cid] = f"{first} {last}".strip() or f"Customer {cid[:8]}…"
    except Exception:
        pass
    return names


def load_cases(limit: int = 5) -> List[Dict[str, Any]]:
    """Load up to `limit` cases from enriched_cases.jsonl, one per customer (no duplicate customer_id).
    Attach customer_name when available."""
    # Read more lines so we have enough to deduplicate by customer_id
    read_limit = max(limit * 3, 20)
    raw_cases: List[Dict[str, Any]] = []
    with ENRICHED_CASES_PATH.open("r") as f:
        for i, line in enumerate(f):
            if i >= read_limit:
                break
            line = line.strip()
            if line:
                raw_cases.append(json.loads(line))

    # Keep first case per customer_id until we have `limit` unique customers
    seen_customer_ids: set = set()
    cases: List[Dict[str, Any]] = []
    for c in raw_cases:
        if len(cases) >= limit:
            break
        cid = c.get("customer_id")
        if cid is not None and cid not in seen_customer_ids:
            seen_customer_ids.add(cid)
            cases.append(c)

    customer_names = _load_customer_names()
    for c in cases:
        cid = c.get("customer_id")
        c["customer_name"] = customer_names.get(cid) if cid else None
    return cases


# Order of graph nodes for progress messages (must match build_graph.py)
_STEP_ORDER = ("tool1_risk", "tool2_behavior", "policy_engine", "llm_justify", "validate")
_STEP_MESSAGES = {
    "tool1_risk": "Calculating risk signals…",
    "tool2_behavior": "Extracting behavior…",
    "policy_engine": "Running policy…",
    "llm_justify": "Generating justification…",
    "validate": "Validating output…",
}


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


def _run_agent_stream(enriched_case: Dict[str, Any]):
    """Generator: yield SSE progress events then final result. Uses graph.stream(stream_mode='values')."""
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

    step_list = list(_STEP_ORDER)
    try:
        for i, state_snapshot in enumerate(graph.stream(state, stream_mode="values")):
            step = step_list[i] if i < len(step_list) else "validate"
            msg = _STEP_MESSAGES.get(step, "Running…")
            yield f"data: {json.dumps({'type': 'progress', 'step': step, 'message': msg, 'index': i + 1, 'total': len(step_list)})}\n\n"
            time.sleep(PROGRESS_STEP_DELAY_SECONDS)

        final = dict(state_snapshot)
        result = {
            "case_id": enriched_case.get("case_id"),
            "customer_id": enriched_case.get("customer_id"),
            "validation_ok": final.get("validation_ok", False),
            "validation_errors": final.get("validation_errors", []),
            "policy_decision": final.get("policy_decision"),
            "llm_justification": final.get("llm_justification"),
            "llm_justification_meta": final.get("llm_justification_meta"),
            "risk_signals": final.get("risk_signals"),
            "behavior_signals": final.get("behavior_signals"),
        }
        yield f"data: {json.dumps({'type': 'done', 'result': result})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'detail': str(e)})}\n\n"


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
            "llm_justification_meta": final_state.get("llm_justification_meta"),
            "risk_signals": final_state.get("risk_signals"),
            "behavior_signals": final_state.get("behavior_signals"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run-stream")
def run_case_stream(req: RunRequest):
    """Stream progress events (SSE) then final result. For UI progress updates."""
    return StreamingResponse(
        _run_agent_stream(req.enriched_case),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
