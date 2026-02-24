
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

from agent.graph.build_graph import build_aml_graph
from agent.policy_engine.policy_engine import PolicyEngine


BASE_DIR = Path(__file__).resolve().parents[2]  # AML_Investigation_Agent/

ENRICHED_CASES_PATH = BASE_DIR / "Generate_Data" / "enriched_cases" / "enriched_cases.jsonl"
POLICY_PATH = BASE_DIR / "policies" / "policy_v1.json"

# outputs
OUT_DIR = BASE_DIR / "agent" / "eval_results"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LANGGRAPH_OUTPUT_PATH = OUT_DIR / "langgraph_agent_outputs.jsonl"


def load_jsonl(path: Path):
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main():
    if not ENRICHED_CASES_PATH.exists():
        raise FileNotFoundError(f"Missing: {ENRICHED_CASES_PATH}")
    if not POLICY_PATH.exists():
        raise FileNotFoundError(f"Missing: {POLICY_PATH}")


    # IMPORTANT:
    # PolicyEngine writes its own audit/decision files.
    # For the LangGraph run, you can either:
    # - pass dummy paths, OR
    # - use separate files for this run.
    policy_engine = PolicyEngine(
        policy_path=POLICY_PATH,
        audit_log_path=OUT_DIR / "policy_audit_langgraph.jsonl",
        decision_output_path=OUT_DIR / "policy_decisions_langgraph.jsonl",
        append_outputs=True,
    )

    graph = build_aml_graph(policy_engine)

    total = 0
    ok = 0

    with LANGGRAPH_OUTPUT_PATH.open("w") as out:
        for enriched_case in load_jsonl(ENRICHED_CASES_PATH):
            total += 1

            # initial state
            state: Dict[str, Any] = {
                "enriched_case": enriched_case
            }

            final_state = graph.invoke(state)

            record = {
                "case_id": enriched_case.get("case_id"),
                "customer_id": enriched_case.get("customer_id"),
                "validation_ok": final_state.get("validation_ok", False),
                "validation_errors": final_state.get("validation_errors", []),
                "policy_decision": final_state.get("policy_decision"),
                "llm_justification": final_state.get("llm_justification"),
            }

            if record["validation_ok"]:
                ok += 1

            out.write(json.dumps(record) + "\n")

    print("\n================ LANGGRAPH RUN COMPLETE ================\n")
    print(f"Input cases: {total}")
    print(f"Valid outputs: {ok}")
    print(f"Saved: {LANGGRAPH_OUTPUT_PATH}")
    print(f"Policy audit: {OUT_DIR / 'policy_audit_langgraph.jsonl'}")
    print(f"Policy decisions: {OUT_DIR / 'policy_decisions_langgraph.jsonl'}")
    print("\n========================================================\n")


if __name__ == "__main__":
    main()