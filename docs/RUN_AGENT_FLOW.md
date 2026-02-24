# Run Agent flow: Frontend → API → Graph → LLM

## 1. Frontend (Run Agent click)

- **What runs:** `runAgent()` in `frontend/src/App.tsx`
- **Request:** `POST http://localhost:8000/api/run`
- **Body:** `{ "enriched_case": <selected case> }`  
  The selected case is the full object from `/api/cases` (case_id, customer_id, customer_snapshot, case_metadata, alerts_in_case, flagged_transactions, behavior_snapshot).

---

## 2. Backend (FastAPI)

- **Endpoint:** `POST /api/run` in `api/main.py`
- **What runs:** `run_case(req)` → `run_agent_on_case(req.enriched_case)`
- **Data passed to graph:** A single dict: `{ "enriched_case": <that same case> }`. Nothing else is sent to the graph at this stage.

---

## 3. Graph invocation

- **Where:** `run_agent_on_case()` builds the graph and calls `graph.invoke(state)`.
- **Initial state:** `state = {"enriched_case": enriched_case}`.
- **Graph steps:**
  1. **tool1_risk** – reads `enriched_case`, writes `risk_signals`.
  2. **tool2_behavior** – reads `enriched_case`, writes `behavior_signals`.
  3. **policy_engine** – reads `enriched_case`, writes `policy_decision` (case_id, decision, reasons, debug_signals, etc.).
  4. **llm_justify** – reads `policy_decision`, calls LM Studio, writes `llm_justification` and `llm_justification_meta`.
  5. **validate** – reads `policy_decision` and `llm_justification`, writes `validation_ok` and `validation_errors`.

After each node, LangGraph **merges** that node’s return value into the state. Only keys that exist on `AgentState` are kept; any other keys (e.g. `llm_justification_meta`) can be dropped if not declared.

---

## 4. Where the LLM is called

- **File:** `agent/graph/node_llm_justification.py`
- **Input:** `state` must contain `state["policy_decision"]` (from the policy_engine node).
- **Steps:**
  1. Build messages from `policy_decision` + system prompt.
  2. `client.chat.completions.create(...)` with `base_url=LM_STUDIO_BASE_URL` (LM Studio).
  3. Parse JSON from `response.choices[0].message.content`.
  4. `validate_justification(parsed)` – checks required keys.
  5. `validate_against_debug_signals(...)` – guardrail (must reference reasons, etc.).
- **On success:** return dict with `llm_justification` (dict) and `llm_justification_meta`.
- **On failure:** same dict but `llm_justification=None` and `llm_justification_meta.error` set.

If any step raises (e.g. API error, invalid JSON, validation/guardrail), the node catches it and returns `llm_justification=None` with the error in `llm_justification_meta`.

---

## 5. Back to API → Frontend

- **API:** Reads `final_state` from `graph.invoke(state)` and returns `llm_justification`, `llm_justification_meta`, etc.
- **Frontend:** Shows “LLM Justification” using `output.llm_justification`; if null, shows `output.llm_justification_meta?.error`.

---

## Likely reasons “no justification” appears

1. **`llm_justification_meta` not in AgentState** – Graph drops it when merging, so API and frontend never see the error message.
2. **LM Studio request differs from test** – e.g. `response_format={"type": "json_object"}` unsupported or model returns non-JSON.
3. **Validation/guardrail fails** – Model output missing a key or failing `validate_against_debug_signals` (e.g. “LLM did not reference matched policy reasons”).

Fix (1) by adding `llm_justification_meta` (and correct type for `llm_justification`) to `AgentState` and returning only the delta from the LLM node. Then check the frontend error message; if it’s still empty, add a fallback or log the exception in the node.
