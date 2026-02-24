# Notes: Python Imports, Working Directory & Running Scripts

A summary of what we learned so you avoid the same mistakes.

---

## 1. The Problem: `ModuleNotFoundError: No module named 'agent'`

### What happened
You ran:
```bash
python agent/graph/build_graph.py
# or
python build_graph.py
```

And got: **`ModuleNotFoundError: No module named 'agent'`**

### Why it happened

Your code uses **absolute imports**:
```python
from agent.graph.state import AgentState
from agent.graph.node import node_calculate_risk_signals
```

Python looks for a top-level package named `agent`. It searches in directories listed in **`sys.path`**.

**Key rule:** When you run `python path/to/script.py`, Python adds **the directory containing the script** to `sys.path`, NOT the project root.

| How you ran it | Directory added to sys.path | Can Python find `agent`? |
|----------------|----------------------------|---------------------------|
| `python agent/graph/build_graph.py` | `agent/graph/` | ❌ No – no `agent` package there |
| `python build_graph.py` (from agent/graph/) | `agent/graph/` | ❌ No |
| `PYTHONPATH=. python agent/runners/run_langgraph_agent.py` (from project root) | project root (because of `PYTHONPATH=.`) | ✅ Yes |

The project root is where the `agent` folder lives. Python must have that directory on its search path to resolve `from agent...`.

---

## 2. The Fix: `cd` + `PYTHONPATH=.`

### Correct way to run

```bash
cd "/Users/ishaangupta/PycharmProjects/AML_Investigation_Agent "
PYTHONPATH=. python agent/runners/run_langgraph_agent.py
```

### Why each part matters

| Command part | Purpose |
|--------------|---------|
| `cd "..."` | Sets the **current working directory (cwd)** to the project root |
| `PYTHONPATH=.` | Adds the **current directory** (`.`) to Python's **import search path** |
| `python agent/runners/...` | Runs the script |

- **`cd`** → affects where **file paths** resolve (e.g. `Generate_Data/enriched_cases/enriched_cases.jsonl`)
- **`PYTHONPATH=.`** → affects where Python looks for **imports** (e.g. `from agent.graph...`)

Both use the same directory (project root), but for different jobs.

---

## 3. Example: Why file paths need `cd`

Your script does something like:

```python
BASE_DIR = Path(__file__).resolve().parents[2]  # project root
ENRICHED_CASES_PATH = BASE_DIR / "Generate_Data" / "enriched_cases" / "enriched_cases.jsonl"
```

Here the path is computed from `__file__`, so it works regardless of `cd`. But many scripts use:

```python
# BAD if cwd is wrong
with open("Generate_Data/enriched_cases/enriched_cases.jsonl") as f:
    ...
```

Or:

```python
# Also depends on cwd
Path("policies/policy_v1.json").exists()
```

If you don't `cd` to the project root, these relative paths point to the wrong place and you get `FileNotFoundError`.

---

## 4. Example: Why imports need PYTHONPATH

Project structure:

```
AML_Investigation_Agent /          ← project root (must be in sys.path)
├── agent/
│   ├── __init__.py
│   ├── graph/
│   │   ├── build_graph.py
│   │   └── state.py
│   └── runners/
│       └── run_langgraph_agent.py
```

When you run `python agent/runners/run_langgraph_agent.py`:

- Without `PYTHONPATH=.`: `sys.path[0]` = `agent/runners/`. Python looks for `agent` there and fails.
- With `PYTHONPATH=.`: current dir (project root) is in `sys.path`. Python finds `agent/` and imports work.

---

## 5. Bonus: Missing packages (`ModuleNotFoundError: No module named 'langgraph'`)

Different error: the package is not installed.

**Fix:**
```bash
pip install langgraph
# or, if you need the full stack:
pip install langgraph langchain langchain-openai
```

Always install dependencies before running. A `requirements.txt` helps:

```text
langgraph
langchain
langchain-openai
```

Install with: `pip install -r requirements.txt`

---

## 6. Cheat sheet: How to run this project

```bash
# 1. Go to project root
cd "/Users/ishaangupta/PycharmProjects/AML_Investigation_Agent "

# 2. Install deps (once)
pip install langgraph langchain langchain-openai

# 3. Run the agent
PYTHONPATH=. python agent/runners/run_langgraph_agent.py
```

---

## 7. Alternative: Run as a module

Instead of `PYTHONPATH=.`:

```bash
cd "/Users/ishaangupta/PycharmProjects/AML_Investigation_Agent "
python -m agent.runners.run_langgraph_agent
```

Using `python -m` adds the current directory to `sys.path`, so `agent` is found. The `-m` flag runs a module by its package path.

---

## Summary table

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Wrong cwd | `FileNotFoundError`, missing data files | `cd` to project root before running |
| Wrong import path | `ModuleNotFoundError: No module named 'agent'` | `PYTHONPATH=.` or `python -m` |
| Package not installed | `ModuleNotFoundError: No module named 'langgraph'` | `pip install <package>` |
