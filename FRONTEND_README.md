# AML Investigation Agent - Frontend

TypeScript/React frontend to run the LangGraph agent on selected cases.

## Quick Start

### 1. Install dependencies

**Backend (Python):**
```bash
cd "/Users/ishaangupta/PycharmProjects/AML_Investigation_Agent "
pip install -r api/requirements.txt
# Also ensure: langgraph, langchain
pip install langgraph langchain
```

**Frontend:**
```bash
cd frontend
npm install
```

### 2. Run both servers

**Terminal 1 - Backend:**
```bash
cd "/Users/ishaangupta/PycharmProjects/AML_Investigation_Agent "
PYTHONPATH=. python api/main.py
```
Backend runs at http://localhost:8000

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
```
Frontend runs at http://localhost:5173

### 3. Use the app

1. Open http://localhost:5173
2. Pick one of the 5 cases from the dropdown
3. Click "Run Agent"
4. View the output (validation, policy decision, LLM justification)
