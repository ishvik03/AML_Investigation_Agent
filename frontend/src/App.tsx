import { useState, useEffect } from 'react'
import './App.css'

interface EnrichedCase {
  case_id: string
  customer_id: string
  customer_snapshot?: string
  case_metadata?: string
  alerts_in_case?: string
  flagged_transactions?: string
  behavior_snapshot?: string
}

interface AgentOutput {
  case_id: string
  customer_id: string
  validation_ok: boolean
  validation_errors: string[]
  policy_decision?: {
    decision: string
    confidence: string
    reasons: string[]
    required_next_actions?: string[]
  }
  llm_justification?: string
  risk_signals?: unknown
  behavior_signals?: unknown
}

function App() {
  const [cases, setCases] = useState<EnrichedCase[]>([])
  const [selectedCase, setSelectedCase] = useState<EnrichedCase | null>(null)
  const [loading, setLoading] = useState(false)
  const [loadingCases, setLoadingCases] = useState(true)
  const [output, setOutput] = useState<AgentOutput | null>(null)
  const [error, setError] = useState<string | null>(null)

  const API_BASE = 'http://localhost:8000'

  useEffect(() => {
    fetch(`${API_BASE}/api/cases?limit=5`)
      .then((res) => {
        if (!res.ok) throw new Error('Failed to fetch cases')
        return res.json()
      })
      .then((data) => {
        setCases(data.cases ?? [])
        if (data.cases?.length) setSelectedCase(data.cases[0])
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoadingCases(false))
  }, [])

  const runAgent = async () => {
    if (!selectedCase) return
    setLoading(true)
    setOutput(null)
    setError(null)
    try {
      const res = await fetch(`${API_BASE}/api/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enriched_case: selectedCase }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail ?? res.statusText)
      }
      const data = await res.json()
      setOutput(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="app">
      <header>
        <h1>AML Investigation Agent</h1>
        <p>Select a case from enriched_cases.jsonl and run the LangGraph agent</p>
      </header>

      <section className="case-selector">
        <h2>Select Case</h2>
        {loadingCases ? (
          <p>Loading cases…</p>
        ) : error && !cases.length ? (
          <p className="error">{error}</p>
        ) : (
          <>
            <select
              value={selectedCase?.case_id ?? ''}
              onChange={(e) => {
                const c = cases.find((x) => x.case_id === e.target.value)
                setSelectedCase(c ?? null)
              }}
            >
              {cases.map((c) => (
                <option key={c.case_id} value={c.case_id}>
                  Case: {c.case_id.slice(0, 8)}… | Customer: {c.customer_id.slice(0, 8)}…
                </option>
              ))}
            </select>
            <button onClick={runAgent} disabled={loading || !selectedCase}>
              {loading ? 'Running…' : 'Run Agent'}
            </button>
          </>
        )}
      </section>

      {error && cases.length > 0 && (
        <section className="error-block">
          <strong>Error:</strong> {error}
        </section>
      )}

      {output && (
        <section className="output">
          <h2>Agent Output</h2>
          <div className="output-grid">
            <div className="output-card">
              <h3>Validation</h3>
              <p className={output.validation_ok ? 'success' : 'failure'}>
                {output.validation_ok ? '✓ OK' : '✗ Failed'}
              </p>
              {output.validation_errors?.length > 0 && (
                <ul>
                  {output.validation_errors.map((e, i) => (
                    <li key={i}>{e}</li>
                  ))}
                </ul>
              )}
            </div>
            {output.policy_decision && (
              <div className="output-card">
                <h3>Policy Decision</h3>
                <p><strong>Decision:</strong> {output.policy_decision.decision}</p>
                <p><strong>Confidence:</strong> {output.policy_decision.confidence}</p>
                {output.policy_decision.reasons?.length > 0 && (
                  <>
                    <p><strong>Reasons:</strong></p>
                    <ul>
                      {output.policy_decision.reasons.map((r, i) => (
                        <li key={i}>{r}</li>
                      ))}
                    </ul>
                  </>
                )}
              </div>
            )}
            {output.llm_justification && (
              <div className="output-card full-width">
                <h3>LLM Justification</h3>
                <p>{output.llm_justification}</p>
              </div>
            )}
          </div>
        </section>
      )}
    </div>
  )
}

export default App
