import { useState, useEffect } from 'react'
import './App.css'

interface EnrichedCase {
  case_id: string
  customer_id: string
  customer_name?: string | null
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
  llm_justification?: Record<string, string> | null
  llm_justification_meta?: {
    ok?: boolean
    error?: string
    model?: string
    debug_response?: Record<string, unknown>
    debug_raw_response?: Record<string, unknown>
  } | null
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
  const [progress, setProgress] = useState<{ message: string; index: number; total: number } | null>(null)

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
    setProgress({ message: 'Starting…', index: 0, total: 5 })
    try {
      const res = await fetch(`${API_BASE}/api/run-stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enriched_case: selectedCase }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail ?? res.statusText)
      }
      const reader = res.body?.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      if (!reader) throw new Error('No response body')
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6))
              if (data.type === 'progress') {
                setProgress({
                  message: data.message ?? 'Running…',
                  index: data.index ?? 0,
                  total: data.total ?? 5,
                })
              } else if (data.type === 'done') {
                setOutput(data.result)
                setProgress(null)
              } else if (data.type === 'error') {
                throw new Error(data.detail ?? 'Stream error')
              }
            } catch (e) {
              if (e instanceof SyntaxError) continue
              throw e
            }
          }
        }
      }
      setProgress(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      setProgress(null)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="app">
      <header className="hero-header">
        <h1 className="hero-title">
          AML Investigation <span className="hero-title-accent">Agent</span>
        </h1>
        <p className="hero-subtitle">
          Assess risk from payments and behavior data with an agentic, automatic AML compliance agent—confidence-driven and audit-ready.
        </p>
      </header>

      <section className="case-selector">
        <h2>Select Case</h2>
        {loadingCases ? (
          <p className="case-selector-loading">Loading cases…</p>
        ) : error && !cases.length ? (
          <p className="error">{error}</p>
        ) : (
          <div className="case-selector-wrap">
            <label htmlFor="case-select" className="case-selector-label">
              Customer / Case
            </label>
            <div className="case-selector-row">
            <select
              id="case-select"
              className="case-select"
              value={selectedCase?.case_id ?? ''}
              onChange={(e) => {
                const c = cases.find((x) => x.case_id === e.target.value)
                setSelectedCase(c ?? null)
              }}
            >
              {cases.map((c) => {
                const displayName = c.customer_name || `Customer ${c.customer_id.slice(0, 8)}…`
                const caseShort = c.case_id.slice(0, 8)
                return (
                  <option key={c.case_id} value={c.case_id}>
                    {displayName} — Case {caseShort}…
                  </option>
                )
              })}
            </select>
            <button className="run-agent-btn" onClick={runAgent} disabled={loading || !selectedCase}>
              {loading ? 'Running…' : 'Run Agent'}
            </button>
            </div>
          </div>
        )}
      </section>

      {!output && !loading && (
        <section className="pitch-section" aria-label="Why and how">
          <div className="pitch-grid">
            <div className="pitch-card">
              <h3 className="pitch-heading">Why investigative automation</h3>
              <ul className="pitch-list">
                <li>Massive alert queues and pending tasks</li>
                <li>Rising regulatory pressure</li>
                <li>Expensive manual investigations</li>
              </ul>
            </div>
            <div className="pitch-card">
              <h3 className="pitch-heading">Problems we solve</h3>
              <ul className="pitch-list">
                <li>Transaction monitoring</li>
                <li>Alert backlogs</li>
                <li>Investigator workload</li>
                <li>Policy inconsistency</li>
                <li>Regulatory reporting</li>
              </ul>
            </div>
            <div className="pitch-card">
              <h3 className="pitch-heading">Who it helps</h3>
              <ul className="pitch-list">
                <li>OCC-regulated banks</li>
                <li>Payroll platforms</li>
                <li>Global exchanges</li>
              </ul>
            </div>
          </div>
          <div className="pitch-flow">
            <h3 className="pitch-heading">How it works</h3>
            <div className="flow-steps">
              <span className="flow-step">Risk signals</span>
              <span className="flow-arrow" aria-hidden>→</span>
              <span className="flow-step">Behavior signals</span>
              <span className="flow-arrow" aria-hidden>→</span>
              <span className="flow-step">Policy engine</span>
              <span className="flow-arrow" aria-hidden>→</span>
              <span className="flow-step">LLM justification</span>
              <span className="flow-arrow" aria-hidden>→</span>
              <span className="flow-step">Validation & case filing</span>
            </div>
          </div>
        </section>
      )}

      {loading && progress && (
        <section className="progress-section" aria-live="polite">
          <div className="progress-card">
            <div className="progress-spinner" aria-hidden />
            <div className="progress-text">
              <span className="progress-message">{progress.message}</span>
              <span className="progress-step">Step {progress.index}/{progress.total}</span>
            </div>
          </div>
        </section>
      )}

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
            <div className="output-card full-width">
              <h3>LLM Justification</h3>
              {output.llm_justification ? (
                typeof output.llm_justification === "object" ? (
                  <div className="llm-justification-fields">
                    {Object.entries(output.llm_justification).map(([key, val]) => (
                      <div key={key}>
                        <strong>{key.replace(/_/g, " ")}:</strong> {String(val)}
                      </div>
                    ))}
                  </div>
                ) : (
                  <p>{String(output.llm_justification)}</p>
                )
              ) : (
                <>
                  <p className="failure">
                    {output.llm_justification_meta?.error
                      ? `LLM failed: ${output.llm_justification_meta.error}`
                      : "No justification returned (LLM may have failed or was skipped)."}
                  </p>
                  {(output.llm_justification_meta?.debug_raw_response ?? output.llm_justification_meta?.debug_response) && (
                    <div className="debug-response">
                      <strong>Debug — raw LLM response:</strong>
                      <pre>
                        {JSON.stringify(
                          output.llm_justification_meta?.debug_raw_response ?? output.llm_justification_meta?.debug_response,
                          null,
                          2
                        )}
                      </pre>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </section>
      )}
    </div>
  )
}

export default App
