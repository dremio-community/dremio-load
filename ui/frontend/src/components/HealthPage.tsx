import { useEffect, useState } from 'react'
import { RefreshCw, CheckCircle, XCircle, AlertTriangle, Clock } from 'lucide-react'
import { getHealthSummary } from '../api/client'

interface JobHealth {
  id: string
  name: string
  source_type: string
  schedule?: string
  enabled: boolean
  health: 'healthy' | 'degraded' | 'failing' | 'never_run'
  success_rate?: number
  total_runs: number
  total_rows: number
  avg_duration_s?: number
  last_run?: { status: string; started_at?: string; rows?: number; finished_at?: string }
  recent_errors: string[]
}

interface HealthSummary {
  total_jobs: number
  healthy: number
  degraded: number
  failing: number
  never_run: number
  jobs: JobHealth[]
}

const HEALTH_CONFIG = {
  healthy:   { color: '#34d399', bg: '#064e3b', icon: CheckCircle,    label: 'Healthy'   },
  degraded:  { color: '#fbbf24', bg: '#451a03', icon: AlertTriangle,  label: 'Degraded'  },
  failing:   { color: '#f87171', bg: '#450a0a', icon: XCircle,        label: 'Failing'   },
  never_run: { color: '#94a3b8', bg: '#1e293b', icon: Clock,          label: 'Never Run' },
}

export default function HealthPage() {
  const [summary, setSummary] = useState<HealthSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState<string>('all')

  const load = async () => {
    setLoading(true); setError(null)
    try { setSummary(await getHealthSummary()) }
    catch (e: any) { setError(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  if (loading) return <div style={centerMsg}>Loading health data…</div>
  if (error) return <div style={{ ...centerMsg, color: '#f87171' }}>{error}</div>
  if (!summary) return null

  const filtered = filter === 'all' ? summary.jobs : summary.jobs.filter(j => j.health === filter)

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>Load Health</h1>
        <button onClick={load} style={btnSecondary}>
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      {/* Summary stat cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 20 }}>
        {(['healthy', 'degraded', 'failing', 'never_run'] as const).map(key => {
          const { color, bg, icon: Icon, label } = HEALTH_CONFIG[key]
          const count = summary[key]
          return (
            <button
              key={key}
              onClick={() => setFilter(filter === key ? 'all' : key)}
              style={{
                padding: '14px 16px', borderRadius: 10, border: `2px solid ${filter === key ? color : '#334155'}`,
                background: filter === key ? bg : '#1e293b', cursor: 'pointer', textAlign: 'left',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                <Icon size={16} color={color} />
                <span style={{ fontSize: 12, color, fontWeight: 600 }}>{label}</span>
              </div>
              <div style={{ fontSize: 28, fontWeight: 700, color }}>{count}</div>
              <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>of {summary.total_jobs} jobs</div>
            </button>
          )
        })}
      </div>

      {/* Job health table */}
      <div style={card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              {['Job', 'Health', 'Success Rate', 'Total Runs', 'Rows Written', 'Avg Duration', 'Last Run'].map(h => (
                <th key={h} style={th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((job, i) => {
              const { color, bg, icon: Icon, label } = HEALTH_CONFIG[job.health]
              return (
                <tr key={job.id} style={{ background: i % 2 === 0 ? 'transparent' : '#0f172a11' }}>
                  <td style={td}>
                    <div style={{ fontWeight: 600, color: '#e2e8f0', fontSize: 13 }}>{job.name}</div>
                    {job.schedule && <div style={{ fontSize: 11, color: '#64748b' }}>{job.schedule}</div>}
                    {!job.enabled && <span style={{ fontSize: 10, color: '#64748b', background: '#1e293b', padding: '1px 5px', borderRadius: 3 }}>disabled</span>}
                  </td>
                  <td style={td}>
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5, padding: '3px 8px', borderRadius: 5, background: bg, color, fontSize: 12, fontWeight: 600 }}>
                      <Icon size={12} /> {label}
                    </span>
                  </td>
                  <td style={td}>
                    {job.success_rate !== undefined ? (
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: job.success_rate >= 0.9 ? '#34d399' : job.success_rate >= 0.7 ? '#fbbf24' : '#f87171' }}>
                          {Math.round(job.success_rate * 100)}%
                        </div>
                        <ProgressBar value={job.success_rate} />
                      </div>
                    ) : <span style={{ color: '#64748b' }}>—</span>}
                  </td>
                  <td style={{ ...td, color: '#e2e8f0' }}>{job.total_runs}</td>
                  <td style={{ ...td, color: '#e2e8f0' }}>{job.total_rows > 0 ? fmtNum(job.total_rows) : '—'}</td>
                  <td style={{ ...td, color: '#e2e8f0' }}>{job.avg_duration_s != null ? `${job.avg_duration_s}s` : '—'}</td>
                  <td style={td}>
                    {job.last_run ? (
                      <div>
                        <span style={{ fontSize: 12, color: job.last_run.status === 'success' ? '#34d399' : '#f87171' }}>
                          {job.last_run.status}
                        </span>
                        {job.last_run.started_at && (
                          <div style={{ fontSize: 11, color: '#64748b' }}>{fmtTime(job.last_run.started_at)}</div>
                        )}
                      </div>
                    ) : <span style={{ color: '#64748b' }}>—</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div style={{ padding: '24px', textAlign: 'center', color: '#64748b', fontSize: 14 }}>
            No jobs match this filter.
          </div>
        )}
      </div>

      {/* Recent errors */}
      {summary.jobs.some(j => j.recent_errors.length > 0) && (
        <div style={{ marginTop: 16 }}>
          <h2 style={{ margin: '0 0 10px', fontSize: 14, fontWeight: 600, color: '#94a3b8' }}>Recent Errors</h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {summary.jobs.filter(j => j.recent_errors.length > 0).map(job => (
              <div key={job.id} style={{ ...card, borderColor: '#450a0a' }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: '#f87171', marginBottom: 6 }}>{job.name}</div>
                {job.recent_errors.map((err, i) => (
                  <div key={i} style={{ fontSize: 12, color: '#fca5a5', marginBottom: 4, opacity: 1 - i * 0.25 }}>
                    {err}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function ProgressBar({ value }: { value: number }) {
  const color = value >= 0.9 ? '#34d399' : value >= 0.7 ? '#fbbf24' : '#f87171'
  return (
    <div style={{ marginTop: 4, height: 4, background: '#334155', borderRadius: 2, width: 80 }}>
      <div style={{ height: '100%', width: `${value * 100}%`, background: color, borderRadius: 2 }} />
    </div>
  )
}

function fmtNum(n: number) {
  return n >= 1_000_000 ? `${(n / 1_000_000).toFixed(1)}M`
    : n >= 1_000 ? `${(n / 1_000).toFixed(1)}K`
    : String(n)
}

function fmtTime(iso: string) {
  try {
    const d = new Date(iso)
    const now = Date.now()
    const diff = now - d.getTime()
    if (diff < 60_000) return 'just now'
    if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
    if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
    return d.toLocaleDateString()
  } catch { return iso }
}

const card: React.CSSProperties = { background: '#1e293b', borderRadius: 10, border: '1px solid #334155', overflow: 'hidden' }
const th: React.CSSProperties = { padding: '10px 14px', textAlign: 'left', fontSize: 11, fontWeight: 600, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid #334155', whiteSpace: 'nowrap' }
const td: React.CSSProperties = { padding: '12px 14px', borderBottom: '1px solid #1e293b', verticalAlign: 'top' }
const centerMsg: React.CSSProperties = { padding: 40, textAlign: 'center', color: '#64748b', fontSize: 14 }
const btnSecondary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '7px 12px', borderRadius: 7, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#94a3b8', fontSize: 13,
}
