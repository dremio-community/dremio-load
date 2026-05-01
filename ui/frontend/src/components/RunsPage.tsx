import { useEffect, useState } from 'react'
import { RefreshCw, CheckCircle, XCircle, Clock } from 'lucide-react'
import { getRuns, getJobs, type Run, type Job } from '../api/client'

function StatusIcon({ status }: { status: string }) {
  if (status === 'ok') return <CheckCircle size={14} color="var(--status-success)" />
  if (status === 'error') return <XCircle size={14} color="var(--status-error)" />
  return <Clock size={14} color="var(--status-warning)" />
}

function duration(s: number | null | undefined) {
  if (s == null) return '—'
  if (s < 60) return `${s.toFixed(1)}s`
  return `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`
}

export default function RunsPage() {
  const [runs, setRuns] = useState<Run[]>([])
  const [jobs, setJobs] = useState<Job[]>([])
  const [filterJob, setFilterJob] = useState('')
  const [loading, setLoading] = useState(true)

  const load = async () => {
    try {
      const [r, j] = await Promise.all([getRuns(filterJob || undefined, 200), getJobs()])
      setRuns(r); setJobs(j)
    } finally { setLoading(false) }
  }

  useEffect(() => { load(); const iv = setInterval(load, 5000); return () => clearInterval(iv) }, [filterJob])

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: 'var(--foreground)' }}>Run History</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <select
            value={filterJob}
            onChange={e => setFilterJob(e.target.value)}
            style={sel}
          >
            <option value="">All jobs</option>
            {jobs.map(j => <option key={j.id} value={j.id}>{j.name}</option>)}
          </select>
          <button onClick={load} style={btnGhost}><RefreshCw size={15} /></button>
        </div>
      </div>

      {loading ? (
        <div style={{ color: 'var(--muted-foreground)', textAlign: 'center', paddingTop: 60 }}>Loading…</div>
      ) : runs.length === 0 ? (
        <div style={{ color: 'var(--muted-foreground)', textAlign: 'center', paddingTop: 60 }}>No runs yet.</div>
      ) : (
        <div style={{ background: 'var(--card)', borderRadius: 10, border: '1px solid var(--border)', overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)', background: 'var(--muted)' }}>
                {['Status', 'Job', 'Table', 'Rows', 'Duration', 'Started', 'Error'].map(h => (
                  <th key={h} style={th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {runs.map((r, i) => (
                <tr key={r.id} style={{ borderBottom: i < runs.length - 1 ? '1px solid var(--border)' : 'none' }}>
                  <td style={td}><StatusIcon status={r.status} /></td>
                  <td style={{ ...td, color: 'var(--foreground)', fontWeight: 500 }}>
                    {jobs.find(j => j.id === r.job_id)?.name ?? r.job_id}
                  </td>
                  <td style={{ ...td, fontFamily: 'monospace', fontSize: 12, color: 'var(--accent)' }}>{r.table_name || '—'}</td>
                  <td style={td}>{r.rows?.toLocaleString() ?? 0}</td>
                  <td style={td}>{duration(r.duration_s)}</td>
                  <td style={td}>
                    {r.started_at ? new Date(r.started_at).toLocaleString() : '—'}
                  </td>
                  <td style={{ ...td, color: 'var(--status-error)', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {r.error || '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

const th: React.CSSProperties = {
  padding: '10px 14px', textAlign: 'left', fontSize: 11, fontWeight: 600,
  color: 'var(--muted-foreground)', textTransform: 'uppercase', letterSpacing: '0.05em',
}
const td: React.CSSProperties = {
  padding: '11px 14px', fontSize: 13, color: 'var(--secondary-foreground)',
}
const sel: React.CSSProperties = {
  background: '#fff', border: '1px solid var(--border)', borderRadius: 6,
  padding: '6px 10px', color: 'var(--foreground)', fontSize: 13, outline: 'none',
}
const btnGhost: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center',
  padding: '7px 10px', borderRadius: 6, border: '1px solid var(--border)', cursor: 'pointer',
  background: 'transparent', color: 'var(--secondary-foreground)', fontSize: 13,
}
