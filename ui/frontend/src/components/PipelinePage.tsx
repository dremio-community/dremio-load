import { useEffect, useState } from 'react'
import { RefreshCw, ArrowRight, CheckCircle, XCircle, AlertCircle, Clock } from 'lucide-react'
import { getPipelineOverview } from '../api/client'

interface JobOverview {
  id: string
  name: string
  source_type: string
  source_label: string
  tables: string[]
  load_mode: string
  schedule?: string
  enabled: boolean
  target_host: string
  target_catalog: string
  target_schema: string
  target_mode: string
  target_table: string
  last_run?: { status: string; started_at?: string; rows?: number; error?: string }
  success_rate?: number
  total_runs: number
}

const SOURCE_COLORS: Record<string, string> = {
  s3: '#f59e0b', azure_blob: '#3b82f6', gcs: '#10b981',
  postgres: '#6366f1', mysql: '#f97316', sqlserver: '#dc2626',
  oracle: '#ef4444', mongodb: '#22c55e', snowflake: '#06b6d4',
  databricks: '#8b5cf6', copy_into: '#0ea5e9',
}

export default function PipelinePage() {
  const [jobs, setJobs] = useState<JobOverview[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = async () => {
    setLoading(true); setError(null)
    try { setJobs(await getPipelineOverview()) }
    catch (e: any) { setError(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  if (loading) return <div style={centerMsg}>Loading pipeline overview…</div>
  if (error) return <div style={{ ...centerMsg, color: '#f87171' }}>{error}</div>

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>Pipeline Overview</h1>
        <button onClick={load} style={btnSecondary}>
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      {jobs.length === 0 ? (
        <div style={emptyState}>No jobs configured yet.</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {jobs.map(job => (
            <PipelineCard key={job.id} job={job} />
          ))}
        </div>
      )}
    </div>
  )
}

function PipelineCard({ job }: { job: JobOverview }) {
  const srcColor = SOURCE_COLORS[job.source_type] ?? '#64748b'
  const lastStatus = job.last_run?.status
  const modeLabel = job.target_mode === 'b' ? 'PyIceberg Direct' : 'Dremio SQL'

  return (
    <div style={card}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        {/* Source block */}
        <div style={{ ...block, borderColor: srcColor, minWidth: 160 }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: srcColor, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>
            Source
          </div>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>{job.source_label}</div>
          {job.tables.length > 0 && (
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 3 }}>
              {job.tables.length === 1 ? job.tables[0] : `${job.tables.length} tables`}
            </div>
          )}
        </div>

        <ArrowRight size={16} color="#334155" />

        {/* Job block */}
        <div style={{ ...block, borderColor: '#4f46e5', flex: 1 }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#818cf8', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>
            Job
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>{job.name}</span>
            {!job.enabled && (
              <span style={badge('#334155', '#64748b')}>Disabled</span>
            )}
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginTop: 3 }}>
            {job.load_mode} {job.schedule ? `· ${job.schedule}` : '· manual'}
          </div>
        </div>

        <ArrowRight size={16} color="#334155" />

        {/* Target block */}
        <div style={{ ...block, borderColor: '#0ea5e9', minWidth: 200 }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#38bdf8', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>
            Target · {modeLabel}
          </div>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>{job.target_host}</div>
          <div style={{ fontSize: 11, color: '#64748b', marginTop: 3 }}>{job.target_table}</div>
        </div>

        {/* Status */}
        <div style={{ minWidth: 110, textAlign: 'right' }}>
          <StatusBadge status={lastStatus} />
          {job.total_runs > 0 && (
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
              {job.success_rate !== undefined ? `${Math.round(job.success_rate * 100)}%` : '—'} success · {job.total_runs} runs
            </div>
          )}
          {!job.total_runs && (
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>Never run</div>
          )}
        </div>
      </div>

      {job.last_run?.error && (
        <div style={{ marginTop: 10, padding: '8px 12px', borderRadius: 6, background: '#450a0a', color: '#fca5a5', fontSize: 12 }}>
          {job.last_run.error}
        </div>
      )}
    </div>
  )
}

function StatusBadge({ status }: { status?: string }) {
  if (!status) return <span style={badge('#1e293b', '#64748b')}>—</span>
  if (status === 'success') return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, ...badgeStyle('#064e3b', '#34d399') }}>
      <CheckCircle size={11} /> Success
    </span>
  )
  if (status === 'error') return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, ...badgeStyle('#450a0a', '#f87171') }}>
      <XCircle size={11} /> Error
    </span>
  )
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, ...badgeStyle('#1c1917', '#a3a3a3') }}>
      <Clock size={11} /> {status}
    </span>
  )
}

function badge(bg: string, color: string): React.CSSProperties {
  return { display: 'inline-block', padding: '2px 7px', borderRadius: 4, fontSize: 11, fontWeight: 600, background: bg, color }
}
function badgeStyle(bg: string, color: string): React.CSSProperties {
  return { padding: '3px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600, background: bg, color }
}

const card: React.CSSProperties = {
  background: '#1e293b', borderRadius: 10, padding: '14px 16px', border: '1px solid #334155',
}
const block: React.CSSProperties = {
  padding: '10px 12px', borderRadius: 8, border: '1px solid', background: '#0f172a',
}
const centerMsg: React.CSSProperties = {
  padding: 40, textAlign: 'center', color: '#64748b', fontSize: 14,
}
const emptyState: React.CSSProperties = {
  padding: '40px', textAlign: 'center', color: '#64748b', fontSize: 14,
  background: '#1e293b', borderRadius: 10, border: '1px solid #334155',
}
const btnSecondary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '7px 12px', borderRadius: 7, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#94a3b8', fontSize: 13,
}
