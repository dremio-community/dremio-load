import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowRight, CheckCircle, XCircle, Clock, Play, Loader2,
  ChevronLeft, Database, Table2, AlertCircle, RefreshCw, Pencil,
} from 'lucide-react'
import { getJob, getJobRuns, getTarget, getDremioPreview, triggerJob } from '../api/client'

const SOURCE_LABELS: Record<string, string> = {
  google_ads: 'Google Ads', linkedin_ads: 'LinkedIn Ads',
  s3: 'Amazon S3', gcs: 'Google Cloud Storage', azure_blob: 'Azure Blob',
  adls: 'Azure Data Lake', postgres: 'PostgreSQL', mysql: 'MySQL',
  sqlserver: 'SQL Server', oracle: 'Oracle', mongodb: 'MongoDB',
  snowflake: 'Snowflake', dynamodb: 'DynamoDB', cosmosdb: 'Cosmos DB',
  spanner: 'Cloud Spanner', salesforce: 'Salesforce', hubspot: 'HubSpot',
  zendesk: 'Zendesk', cassandra: 'Cassandra', clickhouse: 'ClickHouse',
  pinot: 'Apache Pinot', splunk: 'Splunk', databricks: 'Databricks',
  delta: 'Delta Lake', hudi: 'Apache Hudi', copy_into: 'COPY INTO',
}

const SOURCE_COLORS: Record<string, string> = {
  google_ads: '#4285F4', linkedin_ads: '#0A66C2', s3: '#f59e0b',
  gcs: '#10b981', azure_blob: '#3b82f6', adls: '#3b82f6',
  postgres: '#6366f1', mysql: '#f97316', sqlserver: '#dc2626',
  oracle: '#ef4444', mongodb: '#22c55e', snowflake: '#06b6d4',
  dynamodb: '#f59e0b', cosmosdb: '#3b82f6', spanner: '#10b981',
  salesforce: '#00a1e0', hubspot: '#ff7a59', zendesk: '#03363d',
  cassandra: '#1287b1', clickhouse: '#facc15', pinot: '#7c3aed',
  splunk: '#65a30d', databricks: '#8b5cf6', delta: '#64748b',
}

const SOURCE_EMOJI: Record<string, string> = {
  google_ads: '📢', linkedin_ads: '💼', s3: '🪣', gcs: '☁️',
  azure_blob: '🔷', adls: '🔷', postgres: '🐘', mysql: '🐬',
  sqlserver: '🏢', oracle: '🔴', mongodb: '🍃', snowflake: '❄️',
  dynamodb: '⚡', cosmosdb: '🌌', spanner: '🔧', salesforce: '☁️',
  hubspot: '🟠', zendesk: '🎫', cassandra: '👁️', clickhouse: '🟡',
  pinot: '📊', splunk: '🔍', databricks: '🧱', delta: '△',
}

interface Run {
  id: string; job_id: string; table_name?: string
  status: string; rows?: number; error?: string
  started_at?: string; finished_at?: string; duration_s?: number
}

export default function PipelineDetailPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()
  const [job, setJob] = useState<any>(null)
  const [runs, setRuns] = useState<Run[]>([])
  const [target, setTarget] = useState<any>(null)
  const [preview, setPreview] = useState<{ columns: string[]; rows: any[] } | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState('')
  const [triggering, setTriggering] = useState(false)
  const [loading, setLoading] = useState(true)

  const load = async () => {
    if (!jobId) return
    try {
      const [j, r, t] = await Promise.all([getJob(jobId), getJobRuns(jobId), getTarget()])
      setJob(j); setRuns(r); setTarget(t)
    } catch (e: any) {
      console.error(e)
    } finally { setLoading(false) }
  }

  useEffect(() => { load() }, [jobId])

  const handleRun = async () => {
    if (!jobId) return
    setTriggering(true)
    await triggerJob(jobId).catch(() => {})
    setTimeout(() => { setTriggering(false); load() }, 3000)
  }

  const handlePreview = async () => {
    const tbl = job?.target_table || job?.config?.target_table
    if (!tbl) { setPreviewError('No target table configured'); return }
    setPreviewLoading(true); setPreviewError('')
    try {
      const data = await getDremioPreview(tbl)
      if (data.error) throw new Error(data.error)
      setPreview(data)
    } catch (e: any) { setPreviewError(e.message) }
    finally { setPreviewLoading(false) }
  }

  if (loading) return <div style={centerMsg}>Loading pipeline…</div>
  if (!job) return <div style={{ ...centerMsg, color: '#f87171' }}>Job not found</div>

  const srcType = job.source_type || ''
  const srcColor = SOURCE_COLORS[srcType] || '#64748b'
  const srcLabel = SOURCE_LABELS[srcType] || srcType
  const srcEmoji = SOURCE_EMOJI[srcType] || '📦'
  const tables: string[] = job.tables || []
  const conn: Record<string, string> = job.connection || job.config?.connection || {}
  const targetTable = job.target_table || job.config?.target_table || ''

  // Per-table stats from latest runs
  const tableStats: Record<string, Run> = {}
  for (const r of [...runs].reverse()) {
    if (r.table_name) tableStats[r.table_name] = r
  }

  // Recent runs (deduplicated by run-group: same started_at second)
  const recentRuns = runs.slice(0, 20)
  const runGroups: Record<string, Run[]> = {}
  for (const r of recentRuns) {
    const key = r.started_at?.slice(0, 19) || r.id
    if (!runGroups[key]) runGroups[key] = []
    runGroups[key].push(r)
  }
  const runSummaries = Object.values(runGroups).slice(0, 8).map(group => ({
    started_at: group[0].started_at,
    status: group.every(r => r.status === 'success') ? 'success' :
            group.some(r => r.status === 'error') ? 'error' : group[0].status,
    rows: group.reduce((s, r) => s + (r.rows || 0), 0),
    tables: group.length,
    error: group.find(r => r.error)?.error,
  }))

  const successCount = runSummaries.filter(r => r.status === 'success').length
  const successRate = runSummaries.length ? Math.round((successCount / runSummaries.length) * 100) : null
  const lastRun = runSummaries[0]

  // Mask sensitive connection fields
  const sensitiveKeys = ['password', 'secret', 'token', 'key', 'refresh_token', 'client_secret', 'developer_token']
  const connDisplay = Object.entries(conn)
    .filter(([k]) => !sensitiveKeys.some(s => k.toLowerCase().includes(s)))
    .slice(0, 4)

  return (
    <div style={{ padding: 24, maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <button onClick={() => navigate('/pipeline')} style={btnBack}>
          <ChevronLeft size={15} /> Pipeline
        </button>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#f1f5f9', flex: 1 }}>{job.name}</h1>
        <button onClick={() => navigate('/')} style={btnGhost}><Pencil size={14} /> Edit Job</button>
        <button onClick={handleRun} disabled={triggering} style={btnPrimary}>
          {triggering ? <Loader2 size={14} style={{ animation: 'spin 1s linear infinite' }} /> : <Play size={14} />}
          {triggering ? 'Running…' : 'Run Now'}
        </button>
        <button onClick={load} style={btnGhost}><RefreshCw size={14} /></button>
      </div>

      {/* ── Main pipeline visual ─────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '280px 1fr 280px', gap: 0, alignItems: 'stretch', marginBottom: 24 }}>

        {/* SOURCE */}
        <div style={{ ...panel, borderColor: srcColor }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: srcColor, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 12 }}>
            Source
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
            <div style={{ fontSize: 28 }}>{srcEmoji}</div>
            <div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#f1f5f9' }}>{srcLabel}</div>
              <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>{srcType}</div>
            </div>
          </div>

          {/* Connection summary */}
          {connDisplay.length > 0 && (
            <div style={{ marginBottom: 16, padding: '10px 12px', borderRadius: 8, background: '#0a1120' }}>
              {connDisplay.map(([k, v]) => (
                <div key={k} style={{ display: 'flex', gap: 8, marginBottom: 4, fontSize: 11 }}>
                  <span style={{ color: '#475569', minWidth: 80, flexShrink: 0 }}>{k}</span>
                  <span style={{ color: '#94a3b8', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{String(v)}</span>
                </div>
              ))}
            </div>
          )}

          {/* Tables */}
          <div style={{ fontSize: 10, fontWeight: 600, color: '#475569', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 8 }}>
            Tables ({tables.length})
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {tables.map(t => {
              const stat = tableStats[t]
              const st = stat?.status
              return (
                <div key={t} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 8px', borderRadius: 6, background: '#0a1120' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <Table2 size={11} color="#475569" />
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>{t}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    {stat && <span style={{ fontSize: 11, color: '#64748b' }}>{(stat.rows || 0).toLocaleString()} rows</span>}
                    {st === 'success' && <CheckCircle size={11} color="#34d399" />}
                    {st === 'error'   && <XCircle size={11} color="#f87171" />}
                    {!stat && <span style={{ fontSize: 11, color: '#334155' }}>—</span>}
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* CENTER: Flow + Job stats */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '0 8px' }}>
          {/* Arrows */}
          <div style={{ display: 'flex', alignItems: 'center', width: '100%', marginBottom: 8, marginTop: 40 }}>
            <div style={flowLine} />
            <ArrowRight size={20} color="#334155" style={{ flexShrink: 0 }} />
            <div style={flowLine} />
          </div>

          {/* Job card */}
          <div style={{ ...panel, borderColor: '#4f46e5', width: '100%', flex: 1 }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#818cf8', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 12 }}>
              Load Job
            </div>

            {/* Mode + schedule */}
            <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
              <span style={chip('#1e1b4b', '#818cf8')}>{job.load_mode || 'incremental'}</span>
              <span style={chip('#1e293b', '#64748b')}>{job.schedule || 'manual'}</span>
            </div>

            {/* Success rate bar */}
            {successRate !== null && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                  <span style={{ fontSize: 12, color: '#94a3b8' }}>Success rate</span>
                  <span style={{ fontSize: 12, fontWeight: 700, color: successRate === 100 ? '#34d399' : successRate >= 80 ? '#fbbf24' : '#f87171' }}>
                    {successRate}%
                  </span>
                </div>
                <div style={{ height: 6, borderRadius: 3, background: '#1e293b', overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${successRate}%`, background: successRate === 100 ? '#34d399' : successRate >= 80 ? '#fbbf24' : '#f87171', borderRadius: 3, transition: 'width 0.5s' }} />
                </div>
              </div>
            )}

            {/* Run dots */}
            {runSummaries.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 11, color: '#475569', marginBottom: 8 }}>Recent runs (newest first)</div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {runSummaries.map((r, i) => (
                    <div
                      key={i}
                      title={`${r.started_at?.slice(0, 16).replace('T', ' ')} · ${r.rows} rows${r.error ? ' · ' + r.error : ''}`}
                      style={{
                        width: 28, height: 28, borderRadius: 6,
                        background: r.status === 'success' ? '#064e3b' : r.status === 'error' ? '#450a0a' : '#1e293b',
                        border: `1px solid ${r.status === 'success' ? '#34d399' : r.status === 'error' ? '#f87171' : '#334155'}`,
                        display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'default',
                      }}
                    >
                      {r.status === 'success' ? <CheckCircle size={14} color="#34d399" /> : r.status === 'error' ? <XCircle size={14} color="#f87171" /> : <Clock size={14} color="#64748b" />}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Last run summary */}
            {lastRun && (
              <div style={{ padding: '10px 12px', borderRadius: 8, background: '#0a1120', fontSize: 12 }}>
                <div style={{ color: '#64748b', marginBottom: 4 }}>Last run</div>
                <div style={{ color: '#e2e8f0' }}>{lastRun.started_at?.slice(0, 16).replace('T', ' ')} UTC</div>
                <div style={{ color: '#64748b', marginTop: 2 }}>{lastRun.rows.toLocaleString()} rows written</div>
                {lastRun.error && <div style={{ color: '#f87171', marginTop: 4, fontSize: 11 }}>{lastRun.error}</div>}
              </div>
            )}

            {!runSummaries.length && (
              <div style={{ color: '#334155', fontSize: 13, textAlign: 'center', padding: '20px 0' }}>No runs yet — hit Run Now to start</div>
            )}
          </div>

          {/* Arrow out */}
          <div style={{ display: 'flex', alignItems: 'center', width: '100%', marginTop: 8 }}>
            <div style={flowLine} />
            <ArrowRight size={20} color="#334155" style={{ flexShrink: 0 }} />
            <div style={flowLine} />
          </div>
        </div>

        {/* TARGET */}
        <div style={{ ...panel, borderColor: '#0ea5e9' }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#38bdf8', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 12 }}>
            Target · Dremio SQL
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
            <div style={{ fontSize: 28 }}>🎯</div>
            <div>
              <div style={{ fontSize: 15, fontWeight: 700, color: '#f1f5f9' }}>{target?.host || 'Not configured'}</div>
              <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>{target?.catalog || ''}</div>
            </div>
          </div>

          {/* Target table */}
          <div style={{ marginBottom: 16, padding: '10px 12px', borderRadius: 8, background: '#0a1120' }}>
            <div style={{ fontSize: 10, color: '#475569', marginBottom: 6 }}>Destination table</div>
            {targetTable ? (
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Database size={12} color="#0ea5e9" />
                <span style={{ fontSize: 12, color: '#94a3b8', wordBreak: 'break-all' }}>{targetTable}</span>
              </div>
            ) : (
              <div style={{ fontSize: 12, color: '#334155' }}>Not set — edit job to configure</div>
            )}
          </div>

          {/* Preview button */}
          <button
            onClick={handlePreview}
            disabled={previewLoading || !targetTable || !target?.host}
            style={{ ...btnPreview, opacity: (!targetTable || !target?.host) ? 0.4 : 1 }}
          >
            {previewLoading ? <Loader2 size={13} style={{ animation: 'spin 1s linear infinite' }} /> : <Table2 size={13} />}
            {previewLoading ? 'Loading…' : 'Preview Target Data'}
          </button>

          {previewError && (
            <div style={{ marginTop: 8, fontSize: 11, color: '#f87171', display: 'flex', gap: 6 }}>
              <AlertCircle size={12} style={{ flexShrink: 0, marginTop: 1 }} />{previewError}
            </div>
          )}

          {/* Last successful row count */}
          {lastRun?.status === 'success' && (
            <div style={{ marginTop: 16, padding: '10px 12px', borderRadius: 8, background: '#0a1120', fontSize: 12 }}>
              <div style={{ color: '#64748b', marginBottom: 4 }}>Last write</div>
              <div style={{ color: '#34d399', fontWeight: 700, fontSize: 16 }}>{lastRun.rows.toLocaleString()}</div>
              <div style={{ color: '#64748b' }}>rows · {lastRun.started_at?.slice(0, 10)}</div>
            </div>
          )}
        </div>
      </div>

      {/* ── Data preview table ────────────────────────────────── */}
      {preview && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#94a3b8', marginBottom: 10 }}>
            Target Data Preview — {preview.rows.length} rows
          </div>
          <div style={{ overflowX: 'auto', borderRadius: 8, border: '1px solid #1e293b' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ background: '#1e293b' }}>
                  {preview.columns.map(c => (
                    <th key={c} style={{ padding: '8px 12px', textAlign: 'left', color: '#64748b', fontWeight: 600, whiteSpace: 'nowrap', borderBottom: '1px solid #334155' }}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.rows.map((row, i) => (
                  <tr key={i} style={{ background: i % 2 === 0 ? '#0f172a' : '#0a1120' }}>
                    {preview.columns.map(c => (
                      <td key={c} style={{ padding: '7px 12px', color: '#cbd5e1', borderBottom: '1px solid #1e293b11', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {row[c] == null ? <span style={{ color: '#334155' }}>null</span> : String(row[c])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Per-table run history ─────────────────────────────── */}
      <div>
        <div style={{ fontSize: 13, fontWeight: 600, color: '#94a3b8', marginBottom: 10 }}>
          Per-table Run History
        </div>
        <div style={{ borderRadius: 8, border: '1px solid #1e293b', overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: '#1e293b' }}>
                {['Status', 'Table', 'Rows', 'Duration', 'Started', 'Error'].map(h => (
                  <th key={h} style={{ padding: '8px 12px', textAlign: 'left', color: '#64748b', fontWeight: 600, borderBottom: '1px solid #334155' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {runs.slice(0, 30).map((r, i) => (
                <tr key={r.id} style={{ background: i % 2 === 0 ? '#0f172a' : '#0a1120' }}>
                  <td style={{ padding: '7px 12px', borderBottom: '1px solid #1e293b22' }}>
                    {r.status === 'success'
                      ? <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: '#34d399' }}><CheckCircle size={11} /> OK</span>
                      : <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: '#f87171' }}><XCircle size={11} /> Error</span>}
                  </td>
                  <td style={{ padding: '7px 12px', color: '#94a3b8', borderBottom: '1px solid #1e293b22' }}>{r.table_name || '—'}</td>
                  <td style={{ padding: '7px 12px', color: '#cbd5e1', borderBottom: '1px solid #1e293b22' }}>{(r.rows || 0).toLocaleString()}</td>
                  <td style={{ padding: '7px 12px', color: '#64748b', borderBottom: '1px solid #1e293b22' }}>{r.duration_s != null ? `${r.duration_s.toFixed(2)}s` : '—'}</td>
                  <td style={{ padding: '7px 12px', color: '#64748b', borderBottom: '1px solid #1e293b22', whiteSpace: 'nowrap' }}>{r.started_at?.slice(0, 16).replace('T', ' ')}</td>
                  <td style={{ padding: '7px 12px', color: '#f87171', borderBottom: '1px solid #1e293b22', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.error || ''}</td>
                </tr>
              ))}
              {runs.length === 0 && (
                <tr><td colSpan={6} style={{ padding: '20px 12px', textAlign: 'center', color: '#334155' }}>No runs yet</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

const panel: React.CSSProperties = {
  padding: '16px', borderRadius: 10, border: '1px solid',
  background: '#0f172a', display: 'flex', flexDirection: 'column',
}
const flowLine: React.CSSProperties = {
  flex: 1, height: 2, background: 'linear-gradient(90deg, #1e293b, #334155)',
}
const centerMsg: React.CSSProperties = {
  padding: 60, textAlign: 'center', color: '#64748b', fontSize: 14,
}
const btnPrimary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '8px 14px', borderRadius: 7, border: 'none', cursor: 'pointer',
  background: '#34d399', color: '#0f172a', fontWeight: 600, fontSize: 13,
}
const btnGhost: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '8px 12px', borderRadius: 7, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#94a3b8', fontSize: 13,
}
const btnBack: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 4,
  padding: '6px 10px', borderRadius: 6, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#64748b', fontSize: 12,
}
const btnPreview: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6, width: '100%', justifyContent: 'center',
  padding: '9px 12px', borderRadius: 7, border: '1px solid #0ea5e9', cursor: 'pointer',
  background: 'transparent', color: '#38bdf8', fontSize: 13, fontWeight: 600,
}
function chip(bg: string, color: string): React.CSSProperties {
  return { padding: '3px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600, background: bg, color }
}
