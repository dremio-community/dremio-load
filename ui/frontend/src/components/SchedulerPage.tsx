import { useEffect, useState } from 'react'
import { Play, Clock, ToggleLeft, ToggleRight, RefreshCw, Edit2, Check, X } from 'lucide-react'

interface ScheduledJob {
  id: string
  name: string
  source_type: string
  schedule: string | null
  enabled: boolean
  next_run: string | null
  prev_run: string | null
  load_mode: string
  last_status: string | null
  last_run_at: string | null
  running: boolean
}

const SOURCE_ICONS: Record<string, string> = {
  postgres: '🐘', mysql: '🐬', sqlserver: '🪟', oracle: '🔴',
  mongodb: '🍃', snowflake: '❄️', s3: '☁️', gcs: '☁️', azure_blob: '☁️',
  google_ads: '📊', linkedin_ads: '💼', salesforce: '☁️', hubspot: '🟠',
  zendesk: '🎫', clickhouse: '⚡', cassandra: '👁️', dynamodb: '🔷',
  cosmosdb: '🌐', spanner: '🔵', delta: '△', hudi: '🅷', databricks: '🧱',
  splunk: '🟡', pinot: '📌', copy_into: '📋',
}

function formatTime(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  const now = new Date()
  const diffMs = d.getTime() - now.getTime()
  const diffS = Math.round(Math.abs(diffMs) / 1000)
  const past = diffMs < 0

  if (diffS < 60) return past ? 'just now' : 'in <1 min'
  if (diffS < 3600) {
    const m = Math.round(diffS / 60)
    return past ? `${m}m ago` : `in ${m}m`
  }
  if (diffS < 86400) {
    const h = Math.round(diffS / 3600)
    return past ? `${h}h ago` : `in ${h}h`
  }
  return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function cronHuman(cron: string | null): string {
  if (!cron) return 'No schedule'
  const parts = cron.trim().split(/\s+/)
  if (parts.length !== 5) return cron
  const [min, hour, dom, , dow] = parts
  if (min === '0' && hour === '*' && dom === '*' && dow === '*') return 'Every hour'
  if (min !== '*' && hour === '*' && dom === '*' && dow === '*') return `Every hour at :${min.padStart(2, '0')}`
  if (min !== '*' && hour !== '*' && dom === '*' && dow === '*') {
    const h = parseInt(hour)
    const ampm = h >= 12 ? 'PM' : 'AM'
    const h12 = h % 12 || 12
    return `Daily at ${h12}:${min.padStart(2, '0')} ${ampm}`
  }
  if (dom === '*' && dow !== '*') {
    const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
    const d = days[parseInt(dow)] || dow
    return `Weekly on ${d}`
  }
  return cron
}

export default function SchedulerPage() {
  const [jobs, setJobs] = useState<ScheduledJob[]>([])
  const [loading, setLoading] = useState(true)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editCron, setEditCron] = useState('')
  const [triggering, setTriggering] = useState<string | null>(null)

  const load = () => {
    setLoading(true)
    fetch('/api/schedule')
      .then(r => r.json())
      .then(setJobs)
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
    const t = setInterval(load, 30000)
    return () => clearInterval(t)
  }, [])

  const toggleEnabled = (job: ScheduledJob) => {
    fetch(`/api/schedule/${job.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: !job.enabled }),
    }).then(load)
  }

  const saveCron = (job: ScheduledJob) => {
    fetch(`/api/schedule/${job.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ schedule: editCron }),
    }).then(() => { setEditingId(null); load() })
  }

  const triggerJob = (jobId: string) => {
    setTriggering(jobId)
    fetch(`/api/jobs/${jobId}/run`, { method: 'POST' })
      .finally(() => { setTimeout(() => { setTriggering(null); load() }, 1500) })
  }

  const statusColor = (status: string | null) => {
    if (status === 'success') return 'var(--status-success)'
    if (status === 'error') return 'var(--status-error)'
    return 'var(--muted-foreground)'
  }

  if (loading && jobs.length === 0) {
    return <div style={s.center}><RefreshCw size={20} color="var(--muted-foreground)" /></div>
  }

  const scheduled = jobs.filter(j => j.schedule)
  const unscheduled = jobs.filter(j => !j.schedule)

  return (
    <div style={s.page}>
      <div style={s.header}>
        <div>
          <h2 style={s.title}>Scheduler</h2>
          <p style={s.sub}>Manage cron schedules and trigger jobs manually</p>
        </div>
        <button style={s.refreshBtn} onClick={load}>
          <RefreshCw size={14} />
          Refresh
        </button>
      </div>

      {/* Timeline strip */}
      {scheduled.length > 0 && (
        <div style={s.timeline}>
          <div style={s.timelineLabel}><Clock size={13} style={{ marginRight: 6 }} />Upcoming runs</div>
          <div style={s.timelineItems}>
            {scheduled
              .filter(j => j.enabled && j.next_run)
              .slice(0, 8)
              .map(j => (
                <div key={j.id} style={s.timelineChip}>
                  <span>{SOURCE_ICONS[j.source_type] || '📦'}</span>
                  <span style={{ fontWeight: 600 }}>{j.name}</span>
                  <span style={{ color: 'var(--status-success)' }}>{formatTime(j.next_run)}</span>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Scheduled jobs table */}
      {scheduled.length > 0 && (
        <div style={s.section}>
          <div style={s.sectionTitle}>Scheduled Jobs ({scheduled.length})</div>
          <div style={s.table}>
            <div style={s.tableHead}>
              <span>Job</span>
              <span>Schedule</span>
              <span>Next Run</span>
              <span>Last Run</span>
              <span>Status</span>
              <span>Actions</span>
            </div>
            {scheduled.map(job => (
              <div key={job.id} style={{ ...s.tableRow, opacity: job.enabled ? 1 : 0.5 }}>
                <div style={s.jobCell}>
                  <span style={s.icon}>{SOURCE_ICONS[job.source_type] || '📦'}</span>
                  <div>
                    <div style={s.jobName}>{job.name}</div>
                    <div style={s.jobMeta}>{job.source_type} · {job.load_mode}</div>
                  </div>
                </div>

                <div>
                  {editingId === job.id ? (
                    <div style={s.editRow}>
                      <input
                        style={s.cronInput}
                        value={editCron}
                        onChange={e => setEditCron(e.target.value)}
                        placeholder="cron expression"
                        autoFocus
                      />
                      <button style={s.iconBtn} onClick={() => saveCron(job)}><Check size={13} color="var(--status-success)" /></button>
                      <button style={s.iconBtn} onClick={() => setEditingId(null)}><X size={13} color="var(--status-error)" /></button>
                    </div>
                  ) : (
                    <div style={s.cronCell}>
                      <div style={s.cronHuman}>{cronHuman(job.schedule)}</div>
                      <div style={s.cronRaw}>{job.schedule}</div>
                    </div>
                  )}
                </div>

                <div style={{ color: 'var(--secondary-foreground)', fontSize: 13 }}>{formatTime(job.next_run)}</div>
                <div style={{ color: 'var(--muted-foreground)', fontSize: 13 }}>{formatTime(job.last_run_at)}</div>

                <div>
                  {job.running ? (
                    <span style={{ ...s.badge, background: 'var(--status-info-bg)', color: 'var(--status-info)' }}>Running</span>
                  ) : job.last_status === 'success' ? (
                    <span style={{ ...s.badge, background: 'var(--status-success-bg)', color: 'var(--status-success)' }}>OK</span>
                  ) : job.last_status === 'error' ? (
                    <span style={{ ...s.badge, background: 'var(--status-error-bg)', color: 'var(--status-error)' }}>Error</span>
                  ) : (
                    <span style={{ ...s.badge, background: 'var(--muted)', color: 'var(--secondary-foreground)' }}>Never</span>
                  )}
                </div>

                <div style={s.actions}>
                  <button
                    style={s.runBtn}
                    disabled={job.running || triggering === job.id}
                    onClick={() => triggerJob(job.id)}
                    title="Run now"
                  >
                    <Play size={12} />
                    {triggering === job.id ? 'Starting…' : 'Run'}
                  </button>
                  <button
                    style={s.iconBtn}
                    onClick={() => { setEditingId(job.id); setEditCron(job.schedule || '') }}
                    title="Edit schedule"
                  >
                    <Edit2 size={13} color="var(--secondary-foreground)" />
                  </button>
                  <button style={s.iconBtn} onClick={() => toggleEnabled(job)} title={job.enabled ? 'Disable' : 'Enable'}>
                    {job.enabled
                      ? <ToggleRight size={18} color="var(--status-success)" />
                      : <ToggleLeft size={18} color="var(--muted-foreground)" />}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Unscheduled jobs */}
      {unscheduled.length > 0 && (
        <div style={s.section}>
          <div style={s.sectionTitle}>Manual Jobs ({unscheduled.length})</div>
          <div style={s.table}>
            <div style={s.tableHead}>
              <span>Job</span>
              <span>Last Run</span>
              <span>Status</span>
              <span>Actions</span>
            </div>
            {unscheduled.map(job => (
              <div key={job.id} style={s.tableRow}>
                <div style={s.jobCell}>
                  <span style={s.icon}>{SOURCE_ICONS[job.source_type] || '📦'}</span>
                  <div>
                    <div style={s.jobName}>{job.name}</div>
                    <div style={s.jobMeta}>{job.source_type} · {job.load_mode}</div>
                  </div>
                </div>
                <div style={{ color: 'var(--muted-foreground)', fontSize: 13 }}>{formatTime(job.last_run_at)}</div>
                <div>
                  {job.running ? (
                    <span style={{ ...s.badge, background: 'var(--status-info-bg)', color: 'var(--status-info)' }}>Running</span>
                  ) : job.last_status ? (
                    <span style={{ ...s.badge, background: job.last_status === 'success' ? 'var(--status-success-bg)' : 'var(--status-error-bg)', color: statusColor(job.last_status) }}>
                      {job.last_status}
                    </span>
                  ) : (
                    <span style={{ ...s.badge, background: 'var(--muted)', color: 'var(--secondary-foreground)' }}>Never</span>
                  )}
                </div>
                <div style={s.actions}>
                  <button
                    style={s.runBtn}
                    disabled={job.running || triggering === job.id}
                    onClick={() => triggerJob(job.id)}
                  >
                    <Play size={12} />
                    {triggering === job.id ? 'Starting…' : 'Run'}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {jobs.length === 0 && !loading && (
        <div style={s.empty}>No jobs configured yet. Add jobs to your config.yml to get started.</div>
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  page: { padding: 28, maxWidth: 1100 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 24 },
  title: { margin: 0, fontSize: 20, fontWeight: 700, color: 'var(--foreground)' },
  sub: { margin: '4px 0 0', fontSize: 13, color: 'var(--secondary-foreground)' },
  refreshBtn: { display: 'flex', alignItems: 'center', gap: 6, padding: '7px 14px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--secondary-foreground)', cursor: 'pointer', fontSize: 13 },
  center: { display: 'flex', justifyContent: 'center', alignItems: 'center', height: 200 },
  timeline: { background: 'var(--selected)', border: '1px solid var(--border)', borderRadius: 10, padding: '14px 18px', marginBottom: 24 },
  timelineLabel: { display: 'flex', alignItems: 'center', fontSize: 12, color: 'var(--secondary-foreground)', marginBottom: 12, textTransform: 'uppercase', letterSpacing: '0.05em' },
  timelineItems: { display: 'flex', flexWrap: 'wrap', gap: 10 },
  timelineChip: { display: 'flex', alignItems: 'center', gap: 7, background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 20, padding: '5px 12px', fontSize: 12 },
  section: { marginBottom: 32 },
  sectionTitle: { fontSize: 13, fontWeight: 600, color: 'var(--muted-foreground)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 12 },
  table: { background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 10, overflow: 'hidden' },
  tableHead: { display: 'grid', gridTemplateColumns: '2fr 2fr 1fr 1fr 1fr 1.5fr', padding: '10px 16px', background: 'var(--muted)', fontSize: 12, color: 'var(--muted-foreground)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em', gap: 16 },
  tableRow: { display: 'grid', gridTemplateColumns: '2fr 2fr 1fr 1fr 1fr 1.5fr', padding: '14px 16px', borderTop: '1px solid var(--border)', alignItems: 'center', gap: 16 },
  jobCell: { display: 'flex', alignItems: 'center', gap: 10 },
  icon: { fontSize: 20, lineHeight: 1 },
  jobName: { fontWeight: 600, fontSize: 14, color: 'var(--foreground)' },
  jobMeta: { fontSize: 12, color: 'var(--muted-foreground)', marginTop: 2 },
  cronCell: {},
  cronHuman: { fontSize: 13, color: 'var(--foreground)', fontWeight: 500 },
  cronRaw: { fontSize: 11, color: 'var(--secondary-foreground)', marginTop: 2, fontFamily: 'monospace' },
  editRow: { display: 'flex', alignItems: 'center', gap: 6 },
  cronInput: { flex: 1, background: '#fff', border: '1px solid var(--primary)', borderRadius: 5, padding: '4px 8px', color: 'var(--foreground)', fontSize: 12, fontFamily: 'monospace', outline: 'none' },
  badge: { padding: '3px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600, textTransform: 'uppercase' },
  actions: { display: 'flex', alignItems: 'center', gap: 8 },
  runBtn: { display: 'flex', alignItems: 'center', gap: 5, padding: '5px 12px', background: 'var(--status-success-bg)', border: '1px solid var(--status-success)', borderRadius: 6, color: 'var(--status-success)', cursor: 'pointer', fontSize: 12, fontWeight: 600 },
  iconBtn: { background: 'none', border: 'none', cursor: 'pointer', padding: 4, display: 'flex', alignItems: 'center' },
  empty: { color: 'var(--muted-foreground)', fontSize: 14, padding: 40, textAlign: 'center' },
}
