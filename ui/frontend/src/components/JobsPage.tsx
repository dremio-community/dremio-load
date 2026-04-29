import { useEffect, useState } from 'react'
import {
  CheckCircle, XCircle, Clock, Play, RefreshCw, Plus,
  Pause, Trash2, ChevronDown, ChevronUp, Loader2,
} from 'lucide-react'
import {
  getJobs, triggerJob, setJobEnabled, deleteJob,
  type Job,
} from '../api/client'
import JobModal from './JobModal'

function StatusBadge({ job }: { job: Job }) {
  if (job.running) return (
    <span style={badge('emerald')}>
      <Loader2 size={11} style={{ animation: 'spin 1s linear infinite' }} /> Running
    </span>
  )
  if (!job.enabled) return <span style={badge('slate')}>Disabled</span>
  const s = job.last_run?.status
  if (!s) return <span style={badge('slate')}>Never run</span>
  if (s === 'ok') return <span style={badge('emerald')}><CheckCircle size={11} /> OK</span>
  if (s === 'error') return <span style={badge('red')}><XCircle size={11} /> Error</span>
  return <span style={badge('amber')}><Clock size={11} /> {s}</span>
}

function badge(color: 'emerald' | 'red' | 'amber' | 'slate'): React.CSSProperties {
  const colors: Record<string, { bg: string; text: string }> = {
    emerald: { bg: '#064e3b', text: '#34d399' },
    red:     { bg: '#450a0a', text: '#f87171' },
    amber:   { bg: '#451a03', text: '#fbbf24' },
    slate:   { bg: '#1e293b', text: '#94a3b8' },
  }
  const c = colors[color]
  return {
    display: 'inline-flex', alignItems: 'center', gap: 4,
    padding: '2px 8px', borderRadius: 20, fontSize: 11, fontWeight: 600,
    background: c.bg, color: c.text,
  }
}

export default function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [loading, setLoading] = useState(true)
  const [showModal, setShowModal] = useState(false)
  const [editJob, setEditJob] = useState<Job | null>(null)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [triggering, setTriggering] = useState<Set<string>>(new Set())
  const [msg, setMsg] = useState<{ text: string; ok: boolean } | null>(null)

  const load = async () => {
    try {
      const data = await getJobs()
      setJobs(data)
    } catch { /* ignore */ } finally { setLoading(false) }
  }

  useEffect(() => { load(); const iv = setInterval(load, 5000); return () => clearInterval(iv) }, [])

  const flash = (text: string, ok = true) => { setMsg({ text, ok }); setTimeout(() => setMsg(null), 3000) }

  const handleTrigger = async (id: string) => {
    setTriggering(s => new Set(s).add(id))
    try {
      const r = await triggerJob(id)
      flash(r.message || 'Started', r.ok)
    } catch { flash('Failed to trigger', false) }
    finally { setTriggering(s => { const n = new Set(s); n.delete(id); return n }) }
    setTimeout(load, 1000)
  }

  const handleToggle = async (job: Job) => {
    await setJobEnabled(job.id, !job.enabled)
    flash(`Job ${job.enabled ? 'disabled' : 'enabled'}`)
    load()
  }

  const handleDelete = async (job: Job) => {
    if (!confirm(`Delete job "${job.name}"?`)) return
    await deleteJob(job.id)
    flash('Job deleted')
    load()
  }

  const toggleExpand = (id: string) =>
    setExpanded(s => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n })

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>Load Jobs</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={load} style={btnGhost}><RefreshCw size={15} /></button>
          <button onClick={() => { setEditJob(null); setShowModal(true) }} style={btnPrimary}>
            <Plus size={15} /> New Job
          </button>
        </div>
      </div>

      {msg && (
        <div style={{
          marginBottom: 12, padding: '10px 14px', borderRadius: 8, fontSize: 13,
          background: msg.ok ? '#064e3b' : '#450a0a', color: msg.ok ? '#34d399' : '#f87171',
        }}>{msg.text}</div>
      )}

      {loading ? (
        <div style={{ color: '#64748b', textAlign: 'center', paddingTop: 60 }}>Loading…</div>
      ) : jobs.length === 0 ? (
        <div style={{ color: '#64748b', textAlign: 'center', paddingTop: 60 }}>
          No jobs yet. Click <strong>New Job</strong> to create one.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {jobs.map(job => (
            <div key={job.id} style={card}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                    <span style={{ fontWeight: 600, color: '#f1f5f9' }}>{job.name}</span>
                    <StatusBadge job={job} />
                    {job.load_mode && (
                      <span style={badge('amber')}>{job.load_mode}</span>
                    )}
                  </div>
                  <div style={{ fontSize: 12, color: '#64748b', display: 'flex', gap: 12 }}>
                    <span>Type: {job.source_type || '—'}</span>
                    {job.schedule && <span>Schedule: <code style={{ color: '#94a3b8' }}>{job.schedule}</code></span>}
                    {job.last_run?.started_at && (
                      <span>Last: {new Date(job.last_run.started_at).toLocaleString()} · {job.last_run.rows ?? 0} rows</span>
                    )}
                  </div>
                </div>
                <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                  <button
                    onClick={() => handleTrigger(job.id)}
                    disabled={triggering.has(job.id) || job.running}
                    style={{ ...btnSmall, background: '#064e3b', color: '#34d399' }}
                    title="Run now"
                  >
                    {triggering.has(job.id) ? <Loader2 size={13} style={{ animation: 'spin 1s linear infinite' }} /> : <Play size={13} />}
                  </button>
                  <button
                    onClick={() => handleToggle(job)}
                    style={{ ...btnSmall, background: job.enabled ? '#1e293b' : '#451a03', color: job.enabled ? '#94a3b8' : '#fbbf24' }}
                    title={job.enabled ? 'Disable' : 'Enable'}
                  >
                    <Pause size={13} />
                  </button>
                  <button
                    onClick={() => { setEditJob(job); setShowModal(true) }}
                    style={btnSmall}
                    title="Edit"
                  >
                    <RefreshCw size={13} />
                  </button>
                  <button
                    onClick={() => handleDelete(job)}
                    style={{ ...btnSmall, color: '#f87171' }}
                    title="Delete"
                  >
                    <Trash2 size={13} />
                  </button>
                  {(job.tables?.length ?? 0) > 0 && (
                    <button onClick={() => toggleExpand(job.id)} style={btnSmall}>
                      {expanded.has(job.id) ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
                    </button>
                  )}
                </div>
              </div>

              {expanded.has(job.id) && job.tables && (
                <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid #1e293b' }}>
                  <div style={{ fontSize: 11, color: '#64748b', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Tables</div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {job.tables.map((t: string) => (
                      <span key={t} style={{
                        padding: '2px 10px', background: '#1e293b', borderRadius: 12,
                        fontSize: 12, color: '#cbd5e1', fontFamily: 'monospace',
                      }}>{t}</span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {showModal && (
        <JobModal
          job={editJob}
          onClose={() => setShowModal(false)}
          onSaved={() => { setShowModal(false); load() }}
        />
      )}

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}

const card: React.CSSProperties = {
  background: '#1e293b', borderRadius: 10, padding: '14px 16px',
  border: '1px solid #334155',
}
const btnPrimary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '7px 14px', borderRadius: 7, border: 'none', cursor: 'pointer',
  background: '#34d399', color: '#0f172a', fontWeight: 600, fontSize: 13,
}
const btnGhost: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '7px 10px', borderRadius: 7, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#94a3b8', fontSize: 13,
}
const btnSmall: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
  width: 28, height: 28, borderRadius: 6, border: '1px solid #334155', cursor: 'pointer',
  background: '#0f172a', color: '#94a3b8', fontSize: 12,
}
