import { useEffect, useState } from 'react'
import { Database, ChevronRight, ChevronDown, Table, Search } from 'lucide-react'
import { getJobs, getDremioNamespaces, getDremioTables, getDremioPreview, getSourceTables, getSourcePreview } from '../api/client'
import type { Job } from '../api/client'

type Tab = 'dremio' | 'source'

export default function ExplorerPage() {
  const [tab, setTab] = useState<Tab>('dremio')

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ padding: '16px 24px 0', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
        <h1 style={{ margin: '0 0 12px', fontSize: 20, fontWeight: 700, color: 'var(--foreground)' }}>Explorer</h1>
        <div style={{ display: 'flex', gap: 4 }}>
          <TabBtn active={tab === 'dremio'} onClick={() => setTab('dremio')}>Dremio Catalog</TabBtn>
          <TabBtn active={tab === 'source'} onClick={() => setTab('source')}>Source Data</TabBtn>
        </div>
      </div>
      <div style={{ flex: 1, overflow: 'hidden', display: 'flex' }}>
        {tab === 'dremio' ? <DremioExplorer /> : <SourceExplorer />}
      </div>
    </div>
  )
}

// ── Dremio Catalog Explorer ───────────────────────────────────────────────────

function DremioExplorer() {
  const [namespaces, setNamespaces] = useState<string[]>([])
  const [expanded, setExpanded] = useState<Record<string, string[]>>({})
  const [loading, setLoading] = useState(true)
  const [loadingNs, setLoadingNs] = useState<string | null>(null)
  const [preview, setPreview] = useState<{ table: string; columns: string[]; rows: Record<string, unknown>[]; sql: string } | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [search, setSearch] = useState('')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getDremioNamespaces()
      .then(ns => setNamespaces(ns))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  const toggleNs = async (ns: string) => {
    if (expanded[ns]) {
      const next = { ...expanded }; delete next[ns]; setExpanded(next); return
    }
    setLoadingNs(ns)
    try {
      const tables = await getDremioTables(ns)
      setExpanded(e => ({ ...e, [ns]: tables }))
    } catch {}
    setLoadingNs(null)
  }

  const openPreview = async (table: string) => {
    setPreviewLoading(true); setPreview(null)
    try { setPreview(await getDremioPreview(table)) }
    catch {}
    setPreviewLoading(false)
  }

  const filteredNs = namespaces.filter(ns =>
    !search || ns.toLowerCase().includes(search.toLowerCase()) ||
    (expanded[ns] ?? []).some(t => t.toLowerCase().includes(search.toLowerCase()))
  )

  return (
    <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
      {/* Left: namespace tree */}
      <div style={{ width: 280, borderRight: '1px solid var(--border)', display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
        <div style={{ padding: 12, borderBottom: '1px solid var(--border)' }}>
          <div style={searchBox}>
            <Search size={13} color="var(--muted-foreground)" />
            <input
              value={search} onChange={e => setSearch(e.target.value)}
              placeholder="Search…" style={searchInp}
            />
          </div>
        </div>
        <div style={{ flex: 1, overflow: 'auto', padding: '8px 0' }}>
          {loading && <div style={dimMsg}>Loading…</div>}
          {error && <div style={{ ...dimMsg, color: 'var(--status-error)' }}>{error}</div>}
          {filteredNs.map(ns => (
            <div key={ns}>
              <div
                onClick={() => toggleNs(ns)}
                style={treeRow(false)}
              >
                {expanded[ns] ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
                <Database size={13} color="var(--muted-foreground)" />
                <span style={{ fontSize: 13, color: 'var(--foreground)' }}>{ns}</span>
                {loadingNs === ns && <span style={{ fontSize: 11, color: 'var(--muted-foreground)', marginLeft: 'auto' }}>…</span>}
              </div>
              {expanded[ns]?.filter(t => !search || t.toLowerCase().includes(search.toLowerCase())).map(tbl => (
                <div
                  key={tbl}
                  onClick={() => openPreview(`${ns}.${tbl}`)}
                  style={{ ...treeRow(preview?.table === `${ns}.${tbl}`), paddingLeft: 32 }}
                >
                  <Table size={12} color="var(--secondary-foreground)" />
                  <span style={{ fontSize: 12, color: 'var(--foreground)' }}>{tbl}</span>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>

      {/* Right: preview */}
      <div style={{ flex: 1, overflow: 'auto', padding: 20 }}>
        {previewLoading && <div style={dimMsg}>Loading preview…</div>}
        {!previewLoading && !preview && (
          <div style={dimMsg}>Select a table to preview its data.</div>
        )}
        {preview && !previewLoading && (
          <>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <div>
                <h2 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: 'var(--foreground)' }}>{preview.table}</h2>
                <div style={{ fontSize: 12, color: 'var(--secondary-foreground)', marginTop: 2 }}>
                  {preview.rows.length} rows · {preview.columns.length} columns
                </div>
              </div>
            </div>
            <DataGrid columns={preview.columns} rows={preview.rows} />
          </>
        )}
      </div>
    </div>
  )
}

// ── Source Data Explorer ──────────────────────────────────────────────────────

function SourceExplorer() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [selectedJob, setSelectedJob] = useState<Job | null>(null)
  const [sourceTables, setSourceTables] = useState<string[]>([])
  const [tablesLoading, setTablesLoading] = useState(false)
  const [tablesError, setTablesError] = useState<string | null>(null)
  const [preview, setPreview] = useState<{ table: string; columns: string[]; rows: Record<string, unknown>[] } | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [search, setSearch] = useState('')

  useEffect(() => {
    getJobs().then(setJobs).catch(() => {})
  }, [])

  const selectJob = async (job: Job) => {
    setSelectedJob(job); setSourceTables([]); setPreview(null)
    setTablesError(null); setTablesLoading(true)
    try {
      const tables = await getSourceTables(job.id)
      setSourceTables(tables)
    } catch (e: any) {
      setTablesError(e.message)
    }
    setTablesLoading(false)
  }

  const openPreview = async (table: string) => {
    if (!selectedJob) return
    setPreviewLoading(true); setPreview(null)
    try { setPreview(await getSourcePreview(selectedJob.id, table)) }
    catch {}
    setPreviewLoading(false)
  }

  const filteredTables = sourceTables.filter(t => !search || t.toLowerCase().includes(search.toLowerCase()))

  return (
    <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
      {/* Left: jobs list */}
      <div style={{ width: 280, borderRight: '1px solid var(--border)', display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
        <div style={{ padding: '12px', borderBottom: '1px solid var(--border)', fontSize: 11, fontWeight: 600, color: 'var(--muted-foreground)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
          Sources
        </div>
        <div style={{ flex: 1, overflow: 'auto', padding: '8px 0' }}>
          {jobs.length === 0 && <div style={dimMsg}>No jobs configured.</div>}
          {jobs.map(job => (
            <div key={job.id} style={treeRow(selectedJob?.id === job.id)} onClick={() => selectJob(job)}>
              <Database size={13} color={selectedJob?.id === job.id ? 'var(--accent)' : 'var(--muted-foreground)'} />
              <div>
                <div style={{ fontSize: 13, color: 'var(--foreground)' }}>{job.name}</div>
                <div style={{ fontSize: 11, color: 'var(--secondary-foreground)' }}>{job.source_type ?? 'unknown'}</div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Center: table list */}
      <div style={{ width: 220, borderRight: '1px solid var(--border)', display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
        <div style={{ padding: 10, borderBottom: '1px solid var(--border)' }}>
          <div style={searchBox}>
            <Search size={13} color="var(--muted-foreground)" />
            <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Filter…" style={searchInp} />
          </div>
        </div>
        <div style={{ flex: 1, overflow: 'auto', padding: '8px 0' }}>
          {!selectedJob && <div style={dimMsg}>Select a source.</div>}
          {tablesLoading && <div style={dimMsg}>Loading tables…</div>}
          {tablesError && <div style={{ ...dimMsg, color: 'var(--status-error)' }}>{tablesError}</div>}
          {filteredTables.map(tbl => (
            <div key={tbl} style={{ ...treeRow(preview?.table === tbl), paddingLeft: 16 }} onClick={() => openPreview(tbl)}>
              <Table size={12} color="var(--secondary-foreground)" />
              <span style={{ fontSize: 12, color: 'var(--foreground)' }}>{tbl}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Right: preview */}
      <div style={{ flex: 1, overflow: 'auto', padding: 20 }}>
        {previewLoading && <div style={dimMsg}>Loading preview…</div>}
        {!previewLoading && !preview && <div style={dimMsg}>Select a table to preview its data.</div>}
        {preview && !previewLoading && (
          <>
            <div style={{ marginBottom: 12 }}>
              <h2 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: 'var(--foreground)' }}>{preview.table}</h2>
              <div style={{ fontSize: 12, color: 'var(--secondary-foreground)', marginTop: 2 }}>
                {preview.rows.length} rows · {preview.columns.length} columns (sample)
              </div>
            </div>
            <DataGrid columns={preview.columns} rows={preview.rows} />
          </>
        )}
      </div>
    </div>
  )
}

// ── Shared: data grid ─────────────────────────────────────────────────────────

function DataGrid({ columns, rows }: { columns: string[]; rows: Record<string, unknown>[] }) {
  if (!rows.length) return <div style={dimMsg}>No rows returned.</div>
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 12 }}>
        <thead>
          <tr>
            {columns.map(col => (
              <th key={col} style={th}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? 'var(--card)' : 'var(--background)' }}>
              {columns.map(col => (
                <td key={col} style={td}>{String(row[col] ?? '')}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Shared UI helpers ─────────────────────────────────────────────────────────

function TabBtn({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button onClick={onClick} style={{
      padding: '8px 16px', fontSize: 13, fontWeight: 600, border: 'none', cursor: 'pointer',
      background: 'transparent', color: active ? 'var(--accent)' : 'var(--secondary-foreground)',
      borderBottom: active ? '2px solid var(--accent)' : '2px solid transparent',
      transition: 'all 0.15s',
    }}>
      {children}
    </button>
  )
}

const treeRow = (active: boolean): React.CSSProperties => ({
  display: 'flex', alignItems: 'center', gap: 8, padding: '6px 12px', cursor: 'pointer',
  background: active ? 'var(--selected)' : 'transparent',
  borderLeft: active ? '2px solid var(--accent)' : '2px solid transparent',
})

const searchBox: React.CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 6, background: '#fff',
  border: '1px solid var(--border)', borderRadius: 6, padding: '6px 8px',
}
const searchInp: React.CSSProperties = {
  flex: 1, background: 'transparent', border: 'none', outline: 'none',
  color: 'var(--foreground)', fontSize: 12,
}
const dimMsg: React.CSSProperties = { padding: '20px 16px', color: 'var(--muted-foreground)', fontSize: 13 }
const th: React.CSSProperties = {
  padding: '8px 12px', background: 'var(--muted)', color: 'var(--muted-foreground)',
  fontWeight: 600, fontSize: 11, textAlign: 'left', whiteSpace: 'nowrap',
  borderBottom: '1px solid var(--border)', position: 'sticky', top: 0,
}
const td: React.CSSProperties = {
  padding: '7px 12px', borderBottom: '1px solid var(--border)',
  color: 'var(--foreground)', whiteSpace: 'nowrap', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis',
}
