import { useState } from 'react'
import { CheckCircle, Eye, Play, XCircle } from 'lucide-react'
import { previewCopyInto, runCopyInto } from '../api/client'

const FILE_FORMATS = ['parquet', 'json', 'csv', 'delta', 'orc']

export default function CopyIntoPage() {
  const [targetTable, setTargetTable] = useState('')
  const [sourceLocation, setSourceLocation] = useState('')
  const [fileFormat, setFileFormat] = useState('parquet')
  const [pattern, setPattern] = useState('')
  const [csvDelimiter, setCsvDelimiter] = useState(',')
  const [csvHeader, setCsvHeader] = useState(true)
  const [sql, setSql] = useState('')
  const [previewing, setPreviewing] = useState(false)
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null)

  const formatOptions = fileFormat === 'csv'
    ? { delimiter: csvDelimiter, header: csvHeader }
    : undefined

  const handlePreview = async () => {
    setPreviewing(true); setSql(''); setResult(null)
    try {
      const r = await previewCopyInto({
        target_table: targetTable,
        source_location: sourceLocation,
        file_format: fileFormat,
        format_options: formatOptions,
        pattern: pattern || undefined,
      })
      setSql(r.sql)
    } catch (e: any) {
      setResult({ ok: false, message: String(e) })
    } finally { setPreviewing(false) }
  }

  const handleRun = async () => {
    setRunning(true); setResult(null)
    try {
      const r = await runCopyInto({
        target_table: targetTable,
        source_location: sourceLocation,
        file_format: fileFormat,
        format_options: formatOptions,
        pattern: pattern || undefined,
      })
      setResult({ ok: r.ok, message: r.message || 'COPY INTO started' })
    } catch (e: any) {
      setResult({ ok: false, message: String(e) })
    } finally { setRunning(false) }
  }

  return (
    <div style={{ padding: 24, maxWidth: 720 }}>
      <h1 style={{ margin: '0 0 6px', fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>COPY INTO</h1>
      <p style={{ margin: '0 0 20px', color: '#64748b', fontSize: 13 }}>
        Use Dremio's native COPY INTO command to load files from a registered source directly into an Iceberg table.
        Dremio tracks which files have been loaded, so re-running is safe.
      </p>

      {result && (
        <div style={{
          marginBottom: 14, padding: '10px 14px', borderRadius: 8, fontSize: 13,
          display: 'flex', alignItems: 'center', gap: 8,
          background: result.ok ? '#064e3b' : '#450a0a',
          color: result.ok ? '#34d399' : '#f87171',
        }}>
          {result.ok ? <CheckCircle size={15} /> : <XCircle size={15} />}
          {result.message}
        </div>
      )}

      <div style={card}>
        <div style={sectionTitle}>Source & Target</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
          <div>
            <label style={lbl}>Target Table</label>
            <input style={inp} value={targetTable} onChange={e => setTargetTable(e.target.value)}
              placeholder='"Catalog"."schema"."table"' />
          </div>
          <div>
            <label style={lbl}>Source Location (Dremio path)</label>
            <input style={inp} value={sourceLocation} onChange={e => setSourceLocation(e.target.value)}
              placeholder='@my_s3_source/folder/' />
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
          <div>
            <label style={lbl}>File Format</label>
            <select style={inp} value={fileFormat} onChange={e => setFileFormat(e.target.value)}>
              {FILE_FORMATS.map(f => <option key={f} value={f}>{f.toUpperCase()}</option>)}
            </select>
          </div>
          <div>
            <label style={lbl}>File Pattern (regex, optional)</label>
            <input style={inp} value={pattern} onChange={e => setPattern(e.target.value)}
              placeholder='.*\.parquet' />
          </div>
        </div>

        {fileFormat === 'csv' && (
          <div style={{ display: 'grid', gridTemplateColumns: '120px 1fr', gap: 12, marginTop: 12 }}>
            <div>
              <label style={lbl}>Delimiter</label>
              <input style={inp} value={csvDelimiter} onChange={e => setCsvDelimiter(e.target.value)} placeholder="," />
            </div>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, paddingBottom: 2 }}>
              <input
                type="checkbox" id="hdr" checked={csvHeader}
                onChange={e => setCsvHeader(e.target.checked)}
              />
              <label htmlFor="hdr" style={{ fontSize: 13, color: '#94a3b8', cursor: 'pointer' }}>First row is header</label>
            </div>
          </div>
        )}
      </div>

      <div style={{ display: 'flex', gap: 8, marginTop: 14 }}>
        <button
          onClick={handlePreview}
          disabled={previewing || !targetTable || !sourceLocation}
          style={btnSecondary}
        >
          <Eye size={14} /> {previewing ? 'Generating…' : 'Preview SQL'}
        </button>
        <button
          onClick={handleRun}
          disabled={running || !targetTable || !sourceLocation}
          style={btnPrimary}
        >
          <Play size={14} /> {running ? 'Running…' : 'Run COPY INTO'}
        </button>
      </div>

      {sql && (
        <div style={{ marginTop: 16 }}>
          <div style={sectionTitle}>Generated SQL</div>
          <pre style={{
            background: '#0f172a', border: '1px solid #334155', borderRadius: 8,
            padding: 16, fontSize: 12, color: '#94a3b8', overflow: 'auto',
            margin: 0, fontFamily: 'monospace', lineHeight: 1.6,
          }}>{sql}</pre>
        </div>
      )}
    </div>
  )
}

const card: React.CSSProperties = {
  background: '#1e293b', borderRadius: 10, padding: 16, border: '1px solid #334155',
}
const sectionTitle: React.CSSProperties = {
  fontSize: 11, fontWeight: 600, color: '#64748b',
  textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 12,
}
const lbl: React.CSSProperties = { display: 'block', fontSize: 12, color: '#94a3b8', marginBottom: 4 }
const inp: React.CSSProperties = {
  width: '100%', boxSizing: 'border-box',
  background: '#0f172a', border: '1px solid #334155', borderRadius: 7,
  padding: '8px 10px', color: '#e2e8f0', fontSize: 13, outline: 'none',
}
const btnPrimary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '8px 16px', borderRadius: 7, border: 'none', cursor: 'pointer',
  background: '#34d399', color: '#0f172a', fontWeight: 600, fontSize: 13,
}
const btnSecondary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '8px 14px', borderRadius: 7, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#94a3b8', fontSize: 13,
}
