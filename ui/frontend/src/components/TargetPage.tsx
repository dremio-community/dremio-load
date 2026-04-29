import { useEffect, useState } from 'react'
import { CheckCircle, XCircle, Save, Zap } from 'lucide-react'
import { getTarget, saveTarget, testTarget, getNamespaces, type TargetConfig } from '../api/client'

export default function TargetPage() {
  const [cfg, setCfg] = useState<TargetConfig>({
    host: '', port: 9047, username: '', password: '', pat: '',
    catalog: '', schema: '', use_ssl: false,
  })
  const [namespaces, setNamespaces] = useState<string[]>([])
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null)

  useEffect(() => {
    getTarget().then(t => setCfg(c => ({ ...c, ...t }))).catch(() => {})
    getNamespaces().then(setNamespaces).catch(() => {})
  }, [])

  const set = (k: keyof TargetConfig, v: any) => setCfg(c => ({ ...c, [k]: v }))

  const handleSave = async () => {
    setSaving(true); setResult(null)
    try { await saveTarget(cfg); setResult({ ok: true, message: 'Saved' }) }
    catch (e: any) { setResult({ ok: false, message: e.message }) }
    finally { setSaving(false) }
  }

  const handleTest = async () => {
    setTesting(true); setResult(null)
    try { const r = await testTarget(); setResult(r) }
    catch (e: any) { setResult({ ok: false, message: String(e) }) }
    finally { setTesting(false) }
  }

  return (
    <div style={{ padding: 24, maxWidth: 680 }}>
      <h1 style={{ margin: '0 0 20px', fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>Target (Dremio)</h1>

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
        <div style={sectionTitle}>Connection</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 100px', gap: 12, marginBottom: 12 }}>
          <Field label="Host" value={cfg.host} onChange={v => set('host', v)} placeholder="localhost" />
          <Field label="Port" value={String(cfg.port ?? 9047)} onChange={v => set('port', Number(v))} placeholder="9047" />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
          <Field label="Username" value={cfg.username ?? ''} onChange={v => set('username', v)} placeholder="admin" />
          <Field label="Password" value={cfg.password ?? ''} onChange={v => set('password', v)} placeholder="***" secret />
        </div>
        <Field label="Personal Access Token (PAT)" value={cfg.pat ?? ''} onChange={v => set('pat', v)} placeholder="***" secret />
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 12 }}>
          <input
            type="checkbox"
            id="ssl"
            checked={!!cfg.use_ssl}
            onChange={e => set('use_ssl', e.target.checked)}
          />
          <label htmlFor="ssl" style={{ fontSize: 13, color: '#94a3b8', cursor: 'pointer' }}>Use SSL / HTTPS</label>
        </div>
      </div>

      <div style={{ ...card, marginTop: 14 }}>
        <div style={sectionTitle}>Default Target Location</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
          <Field
            label="Catalog / Source"
            value={cfg.catalog ?? ''}
            onChange={v => set('catalog', v)}
            placeholder="my_catalog"
            datalist={namespaces.filter(n => !n.includes('.'))}
          />
          <Field
            label="Schema"
            value={cfg.schema ?? ''}
            onChange={v => set('schema', v)}
            placeholder="my_schema"
          />
        </div>
      </div>

      <div style={{ ...card, marginTop: 14 }}>
        <div style={sectionTitle}>Write Mechanism</div>
        <p style={{ fontSize: 12, color: '#64748b', marginBottom: 12, marginTop: 0 }}>
          Choose how data is written to Dremio.
        </p>
        <div style={{ display: 'flex', gap: 10, marginBottom: 16 }}>
          <ModeCard
            active={cfg.mode !== 'b'}
            label="Dremio SQL"
            badge="Mode A"
            desc="Uses Dremio REST API with SQL MERGE / INSERT statements. Works with all Dremio deployments."
            onClick={() => set('mode', 'a')}
          />
          <ModeCard
            active={cfg.mode === 'b'}
            label="PyIceberg Direct"
            badge="Mode B"
            desc="Writes directly to an Iceberg REST catalog, bypassing Dremio SQL. Faster for large loads."
            onClick={() => set('mode', 'b')}
          />
        </div>

        {cfg.mode === 'b' && (
          <div style={{ borderTop: '1px solid #334155', paddingTop: 14 }}>
            <div style={{ ...sectionTitle, marginBottom: 12 }}>PyIceberg Catalog Settings</div>
            <Field label="Iceberg Catalog URL" value={cfg.iceberg_catalog_url ?? ''} onChange={v => set('iceberg_catalog_url', v)} placeholder="https://catalog.example.com" />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginTop: 12 }}>
              <Field label="Warehouse Path" value={cfg.iceberg_warehouse ?? ''} onChange={v => set('iceberg_warehouse', v)} placeholder="s3://my-bucket/warehouse" />
              <div>
                <label style={lbl}>Catalog Type</label>
                <select
                  value={cfg.iceberg_catalog_type ?? 'rest'}
                  onChange={e => set('iceberg_catalog_type', e.target.value)}
                  style={{ ...inp, cursor: 'pointer' }}
                >
                  <option value="rest">REST</option>
                  <option value="glue">AWS Glue</option>
                  <option value="hive">Hive Metastore</option>
                </select>
              </div>
            </div>
            <div style={{ marginTop: 12 }}>
              <Field label="Bearer Token" value={cfg.iceberg_token ?? ''} onChange={v => set('iceberg_token', v)} placeholder="Bearer token for catalog auth" secret />
            </div>
          </div>
        )}
      </div>

      <div style={{ display: 'flex', gap: 8, marginTop: 16 }}>
        <button onClick={handleSave} disabled={saving} style={btnPrimary}>
          <Save size={14} /> {saving ? 'Saving…' : 'Save'}
        </button>
        <button onClick={handleTest} disabled={testing} style={btnSecondary}>
          <Zap size={14} /> {testing ? 'Testing…' : 'Test Connection'}
        </button>
      </div>
    </div>
  )
}

function ModeCard({ active, label, badge, desc, onClick }: {
  active: boolean; label: string; badge: string; desc: string; onClick: () => void
}) {
  return (
    <div
      onClick={onClick}
      style={{
        flex: 1, padding: '12px 14px', borderRadius: 8, cursor: 'pointer',
        border: `2px solid ${active ? '#34d399' : '#334155'}`,
        background: active ? '#0d2e22' : '#0f172a',
        transition: 'all 0.15s',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: active ? '#34d399' : '#e2e8f0' }}>{label}</span>
        <span style={{
          fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 4,
          background: active ? '#34d399' : '#334155', color: active ? '#0f172a' : '#94a3b8',
        }}>{badge}</span>
      </div>
      <p style={{ margin: 0, fontSize: 12, color: '#64748b', lineHeight: 1.5 }}>{desc}</p>
    </div>
  )
}

function Field({ label, value, onChange, placeholder, secret, datalist }: {
  label: string
  value: string
  onChange: (v: string) => void
  placeholder?: string
  secret?: boolean
  datalist?: string[]
}) {
  const id = `field-${label.replace(/\s+/g, '-').toLowerCase()}`
  return (
    <div>
      <label style={lbl}>{label}</label>
      <input
        id={id}
        list={datalist ? `${id}-list` : undefined}
        type={secret ? 'password' : 'text'}
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder}
        style={inp}
      />
      {datalist && (
        <datalist id={`${id}-list`}>
          {datalist.map(v => <option key={v} value={v} />)}
        </datalist>
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
