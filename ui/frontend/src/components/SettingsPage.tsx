import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { CheckCircle, XCircle, Save, Zap, Bell, Lock, Bot } from 'lucide-react'
import {
  getSecrets, saveSecrets, testSecrets,
  getNotifications, saveNotifications, testNotifications,
  getAgentSettings, saveAgentSettings,
  type VaultConfig,
} from '../api/client'

type Tab = 'notifications' | 'vault' | 'agent'

interface NotifConfig {
  notify_email_enabled?: boolean
  notify_email_smtp_host?: string
  notify_email_smtp_port?: string
  notify_email_smtp_user?: string
  notify_email_smtp_pass?: string
  notify_email_from?: string
  notify_email_to?: string
  notify_slack_enabled?: boolean
  notify_slack_webhook_url?: string
}

export default function SettingsPage() {
  const [searchParams] = useSearchParams()
  const [tab, setTab] = useState<Tab>((searchParams.get('tab') as Tab) ?? 'notifications')
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null)

  const flash = (r: { ok: boolean; message: string }) => {
    setResult(r)
    setTimeout(() => setResult(null), 4000)
  }

  return (
    <div style={{ padding: 24, maxWidth: 680 }}>
      <h1 style={{ margin: '0 0 20px', fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>Settings</h1>

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

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20 }}>
        {([
          { id: 'notifications', label: 'Notifications', icon: Bell },
          { id: 'vault',         label: 'Vault Secrets', icon: Lock },
          { id: 'agent',         label: 'AI Agent',      icon: Bot  },
        ] as const).map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              padding: '7px 14px', borderRadius: 7, border: 'none', cursor: 'pointer', fontSize: 13,
              background: tab === id ? '#334155' : 'transparent',
              color: tab === id ? '#f1f5f9' : '#64748b',
              fontWeight: tab === id ? 600 : 400,
            }}
          >
            <Icon size={14} />
            {label}
          </button>
        ))}
      </div>

      {tab === 'notifications' && <NotificationsTab onResult={flash} />}
      {tab === 'vault'         && <VaultTab onResult={flash} />}
      {tab === 'agent'         && <AgentTab onResult={flash} />}
    </div>
  )
}

// ── Notifications tab ─────────────────────────────────────────────────────────

function NotificationsTab({ onResult }: { onResult: (r: any) => void }) {
  const [cfg, setCfg] = useState<NotifConfig>({})
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)

  useEffect(() => {
    getNotifications().then(d => setCfg(d)).catch(() => {})
  }, [])

  const set = (k: keyof NotifConfig, v: any) => setCfg(c => ({ ...c, [k]: v }))

  const handleSave = async () => {
    setSaving(true)
    try { await saveNotifications(cfg); onResult({ ok: true, message: 'Notification settings saved' }) }
    catch (e: any) { onResult({ ok: false, message: String(e) }) }
    finally { setSaving(false) }
  }

  const handleTest = async () => {
    setTesting(true)
    try { const r = await testNotifications(cfg); onResult(r) }
    catch (e: any) { onResult({ ok: false, message: String(e) }) }
    finally { setTesting(false) }
  }

  return (
    <>
      <p style={{ margin: '0 0 16px', color: '#64748b', fontSize: 13 }}>
        Send email or Slack alerts when a load job fails.
      </p>

      {/* Email */}
      <div style={card}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
          <div style={sectionTitle}>Email (SMTP)</div>
          <Toggle
            checked={!!cfg.notify_email_enabled}
            onChange={v => set('notify_email_enabled', v)}
          />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 120px', gap: 10, marginBottom: 10 }}>
          <Field label="SMTP Host" value={cfg.notify_email_smtp_host ?? ''} onChange={v => set('notify_email_smtp_host', v)} placeholder="smtp.gmail.com" />
          <Field label="Port" value={cfg.notify_email_smtp_port ?? '587'} onChange={v => set('notify_email_smtp_port', v)} placeholder="587" />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 10 }}>
          <Field label="SMTP User" value={cfg.notify_email_smtp_user ?? ''} onChange={v => set('notify_email_smtp_user', v)} placeholder="alerts@company.com" />
          <Field label="SMTP Password" value={cfg.notify_email_smtp_pass ?? ''} onChange={v => set('notify_email_smtp_pass', v)} placeholder="***" secret />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          <Field label="From Address" value={cfg.notify_email_from ?? ''} onChange={v => set('notify_email_from', v)} placeholder="dremio-load@company.com" />
          <Field label="To Address" value={cfg.notify_email_to ?? ''} onChange={v => set('notify_email_to', v)} placeholder="data-team@company.com" />
        </div>
      </div>

      {/* Slack */}
      <div style={{ ...card, marginTop: 14 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
          <div style={sectionTitle}>Slack</div>
          <Toggle
            checked={!!cfg.notify_slack_enabled}
            onChange={v => set('notify_slack_enabled', v)}
          />
        </div>
        <Field
          label="Incoming Webhook URL"
          value={cfg.notify_slack_webhook_url ?? ''}
          onChange={v => set('notify_slack_webhook_url', v)}
          placeholder="https://hooks.slack.com/services/…"
        />
      </div>

      <div style={{ display: 'flex', gap: 8, marginTop: 16 }}>
        <button onClick={handleSave} disabled={saving} style={btnPrimary}>
          <Save size={14} /> {saving ? 'Saving…' : 'Save'}
        </button>
        <button onClick={handleTest} disabled={testing} style={btnSecondary}>
          <Zap size={14} /> {testing ? 'Sending…' : 'Send Test Alert'}
        </button>
      </div>
    </>
  )
}

// ── Vault tab ─────────────────────────────────────────────────────────────────

function VaultTab({ onResult }: { onResult: (r: any) => void }) {
  const [vault, setVault] = useState<VaultConfig>({
    url: '', auth_method: 'token', token: '',
    role_id: '', secret_id: '', namespace: '', mount: 'secret',
  })
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)

  useEffect(() => {
    getSecrets().then(v => setVault(c => ({ ...c, ...v }))).catch(() => {})
  }, [])

  const set = (k: keyof VaultConfig, v: string) => setVault(c => ({ ...c, [k]: v }))

  const handleSave = async () => {
    setSaving(true)
    try { await saveSecrets(vault); onResult({ ok: true, message: 'Vault config saved' }) }
    catch (e: any) { onResult({ ok: false, message: String(e) }) }
    finally { setSaving(false) }
  }

  const handleTest = async () => {
    setTesting(true)
    try { const r = await testSecrets(vault); onResult(r) }
    catch (e: any) { onResult({ ok: false, message: String(e) }) }
    finally { setTesting(false) }
  }

  return (
    <>
      <p style={{ margin: '0 0 16px', color: '#64748b', fontSize: 13 }}>
        Use <code style={{ color: '#c084fc' }}>vault:path#field</code> in connection credentials to resolve secrets from HashiCorp Vault.
        Or use <code style={{ color: '#f59e0b' }}>{'${ENV_VAR}'}</code> for environment variables.
      </p>

      <div style={card}>
        <div style={sectionTitle}>Vault Connection</div>
        <Field label="Vault URL" value={vault.url} onChange={v => set('url', v)} placeholder="https://vault.example.com:8200" />

        <div style={{ marginTop: 12 }}>
          <label style={lbl}>Auth Method</label>
          <select style={inp} value={vault.auth_method} onChange={e => set('auth_method', e.target.value)}>
            <option value="token">Token</option>
            <option value="approle">AppRole</option>
          </select>
        </div>

        {vault.auth_method === 'token' ? (
          <div style={{ marginTop: 12 }}>
            <Field label="Token" value={vault.token} onChange={v => set('token', v)} placeholder="***" secret />
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginTop: 12 }}>
            <Field label="Role ID" value={vault.role_id ?? ''} onChange={v => set('role_id', v)} placeholder="role-id" />
            <Field label="Secret ID" value={vault.secret_id ?? ''} onChange={v => set('secret_id', v)} placeholder="***" secret />
          </div>
        )}

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginTop: 12 }}>
          <Field label="Namespace (HCP Vault)" value={vault.namespace ?? ''} onChange={v => set('namespace', v)} placeholder="admin" />
          <Field label="KV Mount" value={vault.mount ?? 'secret'} onChange={v => set('mount', v)} placeholder="secret" />
        </div>
      </div>

      <div style={{ display: 'flex', gap: 8, marginTop: 16 }}>
        <button onClick={handleSave} disabled={saving} style={btnPrimary}>
          <Save size={14} /> {saving ? 'Saving…' : 'Save'}
        </button>
        <button onClick={handleTest} disabled={testing || !vault.url} style={btnSecondary}>
          <Zap size={14} /> {testing ? 'Testing…' : 'Test Connection'}
        </button>
      </div>
    </>
  )
}

// ── AI Agent tab ──────────────────────────────────────────────────────────────

interface AgentConfig {
  agent_enabled?: boolean
  agent_model?: string
  anthropic_api_key?: string
}

function AgentTab({ onResult }: { onResult: (r: any) => void }) {
  const [cfg, setCfg] = useState<AgentConfig>({})
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    getAgentSettings().then(d => setCfg(d)).catch(() => {})
  }, [])

  const set = (k: keyof AgentConfig, v: any) => setCfg(c => ({ ...c, [k]: v }))

  const handleSave = async () => {
    setSaving(true)
    try {
      await saveAgentSettings(cfg)
      onResult({ ok: true, message: 'AI Agent settings saved' })
      window.dispatchEvent(new CustomEvent('agent-settings-changed', { detail: { agent_enabled: !!cfg.agent_enabled } }))
    }
    catch (e: any) { onResult({ ok: false, message: String(e) }) }
    finally { setSaving(false) }
  }

  return (
    <>
      <p style={{ margin: '0 0 16px', color: '#64748b', fontSize: 13 }}>
        Enable the AI Agent to let users manage data pipelines using natural language.
        Powered by Anthropic Claude.
      </p>

      <div style={card}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <div>
            <div style={sectionTitle}>AI Agent</div>
            <div style={{ fontSize: 12, color: '#475569', marginTop: 2 }}>
              Show the Agent tab in the sidebar and allow chat interactions
            </div>
          </div>
          <Toggle
            checked={!!cfg.agent_enabled}
            onChange={v => set('agent_enabled', v)}
          />
        </div>

        <Field
          label="Anthropic API Key"
          value={cfg.anthropic_api_key ?? ''}
          onChange={v => set('anthropic_api_key', v)}
          placeholder="sk-ant-…"
          secret
        />

        <div style={{ marginTop: 12 }}>
          <label style={lbl}>Model</label>
          <select style={inp} value={cfg.agent_model ?? 'claude-opus-4-7'} onChange={e => set('agent_model', e.target.value)}>
            <option value="claude-opus-4-7">Claude Opus 4.7 (most capable)</option>
            <option value="claude-sonnet-4-6">Claude Sonnet 4.6 (balanced)</option>
            <option value="claude-haiku-4-5-20251001">Claude Haiku 4.5 (fastest)</option>
          </select>
        </div>
      </div>

      <div style={{ marginTop: 16 }}>
        <button onClick={handleSave} disabled={saving} style={btnPrimary}>
          <Save size={14} /> {saving ? 'Saving…' : 'Save'}
        </button>
      </div>
    </>
  )
}

// ── Shared sub-components ─────────────────────────────────────────────────────

function Field({ label, value, onChange, placeholder, secret }: {
  label: string; value: string; onChange: (v: string) => void; placeholder?: string; secret?: boolean
}) {
  return (
    <div>
      <label style={lbl}>{label}</label>
      <input type={secret ? 'password' : 'text'} value={value}
        onChange={e => onChange(e.target.value)} placeholder={placeholder} style={inp} />
    </div>
  )
}

function Toggle({ checked, onChange }: { checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <div
      onClick={() => onChange(!checked)}
      style={{
        width: 36, height: 20, borderRadius: 10, cursor: 'pointer', position: 'relative',
        background: checked ? '#34d399' : '#334155', transition: 'background 0.2s',
      }}
    >
      <div style={{
        position: 'absolute', top: 3, left: checked ? 19 : 3,
        width: 14, height: 14, borderRadius: '50%', background: '#fff',
        transition: 'left 0.2s',
      }} />
    </div>
  )
}

const card: React.CSSProperties = {
  background: '#1e293b', borderRadius: 10, padding: 16, border: '1px solid #334155',
}
const sectionTitle: React.CSSProperties = {
  fontSize: 11, fontWeight: 600, color: '#64748b',
  textTransform: 'uppercase', letterSpacing: '0.05em',
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
