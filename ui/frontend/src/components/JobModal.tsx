import { useEffect, useState } from 'react'
import { X, ChevronRight, ChevronDown, Database, Table2, AlertCircle, ExternalLink } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { createJob, updateJob, getDremioNamespaces, getDremioTables, getTarget, type Job } from '../api/client'

interface Props {
  job: Job | null
  onClose: () => void
  onSaved: () => void
}

// ── Source type definitions ────────────────────────────────────────────────────

const SOURCE_GROUPS = [
  {
    label: 'Object Storage',
    options: [
      { value: 's3',          label: 'Amazon S3' },
      { value: 'azure_blob',  label: 'Azure Blob Storage' },
      { value: 'adls',        label: 'Azure Data Lake Gen2' },
      { value: 'gcs',         label: 'Google Cloud Storage' },
      { value: 's3_compat',   label: 'S3-Compatible (NetApp, Pure, MinIO…)' },
    ],
  },
  {
    label: 'Table Formats',
    options: [
      { value: 'delta',       label: 'Delta Lake' },
      { value: 'hudi',        label: 'Apache Hudi' },
    ],
  },
  {
    label: 'Relational Databases',
    options: [
      { value: 'postgres',    label: 'PostgreSQL' },
      { value: 'mysql',       label: 'MySQL' },
      { value: 'sqlserver',   label: 'SQL Server' },
      { value: 'oracle',      label: 'Oracle' },
      { value: 'snowflake',   label: 'Snowflake' },
    ],
  },
  {
    label: 'NoSQL / Cloud Databases',
    options: [
      { value: 'mongodb',     label: 'MongoDB' },
      { value: 'dynamodb',    label: 'Amazon DynamoDB' },
      { value: 'cosmosdb',    label: 'Azure Cosmos DB' },
      { value: 'spanner',     label: 'Google Cloud Spanner' },
    ],
  },
  {
    label: 'Columnar & Streaming Databases',
    options: [
      { value: 'cassandra',   label: 'Apache Cassandra' },
      { value: 'clickhouse',  label: 'ClickHouse' },
      { value: 'pinot',       label: 'Apache Pinot' },
      { value: 'splunk',      label: 'Splunk' },
    ],
  },
  {
    label: 'SaaS & CRM',
    options: [
      { value: 'salesforce',   label: 'Salesforce' },
      { value: 'hubspot',      label: 'HubSpot' },
      { value: 'zendesk',      label: 'Zendesk' },
    ],
  },
  {
    label: 'Ad Platforms',
    options: [
      { value: 'google_ads',   label: 'Google Ads' },
      { value: 'linkedin_ads', label: 'LinkedIn Ads' },
    ],
  },
  {
    label: 'Dremio Native',
    options: [
      { value: 'copy_into',   label: 'COPY INTO (Dremio SQL)' },
    ],
  },
]

const DEFAULT_TABLES: Record<string, string> = {
  google_ads:   'campaigns, ad_groups, campaign_performance, ad_group_performance, search_terms',
  linkedin_ads: 'campaigns, campaign_groups, ad_analytics',
  salesforce:   'Account, Contact, Opportunity, Lead',
  hubspot:      'contacts, companies, deals, tickets',
  zendesk:      'tickets, users, organizations',
}

const HELP_LINKS: Record<string, { label: string; url: string }> = {
  google_ads:   { label: 'How to get Google Ads API credentials →', url: 'https://developers.google.com/google-ads/api/docs/oauth/overview' },
  linkedin_ads: { label: 'How to get a LinkedIn Ads access token →', url: 'https://learn.microsoft.com/en-us/linkedin/marketing/getting-access' },
}

const LOAD_MODES = [
  { value: 'full',        label: 'Full snapshot (MERGE)' },
  { value: 'ctas',        label: 'Full snapshot (CTAS — drop & recreate)' },
  { value: 'incremental', label: 'Incremental (cursor-based)' },
]

// ── Connection field definitions per source type ───────────────────────────────

type FieldDef = { key: string; label: string; placeholder?: string; secret?: boolean; span?: 2 }

const FIELDS: Record<string, FieldDef[]> = {
  s3: [
    { key: 'bucket',                label: 'Bucket',            placeholder: 'my-bucket' },
    { key: 'prefix',                label: 'Prefix',            placeholder: 'data/' },
    { key: 'region_name',           label: 'Region',            placeholder: 'us-east-1' },
    { key: 'aws_access_key_id',     label: 'Access Key ID',     placeholder: 'AKIAIOSFODNN7…' },
    { key: 'aws_secret_access_key', label: 'Secret Access Key', placeholder: '${AWS_SECRET}', secret: true, span: 2 },
  ],
  azure_blob: [
    { key: 'account_name', label: 'Storage Account Name', placeholder: 'mystorageaccount' },
    { key: 'container',    label: 'Container',             placeholder: 'my-container' },
    { key: 'prefix',       label: 'Prefix',                placeholder: 'data/' },
    { key: 'account_key',  label: 'Account Key',           placeholder: '${AZURE_STORAGE_KEY}', secret: true, span: 2 },
  ],
  adls: [
    { key: 'account_name',  label: 'Storage Account Name',  placeholder: 'mydatalake' },
    { key: 'filesystem',    label: 'Filesystem (Container)', placeholder: 'raw' },
    { key: 'prefix',        label: 'Prefix',                 placeholder: 'ingestion/' },
    { key: 'tenant_id',     label: 'Tenant ID',              placeholder: 'xxxxxxxx-xxxx-…' },
    { key: 'client_id',     label: 'Client ID (App)',        placeholder: 'xxxxxxxx-xxxx-…' },
    { key: 'client_secret', label: 'Client Secret',          placeholder: '${ADLS_SECRET}', secret: true },
  ],
  gcs: [
    { key: 'project',          label: 'GCP Project ID',       placeholder: 'my-project' },
    { key: 'bucket',           label: 'Bucket',               placeholder: 'my-gcs-bucket' },
    { key: 'prefix',           label: 'Prefix',               placeholder: 'data/' },
    { key: 'credentials_file', label: 'Service Account JSON Path', placeholder: '/secrets/sa.json', span: 2 },
  ],
  s3_compat: [
    { key: 'endpoint_url',          label: 'Endpoint URL',      placeholder: 'https://s3.netapp.com' },
    { key: 'bucket',                label: 'Bucket',            placeholder: 'my-bucket' },
    { key: 'prefix',                label: 'Prefix',            placeholder: 'data/' },
    { key: 'region_name',           label: 'Region',            placeholder: 'us-east-1' },
    { key: 'aws_access_key_id',     label: 'Access Key ID',     placeholder: 'my-access-key' },
    { key: 'aws_secret_access_key', label: 'Secret Access Key', placeholder: '${S3_SECRET}', secret: true },
  ],
  postgres: [
    { key: 'host',     label: 'Host',     placeholder: 'localhost' },
    { key: 'port',     label: 'Port',     placeholder: '5432' },
    { key: 'database', label: 'Database', placeholder: 'mydb' },
    { key: 'user',     label: 'User',     placeholder: 'postgres' },
    { key: 'password', label: 'Password', placeholder: '${DB_PASSWORD}', secret: true },
  ],
  mysql: [
    { key: 'host',     label: 'Host',     placeholder: 'localhost' },
    { key: 'port',     label: 'Port',     placeholder: '3306' },
    { key: 'database', label: 'Database', placeholder: 'mydb' },
    { key: 'user',     label: 'User',     placeholder: 'root' },
    { key: 'password', label: 'Password', placeholder: '${DB_PASSWORD}', secret: true },
  ],
  sqlserver: [
    { key: 'host',     label: 'Host',     placeholder: 'localhost' },
    { key: 'port',     label: 'Port',     placeholder: '1433' },
    { key: 'database', label: 'Database', placeholder: 'mydb' },
    { key: 'user',     label: 'User',     placeholder: 'sa' },
    { key: 'password', label: 'Password', placeholder: '${DB_PASSWORD}', secret: true },
  ],
  oracle: [
    { key: 'host',         label: 'Host',         placeholder: 'localhost' },
    { key: 'port',         label: 'Port',         placeholder: '1521' },
    { key: 'service_name', label: 'Service Name', placeholder: 'ORCL' },
    { key: 'user',         label: 'User',         placeholder: 'myuser' },
    { key: 'password',     label: 'Password',     placeholder: '${ORA_PASSWORD}', secret: true },
  ],
  mongodb: [
    { key: 'uri',      label: 'Connection URI', placeholder: 'mongodb://localhost:27017', span: 2 },
    { key: 'database', label: 'Database',       placeholder: 'mydb' },
  ],
  snowflake: [
    { key: 'account',   label: 'Account',                placeholder: 'myaccount.us-east-1' },
    { key: 'user',      label: 'User',                   placeholder: 'myuser' },
    { key: 'password',  label: 'Password',               placeholder: '${SF_PASSWORD}', secret: true },
    { key: 'database',  label: 'Database',               placeholder: 'MY_DB' },
    { key: 'schema',    label: 'Schema',                 placeholder: 'PUBLIC' },
    { key: 'warehouse', label: 'Warehouse',              placeholder: 'COMPUTE_WH' },
    { key: 'role',      label: 'Role (optional)',        placeholder: 'SYSADMIN' },
  ],
  databricks: [
    { key: 'host',      label: 'Workspace Host', placeholder: 'adb-xxx.azuredatabricks.net', span: 2 },
    { key: 'http_path', label: 'HTTP Path',      placeholder: '/sql/1.0/warehouses/xxx', span: 2 },
    { key: 'token',     label: 'Access Token',   placeholder: '${DATABRICKS_TOKEN}', secret: true, span: 2 },
    { key: 'catalog',   label: 'Catalog',        placeholder: 'main' },
    { key: 'schema',    label: 'Schema',         placeholder: 'default' },
  ],
  copy_into: [
    { key: 'source_location', label: 'Source Location (Dremio path)', placeholder: '@my_s3_source/folder/', span: 2 },
    { key: 'file_format',     label: 'File Format',                   placeholder: 'parquet' },
    { key: 'pattern',         label: 'File Pattern (regex)',          placeholder: '.*\\.parquet' },
  ],
  delta: [
    { key: 'table_uri', label: 'Table URI', placeholder: 's3://bucket/delta-tables/', span: 2 },
    { key: 'storage_options.AWS_ACCESS_KEY_ID',     label: 'Access Key ID',     placeholder: 'AKIA…' },
    { key: 'storage_options.AWS_SECRET_ACCESS_KEY', label: 'Secret Access Key', placeholder: '${AWS_SECRET}', secret: true },
    { key: 'storage_options.AWS_REGION',            label: 'Region',            placeholder: 'us-east-1' },
  ],
  hudi: [
    { key: 'table_uri',            label: 'Table URI',       placeholder: 's3://bucket/hudi-tables/', span: 2 },
    { key: 'aws_access_key_id',    label: 'Access Key ID',   placeholder: 'AKIA…' },
    { key: 'aws_secret_access_key',label: 'Secret Access Key',placeholder: '${AWS_SECRET}', secret: true },
    { key: 'region_name',          label: 'Region',          placeholder: 'us-east-1' },
  ],
  salesforce: [
    { key: 'username',       label: 'Username',       placeholder: 'user@company.com' },
    { key: 'password',       label: 'Password',       placeholder: '${SF_PASSWORD}', secret: true },
    { key: 'security_token', label: 'Security Token', placeholder: '${SF_TOKEN}', secret: true },
    { key: 'domain',         label: 'Domain',         placeholder: 'login (prod) or test (sandbox)' },
  ],
  dynamodb: [
    { key: 'region_name',           label: 'Region',          placeholder: 'us-east-1' },
    { key: 'aws_access_key_id',     label: 'Access Key ID',   placeholder: 'AKIA…' },
    { key: 'aws_secret_access_key', label: 'Secret Access Key',placeholder: '${AWS_SECRET}', secret: true },
    { key: 'endpoint_url',          label: 'Endpoint URL',    placeholder: 'http://localhost:8000 (DynamoDB Local)' },
  ],
  cosmosdb: [
    { key: 'endpoint', label: 'Endpoint URL', placeholder: 'https://myaccount.documents.azure.com:443/', span: 2 },
    { key: 'key',      label: 'Account Key',  placeholder: '${COSMOS_KEY}', secret: true, span: 2 },
    { key: 'database', label: 'Database',     placeholder: 'myDatabase' },
  ],
  spanner: [
    { key: 'project',          label: 'GCP Project ID', placeholder: 'my-project' },
    { key: 'instance',         label: 'Instance ID',    placeholder: 'my-instance' },
    { key: 'database',         label: 'Database ID',    placeholder: 'my-database' },
    { key: 'credentials_file', label: 'Service Account JSON Path', placeholder: '/secrets/sa.json', span: 2 },
  ],
  pinot: [
    { key: 'host',     label: 'Broker Host', placeholder: 'localhost' },
    { key: 'port',     label: 'Broker Port', placeholder: '8099' },
    { key: 'scheme',   label: 'Scheme',      placeholder: 'http' },
    { key: 'username', label: 'Username',    placeholder: '(optional)' },
    { key: 'password', label: 'Password',    placeholder: '${PINOT_PASSWORD}', secret: true },
  ],
  splunk: [
    { key: 'host',     label: 'Host',             placeholder: 'splunk.company.com' },
    { key: 'port',     label: 'Management Port',  placeholder: '8089' },
    { key: 'token',    label: 'Auth Token',        placeholder: '${SPLUNK_TOKEN}', secret: true, span: 2 },
    { key: 'username', label: 'Username (alt)',    placeholder: 'admin' },
    { key: 'password', label: 'Password (alt)',    placeholder: '${SPLUNK_PASSWORD}', secret: true },
  ],
  cassandra: [
    { key: 'contact_points', label: 'Contact Points', placeholder: 'host1,host2,host3' },
    { key: 'port',           label: 'CQL Port',        placeholder: '9042' },
    { key: 'keyspace',       label: 'Keyspace',        placeholder: 'my_keyspace' },
    { key: 'username',       label: 'Username',        placeholder: '(optional)' },
    { key: 'password',       label: 'Password',        placeholder: '${CASS_PASSWORD}', secret: true },
    { key: 'local_dc',       label: 'Local DC',        placeholder: 'datacenter1 (optional)' },
  ],
  clickhouse: [
    { key: 'host',     label: 'Host',     placeholder: 'localhost' },
    { key: 'port',     label: 'HTTP Port',placeholder: '8123' },
    { key: 'database', label: 'Database', placeholder: 'default' },
    { key: 'username', label: 'Username', placeholder: 'default' },
    { key: 'password', label: 'Password', placeholder: '${CH_PASSWORD}', secret: true },
  ],
  hubspot: [
    { key: 'token', label: 'Private App Token', placeholder: 'pat-na1-…', secret: true, span: 2 },
  ],
  zendesk: [
    { key: 'subdomain', label: 'Subdomain',   placeholder: 'acme (for acme.zendesk.com)' },
    { key: 'email',     label: 'Agent Email', placeholder: 'you@company.com' },
    { key: 'token',     label: 'API Token',   placeholder: '${ZENDESK_TOKEN}', secret: true, span: 2 },
  ],
  google_ads: [
    { key: 'developer_token',   label: 'Developer Token',              placeholder: 'From ads.google.com → Admin → API Center', secret: true, span: 2 },
    { key: 'client_id',         label: 'OAuth Client ID',              placeholder: 'From Google Cloud Console → Credentials', span: 2 },
    { key: 'client_secret',     label: 'OAuth Client Secret',          placeholder: 'From Google Cloud Console → Credentials', secret: true, span: 2 },
    { key: 'customer_id',       label: 'Customer ID',                  placeholder: '1234567890  (10-digit, no dashes)' },
    { key: 'login_customer_id', label: 'Manager Account ID (optional)', placeholder: 'MCC account ID if using a manager account' },
  ],
  linkedin_ads: [
    { key: 'access_token', label: 'Access Token', placeholder: 'From linkedin.com/developers → your app → Auth tab', secret: true, span: 2 },
    { key: 'account_id',   label: 'Ad Account ID', placeholder: '123456789  (from Campaign Manager URL)' },
  ],
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function JobModal({ job, onClose, onSaved }: Props) {
  const isEdit = !!job
  const navigate = useNavigate()
  const [name, setName] = useState(job?.name ?? '')
  const [sourceType, setSourceType] = useState(job?.source_type ?? 's3')
  const [loadMode, setLoadMode] = useState(job?.load_mode ?? 'incremental')
  const [schedule, setSchedule] = useState(job?.config?.schedule ?? '')
  const [tables, setTables] = useState((job?.tables ?? []).join(', '))
  const [targetTable, setTargetTable] = useState(job?.config?.target_table ?? '')
  const [conn, setConn] = useState<Record<string, string>>(job?.config?.connection ?? {})
  const [onSuccessUrl, setOnSuccessUrl] = useState(job?.config?.on_success_url ?? '')
  const [onFailureUrl, setOnFailureUrl] = useState(job?.config?.on_failure_url ?? '')
  const [tsUrl, setTsUrl] = useState(job?.config?.ts_url ?? '')
  const [tsPipelineToken, setTsPipelineToken] = useState(job?.config?.ts_pipeline_token ?? '')
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')
  const [showCatalog, setShowCatalog] = useState(false)
  const [targetConfigured, setTargetConfigured] = useState<boolean | null>(null)
  const [gOauthEmail, setGOauthEmail] = useState(job?.config?.connection?.refresh_token ? '(previously authorized)' : '')
  const [gOauthPending, setGOauthPending] = useState(false)

  useEffect(() => {
    getTarget().then(t => setTargetConfigured(!!t.host)).catch(() => setTargetConfigured(false))
  }, [])

  const handleBrowse = () => {
    if (targetConfigured === false) {
      setShowCatalog(true) // show the "not configured" state
    } else {
      setShowCatalog(v => !v)
    }
  }

  useEffect(() => {
    if (!isEdit) {
      setConn({})
      if (DEFAULT_TABLES[sourceType]) setTables(DEFAULT_TABLES[sourceType])
    }
  }, [sourceType])

  const fields = FIELDS[sourceType] ?? []
  const isStorage = ['s3', 'azure_blob', 'adls', 'gcs', 's3_compat'].includes(sourceType)
  const isCopyInto = sourceType === 'copy_into'

  const connectWithGoogle = async () => {
    const clientId = conn['client_id'] || ''
    const clientSecret = conn['client_secret'] || ''
    if (!clientId || !clientSecret) {
      setErr('Enter OAuth Client ID and Client Secret before connecting.')
      return
    }
    setGOauthPending(true)
    setErr('')
    try {
      const res = await fetch('/api/oauth/google-ads/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_id: clientId, client_secret: clientSecret }),
      })
      const { auth_url, state, error } = await res.json()
      if (error) { setErr(error); setGOauthPending(false); return }

      const popup = window.open(auth_url, 'gads_oauth', 'width=520,height=640,left=200,top=100')

      const poll = setInterval(async () => {
        if (popup?.closed && gOauthEmail === '') {
          clearInterval(poll)
          setGOauthPending(false)
          return
        }
        try {
          const r = await fetch(`/api/oauth/google-ads/result/${state}`)
          const data = await r.json()
          if (!data.pending) {
            clearInterval(poll)
            setGOauthPending(false)
            if (data.refresh_token) {
              setConn_(  'refresh_token', data.refresh_token)
              setGOauthEmail(data.email || 'your Google account')
            } else {
              setErr('Authorization completed but no refresh token received. Try again.')
            }
          }
        } catch { /* network blip — keep polling */ }
      }, 1500)
    } catch (e: any) {
      setErr(e.message || 'OAuth start failed')
      setGOauthPending(false)
    }
  }

  const handleSave = async () => {
    if (!name.trim()) { setErr('Name is required'); return }
    setSaving(true); setErr('')
    const tableList = tables.split(',').map(t => t.trim()).filter(Boolean)
    const body = {
      name: name.trim(),
      source_type: sourceType,
      load_mode: isCopyInto ? 'copy_into' : loadMode,
      schedule: schedule.trim() || undefined,
      tables: tableList,
      connection: conn,
      target_table: targetTable.trim() || undefined,
      on_success_url: onSuccessUrl.trim() || undefined,
      on_failure_url: onFailureUrl.trim() || undefined,
      ts_url: tsUrl.trim() || undefined,
      ts_pipeline_token: tsPipelineToken.trim() || undefined,
    }
    try {
      if (isEdit) await updateJob(job!.id, body)
      else await createJob(body)
      onSaved()
    } catch (e: any) {
      setErr(e.message || 'Save failed')
    } finally { setSaving(false) }
  }

  const setConn_ = (k: string, v: string) => setConn(c => ({ ...c, [k]: v }))

  return (
    <div style={overlay}>
      <div style={modal}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
          <h2 style={{ margin: 0, fontSize: 17, color: '#f1f5f9' }}>{isEdit ? 'Edit Job' : 'New Load Job'}</h2>
          <button onClick={onClose} style={closeBtn}><X size={16} /></button>
        </div>

        {err && <div style={errBox}>{err}</div>}

        {/* Name */}
        <div style={field}>
          <label style={lbl}>Job Name</label>
          <input style={inp} value={name} onChange={e => setName(e.target.value)} placeholder="My S3 Load" />
        </div>

        {/* Source type */}
        <div style={field}>
          <label style={lbl}>Source Type</label>
          <select style={inp} value={sourceType} onChange={e => setSourceType(e.target.value)}>
            {SOURCE_GROUPS.map(g => (
              <optgroup key={g.label} label={g.label}>
                {g.options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </optgroup>
            ))}
          </select>
        </div>

        {/* Load mode + schedule (hidden for copy_into) */}
        {!isCopyInto && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 14 }}>
            <div style={field}>
              <label style={lbl}>Load Mode</label>
              <select style={inp} value={loadMode} onChange={e => setLoadMode(e.target.value)}>
                {LOAD_MODES.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
            <div style={field}>
              <label style={lbl}>Schedule (cron, optional)</label>
              <input style={inp} value={schedule} onChange={e => setSchedule(e.target.value)} placeholder="0 */6 * * *" />
            </div>
          </div>
        )}

        {/* Target table */}
        <div style={field}>
          <label style={lbl}>Target Table in Dremio</label>
          <div style={{ display: 'flex', gap: 6 }}>
            <input
              style={{ ...inp, flex: 1 }}
              value={targetTable}
              onChange={e => setTargetTable(e.target.value)}
              placeholder='"Catalog"."schema"."table"'
            />
            <button
              type="button"
              onClick={handleBrowse}
              title={targetConfigured === false ? 'Configure target connection first' : 'Browse Dremio catalog'}
              style={{
                display: 'flex', alignItems: 'center', gap: 5,
                padding: '8px 10px', borderRadius: 7, border: '1px solid #334155',
                background: showCatalog ? '#1e3a2f' : '#0f172a',
                color: showCatalog ? '#34d399' : '#64748b', cursor: 'pointer',
                fontSize: 12, whiteSpace: 'nowrap', flexShrink: 0,
              }}
            >
              <Database size={13} /> Browse
            </button>
          </div>
          {showCatalog && targetConfigured === false && (
            <div style={{
              marginTop: 6, padding: '12px 14px', borderRadius: 8,
              border: '1px solid #854d0e', background: '#1c1400',
              display: 'flex', alignItems: 'flex-start', gap: 10,
            }}>
              <AlertCircle size={15} color="#fbbf24" style={{ flexShrink: 0, marginTop: 1 }} />
              <div>
                <div style={{ fontSize: 13, color: '#fde68a', fontWeight: 600, marginBottom: 4 }}>
                  Dremio target not configured yet
                </div>
                <div style={{ fontSize: 12, color: '#92400e', marginBottom: 8 }}>
                  Set up your Dremio connection on the Target page first, then come back to browse your catalog.
                </div>
                <button
                  onClick={() => { onClose(); navigate('/target') }}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: 5,
                    padding: '6px 12px', borderRadius: 6, border: 'none', cursor: 'pointer',
                    background: '#fbbf24', color: '#0f172a', fontWeight: 600, fontSize: 12,
                  }}
                >
                  <ExternalLink size={12} /> Go to Target Settings
                </button>
              </div>
            </div>
          )}
          {showCatalog && targetConfigured !== false && (
            <CatalogPicker
              onSelect={t => { setTargetTable(t); setShowCatalog(false) }}
              onClose={() => setShowCatalog(false)}
              onConnectionError={() => { setShowCatalog(false); navigate('/target') }}
            />
          )}
        </div>

        {/* Tables / Collections (DB sources only) */}
        {!isStorage && !isCopyInto && (
          <div style={field}>
            <label style={lbl}>Tables / Collections (comma-separated)</label>
            <input style={inp} value={tables} onChange={e => setTables(e.target.value)} placeholder="orders, customers, products" />
          </div>
        )}

        {/* Connection fields */}
        {fields.length > 0 && (
          <>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', ...sectionDivider }}>
              <span>Connection</span>
              {HELP_LINKS[sourceType] && (
                <a href={HELP_LINKS[sourceType].url} target="_blank" rel="noopener noreferrer"
                  style={{ fontSize: 11, color: '#38bdf8', textDecoration: 'none', fontWeight: 400, textTransform: 'none', letterSpacing: 0 }}>
                  {HELP_LINKS[sourceType].label}
                </a>
              )}
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {fields.map(f => (
                <div key={f.key} style={f.span === 2 ? { gridColumn: '1 / -1' } : {}}>
                  <label style={lbl}>{f.label}</label>
                  <input
                    style={inp}
                    type={f.secret ? 'password' : 'text'}
                    value={conn[f.key] ?? ''}
                    onChange={e => setConn_(f.key, e.target.value)}
                    placeholder={f.placeholder}
                  />
                </div>
              ))}
            </div>

            {/* Google Ads: OAuth connect button */}
            {sourceType === 'google_ads' && (
              <div style={{ gridColumn: '1 / -1', marginTop: 4 }}>
                {gOauthEmail ? (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', borderRadius: 8, background: '#0f2a1a', border: '1px solid #166534' }}>
                    <span style={{ fontSize: 18 }}>✓</span>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 13, color: '#34d399', fontWeight: 600 }}>Connected to Google Ads</div>
                      <div style={{ fontSize: 12, color: '#64748b', marginTop: 2 }}>{gOauthEmail}</div>
                    </div>
                    <button
                      type="button"
                      onClick={() => { setGOauthEmail(''); setConn_('refresh_token', '') }}
                      style={{ fontSize: 12, color: '#64748b', background: 'none', border: 'none', cursor: 'pointer', textDecoration: 'underline' }}
                    >
                      Re-authorize
                    </button>
                  </div>
                ) : (
                  <button
                    type="button"
                    onClick={connectWithGoogle}
                    disabled={gOauthPending}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 10,
                      width: '100%', padding: '11px 16px', borderRadius: 8,
                      border: '1px solid #334155', cursor: gOauthPending ? 'wait' : 'pointer',
                      background: gOauthPending ? '#1e293b' : '#fff',
                      color: gOauthPending ? '#64748b' : '#3c4043',
                      fontSize: 14, fontWeight: 600, justifyContent: 'center',
                      opacity: gOauthPending ? 0.7 : 1,
                    }}
                  >
                    {!gOauthPending && (
                      <svg width="18" height="18" viewBox="0 0 24 24">
                        <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
                        <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
                        <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/>
                        <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
                      </svg>
                    )}
                    {gOauthPending ? 'Waiting for Google authorization…' : 'Connect with Google'}
                  </button>
                )}
              </div>
            )}

            {/* S3-compatible: path-style toggle */}
            {sourceType === 's3_compat' && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 10 }}>
                <input
                  type="checkbox" id="path_style"
                  checked={conn.path_style === 'true'}
                  onChange={e => setConn_('path_style', String(e.target.checked))}
                />
                <label htmlFor="path_style" style={{ fontSize: 13, color: '#94a3b8', cursor: 'pointer' }}>
                  Use path-style URLs (required for most S3-compatible storage)
                </label>
              </div>
            )}
          </>
        )}

        {/* File format (object storage sources only) */}
        {isStorage && (
          <>
            <div style={sectionDivider}>File Format</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 12 }}>
              <div>
                <label style={lbl}>Format</label>
                <select
                  style={inp}
                  value={conn.file_format ?? ''}
                  onChange={e => setConn_(e.target.value ? 'file_format' : 'file_format', e.target.value)}
                >
                  <option value="">Auto-detect (from file extension)</option>
                  <option value="parquet">Parquet</option>
                  <option value="avro">Avro</option>
                  <option value="csv">CSV</option>
                  <option value="json">JSON</option>
                  <option value="ndjson">NDJSON / JSON Lines</option>
                </select>
              </div>
              {(conn.file_format === 'csv' || !conn.file_format) && (
                <div>
                  <label style={lbl}>CSV Delimiter</label>
                  <select style={inp} value={conn.csv_delimiter ?? ','} onChange={e => setConn_('csv_delimiter', e.target.value)}>
                    <option value=",">Comma ( , )</option>
                    <option value="\t">Tab</option>
                    <option value="|">Pipe ( | )</option>
                    <option value=";">Semicolon ( ; )</option>
                  </select>
                </div>
              )}
            </div>
            {(conn.file_format === 'csv' || !conn.file_format) && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                <input
                  type="checkbox"
                  id="csv_has_header"
                  checked={conn.csv_has_header !== 'false'}
                  onChange={e => setConn_('csv_has_header', e.target.checked ? 'true' : 'false')}
                />
                <label htmlFor="csv_has_header" style={{ fontSize: 13, color: '#94a3b8', cursor: 'pointer' }}>
                  First row is a header
                </label>
              </div>
            )}
          </>
        )}

        {/* Hooks */}
        <div style={sectionDivider}>Post-Run Hooks</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 14 }}>
          <div>
            <label style={lbl}>On Success URL</label>
            <input style={inp} value={onSuccessUrl} onChange={e => setOnSuccessUrl(e.target.value)}
              placeholder="https://…/api/notify" />
          </div>
          <div>
            <label style={lbl}>On Failure URL</label>
            <input style={inp} value={onFailureUrl} onChange={e => setOnFailureUrl(e.target.value)}
              placeholder="https://…/api/notify" />
          </div>
        </div>

        {/* Transform Studio integration */}
        <div style={sectionDivider}>Transform Studio — Trigger Pipeline on Success</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 14 }}>
          <div>
            <label style={lbl}>Transform Studio URL</label>
            <input style={inp} value={tsUrl} onChange={e => setTsUrl(e.target.value)}
              placeholder="http://localhost:8000" />
          </div>
          <div>
            <label style={lbl}>Pipeline Webhook Token</label>
            <input style={inp} value={tsPipelineToken} onChange={e => setTsPipelineToken(e.target.value)}
              placeholder="Paste from TS pipeline Webhooks tab" />
          </div>
        </div>
        <div style={{ fontSize: 12, color: '#475569', marginBottom: 14 }}>
          After a successful load, Dremio Load will call the Transform Studio webhook to trigger the linked pipeline.
        </div>

        {/* Actions */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 22 }}>
          <button onClick={onClose} style={btnGhost}>Cancel</button>
          <button onClick={handleSave} disabled={saving} style={btnPrimary}>
            {saving ? 'Saving…' : isEdit ? 'Update Job' : 'Create Job'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Inline Dremio catalog picker ──────────────────────────────────────────────

function CatalogPicker({ onSelect, onClose, onConnectionError }: {
  onSelect: (table: string) => void
  onClose: () => void
  onConnectionError?: () => void
}) {
  const [namespaces, setNamespaces] = useState<string[]>([])
  const [expanded, setExpanded] = useState<Record<string, string[]>>({})
  const [loading, setLoading] = useState(true)
  const [loadingNs, setLoadingNs] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [isConnErr, setIsConnErr] = useState(false)

  useEffect(() => {
    getDremioNamespaces()
      .then(setNamespaces)
      .catch(e => {
        setError(e.message)
        // Heuristic: connection refused / timeout = config problem
        if (/connect|refused|timeout|ECONNREFUSED/i.test(e.message)) setIsConnErr(true)
      })
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

  const pick = (ns: string, tbl: string) => {
    onSelect(`"${ns}"."${tbl}"`)
  }

  return (
    <div style={{
      marginTop: 6, border: '1px solid #334155', borderRadius: 8,
      background: '#0f172a', maxHeight: 240, overflowY: 'auto',
    }}>
      {loading && <div style={{ padding: '12px 14px', color: '#64748b', fontSize: 12 }}>Connecting to Dremio…</div>}
      {error && (
        <div style={{ padding: '12px 14px' }}>
          <div style={{ color: '#f87171', fontSize: 12, marginBottom: isConnErr ? 8 : 0 }}>{error}</div>
          {isConnErr && (
            <button
              onClick={onConnectionError}
              style={{
                display: 'inline-flex', alignItems: 'center', gap: 5,
                padding: '5px 10px', borderRadius: 5, border: 'none', cursor: 'pointer',
                background: '#fbbf24', color: '#0f172a', fontWeight: 600, fontSize: 11,
              }}
            >
              <ExternalLink size={11} /> Fix Target Settings
            </button>
          )}
        </div>
      )}
      {!loading && !error && namespaces.length === 0 && (
        <div style={{ padding: '12px 14px', color: '#64748b', fontSize: 12 }}>No namespaces found. Check your target connection.</div>
      )}
      {namespaces.map(ns => (
        <div key={ns}>
          <div
            onClick={() => toggleNs(ns)}
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              padding: '7px 12px', cursor: 'pointer', fontSize: 12,
              color: '#94a3b8', borderBottom: '1px solid #1e293b',
            }}
            onMouseEnter={e => (e.currentTarget.style.background = '#1e293b')}
            onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
          >
            {expanded[ns] ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            <Database size={12} color="#64748b" />
            <span>{ns}</span>
            {loadingNs === ns && <span style={{ marginLeft: 'auto', color: '#64748b' }}>…</span>}
          </div>
          {expanded[ns]?.map(tbl => (
            <div
              key={tbl}
              onClick={() => pick(ns, tbl)}
              style={{
                display: 'flex', alignItems: 'center', gap: 6,
                padding: '6px 12px 6px 28px', cursor: 'pointer', fontSize: 12,
                color: '#cbd5e1', borderBottom: '1px solid #1e293b11',
              }}
              onMouseEnter={e => (e.currentTarget.style.background = '#1e3a2f')}
              onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
            >
              <Table2 size={11} color="#64748b" />
              <span>{tbl}</span>
              <span style={{ marginLeft: 'auto', fontSize: 11, color: '#334155' }}>select →</span>
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

const overlay: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.65)',
  display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
}
const modal: React.CSSProperties = {
  background: '#1e293b', borderRadius: 12, padding: 24, width: 660,
  maxHeight: '90vh', overflowY: 'auto', border: '1px solid #334155',
}
const closeBtn: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', color: '#64748b', padding: 4,
}
const field: React.CSSProperties = { marginBottom: 12 }
const lbl: React.CSSProperties = { display: 'block', fontSize: 12, color: '#94a3b8', marginBottom: 4 }
const inp: React.CSSProperties = {
  width: '100%', boxSizing: 'border-box',
  background: '#0f172a', border: '1px solid #334155', borderRadius: 7,
  padding: '8px 10px', color: '#e2e8f0', fontSize: 13, outline: 'none',
}
const sectionDivider: React.CSSProperties = {
  fontSize: 11, fontWeight: 600, color: '#64748b',
  textTransform: 'uppercase', letterSpacing: '0.05em',
  margin: '16px 0 10px', paddingTop: 14, borderTop: '1px solid #1e293b',
}
const errBox: React.CSSProperties = {
  background: '#450a0a', color: '#f87171', borderRadius: 8,
  padding: '10px 14px', marginBottom: 14, fontSize: 13,
}
const btnPrimary: React.CSSProperties = {
  padding: '8px 18px', borderRadius: 7, border: 'none', cursor: 'pointer',
  background: '#34d399', color: '#0f172a', fontWeight: 600, fontSize: 13,
}
const btnGhost: React.CSSProperties = {
  padding: '8px 14px', borderRadius: 7, border: '1px solid #334155', cursor: 'pointer',
  background: 'transparent', color: '#94a3b8', fontSize: 13,
}
