export interface Job {
  id: string
  name: string
  source_type?: string
  load_mode?: string
  schedule?: string
  enabled: boolean
  running: boolean
  last_run?: Run
  tables?: string[]
  config?: Record<string, any>
}

export interface Run {
  id: string
  job_id: string
  table_name?: string
  status: string
  rows?: number
  error?: string
  started_at?: string
  finished_at?: string
  duration_s?: number
}

export interface TargetConfig {
  host: string
  port?: number
  username?: string
  password?: string
  pat?: string
  catalog?: string
  schema?: string
  use_ssl?: boolean
  mode?: 'a' | 'b'           // 'a' = Dremio SQL, 'b' = PyIceberg Direct
  iceberg_catalog_url?: string
  iceberg_warehouse?: string
  iceberg_catalog_type?: string  // 'rest' | 'glue' | 'hive'
  iceberg_token?: string
}

export interface VaultConfig {
  url: string
  auth_method: string
  token: string
  role_id?: string
  secret_id?: string
  namespace?: string
  mount?: string
}

const BASE = ""

async function api(method: string, path: string, body?: unknown) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || err.detail || res.statusText)
  }
  return res.json()
}

export const getJobs       = ()                        => api("GET",    "/api/jobs")
export const createJob     = (b: unknown)              => api("POST",   "/api/jobs", b)
export const updateJob     = (id: string, b: unknown)  => api("PUT",    `/api/jobs/${id}`, b)
export const deleteJob     = (id: string)              => api("DELETE", `/api/jobs/${id}`)
export const triggerJob    = (id: string)              => api("POST",   `/api/jobs/${id}/run`)
export const resetJob      = (id: string, table?: string) =>
  api("POST", `/api/jobs/${id}/reset`, table ? { table } : {})
export const setJobEnabled = (id: string, enabled: boolean) =>
  api("PUT", `/api/jobs/${id}/enabled`, { enabled })

export const getRuns    = (jobId?: string, limit = 100) =>
  api("GET", `/api/runs?${jobId ? `job_id=${jobId}&` : ""}limit=${limit}`)
export const getJobRuns = (id: string) => api("GET", `/api/jobs/${id}/runs`)

export const getTarget     = ()           => api("GET",  "/api/target")
export const saveTarget    = (b: unknown) => api("PUT",  "/api/target", b)
export const testTarget    = ()           => api("POST", "/api/target/test")
export const getNamespaces = ()           => api("GET",  "/api/target/namespaces")

export const getSecrets  = ()           => api("GET",  "/api/settings/secrets")
export const saveSecrets = (b: unknown) => api("PUT",  "/api/settings/secrets", b)
export const testSecrets = (b: unknown) => api("POST", "/api/settings/secrets/test", b)

export const getNotifications  = ()           => api("GET",  "/api/settings/notifications")
export const saveNotifications = (b: unknown) => api("PUT",  "/api/settings/notifications", b)
export const testNotifications = (b: unknown) => api("POST", "/api/settings/notifications/test", b)

export const previewCopyInto = (b: unknown) => api("POST", "/api/copy-into/preview", b)
export const runCopyInto     = (b: unknown) => api("POST", "/api/copy-into/run", b)

export const getPipelineOverview = () => api("GET", "/api/pipeline-overview")
export const getHealthSummary    = () => api("GET", "/api/health/summary")

export const getDremioNamespaces  = ()              => api("GET",  "/api/dremio/namespaces")
export const getDremioTables      = (ns: string)    => api("GET",  `/api/dremio/tables?ns=${encodeURIComponent(ns)}`)
export const getDremioPreview     = (table: string) => api("GET",  `/api/dremio/preview?table=${encodeURIComponent(table)}`)
export const getSourceTables      = (jobId: string) => api("GET",  `/api/source/tables?job_id=${encodeURIComponent(jobId)}`)
export const getSourcePreview     = (jobId: string, table: string) =>
  api("GET", `/api/source/preview?job_id=${encodeURIComponent(jobId)}&table=${encodeURIComponent(table)}`)

export const getAgentSettings  = ()           => api("GET", "/api/settings/agent")
export const saveAgentSettings = (b: unknown) => api("PUT", "/api/settings/agent", b)
export const agentChat         = (messages: unknown[]) => api("POST", "/api/agent/chat", { messages })

export const getJob = (id: string) => api("GET", `/api/jobs/${id}`)
