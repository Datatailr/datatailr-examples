// Thin typed client for the admin backend. All URLs are relative so the app
// works under the platform's /job/<env>/<name>/ prefix.

export type DeploymentState =
  | 'saving'
  | 'provisioning_node'
  | 'waiting_node'
  | 'starting'
  | 'running'
  | 'stopping'
  | 'stopped'
  | 'failed'

export interface Event {
  ts: number
  level: 'info' | 'warning' | 'error'
  message: string
  deployment?: string | null
}

export interface DeploymentSpec {
  name: string
  model: string
  environment: string
  instance_type: string
  disk_size: number
  vram: string
  cpu: number
  memory: string
  extra_args: string
  hf_token_secret: string
  description: string
}

export interface Deployment {
  slug: string
  spec: DeploymentSpec
  desired: 'running' | 'stopped'
  state: DeploymentState
  state_since: number
  created_at: number
  created_by: string
  updated_at: number
  service_version: number | null
  run_id: number | null
  node_id: string | null
  node_name: string | null
  last_error: string | null
  pending_model: string | null
  events: Event[]
  service_name: string
  internal_name: string
  internal_url: string
  gateway?: GatewayInfo
}

export interface GatewayInfo {
  path: string
  base_path: string
  public_base_url: string
  internal_base_url: string
  hostname: string
}

export interface NodeJob {
  slug: string
  model: string
  state: DeploymentState
  run_state: string | null
  vram: string
}

export interface Node {
  node_id: string
  id: string
  name: string
  type: string
  status: string
  state: string
  environment: string
  node_class: string
  created_at: number
  disk_size: number
  private_ip: string
  cpu: { allocated_cpus: number; free_cpus: number; total_cpus: number }
  memory: { allocated_memory: number; free_memory: number; max_memory: number; used_memory: number }
  jobs: { name: string; state: string; run_id: number; num_cpus: number; memory_limit: number }[]
  is_gpu: boolean
  ready: boolean
  price_per_hour: number | null
  gpu_memory_gib: number | null
  llm_jobs: NodeJob[]
  managed: { created_for: string; created_at: number; created_by: string; disk_size?: number; instance_type?: string } | null
}

export interface InstanceType {
  name: string
  cpu?: number
  memory?: number
  price?: number
  storage_gib?: number
  has_gpu?: boolean
  gpu_memory_gib?: number | null
  node_class?: string
  family?: string
}

export interface Stats {
  totals: {
    requests?: number
    users?: number
    prompt_tokens?: number
    completion_tokens?: number
    avg_latency_ms?: number
    errors?: number
  }
  by_hour: { bucket: string; requests: number; tokens: number }[]
  by_model: { model: string; requests: number; tokens: number; avg_latency_ms: number }[]
  by_user: { user: string; requests: number; tokens: number; last_seen: string; models: number }[]
}

export interface Overview {
  deployments: Deployment[]
  gpu_nodes: Node[]
  stats: Stats
  events: Event[]
  orchestrator: { last_tick: number; last_error: string | null }
  gateway: GatewayInfo
}

export interface RequestRow {
  request_id: string
  ts: string
  user: string
  model: string
  deployment: string
  endpoint: string
  stream: boolean
  status: number
  latency_ms: number
  prompt_tokens: number | null
  completion_tokens: number | null
  error: string | null
  client: string
  response_preview?: string
  request_json?: string | null
  response_text?: string | null
}

export interface UserRow {
  user: string
  requests: number
  prompt_tokens: number
  completion_tokens: number
  first_seen: string
  last_seen: string
  models: number
  errors: number
}

export interface Me {
  user: string | null
  is_admin: boolean
  environment: string
  domain?: string
}

export interface ConnectInfo {
  user: string
  environment: string
  domain: string
  gateway: GatewayInfo
  models: { id: string; model: string; state: DeploymentState; description: string }[]
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path.replace(/^\//, ''), {
    ...init,
    headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) },
  })
  if (!res.ok) {
    let message = res.statusText
    try {
      const data = await res.json()
      message = typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail ?? data)
    } catch {
      /* ignore */
    }
    throw new ApiError(res.status, message)
  }
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

const qs = (params: Record<string, string | number | boolean | undefined | null>) => {
  const search = new URLSearchParams()
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') search.set(k, String(v))
  })
  const s = search.toString()
  return s ? `?${s}` : ''
}

export const api = {
  me: () => request<Me>('api/me'),
  overview: () => request<Overview>('api/overview'),
  deployments: () => request<Deployment[]>('api/deployments'),
  deployment: (slug: string) => request<Deployment>(`api/deployments/${slug}`),
  createDeployment: (body: Partial<DeploymentSpec> & { start?: boolean }) =>
    request<Deployment>('api/deployments', { method: 'POST', body: JSON.stringify(body) }),
  startDeployment: (slug: string) => request<Deployment>(`api/deployments/${slug}/start`, { method: 'POST' }),
  stopDeployment: (slug: string) => request<Deployment>(`api/deployments/${slug}/stop`, { method: 'POST' }),
  retryDeployment: (slug: string) => request<Deployment>(`api/deployments/${slug}/retry`, { method: 'POST' }),
  changeModel: (slug: string, body: { model: string; extra_args?: string; vram?: string; hf_token_secret?: string }) =>
    request<Deployment>(`api/deployments/${slug}/change-model`, { method: 'POST', body: JSON.stringify(body) }),
  deleteDeployment: (slug: string) => request<{ deleted: string }>(`api/deployments/${slug}`, { method: 'DELETE' }),
  deploymentLogs: (slug: string, stderr = false, lines = 300, build = false) =>
    request<{ run_id: number | null; run_state: string | null; text: string }>(
      `api/deployments/${slug}/logs${qs({ stderr, lines, build })}`,
    ),
  nodes: (gpu = true) => request<Node[]>(`api/nodes${qs({ gpu })}`),
  instanceTypes: (gpu = true) => request<InstanceType[]>(`api/instance-types${qs({ gpu })}`),
  nodeTemplate: () =>
    request<{ instance_type: string | null; environment: string; disk_size: number; min_disk_size: number }>('api/node-template'),
  newNode: (body: { instance_type: string; environment: string; disk_size: number; num_instances: number }) =>
    request<{ result: unknown }>('api/nodes/new', { method: 'POST', body: JSON.stringify(body) }),
  nodeAction: (nodeId: string, action: 'start' | 'stop' | 'terminate', confirmName?: string) =>
    request<{ result: unknown; node: string }>(`api/nodes/${encodeURIComponent(nodeId)}/${action}`, {
      method: 'POST',
      body: action === 'terminate' ? JSON.stringify({ confirm_name: confirmName }) : undefined,
    }),
  requests: (params: Record<string, string | number | undefined>) =>
    request<{ total: number; items: RequestRow[] }>(`api/requests${qs(params)}`),
  requestDetail: (id: string) => request<RequestRow>(`api/requests/${id}`),
  stats: (hours: number) => request<Stats>(`api/stats${qs({ hours })}`),
  users: () => request<UserRow[]>('api/users'),
  refreshLogs: () => request<{ new_files: number }>('api/logs/refresh', { method: 'POST' }),
  connect: () => request<ConnectInfo>('api/connect'),
}

/** Ask the platform for a personal API token (same call the Excel add-in uses). */
export async function generateApiToken(): Promise<string> {
  const res = await fetch('/user.html', { headers: { 'X-Datatailr-Excel-Request': '1' }, credentials: 'include' })
  if (!res.ok) throw new ApiError(res.status, `Token request failed (${res.status})`)
  const token = (await res.text()).trim()
  if (!token) throw new ApiError(500, 'Empty token returned')
  return token
}
