import { adminKey, clearKey } from './auth'
import type {
  ScriptsResponse,
  Job,
  JobStats,
  LogLine,
  DataMetrics,
  ServiceStatus,
  TestResult,
  ImportRegion,
} from './types'

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
    this.name = 'ApiError'
  }
}

function authHeaders(): Record<string, string> {
  return adminKey.value ? { authorization: `Bearer ${adminKey.value}` } : {}
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers || {}) },
  })
  if (res.status === 401) {
    clearKey()
    throw new ApiError(401, 'Unauthorized — admin key invalid or missing')
  }
  const contentType = res.headers.get('content-type') || ''
  const payload = contentType.includes('application/json') ? await res.json() : await res.text()
  if (!res.ok) {
    const message = typeof payload === 'object' && payload && 'error' in payload ? (payload as any).error : res.statusText
    throw new ApiError(res.status, message)
  }
  return payload as T
}

// ── Config / auth ─────────────────────────────────────────────────────
export interface ConsoleConfig {
  authRequired: boolean
  usingDedicatedAdminKey: boolean
  apiName: string
  version: string
}

export function getConfig(): Promise<ConsoleConfig> {
  return request<ConsoleConfig>('/admin/config')
}

export async function verifyKey(key: string): Promise<boolean> {
  const res = await fetch('/admin/verify', { headers: { authorization: `Bearer ${key}` } })
  return res.ok
}

// ── Scripts & jobs ────────────────────────────────────────────────────
export function getScripts(): Promise<ScriptsResponse> {
  return request<ScriptsResponse>('/admin/scripts')
}

export function runScript(id: string, params: Record<string, unknown>): Promise<{ job: Job }> {
  return request<{ job: Job }>(`/admin/scripts/${id}/run`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ params }),
  })
}

export function getJobs(): Promise<{ jobs: Job[]; stats: JobStats }> {
  return request<{ jobs: Job[]; stats: JobStats }>('/admin/jobs')
}

export function getJob(id: string): Promise<{ job: Job; logs: LogLine[] }> {
  return request<{ job: Job; logs: LogLine[] }>(`/admin/jobs/${id}`)
}

export function cancelJob(id: string): Promise<{ ok: boolean; message: string }> {
  return request<{ ok: boolean; message: string }>(`/admin/jobs/${id}/cancel`, { method: 'POST' })
}

// ── Import regions ────────────────────────────────────────────────────
export type RegionPayload = Omit<ImportRegion, 'isGlobal'>

export function getRegions(): Promise<{ regions: ImportRegion[] }> {
  return request<{ regions: ImportRegion[] }>('/admin/regions')
}

export function createRegion(region: RegionPayload): Promise<{ region: ImportRegion }> {
  return request<{ region: ImportRegion }>('/admin/regions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(region),
  })
}

export function updateRegion(key: string, region: Omit<RegionPayload, 'key'>): Promise<{ region: ImportRegion }> {
  return request<{ region: ImportRegion }>(`/admin/regions/${encodeURIComponent(key)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(region),
  })
}

export function deleteRegion(key: string): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(`/admin/regions/${encodeURIComponent(key)}`, { method: 'DELETE' })
}

// ── Metrics & services ────────────────────────────────────────────────
export function getMetrics(): Promise<DataMetrics> {
  return request<DataMetrics>('/admin/metrics')
}

export function getServices(): Promise<{ services: ServiceStatus[] }> {
  return request<{ services: ServiceStatus[] }>('/admin/services')
}

// ── Endpoint tester ───────────────────────────────────────────────────
export interface TestRequest {
  method: string
  path: string
  query?: string
  body?: string
  auth?: 'api' | 'admin' | 'none'
}

export function testEndpoint(req: TestRequest): Promise<TestResult> {
  return request<TestResult>('/admin/test-endpoint', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(req),
  })
}

// ── Live job log stream (SSE parsed from a fetch body reader) ──────────
export type JobStreamEvent =
  | { type: 'log'; line: LogLine }
  | { type: 'status'; job: Job }

export async function streamJob(
  id: string,
  onEvent: (evt: JobStreamEvent) => void,
  signal: AbortSignal,
): Promise<void> {
  const res = await fetch(`/admin/jobs/${id}/stream`, { headers: authHeaders(), signal })
  if (!res.ok || !res.body) throw new ApiError(res.status, 'Failed to open log stream')

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buf = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buf += decoder.decode(value, { stream: true })

    let sepIndex: number
    // SSE events are separated by a blank line.
    while ((sepIndex = buf.indexOf('\n\n')) >= 0) {
      const raw = buf.slice(0, sepIndex)
      buf = buf.slice(sepIndex + 2)
      let event = 'message'
      let data = ''
      for (const line of raw.split('\n')) {
        if (line.startsWith('event:')) event = line.slice(6).trim()
        else if (line.startsWith('data:')) data += line.slice(5).trim()
      }
      if (!data) continue
      try {
        const parsed = JSON.parse(data)
        if (event === 'log') onEvent({ type: 'log', line: parsed as LogLine })
        else if (event === 'status') onEvent({ type: 'status', job: parsed as Job })
      } catch {
        /* ignore malformed chunk */
      }
    }
  }
}
