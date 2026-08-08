import { adminKey, clearAdminKey } from './auth'
import type {
  ScriptsResponse,
  Job,
  JobStats,
  LogLine,
  DataMetrics,
  ServiceStatus,
  TestResult,
  ImportRegion,
  BoundarySearchResponse,
  DerivedRegion,
  ApiKeySummary,
  CreatedApiKey,
  BillingConfig,
  BillingStatus,
  CreditBalance,
  LedgerEntry,
  PasskeySummary,
  Plan,
  PlansResponse,
  PublicUser,
  SessionSummary,
  UsageReport,
  AbuseSignal,
  AdminUser,
  SuspensionInfo,
  SuspensionKind,
  TermsState,
  ThrottleStats,
} from './types'

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
    this.name = 'ApiError'
  }
}

/**
 * The console authenticates with a session cookie. The bearer header is only
 * sent when signing in with an API key instead — an account key carrying the
 * `admin` scope, which is what automation uses. There is no shared admin
 * secret; `adminAuthHandler` accepts only those two credentials.
 */
function authHeaders(): Record<string, string> {
  return adminKey.value ? { authorization: `Bearer ${adminKey.value}` } : {}
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(path, {
    ...init,
    credentials: 'same-origin',
    headers: { ...authHeaders(), ...(init.headers || {}) },
  })
  if (res.status === 401) {
    // Only the shared key is ours to discard here; a dead session is cleared by
    // the router guard on the next navigation.
    clearAdminKey()
    throw new ApiError(401, 'Unauthorized — sign in again')
  }
  if (res.status === 204) return null as T
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
  accountsEnabled: boolean
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

// ── Boundary catalog ──────────────────────────────────────────────────

export function searchBoundaries(q: string): Promise<BoundarySearchResponse> {
  return request<BoundarySearchResponse>(`/admin/boundaries?q=${encodeURIComponent(q)}`)
}

export function refreshBoundaryCatalog(): Promise<{ count: number; source: string }> {
  return request<{ count: number; source: string }>('/admin/boundaries/refresh', { method: 'POST' })
}

export function resolveBoundary(id: string): Promise<DerivedRegion> {
  return request<DerivedRegion>('/admin/boundaries/resolve', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ id }),
  })
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
  const res = await fetch(`/admin/jobs/${id}/stream`, {
    headers: authHeaders(),
    credentials: 'same-origin',
    signal,
  })
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


// ── Account: profile, sessions and passkeys ───────────────────────────
export function getAccount(): Promise<{ user: PublicUser; terms: TermsState; suspension: SuspensionInfo }> {
  return request<{ user: PublicUser; terms: TermsState; suspension: SuspensionInfo }>('/account')
}

export function updateAccount(name: string): Promise<{ user: PublicUser }> {
  return request<{ user: PublicUser }>('/account', {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name }),
  })
}

export function getSessions(): Promise<SessionSummary[]> {
  return request<SessionSummary[]>('/auth/sessions')
}

export function revokeSession(id: string): Promise<null> {
  return request<null>(`/auth/sessions/${id}`, { method: 'DELETE' })
}

export function signOutOthers(): Promise<null> {
  return request<null>('/auth/sessions?scope=others', { method: 'DELETE' })
}

export function getPasskeys(): Promise<PasskeySummary[]> {
  return request<PasskeySummary[]>('/auth/passkeys/')
}

export function deletePasskey(id: string): Promise<null> {
  return request<null>(`/auth/passkeys/${id}`, { method: 'DELETE' })
}

export function getLinkedProviders(): Promise<{ provider: string; createdAt: string }[]> {
  return request<{ provider: string; createdAt: string }[]>('/auth/oauth/')
}

export function unlinkProvider(provider: string): Promise<null> {
  return request<null>(`/auth/oauth/${provider}`, { method: 'DELETE' })
}

// ── Account: API keys ─────────────────────────────────────────────────
export function getApiKeys(includeRevoked = false): Promise<{ keys: ApiKeySummary[] }> {
  return request<{ keys: ApiKeySummary[] }>(`/account/keys?includeRevoked=${includeRevoked}`)
}

export function createApiKey(payload: {
  name: string
  scopes?: string[]
  allowedOrigins?: string[]
}): Promise<CreatedApiKey> {
  return request<CreatedApiKey>('/account/keys', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export function updateApiKey(
  id: string,
  payload: { name?: string; scopes?: string[]; allowedOrigins?: string[] },
): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(`/account/keys/${id}`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export function revokeApiKey(id: string): Promise<null> {
  return request<null>(`/account/keys/${id}`, { method: 'DELETE' })
}

// ── Account: usage and credits ────────────────────────────────────────
export function getPlans(): Promise<PlansResponse> {
  return request<PlansResponse>('/account/plans')
}

export function getCredits(): Promise<CreditBalance> {
  return request<CreditBalance>('/account/credits')
}

export function getLedger(): Promise<{ entries: LedgerEntry[] }> {
  return request<{ entries: LedgerEntry[] }>('/account/credits/ledger')
}

export function getUsage(from?: string, to?: string): Promise<UsageReport> {
  const params = new URLSearchParams()
  if (from) params.set('from', from)
  if (to) params.set('to', to)
  const query = params.toString()
  return request<UsageReport>(`/account/usage${query ? `?${query}` : ''}`)
}

// ── Billing ───────────────────────────────────────────────────────────
export function getBillingConfig(): Promise<BillingConfig> {
  return request<BillingConfig>('/billing/config')
}

export function getBillingStatus(): Promise<BillingStatus> {
  return request<BillingStatus>('/billing/status')
}

export function startCheckout(payload: {
  plan?: string
  creditPack?: string
}): Promise<{ checkoutUrl: string }> {
  return request<{ checkoutUrl: string }>('/billing/checkout', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export function getPortalUrl(): Promise<{ portalUrl: string }> {
  return request<{ portalUrl: string }>('/billing/portal')
}

export function syncBilling(): Promise<{ planId: string }> {
  return request<{ planId: string }>('/billing/sync', { method: 'POST' })
}

// ── Terms of service ──────────────────────────────────────────────────
export function acceptTerms(version: string): Promise<{ terms: TermsState }> {
  return request<{ terms: TermsState }>('/account/accept-terms', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ version }),
  })
}

// ── Admin moderation ──────────────────────────────────────────────────
export function getAdminUsers(params: {
  search?: string
  status?: 'all' | 'suspended' | 'paid'
  limit?: number
  offset?: number
} = {}): Promise<{ users: AdminUser[]; total: number; limit: number; offset: number; plans: Plan[] }> {
  const query = new URLSearchParams()
  if (params.search) query.set('search', params.search)
  if (params.status && params.status !== 'all') query.set('status', params.status)
  if (params.limit) query.set('limit', String(params.limit))
  if (params.offset) query.set('offset', String(params.offset))
  const suffix = query.toString()
  return request(`/admin/users${suffix ? `?${suffix}` : ''}`)
}

export function suspendUser(
  id: string,
  payload: { reason: string; kind: SuspensionKind; hours?: number },
): Promise<{ suspension: SuspensionInfo }> {
  return request<{ suspension: SuspensionInfo }>(`/admin/users/${id}/suspend`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export function setUserRole(
  id: string,
  role: 'user' | 'admin',
  reason?: string,
): Promise<{ user: { id: string; role: string } }> {
  return request(`/admin/users/${id}/role`, {
    method: 'POST',
    body: JSON.stringify({ role, reason }),
  })
}

export function unsuspendUser(id: string, reason?: string): Promise<{ suspension: SuspensionInfo }> {
  return request<{ suspension: SuspensionInfo }>(`/admin/users/${id}/unsuspend`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ reason }),
  })
}

/**
 * Operator override, independent of the billing provider. This is how an
 * account is put on `demo`, which serves the API unmetered — recorded in the
 * moderation log server-side.
 */
export function setUserPlan(id: string, plan: string, reason?: string): Promise<{ plan: Plan }> {
  return request<{ plan: Plan }>(`/admin/users/${id}/plan`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ plan, reason }),
  })
}

export function getAbuseSignals(
  includeResolved = false,
): Promise<{ signals: AbuseSignal[]; open: number; throttle: ThrottleStats }> {
  return request(`/admin/abuse?includeResolved=${includeResolved}`)
}

export function resolveAbuseSignal(id: string): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(`/admin/abuse/${id}/resolve`, { method: 'POST' })
}
