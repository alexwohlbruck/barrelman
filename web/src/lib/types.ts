// Mirrors the backend manifest / job-runner / metrics shapes.

export type ScriptCategory = 'osm' | 'transit' | 'gbfs' | 'search' | 'routing' | 'database' | 'config'
export type DangerLevel = 'safe' | 'caution' | 'destructive'
export type ParamApply = 'flag' | 'env' | 'positional'
export type ParamType = 'string' | 'number' | 'boolean' | 'select'

export interface ScriptParam {
  name: string
  label: string
  type: ParamType
  apply: ParamApply
  flag?: string
  envVar?: string
  default?: string | number | boolean
  options?: { label: string; value: string }[]
  placeholder?: string
  required?: boolean
  secret?: boolean
  description?: string
}

export interface ScriptDef {
  id: string
  name: string
  description: string
  category: ScriptCategory
  danger: DangerLevel
  longRunning: boolean
  confirm: boolean
  exclusive?: boolean
  exec: { kind: 'process'; command: string; args: string[] } | { kind: 'internal'; handler: string }
  params?: ScriptParam[]
  env?: Record<string, string>
  source?: string
  notes?: string
}

export interface ScriptCategoryGroup {
  key: ScriptCategory
  label: string
  scripts: ScriptDef[]
}

export interface ScriptsResponse {
  categories: ScriptCategoryGroup[]
  scripts: ScriptDef[]
}

export type JobStatus = 'running' | 'succeeded' | 'failed' | 'canceled'
export type LogStreamName = 'stdout' | 'stderr' | 'system'

export interface LogLine {
  seq: number
  t: number
  stream: LogStreamName
  text: string
}

export interface Job {
  id: string
  scriptId: string
  scriptName: string
  category: ScriptCategory
  danger: DangerLevel
  status: JobStatus
  params: Record<string, unknown>
  displayCommand: string
  startedAt: number
  endedAt?: number
  durationMs?: number
  exitCode?: number | null
  error?: string
  logCount: number
}

export interface JobStats {
  total: number
  running: number
  succeeded: number
  failed: number
}

export interface DataMetrics {
  database: { sizeBytes: number | null; sizePretty: string | null }
  geoPlaces: {
    total: number | null
    named: number | null
    intersections: number | null
    withParentContext: number | null
    withEmbedding: number | null
    withCodes: number | null
    parentContextCoverage: number | null
    embeddingCoverage: number | null
    approx: boolean
  }
  gtfs: {
    feeds: number | null
    stops: number | null
    routes: number | null
    transfers: number | null
    tripPatterns: number | null
    shapes: number | null
    feedsWithRt: number | null
    lastImport: string | null
  }
  gbfs: { systems: number | null; stations: number | null }
  transit: { stopAreaMembers: number | null }
  accounts: {
    users: number | null
    activeKeys: number | null
    activeThisCycle: number | null
    creditsThisCycle: number | null
    rejectedThisCycle: number | null
    paidAccounts: number | null
  }
}

// ── Import regions (DB-backed region store) ───────────────────────────
export type Bbox = [west: number, south: number, east: number, north: number]

export interface RegionPelias {
  openaddresses: string[]
  wofIds: string[]
  tigerStates: number[]
  countryCode?: string
}

export interface ImportRegion {
  key: string
  label: string
  osmExtracts: string[]
  osmReplication: string[]
  bbox: Bbox
  gtfsRegion: string
  pelias: RegionPelias
  isGlobal: boolean
  enabled: boolean
}

export interface ServiceStatus {
  name: string
  key: string
  status: 'ok' | 'unavailable'
  url?: string
  latencyMs?: number
  message?: string
}

export interface TestResult {
  ok: boolean
  status: number
  statusText: string
  durationMs: number
  contentType?: string
  bytes?: number
  body?: unknown
  error?: string
}

// ── Accounts, keys, usage and billing ─────────────────────────────────
// Mirrors the backend shapes in src/schema/accounts.ts, src/billing/plans.ts
// and the /auth, /account and /billing routes. Keep in sync with those.

export type UserRole = 'user' | 'admin'
export type KeyEnvironment = 'live' | 'test'
export type EndpointGroup =
  | 'tiles'
  | 'places'
  | 'search'
  | 'geocode'
  | 'spatial'
  | 'routing'
  | 'isochrone'
  | 'transit'
export type Scope = EndpointGroup | '*'

export interface PublicUser {
  id: string
  email: string
  name: string | null
  picture: string | null
  role: UserRole
  plan: string
  createdAt: string | null
}

export interface AuthConfig {
  registrationMode: 'open' | 'invite'
  methods: {
    email: boolean
    passkey: boolean
    oauth: { id: string; label: string }[]
  }
}

export interface SessionSummary {
  id: string
  userAgent: string | null
  createdAt: string
  expiresAt: string
  current: boolean
}

export interface PasskeySummary {
  id: string
  name: string
  deviceType: string
  backedUp: boolean
  lastUsedAt: string | null
  createdAt: string
}

export interface ApiKeySummary {
  id: string
  userId: string
  name: string
  prefix: string
  last4: string
  environment: KeyEnvironment
  scopes: string[]
  lastUsedAt: string | null
  revokedAt: string | null
  expiresAt: string | null
  createdAt: string
}

/** Only ever returned once, at creation. */
export interface CreatedApiKey {
  key: string
  record: ApiKeySummary
  warning: string
}

export interface Plan {
  id: string
  name: string
  description: string
  monthlyCredits: number
  requestsPerMinute: number
  overageAllowed: boolean
  overageCentsPerThousand: number
  rank: number
}

export interface CreditBalance {
  plan: Plan
  monthlyCredits: number
  used: number
  purchased: number
  allowanceRemaining: number
  remaining: number
  overageAllowed: boolean
  overage: number
  cycleResetsAt: string
}

export interface LedgerEntry {
  id: string
  amount: number
  kind: 'purchase' | 'grant' | 'adjustment' | 'refund'
  description: string | null
  createdAt: string
}

export interface UsageBucket {
  day: string
  endpoint: string
  requests: number
  credits: number
  rejected: number
}

export interface KeyUsageSummary {
  apiKeyId: string
  requests: number
  credits: number
}

export interface UsageReport {
  from: string
  to: string
  daily: UsageBucket[]
  byKey: KeyUsageSummary[]
}

export interface PlansResponse {
  plans: Plan[]
  creditCosts: Record<EndpointGroup, number>
  scopes: Scope[]
}

export interface BillingProduct {
  planId: string
  productId: string
  name: string
  priceAmount: number
  priceCurrency: string
  interval: string
}

export interface BillingConfig {
  billingEnabled: boolean
  plans: Plan[]
  products: BillingProduct[]
  creditPacks: { productId: string; credits: number }[]
}

export interface BillingStatus {
  billingEnabled: boolean
  plan: Plan
  hasSubscription: boolean
  balance: CreditBalance
}

// ── Moderation (admin) ────────────────────────────────────────────────

export type SuspensionKind =
  | 'tos-violation'
  | 'abuse'
  | 'automated-abuse'
  | 'billing'
  | 'spam'
  | 'operator-request'

export interface SuspensionInfo {
  suspended: boolean
  reason: string | null
  kind: SuspensionKind | null
  until: string | null
  /** Whether the user can plausibly do something about it themselves. */
  appealable: boolean
}

export interface TermsState {
  required: boolean
  version: string
  url: string
  acceptedVersion: string | null
  acceptedAt: string | null
  /** True when the user must accept before they can create API keys. */
  outstanding: boolean
}

export interface AdminUser {
  id: string
  email: string
  name: string | null
  role: UserRole
  plan: string
  createdAt: string
  suspension: SuspensionInfo
  terms: TermsState
}

export type AbuseSignalKind =
  | 'burn-rate'
  | 'error-hammering'
  | 'multi-account'
  | 'quota-exhausted'
  | 'rate-limit-sustained'

export interface AbuseSignal {
  id: string
  userId: string | null
  kind: AbuseSignalKind
  severity: 'low' | 'medium' | 'high'
  detail: Record<string, unknown> | null
  resolvedAt: string | null
  createdAt: string
  email: string | null
  suspendedAt: string | null
}

export interface ThrottleStats {
  trackedAddresses: number
  trackedKeys: number
  trackedAccounts: number
  penalised: number
  inFlight: number
}
