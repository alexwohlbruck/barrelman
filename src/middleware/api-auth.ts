/**
 * Authentication and metering for the metered public API.
 *
 * One `onBeforeHandle` guard per route group does four things in order:
 * identify the caller, check the key's scopes, check the rate limit, then check
 * and charge credits. Charging up front means the decision and the spend cannot
 * disagree; a 5xx refunds afterwards, because the customer should not pay for
 * our outage.
 *
 * Three kinds of caller are recognised:
 *
 *   - An account key (`brm_live_…` / `brm_test_…`) — metered and rate-limited.
 *   - The legacy shared secret in `BARRELMAN_API_KEY` — unmetered. This is how
 *     Parchment's own server calls barrelman, and how existing deployments keep
 *     working; it is a service credential, not a customer.
 *   - Nobody, when no auth is configured at all — open, for local development.
 *
 * NOTE ON ATTACHMENT: these are plain handlers, attached with
 * `.onBeforeHandle(...)` directly on the instance that declares the routes.
 * Do not wrap them in a plugin and `.use()` it — Elysia scopes a plugin's
 * lifecycle hooks to the plugin instance, which silently leaves the parent's
 * routes unguarded. That is not hypothetical: /brands, /children, /contains and
 * /geocode were reachable with no key at all because they used `.use()`.
 */
import { creditCost, scopeAllows, type EndpointGroup } from '../billing/plans'
import { resolveApiKey, type ResolvedKey } from '../services/api-keys.service'
import { checkQuota } from '../services/credits.service'
import { recordUsage, refundUsage } from '../services/usage.service'
import { getPlan } from '../billing/plans'
import { accountsEnabled } from '../config/accounts.config'
import { clientIp } from '../lib/rate-limit'

/** Prefix that marks a customer key, as opposed to the shared service secret. */
const KEY_PREFIX = 'brm_'

export interface ApiCaller {
  kind: 'account' | 'service' | 'anonymous'
  userId?: string
  keyId?: string
  plan?: string
  /** Billing groups this key may call; `['*']` for all. */
  scopes?: string[]
  isTest?: boolean
}

/** Per-request billing state, carried from the guard to the after-handler. */
export interface MeteringContext {
  caller: ApiCaller
  group: EndpointGroup
  credits: number
  charged: boolean
}

// ── Rate limiting ───────────────────────────────────────────────────────

/**
 * Per-account request ceiling, independent of credits: a burst can exhaust a
 * month's allowance in seconds, and rate limiting is what keeps one customer
 * from degrading the service for everyone else.
 *
 * In-memory and therefore per-replica. With multiple API replicas the effective
 * limit is the configured one times the replica count; that is fine for
 * protecting the process, and the credit ledger — which is in Postgres — remains
 * the accurate record for billing.
 */
interface RateBucket {
  count: number
  resetAt: number
}

const rateBuckets = new Map<string, RateBucket>()

function withinRateLimit(key: string, limit: number): boolean {
  const now = Date.now()
  const bucket = rateBuckets.get(key)
  if (!bucket || bucket.resetAt <= now) {
    rateBuckets.set(key, { count: 1, resetAt: now + 60_000 })
    return true
  }
  bucket.count += 1
  return bucket.count <= limit
}

/** Drop stale buckets so a long-lived process doesn't accumulate them. */
export function pruneRateBuckets(): void {
  const now = Date.now()
  for (const [key, bucket] of rateBuckets) {
    if (bucket.resetAt <= now) rateBuckets.delete(key)
  }
}

export function clearRateBuckets(): void {
  rateBuckets.clear()
}

// ── Caller identification ───────────────────────────────────────────────

function bearerToken(headers: Record<string, string | undefined>): string | null {
  const authorization = headers['authorization']
  if (!authorization) return null
  const trimmed = authorization.trim()
  if (!trimmed.toLowerCase().startsWith('bearer ')) return null
  return trimmed.slice(7).trim() || null
}

/**
 * Also accept the key as a query parameter, for endpoints a browser reaches
 * directly and cannot set headers on — map tiles, mainly, which MapLibre
 * fetches itself. `token` is accepted alongside `api_key` because the tile
 * routes have used that name since before accounts existed.
 *
 * A key in a URL ends up in logs and history, so this is documented as the
 * option for tile URLs specifically, where there is no alternative.
 */
function queryKey(request: Request): string | null {
  try {
    const params = new URL(request.url).searchParams
    return params.get('api_key') ?? params.get('token')
  } catch {
    return null
  }
}

export interface IdentifyResult {
  caller: ApiCaller
  error?: { status: number; body: { error: string; docs?: string } }
}

const DOCS_URL = '/docs'

/**
 * Collaborators, injectable so the guard can be tested without Postgres.
 * `apiAuth()` closes over these at construction time.
 */
export interface ApiAuthDeps {
  resolveApiKey: typeof resolveApiKey
  checkQuota: typeof checkQuota
  recordUsage: typeof recordUsage
}

const defaultDeps: ApiAuthDeps = { resolveApiKey, checkQuota, recordUsage }

export async function identifyCaller(
  headers: Record<string, string | undefined>,
  request: Request,
  deps: Pick<ApiAuthDeps, 'resolveApiKey'> = defaultDeps,
): Promise<IdentifyResult> {
  const serviceKey = process.env.BARRELMAN_API_KEY
  const presented = bearerToken(headers) ?? queryKey(request)

  if (!presented) {
    // No credential presented. `BARRELMAN_API_KEY` unset means "no auth
    // configured", which every guard in this codebase treats as open local
    // development — a fresh clone must be usable with no configuration at all.
    // A public deployment sets the key; `assertAuthConfigured()` below warns
    // loudly on any instance that enables accounts without one.
    if (!serviceKey) return { caller: { kind: 'anonymous' } }

    return {
      caller: { kind: 'anonymous' },
      error: {
        status: 401,
        body: {
          error: accountsEnabled
            ? 'An API key is required. Create one in the console at /console.'
            : 'Missing Authorization header',
          docs: DOCS_URL,
        },
      },
    }
  }

  // The shared service secret is checked first and is never metered.
  if (serviceKey && presented === serviceKey) {
    return { caller: { kind: 'service' } }
  }

  if (!presented.startsWith(KEY_PREFIX)) {
    return {
      caller: { kind: 'anonymous' },
      error: { status: 401, body: { error: 'Invalid API key', docs: DOCS_URL } },
    }
  }

  if (!accountsEnabled) {
    return {
      caller: { kind: 'anonymous' },
      error: { status: 401, body: { error: 'Invalid API key', docs: DOCS_URL } },
    }
  }

  const resolved = await deps.resolveApiKey(presented)
  if (!resolved) {
    // One message for unknown, revoked, expired and suspended alike — telling
    // a caller which of those applies to a key they do not own is a probe.
    return {
      caller: { kind: 'anonymous' },
      error: { status: 401, body: { error: 'Invalid or revoked API key', docs: DOCS_URL } },
    }
  }

  return { caller: toCaller(resolved) }
}

function toCaller(resolved: ResolvedKey): ApiCaller {
  return {
    kind: 'account',
    userId: resolved.userId,
    keyId: resolved.keyId,
    plan: resolved.plan,
    scopes: resolved.scopes,
    isTest: resolved.isTest,
  }
}

// ── The guard ───────────────────────────────────────────────────────────

interface GuardContext {
  headers: Record<string, string | undefined>
  request: Request
  set: { status?: number | string; headers: Record<string, string | number> }
  store?: Record<string, unknown>
}

/**
 * Build the `onBeforeHandle` guard for one billing group.
 *
 * Usage:
 *     new Elysia().onBeforeHandle(apiAuth('search')).get('/search', …)
 */
export function apiAuth(group: EndpointGroup, overrides: Partial<ApiAuthDeps> = {}) {
  const cost = creditCost(group)
  const deps = { ...defaultDeps, ...overrides }

  return async function apiAuthHandler(context: GuardContext) {
    const { headers, request, set } = context

    const { caller, error } = await identifyCaller(headers, request, deps)
    if (error) {
      set.status = error.status
      return error.body
    }

    // Unmetered callers are done: the service credential and the open
    // development mode both skip scopes, limits and credits.
    if (caller.kind !== 'account' || !caller.userId) {
      stash(context, { caller, group, credits: 0, charged: false })
      return
    }

    const plan = getPlan(caller.plan)

    if (!scopeAllows(caller.scopes ?? ['*'], group)) {
      set.status = 403
      return {
        error: `This API key is not permitted to call ${group} endpoints`,
        scope: group,
        docs: DOCS_URL,
      }
    }

    if (!withinRateLimit(caller.userId, plan.requestsPerMinute)) {
      set.status = 429
      set.headers['retry-after'] = '60'
      deps.recordUsage({ userId: caller.userId, apiKeyId: caller.keyId, endpoint: group, credits: 0, rejected: true })
      return {
        error: `Rate limit exceeded — the ${plan.name} plan allows ${plan.requestsPerMinute} requests per minute`,
        docs: DOCS_URL,
      }
    }

    // Test keys exercise the full path — auth, scopes, limits, responses — but
    // never spend credits, so integration suites cost nothing to run.
    if (caller.isTest) {
      stash(context, { caller, group, credits: 0, charged: false })
      set.headers['x-barrelman-credits-charged'] = '0'
      return
    }

    const decision = await deps.checkQuota(caller.userId, cost)
    if (!decision.allowed) {
      set.status = 402
      deps.recordUsage({ userId: caller.userId, apiKeyId: caller.keyId, endpoint: group, credits: 0, rejected: true })
      return {
        error:
          'Credit allowance exhausted for this billing period. ' +
          'Upgrade your plan or add credits at /console/billing.',
        remaining: decision.balance.remaining,
        resetsAt: decision.balance.cycleResetsAt,
        docs: DOCS_URL,
      }
    }

    deps.recordUsage({ userId: caller.userId, apiKeyId: caller.keyId, endpoint: group, credits: cost })
    stash(context, { caller, group, credits: cost, charged: true })

    set.headers['x-barrelman-credits-charged'] = String(cost)
    if (decision.overage) set.headers['x-barrelman-overage'] = 'true'
  }
}

/**
 * Companion `onAfterHandle`: refunds the charge when the request failed with a
 * server error. Attach alongside the guard on the same instance.
 */
export function apiAuthAfter(context: GuardContext) {
  const metering = read(context)
  if (!metering?.charged || !metering.caller.userId) return

  const status = Number(context.set.status ?? 200)
  if (status >= 500) {
    refundUsage({
      userId: metering.caller.userId,
      apiKeyId: metering.caller.keyId,
      endpoint: metering.group,
      credits: metering.credits,
    })
  }
}

/**
 * Per-request metering state.
 *
 * Kept on the request object rather than Elysia's `store`, which is shared
 * across every request on an instance — writing per-request state there would
 * let concurrent requests overwrite each other's billing context.
 */
const METERING = Symbol.for('barrelman.metering')

function stash(context: GuardContext, metering: MeteringContext): void {
  ;(context.request as unknown as Record<symbol, unknown>)[METERING] = metering
}

function read(context: GuardContext): MeteringContext | undefined {
  return (context.request as unknown as Record<symbol, unknown>)[METERING] as MeteringContext | undefined
}

/** The caller resolved for this request, for handlers that need it. */
export function callerFor(request: Request): ApiCaller | undefined {
  return ((request as unknown as Record<symbol, unknown>)[METERING] as MeteringContext | undefined)?.caller
}

export { clientIp }

/**
 * Warn when an instance looks like a public deployment but has no service key,
 * which leaves every data endpoint open. Called once at startup.
 *
 * Deliberately a warning rather than a hard failure: refusing to boot would
 * break the local-development path that this same condition enables.
 */
export function assertAuthConfigured(): void {
  if (process.env.BARRELMAN_API_KEY) return

  if (process.env.NODE_ENV === 'production') {
    console.warn(
      '[auth] BARRELMAN_API_KEY is not set — every data endpoint is OPEN and unmetered. ' +
        'Set it before exposing this instance.',
    )
  } else {
    console.log('[auth] No BARRELMAN_API_KEY set — data endpoints are open (development mode)')
  }
}
