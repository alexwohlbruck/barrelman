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
import {
  checkPenalty,
  checkThrottle,
  clearThrottleState,
  penaltyKeyFor,
  pruneThrottleState,
  recordRejection,
  recordSuccess,
  releaseSlot,
  strikeCount,
} from '../services/throttle.service'
import { recordAbuseSignal } from '../services/moderation.service'
import { resolveApiKey, type ResolvedKey } from '../services/api-keys.service'
import { checkQuota } from '../services/credits.service'
import { recordUsage, refundUsage } from '../services/usage.service'
import { getPlan } from '../billing/plans'
import { accountsEnabled } from '../config/accounts.config'
import { clientIp } from '../lib/rate-limit'
import { safeEqual } from '../lib/crypto'
import { hashIp } from '../services/accounts.service'
import { envNumber } from '../config/env'

/** Prefix that marks a customer key, as opposed to the shared service secret. */
const KEY_PREFIX = 'brm_'

export interface ApiCaller {
  kind: 'account' | 'service' | 'anonymous'
  userId?: string
  keyId?: string
  plan?: string
  /** Billing groups this key may call; `['*']` for all. */
  scopes?: string[]
  /** The account is disabled — the guard turns this into a 403 with the reason. */
  suspended?: boolean
  suspensionReason?: string | null
}

/** Per-request billing state, carried from the guard to the after-handler. */
export interface MeteringContext {
  caller: ApiCaller
  group: EndpointGroup
  credits: number
  charged: boolean
  /**
   * Account whose concurrency slot this request holds, or null when it holds
   * none. The after-handler must release it or the account leaks capacity until
   * the process restarts.
   */
  throttleKey: string | null
}

// Throttling lives in services/throttle.service.ts — four layers plus a
// concurrency cap. Re-exported here so callers keep one import.
export { pruneThrottleState as pruneRateBuckets, clearThrottleState as clearRateBuckets }

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
  // Constant-time: `===` short-circuits at the first differing byte, which
  // leaks the secret's length and prefix through response timing.
  if (serviceKey && safeEqual(presented, serviceKey)) {
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
    suspended: resolved.suspended,
    suspensionReason: resolved.suspensionReason,
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
    const ip = clientIp(request)

    const { caller, error } = await identifyCaller(headers, request, deps)

    /**
     * Every refusal runs through here. A rejection is a strike against the
     * caller: enough of them in a short window and the penalty box refuses
     * outright, which is what actually stops a key-guesser or a client wedged
     * in a retry loop — answering 401 forever is free for them and not for us.
     */
    const reject = (status: number, body: Record<string, unknown>, retryAfterSeconds?: number) => {
      const penaltyKey = penaltyKeyFor(ip, caller.userId)
      recordRejection(penaltyKey)
      set.status = status
      if (retryAfterSeconds) set.headers['retry-after'] = String(retryAfterSeconds)
      if (caller.userId) {
        deps.recordUsage({
          userId: caller.userId,
          apiKeyId: caller.keyId,
          endpoint: group,
          credits: 0,
          rejected: true,
        })
        // Not for an account already suspended: an operator has acted on it,
        // and its client retrying would otherwise bury the genuine signals in
        // the review queue under noise proportional to the retry rate.
        if (!caller.suspended) void flagSustainedAbuse(penaltyKey, caller.userId, ip)
      }
      return { ...body, docs: DOCS_URL }
    }

    /**
     * The penalty box runs before every other check, and again once the caller
     * is identified. Putting it last would mean a caller who only ever trips an
     * early gate — a bad scope, an unknown key — accumulates strikes that are
     * recorded and never enforced, which is precisely the caller it exists for.
     */
    const boxed = checkPenalty(penaltyKeyFor(ip, caller.userId))
    if (!boxed.allowed) {
      set.status = 429
      set.headers['retry-after'] = String(boxed.retryAfterSeconds)
      return { error: boxed.message, layer: boxed.layer, docs: DOCS_URL }
    }

    if (error) return reject(error.status, error.body)

    // Suspension outranks everything below: a suspended account gets a reason,
    // not a scope error or a quota message about credits it cannot spend.
    if (caller.suspended) {
      return reject(403, {
        error: caller.suspensionReason
          ? `This account is suspended: ${caller.suspensionReason}`
          : 'This account is suspended.',
        suspended: true,
        contact: '/console/account',
      })
    }

    // Unmetered callers still pass the throttle — an open deployment or a
    // misbehaving internal service can hammer the upstreams just as hard — but
    // skip scopes and credits.
    if (caller.kind !== 'account' || !caller.userId) {
      const verdict = checkThrottle({ ip, group })
      if (!verdict.allowed) {
        return reject(429, { error: verdict.message, layer: verdict.layer }, verdict.retryAfterSeconds)
      }
      stash(context, { caller, group, credits: 0, charged: false, throttleKey: null })
      return
    }

    const plan = getPlan(caller.plan)

    if (!scopeAllows(caller.scopes ?? ['*'], group)) {
      return reject(403, {
        error: `This API key is not permitted to call ${group} endpoints`,
        scope: group,
      })
    }

    const verdict = checkThrottle({ ip, group, userId: caller.userId, keyId: caller.keyId, plan })
    if (!verdict.allowed) {
      return reject(429, { error: verdict.message, layer: verdict.layer }, verdict.retryAfterSeconds)
    }

    // Past this point a concurrency slot may be held, so every exit has to
    // release it — that is what `throttleKey` in the metering context is for.
    const throttleKey = caller.userId

    const decision = await deps.checkQuota(caller.userId, cost)
    if (!decision.allowed) {
      releaseSlot(throttleKey, group)
      return reject(402, {
        error:
          decision.reason === 'overage-cap-reached'
            ? 'Overage spend limit reached for this billing period. Raise the limit or upgrade at /console/billing.'
            : 'Credit allowance exhausted for this billing period. ' +
              'Upgrade your plan or add credits at /console/billing.',
        reason: decision.reason,
        remaining: decision.balance.remaining,
        resetsAt: decision.balance.cycleResetsAt,
      })
    }

    deps.recordUsage({ userId: caller.userId, apiKeyId: caller.keyId, endpoint: group, credits: cost })
    stash(context, { caller, group, credits: cost, charged: true, throttleKey })
    recordSuccess(caller.userId)

    set.headers['x-barrelman-credits-charged'] = String(cost)
    if (decision.overage) set.headers['x-barrelman-overage'] = 'true'
  }
}

/**
 * Raise an abuse signal once a caller's strikes pass the point where this stops
 * looking like a bug in their client. Best-effort and deduplicated in the
 * service, so it never blocks the response.
 */
async function flagSustainedAbuse(penaltyKey: string, userId: string, ip: string): Promise<void> {
  const strikes = strikeCount(penaltyKey)
  if (strikes < SIGNAL_STRIKE_THRESHOLD) return

  try {
    await recordAbuseSignal({
      userId,
      kind: 'error-hammering',
      severity: strikes > SIGNAL_STRIKE_THRESHOLD * 4 ? 'high' : 'medium',
      detail: { strikes },
      ipHash: hashIp(ip),
    })
  } catch (err) {
    console.error('[abuse] failed to record signal', err)
  }
}

const SIGNAL_STRIKE_THRESHOLD = envNumber('BARRELMAN_ABUSE_SIGNAL_STRIKES', 100)

/**
 * Companion `onAfterHandle`: refunds the charge when the request failed with a
 * server error. Attach alongside the guard on the same instance.
 */
export function apiAuthAfter(context: GuardContext) {
  const metering = read(context)
  if (!metering) return

  // Release the concurrency slot first and unconditionally: a handler that
  // threw still occupied the upstream, and a slot leaked on the error path is
  // exactly how an account ends up permanently unable to call an endpoint.
  if (metering.throttleKey) releaseSlot(metering.throttleKey, metering.group)

  if (!metering.charged || !metering.caller.userId) return

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
