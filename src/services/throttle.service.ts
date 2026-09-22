/**
 * Layered request throttling for the metered API.
 *
 * The per-plan request ceiling alone is not enough. It is per account, so one
 * leaked key can consume the whole budget; it does not bound *concurrency*, so
 * a hundred simultaneous isochrones can saturate GraphHopper while staying well
 * inside a per-minute limit; and it does nothing about unauthenticated traffic,
 * which never reaches an account at all.
 *
 * Four layers, cheapest first, so an abusive caller is rejected before we spend
 * anything on them:
 *
 *   1. **Penalty box** — a caller that has recently collected a stream of 401s,
 *      402s or 429s is refused outright for a short, escalating period. This is
 *      what stops a scripted key-guesser or a client stuck in a retry loop.
 *   2. **Per-IP** — bounds any single source, including anonymous traffic in an
 *      open deployment.
 *   3. **Per-key** — a single credential cannot spend the whole account budget,
 *      so one leaked key does not deny service to the account's other keys.
 *   4. **Per-account** — the plan's published limit.
 *
 * Plus a concurrency cap on expensive groups, which is about protecting the
 * engines behind us rather than being fair to callers.
 *
 * Every layer is counted per *rate bucket* rather than per caller alone, so
 * tiles cannot spend the budget the other endpoints are checked against — see
 * `bucketFor` below.
 *
 * All state is in memory and therefore per-replica: with N replicas the
 * effective limits are N times these. That is fine for protecting the process
 * and the upstreams — the credit ledger in Postgres remains the accurate record
 * for anything with money attached.
 */
import { TILE_RATE_MULTIPLIER, type EndpointGroup, type Plan } from '../billing/plans'
import { envNumber } from '../config/env'

interface Window {
  count: number
  resetAt: number
}

/** A fixed window per key, with a bounded map so unique keys cannot leak memory. */
class WindowCounter {
  private windows = new Map<string, Window>()

  constructor(
    private readonly windowMs: number,
    private readonly maxEntries = 50_000,
  ) {}

  hit(key: string, limit: number): { allowed: boolean; retryAfterSeconds: number } {
    const now = Date.now()
    const existing = this.windows.get(key)

    if (!existing || existing.resetAt <= now) {
      if (this.windows.size >= this.maxEntries) this.evictExpired(now)
      this.windows.set(key, { count: 1, resetAt: now + this.windowMs })
      return { allowed: true, retryAfterSeconds: 0 }
    }

    existing.count += 1
    if (existing.count <= limit) return { allowed: true, retryAfterSeconds: 0 }

    return { allowed: false, retryAfterSeconds: Math.max(1, Math.ceil((existing.resetAt - now) / 1000)) }
  }

  private evictExpired(now: number): void {
    for (const [key, window] of this.windows) {
      if (window.resetAt <= now) this.windows.delete(key)
    }
    // Still full of live windows: drop the oldest rather than grow without
    // bound. Losing a live counter is a smaller problem than exhausting memory.
    if (this.windows.size >= this.maxEntries) {
      const drop = Math.ceil(this.maxEntries * 0.1)
      let i = 0
      for (const key of this.windows.keys()) {
        this.windows.delete(key)
        if (++i >= drop) break
      }
    }
  }

  prune(): void {
    this.evictExpired(Date.now())
  }

  clear(): void {
    this.windows.clear()
  }

  get size(): number {
    return this.windows.size
  }
}

const perIp = new WindowCounter(60_000)
const perKey = new WindowCounter(60_000)
const perAccount = new WindowCounter(60_000)
/** Per (account, address), for plans that bound each caller separately. */
const perAccountIp = new WindowCounter(60_000)

/**
 * Requests per minute allowed from one address when nobody is authenticated.
 * Only reachable in an open deployment (no service key configured), where it is
 * the only thing standing between a scraper and the database.
 */
const ANONYMOUS_IP_LIMIT = envNumber('BARRELMAN_ANON_RPM', 120)

/**
 * Ceiling on any single source address, regardless of how many accounts or keys
 * it presents. Generous, because a corporate NAT or a mobile carrier can put
 * many legitimate users behind one address — this is a backstop against a
 * single abusive host, not a per-user limit.
 */
const IP_LIMIT = envNumber('BARRELMAN_IP_RPM', 3_000)

/**
 * A single key gets this share of its account's per-minute budget. One leaked
 * or runaway key then cannot starve the account's other keys.
 */
const PER_KEY_SHARE = envNumber('BARRELMAN_PER_KEY_SHARE', 0.8)

// ── Rate buckets ────────────────────────────────────────────────────────

/**
 * Tiles are counted in a window of their own, at `TILE_RATE_MULTIPLIER` times
 * whatever limit is being checked — see the constant for why the limits above
 * are the wrong size for a map.
 *
 * Their own window rather than a bigger shared one: on a shared window a
 * single pan spends the budget every other endpoint is then measured against,
 * and a map turns into a 429 on /search.
 */
interface RateBucket {
  /** Suffixed onto every counter key, so buckets cannot spend each other. */
  id: string
  /** Multiple of the limit under test this bucket is allowed. */
  multiplier: number
}

const API_BUCKET: RateBucket = { id: 'api', multiplier: 1 }
const TILE_BUCKET: RateBucket = { id: 'tiles', multiplier: TILE_RATE_MULTIPLIER }

function bucketFor(group: EndpointGroup): RateBucket {
  return group === 'tiles' ? TILE_BUCKET : API_BUCKET
}

// ── Penalty box ─────────────────────────────────────────────────────────

interface Penalty {
  strikes: number
  /** Refused until this timestamp. */
  until: number
  lastStrikeAt: number
}

const penalties = new Map<string, Penalty>()

/** Rejections tolerated before a caller is boxed. */
const STRIKES_BEFORE_PENALTY = envNumber('BARRELMAN_ABUSE_STRIKES', 25)
/** Strikes decay after this long without a new one. */
const STRIKE_DECAY_MS = 10 * 60_000
const MAX_PENALTY_MS = 30 * 60_000

/**
 * Record that a caller was rejected. Strikes escalate geometrically, so an
 * occasional 402 from a real integration costs nothing while a tight retry loop
 * is boxed within seconds.
 */
export function recordRejection(key: string): void {
  const now = Date.now()
  const existing = penalties.get(key)

  if (!existing || now - existing.lastStrikeAt > STRIKE_DECAY_MS) {
    penalties.set(key, { strikes: 1, until: 0, lastStrikeAt: now })
    return
  }

  existing.strikes += 1
  existing.lastStrikeAt = now

  if (existing.strikes >= STRIKES_BEFORE_PENALTY) {
    const over = existing.strikes - STRIKES_BEFORE_PENALTY
    const durationMs = Math.min(MAX_PENALTY_MS, 5_000 * 2 ** Math.min(over, 12))
    existing.until = now + durationMs
  }
}

/** Clear a caller's strikes after a successful request. */
export function recordSuccess(key: string): void {
  const existing = penalties.get(key)
  if (existing && existing.until <= Date.now()) penalties.delete(key)
}

export function penaltyRemaining(key: string): number {
  const existing = penalties.get(key)
  if (!existing || existing.until <= Date.now()) return 0
  return Math.ceil((existing.until - Date.now()) / 1000)
}

/** Total strikes currently held, so the abuse detector can flag sustained abuse. */
export function strikeCount(key: string): number {
  const existing = penalties.get(key)
  if (!existing || Date.now() - existing.lastStrikeAt > STRIKE_DECAY_MS) return 0
  return existing.strikes
}

// ── Concurrency ─────────────────────────────────────────────────────────

/**
 * Simultaneous in-flight requests allowed per account, for groups where one
 * request occupies an upstream engine for a long time. Without this, a caller
 * inside their per-minute limit can still pin every GraphHopper worker.
 */
const CONCURRENCY_LIMITS: Partial<Record<EndpointGroup, number>> = {
  isochrone: envNumber('BARRELMAN_ISOCHRONE_CONCURRENCY_PER_ACCOUNT', 2),
  transit: envNumber('BARRELMAN_TRANSIT_CONCURRENCY_PER_ACCOUNT', 4),
  routing: envNumber('BARRELMAN_ROUTING_CONCURRENCY_PER_ACCOUNT', 8),
}

const inFlight = new Map<string, number>()

function concurrencyKey(accountKey: string, group: EndpointGroup): string {
  return `${accountKey}:${group}`
}

export function acquireSlot(accountKey: string, group: EndpointGroup): boolean {
  const limit = CONCURRENCY_LIMITS[group]
  if (!limit) return true

  const key = concurrencyKey(accountKey, group)
  const current = inFlight.get(key) ?? 0
  if (current >= limit) return false

  inFlight.set(key, current + 1)
  return true
}

export function releaseSlot(accountKey: string, group: EndpointGroup): void {
  const limit = CONCURRENCY_LIMITS[group]
  if (!limit) return

  const key = concurrencyKey(accountKey, group)
  const current = inFlight.get(key) ?? 0
  if (current <= 1) inFlight.delete(key)
  else inFlight.set(key, current - 1)
}

// ── The check ───────────────────────────────────────────────────────────

export interface ThrottleRequest {
  ip: string
  group: EndpointGroup
  /** Absent for anonymous traffic. */
  userId?: string
  keyId?: string
  plan?: Plan
}

export type ThrottleVerdict =
  | { allowed: true }
  | {
      allowed: false
      /** Which layer refused, for the message and for metrics. */
      layer: 'penalty' | 'ip' | 'key' | 'account' | 'concurrency'
      retryAfterSeconds: number
      message: string
    }

/**
 * The identity a penalty attaches to: the account *at an address* when the
 * account is known, otherwise the address alone.
 *
 * Still keyed on the account, so rotating keys does not shed strikes — that is
 * the property this exists for. But not on the account ALONE, which is what it
 * used to be, and which made the box far blunter than intended: one caller
 * hammering one endpoint with a credential that cannot reach it boxed every
 * other caller on the same account, on every other endpoint. On a deployment
 * whose busiest consumer is a server-side tile proxy that is the whole map
 * going dark because something unrelated asked for an admin route.
 *
 * Including the address keeps a misbehaving client's strikes to the host it is
 * misbehaving from. A distributed key-guesser gets a key per address either
 * way, since that is how it was already keyed for unidentified callers.
 */
export function penaltyKeyFor(ip: string, userId?: string): string {
  return userId ? `${userId}@${ip}` : `ip:${ip}`
}

/**
 * Penalty-box check, separated from `checkThrottle` because the guard has to
 * run it *first* — before scopes, before quota, before anything that can itself
 * reject. A caller who only ever trips an early check would otherwise
 * accumulate strikes that are never enforced, which is exactly the caller the
 * penalty box exists for.
 */
export function checkPenalty(key: string): ThrottleVerdict {
  const boxed = penaltyRemaining(key)
  if (boxed === 0) return { allowed: true }

  return {
    allowed: false,
    layer: 'penalty',
    retryAfterSeconds: boxed,
    message:
      'Too many rejected requests. Access is temporarily paused — fix the failing requests before retrying.',
  }
}

export function checkThrottle(request: ThrottleRequest): ThrottleVerdict {
  const { ip, group, userId, keyId, plan } = request
  const bucket = bucketFor(group)
  const scaled = (limit: number) => Math.max(1, Math.round(limit * bucket.multiplier))

  if (!userId) {
    const anon = perIp.hit(`anon:${bucket.id}:${ip}`, scaled(ANONYMOUS_IP_LIMIT))
    if (!anon.allowed) {
      return {
        allowed: false,
        layer: 'ip',
        retryAfterSeconds: anon.retryAfterSeconds,
        message: 'Rate limit exceeded for this address.',
      }
    }
    return { allowed: true }
  }

  /**
   * An operator's own application takes none of the layers below — not the
   * windows and not the concurrency cap. See `Plan.unthrottled`; the caps
   * are per *account*, so a first-party app would otherwise ration its whole
   * user base to two simultaneous isochrones.
   */
  if (plan?.unthrottled) return { allowed: true }

  const accountLimit = scaled(plan?.requestsPerMinute ?? 60)

  /**
   * The address backstop, raised to the account's own limit when that is
   * higher. It is documented as "a backstop against a single abusive host, not
   * a per-user limit", and at a fixed 3,000 it stopped being that as soon as a
   * plan sold more: every server-side integration presents ONE address, so the
   * backstop silently replaced the limit the customer bought. A tile proxy on a
   * 6,000/min plan was cut off at half that, and the map went dark while the
   * account layer sat idle.
   *
   * Anonymous callers keep the fixed ceiling above — there is no plan to read.
   */
  const addressLimit = Math.max(scaled(IP_LIMIT), accountLimit)
  const address = perIp.hit(`ip:${bucket.id}:${ip}`, addressLimit)
  if (!address.allowed) {
    return {
      allowed: false,
      layer: 'ip',
      retryAfterSeconds: address.retryAfterSeconds,
      message: 'Rate limit exceeded for this address.',
    }
  }

  /**
   * Per-visitor ceiling, on the plans that ask for one. Checked before the
   * account layer so a single abusive address is refused on its own account
   * rather than by exhausting the shared budget — on a public demo those are
   * the same number of requests but very different outcomes for everyone else.
   */
  const perIpLimit = plan?.requestsPerMinutePerIp && scaled(plan.requestsPerMinutePerIp)
  if (perIpLimit) {
    const visitor = perAccountIp.hit(`${userId}:${bucket.id}:${ip}`, perIpLimit)
    if (!visitor.allowed) {
      return {
        allowed: false,
        layer: 'ip',
        retryAfterSeconds: visitor.retryAfterSeconds,
        message: `Rate limit exceeded for this address — ${perIpLimit} requests per minute.`,
      }
    }
  }

  if (keyId) {
    const keyLimit = Math.max(1, Math.floor(accountLimit * PER_KEY_SHARE))
    const key = perKey.hit(`${keyId}:${bucket.id}`, keyLimit)
    if (!key.allowed) {
      return {
        allowed: false,
        layer: 'key',
        retryAfterSeconds: key.retryAfterSeconds,
        message: `This key is limited to ${keyLimit} requests per minute. Spread traffic across keys or upgrade your plan.`,
      }
    }
  }

  const account = perAccount.hit(`${userId}:${bucket.id}`, accountLimit)
  if (!account.allowed) {
    return {
      allowed: false,
      layer: 'account',
      retryAfterSeconds: account.retryAfterSeconds,
      message: `Rate limit exceeded — your plan allows ${accountLimit} requests per minute.`,
    }
  }

  if (!acquireSlot(userId, group)) {
    return {
      allowed: false,
      layer: 'concurrency',
      retryAfterSeconds: 1,
      message: `Too many simultaneous ${group} requests. Wait for the ones in flight to finish.`,
    }
  }

  return { allowed: true }
}

// ── Maintenance ─────────────────────────────────────────────────────────

export function pruneThrottleState(): void {
  perIp.prune()
  perKey.prune()
  perAccount.prune()
  perAccountIp.prune()

  const now = Date.now()
  for (const [key, penalty] of penalties) {
    if (penalty.until <= now && now - penalty.lastStrikeAt > STRIKE_DECAY_MS) penalties.delete(key)
  }
}

export function clearThrottleState(): void {
  perIp.clear()
  perKey.clear()
  perAccount.clear()
  perAccountIp.clear()
  penalties.clear()
  inFlight.clear()
}

/** Snapshot for the admin dashboard. */
export function throttleStats() {
  return {
    trackedAddresses: perIp.size,
    trackedKeys: perKey.size,
    trackedAccounts: perAccount.size,
    trackedVisitors: perAccountIp.size,
    penalised: [...penalties.values()].filter((p) => p.until > Date.now()).length,
    inFlight: [...inFlight.values()].reduce((sum, n) => sum + n, 0),
  }
}
