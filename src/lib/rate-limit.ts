/**
 * In-memory fixed-window rate limiting for the auth endpoints.
 *
 * Deliberately process-local: it guards code-sending and sign-in attempts,
 * where a single instance seeing "too many tries" is enough. The metered API
 * quota is a different mechanism entirely (see billing/credits) and is backed
 * by the database. If barrelman ever runs more than one API replica, these
 * counters become per-replica — acceptable for abuse control, not for billing.
 */

import { envNumber } from '../config/env'

interface Bucket {
  count: number
  resetAt: number
}

export interface RateLimiter {
  /** Count one attempt. Returns false once the window's budget is spent. */
  check(key: string): boolean
  /** Seconds until the given key's window rolls over. */
  retryAfter(key: string): number
  /** Forget a key — call after a success so a valid user isn't punished. */
  reset(key: string): void
}

const registry: Array<Map<string, Bucket>> = []

export function createRateLimiter(limit: number, windowMs: number): RateLimiter {
  const buckets = new Map<string, Bucket>()
  registry.push(buckets)

  return {
    check(key: string): boolean {
      const now = Date.now()
      const bucket = buckets.get(key)
      if (!bucket || bucket.resetAt <= now) {
        buckets.set(key, { count: 1, resetAt: now + windowMs })
        return true
      }
      bucket.count += 1
      return bucket.count <= limit
    },
    retryAfter(key: string): number {
      const bucket = buckets.get(key)
      if (!bucket) return 0
      return Math.max(0, Math.ceil((bucket.resetAt - Date.now()) / 1000))
    },
    reset(key: string): void {
      buckets.delete(key)
    },
  }
}

/** Drop expired buckets so a stream of unique keys can't grow the map forever. */
export function pruneRateLimiters(): void {
  const now = Date.now()
  for (const buckets of registry) {
    for (const [key, bucket] of buckets) {
      if (bucket.resetAt <= now) buckets.delete(key)
    }
  }
}

/**
 * How many reverse proxies sit between the internet and this process.
 *
 * This is the difference between a per-address limit and a decoration.
 * `X-Forwarded-For` is a list each hop appends to, so the leftmost entry is
 * whatever the *client* sent — a value it is free to invent, and a fresh one
 * per request buys a fresh bucket. Reading it as the client address, which is
 * the obvious thing to do and what this function used to do, means every
 * per-address limit in the codebase is bypassed by one header.
 *
 * Counting in from the right instead lands on the entry written by a proxy we
 * actually run, which is the first one in the list that was observed rather
 * than inherited. That requires knowing how many of those there are: one for a
 * single reverse proxy, two behind a CDN as well.
 *
 * `0` means nothing is in front and `X-Forwarded-For` is ignored entirely —
 * correct for a directly exposed instance, and wrong (everyone shares the
 * proxy's bucket) for a deployment that has one. The default of 1 matches how
 * barrelman is actually deployed and documented.
 */
const TRUSTED_PROXY_HOPS = envNumber('BARRELMAN_TRUSTED_PROXY_HOPS', 1)

/**
 * Resolves the peer address of a connection, when nothing is proxying. Bun
 * knows it, but only through the server object, which is created after most of
 * this module's consumers are imported — hence the late injection from
 * `index.ts` rather than a direct dependency.
 */
let peerAddress: ((request: Request) => string | null) | null = null

export function setPeerAddressResolver(resolve: (request: Request) => string | null): void {
  peerAddress = resolve
}

/** The client address, as far as the trusted part of the chain can vouch for it. */
export function clientIp(request: Request): string {
  if (TRUSTED_PROXY_HOPS > 0) {
    const forwarded = request.headers.get('x-forwarded-for')
    if (forwarded) {
      const chain = forwarded
        .split(',')
        .map((entry) => entry.trim())
        .filter(Boolean)
      // Clamped, so a chain shorter than the configured hop count (a request
      // that skipped a proxy) falls back to the leftmost entry rather than
      // reading past the end and losing the limit altogether.
      const trusted = chain[Math.max(0, chain.length - TRUSTED_PROXY_HOPS)]
      if (trusted) return trusted
    }

    // Single-hop proxies commonly send this instead, and it is not a list, so
    // there is nothing for a client to prepend to.
    const real = request.headers.get('x-real-ip')
    if (real) return real.trim()
  }

  return peerAddress?.(request) ?? 'unknown'
}
