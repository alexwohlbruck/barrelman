/**
 * In-memory fixed-window rate limiting for the auth endpoints.
 *
 * Deliberately process-local: it guards code-sending and sign-in attempts,
 * where a single instance seeing "too many tries" is enough. The metered API
 * quota is a different mechanism entirely (see billing/credits) and is backed
 * by the database. If barrelman ever runs more than one API replica, these
 * counters become per-replica — acceptable for abuse control, not for billing.
 */

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

/** Best-effort client address from the usual proxy headers. */
export function clientIp(request: Request): string {
  const forwarded = request.headers.get('x-forwarded-for')
  if (forwarded) return forwarded.split(',')[0]!.trim()
  const real = request.headers.get('x-real-ip')
  if (real) return real.trim()
  return 'unknown'
}
