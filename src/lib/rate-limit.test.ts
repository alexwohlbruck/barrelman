/**
 * Tests for the fixed-window limiter guarding the auth endpoints.
 */
import { describe, test, expect } from 'bun:test'
import { clientIp, createRateLimiter, pruneRateLimiters } from './rate-limit'

describe('createRateLimiter', () => {
  test('allows exactly `limit` attempts then refuses', () => {
    const limiter = createRateLimiter(3, 60_000)

    expect(limiter.check('a')).toBe(true)
    expect(limiter.check('a')).toBe(true)
    expect(limiter.check('a')).toBe(true)
    expect(limiter.check('a')).toBe(false)
    expect(limiter.check('a')).toBe(false)
  })

  test('tracks keys independently', () => {
    const limiter = createRateLimiter(1, 60_000)

    expect(limiter.check('a')).toBe(true)
    expect(limiter.check('a')).toBe(false)
    // A different caller must be unaffected by the first one's exhaustion.
    expect(limiter.check('b')).toBe(true)
  })

  test('rolls over once the window elapses', async () => {
    const limiter = createRateLimiter(1, 20)

    expect(limiter.check('a')).toBe(true)
    expect(limiter.check('a')).toBe(false)

    await Bun.sleep(30)
    expect(limiter.check('a')).toBe(true)
  })

  test('reset clears a key so a success is not punished', () => {
    const limiter = createRateLimiter(2, 60_000)

    limiter.check('a')
    limiter.check('a')
    expect(limiter.check('a')).toBe(false)

    limiter.reset('a')
    expect(limiter.check('a')).toBe(true)
  })

  test('retryAfter reports seconds remaining, and zero for unknown keys', () => {
    const limiter = createRateLimiter(1, 60_000)

    expect(limiter.retryAfter('never-seen')).toBe(0)

    limiter.check('a')
    const remaining = limiter.retryAfter('a')
    expect(remaining).toBeGreaterThan(0)
    expect(remaining).toBeLessThanOrEqual(60)
  })

  test('separate limiters do not share buckets', () => {
    const a = createRateLimiter(1, 60_000)
    const b = createRateLimiter(1, 60_000)

    expect(a.check('same-key')).toBe(true)
    expect(a.check('same-key')).toBe(false)
    expect(b.check('same-key')).toBe(true)
  })
})

describe('pruneRateLimiters', () => {
  test('drops expired buckets so unique keys cannot grow the map forever', async () => {
    const limiter = createRateLimiter(1, 10)
    limiter.check('transient')
    expect(limiter.retryAfter('transient')).toBeGreaterThan(0)

    await Bun.sleep(20)
    pruneRateLimiters()

    // retryAfter returns 0 only when the bucket is gone entirely.
    expect(limiter.retryAfter('transient')).toBe(0)
  })

  test('leaves live buckets in place', () => {
    const limiter = createRateLimiter(1, 60_000)
    limiter.check('live')

    pruneRateLimiters()

    expect(limiter.check('live')).toBe(false)
  })
})

describe('clientIp', () => {
  const req = (headers: Record<string, string>) => new Request('http://localhost/x', { headers })

  test('prefers the first entry of x-forwarded-for', () => {
    expect(clientIp(req({ 'x-forwarded-for': '203.0.113.7, 10.0.0.1, 10.0.0.2' }))).toBe('203.0.113.7')
  })

  test('falls back to x-real-ip', () => {
    expect(clientIp(req({ 'x-real-ip': '198.51.100.4' }))).toBe('198.51.100.4')
  })

  test('returns a sentinel when no proxy header is present', () => {
    expect(clientIp(req({}))).toBe('unknown')
  })
})
