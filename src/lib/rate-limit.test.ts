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

/**
 * These cover the property every per-address limit in the codebase rests on:
 * that the address cannot be chosen by the caller. The default configuration
 * is one trusted proxy, so the value to trust is the last entry — the one that
 * proxy observed — and everything to its left is caller-supplied.
 */
describe('clientIp', () => {
  const req = (headers: Record<string, string>) => new Request('http://localhost/x', { headers })

  test('reads the entry written by the trusted proxy, not the one the caller sent', () => {
    // A client claiming to be 203.0.113.7; our proxy appends what it actually saw.
    expect(clientIp(req({ 'x-forwarded-for': '203.0.113.7, 198.51.100.4' }))).toBe('198.51.100.4')
  })

  test('a spoofed chain cannot buy a fresh bucket', () => {
    const spoofed = ['1.1.1.1', '2.2.2.2', '3.3.3.3'].map((claim) =>
      clientIp(req({ 'x-forwarded-for': `${claim}, 198.51.100.4` })),
    )
    // Whatever the caller prepends, every request resolves to the same address,
    // so all three land in one rate-limit window rather than three.
    expect(new Set(spoofed).size).toBe(1)
    expect(spoofed[0]).toBe('198.51.100.4')
  })

  test('a single-entry chain is used as-is', () => {
    expect(clientIp(req({ 'x-forwarded-for': '198.51.100.4' }))).toBe('198.51.100.4')
  })

  test('tolerates a chain shorter than the configured hop count', () => {
    // Clamped rather than reading past the start: losing the limit entirely is
    // worse than falling back to the leftmost entry.
    expect(clientIp(req({ 'x-forwarded-for': ' , 198.51.100.4 , ' }))).toBe('198.51.100.4')
  })

  test('falls back to x-real-ip, which is not a list', () => {
    expect(clientIp(req({ 'x-real-ip': '198.51.100.4' }))).toBe('198.51.100.4')
  })

  test('returns a sentinel when no proxy header is present and no peer is known', () => {
    expect(clientIp(req({}))).toBe('unknown')
  })
})
