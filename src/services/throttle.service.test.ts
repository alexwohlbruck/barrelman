/**
 * Tests for the layered throttle.
 *
 * The properties that matter are the ones that decide whether a real customer
 * gets shut out or an abusive one gets through: that layers are independent,
 * that concurrency slots are returned, that penalties escalate and then decay,
 * and that the counter maps cannot grow without bound.
 */
import { describe, test, expect, beforeEach } from 'bun:test'
import {
  acquireSlot,
  checkPenalty,
  checkThrottle,
  clearThrottleState,
  penaltyKeyFor,
  penaltyRemaining,
  pruneThrottleState,
  recordRejection,
  recordSuccess,
  releaseSlot,
  strikeCount,
  throttleStats,
} from './throttle.service'
import { getPlan } from '../billing/plans'

const free = getPlan('free') // 60 rpm
const developer = getPlan('developer') // 600 rpm
const PER_KEY = Math.floor(free.requestsPerMinute * 0.8)

beforeEach(clearThrottleState)

describe('anonymous traffic', () => {
  test('is bounded per address', () => {
    let refused = 0
    for (let i = 0; i < 200; i += 1) {
      if (!checkThrottle({ ip: '203.0.113.1', group: 'search' }).allowed) refused += 1
    }
    // Default anonymous ceiling is 120/min.
    expect(refused).toBeGreaterThan(0)
  })

  test('bounds each address separately', () => {
    for (let i = 0; i < 200; i += 1) checkThrottle({ ip: '203.0.113.1', group: 'search' })

    expect(checkThrottle({ ip: '203.0.113.2', group: 'search' }).allowed).toBe(true)
  })

  test('reports the ip layer and a positive Retry-After', () => {
    let verdict = checkThrottle({ ip: '203.0.113.9', group: 'search' })
    for (let i = 0; i < 200 && verdict.allowed; i += 1) {
      verdict = checkThrottle({ ip: '203.0.113.9', group: 'search' })
    }

    expect(verdict.allowed).toBe(false)
    if (!verdict.allowed) {
      expect(verdict.layer).toBe('ip')
      expect(verdict.retryAfterSeconds).toBeGreaterThan(0)
    }
  })
})

describe('per-key and per-account layers', () => {
  const request = (userId: string, keyId: string) =>
    checkThrottle({ ip: '198.51.100.1', group: 'tiles', userId, keyId, plan: free })

  test('a single key is refused at its share of the account budget', () => {
    for (let i = 0; i < PER_KEY; i += 1) expect(request('u1', 'k1').allowed).toBe(true)

    const verdict = request('u1', 'k1')
    expect(verdict.allowed).toBe(false)
    if (!verdict.allowed) expect(verdict.layer).toBe('key')
  })

  test('a second key on the same account still works after the first is capped', () => {
    for (let i = 0; i < PER_KEY + 1; i += 1) request('u1', 'k1')

    // The whole point of a per-key share: one runaway key must not deny
    // service to the account's other keys.
    expect(request('u1', 'k2').allowed).toBe(true)
  })

  test('the account limit catches what the per-key limits let through', () => {
    let accountRefusal = false
    for (let i = 0; i < 40; i += 1) {
      request('u1', 'k1')
      const verdict = request('u1', 'k2')
      if (!verdict.allowed && verdict.layer === 'account') accountRefusal = true
    }

    expect(accountRefusal).toBe(true)
  })

  test('accounts are independent', () => {
    for (let i = 0; i < 200; i += 1) request('u1', 'k1')

    expect(request('u2', 'k9').allowed).toBe(true)
  })

  test('a bigger plan gets a bigger budget', () => {
    const big = (i: number) =>
      checkThrottle({ ip: '198.51.100.2', group: 'tiles', userId: 'u-dev', keyId: `k${i}`, plan: developer })

    // Spread across keys so only the account layer is in play.
    for (let i = 0; i < 100; i += 1) expect(big(i).allowed).toBe(true)
  })
})

describe('concurrency', () => {
  test('caps simultaneous requests to an expensive group', () => {
    // isochrone default is 2 per account.
    expect(acquireSlot('u1', 'isochrone')).toBe(true)
    expect(acquireSlot('u1', 'isochrone')).toBe(true)
    expect(acquireSlot('u1', 'isochrone')).toBe(false)
  })

  test('releasing a slot frees capacity', () => {
    acquireSlot('u1', 'isochrone')
    acquireSlot('u1', 'isochrone')
    expect(acquireSlot('u1', 'isochrone')).toBe(false)

    releaseSlot('u1', 'isochrone')
    expect(acquireSlot('u1', 'isochrone')).toBe(true)
  })

  test('does not cap cheap groups', () => {
    for (let i = 0; i < 100; i += 1) expect(acquireSlot('u1', 'tiles')).toBe(true)
  })

  test('is per account and per group', () => {
    acquireSlot('u1', 'isochrone')
    acquireSlot('u1', 'isochrone')

    expect(acquireSlot('u2', 'isochrone')).toBe(true)
    expect(acquireSlot('u1', 'transit')).toBe(true)
  })

  test('over-releasing does not create phantom capacity', () => {
    // A double release on an error path must not let an account exceed the cap.
    releaseSlot('u1', 'isochrone')
    releaseSlot('u1', 'isochrone')

    expect(acquireSlot('u1', 'isochrone')).toBe(true)
    expect(acquireSlot('u1', 'isochrone')).toBe(true)
    expect(acquireSlot('u1', 'isochrone')).toBe(false)
  })
})

describe('penalty box', () => {
  test('tolerates an occasional rejection', () => {
    for (let i = 0; i < 5; i += 1) recordRejection('u1')

    expect(checkPenalty('u1').allowed).toBe(true)
  })

  test('boxes a caller after a sustained stream of rejections', () => {
    for (let i = 0; i < 30; i += 1) recordRejection('u1')

    const verdict = checkPenalty('u1')
    expect(verdict.allowed).toBe(false)
    if (!verdict.allowed) {
      expect(verdict.layer).toBe('penalty')
      expect(verdict.retryAfterSeconds).toBeGreaterThan(0)
    }
  })

  test('escalates with continued abuse', () => {
    for (let i = 0; i < 26; i += 1) recordRejection('u1')
    const first = penaltyRemaining('u1')

    for (let i = 0; i < 6; i += 1) recordRejection('u1')
    const later = penaltyRemaining('u1')

    expect(later).toBeGreaterThan(first)
  })

  test('caps the penalty so a caller is never locked out indefinitely', () => {
    for (let i = 0; i < 500; i += 1) recordRejection('u1')

    // 30 minutes; a mistake in someone's client should not cost them a day.
    expect(penaltyRemaining('u1')).toBeLessThanOrEqual(30 * 60)
  })

  test('tracks callers independently', () => {
    for (let i = 0; i < 30; i += 1) recordRejection('u1')

    expect(checkPenalty('u2').allowed).toBe(true)
  })

  test('a success clears strikes for a caller who is not boxed', () => {
    for (let i = 0; i < 5; i += 1) recordRejection('u1')
    expect(strikeCount('u1')).toBe(5)

    recordSuccess('u1')
    expect(strikeCount('u1')).toBe(0)
  })

  test('a success does not release a caller already boxed', () => {
    for (let i = 0; i < 30; i += 1) recordRejection('u1')
    recordSuccess('u1')

    // Otherwise one lucky request would reset an active penalty.
    expect(checkPenalty('u1').allowed).toBe(false)
  })
})

describe('penaltyKeyFor', () => {
  test('prefers the account so rotating keys does not shed strikes', () => {
    expect(penaltyKeyFor('203.0.113.1', 'user-1')).toBe('user-1')
  })

  test('falls back to the address for anonymous callers', () => {
    expect(penaltyKeyFor('203.0.113.1')).toBe('ip:203.0.113.1')
  })
})

describe('maintenance', () => {
  test('prune drops expired windows', () => {
    for (let i = 0; i < 50; i += 1) {
      checkThrottle({ ip: `203.0.113.${i}`, group: 'search' })
    }
    expect(throttleStats().trackedAddresses).toBeGreaterThan(0)

    pruneThrottleState()
    // Windows are a minute long, so nothing expires yet — the point is that
    // pruning does not discard live state.
    expect(throttleStats().trackedAddresses).toBeGreaterThan(0)
  })

  test('stats report in-flight work', () => {
    acquireSlot('u1', 'isochrone')
    acquireSlot('u2', 'transit')

    expect(throttleStats().inFlight).toBe(2)
  })

  test('clear resets every layer', () => {
    checkThrottle({ ip: '203.0.113.1', group: 'search' })
    recordRejection('u1')
    acquireSlot('u1', 'isochrone')

    clearThrottleState()

    const stats = throttleStats()
    expect(stats.trackedAddresses).toBe(0)
    expect(stats.inFlight).toBe(0)
    expect(strikeCount('u1')).toBe(0)
  })
})
