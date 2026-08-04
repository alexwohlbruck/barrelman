/**
 * Tests for the in-memory side of usage metering — the part that runs on every
 * request. Persistence is exercised by the integration path, not here; these
 * cover the buffering, aggregation and refund arithmetic that decide what
 * eventually gets written.
 */
import { describe, test, expect, beforeEach } from 'bun:test'
import {
  currentCycleStart,
  pendingCredits,
  recordUsage,
  refundUsage,
  startUsageFlush,
  stopUsageFlush,
  utcDay,
} from './usage.service'

/**
 * The buffer is module state, and `bun test` shares one module registry across
 * files — the api-auth tests meter through this same buffer. Ids are namespaced
 * per file as well as per test so the two suites cannot see each other's
 * counters.
 */
let n = 0
const nextUser = () => `usage-test-user-${(n += 1)}`

beforeEach(() => {
  stopUsageFlush()
})

describe('utcDay', () => {
  test('formats as YYYY-MM-DD in UTC', () => {
    expect(utcDay(new Date('2026-08-04T23:30:00Z'))).toBe('2026-08-04')
  })

  test('uses UTC rather than local time', () => {
    // 23:30 UTC is the next day in some zones; billing days must not shift
    // with the server's timezone.
    expect(utcDay(new Date('2026-08-04T23:59:59Z'))).toBe('2026-08-04')
    expect(utcDay(new Date('2026-08-05T00:00:01Z'))).toBe('2026-08-05')
  })
})

describe('currentCycleStart', () => {
  test('is the first of the month in UTC', () => {
    expect(currentCycleStart(new Date('2026-08-04T12:00:00Z'))).toBe('2026-08-01')
    expect(currentCycleStart(new Date('2026-01-31T12:00:00Z'))).toBe('2026-01-01')
  })
})

describe('recordUsage', () => {
  test('accumulates credits for one account', () => {
    const userId = nextUser()

    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'search', credits: 3 })
    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'search', credits: 3 })

    expect(pendingCredits(userId)).toBe(6)
  })

  test('sums across endpoint groups and keys', () => {
    const userId = nextUser()

    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'search', credits: 3 })
    recordUsage({ userId, apiKeyId: 'key-2', endpoint: 'isochrone', credits: 25 })
    recordUsage({ userId, apiKeyId: null, endpoint: 'tiles', credits: 1 })

    expect(pendingCredits(userId)).toBe(29)
  })

  test('keeps accounts separate', () => {
    const a = nextUser()
    const b = nextUser()

    recordUsage({ userId: a, endpoint: 'search', credits: 3 })

    expect(pendingCredits(a)).toBe(3)
    expect(pendingCredits(b)).toBe(0)
  })

  test('a rejected request costs nothing', () => {
    const userId = nextUser()

    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'search', credits: 3, rejected: true })

    // Counted for the console's "you are being throttled" display, not billed.
    expect(pendingCredits(userId)).toBe(0)
  })

  test('reports zero for an account with no activity', () => {
    expect(pendingCredits(nextUser())).toBe(0)
  })
})

describe('refundUsage', () => {
  test('nets out a charge that is still buffered', () => {
    const userId = nextUser()

    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })
    refundUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })

    expect(pendingCredits(userId)).toBe(0)
  })

  test('leaves other charges in the same bucket intact', () => {
    const userId = nextUser()

    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })
    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })
    refundUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })

    expect(pendingCredits(userId)).toBe(10)
  })

  test('writes a compensating entry when the charge was already flushed', () => {
    const userId = nextUser()

    // Nothing buffered for this account, as if the flush already ran.
    refundUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })

    // A negative delta is correct: the counters are additive, so this nets out
    // against what was persisted rather than double-counting.
    expect(pendingCredits(userId)).toBe(-10)
  })

  test('refunds are per key and endpoint', () => {
    const userId = nextUser()

    recordUsage({ userId, apiKeyId: 'key-1', endpoint: 'routing', credits: 10 })
    refundUsage({ userId, apiKeyId: 'key-2', endpoint: 'routing', credits: 10 })

    // The refund lands on key-2's bucket, so the account nets zero overall.
    expect(pendingCredits(userId)).toBe(0)
  })
})

describe('startUsageFlush', () => {
  test('is idempotent, so a hot reload cannot stack intervals', () => {
    // `bun --hot` re-evaluates a module on every edit without tearing down what
    // the last evaluation started; without the guard each save adds a timer.
    expect(() => {
      startUsageFlush()
      startUsageFlush()
      startUsageFlush()
    }).not.toThrow()

    stopUsageFlush()
  })

  test('stop then start works', () => {
    startUsageFlush()
    stopUsageFlush()
    startUsageFlush()
    stopUsageFlush()
  })
})
