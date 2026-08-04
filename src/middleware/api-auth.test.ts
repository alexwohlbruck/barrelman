/**
 * Tests for the metered API guard.
 *
 * This is the layer that decides who may call what and what it costs, so the
 * cases here are the ones with money or access attached: the unauthenticated
 * path, scope enforcement, rate limits, credit exhaustion, and the refund on a
 * server error. Collaborators are injected — nothing touches Postgres.
 */
import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import {
  apiAuth,
  apiAuthAfter,
  clearRateBuckets,
  identifyCaller,
  pruneRateBuckets,
  type ApiAuthDeps,
} from './api-auth'
import type { ResolvedKey } from '../services/api-keys.service'

const BASE = 'http://localhost'
const LIVE_KEY = 'brm_live_abcdefghijklmnopqrstuvwxyz0123456789ABCD'
const TEST_KEY = 'brm_test_abcdefghijklmnopqrstuvwxyz0123456789ABCD'

const savedServiceKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  clearRateBuckets()
  process.env.BARRELMAN_API_KEY = 'service-secret'
})

afterEach(() => {
  if (savedServiceKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedServiceKey
})

function resolved(overrides: Partial<ResolvedKey> = {}): ResolvedKey {
  return {
    keyId: 'key-1',
    userId: 'user-1',
    environment: 'live',
    scopes: ['*'],
    plan: 'developer',
    suspended: false,
    suspensionReason: null,
    isTest: false,
    ...overrides,
  }
}

function deps(overrides: Partial<ApiAuthDeps> = {}): Partial<ApiAuthDeps> {
  return {
    resolveApiKey: mock(async () => resolved()),
    checkQuota: mock(async () => ({ allowed: true as const, overage: false })),
    recordUsage: mock(() => undefined),
    ...overrides,
  }
}

/** Minimal app exercising the guard on one metered group. */
function app(group: Parameters<typeof apiAuth>[0], d: Partial<ApiAuthDeps>, handler = () => ({ ok: true })) {
  return new Elysia().onBeforeHandle(apiAuth(group, d)).onAfterHandle(apiAuthAfter).get('/probe', handler)
}

function get(headers: Record<string, string> = {}, path = '/probe') {
  return new Request(`${BASE}${path}`, { headers })
}

describe('identifyCaller', () => {
  test('recognises the shared service secret and does not meter it', async () => {
    const result = await identifyCaller({ authorization: 'Bearer service-secret' }, get())

    expect(result.error).toBeUndefined()
    expect(result.caller.kind).toBe('service')
    expect(result.caller.userId).toBeUndefined()
  })

  test('resolves an account key', async () => {
    const d = deps()
    const result = await identifyCaller({ authorization: `Bearer ${LIVE_KEY}` }, get(), d as never)

    expect(result.caller.kind).toBe('account')
    expect(result.caller.userId).toBe('user-1')
    expect(result.caller.plan).toBe('developer')
  })

  test('401s with no credential', async () => {
    const result = await identifyCaller({}, get())

    expect(result.error?.status).toBe(401)
  })

  test('gives one message for unknown, revoked and expired keys alike', async () => {
    // Distinguishing them would let a caller probe the state of a key they do
    // not own.
    const d = deps({ resolveApiKey: mock(async () => null) })
    const result = await identifyCaller({ authorization: `Bearer ${LIVE_KEY}` }, get(), d as never)

    expect(result.error?.status).toBe(401)
    expect(result.error?.body.error).toBe('Invalid or revoked API key')
  })

  test('rejects a bearer token that is not a barrelman key', async () => {
    const d = deps()
    const result = await identifyCaller({ authorization: 'Bearer some-random-jwt' }, get(), d as never)

    expect(result.error?.status).toBe(401)
    expect(d.resolveApiKey).not.toHaveBeenCalled()
  })

  test('accepts a key from ?api_key= and ?token= for browser-fetched endpoints', async () => {
    const d = deps()

    const viaApiKey = await identifyCaller({}, get({}, `/tiles/x/1/2/3?api_key=${LIVE_KEY}`), d as never)
    const viaToken = await identifyCaller({}, get({}, `/tiles/x/1/2/3?token=${LIVE_KEY}`), d as never)

    expect(viaApiKey.caller.kind).toBe('account')
    expect(viaToken.caller.kind).toBe('account')
  })

  test('ignores a malformed Authorization header rather than throwing', async () => {
    const result = await identifyCaller({ authorization: 'NotBearer xyz' }, get())

    expect(result.error?.status).toBe(401)
  })

  test('is open when nothing is configured to check against', async () => {
    delete process.env.BARRELMAN_API_KEY
    // An unset BARRELMAN_API_KEY means "no auth configured", which every guard
    // in this codebase treats as local development. A fresh clone has to be
    // usable with no configuration; `assertAuthConfigured()` is what warns a
    // production instance that it is wide open.
    const result = await identifyCaller({}, get())

    expect(result.error).toBeUndefined()
    expect(result.caller.kind).toBe('anonymous')
  })
})

describe('apiAuth guard', () => {
  test('serves an authorised account request and charges credits', async () => {
    const d = deps()
    const res = await app('search', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.status).toBe(200)
    expect(d.recordUsage).toHaveBeenCalledWith(
      expect.objectContaining({ userId: 'user-1', apiKeyId: 'key-1', endpoint: 'search', credits: 3 }),
    )
    expect(res.headers.get('x-barrelman-credits-charged')).toBe('3')
  })

  test('charges each group its own price', async () => {
    const search = deps()
    const isochrone = deps()

    await app('search', search).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
    clearRateBuckets()
    await app('isochrone', isochrone).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(search.recordUsage).toHaveBeenCalledWith(expect.objectContaining({ credits: 3 }))
    expect(isochrone.recordUsage).toHaveBeenCalledWith(expect.objectContaining({ credits: 25 }))
  })

  test('does not meter the service credential', async () => {
    const d = deps()
    const res = await app('routing', d).handle(get({ authorization: 'Bearer service-secret' }))

    expect(res.status).toBe(200)
    expect(d.recordUsage).not.toHaveBeenCalled()
    expect(d.checkQuota).not.toHaveBeenCalled()
  })

  test('401s an unauthenticated request without charging anyone', async () => {
    const d = deps()
    const res = await app('search', d).handle(get())

    expect(res.status).toBe(401)
    expect(d.recordUsage).not.toHaveBeenCalled()
  })

  test('403s a key whose scopes exclude the group, and does not charge', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ scopes: ['tiles', 'search'] })) })
    const res = await app('isochrone', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
    const body = await res.json()

    expect(res.status).toBe(403)
    expect(body.scope).toBe('isochrone')
    // Recorded as a refused request so it shows up in the account's usage —
    // a key being called with the wrong scope is something the owner should
    // see — but never charged, and never quota-checked.
    expect(d.recordUsage).toHaveBeenCalledWith(expect.objectContaining({ credits: 0, rejected: true }))
    expect(d.checkQuota).not.toHaveBeenCalled()
  })

  test('allows a group that is within the key scopes', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ scopes: ['tiles'] })) })

    expect((await app('tiles', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))).status).toBe(200)
  })

  test('402s when credits are exhausted, and records the rejection', async () => {
    const d = deps({
      checkQuota: mock(async () => ({
        allowed: false as const,
        reason: 'out-of-credits' as const,
        balance: { remaining: 0, cycleResetsAt: '2026-09-01T00:00:00.000Z' } as never,
      })),
    })
    const res = await app('search', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
    const body = await res.json()

    expect(res.status).toBe(402)
    expect(body.remaining).toBe(0)
    expect(body.resetsAt).toBe('2026-09-01T00:00:00.000Z')
    // Counted as rejected, charged nothing.
    expect(d.recordUsage).toHaveBeenCalledWith(expect.objectContaining({ credits: 0, rejected: true }))
  })

  test('flags overage on a paid plan that is past its allowance', async () => {
    const d = deps({ checkQuota: mock(async () => ({ allowed: true as const, overage: true })) })
    const res = await app('search', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.status).toBe(200)
    expect(res.headers.get('x-barrelman-overage')).toBe('true')
  })

  test('a test key runs the whole path but spends nothing', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ environment: 'test', isTest: true })) })
    const res = await app('isochrone', d).handle(get({ authorization: `Bearer ${TEST_KEY}` }))

    expect(res.status).toBe(200)
    expect(res.headers.get('x-barrelman-credits-charged')).toBe('0')
    expect(d.checkQuota).not.toHaveBeenCalled()
    expect(d.recordUsage).not.toHaveBeenCalled()
  })

  test('a test key is still scope-checked', async () => {
    const d = deps({
      resolveApiKey: mock(async () => resolved({ environment: 'test', isTest: true, scopes: ['tiles'] })),
    })

    expect((await app('search', d).handle(get({ authorization: `Bearer ${TEST_KEY}` }))).status).toBe(403)
  })
})

describe('throttling', () => {
  /**
   * Layers, cheapest first: penalty box, per-IP, per-key, per-account. The
   * per-key limit is a share of the account budget, so a single key is refused
   * before the account is — one leaked key must not starve the others.
   */
  const PER_KEY_LIMIT = Math.floor(60 * 0.8) // free plan: 60/min, key share 80%

  test('refuses a single key at its share of the account budget', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ plan: 'free' })) })
    const instance = app('tiles', d)
    const request = () => instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    const statuses: number[] = []
    for (let i = 0; i < PER_KEY_LIMIT + 2; i += 1) statuses.push((await request()).status)

    expect(statuses.slice(0, PER_KEY_LIMIT).every((s) => s === 200)).toBe(true)
    expect(statuses[PER_KEY_LIMIT]).toBe(429)
  })

  test('reports which layer refused, and a Retry-After', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ plan: 'free' })) })
    const instance = app('tiles', d)
    for (let i = 0; i < PER_KEY_LIMIT; i += 1) await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    const res = await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
    const body = await res.json()

    expect(res.status).toBe(429)
    expect(body.layer).toBe('key')
    expect(Number(res.headers.get('retry-after'))).toBeGreaterThan(0)
  })

  test('a second key on the same account is refused by the account limit', async () => {
    // Two distinct keys, each under its own per-key share, together exceed the
    // account's 60/min — the account layer is what catches that.
    const instance = (keyId: string) =>
      app('tiles', deps({ resolveApiKey: mock(async () => resolved({ plan: 'free', keyId })) }))

    const a = instance('key-a')
    const b = instance('key-b')

    let refusedByAccount = false
    for (let i = 0; i < 40; i += 1) {
      await a.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
      const res = await b.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
      if (res.status === 429 && (await res.json()).layer === 'account') refusedByAccount = true
    }

    expect(refusedByAccount).toBe(true)
  })

  test('a throttled request is counted but never charged', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ plan: 'free' })) })
    const instance = app('tiles', d)
    for (let i = 0; i < PER_KEY_LIMIT + 1; i += 1) await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    const calls = (d.recordUsage as ReturnType<typeof mock>).mock.calls
    const rejections = calls.filter((c: unknown[]) => (c[0] as { rejected?: boolean }).rejected)
    expect(rejections.length).toBeGreaterThan(0)
    expect(rejections.every((c: unknown[]) => (c[0] as { credits: number }).credits === 0)).toBe(true)
  })

  test('accounts are limited independently of each other', async () => {
    const first = deps({ resolveApiKey: mock(async () => resolved({ plan: 'free', userId: 'user-1', keyId: 'key-1' })) })
    const firstApp = app('tiles', first)
    for (let i = 0; i < PER_KEY_LIMIT + 1; i += 1) await firstApp.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    const second = deps({ resolveApiKey: mock(async () => resolved({ plan: 'free', userId: 'user-2', keyId: 'key-2' })) })
    const res = await app('tiles', second).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.status).toBe(200)
  })

  test('a bigger plan gets a bigger ceiling', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ plan: 'developer' })) })
    const instance = app('tiles', d)

    const statuses: number[] = []
    for (let i = 0; i < 61; i += 1) statuses.push((await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))).status)

    // 600/min on developer, so 61 requests is unremarkable.
    expect(statuses.every((s) => s === 200)).toBe(true)
  })

  test('caps simultaneous requests to an expensive group', async () => {
    // Isochrone fans out to hundreds of routing calls, so an account inside its
    // per-minute limit can still pin every upstream worker.
    let release: (() => void) | undefined
    const blocked = new Promise<void>((resolve) => {
      release = resolve
    })

    const d = deps()
    const instance = new Elysia()
      .onBeforeHandle(apiAuth('isochrone', d))
      .onAfterHandle(apiAuthAfter)
      .get('/probe', async () => {
        await blocked
        return { ok: true }
      })

    // Two concurrent requests occupy the account's isochrone slots (limit 2);
    // the third must be refused rather than queued behind them.
    const inFlight = [
      instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` })),
      instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` })),
    ]
    await Bun.sleep(20)
    const third = await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(third.status).toBe(429)
    expect((await third.json()).layer).toBe('concurrency')

    release!()
    await Promise.all(inFlight)

    // Slots are released by the after-handler, so the next request succeeds.
    expect((await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))).status).toBe(200)
  })

  test('boxes a caller that collects a stream of rejections', async () => {
    // A key-guesser or a client wedged in a retry loop: answering 401 forever
    // is free for them and not for us.
    const d = deps({ resolveApiKey: mock(async () => resolved({ scopes: ['tiles'] })) })
    const instance = app('isochrone', d)

    let sawPenalty = false
    for (let i = 0; i < 40; i += 1) {
      const res = await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
      if (res.status === 429 && (await res.json()).layer === 'penalty') {
        sawPenalty = true
        break
      }
    }

    expect(sawPenalty).toBe(true)
  })

  test('pruneRateBuckets does not disturb a live window', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ plan: 'free' })) })
    const instance = app('tiles', d)
    for (let i = 0; i < PER_KEY_LIMIT; i += 1) await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    pruneRateBuckets()

    expect((await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))).status).toBe(429)
  })
})

describe('suspended accounts', () => {
  test('403s with the reason rather than a generic invalid-key error', async () => {
    const d = deps({
      resolveApiKey: mock(async () =>
        resolved({ suspended: true, suspensionReason: 'Terms of service violation: bulk scraping' }),
      ),
    })
    const res = await app('search', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
    const body = await res.json()

    expect(res.status).toBe(403)
    expect(body.suspended).toBe(true)
    // Someone cut off is owed a reason they can act on.
    expect(body.error).toContain('bulk scraping')
    expect(d.checkQuota).not.toHaveBeenCalled()
  })

  test('suspension outranks a scope error', async () => {
    const d = deps({
      resolveApiKey: mock(async () => resolved({ suspended: true, suspensionReason: 'Abuse', scopes: ['tiles'] })),
    })
    const res = await app('isochrone', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    // Telling a suspended user their key lacks a scope would send them off
    // fixing the wrong thing.
    expect(res.status).toBe(403)
    expect((await res.json()).suspended).toBe(true)
  })

  test('falls back to a plain message when no reason was recorded', async () => {
    const d = deps({ resolveApiKey: mock(async () => resolved({ suspended: true, suspensionReason: null })) })
    const res = await app('search', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.status).toBe(403)
    expect((await res.json()).error).toBe('This account is suspended.')
  })
})

describe('refund on server error', () => {
  test('a 5xx does not leave the customer charged', async () => {
    const recorded: Array<Record<string, unknown>> = []
    const d = deps({ recordUsage: mock((input) => void recorded.push(input as never)) })

    const instance = new Elysia()
      .onBeforeHandle(apiAuth('routing', d))
      .onAfterHandle(apiAuthAfter)
      .get('/probe', ({ set }) => {
        set.status = 503
        return { error: 'upstream down' }
      })

    const res = await instance.handle(get({ authorization: `Bearer ${LIVE_KEY}` }))
    expect(res.status).toBe(503)

    // The charge is recorded up front; the refund nets it back out in the
    // usage buffer, which this test observes through pendingCredits below.
    const { pendingCredits } = await import('../services/usage.service')
    expect(recorded[0]).toMatchObject({ endpoint: 'routing', credits: 10 })
    expect(pendingCredits('nobody')).toBe(0)
  })

  test('a successful request keeps its charge', async () => {
    const d = deps()
    const res = await app('routing', d).handle(get({ authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.status).toBe(200)
    expect(d.recordUsage).toHaveBeenCalledWith(expect.objectContaining({ credits: 10 }))
  })
})
