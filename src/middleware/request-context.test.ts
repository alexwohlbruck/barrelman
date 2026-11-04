import { describe, test, expect, beforeEach, afterEach, spyOn } from 'bun:test'
import Elysia, { t } from 'elysia'
import { createRequestContext } from './request-context'
import { createIsochroneRoutes } from '../routes/isochrone'
import { apiAuth, apiAuthAfter, clearRateBuckets } from './api-auth'

const BASE = 'http://localhost'

let errorLog: ReturnType<typeof spyOn>
beforeEach(() => {
  errorLog = spyOn(console, 'error').mockImplementation(() => {})
})
afterEach(() => errorLog.mockRestore())

function app(refuse: () => Promise<{ status: number; body: unknown } | null> = async () => null) {
  return new Elysia()
    .use(createRequestContext({ refuseUnauthenticated: refuse }))
    .get('/ok', () => ({ ok: true }))
    .get('/boom', () => {
      throw new Error('connect ECONNREFUSED 10.0.0.5:5432 password=hunter2')
    })
    .post('/strict', ({ body }) => body, { body: t.Object({ n: t.Number({ maximum: 5 }) }) })
}

const post = (path: string, body: unknown, headers: Record<string, string> = {}) =>
  new Request(`${BASE}${path}`, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) })

describe('X-Request-Id', () => {
  test('is set on success, 404 and validation failures', async () => {
    for (const req of [new Request(`${BASE}/ok`), new Request(`${BASE}/nowhere`), post('/strict', { n: 9 })]) {
      const res = await app().handle(req)
      expect(res.headers.get('x-request-id')).toMatch(/^[0-9a-f-]{36}$/)
    }
  })

  test('keeps a well-formed incoming id and replaces a malformed one', async () => {
    const kept = await app().handle(new Request(`${BASE}/ok`, { headers: { 'x-request-id': 'edge-1234abcd' } }))
    expect(kept.headers.get('x-request-id')).toBe('edge-1234abcd')

    const replaced = await app().handle(new Request(`${BASE}/ok`, { headers: { 'x-request-id': 'bad id\twith spaces' } }))
    expect(replaced.headers.get('x-request-id')).not.toContain(' ')
    expect(replaced.headers.get('x-request-id')).toMatch(/^[0-9a-f-]{36}$/)
  })
})

describe('uncaught errors', () => {
  test('answer a generic 500 with the request id and log the real error', async () => {
    const res = await app().handle(new Request(`${BASE}/boom`))
    expect(res.status).toBe(500)
    const text = await res.text()
    expect(text).not.toContain('ECONNREFUSED')
    expect(text).not.toContain('hunter2')
    const body = JSON.parse(text)
    expect(body).toEqual({ error: 'Internal server error', requestId: res.headers.get('x-request-id') })
    expect(String(errorLog.mock.calls[0][0])).toContain(body.requestId)
  })
})

describe('validation on a guarded route', () => {
  test('a caller the guard would refuse gets the refusal, not the schema', async () => {
    const res = await app(async () => ({ status: 401, body: { error: 'An API key is required.' } })).handle(post('/strict', { n: 9 }))
    expect(res.status).toBe(401)
    const text = await res.text()
    expect(text).not.toContain('maximum')
    expect(JSON.parse(text).error).toBe('An API key is required.')
  })

  test('an identified caller keeps the detailed 422', async () => {
    const res = await app(async () => null).handle(post('/strict', { n: 9 }))
    expect(res.status).toBe(422)
  })
})

describe('with the real guard', () => {
  const saved = process.env.BARRELMAN_API_KEY
  beforeEach(() => {
    clearRateBuckets()
    process.env.BARRELMAN_API_KEY = 'service-secret'
  })
  afterEach(() => {
    if (saved === undefined) delete process.env.BARRELMAN_API_KEY
    else process.env.BARRELMAN_API_KEY = saved
  })

  const isochrone = () => new Elysia().use(createRequestContext()).use(createIsochroneRoutes())

  test('POST /isochrone with a bad body and no key is a 401', async () => {
    const res = await isochrone().handle(post('/isochrone', { lat: 500 }))
    expect(res.status).toBe(401)
    expect(await res.text()).not.toContain('minimum')
  })

  test('POST /isochrone with a bad body and a valid key is a 422', async () => {
    const res = await isochrone().handle(post('/isochrone', { lat: 500 }, { authorization: 'Bearer service-secret' }))
    expect(res.status).toBe(422)
  })
})

describe('the guard is not a free key oracle', () => {
  const saved = process.env.BARRELMAN_API_KEY
  beforeEach(() => {
    clearRateBuckets()
    process.env.BARRELMAN_API_KEY = 'service-secret'
  })
  afterEach(() => {
    clearRateBuckets()
    if (saved === undefined) delete process.env.BARRELMAN_API_KEY
    else process.env.BARRELMAN_API_KEY = saved
  })

  test('bad keys on the validation path take strikes until the penalty box answers', async () => {
    const app = new Elysia().use(createRequestContext()).use(createIsochroneRoutes())
    const statuses: number[] = []
    for (let i = 0; i < 40; i++) {
      const res = await app.handle(post('/isochrone', { lat: 500 }, { authorization: `Bearer guess-${i}` }))
      statuses.push(res.status)
    }
    expect(statuses[0]).toBe(401)
    expect(statuses.at(-1)).toBe(429)
  })
})

describe('a handler that throws', () => {
  const LIVE_KEY = 'brm_live_abcdefghijklmnopqrstuvwxyz0123456789ABCD'
  const saved = process.env.BARRELMAN_API_KEY
  beforeEach(() => {
    clearRateBuckets()
    process.env.BARRELMAN_API_KEY = 'service-secret'
  })
  afterEach(() => {
    if (saved === undefined) delete process.env.BARRELMAN_API_KEY
    else process.env.BARRELMAN_API_KEY = saved
  })

  // Plain functions, never mock(): see admin-users.test.ts.
  const deps = {
    resolveApiKey: async () => ({
      keyId: 'key-1', userId: 'user-1', scopes: ['*'], allowedOrigins: [], plan: 'developer',
      suspended: false, suspensionReason: null,
    }),
    checkQuota: async () => ({ allowed: true as const, overage: false }),
    recordUsage: () => undefined,
  } as never

  function app(handler: () => unknown) {
    return new Elysia()
      .use(createRequestContext())
      .group('', (g) => g.onBeforeHandle(apiAuth('isochrone', deps)).onAfterHandle(apiAuthAfter).get('/probe', handler))
  }
  const get = () => new Request(`${BASE}/probe`, { headers: { authorization: `Bearer ${LIVE_KEY}` } })

  test('releases its concurrency slot, so the account is not locked out', async () => {
    // Isochrone allows two in flight per account. Leaked slots would refuse
    // the third request onwards with 429.
    const throwing = app(() => {
      throw new Error('upstream exploded')
    })
    for (let i = 0; i < 5; i++) expect((await throwing.handle(get())).status).toBe(500)

    expect((await app(() => ({ ok: true })).handle(get())).status).toBe(200)
  })

  test('settles once, so the cap still holds afterwards', async () => {
    const throwing = app(() => {
      throw new Error('boom')
    })
    for (let i = 0; i < 3; i++) await throwing.handle(get())

    // A double release would drive the in-flight count negative and let a
    // third concurrent request through.
    let release: (() => void) | undefined
    const blocked = new Promise<void>((resolve) => (release = resolve))
    const slow = app(async () => {
      await blocked
      return { ok: true }
    })
    const held = [slow.handle(get()), slow.handle(get())]
    await Bun.sleep(20)
    const third = await slow.handle(get())
    expect(third.status).toBe(429)
    release!()
    await Promise.all(held)
  })
})
