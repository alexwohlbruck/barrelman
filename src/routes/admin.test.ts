/**
 * POST /admin/transit/discover-rt-urls spends the operator's Transitland key
 * and writes gtfs_feeds, so it sits behind the admin guard rather than the
 * transit scope it used to take.
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createAdminRoutes } from './admin'
import { createTransitRoutes } from './transit'

const BASE = 'http://localhost'

function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  })
}

/** A counted stub. Never `mock()` for a guard: see admin-users.test.ts. */
function discoverStub() {
  const calls: Array<string | undefined> = []
  const fn = async (feedId?: string) => {
    calls.push(feedId)
    return { checked: 3, updated: 1, errors: 0 }
  }
  return { fn: fn as never, calls }
}

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => {
  process.env.BARRELMAN_API_KEY = 'service-secret'
})
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

describe('POST /admin/transit/discover-rt-urls', () => {
  test('runs for an admin', async () => {
    const { fn, calls } = discoverStub()
    const app = new Elysia().use(createAdminRoutes({ discoverRtUrls: fn, guard: (async () => undefined) as never }))

    const res = await app.handle(post('/admin/transit/discover-rt-urls', { feedId: 'f-1' }))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ checked: 3, updated: 1, errors: 0 })
    expect(calls).toEqual(['f-1'])
  })

  test('the real admin guard refuses the shared service key', async () => {
    const { fn, calls } = discoverStub()
    const app = new Elysia().use(createAdminRoutes({ discoverRtUrls: fn }))

    const res = await app.handle(
      post('/admin/transit/discover-rt-urls', {}, { authorization: 'Bearer service-secret' }),
    )

    expect(res.status).toBeGreaterThanOrEqual(401)
    expect(res.status).not.toBe(200)
    expect(calls).toHaveLength(0)
  })

  test('a refusing guard stops the handler', async () => {
    const { fn, calls } = discoverStub()
    const guard = async ({ set }: { set: { status?: number } }) => {
      set.status = 403
      return { error: 'Administrator access required' }
    }
    const app = new Elysia().use(createAdminRoutes({ discoverRtUrls: fn, guard: guard as never }))

    const res = await app.handle(post('/admin/transit/discover-rt-urls', {}))

    expect(res.status).toBe(403)
    expect(calls).toHaveLength(0)
  })

  test('is gone from the transit group', async () => {
    const app = new Elysia().use(createTransitRoutes())
    const res = await app.handle(
      post('/transit/discover-rt-urls', {}, { authorization: 'Bearer service-secret' }),
    )
    expect(res.status).toBe(404)
  })
})
