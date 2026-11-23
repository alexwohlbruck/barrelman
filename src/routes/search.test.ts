import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createSearchRoutes } from './search'
import { LIMITS } from '../lib/params'

function post(body: unknown) {
  return new Request('http://localhost/search', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
}

function appWithStub() {
  const seen: unknown[] = []
  const searchPlaces = (async (params: unknown) => {
    seen.push(params)
    return []
  }) as never
  return { app: new Elysia().use(createSearchRoutes({ searchPlaces })), seen }
}

const line = (n: number) => Array.from({ length: n }, (_, i) => [-80.84 + i * 1e-5, 35.22])

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

describe('POST /search route corridor', () => {
  test('accepts a route at the vertex cap', async () => {
    const { app, seen } = appWithStub()
    const res = await app.handle(post({ route: { type: 'LineString', coordinates: line(LIMITS.maxRoutePoints) } }))
    expect(res.status).toBe(200)
    expect(seen).toHaveLength(1)
  })

  test('refuses a route past the vertex cap before searching', async () => {
    const { app, seen } = appWithStub()
    const res = await app.handle(post({ route: { type: 'LineString', coordinates: line(LIMITS.maxRoutePoints + 1) } }))
    expect(res.status).toBe(422)
    expect(seen).toHaveLength(0)
  })

  test('clamps an oversized limit, buffer and offset', async () => {
    const { app, seen } = appWithStub()
    await app.handle(post({ query: 'cafe', limit: 1e6, buffer: 1e9, offset: 1e9 }))
    expect(seen[0]).toMatchObject({
      limit: LIMITS.maxRows,
      buffer: LIMITS.maxCorridorBufferM,
      offset: LIMITS.maxOffset,
    })
  })
})
