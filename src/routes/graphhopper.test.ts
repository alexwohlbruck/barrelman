/**
 * HTTP-layer tests for the /graphhopper proxy.
 *
 * Uses createGraphHopperRoutes() with a mock fetcher to avoid hitting a real
 * GraphHopper container. Covers:
 *   - GET passthrough with query string
 *   - POST passthrough with JSON body and content-type
 *   - Upstream status codes flow through unchanged
 *   - 502 when fetcher throws (GraphHopper unreachable)
 *   - Auth: BARRELMAN_API_KEY required, inbound Bearer never leaks upstream
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createGraphHopperRoutes, type GraphHopperFetcher } from './graphhopper'

const BASE = 'http://localhost'

function req(path: string, init: RequestInit = {}) {
  return new Request(`${BASE}${path}`, init)
}

async function json(res: Response) {
  return res.json()
}

const savedApiKey = process.env.BARRELMAN_API_KEY
const savedGraphHopperUrl = process.env.GRAPHHOPPER_URL

beforeEach(() => {
  // Default: open access (no key configured) so most proxy tests don't need
  // to pass auth headers. Auth-specific tests opt in below.
  delete process.env.BARRELMAN_API_KEY
  process.env.GRAPHHOPPER_URL = 'http://mock-graphhopper:8989'
})

afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey

  if (savedGraphHopperUrl === undefined) delete process.env.GRAPHHOPPER_URL
  else process.env.GRAPHHOPPER_URL = savedGraphHopperUrl
})

// ── Proxy passthrough ────────────────────────────────────────────────────────

describe('GET /graphhopper/*', () => {
  test('proxies /health to upstream and returns JSON body', async () => {
    const upstream = { healthy: true }
    const mockFetch = mock<GraphHopperFetcher>(async () =>
      new Response(JSON.stringify(upstream), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    const res = await app.handle(req('/graphhopper/health'))

    expect(res.status).toBe(200)
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(mockFetch.mock.calls[0][0]).toBe('http://mock-graphhopper:8989/health')
    expect(mockFetch.mock.calls[0][1].method).toBe('GET')

    const body = await json(res)
    expect(body).toEqual(upstream)
  })

  test('forwards query string on GET /route', async () => {
    const mockFetch = mock<GraphHopperFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    const res = await app.handle(req('/graphhopper/route?point=52.5,13.4&point=52.6,13.5&profile=bike'))

    expect(res.status).toBe(200)
    expect(mockFetch.mock.calls[0][0]).toBe(
      'http://mock-graphhopper:8989/route?point=52.5,13.4&point=52.6,13.5&profile=bike',
    )
  })

  test('sets cache-control no-store and CORS headers', async () => {
    const mockFetch = mock<GraphHopperFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    const res = await app.handle(req('/graphhopper/health'))

    expect(res.headers.get('cache-control')).toBe('no-store')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
    expect(res.headers.get('content-type')).toBe('application/json')
  })

  test('forwards upstream 4xx status unchanged', async () => {
    const mockFetch = mock<GraphHopperFetcher>(async () =>
      new Response(JSON.stringify({ message: 'Cannot find point' }), {
        status: 400,
        headers: { 'content-type': 'application/json' },
      }),
    )

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    const res = await app.handle(req('/graphhopper/route'))

    expect(res.status).toBe(400)
    const body = await json(res)
    expect(body.message).toBe('Cannot find point')
  })

  test('returns 502 when upstream fetch throws', async () => {
    const mockFetch = mock<GraphHopperFetcher>(async () => {
      throw new Error('ECONNREFUSED')
    })

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    const res = await app.handle(req('/graphhopper/health'))

    expect(res.status).toBe(502)
    const body = await json(res)
    expect(body.error).toContain('unreachable')
    expect(body.detail).toContain('ECONNREFUSED')
  })
})

describe('POST /graphhopper/*', () => {
  test('forwards POST body and content-type for /route', async () => {
    let receivedBody: ArrayBuffer | undefined
    let receivedContentType: string | undefined
    let receivedMethod: string | undefined

    const mockFetch = mock<GraphHopperFetcher>(async (_url, init) => {
      receivedMethod = init.method
      receivedContentType = (init.headers as Record<string, string>)['content-type']
      receivedBody = init.body as ArrayBuffer
      return new Response('{"paths":[]}', {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    })

    const payload = JSON.stringify({
      points: [[-79.532, 36.044], [-79.520, 36.050]],
      profile: 'bike',
    })

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    const res = await app.handle(
      req('/graphhopper/route', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: payload,
      }),
    )

    expect(res.status).toBe(200)
    expect(receivedMethod).toBe('POST')
    expect(receivedContentType).toBe('application/json')
    expect(new TextDecoder().decode(receivedBody)).toBe(payload)
  })

  test('forwards POST /isochrone path correctly', async () => {
    const mockFetch = mock<GraphHopperFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )

    const app = new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch }))
    await app.handle(
      req('/graphhopper/isochrone', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      }),
    )

    expect(mockFetch.mock.calls[0][0]).toBe('http://mock-graphhopper:8989/isochrone')
  })
})

// ── Auth ─────────────────────────────────────────────────────────────────────

describe('graphhopper auth (BARRELMAN_API_KEY)', () => {
  const API_KEY = 'test_api_secret'

  function makeApp() {
    const mockFetch = mock<GraphHopperFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )
    return {
      app: new Elysia().use(createGraphHopperRoutes({ fetchGraphHopper: mockFetch })),
      mockFetch,
    }
  }

  test('open access when BARRELMAN_API_KEY is not set', async () => {
    delete process.env.BARRELMAN_API_KEY
    const { app } = makeApp()
    const res = await app.handle(req('/graphhopper/health'))
    expect(res.status).toBe(200)
  })

  test('returns 401 when key is set and no auth provided', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app } = makeApp()
    const res = await app.handle(req('/graphhopper/health'))
    expect(res.status).toBe(401)
  })

  test('accepts valid Bearer token', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app } = makeApp()
    const res = await app.handle(
      req('/graphhopper/health', { headers: { Authorization: `Bearer ${API_KEY}` } }),
    )
    expect(res.status).toBe(200)
  })

  test('rejects invalid Bearer token', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app } = makeApp()
    const res = await app.handle(
      req('/graphhopper/health', { headers: { Authorization: 'Bearer wrong' } }),
    )
    expect(res.status).toBe(401)
  })

  test('inbound Authorization header is NOT forwarded upstream', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app, mockFetch } = makeApp()
    await app.handle(
      req('/graphhopper/health', { headers: { Authorization: `Bearer ${API_KEY}` } }),
    )

    const forwardedHeaders = mockFetch.mock.calls[0][1].headers as Record<string, string>
    expect(forwardedHeaders['authorization']).toBeUndefined()
    expect(forwardedHeaders['Authorization']).toBeUndefined()
  })
})
