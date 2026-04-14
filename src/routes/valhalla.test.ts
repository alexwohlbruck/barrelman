/**
 * HTTP-layer tests for the /valhalla proxy.
 *
 * Uses createValhallaRoutes() with a mock fetcher to avoid hitting a real
 * Valhalla container. Covers:
 *   - GET passthrough with query string
 *   - POST passthrough with JSON body and content-type
 *   - Upstream status codes flow through unchanged
 *   - 502 when fetcher throws (Valhalla unreachable)
 *   - Auth: BARRELMAN_API_KEY required, inbound Bearer never leaks upstream
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createValhallaRoutes, type ValhallaFetcher } from './valhalla'

const BASE = 'http://localhost'

function req(path: string, init: RequestInit = {}) {
  return new Request(`${BASE}${path}`, init)
}

async function json(res: Response) {
  return res.json()
}

const savedApiKey = process.env.BARRELMAN_API_KEY
const savedValhallaUrl = process.env.VALHALLA_URL

beforeEach(() => {
  // Default: open access (no key configured) so most proxy tests don't need
  // to pass auth headers. Auth-specific tests opt in below.
  delete process.env.BARRELMAN_API_KEY
  process.env.VALHALLA_URL = 'http://mock-valhalla:8002'
})

afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey

  if (savedValhallaUrl === undefined) delete process.env.VALHALLA_URL
  else process.env.VALHALLA_URL = savedValhallaUrl
})

// ── Proxy passthrough ────────────────────────────────────────────────────────

describe('GET /valhalla/*', () => {
  test('proxies /status to upstream and returns JSON body', async () => {
    const upstream = { version: '3.5.1', tileset_last_modified: 1234567890 }
    const mockFetch = mock<ValhallaFetcher>(async () =>
      new Response(JSON.stringify(upstream), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    const res = await app.handle(req('/valhalla/status'))

    expect(res.status).toBe(200)
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(mockFetch.mock.calls[0][0]).toBe('http://mock-valhalla:8002/status')
    expect(mockFetch.mock.calls[0][1].method).toBe('GET')

    const body = await json(res)
    expect(body).toEqual(upstream)
  })

  test('forwards query string on GET /route', async () => {
    const mockFetch = mock<ValhallaFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    const res = await app.handle(req('/valhalla/route?json=%7B%22locations%22%3A%5B%5D%7D'))

    expect(res.status).toBe(200)
    expect(mockFetch.mock.calls[0][0]).toBe(
      'http://mock-valhalla:8002/route?json=%7B%22locations%22%3A%5B%5D%7D',
    )
  })

  test('sets cache-control no-store and CORS headers', async () => {
    const mockFetch = mock<ValhallaFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    const res = await app.handle(req('/valhalla/status'))

    expect(res.headers.get('cache-control')).toBe('no-store')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
    expect(res.headers.get('content-type')).toBe('application/json')
  })

  test('forwards upstream 4xx status unchanged', async () => {
    const mockFetch = mock<ValhallaFetcher>(async () =>
      new Response(JSON.stringify({ error_code: 154 }), {
        status: 400,
        headers: { 'content-type': 'application/json' },
      }),
    )

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    const res = await app.handle(req('/valhalla/route'))

    expect(res.status).toBe(400)
    const body = await json(res)
    expect(body.error_code).toBe(154)
  })

  test('returns 502 when upstream fetch throws', async () => {
    const mockFetch = mock<ValhallaFetcher>(async () => {
      throw new Error('ECONNREFUSED')
    })

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    const res = await app.handle(req('/valhalla/status'))

    expect(res.status).toBe(502)
    const body = await json(res)
    expect(body.error).toContain('unreachable')
    expect(body.detail).toContain('ECONNREFUSED')
  })
})

describe('POST /valhalla/*', () => {
  test('forwards POST body and content-type for /route', async () => {
    let receivedBody: ArrayBuffer | undefined
    let receivedContentType: string | undefined
    let receivedMethod: string | undefined

    const mockFetch = mock<ValhallaFetcher>(async (_url, init) => {
      receivedMethod = init.method
      receivedContentType = (init.headers as Record<string, string>)['content-type']
      receivedBody = init.body as ArrayBuffer
      return new Response('{"trip":{}}', {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    })

    const payload = JSON.stringify({
      locations: [
        { lat: 35.7796, lon: -78.6382 },
        { lat: 36.0726, lon: -79.792 },
      ],
      costing: 'auto',
    })

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    const res = await app.handle(
      req('/valhalla/route', {
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
    const mockFetch = mock<ValhallaFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )

    const app = new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch }))
    await app.handle(
      req('/valhalla/isochrone', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      }),
    )

    expect(mockFetch.mock.calls[0][0]).toBe('http://mock-valhalla:8002/isochrone')
  })
})

// ── Auth ─────────────────────────────────────────────────────────────────────

describe('valhalla auth (BARRELMAN_API_KEY)', () => {
  const API_KEY = 'test_api_secret'

  function makeApp() {
    const mockFetch = mock<ValhallaFetcher>(async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    )
    return {
      app: new Elysia().use(createValhallaRoutes({ fetchValhalla: mockFetch })),
      mockFetch,
    }
  }

  test('open access when BARRELMAN_API_KEY is not set', async () => {
    delete process.env.BARRELMAN_API_KEY
    const { app } = makeApp()
    const res = await app.handle(req('/valhalla/status'))
    expect(res.status).toBe(200)
  })

  test('returns 401 when key is set and no auth provided', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app } = makeApp()
    const res = await app.handle(req('/valhalla/status'))
    expect(res.status).toBe(401)
  })

  test('accepts valid Bearer token', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app } = makeApp()
    const res = await app.handle(
      req('/valhalla/status', { headers: { Authorization: `Bearer ${API_KEY}` } }),
    )
    expect(res.status).toBe(200)
  })

  test('rejects invalid Bearer token', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app } = makeApp()
    const res = await app.handle(
      req('/valhalla/status', { headers: { Authorization: 'Bearer wrong' } }),
    )
    expect(res.status).toBe(401)
  })

  test('inbound Authorization header is NOT forwarded upstream', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app, mockFetch } = makeApp()
    await app.handle(
      req('/valhalla/status', { headers: { Authorization: `Bearer ${API_KEY}` } }),
    )

    const forwardedHeaders = mockFetch.mock.calls[0][1].headers as Record<string, string>
    expect(forwardedHeaders['authorization']).toBeUndefined()
    expect(forwardedHeaders['Authorization']).toBeUndefined()
  })
})
