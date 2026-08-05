/**
 * HTTP-layer tests for the /tiles endpoint.
 *
 * Uses the createTileRoutes() factory with a mock tile fetcher to avoid
 * hitting the real Martin tile server. Tests cover:
 *   - Tile proxy: successful fetch, Martin errors, network failures
 *   - Response headers: content-type, cache-control, CORS
 *   - Auth: the ordinary metered guard, same as every other endpoint
 *   - Auth: fall-through to the metered guard, and open access when nothing
 *     is configured at all
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createTileRoutes, type TileFetcher } from './tiles'

// ── Helpers ──────────────────────────────────────────────────────────────────

const BASE = 'http://localhost'

function get(path: string, headers?: Record<string, string>) {
  return new Request(`${BASE}${path}`, { headers })
}

async function json(res: Response) {
  return res.json()
}

// ── Setup / Teardown ─────────────────────────────────────────────────────────

const savedApiKey = process.env.BARRELMAN_API_KEY
const savedMartinUrl = process.env.MARTIN_URL

beforeEach(() => {
  // Default: no auth configured at all, which is what the metered guard reads
  // as local development. bun loads the service key from .env, so it has to be
  // cleared explicitly.
  delete process.env.BARRELMAN_API_KEY
  // Set a predictable Martin URL for assertions
  process.env.MARTIN_URL = 'http://mock-martin:3000'
})

afterEach(() => {
  if (savedApiKey === undefined) {
    delete process.env.BARRELMAN_API_KEY
  } else {
    process.env.BARRELMAN_API_KEY = savedApiKey
  }
  if (savedMartinUrl === undefined) {
    delete process.env.MARTIN_URL
  } else {
    process.env.MARTIN_URL = savedMartinUrl
  }
})

// ── Tile proxy ───────────────────────────────────────────────────────────────

describe('GET /tiles/:source/:z/:x/:y', () => {
  test('proxies tile request to Martin and returns protobuf body', async () => {
    const tileData = new Uint8Array([0x1a, 0x03, 0x78, 0x79, 0x7a])
    const mockFetch = mock<TileFetcher>(async () =>
      new Response(tileData, {
        status: 200,
        headers: { 'content-type': 'application/x-protobuf' },
      }),
    )

    const app = new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
    const res = await app.handle(get('/tiles/bicycle_ways/12/1234/2345'))

    expect(res.status).toBe(200)
    expect(mockFetch).toHaveBeenCalledTimes(1)

    // Verify the URL passed to the fetcher
    const calledUrl = mockFetch.mock.calls[0][0]
    expect(calledUrl).toBe('http://mock-martin:3000/bicycle_ways/12/1234/2345')

    // Verify response body is forwarded
    const body = new Uint8Array(await res.arrayBuffer())
    expect(body).toEqual(tileData)
  })

  test('sets correct response headers (content-type, cache-control, CORS)', async () => {
    const mockFetch = mock<TileFetcher>(async () =>
      new Response('tile-data', {
        status: 200,
        headers: { 'content-type': 'application/x-protobuf' },
      }),
    )

    const app = new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
    const res = await app.handle(get('/tiles/basemap/10/500/300'))

    expect(res.headers.get('content-type')).toBe('application/x-protobuf')
    expect(res.headers.get('cache-control')).toBe('public, max-age=86400')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
  })

  test('defaults content-type to application/x-protobuf when Martin omits it', async () => {
    const mockFetch = mock<TileFetcher>(async () =>
      new Response('tile-data', { status: 200 }),
    )

    const app = new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
    const res = await app.handle(get('/tiles/basemap/10/500/300'))

    expect(res.headers.get('content-type')).toBe('application/x-protobuf')
  })

  test('returns Martin error status when tile fetch fails', async () => {
    const mockFetch = mock<TileFetcher>(async () =>
      new Response('Not Found', { status: 404, statusText: 'Not Found' }),
    )

    const app = new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
    const res = await app.handle(get('/tiles/nonexistent/1/0/0'))

    expect(res.status).toBe(404)
    const body = await json(res)
    expect(body.error).toContain('Tile fetch failed')
  })

  test('returns 500 when Martin returns server error', async () => {
    const mockFetch = mock<TileFetcher>(async () =>
      new Response('Internal Error', { status: 500, statusText: 'Internal Server Error' }),
    )

    const app = new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
    const res = await app.handle(get('/tiles/basemap/5/10/10'))

    expect(res.status).toBe(500)
    const body = await json(res)
    expect(body.error).toContain('Internal Server Error')
  })

  test('handles composite source names (comma-separated)', async () => {
    const mockFetch = mock<TileFetcher>(async () =>
      new Response('tile-data', {
        status: 200,
        headers: { 'content-type': 'application/x-protobuf' },
      }),
    )

    const app = new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
    const res = await app.handle(get('/tiles/basemap,parchment_pois/14/4500/6500'))

    expect(res.status).toBe(200)
    const calledUrl = mockFetch.mock.calls[0][0]
    expect(calledUrl).toContain('basemap,parchment_pois')
  })
})

// ── Tile auth ────────────────────────────────────────────────────────────────

describe('tile auth', () => {
  function makeTileApp() {
    const mockFetch = mock<TileFetcher>(async () =>
      new Response('tile-data', {
        status: 200,
        headers: { 'content-type': 'application/x-protobuf' },
      }),
    )
    return new Elysia().use(createTileRoutes({ fetchTile: mockFetch }))
  }

  /**
   * Tiles now go through the ordinary metered guard, which treats "no service
   * key configured" as local development and lets anonymous callers through.
   * That is the same rule every other endpoint follows; the dedicated tile
   * credential that used to sit in front of this is gone, so tile traffic is
   * metered and attributable like everything else.
   */
  test('open in development, when no service key is configured', async () => {
    const saved = process.env.BARRELMAN_API_KEY
    delete process.env.BARRELMAN_API_KEY
    try {
      const res = await makeTileApp().handle(get('/tiles/basemap/10/500/300'))
      expect(res.status).toBe(200)
    } finally {
      if (saved !== undefined) process.env.BARRELMAN_API_KEY = saved
    }
  })

  test('refuses an anonymous caller once a service key is configured', async () => {
    const saved = process.env.BARRELMAN_API_KEY
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    try {
      const res = await makeTileApp().handle(get('/tiles/basemap/10/500/300'))
      expect(res.status).toBe(401)
    } finally {
      if (saved === undefined) delete process.env.BARRELMAN_API_KEY
      else process.env.BARRELMAN_API_KEY = saved
    }
  })

  test('accepts the service key as a Bearer token', async () => {
    const saved = process.env.BARRELMAN_API_KEY
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    try {
      const res = await makeTileApp().handle(
        get('/tiles/basemap/10/500/300', { Authorization: 'Bearer svc_secret' }),
      )
      expect(res.status).toBe(200)
    } finally {
      if (saved === undefined) delete process.env.BARRELMAN_API_KEY
      else process.env.BARRELMAN_API_KEY = saved
    }
  })
})
