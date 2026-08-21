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

import { describe, test, expect, mock, beforeEach, afterEach, beforeAll, afterAll } from 'bun:test'
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
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

// ── Path validation ──────────────────────────────────────────────────────────

describe('tile path validation', () => {
  /**
   * The four segments are concatenated into the upstream Martin URL. Left
   * unchecked, `..` walks out of the tile path and reaches Martin's catalog and
   * per-source metadata — republished through a route the caller is being
   * metered for. The assertion is that nothing is fetched at all, not merely
   * that the response is an error.
   */
  const REJECTED = [
    ['traversal in source', '/tiles/..%2F..%2Fcatalog/1/2/3'],
    ['traversal in y', '/tiles/basemap/1/2/..%2F..%2Fcatalog'],
    ['dot segment as source', '/tiles/../1/2/3'],
    ['query smuggled into y', '/tiles/basemap/1/2/3%3Ffoo%3Dbar'],
    ['non-numeric zoom', '/tiles/basemap/abc/2/3'],
    ['zoom out of range', '/tiles/basemap/999/2/3'],
    ['space in source', '/tiles/basemap%20x/1/2/3'],
  ] as const

  for (const [label, path] of REJECTED) {
    test(`rejects ${label} without calling Martin`, async () => {
      const fetchTile = mock<TileFetcher>(async () => new Response('nope'))
      const app = new Elysia().use(createTileRoutes({ fetchTile }))

      const res = await app.handle(get(path))

      // Some of these never reach the handler at all — `/tiles/../1/2/3` is
      // normalised by the router into a path with no matching route, so it 404s
      // rather than 400s. Either way the request must not reach Martin, which
      // is what is actually being asserted.
      expect(res.status).toBeGreaterThanOrEqual(400)
      expect(fetchTile).not.toHaveBeenCalled()
    })
  }

  test('still allows the shapes real clients send', async () => {
    // Comma-joined sources, deep zoom (x/y run to 2^z - 1), and the `.pbf`
    // suffix some map libraries append.
    const accepted = [
      '/tiles/basemap/0/0/0',
      '/tiles/basemap,parchment_pois/14/4823/6160',
      '/tiles/basemap/22/4194303/4194303',
      '/tiles/basemap/14/4823/6160.pbf',
      '/tiles/some_source-2/10/500/300',
    ]

    for (const path of accepted) {
      const fetchTile = mock<TileFetcher>(async () => new Response('tile'))
      const app = new Elysia().use(createTileRoutes({ fetchTile }))

      const res = await app.handle(get(path))

      expect(res.status).toBe(200)
      expect(fetchTile).toHaveBeenCalledTimes(1)
    }
  })
})

// ── Portolan static tiles ────────────────────────────────────────────────────

describe('portolan tile routes', () => {
  /**
   * Portolan tiles are static files, not a Martin proxy: the routes read a
   * pyramid directory written by `portolan sync`. The fixture mirrors its
   * layout — <dir>/index.json, <dir>/<feed>/tiles.json, <dir>/<feed>/z/x/y.mvt.
   */
  let dir: string
  const TILE_BYTES = new Uint8Array([0x1a, 0x03, 0x78, 0x79, 0x7a])

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), 'portolan-tiles-'))
    writeFileSync(
      join(dir, 'index.json'),
      JSON.stringify([{ feed: 'embark', name: 'EMBARK', bounds: [-97.6, 35.4, -97.5, 35.5], maxzoom: 18 }]),
    )
    mkdirSync(join(dir, 'embark', '8', '66'), { recursive: true })
    writeFileSync(
      join(dir, 'embark', 'tiles.json'),
      JSON.stringify({
        tilejson: '3.0.0',
        name: 'embark',
        tiles: ['{z}/{x}/{y}.mvt'],
        minzoom: 0,
        maxzoom: 18,
        bounds: [-97.6, 35.4, -97.5, 35.5],
        vector_layers: [{ id: 'ribbons', minzoom: 0, maxzoom: 18 }],
      }),
    )
    writeFileSync(join(dir, 'embark', '8', '66', '100.mvt'), TILE_BYTES)
    writeFileSync(
      join(dir, 'embark', 'style.json'),
      JSON.stringify({ feed: 'embark', colors: { '40X': '#0f4d92' } }),
    )
  })

  afterAll(() => {
    rmSync(dir, { recursive: true, force: true })
  })

  function makeApp(tilesDir = dir) {
    // fetchTile mocked so a route mismatch that falls into the Martin proxy
    // fails loudly instead of hitting the network.
    const fetchTile = mock<TileFetcher>(async () => new Response('martin', { status: 200 }))
    return { app: new Elysia().use(createTileRoutes({ fetchTile, portolanTilesDir: tilesDir })), fetchTile }
  }

  test('serves the global index verbatim with json headers', async () => {
    const { app, fetchTile } = makeApp()
    const res = await app.handle(get('/tiles/portolan/index.json'))

    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('application/json')
    expect(res.headers.get('cache-control')).toBe('public, max-age=60')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
    const body = await res.json()
    expect(body[0].feed).toBe('embark')
    expect(fetchTile).not.toHaveBeenCalled()
  })

  test('rewrites the tiles.json template to the public route', async () => {
    const { app } = makeApp()
    const res = await app.handle(get('/tiles/portolan/embark/tiles.json'))

    expect(res.status).toBe(200)
    const body = await res.json()
    // The on-disk template is relative to the pyramid dir and useless to a
    // client; it must come back pointing at this API.
    expect(body.tiles).toEqual(['/tiles/portolan/embark/{z}/{x}/{y}.mvt'])
    // …while everything else survives untouched.
    expect(body.vector_layers[0].id).toBe('ribbons')
    expect(body.maxzoom).toBe(18)
    expect(res.headers.get('cache-control')).toBe('public, max-age=60')
  })

  test('serves the style manifest verbatim', async () => {
    const { app } = makeApp()
    const res = await app.handle(get('/tiles/portolan/embark/style.json'))

    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('application/json')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
    const body = await res.json()
    expect(body.colors['40X']).toBe('#0f4d92')
  })

  test('404s a style manifest for an unknown feed, 400s a bad name', async () => {
    const { app } = makeApp()
    expect((await app.handle(get('/tiles/portolan/nope/style.json'))).status).toBe(404)
    expect((await app.handle(get('/tiles/portolan/..%2Fetc/style.json'))).status).toBe(400)
  })

  test('carries the api_key query into the rewritten template', async () => {
    // A presented key is validated for real by the metered guard, so the test
    // has to present one the guard accepts — the service key, which is also
    // valid via ?api_key= (map libraries cannot set headers).
    const saved = process.env.BARRELMAN_API_KEY
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    try {
      const { app } = makeApp()
      const res = await app.handle(get('/tiles/portolan/embark/tiles.json?api_key=svc_secret'))

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.tiles).toEqual(['/tiles/portolan/embark/{z}/{x}/{y}.mvt?api_key=svc_secret'])
    } finally {
      if (saved === undefined) delete process.env.BARRELMAN_API_KEY
      else process.env.BARRELMAN_API_KEY = saved
    }
  })

  test('serves a tile with protobuf content-type and long cache', async () => {
    const { app, fetchTile } = makeApp()
    const res = await app.handle(get('/tiles/portolan/embark/8/66/100'))

    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('application/x-protobuf')
    expect(res.headers.get('cache-control')).toBe('public, max-age=3600')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
    expect(new Uint8Array(await res.arrayBuffer())).toEqual(TILE_BYTES)
    expect(fetchTile).not.toHaveBeenCalled()
  })

  test('accepts the .mvt suffix some map libraries append', async () => {
    const { app } = makeApp()
    const res = await app.handle(get('/tiles/portolan/embark/8/66/100.mvt'))
    expect(res.status).toBe(200)
  })

  test('a missing tile inside the pyramid is an empty 204, not an error', async () => {
    const { app } = makeApp()
    const res = await app.handle(get('/tiles/portolan/embark/8/66/101'))

    // The cutter only writes tiles a feature touches — absence means empty.
    expect(res.status).toBe(204)
    expect((await res.arrayBuffer()).byteLength).toBe(0)
  })

  test('unknown feed 404s on tiles.json', async () => {
    const { app } = makeApp()
    const res = await app.handle(get('/tiles/portolan/nope/tiles.json'))
    expect(res.status).toBe(404)
  })

  test('a missing tiles dir serves 404s rather than crashing', async () => {
    const { app } = makeApp('/nonexistent/portolan/tiles')
    expect((await app.handle(get('/tiles/portolan/index.json'))).status).toBe(404)
    expect((await app.handle(get('/tiles/portolan/embark/tiles.json'))).status).toBe(404)
  })

  test('rejects traversal and malformed segments without touching the filesystem root', async () => {
    const { app, fetchTile } = makeApp()
    const rejected = [
      '/tiles/portolan/..%2F..%2Fetc/tiles.json',
      '/tiles/portolan/embark%2F..%2F..%2Fother/tiles.json',
      '/tiles/portolan/embark/999/66/100',
      '/tiles/portolan/embark/8/abc/100',
      '/tiles/portolan/em%20bark/8/66/100',
    ]
    for (const path of rejected) {
      const res = await app.handle(get(path))
      expect(res.status).toBeGreaterThanOrEqual(400)
    }
    expect(fetchTile).not.toHaveBeenCalled()
  })

  test('portolan routes sit behind the same metered guard as Martin tiles', async () => {
    const saved = process.env.BARRELMAN_API_KEY
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    try {
      const { app } = makeApp()
      const anon = await app.handle(get('/tiles/portolan/embark/8/66/100'))
      expect(anon.status).toBe(401)

      const authed = await app.handle(
        get('/tiles/portolan/embark/8/66/100', { Authorization: 'Bearer svc_secret' }),
      )
      expect(authed.status).toBe(200)
    } finally {
      if (saved === undefined) delete process.env.BARRELMAN_API_KEY
      else process.env.BARRELMAN_API_KEY = saved
    }
  })
})
