/**
 * HTTP-layer tests for all Barrelman routes using Elysia's app.handle().
 *
 * Service functions are injected directly via the create*Routes() factories —
 * no mock.module() is needed, so this file does not contaminate the module
 * registry and can run in the same `bun test` process as the service tests.
 *
 * Regressions covered:
 *   - /place/:type/:id returns HTTP 404 (not 500) when place not found
 *   - Auth: 401 for missing / wrong key when env key is configured
 *   - Auth: open access when BARRELMAN_API_KEY env var is absent
 *   - Required fields produce 422 when missing
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { authHandler } from '../middleware/auth'
import { createHealthRoutes }   from './health'
import { createSearchRoutes }   from './search'
import { createNearbyRoutes }   from './nearby'
import { createContainsRoutes } from './contains'
import { createChildrenRoutes } from './children'
import { createPlaceRoutes }    from './place'
import { createGeocodeRoutes }  from './geocode'

// ── Service mocks ─────────────────────────────────────────────────────────────

const mockCheckHealth      = mock(async () => ({ status: 'ok' as const, database: 'connected' as const }))
const mockSearchPlaces     = mock(async () => [] as any[])
const mockFindNearby       = mock(async () => [] as any[])
const mockFindContaining   = mock(async () => [] as any[])
const mockFindChildren     = mock(async () => [] as any[])
const mockGetPlace         = mock(async () => null as any)
const mockReverseGeocode   = mock(async () => ({ address: {}, hierarchy: [] }))

// ── App factory ───────────────────────────────────────────────────────────────

function makeApp() {
  return new Elysia()
    .use(createHealthRoutes({ checkHealth: mockCheckHealth }))
    .use(createSearchRoutes({ searchPlaces: mockSearchPlaces }))
    .use(createNearbyRoutes({ findNearby: mockFindNearby }))
    .use(createContainsRoutes({ findContainingAreas: mockFindContaining }))
    .use(createChildrenRoutes({ findChildren: mockFindChildren }))
    .use(createPlaceRoutes({ getPlace: mockGetPlace }))
    .use(createGeocodeRoutes({ reverseGeocode: mockReverseGeocode }))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const VALID_KEY = 'brm_test_key'
const BASE = 'http://localhost'

function get(path: string) {
  return new Request(`${BASE}${path}`)
}

function post(path: string, body: any) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

async function json(res: Response) {
  return res.json()
}

// ── Setup / Teardown ─────────────────────────────────────────────────────────

const savedKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  // Default: no auth required (dev mode)
  delete process.env.BARRELMAN_API_KEY

  mockCheckHealth.mockReset()
  mockSearchPlaces.mockReset()
  mockFindNearby.mockReset()
  mockFindContaining.mockReset()
  mockFindChildren.mockReset()
  mockGetPlace.mockReset()
  mockReverseGeocode.mockReset()

  mockCheckHealth.mockImplementation(async () => ({ status: 'ok', database: 'connected' }))
  mockSearchPlaces.mockImplementation(async () => [])
  mockFindNearby.mockImplementation(async () => [])
  mockFindContaining.mockImplementation(async () => [])
  mockFindChildren.mockImplementation(async () => [])
  mockGetPlace.mockImplementation(async () => null)
  mockReverseGeocode.mockImplementation(async () => ({ address: {}, hierarchy: [] }))
})

afterEach(() => {
  if (savedKey === undefined) {
    delete process.env.BARRELMAN_API_KEY
  } else {
    process.env.BARRELMAN_API_KEY = savedKey
  }
})

// ── /health ───────────────────────────────────────────────────────────────────

describe('GET /health', () => {
  test('returns 200 ok/connected (no auth required)', async () => {
    const app = makeApp()
    const res = await app.handle(get('/health'))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(body.status).toBe('ok')
    expect(body.database).toBe('connected')
  })

  test('reflects disconnected status from service', async () => {
    mockCheckHealth.mockImplementation(async () => ({ status: 'error', database: 'disconnected' }))
    const app = makeApp()
    const res = await app.handle(get('/health'))
    const body = await json(res)
    expect(body.status).toBe('error')
    expect(body.database).toBe('disconnected')
  })
})

// ── Auth middleware ───────────────────────────────────────────────────────────
//
// Elysia named-plugin singletons can only be applied to one app instance — the
// plugin's hooks are skipped for subsequent app instances. Auth tests therefore
// build a minimal app using `authHandler` directly via `.onBeforeHandle()`,
// which correctly re-runs the handler for every new app instance.

describe('auth middleware', () => {
  /** Minimal app that attaches authHandler without going through the plugin singleton. */
  function makeAuthApp() {
    return new Elysia()
      .onBeforeHandle(authHandler as any)
      .post('/search', async () => mockSearchPlaces({ query: '' }))
  }

  test('returns 401 when Authorization header is missing and env key is set', async () => {
    process.env.BARRELMAN_API_KEY = VALID_KEY
    const app = makeAuthApp()
    const res = await app.handle(post('/search', { query: 'cafe' }))
    expect(res.status).toBe(401)
    const body = await json(res)
    expect(body.error).toBeDefined()
  })

  test('returns 401 when wrong Bearer token is provided', async () => {
    process.env.BARRELMAN_API_KEY = VALID_KEY
    const app = makeAuthApp()
    const res = await app.handle(
      new Request(`${BASE}/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer wrong_key' },
        body: JSON.stringify({ query: 'cafe' }),
      }),
    )
    expect(res.status).toBe(401)
  })

  test('returns 200 with correct Bearer token', async () => {
    process.env.BARRELMAN_API_KEY = VALID_KEY
    const app = makeAuthApp()
    const res = await app.handle(
      new Request(`${BASE}/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${VALID_KEY}` },
        body: JSON.stringify({ query: 'cafe' }),
      }),
    )
    expect(res.status).toBe(200)
  })

  test('open access when BARRELMAN_API_KEY env var is absent', async () => {
    delete process.env.BARRELMAN_API_KEY
    const app = makeAuthApp()
    // No Authorization header — should pass through
    const res = await app.handle(post('/search', { query: 'cafe' }))
    expect(res.status).toBe(200)
  })
})

// ── POST /search ──────────────────────────────────────────────────────────────

describe('POST /search', () => {
  test('returns 200 with results array', async () => {
    mockSearchPlaces.mockImplementation(async () => [{ id: 'node/1', name: 'Library' }])
    const app = makeApp()
    const res = await app.handle(post('/search', { query: 'library' }))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(Array.isArray(body)).toBe(true)
    expect(body[0].id).toBe('node/1')
  })

  test('passes all params to searchPlaces service', async () => {
    let captured: any
    mockSearchPlaces.mockImplementation(async (params: any) => { captured = params; return [] })
    const app = makeApp()
    await app.handle(post('/search', {
      query: 'coffee', lat: 36.2, lng: -81.6, radius: 500, limit: 5, semantic: true, autocomplete: false,
    }))
    expect(captured.query).toBe('coffee')
    expect(captured.lat).toBe(36.2)
    expect(captured.lng).toBe(-81.6)
    expect(captured.radius).toBe(500)
    expect(captured.limit).toBe(5)
    expect(captured.semantic).toBe(true)
    expect(captured.autocomplete).toBe(false)
  })

  test('returns 422 when query field is missing', async () => {
    const app = makeApp()
    const res = await app.handle(post('/search', { lat: 36.2, lng: -81.6 }))
    expect(res.status).toBe(422)
  })
})

// ── POST /nearby ──────────────────────────────────────────────────────────────

describe('POST /nearby', () => {
  test('returns 200 with results array', async () => {
    mockFindNearby.mockImplementation(async () => [{ id: 'node/1', name: 'Cafe', distance_m: 50 }])
    const app = makeApp()
    const res = await app.handle(post('/nearby', { lat: 36.2, lng: -81.6 }))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(Array.isArray(body)).toBe(true)
    expect(body[0].id).toBe('node/1')
  })

  test('returns 422 when lat is missing', async () => {
    const app = makeApp()
    const res = await app.handle(post('/nearby', { lng: -81.6 }))
    expect(res.status).toBe(422)
  })

  test('returns 422 when lng is missing', async () => {
    const app = makeApp()
    const res = await app.handle(post('/nearby', { lat: 36.2 }))
    expect(res.status).toBe(422)
  })
})

// ── GET /contains ─────────────────────────────────────────────────────────────

describe('GET /contains', () => {
  test('returns 200 with areas array', async () => {
    mockFindContaining.mockImplementation(async () => [{ id: 'relation/1', name: 'Boone' }])
    const app = makeApp()
    const res = await app.handle(get('/contains?lat=36.2&lng=-81.6'))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(body[0].name).toBe('Boone')
  })

  test('passes lat/lng as numbers to findContainingAreas', async () => {
    let captured: any
    mockFindContaining.mockImplementation(async (p: any) => { captured = p; return [] })
    const app = makeApp()
    await app.handle(get('/contains?lat=36.2168&lng=-81.6746'))
    expect(typeof captured.lat).toBe('number')
    expect(typeof captured.lng).toBe('number')
    expect(captured.lat).toBeCloseTo(36.2168)
  })

  test('passes exclude param when provided', async () => {
    let captured: any
    mockFindContaining.mockImplementation(async (p: any) => { captured = p; return [] })
    const app = makeApp()
    await app.handle(get('/contains?lat=36.2&lng=-81.6&exclude=relation/178973'))
    expect(captured.exclude).toBe('relation/178973')
  })
})

// ── GET /children ─────────────────────────────────────────────────────────────

describe('GET /children', () => {
  test('returns 200 with children array', async () => {
    mockFindChildren.mockImplementation(async () => [{ id: 'node/1', name: 'Library' }])
    const app = makeApp()
    const res = await app.handle(get('/children?id=relation/17208432'))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(Array.isArray(body)).toBe(true)
  })

  test('passes all query params through to findChildren', async () => {
    let captured: any
    mockFindChildren.mockImplementation(async (p: any) => { captured = p; return [] })
    const app = makeApp()
    await app.handle(get('/children?id=relation/1&categories=amenity/cafe&limit=10&offset=5&lat=36.2&lng=-81.6'))
    expect(captured.id).toBe('relation/1')
    expect(captured.categories).toBe('amenity/cafe')
    expect(captured.limit).toBe('10')
    expect(captured.offset).toBe('5')
  })
})

// ── GET /place/:osmType/:osmId ────────────────────────────────────────────────

describe('GET /place/:osmType/:osmId', () => {
  const fullPlace = { id: 'node/123', name: 'Main Library', geom_type: 'point', full_geometry: null }

  test('returns 200 with place data when found', async () => {
    mockGetPlace.mockImplementation(async () => fullPlace)
    const app = makeApp()
    const res = await app.handle(get('/place/node/123'))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(body.id).toBe('node/123')
  })

  test('REGRESSION: returns HTTP 404 (not 500) when place not found', async () => {
    // Bug: `throw error(404, ...)` where error was undefined from context → 500
    // Fix: `set.status = 404; return { error: ... }` → 404
    mockGetPlace.mockImplementation(async () => null)
    const app = makeApp()
    const res = await app.handle(get('/place/node/999999999999'))
    expect(res.status).toBe(404)
    const body = await json(res)
    expect(body.error).toBeDefined()
  })

  test('passes osmType and osmId to getPlace service', async () => {
    let capturedType: string, capturedId: string
    mockGetPlace.mockImplementation(async (type: string, id: string) => {
      capturedType = type; capturedId = id
      return fullPlace
    })
    const app = makeApp()
    await app.handle(get('/place/way/456'))
    expect(capturedType!).toBe('way')
    expect(capturedId!).toBe('456')
  })

  test('works for node, way, and relation types', async () => {
    const app = makeApp()
    for (const osmType of ['node', 'way', 'relation']) {
      mockGetPlace.mockImplementationOnce(async () => ({ ...fullPlace, id: `${osmType}/1` }))
      const res = await app.handle(get(`/place/${osmType}/1`))
      expect(res.status).toBe(200)
    }
  })
})

// ── GET /geocode ──────────────────────────────────────────────────────────────

describe('GET /geocode', () => {
  test('returns 200 with address and hierarchy', async () => {
    mockReverseGeocode.mockImplementation(async () => ({
      address: { city: 'Boone', state: 'North Carolina' },
      hierarchy: [{ name: 'Boone', admin_level: 8 }],
    }))
    const app = makeApp()
    const res = await app.handle(get('/geocode?lat=36.2&lng=-81.6'))
    expect(res.status).toBe(200)
    const body = await json(res)
    expect(body.address.city).toBe('Boone')
    expect(Array.isArray(body.hierarchy)).toBe(true)
  })

  test('passes lat/lng as numbers to reverseGeocode', async () => {
    let capturedLat: number, capturedLng: number
    mockReverseGeocode.mockImplementation(async (lat: number, lng: number) => {
      capturedLat = lat; capturedLng = lng
      return { address: {}, hierarchy: [] }
    })
    const app = makeApp()
    await app.handle(get('/geocode?lat=36.2168&lng=-81.6746'))
    expect(typeof capturedLat!).toBe('number')
    expect(capturedLat!).toBeCloseTo(36.2168)
  })
})
