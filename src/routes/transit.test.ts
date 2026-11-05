/**
 * HTTP-layer tests for /transit/* routes.
 *
 * Uses createTransitRoutes() with mocked service deps.
 * Covers request validation, error handling, and response format.
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createTransitRoutes } from './transit'
import type {
  TransitRouteResponse,
  NearbyStop,
  StopRoutesResult,
} from '../services/transit.service'

// ── Helpers ─────────────────────────────────────────────────────────

const BASE = 'http://localhost'

function post(path: string, body: any, headers: Record<string, string> = {}) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  })
}

function get(path: string) {
  return new Request(`${BASE}${path}`)
}

// ── Mocks ───────────────────────────────────────────────────────────

const mockTransitResponse: TransitRouteResponse = {
  itineraries: [
    {
      duration: 1800,
      startTime: '2023-11-15T08:00:00.000Z',
      endTime: '2023-11-15T08:30:00.000Z',
      walkTime: 300,
      transitTime: 1200,
      waitingTime: 300,
      walkDistance: 400,
      transfers: 0,
      legs: [
        {
          mode: 'WALK',
          from: { name: 'Origin', lat: 35.23, lng: -80.84 },
          to: { name: 'Stop A', lat: 35.24, lng: -80.83, stopId: 'stop_a' },
          startTime: '2023-11-15T08:00:00.000Z',
          endTime: '2023-11-15T08:05:00.000Z',
          duration: 300,
          distance: 400,
          transitLeg: false,
        },
        {
          mode: 'BUS',
          from: { name: 'Stop A', lat: 35.24, lng: -80.83, stopId: 'stop_a' },
          to: { name: 'Stop B', lat: 35.50, lng: -79.50, stopId: 'stop_b' },
          startTime: '2023-11-15T08:05:00.000Z',
          endTime: '2023-11-15T08:25:00.000Z',
          duration: 1200,
          distance: 15000,
          transitLeg: true,
          routeShortName: '9',
          routeColor: 'FF0000',
          agencyName: 'CATS',
        },
      ],
    },
  ],
  metadata: { searchWindow: 120 },
}

const mockNearbyStops: NearbyStop[] = [
  {
    stopId: 'S001', feedId: 'feed_1', stopName: 'Central Station', stopCode: null,
    lat: 35.227, lng: -80.843, distance: 150, locationType: 0,
    parentStation: null, wheelchairBoarding: 1, platformCode: null,
  },
  {
    stopId: 'S002', feedId: 'feed_1', stopName: 'Park Ave', stopCode: '1234',
    lat: 35.235, lng: -80.850, distance: 500, locationType: 0,
    parentStation: null, wheelchairBoarding: 0, platformCode: null,
  },
]

const mockStopRoutes: StopRoutesResult[] = [
  {
    routeId: 'R001', feedId: 'feed_1', routeShortName: '9',
    routeLongName: 'Route 9 - Downtown', routeType: 3,
    routeColor: 'FF0000', routeTextColor: 'FFFFFF', agencyName: 'CATS',
    via: 'station',
  },
]

const savedKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})

afterEach(() => {
  if (savedKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedKey
})

// ── POST /transit/route ─────────────────────────────────────────────

describe('POST /transit/route', () => {
  test('returns transit itineraries for valid request', async () => {
    const getTransitRoute = mock(async () => mockTransitResponse)
    const app = new Elysia().use(createTransitRoutes({ getTransitRoute }))

    const res = await app.handle(post('/transit/route', {
      from: { lat: 35.23, lng: -80.84 },
      to: { lat: 35.50, lng: -79.50 },
    }))

    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body.itineraries).toHaveLength(1)
    expect(body.itineraries[0].legs).toHaveLength(2)
    expect(getTransitRoute).toHaveBeenCalledTimes(1)
  })

  test('passes all optional parameters to service', async () => {
    const getTransitRoute = mock(async () => mockTransitResponse)
    const app = new Elysia().use(createTransitRoutes({ getTransitRoute }))

    await app.handle(post('/transit/route', {
      from: { lat: 35.23, lng: -80.84 },
      to: { lat: 35.50, lng: -79.50 },
      time: '2023-11-15T08:00:00Z',
      arriveBy: true,
      numItineraries: 3,
      transitModes: ['BUS', 'RAIL'],
      maxTransfers: 2,
      wheelchair: true,
    }))

    const call = (getTransitRoute as any).mock.calls[0]
    const request = call[0]
    expect(request.time).toBe('2023-11-15T08:00:00Z')
    expect(request.arriveBy).toBe(true)
    expect(request.numItineraries).toBe(3)
    expect(request.transitModes).toEqual(['BUS', 'RAIL'])
    expect(request.maxTransfers).toBe(2)
    expect(request.wheelchair).toBe(true)
  })

  test('returns 502 when MOTIS is unavailable', async () => {
    const getTransitRoute = mock(async () => {
      throw new Error('Connection refused')
    })
    const app = new Elysia().use(createTransitRoutes({ getTransitRoute }))

    const res = await app.handle(post('/transit/route', {
      from: { lat: 35.23, lng: -80.84 },
      to: { lat: 35.50, lng: -79.50 },
    }))

    expect(res.status).toBe(502)
    const body = await res.json()
    expect(body.error).toBe('Transit routing service unavailable')
  })

  test('validates coordinate ranges', async () => {
    const getTransitRoute = mock(async () => mockTransitResponse)
    const app = new Elysia().use(createTransitRoutes({ getTransitRoute }))

    // Latitude out of range
    const res = await app.handle(post('/transit/route', {
      from: { lat: 95, lng: -80.84 },
      to: { lat: 35.50, lng: -79.50 },
    }))

    expect(res.status).toBe(422)
  })

  test('rejects missing from/to', async () => {
    const getTransitRoute = mock(async () => mockTransitResponse)
    const app = new Elysia().use(createTransitRoutes({ getTransitRoute }))

    const res = await app.handle(post('/transit/route', {
      from: { lat: 35.23, lng: -80.84 },
      // missing "to"
    }))

    expect(res.status).toBe(422)
  })

  test('validates numItineraries bounds', async () => {
    const getTransitRoute = mock(async () => mockTransitResponse)
    const app = new Elysia().use(createTransitRoutes({ getTransitRoute }))

    // numItineraries > 10
    const res = await app.handle(post('/transit/route', {
      from: { lat: 35.23, lng: -80.84 },
      to: { lat: 35.50, lng: -79.50 },
      numItineraries: 100,
    }))

    expect(res.status).toBe(422)
  })
})

// ── Empty plans ─────────────────────────────────────────────────────

describe('an empty plan', () => {
  const LIVE_KEY = 'brm_live_abcdefghijklmnopqrstuvwxyz0123456789ABCD'
  const empty: TransitRouteResponse = {
    itineraries: [],
    warnings: ['No itinerary was found for this trip. Empty plans are not charged.'],
  }
  const auth = () => ({
    resolveApiKey: mock(async () => ({
      keyId: 'key-transit', userId: 'user-transit', scopes: ['*'], allowedOrigins: [],
      plan: 'developer', suspended: false, suspensionReason: null,
    })),
    checkQuota: mock(async () => ({ allowed: true as const, overage: false })),
    recordUsage: mock(() => undefined),
  }) as never

  const trip = {
    from: { lat: 35.23, lng: -80.84 },
    to: { lat: 35.50, lng: -79.50 },
  }

  beforeEach(() => {
    process.env.BARRELMAN_API_KEY = 'service-secret'
  })

  test.each(['/transit/route', '/transit/intermodal-route'])('%s is not charged', async (path) => {
    const app = new Elysia().use(createTransitRoutes({
      getTransitRoute: mock(async () => empty),
      getIntermodalRoute: mock(async () => empty),
      auth: auth(),
    }))

    const res = await app.handle(post(path, trip, { authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.status).toBe(200)
    expect(res.headers.get('x-barrelman-credits-charged')).toBe('0')
    expect((await res.json()).warnings).toEqual(empty.warnings)
  })

  test('a plan with itineraries keeps its charge', async () => {
    const app = new Elysia().use(createTransitRoutes({
      getTransitRoute: mock(async () => mockTransitResponse),
      auth: auth(),
    }))

    const res = await app.handle(post('/transit/route', trip, { authorization: `Bearer ${LIVE_KEY}` }))

    expect(res.headers.get('x-barrelman-credits-charged')).toBe('20')
  })
})

// ── GET /transit/stops ──────────────────────────────────────────────

describe('GET /transit/stops', () => {
  test('returns nearby stops for valid coordinates', async () => {
    const getNearbyStops = mock(async () => mockNearbyStops)
    const app = new Elysia().use(createTransitRoutes({ getNearbyStops }))

    const res = await app.handle(get('/transit/stops?lat=35.23&lng=-80.84'))

    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body).toHaveLength(2)
    expect(body[0].stopName).toBe('Central Station')
    expect(body[0].distance).toBe(150)
    expect(getNearbyStops).toHaveBeenCalledTimes(1)
  })

  test('passes radius and limit parameters', async () => {
    const getNearbyStops = mock(async () => [])
    const app = new Elysia().use(createTransitRoutes({ getNearbyStops }))

    await app.handle(get('/transit/stops?lat=35.23&lng=-80.84&radius=2000&limit=5'))

    const call = (getNearbyStops as any).mock.calls[0][0]
    expect(call.radius).toBe(2000)
    expect(call.limit).toBe(5)
  })

  test('returns 400 for invalid coordinates', async () => {
    const getNearbyStops = mock(async () => [])
    const app = new Elysia().use(createTransitRoutes({ getNearbyStops }))

    const res = await app.handle(get('/transit/stops?lat=abc&lng=-80.84'))

    expect(res.status).toBe(400)
    const body = await res.json()
    expect(body.error).toContain('valid numbers')
  })

  test('returns 400 for out-of-range coordinates', async () => {
    const getNearbyStops = mock(async () => [])
    const app = new Elysia().use(createTransitRoutes({ getNearbyStops }))

    const res = await app.handle(get('/transit/stops?lat=100&lng=-80.84'))

    expect(res.status).toBe(400)
    expect((await res.json()).error).toContain('[-90,90]')
  })

  test('uses default radius and limit', async () => {
    const getNearbyStops = mock(async () => [])
    const app = new Elysia().use(createTransitRoutes({ getNearbyStops }))

    await app.handle(get('/transit/stops?lat=35.23&lng=-80.84'))

    const call = (getNearbyStops as any).mock.calls[0][0]
    expect(call.radius).toBe(1000)
    expect(call.limit).toBe(20)
  })
})

// ── GET /transit/routes ─────────────────────────────────────────────

describe('GET /transit/routes', () => {
  test('returns routes for a valid stop', async () => {
    const getRoutesForStop = mock(async () => mockStopRoutes)
    const app = new Elysia().use(createTransitRoutes({ getRoutesForStop }))

    const res = await app.handle(get('/transit/routes?feedId=feed_1&stopId=S001'))

    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body).toHaveLength(1)
    expect(body[0].routeShortName).toBe('9')
    expect(body[0].routeColor).toBe('FF0000')
  })

  test('returns 400 when feedId or stopId missing', async () => {
    const getRoutesForStop = mock(async () => [])
    const app = new Elysia().use(createTransitRoutes({ getRoutesForStop }))

    const res = await app.handle(get('/transit/routes?feedId=feed_1'))

    // Elysia should return 422 for missing required query param
    expect(res.status).toBe(422)
  })
})

// ── GET /transit/resolve-route ──────────────────────────────────────

describe('GET /transit/resolve-route', () => {
  const resolved = {
    feedId: 'mta-subway',
    routeId: '2',
    routeShortName: '2',
    routeLongName: 'Seventh Avenue Express',
    routeType: 1,
    distance: 84.2,
  }

  test('returns the resolved feed/route pair', async () => {
    const resolveRoute = mock(async () => resolved)
    const app = new Elysia().use(createTransitRoutes({ resolveRoute }))

    const res = await app.handle(
      get('/transit/resolve-route?routeId=2&lat=40.71&lng=-74.00&mode=metro'),
    )

    expect(res.status).toBe(200)
    expect(await res.json()).toMatchObject({ feedId: 'mta-subway', routeId: '2' })
    expect((resolveRoute as any).mock.calls[0][0]).toMatchObject({
      routeId: '2',
      lat: 40.71,
      lng: -74,
      mode: 'metro',
    })
  })

  test('drops a mode that is not a known class rather than filtering by it', async () => {
    const resolveRoute = mock(async () => resolved)
    const app = new Elysia().use(createTransitRoutes({ resolveRoute }))

    await app.handle(get('/transit/resolve-route?routeId=2&lat=40.71&lng=-74.00&mode=subway'))

    expect((resolveRoute as any).mock.calls[0][0].mode).toBeNull()
  })

  test('returns 404 when nothing claims the id here', async () => {
    const resolveRoute = mock(async () => null)
    const app = new Elysia().use(createTransitRoutes({ resolveRoute }))

    const res = await app.handle(get('/transit/resolve-route?routeId=zz&lat=40.71&lng=-74.00'))

    expect(res.status).toBe(404)
  })

  test('rejects out-of-range coordinates', async () => {
    const resolveRoute = mock(async () => resolved)
    const app = new Elysia().use(createTransitRoutes({ resolveRoute }))

    const res = await app.handle(get('/transit/resolve-route?routeId=2&lat=99&lng=-74.00'))

    expect(res.status).toBe(400)
    expect((resolveRoute as any).mock.calls).toHaveLength(0)
  })

  test('rejects a non-numeric coordinate', async () => {
    const resolveRoute = mock(async () => resolved)
    const app = new Elysia().use(createTransitRoutes({ resolveRoute }))

    const res = await app.handle(get('/transit/resolve-route?routeId=2&lat=abc&lng=-74.00'))

    expect(res.status).toBe(400)
  })
})

// ── GET /transit/stations/:id/paths ─────────────────────────────────

describe('GET /transit/stations/:id/paths', () => {
  const body = { complex: { id: 'node/1' }, platforms: [], paths: [] }

  test('takes an encoded OSM id or its short form, and the filters', async () => {
    const getStationPaths = mock(async () => body as never)
    const app = new Elysia().use(createTransitRoutes({ getStationPaths }))

    expect((await app.handle(get('/transit/stations/node%2F1/paths'))).status).toBe(200)
    expect(getStationPaths.mock.calls[0]).toEqual(['node/1', { from: undefined, to: undefined, profile: undefined }] as never)

    await app.handle(get('/transit/stations/w42/paths?from=w7&to=n9&profile=wheelchair'))
    expect(getStationPaths.mock.calls[1]).toEqual(['way/42', { from: 'way/7', to: 'node/9', profile: 'wheelchair' }] as never)
  })

  test('404s an unknown station', async () => {
    const app = new Elysia().use(createTransitRoutes({ getStationPaths: mock(async () => null) }))
    expect((await app.handle(get('/transit/stations/n1/paths'))).status).toBe(404)
  })

  test('rejects an unknown profile', async () => {
    const app = new Elysia().use(createTransitRoutes({ getStationPaths: mock(async () => body as never) }))
    expect((await app.handle(get('/transit/stations/n1/paths?profile=bike'))).status).toBe(422)
  })
})
