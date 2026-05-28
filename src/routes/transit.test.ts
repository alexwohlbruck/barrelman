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
