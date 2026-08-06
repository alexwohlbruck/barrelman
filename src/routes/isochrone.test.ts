/**
 * HTTP-layer tests for /isochrone.
 *
 * Uses createIsochroneRoutes() with an injected service (or injected upstream
 * fetcher) so nothing here needs GraphHopper, MOTIS, or PostGIS. Covers:
 *   - GET/POST parameter parsing into the service request
 *   - GET /isochrone/modes
 *   - 400s for bad input, upstream status mapping for engine failures
 *   - Auth: BARRELMAN_API_KEY enforcement
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createIsochroneRoutes } from './isochrone'
import {
  GraphHopperError,
  MotisError,
  IsochroneRequestError,
  type FetchFn,
  type IsochroneRequest,
  type IsochroneResponse,
} from '../services/isochrone.service'

const BASE = 'http://localhost'

function req(path: string, init: RequestInit = {}) {
  return new Request(`${BASE}${path}`, init)
}

function post(path: string, body: any, init: RequestInit = {}) {
  return req(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(init.headers as any) },
    body: JSON.stringify(body),
    ...init,
  })
}

function makeResponse(overrides: Partial<IsochroneResponse> = {}): IsochroneResponse {
  return {
    mode: 'walk',
    origin: { lat: 35.7796, lng: -78.6382 },
    arriveBy: false,
    isochrones: {
      type: 'FeatureCollection',
      features: [{
        type: 'Feature',
        properties: { mode: 'walk', durationSeconds: 900, durationMinutes: 15, bucket: 0 },
        geometry: { type: 'Polygon', coordinates: [] },
      }],
    },
    meta: { durations: [900], computeMs: 1 },
    ...overrides,
  }
}

/** Service stub that records the request it was handed. */
function makeService(result: IsochroneResponse | (() => never) = makeResponse()) {
  const seen: IsochroneRequest[] = []
  const getIsochrone = mock(async (request: IsochroneRequest) => {
    seen.push(request)
    if (typeof result === 'function') return result()
    return result
  })
  return { getIsochrone, seen }
}

function appWith(deps: Parameters<typeof createIsochroneRoutes>[0]) {
  return new Elysia().use(createIsochroneRoutes(deps))
}

const savedApiKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})

afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

// ── GET /isochrone ───────────────────────────────────────────────────────────

describe('GET /isochrone', () => {
  test('returns a GeoJSON FeatureCollection', async () => {
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382'))

    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body.isochrones.type).toBe('FeatureCollection')
    expect(body.isochrones.features).toHaveLength(1)
  })

  test('parses mode, durations, and transit options', async () => {
    const { getIsochrone, seen } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any }).handle(req(
      '/isochrone?lat=35.7796&lng=-78.6382&mode=pt&durations=300,600,900' +
      '&arriveBy=true&time=2026-07-29T18:00:00Z&simplify=25&maxTransfers=2' +
      '&transitModes=BUS,RAIL&maxEgressTime=300&wheelchair=true&maxStopIsochrones=50',
    ))

    expect(res.status).toBe(200)
    expect(seen[0]).toEqual({
      lat: 35.7796,
      lng: -78.6382,
      mode: 'transit',
      durations: [300, 600, 900],
      arriveBy: true,
      time: '2026-07-29T18:00:00Z',
      simplify: 25,
      maxTransfers: 2,
      transitModes: ['BUS', 'RAIL'],
      maxEgressTime: 300,
      wheelchair: true,
      maxStopIsochrones: 50,
    })
  })

  test('defaults mode to walk and leaves durations to the service', async () => {
    const { getIsochrone, seen } = makeService()
    await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382'))

    expect(seen[0].mode).toBe('walk')
    expect(seen[0].durations).toBeUndefined()
    expect(seen[0].arriveBy).toBe(false)
  })

  test('400 on an unsupported mode', async () => {
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382&mode=hovercraft'))

    expect(res.status).toBe(400)
    expect((await res.json()).error).toContain('Unsupported mode')
    expect(getIsochrone).not.toHaveBeenCalled()
  })

  test('400 on a non-numeric duration', async () => {
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382&durations=300,soon'))

    expect(res.status).toBe(400)
    expect((await res.json()).error).toContain('Invalid duration')
  })

  test('400 when the service rejects the request', async () => {
    const getIsochrone = mock(async () => {
      throw new IsochroneRequestError('lat must be [-90,90], lng must be [-180,180]')
    })
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=999&lng=-78.6382'))

    expect(res.status).toBe(400)
    expect((await res.json()).error).toContain('lat must be')
  })

  test('422 when lat/lng are missing', async () => {
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any }).handle(req('/isochrone'))

    expect(res.status).toBe(422)
    expect(getIsochrone).not.toHaveBeenCalled()
  })
})

// ── POST /isochrone ──────────────────────────────────────────────────────────

describe('POST /isochrone', () => {
  test('accepts a JSON body', async () => {
    const { getIsochrone, seen } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any }).handle(post('/isochrone', {
      lat: 35.7796,
      lng: -78.6382,
      mode: 'bike',
      durations: [600, 1800],
      simplify: 10,
    }))

    expect(res.status).toBe(200)
    expect(seen[0].mode).toBe('bike')
    expect(seen[0].durations).toEqual([600, 1800])
    expect(seen[0].simplify).toBe(10)
  })

  test('400 on an unsupported mode', async () => {
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(post('/isochrone', { lat: 35.7796, lng: -78.6382, mode: 'rocket' }))

    expect(res.status).toBe(400)
    expect(getIsochrone).not.toHaveBeenCalled()
  })

  test('422 on an out-of-range coordinate', async () => {
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(post('/isochrone', { lat: 100, lng: -78.6382 }))

    expect(res.status).toBe(422)
    expect(getIsochrone).not.toHaveBeenCalled()
  })
})

// ── Upstream error mapping ───────────────────────────────────────────────────

describe('upstream errors', () => {
  test('a GraphHopper 4xx passes through with its body', async () => {
    const getIsochrone = mock(async () => {
      throw new GraphHopperError(400, '{"message":"Cannot find point"}')
    })
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382'))

    expect(res.status).toBe(400)
    expect((await res.json()).message).toBe('Cannot find point')
  })

  test('a GraphHopper 5xx becomes a 502', async () => {
    const getIsochrone = mock(async () => {
      throw new GraphHopperError(502, 'GraphHopper unreachable')
    })
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382'))

    expect(res.status).toBe(502)
    expect((await res.json()).error).toContain('GraphHopper unreachable')
  })

  test('a MOTIS failure becomes a 502', async () => {
    const getIsochrone = mock(async () => { throw new MotisError(500, 'timetable error') })
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382&mode=transit'))

    expect(res.status).toBe(502)
  })

  test('an unexpected error becomes a 502 with detail', async () => {
    const getIsochrone = mock(async () => { throw new Error('kaboom') })
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382'))

    expect(res.status).toBe(502)
    const body = await res.json()
    expect(body.error).toBe('Isochrone service unavailable')
    expect(body.detail).toContain('kaboom')
  })
})

// ── GET /isochrone/modes ─────────────────────────────────────────────────────

describe('GET /isochrone/modes', () => {
  test('lists every supported mode with its engine and ceiling', async () => {
    const res = await appWith({}).handle(req('/isochrone/modes'))

    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body.modes.map((m: any) => m.mode)).toEqual(['walk', 'bike', 'car', 'transit'])

    const transit = body.modes.find((m: any) => m.mode === 'transit')
    expect(transit.engine).toBe('motis+graphhopper')
    expect(transit.maxDurationSeconds).toBeGreaterThan(0)
    expect(body.maxContours).toBeGreaterThan(0)
  })
})

// ── Auth ─────────────────────────────────────────────────────────────────────

describe('auth', () => {
  test('401 without a bearer token when a key is configured', async () => {
    process.env.BARRELMAN_API_KEY = 'test-key'
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any })
      .handle(req('/isochrone?lat=35.7796&lng=-78.6382'))

    expect(res.status).toBe(401)
    expect(getIsochrone).not.toHaveBeenCalled()
  })

  test('200 with the right bearer token', async () => {
    process.env.BARRELMAN_API_KEY = 'test-key'
    const { getIsochrone } = makeService()
    const res = await appWith({ getIsochrone: getIsochrone as any }).handle(
      req('/isochrone?lat=35.7796&lng=-78.6382', {
        headers: { authorization: 'Bearer test-key' },
      }),
    )

    expect(res.status).toBe(200)
    expect(getIsochrone).toHaveBeenCalledTimes(1)
  })

  test('/isochrone/modes is behind auth too', async () => {
    process.env.BARRELMAN_API_KEY = 'test-key'
    const res = await appWith({}).handle(req('/isochrone/modes'))
    expect(res.status).toBe(401)
  })
})

// ── Wiring through to the upstreams ──────────────────────────────────────────

describe('service wiring', () => {
  test('passes the injected fetcher through to GraphHopper', async () => {
    const savedUrl = process.env.GRAPHHOPPER_URL
    process.env.GRAPHHOPPER_URL = 'http://mock-gh:8989'

    const fetchFn = mock<FetchFn>(async () => new Response(JSON.stringify({
      type: 'FeatureCollection',
      features: [{
        type: 'Feature',
        properties: { bucket: 0 },
        geometry: { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
      }],
    }), { status: 200, headers: { 'content-type': 'application/json' } }))

    try {
      const res = await appWith({ fetchFn })
        .handle(req('/isochrone?lat=35.7796&lng=-78.6382&mode=car&durations=1234'))

      expect(res.status).toBe(200)
      const url = new URL(fetchFn.mock.calls[0][0])
      expect(url.origin).toBe('http://mock-gh:8989')
      expect(url.searchParams.get('profile')).toBe('car')
      expect(url.searchParams.get('time_limit')).toBe('1234')

      const body = await res.json()
      expect(body.isochrones.features[0].geometry.type).toBe('Polygon')
      expect(body.mode).toBe('car')
    } finally {
      if (savedUrl === undefined) delete process.env.GRAPHHOPPER_URL
      else process.env.GRAPHHOPPER_URL = savedUrl
    }
  })
})
