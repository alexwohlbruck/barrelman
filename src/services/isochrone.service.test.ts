/**
 * Tests for the isochrone service.
 *
 * Both upstreams (GraphHopper, MOTIS) and the PostGIS union are injected, so
 * these run with no containers and no database. Covers:
 *   - Street modes: profile mapping, per-duration requests, bucket shortcut
 *   - Reverse isochrones (arriveBy → reverse_flow / MOTIS arriveBy)
 *   - Transit composition: one-to-all budget, per-stop egress sizing,
 *     grid dedupe, frontier circles, stop-isochrone cap
 *   - Validation: modes, durations, limits, coordinates
 *   - Upstream error propagation
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import {
  getIsochrone,
  parseMode,
  parseDurations,
  canUseBuckets,
  IsochroneRequestError,
  GraphHopperError,
  MotisError,
  MAX_STREET_DURATION,
  MAX_TRANSIT_DURATION,
  MAX_CONTOURS,
  type FetchFn,
  type UnionFn,
} from './isochrone.service'
import { isochroneCache } from '../lib/cache'

// ── Fixtures ────────────────────────────────────────────────────────

/** A square polygon centred on (lat, lon) — stands in for a real contour. */
function square(lat: number, lon: number, size = 0.01) {
  return {
    type: 'Polygon',
    coordinates: [[
      [lon - size, lat - size],
      [lon + size, lat - size],
      [lon + size, lat + size],
      [lon - size, lat + size],
      [lon - size, lat - size],
    ]],
  }
}

function ghResponse(buckets: number, lat = 35.7796, lon = -78.6382) {
  return new Response(JSON.stringify({
    type: 'FeatureCollection',
    features: Array.from({ length: buckets }, (_, i) => ({
      type: 'Feature',
      properties: { bucket: i },
      geometry: square(lat, lon, 0.005 * (i + 1)),
    })),
  }), { status: 200, headers: { 'content-type': 'application/json' } })
}

function motisResponse(stops: Array<{ lat: number; lon: number; minutes: number; stopId?: string }>) {
  return new Response(JSON.stringify({
    one: { lat: 35.7796, lon: -78.6382 },
    all: stops.map((s) => ({
      place: {
        name: s.stopId || 'Stop',
        stopId: s.stopId || 'feed_1',
        lat: s.lat,
        lon: s.lon,
        vertexType: 'TRANSIT',
      },
      duration: s.minutes,
      k: 1,
    })),
  }), { status: 200, headers: { 'content-type': 'application/json' } })
}

/** Union stub: records what it was handed, returns a marker geometry. */
function makeUnion() {
  const calls: Array<{ polygons: any[]; circles: any[]; simplify: number }> = []
  const union: UnionFn = async (polygons, circles, simplify) => {
    calls.push({ polygons, circles, simplify })
    return { type: 'Polygon', coordinates: [], unioned: polygons.length }
  }
  return { union, calls }
}

const savedGh = process.env.GRAPHHOPPER_URL
const savedMotis = process.env.MOTIS_URL

beforeEach(() => {
  process.env.GRAPHHOPPER_URL = 'http://mock-gh:8989'
  process.env.MOTIS_URL = 'http://mock-motis:8080'
  // The service caches polygons across requests; tests assert on call counts.
  isochroneCache.clear()
})

afterEach(() => {
  if (savedGh === undefined) delete process.env.GRAPHHOPPER_URL
  else process.env.GRAPHHOPPER_URL = savedGh
  if (savedMotis === undefined) delete process.env.MOTIS_URL
  else process.env.MOTIS_URL = savedMotis
})

// ── Parsing helpers ─────────────────────────────────────────────────

describe('parseMode', () => {
  test('defaults to walk', () => {
    expect(parseMode(undefined)).toBe('walk')
  })

  test('resolves aliases and is case-insensitive', () => {
    expect(parseMode('foot')).toBe('walk')
    expect(parseMode('Bicycle')).toBe('bike')
    expect(parseMode('DRIVING')).toBe('car')
    expect(parseMode(' pt ')).toBe('transit')
  })

  test('rejects unknown modes', () => {
    expect(() => parseMode('teleport')).toThrow(IsochroneRequestError)
  })
})

describe('parseDurations', () => {
  test('parses a comma-separated seconds list', () => {
    expect(parseDurations('300,600, 900')).toEqual([300, 600, 900])
  })

  test('returns undefined for empty input', () => {
    expect(parseDurations(undefined)).toBeUndefined()
    expect(parseDurations('  ')).toBeUndefined()
  })

  test('rejects non-numeric entries', () => {
    expect(() => parseDurations('300,soon')).toThrow(IsochroneRequestError)
  })
})

describe('canUseBuckets', () => {
  test('true for evenly spaced multiples from zero', () => {
    expect(canUseBuckets([300, 600, 900])).toBe(true)
  })

  test('false for a single duration or uneven spacing', () => {
    expect(canUseBuckets([600])).toBe(false)
    expect(canUseBuckets([300, 600, 1800])).toBe(false)
    expect(canUseBuckets([600, 900])).toBe(false)
  })
})

// ── Street modes ────────────────────────────────────────────────────

describe('street isochrones', () => {
  test('maps modes to GraphHopper profiles', async () => {
    for (const [mode, profile] of [['walk', 'foot'], ['bike', 'bike'], ['car', 'car']] as const) {
      const fetchFn = mock<FetchFn>(async () => ghResponse(1))
      const { union } = makeUnion()
      isochroneCache.clear()

      await getIsochrone(
        { lat: 35.7796, lng: -78.6382, mode, durations: [600] },
        { fetchFn, union },
      )

      const url = new URL(fetchFn.mock.calls[0][0])
      expect(url.pathname).toBe('/isochrone')
      expect(url.searchParams.get('profile')).toBe(profile)
      expect(url.searchParams.get('time_limit')).toBe('600')
      expect(url.searchParams.get('type')).toBe('geojson')
      expect(url.searchParams.get('reverse_flow')).toBeNull()
    }
  })

  test('evenly spaced durations use one bucketed request', async () => {
    const fetchFn = mock<FetchFn>(async () => ghResponse(3))
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'walk', durations: [300, 600, 900] },
      { fetchFn, union },
    )

    expect(fetchFn).toHaveBeenCalledTimes(1)
    const url = new URL(fetchFn.mock.calls[0][0])
    expect(url.searchParams.get('time_limit')).toBe('900')
    expect(url.searchParams.get('buckets')).toBe('3')

    expect(result.isochrones.features.map((f) => f.properties.durationSeconds))
      .toEqual([300, 600, 900])
    expect(result.isochrones.features.map((f) => f.properties.bucket)).toEqual([0, 1, 2])
    expect(result.isochrones.features[0].geometry).not.toBeNull()
  })

  test('unevenly spaced durations fall back to one request each', async () => {
    const fetchFn = mock<FetchFn>(async () => ghResponse(1))
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'bike', durations: [300, 1200, 1800] },
      { fetchFn, union },
    )

    expect(fetchFn).toHaveBeenCalledTimes(3)
    const limits = fetchFn.mock.calls
      .map((c) => new URL(c[0]).searchParams.get('time_limit'))
      .sort((a, b) => Number(a) - Number(b))
    expect(limits).toEqual(['300', '1200', '1800'])
    expect(result.isochrones.features).toHaveLength(3)
  })

  test('falls back to per-duration requests when the bucket count mismatches', async () => {
    // GraphHopper collapsed a contour: 3 buckets asked for, 2 returned.
    let call = 0
    const fetchFn = mock<FetchFn>(async () => ghResponse(++call === 1 ? 2 : 1))
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, durations: [300, 600, 900] },
      { fetchFn, union },
    )

    // 1 bucketed attempt + 3 individual retries
    expect(fetchFn).toHaveBeenCalledTimes(4)
    expect(result.isochrones.features).toHaveLength(3)
  })

  test('arriveBy sets reverse_flow', async () => {
    const fetchFn = mock<FetchFn>(async () => ghResponse(1))
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'car', durations: [600], arriveBy: true },
      { fetchFn, union },
    )

    expect(new URL(fetchFn.mock.calls[0][0]).searchParams.get('reverse_flow')).toBe('true')
    expect(result.arriveBy).toBe(true)
  })

  test('simplify routes each polygon through the union/simplify step', async () => {
    const fetchFn = mock<FetchFn>(async () => ghResponse(1))
    const { union, calls } = makeUnion()

    await getIsochrone(
      { lat: 35.7796, lng: -78.6382, durations: [600], simplify: 25 },
      { fetchFn, union },
    )

    expect(calls).toHaveLength(1)
    expect(calls[0].simplify).toBe(25)
    expect(calls[0].polygons).toHaveLength(1)
  })

  test('no simplify means no database round trip', async () => {
    const fetchFn = mock<FetchFn>(async () => ghResponse(1))
    const { union, calls } = makeUnion()

    await getIsochrone({ lat: 35.7796, lng: -78.6382, durations: [600] }, { fetchFn, union })

    expect(calls).toHaveLength(0)
  })

  test('street modes never touch MOTIS', async () => {
    const fetchFn = mock<FetchFn>(async () => ghResponse(1))
    const { union } = makeUnion()

    await getIsochrone({ lat: 35.7796, lng: -78.6382, durations: [600] }, { fetchFn, union })

    expect(fetchFn.mock.calls.every((c) => c[0].includes('mock-gh'))).toBe(true)
  })
})

// ── Validation ──────────────────────────────────────────────────────

describe('validation', () => {
  const fetchFn = mock<FetchFn>(async () => ghResponse(1))

  test('rejects out-of-range coordinates', async () => {
    await expect(getIsochrone({ lat: 91, lng: 0 }, { fetchFn })).rejects.toThrow(IsochroneRequestError)
    await expect(getIsochrone({ lat: 0, lng: 181 }, { fetchFn })).rejects.toThrow(IsochroneRequestError)
    await expect(getIsochrone({ lat: NaN, lng: 0 }, { fetchFn })).rejects.toThrow(IsochroneRequestError)
  })

  test('rejects non-positive durations', async () => {
    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, durations: [0] }, { fetchFn }),
    ).rejects.toThrow(IsochroneRequestError)
    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, durations: [-60] }, { fetchFn }),
    ).rejects.toThrow(IsochroneRequestError)
  })

  test('enforces per-mode duration ceilings', async () => {
    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, durations: [MAX_STREET_DURATION + 1] }, { fetchFn }),
    ).rejects.toThrow(/exceeds/)
    await expect(
      getIsochrone(
        { lat: 35.7, lng: -78.6, mode: 'transit', durations: [MAX_TRANSIT_DURATION + 1] },
        { fetchFn },
      ),
    ).rejects.toThrow(/exceeds/)
  })

  test('caps the number of contours', async () => {
    const tooMany = Array.from({ length: MAX_CONTOURS + 1 }, (_, i) => (i + 1) * 60)
    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, durations: tooMany }, { fetchFn }),
    ).rejects.toThrow(/At most/)
  })

  test('dedupes and sorts durations', async () => {
    const gh = mock<FetchFn>(async () => ghResponse(1))
    const { union } = makeUnion()
    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, durations: [900, 300, 900] },
      { fetchFn: gh, union },
    )
    expect(result.meta.durations).toEqual([300, 900])
  })

  test('rejects a malformed transit time', async () => {
    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, mode: 'transit', time: 'not-a-date' }, { fetchFn }),
    ).rejects.toThrow(/ISO 8601/)
  })

  test('rejects a negative simplify tolerance', async () => {
    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, simplify: -1 }, { fetchFn }),
    ).rejects.toThrow(IsochroneRequestError)
  })
})

// ── Upstream failures ───────────────────────────────────────────────

describe('upstream errors', () => {
  test('propagates GraphHopper error status and body', async () => {
    const fetchFn = mock<FetchFn>(async () =>
      new Response('{"message":"Cannot find point"}', { status: 400 }),
    )

    const promise = getIsochrone({ lat: 35.7, lng: -78.6, durations: [600] }, { fetchFn })
    await expect(promise).rejects.toThrow(GraphHopperError)
    await promise.catch((err: GraphHopperError) => {
      expect(err.statusCode).toBe(400)
      expect(err.body).toContain('Cannot find point')
    })
  })

  test('a thrown fetch becomes a 502 GraphHopperError', async () => {
    const fetchFn = mock<FetchFn>(async () => { throw new Error('ECONNREFUSED') })

    const promise = getIsochrone({ lat: 35.7, lng: -78.6, durations: [600] }, { fetchFn })
    await expect(promise).rejects.toThrow(GraphHopperError)
    await promise.catch((err: GraphHopperError) => expect(err.statusCode).toBe(502))
  })

  test('propagates MOTIS failures for transit', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return new Response('boom', { status: 500 })
      return ghResponse(1)
    })

    await expect(
      getIsochrone({ lat: 35.7, lng: -78.6, mode: 'transit', durations: [900] }, { fetchFn }),
    ).rejects.toThrow(MotisError)
  })

  test('a failed per-stop walk isochrone does not sink the request', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        return motisResponse([{ lat: 35.79, lon: -78.65, minutes: 5 }])
      }
      // Origin walk succeeds, the stop walk blows up.
      if (url.includes('35.79')) throw new Error('graph error')
      return ghResponse(1)
    })
    const { union, calls } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900] },
      { fetchFn, union },
    )

    expect(result.isochrones.features).toHaveLength(1)
    // Only the origin walk survived into the union.
    expect(calls[0].polygons).toHaveLength(1)
  })
})

// ── Transit composition ─────────────────────────────────────────────

describe('transit isochrones', () => {
  test('queries MOTIS with the largest budget in minutes', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse([])
      return ghResponse(1)
    })
    const { union } = makeUnion()

    await getIsochrone(
      {
        lat: 35.7796,
        lng: -78.6382,
        mode: 'transit',
        durations: [900, 1800],
        time: '2026-07-29T18:00:00.000Z',
        maxTransfers: 2,
        transitModes: ['BUS', 'RAIL'],
      },
      { fetchFn, union },
    )

    const motisCall = fetchFn.mock.calls.find((c) => c[0].includes('one-to-all'))!
    const url = new URL(motisCall[0])
    expect(url.searchParams.get('one')).toBe('35.7796,-78.6382')
    expect(url.searchParams.get('maxTravelTime')).toBe('30')
    expect(url.searchParams.get('time')).toBe('2026-07-29T18:00:00.000Z')
    expect(url.searchParams.get('maxTransfers')).toBe('2')
    expect(url.searchParams.get('transitModes')).toBe('BUS,RAIL')
    expect(url.searchParams.get('maxMatchingDistance')).toBe('250')
    expect(url.searchParams.get('arriveBy')).toBe('false')
  })

  test('sizes each stop walk to the leftover budget, capped by maxEgressTime', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        return motisResponse([
          { lat: 35.80, lon: -78.60, minutes: 5 },   // 600s left, capped to 300
          { lat: 35.81, lon: -78.61, minutes: 12 },  // 180s left
        ])
      }
      return ghResponse(1)
    })
    const { union } = makeUnion()

    await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900], maxEgressTime: 300 },
      { fetchFn, union },
    )

    const stopLimits = fetchFn.mock.calls
      .filter((c) => c[0].includes('/isochrone') && !c[0].includes('35.7796'))
      .map((c) => new URL(c[0]).searchParams.get('time_limit'))
      .sort()
    expect(stopLimits).toEqual(['180', '300'])
  })

  test('the direct walk from the origin is included in every contour', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse([])
      return ghResponse(1)
    })
    const { union, calls } = makeUnion()

    await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [600, 1200] },
      { fetchFn, union },
    )

    // Two contour unions, plus the nesting fold of contour 0 into contour 1.
    expect(calls).toHaveLength(3)
    expect(calls[0].polygons).toHaveLength(1)
    expect(calls[1].polygons).toHaveLength(1)

    const originLimits = fetchFn.mock.calls
      .filter((c) => c[0].includes('/isochrone'))
      .map((c) => new URL(c[0]).searchParams.get('time_limit'))
      .sort((a, b) => Number(a) - Number(b))
    expect(originLimits).toEqual(['600', '1200'])
  })

  test('folds each finished contour into the next so they nest', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse([])
      return ghResponse(1)
    })
    const { union, calls } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [600, 1200], simplify: 20 },
      { fetchFn, union },
    )

    // contour 0 → fold(contour 0 into contour 1) happens after contour 1's own
    // union, and the fold must not re-simplify (that's what moves the edge).
    expect(calls).toHaveLength(3)
    const fold = calls[2]
    expect(fold.simplify).toBe(0)
    expect(fold.polygons).toHaveLength(2)
    expect(fold.polygons).toContain(result.isochrones.features[0].geometry)
  })

  test('caps direct walking at an hour on long budgets', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse([])
      return ghResponse(1)
    })
    const { union } = makeUnion()

    await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [7200] },
      { fetchFn, union },
    )

    const walk = fetchFn.mock.calls.find((c) => c[0].includes('/isochrone'))!
    expect(new URL(walk[0]).searchParams.get('time_limit')).toBe('3600')
  })

  test('excludes stops outside the contour budget', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        return motisResponse([
          { lat: 35.80, lon: -78.60, minutes: 5 },
          { lat: 35.90, lon: -78.50, minutes: 25 },
        ])
      }
      return ghResponse(1)
    })
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [600, 1800] },
      { fetchFn, union },
    )

    expect(result.isochrones.features[0].properties.stops).toBe(1)
    expect(result.isochrones.features[1].properties.stops).toBe(2)
    expect(result.meta.reachableStops).toBe(2)
  })

  test('collapses clustered stops onto one grid cell', async () => {
    // Three platform nodes of the same station, all within ~20m.
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        return motisResponse([
          { lat: 35.8000, lon: -78.6000, minutes: 5 },
          { lat: 35.80005, lon: -78.60005, minutes: 6 },
          { lat: 35.80008, lon: -78.60002, minutes: 7 },
        ])
      }
      return ghResponse(1)
    })
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900] },
      { fetchFn, union },
    )

    // One walk for the cluster (keeping the 5-minute arrival, the largest
    // leftover budget) plus the origin's direct walk.
    expect(result.meta.stopIsochrones).toBe(1)
    const stopWalk = fetchFn.mock.calls.find(
      (c) => c[0].includes('/isochrone') && c[0].includes('35.8'),
    )!
    expect(new URL(stopWalk[0]).searchParams.get('time_limit')).toBe('600')
  })

  test('thins the stop set by coarsening the grid, not by dropping the tail', async () => {
    // Stops marching outward: naive truncation would always sacrifice the far
    // ones (least walking time left), collapsing the contour's outer edge.
    const stops = Array.from({ length: 40 }, (_, i) => ({
      lat: 35.80 + i * 0.002,
      lon: -78.60 - i * 0.002,
      minutes: 5,
    }))
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse(stops)
      return ghResponse(1)
    })
    const { union } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900], maxStopIsochrones: 10 },
      { fetchFn, union },
    )

    expect(result.meta.stopIsochrones).toBeLessThanOrEqual(10)
    expect(result.meta.truncated).toBe(false)
    // Grid coarsened past its ~55m starting resolution to get there.
    expect(result.meta.stopGridMeters).toBeGreaterThan(55)

    // The outermost stop is still represented — coarsening thinned evenly
    // rather than lopping off the frontier.
    const walkPoints = fetchFn.mock.calls
      .filter((c) => c[0].includes('/isochrone'))
      .map((c) => new URL(c[0]).searchParams.get('point')!)
    const farthest = Math.max(...walkPoints.map((p) => Number(p.split(',')[0])))
    expect(farthest).toBeGreaterThan(35.86)
  })

  test('stops past the cap still become frontier circles', async () => {
    const stops = Array.from({ length: 5 }, (_, i) => ({
      lat: 35.80 + i * 0.01,
      lon: -78.60 - i * 0.01,
      minutes: 5 + i,
    }))
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse(stops)
      return ghResponse(1)
    })
    const { union, calls } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900], maxStopIsochrones: 2 },
      { fetchFn, union },
    )

    // The grid can't coarsen past its ceiling, so the cap still bites here.
    expect(result.meta.stopIsochrones).toBe(2)
    expect(result.meta.truncated).toBe(true)
    // origin walk + 2 stop walks
    expect(calls[0].polygons).toHaveLength(3)
    // Whatever missed the cap still marks ground as reachable.
    expect(calls[0].circles.length).toBeGreaterThan(0)
    expect(calls[0].circles[0].radiusM).toBeGreaterThan(0)
  })

  test('stops with under a minute of slack get a circle, not a walk', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        // Arrives at exactly the budget — nowhere left to walk.
        return motisResponse([{ lat: 35.85, lon: -78.55, minutes: 15 }])
      }
      return ghResponse(1)
    })
    const { union, calls } = makeUnion()

    const result = await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900] },
      { fetchFn, union },
    )

    expect(result.meta.stopIsochrones).toBe(0)
    expect(calls[0].circles).toHaveLength(1)
  })

  test('arriveBy inverts both upstreams', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        return motisResponse([{ lat: 35.80, lon: -78.60, minutes: 5 }])
      }
      return ghResponse(1)
    })
    const { union } = makeUnion()

    await getIsochrone(
      { lat: 35.7796, lng: -78.6382, mode: 'transit', durations: [900], arriveBy: true },
      { fetchFn, union },
    )

    const motisCall = fetchFn.mock.calls.find((c) => c[0].includes('one-to-all'))!
    expect(new URL(motisCall[0]).searchParams.get('arriveBy')).toBe('true')

    const walkCalls = fetchFn.mock.calls.filter((c) => c[0].includes('/isochrone'))
    expect(walkCalls.length).toBeGreaterThan(0)
    for (const call of walkCalls) {
      expect(new URL(call[0]).searchParams.get('reverse_flow')).toBe('true')
    }
  })

  test('reuses cached polygons across repeated stop/duration pairs', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) {
        return motisResponse([{ lat: 35.80, lon: -78.60, minutes: 5 }])
      }
      return ghResponse(1)
    })
    const { union } = makeUnion()
    const request = {
      lat: 35.7796, lng: -78.6382, mode: 'transit' as const, durations: [900],
    }

    await getIsochrone(request, { fetchFn, union })
    const afterFirst = fetchFn.mock.calls.length

    await getIsochrone(request, { fetchFn, union })
    const afterSecond = fetchFn.mock.calls.length

    // Second pass re-queries MOTIS (departure times move) but reuses both
    // GraphHopper polygons.
    expect(afterSecond - afterFirst).toBe(1)
  })

  test('reports the resolved query time and mode metadata', async () => {
    const fetchFn = mock<FetchFn>(async (url) => {
      if (url.includes('one-to-all')) return motisResponse([])
      return ghResponse(1)
    })
    const { union } = makeUnion()

    const result = await getIsochrone(
      {
        lat: 35.7796, lng: -78.6382, mode: 'transit',
        durations: [900], time: '2026-07-29T18:00:00.000Z',
      },
      { fetchFn, union },
    )

    expect(result.mode).toBe('transit')
    expect(result.time).toBe('2026-07-29T18:00:00.000Z')
    expect(result.origin).toEqual({ lat: 35.7796, lng: -78.6382 })
    expect(result.meta.computeMs).toBeGreaterThanOrEqual(0)
    expect(result.isochrones.type).toBe('FeatureCollection')
    expect(result.isochrones.features[0].properties.durationMinutes).toBe(15)
  })
})
