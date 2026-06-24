/**
 * Tests for the transit routing service (MOTIS client + adapter).
 *
 * Validates:
 *   - MOTIS OTPAPI response parsing and adaptation
 *   - Polyline decoding accuracy
 *   - Error handling for MOTIS failures
 *   - Edge cases: empty itineraries, missing fields, malformed data
 */

import { describe, test, expect, mock, beforeEach } from 'bun:test'

// Track db.execute calls so we return different stops for origin vs
// destination. Reset in beforeEach.
let dbCallIndex = 0

// Mock the database so getNearbyStops returns controlled stops without a
// real PostgreSQL connection. Each call alternates between an origin stop
// and a destination stop, producing exactly one stop pair per test.
mock.module('../db', () => ({
  db: {
    execute: async () => {
      dbCallIndex++
      const isOrigin = dbCallIndex % 2 === 1
      return [{
        stop_id: isOrigin ? 'mock_stop_origin' : 'mock_stop_dest',
        feed_id: 'mock_feed',
        stop_name: isOrigin ? 'Mock Origin Stop' : 'Mock Dest Stop',
        stop_code: null,
        stop_lat: isOrigin ? 35.23 : 35.77,
        stop_lon: isOrigin ? -80.84 : -78.64,
        location_type: 0,
        parent_station: null,
        wheelchair_boarding: 0,
        platform_code: null,
        distance: 50,
      }]
    },
  },
}))

import {
  getTransitRoute,
  extractFare,
  MotisError,
  type TransitRouteRequest,
} from './transit.service'

// ── Fixtures ────────────────────────────────────────────────────────

/** Minimal MOTIS OTPAPI response with one itinerary */
function makeMotisResponse(overrides: any = {}) {
  return {
    plan: {
      date: 1700000000000,
      from: { name: 'Origin', lat: 35.23, lon: -80.84 },
      to: { name: 'Destination', lat: 35.77, lon: -78.64 },
      itineraries: [
        {
          duration: 3600,
          startTime: 1700000000000,
          endTime: 1700003600000,
          walkTime: 300,
          transitTime: 3000,
          waitingTime: 300,
          walkDistance: 400,
          transfers: 0,
          legs: [
            {
              mode: 'WALK',
              from: { name: 'Origin', lat: 35.23, lon: -80.84, departure: 1700000000000 },
              to: { name: 'Stop A', lat: 35.24, lon: -80.83, arrival: 1700000300000, stopId: 'stop_a' },
              startTime: 1700000000000,
              endTime: 1700000300000,
              duration: 300,
              distance: 400,
              legGeometry: { points: '_p~iF~ps|U', length: 2 },
            },
            {
              mode: 'BUS',
              from: {
                name: 'Stop A', lat: 35.24, lon: -80.83, stopId: 'stop_a',
                departure: 1700000300000,
              },
              to: {
                name: 'Stop B', lat: 35.50, lon: -79.50, stopId: 'stop_b',
                arrival: 1700003300000,
              },
              startTime: 1700000300000,
              endTime: 1700003300000,
              duration: 3000,
              distance: 15000,
              route: '9',
              routeShortName: '9',
              routeLongName: 'Route 9 - Downtown',
              routeColor: 'FF0000',
              routeTextColor: 'FFFFFF',
              agencyName: 'CATS',
              agencyId: 'cats',
              tripId: 'trip_123',
              headsign: 'Downtown',
              routeId: 'route_9',
              legGeometry: { points: '_p~iF~ps|U_ulLnnqC', length: 3 },
              intermediateStops: [
                {
                  name: 'Stop C', lat: 35.35, lon: -80.10, stopId: 'stop_c',
                  arrival: 1700001500000, departure: 1700001530000,
                },
              ],
            },
            {
              mode: 'WALK',
              from: { name: 'Stop B', lat: 35.50, lon: -79.50, departure: 1700003300000, stopId: 'stop_b' },
              to: { name: 'Destination', lat: 35.77, lon: -78.64, arrival: 1700003600000 },
              startTime: 1700003300000,
              endTime: 1700003600000,
              duration: 300,
              distance: 350,
              legGeometry: { points: '_p~iF~ps|U', length: 2 },
            },
          ],
          ...overrides,
        },
      ],
    },
  }
}

function mockFetch(responseBody: any, status = 200): any {
  return mock(async () => ({
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(responseBody),
    json: async () => responseBody,
  }))
}

// ── Tests ───────────────────────────────────────────────────────────

describe('getTransitRoute', () => {
  beforeEach(() => {
    dbCallIndex = 0
  })

  const baseRequest: TransitRouteRequest = {
    from: { lat: 35.23, lng: -80.84 },
    to: { lat: 35.77, lng: -78.64 },
    time: '2023-11-15T08:00:00Z',
  }

  test('parses a standard MOTIS response into our format', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    const result = await getTransitRoute(baseRequest, fetchFn)

    expect(result.itineraries).toHaveLength(1)

    const itin = result.itineraries[0]
    expect(itin.duration).toBe(3600)
    expect(itin.walkTime).toBe(300)
    expect(itin.transitTime).toBe(3000)
    expect(itin.waitingTime).toBe(300)
    expect(itin.walkDistance).toBe(400)
    expect(itin.transfers).toBe(0)
    expect(itin.legs).toHaveLength(3)
  })

  test('adapts walking legs correctly', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    const result = await getTransitRoute(baseRequest, fetchFn)

    const walkLeg = result.itineraries[0].legs[0]
    expect(walkLeg.mode).toBe('WALK')
    expect(walkLeg.transitLeg).toBe(false)
    expect(walkLeg.from.name).toBe('Origin')
    expect(walkLeg.to.name).toBe('Stop A')
    expect(walkLeg.to.stopId).toBe('stop_a')
    expect(walkLeg.duration).toBe(300)
    expect(walkLeg.distance).toBe(400)
    expect(walkLeg.geometry).toBeDefined()
    expect(walkLeg.geometry!.type).toBe('LineString')

    // Walking legs should NOT have transit fields
    expect(walkLeg.routeShortName).toBeUndefined()
    expect(walkLeg.agencyName).toBeUndefined()
    expect(walkLeg.tripId).toBeUndefined()
  })

  test('adapts transit legs with full route metadata', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    const result = await getTransitRoute(baseRequest, fetchFn)

    const busLeg = result.itineraries[0].legs[1]
    expect(busLeg.mode).toBe('BUS')
    expect(busLeg.transitLeg).toBe(true)
    expect(busLeg.routeShortName).toBe('9')
    expect(busLeg.routeLongName).toBe('Route 9 - Downtown')
    expect(busLeg.routeColor).toBe('FF0000')
    expect(busLeg.routeTextColor).toBe('FFFFFF')
    expect(busLeg.agencyName).toBe('CATS')
    expect(busLeg.tripId).toBe('trip_123')
    expect(busLeg.headsign).toBe('Downtown')
    expect(busLeg.routeId).toBe('route_9')

    // Intermediate stops
    expect(busLeg.intermediateStops).toHaveLength(1)
    expect(busLeg.intermediateStops![0].name).toBe('Stop C')
    expect(busLeg.intermediateStops![0].stopId).toBe('stop_c')
  })

  test('converts epoch timestamps to ISO 8601', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    const result = await getTransitRoute(baseRequest, fetchFn)

    const itin = result.itineraries[0]
    // 1700000000000 = 2023-11-14T22:13:20.000Z
    expect(itin.startTime).toBe(new Date(1700000000000).toISOString())
    expect(itin.endTime).toBe(new Date(1700003600000).toISOString())

    const leg = itin.legs[0]
    expect(leg.startTime).toBe(new Date(1700000000000).toISOString())
    expect(leg.from.departure).toBe(new Date(1700000000000).toISOString())
  })

  test('decodes polyline geometry into GeoJSON coordinates', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    const result = await getTransitRoute(baseRequest, fetchFn)

    const walkLeg = result.itineraries[0].legs[0]
    expect(walkLeg.geometry).toBeDefined()
    expect(walkLeg.geometry!.type).toBe('LineString')
    expect(walkLeg.geometry!.coordinates.length).toBeGreaterThan(0)

    // Each coordinate should be [lng, lat]
    for (const coord of walkLeg.geometry!.coordinates) {
      expect(coord).toHaveLength(2)
      expect(typeof coord[0]).toBe('number')
      expect(typeof coord[1]).toBe('number')
    }
  })

  test('handles empty itineraries response', async () => {
    const fetchFn = mockFetch({ plan: { itineraries: [] } })
    const result = await getTransitRoute(baseRequest, fetchFn)

    expect(result.itineraries).toHaveLength(0)
  })

  test('handles response with no plan object', async () => {
    const fetchFn = mockFetch({ error: 'No trips found' })
    const result = await getTransitRoute(baseRequest, fetchFn)

    expect(result.itineraries).toHaveLength(0)
  })

  test('returns empty itineraries when all MOTIS queries fail', async () => {
    const fetchFn = mockFetch({ error: 'Internal error' }, 500)
    const result = await getTransitRoute(baseRequest, fetchFn)

    // Per-pair errors are caught — returns empty results instead of throwing
    expect(result.itineraries).toHaveLength(0)
  })

  test('gracefully handles MOTIS errors without throwing', async () => {
    const fetchFn = mockFetch({ error: 'Bad request' }, 400)
    const result = await getTransitRoute(baseRequest, fetchFn)

    // All pairs failed — returns empty, does not throw
    expect(result.itineraries).toHaveLength(0)
  })

  test('builds correct MOTIS URL with query parameters', async () => {
    const fetchFn = mockFetch(makeMotisResponse())

    const oldUrl = process.env.MOTIS_URL
    process.env.MOTIS_URL = 'http://test-motis:9090'

    try {
      await getTransitRoute({
        ...baseRequest,
        arriveBy: true,
        numItineraries: 3,
        maxTransfers: 2,
        wheelchair: true,
      }, fetchFn)

      // 1 origin stop × 1 dest stop = 1 pair = 1 MOTIS call
      expect(fetchFn).toHaveBeenCalledTimes(1)
      const calledUrl = (fetchFn as any).mock.calls[0][0] as string
      expect(calledUrl).toContain('http://test-motis:9090/api/v1/plan')
      // Stop IDs from mocked getNearbyStops
      expect(calledUrl).toContain('fromPlace=mock_feed_mock_stop_origin')
      expect(calledUrl).toContain('toPlace=mock_feed_mock_stop_dest')
      expect(calledUrl).toContain('arriveBy=true')
      expect(calledUrl).toContain('numItineraries=3')
      expect(calledUrl).toContain('maxTransfers=2')
      expect(calledUrl).toContain('wheelchair=true')
    } finally {
      if (oldUrl === undefined) delete process.env.MOTIS_URL
      else process.env.MOTIS_URL = oldUrl
    }
  })

  test('queries MOTIS with stop IDs, not raw coordinates', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    await getTransitRoute(baseRequest, fetchFn)

    const calledUrl = (fetchFn as any).mock.calls[0][0] as string
    // Uses stop IDs from getNearbyStops, not raw coordinates
    expect(calledUrl).toContain('fromPlace=mock_feed_mock_stop_origin')
    expect(calledUrl).toContain('toPlace=mock_feed_mock_stop_dest')
    // Should NOT pass raw lat/lng as fromPlace/toPlace
    expect(calledUrl).not.toContain('fromPlace=35.23')
  })

  test('handles legs with missing optional fields gracefully', async () => {
    const sparseResponse = {
      plan: {
        itineraries: [{
          duration: 1800,
          startTime: 1700000000000,
          endTime: 1700001800000,
          legs: [{
            mode: 'BUS',
            from: { lat: 35.23, lon: -80.84 },
            to: { lat: 35.50, lon: -79.50 },
            startTime: 1700000000000,
            endTime: 1700001800000,
            // No name, no route info, no geometry, no intermediate stops
          }],
        }],
      },
    }

    const fetchFn = mockFetch(sparseResponse)
    const result = await getTransitRoute(baseRequest, fetchFn)

    const leg = result.itineraries[0].legs[0]
    expect(leg.mode).toBe('BUS')
    expect(leg.from.name).toBe('')
    expect(leg.geometry).toBeUndefined()
    expect(leg.routeShortName).toBeUndefined()
    expect(leg.intermediateStops).toBeUndefined()
  })

  test('handles multi-transfer itinerary', async () => {
    const multiTransferResponse = makeMotisResponse({ transfers: 2 })
    // Add more legs
    multiTransferResponse.plan.itineraries[0].legs.splice(2, 0,
      {
        mode: 'WALK',
        from: { name: 'Stop B', lat: 35.50, lon: -79.50, departure: 1700003300000 },
        to: { name: 'Stop D', lat: 35.55, lon: -79.40, arrival: 1700003500000, stopId: 'stop_d' },
        startTime: 1700003300000,
        endTime: 1700003500000,
        duration: 200,
        distance: 250,
      },
      {
        mode: 'RAIL',
        from: { name: 'Stop D', lat: 35.55, lon: -79.40, stopId: 'stop_d', departure: 1700003600000 },
        to: { name: 'Stop E', lat: 35.70, lon: -78.80, stopId: 'stop_e', arrival: 1700005200000 },
        startTime: 1700003600000,
        endTime: 1700005200000,
        duration: 1600,
        distance: 30000,
        routeShortName: 'Blue',
        routeColor: '0000FF',
        agencyName: 'Amtrak',
        headsign: 'Raleigh',
      },
    )

    const fetchFn = mockFetch(multiTransferResponse)
    const result = await getTransitRoute(baseRequest, fetchFn)

    const legs = result.itineraries[0].legs
    expect(legs).toHaveLength(5)
    expect(legs[0].mode).toBe('WALK')
    expect(legs[1].mode).toBe('BUS')
    expect(legs[2].mode).toBe('WALK')
    expect(legs[3].mode).toBe('RAIL')
    expect(legs[4].mode).toBe('WALK')

    // RAIL leg should have its own route info
    expect(legs[3].routeShortName).toBe('Blue')
    expect(legs[3].routeColor).toBe('0000FF')
    expect(legs[3].agencyName).toBe('Amtrak')
  })

  test('uses current time when no time specified', async () => {
    const fetchFn = mockFetch(makeMotisResponse())
    const before = new Date()

    await getTransitRoute({
      from: { lat: 35.23, lng: -80.84 },
      to: { lat: 35.77, lng: -78.64 },
    }, fetchFn)

    const calledUrl = (fetchFn as any).mock.calls[0][0] as string
    // queryMotis uses `time=` with ISO 8601 datetime, not `date=`
    expect(calledUrl).toContain('time=')
    const todayStr = before.toISOString().split('T')[0]
    expect(calledUrl).toContain(todayStr)
  })
})

// ── extractFare (GTFS Fares v2 via MOTIS withFares) ─────────────────────────

describe('extractFare', () => {
  const product = (amount: number, extra: any = {}) => ({
    name: 'fare', amount, currency: 'USD', ...extra,
  })

  test('sums default-category products across fare legs', () => {
    const fare = extractFare({
      fareTransfers: [
        { effectiveFareLegProducts: [[[product(2.5)]]] },
        { effectiveFareLegProducts: [[[product(2.2)]]] },
      ],
    })
    expect(fare).toEqual({ currency: 'USD', amount: 4.7 })
  })

  test('prefers the default rider category over the first alternative', () => {
    const fare = extractFare({
      fareTransfers: [
        {
          effectiveFareLegProducts: [
            [
              [product(1.25, { riderCategory: { riderCategoryName: 'reduced' } })],
              [product(2.5, { riderCategory: { riderCategoryName: 'adult', isDefaultFareCategory: true } })],
            ],
          ],
        },
      ],
    })
    expect(fare).toEqual({ currency: 'USD', amount: 2.5 })
  })

  test('returns undefined when any fare leg is unpriced (partial data)', () => {
    // Tram leg priced, MTA leg without fare data — total is unknown.
    const fare = extractFare({
      fareTransfers: [
        { effectiveFareLegProducts: [[[product(2.5)]]] },
        { effectiveFareLegProducts: [[]] },
      ],
    })
    expect(fare).toBeUndefined()
  })

  test('free system reports an explicit zero fare', () => {
    const fare = extractFare({
      fareTransfers: [{ effectiveFareLegProducts: [[[product(0)]]] }],
    })
    expect(fare).toEqual({ currency: 'USD', amount: 0 })
  })

  test('returns undefined without fare data', () => {
    expect(extractFare({})).toBeUndefined()
    expect(extractFare({ fareTransfers: [] })).toBeUndefined()
  })
})
