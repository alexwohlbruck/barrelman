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

/** Rows for a test that drives one specific query; null keeps the stop-pair
 *  behaviour every routing test relies on. A function is called with the SQL
 *  text, for a test whose service issues more than one query. Reset in
 *  beforeEach. */
let dbRows: any[] | ((sql: string) => any[]) | null = null
/** SQL the service generated, for a test that asserts on its shape. */
let dbStatements: any[] = []

// Mock the database so getNearbyStops returns controlled stops without a
// real PostgreSQL connection. Each call alternates between an origin stop
// and a destination stop, producing exactly one stop pair per test.
// Spread the real module: a bare replacement drops `connection` and the
// ensure*Schema helpers for every file loaded after this one. See the note in
// motis-config.test.ts.
const actualDb = await import('../db')

mock.module('../db', () => ({
  ...actualDb,
  db: {
    execute: async (query: any) => {
      dbStatements.push(query)
      if (typeof dbRows === 'function') {
        const text = (query?.queryChunks ?? [])
          .flatMap((chunk: any) => chunk?.value ?? [])
          .join(' ')
        return dbRows(text)
      }
      if (dbRows) return dbRows
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
  getRoutesForStop,
  checkMotisHealth,
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

describe('getRoutesForStop', () => {
  /**
   * A station's own lines and the ones a transfer reaches are different
   * answers, and merging them is what put the A and C — a separate Chambers St
   * complex, no free transfer — on a Brooklyn Bridge–City Hall board. The
   * query decides which is which; this pins the contract it reports it under.
   */
  beforeEach(() => {
    dbRows = null
    dbStatements = []
  })

  const row = (shortName: string, atStation: boolean) => ({
    route_id: shortName, feed_id: '5', route_short_name: shortName,
    route_long_name: `${shortName} line`, route_type: 1,
    route_color: '00933C', route_text_color: 'FFFFFF',
    agency_name: 'MTA New York City Transit', at_station: atStation,
  })

  test('marks a line calling here apart from one a transfer reaches', async () => {
    dbRows = [row('4', true), row('6', true), row('J', false)]

    const routes = await getRoutesForStop('5', '640')

    expect(routes.map((r) => [r.routeShortName, r.via])).toEqual([
      ['4', 'station'],
      ['6', 'station'],
      ['J', 'transfer'],
    ])
  })

  test('separates this station from the complex around it', async () => {
    // Both halves have to be in the query: `station` is the seed and its own
    // platforms, `complex` is what transfers.txt reaches. Collapsing them back
    // into one set is the regression this guards.
    dbRows = []
    await getRoutesForStop('5', '640')

    // Drizzle keeps the literal SQL in string chunks, with the bound values
    // interleaved as parameter objects that carry no text.
    const text = (dbStatements[0]?.queryChunks ?? [])
      .flatMap((chunk: any) => chunk?.value ?? [])
      .join(' ')
    expect(text).toContain('station')
    expect(text).toContain('gtfs_transfers')
    expect(text).toContain('at_station')
  })

  test('folds the interchange into the station when a complex is asked for', async () => {
    // Whether a complex member counts as at_station is decided in SQL, which
    // the stub cannot evaluate — so what is pinned is the value bound into the
    // query. A tap on a merged label sends true; a tap on one station sends
    // false and keeps the split the tests above describe.
    //
    // Drizzle interleaves the literal SQL as StringChunks (whose `value` is a
    // string array) with the interpolated values themselves; anything that is
    // not a StringChunk is a bound value.
    const bound = (stmt: any) =>
      (stmt?.queryChunks ?? []).filter((chunk: any) => !Array.isArray(chunk?.value))

    dbRows = []
    await getRoutesForStop('5', '640', undefined, { complex: true })
    expect(bound(dbStatements[0])).toContain(true)

    dbStatements = []
    dbRows = []
    await getRoutesForStop('5', '640')
    expect(bound(dbStatements[0])).toContain(false)
  })
})

describe('nearby lines', () => {
  /**
   * A subway station's bus connections cannot come from `transfers.txt` —
   * that file is scoped to one feed, and the bus is another agency's. So they
   * come from proximity, which brings its own problem: New York is covered by
   * several overlapping bus feeds, so the same M22 arrives under three feed
   * ids and would be listed three times.
   */
  beforeEach(() => {
    dbRows = null
    dbStatements = []
  })

  const inSystem = (shortName: string) => ({
    route_id: shortName, feed_id: '5', route_short_name: shortName,
    route_long_name: `${shortName} line`, route_type: 1,
    route_color: null, route_text_color: null,
    agency_name: 'MTA New York City Transit', at_station: true,
  })

  const near = (routeId: string, feedId: string, shortName: string, distance: number) => ({
    route_id: routeId, feed_id: feedId, route_short_name: shortName,
    route_long_name: `${shortName} bus`, route_type: 3,
    route_color: null, route_text_color: null,
    agency_name: 'MTA New York City Transit', distance,
  })

  test('folds one line arriving from several feeds into one row', async () => {
    // The station's own lines and the nearby ones come from separate queries;
    // only the second one is a spatial search.
    dbRows = (text) =>
      text.includes('ST_DWithin')
        ? [
            near('M22', '7', 'M22', 29),
            near('M22-SBS', '8', 'M22', 140),
            near('M9', '7', 'M9', 88),
          ]
        : [inSystem('4')]

    const routes = await getRoutesForStop('5', '640', { lat: 40.71, lng: -74 })
    const nearby = routes.filter((r) => r.via === 'nearby')

    expect(nearby.map((r) => r.routeShortName)).toEqual(['M22', 'M9'])
    // The closest of the duplicates is the one kept.
    expect(nearby[0].distanceM).toBe(29)
  })

  test('never repeats a line the station already runs', async () => {
    // The M22 both stops here and runs from the stop outside: it is the
    // station's line, and must not also be offered as a nearby one.
    dbRows = (text) =>
      text.includes('ST_DWithin') ? [near('M22', '7', 'M22', 29)] : [inSystem('M22')]

    const routes = await getRoutesForStop('5', '640', { lat: 40.71, lng: -74 })

    expect(routes.filter((r) => r.routeShortName === 'M22')).toHaveLength(1)
    expect(routes[0].via).toBe('station')
  })

  test('caps how many nearby lines it reports, nearest train first', async () => {
    // Two hundred metres of Lower Manhattan is twenty-five lines: the 1 fifty
    // metres away, then a wall of Staten Island express buses. The cap runs
    // after the sort, so the train survives it and the buses are what go.
    const buses = Array.from({ length: 20 }, (_, i) =>
      near(`SIM${i}`, '7', `SIM${i}`, 80 + i),
    )
    dbRows = (text) =>
      text.includes('ST_DWithin')
        ? [...buses, near('1', '5', '1', 50)]
        : [inSystem('R')]

    const routes = await getRoutesForStop('5', 'R26', { lat: 40.707, lng: -74.013 })
    const nearby = routes.filter((r) => r.via === 'nearby')

    expect(nearby).toHaveLength(6)
    expect(nearby[0].routeShortName).toBe('1')
  })

  test('honours an explicit nearby limit', async () => {
    dbRows = (text) =>
      text.includes('ST_DWithin')
        ? [near('M22', '7', 'M22', 20), near('M9', '7', 'M9', 30)]
        : [inSystem('4')]

    const routes = await getRoutesForStop('5', '640', {
      lat: 40.71,
      lng: -74,
      limit: 1,
    })

    expect(routes.filter((r) => r.via === 'nearby')).toHaveLength(1)
  })

  test('asks for nothing extra when no point is given', async () => {
    dbRows = []
    await getRoutesForStop('5', '640')

    expect(dbStatements).toHaveLength(1)
  })
})

describe('checkMotisHealth', () => {
  /**
   * MOTIS only answers 200 when every updater is healthy, so a dead GBFS feed
   * makes it return 400 while it is still serving stoptimes normally. That was
   * read as an outage, and `/health` took the whole transit group down with it.
   */
  const respond = (status: number, body: unknown) =>
    (async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { 'content-type': 'application/json' },
      })) as unknown as typeof fetch

  test('is ok when every subsystem is healthy', async () => {
    expect(await checkMotisHealth(respond(200, { rt: true, gbfs: true }))).toEqual({ status: 'ok' })
  })

  test('stays up when a subsystem is degraded, and names it', async () => {
    const health = await checkMotisHealth(respond(400, { rt: true, gbfs: false }))

    expect(health.status).toBe('ok')
    expect(health.message).toContain('gbfs')
    expect(health.message).not.toContain('rt')
  })

  test('is unavailable for an error that is not a subsystem report', async () => {
    const health = await checkMotisHealth(respond(502, { error: 'bad gateway' }))

    expect(health.status).toBe('unavailable')
    expect(health.message).toContain('502')
  })

  test('is unavailable when nothing answers', async () => {
    const refuse = (async () => {
      throw new Error('Connection refused')
    }) as unknown as typeof fetch

    expect(await checkMotisHealth(refuse)).toEqual({
      status: 'unavailable',
      message: 'Connection refused',
    })
  })
})
