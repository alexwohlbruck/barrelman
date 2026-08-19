/**
 * Tests for the GTFS import service.
 *
 * Validates:
 *   - GTFS CSV parsing (stops, routes, agencies, stop-route derivation)
 *   - transfers.txt generation format
 *   - Edge cases in CSV parsing (missing fields, Unicode, special chars)
 *   - GTFS-RT feed discovery from Transitland
 */

import { describe, test, expect } from 'bun:test'
import {
  parseStops,
  parseRoutes,
  parseAgencies,
  parseShapes,
  deriveStopRoutes,
  parseStopParents,
  deriveTripPatterns,
  deriveRouteShapes,
  deriveBikesAllowed,
  generateTransfersTxt,
  fetchFeedList,
  resolveGtfsBbox,
  sanitizeGtfsZip,
  resolveRtUrlsForFeed,
  FLEX_EXTENSION_FILES,
} from './gtfs.service'
import JSZip from 'jszip'

// ── parseStops ──────────────────────────────────────────────────────

describe('parseStops', () => {
  test('parses standard stops.txt content', () => {
    const csv = [
      'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,wheelchair_boarding',
      'S001,Central Station,35.2271,-80.8431,0,,1',
      'S002,Park Ave,35.2350,-80.8500,0,,0',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops).toHaveLength(2)
    expect(stops[0]).toEqual({
      stopId: 'S001',
      feedId: 'feed_1',
      stopName: 'Central Station',
      stopCode: null,
      stopLat: 35.2271,
      stopLon: -80.8431,
      locationType: 0,
      parentStation: null,
      wheelchairBoarding: 1,
      platformCode: null,
    })
  })

  test('handles stops with parent stations', () => {
    const csv = [
      'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station',
      'STATION,Grand Central,40.7527,-73.9772,1,',
      'PLAT_A,Platform A,40.7527,-73.9772,0,STATION',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops[0].locationType).toBe(1)
    expect(stops[0].parentStation).toBe(null) // empty string → null
    expect(stops[1].locationType).toBe(0)
    expect(stops[1].parentStation).toBe('STATION')
  })

  test('skips stops with missing coordinates', () => {
    const csv = [
      'stop_id,stop_name,stop_lat,stop_lon',
      'S001,Has Coords,35.22,-80.84',
      'S002,No Lat,,80.84',
      'S003,No Lon,35.22,',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops).toHaveLength(1)
    expect(stops[0].stopId).toBe('S001')
  })

  test('handles Unicode stop names', () => {
    const csv = [
      'stop_id,stop_name,stop_lat,stop_lon',
      'S001,Gare du Nord,48.8809,2.3553',
      'S002,東京駅,35.6812,139.7671',
      'S003,Estación de Atocha,40.4068,-3.6914',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops).toHaveLength(3)
    expect(stops[0].stopName).toBe('Gare du Nord')
    expect(stops[1].stopName).toBe('東京駅')
    expect(stops[2].stopName).toBe('Estación de Atocha')
  })

  test('handles stop codes and platform codes', () => {
    const csv = [
      'stop_id,stop_name,stop_code,stop_lat,stop_lon,platform_code',
      'S001,Main St,1234,35.22,-80.84,A',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops[0].stopCode).toBe('1234')
    expect(stops[0].platformCode).toBe('A')
  })

  test('defaults location_type to 0 when missing', () => {
    const csv = [
      'stop_id,stop_name,stop_lat,stop_lon',
      'S001,Simple Stop,35.22,-80.84',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops[0].locationType).toBe(0)
  })

  test('handles extra columns gracefully', () => {
    const csv = [
      'stop_id,stop_name,stop_lat,stop_lon,stop_desc,zone_id,stop_url,stop_timezone',
      'S001,Test Stop,35.22,-80.84,A test stop,zone1,http://example.com,America/New_York',
    ].join('\n')

    const stops = parseStops(csv, 'feed_1')
    expect(stops).toHaveLength(1)
    expect(stops[0].stopId).toBe('S001')
  })
})

// ── parseAgencies ───────────────────────────────────────────────────

describe('parseAgencies', () => {
  test('builds agency_id → name map', () => {
    const csv = [
      'agency_id,agency_name,agency_url,agency_timezone',
      'CATS,Charlotte Area Transit,http://cats.example.com,America/New_York',
      'GT,GoTriangle,http://gotriangle.example.com,America/New_York',
    ].join('\n')

    const map = parseAgencies(csv)
    expect(map.get('CATS')).toBe('Charlotte Area Transit')
    expect(map.get('GT')).toBe('GoTriangle')
  })

  test('handles agency with empty id', () => {
    const csv = [
      'agency_id,agency_name,agency_url,agency_timezone',
      ',Single Agency,http://example.com,America/New_York',
    ].join('\n')

    const map = parseAgencies(csv)
    expect(map.get('')).toBe('Single Agency')
  })
})

// ── parseRoutes ─────────────────────────────────────────────────────

describe('parseRoutes', () => {
  test('parses routes with agency names', () => {
    const csv = [
      'route_id,agency_id,route_short_name,route_long_name,route_type,route_color,route_text_color',
      'R001,CATS,9,Route 9 - Downtown,3,FF0000,FFFFFF',
      'R002,CATS,Blue,Blue Line,1,0000FF,FFFFFF',
    ].join('\n')

    const agencyMap = new Map([['CATS', 'Charlotte Area Transit']])
    const routes = parseRoutes(csv, 'feed_1', agencyMap)

    expect(routes).toHaveLength(2)
    expect(routes[0]).toEqual({
      routeId: 'R001',
      feedId: 'feed_1',
      agencyId: 'CATS',
      agencyName: 'Charlotte Area Transit',
      routeShortName: '9',
      routeLongName: 'Route 9 - Downtown',
      routeType: 3,
      routeColor: 'FF0000',
      routeTextColor: 'FFFFFF',
      routeUrl: null,
    })
  })

  test('handles missing optional fields', () => {
    const csv = [
      'route_id,route_type',
      'R001,3',
    ].join('\n')

    const routes = parseRoutes(csv, 'feed_1', new Map())
    expect(routes[0].routeShortName).toBeNull()
    expect(routes[0].routeLongName).toBeNull()
    expect(routes[0].routeColor).toBeNull()
    expect(routes[0].agencyName).toBeNull()
  })

  test('defaults route_type to 3 (bus) when invalid', () => {
    const csv = [
      'route_id,route_type',
      'R001,invalid',
    ].join('\n')

    const routes = parseRoutes(csv, 'feed_1', new Map())
    expect(routes[0].routeType).toBe(3)
  })

  test('keeps route_type 0 (tram) instead of defaulting it to bus', () => {
    // `parseInt('0') || 3` is 3: every tram and streetcar route in every feed
    // imported as a bus, silently. The Roosevelt Island Tramway is published as
    // type 0, and rode the whole way through the app labelled a bus.
    const csv = [
      'route_id,route_type,route_long_name',
      'R001,0,Roosevelt Island Aerial Tramway',
    ].join('\n')

    const routes = parseRoutes(csv, 'feed_1', new Map())
    expect(routes[0].routeType).toBe(0)
  })

  test.each([
    ['tram', 0],
    ['subway', 1],
    ['rail', 2],
    ['bus', 3],
    ['ferry', 4],
    ['cable tram', 5],
    ['aerial lift', 6],
    ['funicular', 7],
  ])('preserves route_type %s (%i)', (_mode, type) => {
    const csv = ['route_id,route_type', `R001,${type}`].join('\n')

    expect(parseRoutes(csv, 'feed_1', new Map())[0].routeType).toBe(type)
  })
})

// ── deriveStopRoutes ────────────────────────────────────────────────

describe('deriveStopRoutes', () => {
  test('derives unique stop-route associations from trips and stop_times', () => {
    const trips = [
      'trip_id,route_id,service_id',
      'T001,R001,weekday',
      'T002,R001,weekday',
      'T003,R002,weekday',
    ].join('\n')

    const stopTimes = [
      'trip_id,arrival_time,departure_time,stop_id,stop_sequence',
      'T001,08:00:00,08:01:00,S001,1',
      'T001,08:10:00,08:11:00,S002,2',
      'T001,08:20:00,08:21:00,S003,3',
      'T002,09:00:00,09:01:00,S001,1',  // duplicate S001-R001, should be deduped
      'T002,09:10:00,09:11:00,S002,2',
      'T003,08:00:00,08:01:00,S002,1',  // S002 also served by R002
      'T003,08:15:00,08:16:00,S004,2',
    ].join('\n')

    const assocs = deriveStopRoutes(trips, stopTimes, 'feed_1')

    // Should have unique pairs only:
    // S001-R001, S002-R001, S003-R001, S002-R002, S004-R002
    expect(assocs).toHaveLength(5)

    const keys = assocs.map(a => `${a.stopId}-${a.routeId}`)
    expect(keys).toContain('S001-R001')
    expect(keys).toContain('S002-R001')
    expect(keys).toContain('S003-R001')
    expect(keys).toContain('S002-R002')
    expect(keys).toContain('S004-R002')

    // All should have correct feedId
    for (const a of assocs) {
      expect(a.feedId).toBe('feed_1')
    }
  })

  test('handles trip with no matching stop_times', () => {
    const trips = [
      'trip_id,route_id,service_id',
      'T001,R001,weekday',
    ].join('\n')

    const stopTimes = [
      'trip_id,arrival_time,departure_time,stop_id,stop_sequence',
    ].join('\n')

    const assocs = deriveStopRoutes(trips, stopTimes, 'feed_1')
    expect(assocs).toHaveLength(0)
  })

  test('handles stop_times with unknown trip_id', () => {
    const trips = [
      'trip_id,route_id,service_id',
      'T001,R001,weekday',
    ].join('\n')

    const stopTimes = [
      'trip_id,arrival_time,departure_time,stop_id,stop_sequence',
      'UNKNOWN,08:00:00,08:01:00,S001,1',
    ].join('\n')

    const assocs = deriveStopRoutes(trips, stopTimes, 'feed_1')
    expect(assocs).toHaveLength(0)
  })
})

// ── parseStopParents ────────────────────────────────────────────────

describe('parseStopParents', () => {
  test('maps platforms to their parent station, stations to themselves', () => {
    const stops = [
      'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station',
      'A,Station A,40.0,-73.0,1,',
      'A_N,Station A - North,40.0,-73.0,0,A',
      'A_S,Station A - South,40.0,-73.0,0,A',
      'B,Bus Stop B,40.1,-73.1,0,', // no parent → itself
    ].join('\n')

    const map = parseStopParents(stops)
    expect(map.get('A')).toBe('A')
    expect(map.get('A_N')).toBe('A')
    expect(map.get('A_S')).toBe('A')
    expect(map.get('B')).toBe('B')
  })
})

// ── deriveTripPatterns ──────────────────────────────────────────────

describe('deriveTripPatterns', () => {
  const stops = [
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station',
    'A,A,40.0,-73.0,1,',
    'A_N,A north,40.0,-73.0,0,A',
    'B,B,40.1,-73.0,1,',
    'B_N,B north,40.1,-73.0,0,B',
    'C,C,40.2,-73.0,1,',
    'C_N,C north,40.2,-73.0,0,C',
    'D,D,40.3,-73.0,1,',
    'D_N,D north,40.3,-73.0,0,D',
  ].join('\n')
  const parents = parseStopParents(stops)

  test('normalises platforms to stations and dedupes identical patterns', () => {
    const trips = [
      'trip_id,route_id,direction_id',
      'X1,X,0',
      'X2,X,0', // same sequence as X1 → one pattern
      'Y1,Y,0',
      'Z1,Z,0',
    ].join('\n')
    // X and Y run EXPRESS A→B→D (skip C); Z runs LOCAL A→B→C→D. Platform ids.
    const stopTimes = [
      'trip_id,stop_id,stop_sequence',
      'X1,A_N,1', 'X1,B_N,2', 'X1,D_N,3',
      'X2,A_N,1', 'X2,B_N,2', 'X2,D_N,3',
      'Y1,A_N,1', 'Y1,B_N,2', 'Y1,D_N,3',
      'Z1,A_N,1', 'Z1,B_N,2', 'Z1,C_N,3', 'Z1,D_N,4',
    ].join('\n')

    const patterns = deriveTripPatterns(trips, stopTimes, parents, 'feed_1')
    const byRoute = new Map(patterns.map((p) => [p.routeId, p.stopSeq]))

    // X1/X2 collapse to one pattern; X and Y share the express station sequence.
    expect(patterns.filter((p) => p.routeId === 'X')).toHaveLength(1)
    expect(byRoute.get('X')).toBe(',A,B,D,')
    expect(byRoute.get('Y')).toBe(',A,B,D,')
    // Z (local) carries the extra station C, so it won't match the express run.
    expect(byRoute.get('Z')).toBe(',A,B,C,D,')

    // The express run ",A,B,D," is a substring of X and Y but NOT of Z.
    const expressNeedle = ',A,B,D,'
    const directExpress = patterns
      .filter((p) => p.stopSeq.includes(expressNeedle))
      .map((p) => p.routeId)
      .sort()
    expect(directExpress).toEqual(['X', 'Y'])
  })
})

// ── generateTransfersTxt ────────────────────────────────────────────

describe('generateTransfersTxt', () => {
  test('generates valid transfers.txt format', () => {
    const transfers = [
      { fromStopId: 'S001', toStopId: 'S002', walkTime: 180, walkDistance: 250 },
      { fromStopId: 'S002', toStopId: 'S001', walkTime: 185, walkDistance: 250 },
      { fromStopId: 'S003', toStopId: 'S004', walkTime: 300, walkDistance: 400 },
    ]

    const txt = generateTransfersTxt(transfers)
    const lines = txt.split('\n')

    // Header
    expect(lines[0]).toBe('from_stop_id,to_stop_id,transfer_type,min_transfer_time')

    // Data rows
    expect(lines[1]).toBe('S001,S002,2,180')
    expect(lines[2]).toBe('S002,S001,2,185')
    expect(lines[3]).toBe('S003,S004,2,300')
  })

  test('handles empty transfer list', () => {
    const txt = generateTransfersTxt([])
    expect(txt).toBe('from_stop_id,to_stop_id,transfer_type,min_transfer_time\n')
  })

  test('uses transfer_type=2 (timed transfer with min time)', () => {
    const transfers = [
      { fromStopId: 'A', toStopId: 'B', walkTime: 60, walkDistance: 80 },
    ]

    const txt = generateTransfersTxt(transfers)
    expect(txt).toContain(',2,') // transfer_type must be 2
  })
})

// ── fetchFeedList + GTFS-RT discovery ──────────────────────────────

/**
 * Helper: build a mock fetchFn that routes requests to different handlers
 * based on URL patterns. Simulates both Transitland feed-list responses
 * and per-feed RT lookups.
 */
function buildMockFetch(handlers: {
  feedList?: any
  rtFeeds?: Record<string, any>  // keyed by RT onestop_id
}) {
  return async (url: string | URL | Request, _init?: RequestInit) => {
    const urlStr = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url

    // RT feed lookup — matches spec=GTFS_RT&onestop_id=...
    if (urlStr.includes('spec=GTFS_RT') && urlStr.includes('&onestop_id=')) {
      const parsed = new URL(urlStr)
      const onestopId = parsed.searchParams.get('onestop_id') || ''
      const rtFeed = handlers.rtFeeds?.[onestopId]
      return new Response(JSON.stringify({ feeds: rtFeed ? [rtFeed] : [] }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Static feed list
    if (urlStr.includes('transit.land') && urlStr.includes('spec=gtfs')) {
      return new Response(JSON.stringify(handlers.feedList || { feeds: [] }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response('Not found', { status: 404 })
  }
}

describe('fetchFeedList', () => {
  test('returns static feeds from Transitland', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 100,
            onestop_id: 'f-dnh-cats',
            name: 'CATS',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/cats.zip' },
          },
        ],
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(1)
    expect(feeds[0].feedId).toBe('100')
    expect(feeds[0].onestopId).toBe('f-dnh-cats')
    expect(feeds[0].name).toBe('CATS')
    expect(feeds[0].url).toBe('https://example.com/cats.zip')
  })

  test('skips feeds without download URL', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
          {
            id: 2,
            onestop_id: 'f-def',
            spec: 'gtfs',
            urls: {},  // no static_current
          },
        ],
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(1)
    expect(feeds[0].feedId).toBe('1')
  })

  test('skips non-GTFS feeds (e.g. GTFS_RT entries in feed list)', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
          {
            id: 2,
            onestop_id: 'f-abc~rt',
            spec: 'GTFS_RT',
            urls: { realtime_trip_updates: 'https://example.com/rt' },
          },
        ],
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(1)
    expect(feeds[0].onestopId).toBe('f-abc')
  })

  test('discovers and attaches GTFS-RT URLs via ~rt onestop_id convention', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 886,
            onestop_id: 'f-dnh-cats',
            name: 'CATS',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/cats.zip' },
          },
        ],
      },
      rtFeeds: {
        'f-dnh-cats~rt': {
          onestop_id: 'f-dnh-cats~rt',
          spec: 'GTFS_RT',
          urls: {
            realtime_trip_updates: 'https://rt.example.com/trip-updates.pb',
            realtime_vehicle_positions: 'https://rt.example.com/vehicle-positions.pb',
            realtime_alerts: 'https://rt.example.com/alerts.pb',
          },
        },
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(1)
    expect(feeds[0].rtUrls).toBeDefined()
    expect(feeds[0].rtUrls).toHaveLength(3)

    const urls = feeds[0].rtUrls!.map(r => r.url)
    expect(urls).toContain('https://rt.example.com/trip-updates.pb')
    expect(urls).toContain('https://rt.example.com/vehicle-positions.pb')
    expect(urls).toContain('https://rt.example.com/alerts.pb')
  })

  test('handles partial RT URLs (only trip updates available)', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc-agency',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      },
      rtFeeds: {
        'f-abc-agency~rt': {
          onestop_id: 'f-abc-agency~rt',
          spec: 'GTFS_RT',
          urls: {
            realtime_trip_updates: 'https://rt.example.com/updates.pb',
            // no vehicle positions or alerts
          },
        },
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds[0].rtUrls).toHaveLength(1)
    expect(feeds[0].rtUrls![0].url).toBe('https://rt.example.com/updates.pb')
  })

  test('includes authorization headers from RT feed metadata', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc-agency',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      },
      rtFeeds: {
        'f-abc-agency~rt': {
          onestop_id: 'f-abc-agency~rt',
          spec: 'GTFS_RT',
          urls: {
            realtime_trip_updates: 'https://rt.example.com/updates.pb',
          },
          authorization: {
            type: 'header',
            param_name: 'X-Api-Key',
            param_value: 'secret-123',
          },
        },
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds[0].rtUrls).toHaveLength(1)
    expect(feeds[0].rtUrls![0].headers).toEqual({ 'X-Api-Key': 'secret-123' })
  })

  test('omits headers object when no authorization is configured', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      },
      rtFeeds: {
        'f-abc~rt': {
          onestop_id: 'f-abc~rt',
          spec: 'GTFS_RT',
          urls: { realtime_trip_updates: 'https://rt.example.com/updates.pb' },
          // no authorization field
        },
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds[0].rtUrls![0].headers).toBeUndefined()
  })

  test('leaves rtUrls undefined when no RT feed exists', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc-agency',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      },
      rtFeeds: {}, // no RT feeds
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds[0].rtUrls).toBeUndefined()
  })

  test('handles feeds without onestop_id (RT lookup skipped)', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 99,
            // no onestop_id
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(1)
    expect(feeds[0].onestopId).toBe('')
    expect(feeds[0].rtUrls).toBeUndefined()
  })

  test('handles RT lookup API errors gracefully', async () => {
    let callCount = 0
    const mockFetch = async (url: string | URL | Request) => {
      const urlStr = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url

      if (urlStr.includes('spec=GTFS_RT')) {
        callCount++
        // Simulate network error on RT lookup
        throw new Error('Network timeout')
      }

      // Static feed list succeeds
      return new Response(JSON.stringify({
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      }), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }

    // Should not throw — RT failures are handled gracefully via Promise.allSettled
    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(1)
    expect(feeds[0].rtUrls).toBeUndefined()
    expect(callCount).toBe(1)
  })

  test('discovers RT feeds for multiple static feeds in batch', async () => {
    const staticFeeds = Array.from({ length: 3 }, (_, i) => ({
      id: i + 1,
      onestop_id: `f-feed${i + 1}`,
      spec: 'gtfs',
      urls: { static_current: `https://example.com/feed${i + 1}.zip` },
    }))

    const rtFeeds: Record<string, any> = {
      'f-feed1~rt': {
        onestop_id: 'f-feed1~rt',
        spec: 'GTFS_RT',
        urls: { realtime_trip_updates: 'https://rt.example.com/feed1.pb' },
      },
      // feed2 has no RT
      'f-feed3~rt': {
        onestop_id: 'f-feed3~rt',
        spec: 'GTFS_RT',
        urls: {
          realtime_trip_updates: 'https://rt.example.com/feed3-updates.pb',
          realtime_vehicle_positions: 'https://rt.example.com/feed3-vehicles.pb',
        },
      },
    }

    const mockFetch = buildMockFetch({ feedList: { feeds: staticFeeds }, rtFeeds })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    expect(feeds).toHaveLength(3)

    // Feed 1: has 1 RT URL
    expect(feeds[0].rtUrls).toHaveLength(1)
    expect(feeds[0].rtUrls![0].url).toBe('https://rt.example.com/feed1.pb')

    // Feed 2: no RT
    expect(feeds[1].rtUrls).toBeUndefined()

    // Feed 3: has 2 RT URLs
    expect(feeds[2].rtUrls).toHaveLength(2)
  })

  test('ignores non-header authorization types', async () => {
    const mockFetch = buildMockFetch({
      feedList: {
        feeds: [
          {
            id: 1,
            onestop_id: 'f-abc',
            spec: 'gtfs',
            urls: { static_current: 'https://example.com/feed.zip' },
          },
        ],
      },
      rtFeeds: {
        'f-abc~rt': {
          onestop_id: 'f-abc~rt',
          spec: 'GTFS_RT',
          urls: { realtime_trip_updates: 'https://rt.example.com/updates.pb' },
          authorization: {
            type: 'query_param',  // not 'header'
            param_name: 'api_key',
            param_value: 'secret',
          },
        },
      },
    })

    const feeds = await fetchFeedList('nc', 'test-key', mockFetch)
    // Authorization not type=header → no headers attached
    expect(feeds[0].rtUrls![0].headers).toBeUndefined()
  })
})

// ── parseShapes ─────────────────────────────────────────────────────

describe('parseShapes', () => {
  test('parses standard shapes.txt content', () => {
    const csv = [
      'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence',
      'shape-1,35.2271,-80.8431,1',
      'shape-1,35.2350,-80.8500,2',
      'shape-1,35.2400,-80.8550,3',
    ].join('\n')

    const result = parseShapes(csv)
    expect(result.size).toBe(1)
    expect(result.has('shape-1')).toBe(true)
    const coords = result.get('shape-1')!
    expect(coords).toHaveLength(3)
    // [lng, lat] order
    expect(coords[0]).toEqual([-80.8431, 35.2271])
    expect(coords[1]).toEqual([-80.8500, 35.2350])
    expect(coords[2]).toEqual([-80.8550, 35.2400])
  })

  test('handles multiple shape IDs', () => {
    const csv = [
      'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence',
      'shape-a,35.0,-80.0,1',
      'shape-a,35.1,-80.1,2',
      'shape-b,36.0,-81.0,1',
      'shape-b,36.1,-81.1,2',
      'shape-b,36.2,-81.2,3',
    ].join('\n')

    const result = parseShapes(csv)
    expect(result.size).toBe(2)
    expect(result.get('shape-a')!).toHaveLength(2)
    expect(result.get('shape-b')!).toHaveLength(3)
  })

  test('sorts points by sequence number', () => {
    const csv = [
      'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence',
      'shape-1,35.3,-80.3,3',
      'shape-1,35.1,-80.1,1',
      'shape-1,35.2,-80.2,2',
    ].join('\n')

    const result = parseShapes(csv)
    const coords = result.get('shape-1')!
    // Should be sorted by sequence: 1, 2, 3
    expect(coords[0]).toEqual([-80.1, 35.1])
    expect(coords[1]).toEqual([-80.2, 35.2])
    expect(coords[2]).toEqual([-80.3, 35.3])
  })

  test('skips rows with invalid data', () => {
    const csv = [
      'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence',
      'shape-1,35.0,-80.0,1',
      ',35.1,-80.1,2',        // missing shape_id
      'shape-1,abc,-80.2,3',   // invalid lat
      'shape-1,35.3,-80.3,4',
    ].join('\n')

    const result = parseShapes(csv)
    const coords = result.get('shape-1')!
    expect(coords).toHaveLength(2) // only valid rows
  })

  test('returns empty map for empty input', () => {
    const csv = 'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n'
    const result = parseShapes(csv)
    expect(result.size).toBe(0)
  })
})

// ── deriveRouteShapes ───────────────────────────────────────────────

describe('deriveRouteShapes', () => {
  test('picks the most common shape per route', () => {
    const csv = [
      'route_id,trip_id,shape_id,service_id,direction_id',
      'route-1,trip-1,shape-a,weekday,0',
      'route-1,trip-2,shape-a,weekday,0',
      'route-1,trip-3,shape-b,weekday,1',
      'route-2,trip-4,shape-c,weekday,0',
    ].join('\n')

    const result = deriveRouteShapes(csv)
    expect(result.size).toBe(2)
    expect(result.get('route-1')).toBe('shape-a') // 2 trips vs 1
    expect(result.get('route-2')).toBe('shape-c')
  })

  test('skips trips without shape_id', () => {
    const csv = [
      'route_id,trip_id,shape_id,service_id',
      'route-1,trip-1,,weekday',
      'route-1,trip-2,shape-a,weekday',
    ].join('\n')

    const result = deriveRouteShapes(csv)
    expect(result.get('route-1')).toBe('shape-a')
  })

  test('returns empty map when no shapes', () => {
    const csv = [
      'route_id,trip_id,shape_id,service_id',
      'route-1,trip-1,,weekday',
    ].join('\n')

    const result = deriveRouteShapes(csv)
    expect(result.size).toBe(0)
  })
})

// ── sanitizeGtfsZip ─────────────────────────────────────────────────

describe('sanitizeGtfsZip', () => {
  async function createTestZip(files: Record<string, string>): Promise<ArrayBuffer> {
    const zip = new JSZip()
    for (const [name, content] of Object.entries(files)) {
      zip.file(name, content)
    }
    return await zip.generateAsync({ type: 'arraybuffer' })
  }

  async function listZipFiles(buffer: ArrayBuffer): Promise<string[]> {
    const zip = await JSZip.loadAsync(buffer)
    return Object.keys(zip.files).filter(f => !zip.files[f].dir).sort()
  }

  test('strips GTFS-Flex extension files from ZIP', async () => {
    const buffer = await createTestZip({
      'stops.txt': 'stop_id,stop_name\nS1,Main St',
      'routes.txt': 'route_id,route_short_name\nR1,Blue',
      'areas.txt': 'area_id,area_name\nA1,Downtown',
      'stop_areas.txt': 'area_id,stop_id\nA1,S1',
      'locations.geojson': '{"type":"FeatureCollection","features":[]}',
    })

    const { buffer: sanitized, removedFiles } = await sanitizeGtfsZip(buffer)

    expect(removedFiles).toContain('areas.txt')
    expect(removedFiles).toContain('stop_areas.txt')
    expect(removedFiles).toContain('locations.geojson')
    expect(removedFiles).toHaveLength(3)

    const remaining = await listZipFiles(sanitized)
    expect(remaining).toContain('stops.txt')
    expect(remaining).toContain('routes.txt')
    expect(remaining).not.toContain('areas.txt')
    expect(remaining).not.toContain('stop_areas.txt')
    expect(remaining).not.toContain('locations.geojson')
  })

  test('returns original buffer when no flex files present', async () => {
    const buffer = await createTestZip({
      'stops.txt': 'stop_id,stop_name\nS1,Main St',
      'routes.txt': 'route_id,route_short_name\nR1,Blue',
      'trips.txt': 'route_id,trip_id\nR1,T1',
    })

    const { buffer: result, removedFiles } = await sanitizeGtfsZip(buffer)

    expect(removedFiles).toHaveLength(0)
    // Buffer should be the exact same reference (no re-zip)
    expect(result).toBe(buffer)
  })

  test('preserves standard GTFS files while stripping all flex extensions', async () => {
    const standardFiles: Record<string, string> = {
      'agency.txt': 'agency_id,agency_name\nA1,Metro',
      'stops.txt': 'stop_id,stop_name\nS1,Station',
      'routes.txt': 'route_id\nR1',
      'trips.txt': 'trip_id\nT1',
      'stop_times.txt': 'trip_id,stop_id\nT1,S1',
      'calendar.txt': 'service_id\nWKDY',
      'shapes.txt': 'shape_id,shape_pt_lat\nSH1,35.0',
      'transfers.txt': 'from_stop_id,to_stop_id\nS1,S2',
    }
    const flexFiles: Record<string, string> = {
      'areas.txt': 'area_id\nA1',
      'stop_areas.txt': 'area_id,stop_id\nA1,S1',
      'booking_rules.txt': 'booking_rule_id\nBR1',
      'location_groups.txt': 'location_group_id\nLG1',
      'location_group_stops.txt': 'location_group_id,stop_id\nLG1,S1',
      'locations.geojson': '{"type":"FeatureCollection","features":[]}',
    }

    const buffer = await createTestZip({ ...standardFiles, ...flexFiles })
    const { buffer: sanitized, removedFiles } = await sanitizeGtfsZip(buffer)

    expect(removedFiles.sort()).toEqual([...FLEX_EXTENSION_FILES].sort())

    const remaining = await listZipFiles(sanitized)
    for (const stdFile of Object.keys(standardFiles)) {
      expect(remaining).toContain(stdFile)
    }
    for (const flexFile of Object.keys(flexFiles)) {
      expect(remaining).not.toContain(flexFile)
    }
  })

  test('preserves file contents after sanitization', async () => {
    const stopsContent = 'stop_id,stop_name,stop_lat,stop_lon\nS1,Main,35.0,-80.0'
    const buffer = await createTestZip({
      'stops.txt': stopsContent,
      'areas.txt': 'area_id\nA1',
    })

    const { buffer: sanitized } = await sanitizeGtfsZip(buffer)
    const zip = await JSZip.loadAsync(sanitized)
    const content = await zip.file('stops.txt')!.async('string')
    expect(content).toBe(stopsContent)
  })
})

// ── deriveBikesAllowed ──────────────────────────────────────────────

describe('deriveBikesAllowed', () => {
  test('returns 2 when all trips on a route allow bikes', () => {
    const csv = [
      'route_id,trip_id,bikes_allowed',
      'route-1,trip-1,1',
      'route-1,trip-2,1',
      'route-1,trip-3,1',
    ].join('\n')

    const result = deriveBikesAllowed(csv)
    expect(result.get('route-1')).toBe(2) // all trips allow
  })

  test('returns 1 when some trips allow bikes', () => {
    const csv = [
      'route_id,trip_id,bikes_allowed',
      'route-1,trip-1,1',
      'route-1,trip-2,0',
      'route-1,trip-3,1',
    ].join('\n')

    const result = deriveBikesAllowed(csv)
    expect(result.get('route-1')).toBe(1) // some trips allow
  })

  test('returns 0 when no trips allow bikes', () => {
    const csv = [
      'route_id,trip_id,bikes_allowed',
      'route-1,trip-1,0',
      'route-1,trip-2,2', // 2 = not allowed in GTFS spec
      'route-1,trip-3,',  // empty = unknown
    ].join('\n')

    const result = deriveBikesAllowed(csv)
    expect(result.get('route-1')).toBe(0)
  })

  test('handles missing bikes_allowed column', () => {
    const csv = [
      'route_id,trip_id,shape_id',
      'route-1,trip-1,shape-a',
      'route-1,trip-2,shape-b',
    ].join('\n')

    const result = deriveBikesAllowed(csv)
    expect(result.get('route-1')).toBe(0) // unknown
  })

  test('handles multiple routes independently', () => {
    const csv = [
      'route_id,trip_id,bikes_allowed',
      'route-1,trip-1,1',
      'route-1,trip-2,1',
      'route-2,trip-3,0',
      'route-2,trip-4,0',
      'route-3,trip-5,1',
      'route-3,trip-6,0',
    ].join('\n')

    const result = deriveBikesAllowed(csv)
    expect(result.get('route-1')).toBe(2) // all allow
    expect(result.get('route-2')).toBe(0) // none allow
    expect(result.get('route-3')).toBe(1) // some allow
  })

  test('skips rows without route_id', () => {
    const csv = [
      'route_id,trip_id,bikes_allowed',
      ',trip-1,1',
      'route-1,trip-2,1',
    ].join('\n')

    const result = deriveBikesAllowed(csv)
    expect(result.size).toBe(1)
    expect(result.get('route-1')).toBe(2)
  })
})

// ── resolveGtfsBbox ─────────────────────────────────────────────────

describe('resolveGtfsBbox', () => {
  test('returns null for global (every feed, no bbox filter)', () => {
    expect(resolveGtfsBbox('global')).toBeNull()
  })

  test('resolves the legacy named tokens', () => {
    expect(resolveGtfsBbox('nc')).toBe('-84.5,33.8,-75.4,36.6')
    expect(resolveGtfsBbox('nyc')).toBe('-74.3,40.45,-73.7,40.95')
  })

  test('accepts a literal west,south,east,north bbox', () => {
    expect(resolveGtfsBbox('-109.06,36.99,-102.04,41')).toBe('-109.06,36.99,-102.04,41')
  })

  test('tolerates whitespace in a literal bbox', () => {
    expect(resolveGtfsBbox('-109.06, 36.99, -102.04, 41')).toBe('-109.06,36.99,-102.04,41')
  })

  test('throws on an unknown token instead of silently fetching every feed', () => {
    // The whole point: a region whose token nobody hardcoded used to fall
    // through to an unfiltered query and download the global catalog.
    expect(() => resolveGtfsBbox('co')).toThrow(/Unknown GTFS region "co"/)
  })

  test('throws when east/west or north/south are transposed', () => {
    expect(() => resolveGtfsBbox('10,0,-10,20')).toThrow(/malformed/)
    expect(() => resolveGtfsBbox('-10,20,10,0')).toThrow(/malformed/)
  })

  test('throws on out-of-range coordinates', () => {
    expect(() => resolveGtfsBbox('-200,0,10,10')).toThrow(/valid lon\/lat ranges/)
  })
})

// ── resolveRtUrlsForFeed ────────────────────────────────────────────

/**
 * Finding an agency's realtime feeds.
 *
 * Deriving `{staticOnestopId}~rt` holds for some agencies and not at all for
 * others: MTA's realtime feeds are named `f-mta~nyc~rt~subway~a~c~e`, which
 * cannot be derived from the static `f-dr5r-nyctsubway`. Discovery found
 * nothing for the whole of New York and reported success while doing it, so
 * these pin the operator lookup that actually joins them — including the part
 * that matters most for MTA, that one static feed maps to *many* RT feeds.
 */
describe('resolveRtUrlsForFeed', () => {
  const STATIC = { feeds: [{ onestop_id: 'f-dr5r-nyctsubway', urls: {} }] }

  /** Answers each request by what it queries; records the order asked. */
  function trackingFetch(responses: Record<string, unknown>) {
    const calls: string[] = []
    const fetchFn = (async (url: string) => {
      calls.push(url)
      // Longest match wins, so '&onestop_id=f-x~rt' beats '&onestop_id=f-x'.
      const key = Object.keys(responses)
        .filter(k => url.includes(k))
        .sort((a, b) => b.length - a.length)[0]
      return new Response(JSON.stringify(key ? responses[key] : { feeds: [], operators: [] }), { status: 200 })
    }) as unknown as typeof fetch
    return { calls, fetchFn }
  }

  const operatorWith = (...rtIds: string[]) => ({
    operators: [{
      onestop_id: 'o-dr5r-nyct',
      feeds: [
        { onestop_id: 'f-dr5r-nyctsubway', spec: 'GTFS' },
        ...rtIds.map(id => ({ onestop_id: id, spec: 'GTFS_RT' })),
      ],
    }],
  })

  test('collects every realtime feed the operator publishes', async () => {
    const { fetchFn } = trackingFetch({
      '&onestop_id=f-dr5r-nyctsubway&': STATIC,
      '/operators?': operatorWith('f-mta~nyc~rt~subway~a~c~e', 'f-mta~nyc~rt~alerts'),
      '&onestop_id=f-mta~nyc~rt~subway~a~c~e': {
        feeds: [{ urls: { realtime_trip_updates: 'https://mta.example/gtfs-ace' } }],
      },
      '&onestop_id=f-mta~nyc~rt~alerts': {
        feeds: [{ urls: { realtime_alerts: 'https://mta.example/all-alerts' } }],
      },
    })

    const urls = await resolveRtUrlsForFeed('f-dr5r-nyctsubway', 'key', fetchFn)

    // One static feed, many realtime feeds — the case id-derivation could not express.
    expect(urls.map(u => u.url).sort()).toEqual([
      'https://mta.example/all-alerts',
      'https://mta.example/gtfs-ace',
    ])
  })

  test('does not repeat a URL two operator feeds both list', async () => {
    const { fetchFn } = trackingFetch({
      '&onestop_id=f-dr5r-nyctsubway&': STATIC,
      '/operators?': operatorWith('f-a~rt', 'f-b~rt'),
      '&onestop_id=f-a~rt': { feeds: [{ urls: { realtime_alerts: 'https://mta.example/all-alerts' } }] },
      '&onestop_id=f-b~rt': { feeds: [{ urls: { realtime_alerts: 'https://mta.example/all-alerts' } }] },
    })

    const urls = await resolveRtUrlsForFeed('f-dr5r-nyctsubway', 'key', fetchFn)

    expect(urls).toHaveLength(1)
  })

  test('falls back to URLs on the feed\'s own record', async () => {
    const { fetchFn } = trackingFetch({
      '&onestop_id=f-solo&': {
        feeds: [{
          onestop_id: 'f-solo',
          urls: { realtime_alerts: 'https://solo.example/alerts.pb' },
        }],
      },
    })

    const urls = await resolveRtUrlsForFeed('f-solo', 'key', fetchFn)

    expect(urls.map(u => u.url)).toEqual(['https://solo.example/alerts.pb'])
  })

  test('falls back to the legacy ~rt companion record', async () => {
    const { fetchFn } = trackingFetch({
      '&onestop_id=f-legacy&': { feeds: [{ onestop_id: 'f-legacy', urls: {} }] },
      '&onestop_id=f-legacy~rt': {
        feeds: [{ urls: { realtime_vehicle_positions: 'https://legacy.example/vp.pb' } }],
      },
    })

    const urls = await resolveRtUrlsForFeed('f-legacy', 'key', fetchFn)

    expect(urls.map(u => u.url)).toEqual(['https://legacy.example/vp.pb'])
  })

  test('looks a numeric id up by id rather than as an onestop id', async () => {
    const { calls, fetchFn } = trackingFetch({ 'id=886': STATIC })

    await resolveRtUrlsForFeed('886', 'key', fetchFn)

    expect(calls[0]).toContain('&id=886')
  })

  test('carries header authorization onto each RT URL', async () => {
    const { fetchFn } = trackingFetch({
      '&onestop_id=f-solo&': {
        feeds: [{
          onestop_id: 'f-solo',
          urls: { realtime_alerts: 'https://solo.example/alerts.pb' },
          authorization: { type: 'header', param_name: 'x-api-key', param_value: 'secret' },
        }],
      },
    })

    const urls = await resolveRtUrlsForFeed('f-solo', 'key', fetchFn)

    expect(urls[0].headers).toEqual({ 'x-api-key': 'secret' })
  })

  test('bakes query-param authorization into the URL, which carries no headers', async () => {
    // MTA's Bus Time RT feed declares this form; a header-only reader drops it
    // silently and every request goes out unauthenticated.
    const { fetchFn } = trackingFetch({
      '&onestop_id=f-bus&': {
        feeds: [{
          onestop_id: 'f-bus',
          urls: { realtime_alerts: 'https://bus.example/alerts?x=1' },
          authorization: { type: 'query_param', param_name: 'key', param_value: 's3cret' },
        }],
      },
    })

    const urls = await resolveRtUrlsForFeed('f-bus', 'key', fetchFn)

    expect(urls[0].url).toBe('https://bus.example/alerts?x=1&key=s3cret')
    expect(urls[0].headers).toBeUndefined()
  })

  test('a feed with no realtime counterpart anywhere yields nothing', async () => {
    const { fetchFn } = trackingFetch({ '&onestop_id=f-nowhere&': { feeds: [{ onestop_id: 'f-nowhere', urls: {} }] } })

    expect(await resolveRtUrlsForFeed('f-nowhere', 'key', fetchFn)).toEqual([])
  })
})
