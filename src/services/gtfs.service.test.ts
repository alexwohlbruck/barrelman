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
  deriveRouteShapes,
  deriveBikesAllowed,
  generateTransfersTxt,
  fetchFeedList,
  sanitizeGtfsZip,
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
    if (urlStr.includes('spec=GTFS_RT') && urlStr.includes('onestop_id=')) {
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
