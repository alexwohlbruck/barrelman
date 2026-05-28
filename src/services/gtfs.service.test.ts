/**
 * Tests for the GTFS import service.
 *
 * Validates:
 *   - GTFS CSV parsing (stops, routes, agencies, stop-route derivation)
 *   - transfers.txt generation format
 *   - Edge cases in CSV parsing (missing fields, Unicode, special chars)
 */

import { describe, test, expect } from 'bun:test'
import {
  parseStops,
  parseRoutes,
  parseAgencies,
  deriveStopRoutes,
  generateTransfersTxt,
} from './gtfs.service'

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
