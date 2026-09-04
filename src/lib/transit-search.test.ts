import { describe, test, expect } from 'bun:test'
import {
  transitCoreQuery,
  routeTypeMode,
  foldRouteName,
  osmTransitRouteMode,
  reconcileTransitHits,
} from './transit-search'

describe('transitCoreQuery', () => {
  test('strips mode words a rider appends to a line name', () => {
    expect(transitCoreQuery('7 train')).toBe('7')
    expect(transitCoreQuery('green line')).toBe('green')
    expect(transitCoreQuery('Route 9')).toBe('9')
  })

  test('returns empty when nothing but mode words remain', () => {
    expect(transitCoreQuery('the train')).toBe('the')
    expect(transitCoreQuery('train')).toBe('')
  })

  test('folds case and punctuation', () => {
    expect(transitCoreQuery('B-Line')).toBe('b')
  })
})

describe('routeTypeMode', () => {
  test('maps the basic GTFS set', () => {
    expect(routeTypeMode(0)).toBe('tram')
    expect(routeTypeMode(1)).toBe('subway')
    expect(routeTypeMode(2)).toBe('rail')
    expect(routeTypeMode(3)).toBe('bus')
    expect(routeTypeMode(4)).toBe('ferry')
  })

  test('maps the extended European ranges', () => {
    expect(routeTypeMode(109)).toBe('rail')
    expect(routeTypeMode(700)).toBe('bus')
    expect(routeTypeMode(900)).toBe('tram')
  })

  test('falls back to transit for unknown codes', () => {
    expect(routeTypeMode(1700)).toBe('transit')
  })
})

describe('osmTransitRouteMode', () => {
  test('recognizes a transit route relation', () => {
    expect(osmTransitRouteMode({ tags: { type: 'route', route: 'subway' } })).toBe('subway')
    expect(osmTransitRouteMode({ tags: { type: 'route', route: 'bus' } })).toBe('bus')
  })

  test('ignores non-transit routes and non-routes', () => {
    expect(osmTransitRouteMode({ tags: { type: 'route', route: 'bicycle' } })).toBeNull()
    expect(osmTransitRouteMode({ tags: { amenity: 'cafe' } })).toBeNull()
    expect(osmTransitRouteMode({})).toBeNull()
  })
})

// ── reconcileTransitHits ─────────────────────────────────────────────────────

const gtfsRoute = (overrides: any = {}) => ({
  id: 'transit-route/mta:A', kind: 'transit_route', name: 'Eighth Avenue Express',
  geometry: { type: 'Point', coordinates: [-73.98, 40.75] },
  transit: { feedId: 'mta', routeId: 'A', shortName: 'A', longName: 'Eighth Avenue Express', mode: 'subway' },
  ...overrides,
})

const osmRouteRelation = (overrides: any = {}) => ({
  id: 'relation/9', osm_type: 'R', name: 'Eighth Avenue Express',
  tags: { type: 'route', route: 'subway', ref: 'A' },
  categories: [],
  geometry: { type: 'Point', coordinates: [-73.97, 40.74] },
  ...overrides,
})

describe('reconcileTransitHits — routes', () => {
  test('drops an OSM route relation duplicated by a GTFS line hit', () => {
    const out = reconcileTransitHits([gtfsRoute(), osmRouteRelation()])
    expect(out.map((r) => r.id)).toEqual(['transit-route/mta:A'])
  })

  test('matches on ref when the names differ', () => {
    const osm = osmRouteRelation({ name: 'A: Inwood → Far Rockaway' })
    const out = reconcileTransitHits([gtfsRoute(), osm])
    expect(out.map((r) => r.id)).toEqual(['transit-route/mta:A'])
  })

  test('keeps an OSM route in a different mode family — bus 1 is not subway 1', () => {
    const osm = osmRouteRelation({ tags: { type: 'route', route: 'bus', ref: 'A' } })
    const out = reconcileTransitHits([gtfsRoute(), osm])
    expect(out).toHaveLength(2)
  })

  test('keeps a same-name OSM route in another city', () => {
    const osm = osmRouteRelation({ geometry: { type: 'Point', coordinates: [-118.24, 34.05] } })
    const out = reconcileTransitHits([gtfsRoute(), osm])
    expect(out).toHaveLength(2)
  })

  test('rail-family blur: an OSM light_rail dupes a GTFS tram line', () => {
    const hit = gtfsRoute({ transit: { ...gtfsRoute().transit, mode: 'tram' } })
    const osm = osmRouteRelation({ tags: { type: 'route', route: 'light_rail', ref: 'A' } })
    expect(reconcileTransitHits([hit, osm])).toHaveLength(1)
  })

  test('leaves ordinary places alone', () => {
    const cafe = { id: 'node/1', name: 'A Cafe', tags: { amenity: 'cafe' }, categories: ['amenity/cafe'] }
    expect(reconcileTransitHits([gtfsRoute(), cafe])).toHaveLength(2)
  })

  test('drops track segments named after a returned line', () => {
    const line = gtfsRoute({
      transit: { ...gtfsRoute().transit, mode: 'rail', shortName: null, longName: 'Hempstead Branch' },
      name: 'Hempstead Branch',
    })
    const segments = [1, 2, 3].map((i) => ({
      id: `way/${i}`, osm_type: 'W', name: 'Hempstead Branch',
      tags: { railway: 'rail' }, categories: ['railway/rail'],
      geometry: { type: 'Point', coordinates: [-73.97, 40.74] },
    }))
    const out = reconcileTransitHits([line, ...segments])
    expect(out.map((r) => r.id)).toEqual(['transit-route/mta:A'])
  })

  test('an OSM relation whose name contains the GTFS name is the same line', () => {
    const line = gtfsRoute({
      transit: { ...gtfsRoute().transit, mode: 'rail', longName: 'Raritan Valley Line' },
    })
    const relation = osmRouteRelation({
      name: 'NJ Transit Raritan Valley Line: Newark <=> High Bridge',
      tags: { type: 'route', route: 'train' },
    })
    expect(reconcileTransitHits([line, relation])).toHaveLength(1)
  })

  test('containment never applies to short names — bus 7 keeps its road', () => {
    const hit = gtfsRoute({ transit: { ...gtfsRoute().transit, mode: 'rail', shortName: '7', longName: null } })
    const track = {
      id: 'way/9', name: 'Line 7 Industrial Railway Spur',
      tags: { railway: 'rail' }, categories: ['railway/rail'],
      geometry: { type: 'Point', coordinates: [-73.97, 40.74] },
    }
    expect(reconcileTransitHits([hit, track])).toHaveLength(2)
  })

  test('the same line filed in several feeds appears once', () => {
    // The MTA carries all 307 bus routes in each borough's feed.
    const a = gtfsRoute({
      id: 'transit-route/6:M60', transit: { ...gtfsRoute().transit, mode: 'bus', shortName: 'M60-SBS', longName: 'West Side - LaGuardia Airport' },
    })
    const b = gtfsRoute({
      id: 'transit-route/7:M60', transit: { ...gtfsRoute().transit, mode: 'bus', shortName: 'M60-SBS', longName: 'West Side - LaGuardia Airport' },
    })
    expect(reconcileTransitHits([a, b]).map((r) => r.id)).toEqual(['transit-route/6:M60'])
  })

  test('same short name with different long names is two lines', () => {
    const nyc = gtfsRoute()
    const vt = gtfsRoute({
      id: 'transit-route/vt:7',
      transit: { ...gtfsRoute().transit, longName: 'North-South Vermont Bus Route' },
    })
    expect(reconcileTransitHits([nyc, vt])).toHaveLength(2)
  })

  test('a station is never mistaken for track', () => {
    const station = { id: 'node/2', name: 'Hempstead', tags: { railway: 'station' }, categories: ['railway/station'] }
    const line = gtfsRoute({
      transit: { ...gtfsRoute().transit, mode: 'rail', longName: 'Hempstead Branch' },
      name: 'Hempstead Branch',
    })
    expect(reconcileTransitHits([line, station])).toHaveLength(2)
  })
})

const gtfsStop = (overrides: any = {}) => ({
  id: 'transit-stop/mta:s1', kind: 'transit_stop', name: 'Grand Central-42 St',
  geometry: { type: 'Point', coordinates: [-73.9772, 40.7527] },
  transit: { feedId: 'mta', stopId: 's1' },
  ...overrides,
})

const osmStation = (overrides: any = {}) => ({
  id: 'node/5', osm_type: 'N', name: 'Grand Central–42nd Street',
  categories: ['railway/station'], tags: {},
  geometry: { type: 'Point', coordinates: [-73.9770, 40.7528] },
  ...overrides,
})

describe('reconcileTransitHits — stops', () => {
  test('drops a GTFS stop duplicated by a same-name OSM station nearby', () => {
    const out = reconcileTransitHits([osmStation(), gtfsStop()])
    expect(out.map((r) => r.id)).toEqual(['node/5'])
  })

  test('keeps a GTFS stop whose name matches but sits far away', () => {
    const far = gtfsStop({ geometry: { type: 'Point', coordinates: [-73.9, 40.7] } })
    expect(reconcileTransitHits([osmStation(), far])).toHaveLength(2)
  })

  test('keeps a nearby GTFS stop with a different name', () => {
    const other = gtfsStop({ name: 'Bryant Park' })
    expect(reconcileTransitHits([osmStation(), other])).toHaveLength(2)
  })

  test('collapses the same station appearing from two feeds', () => {
    const a = gtfsStop()
    const b = gtfsStop({ id: 'transit-stop/other:77', name: 'Grand Central-42nd St' })
    const out = reconcileTransitHits([a, b])
    expect(out.map((r) => r.id)).toEqual(['transit-stop/mta:s1'])
  })

  test('an OSM place that is not a stop never dedupes a GTFS stop', () => {
    const hotel = osmStation({ categories: ['tourism/hotel'] })
    expect(reconcileTransitHits([hotel, gtfsStop({ name: hotel.name })])).toHaveLength(2)
  })
})
