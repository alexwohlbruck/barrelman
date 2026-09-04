/**
 * Pure logic for the transit layers of /search: folding queries and route
 * names for comparison, mapping GTFS route_types to modes, and reconciling
 * GTFS hits against the OSM rows in the same result set.
 *
 * Kept free of database imports so it can be unit-tested directly; the SQL
 * lives in services/transit-search.service.ts.
 */

import { normalizeStationName, sameStationName } from './station-name'

/**
 * Words a rider appends to a line's name without them being part of it:
 * "7 train", "green line", "route 9". Stripped before comparing the query
 * against route_short_name, where an exact match is the strongest signal.
 */
const MODE_WORDS = new Set([
  'train', 'trains', 'line', 'lines', 'bus', 'buses', 'route', 'routes',
  'tram', 'streetcar', 'trolley', 'ferry', 'subway', 'metro', 'rail',
])

/**
 * The query with mode words removed and case/punctuation folded — the form
 * compared against route_short_name. Returns '' when nothing but mode words
 * remain ("the train"), which callers must treat as "no short-name match".
 */
export function transitCoreQuery(query: string): string {
  const words = query
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter((w) => w && !MODE_WORDS.has(w))
  return words.join(' ')
}

/**
 * Comparable form of a route name. Reuses the station-name fold: the synonym
 * table only helps (route long names are usually street or destination names,
 * exactly what it was built for) and both sides pass through the same fold, so
 * an entry can only create a match, never hide one.
 */
export function foldRouteName(name: string | null | undefined): string {
  return normalizeStationName(name)
}

/**
 * GTFS route_type → the mode vocabulary portolan and the console already use.
 * Covers both the basic 0-12 set and the extended European ranges.
 */
export function routeTypeMode(routeType: number): string {
  const basic: Record<number, string> = {
    0: 'tram', 1: 'subway', 2: 'rail', 3: 'bus', 4: 'ferry', 5: 'cable_car',
    6: 'gondola', 7: 'funicular', 11: 'trolleybus', 12: 'monorail',
  }
  if (basic[routeType]) return basic[routeType]
  if (routeType >= 100 && routeType < 200) return 'rail'
  if (routeType >= 200 && routeType < 300) return 'bus'
  if (routeType >= 400 && routeType < 500) return 'subway'
  if (routeType >= 700 && routeType < 800) return 'bus'
  if (routeType === 800) return 'trolleybus'
  if (routeType >= 900 && routeType < 1000) return 'tram'
  if (routeType >= 1000 && routeType < 1100) return 'ferry'
  if (routeType >= 1300 && routeType < 1400) return 'gondola'
  if (routeType === 1400) return 'funicular'
  return 'transit'
}

/** OSM route= values that describe passenger transit lines. */
const OSM_TRANSIT_ROUTE_VALUES: Record<string, string> = {
  bus: 'bus', trolleybus: 'trolleybus', minibus: 'bus', share_taxi: 'bus',
  train: 'rail', light_rail: 'tram', subway: 'subway', tram: 'tram',
  monorail: 'monorail', ferry: 'ferry', funicular: 'funicular',
}

/**
 * Coarse mode families for dedupe. Rail-ish modes are grouped because the
 * tram/light_rail/subway boundary is drawn differently by OSM and GTFS for
 * the same line; bus-ish likewise. A name match across families (bus "1" vs
 * subway "1") is a genuine coincidence and must not dedupe.
 */
function modeGroup(mode: string): string {
  if (['rail', 'tram', 'subway', 'monorail', 'light_rail'].includes(mode)) return 'rail'
  if (['bus', 'trolleybus'].includes(mode)) return 'bus'
  if (mode === 'ferry') return 'water'
  return mode
}

/** railway= values that mark a way as track of a given mode. */
const OSM_RAIL_INFRASTRUCTURE: Record<string, string> = {
  rail: 'rail', subway: 'subway', tram: 'tram', light_rail: 'tram',
  monorail: 'monorail', narrow_gauge: 'rail', funicular: 'funicular',
}

/**
 * Whether a geo_places row *is* a transit line in OSM's eyes — a route
 * relation, or a track segment named after its line (every mapped segment of
 * the Hempstead Branch carries the name "Hempstead Branch") — and its mode.
 */
export function osmTransitRouteMode(row: any): string | null {
  const tags = row?.tags
  if (!tags) return null
  if (tags.type === 'route') return OSM_TRANSIT_ROUTE_VALUES[tags.route] ?? null
  return OSM_RAIL_INFRASTRUCTURE[tags.railway] ?? null
}

/** Category prefixes that mark a geo_places row as a transit stop/station. */
const OSM_STOP_CATEGORIES = new Set([
  'public_transport/station', 'public_transport/platform', 'public_transport/stop_position',
  'railway/station', 'railway/halt', 'railway/tram_stop',
  'highway/bus_stop', 'amenity/bus_station', 'amenity/ferry_terminal',
])

function isOsmTransitStop(row: any): boolean {
  return Array.isArray(row?.categories) && row.categories.some((c: string) => OSM_STOP_CATEGORIES.has(c))
}

/** Great-circle distance in metres between two GeoJSON Point geometries. */
function pointDistanceM(a: any, b: any): number | null {
  const ca = a?.coordinates
  const cb = b?.coordinates
  if (!Array.isArray(ca) || !Array.isArray(cb)) return null
  const rad = Math.PI / 180
  const dLat = (cb[1] - ca[1]) * rad
  const dLng = (cb[0] - ca[0]) * rad
  const h = Math.sin(dLat / 2) ** 2
    + Math.cos(ca[1] * rad) * Math.cos(cb[1] * rad) * Math.sin(dLng / 2) ** 2
  return 2 * 6371000 * Math.asin(Math.sqrt(h))
}

/** An OSM line and a GTFS line with the same folded name within this distance
 *  of each other (centroid to centroid) are the same line. Wide, because the
 *  two centroids are means over different stop sets of a shape that can span
 *  a metro area. */
const ROUTE_DEDUPE_DISTANCE_M = 100_000

/** A GTFS stop and an OSM stop with the same folded name within this distance
 *  are the same stop — matches the station-linking matview's radius. */
const STOP_DEDUPE_DISTANCE_M = 250

/**
 * Drop the duplicates a merged result set can contain once GTFS hits ride
 * alongside geo_places hits:
 *
 *  - An OSM `type=route` relation duplicated by a GTFS route hit loses — the
 *    GTFS row carries the ids, colours and mode a client needs to open the
 *    line, the OSM relation opens nothing.
 *  - A GTFS stop duplicated by an OSM stop/station loses — the OSM place has
 *    the richer detail page. The portolan link table already excludes matched
 *    stops in SQL; this catches stops portolan hasn't indexed.
 *  - A GTFS stop duplicated by another GTFS stop (two feeds serving one
 *    station) — first one wins.
 *  - A GTFS line duplicated by another GTFS line (the MTA files all 307 bus
 *    routes in each of five borough feeds) — first one wins.
 */
export function reconcileTransitHits(results: any[]): any[] {
  const routeHits = results.filter((r) => r.kind === 'transit_route')
  const osmStops = results.filter((r) => !r.kind && isOsmTransitStop(r))

  const seenStops: any[] = []
  const seenRoutes: any[] = []
  return results.filter((row) => {
    if (row.kind === 'transit_route') {
      const duplicates = (other: any) =>
        modeGroup(other.transit.mode) === modeGroup(row.transit.mode) &&
        foldRouteName(other.transit.shortName) === foldRouteName(row.transit.shortName) &&
        foldRouteName(other.transit.longName) === foldRouteName(row.transit.longName) &&
        (pointDistanceM(row.geometry, other.geometry) ?? 0) <= ROUTE_DEDUPE_DISTANCE_M
      if (seenRoutes.some(duplicates)) return false
      seenRoutes.push(row)
      return true
    }
    return keepAgainstTransit(row, routeHits, osmStops, seenStops)
  })
}

/** Whether a non-transit row survives beside the transit hits — see
 *  reconcileTransitHits, which owns the loop. */
function keepAgainstTransit(row: any, routeHits: any[], osmStops: any[], seenStops: any[]): boolean {
  if (!row.kind) {
    const mode = osmTransitRouteMode(row)
    if (!mode) return true
    const group = modeGroup(mode)
    const refFold = foldRouteName(row.tags?.ref)
    const nameFold = foldRouteName(row.name)
    // Equal folds are the same line; an OSM name may also merely CONTAIN
    // the GTFS name ("NJ Transit Raritan Valley Line: Newark <=> High
    // Bridge" ⊇ "Raritan Valley Line") — one direction only, and only for
    // names long enough that containment can't be a coincidence.
    const matches = (osmFold: string, gtfsFold: string) =>
      !!osmFold && !!gtfsFold &&
      (osmFold === gtfsFold ||
        (gtfsFold.length >= 8 && osmFold.includes(gtfsFold)))
    return !routeHits.some((hit) => {
      if (modeGroup(hit.transit.mode) !== group) return false
      const folds = [foldRouteName(hit.transit.shortName), foldRouteName(hit.transit.longName)]
        .filter(Boolean)
      if (!folds.some((f) => matches(refFold, f) || matches(nameFold, f))) return false
      const d = pointDistanceM(row.geometry, hit.geometry)
      return d == null || d <= ROUTE_DEDUPE_DISTANCE_M
    })
  }

  if (row.kind !== 'transit_stop') return true

  const duplicates = (other: any) => {
    if (!sameStationName(row.name, other.name)) return false
    const d = pointDistanceM(row.geometry, other.geometry)
    return d != null && d <= STOP_DEDUPE_DISTANCE_M
  }
  if (osmStops.some(duplicates) || seenStops.some(duplicates)) return false
  seenStops.push(row)
  return true
}
