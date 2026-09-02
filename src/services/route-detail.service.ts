/**
 * Route Detail Service
 *
 * Returns all data needed to render a transit route detail view:
 * route metadata, ordered stops, and shape geometry.
 *
 * Stop ordering is derived by projecting stops onto the route shape
 * and sorting by distance along the polyline. This gives the
 * geographically correct order regardless of GTFS data quality.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { getRouteShape } from './shapes.service'

/** One other line a rider can reach at a stop on this route. */
export interface StopTransferRoute {
  routeId: string
  routeShortName: string | null
  routeLongName: string | null
  routeType: number | null
  routeColor: string | null
  routeTextColor: string | null
  agencyName: string | null
  /** `station` when it calls at this stop, `transfer` when transfers.txt
   *  reaches it from here without leaving the paid area. */
  via: 'station' | 'transfer'
}

export interface RouteDetailStop {
  stopId: string
  stopName: string
  lat: number
  lng: number
  /** Distance along the route shape in meters (for ordering). */
  distanceAlongRoute: number
  /** Lines other than this route available here — the bullets a stop row
   *  draws under its name. Empty where nothing else calls. */
  routes: StopTransferRoute[]
}

export interface RouteDetailResponse {
  feedId: string
  routeId: string
  routeShortName: string | null
  routeLongName: string | null
  routeColor: string | null
  routeTextColor: string | null
  routeType: number | null
  agencyName: string | null
  /** GTFS `bikes_allowed`: 0 unknown, 1 allowed, 2 not allowed. */
  bikesAllowed: number
  /** Stops ordered by position along the route. One direction only. */
  stops: RouteDetailStop[]
  /** Route shape as [lng, lat] pairs, or null if no shape available. */
  coordinates: [number, number][] | null
  /** Related route IDs that share the same color/trunk (e.g., 1/2/3 on the red line). */
  relatedRouteIds: string[]
}

/**
 * The other lines available at each of a route's stops, keyed by stop id.
 *
 * One query for the whole route rather than one per stop: a long subway line
 * is forty-odd stops, and forty round trips to render a list nobody has
 * scrolled to yet is the difference between a route panel that opens and one
 * that hangs.
 *
 * "Available here" is read the way a rider reads it, so it spans the transfer
 * complex as well as the station: at Brooklyn Bridge–City Hall the J and Z are
 * a walk through the passageway, and a stop row that omits them is describing
 * the feed's filing system rather than the interchange. They stay marked
 * `transfer` so the caller can draw them differently from the lines that
 * actually stop here.
 *
 * The route being viewed is excluded — its own bullet on every one of its own
 * stops is noise.
 */
async function routesAtStops(
  feedId: string,
  routeId: string,
  stopIds: string[],
): Promise<Map<string, StopTransferRoute[]>> {
  const byStop = new Map<string, StopTransferRoute[]>()
  if (!stopIds.length) return byStop

  const idList = sql.join(
    [...new Set(stopIds)].map((id) => sql`${id}`),
    sql`, `,
  )

  const rows = await db.execute(sql`
    WITH seed AS (
      SELECT s.stop_id AS origin,
             COALESCE(NULLIF(s.parent_station, ''), s.stop_id) AS station
      FROM gtfs_stops s
      WHERE s.feed_id = ${feedId} AND s.stop_id IN (${idList})
    ),
    -- the station itself, plus whatever transfers.txt joins it to, one hop
    complex AS (
      SELECT origin, station AS sid, TRUE AS at_station FROM seed
      UNION
      SELECT seed.origin, t.to_stop_id, FALSE
      FROM seed JOIN gtfs_transfers t
        ON t.feed_id = ${feedId} AND t.from_stop_id = seed.station
       AND t.to_stop_id <> t.from_stop_id
      UNION
      SELECT seed.origin, t.from_stop_id, FALSE
      FROM seed JOIN gtfs_transfers t
        ON t.feed_id = ${feedId} AND t.to_stop_id = seed.station
       AND t.to_stop_id <> t.from_stop_id
    ),
    -- platforms file under a parent, and stop_times references the platform
    members AS (
      SELECT origin, sid, at_station FROM complex
      UNION ALL
      SELECT c.origin, s.stop_id, c.at_station
      FROM complex c JOIN gtfs_stops s
        ON s.feed_id = ${feedId} AND s.parent_station = c.sid
    )
    SELECT
      m.origin,
      r.route_id, r.route_short_name, r.route_long_name, r.route_type,
      r.route_color, r.route_text_color, r.agency_name,
      bool_or(m.at_station) AS at_station
    FROM members m
    JOIN gtfs_stop_routes sr ON sr.feed_id = ${feedId} AND sr.stop_id = m.sid
    JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
    WHERE r.route_id <> ${routeId}
    GROUP BY
      m.origin, r.route_id, r.route_short_name, r.route_long_name,
      r.route_type, r.route_color, r.route_text_color, r.agency_name
    ORDER BY m.origin, bool_or(m.at_station) DESC, r.route_type, r.route_short_name
  `)

  for (const row of rows as any[]) {
    const list = byStop.get(row.origin) ?? []
    list.push({
      routeId: row.route_id,
      routeShortName: row.route_short_name ?? null,
      routeLongName: row.route_long_name ?? null,
      routeType: row.route_type != null ? Number(row.route_type) : null,
      routeColor: row.route_color ?? null,
      routeTextColor: row.route_text_color ?? null,
      agencyName: row.agency_name ?? null,
      via: row.at_station ? 'station' : 'transfer',
    })
    byStop.set(row.origin, list)
  }
  return byStop
}

/**
 * Get detailed info for a transit route including ordered stops and shape.
 */
export async function getRouteDetail(
  feedId: string,
  routeId: string,
): Promise<RouteDetailResponse | null> {
  // Get route metadata
  const routeResult = await db.execute(sql`
    SELECT route_id, route_short_name, route_long_name, route_color,
           route_text_color, route_type, agency_name, bikes_allowed
    FROM gtfs_routes
    WHERE feed_id = ${feedId} AND route_id = ${routeId}
    LIMIT 1
  `)

  let route = (routeResult as any[])[0]

  // Cross-feed fallback (same pattern as shapes)
  if (!route) {
    const fallback = await db.execute(sql`
      SELECT feed_id, route_id, route_short_name, route_long_name, route_color,
             route_text_color, route_type, agency_name, bikes_allowed
      FROM gtfs_routes
      WHERE route_id = ${routeId}
      LIMIT 1
    `)
    route = (fallback as any[])[0]
    if (!route) return null
  }

  const actualFeedId = route.feed_id || feedId

  // Get stops for this route
  const stopsResult = await db.execute(sql`
    SELECT DISTINCT ON (s.stop_name)
      sr.stop_id, s.stop_name,
      ST_Y(s.geom::geometry) as lat,
      ST_X(s.geom::geometry) as lng
    FROM gtfs_stop_routes sr
    JOIN gtfs_stops s ON s.feed_id = sr.feed_id AND s.stop_id = sr.stop_id
    WHERE sr.feed_id = ${actualFeedId}
      AND sr.route_id = ${routeId}
      AND s.stop_name IS NOT NULL
    ORDER BY s.stop_name, sr.stop_id
  `)

  const rawStops = (stopsResult as any[]).map(row => ({
    stopId: row.stop_id as string,
    stopName: row.stop_name as string,
    lat: parseFloat(row.lat),
    lng: parseFloat(row.lng),
  }))

  // Get shape
  const shape = await getRouteShape(feedId, routeId)
  const coordinates = shape?.coordinates ?? null

  // Order stops by projecting onto the shape
  let orderedStops: RouteDetailStop[]
  if (coordinates && coordinates.length >= 2) {
    orderedStops = orderStopsByShape(rawStops, coordinates)
  } else {
    // Fallback: order by latitude (north to south for most transit)
    orderedStops = rawStops
      .map(s => ({ ...s, distanceAlongRoute: 0, routes: [] as StopTransferRoute[] }))
      .sort((a, b) => b.lat - a.lat)
  }

  // Bullets per stop, resolved once the order is settled so the map lookup
  // and the rendered list agree on which stops exist.
  const stopRoutes = await routesAtStops(
    actualFeedId,
    routeId,
    orderedStops.map((s) => s.stopId),
  )
  for (const stop of orderedStops) stop.routes = stopRoutes.get(stop.stopId) ?? []

  // Find related routes (same color = same trunk line, e.g., 1/2/3)
  const relatedRouteIds = await findRelatedRoutes(
    actualFeedId,
    routeId,
    route.route_color,
    route.route_type,
  )

  return {
    feedId: actualFeedId,
    routeId,
    routeShortName: route.route_short_name || null,
    routeLongName: route.route_long_name || null,
    routeColor: route.route_color || null,
    routeTextColor: route.route_text_color || null,
    routeType: route.route_type != null ? parseInt(route.route_type, 10) : null,
    agencyName: route.agency_name || null,
    bikesAllowed: route.bikes_allowed ? parseInt(route.bikes_allowed, 10) : 0,
    stops: orderedStops,
    coordinates,
    relatedRouteIds,
  }
}

/**
 * Order stops by projecting them onto the route shape polyline.
 */
function orderStopsByShape(
  stops: Array<{ stopId: string; stopName: string; lat: number; lng: number }>,
  coordinates: [number, number][],
): RouteDetailStop[] {
  // Build cumulative distances along the polyline
  const cumDist = [0]
  for (let i = 1; i < coordinates.length; i++) {
    const [lng1, lat1] = coordinates[i - 1]
    const [lng2, lat2] = coordinates[i]
    cumDist.push(cumDist[i - 1] + haversine(lat1, lng1, lat2, lng2))
  }

  return stops
    .map(stop => {
      const dist = projectOntoPolyline(stop.lat, stop.lng, coordinates, cumDist)
      return { ...stop, distanceAlongRoute: dist, routes: [] as StopTransferRoute[] }
    })
    .sort((a, b) => a.distanceAlongRoute - b.distanceAlongRoute)
}

/**
 * Project a point onto the nearest segment of a polyline.
 * Returns the distance along the polyline to the projection point.
 */
function projectOntoPolyline(
  lat: number,
  lng: number,
  coordinates: [number, number][],
  cumDist: number[],
): number {
  let bestDist = Infinity
  let bestAlongDist = 0

  for (let i = 0; i < coordinates.length - 1; i++) {
    const [aLng, aLat] = coordinates[i]
    const [bLng, bLat] = coordinates[i + 1]

    // Project onto segment
    const dx = bLng - aLng
    const dy = bLat - aLat
    const lenSq = dx * dx + dy * dy
    let t = lenSq === 0 ? 0 : Math.max(0, Math.min(1, ((lng - aLng) * dx + (lat - aLat) * dy) / lenSq))

    const projLat = aLat + dy * t
    const projLng = aLng + dx * t
    const d = haversine(lat, lng, projLat, projLng)

    if (d < bestDist) {
      bestDist = d
      const segLen = cumDist[i + 1] - cumDist[i]
      bestAlongDist = cumDist[i] + segLen * t
    }
  }

  return bestAlongDist
}

function haversine(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6_371_000
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(lat2 - lat1)
  const dLng = toRad(lng2 - lng1)
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

/**
 * Find routes that share the same trunk line (same color + type).
 * E.g., NYC subway 1/2/3 all share red color = same trunk.
 */
async function findRelatedRoutes(
  feedId: string,
  routeId: string,
  routeColor: string | null,
  routeType: string | number | null,
): Promise<string[]> {
  if (!routeColor) return []

  const result = await db.execute(sql`
    SELECT route_id
    FROM gtfs_routes
    WHERE feed_id = ${feedId}
      AND route_color = ${routeColor}
      AND route_type = ${parseInt(String(routeType ?? 0), 10)}
      AND route_id != ${routeId}
    ORDER BY route_id
  `)

  return (result as any[]).map(r => r.route_id as string)
}
