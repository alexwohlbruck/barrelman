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

export interface RouteDetailStop {
  stopId: string
  stopName: string
  lat: number
  lng: number
  /** Distance along the route shape in meters (for ordering). */
  distanceAlongRoute: number
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
  /** Stops ordered by position along the route. One direction only. */
  stops: RouteDetailStop[]
  /** Route shape as [lng, lat] pairs, or null if no shape available. */
  coordinates: [number, number][] | null
  /** Related route IDs that share the same color/trunk (e.g., 1/2/3 on the red line). */
  relatedRouteIds: string[]
}

/**
 * Get detailed info for a transit route including ordered stops and shape.
 */
export async function getRouteDetail(
  feedId: string,
  routeId: string,
): Promise<RouteDetailResponse | null> {
  const safeFeed = feedId.replace(/'/g, "''")
  const safeRoute = routeId.replace(/'/g, "''")

  // Get route metadata
  const routeResult = await db.execute(sql.raw(`
    SELECT route_id, route_short_name, route_long_name, route_color,
           route_text_color, route_type, agency_name, bikes_allowed
    FROM gtfs_routes
    WHERE feed_id = '${safeFeed}' AND route_id = '${safeRoute}'
    LIMIT 1
  `))

  let route = (routeResult as any[])[0]

  // Cross-feed fallback (same pattern as shapes)
  if (!route) {
    const fallback = await db.execute(sql.raw(`
      SELECT feed_id, route_id, route_short_name, route_long_name, route_color,
             route_text_color, route_type, agency_name, bikes_allowed
      FROM gtfs_routes
      WHERE route_id = '${safeRoute}'
      LIMIT 1
    `))
    route = (fallback as any[])[0]
    if (!route) return null
  }

  const actualFeedId = route.feed_id || feedId

  // Get stops for this route
  const stopsResult = await db.execute(sql.raw(`
    SELECT DISTINCT ON (s.stop_name)
      sr.stop_id, s.stop_name,
      ST_Y(s.geom::geometry) as lat,
      ST_X(s.geom::geometry) as lng
    FROM gtfs_stop_routes sr
    JOIN gtfs_stops s ON s.feed_id = sr.feed_id AND s.stop_id = sr.stop_id
    WHERE sr.feed_id = '${actualFeedId.replace(/'/g, "''")}'
      AND sr.route_id = '${safeRoute}'
      AND s.stop_name IS NOT NULL
    ORDER BY s.stop_name, sr.stop_id
  `))

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
      .map(s => ({ ...s, distanceAlongRoute: 0 }))
      .sort((a, b) => b.lat - a.lat)
  }

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
      return { ...stop, distanceAlongRoute: dist }
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

  const safeFeed = feedId.replace(/'/g, "''")
  const safeColor = routeColor.replace(/'/g, "''")

  const result = await db.execute(sql.raw(`
    SELECT route_id
    FROM gtfs_routes
    WHERE feed_id = '${safeFeed}'
      AND route_color = '${safeColor}'
      AND route_type = ${parseInt(String(routeType ?? 0), 10)}
      AND route_id != '${routeId.replace(/'/g, "''")}'
    ORDER BY route_id
  `))

  return (result as any[]).map(r => r.route_id as string)
}
