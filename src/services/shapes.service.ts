/**
 * Shapes Service
 *
 * Serves GTFS route shape geometry for transit vehicle route-snapping.
 * Shape data is imported from shapes.txt during the GTFS import pipeline
 * and stored as coordinate arrays in the gtfs_shapes table.
 *
 * The service resolves route_id → shape_id via the gtfs_routes table's
 * shape_id column, then fetches the coordinate array from gtfs_shapes.
 * Results are cached with a long TTL since shapes are static GTFS data.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'

// ── Types ───────────────────────────────────────────────────────────

export interface ShapeResponse {
  feedId: string
  routeId: string
  shapeId: string
  coordinates: [number, number][] // [lng, lat] pairs
}

// ── Cache ───────────────────────────────────────────────────────────

/** Cache shapes for 1 hour — they're static GTFS data. */
const shapeCache = new LRUCache<string, ShapeResponse | null>({
  max: 500,
  ttl: 3_600_000, // 1 hour
})

// ── Main export ─────────────────────────────────────────────────────

/**
 * Get shape coordinates for a specific route.
 *
 * Resolves the route's canonical shape_id and returns the ordered
 * coordinate array for rendering on a map.
 */
export async function getRouteShape(
  feedId: string,
  routeId: string,
): Promise<ShapeResponse | null> {
  const cacheKey = `${feedId}_${routeId}`
  const cached = shapeCache.get(cacheKey)
  if (cached !== undefined) return cached

  try {
    // Look up the canonical shape_id from gtfs_routes.
    // First try the exact feed, then fall back to any feed that has
    // this route — needed when a unified RT feed (e.g. MTA Bus)
    // returns vehicles tagged with one feed_id but shapes live
    // under per-borough feed_ids.
    const safeFeed = feedId.replace(/'/g, "''")
    const safeRoute = routeId.replace(/'/g, "''")

    let routeResult = await db.execute(sql.raw(`
      SELECT feed_id, shape_id
      FROM gtfs_routes
      WHERE feed_id = '${safeFeed}'
        AND route_id = '${safeRoute}'
        AND shape_id IS NOT NULL AND shape_id != ''
      LIMIT 1
    `))

    let route = (routeResult as any[])[0]

    // Cross-feed fallback
    if (!route?.shape_id) {
      routeResult = await db.execute(sql.raw(`
        SELECT feed_id, shape_id
        FROM gtfs_routes
        WHERE route_id = '${safeRoute}'
          AND shape_id IS NOT NULL AND shape_id != ''
        LIMIT 1
      `))
      route = (routeResult as any[])[0]
    }

    if (!route?.shape_id) {
      return null
    }

    const shapeFeedId = route.feed_id || feedId

    // Fetch the shape coordinates
    const shapeResult = await db.execute(sql.raw(`
      SELECT coordinates
      FROM gtfs_shapes
      WHERE feed_id = '${shapeFeedId.replace(/'/g, "''")}'
        AND shape_id = '${route.shape_id.replace(/'/g, "''")}'
      LIMIT 1
    `))

    const shape = (shapeResult as any[])[0]
    if (!shape?.coordinates) {
      return null
    }

    const coordinates: [number, number][] =
      typeof shape.coordinates === 'string'
        ? JSON.parse(shape.coordinates)
        : shape.coordinates

    const response: ShapeResponse = {
      feedId,
      routeId,
      shapeId: route.shape_id,
      coordinates,
    }

    shapeCache.set(cacheKey, response)
    return response
  } catch (err) {
    console.error(`[Shapes] Failed to fetch shape for ${feedId}/${routeId}:`, err)
    shapeCache.set(cacheKey, null)
    return null
  }
}

/**
 * Get shape coordinates by shape_id directly (for cases where the
 * caller already knows the shape_id, e.g. from trip data).
 */
export async function getShapeById(
  feedId: string,
  shapeId: string,
): Promise<[number, number][] | null> {
  const cacheKey = `shape_${feedId}_${shapeId}`
  const cached = shapeCache.get(cacheKey)
  if (cached !== undefined) return cached?.coordinates ?? null

  try {
    const result = await db.execute(sql.raw(`
      SELECT coordinates
      FROM gtfs_shapes
      WHERE feed_id = '${feedId.replace(/'/g, "''")}'
        AND shape_id = '${shapeId.replace(/'/g, "''")}'
      LIMIT 1
    `))

    const row = (result as any[])[0]
    if (!row?.coordinates) return null

    const coordinates: [number, number][] =
      typeof row.coordinates === 'string'
        ? JSON.parse(row.coordinates)
        : row.coordinates

    return coordinates
  } catch (err) {
    console.error(`[Shapes] Failed to fetch shape ${feedId}/${shapeId}:`, err)
    return null
  }
}
