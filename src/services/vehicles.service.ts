/**
 * Vehicle Positions Service
 *
 * Fetches and serves GTFS-RT VehiclePosition data from transit feeds.
 * RT feed URLs are stored in the gtfs_feeds table (discovered during import).
 * Enriches positions with route colors and short names from gtfs_routes.
 *
 * Positions are cached briefly (10s) to avoid hammering upstream feeds
 * on rapid client polls.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import { LRUCache } from 'lru-cache'

const { transit_realtime } = GtfsRealtimeBindings

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

// ── Types ───────────────────────────────────────────────────────────

export interface VehiclePositionsRequest {
  north: number
  south: number
  east: number
  west: number
  /** Optional: filter to specific feed */
  feedId?: string
  /** Optional: filter to specific route */
  routeId?: string
}

export interface TransitVehicle {
  vehicleId: string
  tripId?: string
  routeId?: string
  feedId: string
  position: { lat: number; lng: number }
  bearing?: number
  speed?: number
  timestamp: string
  routeColor?: string
  routeTextColor?: string
  routeShortName?: string
  routeType?: number
}

export interface VehiclePositionsResponse {
  vehicles: TransitVehicle[]
  feedTimestamps: Record<string, string>
}

// ── Cache ───────────────────────────────────────────────────────────

interface CachedFeed {
  vehicles: TransitVehicle[]
  feedTimestamp: string
  fetchedAt: number
}

/** Per-feed cache: avoids re-fetching the same protobuf within 10s. */
const feedCache = new LRUCache<string, CachedFeed>({
  max: 200,
  ttl: 10_000, // 10 seconds
})

/** Convert a protobuf timestamp (Long or number) to seconds. */
function toSeconds(ts: any): number {
  if (typeof ts === 'number') return ts
  if (ts && typeof ts.toNumber === 'function') return ts.toNumber()
  if (ts && typeof ts.low === 'number') return ts.low + (ts.high || 0) * 0x100000000
  return NaN
}

// ── Route info cache ────────────────────────────────────────────────

interface RouteInfo {
  shortName?: string
  color?: string
  textColor?: string
  type?: number
}

const routeInfoCache = new LRUCache<string, RouteInfo>({
  max: 5000,
  ttl: 300_000, // 5 minutes — route metadata is semi-static
})

// ── Main export ─────────────────────────────────────────────────────

/**
 * Fetch vehicle positions from all feeds with RT URLs, filtered to a
 * bounding box. Returns enriched positions with route metadata.
 */
export async function getVehiclePositions(
  request: VehiclePositionsRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<VehiclePositionsResponse> {
  const { north, south, east, west, feedId, routeId } = request

  // Get feeds with vehicle position RT URLs
  const feeds = await getFeedsWithVehiclePositions(feedId)
  if (feeds.length === 0) {
    return { vehicles: [], feedTimestamps: {} }
  }

  // Fetch all feeds in parallel
  const results = await Promise.allSettled(
    feeds.map(feed => fetchFeedVehicles(feed, fetchFn)),
  )

  // Merge results and filter to bounding box
  const allVehicles: TransitVehicle[] = []
  const feedTimestamps: Record<string, string> = {}

  for (const result of results) {
    if (result.status !== 'fulfilled' || !result.value) continue
    const { vehicles, feedTimestamp, feedId: fId } = result.value

    feedTimestamps[fId] = feedTimestamp

    for (const vehicle of vehicles) {
      // Bounding box filter
      const { lat, lng } = vehicle.position
      if (lat < south || lat > north || lng < west || lng > east) continue

      // Route filter
      if (routeId && vehicle.routeId !== routeId) continue

      allVehicles.push(vehicle)
    }
  }

  // Enrich with route metadata (colors, short names)
  await enrichWithRouteInfo(allVehicles)

  return { vehicles: allVehicles, feedTimestamps }
}

// ── Feed discovery ──────────────────────────────────────────────────

interface FeedRtInfo {
  feedId: string
  vehiclePositionUrl: string
  headers?: Record<string, string>
}

/**
 * Query gtfs_feeds for feeds that have vehicle position RT URLs.
 */
async function getFeedsWithVehiclePositions(
  feedIdFilter?: string,
): Promise<FeedRtInfo[]> {
  const whereClause = feedIdFilter
    ? `WHERE feed_id = '${feedIdFilter.replace(/'/g, "''")}'`
    : ''

  const result = await db.execute(sql.raw(`
    SELECT feed_id, rt_urls
    FROM gtfs_feeds
    WHERE rt_urls IS NOT NULL
    ${feedIdFilter ? `AND feed_id = '${feedIdFilter.replace(/'/g, "''")}'` : ''}
  `))

  const feeds: FeedRtInfo[] = []

  for (const row of result as any[]) {
    const rtUrls: Array<{ url: string; headers?: Record<string, string> }> =
      typeof row.rt_urls === 'string' ? JSON.parse(row.rt_urls) : row.rt_urls

    if (!Array.isArray(rtUrls)) continue

    // Find the vehicle positions URL. Convention: URL contains
    // "vehicleposition" or "vehicle_position" or "VehiclePosition"
    const vpUrl = rtUrls.find(u =>
      /vehicle.?position/i.test(u.url),
    )

    if (vpUrl) {
      feeds.push({
        feedId: row.feed_id,
        vehiclePositionUrl: vpUrl.url,
        headers: vpUrl.headers,
      })
    }
  }

  return feeds
}

// ── Protobuf fetch and parse ────────────────────────────────────────

/**
 * Fetch and decode a single feed's vehicle positions protobuf.
 * Uses per-feed caching to avoid redundant fetches.
 */
async function fetchFeedVehicles(
  feed: FeedRtInfo,
  fetchFn: FetchFn,
): Promise<{ vehicles: TransitVehicle[]; feedTimestamp: string; feedId: string } | null> {
  const cacheKey = feed.feedId

  // Check cache first
  const cached = feedCache.get(cacheKey)
  if (cached) {
    return {
      vehicles: cached.vehicles,
      feedTimestamp: cached.feedTimestamp,
      feedId: feed.feedId,
    }
  }

  try {
    const headers: Record<string, string> = {
      'Accept': 'application/x-protobuf,application/octet-stream',
    }
    if (feed.headers) {
      Object.assign(headers, feed.headers)
    }

    const response = await fetchFn(feed.vehiclePositionUrl, {
      headers,
      signal: AbortSignal.timeout(8000),
    })

    if (!response.ok) {
      console.warn(
        `[Vehicles] Feed ${feed.feedId} returned ${response.status}`,
      )
      return null
    }

    const buffer = await response.arrayBuffer()
    const feedMessage = transit_realtime.FeedMessage.decode(
      new Uint8Array(buffer),
    )

    const headerSec = feedMessage.header?.timestamp
      ? toSeconds(feedMessage.header.timestamp)
      : NaN
    const feedTimestamp = !isNaN(headerSec)
      ? new Date(headerSec * 1000).toISOString()
      : new Date().toISOString()

    const vehicles: TransitVehicle[] = []

    for (const entity of feedMessage.entity) {
      const vp = entity.vehicle
      if (!vp?.position) continue

      const position = vp.position
      if (!position.latitude || !position.longitude) continue

      const vehicleId =
        vp.vehicle?.id || vp.vehicle?.label || entity.id || 'unknown'
      const tripId = vp.trip?.tripId || undefined
      const routeId = vp.trip?.routeId || undefined

      vehicles.push({
        vehicleId: `${feed.feedId}_${vehicleId}`,
        tripId: tripId ? `${feed.feedId}_${tripId}` : undefined,
        routeId: routeId || undefined,
        feedId: feed.feedId,
        position: {
          lat: position.latitude,
          lng: position.longitude,
        },
        bearing: position.bearing ?? undefined,
        speed: position.speed ?? undefined,
        timestamp: vp.timestamp
          ? (() => {
              const sec = toSeconds(vp.timestamp)
              return !isNaN(sec) ? new Date(sec * 1000).toISOString() : feedTimestamp
            })()
          : feedTimestamp,
      })
    }

    // Cache the result
    feedCache.set(cacheKey, {
      vehicles,
      feedTimestamp,
      fetchedAt: Date.now(),
    })

    return { vehicles, feedTimestamp, feedId: feed.feedId }
  } catch (err) {
    console.warn(
      `[Vehicles] Failed to fetch feed ${feed.feedId}:`,
      err instanceof Error ? err.message : err,
    )
    return null
  }
}

// ── Route enrichment ────────────────────────────────────────────────

/**
 * Batch-enrich vehicles with route colors and short names from the DB.
 * Uses an LRU cache to avoid repeated lookups for the same route.
 */
async function enrichWithRouteInfo(
  vehicles: TransitVehicle[],
): Promise<void> {
  // Collect unique (feedId, routeId) pairs that aren't cached
  const uncached: Array<{ feedId: string; routeId: string }> = []

  for (const v of vehicles) {
    if (!v.routeId) continue
    const key = `${v.feedId}_${v.routeId}`
    if (!routeInfoCache.has(key)) {
      uncached.push({ feedId: v.feedId, routeId: v.routeId })
    }
  }

  // Batch lookup uncached routes
  if (uncached.length > 0) {
    const unique = [
      ...new Map(uncached.map(r => [`${r.feedId}_${r.routeId}`, r])).values(),
    ]

    const conditions = unique
      .map(
        p =>
          `(feed_id = '${p.feedId.replace(/'/g, "''")}' AND route_id = '${p.routeId.replace(/'/g, "''")}')`,
      )
      .join(' OR ')

    try {
      const result = await db.execute(
        sql.raw(`
          SELECT feed_id, route_id, route_short_name, route_color, route_text_color, route_type
          FROM gtfs_routes
          WHERE ${conditions}
        `),
      )

      for (const row of result as any[]) {
        const key = `${row.feed_id}_${row.route_id}`
        routeInfoCache.set(key, {
          shortName: row.route_short_name || undefined,
          color: row.route_color || undefined,
          textColor: row.route_text_color || undefined,
          type: row.route_type ? parseInt(row.route_type, 10) : undefined,
        })
      }
    } catch (err) {
      console.error('[Vehicles] Failed to fetch route info:', err)
    }
  }

  // Apply cached route info to vehicles
  for (const v of vehicles) {
    if (!v.routeId) continue
    const key = `${v.feedId}_${v.routeId}`
    const info = routeInfoCache.get(key)
    if (info) {
      v.routeShortName = info.shortName
      v.routeColor = info.color
      v.routeTextColor = info.textColor
      v.routeType = info.type
    }
  }
}
