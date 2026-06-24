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
import { existsSync, readFileSync } from 'fs'
import { join } from 'path'
import JSZip from 'jszip'

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
  /** Next stop ID from TripUpdate data (for arrival-time interpolation). */
  nextStopId?: string
  /** Predicted arrival at next stop (ISO timestamp from TripUpdate). */
  nextStopArrival?: string
  /** Position from the previous GTFS-RT snapshot (if available).
   *  Allows clients to start interpolation immediately on first fetch
   *  by providing two data points — no need to wait for a second poll. */
  previousPosition?: { lat: number; lng: number }
  /** Timestamp of the previous position snapshot. */
  previousTimestamp?: string
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
  ttl: 5_000, // 5 seconds
})

/**
 * Previous vehicle positions, keyed by vehicleId.
 * Updated each time a feed cache entry is refreshed. Allows the first
 * client fetch to include two data points per vehicle so interpolation
 * can start immediately (speed + direction from the delta).
 *
 * Capped to prevent unbounded growth from removed vehicles.
 */
const previousPositions = new LRUCache<string, { lat: number; lng: number; timestamp: string }>({
  max: 10_000,
  ttl: 120_000, // 2 minutes — stale previous positions aren't useful
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

// ── Trip → Route resolver ────────────────────────────────────────

/**
 * Lazily loaded trip→route mapping per feed. Built from trips.txt in
 * the GTFS ZIP files. Some RT feeds (LIRR, Metro-North, NYC Ferry)
 * don't include routeId in VehiclePosition — we resolve it here.
 */
const tripRouteCache = new LRUCache<string, Map<string, string>>({
  max: 50,
  ttl: 3_600_000, // 1 hour
})

const GTFS_DATA_DIR = process.env.GTFS_DATA_DIR || './data/gtfs'

async function getTripRouteMap(feedId: string): Promise<Map<string, string>> {
  const cached = tripRouteCache.get(feedId)
  if (cached) return cached

  const map = new Map<string, string>()
  const zipPath = join(GTFS_DATA_DIR, `${feedId}.zip`)

  if (!existsSync(zipPath)) {
    tripRouteCache.set(feedId, map)
    return map
  }

  try {
    const zipData = readFileSync(zipPath)
    const zip = await JSZip.loadAsync(zipData)
    const tripsFile = zip.file('trips.txt')
    if (!tripsFile) {
      tripRouteCache.set(feedId, map)
      return map
    }

    const content = await tripsFile.async('string')
    const lines = content.split('\n')
    const header = lines[0]?.split(',').map(h => h.trim().replace(/"/g, ''))
    if (!header) {
      tripRouteCache.set(feedId, map)
      return map
    }

    const tripIdIdx = header.indexOf('trip_id')
    const routeIdIdx = header.indexOf('route_id')
    const shortNameIdx = header.indexOf('trip_short_name')

    if (tripIdIdx === -1 || routeIdIdx === -1) {
      tripRouteCache.set(feedId, map)
      return map
    }

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i]?.split(',').map(c => c.trim().replace(/"/g, ''))
      if (!cols || cols.length <= Math.max(tripIdIdx, routeIdIdx)) continue
      const tripId = cols[tripIdIdx]
      const routeId = cols[routeIdIdx]
      if (tripId && routeId) {
        map.set(tripId, routeId)
      }
      // Also index by trip_short_name — some RT feeds (Metro-North)
      // use the short name as the trip identifier
      if (shortNameIdx >= 0) {
        const shortName = cols[shortNameIdx]
        if (shortName && routeId && !map.has(shortName)) {
          map.set(shortName, routeId)
        }
      }
    }

    tripRouteCache.set(feedId, map)
    return map
  } catch (err) {
    console.warn(`[Vehicles] Failed to load trips.txt from ${zipPath}:`, err)
    tripRouteCache.set(feedId, map)
    return map
  }
}

/**
 * Resolve routeId from tripId for vehicles missing it.
 */
async function resolveMissingRoutes(vehicles: TransitVehicle[]): Promise<void> {
  // Collect feeds that need resolution
  const feedIds = new Set<string>()
  for (const v of vehicles) {
    if (!v.routeId && v.tripId) feedIds.add(v.feedId)
  }

  // Load trip→route maps for needed feeds
  const maps = new Map<string, Map<string, string>>()
  for (const feedId of feedIds) {
    maps.set(feedId, await getTripRouteMap(feedId))
  }

  // Apply
  for (const v of vehicles) {
    if (v.routeId || !v.tripId) continue
    const tripMap = maps.get(v.feedId)
    if (!tripMap) continue
    // tripId in the vehicle is prefixed with feedId
    const rawTripId = v.tripId.startsWith(`${v.feedId}_`)
      ? v.tripId.slice(v.feedId.length + 1)
      : v.tripId
    v.routeId = tripMap.get(rawTripId)
  }
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

  // Add interpolated subway positions from TripUpdate feeds
  try {
    const { getSubwayVehiclePositions } = await import('./subway-interpolation.service')
    const subwayVehicles = await getSubwayVehiclePositions({ north, south, east, west })
    allVehicles.push(...subwayVehicles)
  } catch (err) {
    console.warn('[Vehicles] Subway interpolation failed:', err instanceof Error ? err.message : err)
  }

  // Resolve routeId from tripId for feeds that don't include it
  await resolveMissingRoutes(allVehicles)

  // Enrich with route metadata (colors, short names)
  await enrichWithRouteInfo(allVehicles)

  // Enrich with TripUpdate next-stop predictions
  await enrichWithTripUpdates(allVehicles, feeds, fetchFn)

  return { vehicles: allVehicles, feedTimestamps }
}

/**
 * Fetch ALL vehicles matching specific route IDs — no bounding box.
 * Used by the route detail view to show every vehicle on a line.
 */
export async function getVehiclesForRoute(
  routeIds: string[],
  feedId?: string,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<VehiclePositionsResponse> {
  const feeds = await getFeedsWithVehiclePositions(feedId)
  if (feeds.length === 0) return { vehicles: [], feedTimestamps: {} }

  const results = await Promise.allSettled(
    feeds.map(feed => fetchFeedVehicles(feed, fetchFn)),
  )

  const allVehicles: TransitVehicle[] = []
  const feedTimestamps: Record<string, string> = {}

  for (const result of results) {
    if (result.status !== 'fulfilled' || !result.value) continue
    const { vehicles, feedTimestamp, feedId: fId } = result.value
    feedTimestamps[fId] = feedTimestamp
    allVehicles.push(...vehicles)
  }

  // Add subway interpolated positions (no bounds filter)
  try {
    const { getSubwayVehiclePositions } = await import('./subway-interpolation.service')
    const subwayVehicles = await getSubwayVehiclePositions()
    allVehicles.push(...subwayVehicles)
  } catch (err) {
    console.warn('[Vehicles] Subway interpolation failed:', err instanceof Error ? err.message : err)
  }

  await resolveMissingRoutes(allVehicles)
  await enrichWithRouteInfo(allVehicles)
  await enrichWithTripUpdates(allVehicles, feeds, fetchFn)

  // Filter to requested routes AFTER enrichment (so routeId is resolved)
  const routeIdSet = new Set(routeIds)
  const filtered = allVehicles.filter(v =>
    (v.routeId && routeIdSet.has(v.routeId)) ||
    (v.routeShortName && routeIdSet.has(v.routeShortName)),
  )

  return { vehicles: filtered, feedTimestamps }
}

// ── Feed discovery ──────────────────────────────────────────────────

interface FeedRtInfo {
  feedId: string
  vehiclePositionUrl: string
  tripUpdateUrl?: string
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
    // "vehicleposition" or "vehicle_position" or "VehiclePosition".
    // Fallback: if a feed has only one RT URL (combined feed with
    // TripUpdate + VehiclePosition), try it — fetchFeedVehicles
    // filters for VehiclePosition entities from the decoded protobuf.
    const vpUrl = rtUrls.find(u =>
      /vehicle.?position/i.test(u.url),
    ) ?? (rtUrls.length === 1 ? rtUrls[0] : undefined)

    if (vpUrl) {
      const tuUrl = rtUrls.find(u =>
        /trip.?update/i.test(u.url),
      )
      feeds.push({
        feedId: row.feed_id,
        vehiclePositionUrl: vpUrl.url,
        tripUpdateUrl: tuUrl?.url,
        headers: vpUrl.headers,
      })
    }
  }

  return feeds
}

// ── TripUpdate correlation ─────────────────────────────────────────

interface TripNextStop {
  stopId: string
  arrivalTime: number // Unix seconds
}

/** Cache of tripId → next stop info, keyed by feedId. Short TTL. */
const tripUpdateCache = new LRUCache<string, Map<string, TripNextStop>>({
  max: 200,
  ttl: 10_000,
})

/**
 * Fetch TripUpdate data for a feed and build a tripId → next-stop lookup.
 * The "next stop" is the first stop with arrival > now.
 */
async function fetchTripUpdates(
  feed: FeedRtInfo,
  fetchFn: FetchFn,
): Promise<Map<string, TripNextStop>> {
  if (!feed.tripUpdateUrl) return new Map()

  const cacheKey = feed.feedId
  const cached = tripUpdateCache.get(cacheKey)
  if (cached) return cached

  try {
    const headers: Record<string, string> = {}
    if (feed.headers) Object.assign(headers, feed.headers)

    const response = await fetchFn(feed.tripUpdateUrl, {
      headers,
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return new Map()

    const buffer = await response.arrayBuffer()
    const feedMessage = transit_realtime.FeedMessage.decode(new Uint8Array(buffer))

    const nowSec = Math.floor(Date.now() / 1000)
    const map = new Map<string, TripNextStop>()

    for (const entity of feedMessage.entity) {
      const tu = entity.tripUpdate
      if (!tu?.stopTimeUpdate?.length || !tu.trip?.tripId) continue

      // Find the first future stop
      for (const stu of tu.stopTimeUpdate) {
        const arrTime = stu.arrival?.time ? toSeconds(stu.arrival.time) : null
        if (arrTime && arrTime > nowSec && stu.stopId) {
          map.set(tu.trip.tripId, {
            stopId: stu.stopId,
            arrivalTime: arrTime,
          })
          break
        }
      }
    }

    tripUpdateCache.set(cacheKey, map)
    return map
  } catch {
    return new Map()
  }
}

export interface TripStopTime {
  stopId: string
  arrivalTime?: string
  departureTime?: string
}

/**
 * Get all stop times for a specific trip from the TripUpdate feed.
 * Returns past and future stop times with real-time predictions.
 */
export async function getTripStopTimes(
  feedId: string,
  tripId: string,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TripStopTime[]> {
  const feeds = await getFeedsWithVehiclePositions(feedId)
  const feed = feeds[0]
  if (!feed?.tripUpdateUrl) return []

  try {
    const headers: Record<string, string> = {}
    if (feed.headers) Object.assign(headers, feed.headers)

    const response = await fetchFn(feed.tripUpdateUrl, {
      headers,
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return []

    const buffer = await response.arrayBuffer()
    const feedMessage = transit_realtime.FeedMessage.decode(new Uint8Array(buffer))

    for (const entity of feedMessage.entity) {
      const tu = entity.tripUpdate
      if (!tu?.trip?.tripId || tu.trip.tripId !== tripId) continue
      if (!tu.stopTimeUpdate?.length) continue

      return tu.stopTimeUpdate.map((stu: any) => ({
        stopId: stu.stopId || '',
        arrivalTime: stu.arrival?.time
          ? new Date(toSeconds(stu.arrival.time) * 1000).toISOString()
          : undefined,
        departureTime: stu.departure?.time
          ? new Date(toSeconds(stu.departure.time) * 1000).toISOString()
          : undefined,
      }))
    }
  } catch {
    // Silently fail
  }

  return []
}

/**
 * Enrich vehicles with next-stop arrival predictions from TripUpdate data.
 */
async function enrichWithTripUpdates(
  vehicles: TransitVehicle[],
  feeds: FeedRtInfo[],
  fetchFn: FetchFn,
): Promise<void> {
  // Build feed lookup
  const feedMap = new Map(feeds.map(f => [f.feedId, f]))

  // Collect which feeds we need TripUpdate data for
  const feedIds = new Set(vehicles.map(v => v.feedId))

  // Fetch TripUpdate data for each feed in parallel
  const tuMaps = new Map<string, Map<string, TripNextStop>>()
  await Promise.all(
    [...feedIds].map(async fid => {
      const feed = feedMap.get(fid)
      if (feed) {
        tuMaps.set(fid, await fetchTripUpdates(feed, fetchFn))
      }
    }),
  )

  // Apply to vehicles
  for (const v of vehicles) {
    if (!v.tripId) continue
    const tuMap = tuMaps.get(v.feedId)
    if (!tuMap) continue

    // tripId in the vehicle is prefixed with feedId
    const rawTripId = v.tripId.startsWith(`${v.feedId}_`)
      ? v.tripId.slice(v.feedId.length + 1)
      : v.tripId

    const nextStop = tuMap.get(rawTripId)
    if (nextStop) {
      v.nextStopId = nextStop.stopId
      v.nextStopArrival = new Date(nextStop.arrivalTime * 1000).toISOString()
    }
  }
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
      // Some GTFS-RT servers reject specific Accept types with 406.
      // Use */* to maximise compatibility (the response is always protobuf).
      'Accept': '*/*',
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

    // Attach previous positions from the last fetch cycle. The
    // previousPositions map was populated when the PREVIOUS cache entry
    // was set, so it survives the LRU TTL eviction.
    for (const v of vehicles) {
      const prev = previousPositions.get(v.vehicleId)
      if (prev && (prev.lat !== v.position.lat || prev.lng !== v.position.lng)) {
        v.previousPosition = { lat: prev.lat, lng: prev.lng }
        v.previousTimestamp = prev.timestamp
      }
    }

    // Save THIS batch's positions as "previous" for the NEXT cache refresh.
    // Done after attachment so we don't overwrite what we just read.
    for (const v of vehicles) {
      previousPositions.set(v.vehicleId, {
        lat: v.position.lat,
        lng: v.position.lng,
        timestamp: v.timestamp,
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
          SELECT feed_id, route_id, route_short_name, route_long_name, route_color, route_text_color, route_type
          FROM gtfs_routes
          WHERE ${conditions}
        `),
      )

      for (const row of result as any[]) {
        // Cache under both the original feed_id key AND the requested feed_id key
        // so cross-feed lookups (e.g. MTA Bus feed 6 requesting Manhattan route M22
        // which lives under feed 7) are resolved.
        const info: RouteInfo = {
          shortName: row.route_short_name || row.route_long_name || undefined,
          color: row.route_color || undefined,
          textColor: row.route_text_color || undefined,
          type: row.route_type ? parseInt(row.route_type, 10) : undefined,
        }
        routeInfoCache.set(`${row.feed_id}_${row.route_id}`, info)
        for (const req of unique) {
          if (req.routeId === row.route_id) {
            routeInfoCache.set(`${req.feedId}_${req.routeId}`, info)
          }
        }
      }

      // Cross-feed fallback: for routes not found under their feed_id,
      // try a broader search by route_id alone
      const stillMissing = unique.filter(r => !routeInfoCache.has(`${r.feedId}_${r.routeId}`))
      if (stillMissing.length > 0) {
        const routeIds = [...new Set(stillMissing.map(r => r.routeId))]
        const routeConditions = routeIds
          .map(rid => `route_id = '${rid.replace(/'/g, "''")}'`)
          .join(' OR ')

        const fallbackResult = await db.execute(
          sql.raw(`
            SELECT feed_id, route_id, route_short_name, route_long_name, route_color, route_text_color, route_type
            FROM gtfs_routes
            WHERE (${routeConditions})
              AND (route_short_name IS NOT NULL AND route_short_name != ''
                OR route_long_name IS NOT NULL AND route_long_name != '')
            LIMIT ${routeIds.length}
          `),
        )

        for (const row of fallbackResult as any[]) {
          const info: RouteInfo = {
            shortName: row.route_short_name || row.route_long_name || undefined,
            color: row.route_color || undefined,
            textColor: row.route_text_color || undefined,
            type: row.route_type ? parseInt(row.route_type, 10) : undefined,
          }
          for (const req of stillMissing) {
            if (req.routeId === row.route_id) {
              routeInfoCache.set(`${req.feedId}_${req.routeId}`, info)
            }
          }
        }
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
