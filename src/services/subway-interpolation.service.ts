/**
 * Subway Position Interpolation Service
 *
 * The subway has no GPS underground, so no feed carries a train's lat/lng.
 * Two things in the feed do say where a train is, and this service uses
 * both — because either one alone loses most of the fleet:
 *
 *   1. The TripUpdate, when it still holds a stop the train has departed:
 *      interpolate between that stop and the next arrival.
 *   2. The VehiclePosition entity, which anchors a trip to a stop it is
 *      stopped at, incoming at, or in transit to.
 *
 * (1) alone found 2 of the 34 4-trains running one afternoon. The MTA
 * PRUNES passed stops from a TripUpdate, so most trips carry only future
 * stops and have no departed stop to interpolate from; every one of those
 * was dropped. The VehiclePosition entities the same feed publishes named
 * 19 of them outright.
 *
 * Time comes from the FEED's header, never this machine's clock. A server
 * running ten minutes slow read every arrival as further away than it was
 * and every departure as still to come — which on its own cut those 34
 * trains to 8. A feed that says what time it is should be believed.
 *
 * Returns TransitVehicle[] in the same format as GPS-based vehicles.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import { LRUCache } from 'lru-cache'
import type { FetchFn, TransitVehicle } from './vehicles.service'
import { SUBWAY_FEED_ID, placeTrain, toSeconds, type StopPosition } from '../lib/subway-position'

// Decode through the live import binding at call time rather than destructuring
// at load, so a test mock of `gtfs-realtime-bindings` applies even when this
// module gets imported before the mock is registered.
const decodeFeedMessage = (buf: Uint8Array) =>
  GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buf)

// ── Subway feed URLs ───────────────────────────────────────────

const SUBWAY_FEEDS = [
  { id: 'subway-1234567', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs' },
  { id: 'subway-ace', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace' },
  { id: 'subway-bdfm', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm' },
  { id: 'subway-g', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g' },
  { id: 'subway-jz', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz' },
  { id: 'subway-l', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l' },
  { id: 'subway-nqrw', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw' },
  { id: 'subway-sir', url: 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si' },
]

// ── Stop position cache ────────────────────────────────────────

let stopPositions: Map<string, StopPosition> | null = null

async function getStopPositions(): Promise<Map<string, StopPosition>> {
  if (stopPositions) return stopPositions

  const result = await db.execute(sql`
    SELECT stop_id, stop_name,
           ST_Y(geom::geometry) as lat,
           ST_X(geom::geometry) as lng
    FROM gtfs_stops
    WHERE feed_id = ${SUBWAY_FEED_ID}
  `)

  const map = new Map<string, StopPosition>()
  for (const row of result as any[]) {
    map.set(row.stop_id, {
      lat: parseFloat(row.lat),
      lng: parseFloat(row.lng),
      name: row.stop_name,
    })
  }

  stopPositions = map
  return map
}

// ── Route patterns (for the stop a train has just left) ────────
//
// A VehiclePosition says which stop a train is heading to, not which one
// it left — and the TripUpdate no longer carries the one it left. The
// route's own stop order supplies it: find the pattern that runs the pair
// the trip is about to run, and take the station before it.

let routePatterns: Map<string, string[][]> | null = null

async function getRoutePatterns(): Promise<Map<string, string[][]>> {
  if (routePatterns) return routePatterns

  const result = await db.execute(sql`
    SELECT route_id, stop_seq
    FROM gtfs_trip_patterns
    WHERE feed_id = ${SUBWAY_FEED_ID}
    ORDER BY trip_count DESC
  `)

  const map = new Map<string, string[][]>()
  for (const row of result as any[]) {
    const stops = String(row.stop_seq).split(',').filter(Boolean)
    if (stops.length < 2) continue
    const list = map.get(row.route_id) ?? []
    list.push(stops)
    map.set(row.route_id, list)
  }

  routePatterns = map
  return map
}


// ── Feed cache ─────────────────────────────────────────────────

interface CachedSubwayFeed {
  vehicles: TransitVehicle[]
  fetchedAt: number
}

const subwayCache = new LRUCache<string, CachedSubwayFeed>({
  max: 10,
  ttl: 15_000, // 15 seconds — subway feeds update ~every 30s
})

// ── Main export ────────────────────────────────────────────────

/**
 * Get interpolated subway vehicle positions from all MTA TripUpdate feeds.
 * Returns synthetic TransitVehicle entries that look identical to GPS-based
 * vehicles for the frontend.
 */
export async function getSubwayVehiclePositions(
  bounds?: { north: number; south: number; east: number; west: number },
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitVehicle[]> {
  const cacheKey = 'all'
  const cached = subwayCache.get(cacheKey)
  if (cached) {
    return bounds ? filterByBounds(cached.vehicles, bounds) : cached.vehicles
  }

  const [stops, patterns] = await Promise.all([
    getStopPositions(),
    getRoutePatterns().catch(() => new Map<string, string[][]>()),
  ])
  const now = Math.floor(Date.now() / 1000)
  const allVehicles: TransitVehicle[] = []

  // Fetch all subway feeds in parallel
  const results = await Promise.allSettled(
    SUBWAY_FEEDS.map(feed => fetchAndInterpolate(feed, stops, patterns, now, fetchFn)),
  )

  for (const result of results) {
    if (result.status === 'fulfilled' && result.value) {
      allVehicles.push(...result.value)
    }
  }

  subwayCache.set(cacheKey, { vehicles: allVehicles, fetchedAt: Date.now() })

  return bounds ? filterByBounds(allVehicles, bounds) : allVehicles
}

function filterByBounds(
  vehicles: TransitVehicle[],
  bounds: { north: number; south: number; east: number; west: number },
): TransitVehicle[] {
  return vehicles.filter(
    v =>
      v.position.lat >= bounds.south &&
      v.position.lat <= bounds.north &&
      v.position.lng >= bounds.west &&
      v.position.lng <= bounds.east,
  )
}

async function fetchAndInterpolate(
  feed: { id: string; url: string },
  stops: Map<string, StopPosition>,
  patterns: Map<string, string[][]>,
  nowSec: number,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitVehicle[]> {
  try {
    const response = await fetchFn(feed.url, {
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return []

    const buffer = await response.arrayBuffer()
    const feedMessage = decodeFeedMessage(new Uint8Array(buffer))

    // The feed's own clock, not ours — see the note at the top of the file.
    const now = toSeconds(feedMessage.header?.timestamp) || nowSec

    // VehiclePosition entities, by the trip they are running.
    const anchors = new Map<string, any>()
    for (const entity of feedMessage.entity) {
      const v = entity.vehicle
      if (v?.trip?.tripId && v.stopId) anchors.set(v.trip.tripId, v)
    }

    const vehicles: TransitVehicle[] = []

    for (const entity of feedMessage.entity) {
      const tu = entity.tripUpdate
      if (!tu?.trip?.routeId) continue

      const result = placeTrain(tu, anchors.get(tu.trip.tripId ?? ''), stops, patterns, now)
      if (!result) continue

      // Build a unique vehicle ID from the trip
      const tripId = tu.trip.tripId || entity.id || ''
      const vehicleId = `${SUBWAY_FEED_ID}_subway_${tripId}`

      vehicles.push({
        vehicleId,
        tripId: `${SUBWAY_FEED_ID}_${tripId}`,
        routeId: tu.trip.routeId,
        feedId: SUBWAY_FEED_ID,
        position: { lat: result.lat, lng: result.lng },
        bearing: result.bearing,
        speed: result.speed,
        timestamp: new Date(now * 1000).toISOString(),
      })
    }

    return vehicles
  } catch (err) {
    console.warn(
      `[Subway] Failed to fetch ${feed.id}:`,
      err instanceof Error ? err.message : err,
    )
    return []
  }
}
