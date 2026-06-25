/**
 * Subway Position Interpolation Service
 *
 * NYC subway doesn't publish VehiclePosition data (no GPS underground).
 * Instead, we derive train positions from TripUpdate feeds by finding
 * where each train is between two consecutive stations and interpolating
 * the lat/lng based on the time fraction elapsed.
 *
 * For each active trip:
 *   1. Find the last stop the train departed (departure < now)
 *   2. Find the next stop it's arriving at (arrival > now)
 *   3. Compute t = (now - lastDeparture) / (nextArrival - lastDeparture)
 *   4. Lerp lat/lng between the two stop positions
 *   5. Compute bearing from the direction of travel
 *
 * Returns TransitVehicle[] in the same format as GPS-based vehicles.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import { LRUCache } from 'lru-cache'
import type { TransitVehicle } from './vehicles.service'

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

/** The GTFS feed_id for NYC subway in our database. */
const SUBWAY_FEED_ID = '5'

// ── Stop position cache ────────────────────────────────────────

interface StopPosition {
  lat: number
  lng: number
  name: string
}

let stopPositions: Map<string, StopPosition> | null = null

async function getStopPositions(): Promise<Map<string, StopPosition>> {
  if (stopPositions) return stopPositions

  const result = await db.execute(sql.raw(`
    SELECT stop_id, stop_name,
           ST_Y(geom::geometry) as lat,
           ST_X(geom::geometry) as lng
    FROM gtfs_stops
    WHERE feed_id = '${SUBWAY_FEED_ID}'
  `))

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

// ── Feed cache ─────────────────────────────────────────────────

interface CachedSubwayFeed {
  vehicles: TransitVehicle[]
  fetchedAt: number
}

const subwayCache = new LRUCache<string, CachedSubwayFeed>({
  max: 10,
  ttl: 15_000, // 15 seconds — subway feeds update ~every 30s
})

// ── Protobuf timestamp helper ──────────────────────────────────

function toSeconds(ts: any): number {
  if (typeof ts === 'number') return ts
  if (ts && typeof ts.toNumber === 'function') return ts.toNumber()
  if (ts && typeof ts.low === 'number') return ts.low + (ts.high || 0) * 0x100000000
  return NaN
}

// ── Bearing calculation ────────────────────────────────────────

function bearing(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180
  const toDeg = (r: number) => (r * 180) / Math.PI
  const dLng = toRad(lng2 - lng1)
  const y = Math.sin(dLng) * Math.cos(toRad(lat2))
  const x =
    Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
    Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(dLng)
  return (toDeg(Math.atan2(y, x)) + 360) % 360
}

// ── Main export ────────────────────────────────────────────────

/**
 * Get interpolated subway vehicle positions from all MTA TripUpdate feeds.
 * Returns synthetic TransitVehicle entries that look identical to GPS-based
 * vehicles for the frontend.
 */
export async function getSubwayVehiclePositions(
  bounds?: { north: number; south: number; east: number; west: number },
  fetchFn: typeof fetch = globalThis.fetch,
): Promise<TransitVehicle[]> {
  const cacheKey = 'all'
  const cached = subwayCache.get(cacheKey)
  if (cached) {
    return bounds ? filterByBounds(cached.vehicles, bounds) : cached.vehicles
  }

  const stops = await getStopPositions()
  const now = Math.floor(Date.now() / 1000)
  const allVehicles: TransitVehicle[] = []

  // Fetch all subway feeds in parallel
  const results = await Promise.allSettled(
    SUBWAY_FEEDS.map(feed => fetchAndInterpolate(feed, stops, now, fetchFn)),
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
  nowSec: number,
  fetchFn: typeof fetch = globalThis.fetch,
): Promise<TransitVehicle[]> {
  try {
    const response = await fetchFn(feed.url, {
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return []

    const buffer = await response.arrayBuffer()
    const feedMessage = decodeFeedMessage(new Uint8Array(buffer))

    const vehicles: TransitVehicle[] = []

    for (const entity of feedMessage.entity) {
      const tu = entity.tripUpdate
      if (!tu?.stopTimeUpdate?.length) continue
      if (!tu.trip?.routeId) continue

      const result = interpolateTrip(tu, stops, nowSec)
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
        timestamp: new Date(nowSec * 1000).toISOString(),
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

interface InterpolationResult {
  lat: number
  lng: number
  bearing: number
  speed: number
}

function interpolateTrip(
  tripUpdate: any,
  stops: Map<string, StopPosition>,
  nowSec: number,
): InterpolationResult | null {
  const stus = tripUpdate.stopTimeUpdate
  if (!stus || stus.length < 2) return null

  // Find the two bracketing stops: last departed and next arriving.
  // Walk the stop list to find where `now` falls.
  let lastStop: { id: string; time: number } | null = null
  let nextStop: { id: string; time: number } | null = null

  for (let i = 0; i < stus.length; i++) {
    const stu = stus[i]
    const depTime = stu.departure?.time ? toSeconds(stu.departure.time) : null
    const arrTime = stu.arrival?.time ? toSeconds(stu.arrival.time) : null

    // Use departure time for "last stop" and arrival time for "next stop"
    if (depTime && depTime <= nowSec) {
      lastStop = { id: stu.stopId, time: depTime }
    }
    if (arrTime && arrTime > nowSec && !nextStop) {
      nextStop = { id: stu.stopId, time: arrTime }
    }
  }

  // If we only have future stops, the train hasn't departed yet — show at first stop
  if (!lastStop && stus.length > 0) {
    const firstStu = stus[0]
    const firstTime = firstStu.arrival?.time
      ? toSeconds(firstStu.arrival.time)
      : firstStu.departure?.time
        ? toSeconds(firstStu.departure.time)
        : null
    if (firstTime && firstTime > nowSec && firstTime - nowSec < 120) {
      // Within 2 minutes of departure — show at the first stop
      const pos = stops.get(firstStu.stopId)
      if (pos) {
        return { lat: pos.lat, lng: pos.lng, bearing: 0, speed: 0 }
      }
    }
    return null
  }

  if (!lastStop || !nextStop) return null

  const lastPos = stops.get(lastStop.id)
  const nextPos = stops.get(nextStop.id)
  if (!lastPos || !nextPos) return null

  // Compute interpolation fraction
  const totalTime = nextStop.time - lastStop.time
  if (totalTime <= 0) return null

  const elapsed = nowSec - lastStop.time
  const t = Math.max(0, Math.min(1, elapsed / totalTime))

  // Linear interpolation between stops
  const lat = lastPos.lat + (nextPos.lat - lastPos.lat) * t
  const lng = lastPos.lng + (nextPos.lng - lastPos.lng) * t

  // Bearing from last stop to next stop
  const brg = bearing(lastPos.lat, lastPos.lng, nextPos.lat, nextPos.lng)

  // Approximate speed from distance and time
  const distM =
    6_371_000 *
    Math.acos(
      Math.min(
        1,
        Math.sin((lastPos.lat * Math.PI) / 180) *
          Math.sin((nextPos.lat * Math.PI) / 180) +
          Math.cos((lastPos.lat * Math.PI) / 180) *
            Math.cos((nextPos.lat * Math.PI) / 180) *
            Math.cos(((nextPos.lng - lastPos.lng) * Math.PI) / 180),
      ),
    )
  const speed = totalTime > 0 ? distM / totalTime : 0

  return { lat, lng, bearing: brg, speed }
}
