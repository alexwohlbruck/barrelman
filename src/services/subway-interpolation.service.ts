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
import type { TransitVehicle } from './vehicles.service'
import { stopBefore, segmentSeconds, toSeconds } from '../lib/subway-position'

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

/** GTFS-RT names platforms ("239S"); trip patterns name stations ("239"). */
const parentOf = (stopId: string) => stopId.replace(/[NS]$/, '')

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
  fetchFn: typeof fetch = globalThis.fetch,
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
      if (!tu?.stopTimeUpdate?.length) continue
      if (!tu.trip?.routeId) continue

      // Interpolation first — it places a train BETWEEN stations, which is
      // where one usually is. The anchor catches the majority the pruned
      // feed leaves it unable to answer for.
      const result =
        interpolateTrip(tu, stops, now) ??
        anchorPosition(anchors.get(tu.trip.tripId ?? ''), tu, stops, patterns, now)
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

interface InterpolationResult {
  lat: number
  lng: number
  bearing: number
  speed: number
}

/** GTFS-RT VehicleStopStatus. */
const STOPPED_AT = 1

/**
 * Place a train from its VehiclePosition entity.
 *
 * The entity names one stop and the train's relationship to it. Stopped at
 * it means exactly there. Approaching it means somewhere on the run in
 * from the station before — which the route's stop order supplies, and the
 * remaining time to arrival places it along.
 */
function anchorPosition(
  vehicle: any,
  tripUpdate: any,
  stops: Map<string, StopPosition>,
  patterns: Map<string, string[][]>,
  nowSec: number,
): InterpolationResult | null {
  if (!vehicle?.stopId) return null
  const target = stops.get(vehicle.stopId)
  if (!target) return null

  const stus = tripUpdate.stopTimeUpdate ?? []
  const nextAfter = stus.find((s: any) => s.stopId && s.stopId !== vehicle.stopId)
  const bearingTo = (to: StopPosition | undefined) =>
    to ? bearing(target.lat, target.lng, to.lat, to.lng) : 0

  const at = (): InterpolationResult => ({
    lat: target.lat,
    lng: target.lng,
    bearing: bearingTo(nextAfter ? stops.get(nextAfter.stopId) : undefined),
    speed: 0,
  })

  if (vehicle.currentStatus === STOPPED_AT) return at()

  const routeId = tripUpdate.trip?.routeId
  const prevId = routeId
    ? stopBefore(
        patterns.get(routeId) ?? [],
        parentOf(vehicle.stopId),
        nextAfter ? parentOf(nextAfter.stopId) : null,
      )
    : null
  const prev = prevId ? stops.get(prevId) : undefined
  if (!prev) return at()

  // How far along the run in: the arrival still to come, against a typical
  // hop for this trip. Without an arrival time, halfway is the honest
  // answer — the train is between the two, and nothing says where.
  const arrive = toSeconds(
    stus.find((s: any) => s.stopId === vehicle.stopId)?.arrival?.time,
  )
  const hop = segmentSeconds(stus) ?? 90
  const remaining = arrive ? Math.max(0, arrive - nowSec) : hop / 2
  const t = Math.max(0, Math.min(1, 1 - remaining / hop))

  return {
    lat: prev.lat + (target.lat - prev.lat) * t,
    lng: prev.lng + (target.lng - prev.lng) * t,
    bearing: bearing(prev.lat, prev.lng, target.lat, target.lng),
    speed: 0,
  }
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
