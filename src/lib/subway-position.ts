/**
 * Reading a subway train's place in its route from the pieces a realtime
 * feed gives: the stop order it is running, and its own timings.
 *
 * Pure, and kept out of the service so a test that stubs the service does
 * not take them with it.
 */

/** The GTFS feed_id for NYC subway in our database. */
export const SUBWAY_FEED_ID = '5'

/** GTFS-RT timestamps arrive as numbers, longs, or {low, high} pairs. */
export function toSeconds(ts: any): number {
  if (typeof ts === 'number') return ts
  if (ts && typeof ts.toNumber === 'function') return ts.toNumber()
  if (ts && typeof ts.low === 'number') return ts.low + (ts.high || 0) * 0x100000000
  return NaN
}

/**
 * The station before `next` on a pattern this trip is actually running.
 *
 * `after` — the stop the trip reaches AFTER `next` — picks the direction:
 * every route's stops appear in both patterns, so without it a
 * Woodlawn-bound train borrows the southbound predecessor and jumps a
 * station backwards.
 */
export function stopBefore(
  patterns: string[][],
  next: string,
  after: string | null,
): string | null {
  for (const p of patterns) {
    const i = p.indexOf(next)
    if (i <= 0) continue
    if (after && p[i + 1] !== after) continue
    return p[i - 1]
  }
  return null
}

/** A typical station-to-station time for this trip, from its own next two
 *  arrivals. */
export function segmentSeconds(stus: any[]): number | null {
  const times = stus
    .map((s: any) => toSeconds(s.arrival?.time))
    .filter((n: number | null): n is number => !!n)
  if (times.length < 2) return null
  const gap = times[1] - times[0]
  return gap > 0 && gap < 900 ? gap : null
}

export interface StopPosition {
  lat: number
  lng: number
  name: string
}

/** GTFS-RT names platforms ("239S"); trip patterns name stations ("239"). */
const parentOf = (stopId: string) => stopId.replace(/[NS]$/, '')

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


/** How long a train is shown at a terminal: before leaving its first stop, and after reaching its last. */
export const TERMINAL_WINDOW_S = 120

/** A train with only future stops and no position report, due at its first stop soon: waiting there to leave. */
function terminalPosition(
  tripUpdate: any,
  stops: Map<string, StopPosition>,
  nowSec: number,
): InterpolationResult | null {
  const first = tripUpdate.stopTimeUpdate?.[0]
  const time = toSeconds(first?.arrival?.time ?? first?.departure?.time)
  if (!(time > nowSec && time - nowSec < TERMINAL_WINDOW_S)) return null
  const pos = stops.get(first.stopId)
  return pos ? { lat: pos.lat, lng: pos.lng, bearing: 0, speed: 0 } : null
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

/** A stopped report this old means the train has left: MTA sends none on the long runs between express stops. */
export const STALE_STOP_S = 60

/** The update with `stopId` as the stop the train last left, at `at` when the feed has already pruned it. */
function leftFrom(tripUpdate: any, stopId: string, at: number): any {
  const stus = tripUpdate.stopTimeUpdate ?? []
  if (stus.some((s: any) => s.stopId === stopId)) return tripUpdate
  return { ...tripUpdate, stopTimeUpdate: [{ stopId, departure: { time: at } }, ...stus] }
}

/**
 * Where a train is. A fresh stopped report puts it at that platform: its
 * update predicts no dwell, so interpolating would pull it out early. A stale
 * one means it has since left, so it runs on from there. Otherwise
 * interpolation, which places a train between stations; then the anchor,
 * for the many trips the pruned feed leaves no departed stop.
 */
/** A trip with no stops left: the train has just reached its last one, or the trip is over. */
function arrivedPosition(
  tripUpdate: any,
  vehicle: any,
  stops: Map<string, StopPosition>,
  patterns: Map<string, string[][]>,
  nowSec: number,
): InterpolationResult | null {
  if (vehicle?.currentStatus !== STOPPED_AT) return null
  if (nowSec - toSeconds(vehicle.timestamp) > TERMINAL_WINDOW_S) return null
  return anchorPosition(vehicle, tripUpdate, stops, patterns, nowSec)
}

export function placeTrain(
  tripUpdate: any,
  vehicle: any,
  stops: Map<string, StopPosition>,
  patterns: Map<string, string[][]>,
  nowSec: number,
): InterpolationResult | null {
  // MTA uses some stop ids that aren't in the timetable, such as the Rockaway shuttle's H19.
  tripUpdate = { ...tripUpdate, stopTimeUpdate: (tripUpdate.stopTimeUpdate ?? []).filter((s: any) => stops.has(s.stopId)) }
  if (!tripUpdate.stopTimeUpdate.length) return arrivedPosition(tripUpdate, vehicle, stops, patterns, nowSec)
  if (vehicle?.currentStatus === STOPPED_AT) {
    const seen = toSeconds(vehicle.timestamp)
    const placed = nowSec - seen > STALE_STOP_S
      ? interpolateTrip(leftFrom(tripUpdate, vehicle.stopId, seen), stops, nowSec)
      : anchorPosition(vehicle, tripUpdate, stops, patterns, nowSec)
    if (placed) return placed
  }
  return interpolateTrip(tripUpdate, stops, nowSec) ??
    anchorPosition(vehicle, tripUpdate, stops, patterns, nowSec) ??
    terminalPosition(tripUpdate, stops, nowSec)
}
