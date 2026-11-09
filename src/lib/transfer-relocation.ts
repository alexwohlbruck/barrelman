import { haversineM } from './station-graph/guidance'

/** Modes that run behind fare gates, where a walk between stations means paying again. */
export const GATED_MODES: ReadonlySet<string> = new Set([
  'SUBWAY', 'METRO', 'RAIL', 'REGIONAL_RAIL', 'REGIONAL_FAST_RAIL', 'SUBURBAN',
  'HIGHSPEED_RAIL', 'LONG_DISTANCE', 'NIGHT_RAIL', 'FUNICULAR',
])

export interface StopPlace {
  name: string
  lat: number
  lng: number
  stopId?: string
  arrival?: string
  departure?: string
  platformCode?: string
}

type Line = { type: 'LineString'; coordinates: [number, number][] }

export interface Ride {
  mode: string
  from: StopPlace
  to: StopPlace
  startTime: string
  endTime: string
  duration: number
  distance: number
  geometry?: Line
  transitLeg: boolean
  tripId?: string
  intermediateStops?: StopPlace[]
}

/** A trip's every stop in order, and its whole shape. */
export interface Trip {
  stops: StopPlace[]
  shape?: [number, number][]
}

/** Seconds the agency files for changing from one stop to another inside fare control; null when it files none. */
export type InSystemLink = (from: string, to: string) => number | null

const at = (iso?: string) => (iso ? Date.parse(iso) : NaN)

function nearest(shape: [number, number][], p: StopPlace, from = 0): number {
  let best = from, bestD = Infinity
  for (let i = from; i < shape.length; i++) {
    const d = (shape[i][0] - p.lng) ** 2 + (shape[i][1] - p.lat) ** 2
    if (d < bestD) { best = i; bestD = d }
  }
  return best
}

function ride<L extends Ride>(leg: L, trip: Trip, board: number, alight: number): L {
  const from = trip.stops[board], to = trip.stops[alight]
  const startTime = from.departure ?? from.arrival!
  const endTime = to.arrival ?? to.departure!
  let geometry: Line | undefined
  if (trip.shape && trip.shape.length > 1) {
    const i = nearest(trip.shape, from)
    const coordinates = trip.shape.slice(i, nearest(trip.shape, to, i) + 1)
    if (coordinates.length > 1) geometry = { type: 'LineString', coordinates }
  }
  const path = [from, ...trip.stops.slice(board + 1, alight), to]
  return {
    ...leg,
    from,
    to,
    startTime,
    endTime,
    duration: Math.round((at(endTime) - at(startTime)) / 1000),
    distance: Math.round(path.slice(1).reduce((m, s, i) => m + haversineM(path[i], s), 0)),
    geometry,
    intermediateStops: trip.stops.slice(board + 1, alight),
  }
}

/**
 * Move an out-of-system change onto the in-system one the same two trains
 * make. The walk from 21 St to Hunters Point Av catches the 7 a stop before
 * Court Sq, where the G meets it inside the gates: the router ties them on
 * arrival and keeps whichever boards first. Staying on the first train, or
 * leaving it earlier, to change where the agency files a transfer keeps both
 * trains and the arrival, and drops the second fare and the street walk.
 *
 * `padSeconds` is the router's margin on every change, which its walk legs
 * include. Returns the replacement legs, or null when no filed change makes
 * the connection.
 */
export function relocateTransfer<L extends Ride>(
  first: L,
  second: L,
  firstTrip: Trip,
  secondTrip: Trip,
  link: InSystemLink,
  padSeconds = 0,
): [L, Ride, L] | null {
  const board = firstTrip.stops.findIndex((s) => s.stopId === first.from.stopId)
  const alight = secondTrip.stops.findIndex((s) => s.stopId === second.to.stopId)
  if (board < 0 || alight < 0) return null

  let best: { i: number; j: number; seconds: number } | null = null
  for (let i = board + 1; i < firstTrip.stops.length; i++) {
    const a = firstTrip.stops[i]
    const arrives = at(a.arrival ?? a.departure)
    if (!a.stopId || Number.isNaN(arrives)) continue
    for (let j = 0; j < alight; j++) {
      const b = secondTrip.stops[j]
      const filed = b.stopId ? link(a.stopId, b.stopId) : null
      const seconds = filed === null ? null : filed + padSeconds
      if (seconds === null || arrives + seconds * 1000 > at(b.departure ?? b.arrival)) continue
      if (!best || seconds < best.seconds) best = { i, j, seconds }
    }
  }
  if (!best) return null

  const before = ride(first, firstTrip, board, best.i)
  const after = ride(second, secondTrip, best.j, alight)
  const start = at(before.endTime)
  const walk: Ride = {
    mode: 'WALK',
    from: before.to,
    to: after.from,
    startTime: before.endTime,
    endTime: new Date(start + best.seconds * 1000).toISOString(),
    duration: best.seconds,
    distance: Math.round(haversineM(before.to, after.from)),
    geometry: { type: 'LineString', coordinates: [[before.to.lng, before.to.lat], [after.from.lng, after.from.lat]] },
    transitLeg: false,
  }
  return [before, walk, after]
}

export interface Tally<L extends Ride> {
  walkTime: number
  walkDistance: number
  transitTime: number
  waitingTime: number
  legs: L[]
}

/** Replace legs `start..end` (inclusive), moving the totals by the difference. */
export function spliceLegs<L extends Ride>(it: Tally<L>, start: number, end: number, legs: L[]): void {
  const removed = it.legs.slice(start, end + 1)
  const delta = (f: (l: Ride) => number) =>
    legs.reduce((s, l) => s + f(l), 0) - removed.reduce((s, l) => s + f(l), 0)
  const walked = delta((l) => (l.mode === 'WALK' ? l.duration : 0))
  const ridden = delta((l) => (l.transitLeg ? l.duration : 0))
  it.walkTime += walked
  it.walkDistance += delta((l) => (l.mode === 'WALK' ? l.distance : 0))
  it.transitTime += ridden
  it.waitingTime = Math.max(0, it.waitingTime - walked - ridden)
  it.legs.splice(start, end - start + 1, ...legs)
}
