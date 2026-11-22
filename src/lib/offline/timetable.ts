import {
  OFFLINE_PACKAGE_VERSION,
  type PackageBounds,
  type TimetablePackage,
  type TimetablePattern,
  type TimetableRoute,
  type TimetableStop,
  type TimetableTrip,
} from '../../types/offline-package.types'
import { haversineM, round6 } from './bbox'

export interface FeedStop {
  name: string
  lng: number
  lat: number
  parent?: string
}

export interface FeedCall {
  seq: number
  stopId: string
  arrival: number | null
  departure: number | null
}

export interface FeedTrip {
  routeId: string
  serviceId: string
  headsign: string
  directionId?: number
}

export interface FeedRoute {
  shortName: string
  longName: string
  type: number
  color?: string
  textColor?: string
  agency?: string
}

export interface CalendarRow {
  serviceId: string
  /** Monday first, as in calendar.txt. */
  weekdays: boolean[]
  start: string
  end: string
}

export interface CalendarDate {
  serviceId: string
  date: string
  /** 1 adds the service on the date, 2 removes it. */
  type: number
}

export interface FeedTransfer {
  from: string
  to: string
  seconds?: number
}

export interface FeedTimetable {
  feedId: string
  timezone: string
  /** Stops inside the region, plus the stations they belong to. */
  stops: Map<string, FeedStop>
  /** Each trip's calls at stops inside the region. */
  calls: Map<string, FeedCall[]>
  trips: Map<string, FeedTrip>
  routes: Map<string, FeedRoute>
  calendar: CalendarRow[]
  calendarDates: CalendarDate[]
  transfers: FeedTransfer[]
}

const WALK_M_PER_S = 1.2
const TRANSFER_SLACK_S = 60
const MIN_STATION_TRANSFER_S = 120
const MAX_ESTIMATED_TRANSFER_M = 1000

/** `HH:MM:SS` as seconds; GTFS hours run past 24 for trips after midnight. */
export function parseGtfsTime(value: string | undefined): number | null {
  const m = value?.trim().match(/^(\d+):(\d{2}):(\d{2})$/)
  return m ? Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]) : null
}

const toDate = (ymd: string) => new Date(Date.UTC(+ymd.slice(0, 4), +ymd.slice(4, 6) - 1, +ymd.slice(6, 8)))
const toYmd = (d: Date) => d.toISOString().slice(0, 10).replaceAll('-', '')

export function addDays(ymd: string, days: number): string {
  const d = toDate(ymd)
  d.setUTCDate(d.getUTCDate() + days)
  return toYmd(d)
}

/** Each service's running days as a bitmask over `days` days from `startDate`. */
export function serviceMasks(
  calendar: CalendarRow[],
  calendarDates: CalendarDate[],
  startDate: string,
  days: number,
): Map<string, number> {
  const masks = new Map<string, number>()
  const dates = Array.from({ length: days }, (_, d) => addDays(startDate, d))
  for (const row of calendar) {
    let mask = masks.get(row.serviceId) ?? 0
    dates.forEach((date, d) => {
      const weekday = (toDate(date).getUTCDay() + 6) % 7
      if (row.weekdays[weekday] && date >= row.start && date <= row.end) mask |= 1 << d
    })
    masks.set(row.serviceId, mask)
  }
  for (const { serviceId, date, type } of calendarDates) {
    const d = dates.indexOf(date)
    if (d < 0) continue
    const mask = masks.get(serviceId) ?? 0
    masks.set(serviceId, type === 1 ? mask | (1 << d) : mask & ~(1 << d))
  }
  return masks
}

/** Fill a missing time from its neighbours by sequence; drop calls that can't be timed. */
function timedCalls(calls: FeedCall[]): { stopId: string; arrival: number; departure: number }[] {
  const sorted = [...calls].sort((a, b) => a.seq - b.seq)
  const known = sorted.map(c => c.departure ?? c.arrival)
  const out: { stopId: string; arrival: number; departure: number }[] = []
  sorted.forEach((call, i) => {
    let time = known[i]
    if (time === null) {
      let before = i - 1
      while (before >= 0 && known[before] === null) before--
      let after = i + 1
      while (after < known.length && known[after] === null) after++
      if (after >= known.length) after = -1
      if (before < 0 || after < 0) return
      time = Math.round(known[before]! + ((known[after]! - known[before]!) * (i - before)) / (after - before))
    }
    out.push({ stopId: call.stopId, arrival: call.arrival ?? time, departure: call.departure ?? time })
  })
  return out
}

export function deltaEncode(values: number[]): number[] {
  return values.map((v, i) => (i === 0 ? v : v - values[i - 1]))
}

export function buildTimetable(
  feeds: FeedTimetable[],
  options: { bounds: PackageBounds; startDate: string; days: number; generatedAt?: string },
): TimetablePackage {
  const stops: TimetableStop[] = []
  const routes: TimetableRoute[] = []
  const patterns: TimetablePattern[] = []
  const trips: TimetableTrip[] = []
  const transfers: [number, number, number][] = []
  const patternIndex = new Map<string, number>()

  feeds.forEach((feed, feedIndex) => {
    const masks = serviceMasks(feed.calendar, feed.calendarDates, options.startDate, options.days)
    const stopIndex = new Map<string, number>()
    const routeIndex = new Map<string, number>()
    const usedStops = new Set<string>()
    const kept: { tripId: string; trip: FeedTrip; mask: number; calls: ReturnType<typeof timedCalls> }[] = []

    for (const [tripId, raw] of feed.calls) {
      const trip = feed.trips.get(tripId)
      const mask = trip ? (masks.get(trip.serviceId) ?? 0) : 0
      if (!trip || !mask || !feed.routes.has(trip.routeId)) continue
      const calls = timedCalls(raw)
      if (calls.length < 2) continue
      kept.push({ tripId, trip, mask, calls })
      for (const c of calls) usedStops.add(c.stopId)
    }

    const indexStop = (stopId: string): number => {
      const existing = stopIndex.get(stopId)
      if (existing !== undefined) return existing
      const stop = feed.stops.get(stopId)!
      const parent = stop.parent && feed.stops.has(stop.parent) ? indexStop(stop.parent) : undefined
      const index = stops.length
      stops.push({
        feed: feedIndex,
        stopId,
        name: stop.name,
        lng: round6(stop.lng),
        lat: round6(stop.lat),
        ...(parent !== undefined && { parent }),
      })
      stopIndex.set(stopId, index)
      return index
    }
    for (const stopId of [...usedStops].sort()) indexStop(stopId)

    kept.sort((a, b) => a.calls[0].departure - b.calls[0].departure)
    for (const { tripId, trip, mask, calls } of kept) {
      let route = routeIndex.get(trip.routeId)
      if (route === undefined) {
        const r = feed.routes.get(trip.routeId)!
        route = routes.length
        routes.push({ feed: feedIndex, routeId: trip.routeId, ...r })
        routeIndex.set(trip.routeId, route)
      }
      const stopSeq = calls.map(c => stopIndex.get(c.stopId)!)
      const key = [route, trip.headsign, trip.directionId ?? '', stopSeq.join('.')].join('|')
      let pattern = patternIndex.get(key)
      if (pattern === undefined) {
        pattern = patterns.length
        patterns.push({
          route,
          headsign: trip.headsign,
          ...(trip.directionId !== undefined && { directionId: trip.directionId }),
          stops: stopSeq,
        })
        patternIndex.set(key, pattern)
      }
      trips.push({ pattern, tripId, days: mask, times: deltaEncode(calls.flatMap(c => [c.arrival, c.departure])) })
    }

    const seen = new Set<string>()
    const addTransfer = (from: number, to: number, seconds: number) => {
      const key = `${from}>${to}`
      if (from === to || seen.has(key)) return
      seen.add(key)
      transfers.push([from, to, Math.round(seconds)])
    }
    const distance = (a: number, b: number) => haversineM(stops[a].lng, stops[a].lat, stops[b].lng, stops[b].lat)

    for (const t of feed.transfers) {
      const from = stopIndex.get(t.from)
      const to = stopIndex.get(t.to)
      if (from === undefined || to === undefined) continue
      if (t.seconds !== undefined) { addTransfer(from, to, t.seconds); continue }
      const m = distance(from, to)
      if (m <= MAX_ESTIMATED_TRANSFER_M) addTransfer(from, to, m / WALK_M_PER_S + TRANSFER_SLACK_S)
    }

    const byParent = new Map<number, number[]>()
    for (const [, index] of stopIndex) {
      const parent = stops[index].parent
      if (parent !== undefined) byParent.set(parent, [...(byParent.get(parent) ?? []), index])
    }
    for (const children of byParent.values()) {
      for (const a of children) {
        for (const b of children) {
          if (a !== b) addTransfer(a, b, Math.max(MIN_STATION_TRANSFER_S, distance(a, b) / WALK_M_PER_S + TRANSFER_SLACK_S))
        }
      }
    }
  })

  return {
    version: OFFLINE_PACKAGE_VERSION,
    bounds: options.bounds,
    generatedAt: options.generatedAt ?? new Date().toISOString(),
    startDate: options.startDate,
    days: options.days,
    feeds: feeds.map(f => ({ id: f.feedId, timezone: f.timezone })),
    stops,
    routes,
    patterns,
    trips,
    transfers,
  }
}
