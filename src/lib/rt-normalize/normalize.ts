/**
 * Rewrite a GTFS-RT message so MOTIS can apply all of it.
 *
 * MOTIS attaches a trip update to the timetable only on an exact trip_id and,
 * when given, an exact start_time. Feeds that publish their own ids, the actual
 * dispatch time, or trains the schedule never had lose those updates: the
 * train vanishes and the scheduled trip it replaced keeps its timetable times,
 * a departure that never comes. This matches each update to a scheduled run by
 * where and when it stops, not by id, and says what the feed implies:
 *
 *   - matched: the update is restated with that run's trip_id and start time;
 *   - unmatched: the train is added as a NEW trip with the feed's own times;
 *   - a run that should be under way on a route the feed tracks, but that no
 *     update claims, is cancelled.
 */
import type { transit_realtime } from 'gtfs-realtime-bindings'
import {
  formatGtfsTime,
  localDate,
  serviceDayOrigin,
  shiftDate,
  type StaticIndex,
  type StaticTrip,
} from './static-index'

type FeedMessage = transit_realtime.IFeedMessage
type FeedEntity = transit_realtime.IFeedEntity
type TripUpdate = transit_realtime.ITripUpdate
type TripDescriptor = transit_realtime.ITripDescriptor

const SCHEDULED = 0
const ADDED = 1
const CANCELED = 3
const NEW = 8

/** Median gap between an update's times and a run's schedule beyond which they are different trains. */
export const MAX_MEDIAN_GAP_S = 15 * 60
/** Ranking credit for a run whose trip_id contains the update's, or the reverse: the same train renamed by a suffix. */
const SAME_ID_CREDIT_S = 3 * 60
/** Share of an update's stops a run has to serve. */
const MIN_STOP_COVERAGE = 0.6
/** How long past its scheduled start an unclaimed run may go before it is cancelled. */
const CANCEL_GRACE_S = 5 * 60
/** Share of a route's under-way runs the feed must claim before its silence counts as a cancellation. */
const MIN_ROUTE_COVERAGE = 0.5

export interface NormalizeStats {
  updates: number
  exact: number
  /** Named a scheduled trip without a start_date, which MOTIS reads as today's UTC date. */
  dated: number
  matched: number
  added: number
  cancelled: number
  unresolved: number
}

interface Run {
  trip: StaticTrip
  date: string
  origin: number
}

const num = (v: unknown): number => (v == null ? NaN : Number(v))

function stopTime(stu: transit_realtime.TripUpdate.IStopTimeUpdate): number {
  const dep = num(stu.departure?.time)
  return dep > 0 ? dep : num(stu.arrival?.time)
}

const runKey = (r: Run) => `${r.trip.tripId}|${r.trip.serviceId}|${r.date}`

function runs(index: StaticIndex, trips: StaticTrip[], dates: string[]): Run[] {
  const out: Run[] = []
  for (const date of dates) {
    const origin = serviceDayOrigin(date, index.timezone)
    for (const trip of trips) {
      if (index.serviceDates.get(trip.serviceId)?.has(date)) out.push({ trip, date, origin })
    }
  }
  return out
}

/**
 * The scheduled run this update names by trip_id, or none. Without a
 * start_date, the run on whichever recent service day its times fit.
 */
function exactRun(index: StaticIndex, tu: TripUpdate, dates: string[], now: number): Run | null {
  const td = tu.trip!
  if (!td.tripId) return null
  const startMin = td.startTime ? Math.floor(parseClock(td.startTime) / 60) : null
  const candidates = runs(index, index.byTripId.get(td.tripId) ?? [], td.startDate ? [td.startDate] : dates)
    .filter(r => startMin === null || Math.floor(r.trip.times[0]! / 60) === startMin)
  if (candidates.length < 2) return candidates[0] ?? null
  const distance = (r: Run) =>
    fit(r, tu.stopTimeUpdate ?? []) ?? Math.abs(r.origin + (r.trip.times[0]! + r.trip.times[r.trip.times.length - 1]!) / 2 - now)
  return candidates.reduce((best, r) => (distance(r) < distance(best) ? r : best))
}

function parseClock(s: string): number {
  const [h = 0, m = 0, sec = 0] = s.split(':').map(Number)
  return h * 3600 + m * 60 + sec
}

/** Median absolute gap between the update's times and the run's, or null when they share too few stops. */
export function fit(run: Run, stus: transit_realtime.TripUpdate.IStopTimeUpdate[]): number | null {
  const gaps: number[] = []
  let timed = 0
  for (const stu of stus) {
    const t = stopTime(stu)
    if (!(t > 0) || !stu.stopId) continue
    timed++
    const i = run.trip.stopIds.indexOf(stu.stopId)
    if (i >= 0) gaps.push(Math.abs(run.origin + run.trip.times[i]! - t))
  }
  if (!timed || gaps.length < Math.min(2, timed) || gaps.length / timed < MIN_STOP_COVERAGE) return null
  gaps.sort((a, b) => a - b)
  return gaps[gaps.length >> 1]!
}

function sameTrainId(rtId: string | null | undefined, staticId: string): boolean {
  return !!rtId && (staticId.includes(rtId) || rtId.includes(staticId))
}

function restate(run: Run, td: TripDescriptor): TripDescriptor {
  return {
    tripId: run.trip.tripId,
    routeId: run.trip.routeId,
    startDate: run.date,
    startTime: formatGtfsTime(run.trip.times[0]!),
    scheduleRelationship: td.scheduleRelationship === CANCELED ? CANCELED : SCHEDULED,
  }
}

export function normalizeFeed(
  index: StaticIndex,
  message: FeedMessage,
  now: number = num(message.header?.timestamp) || Date.now() / 1000,
): { message: FeedMessage; stats: NormalizeStats } {
  const today = localDate(now, index.timezone)
  const dates = [shiftDate(today, -1), today]
  const stats: NormalizeStats = { updates: 0, exact: 0, dated: 0, matched: 0, added: 0, cancelled: 0, unresolved: 0 }

  const entities = message.entity ?? []
  const claimed = new Map<string, Run>()
  const assigned = new Map<TripUpdate, Run>()
  const loose: TripUpdate[] = []
  const undated = new Map<TripUpdate, Run>()
  const tracked = new Set<string>()

  for (const e of entities) {
    const tu = e.tripUpdate
    if (!tu?.trip) continue
    const sr = tu.trip.scheduleRelationship ?? SCHEDULED
    if (sr !== SCHEDULED && sr !== CANCELED) continue
    stats.updates++
    if (tu.trip.routeId) tracked.add(tu.trip.routeId)
    const exact = exactRun(index, tu, dates, now)
    if (exact && !claimed.has(runKey(exact))) {
      claimed.set(runKey(exact), exact)
      if (tu.trip.startDate) stats.exact++
      else undated.set(tu, exact)
    } else if (tu.stopTimeUpdate?.length) {
      loose.push(tu)
    } else {
      stats.unresolved++
    }
  }

  // Cheapest pairs first, each update and each run used once: two trains a
  // few minutes apart must not both claim the nearer schedule.
  const pairs: { tu: TripUpdate; run: Run; cost: number }[] = []
  for (const tu of loose) {
    const td = tu.trip!
    const pool = td.routeId ? index.byRoute.get(td.routeId) ?? [] : index.trips
    const sameDir = td.directionId == null ? pool : pool.filter(t => t.directionId < 0 || t.directionId === td.directionId)
    // A train dispatched just after midnight may carry the next day's date
    // while running the previous service day's trip.
    for (const run of runs(index, sameDir, td.startDate ? [shiftDate(td.startDate, -1), td.startDate] : dates)) {
      if (claimed.has(runKey(run))) continue
      const gap = fit(run, tu.stopTimeUpdate!)
      if (gap === null || gap > MAX_MEDIAN_GAP_S) continue
      pairs.push({ tu, run, cost: sameTrainId(td.tripId, run.trip.tripId) ? gap - SAME_ID_CREDIT_S : gap })
    }
  }
  pairs.sort((a, b) => a.cost - b.cost)
  for (const { tu, run } of pairs) {
    if (assigned.has(tu) || claimed.has(runKey(run))) continue
    assigned.set(tu, run)
    claimed.set(runKey(run), run)
  }

  const out: FeedEntity[] = []
  const renamed = new Map<string, TripDescriptor>()
  for (const e of entities) {
    const tu = e.tripUpdate
    const run = tu && (undated.get(tu) ?? assigned.get(tu))
    if (tu && run) {
      const trip = restate(run, tu.trip!)
      if (tu.trip!.tripId) renamed.set(tu.trip!.tripId, trip)
      out.push({ ...e, tripUpdate: { ...tu, trip } })
      if (undated.has(tu)) stats.dated++
      else stats.matched++
      continue
    }
    if (!tu || !loose.includes(tu)) { out.push(e); continue }
    const added = asNewTrip(index, tu)
    if (added) {
      out.push({ ...e, tripUpdate: added })
      stats.added++
    } else {
      out.push(e)
      stats.unresolved++
    }
  }

  // Vehicle positions name trips by the feed's ids too.
  for (let i = 0; i < out.length; i++) {
    const v = out[i]!.vehicle
    const trip = v?.trip?.tripId ? renamed.get(v.trip.tripId) : undefined
    if (v && trip) out[i] = { ...out[i], vehicle: { ...v, trip } }
  }

  for (const run of silentRuns(index, dates, now, tracked, claimed)) {
    out.push({
      id: `cancel:${runKey(run)}`,
      tripUpdate: { trip: { ...restate(run, {}), scheduleRelationship: CANCELED } },
    })
    stats.cancelled++
  }

  return { message: { ...message, entity: out }, stats }
}

function asNewTrip(index: StaticIndex, tu: TripUpdate): TripUpdate | null {
  const td = tu.trip!
  if (!td.routeId || !index.routeIds.has(td.routeId)) return null
  const stus = (tu.stopTimeUpdate ?? []).filter(s => s.stopId && index.stopIds.has(s.stopId) && stopTime(s) > 0)
  if (stus.length < 2) return null
  return {
    ...tu,
    trip: {
      tripId: `${td.tripId ?? 'trip'}:${td.startDate ?? ''}`,
      routeId: td.routeId,
      startDate: td.startDate,
      scheduleRelationship: td.scheduleRelationship === ADDED ? ADDED : NEW,
      ...(td.directionId != null && { directionId: td.directionId }),
    },
    stopTimeUpdate: stus,
  }
}

/**
 * Runs that should be moving now, on routes whose trains the feed reports,
 * that no update accounts for. Silence only means "not running" on a route
 * the feed demonstrably covers, so a route where it claims under half of the
 * under-way runs is left alone.
 */
function silentRuns(
  index: StaticIndex,
  dates: string[],
  now: number,
  tracked: Set<string>,
  claimed: Map<string, Run>,
): Run[] {
  const out: Run[] = []
  for (const routeId of tracked) {
    const underway = runs(index, index.byRoute.get(routeId) ?? [], dates).filter(r => {
      const t = r.trip.times
      return r.origin + t[0]! <= now - CANCEL_GRACE_S && r.origin + t[t.length - 1]! > now
    })
    const silent = underway.filter(r => !claimed.has(runKey(r)))
    if (underway.length && (underway.length - silent.length) / underway.length >= MIN_ROUTE_COVERAGE) out.push(...silent)
  }
  return out
}
