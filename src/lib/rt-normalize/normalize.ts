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
 *
 * Updates go out in the order their trips run. MOTIS joins a vehicle block's
 * trips into one run and carries an update's delay on to the end of it, so a
 * late trip applied after the next one overwrites that trip's own times.
 */
import type { transit_realtime } from 'gtfs-realtime-bindings'
import {
  formatGtfsTime,
  localDate,
  serviceDayOrigin,
  shiftDate,
  type StaticIndex,
  type StaticTrip,
  tripPrefix,
} from './static-index'

type FeedMessage = transit_realtime.IFeedMessage
type FeedEntity = transit_realtime.IFeedEntity
type TripUpdate = transit_realtime.ITripUpdate
type TripDescriptor = transit_realtime.ITripDescriptor

const SCHEDULED = 0
const SKIPPED = 1
const ADDED = 1
const CANCELED = 3
const NEW = 8

/** Median gap between an update's times and a run's schedule beyond which they are different trains. */
export const MAX_MEDIAN_GAP_S = 15 * 60
/** Ranking credit for a run whose trip_id contains the update's, or the reverse: the same train renamed by a suffix. */
const SAME_ID_CREDIT_S = 3 * 60
/** Share of an update's stops a run has to serve; a train on another pattern is added, not squeezed onto this one. */
const MIN_STOP_COVERAGE = 0.9
/** How long past its scheduled start an unclaimed run may go before it is cancelled. */
const CANCEL_GRACE_S = 5 * 60
/** Share of a route's under-way runs the feed must claim before its silence counts as a cancellation. */
const MIN_ROUTE_COVERAGE = 0.5
/** Share of updates listing their run to its last stop for a feed's shorter lists to mean the train ends early. */
/** How late a run its update names by id and day may be; MARTA leaves trips hours old in its feed. */
const MAX_NAMED_GAP_S = 2 * 3600
/** Of the stops an update's list spans, the share it must name for the others to read as skipped. */
const MIN_LISTED_SHARE = 0.5
const COMPLETE_LISTS_SHARE = 0.8

export interface NormalizeStats {
  updates: number
  exact: number
  /** Named a scheduled trip but not its run's day (no start_date, or another day's), which MOTIS would misplace. */
  dated: number
  matched: number
  added: number
  cancelled: number
  /** Scheduled stops an update's own stop list passes over, marked SKIPPED. */
  skipped: number
  /** Updates that came before an earlier trip of their own block, which MOTIS would let it overwrite. */
  reordered: number
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
 * The scheduled run this update names by trip_id, or none. The feed's
 * start_date is not trusted on its own: some date an after-midnight run by the
 * calendar day it leaves on, and some leave it out. So the named trip's runs on
 * that day and the one before are weighed by how well they fit the update's
 * times, and one that does not fit is no match.
 */
function exactRun(index: StaticIndex, tu: TripUpdate, dates: string[], now: number): Run | null {
  const td = tu.trip!
  if (!td.tripId) return null
  const startMin = td.startTime ? Math.floor(parseClock(td.startTime) / 60) : null
  const days = td.startDate ? [shiftDate(td.startDate, -1), td.startDate] : dates
  const named = index.byTripId.get(td.tripId)
  const startsAt = (r: Run) => startMin === null || Math.floor(r.trip.times[0]! / 60) === startMin
  const ofNamed = runs(index, named ?? [], days)
  // MARTA states a start_time its timetable doesn't have; the times decide then.
  let candidates = ofNamed.some(startsAt) ? ofNamed.filter(startsAt) : ofNamed
  if (!named) {
    // CapMetro's realtime names `3000797_5797` for the timetable's `3000797_6158`.
    // Taken only when exactly one run of that prefix fits the day and start.
    const prefix = tripPrefix(td.tripId)
    const versions = prefix ? runs(index, index.byTripPrefix.get(prefix) ?? [], td.startDate ? [td.startDate] : days)
      .filter(r => startMin === null || Math.floor(r.trip.times[0]! / 60) === startMin) : []
    candidates = versions.length === 1 ? versions : []
  }
  const timed = (tu.stopTimeUpdate ?? []).some(s => s.stopId && stopTime(s) > 0)
  if (timed) {
    const scored = candidates.map(r => ({ r, gap: fit(r, tu.stopTimeUpdate!) })).filter(c => c.gap !== null)
    const best = scored.reduce<{ r: Run; gap: number | null } | null>((a, b) => (!a || b.gap! < a.gap! ? b : a), null)
    if (best && best.gap! <= MAX_MEDIAN_GAP_S) return best.r
    // Named by id, a train 40 minutes late is still that train: its stated
    // day's run, or with no day stated the nearest, the others being a day off.
    const dated = scored.filter(c => !td.startDate || c.r.date === td.startDate)
    const nearest = dated.reduce<(typeof dated)[number] | null>((a, b) => (!a || b.gap! < a.gap! ? b : a), null)
    return named && nearest && (td.startDate ? dated.length === 1 : true) && nearest.gap! <= MAX_NAMED_GAP_S ? nearest.r : null
  }
  // Nothing timed to weigh: the stated day, else the run nearest now.
  const stated = candidates.find(r => r.date === td.startDate)
  if (stated) return stated
  const middle = (r: Run) => Math.abs(r.origin + (r.trip.times[0]! + r.trip.times[r.trip.times.length - 1]!) / 2 - now)
  return candidates.reduce<Run | null>((best, r) => (!best || middle(r) < middle(best) ? r : best), null)
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
  const stats: NormalizeStats = { updates: 0, exact: 0, dated: 0, matched: 0, added: 0, cancelled: 0, skipped: 0, reordered: 0, unresolved: 0 }

  const entities = message.entity ?? []
  const claimed = new Map<string, Run>()
  const assigned = new Map<TripUpdate, Run>()
  const loose: TripUpdate[] = []
  const redated = new Map<TripUpdate, Run>()
  const placed = new Map<TripUpdate, Run>()
  const tracked = new Map<string, number>()

  for (const e of entities) {
    const tu = e.tripUpdate
    if (!tu?.trip) continue
    const sr = tu.trip.scheduleRelationship ?? SCHEDULED
    if (sr !== SCHEDULED && sr !== CANCELED) continue
    stats.updates++
    if (tu.trip.routeId) tracked.set(tu.trip.routeId, (tracked.get(tu.trip.routeId) ?? 0) + (underWay(tu, now) ? 1 : 0))
    const exact = exactRun(index, tu, dates, now)
    if (exact && !claimed.has(runKey(exact))) {
      claimed.set(runKey(exact), exact)
      placed.set(tu, exact)
      const asPublished = tu.trip.startDate === exact.date && tu.trip.tripId === exact.trip.tripId &&
        (!tu.trip.startTime || tu.trip.startTime === formatGtfsTime(exact.trip.times[0]!))
      if (asPublished) stats.exact++
      else redated.set(tu, exact)
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
    placed.set(tu, run)
    claimed.set(runKey(run), run)
  }

  // Only for runs the update names by id: a run matched on times may be a
  // similar train on another pattern, whose extra stops the real one never had.
  const omissions = new Map<TripUpdate, Omitted>()
  for (const [tu, run] of placed) {
    if (assigned.has(tu)) continue
    const o = omittedStops(run, tu)
    if (o) omissions.set(tu, o)
  }
  const all = [...omissions.values()]
  const share = (of: Omitted[], f: (o: Omitted) => boolean) => of.length > 0 && of.filter(f).length / of.length >= COMPLETE_LISTS_SHARE
  const listsWholeRun = share(all, o => !o.after.length)
  // Where most lists have a gap somewhere, a gap is how the feed writes, not a skip.
  const listsEveryStop = share(all.filter(o => o.dense), o => !o.between.length)
  const withSkips = (tu: TripUpdate): TripUpdate => {
    const o = omissions.get(tu)
    if (!o) return tu
    const skip = [...(listsEveryStop ? o.between : []), ...(listsWholeRun ? o.after : [])]
    if (!skip.length) return tu
    stats.skipped += skip.length
    const order = (id: string | null | undefined) => o.order.get(id ?? '') ?? Infinity
    const stopTimeUpdate = [...(tu.stopTimeUpdate ?? []), ...skip.map(stopId => ({ stopId, scheduleRelationship: SKIPPED }))]
      .sort((a, b) => order(a.stopId) - order(b.stopId))
    return { ...tu, stopTimeUpdate }
  }

  const out: FeedEntity[] = []
  const renamed = new Map<string, TripDescriptor>()
  for (const e of entities) {
    const tu = e.tripUpdate
    const run = tu && (redated.get(tu) ?? assigned.get(tu))
    if (tu && run) {
      const trip = restate(run, tu.trip!)
      if (tu.trip!.tripId) renamed.set(tu.trip!.tripId, trip)
      out.push({ ...e, tripUpdate: { ...withSkips(tu), trip } })
      if (redated.has(tu)) stats.dated++
      else stats.matched++
      continue
    }
    if (tu && placed.has(tu)) {
      const skipped = withSkips(tu)
      out.push(skipped === tu ? e : { ...e, tripUpdate: skipped })
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

  stats.reordered = outOfBlockOrder(entities, placed)
  return { message: { ...message, entity: inRunningOrder(out) }, stats }
}

const firstTime = (e: FeedEntity): number => {
  for (const s of e.tripUpdate?.stopTimeUpdate ?? []) {
    const t = stopTime(s)
    if (t > 0) return t
  }
  return Infinity
}

/** Trip updates by when they first stop; everything else ahead, in place. */
function inRunningOrder(entities: FeedEntity[]): FeedEntity[] {
  const key = (e: FeedEntity) => (e.tripUpdate ? firstTime(e) : -Infinity)
  return entities.map((e, i) => ({ e, i, k: key(e) })).sort((a, b) => a.k - b.k || a.i - b.i).map(x => x.e)
}

function outOfBlockOrder(entities: FeedEntity[], placed: Map<TripUpdate, Run>): number {
  const latest = new Map<string, number>()
  let count = 0
  for (const e of entities) {
    const run = e.tripUpdate && placed.get(e.tripUpdate)
    if (!run?.trip.blockId) continue
    const block = `${run.trip.blockId}|${run.date}`
    const start = run.origin + run.trip.times[0]!
    if (start < (latest.get(block) ?? -Infinity)) count++
    latest.set(block, Math.max(start, latest.get(block) ?? -Infinity))
  }
  return count
}

interface Omitted {
  /** Lists at least half the stops it spans, over more than one. */
  dense: boolean
  /** Scheduled stops between the first and last the update lists that it leaves out. */
  between: string[]
  /** Scheduled stops after the last it lists. */
  after: string[]
  /** Each scheduled stop's position on the run. */
  order: Map<string, number>
}

/**
 * The scheduled stops an update's own stop list passes over. Only for an
 * update whose stops nearly all lie on the run, so a feed naming its stops
 * differently never reads as skipping them.
 */
function omittedStops(run: Run, tu: TripUpdate): Omitted | null {
  const listed = (tu.stopTimeUpdate ?? []).map(s => s.stopId).filter((id): id is string => !!id)
  const positions = listed.map(id => run.trip.stopIds.indexOf(id)).filter(i => i >= 0)
  if (!positions.length || positions.length / listed.length < MIN_STOP_COVERAGE) return null
  const first = Math.min(...positions)
  const last = Math.max(...positions)
  const have = new Set(listed)
  const stops = run.trip.stopIds
  // A list of timepoints, or of the next stop and the last, passes over stops it isn't skipping.
  const dense = new Set(positions).size / (last - first + 1) >= MIN_LISTED_SHARE
  return {
    dense: dense && last > first,
    between: dense ? stops.slice(first, last + 1).filter(id => !have.has(id)) : [],
    after: stops.slice(last + 1),
    order: new Map(stops.map((id, i) => [id, i])),
  }
}

function asNewTrip(index: StaticIndex, tu: TripUpdate): TripUpdate | null {
  const td = tu.trip!
  if (!td.routeId || !index.routeIds.has(td.routeId)) return null
  // A trip the timetable has is not new, however far its times have run from it.
  if (td.tripId && index.byTripId.has(td.tripId)) return null
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

/** Whether an update's train has left its first listed stop. */
function underWay(tu: TripUpdate, now: number) {
  const first = tu.stopTimeUpdate?.[0]
  const t = num(first?.departure?.time) || num(first?.arrival?.time)
  return !!t && t <= now
}

/**
 * Runs that should be moving now, on routes whose trains the feed reports,
 * that no update accounts for. Silence only means "not running" on a route
 * the feed demonstrably covers: one reporting at least half as many moving
 * trains as the timetable has under way. Trains the timetable lacks count,
 * since a service change replaces runs with ones it doesn't have.
 */
function silentRuns(
  index: StaticIndex,
  dates: string[],
  now: number,
  tracked: Map<string, number>,
  claimed: Map<string, Run>,
): Run[] {
  const out: Run[] = []
  for (const [routeId, moving] of tracked) {
    const underway = runs(index, index.byRoute.get(routeId) ?? [], dates).filter(r => {
      const t = r.trip.times
      return r.origin + t[0]! <= now - CANCEL_GRACE_S && r.origin + t[t.length - 1]! > now
    })
    const silent = underway.filter(r => !claimed.has(runKey(r)))
    const reported = Math.max(underway.length - silent.length, moving)
    if (underway.length && reported / underway.length >= MIN_ROUTE_COVERAGE) out.push(...silent)
  }
  return out
}
