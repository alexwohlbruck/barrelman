import { serviceDayOrigin } from './static-index'
import { staticTripId, type LiveFeed } from './exact-times'

/** How long a run may go unreported after its scheduled start before it reads as not running. */
const START_GRACE_S = 5 * 60
/** Share of a route's started runs on a board the agency must report before its silence means anything. */
const MIN_ROUTE_COVERAGE = 0.5

export interface BoardRun {
  tripId: string
  routeKey: string
  live: boolean
}

/** When a MOTIS run left its first stop, from the date and time its id begins with. */
export function runStart(motisTripId: string, timezone: string): number | undefined {
  const m = /^(\d{8})_(\d{1,2}):(\d{2})_/.exec(motisTripId)
  return m ? serviceDayOrigin(m[1]!, timezone) + +m[2]! * 3600 + +m[3]! * 60 : undefined
}

/**
 * Timetable runs on a board that are under way yet absent from the agency's
 * feed, on routes where the feed reports most of the board's under-way runs:
 * the timetable promises them, the agency says nothing is running.
 */
export function untrackedRuns(runs: BoardRun[], live: LiveFeed | undefined, timezone: string, now: number): Set<string> {
  const out = new Set<string>()
  if (!live?.trips.size) return out
  const started = runs.filter(r => (runStart(r.tripId, timezone) ?? Infinity) <= now - START_GRACE_S)
  const byRoute = new Map<string, BoardRun[]>()
  for (const r of started) byRoute.set(r.routeKey, [...(byRoute.get(r.routeKey) ?? []), r])
  for (const route of byRoute.values()) {
    const silent = route.filter(r => !r.live && !live.trips.has(staticTripId(r.tripId)))
    if ((route.length - silent.length) / route.length >= MIN_ROUTE_COVERAGE) for (const r of silent) out.add(r.tripId)
  }
  return out
}
