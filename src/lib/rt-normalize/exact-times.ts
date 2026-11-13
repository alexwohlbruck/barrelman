import type { transit_realtime } from 'gtfs-realtime-bindings'

/** A predicted call, in epoch seconds. */
export interface ExactCall {
  arrival?: number
  departure?: number
}

/** Beyond this from MOTIS's time, a prediction is for some other run of the trip id. */
const MAX_DRIFT_S = 15 * 60

const key = (tripId: string, stopId: string) => `${tripId}|${stopId}`

/** What a feed says now: every timed stop prediction, by trip id and stop id, and every trip it reports running. */
export interface LiveFeed {
  calls: Map<string, ExactCall>
  trips: Set<string>
  routes: Set<string>
}

export function indexCalls(message: transit_realtime.IFeedMessage): LiveFeed {
  const calls = new Map<string, ExactCall>()
  const trips = new Set<string>()
  const routes = new Set<string>()
  for (const entity of message.entity ?? []) {
    const tu = entity.tripUpdate
    const tripId = tu?.trip?.tripId
    if (!tu || !tripId || tu.trip.scheduleRelationship === 3) continue
    trips.add(tripId)
    if (tu.trip.routeId) routes.add(tu.trip.routeId)
    for (const s of tu.stopTimeUpdate ?? []) {
      if (!s.stopId || s.scheduleRelationship === 1) continue
      const arrival = Number(s.arrival?.time) || undefined
      const departure = Number(s.departure?.time) || undefined
      if (arrival || departure) calls.set(key(tripId, s.stopId), { arrival, departure })
    }
  }
  return { calls, trips, routes }
}

/** The static trip id inside a MOTIS trip id (`20261112_19:08_5_<trip_id>`). */
export const staticTripId = (motisTripId: string) => motisTripId.replace(/^\d{8}_\d{1,2}:\d{2}_[^_]+_/, '')

export function exactCall(live: LiveFeed | undefined, motisTripId: string, stopId: string) {
  return live?.calls.get(key(staticTripId(motisTripId), stopId))
}

/**
 * MOTIS keeps whole minutes, floored, and is a poll cycle behind the agency,
 * or misses an update outright; the agency's own prediction, to the second,
 * when it is for this run.
 */
export function refineTime(motisIso: string | undefined, exact: number | undefined): string | undefined {
  return fits(motisIso, exact) ? new Date(exact! * 1000).toISOString() : motisIso
}

/** Whether a prediction is for the run MOTIS placed at `motisIso`. */
export function fits(motisIso: string | undefined, exact: number | undefined): boolean {
  if (!motisIso || !exact) return false
  const motis = Date.parse(motisIso) / 1000
  return Number.isFinite(motis) && Math.abs(exact - motis) <= MAX_DRIFT_S
}
