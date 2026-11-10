/**
 * Keep only the realtime URLs that actually serve a given static feed.
 *
 * Discovery joins a static feed to its realtime through the operator, and an
 * operator lists every realtime feed it publishes: MTA New York City Transit
 * hands its subway feed the Bus Time feed, and each borough's bus feed the
 * eight subway feeds. A regional aggregator's realtime lands on every member
 * agency, often behind a key nobody configured. Attached as-is, MOTIS polls
 * all of them for every dataset and resolves none of the foreign trips.
 *
 * So each candidate is fetched once and judged by what it publishes: a URL
 * that refuses us for lack of credentials is dropped, and so is one whose
 * trips run on none of the static feed's routes. Anything that can't be
 * judged — a timeout, an empty feed overnight, entities with no route — is
 * kept, so a transient failure never costs a feed its realtime.
 */
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import type { FetchFn } from './transit.service'

const decodeFeedMessage = (buf: Uint8Array) =>
  GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buf)

export interface SelectableRtUrl {
  url: string
  headers?: Record<string, string>
  type?: string
}

export interface RtUrlSelection<T extends SelectableRtUrl> {
  kept: T[]
  dropped: Array<{ url: string; reason: 'unauthorized' | 'foreign-routes' }>
}

/** Route, trip and stop ids a realtime feed's trip updates and vehicles refer to. */
function idsOf(buf: Uint8Array) {
  const ids = { routes: new Set<string>(), trips: new Set<string>(), stops: new Set<string>() }
  for (const entity of decodeFeedMessage(buf).entity ?? []) {
    const trip = entity.tripUpdate?.trip ?? entity.vehicle?.trip
    if (trip?.routeId) ids.routes.add(trip.routeId)
    if (trip?.tripId) ids.trips.add(trip.tripId)
    for (const stu of entity.tripUpdate?.stopTimeUpdate ?? []) if (stu.stopId) ids.stops.add(stu.stopId)
  }
  return ids
}

/** A static feed's trip and stop ids, to judge a realtime feed that names no routes. */
export interface StaticTripsAndStops {
  trips: Set<string>
  stops: Set<string>
}

const overlaps = (a: Set<string>, b: Set<string>) => [...a].some(x => b.has(x))

type Verdict = { keep: true; movedTo?: string } | { keep: false; reason: 'unauthorized' | 'foreign-routes' }

async function judge(
  rt: SelectableRtUrl,
  staticRoutes: Set<string>,
  fetchFn: FetchFn,
  staticTripsAndStops?: StaticTripsAndStops,
): Promise<Verdict> {
  try {
    const response = await fetchFn(rt.url, {
      headers: { ...(rt.headers ?? {}) },
      signal: AbortSignal.timeout(15_000),
    })
    if (response.status === 401 || response.status === 403) return { keep: false, reason: 'unauthorized' }
    if (!response.ok) return { keep: true }

    const ids = idsOf(new Uint8Array(await response.arrayBuffer()))
    // MOTIS does not follow redirects, so a feed that moved is polled where it
    // went, once that answers with GTFS-RT rather than a parked-domain page.
    const kept: Verdict = { keep: true, ...(response.redirected && response.url && { movedTo: response.url }) }
    if (rt.type === 'alerts') return kept
    if (ids.routes.size && staticRoutes.size) {
      return overlaps(ids.routes, staticRoutes) ? kept : { keep: false, reason: 'foreign-routes' }
    }
    // MARC's feed names no routes, and reached every Maryland bus and metro feed through their operator.
    const s = staticTripsAndStops
    if (!s || !ids.trips.size || !s.trips.size || overlaps(ids.trips, s.trips) || overlaps(ids.stops, s.stops)) return kept
    return { keep: false, reason: 'foreign-routes' }
  } catch {
    return { keep: true }
  }
}

export async function selectRtUrls<T extends SelectableRtUrl>(
  rtUrls: T[],
  staticRoutes: Set<string>,
  fetchFn: FetchFn = globalThis.fetch,
  staticTripsAndStops?: StaticTripsAndStops,
): Promise<RtUrlSelection<T>> {
  // Trip updates and vehicle positions often share one URL; fetch it once.
  const verdicts = new Map<string, Promise<Verdict>>()
  for (const rt of rtUrls) {
    if (!verdicts.has(rt.url)) verdicts.set(rt.url, judge(rt, staticRoutes, fetchFn, staticTripsAndStops))
  }

  const selection: RtUrlSelection<T> = { kept: [], dropped: [] }
  const reported = new Set<string>()
  for (const rt of rtUrls) {
    const verdict = await verdicts.get(rt.url)!
    if (verdict.keep) {
      selection.kept.push(verdict.movedTo ? { ...rt, url: verdict.movedTo } : rt)
    } else if (!reported.has(rt.url)) {
      reported.add(rt.url)
      selection.dropped.push({ url: rt.url, reason: verdict.reason })
    }
  }
  return selection
}
