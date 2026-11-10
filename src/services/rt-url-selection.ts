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
 * that refuses us for lack of credentials is dropped, and so is one that is
 * gone, has stopped updating, or whose trips run on none of the static feed's
 * routes. Anything that can't be
 * judged — a timeout, an empty feed overnight, entities with no route — is
 * kept, so a transient failure never costs a feed its realtime.
 */
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import type { FetchFn } from './transit.service'
import { envRaw } from '../config/env'

/**
 * An HTTP proxy for agencies that refuse this server's network. Several
 * US agencies drop connections from European datacenters.
 */
export const rtEgressProxy = () => envRaw('RT_EGRESS_PROXY')

const decodeFeedMessage = (buf: Uint8Array) =>
  GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buf)

export interface SelectableRtUrl {
  url: string
  headers?: Record<string, string>
  type?: string
  redirects?: boolean
  egress?: boolean
  flaky?: boolean
}

export interface RtUrlSelection<T extends SelectableRtUrl> {
  kept: T[]
  dropped: Array<{ url: string; reason: DropReason }>
}

/** Route, trip and stop ids a realtime feed's trip updates and vehicles refer to. */
function idsOf(buf: Uint8Array) {
  const message = decodeFeedMessage(buf)
  const ids = { routes: new Set<string>(), trips: new Set<string>(), stops: new Set<string>(), timestamp: Number(message.header?.timestamp ?? 0) }
  for (const entity of message.entity ?? []) {
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

type DropReason = 'unauthorized' | 'foreign-routes' | 'gone' | 'stale'
type Verdict = { keep: true; movedTo?: string; redirects?: true; egress?: true; flaky?: true } | { keep: false; reason: DropReason }

/** A feed whose header is older than this has stopped updating at that address. */
const STALE_AFTER_S = 12 * 3600

async function movedPermanently(rt: SelectableRtUrl, fetchFn: FetchFn): Promise<boolean> {
  const response = await fetchFn(rt.url, { headers: { ...(rt.headers ?? {}) }, redirect: 'manual', signal: AbortSignal.timeout(15_000) })
  await response.body?.cancel()
  return response.status === 301 || response.status === 308
}

async function judge(
  rt: SelectableRtUrl,
  staticRoutes: Set<string>,
  fetchFn: FetchFn,
  staticTripsAndStops?: StaticTripsAndStops,
): Promise<Verdict> {
  const init = { headers: { ...(rt.headers ?? {}) } }
  let response: Response
  let egress = false
  try {
    response = await fetchFn(rt.url, { ...init, signal: AbortSignal.timeout(15_000) })
  } catch {
    const proxy = rtEgressProxy()
    if (!proxy) return { keep: true }
    try {
      response = await fetchFn(rt.url, { ...init, proxy, signal: AbortSignal.timeout(15_000) } as RequestInit)
      egress = true
    } catch {
      return { keep: true }
    }
  }
  try {
    if (response.status === 401 || response.status === 403) return { keep: false, reason: 'unauthorized' }
    if (response.status === 404 || response.status === 410) return { keep: false, reason: 'gone' }
    if (!response.ok) return { keep: true }

    const body = new Uint8Array(await response.arrayBuffer())
    let ids: ReturnType<typeof idsOf>
    try {
      ids = idsOf(body)
    } catch {
      // A page, not a feed: nothing to judge.
      if (body[0] === 0x3c) return { keep: true }
      // San Antonio serves a half-written file about one fetch in four.
      return { keep: true, flaky: true, ...(egress && { egress: true as const }) }
    }
    // MOTIS does not follow redirects. A feed that moved for good is polled
    // where it went, once that answers with GTFS-RT rather than a parked-domain
    // page; one that redirects each time (CapMetro, to that hour's upload) is
    // polled through the relay, which follows it.
    const kept: Verdict = { keep: true, ...(egress && { egress: true as const }) }
    if (response.redirected && response.url && !egress) {
      if (await movedPermanently(rt, fetchFn)) kept.movedTo = response.url
      else kept.redirects = true
    }
    if (ids.timestamp > 0 && Date.now() / 1000 - ids.timestamp > STALE_AFTER_S) return { keep: false, reason: 'stale' }
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
      const { redirects: _r, egress: _e, ...base } = rt
      selection.kept.push({
        ...base,
        ...(verdict.movedTo && { url: verdict.movedTo }),
        ...(verdict.redirects && { redirects: true }),
        ...(verdict.egress && { egress: true }),
        ...((verdict.flaky || rt.flaky) && { flaky: true }),
      } as T)
    } else if (!reported.has(rt.url)) {
      reported.add(rt.url)
      selection.dropped.push({ url: rt.url, reason: verdict.reason })
    }
  }
  return selection
}
