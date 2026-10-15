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

/** Route ids a realtime feed's trip updates and vehicles refer to. */
function routesOf(buf: Uint8Array): Set<string> {
  const routes = new Set<string>()
  for (const entity of decodeFeedMessage(buf).entity ?? []) {
    const routeId = entity.tripUpdate?.trip?.routeId ?? entity.vehicle?.trip?.routeId
    if (routeId) routes.add(routeId)
  }
  return routes
}

type Verdict = 'keep' | 'unauthorized' | 'foreign-routes'

async function judge(
  rt: SelectableRtUrl,
  staticRoutes: Set<string>,
  fetchFn: FetchFn,
): Promise<Verdict> {
  try {
    const response = await fetchFn(rt.url, {
      headers: { ...(rt.headers ?? {}) },
      signal: AbortSignal.timeout(15_000),
    })
    if (response.status === 401 || response.status === 403) return 'unauthorized'
    if (!response.ok || rt.type === 'alerts') return 'keep'

    const routes = routesOf(new Uint8Array(await response.arrayBuffer()))
    if (!routes.size || !staticRoutes.size) return 'keep'
    for (const route of routes) if (staticRoutes.has(route)) return 'keep'
    return 'foreign-routes'
  } catch {
    return 'keep'
  }
}

export async function selectRtUrls<T extends SelectableRtUrl>(
  rtUrls: T[],
  staticRoutes: Set<string>,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<RtUrlSelection<T>> {
  // Trip updates and vehicle positions often share one URL; fetch it once.
  const verdicts = new Map<string, Promise<Verdict>>()
  for (const rt of rtUrls) {
    if (!verdicts.has(rt.url)) verdicts.set(rt.url, judge(rt, staticRoutes, fetchFn))
  }

  const selection: RtUrlSelection<T> = { kept: [], dropped: [] }
  const reported = new Set<string>()
  for (const rt of rtUrls) {
    const verdict = await verdicts.get(rt.url)!
    if (verdict === 'keep') {
      selection.kept.push(rt)
    } else if (!reported.has(rt.url)) {
      reported.add(rt.url)
      selection.dropped.push({ url: rt.url, reason: verdict })
    }
  }
  return selection
}
