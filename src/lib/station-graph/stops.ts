/**
 * From OSM platforms to GTFS stops: which platform each stop boards from, and
 * the time between two stops of one complex, for transfers.txt.
 */
import { straightLinePair } from './estimate'
import type { StationPathParams } from './params'
import { SOURCES, type PathSource } from './types'

export interface StopCandidate {
  platformId: string
  complexId: string
  distanceM: number
  refs: string[]
  gtfsStopIds: string[]
}

export interface GtfsStopRef {
  stopId: string
  parentId: string | null
  /** Short names of the routes calling here. */
  routes: string[]
}

/** How far from a stop its platform can be: stop coordinates often sit on the station, not the platform. */
export const STOP_MATCH_M = 150
/** With nothing to tie them, only a platform this close is taken as the stop's. */
export const STOP_NEAREST_M = 40

const norm = (s: string) => s.trim().toUpperCase()

/**
 * The platforms a stop boards from. A platform that names the stop
 * (`gtfs:stop_id`) settles it; otherwise the nearby platforms serving one of
 * the stop's routes (`route_ref`, or NYC's "(1,2,3)" names); otherwise the
 * nearest unlabelled platform close by. A platform labelled for other lines
 * is never taken.
 */
export function platformsForStop(stop: GtfsStopRef, candidates: StopCandidate[]): string[] {
  const near = candidates.filter((c) => c.distanceM <= STOP_MATCH_M)
  const named = near.filter((c) => c.gtfsStopIds.includes(stop.stopId))
  if (named.length) return named.map((c) => c.platformId)
  const routes = new Set(stop.routes.map(norm))
  const serving = near.filter((c) => c.refs.some((r) => routes.has(norm(r))))
  if (serving.length) return [...new Set(serving.map((c) => c.platformId))]
  const unlabelled = near.filter((c) => !c.refs.length && c.distanceM <= STOP_NEAREST_M).sort((a, b) => a.distanceM - b.distanceM)
  return unlabelled.length ? [unlabelled[0].platformId] : []
}

export interface PlatformTime {
  seconds: number
  source: PathSource
}

export interface StopTime extends PlatformTime {
  /** Seconds error at p90 for the source. */
  uncertaintyS: number
}

const rank = (s: PathSource) => SOURCES.indexOf(s)

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b)
  const m = s.length >> 1
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

/**
 * The time between two stops of one complex. Over every pair of their
 * platforms, the best source present wins and its times are averaged
 * (median), since a stop with two platforms may board from either. Stops
 * that share a platform are a cross-platform change, which this leaves to
 * the feed. Where either stop has no OSM platform, the straight line between
 * the stops is the estimate.
 */
export function stopPairTime(
  params: StationPathParams,
  a: { platforms: string[] },
  b: { platforms: string[] },
  straightM: number,
  lookup: (from: string, to: string) => PlatformTime | undefined,
): StopTime | null {
  if (!a.platforms.length || !b.platforms.length) {
    return { seconds: Math.round(straightLinePair(params, straightM)), source: 'straight_line', uncertaintyS: params.uncertaintyS.straight_line.pair }
  }
  if (a.platforms.some((p) => b.platforms.includes(p))) return null
  const found = a.platforms.flatMap((pa) => b.platforms.map((pb) => lookup(pa, pb))).filter((t): t is PlatformTime => !!t)
  if (!found.length) return null
  const best = found.reduce((x, y) => (rank(y.source) < rank(x.source) ? y : x)).source
  return {
    seconds: Math.round(median(found.filter((t) => t.source === best).map((t) => t.seconds))),
    source: best,
    uncertaintyS: params.uncertaintyS[best].pair,
  }
}
