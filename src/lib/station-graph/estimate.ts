import type { StationPathParams } from './params'

/**
 * T2 platform to platform: the walk between the platforms' edges and
 * centres, a cost per level between them, and the up-and-over through a
 * mezzanine that two separate platforms on one level need.
 */
export function estimatePair(
  p: StationPathParams,
  f: { edgeM: number; centroidM: number; levelA: number | null; levelB: number | null },
): number {
  const e = p.estimate.pair
  const dl = f.levelA !== null && f.levelB !== null ? Math.abs(f.levelA - f.levelB) : 0
  const over = dl === 0 && f.edgeM > e.touchingM ? 1 : 0
  return e.intercept + e.edgeM * f.edgeM + e.centroidM * f.centroidM + e.level * dl + e.overNotTouching * over
}

/** T2 platform to entrance: the distance from the platform's edge, and its depth (1 level when untagged). */
export function estimateStreet(p: StationPathParams, f: { edgeM: number; level: number | null }): number {
  const e = p.estimate.street
  return e.intercept + e.edgeM * f.edgeM + e.depthLevel * Math.abs(f.level ?? 1)
}

/** T3, for a stop with no OSM platform: stop coordinates only. */
export function straightLinePair(p: StationPathParams, metres: number): number {
  return p.straightLine.pair.intercept + p.straightLine.pair.perM * metres
}

export function straightLineStreet(p: StationPathParams, metres: number): number {
  return p.straightLine.street.perM * metres
}
