/**
 * The road a route crosses on a crosswalk. A crossing way carries no name of
 * its own, so it is read off the named road its line cuts through.
 */
import { haversine, type LonLat } from '../route-hazards/route-path'

/** GraphHopper crossing values where a road is crossed on foot or by bike; railway crossings have no road to name. */
const ROAD_CROSSINGS = new Set(['traffic_signals', 'marked', 'unmarked', 'uncontrolled'])
/** Longer than this is a stretch the route rides, not a road it crosses. */
const MAX_CROSSING_METERS = 80
/** A route of more crossings than this is not named at all, rather than named slowly. */
export const MAX_CROSSINGS = 300

export interface CrossingSpan {
  /** Coordinate indexes, as GraphHopper's details give them. */
  from: number
  to: number
  line: LonLat[]
  startDistance: number
  endDistance: number
}

export interface CrossedRoad {
  /** Index into the spans the roads were looked up for. */
  span: number
  name: string
  /** The point of the road nearest the crossing's middle. */
  at: LonLat
}

export interface CrossingName {
  startDistance: number
  endDistance: number
  name: string
}

/** The route's road crossings, from GraphHopper's `crossing` detail. */
export function crossingSpans(
  coordinates: number[][],
  distances: number[],
  details: Record<string, unknown[]> | undefined,
): CrossingSpan[] {
  const spans: CrossingSpan[] = []
  for (const entry of details?.crossing ?? []) {
    if (!Array.isArray(entry)) continue
    const [from, to, value] = entry as [number, number, unknown]
    if (typeof value !== 'string' || !ROAD_CROSSINGS.has(value.toLowerCase()) || to <= from) continue
    const startDistance = distances[from] ?? 0
    const endDistance = distances[to] ?? startDistance
    if (endDistance - startDistance > MAX_CROSSING_METERS) continue
    spans.push({ from, to, startDistance, endDistance, line: coordinates.slice(from, to + 1).map(c => [c[0], c[1]] as LonLat) })
  }
  return spans.length > MAX_CROSSINGS ? [] : spans
}

function midpoint(line: LonLat[]): LonLat {
  const a = line[0]
  const b = line[line.length - 1]
  return [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2]
}

/** Each crossing's road: the one met nearest its middle, where several cut through it. */
export function nameCrossings(spans: CrossingSpan[], roads: CrossedRoad[]): CrossingName[] {
  const best = new Map<number, { name: string; offset: number }>()
  for (const road of roads) {
    const span = spans[road.span]
    if (!span) continue
    const offset = haversine(midpoint(span.line), road.at)
    const current = best.get(road.span)
    if (!current || offset < current.offset) best.set(road.span, { name: road.name, offset })
  }
  return [...best].sort(([a], [b]) => a - b).map(([i, { name }]) => ({
    startDistance: spans[i].startDistance,
    endDistance: spans[i].endDistance,
    name,
  }))
}
