/**
 * Where a route runs along a way, and where it crosses one. The route's
 * geometry follows the OSM ways it drives, so a way it rides keeps within a
 * metre or two of it and points the same way; a way it only crosses does not.
 */

import { bearing, lineAngle, localFrame, type LonLat, type RoutePath } from './route-path'

export interface Run {
  from: number
  to: number
}

/** A route sample within this of a way, and running with it, is on it. */
export const ALONG_METERS = 4
const ALONG_DEG = 25
const SAMPLE_METERS = 5
/** Shorter runs are a corner clipped at a junction, not a way ridden. */
const MIN_RUN_METERS = 10

/** Stretches of the route that ride `way`, sampled within the route range [from, to]. */
export function runsAlong(path: RoutePath, way: LonLat[], from = 0, to = path.length): Run[] {
  if (way.length < 2 || path.length === 0) return []
  const runs: Run[] = []
  let open: Run | null = null
  const end = Math.min(to, path.length)
  for (let s = Math.max(0, from); s <= end + 1e-6; s += SAMPLE_METERS) {
    const d = Math.min(s, end)
    const point = path.at(d)
    const heading = bearing(path.at(Math.max(0, d - 1)), path.at(Math.min(path.length, d + 1)))
    if (rides(way, point, heading)) {
      if (open && d - open.to <= SAMPLE_METERS + 1e-6) open.to = d
      else runs.push((open = { from: d, to: d }))
    }
  }
  return runs.filter(r => r.to - r.from >= MIN_RUN_METERS)
}

function rides(way: LonLat[], point: LonLat, heading: number): boolean {
  const toLocal = localFrame(point)
  for (let i = 0; i < way.length - 1; i++) {
    const [ax, ay] = toLocal(way[i])
    const [bx, by] = toLocal(way[i + 1])
    const dx = bx - ax
    const dy = by - ay
    const len2 = dx * dx + dy * dy
    if (len2 === 0) continue
    const t = Math.min(1, Math.max(0, -(ax * dx + ay * dy) / len2))
    if (Math.hypot(ax + dx * t, ay + dy * t) > ALONG_METERS) continue
    if (lineAngle(bearing(way[i], way[i + 1]), heading) <= ALONG_DEG) return true
  }
  return false
}

export interface Crossing {
  distance: number
  coordinate: LonLat
  /** Angle between the route and the way, 0–90. */
  angle: number
}

/** Where the route crosses `way`, over route segments `first`..`last`. */
export function crossings(path: RoutePath, way: LonLat[], first = 0, last = path.coordinates.length - 2): Crossing[] {
  const out: Crossing[] = []
  const route = path.coordinates
  if (way.length < 2 || route.length < 2) return out
  const toLocal = localFrame(route[Math.max(0, first)])
  const wayLocal = way.map(toLocal)
  for (let i = Math.max(0, first); i <= Math.min(last, route.length - 2); i++) {
    const a = toLocal(route[i])
    const b = toLocal(route[i + 1])
    for (let j = 0; j < wayLocal.length - 1; j++) {
      const t = intersect(a, b, wayLocal[j], wayLocal[j + 1])
      if (t == null) continue
      const span = path.cumulative[i + 1] - path.cumulative[i]
      out.push({
        distance: path.cumulative[i] + span * t,
        coordinate: [route[i][0] + (route[i + 1][0] - route[i][0]) * t, route[i][1] + (route[i + 1][1] - route[i][1]) * t],
        angle: lineAngle(bearing(route[i], route[i + 1]), bearing(way[j], way[j + 1])),
      })
    }
  }
  return out.sort((x, y) => x.distance - y.distance)
}

/** Where along a→b the segment c→d crosses it, or null. */
function intersect(a: [number, number], b: [number, number], c: [number, number], d: [number, number]): number | null {
  const rx = b[0] - a[0]
  const ry = b[1] - a[1]
  const sx = d[0] - c[0]
  const sy = d[1] - c[1]
  const denom = rx * sy - ry * sx
  if (denom === 0) return null
  const qx = c[0] - a[0]
  const qy = c[1] - a[1]
  const t = (qx * sy - qy * sx) / denom
  const u = (qx * ry - qy * rx) / denom
  return t >= 0 && t <= 1 && u >= 0 && u <= 1 ? t : null
}
