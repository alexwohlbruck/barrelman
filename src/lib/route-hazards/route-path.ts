/**
 * A route polyline measured in metres along it, with the lookups the hazard
 * matcher needs: where a point projects onto it, its bearing there, and the
 * short pieces it is queried in. Distances are haversine, as a client measures
 * the same coordinates, so a hazard's distance lands where the client expects.
 */

export type LonLat = [number, number]

const EARTH_RADIUS = 6371000
const RAD = Math.PI / 180
const METERS_PER_DEGREE = 111_320

export function haversine(a: LonLat, b: LonLat): number {
  const dLat = (b[1] - a[1]) * RAD
  const dLon = (b[0] - a[0]) * RAD
  const s = Math.sin(dLat / 2) ** 2 + Math.cos(a[1] * RAD) * Math.cos(b[1] * RAD) * Math.sin(dLon / 2) ** 2
  return 2 * EARTH_RADIUS * Math.asin(Math.sqrt(s))
}

/** Compass bearing from a to b, 0–360. */
export function bearing(a: LonLat, b: LonLat): number {
  const y = Math.sin((b[0] - a[0]) * RAD) * Math.cos(b[1] * RAD)
  const x = Math.cos(a[1] * RAD) * Math.sin(b[1] * RAD) - Math.sin(a[1] * RAD) * Math.cos(b[1] * RAD) * Math.cos((b[0] - a[0]) * RAD)
  return (Math.atan2(y, x) / RAD + 360) % 360
}

/** Smallest angle between two bearings, 0–180. */
export function angleBetween(a: number, b: number): number {
  return Math.abs(((b - a + 540) % 360) - 180)
}

/** Smallest angle between two undirected lines, 0–90. */
export function lineAngle(a: number, b: number): number {
  const d = angleBetween(a, b)
  return d > 90 ? 180 - d : d
}

/** Metres east and north of an origin; exact enough over the few metres a match is judged on. */
export function localFrame(origin: LonLat) {
  const kx = METERS_PER_DEGREE * Math.cos(origin[1] * RAD)
  return (p: LonLat): [number, number] => [(p[0] - origin[0]) * kx, (p[1] - origin[1]) * METERS_PER_DEGREE]
}

export interface Projection {
  /** Metres along the route. */
  distance: number
  /** Metres from the route. */
  offset: number
  /** The route's bearing there. */
  bearing: number
  /** Index of the segment's first vertex. */
  segment: number
}

export interface RoutePath {
  coordinates: LonLat[]
  cumulative: number[]
  length: number
  at(distance: number): LonLat
  /** Every pass of the route within `tolerance` of a point, nearest projection per pass. */
  project(point: LonLat, tolerance: number, segments?: Iterable<number>): Projection[]
}

/** Passes nearer each other than this along the route are one pass. */
const SAME_PASS_METERS = 30

export function routePath(coordinates: LonLat[]): RoutePath {
  const cumulative = [0]
  for (let i = 1; i < coordinates.length; i++) cumulative.push(cumulative[i - 1] + haversine(coordinates[i - 1], coordinates[i]))
  const length = cumulative[cumulative.length - 1] ?? 0

  const at = (distance: number): LonLat => {
    const d = Math.min(Math.max(0, distance), length)
    let lo = 0
    let hi = cumulative.length - 1
    while (hi - lo > 1) {
      const mid = (lo + hi) >> 1
      if (cumulative[mid] <= d) lo = mid
      else hi = mid
    }
    const span = cumulative[hi] - cumulative[lo]
    const t = span > 0 ? (d - cumulative[lo]) / span : 0
    const a = coordinates[lo]
    const b = coordinates[hi] ?? a
    return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
  }

  const project = (point: LonLat, tolerance: number, segments?: Iterable<number>): Projection[] => {
    const toLocal = localFrame(point)
    const hits: Projection[] = []
    const indices = segments ?? coordinates.keys()
    for (const i of indices) {
      if (i < 0 || i >= coordinates.length - 1) continue
      const [ax, ay] = toLocal(coordinates[i])
      const [bx, by] = toLocal(coordinates[i + 1])
      const dx = bx - ax
      const dy = by - ay
      const len2 = dx * dx + dy * dy
      const t = len2 > 0 ? Math.min(1, Math.max(0, -(ax * dx + ay * dy) / len2)) : 0
      const offset = Math.hypot(ax + dx * t, ay + dy * t)
      if (offset > tolerance) continue
      hits.push({
        distance: cumulative[i] + (cumulative[i + 1] - cumulative[i]) * t,
        offset,
        bearing: bearing(coordinates[i], coordinates[i + 1]),
        segment: i,
      })
    }
    hits.sort((a, b) => a.distance - b.distance)
    const passes: Projection[] = []
    for (const hit of hits) {
      const last = passes[passes.length - 1]
      if (last && hit.distance - last.distance < SAME_PASS_METERS) {
        if (hit.offset < last.offset) passes[passes.length - 1] = hit
      } else {
        passes.push(hit)
      }
    }
    return passes
  }

  return { coordinates, cumulative, length, at, project }
}

export interface RoutePiece {
  /** Coordinates sent to the database, densified so the piece's box stays small. */
  coordinates: LonLat[]
  /** The route segments the piece covers. */
  firstSegment: number
  lastSegment: number
}

/**
 * The route cut into pieces of about `size` metres. A spatial index matches a
 * line by its box, so one box over a 20 km route would read every feature in
 * the city; a piece's box holds only what is near it.
 */
export function routePieces(path: RoutePath, size: number): RoutePiece[] {
  const { coordinates, cumulative } = path
  const pieces: RoutePiece[] = []
  let current: RoutePiece | null = null
  let run = 0
  for (let i = 0; i < coordinates.length - 1; i++) {
    const span = cumulative[i + 1] - cumulative[i]
    const steps = Math.max(1, Math.ceil(span / size))
    const step = span / steps
    for (let k = 1; k <= steps; k++) {
      const start = lerp(coordinates[i], coordinates[i + 1], (k - 1) / steps)
      if (current && run + step > size + 1e-6) {
        pieces.push(current)
        current = null
      }
      if (!current) {
        current = { coordinates: [start], firstSegment: i, lastSegment: i }
        run = 0
      }
      current.coordinates.push(lerp(coordinates[i], coordinates[i + 1], k / steps))
      current.lastSegment = i
      run += step
    }
  }
  if (current) pieces.push(current)
  return pieces
}

function lerp(a: LonLat, b: LonLat, t: number): LonLat {
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
}

/** A tolerance in metres as degrees, wide enough in longitude at the route's latitude. */
export function toleranceDegrees(meters: number, latitude: number): number {
  return meters / (METERS_PER_DEGREE * Math.max(0.1, Math.cos(latitude * RAD)))
}
