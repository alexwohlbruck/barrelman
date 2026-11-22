/**
 * Round a walk's bends inside a pedestrian area into Bézier curves that never
 * leave it.
 *
 * The graph crosses an area in straight lines that meet at its corners
 * (import/synthesize-area-paths.py). Each bend between two of those lines
 * becomes a quadratic Bézier from a point on the line in to a point on the line
 * out, with the corner as its control point, so the curve meets both lines
 * tangentially. Every sample of a curve, and every piece between samples, is
 * checked against the area's open space; a curve that would leave it is pulled
 * in tighter, and one that cannot fit leaves the corner as it was.
 */

const M_PER_DEG = 111_320
const ON_EDGE_M = 0.01
const STRAIGHT_RAD = (2 * Math.PI) / 180
const REVERSE_RAD = (178 * Math.PI) / 180
// A run's first and last lines join ways outside the area, so a curve may use most, not all, of them.
const END_SHARE = 0.9
const MAX_TRIM_M = 30
const MIN_TRIM_M = 0.25
const SHRINK = 0.6
// A curve cuts in toward what the bend turns around; it may come this close, or as close as the lines it replaces already do.
const CURVE_CLEARANCE_M = 1
// GraphHopper rounds coordinates to 6 decimals, up to ~6 cm, which can put a point on an area's edge just outside it.
const SETTLE_M = 0.15

type XY = [number, number]

/** An open space, as rings of [lon, lat], outer ring first. */
export interface Space {
  rings: number[][][]
}

interface Room {
  rings: XY[][]
}

export interface Rounded {
  coordinates: number[][]
  /** Where each input point went: its index in `coordinates`; a rounded corner's is its curve's midpoint. */
  indexOf: number[]
}

function frame(origin: number[]) {
  const kx = Math.cos((origin[1] * Math.PI) / 180) * M_PER_DEG
  return {
    xy: (c: number[]): XY => [(c[0] - origin[0]) * kx, (c[1] - origin[1]) * M_PER_DEG],
    lonlat: ([x, y]: XY): [number, number] => [origin[0] + x / kx, origin[1] + y / M_PER_DEG],
  }
}

const sub = (a: XY, b: XY): XY => [a[0] - b[0], a[1] - b[1]]
const len = (a: XY) => Math.hypot(a[0], a[1])
const cross = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

function nearestOnSegment(p: XY, a: XY, b: XY): XY {
  const ab = sub(b, a)
  const l2 = ab[0] * ab[0] + ab[1] * ab[1]
  const t = l2 > 0 ? Math.max(0, Math.min(1, ((p[0] - a[0]) * ab[0] + (p[1] - a[1]) * ab[1]) / l2)) : 0
  return [a[0] + ab[0] * t, a[1] + ab[1] * t]
}

const distanceToSegment = (p: XY, a: XY, b: XY) => len(sub(p, nearestOnSegment(p, a, b)))

function edgeDistance(room: Room, p: XY): number {
  let best = Infinity
  for (const ring of room.rings) {
    for (let i = 1; i < ring.length; i++) best = Math.min(best, distanceToSegment(p, ring[i - 1], ring[i]))
  }
  return best
}

function nearestOnEdge(room: Room, p: XY): XY {
  let best: XY = p, bestDistance = Infinity
  for (const ring of room.rings) {
    for (let i = 1; i < ring.length; i++) {
      const q = nearestOnSegment(p, ring[i - 1], ring[i])
      const d = len(sub(p, q))
      if (d < bestDistance) [best, bestDistance] = [q, d]
    }
  }
  return best
}

/** Inside the room, or on its edge. */
export function within(room: Room, p: XY): boolean {
  let odd = false
  for (const ring of room.rings) {
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const a = ring[i], b = ring[j]
      if (distanceToSegment(p, a, b) <= ON_EDGE_M) return true
      if ((a[1] > p[1]) !== (b[1] > p[1]) && p[0] < ((b[0] - a[0]) * (p[1] - a[1])) / (b[1] - a[1]) + a[0]) odd = !odd
    }
  }
  return odd
}

/** Whether a–b passes through one of the room's edges, rather than touching or running along it. */
export function crossesEdge(room: Room, a: XY, b: XY): boolean {
  for (const ring of room.rings) {
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const c = ring[j], d = ring[i]
      const d1 = cross(c, d, a), d2 = cross(c, d, b), d3 = cross(a, b, c), d4 = cross(a, b, d)
      if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) return true
    }
  }
  return false
}

function bezier(a: XY, v: XY, b: XY, t: number): XY {
  const u = 1 - t
  return [u * u * a[0] + 2 * u * t * v[0] + t * t * b[0], u * u * a[1] + 2 * u * t * v[1] + t * t * b[1]]
}

function toward(from: XY, to: XY, distance: number): XY {
  const d = sub(to, from), l = len(d)
  return [from[0] + (d[0] / l) * distance, from[1] + (d[1] / l) * distance]
}

function deflection(a: XY, v: XY, b: XY): number {
  const h1 = Math.atan2(v[1] - a[1], v[0] - a[0])
  const h2 = Math.atan2(b[1] - v[1], b[0] - v[0])
  return Math.abs(((h2 - h1 + 3 * Math.PI) % (2 * Math.PI)) - Math.PI)
}

/** An even number of steps, more for a sharper or wider bend. */
function stepsFor(turn: number, trim: number): number {
  return Math.min(24, Math.max(4, 2 * Math.ceil((turn * 180) / Math.PI / 20 + trim / 4)))
}

interface Bend {
  trim: number
  steps: number
}

function ends(p: XY, v: XY, q: XY, trim: number): [XY, XY] {
  return [toward(v, p, trim), toward(v, q, trim)]
}

function fits(room: Room, a: XY, v: XY, b: XY, steps: number): boolean {
  let prev = a
  for (let i = 1; i <= steps; i++) {
    const p = bezier(a, v, b, i / steps)
    if (!within(room, p) || crossesEdge(room, prev, p)) return false
    const onLines = distanceToSegment(p, a, v) < distanceToSegment(p, v, b) ? nearestOnSegment(p, a, v) : nearestOnSegment(p, v, b)
    if (edgeDistance(room, p) < Math.min(CURVE_CLEARANCE_M, edgeDistance(room, onLines)) - ON_EDGE_M) return false
    prev = p
  }
  return true
}

/** The widest trim, from `widest` down, at which the curve stays in the room. */
function widestFit(room: Room, p: XY, v: XY, q: XY, widest: number, turn: number): Bend | undefined {
  for (let trim = widest; trim >= MIN_TRIM_M; trim *= SHRINK) {
    const [a, b] = ends(p, v, q, trim)
    const steps = stepsFor(turn, trim)
    if (fits(room, a, v, b, steps)) return { trim, steps }
  }
  return undefined
}

/**
 * Round every bend of `coordinates` ([lon, lat] or [lon, lat, ele]) where the
 * lines in and out both cross the same space. `spaceOf(i)` is the space line i
 * (from point i to i + 1) crosses, or undefined; points in `fixed` stay put.
 */
export function roundBends(coordinates: number[][], spaceOf: (line: number) => Space | undefined, fixed: Set<number> = new Set()): Rounded {
  const n = coordinates.length
  const identity = { coordinates, indexOf: coordinates.map((_, i) => i) }
  if (n < 3) return identity
  const { xy, lonlat } = frame(coordinates[0])
  const pts = coordinates.map(xy)
  const rooms = new Map<Space, Room>()
  const roomOf = (space: Space) => {
    let room = rooms.get(space)
    if (!room) rooms.set(space, room = { rings: space.rings.map(r => r.map(xy)) })
    return room
  }

  const settled = new Set<number>()
  for (let k = 0; k < n; k++) {
    const space = spaceOf(k) ?? (k > 0 ? spaceOf(k - 1) : undefined)
    if (!space) continue
    const room = roomOf(space)
    if (within(room, pts[k])) continue
    const edge = nearestOnEdge(room, pts[k])
    if (len(sub(edge, pts[k])) > SETTLE_M) continue
    pts[k] = edge
    settled.add(k)
  }

  const candidate = (k: number) => {
    if (k <= 0 || k >= n - 1 || fixed.has(k)) return false
    const space = spaceOf(k - 1)
    if (!space || space !== spaceOf(k)) return false
    const turn = deflection(pts[k - 1], pts[k], pts[k + 1])
    return turn >= STRAIGHT_RAD && turn <= REVERSE_RAD
  }
  const bends = new Map<number, Bend>()
  for (let k = 1; k < n - 1; k++) {
    if (!candidate(k)) continue
    const room = roomOf(spaceOf(k)!)
    const [p, v, q] = [pts[k - 1], pts[k], pts[k + 1]]
    if (![p, v, q].every(point => within(room, point))) continue
    const lenIn = len(sub(v, p)), lenOut = len(sub(q, v))
    const widest = Math.min(
      MAX_TRIM_M,
      lenIn * (candidate(k - 1) ? 0.5 : END_SHARE),
      lenOut * (candidate(k + 1) ? 0.5 : END_SHARE),
    )
    const bend = widestFit(room, p, v, q, widest, deflection(p, v, q))
    if (bend) bends.set(k, bend)
  }
  if (!bends.size && !settled.size) return identity
  const at = (k: number) => {
    if (!settled.has(k)) return coordinates[k]
    const c: number[] = lonlat(pts[k])
    if (coordinates[k].length > 2) c.push(coordinates[k][2])
    return c
  }

  const out: number[][] = []
  const indexOf: number[] = []
  const push = (c: number[]) => {
    const last = out[out.length - 1]
    if (last && Math.abs(last[0] - c[0]) < 1e-9 && Math.abs(last[1] - c[1]) < 1e-9) return
    out.push(c)
  }
  for (let k = 0; k < n; k++) {
    const bend = bends.get(k)
    if (!bend) {
      push(at(k))
      indexOf[k] = out.length - 1
      continue
    }
    const [p, v, q] = [pts[k - 1], pts[k], pts[k + 1]]
    const [a, b] = ends(p, v, q, bend.trim)
    const tIn = bend.trim / len(sub(v, p)), tOut = bend.trim / len(sub(q, v))
    const ele = coordinates[k].length > 2
      ? [coordinates[k][2] + (coordinates[k - 1][2] - coordinates[k][2]) * tIn, coordinates[k][2], coordinates[k][2] + (coordinates[k + 1][2] - coordinates[k][2]) * tOut]
      : undefined
    for (let i = 0; i <= bend.steps; i++) {
      const t = i / bend.steps
      const c: number[] = lonlat(bezier(a, v, b, t))
      if (ele) c.push((1 - t) * (1 - t) * ele[0] + 2 * (1 - t) * t * ele[1] + t * t * ele[2])
      push(c)
      if (i === bend.steps / 2) indexOf[k] = out.length - 1
    }
  }
  return { coordinates: out, indexOf }
}
