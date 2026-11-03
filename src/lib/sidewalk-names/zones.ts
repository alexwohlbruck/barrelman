/**
 * Which street an unnamed sidewalk runs beside, along its length.
 *
 * The way is sampled every few metres. At each sample the named roads close by
 * and running the same way are candidates: parallelism decides, so the cross
 * street at a corner, though nearer, never wins. Among near-equal candidates
 * the bigger road does. Runs of the same winner become zones, and a run only a
 * sample or two long folds into its neighbours, so a corner cannot flap.
 *
 * Works in local metres; see `localProjection`.
 */

export type XY = [number, number]

export interface Road {
  name: string
  /** Higher is the bigger road; see ROAD_RANK. */
  rank: number
  points: XY[]
}

/** A stretch of the way, as fractions of its length. */
export interface Zone {
  from: number
  to: number
  name: string
}

export const SAMPLE_STEP = 7.5
/** A wide avenue's centreline lies about this far from its sidewalk. */
export const SEARCH_RADIUS = 25
export const MAX_ANGLE = 25
/** Metres of distance one degree off parallel costs. */
const ANGLE_COST = 0.4
/** Candidates this close in cost go to the bigger road. */
const RANK_SLACK = 3
/** A change of street must hold for this many samples to become a zone. */
export const MIN_RUN = 3
/** Half the chord the sidewalk's direction is read over at a sample. */
const BEARING_REACH = 3

/** Streets a sidewalk takes its name from, by highway class. Pedestrian-only ways and links never do. */
export const ROAD_RANK: Record<string, number> = {
  trunk: 7,
  primary: 6,
  secondary: 5,
  tertiary: 4,
  unclassified: 3,
  residential: 3,
  living_street: 2,
  service: 1,
}

/** Equirectangular metres around `origin` [lon, lat]; plenty for a few kilometres. */
export function localProjection([lon0, lat0]: XY): (p: XY) => XY {
  const ky = 111_320
  const kx = ky * Math.cos((lat0 * Math.PI) / 180)
  return ([lon, lat]) => [(lon - lon0) * kx, (lat - lat0) * ky]
}

export function cumulativeLengths(points: XY[]): number[] {
  const out = [0]
  for (let i = 1; i < points.length; i++) out.push(out[i - 1] + Math.hypot(points[i][0] - points[i - 1][0], points[i][1] - points[i - 1][1]))
  return out
}

function pointAlong(points: XY[], lengths: number[], s: number): XY {
  let i = 1
  while (i < points.length - 1 && lengths[i] < s) i++
  const span = lengths[i] - lengths[i - 1]
  const t = span > 0 ? Math.min(1, Math.max(0, (s - lengths[i - 1]) / span)) : 0
  const [a, b] = [points[i - 1], points[i]]
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
}

/** The angle between two directions, ignoring which way each points: 0–90°. */
export function axisAngle(a: number, b: number): number {
  const d = Math.abs(a - b) % 180
  return Math.min(d, 180 - d)
}

const heading = (a: XY, b: XY) => (Math.atan2(b[1] - a[1], b[0] - a[0]) * 180) / Math.PI

interface Segment {
  a: XY
  b: XY
  road: Road
}

/** Road segments bucketed on a grid one search radius wide. */
class SegmentIndex {
  private cells = new Map<string, Segment[]>()

  constructor(roads: Road[], private size = SEARCH_RADIUS) {
    for (const road of roads)
      for (let i = 1; i < road.points.length; i++) {
        const [a, b] = [road.points[i - 1], road.points[i]]
        const [x0, x1] = [Math.floor(Math.min(a[0], b[0]) / size), Math.floor(Math.max(a[0], b[0]) / size)]
        const [y0, y1] = [Math.floor(Math.min(a[1], b[1]) / size), Math.floor(Math.max(a[1], b[1]) / size)]
        for (let x = x0; x <= x1; x++)
          for (let y = y0; y <= y1; y++) {
            const key = `${x},${y}`
            const cell = this.cells.get(key)
            if (cell) cell.push({ a, b, road })
            else this.cells.set(key, [{ a, b, road }])
          }
      }
  }

  near([px, py]: XY): Set<Segment> {
    const found = new Set<Segment>()
    const [cx, cy] = [Math.floor(px / this.size), Math.floor(py / this.size)]
    for (let x = cx - 1; x <= cx + 1; x++)
      for (let y = cy - 1; y <= cy + 1; y++) for (const s of this.cells.get(`${x},${y}`) ?? []) found.add(s)
    return found
  }
}

function distanceToSegment([px, py]: XY, [ax, ay]: XY, [bx, by]: XY): number {
  const [dx, dy] = [bx - ax, by - ay]
  const len2 = dx * dx + dy * dy
  const t = len2 > 0 ? Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len2)) : 0
  return Math.hypot(ax + dx * t - px, ay + dy * t - py)
}

/** The street beside `p`, for a sidewalk heading `bearing`; null when none runs alongside. */
function streetAt(index: SegmentIndex, p: XY, bearing: number): string | null {
  const best = new Map<Road, number>()
  for (const { a, b, road } of index.near(p)) {
    const angle = axisAngle(bearing, heading(a, b))
    if (angle > MAX_ANGLE) continue
    const d = distanceToSegment(p, a, b)
    if (d > SEARCH_RADIUS) continue
    const cost = d + angle * ANGLE_COST
    if (cost < (best.get(road) ?? Infinity)) best.set(road, cost)
  }
  const ranked = [...best].sort((x, y) => x[1] - y[1])
  if (!ranked.length) return null
  const [[nearest, floor]] = ranked
  let winner = nearest
  for (const [road, cost] of ranked.slice(1)) {
    if (cost - floor > RANK_SLACK) break
    if (road.rank > winner.rank) winner = road
  }
  return winner.name
}

interface Run {
  name: string | null
  start: number
  end: number
}

function runsOf(values: Array<string | null>): Run[] {
  const runs: Run[] = []
  values.forEach((name, i) => {
    const last = runs.at(-1)
    if (last && last.name === name) last.end = i + 1
    else runs.push({ name, start: i, end: i + 1 })
  })
  return runs
}

/** Folds every run shorter than `minRun` samples into its neighbours, shortest first. */
export function settleRuns(values: Array<string | null>, minRun = MIN_RUN): Run[] {
  const runs = runsOf(values)
  for (;;) {
    let shortest = -1
    for (let i = 0; i < runs.length; i++) {
      const len = runs[i].end - runs[i].start
      if (len < minRun && runs.length > 1 && (shortest < 0 || len < runs[shortest].end - runs[shortest].start)) shortest = i
    }
    if (shortest < 0) return runs
    const [prev, run, next] = [runs[shortest - 1], runs[shortest], runs[shortest + 1]]
    if (prev && next && prev.name === next.name) {
      prev.end = next.end
      runs.splice(shortest, 2)
    } else {
      const into = !next || (prev && prev.end - prev.start >= next.end - next.start) ? prev : next
      into.start = Math.min(into.start, run.start)
      into.end = Math.max(into.end, run.end)
      runs.splice(shortest, 1)
    }
  }
}

/** Matches sidewalks against the named `roads` around them; build once per area. */
export function streetMatcher(roads: Road[]): (path: XY[]) => Zone[] {
  const index = new SegmentIndex(roads)
  return path => {
    const lengths = cumulativeLengths(path)
    const length = lengths.at(-1) ?? 0
    if (path.length < 2 || length <= 0 || !roads.length) return []
    const count = Math.max(2, Math.ceil(length / SAMPLE_STEP) + 1)
    const at = Array.from({ length: count }, (_, i) => (length * i) / (count - 1))
    const winners = at.map(s => {
      const from = pointAlong(path, lengths, Math.max(0, s - BEARING_REACH))
      const to = pointAlong(path, lengths, Math.min(length, s + BEARING_REACH))
      return streetAt(index, pointAlong(path, lengths, s), heading(from, to))
    })
    const edge = (i: number) => (i <= 0 ? 0 : i >= count ? 1 : (at[i - 1] + at[i]) / 2 / length)
    const round = (f: number) => Math.round(f * 1e4) / 1e4
    return settleRuns(winners)
      .filter((run): run is Run & { name: string } => run.name !== null)
      .map(run => ({ from: round(edge(run.start)), to: round(edge(run.end)), name: run.name }))
  }
}

export const streetZones = (path: XY[], roads: Road[]): Zone[] => streetMatcher(roads)(path)
