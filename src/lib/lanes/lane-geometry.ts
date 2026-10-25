// A route drawn in its lanes: the centreline offset sideways by a profile
// along it, S-curving where the lane changes and turning through junctions on
// an arc rather than round the centreline's corner.

export interface LaneRun {
  /** Route distances, metres. */
  from: number
  to: number
  /** Metres right of the route to the lane's middle, entering and leaving. */
  entry: number
  exit: number
  /** Where a lane change has to be finished by, metres before `to`. */
  settleBefore: number
  /** Lane widths the change crosses, for its length. */
  laneWidth: number
}

export interface Corner {
  /** Route distance of the junction node. */
  at: number
  /** Radius the lane's middle turns on. */
  radius: number
}

export interface LaneLine {
  coordinates: [number, number][]
  /** Route distance of each coordinate: where on the centreline it stands for. */
  measures: number[]
}

const M_PER_DEG = 111_320
/** A lane change takes this long per lane crossed. */
const CHANGE_PER_LANE_METERS = 35
const MIN_CHANGE_METERS = 12
/** Ways meeting at a different offset blend over this either side. */
const BLEND_METERS = 10
const SAMPLE_METERS = 2.5
/** Turns sharper than this get an arc. */
const MIN_CORNER_DEG = 20
const MAX_TANGENT_METERS = 40

type V = [number, number]

const smoothstep = (t: number) => (t <= 0 ? 0 : t >= 1 ? 1 : t * t * (3 - 2 * t))
const sub = (a: V, b: V): V => [a[0] - b[0], a[1] - b[1]]
const add = (a: V, b: V): V => [a[0] + b[0], a[1] + b[1]]
const scale = (a: V, k: number): V => [a[0] * k, a[1] * k]
const dot = (a: V, b: V) => a[0] * b[0] + a[1] * b[1]
const cross = (a: V, b: V) => a[0] * b[1] - a[1] * b[0]
const norm = (a: V): V => {
  const l = Math.hypot(a[0], a[1])
  return l > 0 ? [a[0] / l, a[1] / l] : [0, 0]
}
/** The unit normal to the right of `d`. */
const right = (d: V): V => [d[1], -d[0]]

export function cumulativeDistances(coords: [number, number][]): number[] {
  const out = [0]
  for (let i = 1; i < coords.length; i++) out.push(out[i - 1] + metresBetween(coords[i - 1], coords[i]))
  return out
}

function metresBetween(a: [number, number], b: [number, number]): number {
  const toRad = Math.PI / 180
  const dLat = (b[1] - a[1]) * toRad
  const dLon = (b[0] - a[0]) * toRad
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a[1] * toRad) * Math.cos(b[1] * toRad) * Math.sin(dLon / 2) ** 2
  return 2 * 6371000 * Math.asin(Math.sqrt(h))
}

/** Metres east and north of `origin`. */
function toLocal(origin: [number, number], p: [number, number]): V {
  return [(p[0] - origin[0]) * M_PER_DEG * Math.cos((origin[1] * Math.PI) / 180), (p[1] - origin[1]) * M_PER_DEG]
}

function fromLocal(origin: [number, number], v: V): [number, number] {
  return [origin[0] + v[0] / (M_PER_DEG * Math.cos((origin[1] * Math.PI) / 180)), origin[1] + v[1] / M_PER_DEG]
}

class Polyline {
  readonly cumulative: number[]
  readonly length: number

  constructor(readonly coords: [number, number][]) {
    this.cumulative = cumulativeDistances(coords)
    this.length = this.cumulative.at(-1) ?? 0
  }

  /** Index of the segment [i, i + 1] holding `s`. */
  segmentAt(s: number): number {
    const c = this.cumulative
    let lo = 0
    let hi = c.length - 1
    if (s <= c[0]) return 0
    if (s >= c[hi]) return Math.max(0, hi - 1)
    while (hi - lo > 1) {
      const mid = (lo + hi) >> 1
      if (c[mid] <= s) lo = mid
      else hi = mid
    }
    return lo
  }

  pointAt(s: number): [number, number] {
    const i = this.segmentAt(s)
    const a = this.coords[i]
    const b = this.coords[Math.min(i + 1, this.coords.length - 1)]
    const span = this.cumulative[i + 1] - this.cumulative[i]
    const t = span > 0 ? Math.min(1, Math.max(0, (s - this.cumulative[i]) / span)) : 0
    return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
  }

  /** Unit direction, in local metres, of the chord from `s0` to `s1`. */
  direction(s0: number, s1: number): V {
    const a = this.pointAt(Math.max(0, s0))
    return norm(toLocal(a, this.pointAt(Math.min(this.length, s1))))
  }

  /** The sideways unit vector at `s`, mitred at a vertex so offset lines meet. */
  sideAt(s: number, atVertex: number | null): V {
    if (atVertex == null || atVertex <= 0 || atVertex >= this.coords.length - 1) {
      const i = this.segmentAt(s)
      return right(norm(toLocal(this.coords[i], this.coords[i + 1])))
    }
    const p = this.coords[atVertex]
    const d1 = norm(toLocal(this.coords[atVertex - 1], p))
    const d2 = norm(toLocal(p, this.coords[atVertex + 1]))
    const n = norm(right(add(d1, d2)))
    const c = dot(n, right(d1))
    return c > 0.5 ? scale(n, 1 / c) : right(d1)
  }
}

/** The offset the runs put the lane at, at `s`, before any corner. */
export function offsetAt(runs: LaneRun[], corners: Corner[], s: number): number {
  const i = runIndexAt(runs, s)
  if (i < 0) return 0
  const own = inRun(runs[i], s)
  const cornerAt = (b: number) => corners.some(c => Math.abs(c.at - b) < 0.5)
  const prev = runs[i - 1]
  if (prev && s - runs[i].from < BLEND_METERS && !cornerAt(runs[i].from)) {
    const t = smoothstep((s - (runs[i].from - BLEND_METERS)) / (2 * BLEND_METERS))
    return inRun(prev, prev.to) * (1 - t) + own * t
  }
  const next = runs[i + 1]
  if (next && runs[i].to - s < BLEND_METERS && !cornerAt(runs[i].to)) {
    const t = smoothstep((s - (runs[i].to - BLEND_METERS)) / (2 * BLEND_METERS))
    return own * (1 - t) + inRun(next, next.from) * t
  }
  return own
}

function runIndexAt(runs: LaneRun[], s: number): number {
  if (runs.length === 0) return -1
  for (let i = 0; i < runs.length; i++) if (s < runs[i].to) return i
  return runs.length - 1
}

/** The lane change inside a run: [start, end] in route distance. */
function changeWindow(run: LaneRun): [number, number] | null {
  if (Math.abs(run.exit - run.entry) < 0.05) return null
  const lanes = Math.abs(run.exit - run.entry) / Math.max(run.laneWidth, 1)
  const end = Math.max(run.from, run.to - run.settleBefore)
  const length = Math.max(MIN_CHANGE_METERS, CHANGE_PER_LANE_METERS * lanes)
  return [Math.max(run.from, end - length), end]
}

function inRun(run: LaneRun, s: number): number {
  const w = changeWindow(run)
  if (!w) return run.entry
  const t = w[1] > w[0] ? smoothstep((s - w[0]) / (w[1] - w[0])) : s >= w[1] ? 1 : 0
  return run.entry + (run.exit - run.entry) * t
}

interface Arc {
  from: number
  to: number
  points: [number, number][]
}

/** The arc a corner's lane turns on, between the lines it leaves and joins. */
function cornerArc(line: Polyline, runs: LaneRun[], corners: Corner[], corner: Corner): Arc | null {
  const reach = 12
  const at = corner.at
  const origin = line.pointAt(at)
  const dIn = line.direction(at - reach, at)
  const dOut = line.direction(at, at + reach)
  const turn = Math.atan2(cross(dIn, dOut), dot(dIn, dOut))
  const deg = Math.abs((turn * 180) / Math.PI)
  if (deg < MIN_CORNER_DEG || deg > 165) return null

  const oIn = offsetAt(runs, corners, at - 0.01)
  const oOut = offsetAt(runs, corners, at + 0.01)
  const pIn = scale(right(dIn), oIn)
  const pOut = scale(right(dOut), oOut)
  // Where the two offset lines cross.
  const denom = cross(dIn, dOut)
  const k = cross(sub(pOut, pIn), dOut) / denom
  const c = add(pIn, scale(dIn, k))

  const half = Math.tan(Math.abs(turn) / 2)
  const before = runs[runIndexAt(runs, at - 0.01)]
  const after = runs[runIndexAt(runs, at + 0.01)]
  const room = Math.min(MAX_TANGENT_METERS, 0.45 * (before ? before.to - before.from : reach), 0.45 * (after ? after.to - after.from : reach))
  const tangent = Math.min(corner.radius * half, Math.max(1, room))
  const radius = tangent / half
  const a = sub(c, scale(dIn, tangent))
  const b = add(c, scale(dOut, tangent))
  const from = at + dot(a, dIn)
  const to = at + dot(b, dOut)
  if (!(from < to)) return null

  // Toward the inside of the turn: right for a right turn.
  const inside = turn < 0 ? right(dIn) : scale(right(dIn), -1)
  const centre = add(a, scale(inside, radius))
  const start = Math.atan2(a[1] - centre[1], a[0] - centre[0])
  const steps = Math.max(2, Math.ceil(deg / 10))
  const points: [number, number][] = []
  // Heading and position about the centre turn together.
  for (let i = 0; i <= steps; i++) {
    const ang = start + (turn * i) / steps
    points.push(fromLocal(origin, add(centre, [Math.cos(ang) * radius, Math.sin(ang) * radius])))
  }
  return { from, to, points }
}

/** The route in its lanes: `runs` cover it end to end, `corners` are its turns. */
export function laneLine(coords: [number, number][], runs: LaneRun[], corners: Corner[]): LaneLine {
  const line = new Polyline(coords)
  if (coords.length < 2 || line.length === 0) return { coordinates: coords.slice(), measures: line.cumulative.slice() }

  const arcs = corners
    .map(c => cornerArc(line, runs, corners, c))
    .filter((a): a is Arc => a != null)
    .sort((a, b) => a.from - b.from)
    .filter((a, i, all) => i === 0 || a.from > all[i - 1].to)

  // Where the offset moves, sample densely; elsewhere the vertices do.
  const busy: [number, number][] = []
  runs.forEach((run, i) => {
    const w = changeWindow(run)
    if (w) busy.push(w)
    if (i > 0 && Math.abs(inRun(runs[i - 1], run.from) - run.entry) > 0.05) busy.push([run.from - BLEND_METERS, run.from + BLEND_METERS])
  })
  const samples = new Map<number, number | null>()
  line.cumulative.forEach((s, i) => samples.set(s, i))
  for (const [a, b] of busy) {
    for (let s = Math.max(0, a); s < Math.min(line.length, b); s += SAMPLE_METERS) if (!samples.has(s)) samples.set(s, null)
  }

  const coordinates: [number, number][] = []
  const measures: number[] = []
  const push = (p: [number, number], s: number) => {
    const last = coordinates.at(-1)
    if (last && last[0] === p[0] && last[1] === p[1]) return
    coordinates.push(p)
    measures.push(s)
  }
  const outside = [...samples.keys()].filter(s => !arcs.some(a => s >= a.from && s <= a.to))
  const events = [...outside.map(s => ({ s, arc: null as Arc | null })), ...arcs.map(a => ({ s: a.from, arc: a }))]
  for (const { s, arc } of events.sort((a, b) => a.s - b.s)) {
    if (arc) {
      const n = arc.points.length - 1
      arc.points.forEach((p, i) => push(p, arc.from + ((arc.to - arc.from) * i) / n))
      continue
    }
    const origin = line.pointAt(s)
    const o = offsetAt(runs, corners, s)
    push(Math.abs(o) < 1e-3 ? origin : fromLocal(origin, scale(line.sideAt(s, samples.get(s) ?? null), o)), s)
  }
  return { coordinates, measures }
}

/** Degrees the route turns at `at`, right positive, read over `reach` metres either side. */
export function headingChange(coords: [number, number][], at: number, reach = 15): number {
  const line = new Polyline(coords)
  const dIn = line.direction(at - reach, at)
  const dOut = line.direction(at, at + reach)
  return (-Math.atan2(cross(dIn, dOut), dot(dIn, dOut)) * 180) / Math.PI
}

/** Metres along `coords` to the point on it nearest `p`. */
export function distanceAlong(coords: [number, number][], p: [number, number]): number {
  const cumulative = cumulativeDistances(coords)
  let best = Infinity
  let along = 0
  for (let i = 0; i + 1 < coords.length; i++) {
    const a = toLocal(p, coords[i])
    const b = toLocal(p, coords[i + 1])
    const ab = sub(b, a)
    const l2 = dot(ab, ab)
    const t = l2 > 0 ? Math.min(1, Math.max(0, -dot(a, ab) / l2)) : 0
    const d = Math.hypot(a[0] + ab[0] * t, a[1] + ab[1] * t)
    if (d < best) {
      best = d
      along = cumulative[i] + (cumulative[i + 1] - cumulative[i]) * t
    }
  }
  return along
}
