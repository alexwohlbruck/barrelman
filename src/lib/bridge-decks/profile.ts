/**
 * Bridge deck geometry and height profiles.
 *
 * Positions are Web Mercator units (0-1 across the world), heights metres.
 * Parchment's client-side fallback (`web/src/lib/map-decks/decks.ts`) follows
 * the same rules, with a layer's clearance standing in for the crossings it
 * cannot see:
 *
 *   - an end that meets a road or railway on the ground sits on the ground
 *   - an end that meets another deck rests on that deck
 *   - between, the deck is one arch: the lowest concave profile over the ground
 *     and what it crosses, climbing no steeper than MAX_GRADE from an end that
 *     is held, eased into vertical curves. A bridge never sags between two
 *     things it clears, so a long viaduct over a street grid runs level
 *   - decks side by side at about one height are joined at the higher one
 */

export type Point = [number, number]

export type Kind = 'road' | 'rail' | 'path'

export type Way = {
  id: number
  points: Point[]
  kind: Kind
  /** OSM `layer`, at least 1. */
  layer: number
  /** Carriageway width in metres. */
  width: number
  /** The road it carries, by class and ref or name, so a junction carries the road on rather than a ramp that happens to run straighter. */
  road?: string
}

export type Chain = {
  ways: number[]
  points: Point[]
  kind: Kind
  layer: number
  /** Metres from the centreline to the left and right edges: the widest, where they vary. */
  edges: [number, number]
  /** Metres from the centreline to the left and right edges at each point, where they vary. */
  sides?: [number[], number[]]
  /**
   * A road deck's edges fitted to the bridge outline it lies in, before it is
   * shaped to it (`edges` stays its carriageway, the least a shape may be):
   * how far it reaches when deciding what meets it and what it takes in, and
   * its edges if it cannot be shaped.
   */
  fit?: [number, number]
  /** Sidewalks folded in: the side, how far out from the centreline, and the stretch along the deck in metres. */
  paths?: Array<{ side: 0 | 1; out: number; from: number; to: number }>
}

/** Steepest a deck climbs, as a grade. */
export const MAX_GRADE = 0.06
/** Height per OSM layer that a deck stands clear of the ground beneath it. */
export const LAYER_CLEARANCE = 6
/** Length of the level stretch kept over what a deck crosses, so easing it does not cut the clearance, in metres. */
export const CURVE = 24
/** Length of the vertical curve a deck's grades are eased into, in metres. */
export const VERTICAL_CURVE = 90
/** Metres each side of a sample whose ground is read together, so a lone tree, mast or stray pixel does not lift a deck. */
export const GROUND_SPAN = 12
/** Farthest an outline point may lie from a deck's centreline and still be its edge, in metres. */
export const MAX_REACH = 16

const WORLD = 40075016.686

export function mercator(lng: number, lat: number): Point {
  const s = Math.sin((lat * Math.PI) / 180)
  return [(lng + 180) / 360, 0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)]
}

export function lngLat([x, y]: Point): [number, number] {
  return [x * 360 - 180, (Math.atan(Math.sinh(Math.PI * (1 - 2 * y))) * 180) / Math.PI]
}

/** Metres per mercator unit at a mercator y. */
export function metresPerUnit(y: number): number {
  return WORLD * Math.cos(Math.atan(Math.sinh(Math.PI * (1 - 2 * y))))
}

/** A bounding box in mercator units: west, north, east, south (y grows southward). */
export type Box = [number, number, number, number]

function boxOf(points: Point[]): Box {
  let [x0, y0, x1, y1] = [Infinity, Infinity, -Infinity, -Infinity]
  for (const [x, y] of points) [x0, y0, x1, y1] = [Math.min(x0, x), Math.min(y0, y), Math.max(x1, x), Math.max(y1, y)]
  return [x0, y0, x1, y1]
}

const boxes = new WeakMap<Point[], Box>()

/** A line's bounding box, worked out once per points array. */
export function box(points: Point[]): Box {
  let b = boxes.get(points)
  if (!b) boxes.set(points, (b = boxOf(points)))
  return b
}

/** Whether two boxes overlap. */
export const meet = (a: Box, b: Box) => a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]

/** Whether a point lies in a box. */
export const covers = (b: Box, [x, y]: Point) => x >= b[0] && x <= b[2] && y >= b[1] && y <= b[3]

/**
 * A box grown by `metres` on every side, at the scale of its poleward edge so
 * it is never short: the cheap test that rules a pair of decks out before the
 * point-by-point one.
 */
export function grow([x0, y0, x1, y1]: Box, metres: number): Box {
  const u = metres / Math.min(metresPerUnit(y0), metresPerUnit(y1))
  return [x0 - u, y0 - u, x1 + u, y1 + u]
}

/** Distances along a line in metres, from its first point. */
export function along(points: Point[]): number[] {
  const out = [0]
  for (let i = 1; i < points.length; i++) {
    const [x0, y0] = points[i - 1]
    const [x1, y1] = points[i]
    out.push(out[i - 1] + Math.hypot(x1 - x0, y1 - y0) * metresPerUnit((y0 + y1) / 2))
  }
  return out
}

/** A line resampled every `step` metres along it, ending on its last point. */
export function resample(points: Point[], step: number): Point[] {
  const d = along(points)
  const total = d[d.length - 1]
  const n = Math.max(1, Math.ceil(total / step - 1e-9))
  const out: Point[] = []
  let i = 1
  for (let k = 0; k <= n; k++) {
    const at = Math.min(k * step, total)
    while (i < d.length - 1 && d[i] < at) i++
    const t = (at - d[i - 1]) / (d[i] - d[i - 1] || 1)
    const [a, b] = [points[i - 1], points[i]]
    out.push([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t])
  }
  return out
}

const key = (p: Point) => `${p[0]},${p[1]}`

/** Unit direction leaving a way's end at `p`, into the way. */
function leaving(w: Way, p: Point): Point {
  const pts = key(w.points[0]) === key(p) ? w.points : [...w.points].reverse()
  const [a, b] = [pts[0], pts[1]]
  const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
  return [(b[0] - a[0]) / len, (b[1] - a[1]) / len]
}

/** Least turn, as the cosine between the two ways leaving a node, at which a road counts as carrying straight on. */
const ONWARD = -0.5

/**
 * Ways joined end to end into chains, one per roadway. OSM ways share their
 * end nodes, so ends match exactly. Only ways of one kind and layer join, so
 * stacked decks never do; where three or more meet, the pair that carries one
 * road on (same class and ref or name, turning no sharper than ONWARD) joins,
 * else the straightest pair, and the rest end there.
 */
export function chains(ways: Way[]): Chain[] {
  const ends = new Map<string, Way[]>()
  for (const w of ways)
    for (const p of [w.points[0], w.points[w.points.length - 1]]) ends.set(key(p), [...(ends.get(key(p)) ?? []), w])
  const used = new Set<number>()
  const out: Chain[] = []
  const next = (p: Point, from: Way) => {
    const like = (ends.get(key(p)) ?? []).filter(o => o.kind === from.kind && o.layer === from.layer)
    if (like.length < 2) return null
    let best: [Way, Way] | null = null
    let score = -Infinity
    for (let i = 0; i < like.length; i++)
      for (let j = i + 1; j < like.length; j++) {
        const [u, v] = [leaving(like[i], p), leaving(like[j], p)]
        const dot = u[0] * v[0] + u[1] * v[1]
        const same = like[i].road !== undefined && like[i].road === like[j].road && dot <= ONWARD
        if ((same ? 2 : 0) - dot > score) [best, score] = [[like[i], like[j]], (same ? 2 : 0) - dot]
      }
    const other = best?.[0] === from ? best[1] : best?.[1] === from ? best[0] : null
    return other && other !== from && !used.has(other.id) ? other : null
  }
  for (const start of [...ways].sort((a, b) => a.id - b.id)) {
    if (used.has(start.id)) continue
    used.add(start.id)
    let points = [...start.points]
    const members = [start]
    for (const forward of [true, false]) {
      let tail = members[forward ? members.length - 1 : 0]
      for (;;) {
        const end = forward ? points[points.length - 1] : points[0]
        const w = next(end, tail)
        if (!w) break
        used.add(w.id)
        const wp = key(w.points[0]) === key(end) ? w.points : [...w.points].reverse()
        points = forward ? [...points, ...wp.slice(1)] : [...[...wp].reverse(), ...points.slice(1)]
        if (forward) members.push(w)
        else members.unshift(w)
        tail = w
      }
    }
    const width = Math.max(...members.map(m => m.width))
    out.push({ ways: members.map(m => m.id), points, kind: start.kind, layer: start.layer, edges: [width / 2, width / 2] })
  }
  return out
}

/**
 * Where a point lies beside a line: metres from it, which side, whether it
 * falls alongside a segment rather than off either end, and where.
 */
export function beside(points: Point[], q: Point, segments?: number[]): { distance: number; left: boolean; alongside: boolean; segment: number; t: number } {
  let best = { distance: Infinity, left: true, alongside: false, segment: 1, t: 0 }
  const scale = metresPerUnit(q[1])
  const count = segments ? segments.length : points.length - 1
  for (let k = 0; k < count; k++) {
    const i = segments ? segments[k] : k + 1
    const [a, b] = [points[i - 1], points[i]]
    const dx = b[0] - a[0]
    const dy = b[1] - a[1]
    const raw = ((q[0] - a[0]) * dx + (q[1] - a[1]) * dy) / (dx * dx + dy * dy || 1)
    const t = Math.max(0, Math.min(1, raw))
    const distance = Math.hypot(a[0] + dx * t - q[0], a[1] + dy * t - q[1]) * scale
    if (distance < best.distance) {
      const alongside = (raw >= 0 || i > 1) && (raw <= 1 || i < points.length - 1)
      best = { distance, left: (q[0] - a[0]) * -dy + (q[1] - a[1]) * dx > 0, alongside, segment: i, t }
    }
  }
  return best
}

/** Metres across a cell of a line's segment index. */
const INDEX_CELL = 24

const indexes = new WeakMap<Point[], { cell: number; cells: Map<number, number[]> }>()

/** A line's segments by grid cell, each by the index of its second point; built once per points array. */
function segmentIndex(points: Point[]) {
  let index = indexes.get(points)
  if (index) return index
  const cell = INDEX_CELL / metresPerUnit(points[0][1])
  const cells = new Map<number, number[]>()
  for (let i = 1; i < points.length; i++) {
    const [a, b] = [points[i - 1], points[i]]
    for (let x = Math.floor(Math.min(a[0], b[0]) / cell); x <= Math.floor(Math.max(a[0], b[0]) / cell); x++)
      for (let y = Math.floor(Math.min(a[1], b[1]) / cell); y <= Math.floor(Math.max(a[1], b[1]) / cell); y++) {
        const k = x * 4194304 + y
        const list = cells.get(k)
        if (list) list.push(i)
        else cells.set(k, [i])
      }
  }
  indexes.set(points, (index = { cell, cells }))
  return index
}

/** The segments of a line that may lie within `metres` of a point, by the index of their second point. */
export function nearSegments(points: Point[], p: Point, metres: number): number[] {
  const { cell, cells } = segmentIndex(points)
  const r = metres / metresPerUnit(p[1])
  const out = new Set<number>()
  for (let x = Math.floor((p[0] - r) / cell); x <= Math.floor((p[0] + r) / cell); x++)
    for (let y = Math.floor((p[1] - r) / cell); y <= Math.floor((p[1] + r) / cell); y++)
      for (const i of cells.get(x * 4194304 + y) ?? []) out.add(i)
  return [...out]
}

/**
 * Road decks fitted to an outline (a `man_made=bridge` area or the
 * carriageway's kerbs): each outline point goes to the nearest road deck, and
 * a deck's edge on each side is the farthest of its points there. A side with
 * none mirrors the other. `rivals` are other decks the points may be nearer
 * to, which take them without being fitted, so a deck never reaches across
 * its twin to the twin's far kerb.
 */
export function fitEdges(decks: Chain[], outline: Point[], rivals: Chain[] = []): Chain[] {
  const roads = decks.filter(d => d.kind === 'road')
  const others = rivals.filter(d => d.kind === 'road' && !roads.includes(d))
  const reach = new Map<Chain, [number, number]>()
  for (const q of outline) {
    let nearest: Chain | null = null
    let near: ReturnType<typeof beside> | null = null
    for (const road of [...roads, ...others]) {
      if (!covers(grow(box(road.points), MAX_REACH), q)) continue
      const b = beside(road.points, q)
      if (!near || b.distance < near.distance) [nearest, near] = [road, b]
    }
    if (!nearest || !near || !roads.includes(nearest) || !near.alongside || near.distance >= MAX_REACH) continue
    const r = reach.get(nearest) ?? [0, 0]
    r[near.left ? 0 : 1] = Math.max(r[near.left ? 0 : 1], near.distance)
    reach.set(nearest, r)
  }
  return decks.map(d => {
    const r = reach.get(d)
    if (!r || (!r[0] && !r[1])) return d
    return { ...d, edges: [r[0] || r[1], r[1] || r[0]] }
  })
}

/**
 * Sidewalks and cycle tracks mapped as bridges of their own beside a road
 * bridge, folded into its deck (`paths`): the deck widens to take them in
 * where they run beside it (see `widen`), not along the whole of it.
 */
export function absorbPaths(decks: Chain[]): Chain[] {
  const roads = decks.filter(d => d.kind === 'road').map(d => ({ ...d, ways: [...d.ways], paths: [...(d.paths ?? [])] }))
  const kept: Chain[] = []
  for (const path of decks) {
    if (path.kind !== 'path') {
      if (path.kind !== 'road') kept.push(path)
      continue
    }
    const samples = path.points.flatMap((p, i) => (i ? [[(p[0] + path.points[i - 1][0]) / 2, (p[1] + path.points[i - 1][1]) / 2] as Point, p] : [p]))
    const width = path.edges[0] + path.edges[1]
    const host = roads.find(road => {
      if (road.layer !== path.layer) return false
      const near = samples.map(q => beside(road.points, q))
      const side = near[0].left
      const inside = near.filter(n => n.alongside).map(n => n.distance)
      return inside.length >= near.length / 2 && Math.max(...inside) - Math.min(...inside) < 4 &&
        near.every(n => n.left === side && n.distance < Math.max((road.fit ?? road.edges)[side ? 0 : 1], 1) + MAX_REACH / 2)
    })
    if (!host) {
      kept.push(path)
      continue
    }
    const near = samples.map(q => beside(host.points, q)).filter(n => n.alongside)
    const d = along(host.points)
    const at = near.map(n => d[n.segment - 1] + (d[n.segment] - d[n.segment - 1]) * n.t)
    host.paths.push({ side: near[0].left ? 0 : 1, out: Math.max(...near.map(n => n.distance + width / 2)), from: Math.min(...at), to: Math.max(...at) })
    host.ways.push(...path.ways)
  }
  return [...roads, ...kept]
}

/** A resampled deck widened to take in its sidewalks, one sample beyond each end of where they run beside it. */
export function widen<C extends Pick<Chain, 'points' | 'edges' | 'sides' | 'paths'>>(chain: C, step: number): C {
  if (!chain.paths?.length) return chain
  const d = along(chain.points)
  const sides = ([0, 1] as const).map(side => chain.sides?.[side].slice() ?? chain.points.map(() => chain.edges[side])) as [number[], number[]]
  for (const { side, out, from, to } of chain.paths)
    d.forEach((s, i) => {
      if (s >= from - step && s <= to + step) sides[side][i] = Math.max(sides[side][i], out)
    })
  return { ...chain, sides, edges: [Math.max(...sides[0]), Math.max(...sides[1])] }
}

/** Metres from a deck's centreline to one side (0 left, 1 right) at a point, or between two by `t`. */
export function sideAt(chain: Pick<Chain, 'edges' | 'sides'>, side: 0 | 1, i: number, t = 0): number {
  const s = chain.sides?.[side]
  if (!s) return chain.edges[side]
  return t ? s[i] + (s[Math.min(i + 1, s.length - 1)] - s[i]) * t : s[i]
}

/** Whether a point lies inside a ring. */
export function inside([x, y]: Point, ring: Point[]): boolean {
  let hit = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i]
    const [xj, yj] = ring[j]
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) hit = !hit
  }
  return hit
}

/** Each vertex's points on a deck's left and right edges. */
export function edgePoints(chain: Pick<Chain, 'points' | 'edges' | 'sides'>): [Point[], Point[]] {
  const pts = chain.points
  const n = pts.length
  const scale = 1 / metresPerUnit(pts[Math.floor(n / 2)][1])
  const left: Point[] = []
  const right: Point[] = []
  pts.forEach((p, i) => {
    const [a, b] = [pts[Math.max(0, i - 1)], pts[Math.min(n - 1, i + 1)]]
    const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    const [nx, ny] = [(-(b[1] - a[1]) / len) * scale, ((b[0] - a[0]) / len) * scale]
    const [l, r] = [sideAt(chain, 0, i), sideAt(chain, 1, i)]
    left.push([p[0] + nx * l, p[1] + ny * l])
    right.push([p[0] - nx * r, p[1] - ny * r])
  })
  return [left, right]
}

/**
 * The ground a deck must clear: under its centreline, or higher under either
 * edge, but rising toward an edge no faster than the deck may climb from its
 * nearer end, so the banks beside an abutment are not climbed over.
 */
export function besideGround(centre: number[], left: number[], right: number[], d: number[]): number[] {
  const total = d[d.length - 1] ?? 0
  return centre.map((g, i) => {
    const edges = [left[i], right[i]].filter(h => !Number.isNaN(h))
    const base = Number.isNaN(g) ? (edges.length ? Math.min(...edges) : NaN) : g
    if (Number.isNaN(base) || !edges.length) return base
    return Math.max(base, Math.min(Math.max(...edges), base + MAX_GRADE * Math.min(d[i], total - d[i])))
  })
}

/** What a deck must stand clear of at a distance along it: an absolute height for its surface. */
export type Need = { at: number; height: number }

/**
 * Ground with lone spikes taken out: at each sample, the median of those
 * within GROUND_SPAN of it, the span shrinking toward the ends so a slope
 * reads true and each end keeps its own.
 */
export function steady(ground: number[], d: number[]): number[] {
  const total = d[d.length - 1] ?? 0
  return ground.map((_, i) => {
    const half = Math.min(GROUND_SPAN, d[i], total - d[i])
    const near: number[] = []
    for (let k = i; k >= 0 && d[i] - d[k] <= half; k--) near.push(ground[k])
    for (let k = i + 1; k < ground.length && d[k] - d[i] <= half; k++) near.push(ground[k])
    near.sort((a, b) => a - b)
    return near[near.length >> 1]
  })
}

/**
 * The lowest concave profile on or over `lower`, through its first and last
 * values: a line drawn taut across the tops of what a deck must clear. It
 * has crests and no sags, as a bridge's grade line does.
 */
export function arch(d: number[], lower: number[]): number[] {
  const hull: number[] = []
  for (let i = 0; i < lower.length; i++) {
    while (hull.length >= 2) {
      const [a, b] = [hull[hull.length - 2], hull[hull.length - 1]]
      if ((d[b] - d[a]) * (lower[i] - lower[a]) - (lower[b] - lower[a]) * (d[i] - d[a]) < 0) break
      hull.pop()
    }
    hull.push(i)
  }
  let k = 0
  return d.map(s => {
    while (k < hull.length - 2 && d[hull[k + 1]] < s) k++
    const [a, b] = [hull[k], hull[Math.min(k + 1, hull.length - 1)]]
    return a === b ? lower[a] : lower[a] + ((lower[b] - lower[a]) * (s - d[a])) / (d[b] - d[a] || 1)
  })
}

/**
 * A deck's grade line over `lower`: its arch, held to MAX_GRADE from each end
 * given a height in `anchors` (the ones on the ground or resting on a deck),
 * but never below the straight line between its ends, which it takes where
 * they lie farther apart in height than MAX_GRADE allows.
 */
export function align(d: number[], lower: number[], anchors: [number | null, number | null]): number[] {
  const n = lower.length
  const total = d[n - 1] || 1
  return arch(d, lower).map((z, i) => {
    if (anchors[0] !== null) z = Math.min(z, anchors[0] + MAX_GRADE * d[i])
    if (anchors[1] !== null) z = Math.min(z, anchors[1] + MAX_GRADE * (total - d[i]))
    return Math.max(z, lower[0] + ((lower[n - 1] - lower[0]) * d[i]) / total)
  })
}

/**
 * Deck height at every vertex. Each grounded end sits on the ground and an end
 * resting on another deck takes the height given; an end that is neither stays
 * a layer's clearance up. Between, the deck arches over the ground and what it
 * crosses (each held level for CURVE metres), and is held to MAX_GRADE from
 * each anchored end.
 */
export function solve(
  chain: Pick<Chain, 'points' | 'layer'> & { grounded: [boolean, boolean] },
  ground: number[],
  needs: Need[],
  resting: [number | null, number | null] = [null, null],
): number[] {
  const d = along(chain.points)
  const n = ground.length
  const end = (i: 0 | 1, g: number) => {
    const rest = resting[i]
    return rest !== null ? Math.max(rest, g) : chain.grounded[i] ? g : g + LAYER_CLEARANCE * Math.max(1, chain.layer)
  }
  const anchored = [chain.grounded[0] || resting[0] !== null, chain.grounded[1] || resting[1] !== null]
  const [za, zb] = [end(0, ground[0]), end(1, ground[n - 1])]
  const lower = steady(ground, d)
  for (const need of needs)
    for (let i = 0; i < n; i++) if (Math.abs(d[i] - need.at) <= CURVE / 2) lower[i] = Math.max(lower[i], need.height)
  lower[0] = za
  lower[n - 1] = zb
  return align(d, lower, [anchored[0] ? za : null, anchored[1] ? zb : null])
}

/** Where one line crosses another, as distances along the first; touching at the first's ends does not count. */
export function crossings(line: Point[], other: Point[], margin = 2, d = along(line)): number[] {
  const total = d[d.length - 1]
  const out: number[] = []
  for (let i = 1; i < line.length; i++) {
    const [p, r] = [line[i - 1], [line[i][0] - line[i - 1][0], line[i][1] - line[i - 1][1]]]
    for (let j = 1; j < other.length; j++) {
      const [q, s] = [other[j - 1], [other[j][0] - other[j - 1][0], other[j][1] - other[j - 1][1]]]
      const den = r[0] * s[1] - r[1] * s[0]
      if (!den) continue
      const t = ((q[0] - p[0]) * s[1] - (q[1] - p[1]) * s[0]) / den
      const u = ((q[0] - p[0]) * r[1] - (q[1] - p[1]) * r[0]) / den
      if (t < 0 || t > 1 || u < 0 || u > 1) continue
      const at = d[i - 1] + (d[i] - d[i - 1]) * t
      if (at > margin && at < total - margin) out.push(at)
    }
  }
  return out
}

/**
 * A profile eased into vertical curves `span` metres long, never below the
 * ground. Each height is averaged over a window that shrinks toward the ends,
 * so the ends stay put and a straight grade stays straight.
 */
export function smooth(z: number[], d: number[], ground: number[], span = VERTICAL_CURVE): number[] {
  const n = z.length
  const total = d[n - 1] ?? 0
  let lo = 0
  return z.map((_, i) => {
    const half = Math.min(span / 2, d[i], total - d[i])
    if (half <= 0) return z[i]
    while (d[i] - d[lo] >= half) lo++
    let sum = 0
    let weight = 0
    for (let k = lo; k < n && d[k] - d[i] < half; k++) {
      const w = half - Math.abs(d[k] - d[i])
      sum += z[k] * w
      weight += w
    }
    return Math.max(sum / weight, ground[i])
  })
}

/** Height at a distance along a profile. */
export function heightAt(d: number[], z: number[], s: number): number {
  if (s <= 0) return z[0]
  for (let i = 1; i < d.length; i++) if (s <= d[i]) return z[i - 1] + ((z[i] - z[i - 1]) * (s - d[i - 1])) / (d[i] - d[i - 1] || 1)
  return z[z.length - 1]
}

/** How far from where a ramp rests on a road the road is not lifted to it, in metres: a ramp climbing away at a gentle grade is still beside it. */
export const LEAVING = 200

/**
 * Decks that run side by side as one: where a deck's edge meets another's
 * within `gap` metres and at most `step` from its height, both take the
 * higher height. Decks farther apart in height (an upper and lower deck) keep
 * their own.
 *
 * An end that lands on the ground stays there: lifting it to a neighbour
 * would leave the deck hanging over the road it lands on. Nor is a deck
 * lifted near where another rests on it (`on`): a ramp climbing away from the
 * road it leaves takes that road's height while beside it, not the other way
 * round.
 */
export function joinNeighbours(
  decks: Array<{ chain: Chain; z: number[]; grounded?: [boolean, boolean]; on?: Array<{ deck: unknown; at: Point }> }>,
  gap = 1.5,
  step = 1.5,
) {
  const widest = decks.map(D => Math.max(...D.chain.edges))
  for (const [a, A] of decks.entries())
    for (const [b, B] of decks.entries()) {
      if (a === b || A.chain.kind === 'rail' || B.chain.kind === 'rail') continue
      const leaves = (B.on ?? []).filter(r => r.deck === A).map(r => r.at)
      // Farther apart than both decks' widest sides and the gap, no edge can meet the other.
      if (!meet(grow(box(A.chain.points), widest[a] + widest[b] + gap), box(B.chain.points))) continue
      const last = A.chain.points.length - 1
      // Farthest a point of one may lie from the other and still meet it.
      const reach = widest[a] + widest[b] + gap
      A.chain.points.forEach((p, i) => {
        if ((i === 0 && A.grounded?.[0]) || (i === last && A.grounded?.[1])) return
        if (leaves.some(q => Math.hypot(q[0] - p[0], q[1] - p[1]) * metresPerUnit(p[1]) < LEAVING)) return
        const candidates = nearSegments(B.chain.points, p, reach)
        if (!candidates.length) return
        const near = beside(B.chain.points, p, candidates)
        if (!near.alongside) return
        const [j, t] = [near.segment, near.t]
        const zb = B.z[j - 1] + (B.z[j] - B.z[j - 1]) * t
        if (Math.abs(zb - A.z[i]) > step) return
        const q: Point = [
          B.chain.points[j - 1][0] + (B.chain.points[j][0] - B.chain.points[j - 1][0]) * t,
          B.chain.points[j - 1][1] + (B.chain.points[j][1] - B.chain.points[j - 1][1]) * t,
        ]
        const facing = beside(A.chain.points, q, nearSegments(A.chain.points, q, reach)).left ? 0 : 1
        if (near.distance > sideAt(A.chain, facing, i) + sideAt(B.chain, near.left ? 0 : 1, j - 1, t) + gap) return
        A.z[i] = Math.max(A.z[i], zb)
      })
    }
}
