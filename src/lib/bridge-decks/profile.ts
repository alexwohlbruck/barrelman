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
 *   - between, the deck runs straight from end to end, rising to clear what it
 *     crosses, climbing no steeper than MAX_GRADE, eased into a vertical curve
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
}

/** Steepest a deck climbs, as a grade. */
export const MAX_GRADE = 0.06
/** Height per OSM layer that a deck stands clear of the ground beneath it. */
export const LAYER_CLEARANCE = 6
/** Length of a vertical curve, and of the level stretch kept over what a deck crosses so easing it does not cut the clearance, in metres. */
export const CURVE = 24
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

/**
 * Ways joined end to end into chains, one per roadway. OSM ways share their
 * end nodes, so ends match exactly. Only ways of one kind and layer join, so
 * stacked decks never do; where three or more meet, the straightest pair
 * carries on and the rest end there.
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
    let straightest = Infinity
    for (let i = 0; i < like.length; i++)
      for (let j = i + 1; j < like.length; j++) {
        const [u, v] = [leaving(like[i], p), leaving(like[j], p)]
        const dot = u[0] * v[0] + u[1] * v[1]
        if (dot < straightest) [best, straightest] = [[like[i], like[j]], dot]
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
export function beside(points: Point[], q: Point): { distance: number; left: boolean; alongside: boolean; segment: number; t: number } {
  let best = { distance: Infinity, left: true, alongside: false, segment: 1, t: 0 }
  const scale = metresPerUnit(q[1])
  for (let i = 1; i < points.length; i++) {
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

/**
 * Road decks fitted to an outline (a `man_made=bridge` area or the
 * carriageway's kerbs): each outline point goes to the nearest road deck, and
 * a deck's edge on each side is the farthest of its points there. A side with
 * none mirrors the other.
 */
export function fitEdges(decks: Chain[], outline: Point[]): Chain[] {
  const roads = decks.filter(d => d.kind === 'road')
  const reach = new Map<Chain, [number, number]>()
  for (const q of outline) {
    let nearest: Chain | null = null
    let near: ReturnType<typeof beside> | null = null
    for (const road of roads) {
      const b = beside(road.points, q)
      if (!near || b.distance < near.distance) [nearest, near] = [road, b]
    }
    if (!nearest || !near || !near.alongside || near.distance >= MAX_REACH) continue
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
 * bridge, folded into its deck: the deck widens to take them in.
 */
export function absorbPaths(decks: Chain[]): Chain[] {
  const roads = decks.filter(d => d.kind === 'road').map(d => ({
    ...d,
    edges: [...d.edges] as [number, number],
    fit: d.fit ? ([...d.fit] as [number, number]) : undefined,
    ways: [...d.ways],
  }))
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
    const side = near[0].left ? 0 : 1
    const out = Math.max(...near.map(n => n.distance + width / 2))
    host.edges[side] = Math.max(host.edges[side], out)
    if (host.fit) host.fit[side] = Math.max(host.fit[side], out)
    host.ways.push(...path.ways)
  }
  return [...roads, ...kept]
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
 * Deck height at every vertex. Each grounded end sits on the ground and an end
 * resting on another deck takes the height given; an end that is neither stays
 * a layer's clearance up. Between, the deck runs straight from end to end,
 * rises to clear what it crosses with ramps no steeper than MAX_GRADE, and is
 * held to MAX_GRADE from each anchored end.
 */
export function solve(
  chain: Pick<Chain, 'points' | 'layer'> & { grounded: [boolean, boolean] },
  ground: number[],
  needs: Need[],
  resting: [number | null, number | null] = [null, null],
): number[] {
  const d = along(chain.points)
  const total = d[d.length - 1] || 1
  const n = ground.length
  const end = (i: 0 | 1, g: number) => resting[i] ?? (chain.grounded[i] ? g : g + LAYER_CLEARANCE * Math.max(1, chain.layer))
  const anchored = [chain.grounded[0] || resting[0] !== null, chain.grounded[1] || resting[1] !== null]
  const za = end(0, ground[0])
  const zb = end(1, ground[n - 1])
  return ground.map((g, i) => {
    const straight = za + ((zb - za) * d[i]) / total
    let z = straight
    for (const need of needs) z = Math.max(z, need.height - MAX_GRADE * Math.max(0, Math.abs(d[i] - need.at) - CURVE / 2))
    if (anchored[0]) z = Math.min(z, za + MAX_GRADE * d[i])
    if (anchored[1]) z = Math.min(z, zb + MAX_GRADE * (total - d[i]))
    return Math.max(z, straight, g)
  })
}

/** Where one line crosses another, as distances along the first; touching at the first's ends does not count. */
export function crossings(line: Point[], other: Point[], margin = 2): number[] {
  const d = along(line)
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

/** A profile eased into a vertical curve over `span` metres, ends kept, never below the ground. */
export function smooth(z: number[], d: number[], ground: number[], span = CURVE): number[] {
  const n = z.length
  return z.map((_, i) => {
    if (i === 0 || i === n - 1) return z[i]
    let sum = 0
    let weight = 0
    for (let k = 0; k < n; k++) {
      const w = span / 2 - Math.abs(d[k] - d[i])
      if (w > 0) {
        sum += z[k] * w
        weight += w
      }
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

/**
 * Decks that run side by side as one: where a deck's edge meets another's
 * within `gap` metres and at about its height, both take the higher height.
 * Decks at different heights (an upper and lower deck) keep their own.
 *
 * An end that lands on the ground stays there: lifting it to a neighbour
 * would leave the deck hanging over the road it lands on. Run it before
 * smoothing, so the steps it makes where a join begins are eased out with
 * the rest of the profile.
 */
export function joinNeighbours(decks: Array<{ chain: Chain; z: number[]; grounded?: [boolean, boolean] }>, gap = 1.5, step = 1.5) {
  const widest = decks.map(D => Math.max(...D.chain.edges))
  for (const [a, A] of decks.entries())
    for (const [b, B] of decks.entries()) {
      if (a === b || A.chain.kind === 'rail' || B.chain.kind === 'rail') continue
      // Farther apart than both decks' widest sides and the gap, no edge can meet the other.
      if (!meet(grow(box(A.chain.points), widest[a] + widest[b] + gap), box(B.chain.points))) continue
      const last = A.chain.points.length - 1
      A.chain.points.forEach((p, i) => {
        if ((i === 0 && A.grounded?.[0]) || (i === last && A.grounded?.[1])) return
        const near = beside(B.chain.points, p)
        if (!near.alongside) return
        const [j, t] = [near.segment, near.t]
        const zb = B.z[j - 1] + (B.z[j] - B.z[j - 1]) * t
        if (Math.abs(zb - A.z[i]) > step) return
        const q: Point = [
          B.chain.points[j - 1][0] + (B.chain.points[j][0] - B.chain.points[j - 1][0]) * t,
          B.chain.points[j - 1][1] + (B.chain.points[j][1] - B.chain.points[j - 1][1]) * t,
        ]
        const facing = beside(A.chain.points, q).left ? 0 : 1
        if (near.distance > sideAt(A.chain, facing, i) + sideAt(B.chain, near.left ? 0 : 1, j - 1, t) + gap) return
        A.z[i] = Math.max(A.z[i], zb)
      })
    }
}
