/**
 * Bridge deck geometry and height profiles.
 *
 * Positions are Web Mercator units (0-1 across the world), heights metres. The
 * rules match Parchment's client-side fallback (`web/src/lib/map-decks/decks.ts`)
 * so a deck looks the same whichever side solved it:
 *
 *   - an end that meets a road or railway on the ground sits on the ground
 *   - an end that meets another deck rests on that deck
 *   - between, the deck clears the ground by its layer's clearance, climbs no
 *     steeper than MAX_GRADE from a landed end, and eases into a vertical curve
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
  /** Metres from the centreline to the left and right edges. */
  edges: [number, number]
}

/** Steepest a deck climbs, as a grade. */
export const MAX_GRADE = 0.06
/** Height per OSM layer that a deck stands clear of the ground beneath it. */
export const LAYER_CLEARANCE = 6
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

/**
 * Ways joined end to end into chains. OSM ways share their end nodes, so ends
 * match exactly; a node where three or more bridge ways meet ends them all.
 */
export function chains(ways: Way[]): Chain[] {
  const ends = new Map<string, Way[]>()
  for (const w of ways)
    for (const p of [w.points[0], w.points[w.points.length - 1]]) ends.set(key(p), [...(ends.get(key(p)) ?? []), w])
  const used = new Set<number>()
  const out: Chain[] = []
  const next = (p: Point, from: Way) => {
    const at = ends.get(key(p)) ?? []
    if (at.length !== 2) return null
    const other = at[0] === from ? at[1] : at[0]
    return other !== from && !used.has(other.id) && other.kind === from.kind && other.layer === from.layer ? other : null
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
  const roads = decks.filter(d => d.kind === 'road').map(d => ({ ...d, edges: [...d.edges] as [number, number], ways: [...d.ways] }))
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
        near.every(n => n.left === side && n.distance < Math.max(road.edges[side ? 0 : 1], 1) + MAX_REACH / 2)
    })
    if (!host) {
      kept.push(path)
      continue
    }
    const near = samples.map(q => beside(host.points, q)).filter(n => n.alongside)
    const side = near[0].left ? 0 : 1
    host.edges[side] = Math.max(host.edges[side], ...near.map(n => n.distance + width / 2))
    host.ways.push(...path.ways)
  }
  return [...roads, ...kept]
}

/** Each vertex's points on a deck's left and right edges. */
export function edgePoints(chain: Pick<Chain, 'points' | 'edges'>): [Point[], Point[]] {
  const pts = chain.points
  const n = pts.length
  const scale = 1 / metresPerUnit(pts[Math.floor(n / 2)][1])
  const left: Point[] = []
  const right: Point[] = []
  pts.forEach((p, i) => {
    const [a, b] = [pts[Math.max(0, i - 1)], pts[Math.min(n - 1, i + 1)]]
    const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    const [nx, ny] = [(-(b[1] - a[1]) / len) * scale, ((b[0] - a[0]) / len) * scale]
    left.push([p[0] + nx * chain.edges[0], p[1] + ny * chain.edges[0]])
    right.push([p[0] - nx * chain.edges[1], p[1] - ny * chain.edges[1]])
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

/**
 * Deck height at every vertex. Each grounded end sits on the ground, an end
 * resting on another deck takes the height given, and an end that is neither
 * stays a layer's clearance up. Between, the deck runs straight from end to
 * end, lifted to its clearance, and held to MAX_GRADE from each anchored end.
 */
export function solve(
  chain: Pick<Chain, 'points' | 'layer'> & { grounded: [boolean, boolean] },
  ground: number[],
  resting: [number | null, number | null] = [null, null],
): number[] {
  const d = along(chain.points)
  const total = d[d.length - 1] || 1
  const clearance = LAYER_CLEARANCE * Math.max(1, chain.layer)
  const n = ground.length
  const end = (i: 0 | 1, g: number) => resting[i] ?? (chain.grounded[i] ? g : g + clearance)
  const anchored = [chain.grounded[0] || resting[0] !== null, chain.grounded[1] || resting[1] !== null]
  const za = end(0, ground[0])
  const zb = end(1, ground[n - 1])
  return ground.map((g, i) => {
    const straight = za + ((zb - za) * d[i]) / total
    let z = Math.max(straight, g + clearance)
    if (anchored[0]) z = Math.min(z, za + MAX_GRADE * d[i])
    if (anchored[1]) z = Math.min(z, zb + MAX_GRADE * (total - d[i]))
    return Math.max(z, Math.max(straight, g))
  })
}

/** A profile eased into a vertical curve over `span` metres, ends kept, never below the ground. */
export function smooth(z: number[], d: number[], ground: number[], span = 24): number[] {
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

/**
 * Mapped deck heights (OSM `ele` on the bridge or its nodes) pulled into a
 * profile: the difference at each one is spread linearly to its neighbours and
 * to zero at each grounded end, and the deck never sinks below the ground.
 */
export function anchor(z: number[], d: number[], ground: number[], anchors: Array<{ at: number; ele: number }>, grounded: [boolean, boolean]): number[] {
  if (!anchors.length) return z
  const total = d[d.length - 1]
  const at = (s: number) => {
    for (let i = 1; i < d.length; i++) if (s <= d[i]) return z[i - 1] + ((z[i] - z[i - 1]) * (s - d[i - 1])) / (d[i] - d[i - 1] || 1)
    return z[z.length - 1]
  }
  const knots = anchors.map(a => ({ s: Math.max(0, Math.min(total, a.at)), r: a.ele - at(a.at) })).sort((a, b) => a.s - b.s)
  if (grounded[0] && knots[0].s > 0) knots.unshift({ s: 0, r: 0 })
  if (grounded[1] && knots[knots.length - 1].s < total) knots.push({ s: total, r: 0 })
  return z.map((v, i) => {
    let r = knots[0].r
    if (d[i] >= knots[knots.length - 1].s) r = knots[knots.length - 1].r
    else
      for (let k = 1; k < knots.length; k++)
        if (d[i] <= knots[k].s) {
          r = knots[k - 1].r + ((knots[k].r - knots[k - 1].r) * (d[i] - knots[k - 1].s)) / (knots[k].s - knots[k - 1].s || 1)
          break
        }
    return Math.max(v + r, ground[i])
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
 */
export function joinNeighbours(decks: Array<{ chain: Chain; z: number[] }>, gap = 1.5, step = 1.5) {
  for (const [a, A] of decks.entries())
    for (const [b, B] of decks.entries()) {
      if (a === b || A.chain.kind === 'rail' || B.chain.kind === 'rail') continue
      A.chain.points.forEach((p, i) => {
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
        if (near.distance > A.chain.edges[facing] + B.chain.edges[near.left ? 0 : 1] + gap) return
        A.z[i] = Math.max(A.z[i], zb)
      })
    }
}
