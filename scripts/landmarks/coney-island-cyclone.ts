/**
 * Coney Island Cyclone — procedural, CC0-1.0.
 * bun scripts/landmarks/coney-island-cyclone.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see ANCHOR), on the
 * ground. Coney Island is flat beach fill, so y = 0 is simply the street.
 *
 * Plan: the two OSM `roller_coaster=track` ways (way/376073239 and
 * way/656566282, both oneway) are one closed circuit of two laps, an outer and
 * an inner long oval nested in the S-shaped lot between West 10th Street and
 * the aquarium car park. Their node order is the direction of
 * travel. They match the aerial photo well; where three tracks share the
 * narrow waist, a few points are nudged sideways so that tracks running
 * side by side at the same height don't overlap.
 *
 * Profile (OSM has no heights): the station sits inside the north loop
 * (way/248496353, 834 Surf Avenue). The train leaves it southward, climbs the
 * lift to the 26 m crest in the middle of the lot — where the CYCLONE sign
 * faces east and west — and plunges down the first drop into the tall fan
 * turn at the south end. It comes back north low under the lift, climbs into
 * the tall turn over the Surf Avenue corner, runs south high over the station,
 * makes the lower turn at the south end, and returns low to the station. So
 * both ends are the two-level stacked turnarounds the photos show.
 *
 * Style: the track is one broad smooth ribbon, banked in the turns — a brown
 * wooden deck with red sides and red edge bands, as the red rails and fencing
 * read from the street. The white steel structure is not drawn member by
 * member: under every stretch of track stands a white "wall" of chunky bents
 * and horizontal ledgers at common levels, which leaves a few big regular
 * openings, the way recessed window bands stand in for windows on a building.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

// ---------------------------------------------------------------- frame ----
// OSM coordinates are projected about this reference point (equirectangular,
// metres), then shifted so the origin is ANCHOR.
const LAT0 = 40.575, LON0 = -73.9778
const MX = 111320 * Math.cos((LAT0 * Math.PI) / 180), MY = 110574
// Centre of the track's bounding box, in the projected frame.
const ANCHOR: [number, number] = [7.85, -44.3]

// ------------------------------------------------------------- vectors -----
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A quad whose winding is chosen so it faces `out`, with per-corner normals. */
function face(p: Part, P: V3[], N: V3[] | V3) {
  const ns = (Array.isArray(N[0]) ? N : [N, N, N, N]) as V3[]
  const f = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = ns.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(f, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [ns[a], ns[b], ns[c]])
  p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [ns[a], ns[c], ns[d]])
}

// ------------------------------------------------------------- circuit -----
// [x, y, z] in the projected frame; z = null is interpolated along the track.
// Comments give the OSM node index along the two ways (Y = way/376073239,
// C = way/656566282) where a point is an OSM node.
type K = [number, number, number | null]
const CIRCUIT: K[] = [
  // Station: down the west side of the station shed, inside it (nudged 1.5 m
  // east of C50→Y0 so the deck stays under the roof).
  [3.7, 16.5, 4],      // C50
  [6.7, 10, 2],
  [9.0, 0, 2],
  [11.2, -11, 2.2],
  // Lift hill, southward.
  [13.0, -20, null],
  [14.3, -29.5, null], // Y0
  [14.0, -38.5, null], // Y1
  [12.3, -50.1, 25.4], // Y2 — the crest, 26 m
  [11.0, -53.5, 26],
  [9.7, -57.0, 25],    // Y3
  // First drop, curving slightly, into the tall south fan turn.
  [5.0, -68, null],
  [0.3, -79.5, 3],
  [-6.7, -94.5, null], // Y4
  [-8.3, -102.0, 17.5], // Y5
  [-7.9, -105.4, 19.5],
  [-5.9, -109.0, 20],
  [-2.1, -112.8, 20],
  [2.4, -114.4, 20],
  [6.8, -113.7, 20],
  [10.9, -110.7, 20],
  [12.8, -106.4, 19.5],
  [13.4, -102.5, 17],  // Y13
  // Back north, low under the lift and crest.
  [12.2, -91, 6],
  [11.0, -79.6, 3.5],  // Y14
  [11.1, -73.1, 4],    // Y15
  [12.1, -62, 10],
  [13.0, -56.6, 3.5],  // Y16
  [16.6, -28, 3],
  [19.1, -8, null],
  // The tall turn over the Surf Avenue corner.
  [22.1, 15.5, 17],    // Y17
  [20.8, 20.4, 17],
  [18.3, 22.8, 17],
  [14.6, 23.8, 17],
  [10.6, 23.1, 17],
  [8.5, 21.7, 17],
  [6.7, 18.7, 16.8],
  [5.9, 16.9, 16.5],   // Y24
  // South, high over the station.
  [5.2, 5.4, 13],      // Y25
  [5.6, 0.9, 11.5],
  [7.3, -8.3, 12],
  [10.6, -30.7, 15.5],   // Y28 = C0, nudged 1.7 m west of the lift
  [9.4, -43.8, 14],    // nudged 1.5 m west
  [9.4, -50.4, 9],
  [5.5, -58.2, 7.5],
  [0.1, -68.1, 12],
  [-3.4, -74.6, 11.5],
  // The lower south turn, inside the tall one.
  [-4.6, -77.2, 10.5],
  [-4.7, -80.8, 10],
  [-3.8, -84.1, 10],
  [-1.3, -86.9, 10],
  [2.2, -88.3, 10],
  [7.0, -87.7, 10],
  [10.4, -85.1, 10],
  [12.6, -80.5, 10],
  [13.7, -72.9, 10],   // C12
  // North along the east side, outside the low return run.
  [17.0, -57, 5],
  [18.7, -42, 10],
  [20.4, -25, 6.5],
  [22.2, -8, 4.5],
  [23.5, 3, 8],
  [24.0, 15.8, 9],     // C13
  // The lower turn at the corner, then down into the station.
  [22.7, 20.8, 9],
  [20.2, 23.9, 9],
  [15.4, 25.5, 9],
  [11.2, 25.8, 8.5],
  [7.7, 24.2, 7.5],
  [4.5, 20.5, 6],
]

// The station shed (way/248496353) and the small shed under the south turn
// (way/248496460), in the projected frame, with their OSM heights.
const STATION: [number, number][] = [
  [13.3, 13.9], [18.9, 15.4], [19.1, 2.9], [16.0, 2.8], [17.1, -0.9], [19.2, -0.9],
  [19.4, -5.7], [16.2, -6.1], [16.9, -12.6], [11.4, -14.0], [4.3, 15.8], [12.4, 18.0],
]
const STATION_H = 6.4
const SHED: [number, number][] = [[5.8, -111.4], [-2.1, -110.5], [-1.3, -103.7], [6.6, -104.6]]
const SHED_H = 4.5

// ------------------------------------------------------------ the spline ---
const STEP = 2.5      // sweep segment length, metres
const W = 3.0         // deck width
const EDGE = 0.6      // red edge band on the deck
const DEPTH = 0.9     // ribbon depth (the red side)
const HEADER = 1.1    // the white stringer band under it, the top of the trestle wall

const pts = CIRCUIT.map(([x, y]) => [x - ANCHOR[0], y - ANCHOR[1]] as [number, number])
const n = pts.length

// Dense centripetal Catmull–Rom through the plan points (closed).
const dense: { x: number; y: number; knot: number }[] = []
for (let i = 0; i < n; i++) {
  const p0 = pts[(i - 1 + n) % n], p1 = pts[i], p2 = pts[(i + 1) % n], p3 = pts[(i + 2) % n]
  const d = (a: number[], b: number[]) => Math.max(Math.hypot(a[0] - b[0], a[1] - b[1]) ** 0.5, 1e-3)
  const t0 = 0, t1 = t0 + d(p0, p1), t2 = t1 + d(p1, p2), t3 = t2 + d(p2, p3)
  const SUB = 40
  for (let k = 0; k < SUB; k++) {
    const t = t1 + ((t2 - t1) * k) / SUB
    const L = (a: number[], b: number[], ta: number, tb: number) =>
      [0, 1].map((c) => ((tb - t) * a[c] + (t - ta) * b[c]) / (tb - ta))
    const A1 = L(p0, p1, t0, t1), A2 = L(p1, p2, t1, t2), A3 = L(p2, p3, t2, t3)
    const B1 = L(A1, A2, t0, t2), B2 = L(A2, A3, t1, t3)
    const C = L(B1, B2, t1, t2)
    dense.push({ x: C[0], y: C[1], knot: k === 0 ? i : -1 })
  }
}
// Arc length of the dense polyline, and the arc length at each circuit point.
const dS = [0]
for (let i = 1; i <= dense.length; i++) {
  const a = dense[i - 1], b = dense[i % dense.length]
  dS.push(dS[i - 1] + Math.hypot(b.x - a.x, b.y - a.y))
}
const TOTAL = dS[dense.length]
const knotS = CIRCUIT.map((_, i) => dS[dense.findIndex((p) => p.knot === i)])

// Heights: monotone cubic (Fritsch–Carlson) through the given z's, closed.
const zk = CIRCUIT.map((c, i) => [knotS[i], c[2]] as const).filter(([, z]) => z !== null) as [number, number][]
// The knots repeated one lap either side, so the curve wraps smoothly.
const ext = [-1, 0, 1].flatMap((w) => zk.map(([ks, z]) => [ks + w * TOTAL, z] as [number, number]))
function heightAt(s: number) {
  s = ((s % TOTAL) + TOTAL) % TOTAL
  const j = ext.findIndex(([ks], k) => ks <= s && ext[k + 1][0] > s)
  const [sm, zm] = ext[j - 1], [s0, z0] = ext[j], [s1, z1] = ext[j + 1], [s2, z2] = ext[j + 2]
  const d0 = (z1 - z0) / (s1 - s0), dm = (z0 - zm) / (s0 - sm), d2 = (z2 - z1) / (s2 - s1)
  const slope = (a: number, b: number) => (a * b <= 0 ? 0 : (2 * a * b) / (a + b))
  const m0 = slope(dm, d0), m1 = slope(d0, d2)
  const h = s1 - s0, t = (s - s0) / h
  const h00 = 2 * t ** 3 - 3 * t ** 2 + 1, h10 = t ** 3 - 2 * t ** 2 + t
  const h01 = -2 * t ** 3 + 3 * t ** 2, h11 = t ** 3 - t ** 2
  return h00 * z0 + h10 * h * m0 + h01 * z1 + h11 * h * m1
}
function planAt(s: number): [number, number] {
  s = ((s % TOTAL) + TOTAL) % TOTAL
  let lo = 0, hi = dense.length
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1
    if (dS[mid] <= s) lo = mid
    else hi = mid
  }
  const a = dense[lo], b = dense[(lo + 1) % dense.length], t = (s - dS[lo]) / (dS[lo + 1] - dS[lo] || 1)
  return [a.x + (b.x - a.x) * t, a.y + (b.y - a.y) * t]
}

// Uniform samples along the circuit.
const N = Math.round(TOTAL / STEP)
type Sample = { p: V3; t: V3; left: V3; up: V3; s: number }
const samples: Sample[] = []
const heading: number[] = []
for (let i = 0; i < N; i++) {
  const s = (i * TOTAL) / N
  const [x0, y0] = planAt(s - 0.5), [x1, y1] = planAt(s + 0.5)
  heading.push(Math.atan2(y1 - y0, x1 - x0))
}
// Bank from plan curvature, smoothed: a fan turn on a wooden twister leans
// hard, so a 10 m radius gets about 35°.
const curv = heading.map((_, i) => {
  let d = heading[(i + 1) % N] - heading[(i - 1 + N) % N]
  while (d > Math.PI) d -= 2 * Math.PI
  while (d < -Math.PI) d += 2 * Math.PI
  return d / (2 * STEP)
})
const bank = curv.map((_, i) => {
  let sum = 0, w = 0
  for (let k = -3; k <= 3; k++) {
    const wk = 4 - Math.abs(k)
    sum += curv[(i + k + N) % N] * wk
    w += wk
  }
  return Math.max(-0.62, Math.min(0.62, (sum / w) * 6))
})
for (let i = 0; i < N; i++) {
  const s = (i * TOTAL) / N
  const [x, y] = planAt(s)
  const z = heightAt(s)
  const [xa, ya] = planAt(s + 0.6), [xb, yb] = planAt(s - 0.6)
  const t = unit([xa - xb, ya - yb, heightAt(s + 0.6) - heightAt(s - 0.6)])
  const up0 = unit(sub([0, 0, 1], mul(t, t[2])))
  const left0 = cross(up0, t)
  // A left turn (positive curvature) lowers the left, inner, edge.
  const b = bank[i], c = Math.cos(b), sn = Math.sin(b)
  const left = unit(sub(mul(left0, c), mul(up0, sn)))
  const up = unit(add(mul(up0, c), mul(left0, sn)))
  samples.push({ p: [x, y, z], t, left, up, s })
}

// ------------------------------------------------------------- the track ---
const deck = new Part(), rail = new Part(), white = new Part(), station = new Part(), roof = new Part()

function section(q: Sample) {
  const { p, left, up } = q
  const at = (l: number, u: number) => add(p, add(mul(left, l), mul(up, u)))
  return {
    L: at(W / 2, 0), Li: at(W / 2 - EDGE, 0), Ri: at(-W / 2 + EDGE, 0), R: at(-W / 2, 0),
    Lb: at(W / 2, -DEPTH), Rb: at(-W / 2, -DEPTH),
    Lh: at(W / 2 - 0.2, -DEPTH - HEADER), Rh: at(-W / 2 + 0.2, -DEPTH - HEADER),
    Lm: at(W / 2 - 0.2, -DEPTH), Rm: at(-W / 2 + 0.2, -DEPTH),
  }
}
for (let i = 0; i < N; i++) {
  const a = samples[i], b = samples[(i + 1) % N]
  const A = section(a), B = section(b)
  face(rail, [A.L, A.Li, B.Li, B.L], [a.up, a.up, b.up, b.up])
  face(deck, [A.Li, A.Ri, B.Ri, B.Li], [a.up, a.up, b.up, b.up])
  face(rail, [A.Ri, A.R, B.R, B.Ri], [a.up, a.up, b.up, b.up])
  face(rail, [A.L, B.L, B.Lb, A.Lb], [a.left, b.left, b.left, a.left])
  const ra = mul(a.left, -1), rb = mul(b.left, -1)
  face(rail, [A.R, A.Rb, B.Rb, B.R], [ra, ra, rb, rb])
  const da = mul(a.up, -1), db = mul(b.up, -1)
  // The underside of the red band, then the white stringer band below it.
  face(deck, [A.Lb, B.Lb, B.Lm, A.Lm], [da, db, db, da])
  face(deck, [A.Rm, B.Rm, B.Rb, A.Rb], [da, db, db, da])
  face(white, [A.Lm, B.Lm, B.Lh, A.Lh], [a.left, b.left, b.left, a.left])
  face(white, [A.Rm, A.Rh, B.Rh, B.Rm], [ra, ra, rb, rb])
  face(white, [A.Lh, B.Lh, B.Rh, A.Rh], [da, db, db, da])
}

// ------------------------------------------------------- the structure -----
// The lowest point a support under (x, y) can stand on: the ground, the top
// of the station roof, or the top of another stretch of track it would
// otherwise run through.
const inside = (poly: [number, number][], x: number, y: number) => {
  let c = false
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [xi, yi] = poly[i], [xj, yj] = poly[j]
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) c = !c
  }
  return c
}
const local = (poly: [number, number][]) => poly.map(([x, y]) => [x - ANCHOR[0], y - ANCHOR[1]] as [number, number])
const stationL = local(STATION), shedL = local(SHED)
function floorAt(x: number, y: number, below: number, self: number, reach: number) {
  let f = 0
  if (inside(stationL, x, y)) f = STATION_H
  if (inside(shedL, x, y)) f = Math.max(f, SHED_H)
  for (let k = 0; k < N; k++) {
    // Skip this stretch of track itself.
    const ds = Math.abs(samples[k].s - samples[self].s)
    if (Math.min(ds, TOTAL - ds) < 8) continue
    const q = samples[k].p
    if (q[2] >= below) continue
    if (Math.hypot(q[0] - x, q[1] - y) < reach) f = Math.max(f, q[2])
  }
  return f
}

/** A box between two plan points, `w` across, from z0 to z1 (no caps). */
function beam(p: Part, a: [number, number], b: [number, number], w: number, z0: number, z1: number, caps = false) {
  const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1
  const nx = -dy / l, ny = dx / l
  const c: V3[] = [
    [a[0] + nx * w / 2, a[1] + ny * w / 2, 0], [b[0] + nx * w / 2, b[1] + ny * w / 2, 0],
    [b[0] - nx * w / 2, b[1] - ny * w / 2, 0], [a[0] - nx * w / 2, a[1] - ny * w / 2, 0],
  ]
  const lo = c.map(([x, y]): V3 => [x, y, z0]), hi = c.map(([x, y]): V3 => [x, y, z1])
  const out: V3[] = [[nx, ny, 0], [dx / l, dy / l, 0], [-nx, -ny, 0], [-dx / l, -dy / l, 0]]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    // Side i runs from corner i to corner j; its outward normal:
    const o = i === 0 ? out[0] : i === 1 ? out[1] : i === 2 ? out[2] : out[3]
    face(p, [lo[i], lo[j], hi[j], hi[i]], o)
  }
  if (caps) {
    face(p, hi, [0, 0, 1])
    face(p, lo, [0, 0, -1])
  }
}

/** A horizontal beam from a to b: its long sides, top and underside. */
function ledger(p: Part, a: [number, number], b: [number, number], w: number, z0: number, z1: number) {
  const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1
  const nx = (-dy / l) * w / 2, ny = (dx / l) * w / 2
  const P = (q: [number, number], k: number, z: number): V3 => [q[0] + nx * k, q[1] + ny * k, z]
  face(p, [P(a, 1, z0), P(b, 1, z0), P(b, 1, z1), P(a, 1, z1)], [nx, ny, 0])
  face(p, [P(a, -1, z0), P(a, -1, z1), P(b, -1, z1), P(b, -1, z0)], [-nx, -ny, 0])
  face(p, [P(a, 1, z1), P(b, 1, z1), P(b, -1, z1), P(a, -1, z1)], [0, 0, 1])
  face(p, [P(a, 1, z0), P(a, -1, z0), P(b, -1, z0), P(b, 1, z0)], [0, 0, -1])
}

const BENT = 2               // a bent every BENT samples (5 m)
const POST_ALONG = 2.4, POST_ACROSS = W - 0.4
const LEDGER_H = 1.6, LEDGER_W = W - 0.8
const LEVELS = [3.8, 8.4, 13, 17.6, 22.2]
type Bent = { i: number; x: number; y: number; top: number; floor: number; dir: [number, number] }
const bents: Bent[] = []
for (let i = 0; i < N; i += BENT) {
  const q = samples[i]
  // The deck's lowest edge, so the bent tucks under a banked track.
  const top = q.p[2] - DEPTH - HEADER - Math.abs(Math.sin(bank[i])) * W / 2 + 0.3
  const floor = floorAt(q.p[0], q.p[1], q.p[2] - 1.5, i, 2.2)
  const dir: [number, number] = unit([q.t[0], q.t[1], 0]).slice(0, 2) as [number, number]
  bents.push({ i, x: q.p[0], y: q.p[1], top, floor, dir })
}
for (const b of bents) {
  if (b.top - b.floor < 0.6) continue
  const h = POST_ALONG / 2
  beam(white, [b.x - b.dir[0] * h, b.y - b.dir[1] * h], [b.x + b.dir[0] * h, b.y + b.dir[1] * h], POST_ACROSS, b.floor, b.top)
}
// Ledgers between neighbouring bents at the common levels, wherever both
// ends stand clear of the track above and of anything below.
for (let k = 0; k < bents.length; k++) {
  const a = bents[k], b = bents[(k + 1) % bents.length]
  for (const lv of LEVELS) {
    const z1 = lv + LEDGER_H / 2, z0 = lv - LEDGER_H / 2
    if (z1 > Math.min(a.top, b.top) - 1.5) continue
    if (z0 < Math.max(a.floor, b.floor) + 0.8) continue
    ledger(white, [a.x, a.y], [b.x, b.y], LEDGER_W, z0, z1)
  }
}

// ------------------------------------------------------------- buildings ---
/** Ear-clipping triangulation of a simple polygon (counter-clockwise). */
function triangulate(poly: [number, number][]) {
  const area = poly.reduce((s, p, i) => s + p[0] * poly[(i + 1) % poly.length][1] - poly[(i + 1) % poly.length][0] * p[1], 0)
  const idx = poly.map((_, i) => i)
  if (area < 0) idx.reverse()
  const out: number[][] = []
  const crossz = (o: number[], a: number[], b: number[]) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const A = poly[ia], B = poly[ib], C = poly[ic]
      if (crossz(A, B, C) <= 0) continue
      const ear = idx.every((j) => j === ia || j === ib || j === ic ||
        !(crossz(A, B, poly[j]) >= 0 && crossz(B, C, poly[j]) >= 0 && crossz(C, A, poly[j]) >= 0))
      if (!ear) continue
      out.push([ia, ib, ic])
      idx.splice(i, 1)
      break
    }
  }
  out.push(idx)
  return out
}
/** Walls and a roof, the roof set in behind a low bevelled parapet. */
function building(poly: [number, number][], h: number) {
  const area = poly.reduce((s, p, i) => s + p[0] * poly[(i + 1) % poly.length][1] - poly[(i + 1) % poly.length][0] * p[1], 0)
  const ring = area < 0 ? [...poly].reverse() : poly
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    const o = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    face(station, [[a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], h], [a[0], a[1], h]], o)
  }
  for (const [i, j, k] of triangulate(ring)) {
    const P = [ring[i], ring[j], ring[k]].map(([x, y]): V3 => [x, y, h])
    roof.tri(P[0], P[1], P[2], undefined, undefined, undefined, [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
  }
}
building(stationL, STATION_H)
building(shedL, SHED_H)

// ---------------------------------------------------------------- output ---
// Colours from the photos: brown-grey weathered deck boards, the red of the
// rails and fencing, white-painted steel, the station's white boarding and
// its pale grey roof (aerial photo).
const parts = [
  { part: deck, material: { name: 'deck', color: 0x7a5546 } },
  { part: rail, material: { name: 'track-red', color: 0xb3302b } },
  { part: white, material: { name: 'structure', color: 0xf0ede6 } },
  { part: station, material: { name: 'station', color: 0xe6e2d8 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
let maxZ = 0
for (const s of samples) maxZ = Math.max(maxZ, s.p[2])
let len3 = 0, steep = 0
for (let i = 0; i < N; i++) {
  const a = samples[i].p, b = samples[(i + 1) % N].p
  len3 += Math.hypot(b[0] - a[0], b[1] - a[1], b[2] - a[2])
  steep = Math.max(steep, Math.atan2(a[2] - b[2], Math.hypot(b[0] - a[0], b[1] - a[1])) * 180 / Math.PI)
}
console.log(`track ${len3.toFixed(0)} m (${TOTAL.toFixed(0)} m in plan), ${N} segments, crest ${maxZ.toFixed(1)} m, steepest descent ${steep.toFixed(0)}°, ${bents.length} bents`)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Coney Island Cyclone', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, crest: 26, trackLength: Math.round(TOTAL),
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/coney-island-cyclone.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(LON0 + ANCHOR[0] / MX).toFixed(7)}, ${(LAT0 + ANCHOR[1] / MY).toFixed(7)}`)

// Clearance report for development: stretches of track that pass within a
// deck's width of each other in plan, with less than 3 m between them.
if (process.env.CLEARANCE) {
  for (let i = 0; i < N; i++) for (let k = i + 1; k < N; k++) {
    const ds = Math.abs(samples[i].s - samples[k].s)
    if (Math.min(ds, TOTAL - ds) < 10) continue
    const a = samples[i].p, b = samples[k].p
    const d = Math.hypot(a[0] - b[0], a[1] - b[1]), dz = Math.abs(a[2] - b[2])
    if (d < W + 0.3 && dz < 3)
      console.log(`clash s=${samples[i].s.toFixed(0)} & ${samples[k].s.toFixed(0)} at (${(a[0] + ANCHOR[0]).toFixed(1)}, ${(a[1] + ANCHOR[1]).toFixed(1)}) plan ${d.toFixed(1)} dz ${dz.toFixed(1)}`)
  }
}
