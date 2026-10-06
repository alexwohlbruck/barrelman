/**
 * Coney Island Cyclone — procedural, CC0-1.0.
 * bun scripts/landmarks/coney-island-cyclone.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see ANCHOR), on the
 * ground. Coney Island is flat beach fill, so z = 0 is simply the street.
 *
 * Sources. Ride data: RCDB 222 (2,850 ft of track, 75 ft to the track at the
 * top of the lift, 85 ft to the sign above it, 58.6° first drop), Wikipedia
 * (twelve drops, six fan turns, the ride in order), the 1988 Landmarks
 * Preservation Commission report (the CYCLONE letters "fixed to the side of
 * the ride's highest peak"). Plan: the two OSM `roller_coaster=track` ways
 * (way/376073239, way/656566282) laid over NYS 2025 orthoimagery. Heights and
 * hill shapes: photos from the Wonder Wheel and the Astrotower (Commons), and
 * along West 10th Street and Surf Avenue.
 *
 * The OSM trace is a schematic: two laps, 558 m in plan, every turn
 * anticlockwise. The real ride is three out-and-back laps, about 869 m, and
 * turns both ways. So the plan here is redrawn, lap by lap, from the ride
 * description and the photos; it keeps the OSM turnarounds where the
 * orthophoto confirms them and puts six tracks through the narrow waist of
 * the lot, where the photos and the orthophoto show five or six.
 *
 * The ride, in order (heights are the track):
 *   station    on the west side under the gabled shed, heading north
 *   N1         an immediate right U-turn at the Surf Avenue end, low (2.2 m)
 *   lift       straight south along the east side, 31°, to the crest (23 m)
 *              in the middle of the lot; the beach is ahead
 *   1st drop   58°, south-south-west, to 1.6 m
 *   S1         climb into the first high-speed U-turn, LEFT, round the very
 *              south end (16 m) — the one anticlockwise turn
 *   2nd drop   north again, diving under the lift crest (3 m), then climbing
 *              across the west side over the station approach
 *   N2         the tall right U-turn at the Surf Avenue corner (15 m)
 *   3rd drop   south down the east side, parallel to the lift; a camelback
 *   S2         a smaller banked right U-turn (4.2 m), diving under the
 *              climb into S1
 *   4th drop   north up the west side; the second camelback
 *   N3         right fan turn at the north end (8.6 m), inside N2 and over N1
 *   hops       south down the east edge: four small airtime hops
 *   S3         the last right U-turn, low under S1 (4 m)
 *   return     north along the west: two last hops, then low through the
 *              tunnel under the 2nd and 4th drops, brake run, station
 * Twelve drops in all, counted by the generator, as Wikipedia gives.
 * Heights pass an energy check (printed): from the 23 m crest, losing 2.5 m
 * of head per 100 m of track, every hill is crossed with speed to spare.
 *
 * The sign: red CYCLONE block letters on a white board hung on the side of
 * the crest just under the track, on both the west (West 10th Street) and
 * east faces, as in the photos and the 1988 LPC report. STYLE.md rules out lettering; this is an exception the user asked
 * for, because the sign is the ride's signature. The letters are extruded
 * blocks of a few rectangles each, not a texture.
 *
 * Style: the track is one broad smooth ribbon, banked in the turns — a brown
 * wooden deck between red edge bands, with red sides, as the red rails and
 * fencing read from the street. The white steel structure is drawn as a
 * white "wall" of chunky bents and ledgers under every stretch of track,
 * leaving a few big openings, the way window bands stand in for windows.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

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
type K = [number, number, number | null]
const CIRCUIT: K[] = []
const ELEMENTS: { name: string; knot: number }[] = []
const at = (x: number, y: number, z: number | null = null) => { CIRCUIT.push([x, y, z]) }
const mark = (name: string) => ELEMENTS.push({ name, knot: CIRCUIT.length })
/** A U-turn: points every ~30° round a circle, from angle a0 to a1 (degrees,
 *  anticlockwise positive, so a1 < a0 is a right turn), both ends included. */
function turn(cx: number, cy: number, r: number, a0: number, a1: number, z: number) {
  const n = Math.max(2, Math.round(Math.abs(a1 - a0) / 30))
  for (let i = 0; i <= n; i++) {
    const a = ((a0 + ((a1 - a0) * i) / n) * Math.PI) / 180
    at(cx + r * Math.cos(a), cy + r * Math.sin(a), z)
  }
}

mark('station')
at(13.3, -12, 2.4); at(13.3, 0, 2.4); at(13.3, 11, 2.4)
mark('N1, right U-turn')
turn(16.9, 17.5, 3.6, 180, 0, 2.2)
at(20.0, 10, 2.0); at(19.1, 0, 1.8)
mark('lift')
at(18.5, -8, 1.6)
at(15.9, -22, 10.1); at(14.7, -34, 17.4); at(14.0, -39.5, 20.8)
mark('crest')
at(13.6, -43.5, 22.5); at(12.9, -48, 23); at(12.2, -52, 22.6); at(11.6, -54.5, 21.6)
mark('first drop')
at(10.9, -57, 18.5); at(9.6, -60.5, 12.8); at(8.2, -64.5, 6); at(6.6, -69, 1.6)
at(3.6, -75.5, 6.4); at(-0.8, -83.5, 10.4); at(-5.0, -89.5, 13.6); at(-7.4, -96, 15.4)
mark('S1, left U-turn')
turn(3.5, -104, 11, 180, 360, 16)
mark('second drop, under the lift')
at(15.0, -95, 14.6); at(15.3, -86, 12.8); at(15.1, -77, 10.8); at(15.6, -68, 8.2)
at(14.8, -61.5, 4.2); at(12.8, -52, 3); at(13.8, -42, 5); at(13.2, -30, 7.6)
at(10.5, -21, 8.4); at(7.0, -12, 9.6); at(4.6, -2, 11.8); at(4.0, 7, 14)
mark('N2, right U-turn')
turn(14.5, 16.5, 10.5, 180, 0, 15)
mark('third drop, camelback')
at(25.0, 8, 14.4); at(23.6, -2, 12.6); at(21.3, -12, 9.6); at(19.9, -22, 5.8)
at(18.6, -31, 3.2); at(17.4, -41, 6.2); at(16.2, -50, 9.6); at(13.8, -57, 10)
at(12.6, -63, 9.2); at(11.2, -69, 4.2); at(11.4, -76, 3.8)
mark('S2, right U-turn')
turn(5.2, -81, 6.3, 0, -180, 4.2)
mark('fourth drop, camelback')
at(-1.0, -75, 4.8); at(1.0, -67, 7.6); at(3.6, -60, 8.3); at(6.0, -52, 7.8)
at(7.6, -44, 7.2); at(7.6, -36, 6.6); at(7.4, -28, 4.2); at(7.8, -18, 2.8)
at(7.4, -6, 4.2); at(7.1, 4, 6.4); at(7.0, 11, 8)
mark('N3, right fan turn')
turn(14.5, 17, 7.5, 180, 0, 8.6)
mark('airtime hops')
at(22.2, 9, 8); at(22.4, -2, 6.2); at(23.0, -12, 2.8); at(23.6, -22, 4.2)
at(22.4, -32, 2.6); at(21.0, -42, 5); at(19.3, -52, 2.6); at(18.4, -58, 4.4)
at(16.6, -67, 2.2); at(15.8, -77, 3.6); at(15.6, -86, 2.8); at(15.2, -94, 3.5)
at(13.6, -100, 4)
mark('S3, right U-turn')
turn(4, -104, 7.5, 0, -180, 4)
mark('return, tunnel and brakes')
at(-3.6, -96, 2.8); at(-4.6, -90, 1.8); at(-4.8, -83, 3.2); at(-3.6, -70, 1.6)
at(1.4, -61, 3.6); at(5.2, -53, 1.8); at(9.2, -44, 1.8); at(10.4, -36, 1.8)
at(11.8, -28, 2); at(12.9, -19, 2.3)

// The station: a gabled shed over the station track, open-sided in reality
// (arcades); here walls and a pitched roof. It replaces the OSM shed
// (way/248496353), which is drawn wider than the roof in the orthophoto.
const STATION = { x0: 9.6, x1: 17.0, y0: -13, y1: 15, eaves: 4.6, ridge: 6.4 }
// The small shed under the south turns (way/248496460), with its OSM height.
const SHED: [number, number][] = [[5.8, -111.4], [-2.1, -110.5], [-1.3, -103.7], [6.6, -104.6]]
const SHED_H = 3.5

// ------------------------------------------------------------ the spline ---
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

// Bank from plan curvature, on a 1 m grid, smoothed: a fan turn on a wooden
// twister leans hard, so a 10 m radius gets about 35°.
const G = Math.round(TOTAL)
const gridS = (i: number) => (i * TOTAL) / G
const heading = Array.from({ length: G }, (_, i) => {
  const [x0, y0] = planAt(gridS(i) - 0.5), [x1, y1] = planAt(gridS(i) + 0.5)
  return Math.atan2(y1 - y0, x1 - x0)
})
const wrap = (d: number) => Math.atan2(Math.sin(d), Math.cos(d))
const curv = heading.map((_, i) => wrap(heading[(i + 1) % G] - heading[(i - 1 + G) % G]) / (2 * TOTAL / G))
const bankGrid = curv.map((_, i) => {
  let sum = 0, w = 0
  for (let k = -7; k <= 7; k++) {
    const wk = 8 - Math.abs(k)
    sum += curv[(i + k + G) % G] * wk
    w += wk
  }
  return Math.max(-0.62, Math.min(0.62, (sum / w) * 6))
})
const pitch = (s: number) => Math.atan2(heightAt(s + 0.5) - heightAt(s - 0.5), 1)

// Samples along the circuit, closer where the track bends (in plan or in
// profile) than on the straights: a new sample once the direction has turned
// 9° or 6 m have passed.
const sampleS: number[] = [0]
{
  let last = 0
  for (let i = 1; i < G; i++) {
    const s = gridS(i)
    const turned = Math.abs(wrap(heading[i] - heading[Math.round((last * G) / TOTAL) % G]))
      + Math.abs(pitch(s) - pitch(last)) + Math.abs(bankGrid[i] - bankGrid[Math.round((last * G) / TOTAL) % G])
    if (s - last >= 6 || (turned > (9 * Math.PI) / 180 && s - last >= 1.8)) {
      sampleS.push(s)
      last = s
    }
  }
}
const N = sampleS.length
type Sample = { p: V3; t: V3; left: V3; up: V3; s: number; bank: number }
const samples: Sample[] = sampleS.map((s) => {
  const [x, y] = planAt(s)
  const z = heightAt(s)
  const [xa, ya] = planAt(s + 0.6), [xb, yb] = planAt(s - 0.6)
  const t = unit([xa - xb, ya - yb, heightAt(s + 0.6) - heightAt(s - 0.6)])
  const up0 = unit(sub([0, 0, 1], mul(t, t[2])))
  const left0 = cross(up0, t)
  // A left turn (positive curvature) lowers the left, inner, edge.
  const b = bankGrid[Math.round((s * G) / TOTAL) % G], c = Math.cos(b), sn = Math.sin(b)
  const left = unit(sub(mul(left0, c), mul(up0, sn)))
  const up = unit(add(mul(up0, c), mul(left0, sn)))
  return { p: [x, y, z], t, left, up, s, bank: b }
})

// ------------------------------------------------------------- the track ---
const deck = new Part(), red = new Part(), white = new Part(), station = new Part(), roof = new Part()

function section(q: Sample) {
  const { p, left, up } = q
  // Clamped to the ground: the low runs sit on it, the band doesn't sink in.
  const at = (l: number, u: number): V3 => { const q = add(p, add(mul(left, l), mul(up, u))); return [q[0], q[1], Math.max(q[2], 0.05)] }
  return {
    L: at(W / 2, 0), Li: at(W / 2 - EDGE, 0), Ri: at(-W / 2 + EDGE, 0), R: at(-W / 2, 0),
    Lb: at(W / 2, -DEPTH), Rb: at(-W / 2, -DEPTH),
    Lh: at(W / 2 - 0.2, -DEPTH - HEADER), Rh: at(-W / 2 + 0.2, -DEPTH - HEADER),
    Lm: at(W / 2 - 0.2, -DEPTH), Rm: at(-W / 2 + 0.2, -DEPTH),
  }
}
// Top (red band, deck, red band), the red sides, and the white stringer band
// below them. No underside: the track is only ever seen from above or level.
for (let i = 0; i < N; i++) {
  const a = samples[i], b = samples[(i + 1) % N]
  const A = section(a), B = section(b)
  face(red, [A.L, A.Li, B.Li, B.L], [a.up, a.up, b.up, b.up])
  face(deck, [A.Li, A.Ri, B.Ri, B.Li], [a.up, a.up, b.up, b.up])
  face(red, [A.Ri, A.R, B.R, B.Ri], [a.up, a.up, b.up, b.up])
  face(red, [A.L, B.L, B.Lb, A.Lb], [a.left, b.left, b.left, a.left])
  const ra = mul(a.left, -1), rb = mul(b.left, -1)
  face(red, [A.R, A.Rb, B.Rb, B.R], [ra, ra, rb, rb])
  face(white, [A.Lb, B.Lb, B.Lh, A.Lh], [a.left, b.left, b.left, a.left])
  face(white, [A.Rb, A.Rh, B.Rh, B.Rb], [ra, ra, rb, rb])
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
const stationL = local([[STATION.x0, STATION.y0], [STATION.x1, STATION.y0], [STATION.x1, STATION.y1], [STATION.x0, STATION.y1]])
const shedL = local(SHED)
// Dense points along the track centreline, for "what's under me" queries.
const probe = Array.from({ length: G }, (_, i) => { const [x, y] = planAt(gridS(i)); return { x, y, z: heightAt(gridS(i)), s: gridS(i) } })
function floorAt(x: number, y: number, below: number, selfS: number, reach: number) {
  let f = 0
  if (inside(stationL, x, y)) f = STATION.ridge
  if (inside(shedL, x, y)) f = Math.max(f, SHED_H)
  for (const q of probe) {
    const ds = Math.abs(q.s - selfS)
    if (Math.min(ds, TOTAL - ds) < 8) continue
    if (q.z - DEPTH >= below) continue
    if (Math.hypot(q.x - x, q.y - y) < reach) f = Math.max(f, q.z)
  }
  return f
}

/** A vertical box between two plan points, `w` across, from z0 to z1. */
function post(p: Part, a: [number, number], b: [number, number], w: number, z0: number, z1: number) {
  const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1
  const nx = -dy / l, ny = dx / l
  const c: [number, number][] = [
    [a[0] + (nx * w) / 2, a[1] + (ny * w) / 2], [b[0] + (nx * w) / 2, b[1] + (ny * w) / 2],
    [b[0] - (nx * w) / 2, b[1] - (ny * w) / 2], [a[0] - (nx * w) / 2, a[1] - (ny * w) / 2],
  ]
  const out: V3[] = [[nx, ny, 0], [dx / l, dy / l, 0], [-nx, -ny, 0], [-dx / l, -dy / l, 0]]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    face(p, [[c[i][0], c[i][1], z0], [c[j][0], c[j][1], z0], [c[j][0], c[j][1], z1], [c[i][0], c[i][1], z1]], out[i])
  }
}

/** A horizontal beam from a to b: its long sides and top. */
function ledger(p: Part, a: [number, number], b: [number, number], w: number, z0: number, z1: number) {
  const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1
  const nx = ((-dy / l) * w) / 2, ny = ((dx / l) * w) / 2
  const P = (q: [number, number], k: number, z: number): V3 => [q[0] + nx * k, q[1] + ny * k, z]
  face(p, [P(a, 1, z0), P(b, 1, z0), P(b, 1, z1), P(a, 1, z1)], [nx, ny, 0])
  face(p, [P(a, -1, z0), P(a, -1, z1), P(b, -1, z1), P(b, -1, z0)], [-nx, -ny, 0])
  face(p, [P(a, 1, z1), P(b, 1, z1), P(b, -1, z1), P(a, -1, z1)], [0, 0, 1])
}

const BENT_EVERY = 5       // metres of track between bents
const POST_ALONG = 2.4, POST_ACROSS = W - 0.4
const LEDGER_H = 1.4, LEDGER_W = W - 0.8
const LEVELS = [3.6, 7.4, 11.2, 15, 18.8]
type Bent = { x: number; y: number; top: number; floor: number; dir: [number, number] }
const bents: Bent[] = []
for (let k = 0, count = Math.round(TOTAL / BENT_EVERY); k < count; k++) {
  const s = (k * TOTAL) / count
  const [x, y] = planAt(s)
  const z = heightAt(s)
  const [xa, ya] = planAt(s + 0.6), [xb, yb] = planAt(s - 0.6)
  const b = Math.abs(bankGrid[Math.round((s * G) / TOTAL) % G])
  // The deck's lowest edge, so the bent tucks under a banked track.
  const top = z - DEPTH - HEADER - (Math.sin(b) * W) / 2 + 0.3
  const floor = floorAt(x, y, z - 1.5, s, 2.4)
  const l = Math.hypot(xa - xb, ya - yb) || 1
  bents.push({ x, y, top, floor, dir: [(xa - xb) / l, (ya - yb) / l] })
}
for (const b of bents) {
  if (b.top - b.floor < 0.6) continue
  const h = POST_ALONG / 2
  post(white, [b.x - b.dir[0] * h, b.y - b.dir[1] * h], [b.x + b.dir[0] * h, b.y + b.dir[1] * h], POST_ACROSS, b.floor, b.top)
}
// Ledgers between neighbouring bents at the common levels, wherever both
// ends stand clear of the track above and of anything below.
for (let k = 0; k < bents.length; k++) {
  const a = bents[k], b = bents[(k + 1) % bents.length]
  for (const lv of LEVELS) {
    const z1 = lv + LEDGER_H / 2, z0 = lv - LEDGER_H / 2
    if (z1 > Math.min(a.top, b.top) - 1.2) continue
    if (z0 < Math.max(a.floor, b.floor) + 0.8) continue
    ledger(white, [a.x, a.y], [b.x, b.y], LEDGER_W, z0, z1)
  }
}

// ------------------------------------------------------------- buildings ---
{
  // The station: four walls and a pitched roof, ridge north–south.
  const [x0, y0] = [STATION.x0 - ANCHOR[0], STATION.y0 - ANCHOR[1]]
  const [x1, y1] = [STATION.x1 - ANCHOR[0], STATION.y1 - ANCHOR[1]]
  const e = STATION.eaves, r = STATION.ridge, xm = (x0 + x1) / 2
  face(station, [[x0, y0, 0], [x1, y0, 0], [x1, y0, e], [x0, y0, e]], [0, -1, 0])
  face(station, [[x1, y1, 0], [x0, y1, 0], [x0, y1, e], [x1, y1, e]], [0, 1, 0])
  face(station, [[x0, y1, 0], [x0, y0, 0], [x0, y0, e], [x0, y1, e]], [-1, 0, 0])
  face(station, [[x1, y0, 0], [x1, y1, 0], [x1, y1, e], [x1, y0, e]], [1, 0, 0])
  station.tri([x0, y0, e], [x1, y0, e], [xm, y0, r])
  station.tri([x1, y1, e], [x0, y1, e], [xm, y1, r])
  const nw = unit([-(r - e), 0, xm - x0]), ne = unit([r - e, 0, x1 - xm])
  face(roof, [[x0, y0, e], [xm, y0, r], [xm, y1, r], [x0, y1, e]], nw)
  face(roof, [[xm, y0, r], [x1, y0, e], [x1, y1, e], [xm, y1, r]], ne)
  // The small shed: a box.
  const s = shedL
  for (let i = 0; i < 4; i++) {
    const a = s[i], b = s[(i + 1) % 4]
    face(station, [[a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], SHED_H], [a[0], a[1], SHED_H]], [b[1] - a[1], -(b[0] - a[0]), 0])
  }
  face(roof, s.map(([x, y]): V3 => [x, y, SHED_H]), [0, 0, 1])
}

// ------------------------------------------------------------- the sign ----
// CYCLONE in red block letters on a white board, hung on the side of the
// crest just under the track, once on the west face (read from West 10th
// Street and the Wonder Wheel) and once on the east. Measured off the Wonder
// Wheel and West 10th Street photos against the 23 m crest: letters about
// 2 m tall and nearly as wide, the word about 12 m long, its top a metre and
// a half under the rails, under the flat top of the hump.
const letters = new Part()
const SIGN = { cap: 2.0, top: 21.3, depth: 0.25, board: 0.12 }
// Glyphs on a 5 × 7 grid: each a few convex polygons (bars and diagonals).
const R = (x0: number, y0: number, x1: number, y1: number): [number, number][] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
const S = 1.5 // stroke
const GLYPHS: Record<string, [number, number][][]> = {
  C: [R(0, 0, S, 7), R(0, 7 - S, 5, 7), R(0, 0, 5, S)],
  Y: [[[0, 7], [1.6, 7], [3.2, 3.3], [1.8, 3.3]], [[3.4, 7], [5, 7], [3.2, 3.3], [1.8, 3.3]], R(1.8, 0, 3.2, 3.4)],
  L: [R(0, 0, S, 7), R(0, 0, 4.6, S)],
  O: [R(0, 0, S, 7), R(5 - S, 0, 5, 7), R(0, 0, 5, S), R(0, 7 - S, 5, 7)],
  N: [R(0, 0, S, 7), R(5 - S, 0, 5, 7), [[0, 7], [S, 7], [5, 0], [5 - S, 0]]],
  E: [R(0, 0, S, 7), R(0, 7 - S, 4.8, 7), R(0, 2.8, 4.2, 4.2), R(0, 0, 4.8, S)],
}
const WORD = 'CYCLONE', ADVANCE = 6.2, UNIT = SIGN.cap / 7
const WORD_W = (WORD.length * ADVANCE - (ADVANCE - 5)) * UNIT
{
  // Along a straight chord of the crest, by its highest point.
  let top = 0
  for (let i = 1; i < N; i++) if (samples[i].p[2] > samples[top].p[2]) top = i
  const c = samples[top]
  const along = unit([c.t[0], c.t[1], 0])          // the direction of travel, south-ish
  const west: V3 = [along[1], -along[0], 0]         // right of travel: west
  const deckAt = (q: V3) => {
    let best = Infinity, z = 0
    for (const p of probe) {
      const d = Math.hypot(p.x - q[0], p.y - q[1])
      if (d < best && p.z > 15) { best = d; z = p.z }
    }
    return z
  }
  const zBase = SIGN.top - SIGN.cap
  for (const side of [1, -1]) {
    const normal = mul(west, side)
    // Shifted 1.5 m back from the apex, so the first drop, which bends west
    // as it falls, doesn't cut across the last letter.
    const base = add(add([c.p[0], c.p[1], 0], mul(along, -1.5)), mul(normal, W / 2 + 0.05))
    // From the west, facing east, the viewer's right is south, the direction
    // of travel over the crest; from the east it is north.
    const right = side === 1 ? along : mul(along, -1)
    // The board: from under the letters up to the red side of the track, in
    // a few strips so its top follows the crest.
    const half = WORD_W / 2 + 0.5, strips = 4
    const P0 = (u: number, d: number, z: number): V3 => add(add(base, mul(right, u)), add(mul(normal, d), [0, 0, z]))
    const topAt = (u: number) => Math.max(deckAt(P0(u, 0, 0)) - DEPTH, SIGN.top + 0.25)
    for (let k = 0; k < strips; k++) {
      const u0 = -half + (2 * half * k) / strips, u1 = -half + (2 * half * (k + 1)) / strips
      face(white, [P0(u0, SIGN.board, zBase - 0.35), P0(u1, SIGN.board, zBase - 0.35), P0(u1, SIGN.board, topAt(u1)), P0(u0, SIGN.board, topAt(u0))], normal)
    }
    face(white, [P0(-half, 0, zBase - 0.35), P0(half, 0, zBase - 0.35), P0(half, SIGN.board, zBase - 0.35), P0(-half, SIGN.board, zBase - 0.35)], [0, 0, -1])
    for (let li = 0; li < WORD.length; li++) {
      const x0 = -WORD_W / 2 + li * ADVANCE * UNIT
      for (const poly of GLYPHS[WORD[li]]) {
        const P = (g: [number, number], d: number): V3 => P0(x0 + g[0] * UNIT, SIGN.board + d, zBase + g[1] * UNIT)
        // The front face (convex), then the side walls, each facing away
        // from the polygon's centre.
        face(letters, poly.map((g) => P(g, SIGN.depth)), normal)
        const cen = poly.reduce((s, g) => [s[0] + g[0] / poly.length, s[1] + g[1] / poly.length], [0, 0]) as [number, number]
        for (let i = 0; i < poly.length; i++) {
          const g0 = poly[i], g1 = poly[(i + 1) % poly.length]
          const out = unit(cross(sub(P(g1, 0), P(g0, 0)), normal))
          const mid = P([(g0[0] + g1[0]) / 2, (g0[1] + g1[1]) / 2], 0)
          face(letters, [P(g0, 0), P(g1, 0), P(g1, SIGN.depth), P(g0, SIGN.depth)], dot(out, sub(mid, P(cen, 0))) >= 0 ? out : mul(out, -1))
        }
      }
    }
  }
}

// ---------------------------------------------------------------- output ---
// Palette: the white steel is `trim`; the station walls `stone` and roofs
// `roof`. Two finishes carry the ride's identity: the brown-grey weathered
// deck boards, and one muted red for the rails, fencing and the sign's
// letters, pulled towards the palette's lightness.
const RED = finish('cyclone-red', 0xb94a42)
const parts = [
  { part: deck, material: finish('cyclone-deck', 0x8e7462) },
  { part: red, material: RED },
  { part: white, material: PALETTE.trim },
  { part: station, material: PALETTE.stone },
  { part: roof, material: PALETTE.roof },
]
// The letters share the red material; one primitive per material.
red.pos.push(...letters.pos); red.nrm.push(...letters.nrm); red.uv.push(...letters.uv)
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)

// Report: length, steepest descent, the elements and an energy check.
let len3 = 0, steep = 0, maxZ = 0
const S3 = Array.from({ length: G }, (_, i) => gridS(i))
const s3: number[] = [0]
for (let i = 0; i < G; i++) {
  const a = S3[i], b = S3[(i + 1) % G] || TOTAL
  const [x0, y0] = planAt(a), [x1, y1] = planAt(b), z0 = heightAt(a), z1 = heightAt(b)
  const d = Math.hypot(x1 - x0, y1 - y0, z1 - z0)
  len3 += d
  s3.push(len3)
  maxZ = Math.max(maxZ, z0)
  steep = Math.max(steep, (Math.atan2(z0 - z1, Math.hypot(x1 - x0, y1 - y0)) * 180) / Math.PI)
}
console.log(`track ${len3.toFixed(0)} m (${TOTAL.toFixed(0)} m in plan), ${N} sections, crest ${maxZ.toFixed(1)} m, steepest descent ${steep.toFixed(0)}°, ${bents.length} bents`)
{
  // Energy: from the crest, v²/2g = h_crest − h − loss, the loss 2.5 m of
  // head per 100 m of track. Speeds at each element, and the slowest point.
  const LOSS = 0.025
  let crestI = 0
  for (let i = 0; i < G; i++) if (heightAt(S3[i]) > heightAt(S3[crestI])) crestI = i
  const zc = heightAt(S3[crestI]) + 0.3 // the train crests at walking pace
  let worst = { v: Infinity, s: 0, z: 0 }
  const vAt = (i: number) => {
    const travelled = (s3[i] - s3[crestI] + len3) % len3
    const head = zc - heightAt(S3[i]) - LOSS * travelled
    return { v: Math.sign(head) * Math.sqrt(2 * 9.81 * Math.abs(head)), travelled }
  }
  // From the crest round to the brakes (the lift and station are powered).
  const stationI = Math.round((knotS[ELEMENTS.find((e) => e.name.startsWith('return'))!.knot + 9] * G) / TOTAL)
  for (let i = (crestI + 10) % G; i !== stationI; i = (i + 1) % G) {
    const { v } = vAt(i)
    if (v < worst.v) worst = { v, s: S3[i], z: heightAt(S3[i]) }
  }
  for (const e of ELEMENTS) {
    const i0 = Math.round((knotS[e.knot] * G) / TOTAL) % G
    const next = ELEMENTS[ELEMENTS.indexOf(e) + 1]
    const i1 = next ? Math.round((knotS[next.knot] * G) / TOTAL) % G : G
    let hi = -Infinity, lo = Infinity
    for (let i = i0; i < i1; i++) { hi = Math.max(hi, heightAt(S3[i])); lo = Math.min(lo, heightAt(S3[i])) }
    const [x, y] = planAt(S3[i0])
    const speed = i0 <= crestI ? 'powered' : `v ${(vAt(i0).v * 3.6).toFixed(0)} km/h`
    console.log(`  ${e.name.padEnd(30)} s ${s3[i0].toFixed(0).padStart(4)} m  at (${x.toFixed(0)}, ${y.toFixed(0)})  z ${lo.toFixed(1)}–${hi.toFixed(1)} m  ${speed}`)
  }
  // Drops: descents of more than a metre between a local top and bottom.
  let drops = 0, peak = heightAt(S3[0]), falling = false
  for (let i = 1; i <= G; i++) {
    const z = heightAt(S3[i % G])
    if (falling) {
      peak = Math.min(peak, z)
      if (z > peak + 1) { falling = false; peak = z }
    } else {
      peak = Math.max(peak, z)
      if (z < peak - 1) { falling = true; drops++; peak = z }
    }
  }
  console.log(`  ${drops} drops`)
  console.log(`  slowest after the first drop: ${(worst.v * 3.6).toFixed(0)} km/h at z ${worst.z.toFixed(1)} m`)
  if (!(worst.v > 2)) throw new Error('energy check failed: a hill the train cannot clear')
}
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Coney Island Cyclone', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, crest: 23, trackLength: Math.round(len3),
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/coney-island-cyclone.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(LON0 + ANCHOR[0] / MX).toFixed(7)}, ${(LAT0 + ANCHOR[1] / MY).toFixed(7)}`)

// Clearance report for development: stretches of track that pass within a
// deck's width of each other in plan, with too little height between them.
if (process.env.CLEARANCE) {
  const seen = new Set<string>()
  for (let i = 0; i < G; i += 2) for (let k = i + 2; k < G; k += 2) {
    const a = probe[i], b = probe[k]
    const ds = Math.abs(a.s - b.s)
    if (Math.min(ds, TOTAL - ds) < 12) continue
    const d = Math.hypot(a.x - b.x, a.y - b.y), dz = Math.abs(a.z - b.z)
    if (d < W + 0.3 && dz < 4.2) {
      const key = `${Math.round(a.s / 10)}-${Math.round(b.s / 10)}`
      if (seen.has(key)) continue
      seen.add(key)
      console.log(`clash s=${a.s.toFixed(0)} & ${b.s.toFixed(0)} at (${(a.x + ANCHOR[0]).toFixed(1)}, ${(a.y + ANCHOR[1]).toFixed(1)}) plan ${d.toFixed(1)} dz ${dz.toFixed(1)}`)
    }
  }
}
