/**
 * Thunder Striker (formerly Intimidator), Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-thunder-striker.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the track's footprint (see ANCHOR), at the
 * lowest ground under it.
 *
 * Plan: the circuit is the chain of OSM `roller_coaster=track` ways from the
 * station in Thunder Road (way/669991776, way/890902047 … way/890901418, all
 * oneway, in the direction of travel). The spur into the train barn
 * (way/890901693) is left out.
 *
 * Profile (OSM has no heights): from the ride's published layout, a B&M
 * hyper coaster of 1,620 m. A 232 ft (71 m) chain lift south-east from the
 * station, a 74° drop, the 178 ft (54 m) first camelback, a sharp right turn
 * back to the ground and a left turn onto the long south-west straight, the
 * 151 ft (46 m) second camelback, the 121 ft (37 m) hammerhead turn at the far
 * end, the 105 and 90 ft (32 and 27 m) camelbacks on the way back alongside,
 * a left turn into the mid-course brakes, the 62 ft (19 m) hill, the diving
 * spiral in the north-east, and the last two small hills (16 and 15 m) into
 * the final brakes and the turn into the station. Heights are given above the
 * local ground and lifted onto a coarse terrain grid (Mapzen/AWS Terrarium,
 * open data).
 *
 * Style: the track is one broad swept ribbon, banked from the train's speed
 * (energy from the crest) and the plan curvature. Red running surface and
 * rails over a red box spine a shade deeper, as the photos show, on pale
 * grey tubular supports: single columns where the track is low, A-frames of
 * two splayed legs where it is high. The station is its flat canopy roof.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

// ---------------------------------------------------------------- frame ----
// OSM coordinates are projected about this reference point (equirectangular,
// metres), then shifted so the origin is ANCHOR.
const LAT0 = 35.103, LON0 = -80.943
const MX = 111320 * Math.cos((LAT0 * Math.PI) / 180), MY = 110574
// Centre of the track's bounding box, in the projected frame.
const ANCHOR: [number, number] = [360, -195]

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

// ---------------------------------------------------------------- data -----
// The OSM circuit, node by node, in the projected frame, starting at the
// station exit (the first node of way/669991776).
const CHAIN: [number, number][] = [
  [321.3, -0.1], [327.2, -8.6], [371.9, -73.4], [397.5, -110.5], [401.1, -115.6], [407.0, -123.8], [415.5, -135.6], [437.8, -166.8],
  [456.1, -192.4], [461.0, -197.9], [465.5, -203.1], [468.1, -206.3], [470.4, -210.6], [471.7, -214.4], [470.6, -220.2], [467.2, -223.3],
  [464.2, -225.2], [461.4, -226.1], [458.7, -226.9], [455.1, -226.8], [451.0, -226.3], [446.6, -225.7], [442.9, -225.4], [437.8, -225.5],
  [433.3, -226.1], [429.2, -227.2], [423.6, -229.2], [418.2, -232.1], [413.4, -235.3], [409.4, -238.6], [405.3, -243.0], [400.8, -248.7],
  [387.4, -266.5], [370.4, -288.2], [352.0, -311.8], [336.2, -333.2], [314.7, -361.2], [295.9, -385.9], [288.7, -396.1], [285.5, -399.9],
  [281.8, -403.1], [278.9, -405.2], [274.2, -407.4], [266.4, -410.5], [261.4, -412.4], [257.3, -414.4], [254.4, -416.7], [251.7, -419.6],
  [250.3, -422.4], [249.6, -425.3], [249.3, -428.2], [249.9, -431.2], [251.0, -433.6], [253.0, -436.3], [255.2, -437.9], [257.9, -439.0],
  [261.6, -439.1], [264.3, -438.6], [267.0, -437.2], [269.3, -435.3], [271.5, -433.0], [274.0, -429.3], [275.1, -425.8], [275.6, -422.4],
  [276.0, -417.3], [277.0, -410.7], [278.3, -406.5], [278.9, -405.2], [280.6, -401.6], [282.7, -397.8], [284.7, -394.5], [293.9, -382.6],
  [306.9, -366.0], [322.9, -344.3], [349.1, -310.5], [378.4, -272.8], [402.4, -240.6], [428.5, -207.6], [434.8, -199.6], [439.1, -193.4],
  [441.6, -188.6], [443.1, -184.1], [444.4, -177.6], [444.4, -173.5], [442.5, -169.7], [440.2, -164.5], [436.7, -154.4], [434.4, -145.1],
  [429.5, -127.0], [425.2, -111.5], [421.7, -95.6], [417.7, -77.8], [413.2, -64.0], [407.5, -46.8], [403.8, -36.0], [403.7, -32.0],
  [404.8, -28.0], [406.2, -25.3], [408.3, -23.0], [410.0, -21.4], [412.6, -19.8], [415.8, -18.5], [419.1, -17.8], [423.5, -17.7],
  [427.6, -18.4], [431.0, -19.5], [434.6, -21.4], [437.9, -24.3], [440.7, -27.9], [443.0, -32.4], [444.1, -36.7], [444.3, -41.3],
  [443.6, -45.4], [442.0, -49.2], [439.3, -53.3], [436.6, -55.9], [433.0, -57.5], [429.6, -58.3], [426.4, -58.5], [423.0, -57.5],
  [419.2, -54.9], [408.1, -43.3], [388.1, -24.8], [356.0, 3.2], [318.4, 37.4], [309.0, 45.6], [306.4, 48.0], [302.7, 49.3],
  [299.7, 49.3], [296.7, 48.0], [295.1, 45.9], [294.1, 42.6], [294.4, 39.2], [296.4, 35.9], [303.9, 25.1], [307.5, 20.2],
]
const GROUND_X0 = 220, GROUND_Y0 = -460, GROUND_STEP = 20
// Rows run south to north, columns west to east; metres above the lowest cell.
const GROUND = [
  [2.1, 1.9, 1.6, 1.1, 0.7, 0.5, 0.2, 0.0, 0.0, 0.2, 0.2, 0.4, 0.7, 0.9, 1.1],
  [2.4, 2.1, 1.8, 1.4, 1.1, 0.8, 0.5, 0.3, 0.3, 0.4, 0.6, 0.8, 1.1, 1.3, 1.5],
  [3.9, 3.6, 2.5, 1.8, 1.5, 1.1, 0.8, 0.7, 0.6, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8],
  [4.4, 4.3, 3.8, 2.1, 1.9, 1.4, 1.1, 1.0, 1.0, 1.2, 1.3, 1.5, 1.7, 2.0, 2.2],
  [5.6, 5.0, 4.6, 3.9, 2.2, 1.8, 1.5, 1.3, 1.4, 1.6, 1.7, 1.9, 2.1, 2.4, 2.6],
  [8.2, 7.1, 6.2, 5.0, 3.2, 2.1, 1.8, 1.7, 1.8, 2.0, 2.2, 2.3, 2.6, 2.7, 2.8],
  [10.1, 8.7, 7.8, 6.1, 5.0, 2.6, 2.3, 2.1, 2.2, 2.4, 2.6, 2.7, 2.9, 2.9, 2.7],
  [10.0, 9.5, 9.0, 7.3, 6.2, 4.6, 2.6, 2.5, 2.5, 2.8, 3.0, 3.1, 3.1, 2.8, 2.5],
  [10.7, 9.6, 9.2, 8.4, 6.3, 5.3, 4.2, 2.8, 2.9, 3.1, 3.3, 3.3, 3.1, 2.7, 2.3],
  [11.3, 10.2, 9.6, 8.6, 6.5, 5.9, 5.4, 3.7, 3.2, 3.3, 3.4, 3.1, 2.8, 2.4, 2.1],
  [12.3, 10.8, 9.9, 9.4, 7.4, 6.1, 5.8, 5.0, 3.5, 3.5, 3.2, 3.0, 2.7, 2.2, 1.8],
  [12.0, 11.3, 10.3, 9.4, 7.8, 6.4, 5.8, 5.9, 5.2, 3.4, 3.3, 3.0, 2.8, 2.2, 1.8],
  [10.7, 10.7, 10.7, 8.9, 8.7, 6.8, 6.4, 5.8, 5.6, 4.7, 3.3, 3.2, 2.7, 2.4, 2.1],
  [9.7, 9.3, 9.8, 8.9, 8.1, 6.8, 6.5, 6.3, 5.7, 5.6, 4.0, 3.3, 3.0, 2.5, 2.5],
  [9.4, 9.2, 9.0, 8.5, 7.3, 7.0, 6.6, 6.4, 5.5, 5.4, 5.4, 3.4, 3.2, 2.9, 2.9],
  [8.7, 8.4, 8.7, 7.7, 6.7, 6.9, 6.4, 6.2, 5.8, 5.4, 6.7, 5.0, 3.4, 3.3, 3.3],
  [7.3, 7.5, 7.2, 7.0, 6.5, 6.3, 6.0, 5.2, 5.7, 5.8, 5.9, 5.1, 4.6, 3.7, 3.6],
  [7.3, 7.4, 7.2, 6.7, 6.4, 6.3, 6.1, 6.0, 5.5, 5.5, 5.4, 6.1, 5.4, 4.0, 3.9],
  [6.8, 6.9, 6.8, 6.2, 5.9, 5.8, 6.2, 6.4, 6.0, 5.7, 5.6, 5.2, 5.1, 4.1, 4.0],
  [6.4, 6.3, 5.9, 5.7, 5.7, 5.9, 6.7, 5.9, 6.1, 5.7, 5.7, 5.6, 4.9, 4.2, 3.9],
  [5.6, 5.8, 5.6, 5.8, 6.0, 5.8, 7.4, 6.5, 5.7, 5.6, 5.7, 7.2, 5.2, 4.1, 3.9],
  [3.1, 5.4, 5.4, 5.9, 6.3, 6.3, 6.5, 5.8, 5.5, 5.7, 5.8, 5.9, 5.1, 4.1, 3.8],
  [3.1, 4.4, 5.5, 6.1, 5.7, 5.5, 6.0, 6.4, 5.9, 5.6, 6.1, 5.2, 5.1, 3.8, 3.5],
  [3.9, 5.2, 5.7, 5.5, 5.3, 5.6, 6.2, 6.1, 5.9, 5.1, 4.7, 4.9, 4.4, 3.4, 3.1],
  [5.3, 5.4, 5.7, 5.6, 5.6, 5.7, 5.8, 5.8, 5.5, 4.3, 5.2, 4.5, 4.3, 3.1, 2.6],
  [5.4, 5.6, 5.6, 5.6, 5.4, 5.5, 5.1, 5.1, 4.6, 4.7, 4.6, 4.5, 3.4, 2.8, 2.3],
  [5.4, 5.5, 5.8, 5.4, 4.4, 4.2, 4.7, 4.9, 4.8, 4.6, 4.3, 3.2, 2.8, 2.5, 2.2],
  [5.9, 5.8, 6.0, 5.7, 4.4, 3.5, 5.0, 5.0, 4.6, 4.1, 3.4, 3.3, 2.7, 2.2, 2.4],
]


/** Ground height (m above the lowest cell) at a projected point, bilinear. */
function groundRaw(x: number, y: number) {
  const fx = Math.max(0, Math.min((x - GROUND_X0) / GROUND_STEP, GROUND[0].length - 1.001))
  const fy = Math.max(0, Math.min((y - GROUND_Y0) / GROUND_STEP, GROUND.length - 1.001))
  const i = Math.floor(fx), j = Math.floor(fy), u = fx - i, v = fy - j
  const R = GROUND
  return (R[j][i] * (1 - u) + R[j][i + 1] * u) * (1 - v) + (R[j + 1][i] * (1 - u) + R[j + 1][i + 1] * u) * v
}

// Heights above the local ground, keyed by distance along the OSM polyline
// (metres from the first node), from the published element heights.
const PROFILE: [number, number][] = [
  [0, 3], [12, 3.5], [30, 11],                         // station exit and the foot of the lift
  [104, 68], [116, 71], [126, 70],                     // lift and crest
  [134, 64], [146, 36], [158, 10], [170, 3],           // the 74° drop
  [196, 40], [216, 54], [236, 44],                     // first camelback
  [262, 18], [286, 4], [320, 5], [348, 7],             // right turn to the ground, left turn
  [392, 30], [422, 46], [452, 30], [492, 6],           // second camelback
  [530, 20], [566, 34], [596, 37], [622, 32], [642, 17], [662, 9], // hammerhead turn
  [690, 8], [722, 32], [756, 7], [786, 27], [818, 12], [850, 15],  // third and fourth camelbacks
  [880, 12], [915, 12], [1000, 11], [1030, 9],         // left turn into the mid-course brakes
  [1056, 19], [1080, 15],                              // fifth hill
  [1104, 17], [1135, 12], [1165, 4], [1180, 3], [1200, 4],        // diving spiral
  [1225, 15], [1248, 4], [1275, 14], [1300, 4],        // the last two hills
  [1320, 3.5], [1360, 3],                              // final brakes and the station
]

// ------------------------------------------------------------ the spline ---
const W = 3.2         // running surface width
const RAIL = 0.7      // depth of the rail band, the running surface's colour
const DEPTH = 2.1     // overall depth, to the bottom of the box spine
const SPINE = 1.5     // width of the spine's underside

const pts = CHAIN.map(([x, y]) => [x - ANCHOR[0], y - ANCHOR[1]] as [number, number])
const n = pts.length
// Polyline distance at each OSM node, which PROFILE is keyed by.
const polyS = [0]
for (let i = 1; i < n; i++) polyS.push(polyS[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]))
const polyTotal = polyS[n - 1] + Math.hypot(pts[0][0] - pts[n - 1][0], pts[0][1] - pts[n - 1][1])

// Dense centripetal Catmull–Rom through the plan points (closed).
const dense: { x: number; y: number; knot: number }[] = []
for (let i = 0; i < n; i++) {
  const p0 = pts[(i - 1 + n) % n], p1 = pts[i], p2 = pts[(i + 1) % n], p3 = pts[(i + 2) % n]
  const d = (a: number[], b: number[]) => Math.max(Math.hypot(a[0] - b[0], a[1] - b[1]) ** 0.5, 1e-3)
  const t0 = 0, t1 = t0 + d(p0, p1), t2 = t1 + d(p1, p2), t3 = t2 + d(p2, p3)
  const SUB = Math.max(4, Math.ceil(Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) / 0.5))
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
const dS = [0]
for (let i = 1; i <= dense.length; i++) {
  const a = dense[i - 1], b = dense[i % dense.length]
  dS.push(dS[i - 1] + Math.hypot(b.x - a.x, b.y - a.y))
}
const TOTAL = dS[dense.length]
const knotS = pts.map((_, i) => dS[dense.findIndex((p) => p.knot === i)])
/** Spline distance for a polyline distance, through the shared nodes. */
function splineS(ps: number) {
  const ks = [...knotS, TOTAL], ps0 = [...polyS, polyTotal]
  let j = 0
  while (j < ks.length - 2 && ps0[j + 1] <= ps) j++
  return ks[j] + ((ps - ps0[j]) / (ps0[j + 1] - ps0[j])) * (ks[j + 1] - ks[j])
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

// The model's datum: the lowest ground under the track and the station.
const ground0 = (x: number, y: number) => groundRaw(x + ANCHOR[0], y + ANCHOR[1])
let DATUM = Infinity
for (const p of dense) DATUM = Math.min(DATUM, ground0(p.x, p.y))
const ground = (x: number, y: number) => ground0(x, y) - DATUM

// Height knots, lifted onto the ground, then a closed monotone cubic.
const zk = PROFILE.map(([ps, z]) => {
  const s = splineS(ps), [x, y] = planAt(s)
  return [s, z + ground(x, y)] as [number, number]
})
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

// ------------------------------------------------- frames along the track --
// Fine samples first (1 m), each with its own banked frame.
const STEP = 1
const N = Math.round(TOTAL / STEP)
const CREST_S = splineS(120)
const LIFT_END = splineS(124)
type Sample = { p: V3; t: V3; left: V3; up: V3; s: number }
const heading: number[] = [], zs: number[] = []
for (let i = 0; i < N; i++) {
  const s = (i * TOTAL) / N
  const [x0, y0] = planAt(s - 1), [x1, y1] = planAt(s + 1)
  heading.push(Math.atan2(y1 - y0, x1 - x0))
  zs.push(heightAt(s))
}
const zCrest = heightAt(CREST_S)
// Bank angle: atan(v²κ / g), v² from the drop below the crest with some loss.
// The chain lift, the station and the brakes run level.
const rawBank = heading.map((_, i) => {
  const s = (i * TOTAL) / N
  let d = heading[(i + 8) % N] - heading[(i - 8 + N) % N]
  while (d > Math.PI) d -= 2 * Math.PI
  while (d < -Math.PI) d += 2 * Math.PI
  const k = d / (16 * STEP)
  if (s < LIFT_END || s > splineS(1300)) return 0
  const v2 = 2 * 9.81 * Math.max(0, zCrest - zs[i]) * 0.8
  return Math.atan((v2 * k) / 9.81)
})
const bank = rawBank.map((_, i) => {
  let sum = 0, w = 0
  for (let k = -12; k <= 12; k++) {
    const wk = 13 - Math.abs(k)
    sum += rawBank[(i + k + N) % N] * wk
    w += wk
  }
  return Math.max(-1.6, Math.min(1.6, sum / w))
})
const fine: Sample[] = []
for (let i = 0; i < N; i++) {
  const s = (i * TOTAL) / N
  const [x, y] = planAt(s)
  const [xa, ya] = planAt(s + 0.8), [xb, yb] = planAt(s - 0.8)
  const t = unit([xa - xb, ya - yb, heightAt(s + 0.8) - heightAt(s - 0.8)])
  const up0 = unit(sub([0, 0, 1], mul(t, t[2])))
  const left0 = cross(up0, t)
  const b = bank[i], c = Math.cos(b), sn = Math.sin(b)
  const left = unit(sub(mul(left0, c), mul(up0, sn)))
  const up = unit(add(mul(up0, c), mul(left0, sn)))
  fine.push({ p: [x, y, zs[i]], t, left, up, s })
}
// Keep a sample wherever the track has turned, pitched or rolled enough
// since the last one kept, and at least every MAX_STEP metres.
const MAX_STEP = 12, TURN = Math.cos((9 * Math.PI) / 180), ROLL = Math.cos((12 * Math.PI) / 180)
const samples: Sample[] = [fine[0]]
for (let i = 1; i < N; i++) {
  const last = samples[samples.length - 1], q = fine[i]
  const dist = Math.hypot(...sub(q.p, last.p))
  if (dist >= MAX_STEP || dot(q.t, last.t) < TURN || dot(q.up, last.up) < ROLL) samples.push(q)
}
const M = samples.length

// ------------------------------------------------------------- the track ---
const deck = new Part(), spine = new Part(), white = new Part(), roofPart = new Part(), trimPart = new Part()

function section(q: Sample) {
  const { p, left, up } = q
  const at = (l: number, u: number) => add(p, add(mul(left, l), mul(up, u)))
  return {
    L: at(W / 2, 0), R: at(-W / 2, 0),
    Lr: at(W / 2 - 0.15, -RAIL), Rr: at(-W / 2 + 0.15, -RAIL),
    Lb: at(SPINE / 2, -DEPTH), Rb: at(-SPINE / 2, -DEPTH),
  }
}
for (let i = 0; i < M; i++) {
  const a = samples[i], b = samples[(i + 1) % M]
  const A = section(a), B = section(b)
  const sideN = (q: Sample, sgn: number) => unit(add(mul(q.left, sgn), mul(q.up, -0.25)))
  face(deck, [A.L, A.R, B.R, B.L], [a.up, a.up, b.up, b.up])
  face(deck, [A.L, B.L, B.Lr, A.Lr], [sideN(a, 1), sideN(b, 1), sideN(b, 1), sideN(a, 1)])
  face(deck, [A.R, A.Rr, B.Rr, B.R], [sideN(a, -1), sideN(a, -1), sideN(b, -1), sideN(b, -1)])
  const lN = (q: Sample, sgn: number) => unit(add(mul(q.left, sgn), mul(q.up, -0.5)))
  face(spine, [A.Lr, B.Lr, B.Lb, A.Lb], [lN(a, 1), lN(b, 1), lN(b, 1), lN(a, 1)])
  face(spine, [A.Rr, A.Rb, B.Rb, B.Rr], [lN(a, -1), lN(a, -1), lN(b, -1), lN(b, -1)])
  const da = mul(a.up, -1), db = mul(b.up, -1)
  face(spine, [A.Lb, B.Lb, B.Rb, A.Rb], [da, db, db, da])
}

// ------------------------------------------------------------- supports ----
/** A six-sided tube from a to b, radius r, smooth-shaded, open ends. */
function tube(p: Part, a: V3, b: V3, r: number) {
  const ax = unit(sub(b, a))
  const ref: V3 = Math.abs(ax[2]) < 0.9 ? [0, 0, 1] : [1, 0, 0]
  const u = unit(cross(ax, ref)), v = cross(ax, u)
  const K = 6
  const ring = (c: V3, k: number) => {
    const ang = (k / K) * 2 * Math.PI
    const nrm = add(mul(u, Math.cos(ang)), mul(v, Math.sin(ang)))
    return { P: add(c, mul(nrm, r)), N: nrm }
  }
  for (let k = 0; k < K; k++) {
    const a0 = ring(a, k), a1 = ring(a, k + 1), b0 = ring(b, k), b1 = ring(b, k + 1)
    face(p, [a0.P, a1.P, b1.P, b0.P], [a0.N, a1.N, b1.N, b0.N])
  }
}

/** Closest distance from point q to any track sample more than `skip` m along from s0. */
function clearOf(q: V3, s0: number, skip = 14) {
  let best = Infinity
  for (const f of samples) {
    const ds = Math.abs(f.s - s0)
    if (Math.min(ds, TOTAL - ds) < skip) continue
    best = Math.min(best, Math.hypot(...sub(f.p, q)))
  }
  return best
}
/** True if the straight leg a→b passes clear of every other stretch of track. */
function legClear(a: V3, b: V3, s0: number) {
  const L = Math.hypot(...sub(b, a))
  for (let d = 0; d <= L; d += 1.5) {
    const q = add(a, mul(sub(b, a), d / L))
    if (clearOf(q, s0) < W * 0.75 + 0.9) return false
  }
  return true
}

const SUPPORT_EVERY = 17
const supports: { s: number; legs: number }[] = []
for (let s = 4; s < TOTAL - 4; s += SUPPORT_EVERY) {
  // Try the planned spot, then a few metres either side.
  for (const off of [0, 4, -4, 8, -8]) {
    const i = Math.round((s + off) / STEP) % N
    const q = fine[i]
    const zr = q.p[2] - ground(q.p[0], q.p[1])
    if (zr < 2.2) break
    const top = add(q.p, mul(q.up, -DEPTH + 0.3))
    // Low track and the lift: one column straight down. High track: an
    // A-frame splayed across the track.
    const across = unit([q.left[0], q.left[1], 0])
    const legs: [V3, V3][] = []
    if (zr < 16) {
      const foot: V3 = [top[0], top[1], ground(top[0], top[1]) - 1]
      legs.push([foot, top])
    } else {
      const spread = Math.min(0.3 * zr, 22)
      for (const sg of [1, -1]) {
        const fx = top[0] + across[0] * spread * sg, fy = top[1] + across[1] * spread * sg
        legs.push([[fx, fy, ground(fx, fy) - 1], top])
      }
    }
    if (!legs.every(([a, b]) => legClear(a, b, q.s))) continue
    const r = zr < 16 ? 0.9 : zr < 40 ? 1.05 : 1.2
    for (const [a, b] of legs) tube(white, a, b, r)
    supports.push({ s: q.s, legs: legs.length })
    break
  }
}

// -------------------------------------------------------------- station ----
// The canopy over the platform (way/239268383), a flat roof on posts.
const STATION: [number, number][] = [
  [302.8, 17.0], [314.6, 0.0], [313.1, -1.0], [312.6, -1.3], [312.3, -1.6], [312.7, -2.2], [314.5, -4.8],
  [315.3, -4.3], [321.3, -0.1], [325.9, 3.0], [327.3, 4.0], [319.2, 15.5], [317.4, 18.2], [313.2, 24.1], [307.5, 20.2],
]
const ROOF_Z = 7.5, ROOF_T = 0.9
{
  const poly = STATION.map(([x, y]) => [x - ANCHOR[0], y - ANCHOR[1]] as [number, number])
  const area = poly.reduce((s, p, i) => s + p[0] * poly[(i + 1) % poly.length][1] - poly[(i + 1) % poly.length][0] * p[1], 0)
  const ring = area < 0 ? [...poly].reverse() : poly
  const g = Math.max(...ring.map(([x, y]) => ground(x, y)))
  const z0 = g + ROOF_Z - ROOF_T, z1 = g + ROOF_Z
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    const o = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    face(trimPart, [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]], o)
  }
  // Ear-clip the roof and its soffit.
  const idx = ring.map((_, i) => i)
  const cz = (o: number[], a: number[], b: number[]) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const A = ring[ia], B = ring[ib], C = ring[ic]
      if (cz(A, B, C) <= 0) continue
      if (!idx.every((j) => j === ia || j === ib || j === ic ||
        !(cz(A, B, ring[j]) >= 0 && cz(B, C, ring[j]) >= 0 && cz(C, A, ring[j]) >= 0))) continue
      const T = [A, B, C]
      roofPart.tri(...(T.map(([x, y]) => [x, y, z1]) as [V3, V3, V3]), undefined, undefined, undefined, [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
      trimPart.tri(...(T.reverse().map(([x, y]) => [x, y, z0]) as [V3, V3, V3]), undefined, undefined, undefined, [[0, 0, -1], [0, 0, -1], [0, 0, -1]])
      idx.splice(i, 1)
      break
    }
  }
  const [a, b, c] = idx.map((i) => ring[i])
  roofPart.tri([a[0], a[1], z1], [b[0], b[1], z1], [c[0], c[1], z1], undefined, undefined, undefined, [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
  for (const [x, y] of [[306, 17], [314.5, 1.5], [323, 3.5], [311, 21]] as [number, number][]) {
    const px = x - ANCHOR[0], py = y - ANCHOR[1]
    tube(trimPart, [px, py, ground(px, py) - 1], [px, py, z0], 0.35)
  }
}

// ---------------------------------------------------------------- output ---
// Colours from the photos, pulled to the palette's lightness: red track,
// its spine a shade deeper, pale grey supports.
const parts = [
  { part: deck, material: finish('striker-red', 0xc9473f) },
  { part: spine, material: finish('striker-red-2', 0xb03a35) },
  { part: white, material: finish('striker-grey', 0xc9cfd4) },
  { part: trimPart, material: PALETTE.stone },
  { part: roofPart, material: PALETTE.roof },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
let maxZ = 0, steep = 0, len3 = 0
for (let i = 0; i < N; i++) {
  const a = fine[i].p, b = fine[(i + 1) % N].p
  maxZ = Math.max(maxZ, a[2])
  len3 += Math.hypot(...sub(b, a))
  steep = Math.max(steep, (Math.atan2(a[2] - b[2], Math.hypot(b[0] - a[0], b[1] - a[1])) * 180) / Math.PI)
}
console.log(`track ${len3.toFixed(0)} m (${TOTAL.toFixed(0)} m in plan), ${M} sections, crest ${maxZ.toFixed(1)} m, steepest ${steep.toFixed(0)}°, max bank ${(Math.max(...bank.map(Math.abs)) * 180 / Math.PI).toFixed(0)}°`)
console.log(`${supports.length} supports, ${supports.reduce((s, x) => s + x.legs, 0)} legs; datum ${DATUM.toFixed(1)} m on the grid`)
console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Thunder Striker', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 71, trackLength: 1620,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/carowinds-thunder-striker.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(LON0 + ANCHOR[0] / MX).toFixed(7)}, ${(LAT0 + ANCHOR[1] / MY).toFixed(7)}`)

// Clearance report: stretches of track within a deck's width of each other
// in plan with less than 4 m between them in height.
if (process.env.CLEARANCE) {
  for (let i = 0; i < N; i += 2) for (let k = i + 20; k < N; k += 2) {
    const ds = Math.abs(fine[i].s - fine[k].s)
    if (Math.min(ds, TOTAL - ds) < 20) continue
    const a = fine[i].p, b = fine[k].p
    const d = Math.hypot(a[0] - b[0], a[1] - b[1]), dz = Math.abs(a[2] - b[2])
    if (d < W + 1 && dz < 4.5)
      console.log(`clash s=${fine[i].s.toFixed(0)} & ${fine[k].s.toFixed(0)} at (${(a[0] + ANCHOR[0]).toFixed(0)}, ${(a[1] + ANCHOR[1]).toFixed(0)}) plan ${d.toFixed(1)} dz ${dz.toFixed(1)}`)
  }
}
if (process.env.PROFILE_DUMP) {
  for (let i = 0; i < N; i += 20) console.log(`s ${fine[i].s.toFixed(0)} (${(fine[i].p[0] + ANCHOR[0]).toFixed(0)}, ${(fine[i].p[1] + ANCHOR[1]).toFixed(0)}) z ${fine[i].p[2].toFixed(1)} g ${ground(fine[i].p[0], fine[i].p[1]).toFixed(1)} bank ${(bank[i] * 180 / Math.PI).toFixed(0)}`)
}
