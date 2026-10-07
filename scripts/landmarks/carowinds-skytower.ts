/**
 * Carolina Skytower, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-skytower.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The origin is the
 * centre of the shaft (OSM way/890251379, a 3.2 m circle tagged height 79.86,
 * the published 262 ft), on the ground.
 *
 * An Intamin Gyro Tower (1973): a plain cylindrical shaft, a ring-shaped
 * observation cabin that turns as it rides up around it, and a cylindrical
 * machinery head with a railing, loudspeakers and a mast on top. Painted
 * since 2007 in the flag's colours, from a close photo of the head:
 *
 * - the shaft white, with red stripes that step sideways a third of the
 *   way up;
 * - the cabin a straight-walled cylinder: a navy base band, a band of dark
 *   windows and an upper band painted as a waving American flag:
 *   red and white stripes slanting round it, and a navy canton with white
 *   stars over one section;
 * - the head a navy drum with white stars in vertical columns.
 *
 * The flag and the stars are geometry, not texture (STYLE.md: no textures),
 * and they are what makes the cabin's turn visible on the map, so they are
 * drawn broader than the real paint: nineteen stripes rather than thirteen
 * thin ones, and a handful of large stars.
 *
 * The cabin moves (experimental; see "Animation" in STYLE.md). It is a glTF
 * node whose pivot is on the shaft's axis at the cabin floor; one clip
 * carries it between the boarding platform and the head while it turns. At
 * t = 0 it is at the top, just under the head, where photos of the tower
 * almost always catch it and where it gives the tower its silhouette.
 * Everything else is the static root.
 *
 * The boarding platform is a raised ring the cabin sinks into, so at the
 * bottom its floor is level with the deck and its saucer drops into the pit
 * inside the ring. The fence ring (way/239268372, 9.7 m across) gives the
 * cabin's size. Neither is an OSM building, so the model replaces nothing.
 */
import { Part, axisAngle, cross, sub, writeGlb, type ChannelSpec, type NodeSpec, type Quat, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const TAU = Math.PI * 2
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A quad whose winding is chosen so it faces `out`, with per-corner normals. */
function face(p: Part, P: V3[], N: V3[]) {
  const f = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = N.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(f, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [N[a], N[b], N[c]])
  p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [N[a], N[c], N[d]])
}

/** A point at radius r, angle a (counter-clockwise from east), height z. */
const P = (r: number, a: number, z: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]

/**
 * A surface of revolution from a profile of [radius, z] points, each profile
 * edge one band of `k` segments, smooth around and flat along the profile.
 * The profile runs so that its left-hand side is the outside: outward-facing
 * walls go up, roofs go inward, undersides go outward. `pick(band, seg)`
 * chooses the part for each quad.
 */
function lathe(profile: [number, number][], pick: (band: number, seg: number) => Part | null, k = 16) {
  for (let b = 0; b < profile.length - 1; b++) {
    const [r0, z0] = profile[b], [r1, z1] = profile[b + 1]
    if (r0 === 0 && r1 === 0) continue
    // Outward normal of the profile edge in the (r, z) plane.
    const er = r1 - r0, ez = z1 - z0, l = Math.hypot(er, ez) || 1
    const nr = ez / l, nz = -er / l
    const N = (a: number): V3 => unit([nr * Math.cos(a), nr * Math.sin(a), nz])
    for (let s = 0; s < k; s++) {
      const part = pick(b, s)
      if (!part) continue
      const a0 = (s / k) * TAU, a1 = ((s + 1) / k) * TAU
      const quad = [P(r0, a0, z0), P(r0, a1, z0), P(r1, a1, z1), P(r1, a0, z1)]
      const norms = [N(a0), N(a1), N(a1), N(a0)]
      if (r0 === 0) part.tri(quad[0], quad[2], quad[3], undefined, undefined, undefined, [norms[0], norms[2], norms[3]])
      else if (r1 === 0) part.tri(quad[0], quad[1], quad[2], undefined, undefined, undefined, [norms[0], norms[1], norms[2]])
      else face(part, quad, norms)
    }
  }
}

/**
 * A flat five-pointed star centred at `c` in the plane spanned by `right` and
 * `up` (unit, perpendicular), facing `n`: ten triangles fanned from the centre.
 */
function star(p: Part, c: V3, right: V3, up: V3, n: V3, size: number) {
  const pts: V3[] = []
  for (let i = 0; i < 10; i++) {
    const a = Math.PI / 2 + (i * Math.PI) / 5, r = i % 2 ? size * 0.42 : size
    pts.push(add(c, add(mul(right, r * Math.cos(a)), mul(up, r * Math.sin(a)))))
  }
  for (let i = 0; i < 10; i++) {
    const tri = [c, pts[i], pts[(i + 1) % 10]]
    const f = cross(sub(tri[1], tri[0]), sub(tri[2], tri[0]))
    if (dot(f, n) >= 0) p.tri(tri[0], tri[1], tri[2], undefined, undefined, undefined, [n, n, n])
    else p.tri(tri[0], tri[2], tri[1], undefined, undefined, undefined, [n, n, n])
  }
}

// Root (fixed) parts.
const white = new Part(), red = new Part(), navy = new Part(), grey = new Part(), stone = new Part()
// Cabin (moving) parts: the node's own mesh, authored about its pivot.
const cWhite = new Part(), cRed = new Part(), cNavy = new Part(), cWin = new Part()

// ---------------------------------------------------------------- shaft ----
const R = 1.6           // shaft radius (OSM: 3.2 m across)
const DRUM_Z0 = 75.6    // underside of the head; the shaft runs up into it
const STEP_Z = 26       // where the stripes jog sideways
// Red stripes: four, a quarter turn apart, so any view shows two, as photos
// do; each shifts one segment sideways at the jog.
const stripe = (seg: number, upper: boolean) => (seg + (upper ? 1 : 0)) % 4 === 1
lathe([[R, -1.5], [R, STEP_Z]], (_, k) => (stripe(k, false) ? red : white))
lathe([[R, STEP_Z], [R, DRUM_Z0]], (_, k) => (stripe(k, true) ? red : white))
// The jog itself: a short red step joining the two stripe positions.
lathe([[R + 0.02, STEP_Z - 0.6], [R + 0.02, STEP_Z + 0.6]], (_, k) => (stripe(k, false) || stripe(k, true) ? red : null))

// ----------------------------------------------------------------- head ----
// A navy drum 4.8 m across and 3.7 m tall with a white rim on top, then a
// railing ring (a low solid band, STYLE.md: no railings) carrying
// loudspeakers, a small white machine house, and the mast.
const HEAD_R = 2.4, DRUM_Z1 = 79.3, KH = 16
lathe([[R, DRUM_Z0], [HEAD_R - 0.2, DRUM_Z0], [HEAD_R, DRUM_Z0 + 0.2]], () => navy, KH)
lathe([[HEAD_R, DRUM_Z0 + 0.2], [HEAD_R, DRUM_Z1]], () => navy, KH)
lathe([
  [HEAD_R, DRUM_Z1], [HEAD_R + 0.05, DRUM_Z1 + 0.05], [HEAD_R + 0.05, DRUM_Z1 + 0.35], [HEAD_R - 0.2, DRUM_Z1 + 0.5],
  [1.0, DRUM_Z1 + 0.5],
], () => white, KH)
// The railing ring, standing just inside the rim.
const RAIL_R = 2.3, RAIL_Z = DRUM_Z1 + 0.5
lathe([[RAIL_R, RAIL_Z], [RAIL_R, RAIL_Z + 0.7], [RAIL_R - 0.1, RAIL_Z + 0.7], [RAIL_R - 0.1, RAIL_Z]], () => white, KH)
// Loudspeakers: squat horns on the ring, eight round, angled outward.
for (let i = 0; i < 8; i++) {
  const a = ((i + 0.5) / 8) * TAU, o: V3 = [Math.cos(a), Math.sin(a), 0], t: V3 = [-Math.sin(a), Math.cos(a), 0]
  const c = add(P(RAIL_R - 0.05, a, 0), [0, 0, RAIL_Z + 0.92])
  const box = (dx: number, dy: number, dz: number): V3 => add(c, add(add(mul(o, dx), mul(t, dy)), [0, 0, dz]))
  const s = 0.22
  const ring = (dz: number, w: number): V3[] => [box(-w, -w, dz), box(w, -w, dz), box(w, w, dz), box(-w, w, dz)]
  const lo = ring(-s, s * 0.8), hi = ring(s, s)
  // Sides, counter-clockwise from above.
  for (let k = 0; k < 4; k++) grey.quad(lo[k], lo[(k + 1) % 4], hi[(k + 1) % 4], hi[k])
  grey.cap(hi, true)
  grey.cap(lo, false)
}
// The machine house and the mast.
lathe([[1.0, RAIL_Z], [1.0, RAIL_Z + 1.3], [0.85, RAIL_Z + 1.45], [0.25, RAIL_Z + 1.45]], () => white, 12)
lathe([[0.22, RAIL_Z + 1.45], [0.14, 87], [0, 87]], () => white, 8)

// Stars on the drum: eight columns of four, one column on every other facet,
// each star flat on its facet so it sits flush.
for (let c = 0; c < 8; c++) {
  const a = ((2 * c + 0.5) / KH) * TAU
  const n: V3 = [Math.cos(a), Math.sin(a), 0], right: V3 = [-Math.sin(a), Math.cos(a), 0]
  const rf = HEAD_R * Math.cos(Math.PI / KH) + 0.03
  for (let j = 0; j < 4; j++) star(white, add(mul(n, rf), [0, 0, DRUM_Z0 + 0.75 + j * 0.8]), right, [0, 0, 1], n, 0.34)
}

// ------------------------------------------------------------- platform ----
// A raised ring deck the cabin sinks into at boarding.
const DECK = 1.5
lathe([[5.15, DECK], [5.15, 0]], () => stone, 24) // inner wall, facing the pit
lathe([[6.8, -1], [6.8, DECK - 0.25], [6.55, DECK], [5.15, DECK]], (b) => (b === 2 ? grey : stone), 24)

// ---------------------------------------------------------------- cabin ----
// Authored about the pivot: on the shaft's axis, at the top of the navy
// base band. A straight-walled cylinder 9.7 m across with a flat floor and
// a flat roof: a navy base band, the window band, then the flag band
// wrapped round the upper wall, proportioned from the photo against the
// 3.2 m shaft.
const KC = 24            // segments round the cabin, so the flag gets 24 sectors
const CAB_R = 4.85, IN_R = 1.78
const BASE_Z = -1.6      // underside of the cabin
const WIN_Z = 1.5        // top of the window band
const ROOF_Z1 = 3.25     // top of the flag band, the flat roof
// Navy base band and the flat underside.
lathe([[IN_R, BASE_Z], [CAB_R, BASE_Z], [CAB_R, 0]], () => cNavy, KC)
lathe([[CAB_R, 0], [CAB_R, WIN_Z]], () => cWin, KC)
// The flat roof, and the wall facing the shaft, so no slit shows through.
lathe([[CAB_R, ROOF_Z1], [IN_R, ROOF_Z1], [IN_R, BASE_Z]], () => cNavy, KC)

/**
 * The flag band: the upper wall from the windows to the roof, cut into KC
 * sectors that slant as they climb, so the stripes run round the cabin at
 * an angle like the waving flag in the photo. Five sectors (75°) are the
 * navy canton; the other nineteen alternate red and white, red at both ends.
 */
const CANTON = 5, SHEAR = (1.2 * TAU) / KC, BANDS = 3
const flagAt = (s: number, a: number): V3 => P(CAB_R, a + SHEAR * s, WIN_Z + (ROOF_Z1 - WIN_Z) * s)
{
  const N = (a: number): V3 => [Math.cos(a), Math.sin(a), 0]
  for (let k = 0; k < KC; k++) {
    const part = k < CANTON ? cNavy : (k - CANTON) % 2 === 0 ? cRed : cWhite
    for (let b = 0; b < BANDS; b++) {
      const s0 = b / BANDS, s1 = (b + 1) / BANDS
      const a0 = (k / KC) * TAU, a1 = ((k + 1) / KC) * TAU
      face(part, [flagAt(s0, a0), flagAt(s0, a1), flagAt(s1, a1), flagAt(s1, a0)],
        [N(a0 + SHEAR * s0), N(a1 + SHEAR * s0), N(a1 + SHEAR * s1), N(a0 + SHEAR * s1)])
    }
  }
  // Stars on the canton in two staggered rows, flat on the wall and lifted
  // just clear of its facets.
  const span = (CANTON / KC) * TAU
  for (const [s, fractions] of [[0.3, [0.17, 0.5, 0.83]], [0.72, [0.3, 0.7]]] as [number, number[]][])
    for (const f of fractions) {
      const a = f * span + SHEAR * s, c = flagAt(s, f * span), n = N(a)
      star(cWhite, add(c, mul(n, 0.06)), unit(cross([0, 0, 1], n)), [0, 0, 1], n, 0.42)
    }
}

// --------------------------------------------------------------- motion ----
/**
 * Up and down in a 120 s loop, turning two full turns (6° a second) the
 * whole time. The real ride takes 5 to 6 minutes a cycle and turns once or
 * twice on the way, too slow to see on a map; this is about three times as
 * fast, still a slow, believable ride. From the top: 6 s held, 48 s down,
 * 12 s held at the platform, 48 s up, 6 s held, so the loop starts and ends
 * in the middle of the pause at the top. Each run starts and stops on a 6 s
 * ramp, sampled every second since keyframes are LINEAR.
 */
const LOOP = 120, TURNS = 2, RAMP = 6
const Z_TOP = DRUM_Z0 - ROOF_Z1 - 0.15   // roof just clear of the drum
const Z_BOTTOM = DECK                     // floor level with the deck
const legs: [number, number, number][] = [ // [start, end, from top (0) to bottom (1)]
  [0, 6, 0], [6, 54, NaN], [54, 66, 1], [66, 114, NaN], [114, 120, 0],
]
/** Progress through a run of length T at time t, on a trapezoidal speed profile. */
function run(t: number, T: number) {
  const v = 1 / (T - RAMP)                // cruising speed, in runs a second
  if (t < RAMP) return (v * t * t) / (2 * RAMP)
  if (t > T - RAMP) return 1 - (v * (T - t) ** 2) / (2 * RAMP)
  return v * (t - RAMP / 2)
}
const depth = (t: number) => {
  const [t0, t1, held] = legs.find(([a, b]) => t >= a && t <= b)!
  if (!Number.isNaN(held)) return held
  const p = run(t - t0, t1 - t0)
  return t0 < 60 ? p : 1 - p
}
const times = Array.from({ length: LOOP + 1 }, (_, i) => i)
const lift: V3[] = times.map((t) => [0, 0, Z_TOP + (Z_BOTTOM - Z_TOP) * depth(t)])
// Counter-clockwise from above; one key a second is 6°, so LINEAR slerp is smooth.
const turn: Quat[] = times.map((t) => axisAngle([0, 0, 1], (t / LOOP) * TURNS * TAU))

const nodes: NodeSpec[] = [{
  name: 'cabin',
  translation: lift[0],
  parts: [
    { part: cNavy, material: finish('flag-blue', 0x4f6292) },
    { part: cWin, material: PALETTE.window },
    { part: cRed, material: finish('flag-red', 0xc8524a) },
    { part: cWhite, material: PALETTE.trim },
  ],
}]
const channels: ChannelSpec[] = [
  { node: 0, path: 'translation', values: lift },
  { node: 0, path: 'rotation', values: turn },
]

// ---------------------------------------------------------------- output ---
// Colours from the photo: white shaft, cabin trim and stars; flag red; a
// navy pulled up to the palette's lightness; railing and speakers in the
// roof grey. Six materials, shared between the root and the cabin.
const parts = [
  { part: white, material: PALETTE.trim },
  { part: red, material: finish('flag-red', 0xc8524a) },
  { part: navy, material: finish('flag-blue', 0x4f6292) },
  { part: grey, material: PALETTE.roof },
  { part: stone, material: PALETTE.stone },
]
const triangles = [...parts, ...nodes.flatMap((n) => n.parts)].reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Carolina Skytower', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 79.86,
}, { nodes, animation: { name: 'ride', times, channels } })
if (glb.length > 250_000) throw new Error(`Size budget exceeded: ${glb.length} bytes`)
const out = new URL('../../landmarks/models/carowinds-skytower.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes, ${LOOP} s a loop, ${TURNS} turns`)
