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
 * machinery head with the flag on top. Painted since 2007 in the flag's
 * colours: a white shaft with red stripes that step sideways a third of the
 * way up, and a blue head with white stars (too fine to model).
 *
 * The cabin moves; it is shown at the top of its travel, just under the head,
 * where photos of the tower almost always catch it and where it gives the
 * tower its silhouette. The low boarding platform ringed by the fence
 * (way/239268372) stays at the foot. Neither is an OSM building, so the model
 * replaces nothing.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

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

const K = 16 // segments around

/**
 * A surface of revolution from a profile of [radius, z] points, each profile
 * edge one band, smooth around and flat along the profile. `pick(band, seg)`
 * chooses the part for each quad.
 */
function lathe(profile: [number, number][], pick: (band: number, seg: number) => Part) {
  for (let b = 0; b < profile.length - 1; b++) {
    const [r0, z0] = profile[b], [r1, z1] = profile[b + 1]
    // Outward normal of the profile edge in the (r, z) plane.
    const er = r1 - r0, ez = z1 - z0, l = Math.hypot(er, ez) || 1
    const nr = ez / l, nz = -er / l
    for (let k = 0; k < K; k++) {
      const a0 = (k / K) * 2 * Math.PI, a1 = ((k + 1) / K) * 2 * Math.PI
      const P = (r: number, z: number, a: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]
      const N = (a: number): V3 => unit([nr * Math.cos(a), nr * Math.sin(a), nz])
      if (r0 === 0 && r1 === 0) continue
      const quad = [P(r0, z0, a0), P(r0, z0, a1), P(r1, z1, a1), P(r1, z1, a0)]
      const norms = [N(a0), N(a1), N(a1), N(a0)]
      const part = pick(b, k)
      if (r0 === 0) part.tri(quad[0], quad[2], quad[3], undefined, undefined, undefined, [norms[0], norms[2], norms[3]])
      else if (r1 === 0) part.tri(quad[0], quad[1], quad[2], undefined, undefined, undefined, [norms[0], norms[1], norms[2]])
      else face(part, quad, norms)
    }
  }
}

const unused = new Part(), white = new Part(), red = new Part(), blue = new Part(), win = new Part(), roof = new Part(), stone = new Part()

// ---------------------------------------------------------------- shaft ----
const R = 1.6           // shaft radius (OSM: 3.2 m across)
const SHAFT_TOP = 72.5
const STEP_Z = 26       // where the stripes jog sideways
// Red stripes: four, a quarter turn apart, so any view shows two, as photos
// do; each shifts one segment sideways at the jog.
const stripe = (seg: number, upper: boolean) => {
  const s = (seg + (upper ? 1 : 0)) % (K / 4)
  return s === 1
}
lathe([[R, -1], [R, STEP_Z]], (_, k) => (stripe(k, false) ? red : white))
lathe([[R, STEP_Z], [R, SHAFT_TOP]], (_, k) => (stripe(k, true) ? red : white))
// The jog itself: a short red step joining the two stripe positions.
lathe([[R + 0.02, STEP_Z - 0.6], [R + 0.02, STEP_Z + 0.6]], (_, k) => (stripe(k, false) || stripe(k, true) ? red : unused))

// ---------------------------------------------------------------- cabin ----
// A ring 9.7 m across (the fence ring's size), white rim above and below a
// continuous window band, tucked in under the head.
const CAB_R = 4.85, CAB_Z0 = 67.0, CAB_Z1 = 72.3
lathe([
  [R + 0.1, CAB_Z0 - 0.2], [CAB_R - 1.2, CAB_Z0],         // underside, sloping out
  [CAB_R - 0.35, CAB_Z0 + 0.25], [CAB_R, CAB_Z0 + 0.7],    // bevelled lower rim
  [CAB_R, CAB_Z0 + 1.2],
], () => white)
lathe([[CAB_R - 0.25, CAB_Z0 + 1.2], [CAB_R - 0.45, CAB_Z1 - 1.0]], () => win)
// The rims' faces either side of the recessed window band.
lathe([[CAB_R, CAB_Z0 + 1.2], [CAB_R - 0.25, CAB_Z0 + 1.2]], () => white)
lathe([[CAB_R - 0.45, CAB_Z1 - 1.0], [CAB_R, CAB_Z1 - 1.0]], () => white)
lathe([
  [CAB_R, CAB_Z1 - 1.0], [CAB_R, CAB_Z1 - 0.45], [CAB_R - 0.35, CAB_Z1], // bevelled upper rim
  [R + 1.6, CAB_Z1 + 0.5], [R + 0.1, CAB_Z1 + 0.6],                      // shallow dished roof
], (b) => (b >= 2 ? roof : white))

// ----------------------------------------------------------------- head ----
// The blue machinery head, a little wider than the shaft, with a white band
// at its foot and a white cap.
const HEAD_R = 2.35
lathe([
  [R, SHAFT_TOP], [HEAD_R - 0.3, SHAFT_TOP + 0.1], [HEAD_R, SHAFT_TOP + 0.4], [HEAD_R, SHAFT_TOP + 1.3],
], () => white)
lathe([[HEAD_R, SHAFT_TOP + 1.3], [HEAD_R, 78.9]], () => blue)
lathe([
  [HEAD_R, 78.9], [HEAD_R, 79.4], [HEAD_R - 0.3, 79.86], [0.4, 79.86], [0, 79.86],
], () => white)
// The flagpole's base, a stub that still reads at map scale.
lathe([[0.3, 79.86], [0.22, 86], [0, 86]], () => white)

// ------------------------------------------------------------- platform ----
// The round boarding platform inside the fence, a low drum.
lathe([
  [4.85, -1], [4.85, 0.6], [4.6, 0.9], [R, 0.9],
], (b) => (b >= 2 ? roof : stone))

// ---------------------------------------------------------------- output ---
// Colours from the photos: white shaft and cabin rims, flag red stripes and
// a flag-blue head, both pulled to the palette's lightness.
const parts = [
  { part: white, material: PALETTE.trim },
  { part: red, material: finish('flag-red', 0xc8524a) },
  { part: blue, material: finish('flag-blue', 0x5a6f9e) },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
  { part: stone, material: PALETTE.stone },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
const glb = writeGlb('Carolina Skytower', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 79.86,
})
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const out = new URL('../../landmarks/models/carowinds-skytower.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
