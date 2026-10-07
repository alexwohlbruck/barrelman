/**
 * Space Mountain, Magic Kingdom, Walt Disney World — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-space-mountain.ts
 *
 * Map frame: x east, y north, z up, metres. Origin at the centre of the
 * cone's rings in OSM (way/373171487, -81.5772341, 28.4191494), on the
 * ground. The building is round, so it is placed at bearing 0.
 *
 * A white concrete cone ribbed by 72 external beams. Each beam runs from a
 * pointed foot standing out past the pale-blue base wall, up the cone to a
 * collar ring two-thirds of the way up; above the collar a shorter ribbed
 * cone rises to a flat top carrying the central mast. Clusters of white
 * spires stand on the collar.
 *
 * Evidence:
 *  - OSM, measured: the outline (way/296326395) and its concentric
 *    building:parts — the base ring (way/1106733755, r 52.8 m, 13 m), the
 *    cone (way/373171488, r 47.9 m), the collar (way/373171487, r 18.6 m),
 *    the upper rings (way/373171486, r 12.8 m; way/373171485, r 8.9 m) — and
 *    the low entrance wing on the south-west (way/1106733754, 5 m), which is
 *    modelled as a plain block so the outline can be replaced.
 *  - Published: 183 ft (56 m) tall, 300 ft (91 m) across the cone, 72 ribs
 *    (Wikipedia).
 *  - Photos: from the Contemporary monorail (Raman Patel, CC BY 3.0) for the
 *    profile — collar, top ring and mast against the eave width; close
 *    ground view (Benjamin D. Esham, CC BY-SA 4.0) for the ribs, the feet
 *    and the spire clusters; Sam Howzit (Flickr, CC BY 2.0) exterior.
 *
 * Estimated: the eave (9.5 m) and collar (31 m) heights, from the monorail
 * photo's proportions against OSM's radii; the mast's form; the spire
 * clusters' count and placement (four clusters of three, evenly spaced —
 * the photos show clusters on every side but not their exact layout).
 */
import { Part, len, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

type XY = [number, number]
const TAU = Math.PI * 2

const shell = new Part()  // the cone's surface
const ribs = new Part()   // ribs, spires, collar, mast
const base = new Part()   // pale blue base wall and the entrance wing

// ---------------------------------------------------------------------------
// Kit

const unit = (a: V3): V3 => { const l = len(a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) { p.tri(a, b, c, undefined, undefined, undefined, n) }
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: [V3, V3, V3, V3]) {
  tri(p, a, b, c, n && [n[0], n[1], n[2]])
  tri(p, a, c, d, n && [n[0], n[2], n[3]])
}

/**
 * A smooth-shaded frustum about a vertical axis: radius r0 at z0 to r1 at z1.
 * r1 = 0 makes a cone. `n` segments, rotated by `phase` turns.
 */
function frustum(p: Part, cx: number, cy: number, r0: number, z0: number, r1: number, z1: number,
  n: number, o: { top?: boolean; bottom?: boolean; phase?: number; flat?: boolean } = {}) {
  const ph = (o.phase ?? 0.5 / n) * TAU
  const k = (r0 - r1) / (z1 - z0)
  const at = (i: number, r: number, z: number): V3 => {
    const t = ph + (i / n) * TAU
    return [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
  }
  const nrm = (t: number): V3 => unit([Math.cos(t), Math.sin(t), k])
  for (let i = 0; i < n; i++) {
    const j = i + 1
    const ti = ph + (i / n) * TAU, tj = ph + (j / n) * TAU
    const ni = o.flat ? nrm((ti + tj) / 2) : nrm(ti), nj = o.flat ? ni : nrm(tj)
    if (r1 <= 0) {
      tri(p, at(i, r0, z0), at(j, r0, z0), [cx, cy, z1], [ni, nj, nrm((ti + tj) / 2)])
    } else {
      quad(p, at(i, r0, z0), at(j, r0, z0), at(j, r1, z1), at(i, r1, z1), [ni, nj, nj, ni])
    }
  }
  if (o.top && r1 > 0) for (let i = 1; i < n - 1; i++) tri(p, at(0, r1, z1), at(i, r1, z1), at(i + 1, r1, z1))
  if (o.bottom) for (let i = 1; i < n - 1; i++) tri(p, at(0, r0, z0), at(i + 1, r0, z0), at(i, r0, z0))
}

const area = (q: XY[]) => q.reduce((s, a, i) => { const b = q[(i + 1) % q.length]; return s + a[0] * b[1] - b[0] * a[1] }, 0) / 2

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
function earcut(poly: XY[]): [number, number, number][] {
  const idx = poly.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 1e-9 && crossz(b, c, p) > 1e-9 && crossz(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = poly[i0], b = poly[i1], c = poly[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(poly[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) idx.splice(0, 1) // degenerate remainder
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

/** A vertical prism over a counter-clockwise polygon, flat-shaded. */
function prism(p: Part, poly: XY[], z0: number, z1: number, o: { top?: boolean; bottom?: boolean; skip?: (a: XY, b: XY) => boolean } = {}) {
  if (area(poly) < 0) poly = [...poly].reverse()
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    if (Math.hypot(b[0] - a[0], b[1] - a[1]) < 1e-6 || o.skip?.(a, b)) continue
    quad(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
  }
  const tris = earcut(poly)
  if (o.top !== false) for (const [i, j, k] of tris) tri(p, [...poly[i], z1] as V3, [...poly[j], z1] as V3, [...poly[k], z1] as V3)
  if (o.bottom) for (const [i, j, k] of tris) tri(p, [...poly[i], z0] as V3, [...poly[k], z0] as V3, [...poly[j], z0] as V3)
}

/**
 * A profile in the (r, z) plane, extruded `w` wide across the radius at
 * angle `t`: one rib. The profile runs counter-clockwise seen from +t.
 */
function rib(p: Part, prof: XY[], t: number, w: number) {
  const c = Math.cos(t), s = Math.sin(t)
  const at = ([r, z]: XY, side: number): V3 => [r * c - side * s, r * s + side * c, z]
  const h = w / 2
  for (let i = 0; i < prof.length; i++) {
    const a = prof[i], b = prof[(i + 1) % prof.length]
    quad(p, at(a, h), at(b, h), at(b, -h), at(a, -h))
  }
  for (const [i, j, k] of earcut(prof)) {
    tri(p, at(prof[i], h), at(prof[k], h), at(prof[j], h))
    tri(p, at(prof[i], -h), at(prof[j], -h), at(prof[k], -h))
  }
}

/** A slender four-sided spike: base w × d at z0, oriented along angle t. */
function spike(p: Part, x: number, y: number, z0: number, h: number, w: number, d: number, t = 0) {
  const c = Math.cos(t), s = Math.sin(t)
  const P = (u: number, v: number): V3 => [x + u * c - v * s, y + u * s + v * c, z0]
  const ring = [P(-d / 2, -w / 2), P(d / 2, -w / 2), P(d / 2, w / 2), P(-d / 2, w / 2)]
  const top: V3 = [x, y, z0 + h]
  for (let i = 0; i < 4; i++) tri(p, ring[i], ring[(i + 1) % 4], top)
}

// ---------------------------------------------------------------------------

const N_RIB = 72
const WALL_R = 46.5, WALL_H = 7.2     // the pale-blue base wall
const EAVE_R = 48.5, EAVE_Z = 8.4     // the cone's lower edge
const COLLAR_R = 18.6, COLLAR_Z = 33.5  // way/373171487
const FOOT_R = 52.8                   // way/1106733755: the ribs' feet
const cone = (r: number) => EAVE_Z + (EAVE_R - r) * (COLLAR_Z - EAVE_Z) / (EAVE_R - COLLAR_R)

// Base wall and the soffit under the eave.
frustum(base, 0, 0, WALL_R, 0, WALL_R, WALL_H, N_RIB, { flat: true, phase: 0 })
frustum(shell, 0, 0, WALL_R, WALL_H, EAVE_R, EAVE_Z, N_RIB, { phase: 0 })

// The cone, eave to collar, and its ribs.
frustum(shell, 0, 0, EAVE_R, EAVE_Z, COLLAR_R, COLLAR_Z, N_RIB, { flat: true, phase: 0 })
{
  const D = 1.15 // how far a rib stands proud of the cone
  const prof: XY[] = [
    [WALL_R - 0.2, 4.6],               // under the foot, against the wall
    [FOOT_R, 5.4],                     // the foot's point
    [FOOT_R, 6.6],
    [EAVE_R + 0.6, cone(EAVE_R) + D],  // onto the cone
    [COLLAR_R + 0.4, cone(COLLAR_R + 0.4) + D],
    [COLLAR_R, cone(COLLAR_R)],
    [EAVE_R, EAVE_Z],
    [WALL_R - 0.2, WALL_H],
  ]
  for (let k = 0; k < N_RIB; k++) rib(ribs, prof, (k + 0.5) / N_RIB * TAU, 0.95)
}

// The collar: a thick white ring where the ribs end.
frustum(ribs, 0, 0, COLLAR_R + 0.9, COLLAR_Z - 0.6, COLLAR_R + 0.9, COLLAR_Z + 1.2, 36)
frustum(ribs, 0, 0, COLLAR_R + 0.9, COLLAR_Z + 1.2, COLLAR_R - 0.6, COLLAR_Z + 1.25, 36)

// Upper cone, collar to the top ring (way/373171486), finely ribbed, then the
// flat top inside way/373171485.
const TOP_Z = 39.5
frustum(shell, 0, 0, COLLAR_R - 0.6, COLLAR_Z + 1.2, 12.8, TOP_Z - 0.6, 36, { flat: true, phase: 0 })
{
  const prof: XY[] = [[COLLAR_R - 0.6, COLLAR_Z + 1.2], [COLLAR_R - 0.3, COLLAR_Z + 1.8], [12.9, TOP_Z - 0.1], [12.8, TOP_Z - 0.6]]
  for (let k = 0; k < 36; k++) rib(ribs, prof, (k + 0.5) / 36 * TAU, 0.6)
}
frustum(ribs, 0, 0, 13.1, TOP_Z - 0.6, 13.1, TOP_Z, 24)
frustum(ribs, 0, 0, 13.1, TOP_Z, 8.9, TOP_Z + 0.6, 24, { top: true })

// Spire clusters on the collar: four of three, the middle one tallest.
for (let c = 0; c < 4; c++) {
  const t0 = (c / 4 + 1 / 8) * TAU
  for (const [dt, h] of [[-0.08, 8.0], [0, 11.0], [0.08, 6.5]] as [number, number][]) {
    const t = t0 + dt
    spike(ribs, (COLLAR_R + 0.3) * Math.cos(t), (COLLAR_R + 0.3) * Math.sin(t), COLLAR_Z + 0.6, h, 0.8, 1.7, t)
  }
}

// The central mast: three tapering blades round a shaft, and the antenna.
{
  const z0 = TOP_Z + 0.6
  frustum(ribs, 0, 0, 1.4, z0, 0.5, 52, 6)
  for (let k = 0; k < 3; k++) {
    const t = (k / 3) * TAU + TAU / 12
    spike(ribs, 1.6 * Math.cos(t), 1.6 * Math.sin(t), z0, 12.0, 0.6, 2.6, t)
  }
  frustum(ribs, 0, 0, 0.5, 52, 0, 56, 4)
}

// The low entrance wing on the south-west (way/1106733754).
const WING: XY[] = [[-55.7, 12.4], [-52.9, 12.1], [-51.6, 11.5], [-52.8, 4.0], [-52.8, -3.4], [-51.8, -10.9], [-49.7, -18.0], [-46.6, -24.9], [-42.6, -31.2], [-37.7, -36.8], [-34.0, -40.2], [-30.6, -42.8], [-36.5, -46.9], [-55.1, -28.3], [-55.2, -22.7]]
prism(base, WING, 0, 5)

// ---------------------------------------------------------------------------

// Space Mountain is white: the shell a touch greyer than the ribs so the
// ribbing reads in flat map light. The base wall's pale blue is its own.
const parts = [
  { part: shell, material: finish('space-shell', 0xd8dcde) },
  { part: ribs, material: finish('space-white', 0xf8f8f4) },
  { part: base, material: finish('space-blue', 0xadc3d3) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Space Mountain', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 56,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-space-mountain.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
