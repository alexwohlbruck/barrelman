/**
 * Mexico pavilion pyramid, EPCOT World Showcase — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-mexico-pyramid.ts
 *
 * Map frame: x across the pavilion, y along its long axis from the plaza
 * (south, -y) to the back of the show building (north), z up, metres. Placed
 * at bearing 54°, the axis of the OSM outline (way/295598769), so the
 * pyramid's front faces 234°, towards the promenade and the lagoon. The
 * anchor is the outline's local origin (near its middle).
 *
 * What it is: a Mesoamerican stepped pyramid in sand-coloured render. It
 * has four tiers, each a battered wall under an upright panelled band and a
 * deep red-brown cornice. A central stair between sloped balustrades carries
 * jade serpent heads at every tier, and a carved stele stands at the stair's
 * foot. The temple on top has a band of windows (the fireworks control room
 * sits up there) and a stepped cap. Behind it is the show building that holds
 * the indoor plaza and the boat ride, with a lower wing either side of the
 * pyramid on the plaza front, banded with the same cornices and carved panels.
 *
 * Evidence
 * - OSM (measured): way/295598769, a 54 × 105 m rectangle with a 19.7 ×
 *   15.5 m projection at its south-west end. That projection is the pyramid
 *   (its centre line is 4.65 m east of the rectangle's), and it holds the
 *   untagged way/529602448. No heights are tagged.
 * - USGS NAIP 2022 orthophoto (public domain): the pyramid sits on the
 *   projection with the plain flat-roofed show building behind. The
 *   building's shadow suggests about 11–13 m, so the model takes 13 m.
 * - Published (Wikipedia): a Mesoamerican pyramid with steps up to the
 *   entrance, and the fireworks control office on top.
 * - Photos (Wikimedia Commons): front from the stair foot (Mexico pavilion
 *   at Epcot.jpg, Benjamin D. Esham, CC BY-SA 4.0), front from the plaza
 *   (Mexico Pavilion (42550348454).jpg, HarshLight, CC BY 2.0), from the
 *   front left (Mexico Pyramid (49560738966).jpg, Eden, Janine and Jim, CC
 *   BY 2.0), and the side and temple (Mexico at EPCOT (cropped).jpg, public
 *   domain).
 * - Estimated from the photos, with people and the stair for scale: tiers
 *   topping out at 6, 10.3, 13.8 and 17 m; the temple to 20.5 m, the cap to
 *   25.2 m; the stair 3.3 m between 1 m balustrades; tier widths stepping in
 *   from the outline's 19.7 m; the plaza wings at 9.6 m, with a set-back upper
 *   wall to 14.5 m.
 * - Invented: the pyramid's back half, which runs into the show building,
 *   and the number of recessed panels per tier. The carved reliefs and
 *   serpent heads are drawn as plain jade panels and blocks.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'
// ---------------------------------------------------------------------------
// Modelling helpers (shared shape with the other EPCOT pavilion generators;
// kept inside this file so the generator stands alone).

type XY = [number, number]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle whose winding is fixed to agree with its normals. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const f = cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [c[0] - a[0], c[1] - a[1], c[2] - a[2]])
  if (len(f) < 1e-9) return
  if (dot(f, add(add(n[0], n[1]), n[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}
/** A flat convex polygon, its normal turned towards `hint`. */
function face(p: Part, pts: V3[], hint: V3) {
  const q = pts.filter((v, i) => len(sub(v, pts[(i + 1) % pts.length])) > 1e-6)
  if (q.length < 3) return
  let n: V3 = [0, 0, 0]
  for (let i = 0; i < q.length; i++) {
    const a = q[i], b = q[(i + 1) % q.length]
    n = add(n, [(a[1] - b[1]) * (a[2] + b[2]), (a[2] - b[2]) * (a[0] + b[0]), (a[0] - b[0]) * (a[1] + b[1])])
  }
  n = unit(n)
  if (dot(n, hint) < 0) n = [-n[0], -n[1], -n[2]]
  for (let i = 1; i < q.length - 1; i++) tri(p, q[0], q[i], q[i + 1], [n, n, n])
}

// Blocks: rectangles with chamfered vertical corners and a rounded top edge.
type Rect = { x0: number; x1: number; y0: number; y1: number; c: number }
const R = (x0: number, x1: number, y0: number, y1: number, c = 0.1): Rect => ({ x0, x1, y0, y1, c })
function ring(r: Rect, d: number, z: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const c = Math.max(0.02, r.c + d * 0.414)
  const pts: V3[] = [[x0 + c, y0, z], [x1 - c, y0, z], [x1, y0 + c, z], [x1, y1 - c, z],
    [x1 - c, y1, z], [x0 + c, y1, z], [x0, y1 - c, z], [x0, y0 + c, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
function band(p: Part, r: Rect, d0: number, z0: number, d1: number, z1: number, n0: XY, n1: XY) {
  const a = ring(r, d0, z0), b = ring(r, d1, z1)
  const at = (h: V3, n: XY): V3 => unit([h[0] * n[0], h[1] * n[0], n[1]])
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    quad(p, a.pts[i], a.pts[j], b.pts[j], b.pts[i], [at(a.nrm[i], n0), at(a.nrm[j], n0), at(a.nrm[j], n1), at(a.nrm[i], n1)])
  }
}
function lid(p: Part, r: Rect, d: number, z: number, up: boolean) {
  const { pts } = ring(r, d, z)
  const n: V3 = [0, 0, up ? 1 : -1]
  for (let i = 1; i < 7; i++) tri(p, pts[0], pts[i], pts[i + 1], [n, n, n])
}
const OUT: XY = [1, 0], UP: XY = [0, 1], S2 = Math.SQRT1_2
/** A solid block: optional bevel under, plain face, bevel over, then the caps. */
function block(p: Part, r: Rect, z0: number, z1: number, o: { bb?: number; bt?: number; top?: Part | null; bottom?: boolean } = {}) {
  const bb = o.bb ?? 0, bt = o.bt ?? 0
  if (bb) band(p, r, -bb, z0, 0, z0 + bb, [S2, -S2], OUT)
  band(p, r, 0, z0 + bb, 0, z1 - bt, OUT, OUT)
  if (bt) band(p, r, 0, z1 - bt, -bt, z1, OUT, UP)
  if (o.top !== null) lid(o.top ?? p, r, -bt, z1, true)
  if (o.bottom) lid(p, r, -bb, z0, false)
}
/** A projecting slab (cornice, string course): bevelled under and over. */
function slab(p: Part, r: Rect, z0: number, z1: number, top?: Part) {
  const b = Math.min(0.25, (z1 - z0) / 3)
  band(p, r, -b, z0, 0, z0 + b, [S2, -S2], OUT)
  band(p, r, 0, z0 + b, 0, z1 - b, OUT, OUT)
  band(p, r, 0, z1 - b, -b, z1, OUT, UP)
  lid(top ?? p, r, -b, z1, true)
  lid(p, r, -b, z0, false)
}

/**
 * A frustum between two axis-aligned rectangles [x0, x1, y0, y1]: hip and
 * pyramid roofs (a top collapsed to a point or a ridge), gables (a top
 * collapsed across one axis) and tapering tiers. Flat-shaded planes.
 */
type Box = [number, number, number, number]
function frustum(p: Part, b: Box, t: Box, z0: number, z1: number, o: { top?: Part | null; bottom?: Part | null; ends?: Part } = {}) {
  const B: V3[] = [[b[0], b[2], z0], [b[1], b[2], z0], [b[1], b[3], z0], [b[0], b[3], z0]]
  const T: V3[] = [[t[0], t[2], z1], [t[1], t[2], z1], [t[1], t[3], z1], [t[0], t[3], z1]]
  const out: V3[] = [[0, -1, 0], [1, 0, 0], [0, 1, 0], [-1, 0, 0]]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    // A side whose top has collapsed to a point and which stands upright is a
    // gable end, and can take its own material.
    const nz = cross(sub(B[j], B[i]), sub(T[i], B[i]))[2]
    const gableEnd = len(sub(T[i], T[j])) < 1e-6 && Math.abs(nz) < 1e-6
    face(gableEnd && o.ends ? o.ends : p, [B[i], B[j], T[j], T[i]], out[i])
  }
  if (o.top !== null && t[1] - t[0] > 1e-6 && t[3] - t[2] > 1e-6) face(o.top ?? p, T, [0, 0, 1])
  if (o.bottom) face(o.bottom, B, [0, 0, -1])
}


/** Ear-clipping triangulation of a simple polygon (counter-clockwise). */
function earcut(poly: XY[]): [number, number, number][] {
  const idx = poly.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) >= 0 && crossz(b, c, p) >= 0 && crossz(c, a, p) >= 0
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i = idx[(k + idx.length - 1) % idx.length], j = idx[k], l = idx[(k + 1) % idx.length]
      if (crossz(poly[i], poly[j], poly[l]) <= 1e-9) continue
      if (idx.some((m) => m !== i && m !== j && m !== l && inside(poly[m], poly[i], poly[j], poly[l]))) continue
      out.push([i, j, l]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
const area = (poly: XY[]) => poly.reduce((s, p, i) => { const q = poly[(i + 1) % poly.length]; return s + p[0] * q[1] - q[0] * p[1] }, 0) / 2
/** A vertical extrusion of a simple polygon, walls flat-shaded. */
function prism(p: Part, poly0: XY[], z0: number, z1: number, o: { top?: Part | null; bottom?: boolean } = {}) {
  const poly = area(poly0) < 0 ? [...poly0].reverse() : poly0
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    if (Math.hypot(b[0] - a[0], b[1] - a[1]) < 1e-6) continue
    face(p, [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]], [b[1] - a[1], a[0] - b[0], 0])
  }
  for (const [i, j, k] of earcut(poly)) {
    if (o.top !== null) face(o.top ?? p, [[...poly[i], z1], [...poly[j], z1], [...poly[k], z1]] as V3[], [0, 0, 1])
    if (o.bottom) face(p, [[...poly[i], z0], [...poly[j], z0], [...poly[k], z0]] as V3[], [0, 0, -1])
  }
}
/** Clip a polygon to the half-plane where `keep(x, y) >= 0` (a linear function). */
function clip(poly: XY[], keep: (p: XY) => number): XY[] {
  const out: XY[] = []
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length], fa = keep(a), fb = keep(b)
    if (fa >= 0) out.push(a)
    if ((fa >= 0) !== (fb >= 0)) { const t = fa / (fa - fb); out.push([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]) }
  }
  return out
}

// Faces of a rectangle: s runs left to right seen from outside, z up. Panels
// are flat shapes set just proud of the wall.
type Face = { o: XY; u: XY; n: V3; L: number }
function faces(r: { x0: number; x1: number; y0: number; y1: number }): Record<'s' | 'e' | 'n' | 'w', Face> {
  return {
    s: { o: [r.x0, r.y0], u: [1, 0], n: [0, -1, 0], L: r.x1 - r.x0 },
    e: { o: [r.x1, r.y0], u: [0, 1], n: [1, 0, 0], L: r.y1 - r.y0 },
    n: { o: [r.x1, r.y1], u: [-1, 0], n: [0, 1, 0], L: r.x1 - r.x0 },
    w: { o: [r.x0, r.y1], u: [0, -1], n: [-1, 0, 0], L: r.y1 - r.y0 },
  }
}
const at = (f: Face, s: number, z: number, d = 0): V3 => [f.o[0] + f.u[0] * s + f.n[0] * d, f.o[1] + f.u[1] * s + f.n[1] * d, z]
function panel(p: Part, f: Face, pts: XY[], d = 0.04) {
  const v = pts.map(([s, z]) => at(f, s, z, d))
  for (let i = 1; i < v.length - 1; i++) tri(p, v[0], v[i], v[i + 1], [f.n, f.n, f.n])
}
const rect = (s0: number, s1: number, z0: number, z1: number): XY[] => [[s0, z0], [s1, z0], [s1, z1], [s0, z1]]
/** Evenly spaced bay centres along a face of length L, `n` bays with `margin` at each end. */
const bays = (L: number, n: number, margin = 0) => Array.from({ length: n }, (_, i) => margin + ((L - 2 * margin) * (i + 0.5)) / n)

// ---------------------------------------------------------------------------
// The pavilion. y runs along the outline's long axis from the plaza (south,
// the pyramid's front) to the back of the show building (north).

const sand = new Part(), brown = new Part(), teal = new Part()
const win = new Part(), roof = new Part()

/** OSM way/295598769 in the model frame (measured). */
const osm: XY[] = [
  [22.1, 53.3], [22.1, 31.3], [26.9, 31.3], [26.8, -3.4], [25.1, -3.4], [25.1, -27.9], [24.5, -27.9], [24.5, -33.2],
  [22.2, -33.2], [22.2, -48.9], [20.3, -48.9], [20.3, -51.8], [14.8, -51.7], [14.5, -67.6], [-5.2, -67.5],
  [-5.1, -52.3], [-25.0, -52.3], [-25.0, -48.9], [-26.9, -48.9], [-26.9, -46.8], [-27.0, 53.0],
]

const SHOW_H = 13.0, WING_H = 9.6, WING_TOP = 14.5
const XC = 4.65 // the pyramid's axis, centred on the outline's projection

// The show building: everything but the pyramid's projection, a plain box
// with a red-brown band round its top. Its plaza front is a lower wing either
// side of the pyramid.
const back = clip(osm, ([, y]) => y + 48.8)
prism(sand, back, 0, SHOW_H - 0.6, { top: null })
prism(brown, back, SHOW_H - 0.6, SHOW_H, { top: roof })
const front = clip(clip(osm, ([, y]) => -48.8 - y), ([, y]) => y + 52.4)
prism(sand, front, 0, WING_H, { top: roof })
// The wing fronts: two cornices like the pyramid's tiers, recessed panels
// below and tall carved panels between them.
for (const [x0, x1] of [[-25.0, -5.2], [14.5, 20.3]] as XY[]) {
  slab(brown, R(x0, x1, -52.9, -51.8, 0.1), 4.9, 5.6)
  slab(brown, R(x0, x1, -52.9, -50.4, 0.1), WING_H - 0.6, WING_H + 0.1, roof)
  const g = faces(R(x0, x1, -52.3, 0)).s
  const n = Math.round(g.L / 4.2)
  for (const s of bays(g.L, n, 0.6)) panel(brown, g, rect(s - 1.0, s + 1.0, 1.2, 3.9), 0.04)
  for (const s of bays(g.L, n, 0.6)) panel(brown, g, rect(s - 1.0, s + 1.0, 6.2, 8.4), 0.04)
  // The set-back upper wall with its tall carved panels.
  block(sand, R(x0, x1, -50.6, -48.8, 0.08), WING_H, WING_TOP - 0.6, { top: null })
  slab(brown, R(x0, x1, -51.0, -48.8, 0.1), WING_TOP - 0.6, WING_TOP, roof)
  const u = faces(R(x0, x1, -50.6, 0)).s
  for (const s of bays(u.L, Math.max(1, n - 1), 1.5)) panel(teal, u, rect(s - 0.8, s + 0.8, WING_H + 0.6, WING_TOP - 1.1), 0.05)
}

// The pyramid: four tiers, each a battered wall under a vertical band and a
// projecting red-brown cornice, split by the stair.
type Tier = { z0: number; z1: number; hw: number; drop: number; front: number }
const tiers: Tier[] = [
  { z0: 0, z1: 6.0, hw: 9.85, drop: 1.6, front: -67.4 },
  { z0: 6.0, z1: 10.3, hw: 7.6, drop: 1.2, front: -64.9 },
  { z0: 10.3, z1: 13.8, hw: 5.8, drop: 0.9, front: -62.5 },
  { z0: 13.8, z1: 17.0, hw: 4.4, drop: 0.6, front: -60.4 },
]
const BACK = -48.6, SW = 2.7 // stair slot half-width, balustrades included
for (const t of tiers) {
  // The front is a short batter under an upright panelled band; the outer
  // sides slope all the way up, which is what gives the pyramid its outline.
  const zv = t.z0 + (t.z1 - t.z0) * 0.45, zc = t.z1 - 0.75
  const fb = (zv - t.z0) * 0.3, sv = t.drop * (zv - t.z0) / (zc - t.z0)
  for (const side of [-1, 1]) {
    const inner = XC + side * SW, outer = (d: number) => XC + side * (t.hw - d)
    const box = (d: number, f: number): Box => side < 0 ? [outer(d), inner, f, BACK] : [inner, outer(d), f, BACK]
    frustum(sand, box(0, t.front), box(sv, t.front + fb), t.z0, zv, { top: null })
    frustum(sand, box(sv, t.front + fb), box(t.drop, t.front + fb), zv, zc, { top: null })
    const top = box(t.drop, t.front + fb)
    slab(brown, R(top[0] - (side < 0 ? 0.45 : 0), top[1] + (side > 0 ? 0.45 : 0), top[2] - 0.45, BACK, 0.12), zc, t.z1, sand)
    // Recessed panels on the upright band.
    const ff = faces(R(top[0], top[1], top[2], BACK)).s
    for (const s of bays(ff.L, ff.L > 4.5 ? 2 : 1, 0.6)) panel(brown, ff, rect(s - 0.85, s + 0.85, zv + 0.35, zc - 0.35), 0.04)
  }
}

// The stair and its balustrades: one sloped mass from the plaza to the
// temple, the balustrades standing proud of the treads, a carved stele at
// their foot and serpent heads up their sides.
{
  const y0 = -68.4, y1 = -59.0, z1 = 17.0, hwS = 1.65
    // Treads (one sloped plane), sides vertical, back into the pyramid.
  const P = (x: number, y: number, z: number): V3 => [x, y, z]
  // Treads in a dozen big steps, so the stair reads as one at map scale.
  const N = 12
  for (let k = 0; k < N; k++) {
    const ya = y0 + ((y1 - y0) * k) / N, yb = y0 + ((y1 - y0) * (k + 1)) / N
    const za = (z1 * k) / N, zb = (z1 * (k + 1)) / N
    face(sand, [P(XC - hwS, ya, za), P(XC + hwS, ya, za), P(XC + hwS, ya, zb), P(XC - hwS, ya, zb)], [0, -1, 0])
    face(sand, [P(XC - hwS, ya, zb), P(XC + hwS, ya, zb), P(XC + hwS, yb, zb), P(XC - hwS, yb, zb)], [0, 0, 1])
  }
  face(sand, [P(XC - hwS, y1, z1), P(XC + hwS, y1, z1), P(XC + hwS, BACK, z1), P(XC - hwS, BACK, z1)], [0, 0, 1])
  for (const side of [-1, 1]) {
    const xi = XC + side * hwS, xo = XC + side * SW, lift = 0.7
    // Balustrade: a sloped block between xi and xo.
    face(sand, [P(xi, y0, lift), P(xo, y0, lift), P(xo, y1, z1 + lift), P(xi, y1, z1 + lift)], [0, -1, 1])
    face(sand, [P(xi, y0, 0), P(xi, y0, lift), P(xi, y1, z1 + lift), P(xi, y1, 0)], [-side, 0, 0])
    face(sand, [P(xo, y0, 0), P(xo, y0, lift), P(xo, y1, z1 + lift), P(xo, BACK, z1 + lift), P(xo, BACK, 0)], [side, 0, 0])
    face(sand, [P(xi, y0, 0), P(xo, y0, 0), P(xo, y0, lift), P(xi, y0, lift)], [0, -1, 0])
    face(sand, [P(xi, y1, z1 + lift), P(xo, y1, z1 + lift), P(xo, BACK, z1 + lift), P(xi, BACK, z1 + lift)], [0, 0, 1])
    // Serpent heads at each tier's cornice.
    for (const t of tiers) {
      const y = y0 + (t.z1 / z1) * (y1 - y0), z = t.z1 + lift
      block(teal, R(XC + side * (SW + hwS) / 2 - 0.65, XC + side * (SW + hwS) / 2 + 0.65, y - 0.8, y + 0.6, 0.15), z - 0.7, z + 0.6, { bt: 0.2 })
    }
  }
  // The stele at the stair's foot: a carved panel under a stepped gable.
  const sy = y0 + 0.4
  block(sand, R(XC - 1.75, XC + 1.75, sy - 0.5, sy + 0.4, 0.08), 0, 4.0, { top: null })
  frustum(sand, [XC - 1.75, XC + 1.75, sy - 0.5, sy + 0.4], [XC - 0.45, XC + 0.45, sy - 0.5, sy + 0.4], 4.0, 5.2)
  const f = faces(R(XC - 1.75, XC + 1.75, sy - 0.5, sy + 0.4)).s
  panel(teal, f, rect(0.3, 3.2, 0.5, 3.7), 0.05)
}

// The temple on top: a band of windows (the fireworks control room), a
// red-brown cornice, and the stepped cap.
{
  const yc = -55.9, h = 2.9
  block(sand, R(XC - h, XC + h, yc - h, yc + h, 0.12), 17.0, 19.7, { top: null })
  const f = faces(R(XC - h, XC + h, yc - h, yc + h))
  for (const side of [f.s, f.e, f.w]) panel(win, side, rect(0.7, side.L - 0.7, 17.9, 19.3))
  slab(brown, R(XC - h - 0.5, XC + h + 0.5, yc - h - 0.5, yc + h + 0.5, 0.15), 19.7, 20.5)
  for (let k = 0; k < 4; k++) {
    const x = XC - h + 0.9 + k * ((2 * h - 1.8) / 3)
    block(brown, R(x - 0.35, x + 0.35, yc - h - 0.75, yc - h - 0.3, 0.06), 19.9, 20.6, { bt: 0.08 })
  }
  frustum(sand, [XC - h, XC + h, yc - h, yc + h], [XC - 1.7, XC + 1.7, yc - 1.7, yc + 1.7], 20.5, 23.3)
  block(sand, R(XC - 1.45, XC + 1.45, yc - 1.45, yc + 1.45, 0.1), 23.3, 23.7, { top: null })
  frustum(sand, [XC - 1.45, XC + 1.45, yc - 1.45, yc + 1.45], [XC - 0.8, XC + 0.8, yc - 0.8, yc + 0.8], 23.7, 25.2)
}

// ---------------------------------------------------------------------------

const parts = [
  { part: sand, material: finish('mexico-sand', 0xe4c09c) },
  { part: brown, material: PALETTE.terracotta },
  { part: teal, material: finish('jade', 0x6fa89c) },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Mexico Pavilion pyramid', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 54, elevation: 0,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-mexico-pyramid.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
