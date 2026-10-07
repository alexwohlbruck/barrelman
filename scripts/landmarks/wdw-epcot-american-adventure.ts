/**
 * The American Adventure, EPCOT World Showcase — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-american-adventure.ts
 *
 * Map frame: x east, y north, z up, metres; placed at bearing 0°, since the
 * OSM outline's long walls run due north–south. The front faces north, over
 * the plaza and fountain to the lagoon. The anchor sits on the portico's
 * axis, 11.5 m west of the outline's own local origin, so the hall is
 * symmetrical about x = 0.
 *
 * What it is: a Georgian red-brick hall with white trim. A three-bay central
 * block carries a steep slate hip roof up to a railed deck, with a white
 * clock tower, an open lantern and a small dome on top, and a tall chimney
 * either side. A two-storey white portico with a balcony stands in front.
 * Lower two-bay wings run to an end pavilion at each side; each end
 * pavilion has its own railed hip roof, a cupola and a pedimented porch.
 * Forced perspective makes the five-storey building look two and a half
 * storeys tall. Behind it is the Liberty Theatre's show building, a plain
 * box covering the rest of the OSM outline, kept below the hall's roofs as
 * the real one is. A flat-roofed annex sits to the east.
 *
 * Evidence
 * - OSM (measured): way/295592223, 80 × 105 m. Its front has a notch for the
 *   wings, a portico projection and end pavilions, which fix the hall's plan
 *   to within a metre or two. Its building:part way/515970173 lies inside.
 *   The outline's height=10 is only the wings' eaves, so it is not used.
 * - USGS NAIP 2022 orthophoto (public domain), measured: the hall is
 *   55.8 m wide, with 11.5 m end pavilions and a 16 m portico, the show
 *   building flat-roofed behind it, and the annex east of the hall.
 * - Published (Wikipedia): five storeys forced to look like two and a half.
 * - Photos (Wikimedia Commons, credited in the placement provenance): front
 *   (Epcotusa.jpg, public domain), front from the fountain (America - EPCOT
 *   - February 2009 by hyku.jpg, CC BY-SA 2.0), from the north-west (The
 *   American Adventure (37043444723).jpg, Sam Howzit, CC BY 2.0), and from
 *   across the lagoon (American Pavillion - panoramio.jpg, Eric Marshall,
 *   CC BY 3.0).
 * - Estimated from the photos, with people and the portico for scale:
 *   portico columns 6.6 m, central eave 11 m, wing eaves 10.3 m, pavilion
 *   eaves 10.8 m, central deck 17 m, clock stage 17 to 21 m, dome top 25.9 m,
 *   pavilion decks 16 m with cupolas to 21 m.
 * - Estimated without evidence: the show building's 12 m (it must sit below
 *   the hall's roofs, which hide it from the plaza) and the annex's 7.5 m.
 *   The depth of the hall's roofs (about 16 m) is what the aerial allows.
 * - Simplified: the windows' green shutters, the banners and the clock face.
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

/** A solid of revolution about (cx, cy) from [r, z] pairs, smooth-shaded. */
function lathe(p: Part, cx: number, cy: number, prof: XY[], seg: number, phase = 0, flat = false) {
  for (let k = 0; k < seg; k++) {
    const a = phase + (k / seg) * 2 * Math.PI, b = phase + ((k + 1) / seg) * 2 * Math.PI
    for (let i = 0; i < prof.length - 1; i++) {
      const [r0, z0] = prof[i], [r1, z1] = prof[i + 1]
      if (r0 < 1e-6 && r1 < 1e-6) continue
      const P = (r: number, z: number, t: number): V3 => [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
      // Profile normal: (dz, -dr) in (radial, up).
      const nr = z1 - z0, nz = -(r1 - r0), l = Math.hypot(nr, nz) || 1
      const N = (t: number): V3 => [Math.cos(t) * nr / l, Math.sin(t) * nr / l, nz / l]
      if (flat) {
        const m = (a + b) / 2
        face(p, [P(r0, z0, a), P(r0, z0, b), P(r1, z1, b), P(r1, z1, a)], N(m))
      } else quad(p, P(r0, z0, a), P(r0, z0, b), P(r1, z1, b), P(r1, z1, a), [N(a), N(b), N(b), N(a)])
    }
  }
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
/** Round-headed opening, counter-clockwise from the bottom left. */
function roundArch(s0: number, s1: number, z0: number, spring: number, seg = 6): XY[] {
  const r = (s1 - s0) / 2, c = s0 + r
  const pts: XY[] = [[s0, z0], [s1, z0]]
  for (let k = 0; k <= seg; k++) { const a = (k / seg) * Math.PI; pts.push([c + r * Math.cos(a), spring + r * Math.sin(a)]) }
  return pts
}
/** Evenly spaced bay centres along a face of length L, `n` bays with `margin` at each end. */
const bays = (L: number, n: number, margin = 0) => Array.from({ length: n }, (_, i) => margin + ((L - 2 * margin) * (i + 0.5)) / n)

// ---------------------------------------------------------------------------
// The pavilion. y runs from the show building (south) to the lagoon (north);
// the front faces +y. x = 0 is the portico's axis.

const brick = new Part(), trim = new Part(), slate = new Part()
const win = new Part(), stone = new Part(), door = new Part()

/** OSM way/295592223 in the model frame (measured): its own local metres, x shifted by 11.5 m onto the portico axis. */
const osm: XY[] = ([
  [-38.8, 46.9], [-38.6, 33.7], [-38.6, 21.1], [-39.3, 21.1], [-39.2, 4.6], [-38.4, 4.6], [-38.2, -10.8], [-36.9, -10.7],
  [-36.7, -28.0], [-31.2, -27.9], [-31.2, -30.8], [-21.8, -30.7], [-21.7, -40.5], [-16.2, -40.4], [-16.1, -48.5],
  [-17.6, -48.5], [-17.6, -51.6], [-3.0, -51.5], [-3.0, -47.2], [-4.3, -47.2], [-4.4, -40.8], [1.0, -40.8], [0.9, -31.6],
  [9.9, -31.6], [9.9, -28.3], [15.5, -28.3], [15.4, -29.2], [21.0, -29.2], [21.0, -28.3], [37.8, -28.2], [37.8, -16.9],
  [40.6, -16.9], [40.6, 5.8], [39.3, 5.8], [39.3, 24.3], [36.7, 24.3], [35.4, 50.5], [27.3, 50.6], [27.3, 46.0],
  [18.5, 45.9], [18.5, 46.9], [5.5, 46.9], [5.5, 40.8], [-1.5, 40.7], [-1.5, 44.3], [-2.8, 44.3], [-2.9, 50.4],
  [-5.2, 50.4], [-5.3, 53.9], [-16.3, 53.8], [-16.3, 50.4], [-19.1, 50.4], [-19.0, 44.5], [-20.6, 44.5], [-20.6, 41.3],
  [-26.8, 41.3], [-26.8, 47.0],
] as XY[]).map(([x, y]) => [x + 11.5, y] as XY)

// Heights (see the header for where each comes from).
const WING_EAVE = 10.3, PAV_EAVE = 10.8, MID_EAVE = 11.0
const SHOW_H = 12, PAV_DECK = 16.0

// The show building: the OSM outline behind the front hall, a plain box
// below the hall's roofs so the forced perspective keeps it out of sight.
prism(stone, clip(osm, ([, y]) => 31 - y), 0, SHOW_H)
// The flat-roofed annex east of the hall (in OSM and the aerial).
// It is hidden by trees from the plaza, so it stays plain like the show
// building.
prism(stone, clip(clip(osm, ([, y]) => y - 30.5), ([x]) => x - 27.9), 0, 7.5)

/** A brick block with a white cornice, white quoins at its front corners. */
function hall(r: Rect, eave: number) {
  block(brick, r, 0, eave, { top: null })
  slab(trim, R(r.x0 - 0.35, r.x1 + 0.35, r.y0 - 0.35, r.y1 + 0.35, 0.2), eave, eave + 0.6)
  const f = faces(r)
  for (const side of [f.n, f.e, f.w]) for (const s of [0.05, side.L - 0.65]) panel(trim, side, rect(s, s + 0.6, 0, eave), 0.03)
  return f
}
/** A hip roof rising from an eave rectangle to a deck or ridge. */
function hip(b: Box, t: Box, z0: number, z1: number, deck: Part | null = slate) {
  frustum(slate, b, t, z0, z1, { top: deck, bottom: trim })
}
/** A low white balustrade around a deck. */
function rail(b: Box, z: number, h = 0.75, w = 0.25) {
  block(trim, R(b[0], b[1], b[2], b[2] + w, 0.04), z, z + h)
  block(trim, R(b[0], b[1], b[3] - w, b[3], 0.04), z, z + h)
  block(trim, R(b[0], b[0] + w, b[2], b[3], 0.04), z, z + h)
  block(trim, R(b[1] - w, b[1], b[2], b[3], 0.04), z, z + h)
}
/** A gabled dormer on a roof slope facing +y, its face at (x, y). */
function dormer(x: number, y: number, z: number, w = 1.5, h = 1.8) {
  block(trim, R(x - w / 2, x + w / 2, y - 2.2, y, 0.05), z - 1.5, z + h, { top: null })
  frustum(slate, [x - w / 2 - 0.15, x + w / 2 + 0.15, y - 2.2, y + 0.2], [x, x, y - 2.2, y + 0.2], z + h, z + h + 0.9, { ends: trim })
  const f = faces(R(x - w / 2, x + w / 2, y - 2.2, y))
  panel(win, f.n, rect(0.35, w - 0.35, 0.2, h - 0.3))
}
/** Window panels on a face, one per bay, between z0 and z1. */
function windows(f: Face, xs: number[], w: number, z0: number, z1: number, arched = false) {
  for (const s of xs) panel(win, f, arched ? roundArch(s - w / 2, s + w / 2, z0, z1 - w / 2) : rect(s - w / 2, s + w / 2, z0, z1))
}

// Central block: three bays, the portico before it, slate hip roof up to a
// railed deck, the clock tower on the deck and a chimney either side.
{
  const r = R(-9.4, 9.4, 30, 46.5, 0.15)
  const f = hall(r, MID_EAVE)
  windows(f.n, bays(f.n.L, 3, 2.6), 1.6, 8.9, 10.5)
  for (const s of [f.n.L / 2 - 3.6, f.n.L / 2, f.n.L / 2 + 3.6]) panel(door, f.n, roundArch(s - 0.95, s + 0.95, 0, 3.2))
  hip([-9.6, 9.6, 29.8, 46.9], [-5.0, 5.0, 34.2, 42.6], MID_EAVE + 0.6, 17.0)
  rail([-5.0, 5.0, 34.2, 42.6], 17.0)
  for (const x of [-4.4, 0, 4.4]) dormer(x, 45.0, 13.0)
  for (const x of [-6.6, 6.6]) {
    block(brick, R(x - 0.7, x + 0.7, 37.2, 38.4, 0.08), 12, 19.4, { top: null })
    slab(brick, R(x - 0.85, x + 0.85, 37.05, 38.55, 0.08), 19.4, 19.8)
  }
  // Clock tower: a white square stage with the clock, a cornice, an open
  // octagonal lantern and a small dome.
  const cy = 39.6
  block(trim, R(-2.2, 2.2, cy - 2.2, cy + 2.2, 0.12), 17.0, 21.0, { top: null })
  slab(trim, R(-2.55, 2.55, cy - 2.55, cy + 2.55, 0.15), 21.0, 21.5)
  const tf = faces(R(-2.2, 2.2, cy - 2.2, cy + 2.2))
  for (const side of [tf.n, tf.s, tf.e, tf.w]) panel(win, side, Array.from({ length: 10 }, (_, k) => [2.2 + 1.05 * Math.cos((k / 10) * 2 * Math.PI), 19.2 + 1.05 * Math.sin((k / 10) * 2 * Math.PI)] as XY))
  lathe(trim, 0, cy, [[1.55, 21.5], [1.55, 23.7], [1.8, 23.7], [1.8, 24.1]], 8, Math.PI / 8, true)
  for (let k = 0; k < 8; k++) {
    const a = Math.PI / 8 + (k + 0.5) * (Math.PI / 4), apo = 1.55 * Math.cos(Math.PI / 8)
    const side: Face = { o: [Math.cos(a) * apo - Math.sin(a) * 0.59, cy + Math.sin(a) * apo + Math.cos(a) * 0.59], u: [Math.sin(a), -Math.cos(a)], n: [Math.cos(a), Math.sin(a), 0], L: 1.18 }
    panel(win, side, roundArch(0.22, 0.96, 21.8, 23.1, 4))
  }
  lathe(slate, 0, cy, [[1.6, 24.1], [1.45, 24.8], [1.0, 25.4], [0.45, 25.75], [0.12, 25.9], [0.12, 26.6], [0, 27.2]], 12)
}

// Wings: two bays, a dormer each.
for (const m of [-1, 1]) {
  const r = m < 0 ? R(-15.2, -9.4, 30, 42.0, 0.1) : R(9.4, 15.2, 30, 42.0, 0.1)
  block(brick, r, 0, WING_EAVE, { top: null })
  slab(trim, R(r.x0, r.x1, r.y0 - 0.35, r.y1 + 0.35, 0.1), WING_EAVE, WING_EAVE + 0.5)
  const f = faces(r)
  windows(f.n, bays(f.n.L, 2, 0.8), 1.5, 1.0, 4.0)
  windows(f.n, bays(f.n.L, 2, 0.8), 1.3, 6.0, 8.6)
  const xc = (r.x0 + r.x1) / 2
  hip([r.x0 - 0.1, r.x1 + 0.1, 29.8, 42.4], [r.x0 + 2.5, r.x1 - 2.5, 36.1, 36.1], WING_EAVE + 0.5, 14.4, null)
  dormer(xc, 40.6, 12.1, 1.4, 1.6)
}

// End pavilions: their own hip roofs, railed decks and cupolas, and a white
// pedimented porch reaching outwards from each front corner.
for (const m of [-1, 1]) {
  const x0 = m < 0 ? -27.9 : 15.2, x1 = m < 0 ? -15.2 : 27.9, xc = (x0 + x1) / 2
  const r = R(x0, x1, 31, 47.0, 0.15)
  const f = hall(r, PAV_EAVE)
  windows(f.n, bays(f.n.L, 2, 1.6), 1.5, 1.0, 4.2)
  windows(f.n, bays(f.n.L, 2, 1.6), 1.6, 6.0, 9.2, true)
  const side = m < 0 ? f.w : f.e
  windows(side, bays(side.L, 4, 1.2), 1.4, 1.0, 4.2)
  windows(side, bays(side.L, 4, 1.2), 1.4, 6.0, 9.0)
  hip([x0 - 0.2, x1 + 0.2, 30.8, 47.2], [xc - 2.4, xc + 2.4, 36.6, 41.6], PAV_EAVE + 0.6, PAV_DECK)
  rail([xc - 2.4, xc + 2.4, 36.6, 41.6], PAV_DECK, 0.7, 0.22)
  dormer(xc, 45.6, 12.6, 1.4, 1.6)
  // Cupola: an octagonal white drum with arched openings, a slate dome.
  const cy = 39.1
  lathe(trim, xc, cy, [[1.25, PAV_DECK], [1.25, PAV_DECK + 2.0], [1.45, PAV_DECK + 2.0], [1.45, PAV_DECK + 2.35]], 8, Math.PI / 8, true)
  for (let k = 0; k < 8; k += 2) {
    const a = Math.PI / 8 + (k + 0.5) * (Math.PI / 4), apo = 1.25 * Math.cos(Math.PI / 8)
    const s: Face = { o: [xc + Math.cos(a) * apo - Math.sin(a) * 0.48, cy + Math.sin(a) * apo + Math.cos(a) * 0.48], u: [Math.sin(a), -Math.cos(a)], n: [Math.cos(a), Math.sin(a), 0], L: 0.96 }
    panel(win, s, roundArch(0.18, 0.78, PAV_DECK + 0.4, PAV_DECK + 1.5, 4))
  }
  lathe(slate, xc, cy, [[1.3, PAV_DECK + 2.35], [1.1, PAV_DECK + 3.0], [0.6, PAV_DECK + 3.45], [0.1, PAV_DECK + 3.6], [0.1, PAV_DECK + 4.4], [0, PAV_DECK + 5.0]], 12)
  // Porch: columns under a flat railed roof, a pediment over its inner half.
  const px0 = m < 0 ? -31.0 : 19.2, px1 = m < 0 ? -19.2 : 31.0
  const pin0 = m < 0 ? -25.0 : 19.2, pin1 = m < 0 ? -19.2 : 25.0
  slab(trim, R(px0, px1, 47.0, 51.4, 0.1), 4.4, 5.0)
  rail([m < 0 ? px0 : pin1, m < 0 ? pin0 : px1, 47.0, 51.4], 5.0, 0.6, 0.18)
  frustum(slate, [pin0, pin1, 47.0, 51.6], [(pin0 + pin1) / 2, (pin0 + pin1) / 2, 47.0, 51.6], 5.0, 6.9, { ends: trim })
  for (const x of [px0 + 0.4, pin0 + 0.4, (pin0 + pin1) / 2 - 1, (pin0 + pin1) / 2 + 1, pin1 - 0.4, px1 - 0.4, m < 0 ? (px0 + pin0) / 2 + 0.4 : (pin1 + px1) / 2 - 0.4])
    lathe(trim, x, 50.9, [[0.28, 0], [0.24, 4.4]], 8)
  lathe(trim, m < 0 ? px0 + 0.4 : px1 - 0.4, 47.6, [[0.28, 0], [0.24, 4.4]], 8)
}

// The portico: eight white columns (paired at the corners) under an
// entablature, a railed balcony on top.
{
  const y0 = 46.5, y1 = 53.7
  for (const x of [-7.5, -6.6, -4.6, -1.7, 1.7, 4.6, 6.6, 7.5]) lathe(trim, x, y1 - 0.5, [[0.42, 0], [0.42, 0.5], [0.36, 0.5], [0.31, 6.4], [0.4, 6.6]], 10)
  for (const x of [-7.5, 7.5]) lathe(trim, x, y0 + 0.9, [[0.36, 0.5], [0.31, 6.4]], 10)
  slab(trim, R(-8.4, 8.4, y0, y1, 0.1), 6.6, 7.8)
  rail([-8.3, 8.3, y0, y1 - 0.1], 7.8, 0.85, 0.22)
}

// ---------------------------------------------------------------------------

const parts = [
  { part: brick, material: finish('red-brick', 0xbf7d66) },
  { part: trim, material: PALETTE.trim },
  { part: slate, material: finish('slate', 0x8d98a4) },
  { part: win, material: PALETTE.window },
  { part: stone, material: PALETTE.stone },
  { part: door, material: PALETTE.entrance },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The American Adventure', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-american-adventure.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
