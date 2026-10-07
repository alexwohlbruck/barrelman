/**
 * Hôtel du Canada and the Canadian Rockies, Canada pavilion, EPCOT World
 * Showcase — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-canada.ts
 *
 * Map frame: x along the château from its north-west end to its south-east
 * end (the end seen across the lagoon), y from the street (south-west) to
 * the garden and rocks (north-east), z up, metres. Placed at bearing 34°,
 * the axis of the château's OSM outline (way/295396527), whose long walls
 * run at 124°. The anchor is near the château's centre.
 *
 * What it is: the pavilion's château, after Ottawa's Château Laurier, in
 * buff stone. A tall rectangular block has two corbelled string courses and
 * a steep green copper roof that rises to a pyramid at each end, with a saddle
 * between them. Stone gablets with finials stand along its eaves. A round
 * tower with a slender copper spire stands at the street front's north-west
 * end. Le Cellier steakhouse is in the low block at its garden foot. North of
 * it is the pavilion's pink granite rockwork, which wraps the round
 * Circle-Vision theatre.
 *
 * Evidence
 * - OSM (measured): the château, way/295396527 (24 × 12 m, untagged,
 *   building:levels=2); Le Cellier, way/295396523, along its north-east side;
 *   the rockwork and theatre, way/295396535 ("Canada Far and Wide"). No
 *   heights are tagged.
 * - USGS NAIP 2022 orthophoto (public domain): the theatre's round white roof
 *   (about 21 m across) inside the rockwork, and the rocks heaped on its south
 *   and east sides. The château's roof does not resolve in it.
 * - Published (Wikipedia): modelled on the Château Laurier; forced
 *   perspective makes three storeys look like six; Le Cellier is a cellar
 *   under the château.
 * - Photos (Wikimedia Commons): from the garden to the east (Canada Pavilion
 *   (43268891311).jpg, HarshLight, CC BY 2.0), from the street steps
 *   (Chateau Laurier Replica at Canada Pavillion - panoramio.jpg, Eric
 *   Marshall, CC BY 3.0), the north-west end (Canada Pavilion
 *   (41459068520).jpg, HarshLight, CC BY 2.0), and across the lagoon (Canada
 *   Pavilion (41459026620).jpg, HarshLight, CC BY 2.0).
 * - Estimated from the photos: eave 17.6 m above Le Cellier's level, with
 *   Le Cellier's door for scale; the street-front corbel course at 12 m; roof
 *   peaks 28 m (the end is 1.6 widths tall to the eave and 2.5 to the peak
 *   from the lagoon); the tower's spire 32.6 m; the street-level terrace
 *   4.5 m above Le Cellier; Le Cellier's block 5.6 m; the rock crags 9–15 m
 *   and the theatre roof 9.5 m.
 * - Invented: the gap between the château and Le Cellier's outline, which
 *   the low block fills, and the crags' shapes and placings, which are only
 *   the rockwork's general heaping.
 * - Simplified: the bay window and small domed oriel, the lower wing beyond
 *   the north-west end (outside this outline) and the corbel teeth.
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
/** A flat disc closing a lathe. */
function disc(p: Part, cx: number, cy: number, r: number, z: number, seg: number, up: boolean, phase = 0) {
  const pts: V3[] = Array.from({ length: seg }, (_, k) => [cx + r * Math.cos(phase + (k / seg) * 2 * Math.PI), cy + r * Math.sin(phase + (k / seg) * 2 * Math.PI), z])
  face(p, pts, [0, 0, up ? 1 : -1])
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
// The château. x runs along the building from its north-west end to its
// south-east end (the end seen from the lagoon), y from the street (south-
// west) to the garden and Le Cellier (north-east). z = 0 is Le Cellier's
// level, the lowest ground the building touches.

const stone = new Part(), copper = new Part(), trim = new Part()
const win = new Part(), door = new Part(), rock = new Part()

const X0 = -11.8, X1 = 12.0, Y0 = -8.9, Y1 = 2.6
const EAVE = 17.6, COURSE = 12.0, STREET = 4.5
const PEAK = 28.0, RIDGE = 25.0

// Main block: tall walls, two corbelled courses.
block(stone, R(X0, X1, Y0, Y1, 0.2), 0, EAVE - 0.6, { top: null })
slab(trim, R(X0 - 0.35, X1 + 0.35, Y0 - 0.35, Y1 + 0.35, 0.25), COURSE - 0.6, COURSE)
slab(trim, R(X0 - 0.45, X1 + 0.45, Y0 - 0.45, Y1 + 0.45, 0.3), EAVE - 0.6, EAVE)
{
  const f = faces(R(X0, X1, Y0, Y1))
  const rows: XY[] = [[STREET + 1.6, STREET + 3.4], [8.9, 10.6], [12.8, 14.3], [15.0, 16.4]]
  for (const side of [f.s, f.n]) {
    for (const [z0, z1] of rows) {
      if (side === f.n && z0 < 7) continue // Le Cellier's block covers it
      for (const s of bays(side.L, 7, 1.2)) panel(win, side, rect(s - 0.5, s + 0.5, z0, z1))
    }
  }
  for (const side of [f.e, f.w])
    for (const [z0, z1] of rows) for (const s of bays(side.L, 3, 1.6)) panel(win, side, rect(s - 0.55, s + 0.55, z0, z1))
  // The doors onto the street-level terrace at the south-east end.
  panel(door, f.e, roundArch(5.0, 7.0, STREET, STREET + 2.6))
}

// Roofs: a steep copper pyramid over each end and a lower hip between, with
// stone gablets along the eaves.
const half = (Y1 - Y0) / 2, yc = (Y0 + Y1) / 2
frustum(copper, [X0 - 0.2, X1 + 0.2, Y0 - 0.2, Y1 + 0.2], [X0 + half + 1.5, X1 - half - 1.5, yc, yc], EAVE, RIDGE, { bottom: trim })
for (const xc of [X0 + half, X1 - half])
  frustum(copper, [xc - half - 0.2, xc + half + 0.2, Y0 - 0.2, Y1 + 0.2], [xc, xc, yc, yc], EAVE, PEAK)

/** A stone gablet standing on the eave: a pointed stone face, its copper roof behind. */
function gablet(f: Face, s: number, w = 1.6) {
  const base = EAVE - 0.2, top = EAVE + 2.4
  const a = at(f, s - w / 2, base), b = at(f, s + w / 2, base), c = at(f, s, top)
  const back: V3 = [-f.n[0] * 1.6, -f.n[1] * 1.6, 0]
  face(stone, [a, b, c], f.n)
  face(stone, [a, b, add(b, back), add(a, back)], [0, 0, -1])
  face(copper, [b, add(b, back), add(c, back), c], add([0, 0, 1], [f.u[0], f.u[1], 0]))
  face(copper, [a, c, add(c, back), add(a, back)], add([0, 0, 1], [-f.u[0], -f.u[1], 0]))
  panel(win, f, [[s - 0.35, base + 0.25], [s + 0.35, base + 0.25], [s + 0.35, base + 1.2], [s, base + 1.55], [s - 0.35, base + 1.2]], 0.04)
  // Finial.
  lathe(trim, c[0] + f.n[0] * 0.05, c[1] + f.n[1] * 0.05, [[0.22, top - 0.2], [0, top + 0.9]], 6)
}
{
  const f = faces(R(X0 - 0.45, X1 + 0.45, Y0 - 0.45, Y1 + 0.45))
  for (const side of [f.s, f.n]) for (const s of bays(side.L, 6, 1.8)) gablet(side, s)
  for (const side of [f.e, f.w]) for (const s of bays(side.L, 2, 2.4)) gablet(side, s, 1.4)
}

// The round tower on the street front: corbelled near the top, a slender
// copper spire.
{
  const cx = X0 + 2.3, cy = Y0 - 0.9, r = 2.5
  lathe(stone, cx, cy, [[r, 0], [r, EAVE + 0.4]], 16)
  lathe(trim, cx, cy, [[r, EAVE + 0.4], [r + 0.45, EAVE + 1.0], [r + 0.45, EAVE + 1.5]], 16)
  lathe(stone, cx, cy, [[r + 0.3, EAVE + 1.5], [r + 0.3, EAVE + 3.3]], 16)
  disc(trim, cx, cy, r + 0.45, EAVE + 1.5, 16, false)
  lathe(trim, cx, cy, [[r + 0.45, EAVE + 3.3], [r + 0.45, EAVE + 3.7]], 16)
  disc(trim, cx, cy, r + 0.45, EAVE + 3.3, 16, false)
  lathe(copper, cx, cy, [[r + 0.5, EAVE + 3.7], [0.08, EAVE + 14.5], [0, EAVE + 15.0]], 16)
  // Three columns of windows round its outer side.
  for (let k = 0; k < 3; k++) {
    const a = -Math.PI / 2 + (k - 1) * 0.7
    const n: V3 = [Math.cos(a), Math.sin(a), 0], u: XY = [-Math.sin(a), Math.cos(a)]
    const fc: Face = { o: [cx + n[0] * r - u[0] * 0.5, cy + n[1] * r - u[1] * 0.5], u, n, L: 1.0 }
    for (const [z0, z1] of [[9.0, 10.6], [12.9, 14.2], [EAVE + 1.9, EAVE + 2.9]] as XY[]) panel(win, fc, rect(0.15, 0.85, z0, z1), 0.06)
  }
}

// Le Cellier: the low block at the garden foot of the north-east front,
// carrying a terrace, its arched entrance facing the garden.
const cellier: XY[] = [[-8.0, 2.0], [12.6, 2.0], [12.7, 15.7], [12.8, 16.4], [8.9, 16.6], [-0.7, 17.2], [-0.9, 15.7], [-4.2, 14.4], [-8.0, 12.9]]
prism(stone, cellier, 0, 5.0, { top: null })
prism(trim, cellier, 5.0, 5.6, { top: stone })
{
  const f: Face = { o: [12.7, 4.0], u: [0, 1], n: [1, 0, 0], L: 11.7 }
  for (const s of [3.0, 6.0, 9.0]) panel(door, f, roundArch(s - 1.0, s + 1.0, 0, 3.0), 0.04)
}

// The rockwork to the north (OSM way/295396535, "Canada Far and Wide"): a
// massif of pink granite following the outline, rounded crags heaped along
// its garden and lagoon side (south and east, as in the aerial and from the
// lagoon), and the flat roof of the Circle-Vision theatre it wraps, which
// shows from above.
const rockOutline: XY[] = [
  [-18.5, 49.8], [-14.4, 46.5], [-10.0, 42.5], [-9.0, 41.4], [-6.3, 38.2], [-5.1, 36.7], [-1.8, 36.7], [-0.9, 34.8],
  [-0.7, 33.0], [-2.9, 32.0], [-4.2, 28.8], [-6.2, 21.8], [-9.4, 16.6], [-15.5, 13.1], [-21.0, 11.8], [-22.1, 10.3],
  [-25.0, 8.9], [-27.3, 9.3], [-30.8, 9.9], [-33.4, 9.1], [-36.3, 5.4], [-35.5, 2.9], [-32.2, 2.2], [-31.0, 0.3],
  [-31.9, -2.3], [-32.7, -5.0], [-35.6, -6.9], [-39.6, -5.6], [-45.4, 0.2], [-44.8, 12.9], [-38.0, 20.7], [-40.7, 26.4],
  [-40.6, 31.7], [-39.1, 37.6], [-37.8, 40.0], [-34.4, 44.1], [-30.9, 46.2], [-25.0, 47.9], [-21.2, 47.8], [-20.1, 49.1],
]
{
  const ring = area(rockOutline) < 0 ? [...rockOutline].reverse() : rockOutline
  const cx = ring.reduce((a, p) => a + p[0], 0) / ring.length, cy = ring.reduce((a, p) => a + p[1], 0) / ring.length
  const levels: [number, number][] = [[1, 0], [0.92, 5.0], [0.78, 8.0]]
  const at = (k: number, i: number): V3 => { const [f, z] = levels[k]; const p = ring[i]; return [cx + (p[0] - cx) * f, cy + (p[1] - cy) * f, z] }
  for (let k = 0; k < levels.length - 1; k++)
    for (let i = 0; i < ring.length; i++) {
      const j = (i + 1) % ring.length
      const e = [ring[j][0] - ring[i][0], ring[j][1] - ring[i][1]]
      face(rock, [at(k, i), at(k, j), at(k + 1, j), at(k + 1, i)], [e[1], -e[0], 0.5])
    }
  const top = ring.map((_, i) => at(levels.length - 1, i))
  for (const [a, b, c] of earcut(top.map((v) => [v[0], v[1]] as XY))) face(rock, [top[a], top[b], top[c]], [0, 0, 1])
  // Circle-Vision theatre roof.
  lathe(rock, -27.2, 29.5, [[10.5, 6.0], [10.5, 9.5]], 16)
  disc(stone, -27.2, 29.5, 10.5, 9.5, 16, true)
  // Crags: irregular five-sided peaks.
  const crags: [number, number, number, number, number][] = [
    [-4.2, 33.8, 2.8, 10.5, 0.2], [-7.4, 29.0, 3.6, 12.5, 1.0], [-9.8, 23.6, 4.4, 15.0, 0.4], [-13.6, 18.8, 4.6, 14.0, 1.3],
    [-19.0, 16.0, 4.2, 12.5, 0.6], [-24.0, 14.2, 3.8, 11.0, 1.5], [-29.2, 13.4, 3.8, 12.0, 0.1], [-34.2, 8.4, 3.6, 10.5, 0.8],
    [-38.5, 3.2, 4.4, 12.5, 1.2], [-41.4, 9.4, 3.6, 10.0, 0.5], [-40.4, 17.5, 3.4, 9.0, 1.4], [-12.8, 40.2, 3.6, 10.0, 0.7],
  ]
  for (const [x, y, r, h, ph] of crags)
    lathe(rock, x, y, [[r, 0], [r * 0.97, h * 0.45], [r * 0.78, h * 0.78], [r * 0.45, h * 0.95], [0, h]], 7, ph)
}

// ---------------------------------------------------------------------------

const parts = [
  { part: stone, material: finish('chateau-stone', 0xe2c9a8) },
  { part: copper, material: PALETTE.copper },
  { part: trim, material: PALETTE.trim },
  { part: win, material: PALETTE.window },
  { part: door, material: PALETTE.entrance },
  { part: rock, material: finish('canada-granite', 0xcfa392) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Hôtel du Canada', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 34, elevation: 0,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-canada.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
