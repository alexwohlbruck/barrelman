/**
 * Coyote Joe's, Charlotte (country music hall and dance club, 4621 Wilkinson
 * Boulevard; about 3,200 capacity) — procedural, CC0-1.0.
 * bun scripts/landmarks/coyote-joes.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 26°: the
 * model's +y runs along the hall's long axis towards the main parking lot and
 * Wilkinson Boulevard, so the entrance end is the model's north face. The
 * anchor is the metre-based centroid of the OSM outline (way/849535273, no
 * height tag; about 35 m by 55 m plus a stepped west side and an 11 m porch
 * block on the north end), and the parts follow it.
 *
 * A big plain hall: a grey corrugated-steel shed with a flat-looking roof,
 * lower steel sheds stepping down along its west side, and at the entrance
 * end a shingled hip-roofed porch on posts over the doors. Its identity is
 * the neon on the north wall beside the porch: the yellow moon, the green
 * saguaro and the white howling coyote, drawn as flat shapes. The red neon
 * name beside them is lettering and is left out.
 *
 * Measured: the outline (OSM). Estimated: the heights (the hall's eaves
 * about 7 m above the entrance, about four times a person in the photo; the
 * sheds 4.5 m; the porch), the porch posts, the neon's size and place.
 * Inferred: the porch and the neon sit on the north end, from the photo
 * (sign wall to the left of a projecting shingled porch) read against the
 * NAIP aerial. Invented: the south, east and west walls, which no photo
 * shows, are plain steel; the lean-to sheds on the east side outside the
 * OSM outline are not modelled. No licensed exterior photo was found.
 *
 * The DEM falls 0.9 m from the entrance end to the south-west corner, so
 * y = 0 is the south ground and the entrance sits at G above it.
 *
 * References: USGS NAIP aerial (public domain) for the footprint, the pale
 * roof and the porch block. Look-only: the exterior photo on Charlotte's Got
 * A Lot (charlottesgotalot.com, the venue's listing, 2017) and WBTV news
 * stills of the neon (2023, 2025). Mapillary has no view of the building.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, type Swatch } from './palette'

// ---------------------------------------------------------------------------
// A small kit for low buildings: an outline with chamfered parapets, a flat
// roof inside them, and flat or boxed features set on a wall's face.

type XY = [number, number]
const UP: V3 = [0, 0, 1], DN: V3 = [0, 0, -1]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }

/** A triangle with a flat normal, flipped if needed so it faces along `n`. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3) {
  const u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], v = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
  const cr = [u[1] * v[2] - u[2] * v[1], u[2] * v[0] - u[0] * v[2], u[0] * v[1] - u[1] * v[0]]
  if (cr[0] * n[0] + cr[1] * n[1] + cr[2] * n[2] < 0) [b, c] = [c, b]
  p.tri(a, b, c, undefined, undefined, undefined, [n, n, n])
}
const quad = (p: Part, a: V3, b: V3, c: V3, d: V3, n: V3) => { tri(p, a, b, c, n); tri(p, a, c, d, n) }

const signedArea = (P: XY[]) => P.reduce((s, a, i) => { const b = P[(i + 1) % P.length]; return s + a[0] * b[1] - b[0] * a[1] }, 0) / 2
/** Ear clipping for a simple anticlockwise polygon (the outlines are not all convex). */
function earcut(P: XY[]): [number, number, number][] {
  const idx = P.map((_, i) => i), out: [number, number, number][] = []
  const inTri = (p: XY, a: XY, b: XY, c: XY) => {
    const s = (q: XY, r: XY) => (r[0] - q[0]) * (p[1] - q[1]) - (r[1] - q[1]) * (p[0] - q[0])
    return s(a, b) > 1e-9 && s(b, c) > 1e-9 && s(c, a) > 1e-9
  }
  for (let guard = 0; idx.length > 3 && guard < 500; guard++) {
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = P[i0], b = P[i1], c = P[i2]
      if ((b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]) <= 1e-9) continue
      if (idx.some(j => j !== i0 && j !== i1 && j !== i2 && inTri(P[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); break
    }
  }
  out.push([idx[0], idx[1], idx[2]])
  return out
}
function capPoly(p: Part, P: XY[], z: number, n: V3 = UP) {
  for (const [a, b, c] of earcut(P)) tri(p, [P[a][0], P[a][1], z], [P[b][0], P[b][1], z], [P[c][0], P[c][1], z], n)
}

/**
 * A walled outline (anticlockwise from above). Each edge's wall rises from
 * y = 0 in runs of parapet heights with a chamfered top; the roof sits at
 * `zRoof` inside the parapets.
 */
class Shell {
  N: number
  IN: XY[]
  constructor(public P: XY[], public wall: Part, public roofPart: Part, public zRoof: number, public T = 0.35, public B = 0.12) {
    if (signedArea(P) < 0) throw new Error('outline must be anticlockwise')
    this.N = P.length
    this.IN = this.inset(T)
  }
  edge(i: number) {
    const a = this.P[i], b = this.P[(i + 1) % this.N], L = Math.hypot(b[0] - a[0], b[1] - a[1])
    const d: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L]
    return { a, b, L, d, n: [d[1], -d[0], 0] as V3 }
  }
  inset(t: number): XY[] {
    return this.P.map((_, i) => {
      const e0 = this.edge((i + this.N - 1) % this.N), e1 = this.edge(i)
      const bis: XY = [-(e0.n[0] + e1.n[0]), -(e0.n[1] + e1.n[1])]
      const c = 1 + e0.n[0] * e1.n[0] + e0.n[1] * e1.n[1]
      return [this.P[i][0] + bis[0] * t / c, this.P[i][1] + bis[1] * t / c] as XY
    })
  }
  /** A point s metres along edge i, pushed out by `out` (negative goes in). */
  at(i: number, s: number, out = 0): XY {
    const e = this.edge(i)
    return [e.a[0] + e.d[0] * s + e.n[0] * out, e.a[1] + e.d[1] * s + e.n[1] * out]
  }
  innerAt(i: number, s: number): XY {
    const e = this.edge(i)
    if (s <= 1e-6) return this.IN[i]
    if (s >= e.L - 1e-6) return this.IN[(i + 1) % this.N]
    return this.at(i, s, -this.T)
  }
  pt(i: number, s: number, z: number, out = 0): V3 { const p = this.at(i, s, out); return [p[0], p[1], z] }
  /** Edge i's wall, as runs of [s0, s1, parapet top]. */
  walls(i: number, runs: [number, number, number][]) {
    const e = this.edge(i), n = e.n, T = this.T, B = this.B, w = this.wall
    runs.forEach(([s0, s1, z1], k) => {
      const A = this.at(i, s0), Bp = this.at(i, s1), Ai = this.innerAt(i, s0), Bi = this.innerAt(i, s1)
      const f = B / T
      const Ab: XY = [A[0] + (Ai[0] - A[0]) * f, A[1] + (Ai[1] - A[1]) * f]
      const Bb: XY = [Bp[0] + (Bi[0] - Bp[0]) * f, Bp[1] + (Bi[1] - Bp[1]) * f]
      const zc = z1 - B
      quad(w, [A[0], A[1], 0], [Bp[0], Bp[1], 0], [Bp[0], Bp[1], zc], [A[0], A[1], zc], n)
      quad(w, [A[0], A[1], zc], [Bp[0], Bp[1], zc], [Bb[0], Bb[1], z1], [Ab[0], Ab[1], z1], unit([n[0], n[1], 1]))
      quad(w, [Ab[0], Ab[1], z1], [Bb[0], Bb[1], z1], [Bi[0], Bi[1], z1], [Ai[0], Ai[1], z1], UP)
      quad(w, [Ai[0], Ai[1], this.zRoof], [Bi[0], Bi[1], this.zRoof], [Bi[0], Bi[1], z1], [Ai[0], Ai[1], z1], [-n[0], -n[1], 0])
      const cap = (S: XY, Sb: XY, Si: XY, zlo: number, dir: number) => {
        if (zlo >= z1 - 1e-6) return
        const cn: V3 = [e.d[0] * dir, e.d[1] * dir, 0]
        quad(w, [S[0], S[1], zlo], [Si[0], Si[1], zlo], [Si[0], Si[1], z1], [S[0], S[1], zc], cn)
        tri(w, [S[0], S[1], zc], [Si[0], Si[1], z1], [Sb[0], Sb[1], z1], cn)
      }
      if (k > 0) cap(A, Ab, Ai, Math.max(runs[k - 1][2], this.zRoof), -1)
      if (k < runs.length - 1) cap(Bp, Bb, Bi, Math.max(runs[k + 1][2], this.zRoof), 1)
    })
  }
  roof() { capPoly(this.roofPart, this.IN, this.zRoof) }
  /** A flat panel on edge i's face. */
  panel(p: Part, i: number, s0: number, s1: number, z0: number, z1: number, out = 0.04) {
    quad(p, this.pt(i, s0, z0, out), this.pt(i, s1, z0, out), this.pt(i, s1, z1, out), this.pt(i, s0, z1, out), this.edge(i).n)
  }
  /** A flat convex shape, in (s, z), on edge i's face. */
  shape(p: Part, i: number, pts: XY[], out = 0.05) {
    const n = this.edge(i).n
    for (let k = 1; k < pts.length - 1; k++) tri(p, this.pt(i, ...pts[0], out), this.pt(i, ...pts[k], out), this.pt(i, ...pts[k + 1], out), n)
  }
  /** Any simple shape in (s, z), flat on edge i's face. */
  shapeAny(p: Part, i: number, pts: XY[], out = 0.05) {
    const q = signedArea(pts) < 0 ? [...pts].reverse() : pts, n = this.edge(i).n
    for (const [a, b, c] of earcut(q)) tri(p, this.pt(i, ...q[a], out), this.pt(i, ...q[b], out), this.pt(i, ...q[c], out), n)
  }
  /** A flat ring in (s, z) on edge i's face. */
  ring(p: Part, i: number, c: XY, r0: number, r1: number, seg = 16, out = 0.05) {
    const n = this.edge(i).n
    for (let k = 0; k < seg; k++) {
      const a0 = 2 * Math.PI * k / seg, a1 = 2 * Math.PI * (k + 1) / seg
      const P0 = (r: number, a: number): V3 => this.pt(i, c[0] + r * Math.cos(a), c[1] + r * Math.sin(a), out)
      quad(p, P0(r0, a0), P0(r1, a0), P0(r1, a1), P0(r0, a1), n)
    }
  }
  /** A convex shape in (s, z) extruded out from the face between o0 and o1. */
  solid(p: Part, i: number, pts: XY[], o0: number, o1: number, side: Part = p) {
    const e = this.edge(i), n = e.n
    this.shape(p, i, pts, o1)
    const cs = pts.reduce((s, q) => s + q[0], 0) / pts.length, cz = pts.reduce((s, q) => s + q[1], 0) / pts.length
    pts.forEach((a, k) => {
      const b = pts[(k + 1) % pts.length]
      const ds = b[0] - a[0], dz = b[1] - a[1]
      let ns = dz, nz = -ds
      if (ns * ((a[0] + b[0]) / 2 - cs) + nz * ((a[1] + b[1]) / 2 - cz) < 0) { ns = -ns; nz = -nz }
      const sn = unit([e.d[0] * ns, e.d[1] * ns, nz])
      quad(side, this.pt(i, a[0], a[1], o0), this.pt(i, b[0], b[1], o0), this.pt(i, b[0], b[1], o1), this.pt(i, a[0], a[1], o1), sn)
    })
    if (o0 > 0.001) { const back: V3 = [-n[0], -n[1], 0]; for (let k = 1; k < pts.length - 1; k++) tri(side, this.pt(i, ...pts[0], o0), this.pt(i, ...pts[k], o0), this.pt(i, ...pts[k + 1], o0), back) }
  }
  /** A box on edge i's face: s0..s1 along, z0..z1 up, o0..o1 out. */
  box(p: Part, i: number, s0: number, s1: number, z0: number, z1: number, o0: number, o1: number, face: Part = p) {
    this.solid(p, i, [[s0, z0], [s1, z0], [s1, z1], [s0, z1]], o0, o1)
    if (face !== p) this.panel(face, i, s0 + 0.08, s1 - 0.08, z0 + 0.08, z1 - 0.08, o1 + 0.02)
  }
}

const circle = (c: XY, r: number, a0 = 0, a1 = 2 * Math.PI, seg = 14): XY[] =>
  Array.from({ length: seg + 1 }, (_, k) => { const a = a0 + (a1 - a0) * k / seg; return [c[0] + r * Math.cos(a), c[1] + r * Math.sin(a)] as XY })
/** A rounded-end "pill" in (s, z). */
const pill = (s0: number, s1: number, z0: number, z1: number, seg = 8): XY[] => {
  const r = (z1 - z0) / 2, zc = (z0 + z1) / 2
  return [...circle([s1 - r, zc], r, -Math.PI / 2, Math.PI / 2, seg), ...circle([s0 + r, zc], r, Math.PI / 2, Math.PI * 1.5, seg)]
}

async function finishModel(id: string, name: string, parts: { part: Part; material: Swatch }[], extras: Record<string, unknown>) {
  const live = parts.filter(p => p.part.triangles > 0)
  const triangles = live.reduce((s, { part }) => s + part.triangles, 0)
  console.log(live.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
  if (live.length > 6) throw new Error(`Too many materials: ${live.length}`)
  if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
  const glb = writeGlb(name, live, { license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor', ...extras })
  if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
  const out = new URL(`../../landmarks/models/${id}.glb`, import.meta.url).pathname
  await Bun.write(out, glb)
  console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
}

const steel = new Part(), roof = new Part(), shingle = new Part()
const lit = new Part(), yellow = new Part(), green = new Part()

const G = 0.9                        // entrance-end ground above the south ground

// --- The hall: the OSM outline less the west sheds and the porch block,
// anticlockwise from the south-west corner. 2 is the north wall east of the
// porch, which carries the neon.
const HALL: XY[] = [[-16.3, -29.6], [18.7, -29.65], [19.2, 25.8], [-0.2, 25.8], [-0.2, 27.6], [-13.0, 27.6], [-13.0, 17.4], [-16.3, 17.4]]
const ZH = 8.1
const H = new Shell(HALL, steel, roof, ZH - 0.3, 0.3, 0.15)
HALL.forEach((_, i) => H.walls(i, [[0, H.edge(i).L, ZH]]))
H.roof()

// --- The lower steel sheds along the west side (their east wall is the
// hall's).
const SHED: XY[] = [[-23.8, -12.5], [-16.3, -12.5], [-16.3, 17.4], [-18.1, 17.4], [-18.1, 8.7], [-23.8, 8.7]]
const ZS = 4.9
const W = new Shell(SHED, steel, roof, ZS - 0.25, 0.3, 0.15)
SHED.forEach((_, i) => { if (i !== 1) W.walls(i, [[0, W.edge(i).L, ZS]]) })
W.roof()

// --- The porch: a shingled hip roof on posts over the entrance, filling the
// OSM block north of the hall.
const PX0 = -0.2, PX1 = 10.8, PY0 = 25.8, PY1 = 36.65
const ZE = G + 3.4, ZR = G + 5.6, OV = 0.45
function post(x: number, y: number) {
  const h = 0.2, q: XY[] = [[x - h, y - h], [x + h, y - h], [x + h, y + h], [x - h, y + h]]
  q.forEach((a, k) => {
    const b = q[(k + 1) % 4], n = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    quad(shingle, [a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], ZE], [a[0], a[1], ZE], n)
  })
}
for (const x of [PX0 + 0.5, (PX0 + PX1) / 2, PX1 - 0.5]) post(x, PY1 - 0.5)
for (const y of [PY0 + 3.8, PY0 + 7.4]) { post(PX0 + 0.5, y); post(PX1 - 0.5, y) }
{
  // Eave ring (open to the hall's wall on the south) and the hip.
  const e: XY[] = [[PX0 - OV, PY0], [PX1 + OV, PY0], [PX1 + OV, PY1 + OV], [PX0 - OV, PY1 + OV]]
  const cx = (PX0 + PX1) / 2, ridge: XY[] = [[cx - 0.01, PY0], [cx + 0.01, PY0], [cx, PY1 + OV - (PX1 - PX0) / 2 - OV]]
  const top = (p: XY): V3 => [p[0], p[1], ZR], eave = (p: XY, dz = 0): V3 => [p[0], p[1], ZE + dz]
  const F = 0.3                                     // fascia depth
  const slope = (a: V3, b: V3, c: V3, d?: V3) => {
    const u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], v = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
    let n = unit([u[1] * v[2] - u[2] * v[1], u[2] * v[0] - u[0] * v[2], u[0] * v[1] - u[1] * v[0]])
    if (n[2] < 0) n = [-n[0], -n[1], -n[2]]
    if (d) quad(shingle, a, b, c, d, n); else tri(shingle, a, b, c, n)
  }
  const R = top(ridge[2])
  // West, north and east slopes; the south edge runs into the hall wall.
  slope(eave(e[0]), eave(e[3]), R, top([cx, PY0]))
  slope(eave(e[3]), eave(e[2]), R)
  slope(eave(e[2]), eave(e[1]), top([cx, PY0]), R)
  for (const [a, b, n] of [[e[0], e[3], [-1, 0, 0]], [e[3], e[2], [0, 1, 0]], [e[2], e[1], [1, 0, 0]]] as [XY, XY, V3][]) {
    quad(shingle, eave(a, -F), eave(b, -F), eave(b), eave(a), n)
  }
  // The soffit, seen from below.
  quad(shingle, eave(e[0], -F), eave(e[1], -F), eave(e[2], -F), eave(e[3], -F), DN)
}
// The entrance doors under the porch, on the hall's north wall (edge 2 runs
// west from the hall's north-east corner).
H.panel(lit, 2, 9.4, 14.6, G, G + 2.7)

// --- The neon, on the north wall east of the porch: a yellow moon ring, the
// green saguaro over it and the white coyote howling beside them. Drawn at
// about 1.5 times the real size so it survives at phone size.
{
  const i = 2, z0 = G + 2.4, k = 1.5
  // Authored in (u, v) metres at real size, u from the wall's east end.
  const tf = ([u, v]: XY): XY => [1.0 + u * k, z0 + v * k]
  H.ring(yellow, i, tf([1.5, 1.9]), 1.0 * k, 1.3 * k, 16, 0.05)
  const cactus: XY[] = [
    [1.05, 0], [1.5, 0], [1.5, 1.0], [2.05, 1.0], [2.05, 2.0], [1.75, 2.0], [1.75, 1.3], [1.5, 1.3],
    [1.5, 3.1], [1.05, 3.1], [1.05, 1.6], [0.8, 1.6], [0.8, 2.3], [0.5, 2.3], [0.5, 1.3], [1.05, 1.3],
  ]
  H.shapeAny(green, i, cactus.map(tf), 0.08)
  const coyote: XY[] = [[2.6, 0], [3.9, 0], [3.8, 0.9], [3.45, 1.55], [3.55, 2.35], [3.25, 2.15], [3.05, 2.5], [3.0, 1.85], [2.75, 1.2]]
  H.shapeAny(lit, i, coyote.map(tf), 0.08)
}

// Colours from the photo and aerial: the grey-green corrugated steel and the
// brown shingles, the neon's yellow and green; the coyote's white neon and
// the doors glow at night.
await finishModel('coyote-joes', "Coyote Joe's", [
  { part: steel, material: finish('cj-steel', 0xb4b9b5) },
  { part: roof, material: PALETTE.roof },
  { part: shingle, material: finish('cj-shingle', 0x857a70) },
  { part: lit, material: PALETTE.entrance },
  { part: yellow, material: finish('cj-neon-yellow', 0xe9c85c) },
  { part: green, material: finish('cj-neon-green', 0x72b56a) },
], { bearing: 26, osm: 'way/849535273', height: ZH })
