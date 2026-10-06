/**
 * The Milestone Club, Charlotte (punk and underground rock club since 1969,
 * 3400 Tuckaseegee Road, Enderly Park) — procedural, CC0-1.0.
 * bun scripts/landmarks/milestone-club.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 28°: the
 * model's +y runs back from Tuckaseegee Road, so the street front is the
 * model's south face and the gravel lot on Karendale Avenue lies off its east
 * face. The anchor is the metre-based centroid of the OSM outline
 * (way/867520324, no height tag; 6.5 m front, 13 m deep), and the walls
 * follow that outline.
 *
 * A small club is its front and its paint: a narrow two-storey brick block
 * painted white, a flat roof behind a plain parapet with a shallow cornice
 * band across the front, dark green shutters on every upper window, the
 * ground floor of the street front clad in white board-and-batten with a
 * green door, a chimney on the roof, and the green blade sign hanging off the
 * east wall by the front corner (drawn as a plain board, no lettering). The
 * big black "MILESTONE" painted on the east wall is lettering and is left out.
 *
 * Measured: the outline (OSM). Estimated: the height (two storeys, about
 * 7.3 m above the street, from the Mapillary views), window positions and
 * sizes, the chimney's place. Invented: the west and rear walls, which no
 * photo shows, are drawn plain. The low green clapboard annex behind the
 * club is outside the OSM outline and is not modelled.
 *
 * The DEM falls 0.9 m from the street front to the rear, so y = 0 is the rear
 * ground and the front sidewalk sits at G above it.
 *
 * References (visual only): Mapillary images 864821379052648,
 * 906090680989943 and 1138812007799594 by whitecotton (Oct 2024, CC BY-SA
 * 4.0); USGS NAIP aerial (public domain) for the flat white roof.
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

const white = new Part(), green = new Part(), clad = new Part()
const win = new Part(), roof = new Part()

// The OSM outline in the model frame, anticlockwise from the front's east
// corner: 0 the east wall, 1 the rear, 2 the west wall, 3 the street front.
const P: XY[] = [[3.22, -6.48], [3.33, 6.48], [-3.18, 6.53], [-3.37, -6.48]]
const G = 0.9                       // street-front ground above the rear ground
const TOP = G + 7.3                 // parapet top
const S = new Shell(P, white, roof, TOP - 0.45)
const L = [0, 1, 2, 3].map(i => S.edge(i).L)
for (let i = 0; i < 4; i++) S.walls(i, [[0, L[i], TOP]])
S.roof()
/** Ground height along the east and west walls, falling to the rear. */
const gEast = (s: number) => G * (1 - s / L[0])
const gWest = (s: number) => G * (s / L[2])

// The cornice: a shallow bevelled band across the top of the street front.
S.solid(white, 3, [[-0.0, TOP - 0.95], [L[3], TOP - 0.95], [L[3], TOP - 0.55], [0, TOP - 0.55]], 0, 0.12)

/** A window with a green shutter either side. */
function shuttered(i: number, sc: number, z0: number) {
  const w = 0.95, h = 1.55, sw = 0.42
  S.panel(win, i, sc - w / 2, sc + w / 2, z0, z0 + h)
  S.box(green, i, sc - w / 2 - sw - 0.05, sc - w / 2 - 0.05, z0 - 0.05, z0 + h + 0.05, 0, 0.07)
  S.box(green, i, sc + w / 2 + 0.05, sc + w / 2 + sw + 0.05, z0 - 0.05, z0 + h + 0.05, 0, 0.07)
}

// --- The street front: two shuttered windows upstairs; downstairs the white
// board-and-batten cladding with the green door in it.
shuttered(3, 2.0, G + 4.05)
shuttered(3, 4.4, G + 4.05)
S.box(clad, 3, 0.2, L[3] - 0.2, G, G + 3.1, 0, 0.1)
S.box(clad, 3, 0.1, L[3] - 0.1, G + 3.1, G + 3.3, 0, 0.22)        // its top trim
S.panel(green, 3, 3.95, 4.95, G, G + 2.2, 0.13)

// --- The east wall: three shuttered windows upstairs over the front two
// thirds (the rear third is blank), the blade sign beside the first, hanging out over the sidewalk by the front corner.
for (const sc of [2.1, 5.3, 8.5]) shuttered(0, sc, gEast(sc) + 4.05)
S.box(green, 0, 0.7, 0.82, G + 3.3, G + 4.45, 0, 1.15)

// --- The chimney, on the roof towards the front.
{
  const c: XY = [1.6, -2.4], h = 0.36, z0 = TOP - 0.45, z1 = TOP + 1.0
  const q: XY[] = [[c[0] - h, c[1] - h], [c[0] + h, c[1] - h], [c[0] + h, c[1] + h], [c[0] - h, c[1] + h]]
  q.forEach((a, k) => {
    const b = q[(k + 1) % 4], n = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    quad(white, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], n)
  })
  capPoly(white, q, z1)
}

// Colours from the daylight photos: the white paint (a little warm), a
// dark shutter green pulled light, the cream cladding.
await finishModel('milestone-club', 'The Milestone Club', [
  { part: white, material: finish('milestone-white', 0xf4f1ea) },
  { part: clad, material: PALETTE.trim },
  { part: green, material: finish('milestone-green', 0x4f8a64) },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
], { bearing: 28, osm: 'way/867520324', height: TOP + 1.0 })
