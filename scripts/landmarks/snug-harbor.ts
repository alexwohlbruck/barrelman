/**
 * Snug Harbor, Plaza Midwood, Charlotte (pirate-themed rock club and bar
 * since 2007, 1228 Gordon Street) — procedural, CC0-1.0.
 * bun scripts/landmarks/snug-harbor.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 94°: the
 * model's +y runs east, back from the front yard on Gordon Street, so the
 * street front (which faces west) is the model's south face. The anchor is
 * the metre-based centroid of the OSM outline (way/323193812, no height
 * tag; 16 m front, 11.4 m deep), and the walls follow it.
 *
 * A small club is its front: a one-storey brick box whose parapet steps up
 * in the middle of the front, the big lit name board in that raised section
 * (drawn as a plain gold board with a lit face, no lettering), a corrugated
 * tin shed awning under it over the door and the lit window, and a brick
 * chimney behind. The picket fence and picnic tables of the front yard, and
 * the back patio and stage, are outside the building and left to the map.
 *
 * Measured: the outline (OSM). Estimated: the heights (one storey, about
 * 4.7 m, the raised middle 5.9 m), the openings and the awning, all from one
 * night photo of the front. Invented: the side and rear walls, which no photo
 * shows, are drawn as plain brick. No licensed exterior photo was found, so
 * the front rests on a look-only reference.
 *
 * The DEM is nearly flat here (0.4 m over the footprint), so the front ground
 * sits at G above y = 0.
 *
 * References: USGS NAIP aerial (public domain) for the footprint and the dark
 * flat roof. Look-only: Drea Photo Artistry's night photo of the front yard
 * in CLTure's "Snug Harbor is set to reopen" (June 2021,
 * clture.org/snug-harbor-reopening). Mapillary has no view of the front.
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

const brick = new Part(), tin = new Part(), sign = new Part()
const win = new Part(), lit = new Part(), roof = new Part()

// The OSM outline in the model frame, anticlockwise: 0 the north wall (model
// west), 1 the Gordon Street front, 2 the south wall, 3 the rear.
const P: XY[] = [[-8.05, 5.66], [-8.0, -5.73], [8.06, -5.64], [8.01, 5.71]]
const G = 0.15
const Z = 4.7, ZM = 5.9
const S = new Shell(P, brick, roof, 4.3)
const L = [0, 1, 2, 3].map(i => S.edge(i).L)
S.walls(0, [[0, L[0], Z]])
S.walls(1, [[0, 5.3, Z], [5.3, 10.7, ZM], [10.7, L[1], Z]])
S.walls(2, [[0, L[2], Z]])
S.walls(3, [[0, L[3], Z]])
S.roof()

/** A sloped tin awning: from the wall at zt out by `dep`, down to zf. */
function awning(i: number, s0: number, s1: number, zt: number, zf: number, dep: number) {
  const n = S.edge(i).n, d = S.edge(i).d, pt = S.pt.bind(S)
  const a = pt(i, s0, zt, 0.02), b = pt(i, s1, zt, 0.02), c = pt(i, s1, zf, dep), e = pt(i, s0, zf, dep)
  const up = unit([n[0] * (zt - zf), n[1] * (zt - zf), dep])
  quad(tin, a, b, c, e, up)
  quad(tin, a, b, c, e, [-up[0], -up[1], -up[2]])
  const v = 0.18
  quad(tin, pt(i, s0, zf - v, dep), pt(i, s1, zf - v, dep), c, e, n)
  for (const [s, dir] of [[s0, -1], [s1, 1]] as [number, number][]) {
    const sn: V3 = [d[0] * dir, d[1] * dir, 0]
    tri(tin, pt(i, s, zt, 0.02), pt(i, s, zf, dep), pt(i, s, zf - v, dep), sn)
    tri(tin, pt(i, s, zt, 0.02), pt(i, s, zf - v, dep), pt(i, s, zf - v, 0.02), sn)
  }
}

// --- The front: the name board high in the raised middle, the tin awning
// under it, the door and a lit window beneath; a small window to the left.
S.box(sign, 1, 5.8, 10.2, 3.95, 5.7, 0, 0.18, lit)
awning(1, 4.6, 11.4, 3.6, 3.05, 1.6)
S.panel(lit, 1, 6.3, 7.4, G, G + 2.2)
S.panel(win, 1, 8.0, 10.4, G + 0.9, G + 2.3)
S.panel(win, 1, 1.2, 2.6, G + 1.0, G + 2.2)

// --- The brick chimney behind the front, to the right of the sign.
{
  const c: XY = [5.0, -2.2], h = 0.4, z0 = 4.3, z1 = 6.6
  const q: XY[] = [[c[0] - h, c[1] - h], [c[0] + h, c[1] - h], [c[0] + h, c[1] + h], [c[0] - h, c[1] + h]]
  q.forEach((a, k) => {
    const b = q[(k + 1) % 4], n = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    quad(brick, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], n)
  })
  capPoly(brick, q, z1)
}

// Colours from the photo and the aerial: red-brown brick pulled light, the
// grey corrugated tin, the gold name board.
await finishModel('snug-harbor', 'Snug Harbor', [
  { part: brick, material: finish('snug-brick', 0xa9654f) },
  { part: tin, material: finish('snug-tin', 0xb9bdbf) },
  { part: sign, material: finish('snug-gold', 0xd9ad55) },
  { part: lit, material: PALETTE.entrance },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
], { bearing: 94, osm: 'way/323193812', height: 6.6 })
