/**
 * Bechtler Museum of Modern Art and the Firebird — procedural, CC0-1.0.
 * bun scripts/landmarks/bechtler-museum.ts
 *
 * Map frame: x across the building towards South Tryon Street (south-east),
 * y along Tryon (north-east), z up, metres. Placed at bearing 50°, the axis of
 * the OSM outline (way/131139728, 34.2 × 27.6 m). The anchor is the outline's
 * centroid, so the box is centred on the origin.
 *
 * Mario Botta's museum is a terracotta box whose top two floors run over the
 * whole footprint, while the ground floors are cut away at the south corner
 * (Tryon and Levine Avenue of the Arts). The overhang there is held by a
 * single cigar-shaped brick column; behind it sit the glass entrance atrium, a
 * low louvred block on the Levine side and a taller louvred block on Tryon.
 *
 * OSM says 30 m, but the photos put the parapet at about 25 m and the soffit
 * at 60% of it, so the model uses those.
 *
 * The Firebird (Niki de Saint Phalle, OSM node/5016558822) stands on the plaza
 * off the south corner: a mirror-mosaic figure on an arch of two legs, arms
 * raised, a gold sun on its chest and a gold crown. It is a few smooth masses.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const brick = new Part(), glass = new Part(), roof = new Part()
const gold = new Part(), blue = new Part(), red = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle with per-corner normals, its winding fixed to agree with them. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const face = cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [c[0] - a[0], c[1] - a[1], c[2] - a[2]])
  if (len(face) < 1e-9) return
  const avg = add(add(n[0], n[1]), n[2])
  if (dot(face, avg) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}

// ---------------------------------------------------------------------------
// Masses: rectangles with 45° chamfered vertical corners and bevelled edges.

type Rect = { x0: number; x1: number; y0: number; y1: number; c: number }
function ring(r: Rect, d: number, z: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const c = Math.max(0.02, r.c + d * 0.414)
  const pts: V3[] = [[x0 + c, y0, z], [x1 - c, y0, z], [x1, y0 + c, z], [x1, y1 - c, z],
    [x1 - c, y1, z], [x0 + c, y1, z], [x0, y1 - c, z], [x0, y0 + c, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
/** One band of a swept profile, from (d0, z0) to (d1, z1) with (out, up) normals. */
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
const OUT: XY = [1, 0], UP: XY = [0, 1], DOWN: XY = [0, -1], S = Math.SQRT1_2
/** A solid block: bevel under, plain face, bevel over, then the caps. */
function block(p: Part, r: Rect, z0: number, z1: number, o: { bb?: number; bt?: number; top?: Part | null; bottom?: Part | null } = {}) {
  const bb = o.bb ?? 0, bt = o.bt ?? 0
  if (bb) band(p, r, -bb, z0, 0, z0 + bb, [S, -S], OUT)
  band(p, r, 0, z0 + bb, 0, z1 - bt, OUT, OUT)
  if (bt) band(p, r, 0, z1 - bt, -bt, z1, OUT, UP)
  if (o.top !== null) lid(o.top ?? p, r, -bt, z1, true)
  if (o.bottom) lid(o.bottom, r, -bb, z0, false)
}

// ---------------------------------------------------------------------------
// Walls with recessed openings: the face is tiled in brick around them, and
// each opening gets brick reveals and a back panel in its own material.

type Opening = { s0: number; s1: number; z0: number; z1: number; depth: number; back: Part }
function wall(a: XY, b: XY, z0: number, z1: number, openings: Opening[]) {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const u: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L]
  const n: V3 = [u[1], -u[0], 0] // outward for a counter-clockwise footprint
  const v = (s: number, z: number, d = 0): V3 => [a[0] + u[0] * s - n[0] * d, a[1] + u[1] * s - n[1] * d, z]
  const ss = [...new Set([0, L, ...openings.flatMap((o) => [o.s0, o.s1])])].sort((p, q) => p - q)
  const zs = [...new Set([z0, z1, ...openings.flatMap((o) => [o.z0, o.z1])])].sort((p, q) => p - q)
  for (let i = 0; i < ss.length - 1; i++)
    for (let k = 0; k < zs.length - 1; k++) {
      const sc = (ss[i] + ss[i + 1]) / 2, zc = (zs[k] + zs[k + 1]) / 2
      if (openings.some((o) => sc > o.s0 && sc < o.s1 && zc > o.z0 && zc < o.z1)) continue
      quad(brick, v(ss[i], zs[k]), v(ss[i + 1], zs[k]), v(ss[i + 1], zs[k + 1]), v(ss[i], zs[k + 1]), n)
    }
  const uu: V3 = [u[0], u[1], 0]
  for (const o of openings) {
    const D = o.depth
    quad(o.back, v(o.s0, o.z0, D), v(o.s1, o.z0, D), v(o.s1, o.z1, D), v(o.s0, o.z1, D), n)
    quad(brick, v(o.s0, o.z0), v(o.s1, o.z0), v(o.s1, o.z0, D), v(o.s0, o.z0, D), [0, 0, 1])
    quad(brick, v(o.s0, o.z1), v(o.s1, o.z1), v(o.s1, o.z1, D), v(o.s0, o.z1, D), [0, 0, -1])
    if (o.s0 > 0) quad(brick, v(o.s0, o.z0), v(o.s0, o.z1), v(o.s0, o.z1, D), v(o.s0, o.z0, D), uu)
    if (o.s1 < L) quad(brick, v(o.s1, o.z0), v(o.s1, o.z1), v(o.s1, o.z1, D), v(o.s1, o.z0, D), [-u[0], -u[1], 0])
  }
}
/** Broad louvre bands: the facade's horizontal brick slats, a few recesses deep enough to shade. */
const louvres = (L: number, zs: number[], h: number, margin = 0.6): Opening[] =>
  zs.map((z) => ({ s0: margin, s1: L - margin, z0: z, z1: z + h, depth: 0.4, back: brick }))

// ---------------------------------------------------------------------------
// The museum.

const X0 = -17.1, X1 = 17.1, Y0 = -13.8, Y1 = 13.8
const H = 25          // parapet
const SOF = 15        // underside of the cantilevered upper floors
const XA = 1.5        // where the full-height back of the building ends

// The upper box over the whole footprint: pale roof membrane on top, brick
// soffit below, bevelled all round so its edges catch the light.
block(brick, { x0: X0, x1: X1, y0: Y0, y1: Y1, c: 0.5 }, SOF, H, { bb: 0.35, bt: 0.45, top: roof, bottom: brick })
// The skylight over the central atrium.
block(glass, { x0: -7, x1: 1, y0: -3.5, y1: 3.5, c: 0.3 }, H - 0.1, H + 0.7, { bt: 0.3 })

// The back of the building, solid brick from the ground to the box: the plain
// full-height wall on the Levine side (photo 02).
block(brick, { x0: X0, x1: XA, y0: Y0, y1: Y1, c: 0.5 }, 0, SOF)

// The louvred block on Tryon, north of the entrance: a shop window at the
// foot, louvres, a gallery window under the soffit (photo 03). Set back from
// the box's faces, so the overhang reads on Tryon and on the north-east side.
{
  const x0 = XA, x1 = 15.9, y0 = 3.0, y1 = 12.3
  wall([x1, y0], [x1, y1], 0, SOF, [
    { s0: 0.6, s1: y1 - y0 - 0.6, z0: 0.4, z1: 4.0, depth: 0.05, back: glass },
    ...louvres(y1 - y0, [5.4, 7.7, 10.0], 1.0),
    { s0: 0.6, s1: y1 - y0 - 0.6, z0: 12.0, z1: 14.3, depth: 0.05, back: glass },
  ])
  wall([x1, y1], [x0, y1], 0, SOF, louvres(x1 - x0, [5.4, 7.7, 10.0], 1.0))
  wall([x0, y0], [x1, y0], 0, SOF, [])
}

// The entrance atrium: a glass box filling the inside corner of the cut-away,
// set back from the Tryon block.
block(glass, { x0: XA - 0.5, x1: 13.2, y0: -2.6, y1: 3.0, c: 0.02 }, 0, SOF, { top: null })

// The low louvred block on the Levine side, with a recessed window above it
// up to the soffit (photos 01 and 02).
{
  const x0 = XA - 0.5, x1 = 7.6, y0 = Y0, y1 = -8.0, top = 7.6
  wall([x0, y0], [x1, y0], 0, top, louvres(x1 - x0, [2.6, 4.9], 1.0, 0.5))
  wall([x1, y0], [x1, y1], 0, top, louvres(y1 - y0, [2.6, 4.9], 1.0, 0.5))
  wall([x1, y1], [x0, y1], 0, top, [])
  lid(brick, { x0, x1, y0, y1, c: 0.02 }, 0, top, true)
  block(glass, { x0, x1: 6.4, y0: -12.5, y1: -8.0, c: 0.02 }, top, SOF, { top: null })
}

// The column: a single brick spindle, swollen in the middle and narrow at
// both ends, flaring into a round capital under the soffit (photo 03).
function lathe(p: Part, cx: number, cy: number, prof: XY[], seg: number) {
  const nrm = prof.map((_, i) => {
    const [r0, z0] = prof[Math.max(0, i - 1)], [r1, z1] = prof[Math.min(prof.length - 1, i + 1)]
    return unit([z1 - z0, 0, -(r1 - r0)])
  })
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * 2 * Math.PI, b = ((k + 1) / seg) * 2 * Math.PI
    const P = ([r, z]: XY, t: number): V3 => [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
    const N = (n: V3, t: number): V3 => [n[0] * Math.cos(t), n[0] * Math.sin(t), n[2]]
    for (let i = 0; i < prof.length - 1; i++)
      quad(p, P(prof[i], a), P(prof[i], b), P(prof[i + 1], b), P(prof[i + 1], a), [N(nrm[i], a), N(nrm[i], b), N(nrm[i + 1], b), N(nrm[i + 1], a)])
  }
}
lathe(brick, 10.6, -6.2, [[0.6, 0], [0.85, 1.2], [1.15, 3.5], [1.3, 6.2], [1.2, 9], [0.92, 11.6], [0.66, 13.6], [0.75, 14.4], [1.5, 15.0]], 16)

// ---------------------------------------------------------------------------
// The Firebird, in its own frame: u across the arch, f forward, z up. It faces
// the Tryon/Levine corner, so the arch is seen face-on from the intersection
// (photo 01) and edge-on from Levine Avenue (photo 02).

const BIRD: XY = [13.8, -22.2]          // OSM node/5016558822 in the model frame
const bearingLocal = (140 * Math.PI) / 180
const F: V3 = [Math.sin(bearingLocal), Math.cos(bearingLocal), 0]
const U: V3 = [-F[1], F[0], 0]
const bw = (u: number, f: number, z: number): V3 => [BIRD[0] + U[0] * u + F[0] * f, BIRD[1] + U[1] * u + F[1] * f, z]
const bn = (u: number, f: number, z: number): V3 => unit([U[0] * u + F[0] * f, U[1] * u + F[1] * f, z])

/** An ellipsoid in the bird frame, smooth-shaded. */
function blob(p: Part, c: V3, r: V3, seg = 12, rings = 8) {
  const pt = (i: number, j: number) => {
    const th = (i / seg) * 2 * Math.PI, ph = -Math.PI / 2 + (j / rings) * Math.PI
    const e: V3 = [Math.cos(ph) * Math.cos(th), Math.cos(ph) * Math.sin(th), Math.sin(ph)]
    return { p: bw(c[0] + r[0] * e[0], c[1] + r[1] * e[1], c[2] + r[2] * e[2]), n: bn(e[0] / r[0], e[1] / r[1], e[2] / r[2]) }
  }
  for (let i = 0; i < seg; i++)
    for (let j = 0; j < rings; j++) {
      const a = pt(i, j), b = pt(i + 1, j), cc = pt(i + 1, j + 1), d = pt(i, j + 1)
      quad(p, a.p, b.p, cc.p, d.p, [a.n, b.n, cc.n, d.n])
    }
}

/**
 * A round tube along a path in the bird's u–z plane, `k` times deeper front
 * to back than across. Ends left open sit in the ground or inside a blob.
 */
function tube(p: Part, path: [number, number, number][], k = 1, seg = 10) {
  const sections = path.map(([u, z, r], i) => {
    const [u0, z0] = path[Math.max(0, i - 1)], [u1, z1] = path[Math.min(path.length - 1, i + 1)]
    const t = unit([u1 - u0, 0, z1 - z0])
    const side: XY = [-t[2], t[0]] // in-plane normal (u, z)
    return Array.from({ length: seg }, (_, s) => {
      const a = (s / seg) * 2 * Math.PI, ca = Math.cos(a), sa = Math.sin(a)
      return {
        p: bw(u + side[0] * r * ca, r * k * sa, z + side[1] * r * ca),
        n: bn(side[0] * ca / r, sa / (r * k), side[1] * ca / r),
      }
    })
  })
  for (let i = 0; i < sections.length - 1; i++)
    for (let s = 0; s < seg; s++) {
      const t = (s + 1) % seg
      const A = sections[i][s], B = sections[i][t], C = sections[i + 1][t], D = sections[i + 1][s]
      quad(p, A.p, B.p, C.p, D.p, [A.n, B.n, C.n, D.n])
    }
}

// The arch: two legs flaring at the feet, joined by a round top.
{
  const R = 1.05, spring = 1.25
  const path: [number, number, number][] = [[-R, 0, 0.5], [-R, 0.55, 0.42], [-R, spring, 0.37]]
  for (let a = 1; a < 12; a++) {
    const t = Math.PI - (a / 12) * Math.PI
    path.push([R * Math.cos(t), spring + R * Math.sin(t), 0.37])
  }
  path.push([R, spring, 0.37], [R, 0.55, 0.42], [R, 0, 0.5])
  tube(glass, path, 1.25)
}
// Body, chest sun, head.
blob(glass, [0, 0, 3.05], [0.85, 0.62, 0.92], 14, 9)
blob(gold, [0, 0.5, 3.0], [0.42, 0.16, 0.42], 12, 6)
blob(glass, [0, 0.04, 4.28], [0.42, 0.4, 0.46])
blob(red, [0, 0.42, 4.18], [0.13, 0.2, 0.12], 8, 5)
// The crown: five gold rays fanned over the head like a sun.
for (const deg of [-64, -32, 0, 32, 64]) {
  const a = (deg * Math.PI) / 180, d: XY = [Math.sin(a), Math.cos(a)]
  const c: V3 = [d[0] * 0.62, -0.05, 4.32 + d[1] * 0.62]
  // A ray is an ellipsoid stretched along its own direction: build it upright
  // and rotate in the u–z plane.
  const seg = 8, rings = 5, rL = 0.3, rW = 0.11, rD = 0.09
  const pt = (i: number, j: number) => {
    const th = (i / seg) * 2 * Math.PI, ph = -Math.PI / 2 + (j / rings) * Math.PI
    const e: V3 = [Math.cos(ph) * Math.cos(th), Math.cos(ph) * Math.sin(th), Math.sin(ph)]
    const lx = rW * e[0], ly = rD * e[1], lz = rL * e[2]
    const nx = e[0] / rW, ny = e[1] / rD, nz = e[2] / rL
    const rot = (x: number, z: number): XY => [x * d[1] + z * d[0], -x * d[0] + z * d[1]]
    const [pu, pz] = rot(lx, lz), [nu, nz2] = rot(nx, nz)
    return { p: bw(c[0] + pu, c[1] + ly, c[2] + pz), n: bn(nu, ny, nz2) }
  }
  for (let i = 0; i < seg; i++)
    for (let j = 0; j < rings; j++) {
      const A = pt(i, j), B = pt(i + 1, j), C = pt(i + 1, j + 1), D = pt(i, j + 1)
      quad(gold, A.p, B.p, C.p, D.p, [A.n, B.n, C.n, D.n])
    }
}
// Arms raised in a W, blue, each ending in a broad red hand.
for (const side of [-1, 1]) {
  tube(blue, [[side * 0.55, 3.35, 0.27], [side * 1.0, 3.5, 0.25], [side * 1.4, 3.75, 0.23], [side * 1.62, 4.2, 0.22], [side * 1.68, 4.6, 0.21]], 1.1, 8)
  blob(red, [side * 1.72, 0, 4.92], [0.3, 0.16, 0.4], 10, 6)
}

// ---------------------------------------------------------------------------

// The shared palette (landmarks/STYLE.md): the terracotta box is the
// palette's own `terracotta`, the roof membrane `roof`. All the glazing is
// `glass`: the museum's glass is its atrium and skylight, which the style
// keeps unlit, and the two small windows on Tryon share it, which also lets
// the Firebird's mirror mosaic read as sky-blue glass and leaves the bird's
// gold, blue and red their own materials within six. Those stay saturated:
// the bird is small sculpture, and its colours are what it is.
const parts = [
  { part: brick, material: PALETTE.terracotta },
  { part: glass, material: PALETTE.glass },
  { part: roof, material: PALETTE.roof },
  { part: gold, material: finish('firebird-gold', 0xdcaa2e, 0.5) },
  { part: blue, material: finish('firebird-blue', 0x3a74c4) },
  { part: red, material: finish('firebird-red', 0xd2463a) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Bechtler Museum of Modern Art', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 50, elevation: 0, height: H,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/bechtler-museum.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
