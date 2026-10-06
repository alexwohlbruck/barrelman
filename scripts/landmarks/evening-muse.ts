/**
 * The Evening Muse, NoDa, Charlotte (listening room in a one-storey brick
 * corner shop at 3227 N Davidson St) — procedural, CC0-1.0.
 * bun scripts/landmarks/evening-muse.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 322°: the
 * model's +y runs back from North Davidson Street, so the Davidson front is
 * the model's south face and East 36th Street runs along its east face. The
 * anchor is the centroid of the OSM outline (way/1192512105, height 5), and
 * the walls follow that outline exactly, its clipped corner included: the
 * main entrance sits on that corner face, turned to the intersection.
 *
 * A small venue is its front and its paint: red-painted brick with a stepped
 * parapet (raised end piers and a raised block where the Davidson front turns
 * the corner), lavender metal awnings over every opening, purple kickplates
 * under the windows, the purple name board over the doors (drawn as a plain
 * board, no lettering; its face is `entrance` so it lights at night), and on
 * the 36th Street wall the purple-and-yellow mural: a downward triangle with a
 * crescent moon, a horn player's trumpet with its bell, and his figure. The
 * rear and the party wall are plain painted brick.
 *
 * The DEM is nearly flat here (0.3 m fall to the rear), so the front ground
 * sits at G above y = 0.
 *
 * References (visual only): City Dweller 2, "The Evening Muse Mid-April 2024"
 * and "The Rat's Nest Mid-April 2024" (Commons, CC BY-SA 4.0); James
 * Willamor, "NoDa, Charlotte, NC, USA - panoramio (2)" (Commons, CC BY-SA 3.0);
 * the venue's own exterior photos on eveningmuse.com (mural and queue views);
 * USGS NAIP aerial (public domain) for the flat roof.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const brick = new Part(), purple = new Part(), yellow = new Part()
const win = new Part(), lit = new Part(), roof = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const UP: V3 = [0, 0, 1], DN: V3 = [0, 0, -1]

/** A triangle with a flat normal, flipped if needed so it faces along `n`. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3) {
  const u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], v = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
  const cr = [u[1] * v[2] - u[2] * v[1], u[2] * v[0] - u[0] * v[2], u[0] * v[1] - u[1] * v[0]]
  if (cr[0] * n[0] + cr[1] * n[1] + cr[2] * n[2] < 0) [b, c] = [c, b]
  p.tri(a, b, c, undefined, undefined, undefined, [n, n, n])
}
const quad = (p: Part, a: V3, b: V3, c: V3, d: V3, n: V3) => { tri(p, a, b, c, n); tri(p, a, c, d, n) }

// ---------------------------------------------------------------------------
// The OSM outline in the model frame, anticlockwise from the party wall's
// front corner: the Davidson front, the clipped corner, 36th Street, the rear,
// and the party wall with Oldnews Vintage.
const P: XY[] = [[-4.53, -7.63], [0.91, -7.60], [5.00, -4.61], [4.85, 7.05], [-4.72, 7.00]]
const N = P.length
const T = 0.35                // parapet thickness
const B = 0.12                // parapet chamfer
const Z_ROOF = 4.4
const G = 0.25                // front sidewalk above the lowest ground

const edge = (i: number) => {
  const a = P[i], b = P[(i + 1) % N], L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const d: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L]
  return { a, b, L, d, n: [d[1], -d[0], 0] as V3 }
}
/** The outline's corners moved inward by t, mitred. */
function inset(t: number): XY[] {
  return P.map((_, i) => {
    const e0 = edge((i + N - 1) % N), e1 = edge(i)
    const bis: XY = [-(e0.n[0] + e1.n[0]), -(e0.n[1] + e1.n[1])]
    // Scale so the point sits t from both edges.
    const c = 1 + e0.n[0] * e1.n[0] + e0.n[1] * e1.n[1]
    return [P[i][0] + bis[0] * t / c, P[i][1] + bis[1] * t / c] as XY
  })
}
const IN = inset(T)
/** A point s metres along edge i, pushed out by `out` (negative goes in). */
function at(i: number, s: number, out = 0): XY {
  const e = edge(i)
  return [e.a[0] + e.d[0] * s + e.n[0] * out, e.a[1] + e.d[1] * s + e.n[1] * out]
}
const innerAt = (i: number, s: number): XY => {
  const e = edge(i)
  if (s <= 1e-6) return IN[i]
  if (s >= e.L - 1e-6) return IN[(i + 1) % N]
  return at(i, s, -T)
}

// ---------------------------------------------------------------------------
// Walls: each edge is split into runs, each run its own parapet height, with
// a chamfered top. Steps between runs get end caps.

type Run = [number, number, number] // s0, s1, top
function walls(i: number, runs: Run[]) {
  const e = edge(i), n = e.n
  runs.forEach(([s0, s1, z1], k) => {
    const A = at(i, s0), Bp = at(i, s1), Ai = innerAt(i, s0), Bi = innerAt(i, s1)
    const f = B / T
    const Ab: XY = [A[0] + (Ai[0] - A[0]) * f, A[1] + (Ai[1] - A[1]) * f]
    const Bb: XY = [Bp[0] + (Bi[0] - Bp[0]) * f, Bp[1] + (Bi[1] - Bp[1]) * f]
    const zc = z1 - B
    quad(brick, [A[0], A[1], 0], [Bp[0], Bp[1], 0], [Bp[0], Bp[1], zc], [A[0], A[1], zc], n)
    quad(brick, [A[0], A[1], zc], [Bp[0], Bp[1], zc], [Bb[0], Bb[1], z1], [Ab[0], Ab[1], z1], unit([n[0], n[1], 1]))
    quad(brick, [Ab[0], Ab[1], z1], [Bb[0], Bb[1], z1], [Bi[0], Bi[1], z1], [Ai[0], Ai[1], z1], UP)
    quad(brick, [Ai[0], Ai[1], Z_ROOF], [Bi[0], Bi[1], Z_ROOF], [Bi[0], Bi[1], z1], [Ai[0], Ai[1], z1], [-n[0], -n[1], 0])
    // Steps to a lower neighbouring run on this edge.
    const cap = (S: XY, Sb: XY, Si: XY, zlo: number, dir: number) => {
      if (zlo >= z1 - 1e-6) return
      const cn: V3 = [e.d[0] * dir, e.d[1] * dir, 0]
      quad(brick, [S[0], S[1], zlo], [Si[0], Si[1], zlo], [Si[0], Si[1], z1], [S[0], S[1], zc], cn)
      tri(brick, [S[0], S[1], zc], [Si[0], Si[1], z1], [Sb[0], Sb[1], z1], cn)
    }
    if (k > 0) cap(A, Ab, Ai, Math.max(runs[k - 1][2], Z_ROOF), -1)
    if (k < runs.length - 1) cap(Bp, Bb, Bi, Math.max(runs[k + 1][2], Z_ROOF), 1)
  })
}

const L = [0, 1, 2, 3, 4].map(i => edge(i).L)
const Z = 5.0                 // the main parapet, OSM's height
const ZP = 5.45               // raised piers and the corner blocks
walls(0, [[0, 0.6, ZP], [0.6, L[0] - 1.05, Z], [L[0] - 1.05, L[0], ZP]])
walls(1, [[0, 0.45, ZP], [0.45, L[1] - 1.15, Z + 0.1], [L[1] - 1.15, L[1], ZP + 0.1]])
walls(2, [[0, 0.6, ZP + 0.1], [0.6, 3.6, Z + 0.2], [3.6, L[2] - 0.6, 4.85], [L[2] - 0.6, L[2], 5.2]])
walls(3, [[0, 0.6, 5.2], [0.6, L[3], 4.85]])
walls(4, [[0, L[4] - 0.6, 4.85], [L[4] - 0.6, L[4], ZP]])

// The flat roof inside the parapets (the outline is convex).
for (let i = 1; i < N - 1; i++) {
  tri(roof, [IN[0][0], IN[0][1], Z_ROOF], [IN[i][0], IN[i][1], Z_ROOF], [IN[i + 1][0], IN[i + 1][1], Z_ROOF], UP)
}

// ---------------------------------------------------------------------------
// Facade helpers, in (s along the edge, z) on an edge's outer face.

const pt = (i: number, s: number, z: number, out: number): V3 => { const p = at(i, s, out); return [p[0], p[1], z] }
function panel(p: Part, i: number, s0: number, s1: number, z0: number, z1: number, out = 0.04) {
  quad(p, pt(i, s0, z0, out), pt(i, s1, z0, out), pt(i, s1, z1, out), pt(i, s0, z1, out), edge(i).n)
}
/** A flat convex shape on an edge's face. */
function shape(p: Part, i: number, pts: XY[], out = 0.05) {
  const n = edge(i).n
  for (let k = 1; k < pts.length - 1; k++) tri(p, pt(i, ...pts[0], out), pt(i, ...pts[k], out), pt(i, ...pts[k + 1], out), n)
}
const circle = (c: XY, r: number, a0 = 0, a1 = 2 * Math.PI, seg = 14): XY[] =>
  Array.from({ length: seg + 1 }, (_, k) => { const a = a0 + (a1 - a0) * k / seg; return [c[0] + r * Math.cos(a), c[1] + r * Math.sin(a)] as XY })
/** A window with a semicircular head. */
function arched(p: Part, i: number, s0: number, s1: number, z0: number, zs: number) {
  const r = (s1 - s0) / 2
  shape(p, i, [[s0, z0], [s1, z0], ...circle([s0 + r, zs], r, 0, Math.PI, 8)])
}
/** A sloped metal awning: from the wall at zt out by `dep`, down to zf. */
function awning(i: number, s0: number, s1: number, zt: number, zf: number, dep: number) {
  const n = edge(i).n
  const a = pt(i, s0, zt, 0.02), b = pt(i, s1, zt, 0.02), c = pt(i, s1, zf, dep), d = pt(i, s0, zf, dep)
  const up = unit([n[0] * (zt - zf), n[1] * (zt - zf), dep])
  quad(purple, a, b, c, d, up)
  quad(purple, a, b, c, d, [-up[0], -up[1], -up[2]])
  // A short valance along the front edge, and the two side cheeks.
  const c2 = pt(i, s1, zf - 0.22, dep), d2 = pt(i, s0, zf - 0.22, dep)
  quad(purple, d2, c2, c, d, n)
  const e = edge(i), side = (s: number, dir: number) => {
    const sn: V3 = [e.d[0] * dir, e.d[1] * dir, 0]
    tri(purple, pt(i, s, zt, 0.02), pt(i, s, zf, dep), pt(i, s, zf - 0.22, dep), sn)
    tri(purple, pt(i, s, zt, 0.02), pt(i, s, zf - 0.22, dep), pt(i, s, zf - 0.22, 0.02), sn)
  }
  side(s0, -1); side(s1, 1)
}

// --- The Davidson front: a poster-covered side door and a poster window,
// each under its own small awning.
panel(win, 0, 0.75, 1.45, G, G + 2.2)
panel(win, 0, 2.15, 4.15, G + 0.85, G + 2.45)
panel(purple, 0, 2.15, 4.15, G + 0.15, G + 0.75)
awning(0, 0.1, 1.95, 3.3, 2.75, 0.85)
awning(0, 2.0, 4.35, 3.3, 2.75, 0.85)

// --- The corner face: the main entrance. Shop windows over purple
// kickplates either side of the double doors, one long awning, the name board.
{
  const l = L[1]
  panel(win, 1, 0.3, 1.65, G + 0.8, G + 2.3)
  panel(purple, 1, 0.3, 1.65, G + 0.1, G + 0.7)
  panel(lit, 1, 1.8, 3.1, G, G + 2.35)
  panel(win, 1, 3.25, l - 0.4, G + 0.8, G + 2.3)
  panel(purple, 1, 3.25, l - 0.4, G + 0.1, G + 0.7)
  awning(1, 0.15, l - 0.2, 3.45, 2.6, 1.05)
  // The name board: a purple box, its face lit.
  const s0 = 1.1, s1 = 3.75, z0 = 3.62, z1 = 4.2, o = 0.14, n = edge(1).n, d = edge(1).d
  quad(purple, pt(1, s0, z0, o), pt(1, s1, z0, o), pt(1, s1, z1, o), pt(1, s0, z1, o), n)
  quad(purple, pt(1, s0, z1, 0), pt(1, s1, z1, 0), pt(1, s1, z1, o), pt(1, s0, z1, o), UP)
  quad(purple, pt(1, s0, z0, 0), pt(1, s1, z0, 0), pt(1, s1, z0, o), pt(1, s0, z0, o), DN)
  quad(purple, pt(1, s0, z0, 0), pt(1, s0, z1, 0), pt(1, s0, z1, o), pt(1, s0, z0, o), [-d[0], -d[1], 0])
  quad(purple, pt(1, s1, z0, 0), pt(1, s1, z1, 0), pt(1, s1, z1, o), pt(1, s1, z0, o), [d[0], d[1], 0])
  panel(lit, 1, s0 + 0.55, s1 - 0.12, z0 + 0.12, z1 - 0.12, o + 0.02)
  // The crescent at the board's left end, as on the real sign.
  panel(yellow, 1, s0 + 0.12, s0 + 0.42, z0 + 0.12, z1 - 0.12, o + 0.02)
}

// --- The 36th Street wall: two small arched windows high up, and the mural,
// which fills the front two thirds of the wall from the sidewalk to the
// parapet.
arched(win, 2, 2.8, 3.4, 2.85, 3.3)
arched(win, 2, 6.45, 7.05, 3.05, 3.5)
{
  // A downward triangle with a yellow crescent moon.
  shape(purple, 2, [[0.75, 4.75], [2.65, 4.75], [1.7, 2.8]])
  const outer = circle([1.75, 4.05], 0.58, Math.PI * 0.5, Math.PI * 1.5, 8)
  const inner = circle([1.97, 4.05], 0.5, Math.PI * 0.56, Math.PI * 1.44, 8)
  for (let k = 0; k < 8; k++) {
    shape(yellow, 2, [outer[k], outer[k + 1], inner[k + 1]], 0.07)
    shape(yellow, 2, [outer[k], inner[k + 1], inner[k]], 0.07)
  }
  // The trumpet: a purple bell ring with a yellow mouth up by the parapet, a
  // long purple horn running down to the player's yellow hands.
  shape(purple, 2, circle([3.95, 4.15], 0.8), 0.05)
  shape(yellow, 2, circle([3.95, 4.15], 0.55), 0.07)
  const a: XY = [4.25, 3.5], b: XY = [6.55, 0.95], w = 0.3
  const nx = -(b[1] - a[1]), ny = b[0] - a[0], nl = Math.hypot(nx, ny)
  const o: XY = [nx / nl * w, ny / nl * w]
  shape(purple, 2, [[a[0] - o[0], a[1] - o[1]], [b[0] - o[0], b[1] - o[1]], [b[0] + o[0], b[1] + o[1]], [a[0] + o[0], a[1] + o[1]]])
  shape(yellow, 2, circle([6.4, 1.15], 0.6, 0, 2 * Math.PI, 10), 0.07)
  // The player: a purple head and shoulders, his face picked out in yellow.
  shape(purple, 2, [[7.0, 0.1], [10.9, 0.1], [11.0, 1.8], [10.5, 3.5], [9.6, 4.3], [8.5, 4.25], [7.6, 3.5], [7.1, 2.0]])
  shape(yellow, 2, [[8.0, 1.7], [9.7, 1.5], [10.0, 2.9], [9.3, 3.7], [8.3, 3.4]], 0.07)
  shape(yellow, 2, [[7.3, 0.25], [8.6, 0.25], [8.4, 1.05], [7.4, 1.2]], 0.07)
}

// ---------------------------------------------------------------------------

// Colours from the daylight photos: the red paint on the brick, pulled a
// little light; one lavender-purple for the awnings, kickplates, name board
// and the mural's purple; the mural's yellow.
const parts = [
  { part: brick, material: finish('red-brick', 0xb44c42) },
  { part: purple, material: finish('muse-purple', 0x8a84cc) },
  { part: yellow, material: finish('muse-yellow', 0xe6c25c) },
  { part: win, material: PALETTE.window },
  { part: lit, material: PALETTE.entrance },
  { part: roof, material: PALETTE.roof },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Evening Muse', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 322, osm: 'way/1192512105', height: ZP + 0.1,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/evening-muse.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
