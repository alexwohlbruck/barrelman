/**
 * Spirit Square and the McGlohon Theater, North Tryon Street, Charlotte: the
 * former First Baptist Church (J. M. McMichael, 1909), a domed neoclassical
 * sanctuary, now the McGlohon Theater, with the church's education wing
 * behind it — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/mcglohon-theater-spirit-square.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 48.7°, the
 * street grid's axis and the outline's short edges: the model's +x runs
 * south-east from North Tryon Street toward College Street, +y north-east
 * toward East 7th Street. So the church's front faces Tryon on the model's
 * west face, set back about 15 m behind the forecourt. The anchor is the
 * metre-based area centroid of way/131139724, the one OSM outline (17 m)
 * that covers the sanctuary and the wing.
 *
 * The dome is the landmark: a round drum ringed with arched windows under a
 * low green copper dome and a small lantern, over the crossing of the nave
 * and transept roofs. The front is three bays: a pedimented centre with the
 * great recessed arch (entrance below, the big arched window above) between
 * two corner pavilions, each crowned with a small green dome. The walls are
 * buff brick with pale stone belt courses and cornice; the sides carry tall
 * round-arched windows and a pedimented transept gable with a big arched
 * window. The education wing behind is a plainer buff block with rows of
 * windows and a hipped roof.
 *
 * Dimensions: the plan is OSM's (sanctuary 32 × 21.8 m, wing to the rear);
 * the dome's 11.2 m diameter and its place on the axis 18 m behind the front
 * are measured on USGS NAIP. Heights are scaled from the photos against
 * OSM's 17 m: cornice 13.5 m (the corner pavilions 11.4 m), pediments
 * 17.5 m, dome crown 25.8 m, lantern 29.4 m. The ground falls about 1 m from Tryon to the wing's far end (AWS
 * terrain tiles), so y = 0 is the low end and the front starts at its own
 * ground level.
 *
 * References (visual only): "First Baptist Church, Charlotte, N.C."
 * (linen postcard, UNC Libraries Commons, Flickr 22680692338, public domain);
 * Dclemens1971, "Former First Baptist Church", "Main Library and former First
 * Baptist Church 01" and "02" (Commons, CC BY 4.0; the 2025 restoration, under
 * scaffolding); Mapillary (JordanAnderson 2022, CC BY-SA 4.0, the south side);
 * USGS NAIP orthoimagery (public domain).
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const buff = new Part(), trim = new Part(), copper = new Part()
const roof = new Part(), win = new Part(), door = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const sub3 = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
const cross3 = (a: V3, b: V3): V3 => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]
const dot3 = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3[]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}
/** A flat convex polygon wound to face `hint`. */
function poly(p: Part, pts: V3[], hint: V3) {
  const N = unit(hint)
  for (let i = 1; i < pts.length - 1; i++) {
    let a = pts[0], b = pts[i], c = pts[i + 1]
    const n = cross3(sub3(b, a), sub3(c, a))
    if (Math.hypot(...n) < 1e-9) continue
    if (dot3(n, hint) < 0) [b, c] = [c, b]
    p.tri(a, b, c, undefined, undefined, undefined, [N, N, N])
  }
}

// ---------------------------------------------------------------------------
// Polygon helpers: rings counter-clockwise from above.

function ccw(r: XY[]): XY[] {
  let a = 0
  r.forEach((p, i) => { const q = r[(i + 1) % r.length]; a += p[0] * q[1] - q[0] * p[1] })
  return a < 0 ? [...r].reverse() : r
}
function triangulate(r: XY[]): [number, number, number][] {
  const idx = r.map((_, i) => i), out: [number, number, number][] = []
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => cr(a, b, p) > 1e-9 && cr(b, c, p) > 1e-9 && cr(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 5000) {
    for (let i = 0; i < idx.length; i++) {
      const a = idx[(i + idx.length - 1) % idx.length], b = idx[i], c = idx[(i + 1) % idx.length]
      if (cr(r[a], r[b], r[c]) <= 1e-9) continue
      if (idx.some((j) => j !== a && j !== b && j !== c && inside(r[j], r[a], r[b], r[c]))) continue
      out.push([a, b, c]); idx.splice(i, 1); break
    }
  }
  out.push([idx[0], idx[1], idx[2]])
  return out
}
function offset(r: XY[], d: number): XY[] {
  const n = r.length
  return r.map((p, i) => {
    const a = r[(i + n - 1) % n], b = r[(i + 1) % n]
    const e1 = unit([p[0] - a[0], p[1] - a[1], 0]), e2 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n1: XY = [e1[1], -e1[0]], n2: XY = [e2[1], -e2[0]]
    const m = unit([n1[0] + n2[0], n1[1] + n2[1], 0]), cos = m[0] * n1[0] + m[1] * n1[1]
    const k = d / Math.max(cos, 0.3)
    return [p[0] + m[0] * k, p[1] + m[1] * k]
  })
}
const edgeN = (a: XY, b: XY): V3 => unit([b[1] - a[1], -(b[0] - a[0]), 0])
function walls(p: Part, r: XY[], z0: number, z1: number, smooth = false) {
  const n = r.length
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, a = r[i], b = r[j], N = edgeN(a, b)
    let na = N, nb = N
    if (smooth) { na = unit(sub3(edgeN(r[(i + n - 1) % n], a), [-N[0], -N[1], 0])); nb = unit(sub3(edgeN(b, r[(j + 1) % n]), [-N[0], -N[1], 0])) }
    quadN(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [na, nb, nb, na])
  }
}
function cap(p: Part, r: XY[], z: number) {
  const N: V3 = [0, 0, 1]
  for (const [a, b, c] of triangulate(r))
    p.tri([r[a][0], r[a][1], z], [r[b][0], r[b][1], z], [r[c][0], r[c][1], z], undefined, undefined, undefined, [N, N, N])
}
/** A solid with a chamfered top edge of size b; the top goes to `top`. */
function prism(p: Part, r: XY[], z0: number, z1: number, b: number, top: Part | null = p, smooth = false) {
  r = ccw(r)
  walls(p, r, z0, z1 - b, smooth)
  const r2 = offset(r, -b)
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length, N = edgeN(r[i], r[j]), M = unit([N[0], N[1], 1])
    quadN(p, [r[i][0], r[i][1], z1 - b], [r[j][0], r[j][1], z1 - b], [r2[j][0], r2[j][1], z1], [r2[i][0], r2[i][1], z1], [M, M, M, M])
  }
  if (top) cap(top, r2, z1)
}
const rect = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
const circle = (c: XY, R: number, n: number, a0 = 0): XY[] =>
  Array.from({ length: n }, (_, k) => { const t = a0 + 2 * Math.PI * k / n; return [c[0] + R * Math.cos(t), c[1] + R * Math.sin(t)] as XY })

/**
 * A wall face from A to B (outside on the right), on which shapes are drawn
 * in (s, z) — s metres along the wall from A — just proud of it.
 */
function face(A: XY, B: XY) {
  const L = Math.hypot(B[0] - A[0], B[1] - A[1]), u: XY = [(B[0] - A[0]) / L, (B[1] - A[1]) / L]
  const n = edgeN(A, B)
  const at = (s: number, z: number, o: number): V3 => [A[0] + u[0] * s + n[0] * o, A[1] + u[1] * s + n[1] * o, z]
  return {
    L,
    rect(p: Part, s0: number, s1: number, z0: number, z1: number, o = 0.04) { poly(p, [at(s0, z0, o), at(s1, z0, o), at(s1, z1, o), at(s0, z1, o)], n) },
    /** A round-headed opening: a semicircle on top of a rectangle. */
    arch(p: Part, sc: number, w: number, z0: number, zs: number, o = 0.04) {
      const r = w / 2, pts: V3[] = [at(sc - r, z0, o), at(sc + r, z0, o)]
      for (let k = 0; k <= 10; k++) { const t = Math.PI * k / 10; pts.push(at(sc + r * Math.cos(t), zs + r * Math.sin(t), o)) }
      poly(p, pts, n)
    },
    /** A pediment: a triangle from s0 to s1 between z0 and its apex at zp. */
    gable(p: Part, s0: number, s1: number, z0: number, zp: number, o = 0) { poly(p, [at(s0, z0, o), at(s1, z0, o), at((s0 + s1) / 2, zp, o)], n) },
  }
}

/** A gable roof whose ridge runs along x (axis 'x') or y, over a rectangle. */
function gableRoof(p: Part, x0: number, x1: number, y0: number, y1: number, ze: number, zr: number, axis: 'x' | 'y', over = 0.35) {
  if (axis === 'x') {
    const yc = (y0 + y1) / 2, a = y0 - over, b = y1 + over, k = (zr - ze) / (yc - y0), zl = ze - over * k
    const nS = unit([0, -(zr - ze), yc - y0]), nN = unit([0, zr - ze, yc - y0])
    quadN(p, [x0, a, zl], [x1, a, zl], [x1, yc, zr], [x0, yc, zr], [nS, nS, nS, nS])
    quadN(p, [x1, b, zl], [x0, b, zl], [x0, yc, zr], [x1, yc, zr], [nN, nN, nN, nN])
  } else {
    const xc = (x0 + x1) / 2, a = x0 - over, b = x1 + over, k = (zr - ze) / (xc - x0), zl = ze - over * k
    const nW = unit([-(zr - ze), 0, xc - x0]), nE = unit([zr - ze, 0, xc - x0])
    quadN(p, [a, y1, zl], [a, y0, zl], [xc, y0, zr], [xc, y1, zr], [nW, nW, nW, nW])
    quadN(p, [b, y0, zl], [b, y1, zl], [xc, y1, zr], [xc, y0, zr], [nE, nE, nE, nE])
  }
}

/** A dome: a spherical cap over a circle of radius R at z0 rising `rise`, smooth-shaded. */
function dome(p: Part, c: XY, z0: number, R: number, rise: number, seg = 16, rings = 5, a0 = 0) {
  const rho = (R * R + rise * rise) / (2 * rise), zc = z0 + rise - rho, phi0 = Math.asin(R / rho)
  const pt = (i: number, k: number): [V3, V3] => {
    const phi = phi0 * (1 - k / rings), t = a0 + 2 * Math.PI * i / seg
    const n: V3 = [Math.sin(phi) * Math.cos(t), Math.sin(phi) * Math.sin(t), Math.cos(phi)]
    return [[c[0] + rho * n[0], c[1] + rho * n[1], zc + rho * n[2]], n]
  }
  for (let k = 0; k < rings; k++) for (let i = 0; i < seg; i++) {
    const [a, na] = pt(i, k), [b, nb] = pt(i + 1, k), [d, nd] = pt(i, k + 1), [e, ne] = pt(i + 1, k + 1)
    if (k === rings - 1) p.tri(a, b, e, undefined, undefined, undefined, [na, nb, ne])
    else quadN(p, a, b, e, d, [na, nb, ne, nd])
  }
}

// ---------------------------------------------------------------------------
// Dimensions.

const g = (x: number) => Math.max(0, 0.95 * (29.5 - x) / 60)
const XF = -30.6, XR = 1.6          // the sanctuary's front (Tryon) and back
const YS = -11.8, YN = 10.0         // its south and north sides
const YC = (YS + YN) / 2            // the axis
const GF = g(XF)                    // ground at the front
const Z_COR = 13.5                  // cornice
const Z_PAV = 11.4                  // the corner pavilions' cornice
const Z_PED = 17.5                  // pediment apex
const P = 5.6                       // corner pavilions' depth and width
const CY0 = YS + P, CY1 = YN - P    // the centre bay
const TX0 = -17.5, TX1 = -8.5       // the transept
const DC: XY = [-13.0, YC]          // the dome's centre, on the crossing
const R_DRUM = 5.6, Z_DRUM0 = Z_COR, Z_DRUM1 = 21.6, Z_DOME = 25.8

// ---------------------------------------------------------------------------
// The sanctuary.

// The body: the centre bay and everything behind the pavilions rise to the
// main cornice; the two corner pavilions stop lower, under their domes.
const BODY: XY[] = [[XF + P, YS], [XR, YS], [XR, YN], [XF + P, YN], [XF + P, CY1], [XF, CY1], [XF, CY0], [XF + P, CY0]]
prism(buff, BODY, 0, Z_COR, 0, null)
for (const [y0, y1] of [[YS, CY0], [CY1, YN]]) {
  const r = ccw(rect(XF, XF + P, y0, y1))
  prism(buff, r, 0, Z_PAV, 0, null)
  prism(trim, offset(r, 0.25), Z_PAV - 0.6, Z_PAV + 0.25, 0.2, null)
  cap(roof, offset(r, 0.1), Z_PAV + 0.2)
}
// Flat roofs over the aisles and pavilions, behind the cornice.
cap(roof, offset(ccw(BODY), 0.1), Z_COR + 0.2)
// Cornice and belt course in pale stone, a little proud, with soft edges.
prism(trim, offset(ccw(BODY), 0.25), Z_COR - 0.6, Z_COR + 0.25, 0.2, null)
prism(trim, offset(ccw(rect(XF, XR, YS, YN)), 0.12), GF + 5.6, GF + 6.1, 0.1, null)
// Nave roof along the axis over the centre bay, transept roof across it.
gableRoof(roof, XF, XR, CY0, CY1, Z_COR, Z_PED, 'x')
gableRoof(roof, TX0, TX1, YS, YN, Z_COR, Z_PED, 'y')
{
  // Pediments: the front and back of the nave, both ends of the transept,
  // each with a pale raking cornice.
  const peds: [XY, XY, number, number][] = [
    [[XF, CY1], [XF, CY0], 0, CY1 - CY0],
    [[XR, CY0], [XR, CY1], 0, CY1 - CY0],
    [[TX0, YS], [TX1, YS], 0, TX1 - TX0],
    [[TX1, YN], [TX0, YN], 0, TX1 - TX0],
  ]
  for (const [A, B, s0, s1] of peds) {
    const f = face(A, B)
    f.gable(buff, s0, s1, Z_COR + 0.25, Z_PED)
    // Raking cornices as thin pale bands just proud.
    const k = (Z_PED - Z_COR - 0.25) / ((s1 - s0) / 2), h = 0.45
    poly(trim, [[0, Z_COR + 0.25], [(s0 + s1) / 2, Z_PED], [(s0 + s1) / 2, Z_PED + h], [-0.3, Z_COR + 0.25 + h - 0.3 * k]].map(([s, z]) => fpt(A, B, s, z, 0.1)), edgeN(A, B))
    poly(trim, [[s1, Z_COR + 0.25], [s1 + 0.3, Z_COR + 0.25 + h - 0.3 * k], [(s0 + s1) / 2, Z_PED + h], [(s0 + s1) / 2, Z_PED]].map(([s, z]) => fpt(A, B, s, z, 0.1)), edgeN(A, B))
  }
}
function fpt(A: XY, B: XY, s: number, z: number, o: number): V3 {
  const L = Math.hypot(B[0] - A[0], B[1] - A[1]), u: XY = [(B[0] - A[0]) / L, (B[1] - A[1]) / L], n = edgeN(A, B)
  return [A[0] + u[0] * s + n[0] * o, A[1] + u[1] * s + n[1] * o, z]
}

// The drum, ringed with arched windows, its cornice, the dome and the lantern.
{
  const N = 16
  prism(buff, circle(DC, R_DRUM, N, Math.PI / N), Z_DRUM0, Z_DRUM1 - 0.5, 0, null, true)
  prism(trim, circle(DC, R_DRUM + 0.3, N, Math.PI / N), Z_DRUM1 - 0.6, Z_DRUM1, 0.2, null, true)
  dome(copper, DC, Z_DRUM1, R_DRUM + 0.1, Z_DOME - Z_DRUM1, 16, 5, Math.PI / N)
  // A window on every other facet.
  const ring = circle(DC, R_DRUM, N, Math.PI / N)
  for (let i = 0; i < N; i += 2) {
    const f = face(ring[i], ring[(i + 1) % N])
    f.arch(win, f.L / 2, 1.3, 18.0, 20.0)
  }
  prism(trim, circle(DC, 1.0, 12), Z_DOME - 0.4, Z_DOME + 2.4, 0.15, null, true)
  dome(copper, DC, Z_DOME + 2.4, 1.25, 0.9, 12, 3)
  prism(trim, circle(DC, 0.12, 6), Z_DOME + 3.2, Z_DOME + 3.6, 0, trim)
}

// The corner pavilions' small domes on square bases.
for (const yc of [YS + P / 2, YN - P / 2]) {
  const c: XY = [XF + P / 2, yc]
  prism(buff, rect(c[0] - 2.0, c[0] + 2.0, yc - 2.0, yc + 2.0), Z_PAV, Z_PAV + 1.0, 0.15, null)
  prism(trim, rect(c[0] - 2.2, c[0] + 2.2, yc - 2.2, yc + 2.2), Z_PAV + 0.9, Z_PAV + 1.3, 0.12, null)
  dome(copper, c, Z_PAV + 1.3, 2.0, 1.9, 12, 4)
}

// The front, facing Tryon: the great arch in the centre bay, the pavilions' windows.
{
  const f = face([XF, YN], [XF, YS])           // s runs from the north corner
  const sc = YN - YC, w = 8.0
  f.arch(trim, sc, w + 1.1, GF + 5.6, GF + 7.9, 0.02) // its pale archivolt
  f.arch(win, sc, w, GF + 6.3, GF + 7.9, 0.05)       // the big arched window
  f.rect(door, sc - 3.0, sc + 3.0, GF, GF + 5.4) // the entrance under it
  for (const d of [-1.1, 1.1]) f.rect(trim, sc + d - 0.3, sc + d + 0.3, GF, GF + 5.4, 0.08) // its two columns
  for (const pc of [P / 2, f.L - P / 2]) {
    for (const d of [-1.3, 0, 1.3]) f.arch(win, pc + d, 0.9, GF + 6.9, GF + 8.9)
    f.rect(win, pc - 0.9, pc + 0.9, GF + 1.6, GF + 4.6)
  }
}
// The sides: pavilion bay, two nave bays, the transept's great arch, two rear bays.
for (const [A, B] of [[[XF, YS], [XR, YS]], [[XR, YN], [XF, YN]]] as [XY, XY][]) {
  const f = face(A, B), south = A[1] === YS
  const s = (x: number) => (south ? x - XF : XR - x)
  const gr = (x: number) => g(x)
  for (const d of [-1.3, 0, 1.3]) f.arch(win, s(XF + P / 2) + d, 0.9, gr(XF) + 6.9, gr(XF) + 8.9)
  for (const x of [-22.6, -19.6, -5.9, -2.0]) {
    f.arch(win, s(x), 1.6, gr(x) + 6.8, gr(x) + 10.4)
    f.rect(win, s(x) - 0.7, s(x) + 0.7, gr(x) + 1.6, gr(x) + 4.6)
  }
  const xt = (TX0 + TX1) / 2
  f.arch(trim, s(xt), 6.3, gr(xt) + 6.1, gr(xt) + 9.9, 0.02)
  f.arch(win, s(xt), 5.4, gr(xt) + 6.6, gr(xt) + 9.9, 0.05)
  for (const d of [-1.8, 0, 1.8]) f.rect(win, s(xt) + d - 0.6, s(xt) + d + 0.6, gr(xt) + 1.6, gr(xt) + 4.6)
}

// ---------------------------------------------------------------------------
// The education wing behind: a plainer buff block with a hipped roof.

{
  const WING: XY[] = [[XR, YS], [23.0, YS], [23.0, -8.5], [29.4, -8.5], [29.5, 12.9], [XR, 12.9]]
  const ZW = 13.0
  prism(buff, WING, 0, ZW, 0.3, roof)
  // The hipped roof over its middle (NAIP shows it red; drawn in roof grey).
  const x0 = 6.0, x1 = 22.0, y0 = -5.5, y1 = 11.0, zr = ZW + 3.4, inset = 4.5
  const r0: V3[] = [[x0, y0, ZW], [x1, y0, ZW], [x1, y1, ZW], [x0, y1, ZW]]
  const r1: V3[] = [[x0 + inset, y0 + inset, zr], [x1 - inset, y0 + inset, zr], [x1 - inset, y1 - inset, zr], [x0 + inset, y1 - inset, zr]]
  for (let i = 0; i < 4; i++) { const j = (i + 1) % 4; poly(roof, [r0[i], r0[j], r1[j], r1[i]], cross3(sub3(r0[j], r0[i]), sub3(r1[i], r0[i]))) }
  poly(roof, r1, [0, 0, 1])
  // Rows of windows, a panel per bay.
  const rows = [[1.8, 4.0], [5.6, 7.8], [9.4, 11.6]]
  const sides: [XY, XY][] = [[[XR, YS], [23.0, YS]], [[23.0, -8.5], [29.4, -8.5]], [[29.4, -8.5], [29.5, 12.9]], [[29.5, 12.9], [XR, 12.9]]]
  for (const [A, B] of sides) {
    const f = face(A, B), n = Math.max(1, Math.floor((f.L - 1.2) / 3.4))
    const pitch = (f.L - 1.2) / n
    for (let k = 0; k < n; k++) {
      const sc = 0.6 + pitch * (k + 0.5), xm = A[0] + (B[0] - A[0]) * sc / f.L
      // Leave the bay nearest the sanctuary blank on the street sides: its corner stair.
      if ((A[0] === XR || B[0] === XR) && Math.abs(xm - XR) < 3) continue
      for (const [z0, z1] of rows) f.rect(win, sc - 0.9, sc + 0.9, g(xm) + z0, g(xm) + z1)
    }
  }
}

// ---------------------------------------------------------------------------

// Colours from the photos: buff brick pulled light; pale stone trim; the
// green copper domes; grey roofs; slate windows; the lit entrance.
const parts = [
  { part: buff, material: finish('buff-brick', 0xe6d5b4) },
  { part: trim, material: PALETTE.trim },
  { part: copper, material: PALETTE.copper },
  { part: roof, material: PALETTE.roof },
  { part: win, material: PALETTE.window },
  { part: door, material: PALETTE.entrance },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('McGlohon Theater at Spirit Square', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 48.7, osm: 'way/131139724', height: Z_DOME + 3.6,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const outFile = new URL('../../landmarks/models/mcglohon-theater-spirit-square.glb', import.meta.url).pathname
await Bun.write(outFile, glb)
console.log(`${outFile}: ${triangles} triangles, ${glb.length} bytes`)
