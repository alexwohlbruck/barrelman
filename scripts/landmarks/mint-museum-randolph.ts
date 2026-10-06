/**
 * Mint Museum Randolph (the 1836 Charlotte branch of the US Mint, rebuilt in
 * Eastover in 1933–36) — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/mint-museum-randolph.ts
 *
 * Map frame: x east, y north, z up, metres. Placed at bearing 2.4°, the axis of
 * the OSM outline (way/784079334), whose area centroid is the anchor. That one
 * outline covers the whole museum: the historic Mint on the west, and the
 * later galleries that wrap round a flat-roofed core behind it.
 *
 * - The Mint: a Federal block 38.8 × 10.3 m under a hipped roof, on a raised
 *   sandstone basement, with a central pavilion (the 1.2 m bump in the OSM
 *   outline) carrying a pediment with the gilded eagle, a small white porch of
 *   two columns, and steps down to the lawn. Cream stucco, white trim, dark
 *   shuttered windows (Wilkinson's cover photo, the 1936 photos).
 * - The galleries: the same stucco and sandstone base, under a ring of hipped
 *   slopes that rise to a flat membrane deck (the aerial), with glazed
 *   skylights on the east slope and the white-roofed glass entrance atrium on
 *   the east front.
 *
 * Ground: USGS 3DEP spot heights round the footprint span only 0.6 m
 * (192.46 m at the south-east corner to 193.08 m by the south-west). y = 0 is
 * the lowest of them; every wall runs down to it, and doors, windows and the
 * steps start above the highest local ground (0.62 m), so nothing sinks.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stucco = new Part(), stone = new Part(), trim = new Part(), roof = new Part()
const deck = new Part(), glass = new Part(), gold = new Part()

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }

/** A triangle whose winding is chosen to face `want`; optional smooth normals. */
function tri(p: Part, a: V3, b: V3, c: V3, want: V3, n?: V3[]) {
  if (dot(cross(sub(b, a), sub(c, a)), want) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, n && [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, want: V3, n?: [V3, V3, V3, V3]) {
  if (len(sub(a, c)) < 1e-6 || len(sub(b, d)) < 1e-6) return
  tri(p, a, b, c, want, n && [n[0], n[1], n[2]])
  tri(p, a, c, d, want, n && [n[0], n[2], n[3]])
}

/** Ear-clipping for a simple counter-clockwise polygon in plan, at height z. */
function capPoly(p: Part, pts: XY[], z: number, up = true) {
  const idx = pts.map((_, i) => i)
  const area = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (q: XY, a: XY, b: XY, c: XY) => area(a, b, q) > 1e-9 && area(b, c, q) > 1e-9 && area(c, a, q) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
    for (let k = 0; k < idx.length; k++) {
      const a = pts[idx[(k + idx.length - 1) % idx.length]], b = pts[idx[k]], c = pts[idx[(k + 1) % idx.length]]
      if (area(a, b, c) <= 1e-9) {
        if (Math.abs(area(a, b, c)) <= 1e-9) { idx.splice(k, 1); break } // collinear
        continue
      }
      if (idx.some(j => inside(pts[j], a, b, c))) continue
      tri(p, [a[0], a[1], z], [b[0], b[1], z], [c[0], c[1], z], [0, 0, up ? 1 : -1])
      idx.splice(k, 1)
      break
    }
  }
  if (idx.length === 3) {
    const [a, b, c] = idx.map(i => pts[i])
    tri(p, [a[0], a[1], z], [b[0], b[1], z], [c[0], c[1], z], [0, 0, up ? 1 : -1])
  }
}

// ---------------------------------------------------------------------------
// Orthogonal plans. A plan is a counter-clockwise ring of corners. Grown by d
// (negative shrinks), each convex corner is cut by a 45° chamfer of c, so
// walls and mouldings get a soft vertical edge; concave corners stay mitred.

type Plan = XY[]
function edgeDir(P: Plan, i: number): XY {
  const a = P[i], b = P[(i + 1) % P.length], l = Math.hypot(b[0] - a[0], b[1] - a[1])
  return [(b[0] - a[0]) / l, (b[1] - a[1]) / l]
}
const outward = (u: XY): XY => [u[1], -u[0]]
const convex = (P: Plan, i: number) => {
  const a = edgeDir(P, (i + P.length - 1) % P.length), b = edgeDir(P, i)
  return a[0] * b[1] - a[1] * b[0] > 0
}
/** Two points per corner (coincident at concave ones), each with its face normal. */
function ring(P: Plan, d: number, c: number) {
  const pts: XY[] = [], nrm: XY[] = []
  P.forEach((v, i) => {
    const uin = edgeDir(P, (i + P.length - 1) % P.length), uout = edgeDir(P, i)
    const nin = outward(uin), nout = outward(uout)
    const m: XY = [v[0] + d * (nin[0] + nout[0]), v[1] + d * (nin[1] + nout[1])]
    const cc = convex(P, i) ? Math.max(0.02, c + d * 0.414) : 0
    pts.push([m[0] - uin[0] * cc, m[1] - uin[1] * cc], [m[0] + uout[0] * cc, m[1] + uout[1] * cc])
    nrm.push(nin, nout)
  })
  return { pts, nrm }
}

/** The plan grown by d with plain mitred corners, for roofs. */
function mitre(P: Plan, d: number): XY[] {
  return P.map((v, i) => {
    const nin = outward(edgeDir(P, (i + P.length - 1) % P.length)), nout = outward(edgeDir(P, i))
    return [v[0] + d * (nin[0] + nout[0]), v[1] + d * (nin[1] + nout[1])] as XY
  })
}

/**
 * A moulding profile swept round a plan: each step runs from (d0, z0) to
 * (d1, z1), d outward of the wall plane, with a (out, up) normal at each end
 * so a bevel rolls from the face to the top.
 */
type Step = { p: Part; d0: number; z0: number; d1: number; z1: number; n0: XY; n1: XY }
const OUT: XY = [1, 0], TOP: XY = [0, 1], UNDER: XY = [0, -1]
const face = (p: Part, d: number, z0: number, z1: number): Step => ({ p, d0: d, z0, d1: d, z1, n0: OUT, n1: OUT })
const bevelUp = (p: Part, d0: number, z0: number, d1: number, z1: number): Step => ({ p, d0, z0, d1, z1, n0: OUT, n1: TOP })
const bevelOut = (p: Part, d0: number, z0: number, d1: number, z1: number): Step => ({ p, d0, z0, d1, z1, n0: UNDER, n1: OUT })

function sweep(P: Plan, c: number, steps: Step[], skip: number[] = []) {
  for (const s of steps) {
    const a = ring(P, s.d0, c), b = ring(P, s.d1, c)
    const m = a.pts.length
    for (let k = 0; k < m; k++) {
      const l = (k + 1) % m
      // Even k is corner k/2's chamfer; odd k is edge (k - 1) / 2.
      if (k % 2 === 1 && skip.includes((k - 1) / 2)) continue
      const n = (h: XY, t: XY): V3 => unit([h[0] * t[0], h[1] * t[0], t[1]])
      const A: V3 = [a.pts[k][0], a.pts[k][1], s.z0], B: V3 = [a.pts[l][0], a.pts[l][1], s.z0]
      const C: V3 = [b.pts[l][0], b.pts[l][1], s.z1], D: V3 = [b.pts[k][0], b.pts[k][1], s.z1]
      if (len(sub(A, B)) < 1e-6 && len(sub(C, D)) < 1e-6) continue
      const hk = a.nrm[k], hl = a.nrm[l]
      const ns: [V3, V3, V3, V3] = [n(hk, s.n0), n(hl, s.n0), n(hl, s.n1), n(hk, s.n1)]
      quad(s.p, A, B, C, D, add(ns[0], ns[2]), ns)
    }
  }
}

/** Vertical chamfer strips at a plan's convex corners. */
function corners(P: Plan, d: number, c: number, z0: number, z1: number, p: Part) {
  const r = ring(P, d, c)
  P.forEach((_, i) => {
    if (!convex(P, i)) return
    const a = r.pts[2 * i], b = r.pts[2 * i + 1], na = r.nrm[2 * i], nb = r.nrm[2 * i + 1]
    const A: V3 = [a[0], a[1], z0], B: V3 = [b[0], b[1], z0], C: V3 = [b[0], b[1], z1], D: V3 = [a[0], a[1], z1]
    const NA: V3 = [na[0], na[1], 0], NB: V3 = [nb[0], nb[1], 0]
    quad(p, A, B, C, D, add(NA, NB), [NA, NB, NB, NA])
  })
}

/**
 * Opening in a wall, placed by the world coordinate along the wall (x on an
 * east–west wall, y on a north–south one): centre, width, sill, head.
 */
type Opening = { at: number; w: number; lo: number; hi: number; depth?: number }

/**
 * The flat faces of a plan's walls between z0 and z1, at offset d, with deep
 * openings: stone or stucco round them, plain reveals, glass at the back.
 */
function walls(P: Plan, d: number, c: number, z0: number, z1: number, p: Part, openings: (edge: number) => Opening[] = () => [], skip: number[] = []) {
  const r = ring(P, d, c)
  P.forEach((V, i) => {
    if (skip.includes(i)) return
    const u = edgeDir(P, i), n = outward(u)
    const j = (i + 1) % P.length
    const s0 = (r.pts[2 * i + 1][0] - V[0]) * u[0] + (r.pts[2 * i + 1][1] - V[1]) * u[1]
    const s1 = (r.pts[2 * j][0] - V[0]) * u[0] + (r.pts[2 * j][1] - V[1]) * u[1]
    const base: XY = [V[0] + n[0] * d, V[1] + n[1] * d]
    const at = (s: number, z: number, depth = 0): V3 => [base[0] + u[0] * s - n[0] * depth, base[1] + u[1] * s - n[1] * depth, z]
    const N: V3 = [n[0], n[1], 0], U: V3 = [u[0], u[1], 0]
    const panel = (a: number, b: number, lo: number, hi: number) => {
      if (b - a > 1e-6 && hi - lo > 1e-6) quad(p, at(a, lo), at(b, lo), at(b, hi), at(a, hi), N)
    }
    const along = u[0] !== 0 ? V[0] * u[0] : V[1] * u[1]
    const list = openings(i)
      .map(o => { const sc = o.at * (u[0] !== 0 ? u[0] : u[1]) - along; return { ...o, a: sc - o.w / 2, b: sc + o.w / 2 } })
      .sort((x, y) => x.a - y.a)
    let s = s0
    for (const o of list) {
      const D = o.depth ?? 0.45
      panel(s, o.a, z0, z1)
      panel(o.a, o.b, z0, o.lo)
      panel(o.a, o.b, o.hi, z1)
      quad(p, at(o.a, o.lo), at(o.a, o.lo, D), at(o.a, o.hi, D), at(o.a, o.hi), U)
      quad(p, at(o.b, o.lo), at(o.b, o.lo, D), at(o.b, o.hi, D), at(o.b, o.hi), [-U[0], -U[1], 0])
      quad(p, at(o.a, o.hi), at(o.b, o.hi), at(o.b, o.hi, D), at(o.a, o.hi, D), [0, 0, -1])
      // The sill catches light, so it is trim; the glass sits at the back.
      quad(trim, at(o.a, o.lo), at(o.b, o.lo), at(o.b, o.lo, D), at(o.a, o.lo, D), [0, 0, 1])
      quad(glass, at(o.a, o.lo, D), at(o.b, o.lo, D), at(o.b, o.hi, D), at(o.a, o.hi, D), N)
      s = o.b
    }
    panel(s, s1, z0, z1)
  })
}

/** A plain box, every face but the bottom. */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number) {
  const c = (x: number, y: number, z: number): V3 => [x, y, z]
  quad(p, c(x0, y0, z0), c(x1, y0, z0), c(x1, y0, z1), c(x0, y0, z1), [0, -1, 0])
  quad(p, c(x1, y0, z0), c(x1, y1, z0), c(x1, y1, z1), c(x1, y0, z1), [1, 0, 0])
  quad(p, c(x1, y1, z0), c(x0, y1, z0), c(x0, y1, z1), c(x1, y1, z1), [0, 1, 0])
  quad(p, c(x0, y1, z0), c(x0, y0, z0), c(x0, y0, z1), c(x0, y1, z1), [-1, 0, 0])
  quad(p, c(x0, y0, z1), c(x1, y0, z1), c(x1, y1, z1), c(x0, y1, z1), [0, 0, 1])
}

/** A hipped roof over a rectangle, ridge along its longer side. */
function hip(x0: number, x1: number, y0: number, y1: number, ze: number, zr: number) {
  const alongY = y1 - y0 >= x1 - x0
  const h = (alongY ? x1 - x0 : y1 - y0) / 2
  if (alongY) {
    const xm = (x0 + x1) / 2, r0: V3 = [xm, y0 + h, zr], r1: V3 = [xm, y1 - h, zr]
    quad(roof, [x0, y0, ze], [x0, y1, ze], r1, r0, [-1, 0, 1])
    quad(roof, [x1, y1, ze], [x1, y0, ze], r0, r1, [1, 0, 1])
    tri(roof, [x0, y0, ze], [x1, y0, ze], r0, [0, -1, 1])
    tri(roof, [x1, y1, ze], [x0, y1, ze], r1, [0, 1, 1])
  } else {
    const ym = (y0 + y1) / 2, r0: V3 = [x0 + h, ym, zr], r1: V3 = [x1 - h, ym, zr]
    quad(roof, [x0, y0, ze], [x1, y0, ze], r1, r0, [0, -1, 1])
    quad(roof, [x1, y1, ze], [x0, y1, ze], r0, r1, [0, 1, 1])
    tri(roof, [x0, y1, ze], [x0, y0, ze], r0, [-1, 0, 1])
    tri(roof, [x1, y0, ze], [x1, y1, ze], r1, [1, 0, 1])
  }
}

/**
 * A gable roof running along x from xf (the gable end) back to xb, ridge at
 * y = yc. The front end gets a white raking fascia; `back` closes the far end.
 */
function gable(p: Part, xf: number, xb: number, yc: number, hw: number, ze: number, zr: number, fascia = 0.35) {
  const dir = Math.sign(xb - xf)
  const L: V3 = [xf, yc - hw, ze], R: V3 = [xf, yc + hw, ze], T: V3 = [xf, yc, zr]
  const Lb: V3 = [xb, yc - hw, ze], Rb: V3 = [xb, yc + hw, ze], Tb: V3 = [xb, yc, zr]
  quad(p, L, Lb, Tb, T, [0, -1, 1.5])
  quad(p, R, Rb, Tb, T, [0, 1, 1.5])
  // Raking fascia: the roof's thickness at the gable end, in trim.
  const down = (v: V3): V3 => [v[0], v[1], v[2] - fascia]
  quad(trim, L, T, down(T), down(L), [-dir, 0, 0])
  quad(trim, T, R, down(R), down(T), [-dir, 0, 0])
  // Soffit under the overhang, so the roof never reads as paper from below.
  const ins = (v: V3): V3 => [v[0] + dir * 0.35, v[1], v[2] - fascia]
  quad(trim, down(L), down(T), ins(T), ins(L), [0, 0, -1])
  quad(trim, down(T), down(R), ins(R), ins(T), [0, 0, -1])
}

/** Smooth round column between z0 and z1. */
function column(x: number, y: number, r: number, z0: number, z1: number, p: Part, seg = 12) {
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * Math.PI * 2, b = ((k + 1) / seg) * Math.PI * 2
    const na: V3 = [Math.cos(a), Math.sin(a), 0], nb: V3 = [Math.cos(b), Math.sin(b), 0]
    quad(p, [x + r * na[0], y + r * na[1], z0], [x + r * nb[0], y + r * nb[1], z0],
      [x + r * nb[0], y + r * nb[1], z1], [x + r * na[0], y + r * na[1], z1], add(na, nb), [na, nb, nb, na])
  }
}

// ---------------------------------------------------------------------------
// The historic Mint. Walls stand 0.4 m inside the OSM outline so the cornice
// (0.45 m) ends on it. Symmetrical about y = YC.

const YC = 0.5
const HX0 = -32.58, HX1 = -22.66, HY0 = -18.48, HY1 = 19.47
const PX = -33.74, PW = 7.95 // pavilion face and half width
const mint: Plan = [
  [HX0, HY0], [HX1, HY0], [HX1, HY1], [HX0, HY1],
  [HX0, YC + PW], [PX, YC + PW], [PX, YC - PW], [HX0, YC - PW],
]
const C = 0.35
const H_BASE = 2.75, H_WALL = 8.6, H_EAVE = 9.4, H_RIDGE = 13.3

// Main-storey windows, basement windows below them, and the door.
// Main windows are drawn with their dark shutters as one opening (p9).
const tall = (at: number): Opening => ({ at, w: 1.9, lo: 3.55, hi: 6.95 })
const low = (at: number): Opening => ({ at, w: 1.25, lo: 0.95, hi: 2.05, depth: 0.35 })
const wingY = (k: number, y0: number, y1: number) => y0 + ((y1 - y0) * (k + 0.5)) / 3
const westUpper = [0, 1, 2].map(k => wingY(k, HY0 + 0.6, YC - PW)).concat([0, 1, 2].map(k => wingY(k, YC + PW, HY1 - 0.6)))
const sideX = [HX0 + 2.6, HX1 - 2.8]
const mintUpper = (e: number): Opening[] => {
  if (e === 7 || e === 3) return westUpper.filter(y => (e === 7 ? y < YC : y > YC)).map(tall)
  if (e === 5) return [tall(YC - 4.6), tall(YC + 4.6), { at: YC, w: 1.7, lo: H_BASE, hi: 6.0, depth: 0.5 }]
  if (e === 0 || e === 2) return sideX.map(tall)
  return []
}
const mintLower = (e: number): Opening[] => {
  if (e === 7 || e === 3) return westUpper.filter(y => (e === 7 ? y < YC : y > YC)).map(low)
  if (e === 5) return [low(YC - 4.6), low(YC + 4.6)]
  if (e === 0 || e === 2) return sideX.map(low)
  return []
}

// Sandstone basement, standing 0.1 m proud, with a rounded water table.
walls(mint, 0.1, C, 0, H_BASE - 0.15, stone, mintLower)
corners(mint, 0.1, C, 0, H_BASE - 0.15, stone)
sweep(mint, C, [bevelUp(stone, 0.1, H_BASE - 0.15, 0, H_BASE)])
// Stucco storey, then the white cornice.
walls(mint, 0, C, H_BASE, H_WALL, stucco, mintUpper)
corners(mint, 0, C, H_BASE, H_WALL, stucco)
sweep(mint, C, [
  face(trim, 0, H_WALL, H_WALL + 0.2), bevelOut(trim, 0, H_WALL + 0.2, 0.45, H_WALL + 0.5),
  face(trim, 0.45, H_WALL + 0.5, H_WALL + 0.68), bevelUp(trim, 0.45, H_WALL + 0.68, 0.3, H_EAVE),
])

// Hipped roof over the main range; the pavilion's pediment roof runs back
// into it. Both rise 3.9 m (the pediment's proportions in the 1936 photos),
// so the pediment's apex meets the ridge.
const RO = 0.3 // roof edge outside the wall plane
hip(HX0 - RO, HX1 + RO, HY0 - RO, HY1 + RO, H_EAVE, H_RIDGE)
const ridgeX = (HX0 + HX1) / 2
gable(roof, PX - RO, ridgeX, YC, PW + RO, H_EAVE, H_RIDGE)
{
  // The cross range behind the pediment, running east from the ridge into
  // the galleries and ending in a hip (the aerial). Its west end is buried
  // under the main ridge.
  const xw = ridgeX, xe = -7, hw = 5.5, re = xe - hw
  const s0: V3 = [xw, YC - hw, H_EAVE], s1: V3 = [xe, YC - hw, H_EAVE], n0: V3 = [xw, YC + hw, H_EAVE], n1: V3 = [xe, YC + hw, H_EAVE]
  const r0: V3 = [xw, YC, H_RIDGE], r1: V3 = [re, YC, H_RIDGE]
  quad(roof, s0, s1, r1, r0, [0, -1, 1])
  quad(roof, n0, n1, r1, r0, [0, 1, 1])
  tri(roof, s1, n1, r1, [1, 0, 1])
}
{
  // Tympanum: stucco, set in the wall plane under the raking cornice.
  const zt = H_EAVE + 0.02, apex = H_RIDGE - 0.4, hw = PW - 0.1
  tri(stucco, [PX, YC - hw, zt], [PX, YC + hw, zt], [PX, YC, apex], [-1, 0, 0])
  // The gilded eagle, wings spread: a flat gold shape just proud of the tympanum.
  const k = 1.1, zc = H_EAVE + 1.15
  const half: XY[] = [[0, 1.25], [0.3, 1.0], [0.45, 0.65], [1.6, 1.0], [2.5, 0.85], [2.15, 0.55], [1.4, 0.35], [0.5, 0.2], [0.35, -0.35], [0.6, -0.7]]
  const outline: XY[] = [...half, ...[...half].reverse().slice(0, -1).map(([s, z]) => [-s, z] as XY)]
    .map(([s, z]) => [YC + s * k, zc + z * k] as XY)
  // capPoly works in plan, so build the shape there and turn it onto the wall.
  const flatGold = new Part()
  const ccw = outline.slice().reverse() // listed clockwise above
  capPoly(flatGold, ccw, 0)
  const toWall = (x: number, y: number, depth: number): V3 => [PX - depth, x, y]
  for (let t = 0; t < flatGold.pos.length; t += 9) {
    const v = [0, 1, 2].map(i => toWall(flatGold.pos[t + i * 3], -flatGold.pos[t + i * 3 + 2], 0.14))
    tri(gold, v[0], v[1], v[2], [-1, 0, 0])
  }
  for (let i = 0; i < outline.length; i++) {
    const a = outline[i], b = outline[(i + 1) % outline.length]
    const mid: XY = [(a[0] + b[0]) / 2 - YC, (a[1] + b[1]) / 2 - zc]
    const e: V3 = [0, b[0] - a[0], b[1] - a[1]]
    let nrm = unit(cross(e, [1, 0, 0]))
    if (dot(nrm, [0, mid[0], mid[1]]) < 0) nrm = [-nrm[0], -nrm[1], -nrm[2]]
    quad(gold, toWall(a[0], a[1], 0), toWall(b[0], b[1], 0), toWall(b[0], b[1], 0.14), toWall(a[0], a[1], 0.14), nrm)
  }
}

// The porch: two white columns under a bevelled entablature, on a sandstone
// stoop at main-floor level, with steps down to the lawn (ground there is
// 0.35 m above y = 0; the bottom tread runs down to 0).
{
  const x0 = PX - 2.6, hw = 2.2
  box(stone, x0, PX, YC - hw, YC + hw, 0, H_BASE)
  const treads = 5, run = 0.55, sw = 1.7, rise = (H_BASE - 0.35) / (treads + 1)
  for (let t = 0; t < treads; t++)
    box(stone, x0 - run * (t + 1), x0 - run * t, YC - sw, YC + sw, 0, H_BASE - rise * (t + 1))
  for (const s of [-1, 1]) column(x0 + 0.55, YC + s * 1.5, 0.3, H_BASE, 6.15, trim)
  const ent: Plan = [[x0, YC - hw], [PX, YC - hw], [PX, YC + hw], [x0, YC + hw]]
  sweep(ent, 0.1, [face(trim, 0, 6.15, 6.85), bevelUp(trim, 0, 6.85, -0.2, 7.05)], [1])
  capPoly(trim, ring(ent, -0.2, 0.1).pts, 7.05)
  capPoly(trim, ring(ent, 0, 0.1).pts, 6.15, false)
}

// ---------------------------------------------------------------------------
// The galleries behind: one plan wrapping the core, notched on the east front
// where the atrium stands. Walls 0.4 m inside the outline; the west edge abuts
// the Mint and is never seen.

const AY0 = -10, AY1 = 0, AX = 20 // atrium
const galleries: Plan = [
  [HX1, -17.38], [-14.93, -17.38], [-14.93, -29.59], [28.2, -29.59], [28.2, AY0],
  [AX, AY0], [AX, AY1], [28.2, AY1], [28.2, 29.58], [-14.19, 29.58], [-14.19, 17.72], [HX1, 17.72],
]
const G_BASE = 2.6, G_WALL = 8.0, G_EAVE = 8.8, G_RIDGE = 11.8, G_SLOPE = 6.0, G_BACK = 7.2, G_DECK = 10.0
const g = (at: number): Opening => ({ at, w: 1.6, lo: 3.4, hi: 6.9 })
const even = (a: number, b: number, n: number) => Array.from({ length: n }, (_, k) => a + ((b - a) * (k + 0.5)) / n)
const galleryWindows = (e: number): Opening[] => {
  switch (e) {
    case 0: return [g((HX1 + -14.93) / 2)]
    case 1: return [g(-23.5)]
    case 2: return even(-14.93, 28.2, 4).map(g)
    case 3: return even(-29.59, AY0, 2).map(g)
    case 7: return even(AY1, 29.58, 2).map(g)
    case 8: return even(-14.19, 28.2, 4).map(g)
    case 9: return [g(23.6)]
    case 10: return [g((HX1 + -14.19) / 2)]
    default: return []
  }
}
const hidden = [4, 5, 6, 11] // atrium notch and the Mint party wall
// Basement windows sit under the gallery windows, as on the Mint (p3).
const galleryLow = (e: number) => galleryWindows(e).map(o => ({ ...o, w: 1.3, lo: 0.95, hi: 1.95, depth: 0.35 }))
walls(galleries, 0.1, C, 0, G_BASE - 0.15, stone, galleryLow, hidden)
corners(galleries, 0.1, C, 0, G_BASE - 0.15, stone)
sweep(galleries, C, [bevelUp(stone, 0.1, G_BASE - 0.15, 0, G_BASE)], hidden)
walls(galleries, 0, C, G_BASE, G_WALL, stucco, galleryWindows, hidden)
corners(galleries, 0, C, G_BASE, G_WALL, stucco)
sweep(galleries, C, [
  face(trim, 0, G_WALL, G_WALL + 0.2), bevelOut(trim, 0, G_WALL + 0.2, 0.45, G_WALL + 0.5),
  face(trim, 0.45, G_WALL + 0.5, G_WALL + 0.62), bevelUp(trim, 0.45, G_WALL + 0.62, 0.3, G_EAVE),
], [11])

// A ring of hipped ranges round a lower flat core (the aerial shows the
// ridges shading the core's edges): outer slopes from the eave to a ridge
// line inset by the slope, a short back slope, then the membrane deck. Plain
// mitred rings, so hips and valleys fall on the diagonals.
{
  const outer = mitre(galleries, RO)
  const ridge = mitre(galleries, -G_SLOPE)
  const inner = mitre(galleries, -G_BACK)
  for (let i = 0; i < galleries.length; i++) {
    const j = (i + 1) % galleries.length
    const n = outward(edgeDir(galleries, i))
    quad(roof, [outer[i][0], outer[i][1], G_EAVE], [outer[j][0], outer[j][1], G_EAVE],
      [ridge[j][0], ridge[j][1], G_RIDGE], [ridge[i][0], ridge[i][1], G_RIDGE], [n[0], n[1], 2])
    quad(roof, [ridge[i][0], ridge[i][1], G_RIDGE], [ridge[j][0], ridge[j][1], G_RIDGE],
      [inner[j][0], inner[j][1], G_DECK], [inner[i][0], inner[i][1], G_DECK], [-n[0], -n[1], 0.7])
  }
  capPoly(deck, inner, G_DECK)
  // Glazed skylight strips on the east slope, either side of the atrium.
  const slopeZ = (x: number) => G_EAVE + ((28.2 + RO - x) * (G_RIDGE - G_EAVE)) / (G_SLOPE + RO)
  const lift = 0.06
  const n = unit([G_RIDGE - G_EAVE, 0, G_SLOPE + RO])
  for (const [y0, y1] of [[-16, AY0 - 2.5], [AY1 + 2.5, 13.5]] as XY[]) {
    const xa = 28.2 + RO - 0.9, xb = 28.2 - G_SLOPE + 0.8
    const P = (x: number, y: number): V3 => add([x, y, slopeZ(x)], n, lift)
    quad(glass, P(xa, y0), P(xa, y1), P(xb, y1), P(xb, y0), n)
  }
}

// The entrance atrium: a glass box with a white gabled roof, its front a hair
// past the gallery cornice so that ends inside it.
{
  const xf = 28.7, ze = 9.4, zr = 13.1, yc = (AY0 + AY1) / 2, hw = (AY1 - AY0) / 2
  const xb = AX - 0.3
  // Glass faces: front, and the two sides where they rise above the galleries.
  quad(glass, [xf, AY0, 0], [xf, AY1, 0], [xf, AY1, ze], [xf, AY0, ze], [1, 0, 0])
  quad(glass, [AX, AY0, 0], [xf, AY0, 0], [xf, AY0, ze], [AX, AY0, ze], [0, -1, 0])
  quad(glass, [xf, AY1, 0], [AX, AY1, 0], [AX, AY1, ze], [xf, AY1, ze], [0, 1, 0])
  tri(glass, [xf, AY0, ze], [xf, AY1, ze], [xf, yc, zr - 0.35], [1, 0, 0])
  tri(glass, [xb, AY0, ze], [xb, AY1, ze], [xb, yc, zr - 0.35], [-1, 0, 0])
  // White frame: a band at the eave, a plinth line, and a centre mullion.
  const band = (z0: number, z1: number, d = 0.12) => {
    quad(trim, [xf + d, AY0 - d, z0], [xf + d, AY1 + d, z0], [xf + d, AY1 + d, z1], [xf + d, AY0 - d, z1], [1, 0, 0])
    quad(trim, [AX, AY0 - d, z0], [xf + d, AY0 - d, z0], [xf + d, AY0 - d, z1], [AX, AY0 - d, z1], [0, -1, 0])
    quad(trim, [xf + d, AY1 + d, z0], [AX, AY1 + d, z0], [AX, AY1 + d, z1], [xf + d, AY1 + d, z1], [0, 1, 0])
    quad(trim, [xf + d, AY0 - d, z1], [xf + d, AY1 + d, z1], [AX, AY1 + d, z1], [AX, AY0 - d, z1], [0, 0, 1])
  }
  band(ze - 0.5, ze)
  band(3.1, 3.4, 0.08)
  for (const y of [yc - 2.5, yc + 2.5]) box(trim, xf, xf + 0.1, y - 0.12, y + 0.12, 0.55, ze - 0.5)
  gable(trim, xf + 0.25, xb, yc, hw + 0.3, ze, zr, 0.3)
}

const parts = [
  { part: stucco, material: { name: 'stucco', color: 0xe0dbcd } },
  { part: stone, material: { name: 'sandstone', color: 0xccb48f } },
  { part: trim, material: { name: 'white-trim', color: 0xf1eee6 } },
  { part: roof, material: { name: 'slate-roof', color: 0x79818a } },
  { part: deck, material: { name: 'flat-roof', color: 0xbdb9b1 } },
  { part: glass, material: { name: 'window', color: 0x4f5b65 } },
  { part: gold, material: { name: 'gilded-eagle', color: 0xc9a548 } },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Mint Museum Randolph', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 2.4, osm: 'way/784079334', footprint: [62.7, 60.0],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/mint-museum-randolph.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
