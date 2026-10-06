/**
 * Grand Central Terminal — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/grand-central-terminal.ts
 *
 * Map frame: x across the building (Vanderbilt -x, Lexington side +x), y along
 * Park Avenue (42nd Street -y), z up, metres. Placed at bearing 29°, the
 * Manhattan grid. Anchor is the centre of the OSM outline's rotated bounding
 * box (way/265947358, 120.6 × 103 m, height 45.8 m), so the model can be
 * exactly symmetrical about x = 0.
 *
 * Masses: the Beaux-Arts head house (42nd Street front and the two side
 * fronts) under a green copper hip roof; the main concourse rising behind it
 * with its own copper roof and pale skylight ridge; the lower north office
 * block up to the MetLife Building, which is a separate building and is not
 * modelled here.
 */
import { Part, addGltfTriangles, len, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stone = new Part(), granite = new Part(), glass = new Part()
const copper = new Part(), terrace = new Part(), sculpture = new Part(), gold = new Part()

const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]

function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) { p.tri(a, b, c, undefined, undefined, undefined, n) }
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: [V3, V3, V3, V3]) {
  tri(p, a, b, c, n && [n[0], n[1], n[2]])
  tri(p, a, c, d, n && [n[0], n[2], n[3]])
}

// ---------------------------------------------------------------------------
// Rings: a rectangle with 45° chamfered corners, grown outward by `d`.
// Points run counter-clockwise from above; each carries the outward normal of
// the face it belongs to, so a chamfer shades as a smooth rounded corner.

type Rect = { x0: number; x1: number; y0: number; y1: number; c: number }
function ring(r: Rect, d: number, z: number) {
  const { x0, x1, y0, y1 } = { x0: r.x0 - d, x1: r.x1 + d, y0: r.y0 - d, y1: r.y1 + d }
  const c = Math.max(0.05, r.c + d * 0.414)
  const pts: V3[] = [[x0 + c, y0, z], [x1 - c, y0, z], [x1, y0 + c, z], [x1, y1 - c, z],
    [x1 - c, y1, z], [x0 + c, y1, z], [x0, y1 - c, z], [x0, y0 + c, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}

/**
 * A moulding profile swept round a chamfered rectangle. Each step runs from
 * (d0, z0) to (d1, z1) — d outward from the wall plane — with a 2D normal
 * (out, up) at each end, so bevels can roll from the face to the top.
 */
type Step = { p: Part; d0: number; z0: number; d1: number; z1: number; n0?: [number, number]; n1?: [number, number] }
function sweep(r: Rect, steps: Step[]) {
  for (const s of steps) {
    const a = ring(r, s.d0, s.z0), b = ring(r, s.d1, s.z1)
    const flat = unit([s.z1 - s.z0, 0, s.d0 - s.d1]) // (out, -, up) in profile space
    const n0 = s.n0 ?? [flat[0], flat[2]], n1 = s.n1 ?? n0
    const at = (h: V3, n: [number, number]): V3 => unit([h[0] * n[0], h[1] * n[0], n[1]])
    for (let i = 0; i < 8; i++) {
      const j = (i + 1) % 8
      // Chamfer edges (odd i) blend the two face normals; faces stay flat.
      const hi = a.nrm[i], hj = a.nrm[j]
      quad(s.p, a.pts[i], a.pts[j], b.pts[j], b.pts[i], [at(hi, n0), at(hj, n0), at(hj, n1), at(hi, n1)])
    }
  }
}
function lid(p: Part, r: Rect, d: number, z: number) {
  const { pts } = ring(r, d, z)
  for (let i = 1; i < 7; i++) tri(p, pts[0], pts[i], pts[i + 1])
}
const OUT: [number, number] = [1, 0], TOP: [number, number] = [0, 1], UNDER: [number, number] = [0, -1]
/** Rounded outer top edge: face normal below, up-facing above. */
const bevelUp = (p: Part, d0: number, z0: number, d1: number, z1: number): Step => ({ p, d0, z0, d1, z1, n0: OUT, n1: TOP })
/** Rounded outer bottom edge of a projecting band: down-facing to face normal. */
const bevelOut = (p: Part, d0: number, z0: number, d1: number, z1: number): Step => ({ p, d0, z0, d1, z1, n0: UNDER, n1: OUT })
const face = (p: Part, d: number, z0: number, z1: number): Step => ({ p, d0: d, z0, d1: d, z1, n0: OUT, n1: OUT })
const flatTop = (p: Part, d0: number, d1: number, z: number): Step => ({ p, d0, z0: z, d1, z1: z, n0: TOP, n1: TOP })

/** Vertical chamfer strips at the corners of a rectangle's walls. */
function corners(r: Rect, z0: number, z1: number, which = [1, 3, 5, 7]) {
  const a = ring(r, 0, z0), b = ring(r, 0, z1)
  for (const i of which) {
    const j = (i + 1) % 8
    quad(stone, a.pts[i], a.pts[j], b.pts[j], b.pts[i], [a.nrm[i], a.nrm[j], a.nrm[j], a.nrm[i]])
  }
}

// ---------------------------------------------------------------------------
// Facades: a wall plane carrying deep, bevelled openings and engaged columns.

type Opening = { s0: number; s1: number; lo: number; hi: number; shape: 'rect' | 'arch' | 'circle'; depth?: number; bevel?: number }
type Wall = { a: XY; u: XY; length: number; z0: number; z1: number }
const ARC = 10, CIRCLE = 12

function wallFrame(w: Wall) {
  const u: V3 = [w.u[0], w.u[1], 0], n: V3 = [w.u[1], -w.u[0], 0]
  const v = (s: number, z: number, depth = 0): V3 => [w.a[0] + u[0] * s - n[0] * depth, w.a[1] + u[1] * s - n[1] * depth, z]
  const plane = (es: number, ez: number): V3 => unit([u[0] * es, u[1] * es, ez])
  return { u, n, v, plane }
}

/** Outline of an opening in (s, z), counter-clockwise seen from outside. */
function outline(o: Opening, inset: number) {
  const sc = (o.s0 + o.s1) / 2
  const pts: XY[] = [], centre: (XY | null)[] = []
  if (o.shape === 'rect') {
    pts.push([o.s0 + inset, o.lo + inset], [o.s1 - inset, o.lo + inset], [o.s1 - inset, o.hi - inset], [o.s0 + inset, o.hi - inset])
    centre.push(null, null, null, null)
  } else if (o.shape === 'arch') {
    const r = (o.s1 - o.s0) / 2, spring = o.hi - r
    pts.push([o.s0 + inset, o.lo + inset], [o.s1 - inset, o.lo + inset]); centre.push(null, null)
    for (let k = 0; k <= ARC; k++) {
      const a = (k * Math.PI) / ARC
      pts.push([sc + (r - inset) * Math.cos(a), spring + (r - inset) * Math.sin(a)]); centre.push([sc, spring])
    }
  } else {
    const r = (o.s1 - o.s0) / 2, zc = (o.lo + o.hi) / 2
    for (let k = 0; k < CIRCLE; k++) {
      const a = (k * 2 * Math.PI) / CIRCLE
      pts.push([sc + (r - inset) * Math.cos(a), zc + (r - inset) * Math.sin(a)]); centre.push([sc, zc])
    }
  }
  return { pts, centre }
}

/** One opening: 45° bevel, then a straight reveal back to blue-grey glass. */
function carve(w: Wall, o: Opening) {
  const { n, v, plane } = wallFrame(w)
  const b = o.bevel ?? 0.5, D = o.depth ?? 1.1
  const outer = outline(o, 0), inner = outline(o, b)
  const m = outer.pts.length
  // In-plane direction pointing into the opening, per corner of each edge.
  const inward = (k: number, l: number, at: number): V3 => {
    const c = outer.centre[at]
    if (outer.centre[k] && outer.centre[l] && c) {
      const p = outer.pts[at]
      return plane(c[0] - p[0], c[1] - p[1])
    }
    const ds = outer.pts[l][0] - outer.pts[k][0], dz = outer.pts[l][1] - outer.pts[k][1]
    return plane(-dz, ds)
  }
  for (let k = 0; k < m; k++) {
    const l = (k + 1) % m
    const ik = inward(k, l, k), il = inward(k, l, l)
    const O = (i: number) => v(outer.pts[i][0], outer.pts[i][1], 0)
    const I = (i: number) => v(inner.pts[i][0], inner.pts[i][1], b)
    const B = (i: number) => v(inner.pts[i][0], inner.pts[i][1], D)
    const ck = unit(add(n, ik)), cl = unit(add(n, il))
    quad(stone, O(k), O(l), I(l), I(k), [ck, cl, cl, ck])
    quad(stone, I(k), I(l), B(l), B(k), [ik, il, il, ik])
  }
  const back = inner.pts.map(([s, z]) => v(s, z, D))
  for (let k = 1; k < back.length - 1; k++) tri(glass, back[0], back[k], back[k + 1])
}

/** Stone between an opening's outline and its bounding box. */
function surround(w: Wall, o: Opening) {
  const { v } = wallFrame(w)
  const { pts } = outline(o, 0)
  const zc = o.shape === 'circle' ? (o.lo + o.hi) / 2 : o.shape === 'arch' ? o.hi - (o.s1 - o.s0) / 2 : Infinity
  const start = o.shape === 'arch' ? 2 : 0, m = pts.length
  for (let k = start; k < m; k++) {
    const l = k + 1 < m ? k + 1 : o.shape === 'circle' ? 0 : -1
    if (l < 0) break
    const P = pts[k], Q = pts[l]
    const upper = (P[1] + Q[1]) / 2 >= zc - 1e-6
    const edge = upper ? o.hi : o.lo
    const poly: XY[] = upper ? [P, [P[0], edge], [Q[0], edge], Q] : [[P[0], edge], [Q[0], edge], Q, P]
    // Lower half runs left to right, so its points are listed from the edge.
    const clean = poly.filter((p, i) => i === 0 || Math.hypot(p[0] - poly[i - 1][0], p[1] - poly[i - 1][1]) > 1e-6)
      .filter((p, i, all) => i < all.length - 1 || Math.hypot(p[0] - all[0][0], p[1] - all[0][1]) > 1e-6)
    for (let i = 1; i < clean.length - 1; i++) stone.tri(v(...clean[0]), v(...clean[i]), v(...clean[i + 1]))
  }
}

/** A flat stone rectangle in the wall plane. */
function panel(w: Wall, s0: number, s1: number, z0: number, z1: number) {
  if (s1 - s0 < 1e-6 || z1 - z0 < 1e-6) return
  const { v } = wallFrame(w)
  stone.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
}

/**
 * A whole wall: openings stacked in vertical bays, solid stone between and
 * above and below them. Bays are [s0, s1] spans holding one or more openings.
 */
function wall(w: Wall, bays: { s0: number; s1: number; openings: Opening[] }[]) {
  const sorted = [...bays].sort((a, b) => a.s0 - b.s0)
  let last = 0
  for (const bay of sorted) {
    panel(w, last, bay.s0, w.z0, w.z1)
    const stack = [...bay.openings].sort((a, b) => a.lo - b.lo)
    let z = w.z0
    for (const o of stack) {
      panel(w, bay.s0, bay.s1, z, o.lo)
      panel(w, bay.s0, o.s0, o.lo, o.hi)
      panel(w, o.s1, bay.s1, o.lo, o.hi)
      surround(w, o)
      carve(w, o)
      z = o.hi
    }
    panel(w, bay.s0, bay.s1, z, w.z1)
    last = bay.s1
  }
  panel(w, last, w.length, w.z0, w.z1)
}

/** A projecting block on a wall: plinths, capitals. Back face omitted. */
function block(p: Part, w: Wall, s0: number, s1: number, z0: number, z1: number, d: number) {
  const { v } = wallFrame(w)
  const f = (s: number, z: number) => v(s, z, -d), b = (s: number, z: number) => v(s, z, 0)
  quad(p, f(s0, z0), f(s1, z0), f(s1, z1), f(s0, z1))
  quad(p, b(s0, z0), f(s0, z0), f(s0, z1), b(s0, z1))
  quad(p, f(s1, z0), b(s1, z0), b(s1, z1), f(s1, z1))
  quad(p, f(s0, z1), f(s1, z1), b(s1, z1), b(s0, z1))
  quad(p, b(s0, z0), b(s1, z0), f(s1, z0), f(s0, z0))
}

/** An engaged half column, smooth-shaded, standing proud of the wall. */
function column(w: Wall, s: number, z0: number, z1: number, r: number) {
  const { u, n, v } = wallFrame(w)
  const seg = 8
  const at = (k: number, z: number): V3 => {
    const a = Math.PI - (k * Math.PI) / seg
    return v(s + r * Math.cos(a), z, -r * Math.sin(a))
  }
  const nm = (k: number): V3 => {
    const a = Math.PI - (k * Math.PI) / seg
    return unit(add([u[0] * Math.cos(a), u[1] * Math.cos(a), 0], n, Math.sin(a)))
  }
  for (let k = 0; k < seg; k++) quad(stone, at(k, z0), at(k + 1, z0), at(k + 1, z1), at(k, z1), [nm(k), nm(k + 1), nm(k + 1), nm(k)])
}

/** A pair of columns with a shared plinth and capital block. */
function columnPair(w: Wall, s0: number, s1: number, z0: number, z1: number) {
  const sc = (s0 + s1) / 2, gap = (s1 - s0) * 0.25, r = 0.85
  block(stone, w, s0 + 0.1, s1 - 0.1, z0, z0 + 1.3, 1.0)
  block(stone, w, s0 + 0.1, s1 - 0.1, z1 - 1.2, z1, 1.0)
  for (const k of [-1, 1]) column(w, sc + k * gap, z0 + 1.3, z1 - 1.2, r)
}

// ---------------------------------------------------------------------------
// Plan. Walls stand 1 m inside the OSM outline so cornices and columns stay
// within it.

const W = 59.3 // half width of the wall line
const YS = -50.5, YN = 50.5, YH = 13 // south wall, north wall, head house / office joint
const head: Rect = { x0: -W, x1: W, y0: YS, y1: YH, c: 0.5 }
const office: Rect = { x0: -W, x1: W, y0: YH, y1: YN, c: 0.5 }

const BASE = 5 // granite podium
const ORDER_TOP = 25.6 // top of capitals
const ARCH_LO = 5.4, ARCH_HI = 23.2

// Head house: granite podium, wall, entablature, attic, copper hip roof.
sweep(head, [
  face(granite, 0.4, 0, 4.6), bevelUp(granite, 0.4, 4.6, 0, BASE),
])
sweep(head, [
  face(stone, 0, ORDER_TOP, 28.2), bevelOut(stone, 0, 28.2, 0.95, 28.9), face(stone, 0.95, 28.9, 29.4),
  bevelUp(stone, 0.95, 29.4, 0.5, 29.8), flatTop(stone, 0.5, 0, 29.8),
  face(stone, 0, 29.8, 32.6), bevelUp(stone, 0, 32.6, -0.45, 33.0),
  { p: copper, d0: -0.45, z0: 33.0, d1: -9.5, z1: 38.6 },
])
// Behind the copper slopes the head house roof is copper too (OSM roof:colour).
lid(copper, head, -9.5, 38.6)
corners(head, BASE, ORDER_TOP, [1, 7])

// 42nd Street: three great arches between paired columns, a narrow window
// bay with an oculus between each pair, and broad end pavilions.
const south: Wall = { a: [-W, YS], u: [1, 0], length: 2 * W, z0: BASE, z1: ORDER_TOP }
const sx = (x: number) => x + W
const ARCH_W = 10.5, PAIR = 4.5, NARROW = 6
const arch = (c: number): Opening => ({ s0: c - ARCH_W / 2, s1: c + ARCH_W / 2, lo: ARCH_LO, hi: ARCH_HI, shape: 'arch', depth: 1.4, bevel: 0.55 })
const narrowBay = (c: number, w = 3.0): Opening[] => [
  { s0: c - w / 2, s1: c + w / 2, lo: 7.5, hi: 16, shape: 'rect', depth: 0.8, bevel: 0.4 },
  { s0: c - 1.6, s1: c + 1.6, lo: 18.2, hi: 21.4, shape: 'circle', depth: 0.7, bevel: 0.35 },
]
{
  const bays: { s0: number; s1: number; openings: Opening[] }[] = []
  const pairs: number[] = []
  // Centre-out layout, mirrored: arch, pair, narrow, pair, arch, pair.
  const xs = [0, ARCH_W / 2 + PAIR + NARROW + PAIR + ARCH_W / 2]
  for (const side of [-1, 1]) {
    const narrowC = side * (ARCH_W / 2 + PAIR + NARROW / 2)
    bays.push({ s0: sx(narrowC - NARROW / 2), s1: sx(narrowC + NARROW / 2), openings: narrowBay(sx(narrowC)) })
    const pavC = side * (xs[1] + ARCH_W / 2 + PAIR + (W - (xs[1] + ARCH_W / 2 + PAIR)) / 2)
    bays.push({ s0: sx(pavC - 4), s1: sx(pavC + 4), openings: narrowBay(sx(pavC), 3.6) })
    for (const x of [ARCH_W / 2 + PAIR / 2, xs[1] - ARCH_W / 2 - PAIR / 2, xs[1] + ARCH_W / 2 + PAIR / 2]) pairs.push(side * x)
  }
  for (const c of [-xs[1], 0, xs[1]]) bays.push({ s0: sx(c - ARCH_W / 2), s1: sx(c + ARCH_W / 2), openings: [arch(sx(c))] })
  wall({ ...south, a: [-W + head.c, YS], length: 2 * W - 2 * head.c }, bays.map(b => ({
    s0: b.s0 - head.c, s1: b.s1 - head.c,
    openings: b.openings.map(o => ({ ...o, s0: o.s0 - head.c, s1: o.s1 - head.c })),
  })))
  for (const x of pairs) columnPair(south, sx(x - PAIR / 2), sx(x + PAIR / 2), BASE, ORDER_TOP)
}

// Vanderbilt and Lexington sides, mirrored: a corner pavilion, then three
// arched windows between paired columns.
const SIDE_ARCH = 10.5, SIDE_PAIR = 4.0
function sideFront(side: number) {
  // East wall runs north (+y); west wall runs south (-y). s measured from the
  // wall start, y given in plan so both sides share one layout.
  const start: XY = side > 0 ? [W, YS + head.c] : [-W, YH]
  const len = YH - YS - head.c
  const w: Wall = { a: start, u: side > 0 ? [0, 1] : [0, -1], length: len, z0: BASE, z1: ORDER_TOP }
  const sy = (y: number) => (side > 0 ? y - start[1] : start[1] - y)
  const span = (y0: number, y1: number): [number, number] => {
    const a = sy(y0), b = sy(y1)
    return [Math.min(a, b), Math.max(a, b)]
  }
  const bays: { s0: number; s1: number; openings: Opening[] }[] = []
  const pav = -43
  const [p0, p1] = span(pav - 4, pav + 4)
  bays.push({ s0: p0, s1: p1, openings: narrowBay(sy(pav), 3.6) })
  let y = -35.5
  const pairs: number[] = []
  for (let k = 0; k < 3; k++) {
    pairs.push(y + SIDE_PAIR / 2)
    const [a0, a1] = span(y + SIDE_PAIR, y + SIDE_PAIR + SIDE_ARCH)
    bays.push({ s0: a0, s1: a1, openings: [{ ...arch((a0 + a1) / 2), s0: a0, s1: a1 }] })
    y += SIDE_PAIR + SIDE_ARCH
  }
  pairs.push(y + SIDE_PAIR / 2)
  wall(w, bays)
  for (const py of pairs) {
    const [a, b] = span(py - SIDE_PAIR / 2, py + SIDE_PAIR / 2)
    columnPair(w, a, b, BASE, ORDER_TOP)
  }
}
sideFront(-1)
sideFront(1)

// Central attic over the middle arch, then a cartouche carrying the clock,
// with the sculpture group standing on and around it.
const attic: Rect = { x0: -9.5, x1: 9.5, y0: YS, y1: YS + 5.5, c: 0.3 }
sweep(attic, [face(stone, 0, 29.8, 33.2), bevelUp(stone, 0, 33.2, -0.4, 33.6)])
lid(stone, attic, -0.4, 33.6)
const cartouche: Rect = { x0: -3.2, x1: 3.2, y0: YS + 0.4, y1: YS + 3.6, c: 0.3 }
sweep(cartouche, [face(stone, 0, 33.6, 38.2), bevelUp(stone, 0, 38.2, -0.45, 38.65)])
lid(stone, cartouche, -0.45, 38.65)
{
  // Clock face: a flush gold disk on the cartouche front.
  const zc = 36.0, r = 2.0, y = YS + 0.4 - 0.05
  for (let k = 0; k < CIRCLE; k++) {
    const a = (k * 2 * Math.PI) / CIRCLE, b = ((k + 1) * 2 * Math.PI) / CIRCLE
    gold.tri([0, y, zc], [r * Math.cos(a), y, zc + r * Math.sin(a)], [r * Math.cos(b), y, zc + r * Math.sin(b)])
  }
}

function smooth(build: (m: Part) => void, crease = 70) {
  const m = new Part()
  build(m)
  addGltfTriangles(sculpture, new Float32Array(m.pos), Uint32Array.from({ length: m.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}
function egg(c: V3, r: V3, n = 10, rows = 5) {
  smooth(p => {
    const pt = (i: number, j: number): V3 => {
      const a = (i * Math.PI * 2) / n, b = -Math.PI / 2 + (j * Math.PI) / rows
      return [c[0] + r[0] * Math.cos(b) * Math.cos(a), c[1] + r[1] * Math.cos(b) * Math.sin(a), c[2] + r[2] * Math.sin(b)]
    }
    for (let j = 0; j < rows; j++) for (let i = 0; i < n; i++) {
      if (j > 0) p.tri(pt(i, j), pt(i + 1, j), pt(i + 1, j + 1))
      if (j < rows - 1) p.tri(pt(i, j), pt(i + 1, j + 1), pt(i, j + 1))
    }
  })
}
/** A thick flat wing: a triangle in the facade plane, extruded in depth. */
function wing(side: number) {
  const y0 = YS + 2.2, y1 = YS + 2.9
  const tip: XY = [side * 4.6, 45.0], root: XY = [side * 0.6, 43.4], low: XY = [side * 3.0, 41.0]
  const P = (p: XY, y: number): V3 => [p[0], y, p[1]]
  const pts = side > 0 ? [root, low, tip] : [root, tip, low]
  tri(sculpture, P(pts[0], y0), P(pts[1], y0), P(pts[2], y0))
  tri(sculpture, P(pts[0], y1), P(pts[2], y1), P(pts[1], y1))
  for (let k = 0; k < 3; k++) {
    const a = pts[k], b = pts[(k + 1) % 3]
    quad(sculpture, P(a, y0), P(a, y1), P(b, y1), P(b, y0))
  }
}
// Minerva (west) and Hercules (east) recline either side of Mercury:
// mirrored, one bold mass each, no fine carving.
for (const side of [-1, 1]) {
  egg([side * 6.0, YS + 2.8, 36.0], [3.8, 2.3, 2.6], 10, 4)
  egg([side * 4.5, YS + 2.8, 38.7], [1.4, 1.2, 2.1], 8, 4)
}
egg([0, YS + 3.6, 39.2], [2.4, 1.9, 3.0], 10, 4)
egg([0, YS + 2.6, 42.4], [1.05, 0.95, 2.2], 8, 4)
egg([0, YS + 2.6, 45.15], [0.6, 0.6, 0.6], 8, 4)
wing(-1)
wing(1)

// Main concourse: clerestory walls, copper hip roof, pale skylight ridge.
const conc: Rect = { x0: -42, x1: 42, y0: -36, y1: 0, c: 0.4 }
sweep(conc, [
  face(stone, 0, 38.6, 40.7), bevelUp(stone, 0, 40.7, -0.4, 41.1),
  { p: copper, d0: -0.4, z0: 41.1, d1: -9.0, z1: 44.5 },
])
{
  const x0 = conc.x0 + 9.0, x1 = conc.x1 - 9.0, y0 = conc.y0 + 9.0, y1 = conc.y1 - 9.0, z = 44.5, ridge = 45.8, ym = (y0 + y1) / 2
  // The roof's own chamfered ring at 44.5 is a hair larger; cap it first.
  lid(copper, conc, -9.0, z - 0.01)
  quad(stone, [x0, y0, z], [x1, y0, z], [x1, ym, ridge], [x0, ym, ridge])
  quad(stone, [x1, y1, z], [x0, y1, z], [x0, ym, ridge], [x1, ym, ridge])
  tri(stone, [x1, y0, z], [x1, y1, z], [x1, ym, ridge])
  tri(stone, [x0, y1, z], [x0, y0, z], [x0, ym, ridge])
}

// North office block: a plainer, lower wing with broad window bands. Its
// south side is buried in the head house and is not drawn.
const OFFICE_TOP = 24.4
sweep(office, [face(granite, 0.4, 0, 4.6), bevelUp(granite, 0.4, 4.6, 0, BASE)])
sweep(office, [
  face(stone, 0, OFFICE_TOP, OFFICE_TOP), bevelOut(stone, 0, OFFICE_TOP, 0.5, 24.9), face(stone, 0.5, 24.9, 25.5),
  bevelUp(stone, 0.5, 25.5, 0.1, 25.9), flatTop(stone, 0.1, -0.6, 25.9),
])
lid(terrace, office, -0.6, 25.9)
corners(office, BASE, OFFICE_TOP, [3, 5])
{
  const len = YN - YH - office.c, bays = 5, pitch = len / bays
  for (const side of [-1, 1]) {
    const w: Wall = { a: side > 0 ? [W, YH] : [-W, YN - office.c], u: side > 0 ? [0, 1] : [0, -1], length: len, z0: BASE, z1: OFFICE_TOP }
    // Same bay positions on both sides, measured from the south end.
    const list = Array.from({ length: bays }, (_, k) => {
      const c = pitch * (k + 0.5), m = side > 0 ? c : len - c, h = pitch * 0.3
      return { s0: m - h, s1: m + h, openings: [{ s0: m - h, s1: m + h, lo: 7, hi: 22.5, shape: 'rect' as const, depth: 0.7, bevel: 0.45 }] }
    })
    wall(w, list)
  }
  // The north wall faces the MetLife Building's base: plain stone.
  wall({ a: [W - office.c, YN], u: [-1, 0], length: 2 * (W - office.c), z0: BASE, z1: OFFICE_TOP }, [])
}

// sRGB colours read off daylight photos: sunlit limestone on the 42nd
// Street front, the pinker granite podium, the dark blue-grey glazing behind
// the window grilles, the patinated copper roofs seen from above, and the
// weathered sculpture group.
const parts = [
  { part: stone, material: { name: 'limestone', color: 0xd8cbb3 } },
  { part: granite, material: { name: 'granite', color: 0xbcab9a } },
  { part: glass, material: { name: 'window', color: 0x6c7d8a } },
  { part: copper, material: { name: 'copper-roof', color: 0x9dba95 } },
  { part: terrace, material: { name: 'flat-roof', color: 0xbdb9b1 } },
  { part: sculpture, material: { name: 'sculpture', color: 0x8c8374 } },
  { part: gold, material: { name: 'clock', color: 0xc9a548 } },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Grand Central Terminal', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 29, osm: 'way/265947358', footprint: [120.6, 103], height: 45.8,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/grand-central-terminal.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
