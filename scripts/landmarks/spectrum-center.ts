/**
 * Spectrum Center, Charlotte — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/spectrum-center.ts
 *
 * Map frame: x across the arena (Brevard/Blue Line side -x, Caldwell side +x),
 * y along its long axis (Trade Street -y, 5th Street +y), z up, metres. Placed
 * at bearing 45°, the Uptown grid; Trade Street runs along the model's -y face.
 * The origin is the centroid of the OSM outline (way/773909122).
 *
 * Masses, after the OSM parts and photos:
 * - a brick podium over the whole outline, with a storefront glass band;
 * - the lower bowl tier (OSM 23 m) wrapping the long sides;
 * - the main drum (OSM 35 m), a rounded square in plan: brick with broad
 *   window bands and a dark louvre band under a pale roof edge, and grey metal
 *   panel faces at the two ends where the high core meets the street;
 * - the high core (OSM 43 m): a pale metal roof that arches along the long
 *   axis and crowns gently across it;
 * - the tall glass entrance atrium on the Trade & Caldwell corner.
 *
 * Windows are slate panels set 0.05 m into the wall, per landmarks/STYLE.md.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const brick = new Part(), panel = new Part(), dark = new Part(), glass = new Part(), roof = new Part()

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const lerp2 = (a: XY, b: XY, t: number): XY => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]

// Outline coordinates were measured from the OSM bounding-box centre; the
// anchor is the outline's centroid, this far from it.
const OX = -0.9, OY = 5.6
const at = (p: XY): XY => [p[0] + OX, p[1] + OY]

function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: [V3, V3, V3, V3]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
function earcut(pts: XY[]): [number, number, number][] {
  const idx = pts.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 0 && crossz(b, c, p) > 0 && crossz(c, a, p) > 0
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = pts[i0], b = pts[i1], c = pts[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some(j => j !== i0 && j !== i1 && j !== i2 && inside(pts[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
function capPoly(p: Part, pts: XY[], z: number) {
  for (const [a, b, c] of earcut(pts)) p.tri([pts[a][0], pts[a][1], z], [pts[b][0], pts[b][1], z], [pts[c][0], pts[c][1], z])
}
/** Move every edge of a counter-clockwise ring outward by d (mitred). */
function offset(pts: XY[], d: number): XY[] {
  const n = pts.length
  return pts.map((p, i) => {
    const a = pts[(i + n - 1) % n], b = pts[(i + 1) % n]
    const e0 = unit([p[0] - a[0], p[1] - a[1], 0]), e1 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n0: XY = [e0[1], -e0[0]], n1: XY = [e1[1], -e1[0]]
    const m = unit([n0[0] + n1[0], n0[1] + n1[1], 0]), k = d / Math.max(0.35, m[0] * n0[0] + m[1] * n0[1])
    return [p[0] + m[0] * k, p[1] + m[1] * k]
  })
}

// ---------------------------------------------------------------------------
// Walls: a ring of segments, each a flat panel with recessed window bands
// between piers. Flush surfaces take the ring's smooth vertex normals so the
// rounded corners shade as curves.

type RingPt = { p: XY; n: XY }
type Band = [number, number]
type SegStyle = { mat: Part; bands: Band[]; bays?: number } | null

/**
 * `pierAt(i)` says whether a pier stands at ring vertex i. Window bands run
 * on unbroken across vertices without one, so a curved wall reads as long
 * ribbon windows between a few broad piers rather than a grid of dots.
 */
function walls(ring: RingPt[], z0: number, z1: number, style: (i: number, a: XY, b: XY) => SegStyle,
  pierAt: (i: number) => boolean = () => true, pier = 1.6, depth = 0.05) {
  const m = ring.length
  for (let i = 0; i < m; i++) {
    const A = ring[i], B = ring[(i + 1) % m]
    const s = style(i, A.p, B.p)
    if (!s) continue
    const L = Math.hypot(B.p[0] - A.p[0], B.p[1] - A.p[1])
    const sn = unit([B.p[1] - A.p[1], A.p[0] - B.p[0], 0])
    const nAt = (t: number): V3 => unit([A.n[0] * (1 - t) + B.n[0] * t, A.n[1] * (1 - t) + B.n[1] * t, 0])
    const P = (t: number, z: number, d = 0): V3 => { const q = lerp2(A.p, B.p, t); return [q[0] - sn[0] * d, q[1] - sn[1] * d, z] }
    const flush = (t0: number, t1: number, za: number, zb: number) => {
      if (t1 - t0 < 1e-6 || zb - za < 1e-6) return
      const n0 = nAt(t0), n1 = nAt(t1)
      quadN(s.mat, P(t0, za), P(t1, za), P(t1, zb), P(t0, zb), [n0, n1, n1, n0])
    }
    const bands = s.bands.filter(([lo, hi]) => hi > z0 && lo < z1)
    const bays = s.bays ?? 1
    if (!bands.length || L < 2 * pier + 1) { flush(0, 1, z0, z1); continue }
    const pw = Math.min(pier / L, 0.2 / bays)
    let last = 0
    for (let k = 0; k < bays; k++) {
      const left = k > 0 || pierAt(i), right = k < bays - 1 || pierAt((i + 1) % m)
      const t0 = k / bays + (left ? pw : 0), t1 = (k + 1) / bays - (right ? pw : 0)
      flush(last, t0, z0, z1)
      let z = z0
      for (const [lo, hi] of bands) {
        flush(t0, t1, z, lo)
        const a = P(t0, lo), b = P(t1, lo), c = P(t1, hi), d = P(t0, hi)
        const ar = P(t0, lo, depth), br = P(t1, lo, depth), cr = P(t1, hi, depth), dr = P(t0, hi, depth)
        quadN(dark, ar, br, cr, dr)                  // window glass
        quadN(s.mat, a, b, br, ar)                   // sill, facing up
        quadN(s.mat, dr, cr, c, d)                   // head, facing down
        if (left) quadN(s.mat, a, ar, dr, d)         // left jamb
        if (right) quadN(s.mat, br, b, c, cr)        // right jamb
        z = hi
      }
      flush(t0, t1, z, z1)
      last = t1
    }
    flush(last, 1, z0, z1)
  }
}

/** A 45° chamfered lip at the top of a ring of walls, then the roof cap. */
function lip(ring: RingPt[], z: number, d: number, mat: Part) {
  const m = ring.length
  for (let i = 0; i < m; i++) {
    const A = ring[i], B = ring[(i + 1) % m]
    const nA: V3 = unit([A.n[0], A.n[1], 1]), nB: V3 = unit([B.n[0], B.n[1], 1])
    quadN(mat, [A.p[0], A.p[1], z - d], [B.p[0], B.p[1], z - d],
      [B.p[0] - B.n[0] * d, B.p[1] - B.n[1] * d, z], [A.p[0] - A.n[0] * d, A.p[1] - A.n[1] * d, z], [nA, nB, nB, nA])
  }
}
const inset = (ring: RingPt[], d: number): XY[] => ring.map(r => [r.p[0] - r.n[0] * d, r.p[1] - r.n[1] * d])

// ---------------------------------------------------------------------------
// Plan shapes. A superellipse gives the arena's rounded-square bowl.

type Super = { cx: number; cy: number; a: number; b: number; n: number }
const sgn = (v: number) => (v < 0 ? -1 : 1)
function superPt(s: Super, th: number): RingPt {
  const c = Math.cos(th), si = Math.sin(th)
  const x = s.a * sgn(c) * Math.abs(c) ** (2 / s.n), y = s.b * sgn(si) * Math.abs(si) ** (2 / s.n)
  const gx = sgn(x) * Math.abs(x / s.a) ** (s.n - 1) / s.a, gy = sgn(y) * Math.abs(y / s.b) ** (s.n - 1) / s.b
  const g = unit([gx, gy, 0])
  return { p: [s.cx + x, s.cy + y], n: [g[0], g[1]] }
}
const thetaOf = (s: Super, x: number, y: number) => {
  const dx = (x - s.cx) / s.a, dy = (y - s.cy) / s.b
  return Math.atan2(sgn(dy) * Math.abs(dy) ** (s.n / 2), sgn(dx) * Math.abs(dx) ** (s.n / 2))
}
const halfY = (s: Super, x: number) => s.b * Math.max(0, 1 - Math.abs((x - s.cx) / s.a) ** s.n) ** (1 / s.n)
function superRing(s: Super, N: number, extraX: number[] = []): RingPt[] {
  const th = Array.from({ length: N }, (_, k) => (2 * Math.PI * k) / N)
  for (const x of extraX) for (const side of [-1, 1]) th.push(thetaOf(s, x, s.cy + side * halfY(s, x)))
  const norm = th.map(t => ((t % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI)).sort((a, b) => a - b)
  return norm.filter((t, i) => i === 0 || t - norm[i - 1] > 1e-3).map(t => superPt(s, t))
}
const insideSuper = (s: Super, p: XY, margin = 0) =>
  Math.abs((p[0] - s.cx) / (s.a - margin)) ** s.n + Math.abs((p[1] - s.cy) / (s.b - margin)) ** s.n < 1

// Core band (the 43 m high roof) runs the drum's long axis.
const XL = -45 + OX, XR = 21 + OX

// The main drum (OSM 35 m) and the lower bowl tier (OSM 23 m), both centred
// on the bowl as the OSM parts are.
const drum: Super = { cx: -11.5 + OX, cy: -13.5 + OY, a: 68.5, b: 68.5, n: 2.8 }
const tier: Super = { cx: -16 + OX, cy: -13.5 + OY, a: 82, b: 66, n: 2.6 }
const DRUM = 35, TIER = 23, PODIUM = 9

// The atrium wraps the south corner, Trade at Caldwell.
const ATRIUM: XY[] = [[14, -82], [46, -82], [57, -51], [44, -47], [24, -60], [14, -64]].map(p => at(p as XY))
const ATRIUM_TOP = 27

// ---------------------------------------------------------------------------
// Podium: the whole outline, simplified, with a storefront glass band.

const outline: XY[] = ([
  [-64, -81], [46, -82], [41, -71], [54, -54], [62, -48], [67, -51], [90, -51], [91, -34], [103, -34],
  [104, 17], [96, 31], [63, 65], [39, 45], [23, 55], [23, 82], [-13, 82], [-14, 64], [-38, 61], [-60, 51],
  [-79, 36], [-89, 20], [-96, 23], [-101, 8], [-104, -12], [-99, -28], [-93, -53], [-74, -42], [-57, -65],
  [-55, -70], [-62, -73],
] as XY[]).map(at)
{
  const m = outline.length
  for (let i = 0; i < m; i++) {
    const A = outline[i], B = outline[(i + 1) % m]
    const L = Math.hypot(B[0] - A[0], B[1] - A[1])
    const sn = unit([B[1] - A[1], A[0] - B[0], 0])
    const seg: RingPt[] = [{ p: A, n: [sn[0], sn[1]] }, { p: B, n: [sn[0], sn[1]] }]
    const inAtrium = (A[1] < -40 + OY && A[0] > 10 && B[1] < -40 + OY && B[0] > 10 && A[0] < 60 && B[0] < 60)
    walls([...seg], 0, PODIUM - 0.5, (k) => k === 0 && !inAtrium ? { mat: brick, bands: [[1.2, 6.2]], bays: Math.max(1, Math.round(L / 14)) } : null)
  }
  // Chamfered parapet and the flat roof.
  const top = offset(outline, -0.5)
  for (let i = 0; i < m; i++) {
    const j = (i + 1) % m
    const A = outline[i], B = outline[j], a = top[i], b = top[j]
    const sn = unit([B[1] - A[1], A[0] - B[0], 1])
    quadN(brick, [A[0], A[1], PODIUM - 0.5], [B[0], B[1], PODIUM - 0.5], [b[0], b[1], PODIUM], [a[0], a[1], PODIUM], [sn, sn, sn, sn])
  }
  capPoly(roof, top, PODIUM)
}

// The 14 m block on the 5th Street side (OSM way/773909125).
{
  const pts: XY[] = ([[-13, 55], [23, 55], [23, 82], [-13, 82]] as XY[]).map(at)
  for (let i = 0; i < 4; i++) {
    const A = pts[i], B = pts[(i + 1) % 4]
    const sn = unit([B[1] - A[1], A[0] - B[0], 0])
    walls([{ p: A, n: [sn[0], sn[1]] }, { p: B, n: [sn[0], sn[1]] }], PODIUM, 13.5,
      k => k === 0 && i !== 0 ? { mat: brick, bands: [[10, 12.6]], bays: 2 } : null)
  }
  const top = offset(pts, -0.5)
  for (let i = 0; i < 4; i++) {
    const A = pts[i], B = pts[(i + 1) % 4], a = top[i], b = top[(i + 1) % 4]
    const sn = unit([B[1] - A[1], A[0] - B[0], 1])
    quadN(brick, [A[0], A[1], 13.5], [B[0], B[1], 13.5], [b[0], b[1], 14], [a[0], a[1], 14], [sn, sn, sn, sn])
  }
  capPoly(roof, top, 14)
}

// ---------------------------------------------------------------------------
// Bowl tier: brick, two broad window bands, skipped where the drum hides it.

{
  const ring = superRing(tier, 44)
  walls(ring, PODIUM - 1, TIER - 0.5, (_, a, b) =>
    insideSuper(drum, a, 0.3) && insideSuper(drum, b, 0.3) ? null : { mat: brick, bands: [[11.5, 15.5], [18, 21]] }, i => i % 4 === 0)
  lip(ring, TIER, 0.5, brick)
  const top = inset(ring, 0.5)
  for (let i = 1; i < top.length - 1; i++) roof.tri([top[0][0], top[0][1], TIER], [top[i][0], top[i][1], TIER], [top[i + 1][0], top[i + 1][1], TIER])
}

// ---------------------------------------------------------------------------
// Main drum: brick with window bands where it meets the street, a dark louvre
// band under the roof edge, grey metal panel faces at the core's two ends.

const isEnd = (a: XY, b: XY) => (a[0] + b[0]) / 2 > XL && (a[0] + b[0]) / 2 < XR
const inAtriumZone = (a: XY, b: XY) => {
  const m: XY = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2]
  return m[0] > 16 + OX && m[1] < -42 + OY
}
const LOUVRE: Band = [26.5, 33.8]
const SLOT = 4
{
  const ring = superRing(drum, 44, [XL, XR])
  walls(ring, PODIUM - 1, DRUM - 0.6, (_, a, b) => {
    if (isEnd(a, b)) return { mat: panel, bands: [] }
    const exposed = !(insideSuper(tier, a, -0.3) && insideSuper(tier, b, -0.3))
    const bands: Band[] = exposed && !inAtriumZone(a, b) ? [[11.5, 15.5], [18, 21], LOUVRE] : [LOUVRE]
    return { mat: brick, bands }
  }, i => i % 4 === 0)
  // Pale metal roof edge: a projecting fascia with a bevelled top.
  const m = ring.length
  for (let i = 0; i < m; i++) {
    const A = ring[i], B = ring[(i + 1) % m]
    if (isEnd(A.p, B.p)) continue
    const o = 0.5
    const P = (r: RingPt, d: number, z: number): V3 => [r.p[0] + r.n[0] * d, r.p[1] + r.n[1] * d, z]
    const nA: V3 = [A.n[0], A.n[1], 0], nB: V3 = [B.n[0], B.n[1], 0]
    const uA = unit([A.n[0], A.n[1], 1]), uB = unit([B.n[0], B.n[1], 1])
    quadN(roof, P(A, 0, DRUM - 1.6), P(B, 0, DRUM - 1.6), P(B, o, DRUM - 1.3), P(A, o, DRUM - 1.3), [unit([A.n[0], A.n[1], -1]), unit([B.n[0], B.n[1], -1]), nB, nA])
    quadN(roof, P(A, o, DRUM - 1.3), P(B, o, DRUM - 1.3), P(B, o, DRUM - 0.5), P(A, o, DRUM - 0.5), [nA, nB, nB, nA])
    quadN(roof, P(A, o, DRUM - 0.5), P(B, o, DRUM - 0.5), P(B, 0, DRUM), P(A, 0, DRUM), [uA, uB, uB, uA])
  }
  for (let i = 0; i < m; i++) {
    const A = ring[i], B = ring[(i + 1) % m]
    if (!isEnd(A.p, B.p)) continue
    const n0: V3 = [A.n[0], A.n[1], 0], n1: V3 = [B.n[0], B.n[1], 0]
    quadN(panel, [A.p[0], A.p[1], DRUM - 0.6], [B.p[0], B.p[1], DRUM - 0.6], [B.p[0], B.p[1], DRUM], [A.p[0], A.p[1], DRUM], [n0, n1, n1, n0])
    // The tall glass slots where the panel face meets the brick: flush strips
    // just proud of the panel, one at each edge of each end face.
    for (const [from, to] of [[A, B], [B, A]] as [RingPt, RingPt][]) {
      if (Math.abs(from.p[0] - XL) > 1e-6 && Math.abs(from.p[0] - XR) > 1e-6) continue
      const L = Math.hypot(to.p[0] - from.p[0], to.p[1] - from.p[1]), t = Math.min(0.9, SLOT / L)
      const q = lerp2(from.p, to.p, t), sn = unit([B.p[1] - A.p[1], A.p[0] - B.p[0], 0]), e = 0.04
      const P = (p: XY, z: number): V3 => [p[0] + sn[0] * e, p[1] + sn[1] * e, z]
      const [a, b] = from === A ? [from.p, q] : [q, from.p]
      // Broken into storey groups with panel showing between, not one stripe.
      for (const [lo, hi] of [[PODIUM + 0.6, 17], [18.4, 25.6], [27, 33.4]])
        dark.quad(P(a, lo), P(b, lo), P(b, hi), P(a, hi))
    }
  }
  for (let i = 1; i < m - 1; i++)
    roof.tri([ring[0].p[0], ring[0].p[1], DRUM], [ring[i].p[0], ring[i].p[1], DRUM], [ring[i + 1].p[0], ring[i + 1].p[1], DRUM])
}

// ---------------------------------------------------------------------------
// High core: a roof that arches along the long axis (eaves 37 m at the ends,
// 43 m mid-span) and crowns 0.8 m across it. Dark louvre sides under a pale
// fascia; the ends continue the grey panel faces below.

{
  const COLS = 10, ROWS = 14, CROWN = 0.8
  const eave = (y: number) => { const t = (y - drum.cy) / drum.b; return 36.6 + 6.4 * (1 - t * t) }
  const z = (x: number, y: number) => { const s = (2 * (x - XL)) / (XR - XL) - 1; return eave(y) + CROWN * (1 - s * s) }
  const nrm = (x: number, y: number): V3 => {
    const t = (y - drum.cy) / drum.b, s = (2 * (x - XL)) / (XR - XL) - 1
    const dzdy = (-2 * 6.4 * t) / drum.b, dzdx = (-2 * CROWN * s * 2) / (XR - XL)
    return unit([-dzdx, -dzdy, 1])
  }
  const xs = Array.from({ length: COLS + 1 }, (_, k) => XL + ((XR - XL) * k) / COLS)
  const span = xs.map(x => [drum.cy - halfY(drum, x), drum.cy + halfY(drum, x)])
  for (let k = 0; k < COLS; k++) {
    for (let r = 0; r < ROWS; r++) {
      const pt = (c: number, rr: number): V3 => {
        const x = xs[c], y = span[c][0] + ((span[c][1] - span[c][0]) * rr) / ROWS
        return [x, y, z(x, y)]
      }
      const a = pt(k, r), b = pt(k + 1, r), c = pt(k + 1, r + 1), d = pt(k, r + 1)
      // The arched metal roof is the pale silver of the panel faces it
      // continues, the arena's crown; the flat roofs below stay `roof`.
      quadN(panel, a, b, c, d, [nrm(a[0], a[1]), nrm(b[0], b[1]), nrm(c[0], c[1]), nrm(d[0], d[1])])
    }
  }
  // Long sides.
  for (const [x, dir] of [[XL, -1], [XR, 1]] as [number, number][]) {
    const c = x === XL ? 0 : COLS
    for (let r = 0; r < ROWS; r++) {
      const y0 = span[c][0] + ((span[c][1] - span[c][0]) * r) / ROWS, y1 = span[c][0] + ((span[c][1] - span[c][0]) * (r + 1)) / ROWS
      const [ya, yb] = dir > 0 ? [y0, y1] : [y1, y0]
      const za = eave(ya), zb = eave(yb), F = 1.1
      quadN(dark, [x, ya, DRUM], [x, yb, DRUM], [x, yb, zb - F], [x, ya, za - F])
      // Fascia stands 0.4 m proud with a bevelled top edge.
      const o = 0.4 * dir, nO: V3 = [dir, 0, 0], nT = unit([dir, 0, 1]), nD = unit([dir, 0, -1])
      quadN(roof, [x, ya, za - F], [x, yb, zb - F], [x + o, yb, zb - F + 0.3], [x + o, ya, za - F + 0.3], [nD, nD, nO, nO])
      quadN(roof, [x + o, ya, za - F + 0.3], [x + o, yb, zb - F + 0.3], [x + o, yb, zb - 0.4], [x + o, ya, za - 0.4], [nO, nO, nO, nO])
      quadN(roof, [x + o, ya, za - 0.4], [x + o, yb, zb - 0.4], [x, yb, zb], [x, ya, za], [nT, nT, nT, nT])
    }
  }
  // Curved ends, panel, flush with the drum's panel faces below.
  for (const end of [0, 1]) {
    for (let k = 0; k < COLS; k++) {
      const [c0, c1] = end === 0 ? [k, k + 1] : [k + 1, k]
      const P = (c: number, h: number): V3 => [xs[c], span[c][end], h]
      const n = (c: number): V3 => superPt(drum, thetaOf(drum, xs[c], span[c][end])).n.concat(0) as V3
      const zt = (c: number) => z(xs[c], span[c][end])
      quadN(panel, P(c0, DRUM), P(c1, DRUM), P(c1, zt(c1)), P(c0, zt(c0)), [n(c0), n(c1), n(c1), n(c0)])
    }
  }
}

// ---------------------------------------------------------------------------
// Entrance atrium: a glass volume on the corner under a thin pale roof slab.

{
  const m = ATRIUM.length
  for (let i = 0; i < m; i++) {
    const A = ATRIUM[i], B = ATRIUM[(i + 1) % m]
    glass.quad([A[0], A[1], 0], [B[0], B[1], 0], [B[0], B[1], ATRIUM_TOP - 2.2], [A[0], A[1], ATRIUM_TOP - 2.2])
  }
  // Roof slab, flush with the glass and bevelled on top.
  const top = offset(ATRIUM, -0.5)
  for (let i = 0; i < m; i++) {
    const j = (i + 1) % m, A = ATRIUM[i], B = ATRIUM[j], a = top[i], b = top[j]
    const sn = unit([B[1] - A[1], A[0] - B[0], 0]), st = unit([sn[0], sn[1], 1])
    quadN(roof, [A[0], A[1], ATRIUM_TOP - 2.2], [B[0], B[1], ATRIUM_TOP - 2.2], [B[0], B[1], ATRIUM_TOP - 0.5], [A[0], A[1], ATRIUM_TOP - 0.5], [sn, sn, sn, sn])
    quadN(roof, [A[0], A[1], ATRIUM_TOP - 0.5], [B[0], B[1], ATRIUM_TOP - 0.5], [b[0], b[1], ATRIUM_TOP], [a[0], a[1], ATRIUM_TOP], [st, st, st, st])
  }
  capPoly(roof, top, ATRIUM_TOP)
}

// The shared palette (landmarks/STYLE.md). The red-brown brick is the
// arena's identity, so it keeps its hue as a finish pulled up to the
// palette's lightness; the silver panel faces and the arched roof they
// rise into are a pale metal finish.
const parts = [
  { part: brick, material: finish('spectrum-brick', 0xc98b7d) },
  { part: panel, material: finish('spectrum-panel', 0xc9ced2, 0.6) },
  { part: dark, material: PALETTE.window },
  { part: glass, material: PALETTE.glass },
  { part: roof, material: PALETTE.roof },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Spectrum Center', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 45, osm: 'way/773909122', footprint: [208, 164], height: 43.8,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/spectrum-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
