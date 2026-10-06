/**
 * First Presbyterian Church, 200 W Trade St, Charlotte — procedural, CC0-1.0,
 * no textures.
 * bun scripts/landmarks/first-presbyterian-charlotte.ts
 *
 * Only the historic church: the 1857 front, narthex and tower, the 1883–84
 * spire, and the 1894–95 nave, transepts and pulpit end. The Sunday School
 * buildings, Fellowship Hall and the 1961 office wing are plain, quieter
 * blocks that fill out the rest of the OSM outline, which maps the whole
 * complex as one way.
 *
 * Map frame: y along the church's axis, +y away from Trade Street (NE),
 * x across it, z up, metres. Placed at bearing 47.9°, the street grid's axis
 * and the outline's long edges, so the tower faces Trade Street on the
 * model's -y side. Origin is on the axis at the middle of the church's own
 * part of the OSM outline (way/502718740, which also covers every later wing).
 *
 * Dimensions: the 1857 front is 13.6 m wide (OSM; the nomination gives the
 * 1857 church as 50 × 80 ft), the tower projects 2.2 m from it, the nave runs
 * 40 m from the front to the pulpit end and the transepts sit 24.6–32.3 m back
 * (all OSM). The spire rises 64 m: 187 ft for the 1857 steeple (NRHP
 * nomination) plus the 25 ft the 1884 rebuild added (the church's history).
 * Eaves, ridge and tower stages are scaled from photos.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stucco = new Part(), trim = new Part(), glass = new Part(), door = new Part()
const slate = new Part(), spireSlate = new Part(), band = new Part()
const SIDES = [-1, 1] as const
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }
const cross3 = (a: V3, b: V3): V3 => [a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0]]
const dot3 = (a: V3, b: V3) => a[0]*b[0] + a[1]*b[1] + a[2]*b[2]
const sub3 = (a: V3, b: V3): V3 => [a[0]-b[0], a[1]-b[1], a[2]-b[2]]

/** Counter-clockwise from above, whatever order it was written in. */
function ccw(ring: XY[]): XY[] {
  let a = 0
  for (let i = 0; i < ring.length; i++) { const p = ring[i], q = ring[(i + 1) % ring.length]; a += p[0]*q[1] - q[0]*p[1] }
  return a < 0 ? [...ring].reverse() : ring
}

/** A flat convex polygon, wound to face `hint`; degenerate slivers dropped. */
function poly(p: Part, pts: V3[], hint: V3) {
  for (let i = 1; i < pts.length - 1; i++) {
    let a = pts[0], b = pts[i], c = pts[i + 1]
    const n = cross3(sub3(b, a), sub3(c, a))
    if (Math.hypot(...n) < 1e-7) continue
    if (dot3(n, hint) < 0) [b, c] = [c, b]
    p.tri(a, b, c)
  }
}

/**
 * A vertical prism over a convex ring. `bevel` trims every corner into a
 * chamfer whose normals blend the two faces it joins, so edges catch a soft
 * highlight instead of reading as a sharp extrusion.
 */
function prism(p: Part, ring: XY[], z0: number, z1: number, bevel = 0, top: Part | null = p) {
  ring = ccw(ring)
  const pts: XY[] = [], nrm: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const u = unit([b[0]-a[0], b[1]-a[1], 0]), v = unit([c[0]-b[0], c[1]-b[1], 0])
    const nu: V3 = [u[1], -u[0], 0], nv: V3 = [v[1], -v[0], 0]
    if (bevel > 0) {
      pts.push([b[0]-u[0]*bevel, b[1]-u[1]*bevel], [b[0]+v[0]*bevel, b[1]+v[1]*bevel]); nrm.push(nu, nv)
    } else { pts.push(b, b); nrm.push(nu, nv) }
  }
  for (let i = 0; i < pts.length; i++) {
    const j = (i + 1) % pts.length
    const a = pts[i], b = pts[j]
    if (Math.hypot(b[0]-a[0], b[1]-a[1]) < 1e-6) continue
    const ns = i % 2 ? [nrm[i], nrm[i]] : [nrm[i], nrm[j]]
    const A0: V3 = [a[0], a[1], z0], B0: V3 = [b[0], b[1], z0], A1: V3 = [a[0], a[1], z1], B1: V3 = [b[0], b[1], z1]
    p.tri(A0, B0, B1, undefined, undefined, undefined, [ns[0], ns[1], ns[1]])
    p.tri(A0, B1, A1, undefined, undefined, undefined, [ns[0], ns[1], ns[0]])
  }
  if (top) poly(top, pts.map(([x, y]) => [x, y, z1] as V3), [0, 0, 1])
}

/** A pyramid from a ring at height z to an apex. */
function spike(p: Part, ring: XY[], z: number, apex: V3) {
  ring = ccw(ring)
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    p.tri([a[0], a[1], z], [b[0], b[1], z], apex)
  }
}
const sq = (cx: number, cy: number, h: number, hy = h): XY[] =>
  [[cx-h, cy-hy], [cx+h, cy-hy], [cx+h, cy+hy], [cx-h, cy+hy]]
/** Octagon with flat faces towards the four axes. */
const oct = (cx: number, cy: number, apothem: number): XY[] => {
  const r = apothem / Math.cos(Math.PI / 8)
  return Array.from({ length: 8 }, (_, k) => {
    const a = Math.PI / 8 + k * Math.PI / 4
    return [cx + r * Math.cos(a), cy + r * Math.sin(a)] as XY
  })
}
/** A slim square shaft under a four-sided spike: the Gothic pinnacle. */
function pinnacle(cx: number, cy: number, z0: number, z1: number, half: number, tip: number, p = trim) {
  prism(p, sq(cx, cy, half), z0, z1, 0, null)
  spike(p, sq(cx, cy, half), z1, [cx, cy, tip])
}

type Opening = { at: XY; w: number; zb: number; zs: number; depth?: number; back?: Part }

/** Equilateral pointed arch: two arcs of radius = width meeting at the apex. */
function archOutline(s0: number, s1: number, zb: number, zs: number, n = 3): XY[] {
  const w = s1 - s0, out: XY[] = [[s0, zb], [s1, zb], [s1, zs]]
  for (let k = 1; k <= n; k++) { const t = k * Math.PI / 3 / n; out.push([s0 + w * Math.cos(t), zs + w * Math.sin(t)]) }
  for (let k = 1; k <= n; k++) { const t = 2 * Math.PI / 3 + k * Math.PI / 3 / n; out.push([s1 + w * Math.cos(t), zs + w * Math.sin(t)]) }
  return out
}

/**
 * The outward face of a wall from A to B (outside on the right), with
 * pointed-arch openings recessed into it: stucco reveals, a dark back.
 */
function wallFace(p: Part, A: XY, B: XY, z0: number, z1: number, openings: Opening[]) {
  const L = Math.hypot(B[0]-A[0], B[1]-A[1]), u: V3 = [(B[0]-A[0])/L, (B[1]-A[1])/L, 0]
  const n: V3 = [u[1], -u[0], 0]
  const v = (s: number, z: number, d = 0): V3 => [A[0] + u[0]*s - n[0]*d, A[1] + u[1]*s - n[1]*d, z]
  const panel = (s0: number, s1: number, za: number, zb: number) => {
    if (s1 - s0 > 1e-4 && zb - za > 1e-4) poly(p, [v(s0, za), v(s1, za), v(s1, zb), v(s0, zb)], n)
  }
  const mine = openings
    .map(o => ({ o, s: (o.at[0]-A[0])*u[0] + (o.at[1]-A[1])*u[1], off: (o.at[0]-A[0])*n[0] + (o.at[1]-A[1])*n[1] }))
    .filter(({ s, off }) => Math.abs(off) < .05 && s > 0 && s < L)
    .sort((a, b) => a.s - b.s)
  // Openings sharing a centre line stack into one column (a door under a
  // window); the wall fills around each.
  const columns: { s: number; list: Opening[] }[] = []
  for (const m of mine) {
    const col = columns.find(c => Math.abs(c.s - m.s) < .05)
    if (col) col.list.push(m.o); else columns.push({ s: m.s, list: [m.o] })
  }
  let last = 0
  for (const { s, list } of columns) {
    list.sort((a, b) => a.zb - b.zb)
    const half = Math.max(...list.map(o => o.w)) / 2, S0 = s - half, S1 = s + half
    panel(last, S0, z0, z1)
    let floor = z0
    for (const o of list) {
      const s0 = s - o.w / 2, s1 = s + o.w / 2, depth = o.depth ?? .5
      const out = archOutline(s0, s1, o.zb, o.zs, o.w >= 3 ? 3 : 2), apex = Math.max(...out.map(q => q[1]))
      panel(S0, S1, floor, o.zb)
      panel(S0, s0, o.zb, apex)
      panel(s1, S1, o.zb, apex)
      floor = apex
      for (let k = 2; k < out.length - 1; k++) {
        const a = out[k], b = out[k + 1]
        poly(p, [v(a[0], a[1]), v(a[0], apex), v(b[0], apex), v(b[0], b[1])], n)
      }
      for (let k = 0; k < out.length; k++) {
        const a = out[k], b = out[(k + 1) % out.length]
        const e: V3 = [b[0]-a[0], 0, b[1]-a[1]]
        if (Math.hypot(...e) < 1e-5) continue
        const inward: V3 = [u[0]*-e[2], u[1]*-e[2], e[0]]
        poly(p, [v(a[0], a[1]), v(b[0], b[1]), v(b[0], b[1], depth), v(a[0], a[1], depth)], unit(inward))
      }
      poly(o.back ?? glass, out.map(([ss, z]) => v(ss, z, depth)), n)
    }
    panel(S0, S1, floor, z1)
    last = S1
  }
  panel(last, L, z0, z1)
}

/** Walls of a convex block, with openings on whichever faces they sit on. */
function block(p: Part, ring: XY[], z0: number, z1: number, openings: Opening[] = [],
  opts: { cap?: Part | null; skip?: (mid: XY) => boolean } = {}) {
  ring = ccw(ring)
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    if (opts.skip?.([(a[0]+b[0])/2, (a[1]+b[1])/2])) continue
    wallFace(p, a, b, z0, z1, openings)
  }
  if (opts.cap) poly(opts.cap, ring.map(([x, y]) => [x, y, z1] as V3), [0, 0, 1])
}

/**
 * A pitched slate roof over a rectangle, ridge along y (or x), with stucco
 * gable ends. The roof overhangs the walls by `eave` so it reads as a lid.
 */
function gable(x0: number, x1: number, y0: number, y1: number, ze: number, zr: number, alongX = false,
  ends: [boolean, boolean] = [true, true], o = .35) {
  if (!alongX) {
    const xm = (x0 + x1) / 2, drop = o * (zr - ze) / ((x1 - x0) / 2)
    poly(slate, [[x0 - o, y0, ze - drop], [xm, y0, zr], [xm, y1, zr], [x0 - o, y1, ze - drop]], [-1, 0, 1])
    poly(slate, [[x1 + o, y0, ze - drop], [xm, y0, zr], [xm, y1, zr], [x1 + o, y1, ze - drop]], [1, 0, 1])
    if (ends[0]) poly(stucco, [[x0, y0, ze], [x1, y0, ze], [xm, y0, zr]], [0, -1, 0])
    if (ends[1]) poly(stucco, [[x0, y1, ze], [x1, y1, ze], [xm, y1, zr]], [0, 1, 0])
  } else {
    const ym = (y0 + y1) / 2, drop = o * (zr - ze) / ((y1 - y0) / 2)
    poly(slate, [[x0, y0 - o, ze - drop], [x0, ym, zr], [x1, ym, zr], [x1, y0 - o, ze - drop]], [0, -1, 1])
    poly(slate, [[x0, y1 + o, ze - drop], [x0, ym, zr], [x1, ym, zr], [x1, y1 + o, ze - drop]], [0, 1, 1])
    if (ends[0]) poly(stucco, [[x0, y0, ze], [x0, y1, ze], [x0, ym, zr]], [-1, 0, 0])
    if (ends[1]) poly(stucco, [[x1, y0, ze], [x1, y1, ze], [x1, ym, zr]], [1, 0, 0])
  }
}

// ---------------------------------------------------------------- dimensions
const FRONT = -18.9, REAR = 21.1          // 1857 front wall, 1895 pulpit end (OSM)
const W = 6.8                             // half width of the nave (OSM 13.6 m)
// Transepts: OSM shows the north-west arm standing 1.9 m proud of the nave;
// the south-east one is mirrored and runs into the Sunday School.
const TR0 = 5.7, TR1 = 13.4, TRX = 8.7
const EAVE = 12.5, RIDGE = 18
const T_HALF = 3.2, T_FRONT = -21.1, T_BACK = T_FRONT + 2 * T_HALF, T_CY = (T_FRONT + T_BACK) / 2
const T_TOP = 22.6                        // top of the tower shaft
const PARAPET = 24.2                      // top of the corbelled parapet band
const SPIRE_BASE = PARAPET, SPIRE_A = 2.45, SPIRE_TOP = 61.2, TIP = 64

// ---------------------------------------------------------------- nave
const naveBays = [-15.8, -9.7, -3.5, 2.6]
const naveButtress = [-12.75, -6.6, -0.45]
const naveOpen: Opening[] = []
for (const s of SIDES) {
  for (const y of naveBays) naveOpen.push({ at: [s * W, y], w: 2.1, zb: 3.2, zs: 8.6 })
  // The 1857 front either side of the tower: a tall lancet in each bay.
  naveOpen.push({ at: [s * 5.1, FRONT], w: 1.8, zb: 3.4, zs: 8.8 })
}
block(stucco, sq(0, (FRONT + TR0) / 2, W, (TR0 - FRONT) / 2), 0, EAVE, naveOpen,
  { skip: ([, y]) => y > TR0 - .1 })
for (const s of SIDES) for (const y of naveButtress) prism(stucco, sq(s * (W + .3), y, .3, .45), 0, EAVE - .4, .12)

// Pulpit end beyond the transepts, closed by a plain gable wall (later wings
// abut it, so it carries no windows).
const choirOpen: Opening[] = SIDES.map(s => ({ at: [s * W, (TR1 + REAR) / 2] as XY, w: 2.1, zb: 3.2, zs: 8.6 }))
block(stucco, sq(0, (TR1 + REAR) / 2, W, (REAR - TR1) / 2), 0, EAVE, choirOpen,
  { skip: ([, y]) => y < TR1 + .1 })
gable(-W, W, FRONT, REAR, EAVE, RIDGE, false, [true, true])

// ---------------------------------------------------------------- transepts
const transOpen: Opening[] = []
for (const s of SIDES) {
  transOpen.push({ at: [s * TRX, (TR0 + TR1) / 2], w: 3.6, zb: 3, zs: 8 })
}
// Each arm on its own, so its front and back walls stop at the nave.
for (const s of SIDES)
  block(stucco, [[s * W, TR0], [s * TRX, TR0], [s * TRX, TR1], [s * W, TR1]], 0, EAVE, transOpen,
    { skip: ([x]) => Math.abs(x) < W + .1 })
gable(-TRX, TRX, TR0, TR1, EAVE, RIDGE, true)

// Corner pinnacles: the front corners of the 1857 gable and the four outer
// corners of the transepts, each an octagonal turret capped by a spike.
const corners: XY[] = [[-W, FRONT], [W, FRONT]]
// Inset a little at the transepts so the turrets stay inside the outline.
for (const s of SIDES) corners.push([s * (TRX - .4), TR0 + .4], [s * (TRX - .4), TR1 - .4])
for (const [x, y] of corners) {
  prism(stucco, oct(x, y, .55), 0, EAVE + 1.6, .12, null)
  prism(trim, oct(x, y, .65), EAVE + 1.6, EAVE + 2.1, .1, trim)
  spike(trim, oct(x, y, .5), EAVE + 2.1, [x, y, EAVE + 6.4])
}
// Small pinnacles on the gable apexes.
pinnacle(0, REAR, RIDGE - .6, RIDGE + .8, .35, RIDGE + 3.2)
for (const s of SIDES) pinnacle(s * TRX, (TR0 + TR1) / 2, RIDGE - .6, RIDGE + .8, .35, RIDGE + 3.2)

// ---------------------------------------------------------------- tower
{
  const h = T_HALF, cy = T_CY
  const o: Opening[] = [
    // Front door under a tall traceried window, as on Trade Street.
    { at: [0, T_FRONT], w: 2.4, zb: 0, zs: 3.4, depth: .8, back: door },
    { at: [0, T_FRONT], w: 2.4, zb: 5.4, zs: 10.4, depth: .6 },
  ]
  // Louvred belfry lights on the three faces clear of the roof.
  o.push({ at: [0, T_FRONT], w: 1.7, zb: 16.6, zs: 19.4, depth: .4 })
  for (const s of SIDES) {
    o.push({ at: [s * h, cy], w: 1.7, zb: 16.6, zs: 19.4, depth: .4 })
    o.push({ at: [s * h, cy], w: 1.2, zb: 4.5, zs: 8.5, depth: .4 })
  }
  block(stucco, sq(0, cy, h), 0, T_TOP, o, { skip: ([, y]) => y > T_BACK - .1 })
  // Clasping buttresses at the front corners, stepping in under the parapet.
  for (const dx of SIDES) {
    prism(stucco, sq(dx * (h - .1), T_FRONT + .1, .5), 0, 14, .15)
    prism(stucco, sq(dx * (h - .05), T_FRONT + .05, .4), 14, T_TOP, .12)
  }
  // Corbelled parapet band, crenellated, with a pinnacle at each corner.
  prism(trim, sq(0, cy, h + .3), T_TOP, PARAPET, .25)
  const m = h + .3, t = .45
  for (const k of [-1.55, 0, 1.55]) {
    for (const s of SIDES) {
      prism(trim, [[k - .45, s * m - (s > 0 ? t : 0)], [k + .45, s * m - (s > 0 ? t : 0)], [k + .45, s * m + (s < 0 ? t : 0)], [k - .45, s * m + (s < 0 ? t : 0)]]
        .map(([x, y]) => [x, cy + y] as XY), PARAPET, PARAPET + .8, 0)
      prism(trim, [[s * m - (s > 0 ? t : 0), k - .45], [s * m + (s < 0 ? t : 0), k - .45], [s * m + (s < 0 ? t : 0), k + .45], [s * m - (s > 0 ? t : 0), k + .45]]
        .map(([x, y]) => [x, cy + y] as XY), PARAPET, PARAPET + .8, 0)
    }
  }
  for (const dx of SIDES) for (const dy of SIDES) {
    const px = dx * (h + .05), py = cy + dy * (h + .05)
    prism(trim, sq(px, py, .4), PARAPET, PARAPET + 1.6, .1, null)
    spike(trim, sq(px, py, .4), PARAPET + 1.6, [px, py, PARAPET + 6.2])
  }
}

// ---------------------------------------------------------------- spire
{
  const cy = T_CY
  const apothem = (z: number) => SPIRE_A + (.22 - SPIRE_A) * (z - SPIRE_BASE) / (SPIRE_TOP - SPIRE_BASE)
  const ring = (z: number, grow = 0) => oct(0, cy, apothem(z) + grow).map(([x, y]) => [x, y, z] as V3)
  // Octagonal slate spire, broken by pale trim bands as on the real one.
  const bands = [32, 42.5, 51.5]
  const levels = [SPIRE_BASE, ...bands.flatMap(z => [z, z + .7]), SPIRE_TOP]
  for (let i = 0; i < levels.length - 1; i++) {
    const isBand = i % 2 === 1
    if (isBand) {
      const z0 = levels[i], z1 = levels[i + 1]
      band.loft([ring(z0, .28), ring(z1, .2)])
      poly(band, ring(z0, .28).reverse(), [0, 0, -1])
      // Slope of the band top back to the spire face.
      band.loft([ring(z1, .2), ring(z1 + .01, 0)])
    } else spireSlate.loft([ring(levels[i]), ring(levels[i + 1])])
  }
  // Finial: a slim pale needle above the slate.
  const top = oct(0, cy, .22)
  spike(band, top, SPIRE_TOP, [0, cy, TIP])

  // Four gabled lucarnes on the cardinal faces at the spire's foot, slate
  // cheeks and pale fronts, each with a dark louvred lancet.
  for (const [dx, dy] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) {
    const px = -dy, py = dx, out = SPIRE_A + .55, half = 1.05, z0 = SPIRE_BASE, z1 = z0 + 4.8, zt = z1 + 2.7
    const P = (along: number, o: number, z: number): V3 => [dx * o + px * along, cy + dy * o + py * along, z]
    const nOut: V3 = [dx, dy, 0]
    // Front: wall rectangle plus gable triangle, with a flush dark lancet.
    poly(band, [P(-half, out, z0), P(half, out, z0), P(half, out, z1), P(-half, out, z1)], nOut)
    poly(band, [P(-half, out, z1), P(half, out, z1), P(0, out, zt)], nOut)
    const lw = .5, l0 = z0 + .7, l1 = z1 + .3
    poly(glass, [P(-lw, out + .02, l0), P(lw, out + .02, l0), P(lw, out + .02, l1), P(0, out + .02, l1 + .9), P(-lw, out + .02, l1)], nOut)
    // Cheeks and roof running back into the spire.
    for (const s of [-1, 1]) {
      poly(spireSlate, [P(s * half, out, z0), P(s * half, 0, z0), P(s * half, 0, z1), P(s * half, out, z1)], [px * s, py * s, 0])
      poly(spireSlate, [P(s * (half + .12), out + .1, z1 - .1), P(0, out + .1, zt), P(0, 0, zt), P(s * (half + .12), 0, z1 - .1)], [px * s, py * s, 1])
    }
  }
}

// ---------------------------------------------------------------- later wings
// The rest of way/502718740, so the model can stand in for the whole outline:
// the 1894–95 Sunday School and its 1916–17 doubling to the south-east, the
// 1952 Fellowship Hall and the 1961 office building to the north-west, and
// the white-roofed block on the 5th Street corner. Rectangles follow the
// outline's corners (model frame). Plain on purpose: a muted stucco, broad
// recessed window bays, and the roofs the aerials show, so the church and
// its spire stay the focus.
const wingWall = new Part(), wingGlass = new Part(), flat = new Part(), green = new Part()
type Rect = [number, number, number, number] // x0, y0, x1, y1
type Wing = { rects: Rect[]; h: number }
const inRect = ([x, y]: XY, [x0, y0, x1, y1]: Rect) => x > x0 && x < x1 && y > y0 && y < y1
// Footprints that hide a neighbour's wall where they meet, with their wall
// heights. The church goes first; its own walls are drawn above.
const solids: { rect: Rect; h: number }[] = [
  { rect: [-W, FRONT, W, REAR], h: EAVE },
  { rect: [-TRX, TR0, TRX, TR1], h: EAVE },
]

const office: Wing = { h: 10.5, rects: [
  [-48.9, -24.1, -18.3, 13.3], [-48.9, -26.4, -20.4, -24.1], [-18.3, 7, -16.6, 13.3]] }
const officePorches: Wing = { h: 9, rects: [[-33.9, -31.5, -24.1, -26.4], [-45.3, -29.5, -36.4, -26.4]] }
const hall: Wing = { h: 6.5, rects: [[-47.5, 13.3, -7.7, 29.4]] }
const hallAnnex: Wing = { h: 6.5, rects: [[-50.7, 19.1, -47.5, 29.4]] }
const school: Wing = { h: 10.5, rects: [[5.5, 4.9, 24.8, 32.1], [24.8, 18.6, 26.2, 32.1]] }
const schoolFront: Wing = { h: 9, rects: [[10.6, -1.9, 24.8, 4.9]] }
const schoolTower: Wing = { h: 14, rects: [[10.6, -1.9, 14.4, 1.9]] }
const link: Wing = { h: 8, rects: [[24.8, 12.2, 32.7, 18.6]] }
const corner: Wing = { h: 11, rects: [[32.7, -1.8, 48.5, 18.6], [48.5, 10.6, 49.6, 18.8], [31.4, 18.6, 48.7, 32]] }
const wings = [office, officePorches, hall, hallAnnex, school, schoolFront, schoolTower, link, corner]
for (const w of wings) for (const rect of w.rects) solids.push({ rect, h: w.h })

/**
 * One stretch of plain wall from A to B (outside on the right) with broad
 * window bays recessed into it, one per ~5 m, running nearly its full height.
 */
function bayWall(A: XY, B: XY, h: number) {
  const L = Math.hypot(B[0]-A[0], B[1]-A[1]), u: V3 = [(B[0]-A[0])/L, (B[1]-A[1])/L, 0]
  const n: V3 = [u[1], -u[0], 0], d = .4
  const v = (s: number, z: number, dd = 0): V3 => [A[0] + u[0]*s - n[0]*dd, A[1] + u[1]*s - n[1]*dd, z]
  const panel = (s0: number, s1: number, z0: number, z1: number) => {
    if (s1 - s0 > 1e-4 && z1 - z0 > 1e-4) poly(wingWall, [v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1)], n)
  }
  const count = L < 3 ? 0 : Math.max(1, Math.round(L / 5))
  const bw = Math.min(1.8, L / count - 1.4), z0 = 1.3, z1 = h - 1.5
  let last = 0
  for (let i = 0; i < count; i++) {
    const c = (i + .5) * L / count, s0 = c - bw / 2, s1 = c + bw / 2
    panel(last, s0, 0, h)
    panel(s0, s1, 0, z0)
    panel(s0, s1, z1, h)
    poly(wingWall, [v(s0, z0), v(s0, z1), v(s0, z1, d), v(s0, z0, d)], u)
    poly(wingWall, [v(s1, z0), v(s1, z1), v(s1, z1, d), v(s1, z0, d)], [-u[0], -u[1], 0])
    poly(wingWall, [v(s0, z1), v(s1, z1), v(s1, z1, d), v(s0, z1, d)], [0, 0, -1])
    poly(wingWall, [v(s0, z0), v(s1, z0), v(s1, z0, d), v(s0, z0, d)], [0, 0, 1])
    poly(wingGlass, [v(s0, z0, d), v(s1, z0, d), v(s1, z1, d), v(s0, z1, d)], n)
    last = s1
  }
  panel(last, L, 0, h)
}

// Walls: every rectangle edge, cut wherever another footprint starts or
// stops, and drawn only where nothing at least as tall stands against it.
const cutsX = [...new Set(solids.flatMap(s => [s.rect[0], s.rect[2]]))]
const cutsY = [...new Set(solids.flatMap(s => [s.rect[1], s.rect[3]]))]
for (const w of wings) for (const [x0, y0, x1, y1] of w.rects) {
  const edges: [XY, XY, XY][] = [ // from, to, outward
    [[x0, y0], [x1, y0], [0, -1]], [[x1, y0], [x1, y1], [1, 0]],
    [[x1, y1], [x0, y1], [0, 1]], [[x0, y1], [x0, y0], [-1, 0]],
  ]
  for (const [a, b, o] of edges) {
    const alongX = a[1] === b[1]
    const lo = alongX ? Math.min(a[0], b[0]) : Math.min(a[1], b[1]), hi = alongX ? Math.max(a[0], b[0]) : Math.max(a[1], b[1])
    const stops = [lo, ...(alongX ? cutsX : cutsY).filter(c => c > lo + .05 && c < hi - .05), hi].sort((p, q) => p - q)
    const forward = alongX ? b[0] > a[0] : b[1] > a[1]
    for (let i = 0; i < stops.length - 1; i++) {
      const m = (stops[i] + stops[i + 1]) / 2
      const probe: XY = alongX ? [m, a[1] + o[1] * .2] : [a[0] + o[0] * .2, m]
      if (solids.some(s => s.h >= w.h - .01 && inRect(probe, s.rect))) continue
      const p = (t: number): XY => alongX ? [t, a[1]] : [a[0], t]
      const [s0, s1] = forward ? [stops[i], stops[i + 1]] : [stops[i + 1], stops[i]]
      bayWall(p(s0), p(s1), w.h)
    }
  }
}

/** A slate mansard: slopes rising `rise` over `inset` to a flat top of `top`. */
function mansard([x0, y0, x1, y1]: Rect, z: number, rise: number, inset: number, top: Part) {
  const o = .3
  const lo: XY[] = [[x0 - o, y0 - o], [x1 + o, y0 - o], [x1 + o, y1 + o], [x0 - o, y1 + o]]
  const hi: XY[] = [[x0 + inset, y0 + inset], [x1 - inset, y0 + inset], [x1 - inset, y1 - inset], [x0 + inset, y1 - inset]]
  slate.loft([lo.map(([x, y]) => [x, y, z] as V3), hi.map(([x, y]) => [x, y, z + rise] as V3)])
  poly(slate, lo.map(([x, y]) => [x, y, z] as V3), [0, 0, -1])
  poly(top, hi.map(([x, y]) => [x, y, z + rise] as V3), [0, 0, 1])
}
const capRect = (p: Part, [x0, y0, x1, y1]: Rect, z: number) =>
  poly(p, [[x0, y0, z], [x1, y0, z], [x1, y1, z], [x0, y1, z]], [0, 0, 1])

// Office building: a slate mansard round a flat roof, as the aerials show.
mansard(office.rects[0], office.h, 2.4, 3.2, flat)
for (const r of office.rects.slice(1)) capRect(flat, r, office.h)
for (const r of officePorches.rects) capRect(flat, r, officePorches.h)
// Fellowship Hall: a steep slate roof, ridge across the block.
{
  const [x0, y0, x1, y1] = hall.rects[0], ym = (y0 + y1) / 2, ze = hall.h, zr = ze + 6.3, o = .3
  const drop = o * (zr - ze) / ((y1 - y0) / 2)
  poly(slate, [[x0 - o, y0 - o, ze - drop], [x0 - o, ym, zr], [x1 + o, ym, zr], [x1 + o, y0 - o, ze - drop]], [0, -1, 1])
  poly(slate, [[x0 - o, y1 + o, ze - drop], [x0 - o, ym, zr], [x1 + o, ym, zr], [x1 + o, y1 + o, ze - drop]], [0, 1, 1])
  poly(wingWall, [[x0, y0, ze], [x0, y1, ze], [x0, ym, zr]], [-1, 0, 0])
  poly(wingWall, [[x1, y0, ze], [x1, y1, ze], [x1, ym, zr]], [1, 0, 0])
  capRect(flat, hallAnnex.rects[0], hallAnnex.h)
}
// Sunday School: slate slopes round the pale green roof seen from above.
mansard(school.rects[0], school.h, 2.2, 3, green)
capRect(flat, school.rects[1], school.h)
capRect(flat, schoolFront.rects[0], schoolFront.h)
// Its small tower, with a plain coping.
{
  const [x0, y0, x1, y1] = schoolTower.rects[0]
  prism(trim, [[x0 - .15, y0 - .15], [x1 + .15, y0 - .15], [x1 + .15, y1 + .15], [x0 - .15, y1 + .15]], schoolTower.h, schoolTower.h + .6, .12, flat)
}
capRect(flat, link.rects[0], link.h)
for (const r of corner.rects) capRect(flat, r, corner.h)

// ---------------------------------------------------------------- write
const parts = [
  { part: stucco, material: { name: 'stucco', color: 0xd2ab84 } },
  { part: trim, material: { name: 'stucco-trim', color: 0xdcbc98 } },
  { part: glass, material: { name: 'window-glass', color: 0x4b5361 } },
  { part: door, material: { name: 'doors', color: 0x6a3f2a } },
  { part: slate, material: { name: 'slate-roof', color: 0x7d8086 } },
  { part: spireSlate, material: { name: 'spire-slate', color: 0x666a72 } },
  { part: band, material: { name: 'spire-trim', color: 0xcfcdc8 } },
  { part: wingWall, material: { name: 'wing-stucco', color: 0xc9b096 } },
  { part: wingGlass, material: { name: 'wing-windows', color: 0x7b8088 } },
  { part: flat, material: { name: 'flat-roof', color: 0xbdb9b1 } },
  { part: green, material: { name: 'green-roof', color: 0x94b3a7 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('First Presbyterian Church, Charlotte', parts, {
  license: 'CC0-1.0', bearing: 47.9, elevation: 0, anchor: [35.2291133, -80.843849], height: TIP,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: ['way/502718740'],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/first-presbyterian-charlotte.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
