/**
 * St. Patrick's Cathedral, Fifth Avenue — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/st-patricks-cathedral.ts
 *
 * Map frame: x across the church, y along its axis with +y towards the east
 * end (Madison Avenue), z up, metres. Placed at bearing 118.4°, so the twin
 * spires face Fifth Avenue on the model's -y side. Origin is on the axis at
 * the centroid of the OSM outline (way/266010379).
 *
 * Footprints follow OSM: the outline (way/266010379, ridge 42 m), the two
 * spire parts (way/161156112, way/161156113, 100.5 m) and the east end with
 * the ambulatory and lady chapel (way/266010403, 29.4 m). Everything is
 * mirrored about x = 0, as the building is.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stone = new Part(), trim = new Part(), glass = new Part()
const rose = new Part(), slate = new Part(), copper = new Part()
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
    // A chamfer (even i) blends its two neighbours; a straight face is flat.
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
/** A bevelled square shaft with a four-sided spike: the Gothic pinnacle. */
function pinnacle(cx: number, cy: number, z0: number, z1: number, half: number, tip: number) {
  // Too slim to show a bevel; the spike above carries the highlight.
  prism(stone, sq(cx, cy, half), z0, z1, 0, null)
  spike(stone, sq(cx, cy, half), z1, [cx, cy, tip])
}

type Opening = { at: XY; w: number; zb: number; zs: number; depth?: number; back?: Part; rose?: { z: number; r: number } }

/** Equilateral pointed arch: two arcs of radius = width meeting at the apex. */
function archOutline(s0: number, s1: number, zb: number, zs: number, n = 3): XY[] {
  const w = s1 - s0, out: XY[] = [[s0, zb], [s1, zb], [s1, zs]]
  for (let k = 1; k <= n; k++) { const t = k * Math.PI / 3 / n; out.push([s0 + w * Math.cos(t), zs + w * Math.sin(t)]) }
  for (let k = 1; k <= n; k++) { const t = 2 * Math.PI / 3 + k * Math.PI / 3 / n; out.push([s1 + w * Math.cos(t), zs + w * Math.sin(t)]) }
  return out
}

/** A disc lying in a wall plane, facing `n`. */
function disc(p: Part, c: V3, u: V3, n: V3, r: number, segs = 16) {
  const pts = Array.from({ length: segs }, (_, k): V3 => {
    const a = k * 2 * Math.PI / segs
    return [c[0] + u[0]*r*Math.cos(a), c[1] + u[1]*r*Math.cos(a), c[2] + r*Math.sin(a)]
  })
  poly(p, pts, n)
}

/**
 * The outward face of a wall from A to B (outside on the right), with
 * pointed-arch openings recessed into it: stone reveals, a dark glass back.
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
  // Openings sharing a centre line stack into one column (a portal under a
  // rose, a lancet under the belfry light); the wall fills around each.
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
      const s0 = s - o.w / 2, s1 = s + o.w / 2, depth = o.depth ?? .6
      // Two segments per side read as a pointed arch on a lancet; big ones get three.
      const out = archOutline(s0, s1, o.zb, o.zs, o.w >= 5 ? 3 : 2), apex = Math.max(...out.map(q => q[1]))
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
        // Reveals face into the opening: across the edge, towards its middle.
        const inward: V3 = [u[0]*-e[2], u[1]*-e[2], e[0]]
        poly(stone, [v(a[0], a[1]), v(b[0], b[1]), v(b[0], b[1], depth), v(a[0], a[1], depth)], unit(inward))
      }
      poly(o.back ?? glass, out.map(([ss, z]) => v(ss, z, depth)), n)
      if (o.rose) disc(rose, v(s, o.rose.z, depth - .04), u, n, o.rose.r)
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

/** A pitched roof over a rectangle, ridge along y (or x), stone gable ends. */
function gable(x0: number, x1: number, y0: number, y1: number, ze: number, zr: number, alongX = false,
  ends: [boolean, boolean] = [true, true], roof = slate) {
  if (!alongX) {
    const xm = (x0 + x1) / 2
    poly(roof, [[x0, y0, ze], [xm, y0, zr], [xm, y1, zr], [x0, y1, ze]], [-1, 0, 1])
    poly(roof, [[x1, y0, ze], [xm, y0, zr], [xm, y1, zr], [x1, y1, ze]], [1, 0, 1])
    if (ends[0]) poly(stone, [[x0, y0, ze], [x1, y0, ze], [xm, y0, zr]], [0, -1, 0])
    if (ends[1]) poly(stone, [[x0, y1, ze], [x1, y1, ze], [xm, y1, zr]], [0, 1, 0])
  } else {
    const ym = (y0 + y1) / 2
    poly(roof, [[x0, y0, ze], [x0, ym, zr], [x1, ym, zr], [x1, y0, ze]], [0, -1, 1])
    poly(roof, [[x0, y1, ze], [x0, ym, zr], [x1, ym, zr], [x1, y1, ze]], [0, 1, 1])
    if (ends[0]) poly(stone, [[x0, y0, ze], [x0, y1, ze], [x0, ym, zr]], [-1, 0, 0])
    if (ends[1]) poly(stone, [[x1, y0, ze], [x1, y1, ze], [x1, ym, zr]], [1, 0, 0])
  }
}

// ---------------------------------------------------------------- dimensions
const FRONT = -40.1, TOWER_BACK = -31.6
const NAVE = 7.5            // half width of the clerestory vessel
const EAVE = 30, RIDGE = 42  // OSM height 42
const AISLE = 18, AISLE_EAVE = 16.5, AISLE_TOP = 21
const TR0 = 2.6, TR1 = 25.8, TRV0 = 6.7, TRV1 = 21.7, TRX = 22.9 // transept
const CHOIR_END = 46.5, APSE_R = 7.5
const AMB = 17.8, AMB_END = 54.2
const LADY = 4.6, LADY_END = 73.9, LADY_TIP = 76.7, LADY_EAVE = 20, LADY_RIDGE = 29.4
const TOWER_C: XY = [14.9, -35.85], TOWER_HALF = 3.6, TOWER_TOP = 44
const SPIRE_BASE = 56, SPIRE_TIP = 100.5

const navebays = [-28.18, -21.34, -14.5, -7.66, -0.82]
const naveButtress = [-24.76, -17.92, -11.08, -4.24]
const choirBays = [29.25, 36.15, 43.05]
const choirButtress = [32.7, 39.6]

// ---------------------------------------------------------------- front
// The west front between the towers: great portal and the rose window.
const frontOpen: Opening[] = [
  { at: [0, FRONT], w: 8, zb: 0, zs: 9.6, depth: 1.2, back: rose },
  { at: [0, FRONT], w: 9, zb: 18, zs: 21.9, depth: .8, back: trim, rose: { z: 25, r: 3.8 } },
]
block(stone, sq(0, (FRONT + TOWER_BACK) / 2, 11.3, (TOWER_BACK - FRONT) / 2), 0, EAVE, frontOpen,
  { cap: stone, skip: ([x, y]) => Math.abs(x) > 11 || y > TOWER_BACK - .1 })
// Gable screen, 1.5 m thick, rising above the nave roof with a small rose.
{
  const y0 = FRONT, y1 = FRONT + 1.5, h = 11.3, top = 46.5
  poly(stone, [[-h, y0, EAVE], [h, y0, EAVE], [0, y0, top]], [0, -1, 0])
  poly(stone, [[-h, y1, EAVE], [h, y1, EAVE], [0, y1, top]], [0, 1, 0])
  for (const s of SIDES) poly(stone, [[s*h, y0, EAVE], [0, y0, top], [0, y1, top], [s*h, y1, EAVE]], [s, 0, 1])
  disc(rose, [0, y0 - .03, 36.2], [1, 0, 0], [0, -1, 0], 1.7, 12)
  pinnacle(0, y0 + .75, top - .6, top + 1, .5, top + 3.4)
}

// ---------------------------------------------------------------- towers
for (const s of SIDES) {
  const cx = s * TOWER_C[0], cy = TOWER_C[1], h = TOWER_HALF
  const o: Opening[] = []
  // Front portal, two tiers of lancets on the exposed faces.
  o.push({ at: [cx, cy - h], w: 3.2, zb: 0, zs: 6, back: rose })
  for (const [at, lower] of [[[cx, cy - h], true], [[cx + s*h, cy], true], [[cx, cy + h], false]] as [XY, boolean][]) {
    if (lower) o.push({ at, w: 2.6, zb: 12, zs: 17.5 })
    o.push({ at, w: 3.2, zb: 25, zs: 35.5 })
  }
  o.push({ at: [cx - s*h, cy], w: 2.6, zb: 31, zs: 37 })
  block(stone, sq(cx, cy, h), 0, TOWER_TOP, o)
  prism(stone, sq(cx, cy, h + .25), TOWER_TOP - .8, TOWER_TOP, .4)
  // Clasping corner buttresses that carry on up as the belfry pinnacles.
  for (const dx of SIDES) for (const dy of SIDES) {
    const bx = cx + dx * 3.4, by = cy + dy * 3.4
    prism(stone, sq(bx, by, .75), 0, 41, .35)
    spike(stone, sq(bx, by, .75), 41, [bx, by, 42.6])
    pinnacle(bx, by, 41.8, 51, .5, 58)
  }
  // Octagonal belfry, its cornice, then the plain octagonal spire.
  const belfry = oct(cx, cy, 3.5)
  const bo: Opening[] = []
  for (const [dx, dy] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) bo.push({ at: [cx + dx*3.5, cy + dy*3.5], w: 1.8, zb: 46, zs: 51, depth: .5 })
  block(stone, belfry, TOWER_TOP, SPIRE_BASE - 1, bo)
  prism(stone, oct(cx, cy, 3.75), SPIRE_BASE - 1, SPIRE_BASE, .3)
  // Gablets over the belfry openings, standing against the spire's foot.
  for (const [dx, dy] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) {
    const px = -dy, py = dx, a = 3.75, t = .5, half = 1.35, z0 = SPIRE_BASE, z1 = SPIRE_BASE + 4.2
    const P = (along: number, out: number, z: number): V3 => [cx + dx*out + px*along, cy + dy*out + py*along, z]
    poly(stone, [P(-half, a, z0), P(half, a, z0), P(0, a, z1)], [dx, dy, 0])
    poly(stone, [P(-half, a, z0), P(0, a, z1), P(0, a - t, z1), P(-half, a - t, z0)], [dx - px, dy - py, 1])
    poly(stone, [P(half, a, z0), P(0, a, z1), P(0, a - t, z1), P(half, a - t, z0)], [dx + px, dy + py, 1])
  }
  const spire = oct(cx, cy, 3.5)
  const r1 = oct(cx, cy, .2)
  stone.loft([spire.map(([x, y]) => [x, y, SPIRE_BASE] as V3), r1.map(([x, y]) => [x, y, SPIRE_TIP - 1.2] as V3)])
  spike(stone, r1, SPIRE_TIP - 1.2, [cx, cy, SPIRE_TIP])
  void belfry
}

// ---------------------------------------------------------------- nave, choir, apse
const clerestory: Opening[] = []
for (const s of SIDES) {
  for (const y of [...navebays, ...choirBays]) clerestory.push({ at: [s*NAVE, y], w: 3.4, zb: AISLE_TOP + 1, zs: 25.6 })
}
block(stone, sq(0, (TOWER_BACK + CHOIR_END) / 2, NAVE, (CHOIR_END - TOWER_BACK) / 2), 0, EAVE, clerestory,
  { skip: ([, y]) => y < TOWER_BACK + .1 || y > CHOIR_END - .1 })
gable(-NAVE, NAVE, FRONT + 1.5, CHOIR_END, EAVE, RIDGE, false, [false, false])
// Strip between the clerestory and the towers, behind the gable screen.
for (const s of SIDES) poly(slate, [[s*NAVE, FRONT + 1.5, EAVE], [s*11.3, FRONT + 1.5, EAVE], [s*11.3, TOWER_BACK, EAVE], [s*NAVE, TOWER_BACK, EAVE]], [0, 0, 1])

// Apse: half a 12-gon, each face with a lancet, under a conical slate roof.
{
  const n = 6, pts: XY[] = []
  for (let k = 0; k <= n; k++) { const a = k * Math.PI / n; pts.push([APSE_R * Math.cos(a), CHOIR_END + APSE_R * Math.sin(a)]) }
  for (let k = 0; k < n; k++) {
    const a = pts[k], b = pts[k + 1], m: XY = [(a[0]+b[0])/2, (a[1]+b[1])/2]
    wallFace(stone, a, b, 0, EAVE, [{ at: m, w: 2.2, zb: AISLE_TOP + 1, zs: 26 }])
    poly(slate, [[a[0], a[1], EAVE], [b[0], b[1], EAVE], [0, CHOIR_END, RIDGE]], [m[0], m[1] - CHOIR_END, 1])
  }
  for (let k = 1; k < n; k++) pinnacle(pts[k][0] * 1.06, CHOIR_END + (pts[k][1] - CHOIR_END) * 1.06, 24, 31.5, .45, 35)
}

// Clerestory piers with pinnacles along both flanks.
for (const s of SIDES) for (const y of [...naveButtress, TRV0 - 4.1, ...choirButtress, CHOIR_END]) {
  prism(stone, sq(s * (NAVE + .4), y, .45, .6), AISLE_TOP - 1, EAVE + .8, 0)
  pinnacle(s * (NAVE + .4), y, EAVE + .8, EAVE + 2.2, .4, EAVE + 5.5)
}

// ---------------------------------------------------------------- aisles
for (const s of SIDES) {
  const o: Opening[] = navebays.map(y => ({ at: [s*AISLE, y] as XY, w: 3.6, zb: 4, zs: 10.5 }))
  block(stone, [[s*NAVE, TOWER_BACK], [s*AISLE, TOWER_BACK], [s*AISLE, TR0], [s*NAVE, TR0]], 0, AISLE_EAVE, o,
    { skip: ([x, y]) => Math.abs(x) < NAVE + .1 || y < TOWER_BACK + .1 || y > TR0 - .1 })
  poly(copper, [[s*AISLE, TOWER_BACK, AISLE_EAVE], [s*NAVE, TOWER_BACK, AISLE_TOP], [s*NAVE, TR0, AISLE_TOP], [s*AISLE, TR0, AISLE_EAVE]], [s, 0, 1])
  for (const y of naveButtress) {
    prism(stone, sq(s * (AISLE + .5), y, .55, .75), 0, AISLE_EAVE + 1, .3, null)
    pinnacle(s * (AISLE + .5), y, AISLE_EAVE + 1, AISLE_EAVE + 4, .45, AISLE_EAVE + 8.5)
  }
}

// ---------------------------------------------------------------- transept
const transeptOpen: Opening[] = []
for (const s of SIDES) {
  transeptOpen.push({ at: [s*TRX, 14.2], w: 4.4, zb: 0, zs: 7, depth: 1, back: rose })
  transeptOpen.push({ at: [s*TRX, 14.2], w: 7, zb: 12.5, zs: 22.5, depth: .8 })
  for (const x of [11.6, 17.4]) for (const y of [TRV0, TRV1]) transeptOpen.push({ at: [s*x, y], w: 3.2, zb: AISLE_TOP + 1, zs: 25.6 })
}
block(stone, sq(0, (TRV0 + TRV1) / 2, TRX, (TRV1 - TRV0) / 2), 0, EAVE, transeptOpen)
gable(-TRX, TRX, TRV0, TRV1, EAVE, RIDGE, true)
for (const s of SIDES) {
  disc(rose, [s * (TRX + .03), 14.2, 34.2], [0, 1, 0], [s, 0, 0], 1.6, 12)
  // Turrets at the corners of each transept front.
  for (const y of [TRV0 + 1.1, TRV1 - 1.1]) {
    const c: XY = [s * (TRX - 1.1), y]
    prism(stone, oct(c[0], c[1], 1.1), 0, EAVE + 4, 0, null)
    spike(stone, oct(c[0], c[1], 1.1), EAVE + 4, [c[0], c[1], EAVE + 10])
  }
  // Transept aisles, under copper lean-tos falling away from the arm.
  for (const [ya, yb, high] of [[TR0, TRV0, TRV0], [TRV1, TR1, TRV1]]) {
    const low = high === TRV0 ? TR0 : TR1
    const o: Opening[] = [{ at: [s*TRX, (ya + yb) / 2], w: 2.4, zb: 4, zs: 10 },
      { at: [s*20.7, low], w: 2.8, zb: 4, zs: 10 }]
    block(stone, [[s*NAVE, ya], [s*TRX, ya], [s*TRX, yb], [s*NAVE, yb]], 0, AISLE_EAVE, o,
      { skip: ([x]) => Math.abs(x) < NAVE + .1 })
    poly(copper, [[s*NAVE, high, AISLE_TOP], [s*TRX, high, AISLE_TOP], [s*TRX, low, AISLE_EAVE], [s*NAVE, low, AISLE_EAVE]], [0, low > high ? 1 : -1, 1])
    poly(stone, [[s*TRX, high, AISLE_EAVE], [s*TRX, high, AISLE_TOP], [s*TRX, low, AISLE_EAVE]], [s, 0, 0])
  }
}

// ---------------------------------------------------------------- choir aisles and ambulatory
for (const s of SIDES) {
  const o: Opening[] = [...choirBays.slice(1).map(y => ({ at: [s*AMB, y] as XY, w: 3.4, zb: 4, zs: 10.5 })),
    { at: [s*14.6, AMB_END], w: 3, zb: 4, zs: 10 }]
  block(stone, [[s*NAVE, TR1], [s*AMB, TR1], [s*AMB, AMB_END], [s*NAVE, AMB_END]], 0, AISLE_EAVE, o,
    { skip: ([x, y]) => Math.abs(x) < NAVE + .1 || y < TR1 + .1 })
  poly(copper, [[s*AMB, TR1, AISLE_EAVE], [s*NAVE, TR1, AISLE_TOP], [s*NAVE, AMB_END, AISLE_TOP], [s*AMB, AMB_END, AISLE_EAVE]], [s, 0, 1])
  poly(stone, [[s*AMB, AMB_END, AISLE_EAVE], [s*NAVE, AMB_END, AISLE_TOP], [s*NAVE, AMB_END, AISLE_EAVE]], [0, 1, 0])
  poly(stone, [[s*NAVE, CHOIR_END, AISLE_EAVE], [s*NAVE, AMB_END, AISLE_EAVE], [s*NAVE, AMB_END, AISLE_TOP], [s*NAVE, CHOIR_END, AISLE_TOP]], [-s, 0, 0])
  // The sacristy wing that carries the transept's width east (OSM).
  block(stone, [[s*AMB, TR1], [s*TRX, TR1], [s*TRX, 33.8], [s*AMB, 33.8]], 0, AISLE_EAVE - 1,
    [{ at: [s*TRX, 29.8], w: 2.6, zb: 3.5, zs: 9 }], { cap: copper, skip: ([x]) => Math.abs(x) < AMB + .1 })
  for (const y of [...choirButtress, CHOIR_END, AMB_END - .6]) {
    prism(stone, sq(s * (AMB + .45), y, .5, .7), 0, AISLE_EAVE + 1, .3, null)
    pinnacle(s * (AMB + .45), y, AISLE_EAVE + 1, AISLE_EAVE + 4, .42, AISLE_EAVE + 8.5)
  }
}
block(stone, sq(0, (CHOIR_END + AMB_END) / 2, NAVE, (AMB_END - CHOIR_END) / 2), 0, AISLE_EAVE, [],
  { cap: copper, skip: () => true })

// ---------------------------------------------------------------- lady chapel
{
  const ring: XY[] = [[LADY, AMB_END], [LADY, LADY_END], [2.3, LADY_TIP], [-2.3, LADY_TIP], [-LADY, LADY_END], [-LADY, AMB_END]]
  const o: Opening[] = []
  for (const s of SIDES) {
    o.push({ at: [s*LADY, 57.9], w: 2.4, zb: 13.8, zs: 17 })
    for (const y of [64.7, 70.9]) o.push({ at: [s*LADY, y], w: 3, zb: 4, zs: 13 })
    o.push({ at: [s*3.45, (LADY_END + LADY_TIP) / 2], w: 2, zb: 4, zs: 13.5 })
  }
  o.push({ at: [0, LADY_TIP], w: 2.8, zb: 4, zs: 13 })
  block(stone, ring, 0, LADY_EAVE, o, { skip: ([, y]) => y < AMB_END + .1 })
  gable(-LADY, LADY, AMB_END, LADY_END, LADY_EAVE, LADY_RIDGE, false, [true, false], copper)
  poly(copper, [[LADY, LADY_END, LADY_EAVE], [2.3, LADY_TIP, LADY_EAVE], [0, LADY_END, LADY_RIDGE]], [1, 1, 1])
  poly(copper, [[-LADY, LADY_END, LADY_EAVE], [-2.3, LADY_TIP, LADY_EAVE], [0, LADY_END, LADY_RIDGE]], [-1, 1, 1])
  poly(copper, [[2.3, LADY_TIP, LADY_EAVE], [-2.3, LADY_TIP, LADY_EAVE], [0, LADY_END, LADY_RIDGE]], [0, 1, 1])
  for (const s of SIDES) {
    for (const y of [61.5, 67.8, LADY_END]) {
      prism(stone, sq(s * (LADY + .4), y, .4, .55), 0, LADY_EAVE + .5, .2, null)
      pinnacle(s * (LADY + .4), y, LADY_EAVE + .5, LADY_EAVE + 2.5, .35, LADY_EAVE + 6)
    }
    // Radiating chapels flanking the lady chapel's west end.
    block(stone, [[s*LADY, AMB_END], [s*11.5, AMB_END], [s*11.5, 61.5], [s*LADY, 61.5]], 0, 13,
      [{ at: [s*11.5, 57.85], w: 2.4, zb: 3, zs: 8 }, { at: [s*8.05, 61.5], w: 2.4, zb: 3, zs: 8 }],
      { cap: copper, skip: ([x, y]) => Math.abs(x) < LADY + .1 || y < AMB_END + .1 })
  }
  // Copper flèche on the ridge.
  prism(copper, oct(0, 70.5, .9), LADY_RIDGE - 2, LADY_RIDGE + 2.5, .1, null)
  spike(copper, oct(0, 70.5, .9), LADY_RIDGE + 2.5, [0, 70.5, LADY_RIDGE + 10])
}

// ---------------------------------------------------------------- write
const parts = [
  { part: stone, material: { name: 'marble', color: 0xd9d1c0 } },
  { part: trim, material: { name: 'marble-shadow', color: 0xb5ab98 } },
  { part: glass, material: { name: 'window', color: 0x5a6672 } },
  { part: rose, material: { name: 'window-2', color: 0x3d4757 } },
  { part: slate, material: { name: 'slate-roof', color: 0x565c63 } },
  { part: copper, material: { name: 'copper-roof', color: 0x7fa595 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb("St. Patrick's Cathedral", parts, {
  license: 'CC0-1.0', bearing: 118.4, elevation: 0, anchor: [40.758574, -73.976317], height: SPIRE_TIP,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: ['way/266010379', 'way/161156112', 'way/161156113', 'way/266010403'],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/st-patricks-cathedral.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
