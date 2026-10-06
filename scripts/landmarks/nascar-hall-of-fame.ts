/**
 * NASCAR Hall of Fame, Charlotte — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/nascar-hall-of-fame.ts
 *
 * Map frame: x across the block (+x towards South Caldwell Street, south-east),
 * y along it (+y towards the plaza and East MLK Jr Boulevard, north-east), z up,
 * metres. Placed at bearing 55°, the long axis of the OSM outline. The anchor is
 * the area centroid of the outline (way/322278731); every coordinate below is
 * that outline and its building:parts measured in this rotated frame.
 *
 * What makes it read as itself is the stainless-steel "racetrack" ribbon: a
 * banked band, leaning outward like a track's banking, wrapped round the oval.
 * It is broken twice in plan — by the NASCAR Plaza office tower on the
 * south-east side (a separate building, not modelled) and by the pale stone
 * block on the north-west side — so it is two sweeps:
 *
 *  - east: from the tower round the Great Hall's rounded end, lifting over the
 *    glass front on the plaza, then leaving the wall as a cantilevered blade
 *    (OSM's metal building:part) that ends against the stone block;
 *  - west: from a pointed tip at the stone block round the west end, ending
 *    against the tower.
 *
 * Under the ribbon the walls stand back by the depth of the banking: pale stone,
 * and glass on the plaza front. The ballroom box (25 m) rises out of the roof.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stone = new Part(), panels = new Part(), glass = new Part()
const ribbon = new Part(), soffit = new Part(), roof = new Part()

// ---------------------------------------------------------------------------
// Small vector helpers.

const unit3 = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const cross3 = (a: V3, b: V3): V3 => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]
const dot3 = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const sub3 = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
const smooth = (t: number) => { const c = Math.min(1, Math.max(0, t)); return c * c * (3 - 2 * c) }

/** A quad with per-corner normals; the winding is chosen to face the normals. */
function quad(p: Part, P: V3[], N: V3[]) {
  const n = N.map(unit3)
  const avg: V3 = [0, 0, 0]
  for (const v of n) for (let k = 0; k < 3; k++) avg[k] += v[k]
  const face = cross3(sub3(P[1], P[0]), sub3(P[2], P[0]))
  const o = dot3(face, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  const tri = (a: number, b: number, c: number) => {
    const area = Math.hypot(...cross3(sub3(P[b], P[a]), sub3(P[c], P[a])))
    if (area > 1e-8) p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [n[a], n[b], n[c]])
  }
  tri(o[0], o[1], o[2])
  tri(o[0], o[2], o[3])
}

// ---------------------------------------------------------------------------
// Polygons: counter-clockwise from above.

const area = (poly: XY[]) => poly.reduce((s, p, i) => {
  const q = poly[(i + 1) % poly.length]
  return s + p[0] * q[1] - q[0] * p[1]
}, 0) / 2
const ccw = (poly: XY[]) => (area(poly) < 0 ? [...poly].reverse() : poly)

/** Ear clipping, for the concave roof of the oval. */
function earClip(poly: XY[]): [number, number, number][] {
  const idx = poly.map((_, i) => i)
  const out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) =>
    crossz(a, b, p) > 1e-9 && crossz(b, c, p) > 1e-9 && crossz(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let clipped = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = poly[i0], b = poly[i1], c = poly[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some(j => j !== i0 && j !== i1 && j !== i2 && inside(poly[j], a, b, c))) continue
      out.push([i0, i1, i2])
      idx.splice(k, 1)
      clipped = true
      break
    }
    if (!clipped) idx.splice(0, 1) // degenerate sliver: drop a collinear point
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

function cap(p: Part, poly: XY[], z: number) {
  for (const [a, b, c] of earClip(poly))
    p.tri([...poly[a], z], [...poly[b], z], [...poly[c], z], undefined, undefined, undefined, [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
}

/** Outward unit normal of each edge, and a mitred inset of each vertex. */
function edgeNormal(a: XY, b: XY): XY {
  const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1
  return [dy / l, -dx / l]
}
function inset(poly: XY[], d: number): XY[] {
  const n = poly.length
  return poly.map((p, i) => {
    const a = edgeNormal(poly[(i + n - 1) % n], p), b = edgeNormal(p, poly[(i + 1) % n])
    const m: XY = [a[0] + b[0], a[1] + b[1]]
    const l = Math.hypot(...m) || 1
    const cos = Math.max(0.5, (m[0] * a[0] + m[1] * a[1]) / l) // limit the mitre
    return [p[0] - (m[0] / l) * (d / cos), p[1] - (m[1] / l) * (d / cos)]
  })
}

/**
 * A prism with a rounded top edge: vertical walls (material per edge and per
 * height band), a bevel rolling over to the roof, and a flat roof.
 */
type Band = { p: Part; z0: number; z1: number; recess?: number }
function prism(poly: XY[], z0: number, z1: number, walls: (a: XY, b: XY) => Band[], top: Part, bevel = 0.45) {
  poly = ccw(poly)
  const n = poly.length, zb = z1 - bevel
  const lip = inset(poly, bevel)
  const all = poly.map((a, i) => walls(a, poly[(i + 1) % n]))
  const recessed = all.map(bands => bands.some(b => b.recess))
  const RECESS = 0.5
  const back = inset(poly, RECESS) // mitred, so neighbouring recessed bays meet
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, a = poly[i], b = poly[j]
    const nm = edgeNormal(a, b), N: V3 = [nm[0], nm[1], 0]
    const bands = all[i]
    for (const band of bands) {
      const lo = Math.max(z0, band.z0), hi = Math.min(zb, band.z1)
      if (hi <= lo) continue
      if (!band.recess) {
        quad(band.p, [[...a, lo], [...b, lo], [...b, hi], [...a, hi]], [N, N, N, N])
        continue
      }
      const A = recessed[(i + n - 1) % n] ? back[i] : ([a[0] - nm[0] * RECESS, a[1] - nm[1] * RECESS] as XY)
      const B = recessed[j] ? back[j] : ([b[0] - nm[0] * RECESS, b[1] - nm[1] * RECESS] as XY)
      quad(band.p, [[...A, lo], [...B, lo], [...B, hi], [...A, hi]], [N, N, N, N])
      // Sills above and below a recessed band, and jambs where it starts and
      // stops, in the surrounding wall's stone.
      const U: V3 = [0, 0, 1], D: V3 = [0, 0, -1]
      quad(stone, [[...a, lo], [...b, lo], [...B, lo], [...A, lo]], [U, U, U, U])
      quad(stone, [[...a, hi], [...b, hi], [...B, hi], [...A, hi]], [D, D, D, D])
      const t: V3 = [b[0] - a[0], b[1] - a[1], 0]
      if (!recessed[(i + n - 1) % n]) quad(stone, [[...a, lo], [...A, lo], [...A, hi], [...a, hi]], [t, t, t, t].map(unit3))
      if (!recessed[j]) { const m = unit3([-t[0], -t[1], 0]); quad(stone, [[...b, lo], [...B, lo], [...B, hi], [...b, hi]], [m, m, m, m]) }
    }
    const mid = unit3([nm[0], nm[1], 1])
    quad(bands.at(-1)!.p === glass ? stone : bands.at(-1)!.p, [[...a, zb], [...b, zb], [...lip[j], z1], [...lip[i], z1]],
      [unit3([nm[0], nm[1], 0.3]), unit3([nm[0], nm[1], 0.3]), mid, mid])
  }
  cap(top, lip, z1)
}

// ---------------------------------------------------------------------------
// OSM geometry in the model frame (bearing 55°, origin at the outline's
// centroid).

/** The rounded east end and SE side, tower corner to the blade's root. */
const EAST: XY[] = [
  [51.5, -5.0], [52.7, 4.3], [53.3, 13.5], [54.3, 23.3], [54.2, 30.8], [54.3, 39.4], [53.0, 53.3], [51.6, 62.5],
  [50.1, 69.6], [48.9, 73.6], [45.2, 82.2], [41.4, 87.9], [37.9, 92.0], [34.1, 94.8], [29.9, 97.2], [25.8, 98.7],
  [21.8, 99.6], [17.7, 100.0], [13.9, 99.6], [8.5, 98.4], [3.7, 96.3], [1.0, 94.1],
]
/** The blade's outer edge (way/766639184), root to tip at the stone block. */
const BLADE: XY[] = [[-1.4, 93.8], [-3.8, 92.6], [-7.8, 89.2], [-12.9, 83.7], [-17.1, 79.0], [-21.2, 72.3], [-23.8, 66.2], [-26.1, 60.3], [-27.2, 56.0]]
/** The north-west side and the west end, stone block to tower. */
const WEST: XY[] = [
  [-44.1, 7.6], [-43.8, 1.4], [-43.5, -4.5], [-44.0, -10.5], [-44.8, -21.9], [-45.0, -35.6], [-44.4, -48.4], [-43.2, -60.5],
  [-41.6, -69.8], [-38.7, -78.4], [-36.2, -83.9], [-32.8, -90.0], [-27.6, -96.3], [-24.5, -99.0], [-20.9, -101.5],
  [-18.7, -102.7], [-15.2, -104.2], [-12.0, -104.9], [-9.2, -105.4], [-4.2, -105.7], [0.4, -105.1], [4.5, -104.0],
  [7.8, -102.7], [10.8, -100.9], [14.1, -98.7], [18.5, -94.6], [22.7, -89.9], [26.1, -85.2], [28.6, -80.7], [30.3, -77.2],
]
/** The glass front between the blade's root and the stone block: the outline itself. */
const FRONT: XY[] = [[-1.9, 91.0], [-5.1, 87.2], [-8.3, 82.5], [-10.5, 78.9], [-12.4, 74.4], [-13.9, 69.9], [-15.0, 64.0], [-15.3, 58.8], [-14.9, 53.7]]
/** Along the tower's north-west face, west to east. */
const TOWER_SIDE: XY[] = [
  [33.3, -77.1], [32.4, -64.2], [28.7, -64.5], [27.3, -56.4], [26.1, -43.6], [25.9, -29.3], [26.2, -18.0], [27.1, -7.7],
  [27.6, -3.1], [28.0, 0.0], [45.0, -1.7], [45.3, -6.5],
]
const BLOCK: XY[] = [[-22.9, 9.5], [-44.1, 7.6], [-45.6, 23.8], [-49.9, 71.7], [-28.8, 73.7], [-27.2, 56.0], [-26.8, 52.4]] // way/1352128758, 21 m
const BALLROOM: XY[] = [[-21.1, -82.3], [24.2, -77.8], [18.1, -9.0], [-27.3, -13.2]] // way/1352128756, 25 m
const LINK: XY[] = [[27.6, -3.1], [17.7, -3.8], [18.1, -9.0], [24.2, -77.8], [30.3, -77.2], [26.1, -43.6], [27.1, -7.7]] // way/1352128757, 21 m (simplified)
const STEPS_W: XY[] = [[-36.2, -83.9], [-34.2, -102.5], [-20.9, -101.5], [-27.6, -96.3]] // way/1352128751, 13 m
const STEPS_E: XY[] = [[14.1, -98.7], [23.2, -97.7], [22.7, -89.9], [18.5, -94.6]] // way/1352128752, 13 m

// ---------------------------------------------------------------------------
// Paths: Chaikin-smoothed, resampled evenly, with an outward normal per sample
// (the paths run counter-clockwise round the building, so outward is to the
// right of travel).

type Sample = { p: XY; n: XY; s: number }
function path(pts: XY[], step = 2.6): Sample[] {
  let q = pts
  for (let it = 0; it < 2; it++) {
    const r: XY[] = [q[0]]
    for (let i = 0; i < q.length - 1; i++) {
      const a = q[i], b = q[i + 1]
      r.push([0.75 * a[0] + 0.25 * b[0], 0.75 * a[1] + 0.25 * b[1]], [0.25 * a[0] + 0.75 * b[0], 0.25 * a[1] + 0.75 * b[1]])
    }
    r.push(q.at(-1)!)
    q = r
  }
  const cum = [0]
  for (let i = 1; i < q.length; i++) cum.push(cum[i - 1] + Math.hypot(q[i][0] - q[i - 1][0], q[i][1] - q[i - 1][1]))
  const L = cum.at(-1)!, n = Math.max(2, Math.round(L / step))
  const at = (s: number): XY => {
    let i = 1
    while (i < q.length - 1 && cum[i] < s) i++
    const t = (s - cum[i - 1]) / (cum[i] - cum[i - 1] || 1)
    return [q[i - 1][0] + (q[i][0] - q[i - 1][0]) * t, q[i - 1][1] + (q[i][1] - q[i - 1][1]) * t]
  }
  const ps = Array.from({ length: n + 1 }, (_, k) => at((k * L) / n))
  return ps.map((p, k) => {
    const a = ps[Math.max(0, k - 1)], b = ps[Math.min(n, k + 1)]
    const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1
    return { p, n: [dy / l, -dx / l] as XY, s: (k * L) / n }
  })
}

const ROOF = 20
const WALL = 4.2 // the stone base stands this far inside the bowl's top edge
const off = (m: Sample, d: number): XY => [m.p[0] + m.n[0] * d, m.p[1] + m.n[1] * d]

/**
 * The steel bowl wall. Its outer face is a loft from the foot, on the stone
 * base's wall line (`-foot` inside the path), out to the top edge on the path
 * itself, so it flares outward as it rises like an upturned bowl or a banked
 * track wall. A thick rounded lip, then the inside face runs back down to the
 * roof. Where the band leaves the wall as the blade (`free`), it is a slim
 * section of its own depth with a dark underside.
 */
type Profile = { zb: number; zt: number; foot: number; free?: number }
function sweepRibbon(S: Sample[], profile: (m: Sample) => Profile) {
  const LIP = 3
  const sections = S.map(m => {
    const { zb, zt, foot, free = 0 } = profile(m)
    const h = Math.max(0.01, zt - zb), bev = Math.min(0.6, h * 0.3)
    const P = (d: number, z: number): V3 => [m.p[0] + m.n[0] * d, m.p[1] + m.n[1] * d, z]
    const face = unit3([m.n[0] * h, m.n[1] * h, -foot]) // outward and down
    // Inside: back down to just over the roof at the wall line; for the free
    // blade, a short vertical back face instead.
    const backD = -(WALL + 0.3) + free * (WALL + 0.3 - LIP - 0.4)
    const backZ = Math.max(zb + 0.6, ROOF + 0.05 + free * (zt - 1.2 - ROOF))
    const backVec: V3 = [m.n[0] * (backD + LIP), m.n[1] * (backD + LIP), backZ - zt]
    const inward = unit3(cross3(backVec, [-m.n[1], m.n[0], 0]))
    return {
      m, zb, zt, foot,
      footP: P(-foot, zb), b0: P(-foot * (bev / h), zt - bev), b1: P(-bev, zt), lip: P(-LIP, zt), back: P(backD, backZ),
      under: P(Math.min(-foot, backD) - 0.2, Math.min(zb + 0.6, backZ)),
      face, up: [0, 0, 1] as V3, roll: unit3([face[0], face[1], face[2] + 1.4]),
      inward: dot3(inward, [0, 0, 1]) >= 0 ? inward : ([-inward[0], -inward[1], -inward[2]] as V3),
      down: unit3([-m.n[0] * 0.6, -m.n[1] * 0.6, -1]),
    }
  })
  for (let k = 0; k < sections.length - 1; k++) {
    const a = sections[k], b = sections[k + 1]
    quad(ribbon, [a.footP, a.b0, b.b0, b.footP], [a.face, a.face, b.face, b.face])
    quad(ribbon, [a.b0, a.b1, b.b1, b.b0], [a.roll, a.up, b.up, b.roll])
    quad(ribbon, [a.b1, a.lip, b.lip, b.b1], [a.up, a.up, b.up, b.up])
    quad(ribbon, [a.lip, a.back, b.back, b.lip], [a.inward, a.inward, b.inward, b.inward])
    quad(soffit, [a.back, a.under, b.under, b.back], [a.down, a.down, b.down, b.down])
    quad(soffit, [a.under, a.footP, b.footP, b.under], [a.down, a.down, b.down, b.down])
  }
  for (const [c, next] of [[sections[0], sections[1]], [sections.at(-1)!, sections.at(-2)!]]) {
    const t = unit3(sub3(c.footP, next.footP))
    const ring = [c.under, c.footP, c.b0, c.b1, c.lip, c.back]
    for (let i = 1; i < ring.length - 1; i++) {
      const tri = [ring[0], ring[i], ring[i + 1]]
      const f = cross3(sub3(tri[1], tri[0]), sub3(tri[2], tri[0]))
      if (Math.hypot(...f) < 1e-8) continue
      const [x, y, z] = dot3(f, t) >= 0 ? tri : [tri[0], tri[2], tri[1]]
      ribbon.tri(x, y, z, undefined, undefined, undefined, [t, t, t])
    }
  }
  return sections
}

/**
 * Long, thin dark slot windows on the bowl's outer face, staggered in height
 * as in the photos: flush dark bands laid a hair proud of the steel, so they
 * stay clean on a curved, flaring face. `z` is the slot's bottom.
 */
function slots(sec: ReturnType<typeof sweepRibbon>, runs: [number, number, number][]) {
  const H = 1.3
  for (const [s0, s1, z] of runs) {
    const span = sec.filter(c => c.m.s >= s0 && c.m.s <= s1 && z > c.zb + 0.5 && z + H < c.zt - 0.8)
    const at = (c: (typeof span)[0], zz: number): V3 => {
      const d = -c.foot * (c.zt - zz) / (c.zt - c.zb) + 0.08
      return [c.m.p[0] + c.m.n[0] * d, c.m.p[1] + c.m.n[1] * d, zz]
    }
    for (let k = 0; k < span.length - 1; k++) {
      const a = span[k], b = span[k + 1]
      if (b.m.s - a.m.s > 4) continue
      quad(glass, [at(a, z), at(b, z), at(b, z + H), at(a, z + H)], [a.face, b.face, b.face, a.face])
    }
  }
}

// East sweep: the full bowl wall along the south-east side, lifting and
// thinning over the glass on the plaza, then the blade, tilted up at its tip.
const east = path([...EAST, ...BLADE])
const rootS = east.reduce((best, m) => (Math.hypot(m.p[0] - 1.0, m.p[1] - 94.1) < Math.hypot(best.p[0] - 1.0, best.p[1] - 94.1) ? m : best)).s
const liftS = east.find(m => m.p[1] > 30)!.s
const tipS = east.at(-1)!.s
const eastSec = sweepRibbon(east, m => {
  if (m.s <= rootS) {
    const t = smooth((m.s - liftS) / (rootS - liftS))
    return { zb: 14 + t * 4, zt: 27 - t * 3.5, foot: WALL - t * 1.6 }
  }
  const t = (m.s - rootS) / (tipS - rootS)
  return { zb: 18 + t * 1.2, zt: 23.5 + t * 0.8, foot: 2.6, free: smooth(t * 4) }
})
slots(eastSec, [[4, 46, 21.5], [26, 72, 17.2], [58, 100, 24.2], [92, 128, 20.5]])

// West sweep: rises out of a point at the stone block, then the full wall.
const west = path(WEST)
const westSec = sweepRibbon(west, m => {
  const t = smooth(m.s / 28)
  return { zb: 23 - t * 9, zt: 23.6 + t * 3.4, foot: 0.6 + t * (WALL - 0.6) }
})
slots(westSec, [[30, 72, 21.8], [60, 105, 17.5], [100, 150, 23.8], [140, 185, 19]])

// ---------------------------------------------------------------------------
// The stone base under the bowl: the outline with the bowl runs set back to
// the wall line, up to the 20 m roof inside the bowl. Its walls carry square
// bays of dark art panels between stone piers; the plaza front is glass.

const eastWall = east.filter(m => m.s <= rootS + 1e-6).map(m => off(m, -WALL))
const westWall = west.map(m => off(m, -WALL))
const BODY: XY[] = [...eastWall, ...FRONT, [-26.8, 52.4], [-22.9, 9.5], ...westWall, ...TOWER_SIDE]
const frontSet = new Set(FRONT.map(p => p.join()))
const bayIndex = new Map<string, number>()
eastWall.forEach((p, i) => bayIndex.set(p.join(), i))
westWall.forEach((p, i) => bayIndex.set(p.join(), i))
prism(BODY, 0, ROOF, (a, b) => {
  const glassy = (frontSet.has(a.join()) && frontSet.has(b.join())) ||
    (a[1] > 40 && b[1] > 40 && a[0] > -2 && b[0] > -2) || (a.join() === eastWall.at(-1)!.join())
  if (glassy) return [{ p: stone, z0: 0, z1: 2.4 }, { p: glass, z0: 2.4, z1: 17.6, recess: 0.5 }, { p: panels, z0: 17.6, z1: ROOF }]
  const i = bayIndex.get(a.join()), j = bayIndex.get(b.join())
  // Three edges of panel (about 8 m), one of pier.
  if (i !== undefined && j !== undefined && i % 4 !== 0 && i > 1 && j < Math.max(eastWall.length, westWall.length) - 1)
    return [{ p: stone, z0: 0, z1: 3 }, { p: soffit, z0: 3, z1: 11, recess: 0.5 }, { p: stone, z0: 11, z1: ROOF }]
  return [{ p: stone, z0: 0, z1: ROOF }]
}, roof, 0.4)

// Pale stone block on the plaza's north-west side, where the blade ends.
prism(BLOCK, 0, 21, () => [{ p: stone, z0: 0, z1: 21 }], roof)
{
  // The big video screen on its plaza face: a flush dark panel.
  const a: XY = [-49.9, 71.7], b: XY = [-28.8, 73.7], nm = edgeNormal(b, a)
  const at = (t: number, z: number): V3 => [a[0] + (b[0] - a[0]) * t + nm[0] * 0.08, a[1] + (b[1] - a[1]) * t + nm[1] * 0.08, z]
  const N: V3 = [nm[0], nm[1], 0]
  quad(glass, [at(0.18, 9.5), at(0.82, 9.5), at(0.82, 18), at(0.18, 18)], [N, N, N, N])
}
// The ballroom box rising out of the roof, and the low link beside the tower.
prism(BALLROOM, ROOF - 0.5, 25, () => [{ p: panels, z0: 0, z1: 25 }], roof)
prism(LINK, ROOF - 0.5, 21, () => [{ p: stone, z0: 0, z1: 21 }], roof, 0.3)
// Stone entrance blocks at the west end's two corners.
prism(STEPS_W, 0, 13, () => [{ p: stone, z0: 0, z1: 13 }], roof)
prism(STEPS_E, 0, 13, () => [{ p: stone, z0: 0, z1: 13 }], roof)

// ---------------------------------------------------------------------------
// sRGB colours from daylight photos: brushed stainless in sun, its shaded
// underside, grey-blue glass of the Great Hall, the pale limestone base, the
// lighter grey panel drum and ballroom. The roof is a mid grey rather than the
// pale membrane default: a big flat roof that pale swallows the silver band,
// and the band is the building's whole identity, so ribbon, walls and roof are
// kept three distinct values.
const parts = [
  { part: ribbon, material: { name: 'stainless-ribbon', color: 0xb8bcc1, roughness: 0.5 } },
  { part: soffit, material: { name: 'ribbon-soffit', color: 0x6f757b, roughness: 0.6 } },
  { part: glass, material: { name: 'glass', color: 0x4f5f6e } },
  { part: stone, material: { name: 'limestone', color: 0xe6e0d2 } },
  { part: panels, material: { name: 'panels', color: 0xd9d5cc } },
  { part: roof, material: { name: 'flat-roof', color: 0x9a9894 } },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('NASCAR Hall of Fame', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 55, osm: 'way/322278731', footprint: [104, 206], height: 25,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/nascar-hall-of-fame.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
