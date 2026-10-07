/**
 * EPCOT United Kingdom pavilion: the Rose & Crown and the High Street —
 * procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-uk.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The anchor is the Rose
 * & Crown outline's centroid. Each building is built on its own OSM grid
 * (the pub on 11°, the High Street blocks on 43°, the Tea Caddy on 66°) and
 * turned into the model frame, so the footprints match OSM.
 *
 * What it is: the pavilion as seen from the lagoon and the promenade. The Rose
 * & Crown pub, red brick with a green shopfront and a domed round turret on
 * its street corner, with its brick dining room on the lagoon and a row of
 * stuccoed houses and the fish shop south of it; across the High Street the
 * Hampton Court range (red brick, white stone strings, crenellations, square
 * towers, a pair of cupola turrets and a cluster of tall chimneys) facing the
 * knot garden, Sportsman's Shoppe's stone gatehouse with its stepped gable,
 * a slate-roofed shop row, a Georgian corner by the square, and on the other
 * side the Georgian Queen's Table front with its pediment and the thatched
 * Tea Caddy cottage.
 *
 * Evidence
 * - OSM (measured): footprints of way/44486037 (Rose & Crown, 2 levels),
 *   way/784027061 (its fish-shop part), way/295399246 (High Street block,
 *   2 levels), way/44486036 (2 levels) and its part way/784025250 (Tea Caddy).
 *   Shop nodes place Sportsman's Shoppe, The Crown & Crest, The Toy Soldier,
 *   Queen's Table, Lords & Ladies, the Tea Caddy and the fish shop. Each
 *   footprint is drawn as a few rectangles on its own grid, so small notches
 *   are filled or trimmed by up to a metre.
 * - Public-domain USGS NAIP orthophoto: roof plan, slate vs. tile roofs, and
 *   the knot garden east of the High Street block that the Hampton Court
 *   range faces.
 * - Photos (Wikimedia Commons): Rose & Crown corner and turret from the street
 *   (United Kingdom Pavilion (41459070980), panoramio 4911, The UK in Epcot),
 *   Hampton Court from the knot garden (41459069700), Sportsman's Shoppe
 *   (United Kingdom street at Epcot), the Georgian front (Epcotuk2).
 * - Estimated from the photos (no heights published or tagged beyond levels):
 *   the pub's cornice 9 m, its turret dome top 13.8 m; Hampton Court 10 m,
 *   towers 13.4–14 m, chimneys 15.2 m; the shop rows 7.4–7.6 m to the eaves.
 * - Invented or simplified: the Rose & Crown's lagoon (dining room) side and
 *   the backs of every building, which no photo shows; the order of the
 *   towers along Hampton Court is from one photo; half-timbering, the knot
 *   garden and the bandstand are left out.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]

const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle whose winding is fixed to agree with its normals. */
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) {
  const f = cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [c[0] - a[0], c[1] - a[1], c[2] - a[2]])
  if (len(f) < 1e-9) return
  if (!n) return p.tri(a, b, c)
  if (dot(f, add(add(n[0], n[1]), n[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}

// ---------------------------------------------------------------------------
// Blocks: rectangles with chamfered vertical corners and a rounded top edge.

type Rect = { x0: number; x1: number; y0: number; y1: number; c: number }
function ring(r: Rect, d: number, z: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const c = Math.max(0.02, r.c + d * 0.414)
  const pts: V3[] = [[x0 + c, y0, z], [x1 - c, y0, z], [x1, y0 + c, z], [x1, y1 - c, z],
    [x1 - c, y1, z], [x0 + c, y1, z], [x0, y1 - c, z], [x0, y0 + c, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
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
const OUT: XY = [1, 0], UP: XY = [0, 1], S2 = Math.SQRT1_2
function block(p: Part, r: Rect, z0: number, z1: number, o: { bb?: number; bt?: number; top?: Part | null; bottom?: boolean } = {}) {
  const bb = o.bb ?? 0, bt = o.bt ?? 0
  if (bb) band(p, r, -bb, z0, 0, z0 + bb, [S2, -S2], OUT)
  band(p, r, 0, z0 + bb, 0, z1 - bt, OUT, OUT)
  if (bt) band(p, r, 0, z1 - bt, -bt, z1, OUT, UP)
  if (o.top !== null) lid(o.top ?? p, r, -bt, z1, true)
  if (o.bottom) lid(p, r, -bb, z0, false)
}
const sq = (cx: number, cy: number, h: number, c = 0.08): Rect => ({ x0: cx - h, x1: cx + h, y0: cy - h, y1: cy + h, c })
const box = (x0: number, x1: number, y0: number, y1: number, c = 0.08): Rect => ({ x0, x1, y0, y1, c })

// ---------------------------------------------------------------------------
// Faces: s runs left to right as seen from outside, z up; panels are flat
// shapes set just proud of the wall.

type Face = { o: XY; u: XY; n: V3; L: number }
function faces(r: { x0: number; x1: number; y0: number; y1: number }): Record<'s' | 'e' | 'n' | 'w', Face> {
  return {
    s: { o: [r.x0, r.y0], u: [1, 0], n: [0, -1, 0], L: r.x1 - r.x0 },
    e: { o: [r.x1, r.y0], u: [0, 1], n: [1, 0, 0], L: r.y1 - r.y0 },
    n: { o: [r.x1, r.y1], u: [-1, 0], n: [0, 1, 0], L: r.x1 - r.x0 },
    w: { o: [r.x0, r.y1], u: [0, -1], n: [-1, 0, 0], L: r.y1 - r.y0 },
  }
}
const at = (f: Face, s: number, z: number, d = 0): V3 => [f.o[0] + f.u[0] * s + f.n[0] * d, f.o[1] + f.u[1] * s + f.n[1] * d, z]
function panel(p: Part, f: Face, pts: XY[], d = 0.04) {
  const v = pts.map(([s, z]) => at(f, s, z, d))
  for (let i = 1; i < v.length - 1; i++) tri(p, v[0], v[i], v[i + 1], [f.n, f.n, f.n])
}
/** A raised rectangle on a face: front plus its four returns. */
function relief(p: Part, f: Face, s0: number, s1: number, z0: number, z1: number, d: number) {
  const u: V3 = [f.u[0], f.u[1], 0]
  quad(p, at(f, s0, z0, d), at(f, s1, z0, d), at(f, s1, z1, d), at(f, s0, z1, d), f.n)
  quad(p, at(f, s0, z1, 0), at(f, s0, z1, d), at(f, s1, z1, d), at(f, s1, z1, 0), [0, 0, 1])
  quad(p, at(f, s0, z0, 0), at(f, s1, z0, 0), at(f, s1, z0, d), at(f, s0, z0, d), [0, 0, -1])
  quad(p, at(f, s0, z0, 0), at(f, s0, z0, d), at(f, s0, z1, d), at(f, s0, z1, 0), [-u[0], -u[1], 0])
  quad(p, at(f, s1, z0, 0), at(f, s1, z1, 0), at(f, s1, z1, d), at(f, s1, z0, d), u)
}
/** Round-headed opening, counter-clockwise from the bottom left. */
function roundArch(s0: number, s1: number, z0: number, spring: number, seg = 6): XY[] {
  const r = (s1 - s0) / 2, c = s0 + r
  const pts: XY[] = [[s0, z0], [s1, z0]]
  for (let k = 0; k <= seg; k++) { const a = (k / seg) * Math.PI; pts.push([c + r * Math.cos(a), spring + r * Math.sin(a)]) }
  return pts
}
/** Equilateral pointed (Gothic) arch. */
function pointedArch(s0: number, s1: number, z0: number, spring: number, seg = 3): XY[] {
  const w = s1 - s0
  const pts: XY[] = [[s0, z0], [s1, z0]]
  for (let k = 0; k <= seg; k++) { const a = (k / seg) * (Math.PI / 3); pts.push([s0 + w * Math.cos(a), spring + w * Math.sin(a)]) }
  for (let k = seg - 1; k >= 0; k--) { const a = (k / seg) * (Math.PI / 3); pts.push([s1 - w * Math.cos(a), spring + w * Math.sin(a)]) }
  return pts
}
function circle(sc: number, zc: number, r: number, seg = 8): XY[] {
  return Array.from({ length: seg }, (_, k) => { const a = (k / seg) * 2 * Math.PI; return [sc + r * Math.cos(a), zc + r * Math.sin(a)] as XY })
}

/** A pyramid over a rectangle, flat-shaded; a ridge if `ridge` > 0 (hip roof). */
function hip(p: Part, r: { x0: number; x1: number; y0: number; y1: number }, z0: number, z1: number, alongY: boolean) {
  const cx = (r.x0 + r.x1) / 2, cy = (r.y0 + r.y1) / 2
  const half = alongY ? Math.max(0, (r.y1 - r.y0) / 2 - (r.x1 - r.x0) / 2) : Math.max(0, (r.x1 - r.x0) / 2 - (r.y1 - r.y0) / 2)
  const A: V3 = alongY ? [cx, cy - half, z1] : [cx - half, cy, z1]
  const B: V3 = alongY ? [cx, cy + half, z1] : [cx + half, cy, z1]
  const c: V3[] = [[r.x0, r.y0, z0], [r.x1, r.y0, z0], [r.x1, r.y1, z0], [r.x0, r.y1, z0]]
  const up: V3 = [0, 0, 1]
  const face = (a: V3, b: V3, t: V3) => {
    const n = unit(cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [t[0] - a[0], t[1] - a[1], t[2] - a[2]]))
    return dot(n, up) >= 0 ? n : ([-n[0], -n[1], -n[2]] as V3)
  }
  if (alongY) {
    const n0 = face(c[0], c[1], A); tri(p, c[0], c[1], A, [n0, n0, n0])
    const n1 = face(c[1], c[2], B); quad(p, c[1], c[2], B, A, n1)
    const n2 = face(c[2], c[3], B); tri(p, c[2], c[3], B, [n2, n2, n2])
    const n3 = face(c[3], c[0], A); quad(p, c[3], c[0], A, B, n3)
  } else {
    const n0 = face(c[0], c[1], A); quad(p, c[0], c[1], B, A, n0)
    const n1 = face(c[1], c[2], B); tri(p, c[1], c[2], B, [n1, n1, n1])
    const n2 = face(c[2], c[3], B); quad(p, c[2], c[3], A, B, n2)
    const n3 = face(c[3], c[0], A); tri(p, c[3], c[0], A, [n3, n3, n3])
  }
}

/** A smooth vertical cylinder (or cone frustum), open at the bottom. */
function cylinder(p: Part, cx: number, cy: number, r0: number, r1: number, z0: number, z1: number, seg = 12, cap = true) {
  const slope = (r0 - r1) / (z1 - z0)
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * 2 * Math.PI, b = ((k + 1) / seg) * 2 * Math.PI
    const na = unit([Math.cos(a), Math.sin(a), slope]), nb = unit([Math.cos(b), Math.sin(b), slope])
    quad(p, [cx + r0 * Math.cos(a), cy + r0 * Math.sin(a), z0], [cx + r0 * Math.cos(b), cy + r0 * Math.sin(b), z0],
      [cx + r1 * Math.cos(b), cy + r1 * Math.sin(b), z1], [cx + r1 * Math.cos(a), cy + r1 * Math.sin(a), z1], [na, nb, nb, na])
    if (cap) tri(p, [cx, cy, z1], [cx + r1 * Math.cos(a), cy + r1 * Math.sin(a), z1], [cx + r1 * Math.cos(b), cy + r1 * Math.sin(b), z1], [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
  }
}


type Mats = { brick: Part; stone: Part; roof: Part; win: Part; green: Part; thatch: Part }
const G: Mats = { brick: new Part(), stone: new Part(), roof: new Part(), win: new Part(), green: new Part(), thatch: new Part() }

/**
 * Build a group in its own grid (axes turned `b` degrees clockwise from
 * north), then copy it into the model frame. Each building here follows its
 * own OSM grid, and the helpers above are axis-aligned.
 */
// UK_ONLY=<n> renders one group alone, to compare it with its photos.
let groupIndex = 0
const only = process.env.UK_ONLY
function cluster(b: number, build: (m: Mats) => void) {
  if (only !== undefined && String(groupIndex++) !== only) return
  const m: Mats = { brick: new Part(), stone: new Part(), roof: new Part(), win: new Part(), green: new Part(), thatch: new Part() }
  build(m)
  const r = (b * Math.PI) / 180, c = Math.cos(r), s = Math.sin(r)
  // glTF (x, up, -north) → local map (x, y) → world map → glTF.
  const turn = (x: number, z: number): [number, number] => {
    const lx = x, ly = -z
    const wx = lx * c + ly * s, wy = -lx * s + ly * c
    return [wx, -wy]
  }
  for (const k of Object.keys(m) as (keyof Mats)[]) {
    const src = m[k], dst = G[k]
    for (let i = 0; i < src.pos.length; i += 3) {
      const [x, z] = turn(src.pos[i], src.pos[i + 2])
      dst.pos.push(x, src.pos[i + 1], z)
      const [nx, nz] = turn(src.nrm[i], src.nrm[i + 2])
      dst.nrm.push(nx, src.nrm[i + 1], nz)
    }
    dst.uv.push(...src.uv)
  }
}

type R = { x0: number; x1: number; y0: number; y1: number }
const rect = (x0: number, x1: number, y0: number, y1: number): R => ({ x0, x1, y0, y1 })
const bx = (r: R, c = 0.12): Rect => ({ ...r, c })

/** A gable roof: two slopes and the two gable ends in the wall's material. */
function gable(roof: Part, wall: Part, r: R, z0: number, z1: number, alongX: boolean, eave = 0.35) {
  const e = eave
  if (alongX) {
    const ym = (r.y0 + r.y1) / 2
    const a: V3 = [r.x0 - e, r.y0 - e, z0], b: V3 = [r.x1 + e, r.y0 - e, z0], c: V3 = [r.x1 + e, ym, z1], d: V3 = [r.x0 - e, ym, z1]
    const n1 = unit([0, -(z1 - z0), (ym - r.y0 + e)])
    quad(roof, a, b, c, d, n1)
    const a2: V3 = [r.x1 + e, r.y1 + e, z0], b2: V3 = [r.x0 - e, r.y1 + e, z0]
    const n2 = unit([0, (z1 - z0), (r.y1 + e - ym)])
    quad(roof, a2, b2, d, c, n2)
    tri(wall, [r.x0, r.y0, z0], [r.x0, r.y1, z0], [r.x0, ym, z1 - 0.1], [[-1, 0, 0], [-1, 0, 0], [-1, 0, 0]])
    tri(wall, [r.x1, r.y1, z0], [r.x1, r.y0, z0], [r.x1, ym, z1 - 0.1], [[1, 0, 0], [1, 0, 0], [1, 0, 0]])
  } else {
    const xm = (r.x0 + r.x1) / 2
    const a: V3 = [r.x1 + e, r.y0 - e, z0], b: V3 = [r.x1 + e, r.y1 + e, z0], c: V3 = [xm, r.y1 + e, z1], d: V3 = [xm, r.y0 - e, z1]
    quad(roof, a, b, c, d, unit([(z1 - z0), 0, (r.x1 + e - xm)]))
    const a2: V3 = [r.x0 - e, r.y1 + e, z0], b2: V3 = [r.x0 - e, r.y0 - e, z0]
    quad(roof, a2, b2, d, c, unit([-(z1 - z0), 0, (xm - r.x0 + e)]))
    tri(wall, [r.x1, r.y0, z0], [r.x0, r.y0, z0], [xm, r.y0, z1 - 0.1], [[0, -1, 0], [0, -1, 0], [0, -1, 0]])
    tri(wall, [r.x0, r.y1, z0], [r.x1, r.y1, z0], [xm, r.y1, z1 - 0.1], [[0, 1, 0], [0, 1, 0], [0, 1, 0]])
  }
}

/** Rows of window panels on the chosen faces of a rectangle. */
function windows(p: Part, r: R, which: ('s' | 'e' | 'n' | 'w')[], rows: [number, number][], w = 1.0, pitch = 2.6, arched = false) {
  const F = faces(r)
  for (const k of which) {
    const f = F[k], n = Math.max(1, Math.floor((f.L - 0.8) / pitch)), step = f.L / n
    for (let i = 0; i < n; i++) {
      const s = (i + 0.5) * step
      for (const [z0, z1] of rows) panel(p, f, arched ? roundArch(s - w / 2, s + w / 2, z0, z1 - w / 2, 4) : [[s - w / 2, z0], [s + w / 2, z0], [s + w / 2, z1], [s - w / 2, z1]], 0.04)
    }
  }
}

/** Crenellations: merlons standing on the wall line round a flat roof. */
function crenels(p: Part, r: R, z: number, which: ('s' | 'e' | 'n' | 'w')[] = ['s', 'e', 'n', 'w'], h = 0.7, w = 0.7, pitch = 1.5, t = 0.35) {
  const F = faces(r)
  for (const k of which) {
    const f = F[k], n = Math.max(1, Math.round(f.L / pitch)), step = f.L / n
    for (let i = 0; i < n; i++) {
      const s = (i + 0.5) * step
      const [cx, cy] = at(f, s, 0, -t / 2)
      const hx = Math.abs(f.u[0]) * w / 2 + Math.abs(f.n[0]) * t / 2, hy = Math.abs(f.u[1]) * w / 2 + Math.abs(f.n[1]) * t / 2
      block(p, box(cx - hx, cx + hx, cy - hy, cy + hy, 0.03), z, z + h, {})
    }
  }
}
const chimney = (p: Part, x: number, y: number, z0: number, z1: number, hw = 0.45) => block(p, sq(x, y, hw, 0.05), z0, z1, { bt: 0.08 })

// ---------------------------------------------------------------------------
// The Rose & Crown (way/44486037, with its part way/784027061), on its own
// 11° grid. The pub proper is the red-brick corner on the street with the
// domed round turret; the lagoon side is its brick dining room; along the
// street to the south run a stuccoed house with a bay window, a gabled house
// and the one-storey fish shop.

cluster(11, (m) => {
  // The brick pub, two storeys under a slate hip roof.
  const pubA = rect(-10.2, -0.5, 6.5, 14.1), pubB = rect(-0.5, 10.4, 6.5, 18.8)
  for (const r of [pubA, pubB]) block(m.brick, bx(r), 0, 8.6, { top: m.roof })
  block(m.stone, bx({ x0: -10.3, x1: 10.5, y0: 6.4, y1: 14.2 }), 8.6, 9.2, { bb: 0.15, top: m.roof })
  block(m.stone, bx({ x0: -0.6, x1: 10.5, y0: 6.4, y1: 18.9 }), 8.6, 9.2, { bb: 0.15, top: m.roof })
  hip(m.roof, { x0: -9.6, x1: 9.8, y0: 7.0, y1: 13.6 }, 9.2, 11.6, false)
  hip(m.roof, { x0: 0.0, x1: 9.8, y0: 7.0, y1: 18.3 }, 9.2, 11.6, true)
  chimney(m.brick, -6.5, 10.3, 10.0, 12.6, 0.6)
  chimney(m.brick, 5.0, 16.5, 10.0, 12.6, 0.6)
  // The green shopfront along the street and round the corner, cream sills,
  // and the upper windows.
  relief(m.green, faces(pubA).w, 0, 7.6, 0.3, 3.9, 0.12)
  relief(m.green, faces(pubA).n, 0, 6.6, 0.3, 3.9, 0.12)
  relief(m.green, faces(pubB).n, 0, 10.9, 0.3, 3.9, 0.12)
  windows(m.win, pubA, ['w', 'n'], [[5.0, 7.0]], 0.95, 2.6)
  windows(m.win, pubB, ['n', 'e'], [[5.0, 7.0]], 0.95, 2.6)
  windows(m.win, pubA, ['w', 'n'], [[0.9, 3.4]], 1.6, 2.6)
  windows(m.win, pubB, ['n'], [[0.9, 3.4]], 1.6, 2.6)
  // The corner turret: brick drum, a cream drum with round lucarnes, a lead dome.
  const tx = -9.3, ty = 13.2, tr = 2.3
  cylinder(m.brick, tx, ty, tr, tr, 0, 9.4, 14)
  cylinder(m.stone, tx, ty, tr + 0.18, tr + 0.18, 3.6, 4.1, 14)
  cylinder(m.stone, tx, ty, tr + 0.2, tr + 0.2, 9.0, 9.6, 14)
  cylinder(m.stone, tx, ty, tr - 0.05, tr - 0.05, 9.6, 11.3, 14)
  cylinder(m.stone, tx, ty, tr + 0.1, tr + 0.1, 11.3, 11.6, 14)
  for (let k = 0; k < 4; k++) {
    const a = Math.PI / 4 + (k * Math.PI) / 2, cx = tx + (tr + 0.0) * Math.cos(a), cy = ty + (tr + 0.0) * Math.sin(a)
    const f: Face = { o: [cx + 0.6 * Math.sin(a), cy - 0.6 * Math.cos(a)], u: [-Math.sin(a), Math.cos(a)], n: [Math.cos(a), Math.sin(a), 0], L: 1.2 }
    panel(m.green, f, circle(0.6, 10.45, 0.48, 8), 0.04)
    panel(m.win, f, roundArch(0.15, 1.05, 5.0, 6.9, 4), 0.06)
  }
  {
    const seg = 14, rings = 4, z0 = 11.6, hgt = 2.2, R0 = tr + 0.05
    for (let k = 0; k < seg; k++)
      for (let j = 0; j < rings; j++) {
        const P = (kk: number, jj: number): V3 => { const a = (kk / seg) * 2 * Math.PI, t = (jj / rings) * (Math.PI / 2); return [tx + R0 * Math.cos(t) * Math.cos(a), ty + R0 * Math.cos(t) * Math.sin(a), z0 + hgt * Math.sin(t)] }
        const Nn = (kk: number, jj: number): V3 => { const a = (kk / seg) * 2 * Math.PI, t = (jj / rings) * (Math.PI / 2); return unit([Math.cos(t) * Math.cos(a), Math.cos(t) * Math.sin(a), Math.sin(t) * R0 / hgt]) }
        quad(m.roof, P(k, j), P(k + 1, j), P(k + 1, j + 1), P(k, j + 1), [Nn(k, j), Nn(k + 1, j), Nn(k + 1, j + 1), Nn(k, j + 1)])
      }
    cylinder(m.stone, tx, ty, 0.07, 0.02, z0 + hgt - 0.05, z0 + hgt + 1.3, 5, false)
  }
  // Lagoon side: the brick dining room under slate hips.
  const din = rect(2.8, 13.2, -10.6, 6.5), din2 = rect(10.4, 14.4, 5.4, 16.2), din3 = rect(6.8, 9.8, -7.3, -5.5)
  block(m.brick, bx(din), 0, 6.4, { bt: 0.15 })
  block(m.brick, bx(din2), 0, 6.4, { bt: 0.15 })
  hip(m.roof, { x0: 2.6, x1: 13.4, y0: -10.8, y1: 6.5 }, 6.4, 9.0, true)
  hip(m.roof, { x0: 10.2, x1: 14.6, y0: 5.4, y1: 16.4 }, 6.4, 8.2, true)
  windows(m.win, din, ['e', 's'], [[1.0, 4.2]], 1.4, 2.6, true)
  windows(m.win, din2, ['e'], [[1.0, 4.2]], 1.4, 2.6, true)
  // The street to the south: a cream house with a bay window, a gabled house,
  // the fish shop.
  const bay = rect(-10.9, -2.5, -0.1, 6.5)
  block(m.stone, bx(bay), 0, 7.6, { top: m.roof })
  hip(m.roof, { x0: -11.2, x1: -2.2, y0: -0.4, y1: 6.8 }, 7.6, 10.6, true)
  block(m.stone, bx({ x0: -11.7, x1: -10.9, y0: 1.6, y1: 4.8 }), 4.4, 7.0, { bt: 0.15, bottom: true })
  panel(m.win, faces({ x0: -11.7, x1: -10.9, y0: 1.6, y1: 4.8 }).w, [[0.4, 4.9], [2.8, 4.9], [2.8, 6.5], [0.4, 6.5]], 0.04)
  relief(m.green, faces(bay).w, 0.4, 6.2, 0.3, 3.6, 0.08)
  const link = rect(-2.5, 2.8, -0.1, 6.5)
  block(m.stone, bx(link), 0, 7.6, { top: m.roof })
  hip(m.roof, { x0: -2.5, x1: 3.1, y0: -0.4, y1: 6.8 }, 7.6, 9.6, true)
  const gab = rect(-7.9, 2.8, -11.1, -0.1)
  block(m.stone, bx(gab), 0, 6.8, { top: m.roof })
  gable(m.roof, m.stone, gab, 6.8, 10.2, false)
  chimney(m.brick, -2.0, -5.6, 8.0, 12.4, 0.5)
  windows(m.win, gab, ['w'], [[1.0, 3.2], [4.4, 5.9]], 1.0, 2.5)
  const fish = rect(-13.5, -6.8, -17.8, -11.1), fish2 = rect(-6.8, 2.8, -14.7, -11.1)
  block(m.stone, bx(fish), 0, 4.4, { top: m.roof })
  hip(m.roof, { x0: -13.8, x1: -6.5, y0: -18.1, y1: -10.8 }, 4.4, 7.0, false)
  block(m.brick, bx(fish2), 0, 4.4, { top: m.roof })
  gable(m.thatch, m.brick, fish2, 4.4, 6.8, true)
  relief(m.green, faces(fish).w, 0.6, 6.2, 2.4, 3.1, 0.1)
  windows(m.win, fish, ['w', 's'], [[0.4, 2.3]], 1.6, 2.6)
})

// ---------------------------------------------------------------------------
// The High Street block (way/295399246), on its 43° grid: the Hampton Court
// range at its lagoon end facing the knot garden, Sportsman's Shoppe's stone
// gatehouse on the street, a slate-roofed shop row and the Georgian-fronted
// corner by the square.

cluster(43, (m) => {
  // Hampton Court: a brick range with white stone strings and crenellations,
  // two square towers, an octagonal turret pair with cupolas at the street
  // corner, a cluster of tall chimneys, and a lower arched wing to the north.
  const hc = rect(-45.2, -35.0, -4.0, 12.5)
  block(m.brick, bx(hc), 0, 10.0, { top: m.roof })
  block(m.stone, bx({ x0: -45.3, x1: -34.9, y0: -4.1, y1: 12.6 }), 5.2, 5.5, { top: null })
  block(m.stone, bx({ x0: -45.3, x1: -34.9, y0: -4.1, y1: 12.6 }), 9.7, 10.1, { top: m.roof })
  crenels(m.stone, hc, 10.1, ['e', 's', 'n'])
  windows(m.win, hc, ['e', 's'], [[1.2, 3.9], [6.2, 8.6]], 1.0, 3.0, true)
  const towers: [number, number, number][] = [[-36.1, 0.6, 14.0], [-36.1, 8.4, 13.4]]
  for (const [x, y, h] of towers) {
    block(m.brick, sq(x, y, 2.2, 0.12), 0, h, { top: m.roof })
    block(m.stone, sq(x, y, 2.25, 0.12), h - 0.3, h, { top: m.roof })
    block(m.stone, sq(x, y, 2.25, 0.12), 5.2, 5.5, { top: null })
    crenels(m.stone, rect(x - 2.25, x + 2.25, y - 2.25, y + 2.25), h, ['s', 'e', 'n', 'w'], 0.7, 0.7, 1.5, 0.35)
    for (const [z0, z1] of [[1.4, 3.6], [6.6, 8.6], [10.6, 12.2]]) panel(m.win, faces(rect(x - 2.2, x + 2.2, y - 2.2, y + 2.2)).e, roundArch(1.75, 2.65, z0, z1 - 0.45, 4), 0.04)
  }
  for (const y of [-3.3, -0.9]) {
    const x = -34.6
    cylinder(m.brick, x, y, 1.0, 1.0, 0, 12.2, 8)
    cylinder(m.stone, x, y, 1.08, 1.08, 12.2, 12.5, 8)
    cylinder(m.roof, x, y, 0.95, 0.0, 12.5, 14.0, 8, false)
  }
  for (const y of [3.6, 4.6, 5.6]) chimney(m.brick, -38.6, y, 9.5, 15.2, 0.42)
  block(m.stone, bx({ x0: -39.1, x1: -38.1, y0: 3.0, y1: 6.2 }), 15.2, 15.6, {})
  // The lower north wing with its two Tudor arches, then the back wing.
  const nw = rect(-45.2, -35.0, 12.5, 17.5)
  block(m.brick, bx(nw), 0, 7.4, { top: m.roof })
  block(m.stone, bx({ x0: -45.3, x1: -34.9, y0: 12.5, y1: 17.6 }), 7.1, 7.5, { top: m.roof })
  crenels(m.stone, nw, 7.5, ['e', 'n'])
  for (const s of [1.4, 3.7]) panel(m.win, faces(nw).e, pointedArch(s - 0.9 + 0.0, s + 0.9, 0, 2.6, 3), 0.05)
  const back = rect(-54.7, -45.2, 7.5, 23.0)
  block(m.brick, bx(rect(-49.3, -45.0, 1.8, 7.7)), 0, 8.5, { top: m.roof })
  block(m.brick, bx(back), 0, 8.5, { top: m.roof })
  crenels(m.stone, back, 8.5, ['n', 'w'])
  windows(m.win, back, ['n', 'w'], [[1.2, 3.6], [5.2, 7.4]], 1.0, 3.0)

  // Sportsman's Shoppe: a pale stone gatehouse with a stepped gable and two
  // round bartizans, the Tudor arch on the street.
  const sp = rect(-49.1, -38.6, -5.5, 2.0)
  block(m.thatch, bx(sp), 0, 9.5, { top: m.roof })
  gable(m.roof, m.thatch, sp, 9.5, 12.5, false, 0.1)
  {
    const f = faces(sp).s, L = f.L
    relief(m.thatch, f, L / 2 - 2.2, L / 2 + 2.2, 0, 12.4, 0.5)
    for (const [s0, s1, z] of [[L / 2 - 1.6, L / 2 + 1.6, 13.2], [L / 2 - 0.9, L / 2 + 0.9, 13.9]] as [number, number, number][])
      relief(m.thatch, f, s0, s1, 12.4, z, 0.5)
    relief(m.stone, f, L / 2 - 1.6, L / 2 + 1.6, 0, 3.4, 0.55)
    panel(m.win, f, pointedArch(L / 2 - 1.2, L / 2 + 1.2, 0, 2.2, 3), 0.55)
    panel(m.win, f, [[L / 2 - 0.8, 6.0], [L / 2 + 0.8, 6.0], [L / 2 + 0.8, 8.0], [L / 2 - 0.8, 8.0]], 0.55)
    for (const s of [L / 2 - 2.5, L / 2 + 2.5]) {
      const [cx, cy] = at(f, s, 0, 0.2)
      cylinder(m.stone, cx, cy, 0.75, 0.75, 8.6, 11.6, 8)
      cylinder(m.stone, cx, cy, 0.3, 0.75, 7.8, 8.6, 8, false)
    }
  }
  for (const y of [-1.0, 0.0]) chimney(m.brick, -40.0, y, 10.0, 14.2, 0.4)

  // The shop row: stucco and timber fronts under slate.
  const row = rect(-61.0, -49.1, -8.0, 7.5)
  block(m.stone, bx(row), 0, 7.6, { top: m.roof })
  gable(m.roof, m.stone, row, 7.6, 11.4, true)
  windows(m.win, row, ['s', 'w'], [[0.9, 3.4], [4.6, 6.6]], 1.1, 2.4)
  chimney(m.brick, -55.0, 2.6, 9.0, 12.8, 0.5)
  // The west end by the square: a Georgian house front and a wing.
  const ge = rect(-80.5, -61.0, -21.0, -7.3)
  block(m.stone, bx(ge), 0, 9.6, { top: m.roof })
  block(m.stone, bx({ x0: -80.6, x1: -60.9, y0: -21.1, y1: -7.2 }), 9.6, 10.4, { bb: 0.15, top: m.roof })
  hip(m.roof, { x0: -79.6, x1: -61.9, y0: -20.1, y1: -8.2 }, 10.4, 13.0, false)
  windows(m.win, ge, ['s', 'w', 'n'], [[1.2, 3.6], [4.8, 7.0], [7.8, 9.0]], 1.0, 2.5)
  chimney(m.brick, -76.0, -14.0, 11.0, 14.0, 0.6)
  chimney(m.brick, -66.0, -14.0, 11.0, 14.0, 0.6)
})

// ---------------------------------------------------------------------------
// Across the street (way/44486036 and its part way/784025250): a timbered
// row, the Georgian Queen's Table facing the square, and the thatched Tea
// Caddy, the Anne Hathaway cottage.

cluster(43, (m) => {
  const row = rect(-49.0, -32.8, -25.6, -14.5)
  block(m.stone, bx(row), 0, 7.4, { top: m.roof })
  gable(m.roof, m.stone, row, 7.4, 11.2, true)
  windows(m.win, row, ['n'], [[0.9, 3.4], [4.6, 6.6]], 1.2, 2.4)
  chimney(m.brick, -40.0, -20.0, 9.0, 12.6, 0.5)
  const qt = rect(-49.6, -41.9, -39.8, -25.6)
  block(m.stone, bx(qt), 0, 10.2, { top: m.roof })
  block(m.stone, bx({ x0: -49.7, x1: -41.8, y0: -39.9, y1: -25.5 }), 10.2, 11.0, { bb: 0.15, top: m.roof })
  windows(m.win, qt, ['w', 's'], [[1.0, 3.6], [4.6, 7.0], [8.0, 9.6]], 1.0, 2.3)
  {
    // Pediment over the columned centre of the square front.
    const f = faces(qt).w, L = f.L, d = 0.25
    relief(m.stone, f, L / 2 - 3.2, L / 2 + 3.2, 0.5, 10.2, d)
    const a = at(f, L / 2 - 3.4, 11.0, d), b = at(f, L / 2 + 3.4, 11.0, d), c = at(f, L / 2, 12.6, d)
    tri(m.stone, a, b, c, [f.n, f.n, f.n])
    windows(m.win, qt, ['w'], [[1.0, 3.6]], 1.0, 2.3)
  }
  chimney(m.brick, -45.8, -28.0, 11.0, 13.4, 0.5)
  chimney(m.brick, -45.8, -37.5, 11.0, 13.4, 0.5)
})
cluster(66, (m) => {
  const tc = rect(-21.0, -10.8, -34.7, -26.9)
  block(m.stone, bx(tc), 0, 4.2, { top: m.roof })
  // Thatch: a steep roof with rounded hips, eaves well over the walls.
  hip(m.thatch, { x0: -21.7, x1: -10.1, y0: -35.4, y1: -26.2 }, 3.9, 8.6, false)
  windows(m.win, tc, ['n', 'w', 'e'], [[1.0, 2.8]], 1.0, 2.3)
  chimney(m.brick, -12.5, -30.8, 6.0, 9.8, 0.5)
})

// ---------------------------------------------------------------------------
// Materials (STYLE.md): red brick is an identity finish, muted; render and
// stone the palette's stone; slate the palette's roof; the pub's green
// shopfront and lucarnes a muted green finish; one warm tan for the thatch,
// the tiled roof behind the fish shop and Sportsman's Shoppe's ashlar, which
// share that hue in the photos and keep the model within six materials.

const parts = [
  { part: G.brick, material: finish('uk-brick', 0xb57462) },
  { part: G.stone, material: PALETTE.stone },
  { part: G.roof, material: PALETTE.roof },
  { part: G.win, material: PALETTE.window },
  { part: G.green, material: finish('pub-green', 0x5f8a72) },
  { part: G.thatch, material: finish('uk-tan', 0xc9ad84) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT United Kingdom pavilion', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 15.6,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.env.UK_OUT ?? new URL('../../landmarks/models/wdw-epcot-uk.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
