/**
 * EPCOT Germany pavilion: the platz — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-germany.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The anchor is the
 * centroid of the pavilion's single OSM outline (way/295593358), which wraps
 * the platz on three sides and runs back over the Biergarten's show building.
 *
 * What it is: a Bavarian/Rhineland town square. The whole footprint is the
 * plain flat-roofed show building it really is from above; standing on it,
 * round the platz and the St George fountain: Das Kaufhaus (after Freiburg's
 * Historisches Kaufhaus) facing the promenade, ochre with tall windows, a
 * balcony, a steep red roof between stepped gables and two corner oriels with
 * pointed caps; on the south side the Glockenspiel clock tower and an orange
 * stepped-gable house, the painted Biergarten front, and behind them the pale
 * crenellated castle walls and round tower; on the east and north-east the
 * white round tower with red roof, lantern and onion dome, the big
 * half-timbered gable of the Stein Haus, the orange stepped gable of the
 * Weinkeller and a red-roofed house towards the promenade.
 *
 * Evidence
 * - OSM (measured): the outline (simplified to the metre: notches under a
 *   metre are dropped) and the shop nodes that place each front (Das
 *   Kaufhaus, Glockenspiel, Sommerfest, Stein Haus, Weinkeller, Kunstarbeit in
 *   Kristall, Die Weihnachts Ecke, Karamell-Küche) and the fountain.
 * - Public-domain USGS NAIP orthophoto: which parts carry red tile roofs (the
 *   Kaufhaus wing, the north-east wing, the platz's south side) and which are
 *   flat show-building roof, and the Kaufhaus roof's size and ridge line.
 * - Photos (Wikimedia Commons): looking south across the platz (Epcot
 *   Germany2.JPG), the fountain and north-east side (Epcot - 52636503733.jpg),
 *   the Kaufhaus front (Germany Pavilion (41459027350).jpg), the Kunstarbeit
 *   stepped gable (Kunstarbeit in Kristall Exterior (39978161491).jpg).
 * - Estimated from the photos (nothing is published or tagged): eaves 7–11 m,
 *   ridges 13–17.5 m, the clock tower 21 m, the castle walls 15.5 m and its
 *   round tower 19.4 m, the show building 9 m.
 * - Invented or simplified: the backs and the show building's walls (plain);
 *   where exactly each front's corners fall along the outline; the castle's
 *   footprint, placed behind the platz where the photos show it.
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

type Mats = { stone: Part; roof: Part; tile: Part; win: Part; ochre: Part; timber: Part }
const fresh = (): Mats => ({ stone: new Part(), roof: new Part(), tile: new Part(), win: new Part(), ochre: new Part(), timber: new Part() })
const G: Mats = fresh()

/**
 * Build a group in its own grid (axes turned `b` degrees clockwise from
 * north), then copy it into the model frame. Each building here follows its
 * own OSM grid, and the helpers above are axis-aligned.
 */
// LANDMARK_GROUP=<n> renders one group alone, to compare it with its photos.
let groupIndex = 0
const only = process.env.LANDMARK_GROUP
function cluster(b: number, build: (m: Mats) => void) {
  if (only !== undefined && String(groupIndex++) !== only) return
  const m: Mats = fresh()
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

/** World (x east, y north) → a group's own grid turned `b` degrees. */
function local(b: number, x: number, y: number): XY {
  const r = (b * Math.PI) / 180
  return [x * Math.cos(r) - y * Math.sin(r), x * Math.sin(r) + y * Math.cos(r)]
}

// ---------------------------------------------------------------------------
// The pavilion's whole footprint (way/295593358), simplified from OSM to the
// metre, extruded as the plain show building the themed fronts stand on.

const OUTLINE: XY[] = [[-26.1, 17.0], [-34.0, 9.7], [-35.9, 6.4], [-36.2, 3.2], [-42.8, 0.4], [-37.1, -13.7], [-27.9, -10.2], [-25.4, -17.2], [-22.4, -22.5], [-24.1, -23.3], [-14.7, -42.6], [-1.3, -35.5], [8.0, -51.1], [17.5, -45.7], [21.1, -46.4], [34.8, -70.2], [59.3, -55.7], [46.9, -35.2], [47.4, -34.2], [42.2, -26.1], [33.0, -10.7], [23.0, 3.0], [17.8, 4.5], [16.9, 6.0], [15.8, 5.4], [12.9, 10.2], [15.1, 11.4], [12.9, 14.8], [10.8, 13.6], [9.7, 15.6], [8.2, 14.7], [6.3, 18.0], [8.5, 19.8], [7.7, 21.9], [5.6, 22.3], [4.3, 24.5], [6.1, 26.6], [4.1, 29.5], [2.9, 28.8], [0.4, 33.1], [-6.8, 28.7], [-4.7, 25.5], [-8.6, 26.3], [-10.1, 22.0], [-11.0, 22.0], [-12.0, 16.9], [-7.7, 16.0], [-8.3, 14.2], [-6.6, 11.0], [-4.4, 10.2], [-3.4, 9.1], [-0.9, 9.6], [1.4, 8.8], [2.5, 6.6], [2.0, 1.7], [1.0, 1.7], [0.2, -4.5], [-4.7, -6.4], [-9.9, -7.3], [-12.2, -9.3], [-18.7, -2.1], [-19.6, 4.0], [-23.6, 3.5], [-18.3, 8.4]]
const SHOW = 9.0

/** Ear-clipping triangulation of a simple polygon, counter-clockwise. */
function triangulate(poly: XY[]): [number, number, number][] {
  const idx = poly.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 0 && crossz(b, c, p) > 0 && crossz(c, a, p) > 0
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = poly[i0], b = poly[i1], c = poly[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(poly[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
if (only === undefined) {
  const area = OUTLINE.reduce((s, p, i) => { const q = OUTLINE[(i + 1) % OUTLINE.length]; return s + p[0] * q[1] - q[0] * p[1] }, 0)
  const poly = area < 0 ? [...OUTLINE].reverse() : OUTLINE
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    const L = Math.hypot(b[0] - a[0], b[1] - a[1]), n: V3 = [(b[1] - a[1]) / L, -(b[0] - a[0]) / L, 0]
    quad(G.stone, [a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], SHOW], [a[0], a[1], SHOW], n)
    // A pale coping line round the flat roof.
    const o = 0.12
    quad(G.stone, [a[0] + n[0] * o, a[1] + n[1] * o, SHOW - 0.35], [b[0] + n[0] * o, b[1] + n[1] * o, SHOW - 0.35], [b[0] + n[0] * o, b[1] + n[1] * o, SHOW], [a[0] + n[0] * o, a[1] + n[1] * o, SHOW], n)
  }
  const up: V3 = [0, 0, 1]
  for (const [i, j, k] of triangulate(poly)) tri(G.roof, [poly[i][0], poly[i][1], SHOW], [poly[j][0], poly[j][1], SHOW], [poly[k][0], poly[k][1], SHOW], [up, up, up])
}

/** A stepped gable on a face: `steps` set-backs from the eave to the peak. */
function stepped(p: Part, f: Face, z0: number, z1: number, steps: number, d = 0.3) {
  const L = f.L
  for (let i = 0; i < steps; i++) {
    const t0 = i / steps, inset = (L / 2) * t0 * 0.92
    const za = z0 + (z1 - z0) * t0, zb = z0 + (z1 - z0) * ((i + 1) / steps)
    relief(p, f, inset, L - inset, za, zb, d)
    // A little capstone on each step.
    if (i < steps - 1) for (const s of [inset, L - inset - 0.5]) relief(p, f, s, s + 0.5, zb, zb + 0.35, d + 0.08)
  }
}
/** Half-timbering drawn as broad flush beams on a face. */
function timbers(p: Part, f: Face, z0: number, z1: number, posts: number) {
  const step = f.L / posts
  for (let i = 0; i <= posts; i++) {
    const s = Math.min(f.L - 0.15, Math.max(0.15, i * step))
    panel(p, f, [[s - 0.15, z0], [s + 0.15, z0], [s + 0.15, z1], [s - 0.15, z1]], 0.05)
  }
  for (const z of [z0, (z0 + z1) / 2, z1 - 0.3]) panel(p, f, [[0, z], [f.L, z], [f.L, z + 0.3], [0, z + 0.3]], 0.05)
  for (let i = 0; i < posts; i += 2) {
    const a = i * step, b = (i + 1) * step, za = (z0 + z1) / 2 + 0.3, zb = z1 - 0.3
    panel(p, f, [[a, za], [a + 0.4, za], [b, zb], [b - 0.4, zb]], 0.05)
  }
}

/** Timbers inside a gable triangle: tie beam, collar, king post and two posts. */
function gableTimbers(p: Part, f: Face, z0: number, z1: number) {
  const L = f.L, m = L / 2, zc = z0 + (z1 - z0) * 0.5
  const beam = (s0: number, s1: number, z: number) => panel(p, f, [[s0, z], [s1, z], [s1, z + 0.3], [s0, z + 0.3]], 0.05)
  const post = (s: number, za: number, zb: number) => panel(p, f, [[s - 0.15, za], [s + 0.15, za], [s + 0.15, zb], [s - 0.15, zb]], 0.05)
  beam(0.2, L - 0.2, z0)
  beam(m - L / 4 + 0.3, m + L / 4 - 0.3, zc)
  post(m, zc, z0 + (z1 - z0) * 0.85)
  for (const s of [m - L / 5, m + L / 5]) post(s, z0, zc)
}

// ---------------------------------------------------------------------------
// The south side of the platz, on a 339° grid: the Glockenspiel clock tower,
// the orange stepped-gable house east of it, and behind them the pale castle
// walls with their round tower that close every photo looking south.

cluster(339, (m) => {
  // Clock tower.
  const [cx, cy] = local(339, -7.6, -9.6)
  const t = rect(cx - 2.4, cx + 2.4, cy - 2.4, cy + 2.4)
  block(m.stone, bx(t), 0, 16.5, { top: m.roof })
  block(m.ochre, bx({ x0: t.x0 - 0.1, x1: t.x1 + 0.1, y0: t.y0 - 0.1, y1: t.y1 + 0.1 }), 0, 2.5, { top: null })
  hip(m.tile, { x0: t.x0 - 0.35, x1: t.x1 + 0.35, y0: t.y0 - 0.35, y1: t.y1 + 0.35 }, 16.5, 21.0, true)
  const ft = faces(t).n
  relief(m.timber, ft, 1.0, 3.8, 12.6, 16.6, 0.12)
  gable(m.tile, m.timber, rect(cx - 1.4, cx + 1.4, t.y1 - 0.2, t.y1 + 0.5), 16.6, 18.8, false, 0.15)
  panel(m.stone, ft, circle(2.4, 14.4, 1.05, 12), 0.2)
  relief(m.timber, ft, 1.2, 3.6, 7.5, 8.2, 0.7)
  for (const s of [1.6, 3.2]) panel(m.win, ft, roundArch(s - 0.55, s + 0.55, 8.4, 10.3, 4), 0.06)
  panel(m.win, ft, roundArch(1.2, 3.6, 0, 2.6, 6), 0.1)
  // Orange stepped-gable house, gable to the platz.
  const [hx, hy] = local(339, -0.6, -7.0)
  const h = rect(hx - 3.8, hx + 3.8, hy - 9, hy + 1.5)
  block(m.ochre, bx(h), 0, 10.5, { top: m.roof })
  gable(m.tile, m.ochre, h, 10.5, 17.0, false, 0.3)
  const fh = faces(h).n
  stepped(m.ochre, fh, 10.5, 17.6, 5, 0.25)
  windows(m.win, h, ['n'], [[4.6, 6.8], [8.0, 9.6]], 1.0, 2.3)
  windows(m.win, rect(hx - 1.5, hx + 1.5, hy - 9, hy + 1.5), ['n'], [[11.4, 12.8], [13.6, 14.9]], 0.6, 1.5)
  relief(m.timber, fh, 0.7, fh.L - 0.7, 3.4, 3.9, 0.9)
  panel(m.win, fh, [[2.6, 0], [5.0, 0], [5.0, 3.0], [2.6, 3.0]], 0.06)
  // The painted Biergarten front west of the tower, red roof with dormers.
  const [bx0, by0] = local(339, -14.8, -12.4)
  const bg = rect(bx0 - 5.5, bx0 + 3.0, by0 - 6, by0 + 2.5)
  block(m.stone, bx(bg), 0, 10.0, { top: m.roof })
  gable(m.tile, m.stone, bg, 10.0, 14.6, true, 0.35)
  windows(m.win, bg, ['n'], [[5.6, 7.4]], 1.1, 2.6)
  panel(m.win, faces(bg).n, roundArch(4.5, 7.5, 0, 2.6, 6), 0.06)
  // The castle: pale crenellated curtain walls behind the platz, with a big
  // round tower.
  const [kx, ky] = local(339, -2.0, -22.0)
  const k = rect(kx - 15, kx + 13, ky - 7, ky + 3)
  block(m.stone, bx(k), 0, 15.5, { top: m.roof })
  crenels(m.stone, k, 15.5, ['n', 'e', 'w', 's'], 0.8, 0.9, 1.8, 0.4)
  windows(m.win, k, ['n'], [[12.0, 13.4]], 0.7, 3.4)
  cylinder(m.stone, kx + 3.0, ky + 3.0, 3.3, 3.3, 0, 18.5, 16)
  cylinder(m.stone, kx + 3.0, ky + 3.0, 3.6, 3.6, 16.6, 17.2, 16)
  for (let i = 0; i < 12; i++) {
    const a = (i / 12) * 2 * Math.PI, x = kx + 3.0 + 3.35 * Math.cos(a), y = ky + 3.0 + 3.35 * Math.sin(a)
    block(m.stone, sq(x, y, 0.38, 0.05), 18.5, 19.4, {})
  }
  // The steep-roofed block at the castle's east end.
  const ke = rect(kx + 7.5, kx + 13, ky - 7, ky + 3)
  block(m.stone, bx(ke), 15.5, 17.5, { top: m.roof })
  hip(m.tile, { x0: ke.x0 - 0.3, x1: ke.x1 + 0.3, y0: ke.y0 - 0.3, y1: ke.y1 + 0.3 }, 17.5, 21.5, true)
})

// ---------------------------------------------------------------------------
// Das Kaufhaus (after Freiburg's), on a 52° grid facing the promenade: ochre
// walls, four tall windows over an arcade, a timber balcony, a steep red
// roof with stepped gable ends and two corner oriels with pointed caps.

cluster(52, (m) => {
  // Centred on the outline's north-east front, which runs 11.6 m from the
  // wing's north-west end to the platz corner; the photos' four windows and
  // two oriels need a little more, so the block is 13 m long.
  const [cx, cy] = local(52, -25.8, 11.5)
  const r = rect(cx - 6.5, cx + 6.5, cy - 5, cy + 5)
  block(m.ochre, bx(r), 0, 11.0, { top: m.roof })
  gable(m.tile, m.ochre, r, 11.0, 17.5, true, 0.3)
  const F = faces(r)
  for (const f of [F.e, F.w]) stepped(m.ochre, f, 11.0, 17.9, 5, 0.25)
  const fn = F.n
  for (let i = 0; i < 4; i++) {
    const s = 3.4 + i * 2.07
    panel(m.win, fn, pointedArch(s - 0.78, s + 0.78, 5.6, 8.8, 3), 0.04)
    panel(m.win, fn, roundArch(s - 0.85, s + 0.85, 0, 2.4, 5), 0.04)
  }
  relief(m.timber, fn, 2.2, fn.L - 2.2, 4.4, 5.3, 0.9)
  for (const s of [2.6, 4.4, 6.5, 8.6, 10.4]) {
    const [dx, dy] = at(fn, s, 0, 0.6)
    block(m.timber, sq(dx, dy, 0.2, 0.03), 3.9, 4.4, {})
  }
  // Corner oriels: polygonal bays from the first floor to the eaves, capped
  // with steep red cones.
  for (const s of [1.2, fn.L - 1.2]) {
    const [ox, oy] = at(fn, s, 0, 0.6)
    cylinder(m.ochre, ox, oy, 0.6, 1.5, 3.8, 5.0, 8, false)
    cylinder(m.ochre, ox, oy, 1.5, 1.5, 5.0, 9.6, 8, false)
    for (let k = 0; k < 8; k++) {
      const a = ((k + 0.5) / 8) * 2 * Math.PI
      const f: Face = { o: [ox + 1.5 * Math.cos(a) * 0.98 + 0.4 * Math.sin(a), oy + 1.5 * Math.sin(a) * 0.98 - 0.4 * Math.cos(a)], u: [-Math.sin(a), Math.cos(a)], n: [Math.cos(a), Math.sin(a), 0], L: 0.8 }
      if (Math.cos(a) * fn.n[0] + Math.sin(a) * fn.n[1] > -0.2) panel(m.win, f, [[0.1, 5.8], [0.7, 5.8], [0.7, 8.6], [0.1, 8.6]], 0.03)
    }
    cylinder(m.tile, ox, oy, 1.75, 0.05, 9.6, 14.2, 8, false)
  }
  for (let i = 0; i < 4; i++) {
    const s = 3.0 + i * 2.35
    const [dx, dy] = at(fn, s, 0, -1.6)
    block(m.ochre, sq(dx, dy, 0.5, 0.05), 12.0, 13.2, { top: null })
    hip(m.tile, { x0: dx - 0.65, x1: dx + 0.65, y0: dy - 0.65, y1: dy + 0.65 }, 13.2, 14.0, true)
  }
})

// ---------------------------------------------------------------------------
// The east and north-east sides of the platz, on a 59° grid: the white round
// tower with its red roof, lantern and onion dome; the big half-timbered
// gable (Stein Haus) facing the platz; a red-roofed house behind towards the
// promenade; and the orange stepped-gable Weinkeller on the east side.

cluster(59, (m) => {
  const [tx, ty] = local(59, -6.4, 13.4)
  cylinder(m.stone, tx, ty, 2.7, 2.7, 0, 10.5, 14)
  cylinder(m.stone, tx, ty, 2.9, 2.9, 7.0, 7.4, 14)
  for (let k = 0; k < 3; k++) {
    const a = Math.PI + (k - 1) * 0.9
    const f: Face = { o: [tx + 2.7 * Math.cos(a) + 0.5 * Math.sin(a), ty + 2.7 * Math.sin(a) - 0.5 * Math.cos(a)], u: [-Math.sin(a), Math.cos(a)], n: [Math.cos(a), Math.sin(a), 0], L: 1.0 }
    panel(m.win, f, [[0.1, 8.3], [0.9, 8.3], [0.9, 9.6], [0.1, 9.6]], 0.05)
    panel(m.win, f, roundArch(0.05, 0.95, 1.0, 2.6, 4), 0.05)
  }
  cylinder(m.tile, tx, ty, 3.2, 1.0, 10.5, 13.6, 14, false)
  cylinder(m.timber, tx, ty, 0.9, 0.9, 13.6, 15.2, 8)
  cylinder(m.tile, tx, ty, 1.15, 0.7, 15.2, 15.8, 8, false)
  cylinder(m.roof, tx, ty, 0.7, 0.9, 15.8, 16.4, 8, false)
  cylinder(m.roof, tx, ty, 0.9, 0.0, 16.4, 17.8, 8, false)
  cylinder(m.roof, tx, ty, 0.05, 0.02, 17.8, 18.6, 4, false)
  // Stein Haus: the big half-timbered gable over the platz.
  const [sx, sy] = local(59, 3.2, 13.8)
  const s = rect(sx - 4.2, sx + 4.2, sy - 5.5, sy + 5.5)
  block(m.stone, bx(s), 0, 8.0, { top: m.roof })
  block(m.ochre, bx({ ...s, x0: s.x0 - 0.05, x1: s.x1 + 0.05, y0: s.y0 - 0.05, y1: s.y1 + 0.05 }), 0, 2.8, { top: null })
  gable(m.tile, m.stone, rect(s.x0, s.x1, s.y0 - 0.6, s.y1), 8.0, 15.8, false, 0.4)
  const fs = faces(rect(s.x0, s.x1, s.y0 - 0.6, s.y1)).s
  timbers(m.timber, faces(s).s, 2.9, 8.0, 6)
  relief(m.timber, faces(s).s, 0.6, s.x1 - s.x0 - 0.6, 3.1, 4.2, 0.8)
  windows(m.win, s, ['s'], [[4.6, 6.4]], 0.9, 2.1)
  panel(m.win, fs, [[3.4, 9.6], [5.0, 9.6], [5.0, 11.4], [3.4, 11.4]], 0.06)
  for (const [a, b, z0, z1] of [[0.8, 4.2, 8.0, 14.4], [7.6, 4.2, 8.0, 14.4]] as [number, number, number, number][]) panel(m.timber, fs, [[a - 0.2, z0], [a + 0.2, z0], [b + 0.2, z1], [b - 0.2, z1]], 0.07)
  // The red-roofed house behind, gable to the promenade.
  const [rx, ry] = local(59, -3.0, 24.5)
  const h2 = rect(rx - 4.2, rx + 4.2, ry - 4.5, ry + 4.5)
  block(m.stone, bx(h2), 0, 7.2, { top: m.roof })
  gable(m.tile, m.stone, h2, 7.2, 13.4, false, 0.4)
  gableTimbers(m.timber, faces(h2).s, 7.2, 13.4)
  gableTimbers(m.timber, faces(h2).n, 7.2, 13.4)
  windows(m.win, h2, ['n', 'w'], [[1.0, 2.8], [4.2, 5.8]], 0.9, 2.2)
})
cluster(0, (m) => {
  // Weinkeller and Kunstarbeit in Kristall: orange, stepped gable to the platz.
  const w = rect(2.2, 11.5, -0.5, 7.5)
  block(m.ochre, bx(w), 0, 9.5, { top: m.roof })
  gable(m.tile, m.ochre, w, 9.5, 15.5, true, 0.3)
  stepped(m.ochre, faces(w).w, 9.5, 16.0, 5, 0.25)
  windows(m.win, w, ['w'], [[4.4, 6.2], [7.0, 8.6]], 0.9, 2.0)
  relief(m.timber, faces(w).w, 1.0, 7.0, 3.5, 4.1, 0.9)
  panel(m.win, faces(w).w, roundArch(2.8, 5.2, 0, 2.8, 6), 0.06)
})

// ---------------------------------------------------------------------------
// Materials (STYLE.md): pale render and the castle in the palette's stone;
// the flat show-building roof in `roof`; red tile roofs the palette's
// terracotta; the orange and ochre fronts one muted identity finish; the
// timber of the half-timbering, balconies and clock frame a brown that stays
// lighter than charcoal.

const parts = [
  { part: G.stone, material: PALETTE.stone },
  { part: G.roof, material: PALETTE.roof },
  { part: G.tile, material: PALETTE.terracotta },
  { part: G.win, material: PALETTE.window },
  { part: G.ochre, material: finish('platz-ochre', 0xe2ab7f) },
  { part: G.timber, material: finish('timber', 0x8d6c57) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT Germany pavilion', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 21.5,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.env.LANDMARK_OUT ?? new URL('../../landmarks/models/wdw-epcot-germany.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
