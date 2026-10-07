/**
 * EPCOT Italy pavilion: the Campanile and the Doge's Palace — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-italy.ts
 *
 * Map frame: x across the piazza (east-ish), y towards the lagoon, z up,
 * metres. Placed at bearing 347°, the axis shared by the campanile outline
 * (way/295592599, edges at 345°/75°) and the Doge's Palace outline
 * (way/295592598, west face at 347°). The anchor is the campanile outline's
 * centroid, so the tower stands on the origin and the palace sits east of it.
 *
 * What it is: Disney's reduced copy of Piazza San Marco. A red-brick bell
 * tower with white belfry, attic and green pyramidal spire topped by a gilded
 * angel; beside it the pink-and-white Doge's Palace, a Gothic arcade on the
 * ground, a loggia of small arches under a band of quatrefoils, a pink
 * diamond-patterned upper wall with a few big pointed windows, a white
 * crenellated cresting and a low pale green hip roof; and the two columns of
 * San Marco (winged lion) and San Todaro on the piazza towards the lagoon.
 *
 * Evidence
 * - OSM (measured): campanile outline 3.7 × 4.5 m (drawn as a 4.0 m square);
 *   palace outline 14.1 × 20.7 m plus a 2.7 × 7 m bump on its back (east)
 *   face; the two columns' plinths (way/493363885, way/493363886), 3.8 × 2.7 m,
 *   8.5 m apart, just north of the palace. No heights are tagged anywhere.
 * - Published: the tower is 83 ft (25.3 m) tall (WDW News Today, AllEars).
 * - Photos (Wikimedia Commons, see the report/provenance): from the lagoon
 *   (Lake Italy, Epcot.JPG), the piazza (Italy Epcot Panarama.JPG) and the
 *   south-west corner (EpcotItalyDoge.JPG). Proportions of the tower's stages
 *   are measured off the lagoon photo; the tower is 6.9 widths tall there,
 *   which with OSM's 3.7–4.5 m width gives 26–29 m, so the model takes 26.5 m
 *   to the angel's head, a little over the published 83 ft.
 * - Estimated from the photos: palace cornice 10 m (9.2–9.8 m against the
 *   tower in two photos), cresting 0.7 m, roof ridge 1.0 m above the cornice (only a sliver shows over the cresting from the lagoon); the
 *   columns 9.8 m to the statues' heads. Window counts per face are from the
 *   photos; the east (service) face has no photo and is left plain.
 * - Invented: the shapes of the angel and the two statues, which are only
 *   tokens at map scale.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const brick = new Part(), stone = new Part(), pink = new Part()
const win = new Part(), green = new Part(), gold = new Part()

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

// ---------------------------------------------------------------------------
// The Campanile. Stage heights are fractions of the lagoon photo's tower.

const T = 26.5
const W = 2.0                      // shaft half-width (4.0 m square)
const zShaft = 0.511 * T           // top of the brick shaft
const zCorbel = 0.548 * T          // white arcaded band
const zBelfryBase = zCorbel + 0.45
const zBelfry = 0.632 * T          // top of the belfry arcade
const zCornice = 0.672 * T
const zAttic = 0.732 * T
const zSpire = 0.757 * T
const zTip = 0.968 * T

// Shaft: a brick core with five pilaster strips per face (St Mark's lesenes),
// corner piers included, from the plinth to the white band.
block(brick, sq(0, 0, W - 0.1, 0.05), 0, zShaft, { top: null })
block(stone, sq(0, 0, W + 0.05, 0.1), 0, 0.5, { bt: 0.1, top: null })
for (const f of Object.values(faces(sq(0, 0, W - 0.1)))) {
  for (const s of [0.15, 1.17, 1.9, 2.63, 3.65].map((v) => v * (f.L / 3.8))) {
    const half = 0.24
    relief(brick, f, Math.max(0, s - half), Math.min(f.L, s + half), 0.5, zShaft, 0.1)
  }
}
// Arched door on the piazza (west) face.
{
  const f = faces(sq(0, 0, W - 0.1)).w
  panel(win, f, roundArch(f.L / 2 - 0.55, f.L / 2 + 0.55, 0.5, 2.3), 0.12)
}
// The white arcaded band under the belfry, a little wider than the shaft.
block(stone, sq(0, 0, W + 0.08, 0.1), zShaft, zCorbel, { bb: 0.12, top: null })
// Belfry: balustraded base, four round arches a face, heavy cornice.
block(stone, sq(0, 0, W + 0.18, 0.12), zCorbel, zBelfryBase, { bt: 0.08, top: null })
block(stone, sq(0, 0, W, 0.12), zBelfryBase, zBelfry, { top: null })
for (const f of Object.values(faces(sq(0, 0, W)))) {
  const span = (f.L - 0.5) / 4
  for (let i = 0; i < 4; i++) {
    const s0 = 0.25 + i * span + 0.1, s1 = 0.25 + (i + 1) * span - 0.1
    panel(win, f, roundArch(s0, s1, zBelfryBase + 0.15, zBelfry - 0.35 - (s1 - s0) / 2), 0.03)
  }
}
block(stone, sq(0, 0, W + 0.25, 0.15), zBelfry, zCornice, { bb: 0.2, bt: 0.12, top: null })
// Attic: brick with a white relief panel (the lions and Justice) on each face.
block(brick, sq(0, 0, W - 0.1, 0.06), zCornice, zAttic, { top: null })
for (const f of Object.values(faces(sq(0, 0, W - 0.1)))) {
  relief(stone, f, 0.5, f.L - 0.5, zCornice + 0.35, zAttic - 0.3, 0.06)
  relief(brick, f, 0.85, f.L - 0.85, zCornice + 0.6, zAttic - 0.55, 0.1)
}
block(stone, sq(0, 0, W + 0.12, 0.12), zAttic, zSpire, { bb: 0.12, bt: 0.1, top: stone })
// Spire: a green pyramid with white corner pinnacles at its foot.
{
  const b = W - 0.2, z0 = zSpire
  const c: V3[] = [[-b, -b, z0], [b, -b, z0], [b, b, z0], [-b, b, z0]]
  const apex: V3 = [0, 0, zTip]
  for (let i = 0; i < 4; i++) {
    const a = c[i], d = c[(i + 1) % 4]
    const n = unit(cross([d[0] - a[0], d[1] - a[1], 0], [apex[0] - a[0], apex[1] - a[1], apex[2] - a[2]]))
    tri(green, a, d, apex, [n, n, n])
  }
  // Pale ridges down the four hips, as on the real spire.
  for (const [x, y] of [[-1, -1], [1, -1], [1, 1], [-1, 1]] as XY[]) {
    const p0: V3 = [x * (b + 0.05), y * (b + 0.05), z0 + 0.02], p1: V3 = [0, 0, zTip + 0.02]
    const side: V3 = unit([-y, x, 0]), out: V3 = unit([x, y, 0.55])
    const w0 = 0.12
    quad(stone, add(p0, side, -w0), add(p0, side, w0), add(p1, side, 0.02), add(p1, side, -0.02), out)
    block(stone, sq(x * (b + 0.02), y * (b + 0.02), 0.2, 0.04), z0, z0 + 0.8, { bt: 0.1, top: stone })
    cylinder(stone, x * (b + 0.02), y * (b + 0.02), 0.16, 0.0, z0 + 0.8, z0 + 1.15, 6, false)
  }
}
// The gilded angel: a small sphere-topped figure with raised wings.
{
  const z0 = zTip - 0.05
  cylinder(gold, 0, 0, 0.18, 0.12, z0, z0 + 0.25, 8)
  cylinder(gold, 0, 0, 0.17, 0.08, z0 + 0.25, z0 + 0.75, 8)
  cylinder(gold, 0, 0, 0.1, 0.0, z0 + 0.75, z0 + 0.95, 8, false)
  for (const sx of [-1, 1]) {
    const a: V3 = [0.05 * sx, 0, z0 + 0.6], b: V3 = [0.45 * sx, 0.05, z0 + 0.95], c: V3 = [0.4 * sx, 0, z0 + 0.35]
    tri(gold, a, b, c, [[0, -1, 0], [0, -1, 0], [0, -1, 0]])
    tri(gold, a, c, b, [[0, 1, 0], [0, 1, 0], [0, 1, 0]])
  }
}

// ---------------------------------------------------------------------------
// The Doge's Palace, from OSM: x 4.5–18.6, y −5.1–15.6, with a bump on the
// back (east) face.

const P = { x0: 4.5, x1: 18.6, y0: -5.1, y1: 15.6 }
const zArcade = 3.6, zLoggia = 6.5, zTop = 10.0, zCrest = 10.7, zRidge = 11.0
const PF = faces(P)
// Lower storeys in cream stone, the pink wall above, a thin white cornice.
block(stone, { ...P, c: 0.15 }, 0, zLoggia, { top: null })
block(pink, { ...P, c: 0.15 }, zLoggia, zTop, { top: null })
block(stone, { ...P, c: 0.15 }, zTop - 0.25, zTop, { bb: 0.12, top: null })
block(stone, box(P.x1 - 0.2, 21.3, 1.5, 8.5), 0, zLoggia, { top: null })
block(pink, box(P.x1 - 0.2, 21.3, 1.5, 8.5), zLoggia, zTop - 0.6, { bt: 0.25, top: green })

// Ground arcade (pointed arches, ~2.3 m bays) and the loggia above it (twice
// as many, smaller arches under a row of quatrefoil roundels), on the three
// public faces. The back face is plain stone.
for (const [key, bays] of [['w', 9], ['n', 6], ['s', 6]] as const) {
  const f = PF[key]
  const bay = (f.L - 0.4) / bays
  for (let i = 0; i < bays; i++) {
    const s0 = 0.2 + i * bay + 0.32, s1 = 0.2 + (i + 1) * bay - 0.32
    panel(win, f, pointedArch(s0, s1, 0, zArcade - 0.35 - (s1 - s0) * 0.87), 0.04)
  }
  const small = bay / 2
  for (let i = 0; i < bays * 2; i++) {
    const s0 = 0.2 + i * small + 0.2, s1 = 0.2 + (i + 1) * small - 0.2
    panel(win, f, pointedArch(s0, s1, zArcade + 0.75, zLoggia - 1.25 - (s1 - s0) * 0.87, 2), 0.04)
    if (i > 0) panel(win, f, circle(0.2 + i * small, zLoggia - 0.5, 0.34, 8), 0.04)
  }
  // The loggia's balustrade: a pale band proud of the wall.
  relief(stone, f, 0.15, f.L - 0.15, zArcade + 0.1, zArcade + 0.75, 0.12)
}

// Upper wall: big pointed windows and roundels, and on the piazza face the
// white ceremonial balcony bay rising through the cresting.
{
  const w = PF.w, mid = w.L / 2 // the windows sit between the loggia and the cresting
  for (const s of [mid - 7.9, mid - 4.4, mid + 4.4, mid + 7.9]) panel(win, w, pointedArch(s - 0.62, s + 0.62, 7.2, 8.5, 3), 0.04)
  for (const s of [mid - 6.15, mid + 6.15]) panel(win, w, circle(s, 9.15, 0.36, 8), 0.04)
  relief(stone, w, mid - 1.4, mid + 1.4, zLoggia, zTop + 0.9, 0.18)
  panel(win, w, pointedArch(mid - 0.6, mid + 0.6, 7.0, 8.4, 3), 0.22)
  block(stone, sq(w.o[0] - 0.12, w.o[1] - mid, 0.32, 0.05), zTop + 0.9, zTop + 1.9, { top: stone })
  for (const key of ['n', 's'] as const) {
    const f = PF[key], m = f.L / 2
    for (const s of [m - 3.3, m + 3.3]) panel(win, f, pointedArch(s - 0.62, s + 0.62, 7.2, 8.5, 3), 0.04)
    for (const s of [m - 5.6, m, m + 5.6]) panel(win, f, circle(s, 9.15, 0.36, 8), 0.04)
  }
}

// The cresting: a row of white pointed merlons along every edge, drawn as
// thin double-faced teeth on the wall line.
for (const f of Object.values(PF)) {
  const n = Math.round(f.L / 0.75), step = f.L / n
  for (let i = 0; i < n; i++) {
    const s0 = i * step + 0.12, s1 = (i + 1) * step - 0.12, sm = (s0 + s1) / 2
    const a = at(f, s0, zTop - 0.02, -0.05), b = at(f, s1, zTop - 0.02, -0.05), c = at(f, sm, zCrest, -0.05)
    const nn = f.n, back: V3 = [-nn[0], -nn[1], 0]
    tri(stone, a, b, c, [nn, nn, nn])
    tri(stone, a, c, b, [back, back, back])
  }
}
// Low hip roof, pale green metal, set inside the cresting.
hip(green, { x0: P.x0 + 0.35, x1: P.x1 - 0.35, y0: P.y0 + 0.35, y1: P.y1 - 0.35 }, zTop - 0.02, zRidge, true)

// ---------------------------------------------------------------------------
// The two columns, on their OSM plinths: San Marco's winged lion (bronze,
// drawn in the roof's green) and San Todaro.

for (const [cx, cy, lion] of [[2.6, 18.5, true], [-5.9, 19.9, false]] as [number, number, boolean][]) {
  block(stone, sq(cx, cy, 1.5, 0.1), 0, 0.3, { top: null })
  block(stone, sq(cx, cy, 1.15, 0.1), 0.3, 0.6, { top: null })
  block(stone, sq(cx, cy, 0.8, 0.08), 0.6, 1.1, { bt: 0.08 })
  cylinder(stone, cx, cy, 0.42, 0.36, 1.1, 7.7, 12, false)
  block(stone, sq(cx, cy, 0.55, 0.06), 7.7, 8.3, { bb: 0.12, bt: 0.06 })
  if (lion) {
    block(green, box(cx - 0.75, cx + 0.75, cy - 0.32, cy + 0.32, 0.1), 8.3, 9.0, { bt: 0.12 })
    block(green, box(cx + 0.3, cx + 0.85, cy - 0.25, cy + 0.25, 0.1), 8.8, 9.45, { bt: 0.12 })
    for (const sy of [-1, 1]) {
      const a: V3 = [cx - 0.4, cy + sy * 0.3, 8.9], b: V3 = [cx + 0.25, cy + sy * 0.3, 9.0], c: V3 = [cx - 0.55, cy + sy * 0.3, 9.85]
      tri(green, a, b, c, [[0, sy, 0], [0, sy, 0], [0, sy, 0]])
      tri(green, a, c, b, [[0, -sy, 0], [0, -sy, 0], [0, -sy, 0]])
    }
  } else {
    cylinder(stone, cx, cy, 0.3, 0.24, 8.3, 9.4, 8, false)
    cylinder(stone, cx, cy, 0.17, 0.0, 9.4, 9.85, 8, false)
  }
}

// ---------------------------------------------------------------------------
// Materials (STYLE.md): the tower's red brick is the palette's terracotta,
// the white stone the palette's stone, the palace's pink a muted identity
// finish; the spire, the palace roof and the bronze lion share one green.

const parts = [
  { part: brick, material: PALETTE.terracotta },
  { part: stone, material: PALETTE.stone },
  { part: pink, material: finish('doge-pink', 0xe8bfb2) },
  { part: win, material: PALETTE.window },
  { part: green, material: PALETTE.copper },
  { part: gold, material: finish('angel-gold', 0xd9b04a, 0.5) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT Italy pavilion', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 347, elevation: 0, height: T,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-italy.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
