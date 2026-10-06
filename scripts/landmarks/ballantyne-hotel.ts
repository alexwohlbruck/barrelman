/**
 * The Ballantyne Hotel, Charlotte NC — procedural, CC0-1.0.
 * bun scripts/landmarks/ballantyne-hotel.ts
 *
 * Map frame: x across the main block (east-north-east, towards the pond),
 * y along it (north-north-west), z up, metres. Placed at bearing 346°, the
 * axis of the OSM outline (way/322618546, one L-shaped outline, height 40,
 * no parts). The anchor is a point inside the tower, so x and y below read
 * straight off the outline in that rotated frame.
 *
 * The outline holds the whole complex, and the photos split it into:
 * - the tower: nine storeys over a lower level on the pond side, 87 m long,
 *   in cream render with white trim. A pedimented centre pavilion two-fifths
 *   of the way up from its south end carries a gable roof and the white,
 *   dark-domed cupola; a plain block with the hotel's monogram closes the
 *   north end, with a small box on its roof (Commons photos 03 and 04).
 * - on the pond side, a two-storey colonnaded porch under the pediment
 *   (photo 01); on the entrance side, the porte-cochère.
 * - low wings, two storeys over a lower level, under hipped metal roofs:
 *   south of the tower, and the long east arm of meeting rooms and spa.
 * - octagonal pavilions with hipped roofs and ball finials at the corners
 *   (photos 02 and 03).
 *
 * The brief asked for brick; every photo shows cream render with white
 * trim, so that is what the model uses: `stone` and `trim`.
 *
 * Ground: the site falls about 4.5 m from Johnston Road (west) to the pond
 * (east) in the terrain tiles, and the south end of the east arm sits higher
 * again. y = 0 is the lowest point, at the tower's pond side. Walls run down
 * to 0 everywhere, sinking into the slope, and each wall's window panels
 * start at that wall's own ground (`ground()`), so none are buried.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const stone = new Part(), trim = new Part(), roof = new Part(), win = new Part(), dome = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const sub = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
const clamp = (x: number, a: number, b: number) => Math.max(a, Math.min(b, x))

/** A triangle with per-corner normals, its winding fixed to agree with them. */
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) {
  const face = cross(sub(b, a), sub(c, a))
  if (len(face) < 1e-9) return
  const N = n ?? [unit(face), unit(face), unit(face)]
  if (dot(face, add(add(N[0], N[1]), N[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, N)
  else p.tri(a, c, b, undefined, undefined, undefined, [N[0], N[2], N[1]])
}
function poly(p: Part, pts: V3[], n: V3) {
  for (let i = 1; i < pts.length - 1; i++) tri(p, pts[0], pts[i], pts[i + 1], [n, n, n])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}

/**
 * Height of the real ground above y = 0, from the terrain tiles (z15). The
 * tower's half falls west to east; the south block and east arm also rise
 * to the south.
 */
function ground(x: number, y: number) {
  if (y > -47) return clamp(0.3 + (12.7 - x) * 0.145, 0, 4.5)
  return clamp(-0.04 - 0.0385 * x - 0.075 * y, 0, 4.5)
}

// ---------------------------------------------------------------------------
// Chamfered rectangles. Corners are cut by `c` (one value, or one per corner
// SW, SE, NE, NW); a ring grown outward by `d` keeps its faces parallel.

type Rect = { x0: number; x1: number; y0: number; y1: number; c: number | [number, number, number, number] }
function ring(r: Rect, d: number, z: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const cs = typeof r.c === 'number' ? [r.c, r.c, r.c, r.c] : r.c
  const k = cs.map((c) => Math.max(0.02, c + d * 0.414))
  const pts: V3[] = [[x0 + k[0], y0, z], [x1 - k[1], y0, z], [x1, y0 + k[1], z], [x1, y1 - k[2], z],
    [x1 - k[2], y1, z], [x0 + k[3], y1, z], [x0, y1 - k[3], z], [x0, y0 + k[0], z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
function band(p: Part, r: Rect, d0: number, z0: number, d1: number, z1: number, n0: XY, n1: XY) {
  const a = ring(r, d0, z0), b = ring(r, d1, z1)
  const at = (h: V3, n: XY): V3 => unit([h[0] * n[0], h[1] * n[0], n[1]])
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    // A chamfer edge (odd i) whose ends share a face normal is a big cut
    // corner, not a rounded one: shade it flat.
    let na = a.nrm[i], nb = a.nrm[j]
    if (i % 2 === 1) {
      const e = sub(a.pts[j], a.pts[i])
      if (Math.hypot(e[0], e[1]) > 1.2) { na = nb = unit([e[1], -e[0], 0]) }
    }
    quad(p, a.pts[i], a.pts[j], b.pts[j], b.pts[i], [at(na, n0), at(nb, n0), at(nb, n1), at(na, n1)])
  }
}
function lid(p: Part, r: Rect, d: number, z: number, up = true) {
  poly(p, ring(r, d, z).pts, [0, 0, up ? 1 : -1])
}
const OUT: XY = [1, 0], UP: XY = [0, 1], S = Math.SQRT1_2
const DOWN_OUT: XY = [S, -S]

/** A projecting moulding: bevel out from the wall, a face, bevel back to the top. */
function cornice(p: Part, r: Rect, z: number, h: number, proj: number) {
  const b = Math.min(proj, h / 3)
  band(p, r, 0, z, proj, z + b, DOWN_OUT, OUT)
  band(p, r, proj, z + b, proj, z + h - b, OUT, OUT)
  band(p, r, proj, z + h - b, 0, z + h, OUT, UP)
}

/** A flush trim stripe: a belt course too shallow to need bevels at map scale. */
function stripe(p: Part, r: Rect, z: number, h: number) {
  band(p, r, 0.04, z, 0.04, z + h, OUT, OUT)
}

/**
 * A hipped roof over a chamfered rectangle: slopes rise `slope` per metre
 * from an eave overhanging by 0.3 m, either to a ridge (narrow plans) or to
 * a flat top `w` in from the edge (deep plans, where the real roofs are
 * hipped round a flat deck).
 */
function hipRoof(r: Rect, z: number, w: number, slope: number) {
  const half = Math.min(r.x1 - r.x0, r.y1 - r.y0) / 2
  const run = Math.min(w, half - 0.05)
  const rise = run * slope
  const n: XY = [rise, run + 0.3]
  const l = Math.hypot(n[0], n[1])
  band(roof, r, 0.3, z, -run, z + rise, [n[0] / l, n[1] / l], [n[0] / l, n[1] / l])
  lid(roof, r, -run, z + rise)
  // The eave's underside, so the overhang has a thickness from below.
  lid(trim, r, 0.3, z, false)
  return z + rise
}

// ---------------------------------------------------------------------------
// Masses, kept so window panels can tell a wall that faces the open air from
// one buried against a neighbour.

type Mass = { inside: (x: number, y: number) => boolean; top: number }
const masses: Mass[] = []
function rectMass(r: Rect, top: number) {
  masses.push({ inside: (x, y) => x > r.x0 && x < r.x1 && y > r.y0 && y < r.y1, top })
}
function octMass(cx: number, cy: number, a: number, top: number) {
  masses.push({ inside: (x, y) => Math.abs(x - cx) < a && Math.abs(y - cy) < a && Math.abs(x - cx) + Math.abs(y - cy) < a * 1.414, top })
}
const buried = (x: number, y: number, z: number) => masses.some((m) => m.inside(x, y) && m.top > z)

// ---------------------------------------------------------------------------
// Window panels: slate `window` panels laid 4 cm proud of a wall, one per bay
// and storey group, rectangular or with a semicircular head.

type Row = { z0: number; z1: number; arch?: boolean; w?: number }
const ARC = 8

/** Panels along a wall from a to b (outward to the right of a→b, seen from above). */
function panels(a: XY, b: XY, rows: Row[], pitch: number, opts: { width?: number; skip?: (s: number, L: number) => boolean; rowsAt?: (s: number) => Row[] | null; margin?: number } = {}) {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const u: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L]
  const n: V3 = [u[1], -u[0], 0]
  const margin = opts.margin ?? 1.4
  const count = Math.max(0, Math.round((L - 2 * margin) / pitch))
  if (!count) return
  const step = (L - 2 * margin) / count
  for (let i = 0; i < count; i++) {
    const s = margin + step * (i + 0.5)
    if (opts.skip?.(s, L)) continue
    const x = a[0] + u[0] * s, y = a[1] + u[1] * s
    const g = ground(x, y)
    for (const row of opts.rowsAt?.(s) ?? rows) {
      const w = row.w ?? opts.width ?? 1.4
      if (row.z0 < g + 0.6) continue
      if (buried(x + n[0] * 0.6, y + n[1] * 0.6, row.z0 + 0.5)) continue
      panel(a, u, n, s, w, row.z0, row.z1, !!row.arch)
    }
  }
}
function panel(a: XY, u: XY, n: V3, s: number, w: number, z0: number, z1: number, arch: boolean) {
  const off = 0.04
  const P = (ss: number, z: number): V3 => [a[0] + u[0] * ss + n[0] * off, a[1] + u[1] * ss + n[1] * off, z]
  const r = w / 2
  const top = arch ? z1 - r : z1
  quad(win, P(s - r, z0), P(s + r, z0), P(s + r, top), P(s - r, top), n)
  if (!arch) return
  for (let k = 0; k < ARC; k++) {
    const t0 = (k / ARC) * Math.PI, t1 = ((k + 1) / ARC) * Math.PI
    tri(win, P(s, top), P(s + r * Math.cos(t0), top + r * Math.sin(t0)), P(s + r * Math.cos(t1), top + r * Math.sin(t1)), [n, n, n])
  }
}
/** A round window, flush on a wall. */
function oculus(a: XY, u: XY, s: number, z: number, r: number) {
  const n: V3 = [u[1], -u[0], 0]
  const P = (ss: number, zz: number): V3 => [a[0] + u[0] * ss + n[0] * 0.04, a[1] + u[1] * ss + n[1] * 0.04, zz]
  for (let k = 0; k < 12; k++) {
    const t0 = (k / 12) * 2 * Math.PI, t1 = ((k + 1) / 12) * 2 * Math.PI
    tri(win, P(s, z), P(s + r * Math.cos(t0), z + r * Math.sin(t0)), P(s + r * Math.cos(t1), z + r * Math.sin(t1)), [n, n, n])
  }
}
/** The four faces of a rectangle as a→b pairs, outward to the right: S, E, N, W. */
const faces = (r: Rect): [XY, XY][] => [
  [[r.x0, r.y0], [r.x1, r.y0]], [[r.x1, r.y0], [r.x1, r.y1]], [[r.x1, r.y1], [r.x0, r.y1]], [[r.x0, r.y1], [r.x0, r.y0]],
]

// ---------------------------------------------------------------------------
// The tower.

const BELT1 = 4.2, BELT2 = 14.0, BELT3 = 23.7  // over the pond-side lower level, the third and sixth floors
const CORNICE = 33.4, PARAPET = 35.0
// One panel per bay for each group of three floors, with a belt course and a
// broad spandrel between groups, so the bays never run together into stripes.
const TOWER_ROWS: Row[] = [
  { z0: 5.8, z1: 13.1, w: 1.3 }, { z0: 15.4, z1: 22.9, w: 1.3 }, { z0: 25.1, z1: 32.6, w: 1.3 },
]
const LOWER_ROW: Row[] = [{ z0: 1.0, z1: 3.7, arch: true, w: 2.2 }]

function towerWalls(r: Rect, top: number) {
  band(stone, r, 0, 0, 0, BELT1, OUT, OUT)
  cornice(trim, r, BELT1, 0.6, 0.15)
  band(stone, r, 0, BELT1 + 0.6, 0, CORNICE, OUT, OUT)
  stripe(trim, r, BELT2, 0.4)
  stripe(trim, r, BELT3, 0.4)
  cornice(trim, r, CORNICE, 0.8, 0.4)
  if (top > CORNICE + 0.8) band(stone, r, 0, CORNICE + 0.8, 0, top - 0.15, OUT, OUT)
}
function flatTop(r: Rect, top: number) {
  band(trim, r, 0, top - 0.15, -0.3, top, OUT, UP)
  lid(roof, r, -0.3, top)
}

// Loggias: stacks of balconies with an arched head, at the pediment's centre
// and one bay in from each end (photos 01 and 04).
const LOGGIAS = [25.5, 49.0, 2.6]
const loggia = (y: number) => LOGGIAS.some((l) => Math.abs(l - y) < 1.9)

const LOGGIA_ROWS: Row[] = [
  { z0: 5.8, z1: 13.1, w: 2.6 }, { z0: 15.4, z1: 22.9, w: 2.6 }, { z0: 25.1, z1: 32.9, w: 2.6, arch: true },
]

function towerPanels(r: Rect, which: number[], opts: { skip?: (s: number, L: number) => boolean } = {}) {
  faces(r).forEach(([a, b], i) => {
    if (!which.includes(i)) return
    const alongY = i === 1 || i === 3
    const y = (s: number) => (i === 1 ? a[1] + s : a[1] - s)
    panels(a, b, TOWER_ROWS, 3.6, {
      skip: opts.skip,
      rowsAt: (s) => (alongY && loggia(y(s)) ? LOGGIA_ROWS : null),
    })
    // The pond side's lower level, under the first belt.
    panels(a, b, LOWER_ROW, 4.5)
  })
}

// South section, pediment pavilion, north section, monogram block.
const T_SOUTH: Rect = { x0: -14.9, x1: 13.5, y0: -1.3, y1: 15.0, c: 0.4 }
const T_MID: Rect = { x0: -16.3, x1: 14.9, y0: 15.0, y1: 36.0, c: 0.4 }
const T_NORTH: Rect = { x0: -14.9, x1: 13.5, y0: 36.0, y1: 55.6, c: 0.4 }
const T_NEND: Rect = { x0: -15.3, x1: 15.2, y0: 55.6, y1: 68.0, c: 0.4 }
const T_BLOCK: Rect = { x0: -10.1, x1: 14.6, y0: 68.0, y1: 86.0, c: 0.4 }
for (const r of [T_SOUTH, T_MID, T_NORTH, T_NEND, T_BLOCK]) rectMass(r, PARAPET)

for (const r of [T_SOUTH, T_NORTH, T_NEND, T_BLOCK]) { towerWalls(r, PARAPET); flatTop(r, PARAPET) }
towerWalls(T_MID, CORNICE + 0.8)

// Bays on the long faces, the south face above the low wing, and the
// monogram block's west and north faces (its east face is blank but for the
// monogram; the middle of its north face too).
towerPanels(T_SOUTH, [0, 1, 3])
towerPanels(T_MID, [1, 3], { skip: (s, L) => s < 1.2 || s > L - 1.2 })
towerPanels(T_NORTH, [1, 3])
towerPanels(T_NEND, [1, 3])
towerPanels(T_BLOCK, [2, 3], { skip: (s, L) => s > L * 0.3 && s < L * 0.7 })

// The rooftop box on the monogram block.
{
  const r: Rect = { x0: -3.5, x1: 7.5, y0: 72, y1: 82, c: 0.3 }
  band(stone, r, 0, PARAPET - 0.2, 0, 38.3, OUT, OUT)
  flatTop(r, 38.5)
}

// The centre pavilion's gable roof, running across the tower, with a
// pediment at each end: stone tympanum, white raking cornices, an oculus.
const GABLE_Y0 = T_MID.y0, GABLE_Y1 = T_MID.y1, GABLE_YC = (GABLE_Y0 + GABLE_Y1) / 2
const EAVE = CORNICE + 0.8, RIDGE = EAVE + 6.3
{
  const x0 = T_MID.x0 - 0.45, x1 = T_MID.x1 + 0.45, o = 0.35
  const nS = unit([0, -(RIDGE - EAVE), GABLE_YC - GABLE_Y0])
  const nN = unit([0, RIDGE - EAVE, GABLE_YC - GABLE_Y0])
  quad(roof, [x0, GABLE_Y0 - o, EAVE - 0.18], [x1, GABLE_Y0 - o, EAVE - 0.18], [x1, GABLE_YC, RIDGE], [x0, GABLE_YC, RIDGE], nS)
  quad(roof, [x1, GABLE_Y1 + o, EAVE - 0.18], [x0, GABLE_Y1 + o, EAVE - 0.18], [x0, GABLE_YC, RIDGE], [x1, GABLE_YC, RIDGE], nN)
  for (const side of [1, -1] as const) {
    const x = side > 0 ? T_MID.x1 : T_MID.x0
    const n: V3 = [side, 0, 0]
    // Tympanum, set back a little behind the raking cornices.
    tri(stone, [x - side * 0.1, GABLE_Y0, EAVE], [x - side * 0.1, GABLE_Y1, EAVE], [x - side * 0.1, GABLE_YC, RIDGE - 0.4], [n, n, n])
    // Raking cornices: a bevelled bar up each slope.
    for (const [ya, yb] of [[GABLE_Y0 - o, GABLE_YC], [GABLE_Y1 + o, GABLE_YC]] as XY[]) {
      const za = EAVE - 0.18, zb = RIDGE
      const t = unit([0, yb - ya, zb - za]), up = unit([0, -t[2] * Math.sign(yb - ya), Math.abs(t[1])])
      const h = 0.55
      const A: V3 = [x + side * 0.25, ya, za], B: V3 = [x + side * 0.25, yb, zb]
      const A2: V3 = [A[0], A[1] - up[1] * h, A[2] - up[2] * h], B2: V3 = [B[0], B[1] - up[1] * h, B[2] - up[2] * h]
      const Ai: V3 = [x - side * 0.15, A[1], A[2]], Bi: V3 = [x - side * 0.15, B[1], B[2]]
      quad(trim, A2, B2, B, A, n)
      quad(trim, A, B, Bi, Ai, up)
      quad(trim, A2, B2, [Bi[0], B2[1], B2[2]], [Ai[0], A2[1], A2[2]], [-up[0], -up[1], -up[2]])
    }
    oculus([x - side * 0.1, side > 0 ? GABLE_Y0 : GABLE_Y1], [0, side], GABLE_YC - GABLE_Y0, EAVE + 1.9, 0.85)
  }
}

// The cupola on the ridge: a stone base under a little hipped skirt, a white
// octagonal drum with arched openings, a cornice, a dark dome and a spire.
const CUP: XY = [-0.7, GABLE_YC]
function octagon(cx: number, cy: number, R: number, z: number): V3[] {
  return Array.from({ length: 8 }, (_, k) => {
    const t = ((k + 0.5) / 8) * 2 * Math.PI
    return [cx + R * Math.cos(t), cy + R * Math.sin(t), z] as V3
  })
}
/** Walls of an octagonal prism, flat-shaded. */
function octWalls(p: Part, cx: number, cy: number, R: number, z0: number, z1: number, R1 = R) {
  const a = octagon(cx, cy, R, z0), b = octagon(cx, cy, R1, z1)
  for (let k = 0; k < 8; k++) {
    const j = (k + 1) % 8
    const t = ((k + 1) / 8) * 2 * Math.PI
    const n = unit([Math.cos(t) * (z1 - z0), Math.sin(t) * (z1 - z0), R - R1])
    quad(p, a[k], a[j], b[j], b[k], n)
  }
}
function octLid(p: Part, cx: number, cy: number, R: number, z: number, up = true) {
  poly(p, octagon(cx, cy, R, z), [0, 0, up ? 1 : -1])
}
/** An octagonal pyramid roof from an eave ring to an apex. */
function octRoof(p: Part, cx: number, cy: number, R: number, z: number, apex: number) {
  const a = octagon(cx, cy, R, z), top: V3 = [cx, cy, apex]
  for (let k = 0; k < 8; k++) {
    const j = (k + 1) % 8
    tri(p, a[k], a[j], top, (() => { const n = unit(cross(sub(a[j], a[k]), sub(top, a[k]))); return [n, n, n] })())
  }
  octLid(trim, cx, cy, R, z, false)
}
/** A smooth ball or dome: `rings` latitude bands from `lat0` to the pole. */
function ball(p: Part, c: V3, r: number, h: number, lat0 = -Math.PI / 2, seg = 12, rings = 6) {
  const pt = (i: number, j: number) => {
    const th = (i / seg) * 2 * Math.PI, ph = lat0 + (j / rings) * (Math.PI / 2 - lat0)
    const e: V3 = [Math.cos(ph) * Math.cos(th), Math.cos(ph) * Math.sin(th), Math.sin(ph)]
    return { p: [c[0] + r * e[0], c[1] + r * e[1], c[2] + h * e[2]] as V3, n: unit([e[0] / r, e[1] / r, e[2] / h]) }
  }
  for (let i = 0; i < seg; i++)
    for (let j = 0; j < rings; j++) {
      const A = pt(i, j), B = pt(i + 1, j), C = pt(i + 1, j + 1), D = pt(i, j + 1)
      if (j === rings - 1) tri(p, A.p, B.p, C.p, [A.n, B.n, C.n])
      else quad(p, A.p, B.p, C.p, D.p, [A.n, B.n, C.n, D.n])
    }
}
{
  // Proportions from photo 04: the drum about as tall as it is wide, the
  // dome a little lower, the spire as tall again as the dome.
  const [cx, cy] = CUP, R = 3.3
  const base: Rect = { x0: cx - 4.0, x1: cx + 4.0, y0: cy - 4.0, y1: cy + 4.0, c: 0.4 }
  band(roof, base, 0, RIDGE - 1.6, 0, RIDGE - 0.2, OUT, OUT)
  hipRoof(base, RIDGE - 0.2, 1.3, 0.7)
  const z0 = RIDGE + 0.6, z1 = z0 + 5.0
  octWalls(trim, cx, cy, R, RIDGE, z1)
  // Arched openings on the eight faces.
  const corners = octagon(cx, cy, R, 0)
  const side = 2 * R * Math.sin(Math.PI / 8)
  for (let k = 0; k < 8; k++) {
    const t = ((k + 1) / 8) * 2 * Math.PI
    const u: XY = [-Math.sin(t), Math.cos(t)]
    panel([corners[k][0], corners[k][1]], u, [Math.cos(t), Math.sin(t), 0], side / 2, 1.4, z0 + 0.3, z1 - 0.5, true)
  }
  octWalls(trim, cx, cy, R, z1, z1 + 0.2, R + 0.35)
  octWalls(trim, cx, cy, R + 0.35, z1 + 0.2, z1 + 0.65)
  octLid(trim, cx, cy, R + 0.35, z1 + 0.65)
  ball(dome, [cx, cy, z1 + 0.65], R * 0.98, 3.3, 0, 12, 4)
  // The spire: a slim cone on a small ball.
  const zb = z1 + 0.65 + 3.3
  ball(dome, [cx, cy, zb + 0.2], 0.4, 0.4, -Math.PI / 2, 8, 4)
  const tip: V3 = [cx, cy, zb + 5.0]
  for (let k = 0; k < 6; k++) {
    const t0 = (k / 6) * 2 * Math.PI, t1 = ((k + 1) / 6) * 2 * Math.PI
    const p0: V3 = [cx + 0.16 * Math.cos(t0), cy + 0.16 * Math.sin(t0), zb + 0.5], p1: V3 = [cx + 0.16 * Math.cos(t1), cy + 0.16 * Math.sin(t1), zb + 0.5]
    tri(dome, p0, p1, tip, [unit([Math.cos(t0), Math.sin(t0), 0.05]), unit([Math.cos(t1), Math.sin(t1), 0.05]), [0, 0, 1]])
  }
}

// ---------------------------------------------------------------------------
// The pond-side porch under the pediment: a solid lower level with arched
// windows, two storeys of white columns, an entablature and a terrace on top.

{
  const r: Rect = { x0: 13.5, x1: 18.7, y0: 11.9, y1: 39.2, c: 0.3 }
  rectMass({ ...r, x0: 13.6 }, BELT1 + 0.6)
  band(stone, r, 0, 0, 0, BELT1, OUT, OUT)
  cornice(trim, r, BELT1, 0.6, 0.15)
  lid(trim, r, 0, BELT1 + 0.6)
  panels([r.x1, r.y0], [r.x1, r.y1], LOWER_ROW, 4.5)
  panels([r.x0, r.y0], [r.x1, r.y0], LOWER_ROW, 4.5)
  panels([r.x1, r.y1], [r.x0, r.y1], LOWER_ROW, 4.5)
  const colTop = 11.2
  const columns: XY[] = []
  for (let k = 0; k <= 7; k++) columns.push([r.x1 - 0.6, r.y0 + 0.6 + (k * (r.y1 - r.y0 - 1.2)) / 7])
  columns.push([r.x0 + 2.4, r.y0 + 0.6], [r.x0 + 2.4, r.y1 - 0.6])
  for (const [x, y] of columns) octWalls(trim, x, y, 0.42, BELT1 + 0.6, colTop)
  const top: Rect = { ...r, c: 0.3 }
  band(trim, top, 0, colTop, 0, colTop + 0.9, OUT, OUT)
  lid(trim, top, 0, colTop, false)
  flatTop(top, colTop + 1.2)
  band(trim, top, 0, colTop + 0.9, 0, colTop + 1.05, OUT, OUT)
}

// The porte-cochère on the entrance (west) side: white piers carrying an
// entablature and a flat roof.
{
  const r: Rect = { x0: -25.8, x1: -15.7, y0: 19.6, y1: 30.5, c: 0.3 }
  const top = 11.6
  for (const [x, y] of [[r.x0 + 0.6, r.y0 + 0.6], [r.x0 + 0.6, r.y1 - 0.6], [r.x0 + 0.6, 25.05], [r.x1 - 0.8, r.y0 + 0.6], [r.x1 - 0.8, r.y1 - 0.6]] as XY[]) {
    const p: Rect = { x0: x - 0.55, x1: x + 0.55, y0: y - 0.55, y1: y + 0.55, c: 0.12 }
    band(trim, p, 0, 0, 0, top - 1.2, OUT, OUT)
  }
  band(trim, r, 0, top - 1.2, 0, top - 0.2, OUT, OUT)
  lid(trim, r, 0, top - 1.2, false)
  cornice(trim, r, top - 0.2, 0.5, 0.2)
  flatTop(r, top + 0.3)
}

// ---------------------------------------------------------------------------
// The low wings: render walls with arched windows on the main floor and
// square ones above, a cornice, and a hipped metal roof.

const WING_EAVE = 12.0
const WING_ROWS: Row[] = [
  { z0: 1.2, z1: 3.6, w: 1.4 },
  { z0: 5.4, z1: 8.6, arch: true, w: 1.8 },
  { z0: 9.4, z1: 11.2, w: 1.4 },
]
function wing(r: Rect, eave = WING_EAVE, hip = 4.5) {
  rectMass(r, eave + 0.6)
  band(stone, r, 0, 0, 0, eave, OUT, OUT)
  stripe(trim, r, BELT1, 0.5)
  cornice(trim, r, eave, 0.6, 0.3)
  return { r, eave, hip }
}
function wingWindows(r: Rect, which = [0, 1, 2, 3]) {
  const { pts } = ring(r, 0, 0)
  // The ring's straight faces, between the chamfers.
  const sides: [XY, XY][] = [0, 2, 4, 6].map((i) => [[pts[i][0], pts[i][1]], [pts[i + 1][0], pts[i + 1][1]]])
  sides.forEach(([a, b], i) => { if (which.includes(i)) panels(a, b, WING_ROWS, 4.2, { margin: 1.2 }) })
}

const L_SOUTH: Rect = { x0: -14.6, x1: 15.4, y0: -47.0, y1: -1.3, c: 0.3 }
const L_SW: Rect = { x0: -16.5, x1: 20.6, y0: -83.0, y1: -47.0, c: [3.4, 0.3, 0.3, 0.3] }
const E_WEST: Rect = { x0: 20.6, x1: 38.5, y0: -72.5, y1: -47.0, c: 0.3 }
const E_MAIN: Rect = { x0: 38.5, x1: 89.0, y0: -76.5, y1: -55.5, c: 0.3 }
const E_END: Rect = { x0: 89.0, x1: 95.5, y0: -76.2, y1: -53.7, c: 0.3 }
const E_BUMP: Rect = { x0: 34.1, x1: 40.0, y0: -48.0, y1: -42.1, c: 0.3 }
const E_SOUTH: Rect = { x0: 57.7, x1: 76.8, y0: -80.0, y1: -76.5, c: 0.3 }
const W_ENTRY: Rect = { x0: -25.0, x1: -14.6, y0: -44.5, y1: -37.9, c: 0.3 }

const wings = [wing(L_SOUTH), wing(L_SW), wing(E_WEST), wing(E_MAIN), wing(E_END, 13.0, 3.3)]
// Small one-storey annexes with flat roofs.
for (const r of [E_BUMP, E_SOUTH, W_ENTRY]) {
  rectMass(r, 9.6)
  band(stone, r, 0, 0, 0, 9.0, OUT, OUT)
  cornice(trim, r, 9.0, 0.5, 0.25)
  flatTop(r, 9.8)
}

// ---------------------------------------------------------------------------
// Octagonal pavilions with hipped roofs and ball finials: one at the corner
// between the south wing and the east arm, facing the pond (photo 01), and
// one at the tower's north-west corner (photos 02 and 03).

function pavilion(cx: number, cy: number, a: number, eave: number, apex: number) {
  const R = a / Math.cos(Math.PI / 8)
  octMass(cx, cy, a, eave)
  octWalls(stone, cx, cy, R, 0, eave)
  octWalls(trim, cx, cy, R, eave, eave + 0.25, R + 0.3)
  octWalls(trim, cx, cy, R + 0.3, eave + 0.25, eave + 0.7)
  octRoof(roof, cx, cy, R + 0.35, eave + 0.7, apex)
  ball(trim, [cx, cy, apex + 0.55], 0.6, 0.6, -Math.PI / 2, 10, 5)
  return R
}
function pavilionWindows(cx: number, cy: number, R: number, eave: number) {
  const pts = octagon(cx, cy, R, 0)
  for (let k = 0; k < 8; k++) {
    const j = (k + 1) % 8
    const a: XY = [pts[k][0], pts[k][1]], b: XY = [pts[j][0], pts[j][1]]
    const L = Math.hypot(b[0] - a[0], b[1] - a[1])
    const u: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], n: V3 = [u[1], -u[0], 0]
    const mx = (a[0] + b[0]) / 2 + n[0] * 0.6, my = (a[1] + b[1]) / 2 + n[1] * 0.6
    const g = ground(mx, my)
    if (buried(mx, my, 6)) continue
    if (g < 3.2) panel(a, u, n, L / 2, 2.2, 1.0, 3.8, true)
    panel(a, u, n, L / 2, 2.4, Math.max(5.0, g + 0.8), eave - 3.0, true)
    oculus(a, [u[0], u[1]], L / 2, eave - 1.5, 0.7)
  }
}
const C_POND: [number, number, number] = [20.7, -40.5, 5.9]
const C_NW: [number, number, number] = [-7.0, 86.0, 6.2]
const rPond = pavilion(C_POND[0], C_POND[1], C_POND[2], 13.5, 19.0)
const rNW = pavilion(C_NW[0], C_NW[1], C_NW[2], 14.0, 19.8)

// Windows last, once every mass is known.
wingWindows(L_SOUTH, [1, 3])
wingWindows(L_SW, [0, 1, 3])
wingWindows(E_WEST, [0, 2])
wingWindows(E_MAIN, [0, 1, 2])
wingWindows(E_END, [0, 1, 2])
for (const w of wings) hipRoof(w.r, w.eave + 0.6, w.hip, 0.55)
pavilionWindows(C_POND[0], C_POND[1], rPond, 13.5)
pavilionWindows(C_NW[0], C_NW[1], rNW, 14.0)

// ---------------------------------------------------------------------------

// Colours on the shared palette. The render is the palette's `stone` cream,
// the cornices, columns and cupola its `trim`; the standing-seam roofs `roof`.
// The cupola's dome reads dark slate in the photos; it keeps a slate tone
// pulled up to the palette's lightness.
const parts = [
  { part: stone, material: PALETTE.stone },
  { part: trim, material: PALETTE.trim },
  { part: roof, material: PALETTE.roof },
  { part: win, material: PALETTE.window },
  { part: dome, material: finish('cupola-dome', 0x8794a1) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (process.env.DEBUG) for (const { part, material } of parts) console.log(material.name, part.triangles)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Ballantyne Hotel', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 346, elevation: 0, height: RIDGE + 11,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/ballantyne-hotel.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
