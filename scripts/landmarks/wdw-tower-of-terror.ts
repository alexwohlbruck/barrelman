/**
 * The Twilight Zone Tower of Terror (the Hollywood Tower Hotel), Disney's
 * Hollywood Studios — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/wdw-tower-of-terror.ts
 *
 * Map frame: x along the hotel's front (+x towards the south-east wing), y
 * back into the building, z up, metres. Placed at bearing 41°, the axis of the
 * OSM outline, so the sign front (−y) faces 221°, down Sunset Boulevard. The
 * anchor is the centroid of the outline way/530750421.
 *
 * Evidence
 * - OSM (measured): outline way/530750421 and the three buildings mapped
 *   inside it (way/446195251 tower and back wing, way/446195252 front wing,
 *   way/530750420 lobby) plus way/691274043, an octagon inside the lobby.
 *   None carries a height. All five sit square to a 41° grid, which is the
 *   bearing.
 * - Published: 199 ft (60.7 m) to the top, kept under 200 ft so the tower
 *   needs no aircraft beacon (Wikipedia, "The Twilight Zone Tower of Terror").
 * - USGS NAIP orthophoto (public domain): plan, the red tile roofs, and the
 *   tower's shadow, whose width puts the tall block at roughly 20–25 m deep.
 * - Photos (Wikimedia Commons), for heights scaled off the 60.7 m tip and the
 *   ~34 m width of the front:
 *     "Hollywood Tower Hotel (29380298128).jpg", HarshLight, CC BY 2.0 — front
 *     "Hollywood Tower Hotel.jpg", Benjamin D. Esham, CC BY-SA 4.0 — front, down Sunset Blvd
 *     "Hollywood Tower Hotel - panoramio.jpg", Jake Toczek, CC BY 3.0 — front and SE wing
 *     "The Twilight Zone Tower of Terror, 2024.jpg", Jedi94, CC BY-SA 4.0 — front
 *     "Tower of Terror (Disney's Hollywood Studios)-May 2023.JPG", Benoît Prieur, CC0 — from the south-east
 *     "Tower of Terror (Disney's Hollywood Studios)-May 2023 - 2.JPG", Benoît Prieur, CC0 — rear crown
 *     "Hollywood Tower Hotel (49560241018).jpg", Eden, Janine and Jim, CC BY 2.0 — from the queue garden
 *
 * Estimated, and worth checking: the tall block's depth (22 m, from the
 * shadow and from how little of the side shows in the Sunset Boulevard
 * photos); the back wing's height (27 m), which no licensed photo shows
 * clearly; the wing heights (13, 24 and 33 m) scaled from the photos. The
 * crown is simplified to six spired turrets, a tiled hip roof and the central
 * pavilion. The sign carries its lettering, as STYLE.md allows for a sign
 * that is the landmark: block capitals, not the real typeface.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const stucco = new Part(), tile = new Part(), scorch = new Part()
const trim = new Part(), win = new Part()
const metal = trim // the spires are pale render, like the trim

const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle whose winding is fixed to face `n` (flat) or its corner normals. */
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3 | V3[]) {
  const f = cross(sub(b, a), sub(c, a))
  if (len(f) < 1e-9) return
  const N = n === undefined ? undefined : Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3]
  const ref = N ? unit([N[0][0] + N[1][0] + N[2][0], N[0][1] + N[1][1] + N[2][1], N[0][2] + N[1][2] + N[2][2]]) : f
  if (dot(f, ref) >= 0) p.tri(a, b, c, undefined, undefined, undefined, N)
  else p.tri(a, c, b, undefined, undefined, undefined, N && [N[0], N[2], N[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3 | V3[]) {
  const N = n === undefined ? undefined : Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, N && [N[0], N[1], N[2]])
  tri(p, a, c, d, N && [N[0], N[2], N[3]])
}

// ---------------------------------------------------------------------------
// Blocks: rectangles with chamfered vertical corners and a rounded top edge.

type Rect = { x0: number; x1: number; y0: number; y1: number }
function ring(r: Rect, d: number, z: number, c: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const k = Math.max(0.02, c + d * 0.414)
  const pts: V3[] = [[x0 + k, y0, z], [x1 - k, y0, z], [x1, y0 + k, z], [x1, y1 - k, z],
    [x1 - k, y1, z], [x0 + k, y1, z], [x0, y1 - k, z], [x0, y0 + k, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
type XY = [number, number]
function band(p: Part, r: Rect, c: number, d0: number, z0: number, d1: number, z1: number, n0: XY, n1: XY) {
  const a = ring(r, d0, z0, c), b = ring(r, d1, z1, c)
  const at = (h: V3, n: XY): V3 => unit([h[0] * n[0], h[1] * n[0], n[1]])
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    quad(p, a.pts[i], a.pts[j], b.pts[j], b.pts[i], [at(a.nrm[i], n0), at(a.nrm[j], n0), at(a.nrm[j], n1), at(a.nrm[i], n1)])
  }
}
function lid(p: Part, r: Rect, d: number, z: number, c: number, up = true) {
  const { pts } = ring(r, d, z, c)
  const n: V3 = [0, 0, up ? 1 : -1]
  for (let i = 1; i < 7; i++) tri(p, pts[0], pts[i], pts[i + 1], n)
}
const OUT: XY = [1, 0], UP: XY = [0, 1], DOWN: XY = [0, -1], S = Math.SQRT1_2
/** A solid block from z0 to z1 with a bevel `bt` rolling over its top edge. */
function block(p: Part, r: Rect, z0: number, z1: number, o: { c?: number; bt?: number; top?: Part | null } = {}) {
  const c = o.c ?? 0.4, bt = o.bt ?? 0.35
  band(p, r, c, 0, z0, 0, z1 - bt, OUT, OUT)
  if (bt) band(p, r, c, 0, z1 - bt, -bt, z1, OUT, UP)
  if (o.top !== null) lid(o.top ?? p, r, -bt, z1, c)
}
/** A projecting cornice: rounded under-edge, face, rounded top. */
function cornice(p: Part, r: Rect, z0: number, z1: number, out: number, top: Part = p) {
  const c = 0.4
  band(p, r, c, 0, z0, out, z0 + out, DOWN, OUT)
  band(p, r, c, out, z0 + out, out, z1 - 0.2, OUT, OUT)
  band(p, r, c, out, z1 - 0.2, out - 0.2, z1, OUT, UP)
  lid(top, r, out - 0.2, z1, c)
}

/** A hipped tile roof over a rectangle, eaves at z0, overhanging by `o`. */
function hip(p: Part, r: Rect, z0: number, rise: number, o = 0.5) {
  const x0 = r.x0 - o, x1 = r.x1 + o, y0 = r.y0 - o, y1 = r.y1 + o
  const w = x1 - x0, d = y1 - y0
  const h = Math.min(w, d) / 2
  const z1 = z0 + rise
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2
  let a: V3, b: V3
  if (w >= d) { a = [x0 + h, cy, z1]; b = [x1 - h, cy, z1] } else { a = [cx, y0 + h, z1]; b = [cx, y1 - h, z1] }
  const c00: V3 = [x0, y0, z0], c10: V3 = [x1, y0, z0], c11: V3 = [x1, y1, z0], c01: V3 = [x0, y1, z0]
  const nOf = (u: V3, v: V3, q: V3): V3 => unit(cross(sub(v, u), sub(q, u)))
  if (w >= d) {
    quad(p, c00, c10, b, a, nOf(c00, c10, b))
    quad(p, c11, c01, a, b, nOf(c11, c01, a))
    tri(p, c10, c11, b, nOf(c10, c11, b))
    tri(p, c01, c00, a, nOf(c01, c00, a))
  } else {
    quad(p, c10, c11, b, a, nOf(c10, c11, b))
    quad(p, c01, c00, a, b, nOf(c01, c00, a))
    tri(p, c00, c10, a, nOf(c00, c10, a))
    tri(p, c11, c01, b, nOf(c11, c01, b))
  }
  // Fascia and soffit, so the eave reads as a thick tiled edge.
  const t = 0.35
  const lo = [c00, c10, c11, c01].map(([x, y]) => [x, y, z0 - t] as V3)
  const hi = [c00, c10, c11, c01]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    const n = unit(cross(sub(hi[j], lo[i]), [0, 0, 1]))
    quad(p, lo[i], lo[j], hi[j], hi[i], [-n[0], -n[1], 0])
  }
  tri(p, lo[0], lo[2], lo[1], [0, 0, -1]); tri(p, lo[0], lo[3], lo[2], [0, 0, -1])
}

/** A square pyramid cap, `half` wide, from z0 to z0 + h. */
function pyramid(p: Part, cx: number, cy: number, half: number, z0: number, h: number) {
  const q: V3[] = [[cx - half, cy - half, z0], [cx + half, cy - half, z0], [cx + half, cy + half, z0], [cx - half, cy + half, z0]]
  const top: V3 = [cx, cy, z0 + h]
  for (let i = 0; i < 4; i++) tri(p, q[i], q[(i + 1) % 4], top, unit(cross(sub(q[(i + 1) % 4], q[i]), sub(top, q[i]))))
  tri(p, q[0], q[2], q[1], [0, 0, -1]); tri(p, q[0], q[3], q[2], [0, 0, -1])
}
/** A slim smooth-shaded spire: a cone on a short round base. */
function spire(p: Part, cx: number, cy: number, r: number, z0: number, h: number, seg = 8) {
  const top: V3 = [cx, cy, z0 + h]
  const slope = r / h
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * 2 * Math.PI, b = ((k + 1) / seg) * 2 * Math.PI
    const pa: V3 = [cx + r * Math.cos(a), cy + r * Math.sin(a), z0], pb: V3 = [cx + r * Math.cos(b), cy + r * Math.sin(b), z0]
    const na = unit([Math.cos(a), Math.sin(a), slope]), nb = unit([Math.cos(b), Math.sin(b), slope])
    const nm = unit([Math.cos((a + b) / 2), Math.sin((a + b) / 2), slope])
    tri(p, pa, pb, top, [na, nb, nm])
  }
}

// ---------------------------------------------------------------------------
// Faces: flat panels laid on a wall, 4 cm proud of it.

type Face = { o: V3; u: V3; n: V3 }
/** The four walls of a rectangle, each running counter-clockwise from above. */
const faces = (r: Rect) => ({
  front: { o: [r.x0, r.y0, 0], u: [1, 0, 0], n: [0, -1, 0] } as Face,
  east: { o: [r.x1, r.y0, 0], u: [0, 1, 0], n: [1, 0, 0] } as Face,
  back: { o: [r.x1, r.y1, 0], u: [-1, 0, 0], n: [0, 1, 0] } as Face,
  west: { o: [r.x0, r.y1, 0], u: [0, -1, 0], n: [-1, 0, 0] } as Face,
})
const on = (f: Face, s: number, z: number, d = 0.04): V3 =>
  [f.o[0] + f.u[0] * s + f.n[0] * d, f.o[1] + f.u[1] * s + f.n[1] * d, z]
function panel(p: Part, f: Face, s0: number, s1: number, z0: number, z1: number, d = 0.04) {
  quad(p, on(f, s0, z0, d), on(f, s1, z0, d), on(f, s1, z1, d), on(f, s0, z1, d), f.n)
}
/** A round-headed window: a rectangle under a semicircle. */
function arched(p: Part, f: Face, s0: number, s1: number, z0: number, spring: number, d = 0.04) {
  panel(p, f, s0, s1, z0, spring, d)
  const r = (s1 - s0) / 2, sc = s0 + r, seg = 8
  for (let k = 0; k < seg; k++) {
    const a = Math.PI - (k / seg) * Math.PI, b = Math.PI - ((k + 1) / seg) * Math.PI
    tri(p, on(f, sc, spring, d), on(f, sc + r * Math.cos(a), spring + r * Math.sin(a), d), on(f, sc + r * Math.cos(b), spring + r * Math.sin(b), d), f.n)
  }
}
/** A raised box on a face: front, top, bottom and two sides. */
function boss(p: Part, f: Face, s0: number, s1: number, z0: number, z1: number, d0: number, d1: number) {
  const A = (s: number, z: number, d: number) => on(f, s, z, d)
  const u: V3 = f.u, nu: V3 = [-u[0], -u[1], -u[2]]
  quad(p, A(s0, z0, d1), A(s1, z0, d1), A(s1, z1, d1), A(s0, z1, d1), f.n)
  quad(p, A(s0, z1, d0), A(s0, z1, d1), A(s1, z1, d1), A(s1, z1, d0), [0, 0, 1])
  quad(p, A(s0, z0, d0), A(s1, z0, d0), A(s1, z0, d1), A(s0, z0, d1), [0, 0, -1])
  quad(p, A(s1, z0, d0), A(s1, z1, d0), A(s1, z1, d1), A(s1, z0, d1), u)
  quad(p, A(s0, z0, d0), A(s0, z0, d1), A(s0, z1, d1), A(s0, z1, d0), nu)
}
/** Columns of window panels: one per column, spanning `per` floors at a time. */
function windowGrid(f: Face, cols: number[], w: number, z0: number, z1: number, storey: number, per = 2, gap = 1.1) {
  for (let z = z0; z + storey * per - gap <= z1 + 0.01; z += storey * per)
    for (const s of cols) panel(win, f, s - w / 2, s + w / 2, z + 0.6, z + storey * per - gap + 0.6)
}

// Deterministic jitter for the scar's broken edges.
let seed = 7
const rand = () => ((seed = (seed * 16807) % 2147483647) / 2147483647)

// ---------------------------------------------------------------------------
// Plan, in the model frame (see header). The tall block is T; its south-east
// corner carries the projecting corner tower with the corbelled cornice.

const T: Rect = { x0: -25, x1: 10, y0: -12.3, y1: 3.2 }
const TOP = 44.5          // main cornice of the tower
const CB: Rect = { x0: 2.6, x1: 10.7, y0: -13.1, y1: -3.6 }  // corner tower
const W1: Rect = { x0: -20.3, x1: 5.6, y0: -26.4, y1: -12.3 } // front wing
const W1b: Rect = { x0: 5.6, x1: 9.6, y0: -15, y1: -12.3 }
const B2: Rect = { x0: 9.6, x1: 18, y0: -15, y1: 2 }          // the 33 m block beside the tower
const W3: Rect = { x0: 18, x1: 32.6, y0: -15, y1: 5.6 }       // south-east wing
const W3b: Rect = { x0: 10, x1: 25.4, y0: 2, y1: 12.9 }
const CL: Rect = { x0: -25.6, x1: -20.6, y0: -13.1, y1: -7.5 }  // the narrower corner tower on the left
const BK: Rect = { x0: -27.5, x1: 13.7, y0: 3.2, y1: 24.8 }    // back wing

// --- The tower --------------------------------------------------------------
block(stucco, T, 0, TOP, { c: 0.5, bt: 0 , top: null })
cornice(stucco, { x0: T.x0, x1: T.x1, y0: T.y0, y1: T.y1 }, TOP - 1.2, TOP + 0.4, 0.5)
// The corner tower stands proud of the front and the side, and ends in a
// heavier corbelled cornice under its two spired turrets.
block(stucco, CB, 0, TOP - 1.6, { c: 0.3, bt: 0, top: null })
cornice(stucco, CB, TOP - 2.2, TOP + 0.6, 1.0)
block(stucco, CL, 0, TOP - 1.6, { c: 0.3, bt: 0, top: null })
cornice(stucco, CL, TOP - 2.2, TOP + 0.6, 0.9)

// Crown: a stucco attic set in from the cornice, under a tile hip roof, with
// the central pavilion rising through it.
const ATTIC: Rect = { x0: T.x0 + 1.2, x1: T.x1 - 1.2, y0: T.y0 + 1.2, y1: T.y1 - 1.2 }
block(stucco, ATTIC, TOP, 50, { c: 0.3, bt: 0.25, top: null })
hip(tile, { x0: ATTIC.x0 + 0.2, x1: ATTIC.x1 - 0.2, y0: ATTIC.y0 + 0.2, y1: ATTIC.y1 - 0.2 }, 50, 3.0, 0.5)
const PAV: Rect = { x0: -10.5, x1: -1.5, y0: -9.3, y1: 0.5 }
block(stucco, PAV, 49, 56.3, { c: 0.3, bt: 0.25, top: null })
hip(tile, PAV, 56.3, 3.4, 0.7)
pyramid(tile, (PAV.x0 + PAV.x1) / 2, (PAV.y0 + PAV.y1) / 2, 1.0, 59.4, 0.6)
spire(metal, (PAV.x0 + PAV.x1) / 2, (PAV.y0 + PAV.y1) / 2, 0.22, 59.8, 0.9, 6)
// A tiled gable-fronted dormer on the attic's front, left of the pavilion.
block(stucco, { x0: -19, x1: -14, y0: ATTIC.y0 - 0.2, y1: ATTIC.y0 + 3 }, TOP, 53, { c: 0.2, bt: 0.2, top: null })
hip(tile, { x0: -19, x1: -14, y0: ATTIC.y0 - 0.2, y1: ATTIC.y0 + 3 }, 53, 1.6, 0.4)

// Turrets with tile caps and slim spires: the hotel's skyline.
const turret = (cx: number, cy: number, half: number, z0: number, z1: number) => {
  block(stucco, { x0: cx - half, x1: cx + half, y0: cy - half, y1: cy + half }, z0, z1, { c: 0.2, bt: 0.2, top: null })
  pyramid(tile, cx, cy, half + 0.35, z1, 2.0)
  spire(metal, cx, cy, 0.32, z1 + 1.5, Math.min(4.2, 60.2 - z1 - 1.5))
}
turret((CL.x0 + CL.x1) / 2, CL.y0 + 2.2, 1.9, TOP + 0.6, 55.6)
turret(-9.5 - 4.2, T.y0 + 1.6, 1.4, TOP, 54)
turret(CB.x0 + 1.9, CB.y0 + 1.9, 1.8, TOP + 0.6, 56.1)
turret(CB.x1 - 1.9, CB.y0 + 1.9, 1.8, TOP + 0.6, 56.1)
turret(T.x0 + 1.9, T.y1 - 1.9, 1.9, TOP, 55)
turret(T.x1 - 1.9, T.y1 - 1.9, 1.9, TOP, 55)

// --- Lower masses -------------------------------------------------------------
block(stucco, W1, 0, 13, { c: 0.3, bt: 0.25, top: null }); hip(tile, W1, 13, 3.4, 0.6)
block(stucco, W1b, 0, 13, { c: 0.2, bt: 0.2 })
block(stucco, B2, 0, 33, { c: 0.3, bt: 0, top: null }); cornice(trim, B2, 32, 33.6, 0.45, stucco)
block(stucco, W3, 0, 24, { c: 0.3, bt: 0.25, top: null }); hip(tile, W3, 24, 4, 0.6)
block(stucco, W3b, 0, 24, { c: 0.3, bt: 0.25, top: null }); hip(tile, W3b, 24, 3.4, 0.6)
block(stucco, BK, 0, 27, { c: 0.4, bt: 0.25, top: null }); hip(tile, BK, 27, 4.2, 0.6)

// --- The front: the scar, the sign, the windows -------------------------------
const F = faces(T).front
// The lightning strike: a wide band of the front torn away from the attic down
// to the arcade, its edges ragged. Drawn as a charcoal slate panel laid on the
// wall in strips, each edge jittered so the outline reads as broken.
{
  const zs: number[] = []
  for (let z = 19.5; z < TOP - 0.6; z += 1.6) zs.push(z)
  zs.push(TOP - 1.2)
  const left = zs.map((z, i) => 5.5 + (rand() - 0.5) * 2.4 + (i % 3 === 0 ? -1.2 : 0))
  const right = zs.map((z, i) => 25.6 + (rand() - 0.5) * 2.2 + (z < 26 ? -2.5 : 0))
  for (let i = 0; i < zs.length - 1; i++)
    quad(scorch, on(F, left[i], zs[i], 0.05), on(F, right[i], zs[i], 0.05), on(F, right[i + 1], zs[i + 1], 0.05), on(F, left[i + 1], zs[i + 1], 0.05), F.n)
  // Exposed floors inside the scar: slate window bands with the broken
  // balconies' pale slab edges between them.
  for (let z = 21.2; z < TOP - 3; z += 3.4) {
    panel(win, F, 9.5, 15.5, z, z + 2.0, 0.08)
    panel(win, F, 18, 22.5, z + 0.3, z + 2.1, 0.08)
    boss(trim, F, 9, 16, z - 0.35, z, 0.05, 0.9)
  }
  // The scar runs up the attic too, to the foot of the turrets.
  const A = faces(ATTIC).front
  panel(scorch, A, 4.8, 13.5, TOP, 49.4, 0.05)
}

// The sign: a dark frame standing off the scar, carrying block capitals.
{
  const s0 = 0.4, s1 = 25.2, z0 = 31.4, z1 = 37.6
  boss(scorch, F, s0, s1, z0, z1, 0.05, 1.2)
  const FS: Face = { o: on(F, 0, 0, 1.2), u: F.u, n: F.n }
  // A 3 × 5 stroke font, cells in letter units.
  const GLYPH: Record<string, number[][]> = {
    H: [[0, 0, 1, 5], [2, 0, 3, 5], [1, 2, 2, 3]],
    O: [[0, 0, 1, 5], [2, 0, 3, 5], [1, 0, 2, 1], [1, 4, 2, 5]],
    L: [[0, 0, 1, 5], [1, 0, 3, 1]],
    Y: [[0, 3, 1, 5], [2, 3, 3, 5], [0, 2, 3, 3], [1, 0, 2, 2]],
    W: [[0, 0, 1, 5], [2, 0, 3, 5], [1, 0, 2, 2.4]],
    D: [[0, 0, 1, 5], [1, 0, 2, 1], [1, 4, 2, 5], [2, 0.6, 3, 4.4]],
    T: [[0, 4, 3, 5], [1, 0, 2, 4]],
    E: [[0, 0, 1, 5], [1, 0, 3, 1], [1, 2, 2.6, 3], [1, 4, 3, 5]],
    R: [[0, 0, 1, 5], [1, 4, 2, 5], [2, 2.6, 3, 4.6], [1, 2, 2.2, 3], [2, 0, 3, 2]],
  }
  const write = (text: string, sx: number, z: number, h: number, pitch: number) => {
    const k = h / 5
    let s = sx
    for (const ch of text) {
      for (const [a, b, c, d] of GLYPH[ch] ?? []) boss(trim, FS, s + a * k * 0.8, s + c * k * 0.8, z + b * k, z + d * k, 0, 0.25)
      s += pitch
    }
  }
  write('HOLLYWOOD', s0 + 0.6, 32.6, 4.2, 1.66)
  write('TOWER', s0 + 0.6 + 9 * 1.66 + 0.5, 32.6, 4.2, 1.66)
  // "The" and the script "Hotel", as plain bars at their places.
  boss(trim, FS, s0 + 0.6, s0 + 3.4, 37.9, 38.8, -1.1, 0.2)
  boss(scorch, F, 14.5, 23.5, 29.6, 31.6, 0.05, 1.0)
  boss(trim, { o: on(F, 0, 0, 1.0), u: F.u, n: F.n }, 15.3, 22.7, 30.1, 31.1, 0, 0.2)
}

// The arcade under the scar: round-headed openings on the left of the front.
for (let i = 0; i < 4; i++) arched(win, F, 3.2 + i * 2.6, 4.9 + i * 2.6, 19.8, 22.4)
// Tall paired windows on the left of the front, under the scar.
for (const s of [1.8, 9, 14]) arched(win, F, s, s + 1.2, 6, 9.5)

// The corner tower: paired arched windows stacked up its front and side.
{
  const C = faces(CB)
  for (let z = 21; z < 43; z += 3.6) { panel(win, C.front, 2.2, 3.4, z, z + 2.1); panel(win, C.front, 4.7, 5.9, z, z + 2.1) }
  for (let z = 21; z < 43; z += 3.6) { panel(win, C.east, 2.2, 3.4, z, z + 2.1); panel(win, C.east, 6, 7.2, z, z + 2.1) }
  // Balconies with tile hoods on its lower front, as at the real hotel.
  for (const z of [14.5, 18.5]) {
    boss(trim, C.front, 1.4, 6.8, z - 0.5, z, 0.05, 1.3)
    boss(tile, C.front, 1.2, 7.0, z + 2.8, z + 3.2, 0.05, 1.5)
  }
}
// Attic and crown windows: the arched window in the pavilion front and the
// pairs either side, as on the photos of every face.
{
  const P = faces(PAV).front
  arched(win, P, 2.6, 6.4, 52, 54.3)
  const A = faces(ATTIC)
  for (const s of [17.5, 19.2]) arched(win, A.front, s, s + 1.2, TOP + 1.2, TOP + 3)
  for (const s of [9, 10.7, 21, 22.7]) arched(win, A.back, s, s + 1.2, TOP + 1.2, TOP + 3)
  arched(win, A.back, 14.4, 17.6, TOP + 1.2, TOP + 3.4)
  for (const s of [5, 6.7, 12.5, 14.2]) arched(win, A.east, s, s + 1.2, TOP + 1.2, TOP + 3)
  for (const s of [5, 6.7, 12.5, 14.2]) arched(win, A.west, s, s + 1.2, TOP + 1.2, TOP + 3)
}
// Side and back faces of the tower: hotel-room windows in columns, two
// storeys to a panel, above the wings around it.
{
  const Tf = faces(T)
  windowGrid(Tf.east, [12, 15, 18.5], 1.3, 33.6, TOP - 2, 3.4)
  windowGrid(Tf.west, [3.5, 7, 15, 18.5], 1.3, 3.5, TOP - 2, 3.4)
  windowGrid(Tf.back, [4, 8, 14, 21, 27, 31], 1.3, 33.6, TOP - 2, 3.4)
}
// The wings: rows of hotel windows, two storeys to a panel.
{
  const w3 = faces(W3)
  windowGrid(w3.front, [2, 5, 8.5, 12], 1.4, 3.5, 22.5, 3.4)
  windowGrid(w3.east, [3, 7, 11, 15, 18.5], 1.4, 3.5, 22.5, 3.4)
  const b2 = faces(B2)
  windowGrid(b2.front, [2.2, 6.2], 1.5, 3.5, 30.5, 3.4)
  for (const z of [17, 23.5]) {
    boss(trim, b2.front, 1, 7.4, z - 0.5, z, 0.05, 1.3)
    boss(tile, b2.front, 0.8, 7.6, z + 2.8, z + 3.2, 0.05, 1.5)
  }
  const w1 = faces(W1)
  for (const s of [2.5, 6.5, 10.5, 15, 19, 23]) arched(win, w1.front, s - 0.7, s + 0.7, 3, 7.5)
  for (const s of [3, 7, 11]) arched(win, w1.west, s - 0.7, s + 0.7, 3, 7.5)
  const bk = faces(BK)
  windowGrid(bk.back, [3, 7, 11, 15, 19, 23, 27, 31, 35, 38.5], 1.3, 3.5, 25.5, 3.4)
  windowGrid(bk.west, [3, 7, 11, 15, 18.5], 1.3, 3.5, 25.5, 3.4)
  windowGrid(bk.east, [3, 7, 11, 15, 18.5], 1.3, 3.5, 25.5, 3.4)
}
// The main doors under the front wing: a lit entrance.
const entrance = new Part()
{
  const w1 = faces(W1)
  arched(entrance, w1.front, 11.4, 14.4, 0, 3.4, 0.05)
}

// ---------------------------------------------------------------------------

// The hotel is pink stucco under red-orange tile; the scar is its one dark
// feature, kept at charcoal. Stucco is the photo's salmon pink pulled to the
// palette's lightness so it reads as "the pink one" beside the pale park.
const parts = [
  { part: stucco, material: finish('hotel-stucco', 0xe3a693) },
  { part: tile, material: PALETTE.terracotta },
  { part: scorch, material: finish('scorch', 0x787e89) },
  { part: trim, material: PALETTE.trim },
  { part: win, material: PALETTE.window },
  { part: entrance, material: PALETTE.entrance },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Twilight Zone Tower of Terror', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 41, elevation: 0, height: 60.7,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-tower-of-terror.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
