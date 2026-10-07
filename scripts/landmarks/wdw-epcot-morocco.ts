/**
 * EPCOT Morocco pavilion: the Koutoubia minaret — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-morocco.ts
 *
 * Map frame: x and y along the pavilion's grid, z up, metres. Placed at
 * bearing 29°, the axis of the fortress buildings around it (the long edges
 * of OSM way/295448855 run 29°/119°). The anchor is the minaret's centre.
 *
 * What it is: Disney's replica of the Koutoubia minaret of Marrakesh, a plain
 * square tower of rose-tan sandstone, each face carrying tiers of recessed
 * arched windows, a band of green zellige tile under a crown of stepped
 * merlons, and a small lantern of the same design topped by a ribbed brass
 * dome and a finial of three balls. At its foot, towards the plaza and the
 * fountain, stands a white arcaded pavilion under a green-tiled pyramid roof,
 * which every plaza photo shows in front of the minaret's base.
 *
 * Evidence
 * - OSM: the minaret is not mapped. It stands in an unmapped gap east of the
 *   pavilion's buildings (way/295448855, /858, /859), so the model replaces
 *   nothing and simply stands there.
 * - Position (measured, public-domain USGS NAIP orthophoto): an 8.8 m square
 *   with a dark core (the tower top round its lantern) about 28 m east and
 *   4 m north of node -81.5520, 28.3680; the green pyramid roof 17 m north of
 *   it. A tower's top is displaced in an orthophoto, so the base may be off by
 *   a few metres. Checked against the plaza photos: from the plaza the
 *   minaret rises just behind the green roof, the Tangierine Café's fortress
 *   tower to the right, as computed from these positions.
 * - Published: no height is published for the replica.
 * - Estimated from the lagoon photo (Morocco Pavilion (42550347444).jpg),
 *   scaled from the Spice Road Table in front of it: the shaft is 3.6 widths
 *   tall to the merlon tops there, so with the orthophoto's 8.8 m top (merlons
 *   included) the shaft is drawn 7.6 m wide and 27 m to the merlon tops, the
 *   lantern 3.8 m wide, the finial at about 33 m. Stage proportions (tile band, lantern, dome) are from the
 *   close photos (Morocco Pavilion (cropped).jpg, (32272553154).jpg).
 * - Simplified: the real faces differ (the windows step with the internal
 *   ramp); every face here carries the same three tiers. The pavilion is a
 *   9 m square from the orthophoto, its height estimated from photo 32272556694.
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


const sand = new Part(), win = new Part(), tile = new Part(), brass = new Part(), white = new Part()

// ---------------------------------------------------------------------------
// The minaret.

const H = 7.6 / 2          // shaft half-width
const zBand0 = 24.3, zBand1 = 25.7
const L = 1.9              // lantern half-width
const zL1 = 28.9, zLBand = 29.6, zDome = 31.6

block(sand, sq(0, 0, H, 0.15), 0, zBand0, { top: null })
block(tile, sq(0, 0, H + 0.04, 0.15), zBand0, zBand1, { top: null })
block(sand, sq(0, 0, H + 0.1, 0.18), zBand1, zBand1 + 0.35, { bb: 0.1, top: sand })

/** Stepped merlons along a square's edge: blocks with a pyramid cap. */
function merlons(half: number, z0: number, h: number, w: number, count: number) {
  for (const f of Object.values(faces(sq(0, 0, half)))) {
    const step = f.L / count
    for (let i = 0; i < count; i++) {
      const s = (i + 0.5) * step
      const [cx, cy] = at(f, s, 0, -w / 2)
      block(sand, sq(cx, cy, w / 2, 0.03), z0, z0 + h * 0.65, { top: null })
      // The stepped top drawn as a pyramid, the shape it makes from afar.
      const b = w / 2, zt = z0 + h * 0.65
      const c: V3[] = [[cx - b, cy - b, zt], [cx + b, cy - b, zt], [cx + b, cy + b, zt], [cx - b, cy + b, zt]]
      const apex: V3 = [cx, cy, z0 + h]
      for (let k = 0; k < 4; k++) {
        const a = c[k], d = c[(k + 1) % 4]
        const n = unit(cross([d[0] - a[0], d[1] - a[1], 0], [apex[0] - a[0], apex[1] - a[1], apex[2] - a[2]]))
        tri(sand, a, d, apex, [n, n, n])
      }
    }
  }
}
merlons(H - 0.05, zBand1 + 0.35, 1.0, 0.55, 6)

// Three tiers of recessed arched windows per face: a broad blind arcade of
// four under the tile band, a pair below it, and a single big arched panel
// lower down, then a tall plain base.
for (const f of Object.values(faces(sq(0, 0, H)))) {
  const m = f.L / 2
  relief(sand, f, m - 2.9, m + 2.9, 18.6, 23.2, 0.03)
  for (let i = 0; i < 4; i++) {
    const s0 = m - 2.7 + i * 1.35 + 0.18, s1 = s0 + 1.0
    panel(win, f, roundArch(s0, s1, 19.1, 22.1, 6), 0.07)
  }
  for (const s of [m - 0.85, m + 0.85]) panel(win, f, roundArch(s - 0.5, s + 0.5, 14.6, 16.8, 6), 0.04)
  panel(win, f, roundArch(m - 1.5, m + 1.5, 7.6, 10.4, 8), 0.04)
  panel(win, f, roundArch(m - 0.42, m + 0.42, 8.0, 9.2, 6), 0.08)
}

// The lantern: same design in small, standing on the platform.
block(sand, sq(0, 0, L, 0.08), zBand1 + 0.35, zL1, { top: null })
block(tile, sq(0, 0, L + 0.03, 0.08), zL1, zLBand, { top: null })
block(sand, sq(0, 0, L + 0.08, 0.1), zLBand, zLBand + 0.2, { bb: 0.06, top: sand })
merlons(L - 0.04, zLBand + 0.2, 0.55, 0.32, 4)
for (const f of Object.values(faces(sq(0, 0, L)))) {
  const m = f.L / 2
  for (const s of [m - 0.6, m + 0.6]) panel(win, f, roundArch(s - 0.36, s + 0.36, 26.9, 28.0, 6), 0.04)
}
// Ribbed brass dome and the three-ball finial.
{
  const seg = 12, rings = 5, R = L - 0.25, z0 = zLBand + 0.2
  for (let k = 0; k < seg; k++)
    for (let j = 0; j < rings; j++) {
      const p = (kk: number, jj: number): V3 => {
        const a = (kk / seg) * 2 * Math.PI, t = (jj / rings) * (Math.PI / 2)
        // Ribs: the radius swells at each gore's middle.
        const rib = 1 + 0.06 * Math.cos(kk * Math.PI)
        return [R * rib * Math.cos(t) * Math.cos(a), R * rib * Math.cos(t) * Math.sin(a), z0 + (zDome - z0) * Math.sin(t)]
      }
      const n = (kk: number, jj: number): V3 => {
        const a = (kk / seg) * 2 * Math.PI, t = (jj / rings) * (Math.PI / 2)
        return unit([Math.cos(t) * Math.cos(a), Math.cos(t) * Math.sin(a), Math.sin(t) * (R / (zDome - z0))])
      }
      quad(brass, p(k, j), p(k + 1, j), p(k + 1, j + 1), p(k, j + 1), [n(k, j), n(k + 1, j), n(k + 1, j + 1), n(k, j + 1)])
    }
  cylinder(brass, 0, 0, 0.08, 0.06, zDome - 0.05, zDome + 1.6, 6, false)
  for (const [z, r] of [[zDome + 0.35, 0.22], [zDome + 0.8, 0.2], [zDome + 1.2, 0.16]]) {
    cylinder(brass, 0, 0, 0.02, r, z - r, z, 8, false)
    cylinder(brass, 0, 0, r, 0.02, z, z + r, 8, false)
  }
}

// ---------------------------------------------------------------------------
// The white pavilion with the green-tiled pyramid roof, towards the plaza.

{
  const cx = -1.6, cy = 17.4, h = 4.5, z1 = 6.0
  const r = sq(cx, cy, h, 0.1)
  block(white, r, 0, z1, { top: null })
  for (const f of Object.values(faces(r))) {
    const bays = 3, span = f.L / bays
    for (let i = 0; i < bays; i++) {
      const s0 = i * span + 0.55, s1 = (i + 1) * span - 0.55
      panel(win, f, roundArch(s0, s1, 0, 3.4 - (s1 - s0) / 2 + 0.6, 6), 0.04)
    }
  }
  // Overhanging eaves, then the pyramid.
  block(tile, sq(cx, cy, h + 0.5, 0.1), z1, z1 + 0.25, { top: null, bottom: true })
  hip(tile, { x0: cx - h - 0.5, x1: cx + h + 0.5, y0: cy - h - 0.5, y1: cy + h + 0.5 }, z1 + 0.25, z1 + 4.2, true)
}

// ---------------------------------------------------------------------------
// Materials (STYLE.md): the rose-tan sandstone and the brass dome are
// identity finishes, muted; the zellige band and roof tiles are the palette's
// copper green; the pavilion's white render is `trim`.

const parts = [
  { part: sand, material: finish('koutoubia-sandstone', 0xd49b80) },
  { part: win, material: PALETTE.window },
  { part: tile, material: PALETTE.copper },
  { part: brass, material: finish('minaret-brass', 0xc9a463, 0.5) },
  { part: white, material: PALETTE.trim },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT Morocco pavilion minaret', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 29, elevation: 0, height: zDome + 1.6,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-morocco.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
