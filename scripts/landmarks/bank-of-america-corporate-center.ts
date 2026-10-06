/**
 * Bank of America Corporate Center, Charlotte — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/bank-of-america-corporate-center.ts
 *
 * Map frame here: x = across the tower, y = along bearing 48.8°, z = metres up.
 * Anchor -80.8422257, 35.2273121 (the OSM outline's centroid); bearing 48.8°,
 * the outline's edges, which follow Uptown's rotated grid.
 *
 * The shape is a rounded bullet. A podium at the OSM outline (0–42 m: a
 * low darker rose base, tower granite above, a cornice at the top) carries
 * a near-straight shaft. Every face has a broad central slab standing proud
 * of recessed, glazed corners. Above 155 m the faces curve in through six
 * shallow steps, the corners a step ahead of the slabs, to a blunt top at
 * 218 m; each step's lip carries a comb of short silver fins. On the top,
 * up to about 255 m, a broad silver crown: three stepped pale cores ringed
 * by fins in shallow steps, ragged along the top, flat-topped in the middle.
 *
 * The granite is a lattice of recessed window panels: broad glazed bays
 * between narrow piers, crossed by a spandrel every few floors, so the skin
 * reads as punched windows rather than stripes.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[] }
const granite = new Part(), base = new Part(), glass = new Part()
const ledge = new Part(), core = new Part(), silver = new Part()
const BEVEL = .45
const RECESS = .55
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const up: V3 = [0, 0, 1]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na?: V3, nb?: V3, nc = nb, nd = na) {
  if (!na) { p.quad(a, b, c, d); return }
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb!, nc!])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc!, nd!])
}

/**
 * One tier's plan, four-fold symmetric: a central slab (|t| < c) at
 * half-width `slab`, flanks at `flank`, and a square re-entrant notch `n`
 * at each corner, which is glazed.
 */
function plan(slab: number, flank: number, c: number, n: number): XY[] {
  const a = flank
  const face: XY[] = [[a, -(a - n)]]
  if (slab - flank > .05) face.push([a, -c], [slab, -c], [slab, c], [a, c])
  face.push([a, a - n], [a - n, a - n])
  const ring: XY[] = []
  for (let k = 0; k < 4; k++) for (const [x, y] of face) {
    const r = (k * Math.PI) / 2, cs = Math.round(Math.cos(r)), sn = Math.round(Math.sin(r))
    ring.push([x * cs - y * sn, x * sn + y * cs])
  }
  return ring
}

/** Round the exposed convex corners of a plan, with analytic smooth normals. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    const turn = u[0] * v[1] - u[1] * v[0]
    const r = Math.min(radius, l0 * .3, l1 * .3)
    if (turn < -1e-5) { points.push(b); normals.push(unit([u[1] + v[1], -u[0] - v[0], 0])); continue }
    if (Math.abs(turn) < 1e-5) { points.push(b); normals.push([u[1], -u[0], 0]); continue }
    points.push([b[0] - u[0] * r, b[1] - u[1] * r], [b[0] + v[0] * r, b[1] + v[1] * r])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals }
}

/** Offset a ring inward; convex and concave corners follow the bisector. */
function inset(ring: XY[], distance: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const d = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * distance / d, b[1] - (u[1] + v[1]) * distance / d]
  })
}

/** A fan cap from the centre; every plan here is star-shaped about it. */
function cap(p: Part, ring: XY[], z: number) {
  const centre: V3 = [0, 0, z]
  const pts = at(ring, z)
  for (let i = 0; i < ring.length; i++) p.tri(centre, pts[i], pts[(i + 1) % ring.length])
}

/** A recessed window panel with bevelled stone reveals. */
function opening(stone: Part, v: (s: number, z: number, d?: number) => V3, normal: V3, ux: number, uy: number, s0: number, s1: number, z0: number, z1: number) {
  const b = Math.min(.35, (s1 - s0) * .15, (z1 - z0) * .12)
  const outer: XY[] = [[s0, z0], [s1, z0], [s1, z1], [s0, z1]]
  const inner: XY[] = [[s0 + b, z0 + b], [s1 - b, z0 + b], [s1 - b, z1 - b], [s0 + b, z1 - b]]
  const o = outer.map(([s, z]) => v(s, z)), i = inner.map(([s, z]) => v(s, z, -RECESS))
  for (let k = 0; k < 4; k++) {
    const j = (k + 1) % 4
    const ds = outer[j][0] - outer[k][0], dz = outer[j][1] - outer[k][1], e = Math.hypot(ds, dz)
    const inward: V3 = [-ux * dz / e, -uy * dz / e, ds / e]
    const n = unit(add(normal, inward))
    quad(stone, o[k], o[j], i[j], i[k], normal, normal, n, n)
  }
  glass.quad(i[0], i[1], i[2], i[3])
  return b
}

/**
 * One wall of a tier. Long walls get a lattice of window panels: `cols`
 * broad bays between narrow piers, and a spandrel every `rowHeight`. The
 * returns of the corner notches are glazed full height; the short steps
 * between slab and flank stay plain stone.
 */
function wall(rim: Rim, i: number, bottom: number, top: number, stone: Part, bay: number, rowHeight: number, solid = .4) {
  const a = rim.points[i], b = rim.points[(i + 1) % rim.points.length]
  const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
  const normal: V3 = [uy, -ux, 0]
  const v = (s: number, z: number, d = 0): V3 => [a[0] + ux * s + uy * d, a[1] + uy * s - ux * d, z]
  const flat = (p: Part, s0: number, s1: number, z0: number, z1: number) => p.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
  const diagonal = (p: XY) => Math.abs(Math.abs(p[0]) - Math.abs(p[1])) < .01
  if (length > 1 && (diagonal(a) || diagonal(b))) { flat(glass, 0, length, bottom, top); return }
  const cols = Math.round(length / bay), rows = Math.max(1, Math.round((top - bottom - 1.2) / rowHeight))
  if (length < 3 || cols < 1 || top - bottom < 4) {
    quad(stone, v(0, bottom), v(length, bottom), v(length, top), v(0, top), rim.normals[i], rim.normals[(i + 1) % rim.points.length])
    return
  }
  const pitch = length / cols, pier = pitch * solid
  const lo = bottom + .6, hi = top - .6, rh = (hi - lo) / rows, spandrel = rh * solid
  flat(stone, 0, length, bottom, lo)
  flat(stone, 0, length, hi, top)
  let last = 0
  for (let c = 0; c < cols; c++) {
    const s0 = c * pitch + pier / 2, s1 = (c + 1) * pitch - pier / 2
    flat(stone, last, s0, lo, hi)
    // One recessed bay per column, crossed by stone spandrels set just
    // behind the pier face: reads as stacked window panels for a third of
    // the triangles of separate openings.
    const b = opening(stone, v, normal, ux, uy, s0, s1, lo, hi)
    for (let r = 1; r < rows; r++) {
      const z = lo + r * rh, z0 = z - spandrel / 2, z1 = z + spandrel / 2, d = -.2
      stone.quad(v(s0 + b, z0, d), v(s1 - b, z0, d), v(s1 - b, z1, d), v(s0 + b, z1, d))
      stone.quad(v(s0 + b, z1, d), v(s1 - b, z1, d), v(s1 - b, z1, -RECESS), v(s0 + b, z1, -RECESS))
    }
    last = s1
  }
  flat(stone, last, length, lo, hi)
}

/** A rounded coping round a tier's top, and the ledge inside it. */
function coping(rim: Rim, z: number, surface: Part) {
  const inner = inset(rim.points, .45)
  const a = at(rim.points, z - .45), b = at(inner, z)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = rim.normals[i], nj = rim.normals[j]
    quad(granite, a[i], a[j], b[j], b[i], ni, nj, up, up)
  }
  cap(surface, inner, z)
}

function tier(ring: XY[], bottom: number, top: number, stone: Part, bay: number, rowHeight: number, surface = ledge, solid = .4) {
  const rim = soften(ring)
  for (let i = 0; i < rim.points.length; i++) wall(rim, i, bottom, top - .45, stone, bay, rowHeight, solid)
  coping(rim, top, surface)
}

// ---- Podium: the OSM outline, 48.4 × 49 m --------------------------------

// The walls stand 0.6 m inside the outline, so a plain granite cornice band
// can run round the top at the outline itself. Only the bottom 14 m are the
// darker rose granite; above it the podium wears the tower's own granite
// and windows, so the tower reads as coming down to a low base.
const podium = (inner: number): XY[] => [[-24.2 + inner, -24.5 + inner], [24.2 - inner, -24.5 + inner], [24.2 - inner, 24.5 - inner], [-24.2 + inner, 24.5 - inner]]
{
  const rim = soften(podium(.6))
  for (let i = 0; i < rim.points.length; i++) {
    wall(rim, i, 0, 14, base, 8, 14, .45)
    wall(rim, i, 14, 37.5, granite, 5.4, 12)
  }
  const outer = soften(podium(0))
  for (let i = 0; i < outer.points.length; i++) wall(outer, i, 37.5, 41.55, granite, 100, 100)
  // The cornice's underside, between the wall and the band's face.
  const o = at(outer.points, 37.5), w = at(rim.points, 37.5)
  for (let i = 0; i < o.length; i++) {
    const j = (i + 1) % o.length
    granite.quad(o[j], o[i], w[i], w[j])
  }
  coping(outer, 42, ledge)
}

// ---- Tower ----------------------------------------------------------------

/**
 * The slab half-width up the tower: straight to 155 m, then curving in ever
 * faster along a parabola to a blunt top 13 m across at 218 m, where the
 * silver crown takes over.
 */
const TOP = 218
const slabAt = (z: number) => {
  const u = Math.min(1, Math.max(0, (z - 155) / (TOP - 155)))
  return 21 - 8 * u * u
}

// Tier boundaries: one tall shaft, then shallow, closely spaced steps.
const levels = [42, 155, 168, 180, 191, 201, 210, TOP]
const rings: XY[][] = []
for (let k = 0; k < levels.length - 1; k++) {
  const z1 = levels[k + 1]
  const slab = k === 0 ? 21 : slabAt(z1)
  // The corners step back one tier ahead of the slabs: the flank of this
  // tier is set behind the slab of the next.
  const next = slabAt(levels[Math.min(k + 2, levels.length - 1)])
  const flank = Math.min(slab - 1.4, k === 0 ? 19.4 : next - .8)
  const c = slab * .46, n = Math.max(1.3, Math.min(3, flank * .14))
  rings.push(plan(slab, flank, c, n))
}
rings.forEach((ring, k) => tier(ring, levels[k], levels[k + 1], granite, k === 0 ? 5.4 : 10, k === 0 ? 15 : 11))

// ---- Silver: fins on the setback lips and the crown -----------------------

/** Points every `spacing` metres round a closed path, evenly spread. */
function along(path: XY[], spacing: number): XY[] {
  const lengths = path.map((p, i) => { const q = path[(i + 1) % path.length]; return Math.hypot(q[0] - p[0], q[1] - p[1]) })
  const total = lengths.reduce((s, l) => s + l, 0), count = Math.max(4, Math.round(total / spacing / 4) * 4)
  const out: XY[] = []
  for (let k = 0; k < count; k++) {
    let d = (k + .5) * total / count
    for (let i = 0; i < path.length; i++) {
      if (d > lengths[i]) { d -= lengths[i]; continue }
      const p = path[i], q = path[(i + 1) % path.length]
      out.push([p[0] + (q[0] - p[0]) * d / lengths[i], p[1] + (q[1] - p[1]) * d / lengths[i]])
      break
    }
  }
  return out
}
function inside([x, y]: XY, ring: XY[]) {
  let hit = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i], [xj, yj] = ring[j]
    if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) hit = !hit
  }
  return hit
}

/**
 * A square silver fin. Only its sides: a 1 m top is invisible from the
 * map, and leaving it off pays for the setback combs.
 */
function fin(x: number, y: number, z0: number, z1: number, w: number) {
  const r = w / 2
  const sq = (z: number): V3[] => [[x - r, y - r, z], [x + r, y - r, z], [x + r, y + r, z], [x - r, y + r, z]]
  silver.loft([sq(z0), sq(z1)])
}

/**
 * A flat fin blade facing out from the face it stands on, both sides
 * drawn: a comb reads the same, at a third of a solid fin's triangles.
 */
function blade([x, y]: XY, z0: number, z1: number, w: number) {
  const r = w / 2, alongY = Math.abs(x) > Math.abs(y)
  const a: XY = alongY ? [x, y - r] : [x - r, y], b: XY = alongY ? [x, y + r] : [x + r, y]
  const q: V3[] = [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]]
  silver.quad(q[0], q[1], q[2], q[3])
  silver.quad(q[1], q[0], q[3], q[2])
}

// Setback lips: a comb of short fins on every step from the shaft's top
// up, set inside the lip and only where the terrace is free of the tier
// above, so none stands in a wall or past an edge.
for (let k = 0; k < rings.length - 1; k++) {
  const above = rings[k + 1]
  for (const p of along(inset(rings[k], .75), 2.6)) {
    const corners: XY[] = [[p[0] - .5, p[1] - .5], [p[0] + .5, p[1] - .5], [p[0] + .5, p[1] + .5], [p[0] - .5, p[1] + .5]]
    if (corners.some(q => inside(q, above))) continue
    blade(p, levels[k + 1], levels[k + 1] + 5, 1)
  }
}

/** A plain pale core drum, so the crown's fins read as one silver mass. */
function drum(ring: XY[], z0: number, z1: number) {
  const rim = soften(ring, .3)
  const a = at(rim.points, z0), b = at(rim.points, z1)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length
    quad(core, a[i], a[j], b[j], b[i], rim.normals[i], rim.normals[j])
  }
  cap(core, rim.points, z1)
}
const octagon = (h: number, cut: number): XY[] =>
  [[h - cut, -h], [h, -(h - cut)], [h, h - cut], [h - cut, h], [-(h - cut), h], [-h, h - cut], [-h, -(h - cut)], [-(h - cut), -h]]

// The crown, 218–256 m: three stepped pale cores, each ringed by fins that
// rise past its top in shallow steps, with a wide flat-topped cluster in
// the middle, so it reads as a broad, ragged silver crown rather than a
// cone. The outer ring nearly fills the top tier; each ring stands on the
// step below it, inside its edge.
const cores: { ring: XY[]; z0: number; z1: number }[] = [
  { ring: inset(rings.at(-1)!, 1.8), z0: TOP, z1: 232 },
  { ring: octagon(9.4, 2.7), z0: 232, z1: 241 },
  { ring: octagon(7.4, 2.2), z0: 241, z1: 248 },
]
// A fixed pseudo-random sequence (Park–Miller), so the ragged top edge is
// the same on every run.
let seed = 48271
const jitter = () => { seed = (seed * 16807) % 2147483647; return (seed / 2147483647 * 2 - 1) * 1.5 }
const stands = [rings.at(-1)!, ...cores.map(c => c.ring)]
const finTops = [236, 244, 250, 254]
stands.forEach((ring, k) => {
  const z0 = k === 0 ? TOP : cores[k - 1].z1
  for (const [x, y] of along(inset(ring, .8), 2.8)) fin(x, y, z0, finTops[k] + jitter(), 1)
})
for (const c of cores) drum(c.ring, c.z0, c.z1)
for (const x of [-3, 0, 3]) for (const y of [-3, 0, 3]) fin(x, y, 248, 254 + jitter(), 1)

// sRGB colours from daylight photos: p4 and its crop (q1) for the granite
// and the crown, sampled by eye from the sunlit face.
const parts = [
  { part: granite, material: { name: 'rose-grey-granite', color: 0xc6b5ab } },
  { part: base, material: { name: 'rose-granite-base', color: 0xb19488 } },
  { part: glass, material: { name: 'windows', color: 0x66768a, roughness: .6 } },
  { part: ledge, material: { name: 'stone-ledges', color: 0xab9d94 } },
  { part: core, material: { name: 'crown-core', color: 0xc9cdd0, roughness: .6 } },
  { part: silver, material: { name: 'silver-fins', color: 0xe8ebec, roughness: .5 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Bank of America Corporate Center', parts, {
  license: 'CC0-1.0', bearing: 48.8, elevation: 0, anchor: [35.2273121, -80.8422257], height: 255.5,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  footprint: { x: [-24.2, 24.2], y: [-24.5, 24.5] },
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/bank-of-america-corporate-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
