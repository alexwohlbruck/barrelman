/**
 * Bank of America Corporate Center, Charlotte — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/bank-of-america-corporate-center.ts
 *
 * Map frame here: x = across the tower, y = along bearing 48.8°, z = metres up.
 * Anchor -80.8422257, 35.2273121 (the OSM outline's centroid); bearing 48.8°,
 * the outline's edges, which follow Uptown's rotated grid. The stages follow
 * the OSM building:parts (podium to 42 m, shaft to 161 m, then setbacks at
 * 177, 208, 224 and 235 m). Each stage is a square with a broad central slab
 * proud of its flanks and glazed re-entrant corners, which is what gives the
 * tower its stepped, rounded taper. Above 235 m the aluminium crown: a pale
 * stepped lantern ringed by tiers of tall "organ pipe" spires, highest in the
 * middle, all inside the top stage's footprint.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[] }
const granite = new Part(), plinth = new Part(), glass = new Part()
const terraceRoof = new Part(), crown = new Part(), spire = new Part(), roof = new Part()
const BEVEL = .5
const RECESS = .6
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const up: V3 = [0, 0, 1]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

/**
 * One stage's plan: a square of half-width `h` whose faces carry a central
 * slab (|t| < c) standing `step` proud of the flanks, with a square
 * re-entrant notch `n` at each corner. Built for one face and turned
 * through the other three, so the tower is exactly four-fold symmetric.
 */
function stagePlan(h: number, step: number, c: number, n: number): XY[] {
  const a = h - step
  const face: XY[] = [[a, -(a - n)]]
  if (step > 0) face.push([a, -c], [h, -c], [h, c], [a, c])
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

/** A cap that is safe on a star-shaped ring: a fan from its centroid. */
function cap(p: Part, ring: XY[], z: number) {
  const centre: V3 = [ring.reduce((s, v) => s + v[0], 0) / ring.length, ring.reduce((s, v) => s + v[1], 0) / ring.length, z]
  const pts = at(ring, z)
  for (let i = 0; i < ring.length; i++) p.tri(centre, pts[i], pts[(i + 1) % ring.length])
}

/**
 * Granite walls with recessed vertical window bands. Long faces get broad
 * bands following the real bay rhythm; the short returns of the corner
 * notches are glazed, as on the building; bevels and slab steps stay stone.
 */
function facade(rim: Rim, bottom: number, top: number, stone = granite, bayWidth = 5.6) {
  for (let i = 0; i < rim.points.length; i++) {
    const a = rim.points[i], b = rim.points[(i + 1) % rim.points.length]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
    const n: V3 = [uy, -ux, 0]
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s + uy * depth, a[1] + uy * s - ux * depth, z]
    const panel = (p: Part, s0: number, s1: number, z0: number, z1: number) => p.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
    if (length < 1.6) {
      quad(stone, v(0, bottom), v(length, bottom), v(length, top), v(0, top), rim.normals[i], rim.normals[(i + 1) % rim.points.length])
      continue
    }
    if (length < 3.4) { panel(glass, 0, length, bottom, top); continue }
    const count = Math.max(1, Math.min(6, Math.round(length / bayWidth)))
    const spacing = length / count, width = spacing * .36, lo = bottom + .8, hi = top - .8
    panel(stone, 0, length, bottom, lo)
    panel(stone, 0, length, hi, top)
    let last = 0
    for (let j = 0; j < count; j++) {
      const s0 = spacing * (j + .5) - width / 2, s1 = s0 + width
      panel(stone, last, s0, lo, hi)
      const outer: XY[] = [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
      const inner = inset(outer, .35)
      const back = inner.map(([s, z]) => v(s, z, -RECESS))
      glass.tri(back[0], back[1], back[2]); glass.tri(back[0], back[2], back[3])
      for (let k = 0; k < 4; k++) {
        const l = (k + 1) % 4
        const ds = outer[l][0] - outer[k][0], dz = outer[l][1] - outer[k][1], edge = Math.hypot(ds, dz)
        const innerN = unit([n[0] * .4 - ux * dz / edge, n[1] * .4 - uy * dz / edge, ds / edge])
        quad(stone, v(...outer[k]), v(...outer[l]), back[l], back[k], n, n, innerN, innerN)
      }
      last = s1
    }
    panel(stone, last, length, lo, hi)
  }
}

/** A rounded coping round a stage's top, and its flat terrace inside. */
function terrace(rim: Rim, bottom: number, top: number, coping: Part, surface: Part) {
  const middle = inset(rim.points, .45), inner = inset(rim.points, 1)
  const a = at(rim.points, bottom), b = at(middle, top), c = at(inner, top - .5)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = rim.normals[i], nj = rim.normals[j]
    quad(coping, a[i], a[j], b[j], b[i], ni, nj, up, up)
    quad(coping, b[i], b[j], c[j], c[i], up, up, [-nj[0], -nj[1], 0], [-ni[0], -ni[1], 0])
  }
  cap(surface, inner, top - .5)
}

function stage(ring: XY[], bottom: number, top: number, surface = terraceRoof) {
  const rim = soften(ring)
  facade(rim, bottom, top - .7)
  terrace(rim, top - .7, top, granite, surface)
}

// ---- Granite tower -------------------------------------------------------

// Podium: the OSM outline is a 48.4 × 49.2 m square. A darker rose granite
// base carries the lobby storeys; the tower's own granite starts above it.
const podium: XY[] = [[-24.2, -24.5], [24.2, -24.5], [24.2, 24.5], [-24.2, 24.5]]
const podiumRim = soften(podium)
facade(podiumRim, 0, 14, plinth, 8)
facade(podiumRim, 14, 41.3)
terrace(podiumRim, 41.3, 42, granite, terraceRoof)

// [bottom, top, half-width, slab step, slab half-width, corner notch]
const stages: [number, number, number, number, number, number][] = [
  [42, 161, 21, 2, 9, 3.2],
  [161, 177, 19.3, 2.2, 8, 2.8],
  [177, 208, 16.8, 2.4, 7, 2.6],
  [208, 224, 13.6, 2.2, 5.6, 2.2],
  [224, 235, 11.8, 1.8, 4.6, 1.8],
]
for (const [z0, z1, h, step, c, n] of stages) stage(stagePlan(h, step, c, n), z0, z1)

// ---- Aluminium crown -----------------------------------------------------

/** A plain solid prism with softened corners, for the crown's lantern. */
function block(p: Part, ring: XY[], z0: number, z1: number, top: Part) {
  const rim = soften(ring, .35)
  const a = at(rim.points, z0), b = at(rim.points, z1)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length
    quad(p, a[i], a[j], b[j], b[i], rim.normals[i], rim.normals[j])
  }
  cap(top, rim.points, z1)
}
const circle = (r: number, n: number, phase = 0): XY[] =>
  Array.from({ length: n }, (_, i) => {
    const t = phase + (i * 2 * Math.PI) / n
    return [r * Math.cos(t), r * Math.sin(t)] as XY
  })

/**
 * One organ-pipe spire: a square shaft whose top quarter tapers to a point.
 * Broad enough (1.4–2.2 m) to survive a phone-sized view.
 */
function pipe(x: number, y: number, z0: number, z1: number, w: number) {
  const neck = z1 - Math.max(3, (z1 - z0) * .4)
  const ring = (z: number, r: number): V3[] => [[x - r, y - r, z], [x + r, y - r, z], [x + r, y + r, z], [x - r, y + r, z]]
  const base = ring(z0, w / 2), shoulder = ring(neck, w / 2), tip: V3 = [x, y, z1]
  spire.loft([base, shoulder])
  for (let i = 0; i < 4; i++) spire.tri(shoulder[i], shoulder[(i + 1) % 4], tip)
}

/**
 * Spires along a square ring: one on each face at every offset in `along`,
 * plus one in each corner notch. Face pipes rise towards the face centre.
 */
function squareRing(half: number, along: number[], corner: number, z0: number, faceTop: number, cornerTop: number, w: number) {
  const reach = Math.max(...along.map(Math.abs)) || 1
  for (let k = 0; k < 4; k++) {
    const r = (k * Math.PI) / 2, cs = Math.round(Math.cos(r)), sn = Math.round(Math.sin(r))
    const place = (x: number, y: number, top: number) => pipe(x * cs - y * sn, x * sn + y * cs, z0, top, w)
    for (const t of along) place(half, t, faceTop - (faceTop - cornerTop) * .5 * Math.abs(t) / reach)
    place(corner, corner, cornerTop)
  }
}

// Lantern: three pale stepped drums inside the 224–235 m stage.
block(crown, stagePlan(9.4, 1, 3.6, 1.6), 235, 244, roof)
block(crown, circle(6, 12, Math.PI / 12), 244, 249.5, roof)
block(crown, circle(3.8, 12), 249.5, 254, roof)

// Four tiers of pipes, each on the step it stands on, inside its edge.
// Ring 1 stands on the 235 m stage: face pipes on its slab, corner pipes
// in its notches. Ring 2 likewise on the lantern's 244 m top.
squareRing(10.5, [-3.2, 0, 3.2], 7, 235, 252, 246, 2.2)
squareRing(8.2, [-2.4, 2.4], 5.7, 244, 257, 252.5, 2)
for (const [x, y] of circle(4.4, 8, Math.PI / 8)) pipe(x, y, 249.5, 261, 1.8)
for (const [x, y] of circle(2.5, 4, Math.PI / 4)) pipe(x, y, 254, 263, 1.4)
pipe(0, 0, 254, 265, 1.6)

// sRGB colours sampled by eye from daylight photos (Commons, see notes).
const parts = [
  { part: granite, material: { name: 'pink-beige-granite', color: 0xd8c4b6 } },
  { part: plinth, material: { name: 'rose-granite-base', color: 0xae8c7e } },
  { part: glass, material: { name: 'window-bands', color: 0x66758a, roughness: .6 } },
  { part: terraceRoof, material: { name: 'terracotta-terraces', color: 0xc8968a } },
  { part: crown, material: { name: 'crown-lantern', color: 0x8794a0, roughness: .6 } },
  { part: roof, material: { name: 'crown-roof', color: 0xbdb9b1 } },
  { part: spire, material: { name: 'aluminium-spires', color: 0xe2e5e6, roughness: .5 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Bank of America Corporate Center', parts, {
  license: 'CC0-1.0', bearing: 48.8, elevation: 0, anchor: [35.2273121, -80.8422257], height: 265,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  footprint: { x: [-24.2, 24.2], y: [-24.5, 24.5] },
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/bank-of-america-corporate-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
