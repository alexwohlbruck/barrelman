/**
 * Queens Museum (the New York City Building, 1939) — original procedural
 * geometry, CC0-1.0.
 * bun scripts/landmarks/queens-museum.ts
 *
 * Authoring frame: x across the building (+x faces the Unisphere, east-north-
 * east), y along its long axis, z up, metres. Catalog bearing 337.3°, anchor
 * at the OSM outline's centroid. The outline (way/284856409, height 15.3) is
 * a long body flanked by two wider end pavilions, each with a shallow curved
 * flagpole niche in its long sides, and a lower block on each end.
 *
 * The building is mirrored end to end. Its two long faces differ: the east
 * (park) face is a colonnade of square limestone piers between tall glazed
 * bays, and the west (parkway) face carries the 2013 glass curtain. A broad
 * skylight lantern over the central atrium sits on the roof.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[] }

const stone = new Part(), glass = new Part(), curtain = new Part(), block = new Part()
const band = new Part(), roof = new Part(), skylight = new Part()
const BEVEL = .5, RECESS = .7
const up: V3 = [0, 0, 1]
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])
const rect = (x0: number, y0: number, x1: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

/** Round every convex plan corner with a two-normal bevel; concave ones stay sharp. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    const turn = u[0] * v[1] - u[1] * v[0]
    // Gentle turns (the niche arcs) and concave corners take an averaged
    // normal, so a curve shades as a curve.
    if (turn < .3) { points.push(b); normals.push(unit([u[1] + v[1], -u[0] - v[0], 0])); continue }
    const r = Math.min(radius, l0 * .3, l1 * .3)
    points.push([b[0] - u[0] * r, b[1] - u[1] * r], [b[0] + v[0] * r, b[1] + v[1] * r])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals }
}

function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

/** A fan from the centroid: every ring here is star-shaped about it. */
function cap(p: Part, ring: XY[], z: number, upward = true) {
  const c: V3 = [ring.reduce((s, v) => s + v[0], 0) / ring.length, ring.reduce((s, v) => s + v[1], 0) / ring.length, z]
  const pts = at(ring, z)
  for (let i = 0; i < pts.length; i++) {
    const j = (i + 1) % pts.length
    if (upward) p.tri(c, pts[i], pts[j]); else p.tri(c, pts[j], pts[i])
  }
}

function wall(p: Part, rim: Rim, z0: number, z1: number) {
  const a = at(rim.points, z0), b = at(rim.points, z1)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length
    quad(p, a[i], a[j], b[j], b[i], rim.normals[i], rim.normals[j])
  }
}

/** What to set into one straight face: `bays` window bands, or one long curtain. */
type Face = { bays: number; width: number; lo: number; hi: number; mat: Part; from?: number; to?: number }

/**
 * Walls of a softened ring between z0 and z1; a face matched by `pick` gets
 * recessed bands with bevelled reveals, everything else is plain stone.
 */
function facade(rim: Rim, z0: number, z1: number, pick: (n: V3, length: number) => Face | null) {
  const N = rim.points.length
  for (let i = 0; i < N; i++) {
    const a = rim.points[i], b = rim.points[(i + 1) % N]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
    const n: V3 = [uy, -ux, 0]
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s - n[0] * depth, a[1] + uy * s - n[1] * depth, z]
    const face = length > 4 ? pick(n, length) : null
    if (!face) {
      quad(stone, v(0, z0), v(length, z0), v(length, z1), v(0, z1), rim.normals[i], rim.normals[(i + 1) % N])
      continue
    }
    const panel = (s0: number, s1: number, za: number, zb: number) => stone.quad(v(s0, za), v(s1, za), v(s1, zb), v(s0, zb))
    const from = face.from ?? 0, to = length - (face.to ?? 0)
    const spacing = (to - from) / face.bays, w = spacing * face.width
    panel(0, length, z0, face.lo)
    panel(0, length, face.hi, z1)
    let last = 0
    for (let k = 0; k < face.bays; k++) {
      const s0 = from + spacing * (k + .5) - w / 2, s1 = s0 + w
      panel(last, s0, face.lo, face.hi)
      const outer: XY[] = [[s0, face.lo], [s1, face.lo], [s1, face.hi], [s0, face.hi]]
      const inner = inset(outer, BEVEL)
      const back = inner.map(([s, z]) => v(s, z, RECESS))
      face.mat.quad(back[0], back[1], back[2], back[3])
      for (let e = 0; e < 4; e++) {
        const f = (e + 1) % 4
        const ds = outer[f][0] - outer[e][0], dz = outer[f][1] - outer[e][1], el = Math.hypot(ds, dz)
        // The reveal's inner edge turns into the recess; the outer stays on the pier.
        const into = unit([n[0] * .4 - ux * dz / el, n[1] * .4 - uy * dz / el, ds / el])
        quad(stone, v(...outer[e]), v(...outer[f]), back[f], back[e], n, n, into, into)
      }
      last = s1
    }
    panel(last, length, face.lo, face.hi)
  }
}

/**
 * The parapet: a broad dark band set 0.3 m back under a rounded pale coping,
 * then the flat roof inside it.
 */
function crown(rim: Rim, z0: number, z1: number, top: number, surface = roof) {
  const back = inset(rim.points, .3)
  const lower = at(rim.points, z0), b0 = at(back, z0 + .05), b1 = at(back, z1), upper = at(rim.points, z1)
  for (let i = 0; i < lower.length; i++) {
    const j = (i + 1) % lower.length, ni = rim.normals[i], nj = rim.normals[j]
    // A short sloped soffit into the band and back out keeps it a recess, not a slot.
    stone.quad(lower[i], lower[j], b0[j], b0[i])
    quad(band, b0[i], b0[j], b1[j], b1[i], ni, nj)
    stone.quad(b1[i], b1[j], upper[j], upper[i])
  }
  coping(rim, z1, top, surface)
}

/** Two smooth chamfers form a rounded coping, then the roof surface. */
function coping(rim: Rim, z0: number, top: number, surface: Part) {
  const middle = inset(rim.points, .45), inner = inset(rim.points, 1)
  const a = at(rim.points, z0), b = at(middle, top), c = at(inner, top - .45)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = rim.normals[i], nj = rim.normals[j]
    quad(stone, a[i], a[j], b[j], b[i], ni, nj, up, up)
    quad(stone, b[i], b[j], c[j], c[i], up, up, [-nj[0], -nj[1], 0], [-ni[0], -ni[1], 0])
  }
  cap(surface, inner, top - .45)
}

const H = 15.3, CORNICE = 11, BAND = 13.7
const east = (n: V3) => n[0] > .9, west = (n: V3) => n[0] < -.9

// Central body: the colonnade storey, then a stone attic set back 1.6 m.
const body = soften(rect(-27, -37, 27, 37))
facade(body, 0, CORNICE - .6, (n) =>
  east(n) ? { bays: 9, width: .8, lo: .5, hi: 8.6, mat: glass, from: 1, to: 1 } :
  // The 2013 west front: a long glass curtain across the body's whole
  // exposed length, read as six broad panes between short stone returns.
  west(n) ? { bays: 6, width: .93, lo: .5, hi: 9.2, mat: curtain, from: 4.5, to: 4.5 } : null)
coping(body, CORNICE - .6, CORNICE, stone)
const attic = soften(rect(-25.4, -37, 25.4, 37))
wall(stone, attic, CORNICE - .45, BAND)
crown(attic, BAND, H - .6, H)

// End pavilions, full height, each long side scooped by the curved niche
// that holds the flagpoles. The niche is the OSM arc: a 10 m chord, 3.4 m deep.
function niche(x: number, yc: number, side: 1 | -1): XY[] {
  const chord = 5, sag = 3.4, r = (chord * chord + sag * sag) / (2 * sag)
  const cx = x + side * (r - sag), half = Math.asin(chord / r)
  const pts: XY[] = []
  for (let k = 0; k <= 8; k++) {
    const t = side > 0 ? -half + 2 * half * k / 8 : half - 2 * half * k / 8
    pts.push([cx - side * r * Math.cos(t), yc + r * Math.sin(t)])
  }
  return pts
}
function pavilion(flip: 1 | -1) {
  // Built for the north end and mirrored for the south, keeping the ring
  // counter-clockwise either way.
  let ring: XY[] = [[-33, 36], [33, 36], ...niche(33, 47, 1), [33, 58], [-33, 58], ...niche(-33, 47, -1)]
  if (flip < 0) ring = ring.map(([x, y]): XY => [x, -y]).reverse()
  const rim = soften(ring)
  wall(stone, rim, 0, BAND)
  crown(rim, BAND, H - .6, H)
}
pavilion(1); pavilion(-1)

// The low end blocks, faced with a glass-block colonnade.
for (const flip of [1, -1] as const) {
  let ring = rect(-25.8, 57, 25.8, 64)
  if (flip < 0) ring = ring.map(([x, y]): XY => [x, -y]).reverse()
  const rim = soften(ring)
  facade(rim, 0, CORNICE - .6, (n, length) =>
    Math.abs(n[1]) > .9 && length > 40 ? { bays: 6, width: .74, lo: .5, hi: 8.6, mat: block } : null)
  coping(rim, CORNICE - .6, CORNICE, roof)
}

// The skylight lantern over the 2013 atrium: a low glazed hip on a stone curb.
{
  const curb = soften(rect(-12, -15, 12, 15), .4)
  wall(stone, curb, H - .5, H + .5)
  const base = inset(curb.points, .15), ridge = inset(curb.points, 2.6)
  const lo = at(base, H + .5), hi = at(ridge, H + 1.7), rimTop = at(curb.points, H + .5)
  for (let i = 0; i < lo.length; i++) {
    const j = (i + 1) % lo.length
    stone.quad(rimTop[i], rimTop[j], lo[j], lo[i])
    skylight.quad(lo[i], lo[j], hi[j], hi[i])
  }
  cap(skylight, ridge, H + 1.7)
}

const parts = [
  { part: stone, material: { name: 'limestone', color: 0xd3c8b4 } },
  { part: glass, material: { name: 'colonnade-glass', color: 0x7a8892 } },
  { part: curtain, material: { name: 'glass-curtain', color: 0x8695a0 } },
  { part: block, material: { name: 'glass-block', color: 0x6f7b84 } },
  { part: band, material: { name: 'parapet-band', color: 0x5a524c } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  { part: skylight, material: { name: 'skylight', color: 0x9aabb6, roughness: .6 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Queens Museum', parts, {
  license: 'CC0-1.0', bearing: 337.3, elevation: 0, anchor: [40.7458734, -73.8467451], height: H,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: ['way/284856409'], footprint: { x: [-33, 33], y: [-64, 64] },
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/queens-museum.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
