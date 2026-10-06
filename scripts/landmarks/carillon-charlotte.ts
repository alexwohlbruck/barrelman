/**
 * The Carillon, Charlotte (1991) — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/carillon-charlotte.ts
 *
 * Map frame turned to the building: x runs south-east along West Trade
 * Street, y north-east along Poplar Street, z up, metres. Anchor
 * 35.2285233,-80.8453018 (the centroid of OSM way/90480703); bearing 49.5°,
 * elevation 0.
 *
 * OSM draws one outline, an L: 39.5 × 54.3 m with the corner at Trade and
 * the entrance plaza cut out. The model is a granite podium over that whole
 * outline, and above it a central gabled spine between two lower wings that
 * step up towards it — the "gabled setbacks" — with the spine's half-round
 * glass bay facing Trade and a pointed pinnacle on the gable apex.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[] }
const granite = new Part(), base = new Part(), glass = new Part()
const terrace = new Part(), roof = new Part(), metal = new Part()
const BEVEL = .5
const RECESS = .6
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const up: V3 = [0, 0, 1]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])
const rect = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

/** Trim every convex plan corner into a small rounded bevel. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    const r = Math.min(radius, l0 * .2, l1 * .2)
    if (u[0] * v[1] - u[1] * v[0] < -1e-5) {
      points.push(b); normals.push(unit([u[1] + v[1], -u[0] - v[0], 0])); continue
    }
    points.push([b[0] - u[0] * r, b[1] - u[1] * r], [b[0] + v[0] * r, b[1] + v[1] * r])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals }
}

function inset(ring: XY[], distance: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const d = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * distance / d, b[1] - (u[1] + v[1]) * distance / d]
  })
}

/** A fan from the centroid: every ring here is star-shaped about it. */
function cap(p: Part, ring: XY[], z: number, upward = true) {
  const centre: V3 = [ring.reduce((s, v) => s + v[0], 0) / ring.length, ring.reduce((s, v) => s + v[1], 0) / ring.length, z]
  const points = at(ring, z)
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    if (upward) p.tri(centre, points[i], points[j]); else p.tri(centre, points[j], points[i])
  }
}

/**
 * Recessed window bands between granite piers on every face of a rim.
 * `bands` picks the count for a face from its length; 0 leaves it plain.
 */
function facade(rim: Rim, bottom: number, top: number, wall: Part, bands: (length: number, i: number) => number) {
  for (let i = 0; i < rim.points.length; i++) {
    const a = rim.points[i], b = rim.points[(i + 1) % rim.points.length]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
    const n: V3 = [uy, -ux, 0]
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s + uy * depth, a[1] + uy * s - ux * depth, z]
    const panel = (s0: number, s1: number, z0: number, z1: number) => wall.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
    const count = length < 3 ? 0 : bands(length, i)
    if (!count) {
      quad(wall, v(0, bottom), v(length, bottom), v(length, top), v(0, top), rim.normals[i], rim.normals[(i + 1) % rim.points.length])
      continue
    }
    const spacing = length / count, width = Math.min(spacing * .5, spacing - 2), lo = bottom + 1, hi = top - 1
    panel(0, length, bottom, lo)
    panel(0, length, hi, top)
    let last = 0
    for (let j = 0; j < count; j++) {
      const s0 = spacing * (j + .5) - width / 2, s1 = s0 + width
      panel(last, s0, lo, hi)
      const outer: XY[] = [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
      const inner = inset(outer, .35)
      const back = inner.map(([s, z]) => v(s, z, -RECESS))
      glass.tri(back[0], back[1], back[2]); glass.tri(back[0], back[2], back[3])
      for (let k = 0; k < 4; k++) {
        const l = (k + 1) % 4
        const ds = outer[l][0] - outer[k][0], dz = outer[l][1] - outer[k][1], edge = Math.hypot(ds, dz)
        const innerN = unit([n[0] * .4 - ux * dz / edge, n[1] * .4 - uy * dz / edge, ds / edge])
        quad(wall, v(...outer[k]), v(...outer[l]), back[l], back[k], n, n, innerN, innerN)
      }
      last = s1
    }
    panel(last, length, lo, hi)
  }
}

/** A rounded coping round a flat roof or setback terrace. */
function coping(rim: Rim, bottom: number, top: number, surface: Part, edge = granite) {
  const middle = inset(rim.points, .45), inner = inset(rim.points, 1)
  const a = at(rim.points, bottom), b = at(middle, top), c = at(inner, top - .5)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = rim.normals[i], nj = rim.normals[j]
    quad(edge, a[i], a[j], b[j], b[i], ni, nj, up, up)
    quad(edge, b[i], b[j], c[j], c[i], up, up, [-nj[0], -nj[1], 0], [-ni[0], -ni[1], 0])
  }
  cap(surface, inner, top - .5)
}

/** One stage: window bands to just under `top`, then a coping and roof. */
function stage(ring: XY[], bottom: number, top: number, surface: Part, bands = (l: number) => Math.max(1, Math.round(l / 7.2))) {
  const rim = soften(ring)
  // A setback a floor or two tall gets no bands: they would read as thin
  // dark stripes from the map.
  facade(rim, bottom, top - .7, granite, top - bottom < 7 ? () => 0 : bands)
  coping(rim, top - .7, top, surface)
}

// ---------------------------------------------------------------- podium
// The OSM outline, x -19…20.5, y -25.9…28.4, entrance corner cut out.
const outline: XY[] = [[-19, -25.9], [20.5, -25.9], [20.5, 19.2], [8, 19.2], [8, 28.4], [-19, 28.4]]
const PODIUM = 18
{
  const rim = soften(outline)
  facade(rim, 0, PODIUM - .8, base, (l) => Math.max(1, Math.round(l / 9)))
  coping(rim, PODIUM - .8, PODIUM, roof, base)
}

// ---------------------------------------------------------------- tower
// Plan set 1 m in from the podium. The spine is centred on the building's
// width; the wings run into it by half a metre so their inner walls hide.
const S0 = -7.5, S1 = 8.5, SC = (S0 + S1) / 2
const Y0 = -24.9, SPINE_FACE = 24.4
const WING = 80, EAVE = 100, APEX = 113
const wings = [
  { outer: -18, inner: S0 + .5, y1: 27.4 }, // north-west, along Poplar
  { outer: 19.5, inner: S1 - .5, y1: 18.2 }, // south-east, behind the plaza
]
// Each wing rises in two setbacks towards the spine, so from Trade the
// tower reads as a stepped gable around the pitched crown.
const steps = [[PODIUM, WING, 0], [WING, 84.5, 3.5], [84.5, 89, 7]] as const
for (const w of wings) {
  const dir = Math.sign(w.inner - w.outer)
  for (const [z0, z1, cut] of steps) {
    const xo = w.outer + dir * cut
    const ring = dir > 0 ? rect(xo, w.inner, Y0, w.y1) : rect(w.inner, xo, Y0, w.y1)
    stage(ring, z0, z1, terrace, (l) => l > 30 ? 7 : l > 9 ? 2 : l > 5 ? 1 : 0)
  }
}

// The spine: granite walls with bands on its flanks and its south-west end.
{
  const ring = rect(S0, S1, Y0, SPINE_FACE)
  const rim = soften(ring)
  // Face order from soften: south-west end, south-east flank, north-east
  // end (the bay face, kept plain: the bay covers it), north-west flank.
  facade(rim, PODIUM, EAVE, granite, (l, i) => {
    if (l > 30) return 7
    const midY = (rim.points[i][1] + rim.points[(i + 1) % rim.points.length][1]) / 2
    return midY > 0 ? 0 : 2
  })
}

// Half-round glass bay on the Trade face, rising to a granite balcony rim.
const BAY_HALF = 5.5, BAY_OUT = 3
const bayR = (BAY_HALF ** 2 + BAY_OUT ** 2) / (2 * BAY_OUT)
const bayC: XY = [SC, SPINE_FACE + BAY_OUT - bayR]
const bayArc = (r: number, n = 10): XY[] => {
  const a0 = Math.asin((SPINE_FACE - bayC[1]) / bayR)
  return Array.from({ length: n + 1 }, (_, k) => {
    const a = a0 + (Math.PI - 2 * a0) * k / n
    return [bayC[0] + r * Math.cos(a), bayC[1] + r * Math.sin(a)]
  })
}
function bay(p: Part, r: number, z0: number, z1: number, top = true) {
  const arc = bayArc(r)
  for (let k = 0; k < arc.length - 1; k++) {
    const [a, b] = [arc[k], arc[k + 1]]
    const na = unit([a[0] - bayC[0], a[1] - bayC[1], 0]), nb = unit([b[0] - bayC[0], b[1] - bayC[1], 0])
    // Built from the left so the face winds outwards.
    quad(p, [b[0], b[1], z0], [a[0], a[1], z0], [a[0], a[1], z1], [b[0], b[1], z1], nb, na)
  }
  if (!top) return
  const ring: XY[] = [...arc].reverse()
  const mid: XY = [bayC[0], SPINE_FACE + .5]
  for (let k = 0; k < ring.length - 1; k++) roof.tri([mid[0], mid[1], z1], [ring[k][0], ring[k][1], z1], [ring[k + 1][0], ring[k + 1][1], z1])
}
bay(granite, bayR, PODIUM, PODIUM + 2.5)
bay(glass, bayR, PODIUM + 2.5, 95)
bay(granite, bayR + .25, 95, 97.5)

// ---------------------------------------------------------------- crown
// A steep copper roof along the spine, glazed gables at both ends, slim
// pinnacles at the eave corners and a pointed spire on the Trade apex.
coping(soften(rect(S0, S1, Y0, SPINE_FACE)), EAVE - .2, EAVE + .5, roof)
{
  const zE = EAVE + .5, xL = S0 + .3, xR = S1 - .3, y0 = Y0 + .3, y1 = SPINE_FACE - .3
  const slopeL = unit([-(APEX - zE), 0, SC - xL]), slopeR = unit([APEX - zE, 0, xR - SC])
  quad(metal, [xL, y1, zE], [xL, y0, zE], [SC, y0, APEX], [SC, y1, APEX], slopeL, slopeL)
  quad(metal, [xR, y0, zE], [xR, y1, zE], [SC, y1, APEX], [SC, y0, APEX], slopeR, slopeR)
  for (const [y, out] of [[y1, 1], [y0, -1]] as const) {
    const tri = (p: Part, a: V3, b: V3, c: V3) => out > 0 ? p.tri(a, b, c) : p.tri(b, a, c)
    tri(granite, [xR, y, zE], [xL, y, zE], [SC, y, APEX])
    // A flush glazed triangle inside a broad granite frame.
    const g = y + out * .06, ins = 1.1
    tri(glass, [xR - ins * 1.6, g, zE + .6], [xL + ins * 1.6, g, zE + .6], [SC, g, APEX - ins * 1.9])
  }
}

/** A square post with a pyramid cap, its corners slightly chamfered. */
function pinnacle(p: Part, cx: number, cy: number, half: number, z0: number, z1: number, tip: number) {
  const ring = (r: number, z: number): V3[] => {
    const k = r * .3
    return [[cx - r + k, cy - r, z], [cx + r - k, cy - r, z], [cx + r, cy - r + k, z], [cx + r, cy + r - k, z],
      [cx + r - k, cy + r, z], [cx - r + k, cy + r, z], [cx - r, cy + r - k, z], [cx - r, cy - r + k, z]]
  }
  const lo = ring(half, z0), hi = ring(half, z1)
  p.loft([lo, hi])
  const apex: V3 = [cx, cy, tip]
  for (let i = 0; i < 8; i++) p.tri(hi[i], hi[(i + 1) % 8], apex)
}
for (const x of [S0 + .9, S1 - .9]) for (const y of [Y0 + .9, SPINE_FACE - .9]) pinnacle(granite, x, y, .9, EAVE, EAVE + 4.5, EAVE + 7)
// The spire on the Trade gable, and a smaller pinnacle on the far one.
pinnacle(granite, SC, SPINE_FACE - 1.6, 1.7, APEX - 5, APEX + 2.5, APEX + 6.5)
pinnacle(metal, SC, SPINE_FACE - 1.6, .35, APEX + 6, APEX + 6.4, 122)
pinnacle(granite, SC, Y0 + 1.2, 1, APEX - 4, APEX + 1, APEX + 3.5)

// ---------------------------------------------------------------- write
// sRGB, read from daylight photos: a warm rose-taupe granite, a slightly
// paler podium, dark blue-grey reflective glass.
const parts = [
  { part: granite, material: { name: 'granite', color: 0x9e8579 } },
  { part: base, material: { name: 'podium-granite', color: 0xa99488 } },
  { part: glass, material: { name: 'window', color: 0x5f7187 } },
  { part: terrace, material: { name: 'terracotta-terraces', color: 0xc8968a } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  // Weathered, unpatinated copper: brown in the photos, never green.
  { part: metal, material: { name: 'copper-crown', color: 0x9a6e58, roughness: .6 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Carillon', parts, {
  license: 'CC0-1.0', bearing: 49.5, elevation: 0, anchor: [35.2285233, -80.8453018], height: 122,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: ['way/90480703'],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/carillon-charlotte.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
