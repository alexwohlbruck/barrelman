/**
 * Flatiron Building — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/flatiron-building.ts
 *
 * Map frame: x east, y north, z up, metres, rotated so +y runs along the
 * wedge's long axis (BEARING). Anchor is the OSM outline's centroid. The
 * footprint is the OSM outline (way/264768896) itself, rotated into that
 * frame; the low rooftop attic follows its building:part (way/764237515,
 * 88 m). No textures: window bands, piers, belts and the cornice are geometry.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const BEARING = 18
const ANCHOR = { lng: -73.9896529, lat: 40.7410024 }

// OSM outline and part, metres east/north of the anchor, counter-clockwise.
const OUTLINE_EN: XY[] = ([[4.03, -26.83], [3.22, -26.61], [3.11, -26.55], [2.85, -26.41], [1.85, -25.85], [1.58, -25.69],
  [-17.03, -15.29], [-17.39, -14.97], [-17.97, -14.21], [-18.34, -13.3], [-18.44, -12.35], [-18.29, -11.39], [-17.89, -10.51],
  [-6.05, 10.54], [-0.89, 19.72], [0.96, 23.0], [5.8, 31.6], [6.56, 32.25], [7.46, 32.71], [7.95, 32.84], [8.96, 32.94],
  [9.95, 32.77], [10.42, 32.61], [11.29, 32.1], [11.68, 31.77], [12.31, 30.98], [12.78, 29.01], [9.89, 2.01], [7.0, -24.98],
  [6.61, -25.72], [6.02, -26.3], [5.28, -26.69], [4.65, -26.84]] as XY[]).reverse()
const PART_EN: XY[] = [[7.95, 24.55], [6.09, 25.22], [-14.27, -11.23], [3.31, -20.89]]

const rad = (BEARING * Math.PI) / 180
const rotate = ([e, n]: XY): XY => [e * Math.cos(rad) - n * Math.sin(rad), e * Math.sin(rad) + n * Math.cos(rad)]

/** Drop vertices that sit on a straight run, so each street front is one edge. */
function merge(ring: XY[]): XY[] {
  const out: XY[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const t0 = Math.atan2(b[1] - a[1], b[0] - a[0]), t1 = Math.atan2(c[1] - b[1], c[0] - b[0])
    let d = Math.abs(t1 - t0); if (d > Math.PI) d = 2 * Math.PI - d
    if (d > (2.5 * Math.PI) / 180) out.push(b)
  }
  return out
}
const RING = merge(OUTLINE_EN.map(rotate))
const PART = PART_EN.map(rotate)

const stone = new Part(), base = new Part(), crown = new Part(), glass = new Part(), roof = new Part()
const up: V3 = [0, 0, 1]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map((n) => n / l) as V3 }
const LONG = 8 // edges longer than this are street fronts; the rest round the corners
const BEVEL = 0.5
const RECESS = 0.6

function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

/** Outward offset with mitred corners (negative insets). */
function offset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] + ((u[0] + v[0]) * d) / k, b[1] + ((u[1] + v[1]) * d) / k]
  })
}

type Edge = { a: XY; b: XY; length: number; ux: number; uy: number; n: V3; long: boolean }
const edgesOf = (ring: XY[]): Edge[] => ring.map((a, i) => {
  const b = ring[(i + 1) % ring.length], length = Math.hypot(b[0] - a[0], b[1] - a[1])
  const ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
  return { a, b, length, ux, uy, n: [uy, -ux, 0] as V3, long: length > LONG }
})
const EDGES = edgesOf(RING)
/** A corner normal is smooth round the rounded corners, flat on the street fronts. */
const endNormals = (edges: Edge[]) => edges.map((e, i) => {
  const prev = edges[(i + edges.length - 1) % edges.length], next = edges[(i + 1) % edges.length]
  const avg = (o: Edge): V3 => unit([e.n[0] + o.n[0], e.n[1] + o.n[1], 0])
  return [e.long ? e.n : prev.long ? prev.n : avg(prev), e.long ? e.n : next.long ? next.n : avg(next)] as [V3, V3]
})
const NORMALS = endNormals(EDGES)

/**
 * A moulding swept round the whole ring: `profile` is (outward offset, z)
 * pairs bottom to top. Each step is shaded flat up the profile, smooth round
 * the plan, so ledges catch a soft highlight.
 */
function moulding(p: Part, profile: [number, number][]) {
  const rings = profile.map(([d, z]) => ({ pts: offset(RING, d), z, d }))
  for (let k = 0; k < rings.length - 1; k++) {
    const r0 = rings[k], r1 = rings[k + 1]
    const dd = r1.d - r0.d, dz = r1.z - r0.z, l = Math.hypot(dd, dz)
    const h = dz / l, v = -dd / l
    for (let i = 0; i < RING.length; i++) {
      const j = (i + 1) % RING.length, [ni, nj] = NORMALS[i]
      const tilt = (n: V3): V3 => unit([n[0] * h, n[1] * h, v])
      quadN(p, [...r0.pts[i], r0.z], [...r0.pts[j], r0.z], [...r1.pts[j], r1.z], [...r1.pts[i], r1.z], tilt(ni), tilt(nj))
    }
  }
}

/** Plain wall on the rounded corner edges between two heights. */
function corners(p: Part, z0: number, z1: number) {
  EDGES.forEach((e, i) => {
    if (e.long) return
    const [na, nb] = NORMALS[i]
    quadN(p, [...e.a, z0], [...e.b, z0], [...e.b, z1], [...e.a, z1], na, nb)
  })
}

/** Inward corner offset of a window outline in facade (s, z) space. */
function insetPoly(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - ((u[0] + v[0]) * d) / k, b[1] - ((u[1] + v[1]) * d) / k]
  })
}

/**
 * One row of broad recessed window bands across every street front, filling
 * the face from z0 to z1: a sill strip, a head strip, bevelled piers between
 * bands. `arch` gives the band a semicircular head.
 */
function bandRow(wall: Part, z0: number, z1: number, margin: number, arch = false) {
  for (const e of EDGES) {
    if (!e.long) continue
    const { a, ux, uy, n, length } = e
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s - n[0] * depth, a[1] + uy * s - n[1] * depth, z]
    const panel = (s0: number, s1: number, za: number, zb: number) => quadN(wall, v(s0, za), v(s1, za), v(s1, zb), v(s0, zb), n, n)
    const count = Math.max(2, Math.round(length / 6.6))
    const spacing = length / count, width = spacing * 0.58, lo = z0 + margin, hi = z1 - margin
    panel(0, length, z0, lo)
    panel(0, length, hi, z1)
    let last = 0
    for (let j = 0; j < count; j++) {
      const s0 = spacing * (j + 0.5) - width / 2, s1 = s0 + width, r = width / 2
      panel(last, s0, lo, hi)
      const spring = hi - r
      const outer: XY[] = arch
        ? [[s0, lo], [s1, lo], ...Array.from({ length: 9 }, (_, k): XY => {
          const t = (k * Math.PI) / 8
          return [(s0 + s1) / 2 + r * Math.cos(t), spring + r * Math.sin(t)]
        })]
        : [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
      // The arch's first point repeats the right jamb top; drop duplicates.
      const ring = outer.filter((p, k) => k === 0 || Math.hypot(p[0] - outer[k - 1][0], p[1] - outer[k - 1][1]) > 1e-6)
      const inner = insetPoly(ring, BEVEL)
      const back = inner.map(([s, z]) => v(s, z, RECESS))
      for (let k = 1; k < back.length - 1; k++) glass.tri(back[0], back[k], back[k + 1], undefined, undefined, undefined, [n, n, n])
      for (let k = 0; k < ring.length; k++) {
        const l = (k + 1) % ring.length
        const ds = ring[l][0] - ring[k][0], dz = ring[l][1] - ring[k][1], el = Math.hypot(ds, dz)
        // The reveal's inner edge turns into the recess; the outer stays on the pier.
        const innerN = unit([n[0] * 0.4 - (ux * dz) / el, n[1] * 0.4 - (uy * dz) / el, ds / el])
        quadN(wall, v(...ring[k]), v(...ring[l]), back[l], back[k], n, n, innerN, innerN)
      }
      if (arch) {
        // Spandrels: fill between the arch head and the row's top line.
        for (let k = 2; k < ring.length - 1; k++) {
          const p = ring[k], q = ring[k + 1]
          quadN(wall, v(...p), v(p[0], hi), v(q[0], hi), v(...q), n, n)
        }
      }
      last = s1
    }
    panel(last, length, lo, hi)
  }
}

function capRing(p: Part, ring: XY[], z: number, upward = true) {
  const c: V3 = [ring.reduce((s, q) => s + q[0], 0) / ring.length, ring.reduce((s, q) => s + q[1], 0) / ring.length, z]
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length, a: V3 = [...ring[i], z], b: V3 = [...ring[j], z]
    if (upward) p.tri(c, a, b, undefined, undefined, undefined, [up, up, up])
    else p.tri(c, b, a)
  }
}

// --- Rusticated base: four storeys in darker stone, two rows of bands. ---
const BASE_TOP = 17.4, SHAFT0 = 18.6, SHAFT1 = 66.6, CROWN0 = 67.6, ARCH0 = 74.6, CORNICE0 = 82, TOP = 86
moulding(base, [[0.25, 0], [0.25, 0.9], [0, 1.2]]) // granite plinth
bandRow(base, 1.2, 9.4, 0.5)
bandRow(base, 9.4, BASE_TOP, 0.6)
corners(base, 1.2, BASE_TOP)
moulding(crown, [[0, BASE_TOP], [0.4, BASE_TOP + 0.35], [0.4, SHAFT0 - 0.3], [0, SHAFT0]])

// --- Shaft: one tall band per bay, the oriel rhythm read as deep bays. ---
bandRow(stone, SHAFT0, SHAFT1, 0.4)
corners(stone, SHAFT0, SHAFT1)
moulding(crown, [[0, SHAFT1], [0.4, SHAFT1 + 0.35], [0.4, CROWN0 - 0.3], [0, CROWN0]])

// --- Crown: a squat row, then the tall arched windows under the cornice. ---
bandRow(crown, CROWN0, ARCH0, 0.6)
bandRow(crown, ARCH0, CORNICE0, 0.6, true)
corners(crown, CROWN0, CORNICE0)

// The narrow prow carries one column of windows: flush glass on the two
// rounded faces either side of its tip, laid 2 cm proud so it never z-fights.
const tip = RING.reduce((m, q) => (q[1] > m[1] ? q : m))
const prow = EDGES.map((e, i) => ({ e, i, d: Math.hypot((e.a[0] + e.b[0]) / 2 - tip[0], (e.a[1] + e.b[1]) / 2 - tip[1]) }))
  .filter(({ e }) => !e.long).sort((x, y) => x.d - y.d).slice(0, 2)
for (const { e, i } of prow) {
  const [na, nb] = NORMALS[i]
  const at = (t: number, z: number, n: V3): V3 => [e.a[0] + (e.b[0] - e.a[0]) * t + n[0] * 0.02, e.a[1] + (e.b[1] - e.a[1]) * t + n[1] * 0.02, z]
  const t0 = e.b[0] === tip[0] && e.b[1] === tip[1] ? 0.15 : 0, t1 = t0 ? 1 : 0.85
  for (const [z0, z1] of [[SHAFT0 + 0.6, SHAFT1 - 0.6], [CROWN0 + 0.8, CORNICE0 - 0.8]])
    quadN(glass, at(t0, z0, na), at(t1, z0, nb), at(t1, z1, nb), at(t0, z1, na), na, nb)
}

// Heavy projecting cornice, bevelled lip, then a pale coping round the roof.
moulding(crown, [[0, CORNICE0], [0.5, CORNICE0 + 0.8], [1.65, CORNICE0 + 2.6], [1.65, TOP - 0.45], [1.25, TOP]])
const copingOuter = offset(RING, 1.25), copingInner = offset(RING, -0.9), roofRing = offset(RING, -1.3)
for (let i = 0; i < RING.length; i++) {
  const j = (i + 1) % RING.length, [ni, nj] = NORMALS[i]
  const at = (r: XY[], k: number, z: number): V3 => [...r[k], z]
  quadN(crown, at(copingOuter, i, TOP), at(copingOuter, j, TOP), at(copingInner, j, TOP), at(copingInner, i, TOP), up, up)
  const ini: V3 = [-ni[0], -ni[1], 0], inj: V3 = [-nj[0], -nj[1], 0]
  quadN(crown, at(copingInner, i, TOP), at(copingInner, j, TOP), at(roofRing, j, TOP - 0.5), at(roofRing, i, TOP - 0.5),
    unit([ini[0], ini[1], 1]), unit([inj[0], inj[1], 1]))
}
capRing(roof, roofRing, TOP - 0.5)

/**
 * The three street fronts' lines moved inward by `d` and intersected, with each
 * corner cut back by `cut` in three steps. Built from the long edges alone,
 * because insetting the many short edges of OSM's rounded corners by more
 * than their length folds them over.
 */
function atticRing(d: number, cut: number): XY[] {
  const fronts = EDGES.filter((e) => e.long)
  const meet = (e: Edge, f: Edge): XY => {
    const p: XY = [e.a[0] - e.n[0] * d, e.a[1] - e.n[1] * d], q: XY = [f.a[0] - f.n[0] * d, f.a[1] - f.n[1] * d]
    const den = e.ux * f.uy - e.uy * f.ux
    const t = ((q[0] - p[0]) * f.uy - (q[1] - p[1]) * f.ux) / den
    return [p[0] + e.ux * t, p[1] + e.uy * t]
  }
  const out: XY[] = []
  fronts.forEach((e, i) => {
    const prev = fronts[(i + fronts.length - 1) % fronts.length], c = meet(prev, e)
    const a: XY = [c[0] - prev.ux * cut, c[1] - prev.uy * cut], b: XY = [c[0] + e.ux * cut, c[1] + e.uy * cut]
    const m: XY = [(a[0] + b[0] + c[0] * 0.6) / 2.6, (a[1] + b[1] + c[1] * 0.6) / 2.6]
    out.push(a, [(a[0] + m[0]) / 2 * 0.9 + m[0] * 0.1, (a[1] + m[1]) / 2 * 0.9 + m[1] * 0.1], m,
      [(b[0] + m[0]) / 2 * 0.9 + m[0] * 0.1, (b[1] + m[1]) / 2 * 0.9 + m[1] * 0.1], b)
  })
  return out
}

// Low rooftop attic up to the building:part's 88 m. The part's own quad is
// lopsided, so the attic follows the outline instead, set back far enough to
// cover the part (which sits 3 m or more inside the walls) without crowding
// the cornice.
const attic = atticRing(2.4, 2.2), atticEdges = edgesOf(attic)
const atticNormals = endNormals(atticEdges.map((e) => ({ ...e, long: e.length > 4 })))
atticEdges.forEach((e, i) => quadN(crown, [...e.a, TOP - 0.5], [...e.b, TOP - 0.5], [...e.b, 88], [...e.a, 88], ...atticNormals[i]))
// The attic must cover the part it replaces.
const inside = (ring: XY[], [x, y]: XY) => ring.reduce((c, a, i) => {
  const b = ring[(i + 1) % ring.length]
  return (a[1] > y) !== (b[1] > y) && x < ((b[0] - a[0]) * (y - a[1])) / (b[1] - a[1]) + a[0] ? !c : c
}, false)
if (!PART.every((q) => inside(attic, q))) throw new Error('attic does not cover building:part 764237515')
const atticTop = offset(attic, -0.4)
atticEdges.forEach((_, i) => {
  const j = (i + 1) % attic.length, [ni, nj] = atticNormals[i]
  quadN(crown, [...attic[i], 88], [...attic[j], 88], [...atticTop[j], 88.3], [...atticTop[i], 88.3], unit([ni[0], ni[1], 1]), unit([nj[0], nj[1], 1]))
})
capRing(roof, atticTop, 88.3)

// Close the bottom so the shadow pass sees a solid.
capRing(base, offset(RING, 0.25), 0, false)

// sRGB colours from daylight photos: cream terracotta shaft, greyer
// rusticated limestone base, paler crown, grey-blue glazing.
const parts = [
  { part: stone, material: { name: 'terracotta-shaft', color: 0xd9ccb4 } },
  { part: base, material: { name: 'rusticated-base', color: 0xb8a98f } },
  { part: crown, material: { name: 'crown-cornice', color: 0xe4d9c4 } },
  { part: glass, material: { name: 'window-bands', color: 0x7b8790, roughness: 0.85 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Flatiron Building', parts, {
  license: 'CC0-1.0', bearing: BEARING, elevation: 0, anchor: [ANCHOR.lat, ANCHOR.lng], height: 88.3,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: ['way/264768896', 'way/764237515'],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/flatiron-building.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
console.log('edges', EDGES.map((e) => `${e.length.toFixed(1)}${e.long ? '*' : ''}`).join(' '))
