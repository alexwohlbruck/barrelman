/**
 * Deno's Wonder Wheel, Coney Island — procedural, CC0-1.0.
 * bun scripts/landmarks/wonder-wheel.ts
 *
 * Map frame: x east, y north, z up, metres; origin on the ground under the
 * axle. The wheel is built turning in the x–z plane, so its axle runs along y.
 * Placed at bearing 6°: the aerial shows the wheel's plane running 96°/276°,
 * a few degrees off east–west, parallel to the station building beneath it.
 *
 * A 1920 eccentric wheel, 46 m tall. Its disc is two parallel faces of mint
 * lattice about 3 m apart, each with a rim, an inner ring and sixteen salmon
 * spokes, carried on an axle between two blue A-frame towers. Of its 24 cars,
 * eight white ones hang fixed from the rim; sixteen red and blue ones slide on
 * serpentine tracks between the inner ring and the rim, so a car near the top
 * has slid in toward the hub and one near the bottom has slid out to the rim.
 *
 * The lattice is far too fine to model (STYLE.md: no thin ribs), so each face
 * is reduced to broad members: a solid rim band, an inner ring, chunky spokes
 * and the wavy tracks. The cars hang in the gap between the two faces, the
 * towers stand outside them, so nothing passes through anything else.
 *
 * It replaces nothing: OSM maps the wheel as a node (attraction=big_wheel).
 * The 4.3 m station building it straddles (way/248496318) stays drawn, and
 * the bottom of the wheel dips into it, as the real one does at boarding.
 * The ground is flat here (2.6–2.7 m in the terrain tiles), so elevation 0.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

const structure = new Part()  // mint rim bands, inner rings, tracks, hub
const spokes = new Part()     // salmon spokes
const towers = new Part()     // blue towers, axle ends, and the blue cars
const red = new Part()
const white = new Part()
const yellow = new Part()     // door panels on the sliding cars

const TAU = Math.PI * 2
const H = 25                  // axle height: 46 m to the rim's top
const R_RIM = 20.1            // rim band centre line; 42 m across the outer edge
const R_INNER = 8.6           // inner ring
const FACE = 1.45             // each face's distance from the mid-plane
const FOOT = 8.5              // tower feet either side of the axle, from the photos

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A point on the wheel at radius r, angle θ (0 = east, π/2 = up), face y. */
const wheel = (r: number, t: number, y: number): V3 => [r * Math.cos(t), y, H + r * Math.sin(t)]

/** A triangle whose winding follows its normals, whatever order it came in. */
function tri(p: Part, v: V3[], n: V3[]) {
  const face = cross(sub(v[1], v[0]), sub(v[2], v[0]))
  if (dot(face, add(add(n[0], n[1]), n[2])) >= 0) p.tri(v[0], v[1], v[2], undefined, undefined, undefined, n)
  else p.tri(v[0], v[2], v[1], undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function fan(p: Part, ring: V3[], n: V3) {
  for (let i = 1; i < ring.length - 1; i++) tri(p, [ring[0], ring[i], ring[i + 1]], [n, n, n])
}

type Profile = [number, number][]
/** A chamfered rectangle, counter-clockwise: half-widths a (along N), b (along B). */
const rect = (a: number, b: number, c = 0): Profile => c
  ? [[-a + c, -b], [a - c, -b], [a, -b + c], [a, b - c], [a - c, b], [-a + c, b], [-a, b - c], [-a, -b + c]]
  : [[-a, -b], [a, -b], [a, b], [-a, b]]

/**
 * Sweep a cross-section along a path. Each section is a profile in the
 * frame (N, B) at its path point; facets are flat across the section and
 * smooth along the path, so a band reads as a rounded member catching light.
 */
function sweep(p: Part, path: V3[], frames: { N: V3; B: V3 }[], profiles: Profile[], closed = false, caps = false) {
  const m = profiles[0].length
  const at = (i: number, k: number) => {
    const [a, b] = profiles[i][k]
    return add(path[i], add(mul(frames[i].N, a), mul(frames[i].B, b)))
  }
  const normal = (i: number, k: number) => {
    const [a, b] = profiles[i][k], [c, d] = profiles[i][(k + 1) % m]
    return unit(add(mul(frames[i].N, d - b), mul(frames[i].B, -(c - a))))
  }
  const last = closed ? path.length : path.length - 1
  for (let i = 0; i < last; i++) {
    const j = (i + 1) % path.length
    for (let k = 0; k < m; k++) {
      const l = (k + 1) % m
      const ni = normal(i, k), nj = normal(j, k)
      tri(p, [at(i, k), at(i, l), at(j, l)], [ni, ni, nj])
      tri(p, [at(i, k), at(j, l), at(j, k)], [ni, nj, nj])
    }
  }
  if (caps && !closed) {
    const n = path.length - 1
    fan(p, profiles[0].map((_, k) => at(0, k)), unit(sub(path[0], path[1])))
    fan(p, profiles[n].map((_, k) => at(n, k)), unit(sub(path[n], path[n - 1])))
  }
}

/** Frames for a path lying in a face of the wheel: B across the disc (y). */
function inPlane(path: V3[], closed: boolean) {
  return path.map((_, i) => {
    const prev = path[closed ? (i + path.length - 1) % path.length : Math.max(0, i - 1)]
    const next = path[closed ? (i + 1) % path.length : Math.min(path.length - 1, i + 1)]
    const T = unit(sub(next, prev)), B: V3 = [0, 1, 0]
    return { N: unit(cross(T, B)), B }
  })
}

/** A straight member from a to b, tapering from section s0 to s1. */
function beam(p: Part, a: V3, b: V3, s0: Profile, s1: Profile, side: V3 = [0, 0, 1], caps = true) {
  const T = unit(sub(b, a))
  let N = cross(side, T)
  if (Math.hypot(...N) < 1e-6) N = cross([1, 0, 0], T)
  N = unit(N)
  const B = unit(cross(T, N))
  sweep(p, [a, b], [{ N, B }, { N, B }], [s0, s1], false, caps)
}

// --- The two faces: rim band, inner ring, sixteen spokes ---------------------
for (const y of [-FACE, FACE]) {
  const rim = Array.from({ length: 28 }, (_, i) => wheel(R_RIM, i / 28 * TAU, y))
  sweep(structure, rim, inPlane(rim, true), rim.map(() => rect(0.9, 0.3, 0.2)), true)
  const ring = Array.from({ length: 16 }, (_, i) => wheel(R_INNER, (i + 0.5) / 16 * TAU, y))
  sweep(structure, ring, inPlane(ring, true), ring.map(() => rect(0.42, 0.26)), true)
  for (let k = 0; k < 16; k++) {
    const t = (k + 0.5) / 16 * TAU
    beam(spokes, wheel(2.4, t, y), wheel(R_RIM - 0.6, t, y), rect(0.34, 0.22, 0.1), rect(0.3, 0.22, 0.1), [0, 1, 0], false)
  }
}

// --- Hub and axle -------------------------------------------------------------
const circle = (r: number, n: number): Profile => Array.from({ length: n }, (_, i) => [r * Math.cos(i / n * TAU), r * Math.sin(i / n * TAU)])
beam(structure, [0, -FACE - 0.5, H], [0, FACE + 0.5, H], circle(2.7, 16), circle(2.7, 16), [1, 0, 0])
beam(towers, [0, -3.6, H], [0, 3.6, H], circle(0.9, 12), circle(0.9, 12), [1, 0, 0])

// --- Cars ---------------------------------------------------------------------
/**
 * A car hanging from a pivot: a box with chamfered edges, 2.6 m across the
 * face of the wheel, 1.9 m deep and 2.4 m tall, so it fits between the faces.
 */
function car(p: Part, pivot: V3, door = false) {
  const [x, y, z] = pivot, w = 1.3, d = 0.95, h = 2.4, c = 0.35
  const ring = (zz: number, inset: number): V3[] =>
    rect(w - inset, d - inset, c - inset * 0.6).map(([a, b]) => [x + a, y + b, zz] as V3)
  const top = z - 0.15, bot = top - h
  const rings = [ring(bot, 0.22), ring(bot + c, 0), ring(top - c, 0), ring(top, 0.22)]
  p.loft(rings)
  p.cap(rings[3], true)
  p.cap(rings[0], false)
  if (!door) return
  // A yellow door panel set flush into each broad face, as on the real cars.
  for (const s of [-1, 1]) {
    const yy = y + s * (d + 0.02), x0 = x - 0.35, x1 = x + 0.55, z0 = bot + 0.45, z1 = top - 0.5
    const n: V3 = [0, s, 0]
    tri(yellow, [[x0, yy, z0], [x1, yy, z0], [x1, yy, z1]], [n, n, n])
    tri(yellow, [[x0, yy, z0], [x1, yy, z1], [x0, yy, z1]], [n, n, n])
  }
}

// Twenty-four cars at 15°: every third one fixed to the rim, the rest sliding.
const TRACK = (t: number, base: number) => {
  const r = R_INNER + 0.3 + (R_RIM - 1.2 - R_INNER) * t
  return { r, a: base + 0.11 * Math.sin(TAU * t) * (1 - 0.35 * t) }
}
let sliding = 0
for (let k = 0; k < 24; k++) {
  const base = -Math.PI / 2 + k / 24 * TAU
  if (k % 3 === 0) {
    // Pivoted a metre outside the rim's centre line, so a fixed car sits
    // astride the rim as in the photos, standing proud of it at the sides.
    car(white, wheel(R_RIM + 1, base, 0))
    continue
  }
  // The serpentine track this car slides on, one per face.
  for (const y of [-FACE + 0.3, FACE - 0.3]) {
    const path = Array.from({ length: 6 }, (_, i) => { const { r, a } = TRACK(i / 5, base); return wheel(r, a, y) })
    sweep(structure, path, inPlane(path, false), path.map(() => rect(0.45, 0.18)), false)
  }
  // Gravity slides a car out to the rim near the bottom and in near the top.
  const { r, a } = TRACK((1 - Math.sin(base)) / 2, base)
  car(sliding++ % 2 ? towers : red, wheel(r, a, 0), true)
}

// --- Towers: an A-frame on each side, legs splayed in and out of the plane ----
for (const s of [-1, 1]) {
  const apex = (x: number): V3 => [x, s * 3.1, H - 0.6]
  const foot = (x: number): V3 => [x, s * 6.6, 0.5]
  const at = (x: number, z: number): V3 => {
    const t = (z - 0.5) / (H - 1.1)
    return [x * (1 - t) + Math.sign(x) * 1.1 * t, s * (6.6 - 3.5 * t), z]
  }
  // Each leg is a broad lattice column in the real tower, so a wide member:
  // 2.2 m across the face at the foot, 1.6 m deep, tapering to the bearing.
  for (const x of [-FOOT, FOOT]) {
    beam(towers, foot(x), apex(Math.sign(x) * 1.1), rect(0.8, 1.1, 0.25), rect(0.5, 0.6, 0.15))
    // A footing pad, so the leg's slanted end never dips below the ground.
    beam(towers, [x, s * 6.6, 0], [x, s * 6.6, 0.9], rect(1.4, 1.2, 0.3), rect(1.3, 1.1, 0.3), [1, 0, 0])
  }
  for (const z of [6, 12.5, 18.5]) beam(towers, at(-FOOT, z), at(FOOT, z), rect(0.45, 0.5, 0.12), rect(0.45, 0.5, 0.12))
  // The bearing block where the legs meet the axle.
  beam(towers, [0, s * 2.4, H], [0, s * 3.8, H], rect(1.7, 1.5, 0.3), rect(1.7, 1.5, 0.3), [0, 0, 1])
}

const parts = [
  // Colours sampled from daylight photos (Commons, 2016 and 2023): pale mint
  // lattice, salmon spokes, the towers' bright blue, and the cars' red, blue,
  // white and yellow.
  { part: structure, material: { name: 'mint', color: 0x86c2a8, roughness: 0.6, doubleSided: true } },
  { part: spokes, material: { name: 'salmon', color: 0xe9a090, roughness: 0.6 } },
  { part: towers, material: { name: 'blue', color: 0x2f62a8, roughness: 0.6 } },
  { part: red, material: { name: 'car-red', color: 0xbc3a3e, roughness: 0.5 } },
  { part: white, material: { name: 'car-white', color: 0xece9e2, roughness: 0.5 } },
  { part: yellow, material: { name: 'car-yellow', color: 0xe8c848, roughness: 0.5 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Wonder Wheel', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', height: 46, bearing: 6,
})
await Bun.write(new URL('../../landmarks/models/wonder-wheel.glb', import.meta.url), glb)
console.log(`wonder-wheel.glb: ${triangles} triangles, ${glb.length} bytes`)
