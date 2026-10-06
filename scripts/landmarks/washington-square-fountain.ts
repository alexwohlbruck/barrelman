/**
 * Washington Square Fountain (Tisch Fountain) — procedural, CC0-1.0.
 * bun scripts/landmarks/washington-square-fountain.ts
 *
 * Map frame: x east, y north, z up, metres; origin at the centre of the basin.
 * Placed at bearing 32°, so +y points up the park's axis at the arch, which the
 * 2009 rebuild aligned the fountain with.
 *
 * The OSM water polygon (way/959929617, shared with the retaining wall
 * way/1232773488) is a circle of radius 11.73 m, so the outer foot of the rim
 * is exactly that. Inside: a low moulded granite rim with a broad seat, two
 * seating steps down to a flat muted-blue pool, a stone drum at the centre
 * carrying the tall jet, and eight square pedestals around the rim, as in the
 * photos. Everything is a solid of revolution or repeated by rotation, so the
 * model is exactly round and nothing passes the polygon's edge.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

const rim = new Part()        // outer wall and seating steps
const cap = new Part()        // the seat on top of the rim
const pedestals = new Part()
const drum = new Part()
const water = new Part()
const jet = new Part()

const SEG = 28                // around the basin; it is seen large from above
const R = 11.73               // OSM polygon radius: the rim's outer foot
const TOP = 0.86              // rim seat height above the plaza
const WATER = 0.2             // pool surface

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A quad with per-corner normals; winding follows the normals. */
function q(p: Part, P: V3[], ns: V3[]) {
  const N = ns.map(unit)
  const face = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = N.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(face, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [N[a], N[b], N[c]])
  if (Math.hypot(...sub(P[a], P[d])) > 1e-6 && Math.hypot(...sub(P[c], P[d])) > 1e-6)
    p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [N[a], N[c], N[d]])
}
function fan(p: Part, ring: V3[], n: V3) {
  for (let i = 1; i < ring.length - 1; i++) {
    const ok = dot(cross(sub(ring[i], ring[0]), sub(ring[i + 1], ring[0])), n) >= 0
    p.tri(ring[0], ok ? ring[i] : ring[i + 1], ok ? ring[i + 1] : ring[i], undefined, undefined, undefined, [n, n, n])
  }
}

/**
 * Revolve a profile of (radius, height, normal-radial, normal-up) points.
 * Consecutive points form a band; repeat a point with a new normal for a hard
 * edge (the zero-length band between them is skipped).
 */
type Pt = [number, number, number, number]
function revolve(p: Part, prof: Pt[], seg = SEG, cx = 0, cy = 0) {
  for (let i = 0; i < seg; i++) {
    const a = i * 2 * Math.PI / seg, b = (i + 1) * 2 * Math.PI / seg
    const at = ([r, z]: Pt, t: number): V3 => [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
    const nAt = ([, , nr, nz]: Pt, t: number): V3 => [nr * Math.cos(t), nr * Math.sin(t), nz]
    for (let k = 0; k < prof.length - 1; k++) {
      const u = prof[k], v = prof[k + 1]
      if (u[0] === v[0] && u[1] === v[1]) continue
      q(p, [at(u, a), at(u, b), at(v, b), at(v, a)], [nAt(u, a), nAt(u, b), nAt(v, b), nAt(v, a)])
    }
  }
}
const disc = (p: Part, r: number, z: number, seg = SEG) =>
  fan(p, Array.from({ length: seg }, (_, i): V3 => [r * Math.cos(i * 2 * Math.PI / seg), r * Math.sin(i * 2 * Math.PI / seg), z]), [0, 0, 1])

// Outer wall: a plinth at the polygon's edge, a chamfer back to the wall face,
// and a rounded nosing up to the seat.
const S = Math.SQRT1_2
revolve(rim, [
  [R, 0, 1, 0], [R, 0.2, 1, 0],
  [R, 0.2, .5, .87], [R - .15, 0.29, .5, .87],
  [R - .15, 0.29, 1, 0], [R - .15, TOP - .14, 1, 0], [R - .26, TOP, S, S],
])
// The seat: broad and flat, what a phone sees as the ring.
revolve(cap, [[R - .26, TOP, S, S], [R - .45, TOP, 0, 1], [10.92, TOP, 0, 1], [10.78, TOP - .1, -S, S]])
// Three seating steps down to the pool (photos 02 and 07), each a broad tread
// with a softened nosing; the lowest runs into the water.
const steps: Pt[] = [[10.78, TOP - .1, -S, S]]
for (const [r0, r1, z] of [[10.75, 10.15, .66], [10.15, 9.55, .47], [9.55, 8.95, .3]]) {
  // Riser, a hard edge, then the tread; its outer normal tilts so the nosing
  // catches a highlight without spending a band on a chamfer.
  steps.push([r0, z, -1, 0], [r0, z, 0, 1], [r1, z, -S, S])
}
steps.push([8.95, WATER - .05, -1, 0])
revolve(rim, steps)
disc(water, 8.97, WATER)

// The central drum the jets rise from: a low granite cylinder with a rounded top.
revolve(drum, [[2.0, WATER - .05, 1, 0], [2.0, 0.5, 1, 0], [1.88, 0.62, S, S], [0, 0.62, 0, 1]], 16)

// The jet: one smooth revolved form, the tall tapering column rising out of a
// broad foaming skirt where the ring of plumes around it falls back (photos
// 01 and 04). One soft shape reads better from above than a cluster of spikes.
{
  const rings: [number, number][] = [
    [1.3, .62], [1.2, 1.1], [.9, 1.7], [.68, 2.5], [.6, 3.4], [.55, 5.5], [.44, 7.3], [.3, 8.2], [.14, 8.6], [0, 8.75],
  ]
  const prof: Pt[] = rings.map(([r, z], i) => {
    const [r0, z0] = rings[Math.max(i - 1, 0)], [r1, z1] = rings[Math.min(i + 1, rings.length - 1)]
    const n = unit([z1 - z0, 0, -(r1 - r0)])
    return [r, z, n[0], n[2]]
  })
  revolve(jet, prof, 14)
}

// Eight square pedestals on the rim, one on each axis and diagonal: a plain
// die and a broader bevelled cap with a low hipped top.
function pedestal(t: number) {
  const c = Math.cos(t), s = Math.sin(t)
  const toWorld = ([u, v, z]: V3): V3 => [u * c - v * s, u * s + v * c, z]
  const nW = ([u, v, z]: V3): V3 => [u * c - v * s, u * s + v * c, z]
  const U = 10.95
  const box = (h: number, r: number) => (z: number) => [
    [U - h + r, -h, z], [U + h - r, -h, z], [U + h, -h + r, z], [U + h, h - r, z],
    [U + h - r, h, z], [U - h + r, h, z], [U - h, h - r, z], [U - h, -h + r, z],
  ] as V3[]
  const N8: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  const band = (a: V3[], b: V3[], na: V3[], nb: V3[]) => {
    for (let i = 0; i < 8; i++) {
      const j = (i + 1) % 8
      q(pedestals, [a[i], a[j], b[j], b[i]].map(toWorld), [na[i], na[j], nb[j], nb[i]].map(nW))
    }
  }
  const tilt = (k: number): V3[] => N8.map(n => unit([n[0], n[1], k]))
  // The die is plain: its corners hide under the cap at map distance.
  const h = .6, dz0 = 0.5, dz1 = 1.3
  for (const [n, a, b] of [[[0, -1, 0], [U - h, -h], [U + h, -h]], [[1, 0, 0], [U + h, -h], [U + h, h]],
    [[0, 1, 0], [U + h, h], [U - h, h]], [[-1, 0, 0], [U - h, h], [U - h, -h]]] as [V3, number[], number[]][])
    q(pedestals, ([[a[0], a[1], dz0], [b[0], b[1], dz0], [b[0], b[1], dz1], [a[0], a[1], dz1]] as V3[]).map(toWorld), [n, n, n, n].map(nW))
  const cp = box(.7, .14)
  band(cp(1.3), cp(1.62), N8, N8)
  band(cp(1.62), box(.5, .1)(1.82), tilt(1.2), tilt(1.2))
  fan(pedestals, box(.5, .1)(1.82).map(toWorld), [0, 0, 1])
  fan(pedestals, cp(1.3).map(toWorld), [0, 0, -1])
}
for (let k = 0; k < 8; k++) pedestal(Math.PI / 2 + k * Math.PI / 4)

// Colours from the photos: the rim and steps are a dark warm-grey granite
// (photo 01's sunlit rim, photo 07 from above), the seat a shade lighter where
// it is worn, the pedestals a browner stone, the pool a muted blue.
const parts = [
  { part: rim, material: { name: 'granite', color: 0x7b7a73 } },
  { part: cap, material: { name: 'granite-seat', color: 0x8d8c85 } },
  { part: pedestals, material: { name: 'pedestal-stone', color: 0x8c8577 } },
  { part: drum, material: { name: 'drum', color: 0x85847e } },
  { part: water, material: { name: 'water', color: 0x7fa2b6 } },
  { part: jet, material: { name: 'jet', color: 0xe6eef2 } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 2000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Washington Square Fountain', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 32, elevation: 0, diameter: 2 * R, rimHeight: TOP, jetHeight: 9,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/washington-square-fountain.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
