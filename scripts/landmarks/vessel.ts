/**
 * Vessel, Hudson Yards. Procedural CC0 geometry in metres, x east/y north/z up.
 * bun scripts/landmarks/vessel.ts
 *
 * Sixteen levels of five staggered landings, each joined to the two landings
 * above and below by a straight flight, form the open honeycomb basket that
 * flares from a 15 m base to a 47 m rim. Every landing and flight is one
 * broad bevelled band: copper on its outer, inner and edge faces, dark bronze
 * underneath, a warm deck on top. No railings, treads or glass: at map
 * distance the see-through diamonds and the flaring silhouette are the
 * identity, and fine detail only aliases.
 */
import { Part, writeGlb, cross, sub, type V3 } from './mesh'

const copper = new Part()
const undersides = new Part()
const deck = new Part()
const TAU = Math.PI * 2
const LEVELS = 16
const BAYS = 5
const HEIGHT = 45.7
const FIRST = 1.1
const DEPTH = 2.8
const HALF_ANGLE = 0.125
// Deep enough to read as a copper stringer at phone sizes, shallow enough to
// keep the diamonds between flights wide open.
const THICKNESS = 1.6
const BEVEL = 0.45

const zAt = (level: number) => FIRST + (HEIGHT - FIRST) * level / (LEVELS - 1)
const radius = (level: number) => 7.5 + 16.1 * Math.pow(level / (LEVELS - 1), 0.7)
// Alternate levels turn by half a bay, so each landing sits over a diamond.
const angle = (level: number, bay: number) => bay * TAU / BAYS + (level % 2) * Math.PI / BAYS
// Lower landings are deeper where the real atrium is tight and the flare fast.
const depthAt = (level: number) => DEPTH + (level === 0 ? 0 : 0.85 * Math.max(0, 1 - Math.abs(level - 1) / 4))
// The ground row rests on the plaza: its band stops at z = 0, never below.
const thicknessAt = (level: number) => Math.min(THICKNESS, zAt(level))

const unit = (a: V3): V3 => { const l = Math.hypot(...a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/**
 * The band's cross-section at a landing edge: a chamfered rectangle in the
 * vertical radial plane at `a`. The outer face stays on the original radius
 * and the deck on the original level, so the envelope is unchanged.
 * Order: outer, chamfer, underside, chamfer, inner, chamfer, deck, chamfer.
 */
function section(level: number, a: number): V3[] {
  const r = radius(level), z = zAt(level), d = depthAt(level), t = thicknessAt(level)
  const b = Math.min(BEVEL, t * 0.3)
  const at = (u: number, v: number): V3 => [(r - u) * Math.cos(a), (r - u) * Math.sin(a), z - v]
  return [at(0, b), at(0, t - b), at(b, t), at(d - b, t), at(d, t - b), at(d, b), at(d - b, 0), at(b, 0)]
}

const MAIN: Part[] = [copper, undersides, copper, deck] // outer, underside, inner, deck
let bands = 0

/**
 * Loft one band between two sections. Main faces are flat-shaded; each
 * chamfer takes its neighbours' normals at its two edges, so it shades as a
 * rounded fillet and welds to the faces either side.
 */
function band(A: V3[], B: V3[]) {
  const centre = [...A, ...B].reduce(add, [0, 0, 0] as V3).map(v => v / 16) as V3
  const faces: { q: V3[]; n: V3 }[] = []
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    let q = [A[i], A[j], B[j], B[i]]
    let n = unit(add(cross(sub(q[1], q[0]), sub(q[2], q[0])), cross(sub(q[2], q[0]), sub(q[3], q[0]))))
    const m = q.reduce(add, [0, 0, 0] as V3).map(v => v / 4) as V3
    if (dot(n, sub(m, centre)) < 0) { q = [q[0], q[3], q[2], q[1]]; n = n.map(v => -v) as V3 }
    faces.push({ q, n })
  }
  faces.forEach(({ q, n }, i) => {
    if (i % 2 === 0) {
      const part = MAIN[i / 2]
      part.tri(q[0], q[1], q[2], undefined, undefined, undefined, [n, n, n])
      part.tri(q[0], q[2], q[3], undefined, undefined, undefined, [n, n, n])
      return
    }
    // A chamfer from section point i to i+1: point i belongs to face i-1,
    // point i+1 to face i+1. Map each corner of q back to its section index.
    const prev = faces[i - 1].n, next = faces[(i + 1) % 8].n
    const normalOf = (p: V3) => (p === A[i] || p === B[i]) ? prev : next
    const ns = q.map(normalOf)
    copper.tri(q[0], q[1], q[2], undefined, undefined, undefined, [ns[0], ns[1], ns[2]])
    copper.tri(q[0], q[2], q[3], undefined, undefined, undefined, [ns[0], ns[2], ns[3]])
  })
  bands++
}

// Landings: each spans its bay between two shared edge sections.
const left: V3[][][] = [], right: V3[][][] = []
for (let level = 0; level < LEVELS; level++) {
  left.push([]); right.push([])
  for (let bay = 0; bay < BAYS; bay++) {
    const a = angle(level, bay)
    left[level].push(section(level, a - HALF_ANGLE))
    right[level].push(section(level, a + HALF_ANGLE))
    band(left[level][bay], right[level][bay])
  }
}

// Flights: every landing climbs both ways to the staggered row above. The
// flights start and end on the landings' own sections, so the lattice is
// seamless and the diamonds between them are real openings.
for (let level = 0; level < LEVELS - 1; level++) {
  for (let bay = 0; bay < BAYS; bay++) {
    const up = (bay + (level % 2)) % BAYS
    band(right[level][bay], left[level + 1][up])
    band(left[level][bay], right[level + 1][(up + BAYS - 1) % BAYS])
  }
}

// sRGB from the sunlit cladding in the reference photos (Wikimedia Commons):
// the bright copper of the stringers, the brown shadowed soffits, and the
// darker warm walking surface seen from above.
const parts = [
  { part: copper, material: { name: 'copper-cladding', color: 0xb06a45, roughness: 0.45 } },
  { part: undersides, material: { name: 'bronze-undersides', color: 0x47302a, roughness: 0.6 } },
  { part: deck, material: { name: 'deck', color: 0x5c4135, roughness: 0.7 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5_000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Vessel', parts, {
  license: 'CC0-1.0',
  frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  height: HEIGHT, levels: LEVELS, landings: LEVELS * BAYS, flights: bands - LEVELS * BAYS,
  bearing: 0, elevation: 0, replaces: ['relation/16231018'],
})
if (glb.length > 250_000) throw new Error(`Landmark exceeds 250 KB: ${glb.length}`)
const out = new URL('../../landmarks/models/vessel.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes; ${bands} bands`)
