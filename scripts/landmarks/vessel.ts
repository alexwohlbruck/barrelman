/**
 * Vessel, Hudson Yards. Procedural CC0 geometry in metres, x east/y north/z up.
 * bun scripts/landmarks/vessel.ts
 *
 * Five staggered landings on sixteen levels form the open, flaring basket.
 * The stairs are structural geometry, including their copper undersides;
 * there is no cone hidden behind the lattice and no cap across the atrium.
 */
import { Part, writeGlb, type V3 } from './mesh'

const bronze = new Part()
const treads = new Part()
const stone = new Part()
const TAU = Math.PI * 2
const LEVELS = 16
const HEIGHT = 45.7
const FIRST = 1.1
const TOP = HEIGHT
const DEPTH = 2.8
const HALF_ANGLE = 0.125
// Broad copper edges survive the 80 px silhouette without detached rail lines.
const THICKNESS = 1.0
const polar = (r: number, a: number, z: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]
const up = (p: V3, z: number): V3 => [p[0], p[1], p[2] + z]
const zAt = (level: number) => FIRST + (TOP - FIRST) * level / (LEVELS - 1)
const radius = (level: number) => 7.5 + 16.1 * Math.pow(level / (LEVELS - 1), 0.7)
const angle = (level: number, bay: number) => bay * TAU / 5 + (level % 2) * Math.PI / 5

// A four-corner deck, CCW above, with outward copper sides and underside.
function deck(corners: V3[], thickness = THICKNESS) {
  const lower = corners.map(p => up(p, -thickness))
  bronze.loft([lower, corners])
  bronze.cap(lower, false)
  treads.cap(corners, true)
}

type Landing = { innerLeft: V3; outerLeft: V3; outerRight: V3; innerRight: V3 }
const landings: Landing[][] = []
for (let level = 0; level < LEVELS; level++) {
  const r = radius(level), z = zAt(level)
  // Broader lower landings follow the tight OSM atrium while the exterior
  // flares rapidly; upper stairs settle to a consistent walking width.
  const depth = DEPTH + (level === 0 ? 0 : 0.85 * Math.max(0, 1 - Math.abs(level - 1) / 4))
  landings.push([])
  for (let bay = 0; bay < 5; bay++) {
    const a = angle(level, bay)
    const innerLeft = polar(r - depth, a - HALF_ANGLE, z)
    const outerLeft = polar(r, a - HALF_ANGLE, z)
    const outerRight = polar(r, a + HALF_ANGLE, z)
    const innerRight = polar(r - depth, a + HALF_ANGLE, z)
    deck([innerLeft, outerLeft, outerRight, innerRight])
    landings[level].push({ innerLeft, outerLeft, outerRight, innerRight })
  }
}

let flights = 0
function flight(aInner: V3, aOuter: V3, bInner: V3, bOuter: V3, ccw: boolean, thickness = THICKNESS) {
  const corners = [aInner, aOuter, bOuter, bInner]
  // At phone sizes the flight is one broad band: individual treads and
  // detached handrails alias, while the real open diamonds carry its identity.
  deck(ccw ? corners : corners.reverse(), thickness)
  flights++
}

// Each landing splits in both directions to the next staggered row. The
// endpoints meet the landing edges, so the diagonal bands form real holes.
for (let level = 0; level < LEVELS - 1; level++) {
  for (let bay = 0; bay < 5; bay++) {
    const a = landings[level][bay]
    const right = landings[level + 1][(bay + (level % 2)) % 5]
    const left = landings[level + 1][(bay + (level % 2) + 4) % 5]
    flight(a.innerRight, a.outerRight, right.innerLeft, right.outerLeft, true)
    flight(a.innerLeft, a.outerLeft, left.innerRight, left.outerRight, false)
  }
}

// Four short entrance flights and modest shoes support the lower basket;
// the central ground opening remains unfilled.
for (let bay = 0; bay < 5; bay++) {
  const a = angle(0, bay), r = radius(0)
  const ring = (z: number): V3[] => [polar(r - DEPTH, a - HALF_ANGLE, z),
    polar(r, a - HALF_ANGLE, z), polar(r, a + HALF_ANGLE, z), polar(r - DEPTH, a + HALF_ANGLE, z)]
  stone.loft([ring(0), ring(FIRST - THICKNESS)])
  stone.cap(ring(0), false)
  if (bay < 4) {
    const landing = landings[0][bay]
    flight(polar(3.8, a - HALF_ANGLE, 0.25), polar(3.8, a + HALF_ANGLE, 0.25),
      landing.innerLeft, landing.innerRight, false, 0.25)
  }
}

// The shared writer converts these swatches to linear RGB. Parchment sends
// baseColorFactor directly to its sRGB framebuffer, so compensate here (not
// in the shared kit) to retain the intended pale, warm copper on the map.
const mapColor = (hex: number) => [16, 8, 0].reduce((value, shift) =>
  value | Math.round(Math.pow(((hex >> shift) & 255) / 255, 1 / 2.2) * 255) << shift, 0)

const parts = [
  { part: bronze, material: { name: 'bronze', color: mapColor(0xb98c6f), roughness: 0.48 } },
  { part: treads, material: { name: 'bronze-walkways', color: mapColor(0xc39b7e), roughness: 0.6 } },
  { part: stone, material: { name: 'stone', color: mapColor(0xc9b9a6) } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 12_000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Vessel', parts, {
  license: 'CC0-1.0',
  frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  height: HEIGHT, levels: LEVELS, landings: 80, flights,
  bearing: 0, elevation: 0, replaces: ['relation/16231018'],
})
if (glb.length > 300_000) throw new Error(`Landmark exceeds 300 KB: ${glb.length}`)
const out = new URL('../../landmarks/models/vessel.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes; ${flights} flights, 80 landings`)
