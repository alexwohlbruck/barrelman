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
const nosings = new Part()
const rails = new Part()
const stone = new Part()
const TAU = Math.PI * 2
const LEVELS = 16
const HEIGHT = 45.7
const RAIL = 1.05
const FIRST = 1.1
const TOP = HEIGHT - RAIL
const DEPTH = 2.8
const HALF_ANGLE = 0.125
const THICKNESS = 0.9
const polar = (r: number, a: number, z: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]
const mix = (a: V3, b: V3, t: number): V3 => a.map((v, i) => v + (b[i] - v) * t) as V3
const up = (p: V3, z: number): V3 => [p[0], p[1], p[2] + z]
const zAt = (level: number) => FIRST + (TOP - FIRST) * level / (LEVELS - 1)
const radius = (level: number) => 7.5 + 16.1 * Math.pow(level / (LEVELS - 1), 0.7)
const angle = (level: number, bay: number) => bay * TAU / 5 + (level % 2) * Math.PI / 5

// A four-corner deck, CCW above, with outward copper sides and underside.
function deck(corners: V3[], thickness = THICKNESS, walkingSurface = true) {
  const lower = corners.map(p => up(p, -thickness))
  bronze.loft([lower, corners])
  bronze.cap(lower, false)
  if (walkingSurface) treads.cap(corners, true)
}

// Narrow rectangular beams keep handrails legible without opaque glazing.
function beam(a: V3, b: V3, width: number, height: number) {
  const dx = b[0] - a[0], dy = b[1] - a[1]
  const d = Math.hypot(dx, dy)
  const side: V3 = [-dy / d * width / 2, dx / d * width / 2, 0]
  const section = (p: V3): V3[] => [
    [p[0] - side[0], p[1] - side[1], p[2] - height / 2],
    [p[0] + side[0], p[1] + side[1], p[2] - height / 2],
    [p[0] + side[0], p[1] + side[1], p[2] + height / 2],
    [p[0] - side[0], p[1] - side[1], p[2] + height / 2],
  ]
  const s = section(a), e = section(b)
  for (let k = 0; k < 4; k++) rails.quad(s[k], s[(k + 1) % 4], e[(k + 1) % 4], e[k])
  // Ends meet adjacent rails; omit their hidden caps to spend faces on stairs.
}

function railing(a: V3, b: V3) {
  beam(up(a, RAIL - 0.06), up(b, RAIL - 0.06), 0.12, 0.12)
  const c = mix(a, b, 0.5)
  const d = Math.hypot(b[0] - a[0], b[1] - a[1])
  const dx = (b[0] - a[0]) / d * 0.06, dy = (b[1] - a[1]) / d * 0.06
  const l: V3 = [c[0] - dx, c[1] - dy, c[2]]
  const r: V3 = [c[0] + dx, c[1] + dy, c[2]]
  rails.quad(l, r, up(r, RAIL), up(l, RAIL))
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
    railing(outerLeft, outerRight)
    railing(innerLeft, innerRight)
    landings[level].push({ innerLeft, outerLeft, outerRight, innerRight })
  }
}

let flights = 0
function flight(aInner: V3, aOuter: V3, bInner: V3, bOuter: V3, ccw: boolean, thickness = THICKNESS) {
  const corners = [aInner, aOuter, bOuter, bInner]
  deck(ccw ? corners : corners.reverse(), thickness, false)
  // Six deliberately broad tread groups read at map scale. The inclined
  // copper edge is a stringer, enclosing the tread ends below its upper lip.
  const steps = 6
  for (let step = 0; step < steps; step++) {
    const a = mix(aInner, bInner, step / steps)
    const b = mix(aOuter, bOuter, step / steps)
    const c = mix(aOuter, bOuter, (step + 1) / steps)
    const d = mix(aInner, bInner, (step + 1) / steps)
    const cLow: V3 = [c[0], c[1], a[2]], dLow: V3 = [d[0], d[1], a[2]]
    const top = [a, b, cLow, dLow], riser = [dLow, cLow, c, d]
    treads.cap(ccw ? top : top.reverse(), true)
    treads.cap(ccw ? riser : riser.reverse(), true)
    if (step % 2 === 0) {
      // A few broad nosings identify the stairs even in the top-down view.
      const edge = [mix(dLow, a, 0.12), mix(cLow, b, 0.12), cLow, dLow].map(p => up(p, 0.008))
      nosings.cap(ccw ? edge : edge.reverse(), true)
    }
  }
  railing(aInner, bInner)
  railing(aOuter, bOuter)
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

const parts = [
  { part: bronze, material: { name: 'bronze', color: 0xb98158, roughness: 0.48 } },
  { part: treads, material: { name: 'steel-stairs', color: 0x746a60 } },
  { part: nosings, material: { name: 'steel-nosings', color: 0xac9b85 } },
  { part: rails, material: { name: 'bronze-handrails', color: 0xd0a17b, roughness: 0.5, doubleSided: true } },
  { part: stone, material: { name: 'stone', color: 0xb5a89a } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 12_000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Vessel', parts, {
  license: 'CC0-1.0',
  frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  height: HEIGHT, levels: LEVELS, landings: 80, flights,
  bearing: 0, elevation: 0, replaces: ['relation/16231018'],
})
const out = new URL('../../landmarks/models/vessel.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes; ${flights} flights, 80 landings`)
