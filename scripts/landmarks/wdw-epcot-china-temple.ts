/**
 * EPCOT China pavilion: the Temple of Heaven — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-china-temple.ts
 *
 * Map frame: x east, y north, z up, metres; origin on the ground at the
 * centre of the round OSM building (way/383670241, a 19-point circle of
 * radius 7.5–8.6 m, mean 8.0 m, untagged beyond building=yes). The model is
 * built with its door facing +y; the catalog turns it to 286°, the bearing
 * of OSM's entrance node/4852835323 from the centre, which matches the
 * photos: the walkway from the courtyard runs straight at the door, with the
 * rest of the pavilion (way/295597521) joined on the hall's left.
 *
 * A half-scale replica of the Hall of Prayer for Good Harvests, housing the
 * Reflections of China theatre. A round red hall on a white marble terrace,
 * under three diminishing tiers of blue glazed roof, each with a painted
 * blue-green frieze drum beneath it, a gold-framed name tablet on the top
 * drum and a gold ball finial.
 *
 * Evidence:
 * - Measured: the footprint radius (OSM) and every height and radius below,
 *   from two near-level photos scaled to that radius (Wikimedia Commons:
 *   "China showcase at Epcot.jpg", Nixinova, public domain, from the
 *   courtyard; "China pavilion at Epcot.jpg", Benjamin D. Esham,
 *   CC BY-SA 4.0, from the pond side). The lowest eave spans about the
 *   building's own height in both.
 * - Published: half scale of the Beijing hall (38 m tall, ~32 m across the
 *   body), which agrees with the ~18 m total and 8.6 m eave radius measured.
 * - Estimated: the terrace's three steps (the photos show one broad white
 *   platform with balustrades; the steps stand in for that and for the real
 *   hall's three-tier marble base). The balustrades themselves are left out
 *   as railings.
 * - Invented: nothing structural. The plaque is a plain gold-edged panel.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const tiles = new Part()    // the blue glazed roofs and the finial's neck
const red = new Part()      // the hall's red walls and the eave edges
const frieze = new Part()   // painted blue-green beam drums under each roof
const gold = new Part()     // finial ball and the tablet frame
const marble = new Part()   // white terrace
const door = new Part()     // the dark doorway

type RZ = [number, number]
const SEG = 24
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle whose winding follows its normals. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const f = cross(sub(b, a), sub(c, a))
  if (len(f) < 1e-9) return
  const avg: V3 = [n[0][0] + n[1][0] + n[2][0], n[0][1] + n[1][1] + n[2][1], n[0][2] + n[1][2] + n[2][2]]
  if (dot(f, avg) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}

/**
 * A surface of revolution about the z axis, from a profile listed bottom to
 * top along the outside. Smooth around; along the profile smooth too when
 * `smooth`, which is what makes a curved roof read as one sweep, otherwise
 * each band is flat-shaded so a step keeps its edge.
 */
function lathe(p: Part, prof: RZ[], smooth = false, seg = SEG) {
  const segN = prof.slice(0, -1).map(([r0, z0], i) => {
    const [r1, z1] = prof[i + 1]
    const l = Math.hypot(z1 - z0, r1 - r0) || 1
    return [(z1 - z0) / l, -(r1 - r0) / l] as RZ
  })
  const at = (i: number, side: 0 | 1): RZ => {
    if (!smooth) return segN[side ? i - 1 : i]
    const a = segN[Math.max(0, i - 1)], b = segN[Math.min(segN.length - 1, i)]
    const l = Math.hypot(a[0] + b[0], a[1] + b[1]) || 1
    return [(a[0] + b[0]) / l, (a[1] + b[1]) / l]
  }
  for (let k = 0; k < seg; k++) {
    const t0 = (k / seg) * 2 * Math.PI, t1 = ((k + 1) / seg) * 2 * Math.PI
    const P = ([r, z]: RZ, t: number): V3 => [r * Math.cos(t), r * Math.sin(t), z]
    const N = ([nr, nz]: RZ, t: number): V3 => unit([nr * Math.cos(t), nr * Math.sin(t), nz])
    for (let i = 0; i < prof.length - 1; i++) {
      const na = at(i, 0), nb = smooth ? at(i + 1, 0) : at(i + 1, 1)
      const a = P(prof[i], t0), b = P(prof[i], t1), c = P(prof[i + 1], t1), d = P(prof[i + 1], t0)
      const n = [N(na, t0), N(na, t1), N(nb, t1), N(nb, t0)]
      tri(p, a, b, c, [n[0], n[1], n[2]])
      tri(p, a, c, d, [n[0], n[2], n[3]])
    }
  }
}

/** A concave roof sweep from the eave (r0, z0) up to (r1, z1). */
function roofCurve(r0: number, z0: number, r1: number, z1: number, steps = 5, sag = 0.55): RZ[] {
  // Glazed Chinese roofs dip in the middle: the run is quick near the eave
  // and steepens towards the drum above, the curve of a quadratic.
  const out: RZ[] = []
  for (let s = 0; s <= steps; s++) {
    const t = s / steps
    out.push([r0 + (r1 - r0) * t, z0 + (z1 - z0) * (t * (1 - sag) + t * t * sag)])
  }
  return out
}

// ---------------------------------------------------------------------------
// Terrace: three white marble steps.
lathe(marble, [[8.5, 0], [8.5, 0.4], [8.05, 0.4], [8.05, 0.8], [7.6, 0.8], [7.6, 1.2], [6.8, 1.2]])

// The flight of steps up to the door, on the +y side (photo from the
// courtyard): two flat blocks stepping down off the terrace.
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number) {
  const c: V3[] = [[x0, y0, z0], [x1, y0, z0], [x1, y1, z0], [x0, y1, z0]]
  const t: V3[] = c.map(([x, y]) => [x, y, z1])
  p.loft([c, t])
  p.cap(t, true)
}
box(marble, -1.5, 1.5, 7.4, 9.0, 0, 0.8)
box(marble, -1.5, 1.5, 9.0, 9.6, 0, 0.4)

// The hall: red walls, then the frieze swelling out into the eave brackets.
const BODY = 6.8
lathe(red, [[BODY, 1.2], [BODY, 4.4]])
lathe(frieze, [[BODY, 4.4], [BODY + 0.05, 5.3], [7.6, 5.7]])

/**
 * One roof tier: a soffit from the frieze out to the eave, a red edge band,
 * the blue sweep up to the next drum.
 */
function tier(soffitR: number, soffitZ: number, eaveR: number, eaveZ: number, topR: number, topZ: number, sag = 0.55) {
  lathe(frieze, [[soffitR, soffitZ], [eaveR, eaveZ]])
  lathe(red, [[eaveR, eaveZ], [eaveR, eaveZ + 0.28]])
  lathe(tiles, roofCurve(eaveR, eaveZ + 0.28, topR, topZ, 6, sag), true)
}
tier(7.6, 5.7, 8.6, 5.95, 5.6, 7.65, 0.7)
// Second drum and tier.
lathe(frieze, [[5.6, 7.65], [5.6, 8.35], [6.1, 8.6]])
tier(6.1, 8.6, 7.1, 8.8, 3.85, 11.0, 0.7)
// Top drum: the tallest frieze, carrying the name tablet.
lathe(frieze, [[3.85, 11.0], [3.85, 12.55], [4.4, 12.85]])
tier(4.4, 12.85, 5.35, 13.05, 0.7, 16.65, 0.6)
// Finial: a blue neck, a gold collar, the gold ball.
lathe(tiles, [[0.7, 16.65], [0.62, 17.1], [0.3, 17.15]])
lathe(gold, [[0.3, 17.15], [0.28, 17.35], [0.18, 17.38]])
{
  const R = 0.5, C = 17.82, prof: RZ[] = []
  for (let j = 0; j <= 8; j++) {
    const a = -Math.PI / 2 + (j / 8) * Math.PI
    prof.push([Math.max(1e-4, R * Math.cos(a)), C + R * Math.sin(a)])
  }
  prof[0][0] = 0.001
  prof[prof.length - 1][0] = 0
  lathe(gold, prof, true, 16)
}

// ---------------------------------------------------------------------------
// Flush details on the +y side: the doorway on the red wall, and the name
// tablet on the top drum. Each is a slightly curved panel following its drum.

/** A curved panel on a cylinder of radius r, centred on +y, `half` radians wide. */
function panel(p: Part, r: number, half: number, z0: number, z1: number, steps = 3) {
  for (let s = 0; s < steps; s++) {
    const a0 = Math.PI / 2 - half + (2 * half * s) / steps, a1 = Math.PI / 2 - half + (2 * half * (s + 1)) / steps
    const P = (a: number, z: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]
    const n0: V3 = [Math.cos(a0), Math.sin(a0), 0], n1: V3 = [Math.cos(a1), Math.sin(a1), 0]
    tri(p, P(a0, z0), P(a1, z0), P(a1, z1), [n0, n1, n1])
    tri(p, P(a0, z0), P(a1, z1), P(a0, z1), [n0, n1, n0])
  }
}
// The doorway: a dark opening about 2.4 m wide, as in the photos. It stays
// unlit at night: the palette's `entrance` is pale, and a pale door read as a
// sign board, not an opening.
panel(door, BODY + 0.03, 1.2 / BODY, 1.2, 3.9)
// The tablet: a gold frame with a blue face, standing just proud of the drum.
panel(gold, 3.85 + 0.05, 0.75 / 3.85, 11.15, 12.75)
panel(tiles, 3.85 + 0.1, 0.5 / 3.85, 11.35, 12.55)

// ---------------------------------------------------------------------------
// The palette (landmarks/STYLE.md): the colours are this building's identity
// — cobalt roofs, vermilion walls, a blue-green painted frieze, gold — so each
// is a finish read off the photos and pulled towards the palette's lightness.
// The terrace is the palette's own pale `trim`; the doorway a dark finish.
const parts = [
  { part: tiles, material: finish('temple-blue', 0x4f68a6) },
  { part: red, material: finish('temple-red', 0xb9533f) },
  { part: frieze, material: finish('temple-frieze', 0x5f8f8a) },
  { part: gold, material: finish('temple-gold', 0xd2aa4c, 0.5) },
  { part: marble, material: PALETTE.trim },
  { part: door, material: finish('temple-door', 0x5e4339) },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT China pavilion, Temple of Heaven', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 286, osm: 'way/383670241', height: 18.32,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/wdw-epcot-china-temple.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
