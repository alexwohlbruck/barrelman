/**
 * Test Track, EPCOT (Walt Disney World): procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-test-track.ts   (REPORT=1 for the track table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the centre of the round show building (the area
 * centroid of OSM way/295949487), on the ground (EPCOT is flat).
 *
 * What it is. The old World of Motion drum, about 98 m across, with Test
 * Track's outdoor run around and beyond it: out of the building's north-east
 * side along a straight over the service road, a 90° right turn and a 270°
 * left turn round the "boot" over the cast car park, back along the straight,
 * then a full counter-clockwise lap of the drum on the 50° banked curve, and
 * back in. The drum is clad in dark charcoal panels (reflective, they read near-black from above) over a recessed
 * ground storey, with a low white dome of a roof. At the north-west entrance a
 * low vestibule sits under the 2025 canopy.
 *
 * Track. The outdoor track is one OSM way (way/721055302,
 * `roller_coaster=track`, `slot_car=track`, 793 m), drawn with
 * ./coaster-kit.ts as an open run (`shuttle`), from the doors where it leaves
 * the drum to the doors where it goes back in. The kit's output is read back
 * and merged with the building here, keeping its deck, beam and supports and
 * dropping its station canopy (the station is indoors). The cars are powered,
 * so the kit's energy walk is used only to set the speeds that bank the
 * track: about 50 km/h out to the boot and round it, then the speed run at
 * the published 104 km/h back along the straight and round the drum, where the
 * banking is set by hand to the published 50°.
 *
 * Evidence.
 *   Measured: the drum (OSM, a circle of 48.8 m radius, checked on USGS NAIP,
 *     public domain); the track's plan (OSM, which NAIP and the 2006 aerial
 *     plaque agree with: a lane each way along the straight, 4 m apart); the
 *     vestibule and canopy outlines (OSM way/295315993, way/1539425232).
 *   Published: top speed 65 mph (104.6 km/h) on a 50° banked curve, the lap of
 *     the building (Wikipedia, Disney).
 *   Estimated: the track's height (6.5 m everywhere, from the photos, where the
 *     shelf runs over the 3 m doors with room to spare); the drum's height
 *     (19 m to the eaves, the dome rising to 22 m), from the Jurvetson
 *     aerial and the entrance photos; the canopy (6.5 to 8.5 m).
 *   Invented: the boot's speed (no published figure), and so its banking.
 *
 * Photos (Wikimedia Commons): "Side of Test Track" (Michael Rivera,
 * CC BY-SA 4.0); "Test Track, EPCOT (2024)" (Jedi94, CC BY-SA 4.0); "Test
 * Track 3.0 Marquee 2025" (ARC Adventures, CC BY 3.0); "Aerial photo plaque
 * of the Mission SPACE pavilion next door, Test Track, June 2006" (ThrillZing,
 * CC BY 4.0); "EPCOT from above" (Steve Jurvetson, CC BY 2.0). Plan: OSM and
 * USGS NAIP (public domain).
 */
import { tmpdir } from 'node:os'
import { Part, addGltfTriangles, readGlb, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'
import { buildCoaster } from './coaster-kit'

type XY = [number, number]
const ANCHOR = { lon: -81.54721781, lat: 28.37284435 }

// way/721055302, node by node in the direction of travel, metres from the
// drum's centre: out of the doors on the north-east side, along the straight,
// round the boot, back along the straight, and the lap of the drum.
const CHAIN: XY[] = [
  [34.2, 34.9], [38.1, 34.2], [39.9, 33.6], [43.1, 32.4], [45.7, 31.0], [125.7, -10.7], [128.8, -12.3], [131.7, -13.9], [133.9, -15.2],
  [136.1, -16.6], [138.0, -18.2], [139.6, -20.0], [141.0, -22.0], [142.3, -24.4], [143.1, -26.8], [143.7, -29.4], [143.8, -32.1], [143.5, -34.6],
  [142.8, -37.4], [141.8, -40.0], [140.6, -42.4], [130.0, -60.0], [128.6, -62.4], [127.4, -65.0], [126.5, -67.5], [126.0, -70.1], [125.7, -72.9],
  [125.9, -75.4], [126.2, -78.1], [126.9, -80.5], [127.9, -83.1], [129.2, -85.6], [130.7, -87.9], [132.5, -90.0], [134.6, -92.0], [136.7, -93.7],
  [139.0, -95.3], [141.6, -96.7], [144.0, -97.6], [146.7, -98.4], [149.2, -99.1], [151.8, -99.5], [154.7, -99.7], [157.7, -99.5], [160.8, -99.2],
  [163.4, -98.6], [166.1, -97.7], [168.6, -96.8], [171.1, -95.5], [173.4, -94.1], [175.7, -92.5], [177.8, -90.6], [179.7, -88.7], [181.4, -86.6],
  [183.0, -84.4], [184.5, -82.0], [185.8, -79.6], [186.9, -76.8], [187.9, -74.1], [188.6, -71.3], [189.0, -68.3], [189.3, -65.4], [189.3, -62.5],
  [189.2, -59.6], [188.7, -56.7], [188.1, -54.0], [187.2, -51.2], [186.2, -48.4], [185.0, -45.8], [183.6, -43.1], [182.2, -41.0], [180.5, -38.8],
  [178.7, -36.8], [176.6, -34.7], [174.5, -32.9], [172.3, -31.2], [168.5, -28.6], [164.9, -26.5], [159.1, -23.3], [38.9, 39.4], [28.5, 44.7],
  [23.8, 46.8], [18.5, 48.9], [10.0, 51.3], [2.3, 52.2], [-6.2, 51.9], [-14.1, 50.3], [-22.4, 47.3], [-29.7, 43.1], [-36.3, 37.8], [-41.8, 31.7],
  [-46.3, 24.8], [-49.6, 17.2], [-51.8, 9.1], [-52.6, 1.2], [-52.1, -7.2], [-50.2, -15.7], [-47.2, -23.1], [-43.0, -30.0], [-37.9, -36.3], [-31.4, -41.9],
  [-24.6, -46.2], [-17.3, -49.3], [-9.6, -51.4], [-1.4, -52.2], [6.5, -51.8], [14.4, -50.2], [21.7, -47.6], [29.2, -43.4], [35.8, -38.3], [41.0, -32.7],
  [45.4, -26.3], [49.1, -18.5], [51.5, -10.5], [52.5, -2.8], [52.3, 5.3], [50.7, 13.7], [47.8, 21.7], [44.5, 26.3], [40.8, 29.9], [36.1, 32.8],
]
const TRACK_Z = 6.5
// Polyline distances (m along CHAIN): the boot runs from 98 to 335, the
// return straight to 466 and the lap of the drum from there to the end (793);
// the hand banking holds 50° over the lap, easing in and out over 30 m.
const LAP: [number, number] = [480, 755]

// ------------------------------------------------------------- track ----
// The kit writes its own GLB; it goes to a scratch file and is read back.
const scratch = `${tmpdir()}/wdw-epcot-test-track-kit.glb`
await buildCoaster({
  name: 'Test Track',
  out: scratch,
  chain: CHAIN,
  anchor: [0, 0],
  ground: { x0: -300, y0: -300, step: 600, rows: [[0, 0], [0, 0]], base: 30 },
  profile: [
    { name: 'out of the doors', s: 0, z: TRACK_Z },
    { name: 'back in', s: 790, z: TRACK_Z },
  ],
  // Powered cars: the "lift" is the doorway, a launch sets the cruising
  // speed out to the boot (about 50 km/h) and a second the speed run home.
  lift: [0, 2],
  launches: [
    { from: 2, to: 20, z: TRACK_Z, head: 10, name: 'out to the boot' },
    { from: 340, to: 460, z: TRACK_Z, head: 43, name: 'speed run, 104 km/h' },
  ],
  brakes: [{ from: 785, to: 792, z: TRACK_Z, head: 2 }],
  banks: [
    { from: LAP[0], to: LAP[1], deg: 50, name: 'the banked lap of the drum' },
    { from: 100, to: 140, deg: 20, name: 'the 90° right turn into the boot' },
    // Level through both doorways.
    { from: 0, to: 8, deg: 0, name: 'out of the doors' },
    { from: 782, to: 792, deg: 0, name: 'back in' },
  ],
  losses: [0.001, 0],
  shuttle: true,
  track: { W: 3.8, RAIL: 1.1, DEPTH: 2.2, SPINE: 3.0 },
  station: { ring: [[-2, -2], [2, -2], [0, 2]], posts: [], roofZ: 1 },
  colours: { deck: ['test-track-deck', 0xbabfc3], spine: ['test-track-beam', 0x9ba3aa], supports: 'trim' },
  supportR: [0.45, 0.55, 0.65],
  supportEvery: 14,
  meta: { height: 22, trackLength: 793 },
})

const deck = new Part(), beam = new Part(), white = new Part()
const KEEP: Record<string, Part> = { 'test-track-deck': deck, 'test-track-beam': beam, trim: white }
for (const prim of readGlb(new Uint8Array(await Bun.file(scratch).arrayBuffer()))) {
  const part = KEEP[prim.material.name]
  if (part) addGltfTriangles(part, prim.position, prim.index, { creaseDegrees: 30 })
}

// ---------------------------------------------------------- helpers ----
const signedArea = (p: XY[]) => p.reduce((s, a, i) => s + a[0] * p[(i + 1) % p.length][1] - p[(i + 1) % p.length][0] * a[1], 0) / 2
const ccw = (p: XY[]) => (signedArea(p) < 0 ? [...p].reverse() : p)
function earClip(poly: XY[]): [XY, XY, XY][] {
  const p = ccw(poly), idx = p.map((_, i) => i), out: [XY, XY, XY][] = []
  const cr = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const A = p[ia], B = p[ib], C = p[ic]
      if (cr(A, B, C) <= 1e-9) continue
      if (idx.some((j) => j !== ia && j !== ib && j !== ic && cr(A, B, p[j]) >= -1e-9 && cr(B, C, p[j]) >= -1e-9 && cr(C, A, p[j]) >= -1e-9)) continue
      out.push([A, B, C]); idx.splice(i, 1); cut = true
      break
    }
    if (!cut) idx.splice(0, 1)
  }
  if (idx.length === 3) out.push([p[idx[0]], p[idx[1]], p[idx[2]]])
  return out
}
/** Walls round a polygon from z0 to z1, and a flat roof. */
function prism(walls: Part, top: Part, poly: XY[], z0: number, z1: number) {
  const p = ccw(poly)
  for (let i = 0; i < p.length; i++) {
    const a = p[i], b = p[(i + 1) % p.length]
    walls.quad([a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
  }
  for (const [A, B, C] of earClip(p)) top.tri([A[0], A[1], z1], [B[0], B[1], z1], [C[0], C[1], z1])
}

// ------------------------------------------------------------ drum ----
const cladding = new Part(), glazing = new Part(), roof = new Part()
{
  const R = 48.8, SEG = 48, RECESS = 1.6, Z1 = 6.0, EAVE = 19.0, LIP = 1.0
  const P = (r: number, k: number, z: number): V3 => [r * Math.cos((2 * Math.PI * k) / SEG), r * Math.sin((2 * Math.PI * k) / SEG), z]
  const nrm = (k: number, up = 0): V3 => { const c = Math.cos((2 * Math.PI * k) / SEG), s = Math.sin((2 * Math.PI * k) / SEG), l = Math.hypot(1, up); return [c / l, s / l, up / l] }
  // Smooth-shaded rings: corner normals point out from the axis.
  const band = (part: Part, r0: number, z0: number, r1: number, z1: number, up0 = 0, up1 = 0) => {
    for (let k = 0; k < SEG; k++) {
      const a0 = P(r0, k, z0), b0 = P(r0, k + 1, z0), b1 = P(r1, k + 1, z1), a1 = P(r1, k, z1)
      part.tri(a0, b0, b1, undefined, undefined, undefined, [nrm(k, up0), nrm(k + 1, up0), nrm(k + 1, up1)])
      part.tri(a0, b1, a1, undefined, undefined, undefined, [nrm(k, up0), nrm(k + 1, up1), nrm(k, up1)])
    }
  }
  // Ground storey: dark, glazed in places, set back under the cladding.
  band(glazing, R - RECESS, 0, R - RECESS, Z1)
  // Soffit under the cladding's overhang.
  for (let k = 0; k < SEG; k++) cladding.quad(P(R - RECESS, k, Z1), P(R - RECESS, k + 1, Z1), P(R, k + 1, Z1), P(R, k, Z1))
  // The panelled drum and its rounded lip.
  band(cladding, R, Z1, R, EAVE)
  band(cladding, R, EAVE, R - LIP * 0.3, EAVE + LIP * 0.7, 0, 1)
  band(roof, R - LIP * 0.3, EAVE + LIP * 0.7, R - LIP, EAVE + LIP, 1, 4)
  // The dome: three shallow rings to a cap at 22 m.
  const rings: [number, number][] = [[R - LIP, EAVE + LIP], [34, 21.1], [18, 21.8], [0, 22.0]]
  for (let j = 0; j + 1 < rings.length; j++) {
    const [r0, z0] = rings[j], [r1, z1] = rings[j + 1]
    if (r1 > 0) band(roof, r0, z0, r1, z1, 6, 10)
    else for (let k = 0; k < SEG; k++) roof.tri(P(r0, k, z0), P(r0, k + 1, z0), [0, 0, z1])
  }
}

// The lap's shelf: the track round the drum is carried on a deep cantilevered
// box whose outer face reads as a band below the banked deck, as in the
// photos. It runs all the way round except where the straight joins, in the
// north-east.
{
  const SEG = 48, RS = 54.3, Z0 = 4.2, Z1 = 6.3
  for (let k = 0; k < SEG; k++) {
    const a0 = (2 * Math.PI * k) / SEG, a1 = (2 * Math.PI * (k + 1)) / SEG
    if (a1 > (30 * Math.PI) / 180 && a0 < (58 * Math.PI) / 180) continue
    const p = (a: number, z: number): V3 => [RS * Math.cos(a), RS * Math.sin(a), z]
    const n = (a: number): V3 => [Math.cos(a), Math.sin(a), 0]
    beam.tri(p(a0, Z0), p(a1, Z0), p(a1, Z1), undefined, undefined, undefined, [n(a0), n(a1), n(a1)])
    beam.tri(p(a0, Z0), p(a1, Z1), p(a0, Z1), undefined, undefined, undefined, [n(a0), n(a1), n(a0)])
    beam.quad(p(a1, Z0), p(a0, Z0), [Math.cos(a0) * (RS - 4), Math.sin(a0) * (RS - 4), Z0], [Math.cos(a1) * (RS - 4), Math.sin(a1) * (RS - 4), Z0])
  }
}

// ----------------------------------------- entrance vestibule, canopy ----
// The vestibule (the part of way/295315993 outside the drum) is a low glazed
// block under the track; the canopy (way/1539425232) is a white slab, tilted
// up towards the plaza, on four raking columns.
{
  const VEST: XY[] = [[-13.2, 46.9], [-20.9, 44.1], [-22.8, 48.0], [-23.5, 48.6], [-24.5, 49.5], [-27.2, 51.2], [-32.6, 47.7], [-37.6, 43.5], [-36.5, 40.3], [-33.8, 35.2], [-25.0, 43.0]]
  prism(glazing, roof, VEST, 0, 4.2)
  const C: XY[] = [[-58.4, 58.2], [-44.6, 37.8], [-24.0, 51.5], [-37.7, 71.9]]
  // Height: 6.5 m on the drum side (the edge from C[1] to C[2]), 8.5 m at the plaza edge.
  const mid12: XY = [(C[1][0] + C[2][0]) / 2, (C[1][1] + C[2][1]) / 2], mid30: XY = [(C[3][0] + C[0][0]) / 2, (C[3][1] + C[0][1]) / 2]
  const ax: XY = [mid30[0] - mid12[0], mid30[1] - mid12[1]], axL = Math.hypot(...ax)
  const zc = (q: XY) => 6.5 + (2 * ((q[0] - mid12[0]) * ax[0] + (q[1] - mid12[1]) * ax[1])) / (axL * axL)
  const T = 1.4
  const ring = ccw(C)
  for (let i = 0; i < 4; i++) {
    const a = ring[i], b = ring[(i + 1) % 4]
    cladding.quad([a[0], a[1], zc(a) - T], [b[0], b[1], zc(b) - T], [b[0], b[1], zc(b)], [a[0], a[1], zc(a)])
  }
  roof.tri([...ring[0], zc(ring[0])], [...ring[1], zc(ring[1])], [...ring[2], zc(ring[2])])
  roof.tri([...ring[0], zc(ring[0])], [...ring[2], zc(ring[2])], [...ring[3], zc(ring[3])])
  cladding.tri([...ring[2], zc(ring[2]) - T], [...ring[1], zc(ring[1]) - T], [...ring[0], zc(ring[0]) - T])
  cladding.tri([...ring[3], zc(ring[3]) - T], [...ring[2], zc(ring[2]) - T], [...ring[0], zc(ring[0]) - T])
  // Columns: from the ground 3 m in from each corner, raking out to the slab.
  const cx = (C[0][0] + C[1][0] + C[2][0] + C[3][0]) / 4, cy = (C[0][1] + C[1][1] + C[2][1] + C[3][1]) / 4
  for (const q of C) {
    const top: XY = [q[0] + (cx - q[0]) * 0.25, q[1] + (cy - q[1]) * 0.25], foot: XY = [q[0] + (cx - q[0]) * 0.4, q[1] + (cy - q[1]) * 0.4]
    const h = 0.3, zt = zc(top) - T
    const sq = (p: XY, z: number): V3[] => [[p[0] - h, p[1] - h, z], [p[0] + h, p[1] - h, z], [p[0] + h, p[1] + h, z], [p[0] - h, p[1] + h, z]]
    white.loft([sq(foot, 0), sq(top, zt)])
  }
}

// ------------------------------------------------------------ write ----
const parts = [
  { part: deck, material: finish('test-track-deck', 0xbabfc3) },
  { part: beam, material: finish('test-track-beam', 0x9ba3aa) },
  { part: white, material: PALETTE.trim },
  { part: cladding, material: finish('test-track-cladding', 0x545b64) },
  { part: glazing, material: PALETTE.window },
  { part: roof, material: finish('test-track-roof', 0xe2e5e6) },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Test Track', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 22, trackLength: 793,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-test-track.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes; anchor ${ANCHOR.lon}, ${ANCHOR.lat}`)
