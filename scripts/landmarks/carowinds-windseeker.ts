/**
 * WindSeeker, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-windseeker.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The origin is the
 * centre of the tower: OSM way/893187005, a 6 m circle tagged building=yes,
 * height=92, inside the ride's fence ring (way/890209625, 25 m across,
 * tourism=attraction, name "Windseeker").
 *
 * A Mondial WindSeeker (2012): a slim steel tower 301 ft (91.7 m) tall with
 * a red "UFO" cap, and a carriage that climbs it carrying 32 two-seat
 * swings (64 riders) on long hangers from a red umbrella frame.
 *
 * Evidence:
 * - Published (Wikipedia "WindSeeker"): 301 ft / 92 m, 32 twin seats, 64
 *   riders, the red UFO on top; Carowinds' ride has the same appearance as
 *   Cedar Point's, Kings Island's and Canada's Wonderland's.
 * - OSM: the tower circle and the fence ring above, for anchor and size.
 * - Photos (licensed):
 *   - Martin Lewison, "WindSeeker (Carowinds) 01/02/03", Wikimedia Commons,
 *     CC BY-SA 2.0: the tower from the entrance plaza beside the SkyTower,
 *     the swing ring from below, and the frame and seats close up.
 *   - mliu92, "Windseeker 0680", "Shading 0696", "Carolina Cobra 0522",
 *     Flickr/Commons, CC BY-SA 2.0: the carriage near the top with the swings
 *     out, and the hangers.
 *   - @englishinvader, "WindSeeker Media Day", Flickr, CC BY-NC-SA 2.0: the
 *     carriage at the bottom, with the frame's shape and the blue tower base.
 *   - Jonathan Goble, "Cedar Point WindSeeker Full Shot", Wikimedia Commons,
 *     CC BY-SA 3.0: the identical ride whole, used for the paint bands and
 *     the carriage's proportions against the tower.
 *
 * Measured from photos (estimates, about ±15%): the tower's paint bands
 * (dark blue to 18 m, then light blue / yellow / light blue; the same three
 * bands again just under the carriage's top stop), the tower's radius
 * (1.9 m at the foot to 1.5 m at the head), the carriage's 10 m height and
 * the frame's 9.6 m rim radius, and the 10 m swings, hinge to seat (about twice the
 * red drum's height in the photos; the seats' flight circle comes out at
 * about 35 m, against 30–36 m measured on the Cedar Point photo). Swing angle at full
 * speed, about 45°, is from the photos and the brief.
 *
 * Abstracted: the tower is a solid tapered column (no lattice, ladder or
 * rails); the frame's lattice trusses are sixteen arms of three chunky
 * bars each, with a solid rim ring; the machinery inside the carriage is a red drum. The real frame
 * has lighter members between the blades; the 32 hangers are spread evenly
 * round the rim, two per blade bay.
 *
 * It moves (experimental; see "Animation" in STYLE.md): the carriage is one
 * node, translated up the tower and turned about it. Each swing hangs from
 * its own hinge node on the rim and swings outward as the spin rises. The
 * static pose (frame 0) is at the top, spinning, swings out: the way it is
 * nearly always seen and photographed.
 *
 * The tower circle is the only OSM building it covers, so it replaces that.
 */
import { Part, axisAngle, cross, sub, writeGlb, type ChannelSpec, type NodeSpec, type Quat, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const TAU = Math.PI * 2
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A quad whose winding is chosen so it faces `out`, with per-corner normals. */
function face(p: Part, P: V3[], N: V3[]) {
  const f = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = N.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(f, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [N[a], N[b], N[c]])
  p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [N[a], N[c], N[d]])
}

/** A point at radius r, angle a (counter-clockwise from east), height z. */
const P = (r: number, a: number, z: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]

/**
 * A surface of revolution from a profile of [radius, z] points, smooth
 * around and flat along the profile; the profile's left-hand side is the
 * outside. `pick(band, seg)` chooses the part for each quad.
 */
function lathe(profile: [number, number][], pick: (band: number, seg: number) => Part | null, k = 16) {
  for (let b = 0; b < profile.length - 1; b++) {
    const [r0, z0] = profile[b], [r1, z1] = profile[b + 1]
    if (r0 === 0 && r1 === 0) continue
    const er = r1 - r0, ez = z1 - z0, l = Math.hypot(er, ez) || 1
    const nr = ez / l, nz = -er / l
    const N = (a: number): V3 => unit([nr * Math.cos(a), nr * Math.sin(a), nz])
    for (let s = 0; s < k; s++) {
      const part = pick(b, s)
      if (!part) continue
      const a0 = (s / k) * TAU, a1 = ((s + 1) / k) * TAU
      const quad = [P(r0, a0, z0), P(r0, a1, z0), P(r1, a1, z1), P(r1, a0, z1)]
      const norms = [N(a0), N(a1), N(a1), N(a0)]
      if (r0 === 0) part.tri(quad[0], quad[2], quad[3], undefined, undefined, undefined, [norms[0], norms[2], norms[3]])
      else if (r1 === 0) part.tri(quad[0], quad[1], quad[2], undefined, undefined, undefined, [norms[0], norms[1], norms[2]])
      else face(part, quad, norms)
    }
  }
}

/**
 * A flat-shaded convex solid from a polygon `front` and the same polygon
 * moved to `back`: two caps and a quad per edge, each turned to face away
 * from the solid's centre.
 */
function prism(p: Part, front: V3[], back: V3[]) {
  const all = [...front, ...back]
  const c = mul(all.reduce(add, [0, 0, 0] as V3), 1 / all.length)
  const flat = (pts: V3[]) => {
    let n = unit(cross(sub(pts[1], pts[0]), sub(pts[2], pts[0])))
    const m = mul(pts.reduce(add, [0, 0, 0] as V3), 1 / pts.length)
    if (dot(n, sub(m, c)) < 0) n = mul(n, -1)
    for (let i = 1; i < pts.length - 1; i++) {
      const t = [pts[0], pts[i], pts[i + 1]]
      const f = cross(sub(t[1], t[0]), sub(t[2], t[0]))
      if (dot(f, n) >= 0) p.tri(t[0], t[1], t[2], undefined, undefined, undefined, [n, n, n])
      else p.tri(t[0], t[2], t[1], undefined, undefined, undefined, [n, n, n])
    }
  }
  flat(front)
  flat(back)
  for (let i = 0; i < front.length; i++) {
    const j = (i + 1) % front.length
    flat([front[i], front[j], back[j], back[i]])
  }
}

/** A square bar of half-width w from a to b. */
function bar(p: Part, a: V3, b: V3, w: number) {
  const d = unit(sub(b, a))
  const s = unit(Math.abs(d[2]) > 0.95 ? cross(d, [1, 0, 0]) : cross(d, [0, 0, 1]))
  const u = cross(s, d)
  const ring = (c: V3) => [[1, 1], [-1, 1], [-1, -1], [1, -1]].map(([i, j]) => add(c, add(mul(s, i * w), mul(u, j * w))))
  prism(p, ring(a), ring(b))
}

/** An axis-aligned box, in whatever frame the part is authored in. */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number) {
  prism(p, [[x0, y0, z0], [x1, y0, z0], [x1, y1, z0], [x0, y1, z0]], [[x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1]])
}

// ------------------------------------------------------------ materials ----
// Six, from the photos, pulled toward the palette's lightness:
const RED = finish('ride-red', 0xc9503f)        // frame, carriage, UFO, seat tubs
const BLUE = finish('ride-blue', 0x4a66ad)      // tower foot, seat backs
const SKY = finish('ride-sky', 0x72b3dd)        // light-blue tower bands
const YELLOW = finish('ride-yellow', 0xe6c552)  // the tower's yellow bands
const SILVER = finish('tower-silver', 0xd2d6da) // the grey steel tower
const WHITE = PALETTE.trim                      // the carriage's machinery drum

// ---------------------------------------------------------------- tower ----
const red = new Part(), blue = new Part(), sky = new Part(), yellow = new Part(), silver = new Part()
const TOWER_TOP = 90          // under the UFO
const R0 = 1.9, R1 = 1.5      // tapered, foot to head
const rAt = (z: number) => R0 + (R1 - R0) * (z / TOWER_TOP)
// Paint bands, bottom up: [top of band, part].
const bands: [number, Part][] = [
  [18, blue], [21.2, sky], [22.5, yellow], [24.5, sky],
  [74.5, silver], [76, sky], [77.5, yellow], [79.5, sky], [TOWER_TOP, silver],
]
{
  const profile: [number, number][] = [[rAt(-1), -1]]
  for (const [z] of bands) profile.push([rAt(z), z])
  lathe(profile, (b) => bands[b][1], 12)
}

// ------------------------------------------------------------------ UFO ----
// A flattened red saucer on the tower head, with a short white mast and
// beacon to the published 301 ft.
const white = new Part()
lathe([[1.45, TOWER_TOP - 0.05], [3.5, 90.3], [3.9, 90.6], [3.9, 91.0], [2.2, 91.45], [0.3, 91.45]], () => red, 16)
lathe([[0.3, 91.45], [0.12, 91.75], [0, 91.75]], () => white, 8)

// -------------------------------------------------------------- carriage ----
// Authored about its pivot: on the tower's axis, at the underside of the
// bottom ring. From the bottom: a wide red ring, the red machinery drum, the
// ring the frame hangs off, and a white drum with a light-blue band. The
// frame is sixteen blades from the drum out to the rim, thick at the drum
// and meeting at the rim like a shallow lens, so from below it reads as an
// umbrella.
const cRed = new Part(), cWhite = new Part(), cSky = new Part()
const RIM_R = 9.6, RIM_Z = 6.0      // the hinge circle
const RING_Z = 6.5                   // the ring the arms leave the drum from
const DRUM_TOP = 10                  // top of the carriage
lathe([
  [2.05, 0.7], [2.05, 0], [4.6, 0], [4.6, 0.7], [3.6, 0.7], [3.6, RING_Z], [4.0, RING_Z], [4.0, RING_Z + 0.5], [2.6, RING_Z + 0.5],
], () => cRed, 24)
lathe([[2.6, RING_Z + 0.5], [2.6, 8.0], [2.6, 8.8], [2.6, DRUM_TOP], [1.6, DRUM_TOP]], (b) => (b === 1 ? cSky : cWhite), 24)
// The rim: a square-section ring the hangers hang from.
lathe([[RIM_R + 0.2, RIM_Z - 0.05], [RIM_R + 0.2, RIM_Z + 0.4], [RIM_R - 0.2, RIM_Z + 0.4], [RIM_R - 0.2, RIM_Z - 0.05], [RIM_R + 0.2, RIM_Z - 0.05]], () => cRed, 32)
// The arms, one per blade bay: a bottom chord from the drum's foot up to
// the rim, a top chord from the arm ring down to it, and a web strut
// between them. The white drum stands clear above them, as in the photos. Chunky bars rather than the real trusses' lattice,
// but open, so the frame reads as the red fan it is and not a solid dish.
const ARMS = 16
for (let i = 0; i < ARMS; i++) {
  const a = (i / ARMS) * TAU
  const foot = P(3.6, a, 0.75), head = P(3.8, a, RING_Z + 0.25), rim = P(RIM_R, a, RIM_Z + 0.2)
  bar(cRed, foot, rim, 0.2)
  bar(cRed, head, rim, 0.2)
  bar(cRed, P(5.4, a, 2.2), P(6.6, a, 6.45), 0.14)
}

// ---------------------------------------------------------------- swing ----
// Authored about its hinge on the rim, hanging straight down; +x points out
// from the tower, +y along the rim. A hanger and a lower link behind the
// seat back, then a two-seat tub facing outward: red shell, blue seat and
// back. One mesh, used by all 32 swings. The hangers are red: up close
// their upper half is orange, but from any distance (the photo from the
// park entrance) they read red with the frame, and a seventh colour would
// break STYLE's limit of six.
const sRed = new Part(), sBlue = new Part()
const SEAT_W = 0.85                     // half the bench's width
box(sRed, -0.13, 0.13, -0.13, 0.13, -5.6, 0.2)
box(sRed, -0.42, -0.18, -0.12, 0.12, -8.75, -5.6)
box(sRed, -0.3, 0.75, -SEAT_W, SEAT_W, -10.0, -9.45)      // the tub
box(sBlue, -0.25, 0.7, -SEAT_W + 0.08, SEAT_W - 0.08, -9.45, -9.33) // seat
box(sBlue, -0.45, -0.18, -SEAT_W, SEAT_W, -9.45, -8.6)    // seat back
const SWING_L = 9.6                     // hinge to the swing's centre of mass

// --------------------------------------------------------------- motion ----
/**
 * A 140 s loop, sampled every half second. The real cycle is about three
 * minutes; this is shortened mainly by trimming the waits, and the spin is
 * the real one, since a 7–8 rpm turn already reads on a map.
 *
 * From frame 0, at the top spinning at full speed:
 *   0–10    held at the top, spinning, swings out
 *   10–50   down the tower, the spin dying away and the swings falling in
 *   50–75   stopped at the bottom, loading
 *   75–120  up the tower, starting to spin, the swings lifting partway
 *   120–128 at the top, spinning up to full speed; the swings fly out
 *   128–140 held at the top, back to frame 0
 *
 * The swing angle is the conical pendulum's for the spin at that moment,
 * tan φ = ω²(R + L sin φ)/g, so the swings rise and fall with the speed.
 * The top speed is set so the carriage makes a whole even number of turns a
 * loop, which brings its rotation back exactly to frame 0; that lands the
 * swing angle close to the photos' 45°.
 */
const LOOP = 140, DT = 0.5, RAMP = 6, G = 9.81
const Z_BOTTOM = 4.5                         // seats just clear of the ground
const Z_TOP = TOWER_TOP - DRUM_TOP - 0.2     // the carriage's top just under the UFO
const smooth = (x: number) => { const s = Math.min(1, Math.max(0, x)); return s * s * (3 - 2 * s) }
/** Progress through a run of length T at time t, on a trapezoidal speed profile. */
function run(t: number, T: number) {
  const v = 1 / (T - RAMP)
  if (t < RAMP) return (v * t * t) / (2 * RAMP)
  if (t > T - RAMP) return 1 - (v * (T - t) ** 2) / (2 * RAMP)
  return v * (t - RAMP / 2)
}
/** Height of the pivot above the ground at time t. */
function lift(t: number) {
  if (t <= 10 || t >= 120) return Z_TOP
  if (t < 50) return Z_TOP + (Z_BOTTOM - Z_TOP) * run(t - 10, 40)
  if (t <= 75) return Z_BOTTOM
  return Z_BOTTOM + (Z_TOP - Z_BOTTOM) * run(t - 75, 45)
}
/** Spin speed as a fraction of full speed. */
function spin(t: number) {
  if (t <= 10) return 1
  if (t < 50) return 1 - smooth((t - 10) / 40)
  if (t <= 75) return 0
  if (t < 120) return 0.7 * smooth((t - 75) / 45)
  if (t < 128) return 0.7 + 0.3 * smooth((t - 120) / 8)
  return 1
}
/** The conical pendulum's angle at spin rate w (rad/s). */
function swingAngle(w: number) {
  let phi = 0
  for (let i = 0; i < 60; i++) phi = Math.atan((w * w * (RIM_R + SWING_L * Math.sin(phi))) / G)
  return phi
}
const times = Array.from({ length: LOOP / DT + 1 }, (_, i) => i * DT)
// Full speed for a 45° swing, then nudged to an even number of turns a loop.
const W45 = Math.sqrt(G / (RIM_R + SWING_L * Math.SQRT1_2))
let area = 0
for (let i = 1; i < times.length; i++) area += ((spin(times[i - 1]) + spin(times[i])) / 2) * DT
const TURNS = Math.max(2, 2 * Math.round((W45 * area) / TAU / 2))
const W_MAX = (TURNS * TAU) / area
const PHI_MAX = swingAngle(W_MAX)
const angle: number[] = [0]
for (let i = 1; i < times.length; i++) angle.push(angle[i - 1] + ((spin(times[i - 1]) + spin(times[i])) / 2) * DT * W_MAX)

const lifts: V3[] = times.map((t) => [0, 0, lift(t)])
// Counter-clockwise from above; at most about 24° between keys.
const turn: Quat[] = angle.map((a) => axisAngle([0, 0, 1], a))
// A swing tilts about the rim's tangent (its own +y), its foot going outward.
const tilt: Quat[] = times.map((t) => axisAngle([0, 1, 0], -swingAngle(W_MAX * spin(t))))

// ----------------------------------------------------------------- nodes ----
// Node 0 is the carriage. Each swing is two nodes: a hinge node fixed to the
// carriage, turned to face out at its angle round the rim, and the swing
// under it, tilting about the hinge. Both are placed at the hinge, so the
// swing pivots there.
const SWINGS = 32
const nodes: NodeSpec[] = [{
  name: 'carriage',
  translation: lifts[0],
  parts: [
    { part: cRed, material: RED },
    { part: cWhite, material: WHITE },
    { part: cSky, material: SKY },
  ],
}]
const channels: ChannelSpec[] = [
  { node: 0, path: 'translation', values: lifts },
  { node: 0, path: 'rotation', values: turn },
]
const swingParts = [
  { part: sRed, material: RED },
  { part: sBlue, material: BLUE },
]
const yaw = new Map<string, Quat>()
for (let i = 0; i < SWINGS; i++) {
  // Two per blade bay, a quarter of a bay either side of its middle.
  const a = ((i + 0.5) / SWINGS) * TAU
  yaw.set(`hinge-${i}`, axisAngle([0, 0, 1], a))
  nodes.push({ name: `hinge-${i}`, parent: 0, parts: [] })
  nodes.push({ name: `swing-${i}`, parent: nodes.length - 1, translation: [RIM_R, 0, RIM_Z], parts: swingParts })
  channels.push({ node: nodes.length - 1, path: 'rotation', values: tilt })
}

// ---------------------------------------------------------------- output ---
const parts = [
  { part: red, material: RED },
  { part: blue, material: BLUE },
  { part: sky, material: SKY },
  { part: yellow, material: YELLOW },
  { part: silver, material: SILVER },
  { part: white, material: WHITE },
]
const triangles = [...parts, ...nodes.flatMap((n) => n.parts)].reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const raw = writeGlb('WindSeeker', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 91.7,
}, { nodes, animation: { name: 'ride', times, channels } })

/**
 * mesh.ts places a node but cannot turn it, and these nodes need turning:
 * each hinge faces out at its own angle (so one swing mesh serves all 32),
 * and each swing starts tilted out, as frame 0 has it. Set their rotations
 * in the GLB's JSON (glTF frame: map (x, y, z) is glTF (x, z, -y)).
 */
function setRotations(glb: Uint8Array, rotations: Map<string, Quat>): Uint8Array {
  const view = new DataView(glb.buffer, glb.byteOffset, glb.byteLength)
  const jsonLen = view.getUint32(12, true)
  const json = JSON.parse(new TextDecoder().decode(glb.subarray(20, 20 + jsonLen)))
  for (const node of json.nodes) {
    const q = rotations.get(node.name)
    if (q) node.rotation = [q[0], q[2], -q[1], q[3]]
  }
  let text = JSON.stringify(json)
  while (text.length % 4) text += ' '
  const jsonBytes = new TextEncoder().encode(text)
  const bin = glb.subarray(20 + jsonLen)
  const out = new Uint8Array(20 + jsonBytes.length + bin.length)
  const o = new DataView(out.buffer)
  o.setUint32(0, 0x46546c67, true); o.setUint32(4, 2, true); o.setUint32(8, out.length, true)
  o.setUint32(12, jsonBytes.length, true); o.setUint32(16, 0x4e4f534a, true)
  out.set(jsonBytes, 20)
  out.set(bin, 20 + jsonBytes.length)
  return out
}
const rotations = new Map(yaw)
for (let i = 0; i < SWINGS; i++) rotations.set(`swing-${i}`, tilt[0])
const glb = setRotations(raw, rotations)
if (glb.length > 250_000) throw new Error(`Size budget exceeded: ${glb.length} bytes`)
const out = new URL('../../landmarks/models/carowinds-windseeker.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes, ${LOOP} s a loop, ${TURNS} turns, ` +
  `top speed ${(W_MAX * 60 / TAU).toFixed(1)} rpm, swings out ${(PHI_MAX * 180 / Math.PI).toFixed(1)}°`)
