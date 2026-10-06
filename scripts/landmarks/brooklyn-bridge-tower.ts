/**
 * Brooklyn Bridge tower — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/brooklyn-bridge-tower.ts
 *
 * One model, placed twice (the Manhattan and Brooklyn towers are identical).
 * Map frame: x across the bridge, y along its axis, z up, metres. Placed at
 * bearing 316.2°, the line from the Brooklyn tower to the Manhattan one.
 *
 * The overall rectangle is OSM's (ways 317352708 and 1255363983, which agree
 * to 0.1 m): 42.8 × 16.8 m, three piers with a 2.8 m buttress on each end.
 * OSM sets the wall between the piers back 4.6 m; that made the tower read as
 * three slabs from above, so here it is a 1.2 m recess and the piers read as
 * pilasters on one wall. The two pointed arches run straight through the full
 * depth from the roadway band (36.5 m) to a point at 70 m, with dark reveals.
 * Below the roadway the tower is one solid block with shallow pilasters, as
 * photographed from Brooklyn Bridge Park. OSM traces the arches 9.3 m wide
 * and the centre pier 6.2 m, but square-on photos from the deck show each
 * opening close to twice the centre pier, so the arches are opened to 10 m.
 * Height is the real 84.3 m (276.5 ft) above high water to the tops of the
 * pier caps.
 *
 * y = 0 is the waterline: the towers stand in the river, so the lowest
 * ground under the footprint is the water surface, which the map draws at 0.
 * No cables and no deck span: the basemap draws the bridge. The roadway
 * through the arches is a flat inset strip, not a stub.
 *
 * Styled to landmarks/STYLE.md: warm granite and limestone, bevelled piers,
 * a projecting cornice and deck-level string course, archivolts as raised
 * bands inside the curtain face. Mirrored about both axes.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

const stone = new Part()    // piers, buttresses, lower block
const curtain = new Part()  // recessed curtain wall between the piers
const trim = new Part()     // plinth, string course, cornice, archivolts, caps
const soffit = new Part()   // arch intrados, in shadow
const roof = new Part()     // top surfaces: the tower's own stone, a shade darker
const road = new Part()     // roadway strip through the arches

// Plan (OSM, rotated into the bridge frame).
const PX = 2.8                  // centre pier half-width (OSM 3.1; see below)
const OX0 = 12.8, OX1 = 18.6    // outer pier (OSM 12.4)
const EX = 21.4                 // end of the buttress / lower block
const PD = 8.3                  // pier half-depth
const CD = 7.1                  // curtain wall half-depth: a 1.2 m recess, not OSM's 4.6
const BD = 3.6                  // end buttress half-depth
const LD = 8.4                  // lower block half-depth

// Heights.
const PLINTH = 2.0
const DECK0 = 35.0, DECK = 36.5 // string course; its top is the roadway
const SPRING = 61.0, RISE = 9.0 // pointed arches: 10 m wide, crown at 70 m
const COR0 = 79.4, COR1 = 81.2  // cornice
const TOP = 82.2                // wall top
const CAP = 84.3                // pier caps (cable saddle housings)

const XC = (PX + OX0) / 2       // arch centre line
const A = (OX0 - PX) / 2        // arch half-span
const C = (RISE * RISE - A * A) / (2 * A) // arc centres sit ±C from XC on the spring line
const R = A + C
const SEG = 8                   // segments per half-arch
const BAND = 0.9, PROUD = 0.25  // archivolt width and projection

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

// Mirror applied to everything emitted through `q`, so one quarter is authored.
// FX additionally reflects about the arch centre line, so one half-arch
// becomes all four.
let MX = 1, MY = 1, FX = false
const m = (v: V3): V3 => [(FX ? 2 * XC - v[0] : v[0]) * MX, v[1] * MY, v[2]]
const mv = (v: V3): V3 => [(FX ? -v[0] : v[0]) * MX, v[1] * MY, v[2]] // directions: no offset
function mirrored(build: () => void, xs = [1, -1], ys = [1, -1]) {
  for (const sx of xs) for (const sy of ys) { MX = sx; MY = sy; build() }
  MX = 1; MY = 1
}

/** A quad with per-corner normals; winding follows the normals, so mirroring is safe. */
function q(p: Part, pts: V3[], ns: V3[]) {
  const P = pts.map(m), N = ns.map(n => unit(mv(n)))
  const face = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = N.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(face, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [N[a], N[b], N[c]])
  p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [N[a], N[c], N[d]])
}
const flat = (p: Part, pts: V3[], n: V3) => q(p, pts, [n, n, n, n])
function fan(p: Part, ring: V3[], n: V3) {
  for (let i = 1; i < ring.length - 1; i++) {
    const P = [ring[0], ring[i], ring[i + 1]].map(m), N = unit(mv(n))
    const ok = dot(cross(sub(P[1], P[0]), sub(P[2], P[0])), N) >= 0
    p.tri(P[0], ok ? P[1] : P[2], ok ? P[2] : P[1], undefined, undefined, undefined, [N, N, N])
  }
}

/** Plan rim of a rectangle with chamfered corners, each point with its face normal. */
function rim(x0: number, x1: number, y0: number, y1: number, r: number, z: number) {
  r = Math.max(r, 0.04)
  const S: V3 = [0, -1, 0], E: V3 = [1, 0, 0], N: V3 = [0, 1, 0], W: V3 = [-1, 0, 0]
  return {
    pts: [[x0 + r, y0, z], [x1 - r, y0, z], [x1, y0 + r, z], [x1, y1 - r, z],
      [x1 - r, y1, z], [x0 + r, y1, z], [x0, y1 - r, z], [x0, y0 + r, z]] as V3[],
    ns: [S, S, E, E, N, N, W, W],
  }
}

/**
 * A box with rounded vertical edges and optional rounded top and bottom
 * edges. Its top cap goes to `lid` (the roof material) unless told otherwise.
 */
function rbox(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number,
  { r = 0.45, top = 0, bot = 0, lid = p as Part | null, base = false } = {}) {
  const up: V3 = [0, 0, 1], down: V3 = [0, 0, -1]
  const rings: { pts: V3[]; ns: V3[] }[] = []
  if (bot > 0) rings.push({ ...rim(x0 + bot, x1 - bot, y0 + bot, y1 - bot, r - bot * .41, z0), ns: Array(8).fill(down) })
  rings.push(rim(x0, x1, y0, y1, r, z0 + bot))
  rings.push(rim(x0, x1, y0, y1, r, z1 - top))
  if (top > 0) rings.push({ ...rim(x0 + top, x1 - top, y0 + top, y1 - top, r - top * .41, z1), ns: Array(8).fill(up) })
  for (let k = 0; k < rings.length - 1; k++) for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8, a = rings[k], b = rings[k + 1]
    q(p, [a.pts[i], a.pts[j], b.pts[j], b.pts[i]], [a.ns[i], a.ns[j], b.ns[j], b.ns[i]])
  }
  if (lid) fan(lid, rings[rings.length - 1].pts, up)
  if (base) fan(p, rings[0].pts, down)
}

// ---- Below the roadway: one solid block on a waterline plinth --------------

rbox(trim, -EX - .5, EX + .5, -LD - .5, LD + .5, 0, PLINTH, { r: .5, top: .3, lid: roof })
// The wall between the pilasters, set back 0.5 m and toned like the curtain
// above, so the bays line up with the arches and the piers read top to bottom.
rbox(curtain, -EX + .5, EX - .5, -LD + .5, LD - .5, PLINTH, DECK0, { r: .3, lid: null })
mirrored(() => {
  // Pilasters under the three piers; the outer ones run out to the ends,
  // where a slot either side of the buttress line carries it down too.
  for (const [y0, y1] of [[-LD, -BD - .8], [-BD, BD], [BD + .8, LD]])
    rbox(stone, OX0 + .4, EX, y0, y1, PLINTH, DECK0, { r: .45, lid: null })
}, [1, -1], [1])
rbox(stone, -PX, PX, -LD, LD, PLINTH, DECK0, { r: .45, lid: null })

// String course at the roadway: a projecting band all round.
rbox(trim, -EX - .35, EX + .35, -LD - .35, LD + .35, DECK0, DECK, { r: .5, top: .25, bot: .2, lid: roof })

// The roadway through each arch, a flat inset strip on the string course.
mirrored(() => {
  flat(road, [[PX, -LD - .2, DECK + .02], [OX0, -LD - .2, DECK + .02],
    [OX0, LD + .2, DECK + .02], [PX, LD + .2, DECK + .02]], [0, 0, 1])
}, [1, -1], [1])

// ---- Above the roadway: piers, buttresses, curtain wall ---------------------

rbox(stone, -PX, PX, -PD, PD, DECK, TOP, { r: .5, lid: roof })
mirrored(() => {
  rbox(stone, OX0, OX1, -PD, PD, DECK, TOP, { r: .5, lid: roof })
  // The end buttress stops a little under the cornice, which wraps it.
  rbox(stone, OX1 - .3, EX, -BD, BD, DECK, COR0 + .2, { r: .45, lid: null })
}, [1, -1], [1])

// Pointed arch: two arcs of radius `rad` centred ±C from the arch centre line.
// t = 0 at the spring, 1 at the crown; returns the left-arc point in (x, z).
function arc(rad: number, t: number): [number, number, number, number] {
  const crown = Math.acos(-C / rad)
  const th = Math.PI + (crown - Math.PI) * t
  const cx = XC + C
  return [cx + rad * Math.cos(th), SPRING + rad * Math.sin(th), Math.cos(th), Math.sin(th)]
}

// One half-arch (the half toward the centre pier at x > 0), mirrored to all eight.
for (const f of [false, true]) mirrored(() => {
  FX = f
  // Curtain face above the arch, strip by strip, so the opening is real.
  for (let i = 0; i < SEG; i++) {
    const [xa, za] = arc(R, i / SEG), [xb, zb] = arc(R, (i + 1) / SEG)
    flat(curtain, [[xa, CD, za], [xb, CD, zb], [xb, CD, TOP], [xa, CD, TOP]], [0, 1, 0])
  }
  // Intrados through the wall, smooth along each arc and creased at the crown.
  for (let i = 0; i < SEG; i++) {
    const [xa, za, ca, sa] = arc(R, i / SEG), [xb, zb, cb, sb] = arc(R, (i + 1) / SEG)
    const na: V3 = [-ca, 0, -sa], nb: V3 = [-cb, 0, -sb]
    q(soffit, [[xa, 0, za], [xb, 0, zb], [xb, CD, zb], [xa, CD, za]], [na, nb, nb, na])
  }
  // Dark reveal on the jamb: the pier's inner face, through the wall's depth.
  // Laid a hair proud of the pier so it wins the depth test.
  flat(soffit, [[PX + .01, 0, DECK], [PX + .01, CD, DECK], [PX + .01, CD, SPRING], [PX + .01, 0, SPRING]], [1, 0, 0])
  // Archivolt: a raised pointed band on the curtain face.
  const RO = R + BAND
  for (let i = 0; i < SEG; i++) {
    const t0 = i / SEG, t1 = (i + 1) / SEG
    const [xi0, zi0, ci0, si0] = arc(R, t0), [xi1, zi1, ci1, si1] = arc(R, t1)
    const [xo0, zo0, co0, so0] = arc(RO, t0), [xo1, zo1, co1, so1] = arc(RO, t1)
    const y1 = CD + PROUD
    flat(trim, [[xi0, y1, zi0], [xi1, y1, zi1], [xo1, y1, zo1], [xo0, y1, zo0]], [0, 1, 0])
    q(trim, [[xo0, CD, zo0], [xo1, CD, zo1], [xo1, y1, zo1], [xo0, y1, zo0]],
      [[co0, 0, so0], [co1, 0, so1], [co1, 0, so1], [co0, 0, so0]])
    q(trim, [[xi0, CD, zi0], [xi1, CD, zi1], [xi1, y1, zi1], [xi0, y1, zi0]],
      [[-ci0, 0, -si0], [-ci1, 0, -si1], [-ci1, 0, -si1], [-ci0, 0, -si0]])
  }
  FX = false
}, [1, -1], [1, -1])
// The curtain wall's own top, between the piers.
mirrored(() => {
  flat(roof, [[PX, -CD, TOP], [OX0, -CD, TOP], [OX0, CD, TOP], [PX, CD, TOP]], [0, 0, 1])
}, [1, -1], [1])

// ---- Cornice and caps --------------------------------------------------------

const CO = .45 // cornice projection
rbox(trim, -PX - CO, PX + CO, -PD - CO, PD + CO, COR0, COR1, { r: .5, top: .2, bot: .25, lid: null })
mirrored(() => {
  rbox(trim, OX0 - CO, OX1 + CO, -PD - CO, PD + CO, COR0, COR1, { r: .5, top: .2, bot: .25, lid: null })
  rbox(trim, PX, OX0, -CD - CO, CD + CO, COR0, COR1, { r: .2, top: .2, bot: .25, lid: null })
  rbox(trim, OX1, EX + CO, -BD - CO, BD + CO, COR0, COR1, { r: .5, top: .2, bot: .25, lid: roof })
}, [1, -1], [1])

// Saddle housings over the three piers: plain raised blocks.
rbox(trim, -PX + .5, PX - .5, -PD + .6, PD - .6, TOP, CAP, { r: .4, top: .3, lid: roof })
mirrored(() => {
  rbox(trim, OX0 + .5, OX1 - .5, -PD + .6, PD - .6, TOP, CAP, { r: .4, top: .3, lid: roof })
}, [1, -1], [1])

// Colours from daylight photos: the sunlit face from Brooklyn Bridge Park
// (warm buff granite and limestone), the cornice a touch paler, the curtain
// a touch deeper, the arch soffit in its own shade.
const parts = [
  { part: stone, material: { name: 'granite', color: 0xd2c2ab } },
  { part: curtain, material: { name: 'curtain-wall', color: 0xc3b19a } },
  { part: trim, material: { name: 'limestone-trim', color: 0xdcd0bd } },
  { part: soffit, material: { name: 'arch-soffit', color: 0x857766 } },
  { part: roof, material: { name: 'tower-top', color: 0xb8aa95 } },
  { part: road, material: { name: 'roadway', color: 0x857d71 } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Brooklyn Bridge tower', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 316.2, elevation: 0, footprint: [2 * EX, 2 * LD], height: CAP,
  roadway: DECK, archCrown: SPRING + RISE,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/brooklyn-bridge-tower.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
