/**
 * Spaceship Earth, EPCOT (Walt Disney World) — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-spaceship-earth.ts
 *
 * Map frame: x east, y north, z up, metres; origin on the ground at the
 * centre of the circle fitted to the OSM outline (way/146221550: radius
 * 25.45 m, every node within 0.1 m of it). Placed at
 * bearing 0: the park's entrance axis runs north–south through the sphere.
 *
 * A geodesic sphere of silver triangular facets held off the ground on three
 * pairs of steel legs. It is modelled as what it is: a subdivided
 * icosahedron, each triangle raised into a shallow three-sided pyramid as the
 * real cladding is, flat-shaded so every facet catches the light on its own.
 * The facet variation is a second, slightly darker silver on three pyramids
 * in ten, as the real panels differ a little in tone.
 * Under the sphere sit a dark glazed entrance core and, to the south, the low
 * block joining it to the Project Tomorrow building.
 *
 * Evidence:
 * - OSM: outline way/146221550 (height 55, roof dome), and two buildings
 *   wholly inside it — way/295176801 (3 levels, the core under the sphere)
 *   and way/298641632 (a trapezoid on the south side under the sphere). The
 *   model covers all three; a 3-level extrusion would poke through the
 *   sphere's underside if left drawn. Project Tomorrow (way/295176970,
 *   tagged 25 m) is a separate building to the south and stays drawn.
 * - Published (Wikipedia, Disney): 180 ft (54.9 m) tall, 165 ft (50.3 m)
 *   across, so the sphere clears the ground by about 4.6 m; six legs in
 *   three pairs.
 * - Photos (Wikimedia Commons, credited in the report): the entrance views
 *   from the north (chensiyuan 2010, Katie Rommel-Esham, InSapphoWeTrust,
 *   Theme Park Tourist 2015) and the 2019 aerial from the south-west
 *   (Gfgbeach). They give the leg pairs at the north-east and north-west,
 *   left and right of the entrance, and the third pair to the south under
 *   Project Tomorrow; the facet density (about 32 facets across the visible
 *   face, so frequency 10; the model uses 8 for the file budget); and the
 *   colours: silver facets, legs with a pale top and warm grey flanks.
 * - Estimated: the legs' cross-section, slope (about 66°) and spread, and
 *   where they meet the sphere (about 12 m up). The entrance photos and the
 *   aerial disagree on how far the feet spread, so the model splits the
 *   difference, and the pairs are set at exactly 60°, 180° and 300°. Also
 *   estimated: the core's radius and height, the south block's 6 m (from
 *   the aerial) and the pyramid depth. The icosahedron's orientation (a
 *   vertex at the top) is a choice; the real panel layout isn't recorded.
 */
import { Part, cross, sub, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const silver = new Part(), silver2 = new Part(), legTop = new Part(), legSide = new Part()
const core = new Part()
// The base and the low south block are pale grey steel and render like the
// legs' tops, so they share that material and the model keeps to five.
const base = legTop

const R = 24.9            // geodesic vertices; the apexes stand proud to the outline's 25.45 m
const APEX = 0.75         // how far each facet's apex rises above its triangle
const TOP = 55            // OSM height, published 54.9 m
const CZ = TOP - R - 0.6  // sphere centre height: the topmost apex sits at 55 m, the underside 4.6 m up
/**
 * Geodesic frequency: 20·F² facets. The real sphere is close to 10, but
 * every face of a flat-shaded pyramid needs corners of its own in the GLB,
 * which puts frequency 10 at 430 KB. Frequency 8 (about 26 facets across the
 * visible face) fits in 250 KB, and at map distance reads as the sphere's
 * sparkle rather than a smooth ball.
 */
const F = 8

const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (len(a) || 1))
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A flat triangle wound to face away from `out`, whatever order it came in. */
function face(p: Part, a: V3, b: V3, c: V3, out: V3) {
  if (dot(cross(sub(b, a), sub(c, a)), out) >= 0) p.tri(a, b, c)
  else p.tri(a, c, b)
}

// ---------------------------------------------------------------------------
// The sphere. An icosahedron with a vertex at the top, each face split into
// F² triangles whose corners are pushed out onto the sphere.

const PHI = (1 + Math.sqrt(5)) / 2
const ico: V3[] = [
  [-1, PHI, 0], [1, PHI, 0], [-1, -PHI, 0], [1, -PHI, 0],
  [0, -1, PHI], [0, 1, PHI], [0, -1, -PHI], [0, 1, -PHI],
  [PHI, 0, -1], [PHI, 0, 1], [-PHI, 0, -1], [-PHI, 0, 1],
].map((v) => unit(v as V3))
const icoFaces = [
  [0, 11, 5], [0, 5, 1], [0, 1, 7], [0, 7, 10], [0, 10, 11],
  [1, 5, 9], [5, 11, 4], [11, 10, 2], [10, 7, 6], [7, 1, 8],
  [3, 9, 4], [3, 4, 2], [3, 2, 6], [3, 6, 8], [3, 8, 9],
  [4, 9, 5], [2, 4, 11], [6, 2, 10], [8, 6, 7], [9, 8, 1],
]
// Turn the icosahedron so vertex 0 points straight up (+z), then a little
// about z so no seam of the pattern lines up with the entrance axis.
const up0 = ico[0]
const axis = unit(cross(up0, [0, 0, 1]))
const ang = Math.acos(dot(up0, [0, 0, 1]))
function rotate(v: V3, k: V3, a: number): V3 {
  const c = Math.cos(a), s = Math.sin(a)
  return add(add(mul(v, c), mul(cross(k, v), s)), mul(k, dot(k, v) * (1 - c)))
}
const spin = (18 * Math.PI) / 180
const dirs = ico.map((v) => rotate(rotate(v, axis, ang), [0, 0, 1], spin))

const centre: V3 = [0, 0, CZ]
const onSphere = (d: V3): V3 => add(centre, unit(d), R)

let facets = 0
for (const [ia, ib, ic] of icoFaces) {
  const A = dirs[ia], B = dirs[ib], C = dirs[ic]
  const P = (i: number, j: number): V3 => {
    // i steps from A towards B, j from A towards C.
    const k = F - i - j
    return onSphere(add(add(mul(A, k / F), mul(B, i / F)), mul(C, j / F)))
  }
  const tris: [V3, V3, V3][] = []
  for (let i = 0; i < F; i++)
    for (let j = 0; j < F - i; j++) {
      tris.push([P(i, j), P(i + 1, j), P(i, j + 1)])
      if (i + j < F - 1) tris.push([P(i + 1, j), P(i + 1, j + 1), P(i, j + 1)])
    }
  for (const [a, b, c] of tris) {
    const m = mul(add(add(a, b), c), 1 / 3)
    const out = unit(sub(m, centre))
    facets++
    // Facets facing well down are only seen from below the ground plane of
    // the map's camera; they stay flat triangles and save two thirds.
    if (out[2] < -0.72) {
      face(silver, a, b, c, out)
      continue
    }
    const apex = add(m, out, APEX)
    // Subtle facet variation: alternate pyramids by a fixed, irregular rule
    // get the second silver, as the real cladding's panels differ slightly
    // in tone. Hashing the position keeps it stable between runs.
    const h = Math.abs(Math.sin(m[0] * 12.9898 + m[1] * 78.233 + m[2] * 37.719) * 43758.5453) % 1
    const part = h < 0.3 ? silver2 : silver
    // Each face is flat-shaded along its base, where it meets the next
    // pyramid, so the facet edges stay crisp; its apex corner takes the
    // pyramid's own axis instead, so the three faces share one apex vertex.
    // That saves a fifth of the file, which is what pays for frequency 8.
    for (const [u, v] of [[a, b], [b, c], [c, a]] as [V3, V3][]) {
      let n = unit(cross(sub(v, u), sub(apex, u)))
      let [p0, p1] = [u, v]
      if (dot(n, out) < 0) { n = mul(n, -1); [p0, p1] = [v, u] }
      part.tri(p0, p1, apex, undefined, undefined, undefined, [n, n, out])
    }
  }
}

// ---------------------------------------------------------------------------
// The legs: three pairs, at the north-east and north-west either side of the
// entrance and one to the south. Each leg is a chamfered steel box leaning in
// at about 66°, its top buried in the sphere and its foot on the ground just
// inside the outline.

/** A prism between two horizontal outlines (same corner count, counter-clockwise from above). */
function prism(p: Part, lo: V3[], hi: V3[], capTop = true, capBottom = true) {
  const n = lo.length
  const cl = mul(lo.reduce((s, v) => add(s, v), [0, 0, 0] as V3), 1 / n)
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n
    const out: V3 = [(lo[i][0] + lo[j][0]) / 2 - cl[0], (lo[i][1] + lo[j][1]) / 2 - cl[1], 0]
    face(p, lo[i], lo[j], hi[j], out)
    face(p, lo[i], hi[j], hi[i], out)
  }
  for (let i = 1; i < n - 1; i++) {
    if (capTop) face(p, hi[0], hi[i], hi[i + 1], [0, 0, 1])
    if (capBottom) face(p, lo[0], lo[i], lo[i + 1], [0, 0, -1])
  }
}

const LEG_FOOT_IN = 19.2, LEG_FOOT_OUT = 23.8 // radial extent of a foot
const LEG_TOP_Z = 19, LEG_LEAN = 8.4           // the leg climbs 19 m over 8.4 m inward, about 66°
const LEG_W = 2.8, LEG_GAP = 0.9               // width of one leg, gap between the pair
const CH = 0.4                                  // edge chamfer

/**
 * One leg: a chamfered box leaning inward. Its outer face, which looks up
 * and out, is pale; its sides are the warm grey of the painted steel
 * (photos: the entrance views, where each leg shows a light top over a
 * darker flank).
 */
function leg(d: V3, q: V3, s0: number, s1: number) {
  const ring = (z: number): V3[] => {
    const shift = (z / LEG_TOP_Z) * LEG_LEAN
    const r0 = LEG_FOOT_IN - shift, r1 = LEG_FOOT_OUT - shift
    const pts: [number, number][] = [
      [r0 + CH, s0], [r1 - CH, s0], [r1, s0 + CH], [r1, s1 - CH],
      [r1 - CH, s1], [r0 + CH, s1], [r0, s1 - CH], [r0, s0 + CH],
    ]
    return pts.map(([r, s]) => [d[0] * r + q[0] * s, d[1] * r + q[1] * s, z])
  }
  const lo = ring(0), hi = ring(LEG_TOP_Z)
  const mid = mul(lo.reduce((acc, v) => add(acc, v), [0, 0, 0] as V3), 1 / 8)
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    const out: V3 = [(lo[i][0] + lo[j][0]) / 2 - mid[0], (lo[i][1] + lo[j][1]) / 2 - mid[1], 0]
    // Faces 1–3 are the outer face and its chamfers.
    const p = i >= 1 && i <= 3 ? legTop : legSide
    face(p, lo[i], lo[j], hi[j], out)
    face(p, lo[i], hi[j], hi[i], out)
  }
}

for (const bearing of [60, 180, 300]) {
  const b = (bearing * Math.PI) / 180
  const d: V3 = [Math.sin(b), Math.cos(b), 0]   // outward
  const q: V3 = [-d[1], d[0], 0]                // across
  leg(d, q, -LEG_GAP / 2 - LEG_W, -LEG_GAP / 2)
  leg(d, q, LEG_GAP / 2, LEG_GAP / 2 + LEG_W)
}

// ---------------------------------------------------------------------------
// Under the sphere: the glazed entrance core, on a pale plinth, and the low
// block on the south side that joins it to Project Tomorrow (way/298641632).

const circle = (r: number, z: number, n = 16, cx = 0, cy = 0): V3[] =>
  Array.from({ length: n }, (_, i) => {
    const a = (i / n) * Math.PI * 2 + Math.PI / n
    return [cx + r * Math.cos(a), cy + r * Math.sin(a), z]
  })
// The core stands a little north of centre, over OSM's 3-level core
// (way/295176801), and runs up into the sphere's underside.
prism(base, circle(10, 0, 16, 0.5, 4), circle(10, 0.5, 16, 0.5, 4), true, false)
prism(core, circle(9.2, 0.5, 16, 0.5, 4), circle(9.2, 9.5, 16, 0.5, 4), false, false)
// The low block to the south, under the sphere's rim (way/298641632).
{
  const pts = (z: number): V3[] => [[-15.3, -20.3, z], [-7.6, -24.2, z], [0, -25.4, z], [7.6, -24.2, z], [16.2, -19.6, z], [10.2, -4, z], [-8.3, -3.7, z]]
  prism(base, pts(0), pts(6), true, false)
}

// ---------------------------------------------------------------------------

// Five materials: two silvers for the facets (the second a shade darker on
// three pyramids in ten), the legs' pale top (also the base) and warm grey
// flank, and the dark glazed core as `window`, since it is the lit entrance.
const parts = [
  { part: silver, material: finish('spaceship-earth-silver', 0xd5d9dd, 0.45) },
  { part: silver2, material: finish('spaceship-earth-silver-2', 0xcbd0d5, 0.45) },
  { part: legTop, material: finish('spaceship-earth-leg', 0xd9dcdf) },
  { part: legSide, material: finish('spaceship-earth-leg-flank', 0xaaa19b) },
  { part: core, material: PALETTE.window },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Spaceship Earth', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: TOP,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-spaceship-earth.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles (${facets} facets), ${glb.length} bytes`)
