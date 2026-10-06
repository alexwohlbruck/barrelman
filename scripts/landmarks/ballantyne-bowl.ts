/**
 * The Amp Ballantyne ("the Ballantyne Bowl"), Charlotte NC — procedural, CC0-1.0.
 * bun scripts/landmarks/ballantyne-bowl.ts
 *
 * Map frame: x across the stage (east-south-east), y towards the audience
 * (north-east), z up, metres. Placed at bearing 43.3°, the axis of the OSM
 * outline (way/1128360784, 32.0 × 24.0 m, leisure=bandstand). The anchor is
 * the outline's centre, so the model is symmetrical about x = 0.
 *
 * The amphitheatre's stage is one big gesture: a thin, wide roof plate with a
 * timber soffit and a dark edge, held 9 m over the stage on two board-formed
 * concrete pylons and cantilevered about 10 m towards the lawn. The soffit
 * is a shallow inverted hip — deepest over the pylons, rising to a thin edge
 * on every side. Each pylon is a faceted block: plain inner and upper faces,
 * and an outer face that flares out towards the base below a fold line that
 * climbs from the back to the front (Commons photos 04–06).
 *
 * The stage sits on a concrete plinth faced with stacked stone where it
 * meets the pond behind it. The audience sits on lawn terraces with low
 * concrete risers: those are the map's grass, so they are not modelled.
 *
 * OSM parts: the roof (way/1271482543, 10–15 m) and the two pylons
 * (way/1271482544, way/1271482545, 10 m). The pylons' inner faces are moved
 * ~1.5 m inward from OSM, whose parts are guessed from above the roof; the
 * front view (photo 03) puts them under the roof's quarter points.
 *
 * Ground: the pond behind the stage is the low side. The terrain tiles put
 * the lawn edge about 1.5 m above it, so the stage deck at 2.8 m stands
 * about 1.3 m over the front plaza, as in the photos.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

const concrete = new Part(), timber = new Part(), roof = new Part(), fascia = new Part(), stone = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const sub = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]

/** A triangle with per-corner normals, its winding fixed to agree with them. */
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) {
  const face = cross(sub(b, a), sub(c, a))
  if (len(face) < 1e-9) return
  const N = n ?? [unit(face), unit(face), unit(face)]
  if (dot(face, add(add(N[0], N[1]), N[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, N)
  else p.tri(a, c, b, undefined, undefined, undefined, [N[0], N[2], N[1]])
}
/** A flat convex polygon facing `n`. */
function poly(p: Part, pts: V3[], n: V3) {
  const N = [n, n, n]
  for (let i = 1; i < pts.length - 1; i++) tri(p, pts[0], pts[i], pts[i + 1], N)
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}

// ---------------------------------------------------------------------------
// Chamfered rectangles: rings of eight points, each carrying the outward
// normal of its face, so a chamfer shades as a rounded corner.

type Rect = { x0: number; x1: number; y0: number; y1: number; c: number }
function ring(r: Rect, d: number, z: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const c = Math.max(0.02, r.c + d * 0.414)
  const pts: V3[] = [[x0 + c, y0, z], [x1 - c, y0, z], [x1, y0 + c, z], [x1, y1 - c, z],
    [x1 - c, y1, z], [x0 + c, y1, z], [x0, y1 - c, z], [x0, y0 + c, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
/** One band of a swept profile, (d0, z0) to (d1, z1), with (out, up) normals at each end. */
function band(p: Part, r: Rect, d0: number, z0: number, d1: number, z1: number, n0: XY, n1: XY) {
  const a = ring(r, d0, z0), b = ring(r, d1, z1)
  const at = (h: V3, n: XY): V3 => unit([h[0] * n[0], h[1] * n[0], n[1]])
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    quad(p, a.pts[i], a.pts[j], b.pts[j], b.pts[i], [at(a.nrm[i], n0), at(a.nrm[j], n0), at(a.nrm[j], n1), at(a.nrm[i], n1)])
  }
}
function lid(p: Part, r: Rect, d: number, z: number, up: boolean) {
  poly(p, ring(r, d, z).pts, [0, 0, up ? 1 : -1])
}
const OUT: XY = [1, 0], UP: XY = [0, 1], DOWN: XY = [0, -1], S = Math.SQRT1_2

// ---------------------------------------------------------------------------
// The roof plate: a flat top, a thin bevelled edge, and an inverted-hip
// soffit falling from the edge to a flat panel over the pylons.

const ROOF: Rect = { x0: -16, x1: 16, y0: -12, y1: 12, c: 0.5 }
// A 0.7 m edge: the plate reads thin, its dark fascia a crisp line (photos 04–06).
const TOP = 14.55, EDGE_TOP = 14.45, EDGE_BOT = 13.95, SOFFIT_EDGE = 13.85
const LOW: Rect = { x0: -12.8, x1: 12.8, y0: -10.6, y1: 2.3, c: 0.3 }
const SOFFIT_LOW = 11.2

lid(roof, ROOF, -0.1, TOP, true)
band(fascia, ROOF, 0, EDGE_TOP, -0.1, TOP, OUT, UP)
band(fascia, ROOF, 0, EDGE_BOT, 0, EDGE_TOP, OUT, OUT)
band(timber, ROOF, -0.1, SOFFIT_EDGE, 0, EDGE_BOT, [S, -S], OUT)
{
  // The sloping soffit, faceted: each of its four planes keeps its own normal,
  // so the creases from the corners down to the pylons read as in photo 04.
  const a = ring(ROOF, -0.1, SOFFIT_EDGE).pts, b = ring(LOW, 0, SOFFIT_LOW).pts
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    const n = unit(cross(sub(b[j], a[i]), sub(a[j], a[i])))
    quad(timber, a[i], a[j], b[j], b[i], n[2] < 0 ? n : [-n[0], -n[1], -n[2]])
  }
  lid(timber, LOW, 0, SOFFIT_LOW, false)
}

// ---------------------------------------------------------------------------
// The pylons, mirrored. Built for the east one (x > 0) and reflected.

const XI = 7.9, XO = 12.6, XB = 13.8      // inner face; outer face at the top; flared outer foot
const YB = -10.4, YFI = 0.3, YFO = 2.0    // back; front at the inner and the outer corner
const ZT = 10.6                           // top of the concrete, under a steel cap
const FOLD_BACK = 5.5, FOLD_FRONT = 8.5   // the outer face's fold, climbing towards the front

function pylon(side: 1 | -1) {
  const P = (x: number, y: number, z: number): V3 => [side * x, y, z]
  const N = (x: number, y: number, z: number): V3 => unit([side * x, y, z])
  const yFoot = YFI + ((YFO - YFI) * (XB - XI)) / (XO - XI) // the front plane, carried out to the flared foot
  const BIb = P(XI, YB, 0), BIf = P(XI, YFI, 0), BOf = P(XB, yFoot, 0), BOb = P(XB, YB, 0)
  const TIb = P(XI, YB, ZT), TIf = P(XI, YFI, ZT), TOf = P(XO, YFO, ZT), TOb = P(XO, YB, ZT)
  const FOb = P(XO, YB, FOLD_BACK), FOf = P(XO, YFO, FOLD_FRONT)
  poly(concrete, [BIb, BIf, TIf, TIb], N(-1, 0, 0))
  poly(concrete, [BIb, BOb, FOb, TOb, TIb], N(0, -1, 0))
  poly(concrete, [BIf, BOf, FOf, TOf, TIf], N(-(YFO - YFI), XO - XI, 0))
  poly(concrete, [FOb, FOf, TOf, TOb], N(1, 0, 0))
  // The flared lower face is warped; two flat facets, as the real one folds.
  for (const t of [[BOb, BOf, FOf], [BOb, FOf, FOb]] as V3[][]) {
    let n = unit(cross(sub(t[1], t[0]), sub(t[2], t[0])))
    if (n[0] * side < 0) n = [-n[0], -n[1], -n[2]]
    tri(concrete, t[0], t[1], t[2], [n, n, n])
  }
  // The dark steel bearing block between the concrete and the roof.
  const cx = (XI + XO) / 2
  const cap: Rect = { x0: side > 0 ? cx - 2.0 : -cx - 2.0, x1: side > 0 ? cx + 2.0 : -cx + 2.0, y0: YB + 0.6, y1: YFI - 0.4, c: 0.1 }
  band(fascia, cap, 0, ZT - 0.05, 0, SOFFIT_LOW + 0.02, OUT, OUT)
}
pylon(1)
pylon(-1)

// ---------------------------------------------------------------------------
// The stage plinth: a concrete deck, its pond side faced in stacked stone.

const STAGE = 2.8, STONE_TOP = 1.7
const PLINTH: Rect = { x0: -13.2, x1: 13.2, y0: -11.9, y1: 0.9, c: 0.3 }
lid(concrete, PLINTH, -0.15, STAGE, true)
band(concrete, PLINTH, 0, STAGE - 0.15, -0.15, STAGE, OUT, UP)
band(concrete, PLINTH, 0, STONE_TOP, 0, STAGE - 0.15, OUT, OUT)
band(stone, PLINTH, 0, 0, 0, STONE_TOP, OUT, OUT)

// ---------------------------------------------------------------------------

// Colours from photos 04–06 in daylight, on the shared palette. The concrete
// is a pale board-formed grey and the soffit a light warm timber, both kept at
// the palette's lightness. The roof's dark steel edge and the bearing blocks
// are charcoal, the darkest STYLE.md allows for a defining colour (the real
// steel is near-black navy); the deck is a mid grey a step lighter, so from
// above it reads as a dark-edged plate rather than a pale slab.
const parts = [
  { part: concrete, material: finish('concrete', 0xdcd9d2) },
  { part: timber, material: finish('timber', 0xe6c296) },
  { part: roof, material: finish('roof-deck', 0x8d949b) },
  { part: fascia, material: finish('steel-fascia', 0x4a4f57) },
  { part: stone, material: finish('fieldstone', 0xb9b5ad) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Amp Ballantyne', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 43.3, elevation: 0, height: TOP,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/ballantyne-bowl.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
