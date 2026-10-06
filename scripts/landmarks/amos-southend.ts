/**
 * Amos' Southend, South End, Charlotte (music hall, 1423 S Tryon St) —
 * procedural, CC0-1.0.
 * bun scripts/landmarks/amos-southend.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 130°: the
 * model's +y runs back from South Tryon Street along the outline's long side
 * (way/432937734, 20 × 44 m), so the Tryon front is the model's south face and
 * the parking lot lies along its east (+x) face. The anchor is the outline's
 * centroid.
 *
 * One building holds two venues. OSM tags way/432937734 "The Gin Mill"; it was
 * "Amos' South End" until 2023, and Amos' is now a point
 * (node/11291263920) inside the same outline. Since 2017 the Gin Mill has held
 * the front block on Tryon and Amos' the hall behind it, so the model is the
 * whole outline:
 *
 * - the front block: one storey of red brick under a deep, thin flat roof
 *   that overhangs the Tryon front and the parking side, big black-framed
 *   windows, a stair up the parking side to the rooftop patio, and the Gin
 *   Mill's blade sign;
 * - the hall: a taller white-painted block box under a shallow barrel roof,
 *   a few small windows and a door on the parking side, and the AMOS' blade
 *   sign at its front corner where it steps out past the Gin Mill.
 *
 * Signs are plain shapes, no lettering; their faces are `entrance`, so they
 * light at night. The ground falls 0.9 m from Tryon to the back, so the front
 * stands on G.
 *
 * References (visual only, none openly licensed): Nancy Pierce for
 * southendclt.org (the Gin Mill's brick front and eave); Charlotte Stories
 * (the 2017 Gin Mill rendering, and a 2016 view from above showing the hall's
 * barrel roof); WBTV (the pre-2017 front); amossouthend.com (the AMOS' blade
 * sign). USGS NAIP (public domain) and the OSM outline for the plan.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const brick = new Part(), paint = new Part(), roof = new Part()
const win = new Part(), lit = new Part(), metal = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3) {
  const N = n && [n, n, n] as V3[]
  p.tri(a, b, c, undefined, undefined, undefined, N)
  p.tri(a, c, d, undefined, undefined, undefined, N)
}
const S: V3 = [0, -1, 0], E: V3 = [1, 0, 0], W: V3 = [-1, 0, 0], NN: V3 = [0, 1, 0], UP: V3 = [0, 0, 1], DN: V3 = [0, 0, -1]

/** An axis-aligned box with a chamfer of b round its top edge; sides S, E, N, W. */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, b = 0, top: Part = p, sides = [true, true, true, true], bottom?: Part) {
  const zt = z1 - b
  if (sides[0]) quadN(p, [x0, y0, z0], [x1, y0, z0], [x1, y0, zt], [x0, y0, zt], S)
  if (sides[1]) quadN(p, [x1, y0, z0], [x1, y1, z0], [x1, y1, zt], [x1, y0, zt], E)
  if (sides[2]) quadN(p, [x1, y1, z0], [x0, y1, z0], [x0, y1, zt], [x1, y1, zt], NN)
  if (sides[3]) quadN(p, [x0, y1, z0], [x0, y0, z0], [x0, y0, zt], [x0, y1, zt], W)
  if (b > 0) {
    const i0 = x0 + b, i1 = x1 - b, j0 = y0 + b, j1 = y1 - b
    quadN(p, [x0, y0, zt], [x1, y0, zt], [i1, j0, z1], [i0, j0, z1], unit([0, -1, 1]))
    quadN(p, [x1, y0, zt], [x1, y1, zt], [i1, j1, z1], [i1, j0, z1], unit([1, 0, 1]))
    quadN(p, [x1, y1, zt], [x0, y1, zt], [i0, j1, z1], [i1, j1, z1], unit([0, 1, 1]))
    quadN(p, [x0, y1, zt], [x0, y0, zt], [i0, j0, z1], [i0, j1, z1], unit([-1, 0, 1]))
    quadN(top, [i0, j0, z1], [i1, j0, z1], [i1, j1, z1], [i0, j1, z1], UP)
  } else quadN(top, [x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1], UP)
  if (bottom) quadN(bottom, [x0, y1, z0], [x1, y1, z0], [x1, y0, z0], [x0, y0, z0], DN)
}
/** Flat panels on the four wall directions, a few cm proud. */
const southPanel = (p: Part, x0: number, x1: number, y: number, z0: number, z1: number) => quadN(p, [x0, y, z0], [x1, y, z0], [x1, y, z1], [x0, y, z1], S)
const eastPanel = (p: Part, x: number, y0: number, y1: number, z0: number, z1: number) => quadN(p, [x, y0, z0], [x, y1, z0], [x, y1, z1], [x, y0, z1], E)
const westPanel = (p: Part, x: number, y0: number, y1: number, z0: number, z1: number) => quadN(p, [x, y1, z0], [x, y0, z0], [x, y0, z1], [x, y1, z1], W)

// ---------------------------------------------------------------------------
// Dimensions, from the OSM outline in the model frame and the photos.

const G = 0.8                 // Tryon sidewalk above the lowest ground, at the back
// Front block (the Gin Mill).
const FX0 = -10.9, FX1 = 7.7, FY0 = -22.4, FY1 = -10.2
const Z_F = G + 4.4           // its walls
// Hall (Amos').
const HX0 = -9.5, HX1 = 10.7, HY0 = -10.2, HY1 = 21.6
const Z_H = 7.0               // hall eaves
const RISE = 1.6              // barrel roof rise, to about OSM's 8 m above Tryon
const SEG = 8                 // segments across the barrel

// ---- Front block: brick, with the notch on its west side from the outline.
box(brick, FX0, FX1, FY0, -12.8, 0, Z_F, 0, roof, [true, true, false, true])
box(brick, HX0, FX1, -12.8, FY1, 0, Z_F, 0, roof, [false, true, false, false])
quadN(brick, [FX0, -12.8, 0], [HX0, -12.8, 0], [HX0, -12.8, Z_F], [FX0, -12.8, Z_F], NN)
westPanel(brick, HX0, -12.8, FY1, 0, Z_F)

// The deep flat roof: a thin plate overhanging Tryon and the parking side.
{
  const o = 1.6, t = 0.35
  box(metal, FX0 - 0.2, FX1 + o, FY0 - o, FY1, Z_F, Z_F + t, 0.08, roof, [true, true, false, true], roof)
}

// Tryon front: three big black-framed windows and the door.
{
  const y = FY0 - 0.03
  for (const [a, b] of [[-9.6, -6.0], [-4.8, -1.2], [2.6, 6.4]] as XY[]) {
    southPanel(metal, a - 0.15, b + 0.15, y, G + 0.55, G + 3.35)
    southPanel(win, a, b, y - 0.02, G + 0.7, G + 3.2)
  }
  southPanel(lit, 0.0, 1.6, y, G, G + 2.4)
}
// Parking side of the front block: a door and a window under the eave, and
// the stair up to the rooftop patio, run as a sloped block along the wall.
{
  const x = FX1 + 0.03
  eastPanel(lit, x, -16.0, -14.9, G, G + 2.4)
  eastPanel(win, x, -13.9, -11.6, G + 1.0, G + 2.6)
  // The stair: from the sidewalk near the hall up to the roof edge.
  const w = 1.2, x0 = FX1, x1 = FX1 + w, ya = -11.0, yb = -17.0
  const pts: V3[] = [[x1, ya, G], [x1, yb, Z_F], [x0, yb, Z_F], [x0, ya, G]]
  // Wound so the flight's face points up and out; mesh.ts takes its normal from the winding.
  quadN(metal, pts[3], pts[2], pts[1], pts[0])
  quadN(metal, [x1, ya, G], [x1, ya, 0], [x1, yb, 0], [x1, yb, Z_F], E)
}

// The Gin Mill's blade sign, off the front block's parking-side corner.
{
  const y = FY0 + 1.2, x0 = FX1 + 1.2, x1 = FX1 + 0.05, z0 = G + 1.9, z1 = G + 4.0
  box(metal, x1, x0, y - 0.13, y + 0.13, z0, z1, 0.03, metal, [true, true, true, false], metal)
  quadN(lit, [x1 + 0.12, y - 0.15, z0 + 0.12], [x0 - 0.1, y - 0.15, z0 + 0.12], [x0 - 0.1, y - 0.15, z1 - 0.12], [x1 + 0.12, y - 0.15, z1 - 0.12], S)
  quadN(lit, [x0 - 0.1, y + 0.15, z0 + 0.12], [x1 + 0.12, y + 0.15, z0 + 0.12], [x1 + 0.12, y + 0.15, z1 - 0.12], [x0 - 0.1, y + 0.15, z1 - 0.12], NN)
}

// ---- The hall: white-painted block walls under a shallow barrel roof.
box(paint, HX0, HX1, HY0, HY1, 0, Z_H, 0, paint, [true, true, true, true])
{
  const W2 = (HX1 - HX0) / 2, xc = (HX0 + HX1) / 2
  // A circular arc through the eaves and the crown.
  const R = (W2 * W2 + RISE * RISE) / (2 * RISE), half = Math.asin(W2 / R)
  const arc = Array.from({ length: SEG + 1 }, (_, i) => {
    const t = -half + (2 * half * i) / SEG
    // At t = ±half this is the eave line; at t = 0 it is RISE higher.
    return { x: xc + R * Math.sin(t), z: Z_H + R * (Math.cos(t) - Math.cos(half)), n: unit([Math.sin(t), 0, Math.cos(t)]) }
  })
  const ov = 0.3 // a little overhang at the gable ends and eaves
  for (let i = 0; i < SEG; i++) {
    const a = arc[i], b = arc[i + 1]
    roof.tri([a.x, HY0 - ov, a.z], [b.x, HY0 - ov, b.z], [b.x, HY1 + ov, b.z], undefined, undefined, undefined, [a.n, b.n, b.n])
    roof.tri([a.x, HY0 - ov, a.z], [b.x, HY1 + ov, b.z], [a.x, HY1 + ov, a.z], undefined, undefined, undefined, [a.n, b.n, a.n])
  }
  // The gable ends: fans from the eave line up to the arc, front and back.
  for (const [y, n] of [[HY0, S], [HY1, NN]] as [number, V3][]) {
    for (let i = 0; i < SEG; i++) {
      const a = arc[i], b = arc[i + 1]
      const A: V3 = [a.x, y, Z_H], B: V3 = [b.x, y, Z_H], C: V3 = [b.x, y, b.z - 0.02], D: V3 = [a.x, y, a.z - 0.02]
      if (n === S) quadN(paint, A, B, C, D, S)
      else quadN(paint, B, A, D, C, NN)
    }
  }
  // A thin roof edge under the overhang, so the barrel reads as a lid.
  box(roof, HX0 - 0.15, HX1 + 0.15, HY0 - 0.3, HY1 + 0.3, Z_H - 0.3, Z_H, 0, roof, [true, true, true, true], roof)
}

// Rooftop units on the hall, as in the view from above.
box(metal, -3.5, -0.5, 2, 5, Z_H + RISE - 0.1, Z_H + RISE + 1.0, 0.1, roof)
box(metal, 2.0, 4.2, 9, 11.5, Z_H + RISE - 0.3, Z_H + RISE + 0.7, 0.1, roof)

// The hall's front strip, where it steps out past the Gin Mill on the parking
// side: Amos' doors, brick below the white wall as in the photos.
{
  const y = HY0 - 0.03
  southPanel(brick, FX1, HX1, y, 0, G + 3.2)
  southPanel(lit, FX1 + 0.7, HX1 - 0.7, y - 0.02, G, G + 2.4)
}
// The parking side of the hall: small windows and a side door.
{
  const x = HX1 + 0.03
  for (const y of [-3, 4, 11]) eastPanel(win, x, y, y + 1.6, 3.0, 4.4)
  eastPanel(lit, x, 16.5, 18.0, 0, 2.3)
  // The back wall: a loading door.
  quadN(metal, [HX1 - 3, HY1 + 0.03, 0], [HX1 - 7, HY1 + 0.03, 0], [HX1 - 7, HY1 + 0.03, 3.6], [HX1 - 3, HY1 + 0.03, 3.6], NN)
  // The north-east side, as in the 2016 view from above: three windows and a door.
  for (const y of [-6, 0, 6]) westPanel(win, HX0 - 0.03, y, y + 1.6, 3.0, 4.4)
  westPanel(lit, HX0 - 0.03, 12, 13.4, 0, 2.3)
}

// The AMOS' blade sign: a tall upright board off the hall's front corner on
// the parking side, seen from Tryon.
{
  const y = HY0 + 0.6, x1 = HX1 + 0.05, x0 = HX1 + 1.3, z0 = G + 2.6, z1 = Z_H - 0.4
  box(metal, x1, x0, y - 0.15, y + 0.15, z0, z1, 0.04, metal, [true, true, true, false], metal)
  quadN(lit, [x1 + 0.12, y - 0.17, z0 + 0.15], [x0 - 0.12, y - 0.17, z0 + 0.15], [x0 - 0.12, y - 0.17, z1 - 0.15], [x1 + 0.12, y - 0.17, z1 - 0.15], S)
  quadN(lit, [x0 - 0.12, y + 0.17, z0 + 0.15], [x1 + 0.12, y + 0.17, z0 + 0.15], [x1 + 0.12, y + 0.17, z1 - 0.15], [x0 - 0.12, y + 0.17, z1 - 0.15], NN)
}

// ---------------------------------------------------------------------------

// Colours from the photos: the Gin Mill's red brick, pulled to the palette's
// lightness; the hall's white paint as `stone`; dark bronze-grey for the eave,
// frames and signs' cases; slate windows; lit sign faces and doors.
const parts = [
  { part: brick, material: finish('red-brick', 0xbd7a63) },
  { part: paint, material: PALETTE.stone },
  { part: roof, material: PALETTE.roof },
  { part: metal, material: finish('dark-frame', 0x5c6168) },
  { part: win, material: PALETTE.window },
  { part: lit, material: PALETTE.entrance },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb("Amos' Southend", parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 130, osm: 'way/432937734', height: Z_H + RISE + 1.0,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/amos-southend.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
