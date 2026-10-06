/**
 * Visulite Theatre, Elizabeth, Charlotte (1938 Art Deco cinema, now a music
 * venue) — procedural, CC0-1.0.
 * bun scripts/landmarks/visulite-theatre.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 46°: the
 * model's +y runs back from Elizabeth Avenue along the outline's long side
 * (way/957552031, 13.6 × 38.3 m), so the street front is the model's south
 * face. The anchor is the outline's centroid.
 *
 * A small club is its front: charcoal-painted brick over a pale blue-grey
 * ground storey; on the west half a projecting marquee box, its white
 * attraction boards in a dark frame, with the name standing on top and a
 * vertical blade at its outer end; on the east half the silver Art Deco
 * eagle over a boarded window, and a striped pier at the east edge. Signs are
 * plain shapes with no lettering; the boards and the marquee soffit are
 * `entrance`, so the marquee lights up at night. Behind the front the
 * auditorium is a plain pale-painted box.
 *
 * The blade: today's front has the name standing on the marquee; older photos
 * show ribbed fins at the marquee's west end (2000s) and white upright pieces
 * on it (2009), after the 1938 front's central "V" tower. It is drawn as one
 * plain vertical fin at the marquee's outer end.
 *
 * The DEM falls 0.9 m from the back to the front-west corner, so y = 0 is the
 * front and the walls simply run up from it.
 *
 * References (visual only): WCCB / Deeandra Michel, "Get to know your city:
 * Elizabeth" (March 2025, the current front); Cinema Treasures photos 215802,
 * 165355, 286221 (older fronts); Flickr "Management's Dark Cloud" (shallowend,
 * 2009, CC BY-NC-SA 2.0) and "Visulite Theater marquee" (comecloser, CC
 * BY-NC-ND 2.0); USGS NAIP (public domain) and the OSM outline for the plan.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const dark = new Part(), pale = new Part(), silver = new Part()
const board = new Part(), win = new Part(), roof = new Part()

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
/** A flat panel on a south-facing wall at y, from (x0, z0) to (x1, z1). */
function southPanel(p: Part, x0: number, x1: number, y: number, z0: number, z1: number) {
  quadN(p, [x0, y, z0], [x1, y, z0], [x1, y, z1], [x0, y, z1], S)
}
/** A flat convex shape on a south-facing wall, corners (x, z) anticlockwise as seen from the street. */
function southShape(p: Part, pts: XY[], y: number) {
  for (let i = 1; i < pts.length - 1; i++) {
    const a = pts[0], b = pts[i], c = pts[i + 1]
    p.tri([a[0], y, a[1]], [b[0], y, b[1]], [c[0], y, c[1]], undefined, undefined, undefined, [S, S, S])
  }
}

// ---------------------------------------------------------------------------
// Dimensions, from the OSM outline and the 2025 street photo.

const X0 = -6.8, X1 = 6.8, Y0 = -19.17, Y1 = 19.17
const Z_BODY = 6.4            // auditorium roof
const Z_FRONT = 7.3           // front parapet
const Z_PALE = 3.3            // top of the pale ground storey
const D = 0.7                 // depth of the front wall's parapet block

// The auditorium, plain painted brick under a flat roof. Its long flanks are
// mostly hidden by neighbours; they take the pale paint so the charcoal stays
// a feature of the front rather than a dark slab down the block.
box(pale, X0, X1, Y0 + D, Y1, 0, Z_BODY, 0.3, roof, [false, true, true, true])
// The front wall: a slab rising above the roof, with a pale coping.
box(dark, X0, X1, Y0, Y0 + D, 0, Z_FRONT - 0.25, 0, dark, [true, true, false, true])
box(silver, X0 - 0.05, X1 + 0.05, Y0 - 0.05, Y0 + D, Z_FRONT - 0.25, Z_FRONT, 0.08, silver, [true, true, true, true])
quadN(dark, [X1, Y0 + D, Z_BODY - 0.3], [X0, Y0 + D, Z_BODY - 0.3], [X0, Y0 + D, Z_FRONT - 0.25], [X1, Y0 + D, Z_FRONT - 0.25], NN)

// The pale ground storey, and a brick ledge course across the upper wall.
const yf = Y0 - 0.03
southPanel(pale, X0, X1 - 1.15, yf, 0, Z_PALE)
box(dark, X0, X1, Y0 - 0.12, Y0, 5.45, 5.7, 0.04, dark, [true, true, false, true])

// The striped pier at the east edge: pale bars on the charcoal.
for (let z = 0.15; z < 5.3; z += 0.55) southPanel(pale, X1 - 1.0, X1 - 0.3, yf, z, z + 0.32)

// The boarded window under the eagle: a dark panel in a raised frame.
box(dark, 0.8, 4.4, Y0 - 0.1, Y0, 0.45, 2.75, 0.03, dark, [true, true, false, true])
southPanel(roof, 1.05, 4.15, Y0 - 0.11, 0.7, 2.5)

// The Art Deco eagle, silver and flat on the wall: a V body and two swept
// wings of stepped feathers, mirrored about its centre line.
{
  const cx = 3.2, y = Y0 - 0.06
  southShape(silver, [[cx - 0.45, 4.85], [cx, 3.6], [cx + 0.45, 4.85]], y)
  southShape(silver, [[cx - 0.2, 3.6], [cx + 0.2, 3.6], [cx + 0.3, 3.95], [cx - 0.3, 3.95]], y)
  for (const s of [-1, 1]) {
    // Three feather rows, each a quad from the shoulder out to the tip.
    const rows: [number, number, number][] = [[4.9, 2.1, 5.45], [4.6, 1.8, 5.0], [4.3, 1.4, 4.55]]
    for (const [zt, reach, ztip] of rows) {
      const a: XY = [cx + s * 0.4, zt - 0.05], b: XY = [cx + s * (0.4 + reach), ztip], c: XY = [cx + s * (0.4 + reach - 0.25), ztip - 0.35], d: XY = [cx + s * 0.4, zt - 0.45]
      // Anticlockwise as seen from the street, so each wing faces out.
      southShape(silver, s > 0 ? [d, c, b, a] : [a, b, c, d], y - 0.01)
    }
  }
}

// Doors and the poster case on the pale storey, under the marquee.
southPanel(dark, X0 + 0.15, X0 + 1.1, Y0 - 0.05, 0, 2.4)
southPanel(win, -4.0, -2.9, Y0 - 0.05, 0.9, 1.75)

// The marquee: a box off the wall over the west half, its attraction boards
// on the front and east end in a dark frame, and a lit soffit.
const MX0 = X0 + 0.1, MX1 = 0.5, MY = Y0 - 2.4, MZ0 = 3.2, MZ1 = 4.9
{
  box(dark, MX0, MX1, MY, Y0, MZ0, MZ1, 0.12, roof, [true, true, false, true], board)
  const fy = MY - 0.04
  southPanel(board, MX0 + 0.5, MX1 - 0.25, fy, MZ0 + 0.25, MZ1 - 0.3)
  // The east end board.
  quadN(board, [MX1 + 0.04, MY + 0.3, MZ0 + 0.25], [MX1 + 0.04, Y0 - 0.3, MZ0 + 0.25], [MX1 + 0.04, Y0 - 0.3, MZ1 - 0.3], [MX1 + 0.04, MY + 0.3, MZ1 - 0.3], E)
  // The name, standing on the marquee as one plain pale panel on a dark back.
  box(dark, -5.5, -1.6, MY + 1.0, MY + 1.25, MZ1 - 0.05, MZ1 + 1.15, 0.06, dark)
  southPanel(silver, -5.35, -1.75, MY + 0.97, MZ1 + 0.1, MZ1 + 1.0)
}

// The vertical blade at the marquee's outer west end: a thin upright fin
// standing out from the wall, pale faces in a dark frame.
{
  const x0 = MX0 + 0.05, x1 = MX0 + 0.45, y0 = MY + 0.2, y1 = Y0, z0 = MZ0 - 0.4, z1 = 8.6
  box(dark, x0, x1, y0, y1, z0, z1, 0.08, dark, [true, true, false, true], dark)
  quadN(board, [x0 - 0.03, y1 - 0.25, z0 + 0.4], [x0 - 0.03, y0 + 0.25, z0 + 0.4], [x0 - 0.03, y0 + 0.25, z1 - 0.35], [x0 - 0.03, y1 - 0.25, z1 - 0.35], W)
  quadN(board, [x1 + 0.03, y0 + 0.25, z0 + 0.4], [x1 + 0.03, y1 - 0.25, z0 + 0.4], [x1 + 0.03, y1 - 0.25, z1 - 0.35], [x1 + 0.03, y0 + 0.25, z1 - 0.35], E)
}

// ---------------------------------------------------------------------------

// Colours from the 2025 photo: charcoal paint (a defining dark, kept at the
// palette's floor), the pale blue-grey ground storey, brushed silver for the
// eagle, coping and name, white attraction boards.
const parts = [
  { part: dark, material: finish('charcoal-paint', 0x5a5f67) },
  { part: pale, material: finish('pale-paint', 0xc9d3db) },
  { part: silver, material: finish('silver', 0xd9dde0, 0.5) },
  { part: board, material: PALETTE.entrance },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Visulite Theatre', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 46, osm: 'way/957552031', height: 8.6,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/visulite-theatre.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
