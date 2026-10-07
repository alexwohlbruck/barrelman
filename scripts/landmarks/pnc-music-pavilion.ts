/**
 * PNC Music Pavilion (Truliant Amphitheater since 2025), Charlotte NC — procedural, CC0-1.0.
 * bun scripts/landmarks/pnc-music-pavilion.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The venue's axis runs
 * due east–west: the stage house is at the east end and faces west, over the
 * covered reserved seats, towards the lawn. The anchor is the middle of the
 * pavilion roof and stage house together (OSM way/577606075 and
 * way/577606076), on the axis, so the model is symmetrical about y = 0.
 *
 * The pavilion is one big flat roof, an octagon about 63 × 81 m in plan,
 * carried at the stage end on the stage house's flanking walls and at the
 * lawn end on four columns. Its steel deck sits on deep painted trusses
 * spanning north–south, which are what you see from the seats (2009 and 2016
 * photos). Here the deck is a thin pale slab with a white edge, and the truss
 * zone under it a recessed band with the trusses as deep light beams, so the
 * roof reads as a lid on visible structure rather than a solid block.
 *
 * The stage house is pale cream concrete (the same in every interior photo):
 * a fly tower over the proscenium standing above the roof, and lower wings
 * north and south whose west faces are the tall side walls the video screens
 * hang on. The proscenium is a dark opening between broad piers.
 *
 * The reserved seats are concrete on grade, sloping about 3 m up to the
 * lawn; the lawn is grass. Both are the map's ground, so neither is modelled.
 *
 * Ground: the terrain under the footprint is lowest just behind the stage
 * (~193.8 m). The stage floor is ~1.4 m above that and the back row of seats
 * under the roof's west edge ~4.2 m, from the terrarium DEM.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const stone = new Part(), roofTop = new Part(), membrane = new Part(), steel = new Part(), soffit = new Part(), stage = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const sub = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle facing `n`, its winding fixed to agree. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3) {
  const f = cross(sub(b, a), sub(c, a))
  if (len(f) < 1e-9) return
  const N = [n, n, n]
  if (dot(f, n) >= 0) p.tri(a, b, c, undefined, undefined, undefined, N)
  else p.tri(a, c, b, undefined, undefined, undefined, N)
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3) { tri(p, a, b, c, n); tri(p, a, c, d, n) }
function fan(p: Part, pts: V3[], n: V3) { for (let i = 1; i < pts.length - 1; i++) tri(p, pts[0], pts[i], pts[i + 1], n) }

/** Counter-clockwise convex polygon, offset inward by d (outward if negative). */
function inset(pts: XY[], d: number): XY[] {
  const n = pts.length
  const lines = pts.map((a, i) => {
    const b = pts[(i + 1) % n], dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy)
    const nx = -dy / l, ny = dx / l // inward normal for CCW
    return { p: [a[0] + nx * d, a[1] + ny * d] as XY, d: [dx / l, dy / l] as XY }
  })
  return lines.map((L, i) => {
    const M = lines[(i - 1 + n) % n]
    const det = M.d[0] * L.d[1] - M.d[1] * L.d[0]
    if (Math.abs(det) < 1e-9) return L.p
    const t = ((L.p[0] - M.p[0]) * L.d[1] - (L.p[1] - M.p[1]) * L.d[0]) / det
    return [M.p[0] + M.d[0] * t, M.p[1] + M.d[1] * t] as XY
  })
}
const at = (q: XY, z: number): V3 => [q[0], q[1], z]
const outward = (a: XY, b: XY): V3 => unit([b[1] - a[1], -(b[0] - a[0]), 0])

/** Vertical walls of a CCW polygon from z0 to z1. */
function walls(p: Part, pts: XY[], z0: number, z1: number, inward = false) {
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length]
    let n = outward(a, b)
    if (inward) n = [-n[0], -n[1], 0]
    quad(p, at(a, z0), at(b, z0), at(b, z1), at(a, z1), n)
  }
}
/** A 45° chamfer band from the outline at z0 to the outline inset by c at z0 + c. */
function chamfer(p: Part, pts: XY[], z0: number, c: number) {
  const q = inset(pts, c)
  for (let i = 0; i < pts.length; i++) {
    const j = (i + 1) % pts.length, o = outward(pts[i], pts[j])
    quad(p, at(pts[i], z0), at(pts[j], z0), at(q[j], z0 + c), at(q[i], z0 + c), unit([o[0], o[1], 1]))
  }
  return q
}
/** A bevelled prism: walls to z1 - c, a chamfer, and a lid. */
function block(side: Part, top: Part, pts: XY[], z0: number, z1: number, c = 0.4) {
  walls(side, pts, z0, z1 - c)
  const q = chamfer(side, pts, z1 - c, c)
  fan(top, q.map((v) => at(v, z1)), [0, 0, 1])
}
const rect = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
const mirror = (pts: XY[]): XY[] => pts.map(([x, y]) => [x, -y] as XY).reverse()

// ---------------------------------------------------------------------------
// Heights above the lowest ground (just behind the stage).

const STAGE_FLOOR = 2.6
const TRUSS_BOT = 17.4    // underside of the trusses, flush with the side walls' tops
const ZONE_BOT = 18.0     // underside of the deck's recessed truss band
const DECK_BOT = 19.9     // underside of the white deck edge
const DECK_TOP = 21.2
const WING_TOP = 18.0
const FLY_TOP = 30.0
const DOCK_TOP = 11.0

// ---------------------------------------------------------------------------
// The roof: the OSM octagon, made symmetrical about the axis.

const ROOF: XY[] = [
  [-42.0, -16.2], [-31.0, -40.6], [5.0, -40.6], [20.7, -16.4],
  [20.7, 16.4], [5.0, 40.6], [-31.0, 40.6], [-42.0, 16.2],
]
{
  // The deck: a slab with a light steel edge, a soft top bevel, and the
  // white membrane that makes the roof a bright octagon from above (NAIP).
  walls(steel, ROOF, DECK_BOT, DECK_TOP - 0.45)
  const q = chamfer(steel, ROOF, DECK_TOP - 0.45, 0.45)
  fan(membrane, q.map((v) => at(v, DECK_TOP)), [0, 0, 1])
  // The deck's underside, visible where the truss band is set back.
  const band = inset(ROOF, 1.4)
  for (let i = 0; i < ROOF.length; i++) {
    const j = (i + 1) % ROOF.length
    quad(soffit, at(ROOF[i], DECK_BOT), at(ROOF[j], DECK_BOT), at(band[j], DECK_BOT), at(band[i], DECK_BOT), [0, 0, -1])
  }
  // The truss band: a shadowed recess, closed underneath by the deck soffit.
  walls(soffit, band, ZONE_BOT, DECK_BOT)
  fan(soffit, band.map((v) => at(v, ZONE_BOT)), [0, 0, -1])
}

/** The roof's half-width at x, from its outline. */
function halfWidth(x: number) {
  const pts = inset(ROOF, 0.6)
  let best = 0
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length]
    if ((a[0] - x) * (b[0] - x) > 0 || a[0] === b[0]) continue
    const t = (x - a[0]) / (b[0] - a[0])
    best = Math.max(best, Math.abs(a[1] + (b[1] - a[1]) * t))
  }
  return best
}

// The trusses: deep light beams spanning north–south under the deck, their
// ends showing in the recessed band all round the long sides.
const TRUSS_W = 0.9
for (const x of [-37, -28, -19, -10, -1, 8, 16]) {
  const h = Math.min(halfWidth(x - TRUSS_W / 2), halfWidth(x + TRUSS_W / 2))
  block(steel, steel, rect(x - TRUSS_W / 2, x + TRUSS_W / 2, -h, h), TRUSS_BOT, DECK_BOT, 0.15)
  fan(steel, rect(x - TRUSS_W / 2, x + TRUSS_W / 2, -h, h).map((v) => at(v, TRUSS_BOT)), [0, 0, -1])
}
// The two edge trusses along the open lawn side, east–west under the
// chamfered corners, tying the column heads together.
for (const s of [1, -1]) {
  const a: XY = [-40.5, s * 15.6], b: XY = [-30.2, s * 38.6]
  const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy), nx = -dy / l * 0.45, ny = dx / l * 0.45
  let pts: XY[] = [[a[0] - nx, a[1] - ny], [b[0] - nx, b[1] - ny], [b[0] + nx, b[1] + ny], [a[0] + nx, a[1] + ny]]
  if (s < 0) pts = pts.reverse()
  const area = pts.reduce((sum, p, i) => { const q = pts[(i + 1) % 4]; return sum + p[0] * q[1] - q[0] * p[1] }, 0)
  if (area < 0) pts = pts.reverse()
  block(steel, steel, pts, TRUSS_BOT, DECK_BOT, 0.15)
  fan(steel, pts.map((v) => at(v, TRUSS_BOT)), [0, 0, -1])
}
// A longitudinal truss along the axis, carrying the lighting bridge.
block(steel, steel, rect(-41.3, 20.6, -0.45, 0.45), TRUSS_BOT + 0.2, ZONE_BOT + 0.01, 0.15)
fan(steel, rect(-41.3, 20.6, -0.45, 0.45).map((v) => at(v, TRUSS_BOT + 0.2)), [0, 0, -1])

// The columns at the lawn end: square, bevelled, under the corner trusses.
for (const [x, y] of [[-40.0, 15.2], [-40.0, -15.2], [-30.6, 37.6], [-30.6, -37.6]] as XY[]) {
  const c = 0.9
  block(steel, steel, rect(x - c, x + c, y - c, y + c), 0, TRUSS_BOT + 0.01, 0.3)
}

// ---------------------------------------------------------------------------
// The stage house, cream concrete.

// The fly tower over the stage, rising above the roof: about 17 m deep
// from the proscenium, as its grey roof shows in the orthophoto.
// Its shadow falls ~14 m across the north wing's roof, so it stands well
// clear of the pavilion roof. Its light roof carries two rows of four hatches.
const FLY = rect(20.7, 37.4, -16.4, 16.4)
block(stone, membrane, FLY, 0, FLY_TOP, 0.5)
for (const x of [24.6, 30.2]) for (const y of [-10.3, -4.6, 4.6, 10.3]) {
  block(roofTop, roofTop, rect(x - 1.1, x + 1.1, y - 1.1, y + 1.1), FLY_TOP - 0.1, FLY_TOP + 0.8, 0.2)
}
// The backstage block behind it, lower.
block(stone, roofTop, [[37.4, -16.4], [41.0, -16.4], [41.0, -15.8], [45.1, -15.8], [45.1, 15.8], [41.0, 15.8], [41.0, 16.4], [37.4, 16.4]], 0, DOCK_TOP, 0.4)
// The wings: their west faces are the side walls flanking the seats.
const WING_N: XY[] = [[20.7, 16.4], [41.0, 16.4], [41.0, 25.0], [35.0, 36.0], [28.5, 45.6], [10.4, 33.2]]
// The north wing's roof is white like the pavilion's; the south one's dark.
block(stone, membrane, WING_N, 0, WING_TOP, 0.4)
block(stone, roofTop, mirror(WING_N), 0, WING_TOP, 0.4)

// The proscenium opening: a dark recess between broad piers, under the roof.
{
  const x = 20.7 - 0.04, y = 11.8, z0 = STAGE_FLOOR, z1 = 16.6
  quad(stage, [x, -y, z0], [x, y, z0], [x, y, z1], [x, -y, z1], [-1, 0, 0])
  // The stage lip below it, a pale step out into the pit.
  block(stone, stone, rect(17.6, 20.7, -y, y), 0, STAGE_FLOOR, 0.15)
}

// ---------------------------------------------------------------------------

// Colours from the 2009 and 2016 photos and the USGS NAIP orthophoto, on the
// shared palette. The concrete reads a warm cream in every shot, so it is
// `stone`. The pavilion roof is a white membrane, kept a neutral off-white.
// The trusses and deck edge are painted a light grey; the deck's underside
// and the shadowed truss band are the palette's roof grey a step darker. The
// proscenium is the stage's black, held at charcoal.
const parts = [
  { part: stone, material: PALETTE.stone },
  { part: roofTop, material: PALETTE.roof },
  { part: membrane, material: finish('membrane', 0xeceeec) },
  { part: steel, material: finish('steel', 0xd3d6d6) },
  { part: soffit, material: finish('roof-soffit', 0x7f878e) },
  { part: stage, material: finish('stage', 0x4a4f57) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('PNC Music Pavilion', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: FLY_TOP,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/pnc-music-pavilion.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
