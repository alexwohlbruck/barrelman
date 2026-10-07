/**
 * EPCOT Japan pavilion: the Goju-no-to pagoda — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-japan-pagoda.ts
 *
 * Map frame: x across the pagoda, y towards its front stairs, z up, metres;
 * origin on the ground at the centre of the OSM footprint (way/295587740,
 * 11.4 × 11.2 m, height=26, roof:colour=blue). The footprint's sides run
 * 359°/89°, and the front stairs face the plaza and the lagoon to the north,
 * so the catalog places it at bearing 359.
 *
 * A five-storey pagoda after the Hōryū-ji type: a granite plinth with a
 * wooden deck and front stairs, then five storeys diminishing upward, each a
 * cream plaster wall framed by dark timber posts with a dark bracket band
 * under deep, slightly upturned hip eaves of blue-grey tile, and a bronze
 * sōrin spire of nine rings and a flame finial on the top roof.
 *
 * Evidence:
 * - OSM: the footprint (taken as the plinth) and the 26 m overall height.
 * - Published: 83 ft (25.3 m) to the top of the spire.
 * - Measured: the storey heights and the eave and wall widths, from a
 *   near-frontal photo ("Japanese pagoda at Epcot.jpg", Benjamin D. Esham,
 *   CC BY-SA 4.0) scaled to the 26 m height, checked against a corner view
 *   ("Pagoda (34163831116).jpg", Sam Howzit, CC BY 2.0) and an older one
 *   ("Japanese pagoda in Walt Disney World.jpg", Rafi B., CC BY 2.0).
 * - Colour: the body is dark-stained timber and cream plaster, not vermilion
 *   (every photo agrees). The roofs were teal-grey before a repaint and are
 *   navy-blue since; the model takes the blue-grey between, muted.
 * - Estimated: the eave lift at the corners, the bracket bands' depth, the
 *   stairs (only the front flight is modelled; the side flights are left out).
 * - Left out: the deck and stair railings, the bells, the latticed doors.
 *   The torii in the lagoon (way/118933968) is a separate structure and is
 *   not part of this model.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const timber = new Part(), plaster = new Part(), tiles = new Part()
const bronze = new Part(), granite = new Part()

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

/** A box centred on (cx, cy), half sizes hx, hy, between z0 and z1. */
function box(p: Part, cx: number, cy: number, hx: number, hy: number, z0: number, z1: number, bottom = false) {
  const c: V3[] = [[cx - hx, cy - hy, z0], [cx + hx, cy - hy, z0], [cx + hx, cy + hy, z0], [cx - hx, cy + hy, z0]]
  const t: V3[] = c.map(([x, y]) => [x, y, z1])
  p.loft([c, t])
  p.cap(t, true)
  if (bottom) p.cap(c, false)
}

/**
 * A square block with a chamfered top edge (STYLE.md's bevel), the lid in
 * `top` — the plinth and the deck.
 */
function bevelBlock(p: Part, h: number, z0: number, z1: number, b: number, top = p) {
  const sq = (s: number, z: number): V3[] => [[-s, -s, z], [s, -s, z], [s, s, z], [-s, s, z]]
  p.loft([sq(h, z0), sq(h, z1 - b), sq(h - b, z1)])
  top.cap(sq(h - b, z1), true)
}

// ---------------------------------------------------------------------------
// Square rings with upturned corners, for the eaves.

/**
 * 16 points round a square of half-width `h` at height z, counter-clockwise
 * from above, each lifted by `lift` times the square of its distance from the
 * middle of its side — so a side sags in the middle and turns up at the
 * corners, the line of a temple eave.
 */
function eaveRing(h: number, z: number, lift: number): V3[] {
  const out: V3[] = []
  const sides: [V3, V3][] = [[[-1, -1, 0], [1, 0, 0]], [[1, -1, 0], [0, 1, 0]], [[1, 1, 0], [-1, 0, 0]], [[-1, 1, 0], [0, -1, 0]]]
  for (const [corner, dir] of sides)
    for (const s of [0, 0.25, 0.5, 0.75]) {
      const u = Math.abs(1 - 2 * s) // 1 at the corner, 0 mid-side
      out.push([h * (corner[0] + 2 * s * dir[0]), h * (corner[1] + 2 * s * dir[1]), z + lift * u * u])
    }
  return out
}

/** A plain square ring with the same 16-point layout, for lofting onto eaves. */
const flatRing = (h: number, z: number) => eaveRing(h, z, 0)

/**
 * A storey: an optional deck band, the plaster wall between timber posts, the
 * dark bracket band, the soffit out to the eave, the eave edge and the tile
 * slope up to the next storey's base (or to the spire's foot at the top).
 */
type Storey = { wall: number; z0: number; deck?: number; band: number; eave: number; eaveZ: number; lift: number; next: number; nextZ: number; doors?: boolean }
function storey(s: Storey) {
  let z = s.z0
  if (s.deck) {
    // The balcony: a timber band a little wider than the wall.
    box(timber, 0, 0, s.wall + 0.25, s.wall + 0.25, z, z + s.deck)
    z += s.deck
  }
  const wallTop = s.eaveZ - s.band
  box(plaster, 0, 0, s.wall, s.wall, z, wallTop)
  // Timber frame: corner posts and two posts per side make three bays, plus a
  // sill and a head beam. Posts stand 0.06 m proud of the plaster.
  const w = s.wall, P = 0.06
  for (const sx of [-1, 1]) for (const sy of [-1, 1]) box(timber, sx * w, sy * w, 0.16, 0.16, z, wallTop)
  for (const t of [-1 / 3, 1 / 3]) {
    box(timber, t * 2 * w, w, 0.11, P, z, wallTop)
    box(timber, t * 2 * w, -w, 0.11, P, z, wallTop)
    box(timber, w, t * 2 * w, P, 0.11, z, wallTop)
    box(timber, -w, t * 2 * w, P, 0.11, z, wallTop)
  }
  const head = wallTop - (wallTop - z) * 0.3
  box(timber, 0, 0, w + P, w + P, head - 0.14, head)
  if (s.doors) {
    // The ground storey's bays are latticed timber doors, not plaster: dark
    // panels in every bay up to the head beam.
    const ph = (2 * w) / 3
    for (const k of [-1, 0, 1]) {
      const c = k * ph, hw = ph / 2 - 0.2
      box(timber, c, w, hw, 0.04, z, head - 0.18)
      box(timber, c, -w, hw, 0.04, z, head - 0.18)
      box(timber, w, c, 0.04, hw, z, head - 0.18)
      box(timber, -w, c, 0.04, hw, z, head - 0.18)
    }
  }
  // Bracket band: dark, stepping out under the eave.
  timber.loft([flatRing(w + 0.1, wallTop), flatRing(w + 0.45, wallTop + s.band * 0.6)])
  // Soffit: from the bracket band out to the eave's underside, which lifts at
  // the corners with the eave.
  timber.loft([flatRing(w + 0.45, wallTop + s.band * 0.6), eaveRing(s.eave, s.eaveZ, s.lift)])
  // Eave edge: a deep timber fascia following the upturn.
  const T = 0.26
  timber.loft([eaveRing(s.eave, s.eaveZ, s.lift), eaveRing(s.eave, s.eaveZ + T, s.lift)])
  // Tiles: a concave hip roof, quick off the eave then steeper.
  const midH = s.eave + (s.next - s.eave) * 0.5
  const midZ = s.eaveZ + T + (s.nextZ - s.eaveZ - T) * 0.32
  tiles.loft([eaveRing(s.eave + 0.02, s.eaveZ + T, s.lift), eaveRing(midH, midZ, s.lift * 0.35), flatRing(s.next, s.nextZ)])
}

// ---------------------------------------------------------------------------
// Plinth, deck and front stairs.

const PLINTH = 5.6
bevelBlock(granite, PLINTH, 0, 1.5, 0.3)
bevelBlock(timber, 4.9, 1.5, 1.85, 0.1)
// Front stairs, on the +y side, stepping down off the plinth.
for (let k = 0; k < 4; k++) {
  const y0 = PLINTH - 0.1, y1 = PLINTH + 0.55 * (k + 1)
  box(granite, 0, (y0 + y1) / 2, 1.5, (y1 - y0) / 2, 0, (1.5 * (4 - k)) / 4)
}

// The five storeys. Wall half-widths and eave heights measured from the
// frontal photo; eaves diminish from 6.0 to 4.3 m half-width.
const S: Storey[] = [
  { wall: 3.45, z0: 1.85, band: 0.55, eave: 6.0, eaveZ: 5.55, lift: 0.65, next: 3.25, nextZ: 7.0, doors: true },
  { wall: 3.0, z0: 7.0, deck: 0.3, band: 0.5, eave: 5.5, eaveZ: 9.35, lift: 0.6, next: 2.95, nextZ: 10.55 },
  { wall: 2.7, z0: 10.55, deck: 0.3, band: 0.5, eave: 5.1, eaveZ: 12.35, lift: 0.56, next: 2.65, nextZ: 13.5 },
  { wall: 2.4, z0: 13.5, deck: 0.3, band: 0.45, eave: 4.7, eaveZ: 15.1, lift: 0.52, next: 2.35, nextZ: 16.25 },
  { wall: 2.15, z0: 16.25, deck: 0.3, band: 0.45, eave: 4.3, eaveZ: 17.8, lift: 0.48, next: 0.5, nextZ: 19.45 },
]
for (const s of S) storey(s)
tiles.cap(flatRing(0.5, 19.45).filter((_, i) => i % 4 === 0), true)

// ---------------------------------------------------------------------------
// The sōrin: a square base, a mast with nine rings, the flame and the jewel.

function cylinder(p: Part, r0: number, r1: number, z0: number, z1: number, seg = 10, caps = true) {
  const ring = (r: number, z: number): V3[] => Array.from({ length: seg }, (_, k) => {
    const a = (k / seg) * 2 * Math.PI
    return [r * Math.cos(a), r * Math.sin(a), z]
  })
  const a = ring(r0, z0), b = ring(r1, z1)
  // Smooth round the side.
  for (let k = 0; k < seg; k++) {
    const l = (k + 1) % seg
    const n = (i: number): V3 => unit([Math.cos((i / seg) * 2 * Math.PI), Math.sin((i / seg) * 2 * Math.PI), (r0 - r1) / Math.max(1e-6, z1 - z0)])
    tri(p, a[k], a[l], b[l], [n(k), n(l), n(l)])
    tri(p, a[k], b[l], b[k], [n(k), n(l), n(k)])
  }
  if (caps) {
    if (r1 > 0) p.cap(b, true)
    if (r0 > 0) p.cap(a, false)
  }
}
box(bronze, 0, 0, 0.55, 0.55, 19.3, 19.75)
cylinder(bronze, 0.55, 0.3, 19.75, 20.1)
cylinder(bronze, 0.14, 0.12, 20.1, 25.0, 8, false)
for (let k = 0; k < 9; k++) {
  const z = 20.35 + k * 0.42
  cylinder(bronze, 0.5, 0.5, z, z + 0.16, 10)
}
// The suien flame: a flat openwork vane, drawn as a solid flattened lozenge
// seen broadside from the front and the back.
{
  const z0 = 24.25, z1 = 25.35, h = 0.48, d = 0.12
  const pts: [number, number][] = [[0, z0], [h, z0 + 0.35], [h * 0.8, z1 - 0.25], [0, z1], [-h * 0.8, z1 - 0.25], [-h, z0 + 0.35]]
  for (const [ax, ay] of [[1, 0], [0, 1]] as [number, number][]) {
    const P = ([u, z]: [number, number], s: number): V3 => [ax * u + ay * s * d, ay * u - ax * s * d, z]
    for (let i = 1; i < pts.length - 1; i++) {
      bronze.tri(P(pts[0], 1), P(pts[i], 1), P(pts[i + 1], 1))
      bronze.tri(P(pts[0], -1), P(pts[i + 1], -1), P(pts[i], -1))
    }
    for (let i = 0; i < pts.length; i++) {
      const j = (i + 1) % pts.length
      bronze.quad(P(pts[i], 1), P(pts[i], -1), P(pts[j], -1), P(pts[j], 1))
    }
  }
}
cylinder(bronze, 0.0, 0.2, 25.4, 25.65, 8, false)
cylinder(bronze, 0.2, 0.0, 25.65, 26.0, 8, false)

// ---------------------------------------------------------------------------
// The palette (landmarks/STYLE.md): the plaster is the palette's `stone`, the
// spire its `metal`. Timber, tile and granite are finishes read off the
// photos — the dark timber against cream plaster is what makes the pagoda
// read, so it stays dark, but no darker than the style's charcoal.
const parts = [
  { part: plaster, material: PALETTE.stone },
  { part: timber, material: finish('pagoda-timber', 0x6e5747) },
  { part: tiles, material: finish('pagoda-tile', 0x6f8199) },
  { part: bronze, material: PALETTE.metal },
  { part: granite, material: finish('pagoda-granite', 0xbcb4a6) },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT Japan pavilion, Goju-no-to pagoda', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 359, osm: 'way/295587740', height: 26,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/wdw-epcot-japan-pagoda.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
