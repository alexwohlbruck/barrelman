/**
 * Ovens Auditorium, Charlotte (1955, A. G. Odell Jr. & Associates) — procedural, CC0-1.0.
 * bun scripts/landmarks/ovens-auditorium.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 42°: the model's
 * +y runs north-east along the building's axis, from the stage house at the
 * back to the lobby on the entrance plaza facing Independence Boulevard. The
 * anchor is the centroid of the OSM outline (way/836535412). The BOplex
 * Connector (way/918754955) that joins it to the coliseum is left out; the
 * neighbouring `bojangles-coliseum` model stands for the dome.
 *
 * Read from front to back:
 * - the lobby: a long two-storey box whose upper floor is a continuous band
 *   of tall glass between a thin cream floor slab and a thin cream fascia,
 *   floating over a recessed glazed ground floor. A flat canopy on four
 *   square columns projects over the doors in the middle (OSM's 20 × 6 m
 *   front bump). The south-east end of the upper floor is a solid wall of
 *   turquoise glazed tile, with a low cream box beside it;
 * - the auditorium: a tall, windowless block of buff precast panels whose
 *   front wall bows out in plan and rises well over the lobby, and whose
 *   south-east side fans out towards the front (the curve in the OSM outline);
 * - the stage house: the tallest block, plain panels, with low back-of-house
 *   wings round it and a low wing down the north-west side.
 *
 * Heights are not in OSM. From the photos: lobby ~11 m over the plaza, the
 * auditorium about twice that, and the stage house some 7 m higher again
 * (its shadow across the auditorium roof in the NAIP image). The terrain
 * falls about 1.8 m from the plaza to the back corner (Terrarium z15), so
 * y = 0 is the back and the plaza sits at G.
 *
 * References: USGS NAIP orthoimagery (public domain) for the plan and roof;
 * James Willamor's 2007 aerial on Commons (CC BY-SA 3.0); and, as visual
 * reference only, the venue's own photos of the front and south-east corner
 * and its 1955 opening photograph (boplex.com).
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, windowVariant } from './palette'

const panel = new Part(), trim = new Part(), roof = new Part()
const tile = new Part(), win = new Part(), glassUp = new Part()

type XY = [number, number]
const G = 1.6 // plaza level above the model's y = 0

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3[]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}

// ---------------------------------------------------------------------------
// Polygon helpers: rings are counter-clockwise from above.

function area(r: XY[]) { let a = 0; r.forEach((p, i) => { const q = r[(i + 1) % r.length]; a += p[0] * q[1] - q[0] * p[1] }); return a / 2 }
function triangulate(r: XY[]): [number, number, number][] {
  const idx = r.map((_, i) => i), out: [number, number, number][] = []
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => cr(a, b, p) > 1e-9 && cr(b, c, p) > 1e-9 && cr(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 2000) {
    for (let i = 0; i < idx.length; i++) {
      const a = idx[(i + idx.length - 1) % idx.length], b = idx[i], c = idx[(i + 1) % idx.length]
      if (cr(r[a], r[b], r[c]) <= 1e-9) continue
      if (idx.some((j) => j !== a && j !== b && j !== c && inside(r[j], r[a], r[b], r[c]))) continue
      out.push([a, b, c]); idx.splice(i, 1); break
    }
  }
  out.push([idx[0], idx[1], idx[2]])
  return out
}
function offset(r: XY[], d: number): XY[] {
  const n = r.length
  return r.map((p, i) => {
    const a = r[(i + n - 1) % n], b = r[(i + 1) % n]
    const e1 = unit([p[0] - a[0], p[1] - a[1], 0]), e2 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n1: XY = [e1[1], -e1[0]], n2: XY = [e2[1], -e2[0]]
    const m = unit([n1[0] + n2[0], n1[1] + n2[1], 0]), cos = m[0] * n1[0] + m[1] * n1[1]
    const k = d / Math.max(cos, 0.3)
    return [p[0] + m[0] * k, p[1] + m[1] * k]
  })
}
const edgeN = (a: XY, b: XY): V3 => unit([b[1] - a[1], -(b[0] - a[0]), 0])
/** Vertex normals: averaged where the turn is gentle (a curve), per edge at corners. */
function vertexNormals(r: XY[], smooth: boolean[]) {
  const n = r.length
  return r.map((p, i) => {
    const a = edgeN(r[(i + n - 1) % n], p), b = edgeN(p, r[(i + 1) % n])
    return smooth[i] ? unit([a[0] + b[0], a[1] + b[1], 0]) : null
  })
}
function walls(p: Part, r: XY[], z0: number, z1: number, smooth?: boolean[]) {
  const n = r.length, vn = smooth ? vertexNormals(r, smooth) : r.map(() => null)
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, a = r[i], b = r[j], E = edgeN(a, b)
    const na = vn[i] ?? E, nb = vn[j] ?? E
    quadN(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [na, nb, nb, na])
  }
}
function cap(p: Part, r: XY[], z: number, up = true) {
  const N: V3 = [0, 0, up ? 1 : -1]
  for (const [a, b, c] of triangulate(r)) {
    const A: V3 = [r[a][0], r[a][1], z], B: V3 = [r[b][0], r[b][1], z], C: V3 = [r[c][0], r[c][1], z]
    if (up) p.tri(A, B, C, undefined, undefined, undefined, [N, N, N])
    else p.tri(A, C, B, undefined, undefined, undefined, [N, N, N])
  }
}
/** A solid with a bevelled top edge of size b; the roof goes to `top`. */
function prism(p: Part, r: XY[], z0: number, z1: number, b: number, top: Part | null = roof, smooth?: boolean[]) {
  if (area(r) <= 0) throw new Error('rings must be counter-clockwise')
  walls(p, r, z0, z1 - b, smooth)
  if (b <= 0) { if (top) cap(top, r, z1); return }
  const r2 = offset(r, -b), n = r.length, vn = smooth ? vertexNormals(r, smooth) : r.map(() => null)
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, E = edgeN(r[i], r[j])
    const Ma = unit([(vn[i] ?? E)[0], (vn[i] ?? E)[1], 1]), Mb = unit([(vn[j] ?? E)[0], (vn[j] ?? E)[1], 1])
    quadN(p, [r[i][0], r[i][1], z1 - b], [r[j][0], r[j][1], z1 - b], [r2[j][0], r2[j][1], z1], [r2[i][0], r2[i][1], z1], [Ma, Mb, Mb, Ma])
  }
  if (top) cap(top, r2, z1)
}
const rect = (x0: number, y0: number, x1: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
/** A flat rectangle on a wall plane: x = const (facing ±x) or y = const (facing ±y). */
function panelX(p: Part, x: number, y0: number, y1: number, z0: number, z1: number, dir: 1 | -1) {
  const N: V3 = [dir, 0, 0]
  if (dir > 0) quadN(p, [x, y0, z0], [x, y1, z0], [x, y1, z1], [x, y0, z1], [N, N, N, N])
  else quadN(p, [x, y1, z0], [x, y0, z0], [x, y0, z1], [x, y1, z1], [N, N, N, N])
}
function panelY(p: Part, y: number, x0: number, x1: number, z0: number, z1: number, dir: 1 | -1) {
  const N: V3 = [0, dir, 0]
  if (dir > 0) quadN(p, [x1, y, z0], [x0, y, z0], [x0, y, z1], [x1, y, z1], [N, N, N, N])
  else quadN(p, [x0, y, z0], [x1, y, z0], [x1, y, z1], [x0, y, z1], [N, N, N, N])
}

// ---------------------------------------------------------------------------
// The auditorium: a fan in plan, bowed front, flat roof.

const Z_HALL = 21
const HALL_X0 = -16.5
/** The bowed front: a circular arc, apex y = 33.5 at x = 4.25, ends y ≈ 27 at x = -16.5 and 25. */
const ARC = { cx: 4.25, cy: -4.5, r: 38 }
const arcY = (x: number) => ARC.cy + Math.sqrt(ARC.r ** 2 - (x - ARC.cx) ** 2)
const hall: XY[] = [[HALL_X0, -12.6], [15.8, -12.6], [17.2, -9.3], [19.6, -4.8], [21.4, -0.4], [22.9, 6.6], [24.0, 14.5], [24.7, 21.0], [25.0, 27.0]]
const hallSmooth: boolean[] = [false, false, true, true, true, true, true, true, false]
for (let i = 1; i < 12; i++) { const x = 25 - (41.5 * i) / 12; hall.push([x, arcY(x)]); hallSmooth.push(true) }
hall.push([HALL_X0, arcY(HALL_X0)]); hallSmooth.push(false)
// The top edge is a broad chamfer, which catches the light in the aerials.
prism(panel, hall, 0, Z_HALL, 1.0, roof, hallSmooth)

// ---------------------------------------------------------------------------
// The stage house and the low wings behind and beside it.

const Z_FLY = 28
prism(panel, rect(-16, -31, 16, -12.4), 0, Z_FLY, 0.8)
// The long smoke-vent hatch on its roof.
prism(roof, rect(-9, -24.5, 7, -21.5), Z_FLY, Z_FLY + 1.0, 0.25)
// Back of house, the south-east annex (loading), and the north-west wing.
prism(panel, rect(-17, -41.9, 16.8, -30.8), 0, 10, 0.35)
prism(panel, [[16.6, -38.3], [28.1, -38.3], [28.1, -17.2], [23.0, -16.8], [22.9, -13.4], [16.6, -12.8]], 0, 9, 0.35)
prism(panel, [[-25.9, -41.7], [-16.8, -41.7], [-16.8, 30.5], [-20, 30.5], [-25.9, 29.1]], 0, 7.5, 0.35)

// ---------------------------------------------------------------------------
// The lobby.

const LX0 = -15, LX1 = 21.5, LY0 = 26, LY1 = 42.3 // upper-floor box
const GF = 40.6                                   // ground-floor glass line, set back
const Z_SLAB0 = G + 4.4, Z_SLAB1 = G + 5.3        // cream floor slab
const Z_FAS0 = G + 10.4, Z_TOP = G + 11.3         // cream fascia

// Ground floor: a glazed box set back under the upper floor, on a plinth that
// the plaza buries at the front.
prism(trim, rect(LX0 + 0.6, LY0, LX1 - 0.4, GF), 0, G, 0.0, null)
{
  const r = rect(LX0 + 0.6, LY0, LX1 - 0.4, GF)
  walls(win, r, G, Z_SLAB0)
}
// Upper floor: slab, glass band, fascia; the slab and fascia stand a little
// proud of the glass.
const upper = rect(LX0, LY0, LX1, LY1)
prism(trim, offset(upper, 0.25), Z_SLAB0, Z_SLAB1, 0.15, null)
cap(trim, offset(upper, 0.25), Z_SLAB0, false)
walls(glassUp, upper, Z_SLAB1, Z_FAS0)
prism(trim, offset(upper, 0.25), Z_FAS0, Z_TOP, 0.2, roof)
cap(trim, offset(upper, 0.25), Z_FAS0, false)
// Mullions: pale lines every ~2.6 m on the front, so it reads as a glass wall.
{
  const n = 13, w = 0.11, y = LY1 + 0.06
  for (let i = 1; i < n; i++) {
    const x = LX0 + ((LX1 - LX0) * i) / n
    panelY(trim, y, x - w, x + w, Z_SLAB1, Z_FAS0, 1)
  }
  // and on the north-west end
  for (const yy of [29.5, 33.5, 37.8]) panelX(trim, LX0 - 0.06, yy - w, yy + w, Z_SLAB1, Z_FAS0, -1)
}
// The south-east end of the upper floor: turquoise glazed tile, flush on the glass.
panelX(tile, LX1 + 0.05, LY0 + 0.4, LY1 - 0.9, Z_SLAB1, Z_FAS0, 1)

// The entrance canopy: a thin flat slab on four square columns.
{
  const C0 = -6.5, C1 = 13.3, CY = 48
  const slab: XY[] = rect(C0, LY1, C1, CY)
  prism(trim, slab, Z_SLAB0, Z_SLAB1 - 0.1, 0.15, roof)
  cap(trim, slab, Z_SLAB0, false)
  for (const x of [-5.6, 0.4, 6.4, 12.4]) prism(trim, rect(x - 0.35, CY - 1.05, x + 0.35, CY - 0.35), 0, Z_SLAB0, 0.0, null)
}
// The low cream box beside the south-east end.
prism(trim, rect(20.6, 21, 27.2, 35.2), 0, Z_SLAB0, 0.3)

// ---------------------------------------------------------------------------

// Colours from the daylight photos. The precast panels are a warm buff, pulled
// to the palette's lightness; cream trim; the lobby's upper glass reads light
// blue-grey, the recessed ground floor darker; the tile is turquoise, kept
// muted.
const parts = [
  { part: panel, material: finish('ovens-panel', 0xe0cfb2) },
  { part: trim, material: PALETTE.trim },
  { part: roof, material: PALETTE.roof },
  { part: tile, material: finish('ovens-tile', 0x5fb3cc) },
  { part: win, material: PALETTE.window },
  { part: glassUp, material: windowVariant(2, 0x93abbe) },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Ovens Auditorium', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 42, osm: 'way/836535412', height: Z_FLY + 1,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/ovens-auditorium.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
