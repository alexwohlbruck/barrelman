/**
 * Carolina Theatre, 230 North Tryon Street, Charlotte (1927 movie palace,
 * restored and reopened in 2025 as part of Belk Place) — procedural,
 * CC0-1.0, no textures.
 * bun scripts/landmarks/carolina-theatre.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 47.3°, the
 * street grid's axis: the model's +y runs north-east along North Tryon Street
 * toward East 6th Street, +x runs south-east along 6th Street toward College
 * Street. So the Tryon front is the model's west face and 6th Street its
 * north face. The anchor is the metre-based area centroid of the theatre's
 * OSM outline, way/502725466.
 *
 * That outline holds the whole theatre and nothing else, and OSM splits it
 * into parts, which this follows (heights are OSM's):
 * - the glass pavilion on the Tryon corner (Belk Place lobby, parts
 *   1410822079–085, 091; 26–29 m): a lower lobby of clear glass in a pale
 *   frame that stands 0.4 m proud on Tryon and turns the corner onto 6th
 *   Street (1410822091, 16 m), and above it a box of reflective glass under a
 *   white top band, with the vertical CAROLINA blade sign beside the door;
 * - the 1927 auditorium (1410822084): a red-brick hall, 19 m to a low gable
 *   whose ridge runs back from Tryon, blank-walled along 6th Street;
 * - the stage house at the College Street end (1410822086, 22 m);
 * - the low brick annexes along 6th Street (1410822087, 088, 090; 5–7 m),
 *   the two-storey one with its three arched windows;
 * - the back-of-house blocks on the south side (1410822092–095; 20–25 m).
 *
 * The InterContinental hotel planned above the theatre is not in OSM and not
 * modelled.
 *
 * The ground falls about 1.4 m from Tryon to the College Street end (AWS
 * terrain tiles), so y = 0 is the College Street end and each block's height
 * sits on its own ground.
 *
 * References (visual only): City Dweller 2, "Carolina Theater entrance Late
 * March 2025", "Carolina Theater along North Tryon St December 26, 2025",
 * "Intercontinental Hotel at Belk Place Early June 2024" and "... site before
 * construction Late December 2024" (Commons, CC BY-SA 4.0); Fortibus,
 * "Carolina Theatre Charlotte 1" and "2" (Commons, CC BY-SA 4.0, the 6th
 * Street side before restoration); USGS NAIP orthoimagery (public domain).
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, windowVariant } from './palette'

const brick = new Part(), trim = new Part(), roof = new Part()
const win = new Part(), glass = new Part(), lit = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3[]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}

// ---------------------------------------------------------------------------
// Polygon helpers: rings counter-clockwise from above.

function triangulate(r: XY[]): [number, number, number][] {
  const idx = r.map((_, i) => i), out: [number, number, number][] = []
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => cr(a, b, p) > 1e-9 && cr(b, c, p) > 1e-9 && cr(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 5000) {
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
function ccw(r: XY[]): XY[] {
  let a = 0
  r.forEach((p, i) => { const q = r[(i + 1) % r.length]; a += p[0] * q[1] - q[0] * p[1] })
  return a < 0 ? [...r].reverse() : r
}
const edgeN = (a: XY, b: XY): V3 => unit([b[1] - a[1], -(b[0] - a[0]), 0])
function walls(p: Part, r: XY[], z0: number, z1: number, skip: number[] = []) {
  for (let i = 0; i < r.length; i++) {
    if (skip.includes(i)) continue
    const j = (i + 1) % r.length, a = r[i], b = r[j], N = edgeN(a, b)
    quadN(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [N, N, N, N])
  }
}
function cap(p: Part, r: XY[], z: number) {
  const N: V3 = [0, 0, 1]
  for (const [a, b, c] of triangulate(r))
    p.tri([r[a][0], r[a][1], z], [r[b][0], r[b][1], z], [r[c][0], r[c][1], z], undefined, undefined, undefined, [N, N, N])
}
/** A solid with a chamfered top edge of size b (in `edge`); the top goes to `top`. */
function prism(p: Part, r: XY[], z0: number, z1: number, b: number, top: Part | null = p, edge: Part = p) {
  r = ccw(r)
  walls(p, r, z0, z1 - b)
  const r2 = offset(r, -b)
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length, N = edgeN(r[i], r[j]), M = unit([N[0], N[1], 1])
    quadN(edge, [r[i][0], r[i][1], z1 - b], [r[j][0], r[j][1], z1 - b], [r2[j][0], r2[j][1], z1], [r2[i][0], r2[i][1], z1], [M, M, M, M])
  }
  if (top) cap(top, r2, z1)
}
const rect = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
/** A flat panel on a vertical wall, from p to q along it (outside on the right), pushed out by o. */
function panel(part: Part, p: XY, q: XY, z0: number, z1: number, o = 0.04) {
  const N = edgeN(p, q), a: XY = [p[0] + N[0] * o, p[1] + N[1] * o], b: XY = [q[0] + N[0] * o, q[1] + N[1] * o]
  quadN(part, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [N, N, N, N])
}
/** A round-headed window on a wall running along +x at y (facing +y), flush. */
function archNorth(part: Part, xc: number, y: number, w: number, z0: number, zs: number, o = 0.04) {
  const r = w / 2, N: V3 = [0, 1, 0], pts: XY[] = [[xc + r, z0], [xc + r, zs]]
  for (let k = 1; k < 8; k++) { const t = k * Math.PI / 8; pts.push([xc + r * Math.cos(t), zs + r * Math.sin(t)]) }
  pts.push([xc - r, zs], [xc - r, z0])
  for (let i = 1; i < pts.length - 1; i++)
    part.tri([pts[0][0], y + o, pts[0][1]], [pts[i + 1][0], y + o, pts[i + 1][1]], [pts[i][0], y + o, pts[i][1]], undefined, undefined, undefined, [N, N, N])
}

// ---------------------------------------------------------------------------
// Ground: 1.4 m higher on Tryon than at the College Street end.
const g = (x: number) => Math.max(0, 1.4 * (30.5 - x) / 61)

// ---------------------------------------------------------------------------
// The glass pavilion on the corner.

const PX0 = -30.1, PX1 = -9.0, PY0 = -9.4, PY1 = 19.6
const GP = g(-20)
const Z_LOBBY = GP + 15.5      // top of the lower lobby's frame (OSM strip, 16 m)
const Z_TOP = GP + 27          // pavilion roof (OSM 26–27 m)
const Z_BAND = Z_TOP - 1.5     // the white top band
const Z_LOUVRE = Z_BAND - 0.8  // the dark strip under it

// The box itself is reflective glass from the lobby frame up.
{
  const r = rect(PX0, PX1, PY0, PY1)
  // Glass walls to the band, then the white band with a soft top edge.
  walls(glass, r, 0, Z_BAND)
  prism(trim, r, Z_BAND, Z_TOP, 0.4, roof)
  // The back extension over the auditorium's front (1410822081, 26 m).
  const back = rect(PX1, -4.9, -2.2, 12.2)
  walls(glass, back, 0, Z_TOP - 1.0, [3])
  prism(trim, back, Z_TOP - 1.6, Z_TOP - 1.0, 0.2, roof)
  // A plant screen on the roof (1410822080, 29 m).
  prism(roof, rect(-16.1, -8.9 - 0.4, 2.4, 12.2), Z_TOP - 0.2, Z_TOP + 1.2, 0.3, roof)
}
// The dark strip under the band, all round.
for (const [p, q] of [[[PX0, PY1], [PX0, PY0]], [[PX1, PY1], [PX0, PY1]], [[PX0, PY0], [PX1, PY0]]] as [XY, XY][])
  panel(roof, p, q, Z_LOUVRE, Z_BAND)
// Upper glass: a floor line and a few mullions in pale trim.
{
  const lines = (p: XY, q: XY, z0: number, n: number) => {
    const L = Math.hypot(q[0] - p[0], q[1] - p[1]), u: XY = [(q[0] - p[0]) / L, (q[1] - p[1]) / L]
    panel(trim, p, q, (z0 + Z_LOUVRE) / 2 - 0.15, (z0 + Z_LOUVRE) / 2 + 0.15, 0.05)
    for (let k = 1; k < n; k++) {
      const s = L * k / n, a: XY = [p[0] + u[0] * (s - 0.12), p[1] + u[1] * (s - 0.12)], b: XY = [p[0] + u[0] * (s + 0.12), p[1] + u[1] * (s + 0.12)]
      panel(trim, a, b, z0, Z_LOUVRE, 0.05)
    }
  }
  lines([PX0, PY1], [PX0, PY0], Z_LOBBY, 6)          // Tryon
  lines([PX1, PY1], [PX0, PY1], Z_LOBBY - 3, 4)      // 6th Street (the lobby frame stops short)
  lines([PX0, PY0], [PX1, PY0], 20.5, 4)             // over the Foundation building
}

// The lower lobby: a frame of pale panels 0.4 m proud, clear lit glass within.
{
  const F = 0.55, O = 0.4
  // Tryon face.
  const xf = PX0 - O
  prism(trim, [[xf, PY0 + 0.4], [PX0, PY0 + 0.4], [PX0, PY1 - 0.2], [xf, PY1 - 0.2]], 0, Z_LOBBY, 0.2)
  // Turn the corner onto 6th Street as far as the OSM strip runs.
  const yf = PY1 + O
  prism(trim, [[PX0 - O, PY1 - 0.2], [-21.0, PY1 - 0.2], [-21.0, yf], [PX0 - O, yf]], 0, Z_LOBBY, 0.2)
  // Glass within the frame: Tryon, then 6th Street.
  panel(win, [xf, PY1 - F], [xf, PY0 + 0.4 + F], GP + 0.0, Z_LOBBY - F, 0.03)
  panel(win, [-21 - F, yf], [PX0 - O + F, yf], g(-25), Z_LOBBY - F, 0.03)
  // Interior white piers seen through the glass, and the door's stone portal.
  for (const y of [3.2, 11.4]) panel(trim, [xf, y + 0.6], [xf, y - 0.6], GP, Z_LOBBY - F, 0.06)
  panel(trim, [xf, -2.0], [xf, -5.8], GP, GP + 12.2, 0.07)
  panel(lit, [xf, -2.9], [xf, -4.9], GP, GP + 3.4, 0.1)
  // 6th Street, beyond the lobby frame: plain glass down to the pavement.
  panel(win, [PX1 - 0.3, PY1], [-21 + 0.3, PY1], g(-15), Z_LOBBY - 3, 0.04)
}
// The CAROLINA blade sign: a tall slim box standing off the front by the door.
{
  const y0 = -7.6, y1 = -6.8, x0 = PX0 - 0.4 - 1.7, x1 = PX0 - 0.4
  prism(lit, rect(x0, x1, y0, y1), GP + 3.6, GP + 13.2, 0.12, roof)
  // Its underside.
  quadN(lit, [x0, y0, GP + 3.6], [x0, y1, GP + 3.6], [x1, y1, GP + 3.6], [x1, y0, GP + 3.6], [[0, 0, -1], [0, 0, -1], [0, 0, -1], [0, 0, -1]])
}

// ---------------------------------------------------------------------------
// The 1927 theatre in red brick.

// Auditorium: walls to the eaves, a low gable along x with brick gable ends.
{
  const X0 = -9.6, X1 = 20.7, Y0 = -9.3, Y1 = 16.4, YC = (Y0 + Y1) / 2
  const ZE = g(5) + 15, ZR = g(5) + 19
  walls(brick, rect(X0, X1, Y0, Y1), 0, ZE)
  // A slim brick parapet on the long sides.
  prism(brick, rect(X0, X1, Y1 - 0.5, Y1), ZE, ZE + 0.7, 0.2)
  prism(brick, rect(X0, X1, Y0, Y0 + 0.5), ZE, ZE + 0.7, 0.2)
  // Roof slopes.
  const sN = unit([0, ZR - ZE, YC - Y0]), nN = unit([0, -(ZR - ZE), YC - Y0])
  quadN(roof, [X0, Y0 + 0.5, ZE + 0.4], [X1, Y0 + 0.5, ZE + 0.4], [X1, YC, ZR], [X0, YC, ZR], [sN, sN, sN, sN].map(() => unit([0, -(ZR - ZE), YC - Y0])) as V3[])
  quadN(roof, [X1, Y1 - 0.5, ZE + 0.4], [X0, Y1 - 0.5, ZE + 0.4], [X0, YC, ZR], [X1, YC, ZR], [nN, nN, nN, nN].map(() => unit([0, ZR - ZE, YC - Y0])) as V3[])
  // Gable ends (the Tryon one sits behind the pavilion; the College one shows above the stage house's sides).
  for (const [x, s] of [[X0, -1], [X1, 1]] as [number, number][]) {
    const N: V3 = [s, 0, 0]
    const A: V3 = [x, Y0, ZE], B: V3 = [x, Y1, ZE], C: V3 = [x, YC, ZR + 0.5]
    if (s > 0) brick.tri(A, B, C, undefined, undefined, undefined, [N, N, N])
    else brick.tri(B, A, C, undefined, undefined, undefined, [N, N, N])
  }
}
// Stage house, at the College Street end.
prism(brick, rect(20.7, 30.5, -4.0, 14.7), 0, 22, 0.35, roof)
// The annexes along 6th Street.
prism(brick, rect(-8.9, -0.1, 16.3, 19.5), 0, g(-4.5) + 7.2, 0.3, roof)
prism(brick, rect(-0.1, 20.8, 16.3, 19.5), 0, g(10) + 5.0, 0.3, roof)
prism(brick, rect(20.7, 30.6, 14.7, 19.6), 0, 7.0, 0.3, roof)
// Their windows: three arched on the two-storey annex, two big grid windows below.
for (const xc of [-6.4, -4.5, -2.6]) archNorth(glass, xc, 19.5, 1.3, g(-4.5) + 3.7, g(-4.5) + 5.6)
for (const [a, b] of [[1.0, 5.6], [6.4, 11.0]]) panel(glass, [b, 19.5], [a, 19.5], g(3) + 0.9, g(3) + 3.6)
for (const xc of [23.2, 25.6, 28.0]) panel(glass, [xc + 0.6, 19.6], [xc - 0.6, 19.6], 1.2, 2.6)
panel(lit, [16.4, 19.5], [14.6, 19.5], g(15), g(15) + 2.6)

// The back-of-house blocks on the south side.
prism(brick, [[-12.0, -20.9], [-5.6, -21.4], [-5.1, -9.3], [-11.5, -9.3]], 0, g(-8) + 25, 0.35, roof)
prism(brick, [[-12.2, -26.1], [-3.5, -26.7], [-3.0, -21.5], [-5.6, -21.4], [-12.0, -20.9]], 0, g(-8) + 25, 0.35, roof)
prism(brick, rect(7.2, 15.0, -14.0, -9.3), 0, g(11) + 23, 0.35, roof)
prism(brick, [[7.2, -14.0], [15.0, -14.0], [14.7, -19.3], [18.5, -19.3], [18.5, -28.2], [15.5, -28.0], [-3.5, -26.7], [-3.0, -21.5], [-5.6, -21.4], [-5.1, -9.3], [7.2, -9.3]], 0, g(5) + 20, 0.35, roof)
// A band of windows on the stage-house block's College Street face.
for (const z of [6, 12]) panel(glass, [18.5, -27.4], [18.5, -20.0], z, z + 2.2)

// ---------------------------------------------------------------------------

// Colours from the daylight photos: the orange-red brick pulled light; the
// pavilion's pale frame and band as trim; its reflective blue-green glass as
// a light sky slate; the lit lobby glass, door portal glazing and blade sign
// as entrance, so the lobby glows at night as it does.
const parts = [
  { part: brick, material: finish('red-brick', 0xc0846f) },
  { part: trim, material: PALETTE.trim },
  { part: roof, material: PALETTE.roof },
  { part: win, material: { ...PALETTE.window, color: 0xbfcfd9 } },
  { part: glass, material: windowVariant(2, 0x93acbf) },
  { part: lit, material: PALETTE.entrance },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Carolina Theatre', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 47.3, osm: 'way/502725466', height: Z_TOP + 1.2,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const outFile = new URL('../../landmarks/models/carolina-theatre.glb', import.meta.url).pathname
await Bun.write(outFile, glb)
console.log(`${outFile}: ${triangles} triangles, ${glb.length} bytes`)
