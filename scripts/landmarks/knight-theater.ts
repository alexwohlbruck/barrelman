/**
 * Knight Theater, Levine Center for the Arts, Charlotte (2010) — procedural, CC0-1.0.
 * bun scripts/landmarks/knight-theater.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 315°: the
 * model's +y runs north-west along the outline's long side (way/131139733,
 * 68 × 40 m), so the lobby on the forecourt off Levine Avenue of the Arts is
 * the model's south face. The anchor is the outline's centroid.
 *
 * What makes it the Knight is its lobby: a four-storey glass bay whose plan is
 * an S — a narrow convex bay on the west, a notch, then a wider bay with a
 * rounded shoulder running flat to the east — wrapped top and bottom by
 * brushed-aluminium bands, the lower one carrying the name over a glazed
 * ground floor. Behind it the theatre is a plain buff-brick block to the
 * OSM height (27 m), and on the east a recessed grey curtain wall faces the
 * forecourt beside the Bechtler.
 *
 * References: Knight Foundation, "North Carolina Blumenthal Performing Arts
 * Center" 1–3 (Commons, CC BY-SA 2.0; they show the Knight), and Mapillary
 * street views (JordanAnderson 2022, timhibbard 2015) of the forecourt and the
 * brick flanks on Church Street and Levine Avenue. The DEM puts the lobby
 * ground about 0.8 m above the lowest ground under the footprint.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, windowVariant } from './palette'

const brick = new Part(), alu = new Part(), lobbyGlass = new Part()
const win = new Part(), roof = new Part(), door = new Part()

type XY = [number, number]
const G = 0.8 // lobby ground above the model's y = 0

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3[]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}

// ---------------------------------------------------------------------------
// Polygon helpers: rings are counter-clockwise from above.

function area(r: XY[]) { let a = 0; r.forEach((p, i) => { const q = r[(i + 1) % r.length]; a += p[0] * q[1] - q[0] * p[1] }); return a / 2 }
/** Ear clipping, enough for the few small rings here. */
function triangulate(r: XY[]): [number, number, number][] {
  const idx = r.map((_, i) => i), out: [number, number, number][] = []
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => cr(a, b, p) > 1e-9 && cr(b, c, p) > 1e-9 && cr(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
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
/** The ring moved outward by d (inward if negative), mitred at the corners. */
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
/** Outward wall normals: per edge, or averaged at a vertex for smooth runs. */
function edgeN(a: XY, b: XY): V3 { return unit([b[1] - a[1], -(b[0] - a[0]), 0]) }
/** Vertical walls round a ring from z0 to z1. */
function walls(p: Part, r: XY[], z0: number, z1: number, open = false) {
  const n = r.length
  for (let i = 0; i < (open ? n - 1 : n); i++) {
    const a = r[i], b = r[(i + 1) % n], N = edgeN(a, b)
    quadN(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [N, N, N, N])
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
/** A solid with a chamfered top edge of size b. */
function prism(p: Part, r: XY[], z0: number, z1: number, b: number, top: Part = p) {
  walls(p, r, z0, z1 - b)
  const r2 = offset(r, -b), n = r.length
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, N = edgeN(r[i], r[j]), M = unit([N[0], N[1], 1])
    quadN(p, [r[i][0], r[i][1], z1 - b], [r[j][0], r[j][1], z1 - b], [r2[j][0], r2[j][1], z1], [r2[i][0], r2[i][1], z1], [M, M, M, M])
  }
  cap(top, r2, z1)
}

// ---------------------------------------------------------------------------
// The theatre block, from the OSM outline, with its south face pulled back to
// the line behind the lobby (y = -28).

const Z_BODY = 27
// The strip behind the lobby (and the east block beside it) only reaches the
// lobby's coping: from the forecourt nothing shows over the coping but a
// small rooftop box, so the full-height block starts further back.
const Y_STEP = -18
const FRONT: XY[] = [[-19.7, -28], [1.4, -28], [1.4, -39.6], [20.0, -41.4], [20.41, Y_STEP], [-19.7, Y_STEP]]
const BODY: XY[] = [
  [-19.7, Y_STEP], [20.41, Y_STEP], [21.1, 21.2], [14.0, 21.2], [14.4, 35.9],
  [-1.9, 36.1], [-2.2, 41.8], [-11.9, 42.2], [-19.7, 36.7],
]
for (const r of [FRONT, BODY]) if (area(r) <= 0) throw new Error('rings must be counter-clockwise')
/** A brick mass with an aluminium coping, bevelled on top. */
function mass(r: XY[], z: number) {
  prism(brick, r, 0, z - 0.9, 0.35, roof)
  prism(alu, offset(r, -0.35), z - 0.9, z, 0.3, roof)
}
// Z_TOP is defined with the lobby below; the front strip matches it.
mass(BODY, Z_BODY)
// The pale rooftop box seen over the coping from the forecourt.
prism(alu, [[-13, -26], [-4, -26], [-4, -21], [-13, -21]], 22, 25.2, 0.3, roof)

// The recessed grey curtain wall on the east of the forecourt (OSM's short
// west-facing face at x = 1.4), with a band of brick over it.
{
  const x = 1.4 - 0.05, N: V3 = [-1, 0, 0]
  const y0 = -39.4, y1 = -28.3
  quadN(win, [x, y1, G + 0.3], [x, y0, G + 0.3], [x, y0, 22], [x, y1, 22], [N, N, N, N])
  // Floor lines in aluminium, so it reads as a curtain wall and not a slab.
  for (const z of [G + 5.2, 11.5, 17]) {
    quadN(alu, [x - 0.06, y1, z], [x - 0.06, y0, z], [x - 0.06, y0, z + 0.35], [x - 0.06, y1, z + 0.35], [N, N, N, N])
  }
}

// Punched windows on the brick flanks: a row of tall ground-floor openings on
// Levine Avenue (west) and Church Street (north), as in the street views.
{
  const W: V3 = [-1, 0, 0]
  for (let i = 0; i < 6; i++) {
    const y0 = -22 + i * 9, y1 = y0 + 4.2, x = -19.7 - 0.04
    quadN(win, [x, y1, G + 0.6], [x, y0, G + 0.6], [x, y0, G + 4.2], [x, y1, G + 4.2], [W, W, W, W])
  }
  const Nn = unit([0, 1, 0])
  for (const [x0, x1] of [[-9, -5], [2, 6], [8, 12]] as XY[]) {
    // the north face between x = -1.9 and 14.4 sits at y ≈ 36
    if (x0 < -1.9) continue
    const y = 36.05
    quadN(win, [x1, y, G + 0.6], [x0, y, G + 0.6], [x0, y, G + 4.2], [x1, y, G + 4.2], [Nn, Nn, Nn, Nn])
  }
}

// ---------------------------------------------------------------------------
// The lobby: an S-curve in plan, from the west corner to the east end.

const Y_BACK = -28
/** The lobby front, west to east, counter-clockwise round the bay's front. */
function lobbyFront(inset = 0): { p: XY; n: V3 }[] {
  const out: { p: XY; n: V3 }[] = []
  const arc = (cx: number, cy: number, r: number, a0: number, a1: number, seg: number) => {
    for (let i = 0; i <= seg; i++) {
      const a = ((a0 + ((a1 - a0) * i) / seg) * Math.PI) / 180
      const n: V3 = [Math.cos(a), Math.sin(a), 0]
      out.push({ p: [cx + (r - inset) * n[0], cy + (r - inset) * n[1]], n })
    }
  }
  // West end: straight off the block, then the narrow convex bay.
  out.push({ p: [-19.7 + inset, Y_BACK], n: [-1, 0, 0] })
  arc(-15.1, -30.0, 4.6, 180, 318, 10)
  // The second bay's rounded shoulder, then its flat front to the east end.
  arc(-7.6, -31.6, 3.5, 222, 270, 4)
  const yE = -35.1 + inset
  out.push({ p: [-2.0, yE], n: [0, -1, 0] })
  out.push({ p: [1.4 - inset, yE + 0.15], n: [0, -1, 0] })
  return out
}

/** A curved band of the front between z0 and z1, pushed out by `d`. */
function frontBand(p: Part, z0: number, z1: number, inset: number) {
  const f = lobbyFront(inset)
  for (let i = 0; i < f.length - 1; i++) {
    const a = f[i], b = f[i + 1]
    // Sharp edges at the notch and the ends, smooth round the bays.
    const kink = Math.abs(a.n[0] * b.n[0] + a.n[1] * b.n[1]) < 0.75
    const nb = kink ? edgeN(a.p, b.p) : b.n, na = kink ? edgeN(a.p, b.p) : a.n
    quadN(p, [a.p[0], a.p[1], z0], [b.p[0], b.p[1], z0], [b.p[0], b.p[1], z1], [a.p[0], a.p[1], z1], [na, nb, nb, na])
  }
}
/** The flat top or underside of a band, between the front curve and the block. */
function frontLid(p: Part, z: number, inset: number, up: boolean) {
  const f = lobbyFront(inset).map((q) => q.p)
  // The front runs west to east; close it back along the block face.
  // f runs west → east along the south and starts on the block face, so
  // returning along y = -28 closes it counter-clockwise.
  const ring: XY[] = [...f, [1.4 - inset, Y_BACK]]
  cap(p, ring, z, up)
}

const Z_G1 = G + 5.0      // top of the glazed ground floor
const Z_BAND = G + 7.2    // top of the name band
const Z_COPE0 = G + 20.2  // bottom of the top coping
const Z_TOP = G + 22.0
mass(FRONT, Z_TOP)

// Ground floor: glazing set back under the band, doors in the middle.
frontBand(lobbyGlass, G, Z_G1, 0.9)
{
  const f = lobbyFront(0.85)
  // Doors: the flat run and the shoulder of the east bay.
  for (let i = 0; i < f.length - 1; i++) {
    const a = f[i], b = f[i + 1]
    if (a.p[0] < -11 || b.p[0] > 1.0) continue
    const N = edgeN(a.p, b.p)
    quadN(door, [a.p[0] + N[0] * 0.04, a.p[1] + N[1] * 0.04, G], [b.p[0] + N[0] * 0.04, b.p[1] + N[1] * 0.04, G],
      [b.p[0] + N[0] * 0.04, b.p[1] + N[1] * 0.04, G + 3.0], [a.p[0] + N[0] * 0.04, a.p[1] + N[1] * 0.04, G + 3.0], [N, N, N, N])
  }
}
// The name band: brushed aluminium, a little proud of the glass, chamfered.
frontBand(alu, Z_G1, Z_BAND - 0.3, -0.25)
{
  const a = lobbyFront(-0.25), b = lobbyFront(0.05)
  for (let i = 0; i < a.length - 1; i++) {
    const n0 = unit([a[i].n[0], a[i].n[1], 1]), n1 = unit([a[i + 1].n[0], a[i + 1].n[1], 1])
    quadN(alu, [a[i].p[0], a[i].p[1], Z_BAND - 0.3], [a[i + 1].p[0], a[i + 1].p[1], Z_BAND - 0.3],
      [b[i + 1].p[0], b[i + 1].p[1], Z_BAND], [b[i].p[0], b[i].p[1], Z_BAND], [n0, n1, n1, n0])
  }
}
frontLid(alu, Z_G1, -0.25, false)

// The glass: three tiers, split by thin aluminium transoms.
frontBand(lobbyGlass, Z_BAND, Z_COPE0, 0)
for (const z of [G + 11.6, G + 15.9]) frontBand(alu, z, z + 0.32, -0.08)
// Vertical mullions every ~2.4 m along the curve: a few pale lines, as the
// photos show, so the bay reads as a glass wall and not a solid drum.
{
  const f = lobbyFront(-0.06)
  let run = 0, next = 1.2
  for (let i = 0; i < f.length - 1; i++) {
    const a = f[i].p, b = f[i + 1].p, L = Math.hypot(b[0] - a[0], b[1] - a[1])
    while (next <= run + L) {
      const t = (next - run) / L, c: XY = [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
      const u: XY = [(b[0] - a[0]) / L * 0.1, (b[1] - a[1]) / L * 0.1], N = edgeN(a, b)
      quadN(alu, [c[0] - u[0], c[1] - u[1], Z_BAND], [c[0] + u[0], c[1] + u[1], Z_BAND],
        [c[0] + u[0], c[1] + u[1], Z_COPE0], [c[0] - u[0], c[1] - u[1], Z_COPE0], [N, N, N, N])
      next += 2.4
    }
    run += L
  }
}
// West end wall of the bay, glass too (it wraps round the corner).
// The coping: aluminium, proud and chamfered on top like the band.
frontBand(alu, Z_COPE0, Z_TOP - 0.3, -0.2)
{
  const a = lobbyFront(-0.2), b = lobbyFront(0.1)
  for (let i = 0; i < a.length - 1; i++) {
    const n0 = unit([a[i].n[0], a[i].n[1], 1]), n1 = unit([a[i + 1].n[0], a[i + 1].n[1], 1])
    quadN(alu, [a[i].p[0], a[i].p[1], Z_TOP - 0.3], [a[i + 1].p[0], a[i + 1].p[1], Z_TOP - 0.3],
      [b[i + 1].p[0], b[i + 1].p[1], Z_TOP], [b[i].p[0], b[i].p[1], Z_TOP], [n0, n1, n1, n0])
  }
}
frontLid(alu, Z_COPE0, -0.2, false)
frontLid(roof, Z_TOP, 0.1, true)

// ---------------------------------------------------------------------------

// Colours from the daylight photos: buff brick pulled to the palette's
// lightness; brushed aluminium a cool light grey; the lobby glass reads light
// blue-grey by day (a lighter window variant, so it still glows at night).
const parts = [
  { part: brick, material: finish('knight-brick', 0xdcc6a6) },
  { part: alu, material: finish('aluminium', 0xd3d6d8, 0.5) },
  { part: lobbyGlass, material: windowVariant(2, 0x9fb6c8) },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
  { part: door, material: PALETTE.entrance },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Knight Theater', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 315, osm: 'way/131139733', height: Z_BODY,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/knight-theater.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
