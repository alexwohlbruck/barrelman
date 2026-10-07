/**
 * Guardians of the Galaxy: Cosmic Rewind, EPCOT (Walt Disney World):
 * procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-cosmic-rewind.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the area centroid of the OSM outline (see ANCHOR), on
 * the ground (EPCOT is flat).
 *
 * What it is. One OSM way (way/719079634) draws three things joined together,
 * and the model draws all three:
 *   - the Wonders of Xandar pavilion, the old Universe of Energy building
 *     (1982), reclad in slate grey: a narrow front on the plaza, two long
 *     diagonal sides of sawtooth fins, a roof that rises in shallow steps from
 *     the front, and a taller block at the back;
 *   - a long connector running north-east from the pavilion's back corner,
 *     which climbs towards the show building;
 *   - the show building, a plain box painted to vanish into the sky: pale
 *     sky blue over a sage-green band at the bottom, with six T-shaped plant
 *     units on the roof.
 * In front of the pavilion stands the Nova Corps Starblaster on its leaning
 * pylon, the pavilion's emblem. It is outside the OSM outline and replaces
 * nothing.
 *
 * Evidence.
 *   Measured: the plan of all three parts, from the OSM outline, checked over
 *     USGS NAIP orthoimagery (public domain), which agrees to a metre or two;
 *     the roof units' positions and the Starblaster's spot, from NAIP.
 *   Published: the Starblaster is 51 ft (15.5 m) tall (Disney, via WDWInfo);
 *     the show building is "more than 12 stories" and holds four Spaceship
 *     Earths by volume (Disney). Four times Spaceship Earth's 2.2 million ft³
 *     over the OSM footprint (95 × 69 m) gives 38 m, which the NAIP shadow
 *     (about 46 m long) agrees with.
 *   Estimated: the pavilion's heights (a 10 m front, rising to 17 m, the back
 *     block at 20 m), from photos against the 15.5 m Starblaster; the
 *     connector's profile (16 m at the pavilion, climbing to 31 m near the
 *     show building), from Steve Jurvetson's aerial and the construction photo;
 *     the green band's height (9 m), from the same two photos; the pylon's
 *     lean and the Starblaster's blade lengths, from the plaza photos.
 *   Invented: nothing structural. The Starblaster is drawn as a faceted head
 *     and eight flat blades, which is a simplification, not a copy.
 *
 * Photos (Wikimedia Commons): "Guardians of the Galaxy Cosmic Rewind-Cast
 * Preview (52030963400) (cropped)" (elisfkc2, CC BY-SA 2.0); "Guardians of the
 * Galaxy Cosmic Rewind at EPCOT" (Jedi94, CC BY-SA 4.0); "Guardians of the
 * Galaxy Cosmic Rewind at Epcot (2026-07-27 k82z)" (CoasterCredit, CC0);
 * "Guardians of the Galaxy Cosmic Rewind 2" (Wacky Windjammer, CC BY-SA 4.0,
 * the show building from the car park); "Guardians of the Galaxy Cosmic Rewind
 * construction 1" (Alexander Alzona, CC BY-SA 2.0) and "… construction 3"
 * (Anthony Quintano, CC BY 2.0); "EPCOT from above" (Steve Jurvetson,
 * CC BY 2.0). Plan and roof: USGS NAIP (public domain).
 *
 * Style: palette roof; the pavilion's slate grey is kept dark because it is
 * what the pavilion is, but no darker than the style's charcoal floor; the
 * show building's sky blue and sage green are its paint scheme, muted to the
 * palette's lightness.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]

// ------------------------------------------------------------------ plan ----
// OSM way/719079634 in metres east/north of (-81.5470, 28.3752).
const REF = { lon: -81.547, lat: 28.3752 }
const MX = 111320 * Math.cos((REF.lat * Math.PI) / 180), MY = 110574
const OSM: XY[] = [
  [71.3, 122.2], [158.4, 84.8], [130.1, 21.8], [46.5, 58.3], [31.2, 43.7], [4.0, 15.6], [-12.4, -1.5], [-14.0, 0.1],
  [-21.4, -6.9], [-24.8, -10.3], [-23.5, -10.4], [-25.8, -13.1], [-27.8, -13.2], [-29.6, -15.3], [-28.3, -15.4], [-31.4, -18.0],
  [-32.9, -18.1], [-35.2, -20.8], [-34.1, -20.8], [-36.6, -23.3], [-37.5, -23.3], [-39.4, -25.5], [-38.4, -25.5], [-41.8, -28.7],
  [-43.5, -28.7], [-45.4, -31.0], [-44.2, -31.0], [-45.9, -32.8], [-47.0, -34.0], [-48.3, -34.0], [-49.6, -35.5], [-50.4, -36.5],
  [-49.6, -36.6], [-51.6, -38.7], [-53.1, -38.6], [-54.1, -39.4], [-56.2, -41.3], [-55.2, -41.3], [-56.7, -42.8], [-63.9, -49.8],
  [-65.3, -51.2], [-79.5, -51.2], [-81.9, -51.3], [-97.8, -51.3], [-98.8, -50.2], [-105.3, -43.1], [-106.4, -41.7], [-105.6, -41.6],
  [-108.7, -39.0], [-109.9, -39.0], [-112.1, -36.5], [-111.1, -36.5], [-112.4, -35.3], [-114.0, -33.7], [-114.9, -33.7], [-117.0, -31.1],
  [-116.3, -31.1], [-117.5, -29.8], [-118.7, -28.5], [-120.1, -28.5], [-122.2, -26.1], [-121.3, -26.1], [-123.1, -24.5], [-124.7, -23.1],
  [-125.6, -23.1], [-127.4, -21.1], [-126.7, -21.1], [-129.5, -18.1], [-130.4, -18.1], [-132.5, -16.1], [-131.6, -16.0], [-134.2, -13.6],
  [-135.3, -13.7], [-137.6, -11.1], [-136.8, -11.0], [-144.6, -3.2], [-147.9, 0.1], [-113.9, 33.7], [-95.3, 35.3], [-94.7, 59.3],
  [-38.1, 60.2], [-38.0, 33.9], [-47.1, 33.6], [-23.7, 10.0], [3.6, 37.0], [2.3, 38.1], [5.2, 41.2], [6.6, 40.0],
  [15.8, 49.2], [45.0, 78.9], [51.4, 75.9],
]
// The anchor: the outline's area centroid.
const ANCHOR = (() => {
  let a = 0, cx = 0, cy = 0
  for (let i = 0; i < OSM.length; i++) {
    const [x0, y0] = OSM[i], [x1, y1] = OSM[(i + 1) % OSM.length], k = x0 * y1 - x1 * y0
    a += k; cx += (x0 + x1) * k; cy += (y0 + y1) * k
  }
  return [cx / (3 * a), cy / (3 * a)] as XY
})()
const L = (p: XY): XY => [p[0] - ANCHOR[0], p[1] - ANCHOR[1]]
const at = (i: number) => OSM[i]

// The three parts of the outline (index ranges into OSM), each counter-clockwise.
const SHOW: XY[] = [at(3), at(2), at(1), at(0)].map(L)
const CORRIDOR: XY[] = [at(83), at(6), at(5), at(4), at(3), at(90), at(89), at(88), at(87), at(86), at(85), at(84)].map(L).reverse()
const PAVILION: XY[] = [...OSM.slice(6, 84)].map(L).reverse()

// ------------------------------------------------------------- helpers ----
const signedArea = (p: XY[]) => p.reduce((s, a, i) => s + a[0] * p[(i + 1) % p.length][1] - p[(i + 1) % p.length][0] * a[1], 0) / 2
const ccw = (p: XY[]) => (signedArea(p) < 0 ? [...p].reverse() : p)

/** Ear-clipping triangulation of a simple polygon (counter-clockwise). */
function earClip(poly: XY[]): [XY, XY, XY][] {
  const p = ccw(poly), idx = p.map((_, i) => i), out: [XY, XY, XY][] = []
  const cr = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const A = p[ia], B = p[ib], C = p[ic]
      if (cr(A, B, C) <= 1e-9) continue
      if (idx.some((j) => j !== ia && j !== ib && j !== ic && cr(A, B, p[j]) >= -1e-9 && cr(B, C, p[j]) >= -1e-9 && cr(C, A, p[j]) >= -1e-9)) continue
      out.push([A, B, C]); idx.splice(i, 1); cut = true
      break
    }
    if (!cut) idx.splice(0, 1) // degenerate sliver: drop a vertex rather than loop
  }
  if (idx.length === 3) out.push([p[idx[0]], p[idx[1]], p[idx[2]]])
  return out
}

/** Clip a polygon to the slab a <= dir·p <= b (Sutherland–Hodgman, twice). */
function slab(poly: XY[], dir: XY, a: number, b: number): XY[] {
  const half = (pts: XY[], keep: (t: number) => boolean, edge: number) => {
    const out: XY[] = []
    for (let i = 0; i < pts.length; i++) {
      const P = pts[i], Q = pts[(i + 1) % pts.length]
      const tp = P[0] * dir[0] + P[1] * dir[1], tq = Q[0] * dir[0] + Q[1] * dir[1]
      if (keep(tp)) out.push(P)
      if (keep(tp) !== keep(tq)) { const u = (edge - tp) / (tq - tp); out.push([P[0] + (Q[0] - P[0]) * u, P[1] + (Q[1] - P[1]) * u]) }
    }
    return out
  }
  return half(half(poly, (t) => t >= a, a), (t) => t <= b, b)
}

/**
 * A block over a polygon whose roof height is `z(x, y)`, piecewise linear
 * along `dir` with breaks at `cuts`: walls on every edge (split where they
 * cross a break, so their tops follow the roof) and a roof triangulated per
 * slab. `skirt` lets a wall start above the ground for chosen edges.
 */
function block(walls: Part, roof: Part, poly: XY[], z: (x: number, y: number) => number, dir: XY = [0, 1], cuts: number[] = [], z0 = 0, skirt?: (a: XY, b: XY) => number) {
  const p = ccw(poly)
  const t = (q: XY) => q[0] * dir[0] + q[1] * dir[1]
  for (let i = 0; i < p.length; i++) {
    const a = p[i], b = p[(i + 1) % p.length]
    const ta = t(a), tb = t(b)
    const us = [0, 1, ...cuts.filter((c) => (c - ta) * (c - tb) < 0).map((c) => (c - ta) / (tb - ta))].sort((m, n) => m - n)
    const base = skirt ? skirt(a, b) : z0
    for (let k = 0; k + 1 < us.length; k++) {
      const A: XY = [a[0] + (b[0] - a[0]) * us[k], a[1] + (b[1] - a[1]) * us[k]]
      const B: XY = [a[0] + (b[0] - a[0]) * us[k + 1], a[1] + (b[1] - a[1]) * us[k + 1]]
      walls.quad([A[0], A[1], base], [B[0], B[1], base], [B[0], B[1], z(...B)], [A[0], A[1], z(...A)])
    }
  }
  const ts = p.map(t), edges = [Math.min(...ts) - 1, ...cuts, Math.max(...ts) + 1]
  for (let k = 0; k + 1 < edges.length; k++) {
    const piece = slab(p, dir, edges[k], edges[k + 1])
    if (piece.length < 3 || Math.abs(signedArea(piece)) < 0.01) continue
    for (const [A, B, C] of earClip(piece)) roof.tri([A[0], A[1], z(...A)], [B[0], B[1], z(...B)], [C[0], C[1], z(...C)])
  }
}

/** An axis box from its centre, half-sizes along a unit axis `u` (and its normal), z0 to z1. */
function box(part: Part, c: XY, u: XY, hu: number, hv: number, z0: number, z1: number, top = true) {
  const v: XY = [-u[1], u[0]]
  const P = (su: number, sv: number): XY => [c[0] + u[0] * hu * su + v[0] * hv * sv, c[1] + u[1] * hu * su + v[1] * hv * sv]
  const ring = [P(-1, -1), P(1, -1), P(1, 1), P(-1, 1)]
  for (let i = 0; i < 4; i++) {
    const a = ring[i], b = ring[(i + 1) % 4]
    part.quad([a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
  }
  if (top) { part.tri([...ring[0], z1], [...ring[1], z1], [...ring[2], z1]); part.tri([...ring[0], z1], [...ring[2], z1], [...ring[3], z1]) }
}

// ----------------------------------------------------------- materials ----
const slate = new Part(), roof = new Part(), sky = new Part(), green = new Part(), glazing = new Part(), gold = new Part()

// ------------------------------------------------------- show building ----
// A box 38 m tall: sage green to 9 m, sky blue above, a chamfered parapet,
// a membrane roof with six T-shaped plant units.
const H = 38, BAND = 9, CH = 0.6
{
  const r = ccw(SHOW)
  const cx = r.reduce((s, q) => s + q[0], 0) / 4, cy = r.reduce((s, q) => s + q[1], 0) / 4
  const inset = r.map(([x, y]) => { const dx = cx - x, dy = cy - y, l = Math.hypot(dx, dy); return [x + (dx / l) * CH * Math.SQRT2, y + (dy / l) * CH * Math.SQRT2] as XY })
  for (let i = 0; i < 4; i++) {
    const a = r[i], b = r[(i + 1) % 4], ai = inset[i], bi = inset[(i + 1) % 4]
    green.quad([a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], BAND], [a[0], a[1], BAND])
    sky.quad([a[0], a[1], BAND], [b[0], b[1], BAND], [b[0], b[1], H - CH], [a[0], a[1], H - CH])
    sky.quad([a[0], a[1], H - CH], [b[0], b[1], H - CH], [bi[0], bi[1], H], [ai[0], ai[1], H])
  }
  roof.tri([...inset[0], H], [...inset[1], H], [...inset[2], H])
  roof.tri([...inset[0], H], [...inset[2], H], [...inset[3], H])

  // Pilasters on the north-east face, the long one the car-park photo shows
  // beside the connector: nine shallow piers, the same paint, that catch
  // the light.
  const NE0 = L(at(1)), NE1 = L(at(0))
  const lenNE = Math.hypot(NE1[0] - NE0[0], NE1[1] - NE0[1])
  const f: XY = [(NE1[0] - NE0[0]) / lenNE, (NE1[1] - NE0[1]) / lenNE]
  const out: XY = [f[1], -f[0]] // outward of the counter-clockwise edge E→N
  for (let k = 1; k <= 9; k++) {
    const s = (lenNE * k) / 10
    const c: XY = [NE0[0] + f[0] * s + out[0] * 0.35, NE0[1] + f[1] * s + out[1] * 0.35]
    box(sky, c, f, 1.0, 0.35, BAND, H - CH - 0.2, true)
  }

  // Roof plant: a 13.6 m bar along the building with a 7 m stem towards the
  // south-west, 2.6 m tall, at the six spots NAIP shows.
  const E = L(at(1)), N = L(at(0))
  const along: XY = [(E[0] - N[0]) / Math.hypot(E[0] - N[0], E[1] - N[1]), (E[1] - N[1]) / Math.hypot(E[0] - N[0], E[1] - N[1])]
  const sw: XY = [along[1], -along[0]]
  for (const p of [[77.9, 98.5], [101.7, 90.0], [128.0, 79.8], [66.0, 74.7], [90.7, 64.5], [115.3, 56.0]] as XY[]) {
    const c = L(p)
    box(sky, c, along, 6.8, 1.5, H, H + 2.6)
    box(sky, [c[0] + sw[0] * 4.6, c[1] + sw[1] * 4.6], along, 1.5, 3.1, H, H + 2.6)
  }
}

// ----------------------------------------------------------- connector ----
// Along its axis from the pavilion (t = 0) to the show building (t ≈ 92 m):
// 16 m for the first 41 m, climbing to 31 m by 69 m, then level.
{
  const a0 = L([-18.0, 4.2]), a1 = L([49.0, 67.0])
  const len = Math.hypot(a1[0] - a0[0], a1[1] - a0[1])
  const dir: XY = [(a1[0] - a0[0]) / len, (a1[1] - a0[1]) / len]
  const t0 = a0[0] * dir[0] + a0[1] * dir[1]
  const z = (x: number, y: number) => {
    const t = x * dir[0] + y * dir[1] - t0
    return t <= 41 ? 16 : t >= 69 ? 31 : 16 + ((t - 41) / 28) * 15
  }
  block(sky, roof, CORRIDOR, z, dir, [t0 + 41, t0 + 69])
}

// ------------------------------------------------------------ pavilion ----
// The front eave is 10 m; the roof rises to 17 m 37 m back, then runs level;
// the back block is 20 m. The front's middle (the entrance) is open below 7 m.
const FRONT_Y = -51.3 - ANCHOR[1], RIDGE_Y = -14 - ANCHOR[1], BACK_Y = 33.8 - ANCHOR[1]
const pavZ = (x: number, y: number) => (y >= BACK_Y - 0.05 ? 20 : y >= RIDGE_Y ? 17 : 10 + ((y - FRONT_Y) / (RIDGE_Y - FRONT_Y)) * 7)
{
  const entrance = (a: XY, b: XY) => (Math.abs(a[1] - FRONT_Y) < 0.3 && Math.abs(b[1] - FRONT_Y) < 0.3 ? 5.5 : 0)
  block(slate, roof, PAVILION, pavZ, [0, 1], [RIDGE_Y, BACK_Y - 0.05], 0, entrance)
  // The back block's south face, where it rises over the 17 m roof.
  const bx0 = -94.9 - ANCHOR[0], bx1 = -38.05 - ANCHOR[0]
  slate.quad([bx0, BACK_Y - 0.05, 17], [bx1, BACK_Y - 0.05, 17], [bx1, BACK_Y - 0.05, 20], [bx0, BACK_Y - 0.05, 20])

  // The stepped roof: six shallow risers across the slope, each a 0.6 m lip.
  const x0 = -150 - ANCHOR[0], x1 = 10 - ANCHOR[0]
  for (let k = 1; k <= 6; k++) {
    const y = FRONT_Y + ((RIDGE_Y - FRONT_Y) * k) / 7
    const span = slab(ccw(PAVILION), [0, 1], y - 0.01, y + 0.01)
    if (span.length < 2) continue
    const xs = span.map((q) => q[0])
    const lo = Math.max(x0, Math.min(...xs)) + 2.5, hi = Math.min(x1, Math.max(...xs)) - 2.5
    const z = pavZ(lo, y)
    // A riser facing the front, and the lip's flat top back to the slope.
    roof.quad([lo, y, z], [hi, y, z], [hi, y, z + 0.6], [lo, y, z + 0.6])
    const yb = y + 0.6 / (7 / (RIDGE_Y - FRONT_Y))
    roof.quad([lo, y, z + 0.6], [hi, y, z + 0.6], [hi, yb, z + 0.6], [lo, yb, z + 0.6])
  }

  // The entrance: a recessed glazed front 5 m back under the canopy, with its
  // side returns and soffit.
  const ex0 = -96.6 - ANCHOR[0], ex1 = -66.4 - ANCHOR[0], ey = FRONT_Y + 5
  glazing.quad([ex0, ey, 0], [ex1, ey, 0], [ex1, ey, 5.5], [ex0, ey, 5.5])
  slate.quad([ex0, FRONT_Y, 0], [ex0, ey, 0], [ex0, ey, 5.5], [ex0, FRONT_Y, 5.5])
  slate.quad([ex1, ey, 0], [ex1, FRONT_Y, 0], [ex1, FRONT_Y, 5.5], [ex1, ey, 5.5])
  slate.quad([ex0, FRONT_Y, 5.5], [ex0, ey, 5.5], [ex1, ey, 5.5], [ex1, FRONT_Y, 5.5])

  // The fins: OSM draws each diagonal side as a sawtooth, whose short steps
  // are the faces of the tall fins. Each becomes a pale slab standing 1.4 m
  // proud of the eave, so the sides read as the row of blades they are.
  const p = ccw(PAVILION)
  for (let i = 0; i < p.length; i++) {
    const a = p[i], b = p[(i + 1) % p.length]
    const l = Math.hypot(b[0] - a[0], b[1] - a[1])
    if (l < 0.7 || l > 2.0 || a[1] > -31 - ANCHOR[1] || a[1] < FRONT_Y + 0.5) continue
    const u: XY = [(b[0] - a[0]) / l, (b[1] - a[1]) / l]
    const c: XY = [(a[0] + b[0]) / 2 + u[1] * 0.2, (a[1] + b[1]) / 2 - u[0] * 0.2]
    box(roof, c, u, l / 2 + 0.25, 0.55, 0, pavZ(...c) + 1.4)
  }
}

// --------------------------------------------------------- Starblaster ----
// On the plaza on the pavilion's axis: a broad pylon leaning a little out
// over the plaza to a faceted gold head at 7.6 m, with seven long blades in a
// plane facing the plaza (south). The top blade's tip is the published
// 15.5 m; the side blades span about 17 m, as in the plaza photos.
{
  const base = L([-82.0, -62.0])
  const lean = 1.2, headZ = 7.6
  const head: V3 = [base[0], base[1] - lean, headZ]
  // Pylon: a tapered four-sided column, ground to the head.
  const ring = (cx: number, cy: number, z: number, hx: number, hy: number): V3[] => [[cx - hx, cy - hy, z], [cx + hx, cy - hy, z], [cx + hx, cy + hy, z], [cx - hx, cy + hy, z]]
  const r0 = ring(base[0], base[1], 0, 1.4, 0.9), r1 = ring(head[0], head[1] + 0.4, headZ - 1.0, 0.9, 0.7)
  slate.loft([r0, r1])
  slate.cap(r1, true)
  // Head: an elongated octahedron, nose to the plaza and tipped up.
  const nose: V3 = [head[0], head[1] - 2.8, headZ + 0.6], tail: V3 = [head[0], head[1] + 2.0, headZ - 0.2]
  const mid = (u: number, w: number, h: number): V3 => [head[0] + u * w, head[1] - 0.3, headZ + h]
  const rim = [mid(1, 1.9, 0), mid(0, 0, 1.5), mid(-1, 1.9, 0), mid(0, 0, -1.3)]
  for (let i = 0; i < 4; i++) {
    const a = rim[i], b = rim[(i + 1) % 4]
    gold.tri(nose, a, b)
    gold.tri(tail, b, a)
  }
  // Blades: flat tapered wings from the head, slate at the root and gold at
  // the tip, [angle in the blade plane (0 = east, 90 = up), reach from the
  // head's centre in metres].
  const blades: [number, number][] = [[90, 7.9], [40, 3.8], [140, 3.8], [4, 8.4], [176, 8.4], [-58, 5.4], [-122, 5.4]]
  for (const [deg, reach] of blades) {
    const a = (deg * Math.PI) / 180, d: V3 = [Math.cos(a), 0, Math.sin(a)], n: V3 = [-Math.sin(a), 0, Math.cos(a)]
    const P = (s: number, w: number, y: number): V3 => [head[0] + d[0] * s + n[0] * w, head[1] + 0.4 + y, headZ + d[2] * s + n[2] * w]
    const T = 0.3
    const sect = (s: number, w: number) => [P(s, -w, T), P(s, w, T), P(s, w, -T), P(s, -w, -T)]
    const root = sect(1.0, 0.85), knee = sect(1.0 + (reach - 1.0) * 0.42, 0.65), tip = P(reach, 0, 0)
    slate.loft([root, knee])
    for (let i = 0; i < 4; i++) gold.tri(knee[i], knee[(i + 1) % 4], tip)
  }
}

// --------------------------------------------------------------- write ----
const parts = [
  { part: slate, material: finish('xandar-slate', 0x646a72) },
  { part: roof, material: PALETTE.roof },
  { part: sky, material: finish('rewind-sky', 0xb8cad8) },
  { part: green, material: finish('rewind-sage', 0xa6b597) },
  { part: glazing, material: PALETTE.window },
  { part: gold, material: finish('nova-gold', 0xd8a945, 0.6) },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Guardians of the Galaxy: Cosmic Rewind', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: H,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-epcot-cosmic-rewind.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(REF.lon + ANCHOR[0] / MX).toFixed(7)}, ${(REF.lat + ANCHOR[1] / MY).toFixed(7)}`)
