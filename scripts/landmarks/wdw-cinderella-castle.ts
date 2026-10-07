/**
 * Cinderella Castle, Magic Kingdom, Walt Disney World — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-cinderella-castle.ts
 *
 * Map frame: x east, y north, z up, metres. Origin at the area centroid of the
 * OSM outline way/121516748 (-81.5811782, 28.4195023), on the ground. Placed
 * at bearing 0: the archway runs due north–south, from the hub (south) into
 * Fantasyland (north), and OSM's parts sit square to it.
 *
 * The castle is three layers, and the model keeps them:
 *  - a grey stone base: the curtain wall on the OSM outline, ringed by round
 *    towers with blue conical roofs, the front pair with a second blue
 *    "skirt" roof below the cone; pierced north–south by the archway;
 *  - the pink upper castle: the keep, the rear hall over the Fantasyland
 *    terrace with its tall gothic windows and gables, the pink gable over the
 *    archway with the clock and a steep blue roof behind it;
 *  - the turrets and the central spire, east of the arch's axis, with two
 *    galleries, a gold upper stage and a gold needle.
 *
 * Evidence (the doubts are listed at the end):
 *  - OSM, measured: the outline (846 m², 35 × 33 m) and its building:parts —
 *    the round towers with their heights (way/1109167338–1109167344: 19 and
 *    22 m), the keep (way/1109167349, 24 m), the spire (way/1109167351,
 *    55 m), the west tower (way/1109167352, 35 m), the front-right turret
 *    (way/1351675224, 19 m), the square tower (way/1351675222/3, 15–18 m) and
 *    the base (way/1109167348, 13 m). The archway's axis is the notch in the
 *    outline's south side, x = −2.3, midway between the two front towers.
 *  - Published: 189 ft (57.6 m) to the spire's tip (Wikipedia). OSM says 55.
 *    The model's needle ends at 55.5 m and its finial at 57.5.
 *  - Photos (Wikimedia Commons / Flickr via Openverse, licensed): front from
 *    the hub (Jedi94, 2024, CC BY-SA 4.0), rear from Fantasyland (Farragutful,
 *    2022, CC BY-SA 4.0), the west side across the moat (mrkathika, CC BY-SA
 *    2.0), front (Jazzmanluc5, CC BY-SA 3.0); USGS aerial (public domain)
 *    for the plan. The spire's offset east of the arch, ~4 m, is measured on
 *    the 2024 front photo.
 *  - Look-only: a fan-drawn north elevation (The Disney Blueprint Page,
 *    CC BY-NC-SA) gave the relative heights of the terrace (7 m), the rear
 *    hall (18 m), the keep's crenellation (22 m) and the spire's two
 *    galleries (28 and 37 m above the main floor), scaled ×1.1 here to the
 *    published height. Its geometry is not copied; it only confirmed levels
 *    the photos show.
 *
 * Colours are the castle's since its 2021 repaint: grey stone base, pale
 * rose-pink upper walls, royal-blue roofs and gold finials and spire.
 *
 * Doubts: heights other than OSM's and the published total are estimated
 * from photos taken from the ground, which foreshorten the top. The turrets
 * on the keep are placed from photos, not OSM, which only maps the west
 * tower and the spire up there. The real castle has many more small turrets
 * and pinnacles; the model keeps the ones that make its silhouette. The
 * archway is open right through, as it really is.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const TAU = Math.PI * 2

const stone = new Part()   // grey stone base and towers
const pink = new Part()    // upper walls
const blue = new Part()    // roofs
const gold = new Part()    // finials, spire top, clock
const glazing = new Part() // windows

// ---------------------------------------------------------------------------
// Kit

const unit = (a: V3): V3 => { const l = len(a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) { p.tri(a, b, c, undefined, undefined, undefined, n) }
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: [V3, V3, V3, V3]) {
  tri(p, a, b, c, n && [n[0], n[1], n[2]])
  tri(p, a, c, d, n && [n[0], n[2], n[3]])
}

/**
 * A smooth-shaded frustum about a vertical axis: radius r0 at z0 to r1 at z1.
 * r1 = 0 makes a cone. `n` segments, rotated by `phase` turns.
 */
function frustum(p: Part, cx: number, cy: number, r0: number, z0: number, r1: number, z1: number,
  n: number, o: { top?: boolean; bottom?: boolean; phase?: number; flat?: boolean } = {}) {
  const ph = (o.phase ?? 0.5 / n) * TAU
  const k = (r0 - r1) / (z1 - z0)
  const at = (i: number, r: number, z: number): V3 => {
    const t = ph + (i / n) * TAU
    return [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
  }
  const nrm = (t: number): V3 => unit([Math.cos(t), Math.sin(t), k])
  for (let i = 0; i < n; i++) {
    const j = i + 1
    const ti = ph + (i / n) * TAU, tj = ph + (j / n) * TAU
    const ni = o.flat ? nrm((ti + tj) / 2) : nrm(ti), nj = o.flat ? ni : nrm(tj)
    if (r1 <= 0) {
      tri(p, at(i, r0, z0), at(j, r0, z0), [cx, cy, z1], [ni, nj, nrm((ti + tj) / 2)])
    } else {
      quad(p, at(i, r0, z0), at(j, r0, z0), at(j, r1, z1), at(i, r1, z1), [ni, nj, nj, ni])
    }
  }
  if (o.top && r1 > 0) for (let i = 1; i < n - 1; i++) tri(p, at(0, r1, z1), at(i, r1, z1), at(i + 1, r1, z1))
  if (o.bottom) for (let i = 1; i < n - 1; i++) tri(p, at(0, r0, z0), at(i + 1, r0, z0), at(i, r0, z0))
}

const area = (q: XY[]) => q.reduce((s, a, i) => { const b = q[(i + 1) % q.length]; return s + a[0] * b[1] - b[0] * a[1] }, 0) / 2

/** Keep the part of a polygon where a·x + b·y + c ≥ 0 (Sutherland–Hodgman). */
function clip(poly: XY[], a: number, b: number, c: number): XY[] {
  const f = (p: XY) => a * p[0] + b * p[1] + c
  const out: XY[] = []
  for (let i = 0; i < poly.length; i++) {
    const p = poly[i], q = poly[(i + 1) % poly.length], fp = f(p), fq = f(q)
    if (fp >= 0) out.push(p)
    if ((fp >= 0) !== (fq >= 0)) {
      const t = fp / (fp - fq)
      out.push([p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t])
    }
  }
  return out
}

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
function earcut(poly: XY[]): [number, number, number][] {
  const idx = poly.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 1e-9 && crossz(b, c, p) > 1e-9 && crossz(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = poly[i0], b = poly[i1], c = poly[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(poly[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) idx.splice(0, 1) // degenerate remainder
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

/** A vertical prism over a counter-clockwise polygon, flat-shaded. */
function prism(p: Part, poly: XY[], z0: number, z1: number, o: { top?: boolean; bottom?: boolean; skip?: (a: XY, b: XY) => boolean } = {}) {
  if (area(poly) < 0) poly = [...poly].reverse()
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    if (Math.hypot(b[0] - a[0], b[1] - a[1]) < 1e-6 || o.skip?.(a, b)) continue
    quad(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
  }
  const tris = earcut(poly)
  if (o.top !== false) for (const [i, j, k] of tris) tri(p, [...poly[i], z1] as V3, [...poly[j], z1] as V3, [...poly[k], z1] as V3)
  if (o.bottom) for (const [i, j, k] of tris) tri(p, [...poly[i], z0] as V3, [...poly[k], z0] as V3, [...poly[j], z0] as V3)
}

/** A box with its vertical edges chamfered by c. */
function cbox(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, c = 0.4, top = true) {
  prism(p, [[x0 + c, y0], [x1 - c, y0], [x1, y0 + c], [x1, y1 - c], [x1 - c, y1], [x0 + c, y1], [x0, y1 - c], [x0, y0 + c]], z0, z1, { top })
}

/**
 * A steep gable roof over a rectangle, ridge along y (`along` 'y') or x.
 * The slopes go to `roof`, the triangular gable ends to `wall`.
 */
function gable(roof: Part, wall: Part | null, x0: number, x1: number, y0: number, y1: number, ze: number, zr: number, along: 'x' | 'y', ends: [boolean, boolean] = [true, true]) {
  if (along === 'y') {
    const xm = (x0 + x1) / 2
    quad(roof, [x1, y0, ze], [x1, y1, ze], [xm, y1, zr], [xm, y0, zr])
    quad(roof, [x0, y1, ze], [x0, y0, ze], [xm, y0, zr], [xm, y1, zr])
    if (wall && ends[0]) tri(wall, [x0, y0, ze], [x1, y0, ze], [xm, y0, zr])
    if (wall && ends[1]) tri(wall, [x1, y1, ze], [x0, y1, ze], [xm, y1, zr])
  } else {
    const ym = (y0 + y1) / 2
    quad(roof, [x0, y0, ze], [x1, y0, ze], [x1, ym, zr], [x0, ym, zr])
    quad(roof, [x1, y1, ze], [x0, y1, ze], [x0, ym, zr], [x1, ym, zr])
    if (wall && ends[0]) tri(wall, [x0, y1, ze], [x0, y0, ze], [x0, ym, zr])
    if (wall && ends[1]) tri(wall, [x1, y0, ze], [x1, y1, ze], [x1, ym, zr])
  }
}

/** A hipped roof over a rectangle, ridge along its longer side. */
function hip(p: Part, x0: number, x1: number, y0: number, y1: number, ze: number, zr: number) {
  const w = x1 - x0, d = y1 - y0
  if (w >= d) {
    const ym = (y0 + y1) / 2, a = x0 + d / 2, b = x1 - d / 2
    quad(p, [x0, y0, ze], [x1, y0, ze], [b, ym, zr], [a, ym, zr])
    quad(p, [x1, y1, ze], [x0, y1, ze], [a, ym, zr], [b, ym, zr])
    tri(p, [x1, y0, ze], [x1, y1, ze], [b, ym, zr])
    tri(p, [x0, y1, ze], [x0, y0, ze], [a, ym, zr])
  } else {
    const xm = (x0 + x1) / 2, a = y0 + w / 2, b = y1 - w / 2
    quad(p, [x1, y0, ze], [x1, y1, ze], [xm, b, zr], [xm, a, zr])
    quad(p, [x0, y1, ze], [x0, y0, ze], [xm, a, zr], [xm, b, zr])
    tri(p, [x0, y0, ze], [x1, y0, ze], [xm, a, zr])
    tri(p, [x1, y1, ze], [x0, y1, ze], [xm, b, zr])
  }
}

/** Merlons along a polygon's edges, set in from the face by nothing: flush with the wall. */
function merlons(p: Part, poly: XY[], z: number, h: number, w: number, d: number, pitch: number, keep?: (a: XY, b: XY) => boolean) {
  if (area(poly) < 0) poly = [...poly].reverse()
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    if (keep && !keep(a, b)) continue
    const L = Math.hypot(b[0] - a[0], b[1] - a[1])
    const n = Math.floor((L - w) / pitch)
    if (n < 0 || L < w + 0.4) continue
    const ux = (b[0] - a[0]) / L, uy = (b[1] - a[1]) / L, nx = -uy, ny = ux // inward normal (CCW)
    const start = (L - n * pitch - w) / 2
    for (let k = 0; k <= n; k++) {
      const s = start + k * pitch
      const c0: XY = [a[0] + ux * s, a[1] + uy * s], c1: XY = [a[0] + ux * (s + w), a[1] + uy * (s + w)]
      prism(p, [c0, c1, [c1[0] + nx * d, c1[1] + ny * d], [c0[0] + nx * d, c0[1] + ny * d]], z, z + h)
    }
  }
}

/** Merlons round a circle. */
function ringMerlons(p: Part, cx: number, cy: number, r: number, z: number, h: number, count: number, d = 0.35) {
  for (let k = 0; k < count; k++) {
    const t0 = (k / count) * TAU, t1 = t0 + (TAU / count) * 0.5
    const P = (t: number, rr: number): XY => [cx + rr * Math.cos(t), cy + rr * Math.sin(t)]
    prism(p, [P(t0, r - d), P(t0, r), P(t1, r), P(t1, r - d)], z, z + h)
  }
}

/**
 * A flat window panel on a wall plane, 0.05 m proud: rectangular, or with a
 * pointed (gothic) head. `face` is the wall's outward normal axis.
 */
function windowPanel(p: Part, face: 'n' | 's' | 'e' | 'w', plane: number, u: number, w: number, z0: number, z1: number, pointed = false) {
  const pts: [number, number][] = [[-w / 2, z0], [w / 2, z0]]
  if (pointed) {
    const spring = z1 - w * 0.75
    pts.push([w / 2, spring], [w * 0.36, spring + w * 0.42], [0, z1], [-w * 0.36, spring + w * 0.42], [-w / 2, spring])
  } else pts.push([w / 2, z1], [-w / 2, z1])
  const off = 0.05
  const to = ([s, z]: [number, number]): V3 =>
    face === 's' ? [u + s, plane - off, z] : face === 'n' ? [u - s, plane + off, z]
      : face === 'e' ? [plane + off, u + s, z] : [plane - off, u - s, z]
  for (let i = 1; i < pts.length - 1; i++) tri(p, to(pts[0]), to(pts[i]), to(pts[i + 1]))
}

/** A gold finial spike from z0 up by h. */
function finial(cx: number, cy: number, z0: number, h: number, r = 0.16) {
  frustum(gold, cx, cy, r, z0, 0, z0 + h, 4)
}

// ---------------------------------------------------------------------------
// Plan, from OSM way/121516748, Douglas–Peucker simplified to 0.45 m (the
// round towers' bumps reduce to a few corners, and the drums cover them).

const OUTLINE: XY[] = [[-6.4, 14.6], [-12.9, 10.0], [-14.2, 10.4], [-15.6, 9.6], [-15.9, 7.7], [-14.6, 6.5], [-15.6, 2.7], [-17.6, 1.6], [-17.6, 0.1], [-15.7, -1.1], [-15.1, -5.3], [-15.9, -6.3], [-15.9, -7.8], [-15.1, -8.7], [-13.5, -9.1], [-10.7, -12.5], [-11.0, -14.0], [-10.1, -15.6], [-8.0, -16.4], [-4.9, -14.9], [-4.9, -15.4], [0.3, -15.4], [0.3, -14.2], [1.2, -14.2], [2.1, -15.7], [4.4, -16.3], [5.9, -15.3], [6.4, -13.7], [8.3, -12.8], [8.3, -13.7], [10.3, -13.7], [11.5, -12.8], [12.8, -9.8], [12.1, -9.3], [13.5, -7.3], [15.0, -7.9], [16.4, -4.6], [15.1, -4.0], [15.8, -0.1], [17.1, 0.9], [17.4, 2.1], [16.6, 3.5], [15.3, 3.9], [14.4, 7.4], [16.3, 8.8], [16.1, 10.4], [14.0, 11.4], [13.5, 13.5], [12.6, 13.8], [11.7, 12.6], [10.0, 12.6], [6.6, 14.6], [6.7, 15.6], [6.0, 16.1], [4.9, 15.0], [1.2, 15.1], [0.3, 16.3], [-0.7, 16.1], [-1.1, 15.0], [-4.5, 15.1], [-5.7, 16.1]]

const AX = -2.3                 // archway axis
const ARCH_W = 4.6, ARCH_SPRING = 3.3
const AX0 = AX - ARCH_W / 2, AX1 = AX + ARCH_W / 2
const WALL = 10.5               // curtain wall top, south of the terrace
const TERRACE = 7.5             // the Fantasyland terrace over the rear arcade
const TERRACE_Y = 9.0           // where the base steps down to it
const FRONT = -15.4, REAR = 15.0 // the archway's two faces

// ---------------------------------------------------------------------------
// 1. Stone base: the outline in four pieces round the archway, stepping down
// to the terrace at the back.

const west = clip(OUTLINE, -1, 0, AX0)  // x ≤ AX0
const east = clip(OUTLINE, 1, 0, -AX1)  // x ≥ AX1
for (const side of [west, east]) {
  const front = clip(side, 0, -1, TERRACE_Y), rear = clip(side, 0, 1, -TERRACE_Y)
  prism(stone, front, 0, WALL)
  prism(stone, rear, 0, TERRACE)
  // Crenellations on the outer faces only, not along the cuts.
  const outer = (a: XY, b: XY) => !(Math.abs(a[0] - b[0]) < 1e-6 && (Math.abs(a[0] - AX0) < 1e-6 || Math.abs(a[0] - AX1) < 1e-6))
    && !(Math.abs(a[1] - b[1]) < 1e-6 && Math.abs(a[1] - TERRACE_Y) < 1e-6)
  merlons(stone, front, WALL, 0.9, 0.8, 0.45, 1.6, outer)
}

// The rear arcade's open arch east of the archway, into the shop under the
// terrace; it glows at night like the shopfronts it stands for.
windowPanel(glazing, 'n', REAR, 3.0, 2.8, 0, 5.0, true)

// The archway: a semicircular vault through the middle strip.
{
  const R = ARCH_W / 2, N = 10
  const arc: XY[] = [] // (x, z), west to east
  for (let i = 0; i <= N; i++) {
    const t = Math.PI - (i / N) * Math.PI
    arc.push([AX + R * Math.cos(t), ARCH_SPRING + R * Math.sin(t)])
  }
  const spans: [number, number, number][] = [[FRONT, TERRACE_Y, WALL], [TERRACE_Y, REAR, TERRACE]]
  for (const [y0, y1, top] of spans) {
    // Front and rear faces: the strip between the arch and the top.
    for (let i = 0; i < N; i++) {
      const [xa, za] = arc[i], [xb, zb] = arc[i + 1]
      quad(stone, [xa, y0, za], [xb, y0, zb], [xb, y0, top], [xa, y0, top])
      quad(stone, [xb, y1, zb], [xa, y1, za], [xa, y1, top], [xb, y1, top])
      // The vault, facing down into the passage.
      const nA = unit([AX - xa, 0, ARCH_SPRING - za]), nB = unit([AX - xb, 0, ARCH_SPRING - zb])
      quad(stone, [xa, y1, za], [xb, y1, zb], [xb, y0, zb], [xa, y0, za], [nA, nB, nB, nA])
    }
    quad(stone, [AX0, y0, top], [AX1, y0, top], [AX1, y1, top], [AX0, y1, top])
  }
  // The arch's jambs below the springing are the cut faces of the two sides.
}

// ---------------------------------------------------------------------------
// 2. Round towers on the curtain wall. OSM centres, radii and heights.

type Tower = { c: XY; r: number; h: number; skirt?: boolean; seg?: number }
const TOWERS: Tower[] = [
  { c: [-8.5, -13.9], r: 2.7, h: 22, skirt: true },  // front, west of the arch
  { c: [3.9, -14.0], r: 2.6, h: 22, skirt: true },   // front, east of the arch
  { c: [-14.1, -7.0], r: 2.1, h: 22, skirt: true },  // south-west corner
  { c: [-15.6, 0.8], r: 2.0, h: 19 },                // west
  { c: [-14.2, 8.4], r: 2.0, h: 22, skirt: true },   // north-west
  { c: [15.5, 1.8], r: 2.0, h: 22, skirt: true },    // east
  { c: [14.4, 9.4], r: 2.2, h: 19 },                 // north-east
]
for (const t of TOWERS) {
  // Levels from the 2024 front photo, scaled on the arch: the drum stands a
  // little over the wall, a corbelled crown, then either a blue skirt roof,
  // a short upper drum and the cone, or the cone straight off the crown.
  const [x, y] = t.c, r = t.r, n = t.seg ?? 14
  const drum = WALL + 0.4
  frustum(stone, x, y, r, 0, r, drum, n)
  frustum(stone, x, y, r, drum, r + 0.35, drum + 0.5, n)
  frustum(stone, x, y, r + 0.35, drum + 0.5, r + 0.35, drum + 1.1, n, { top: true })
  const z0 = drum + 1.1
  if (t.skirt) {
    // The upper drum is about two-thirds the lower one's width.
    const r2 = r * 0.64
    frustum(blue, x, y, r + 0.3, z0, r2, z0 + 1.3, n)
    frustum(stone, x, y, r2, z0 + 1.3, r2, z0 + 2.4, n)
    frustum(stone, x, y, r2, z0 + 2.4, r2 + 0.25, z0 + 2.8, n)
    frustum(blue, x, y, r2 + 0.3, z0 + 2.8, 0, t.h - 1.2, n)
    finial(x, y, t.h - 1.4, 1.9)
  } else {
    ringMerlons(stone, x, y, r + 0.35, z0, 0.8, 8)
    frustum(blue, x, y, r - 0.1, z0 + 0.2, 0, t.h - 1.0, n)
    finial(x, y, t.h - 1.2, 1.7)
  }
}

// Front-right turret (way/1351675224) and the south-west corner turret on
// the wall between the corner and front towers.
for (const [x, y, r, top] of [[9.6, -11.2, 1.4, 22.5], [-12.1, -11.0, 1.0, 19.5]] as [number, number, number, number][]) {
  frustum(stone, x, y, r, 0, r, WALL + 3.5, 12)
  frustum(stone, x, y, r, WALL + 3.5, r + 0.3, WALL + 4.0, 12, { top: true })
  frustum(blue, x, y, r + 0.3, WALL + 4.0, 0, top, 12)
  finial(x, y, top - 0.2, 1.5)
}

// The square tower on the east front (way/1351675222/3), crenellated, with
// a small turret on its outer corner.
{
  const sq: XY[] = [[12.0, -7.6], [15.9, -7.6], [15.9, -3.5], [12.0, -3.5]]
  prism(stone, sq, 0, 15)
  merlons(stone, sq, 15, 0.9, 0.8, 0.45, 1.5)
  frustum(stone, 15.6, -3.9, 0.8, 15, 0.8, 16.5, 10)
  frustum(blue, 15.6, -3.9, 1.0, 16.5, 0, 20, 10)
  finial(15.6, -3.9, 19.8, 1.3)
}

// ---------------------------------------------------------------------------
// 3. The pink upper castle.

// The keep (way/1109167349), 24 m with a crenellated parapet.
const KEEP = { x0: -7.6, x1: 5.8, y0: -8.8, y1: 5.1 }
const KEEP_TOP = 25.0
cbox(pink, KEEP.x0, KEEP.x1, KEEP.y0, KEEP.y1, WALL, KEEP_TOP, 0.5, false)
// A low blue hip behind the parapet: from the ground it hides behind the
// merlons, from above the keep reads roofed, as it is, not as a flat deck.
hip(blue, KEEP.x0 + 0.3, KEEP.x1 - 0.3, KEEP.y0 + 0.3, KEEP.y1 - 0.3, KEEP_TOP, 29.0)
merlons(pink, [[KEEP.x0, KEEP.y0], [KEEP.x1, KEEP.y0], [KEEP.x1, KEEP.y1], [KEEP.x0, KEEP.y1]], KEEP_TOP, 0.9, 0.8, 0.45, 1.5)
// The blue lean-to roofs round the keep's foot, over the wall walk: the
// band of blue the photos show between the grey base and the pink keep.
{
  const d = 2.4, zi = WALL + 2.6, zo = WALL + 0.5
  const i: XY[] = [[KEEP.x0, KEEP.y0], [KEEP.x1, KEEP.y0], [KEEP.x1, KEEP.y1], [KEEP.x0, KEEP.y1]]
  const o: XY[] = [[KEEP.x0 - d, KEEP.y0 - d], [KEEP.x1 + d, KEEP.y0 - d], [KEEP.x1 + d, KEEP.y1], [KEEP.x0 - d, KEEP.y1]]
  // South, east and west sides; the north side meets the rear hall.
  for (const [a, b] of [[0, 1], [1, 2], [3, 0]]) {
    quad(blue, [o[a][0], o[a][1], zo], [o[b][0], o[b][1], zo], [i[b][0], i[b][1], zi], [i[a][0], i[a][1], zi])
  }
}
// Keep windows: two storeys on the south face and the sides.
for (const u of [-6.2, -5.0]) windowPanel(glazing, 's', KEEP.y0, u, 0.8, 14.5, 16.6, true)
for (const u of [3.9, 5.0]) windowPanel(glazing, 's', KEEP.y0, u, 0.7, 19.5, 21.2, true)
for (const u of [-6.0, -2.0, 2.0]) {
  windowPanel(glazing, 'w', KEEP.x0, u, 0.9, 15.5, 18.0, true)
  windowPanel(glazing, 'e', KEEP.x1, u, 0.9, 15.5, 18.0, true)
}

// The keep's steep blue roof, west of the spire, with its pink gable and a
// dormer window facing the hub.
{
  const x0 = -5.4, x1 = -0.8, y0 = -8.4, y1 = -3.4
  gable(blue, pink, x0, x1, y0, y1, KEEP_TOP + 0.9, 33.5, 'y')
  windowPanel(glazing, 's', y0, (x0 + x1) / 2, 0.9, 27.0, 29.8, true)
  finial((x0 + x1) / 2, y0, 33.0, 1.8)
}

// The rear hall over the Fantasyland terrace: tall gothic windows and four
// blue-roofed gables facing north.
{
  const y0 = 4.5, y1 = 10.0, x0 = -11.2, x1 = 10.6, top = 18.0
  cbox(pink, x0, x1, y0, y1, TERRACE, top, 0.4)
  const bays = [-8.6, -4.0, 0.6, 5.4, 8.6]
  for (const u of [-9.4, -6.6, -3.6, 2.6, 5.4, 8.2]) windowPanel(glazing, 'n', y1, u, 1.5, TERRACE + 3.0, TERRACE + 8.6, true)
  // Gables over the bays, roofs running back into the keep.
  for (const [g0, g1] of [[-11.2, -6.0], [-5.6, -0.6], [0.2, 5.2], [5.6, 10.6]] as [number, number][]) {
    gable(blue, pink, g0, g1, y0 + 1.5, y1, top, top + 3.8, 'y', [true, true])
    finial((g0 + g1) / 2, y1, top + 3.5, 1.5)
  }
  void bays
}

// The pink frame over the archway on the hub side, the steep blue roof
// behind it running back to the keep, and the narrow pink gothic gable with
// the clock standing in front of the roof's blue gable end.
{
  const x0 = AX - 2.7, x1 = AX + 2.7, yf = FRONT - 0.4
  const eave = WALL + 1.0, ridge = 21.5
  prism(pink, [[x0, yf], [x1, yf], [x1, FRONT + 0.1], [x0, FRONT + 0.1]], ARCH_SPRING + ARCH_W / 2 + 0.4, eave)
  gable(blue, blue, x0, x1, yf, KEEP.y0 + 0.2, eave, ridge, 'y', [true, false])
  // The gothic gable: a tall pointed pink panel just proud of the blue end.
  const g = 1.35, yg = yf - 0.25
  prism(pink, [[AX - g, yg], [AX + g, yg], [AX + g, yf + 0.3], [AX - g, yf + 0.3]], eave, eave + 2.2)
  quad(pink, [AX - g, yg, eave + 2.2], [AX + g, yg, eave + 2.2], [AX + 0.05, yg, ridge - 0.3], [AX - 0.05, yg, ridge - 0.3])
  quad(pink, [AX + g, yg, eave + 2.2], [AX + g, yf + 0.3, eave + 2.2], [AX + 0.05, yf + 0.3, ridge - 0.3], [AX + 0.05, yg, ridge - 0.3])
  quad(pink, [AX - g, yf + 0.3, eave + 2.2], [AX - g, yg, eave + 2.2], [AX - 0.05, yg, ridge - 0.3], [AX - 0.05, yf + 0.3, ridge - 0.3])
  // Pinnacles either side.
  for (const x of [x0 - 0.1, x1 + 0.1]) {
    frustum(pink, x, yf + 0.3, 0.35, WALL - 1, 0.35, eave + 2.5, 6)
    frustum(gold, x, yf + 0.3, 0.42, eave + 2.5, 0, eave + 7.0, 6)
  }
  // The clock, gold, on the gable.
  {
    const cz = 14.6, R = 0.95, n = 12, y = yg - 0.06
    for (let i = 0; i < n; i++) {
      const a = (i / n) * TAU, b = ((i + 1) / n) * TAU
      tri(gold, [AX, y, cz], [AX + R * Math.cos(b), y, cz + R * Math.sin(b)], [AX + R * Math.cos(a), y, cz + R * Math.sin(a)])
    }
  }
  // The balcony window over the arch.
  windowPanel(glazing, 's', yf, AX, 2.6, 8.4, 11.8, true)
  finial(AX, yg, ridge - 0.5, 2.0)
}

// ---------------------------------------------------------------------------
// 4. Turrets on the keep: pink shafts and blue cones, gold finials.

type Turret = { c: XY; r: number; z0: number; gallery: number; tip: number }
const TURRETS: Turret[] = [
  { c: [-7.5, -0.5], r: 1.9, z0: WALL, gallery: 29.0, tip: 39.5 }, // west tower (way/1109167352)
  { c: [5.4, -2.6], r: 1.4, z0: KEEP_TOP - 2, gallery: 31.5, tip: 40.0 }, // east of the spire
  { c: [-1.2, 1.6], r: 1.15, z0: KEEP_TOP - 2, gallery: 34.6, tip: 44.0 }, // behind the keep's gable
  { c: [-7.0, 4.4], r: 1.2, z0: KEEP_TOP - 2, gallery: 27.5, tip: 33.5 },
  { c: [5.2, 4.4], r: 1.2, z0: KEEP_TOP - 2, gallery: 28.5, tip: 35.0 },
  { c: [-11.0, 9.6], r: 0.9, z0: TERRACE, gallery: 20.5, tip: 25.5 }, // rear hall corners
  { c: [10.4, 9.6], r: 0.9, z0: TERRACE, gallery: 20.5, tip: 25.5 },
]
for (const t of TURRETS) {
  const [x, y] = t.c, r = t.r, n = r > 1.5 ? 14 : 10
  frustum(pink, x, y, r, t.z0, r, t.gallery, n)
  frustum(pink, x, y, r, t.gallery, r + 0.3, t.gallery + 0.5, n)
  frustum(pink, x, y, r + 0.3, t.gallery + 0.5, r + 0.3, t.gallery + 1.0, n, { top: true })
  frustum(blue, x, y, r + 0.35, t.gallery + 1.0, 0, t.tip, n)
  finial(x, y, t.tip - 0.3, r > 1.5 ? 2.2 : 1.6)
}
// The west tower's two windows facing the hub and the west.
windowPanel(glazing, 's', -0.5 - 1.9, -7.5, 0.7, 25.5, 27.5, true)

// ---------------------------------------------------------------------------
// 5. The central spire (way/1109167351), east of the archway's axis.

{
  const x = 1.9, y = -7.0
  const n = 12
  // Pink shaft to the first gallery, just over the keep's parapet.
  frustum(pink, x, y, 2.0, WALL, 2.0, 28.0, n)
  frustum(pink, x, y, 2.0, 28.0, 2.5, 28.9, n)
  frustum(gold, x, y, 2.5, 28.9, 2.5, 29.6, n, { top: true })
  // The long upper stage to the second gallery, ringed by gold pinnacles.
  frustum(gold, x, y, 1.45, 29.6, 1.45, 40.0, n)
  for (let k = 0; k < 4; k++) {
    const a = (k / 4 + 1 / 8) * TAU
    frustum(gold, x + 2.05 * Math.cos(a), y + 2.05 * Math.sin(a), 0.32, 29.6, 0, 35.0, 6)
    frustum(gold, x + 1.45 * Math.cos(a + TAU / 8), y + 1.45 * Math.sin(a + TAU / 8), 0.25, 36.0, 0, 39.8, 5)
  }
  frustum(gold, x, y, 1.45, 40.0, 1.85, 40.7, n)
  frustum(gold, x, y, 1.85, 40.7, 1.85, 41.3, n, { top: true })
  // Lantern, its pinnacles, and the gold needle.
  frustum(gold, x, y, 0.95, 41.3, 0.95, 44.0, 8)
  for (let k = 0; k < 4; k++) {
    const a = (k / 4 + 1 / 8) * TAU
    frustum(gold, x + 1.4 * Math.cos(a), y + 1.4 * Math.sin(a), 0.2, 41.3, 0, 45.5, 5)
  }
  frustum(gold, x, y, 0.85, 44.0, 0, 55.5, 8)
  finial(x, y, 55.0, 2.5, 0.12)
  // Slit windows on the shaft, south, east and west.
  windowPanel(glazing, 's', y - 2.0, x, 0.6, 21.5, 24.0, true)
  windowPanel(glazing, 'w', x - 2.0, y, 0.6, 21.5, 24.0, true)
  windowPanel(glazing, 'e', x + 2.0, y, 0.6, 21.5, 24.0, true)
}

// ---------------------------------------------------------------------------

// Identity colours (STYLE.md: a finish where colour is identity): the castle
// reads as grey stone, rose pink and royal blue with gold. The blue is the
// one dark colour, kept well clear of charcoal because the roofs are what
// everyone knows it by; the stone and pink sit at the palette's lightness.
const parts = [
  { part: stone, material: finish('castle-stone', 0xc3c6cc) },
  { part: pink, material: finish('castle-pink', 0xefd0ca) },
  { part: blue, material: finish('castle-blue', 0x4a72c4, 0.6) },
  { part: gold, material: finish('castle-gold', 0xd9b45e, 0.5) },
  { part: glazing, material: PALETTE.window },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Cinderella Castle', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 57.5,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-cinderella-castle.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
void cross; void sub
