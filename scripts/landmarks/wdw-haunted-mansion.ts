/**
 * The Haunted Mansion, Liberty Square, Magic Kingdom — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-haunted-mansion.ts
 *
 * Map frame, turned: x along the mansion's front (toward 98°), y back from
 * it (toward 8°), z up, metres. Placed at bearing 8°, the line of the front
 * terrace in OSM. Authored about the area centroid of the OSM outline
 * way/41831418 (-81.5828906, 28.4208882), then shifted so the origin is the
 * mansion's own centre (-81.583103, 28.4204763), on the ground.
 *
 * The outline covers the mansion at its south-west corner and the big show
 * building behind it. The model replaces the outline and the mansion's
 * parts, and leaves the show building's own part (way/1107979230, 8 m) to
 * the map, which draws it as the plain block it is.
 *
 * A Dutch-gothic Hudson Valley manor in red brick with sandstone trim, on a
 * balustraded terrace: a left wing under a steep slate roof with a coped
 * front gable, the square brick tower with its open cupola and copper dome,
 * the centre with its porch, and on the right the tall stone-coped front
 * gable with its twin chimneys and the octagonal glass conservatory in
 * front of it. Iron cresting runs along the ridges.
 *
 * Evidence:
 *  - OSM, measured: the outline; the connector behind the mansion (way/1107979231, 4 m), the mansion's
 *    front strip (way/1107979233), the round end that is the conservatory
 *    (way/1107979236), and two small round parts that sit where the tower
 *    (way/1107979234) and the left chimneys (way/1107979235) stand. OSM's
 *    mansion heights (7–12 m) are too low against every photo, and its
 *    main block (way/1107979232) is drawn turned 45° to the front; neither
 *    is followed.
 *  - Photos: across the Rivers of America with a long lens (SteamFan,
 *    CC BY 2.5), scaled on the conservatory's OSM width (6.5 m): eaves 11 m,
 *    the big gable's apex 15.3 m, chimneys 18 m, tower 14.6 m and its dome
 *    18.6 m above the terrace; close front three-quarter (Eden, Janine and
 *    Jim, CC BY 2.0); front (Benjamin D. Esham, CC BY-SA 4.0).
 *
 * Estimated: the terrace's height (2.5 m), the mansion's depth (11 m) and
 * everything behind the front face, which no photo shows over the trees.
 * The mansion is a little narrower than the photos suggest (~22 m against
 * ~28 m), held to OSM's front strip.
 */
import { Part, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const TAU = Math.PI * 2

const brick = new Part()
const stone = new Part()   // sandstone trim, terrace, porch, the connector
const slate = new Part()   // roofs, iron cresting
const copper = new Part()  // the cupola's dome
const glass = new Part()   // the conservatory
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

/** A slender four-sided stone pinnacle. */
function pinnacle(x: number, y: number, z: number, h: number, w = 0.5) {
  const r: V3[] = [[x - w / 2, y - w / 2, z], [x + w / 2, y - w / 2, z], [x + w / 2, y + w / 2, z], [x - w / 2, y + w / 2, z]]
  for (let i = 0; i < 4; i++) tri(stone, r[i], r[(i + 1) % 4], [x, y, z + h])
}

/** A chimney stack: brick shaft, stone cap. */
function chimney(x: number, y: number, z0: number, z1: number, w = 0.9) {
  prism(brick, [[x - w / 2, y - w / 2], [x + w / 2, y - w / 2], [x + w / 2, y + w / 2], [x - w / 2, y + w / 2]], z0, z1 - 0.4, { top: false })
  prism(stone, [[x - w / 2 - 0.12, y - w / 2 - 0.12], [x + w / 2 + 0.12, y - w / 2 - 0.12], [x + w / 2 + 0.12, y + w / 2 + 0.12], [x - w / 2 - 0.12, y + w / 2 + 0.12]], z1 - 0.4, z1)
}

/**
 * A front gable wall facing -y at y = yf, from x0 to x1: brick up to the
 * eave and the triangle to the apex, with sandstone coping along the rakes
 * and pinnacles at the shoulders. The slate roof behind runs back to y1.
 */
function frontGable(x0: number, x1: number, yf: number, y1: number, z0: number, ze: number, zr: number) {
  prism(brick, [[x0, yf], [x1, yf], [x1, y1], [x0, y1]], z0, ze, { top: false })
  gable(slate, brick, x0, x1, yf, y1, ze, zr, 'y', [true, false])
  const xm = (x0 + x1) / 2, t = 0.55, d = 0.35
  // Coping: a stone band along each rake, just proud of the wall.
  for (const [xa, xb] of [[x0 - 0.15, xm], [x1 + 0.15, xm]]) {
    const s = xb > xa ? 1 : -1
    const A: V3 = [xa, yf - d, ze], B: V3 = [xb, yf - d, zr + t], C: V3 = [xb, yf - d, zr], D: V3 = [xa, yf - d, ze - t]
    const A2: V3 = [xa, yf + 0.3, ze], B2: V3 = [xb, yf + 0.3, zr + t]
    if (s > 0) { quad(stone, D, C, B, A); quad(stone, A, B, B2, A2) } else { quad(stone, A, B, C, D); quad(stone, A2, B2, B, A) }
  }
  pinnacle(x0 - 0.1, yf - 0.1, ze, 1.6)
  pinnacle(x1 + 0.1, yf - 0.1, ze, 1.6)
}

/** Iron cresting along a ridge: a low dark fin. */
function crest(x0: number, y0: number, x1: number, y1: number, z: number, h = 0.6) {
  const nx = -(y1 - y0), ny = x1 - x0, l = Math.hypot(nx, ny), t = 0.08
  const ox = (nx / l) * t, oy = (ny / l) * t
  prism(slate, [[x0 - ox, y0 - oy], [x1 - ox, y1 - oy], [x1 + ox, y1 + oy], [x0 + ox, y0 + oy]], z, z + h)
}

// ---------------------------------------------------------------------------
// Levels, metres above the ground in front of the terrace.

const T = 2.5        // terrace and the mansion's ground floor
const EAVE = 13.5
const RIDGE = 16.5
const APEX = 17.8    // the big front gable
const FRONT = -53.0  // the mansion's front face
const BACK = -42.0
const L = -25.0, R = -3.5

// The connector behind the mansion (way/1107979231), plain, as far back as
// the show building, which the map keeps drawing from its own OSM part.
prism(stone, [[-6.6, -29.7], [-21.9, -29.7], [-21.4, BACK], [-6.5, BACK]], 0, 4)

// The terrace: a stone plinth under the mansion and in front of it, with a
// balustrade along its front.
{
  const tf = -56.6
  prism(stone, [[L - 0.5, tf], [R + 0.7, tf], [R + 0.7, BACK], [L - 0.5, BACK]], 0, T)
  const b = 0.35
  prism(stone, [[L - 0.5, tf], [R + 0.7, tf], [R + 0.7, tf + b], [L - 0.5, tf + b]], T, T + 1.0)
  for (const x of [L - 0.5, R + 0.7]) prism(stone, [[x - (x < L ? 0 : b), tf], [x + (x < L ? b : 0), tf], [x + (x < L ? b : 0), FRONT], [x - (x < L ? 0 : b), FRONT]], T, T + 1.0)
}

// The main body: brick, three storeys, under a hipped slate roof with iron
// cresting along its ridge.
prism(brick, [[L, FRONT], [R, FRONT], [R, BACK], [L, BACK]], T, EAVE, { top: false })
prism(stone, [[L - 0.3, FRONT - 0.3], [R + 0.3, FRONT - 0.3], [R + 0.3, BACK + 0.3], [L - 0.3, BACK + 0.3]], EAVE - 0.5, EAVE)
hip(slate, L - 0.3, R + 0.3, FRONT - 0.3, BACK + 0.3, EAVE, RIDGE + 1.0)
{
  const ym = (FRONT + BACK) / 2, half = (BACK - FRONT) / 2 + 0.3
  crest(L - 0.3 + half, ym, R + 0.3 - half, ym, RIDGE + 1.0)
}

// Sandstone quoins: broad stone strips on the body's corners, the mark of
// the mansion's walls in every photo.
for (const [x, y, sx, sy] of [[L, FRONT, 1, 1], [R, FRONT, -1, 1], [L, BACK, 1, -1], [R, BACK, -1, -1]] as [number, number, number, number][]) {
  const q = 0.7, o = 0.12
  prism(stone, [[x - sx * o, y - sy * o], [x + sx * q, y - sy * o], [x + sx * q, y + sy * q], [x - sx * o, y + sy * q]].map(([a, b]) => [a, b] as XY), T, EAVE - 0.5)
}

// Left wing: a coped front gable in the roof, and the pair of chimneys
// (way/1107979235) behind it.
frontGable(-22.9, -19.3, FRONT - 0.2, FRONT + 4.0, EAVE - 0.5, EAVE, EAVE + 2.6)
chimney(-24.3, -47.8, RIDGE - 1, RIDGE + 3.6)
chimney(-23.2, -47.8, RIDGE - 1, RIDGE + 3.6)

// The big right-hand gable, coped in stone, with stone pilasters, and its
// twin chimneys at the apex.
{
  const x0 = -11.6, x1 = R + 0.3
  frontGable(x0, x1, FRONT - 0.6, BACK + 2, T, EAVE, APEX)
  const xm = (x0 + x1) / 2
  for (const x of [x0 + 0.4, xm, x1 - 0.4])
    prism(stone, [[x - 0.45, FRONT - 0.8], [x + 0.45, FRONT - 0.8], [x + 0.45, FRONT - 0.55], [x - 0.45, FRONT - 0.55]], T, x === xm ? APEX - 0.3 : EAVE)
  chimney(xm - 0.6, FRONT + 0.6, APEX - 1.5, APEX + 2.6, 0.8)
  chimney(xm + 0.6, FRONT + 0.6, APEX - 1.5, APEX + 2.6, 0.8)
  for (const x of [xm - 2.0, xm + 2.0]) windowPanel(glazing, 's', FRONT - 0.6, x, 1.0, EAVE - 3.6, EAVE - 0.8, true)
  windowPanel(glazing, 's', FRONT - 0.6, xm, 0.6, EAVE + 1.0, EAVE + 2.6, true)
}

// The tower (way/1107979234): square brick, a stone cornice, an open
// cupola of stone columns, the copper dome and its weather vane.
{
  const cx = -17.8, cy = FRONT - 0.2, w = 1.6
  const TOP = T + 14.6
  cbox(brick, cx - w, cx + w, cy - w, cy + w, T, TOP, 0.25)
  for (const [sx, sy] of [[-1, -1], [1, -1], [1, 1], [-1, 1]]) {
    const x = cx + sx * w, y = cy + sy * w, q = 0.5, o = 0.1
    prism(stone, [[x + sx * o, y + sy * o], [x - sx * q, y + sy * o], [x - sx * q, y - sy * q], [x + sx * o, y - sy * q]], T, TOP)
  }
  cbox(stone, cx - w - 0.25, cx + w + 0.25, cy - w - 0.25, cy + w + 0.25, TOP, TOP + 0.5, 0.3)
  windowPanel(glazing, 's', cy - w, cx, 0.7, TOP - 4.5, TOP - 2.6, true)
  const cz0 = TOP + 0.5, cz1 = TOP + 2.5
  for (let k = 0; k < 8; k++) {
    const a = (k / 8) * TAU
    frustum(stone, cx + 1.05 * Math.cos(a), cy + 1.05 * Math.sin(a), 0.16, cz0, 0.16, cz1, 4)
  }
  frustum(stone, cx, cy, 0.7, cz0, 0.7, cz1, 8)
  frustum(stone, cx, cy, 1.35, cz1, 1.35, cz1 + 0.4, 8)
  // An onion-ish dome: a short drum, then a swelling cap.
  const prof: [number, number][] = [[1.25, 0], [1.3, 0.5], [1.15, 1.0], [0.8, 1.45], [0.35, 1.75], [0, 1.85]]
  for (let i = 0; i < prof.length - 1; i++) frustum(copper, cx, cy, prof[i][0], cz1 + 0.4 + prof[i][1], prof[i + 1][0], cz1 + 0.4 + prof[i + 1][1], 10)
  frustum(slate, cx, cy, 0.07, cz1 + 2.2, 0.03, cz1 + 3.6, 4)
}

// The centre's porch over the front door.
{
  const x0 = -15.4, x1 = -12.4
  prism(stone, [[x0, FRONT - 2.2], [x1, FRONT - 2.2], [x1, FRONT], [x0, FRONT]], T, T + 3.8)
  gable(slate, stone, x0 - 0.2, x1 + 0.2, FRONT - 2.4, FRONT, T + 3.8, T + 5.2, 'y', [true, false])
  windowPanel(glazing, 's', FRONT - 2.2, (x0 + x1) / 2, 1.3, T + 0.2, T + 2.8, true)
}

// Windows: two storeys on the left wing and the centre, front and sides.
for (const x of [-23.5, -21.1, -15.6, -12.9]) {
  if (x > -16) windowPanel(glazing, 's', FRONT, x + 0.1, 1.0, T + 5.4, T + 8.4, true)
  else {
    windowPanel(glazing, 's', FRONT, x, 1.0, T + 1.0, T + 3.8, true)
    windowPanel(glazing, 's', FRONT, x, 1.0, T + 5.4, T + 8.4, true)
  }
}
windowPanel(glazing, 's', FRONT - 0.2, -21.1, 0.9, EAVE + 0.1, EAVE + 1.8, true)
for (const y of [-50.5, -46.5]) {
  windowPanel(glazing, 'w', L, y, 1.0, T + 1.0, T + 3.8, true)
  windowPanel(glazing, 'w', L, y, 1.0, T + 5.4, T + 8.4, true)
  windowPanel(glazing, 'e', R, y + 1, 1.0, T + 5.4, T + 8.4, true)
}

// The conservatory (way/1107979236): an octagon of glass under a conical
// glass roof, an iron band between, in front of the big gable.
{
  const cx = -8.3, cy = -53.6, r = 3.1, n = 8, wallTop = T + 3.4
  frustum(stone, cx, cy, r + 0.15, T, r + 0.15, T + 0.5, n, { flat: true, phase: 1 / 16 })
  frustum(glass, cx, cy, r, T + 0.5, r, wallTop, n, { flat: true, phase: 1 / 16 })
  frustum(slate, cx, cy, r + 0.2, wallTop, r + 0.2, wallTop + 0.4, n, { flat: true, phase: 1 / 16 })
  frustum(glass, cx, cy, r + 0.2, wallTop + 0.4, 0, T + 7.7, n, { flat: true, phase: 1 / 16 })
  frustum(slate, cx, cy, 0.1, T + 7.6, 0, T + 8.6, 4)
}

// ---------------------------------------------------------------------------

// Red brick and sandstone are the mansion's identity; the slate is dark and
// kept so, no darker than charcoal, as the roofs read dark in every photo.
// The dome is its weathered copper-bronze; the conservatory is glass.
const parts = [
  { part: brick, material: finish('mansion-brick', 0xc98a78) },
  { part: stone, material: PALETTE.stone },
  { part: slate, material: finish('mansion-slate', 0x666d78) },
  { part: copper, material: finish('mansion-dome', 0xa47f62) },
  { part: glass, material: PALETTE.glass },
  { part: glazing, material: PALETTE.window },
]
// Shift to the mansion's centre (Part stores glTF axes: x, z, -y).
const OX = -14.25, OY = -48.0
for (const { part } of parts) for (let i = 0; i < part.pos.length; i += 3) { part.pos[i] -= OX; part.pos[i + 2] += OY }
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Haunted Mansion', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 8, elevation: 0, height: T + 14.6 + 6.5,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-haunted-mansion.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
