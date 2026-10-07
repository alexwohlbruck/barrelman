/**
 * Main Street, U.S.A. Station (Walt Disney World Railroad), Magic Kingdom —
 * procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-main-street-station.ts
 *
 * Map frame: x east, y north, z up, metres. Origin at the area centroid of
 * the OSM outline way/296125663 (-81.5812004, 28.4164917), on the ground of
 * the entrance plaza, which is the lowest ground it touches. Bearing 0: the
 * station's long side runs east–west along the railway berm, facing the
 * park entrance to the south and Town Square to the north.
 *
 * A Second Empire station on top of the railway berm: a cream stone base
 * (the berm's retaining wall, pierced by the arcades on the Town Square
 * side), the long platform canopy with its red-brown roof on the south, and
 * above it the station itself — a brick storey under a cream cornice, a
 * sage-green slate mansard with dormers, taller mansard pavilions at each
 * end and the central brick clock tower with its curved mansard clock dome.
 * Black iron cresting crowns the pavilions, the tower and the main roof.
 *
 * Evidence:
 *  - OSM, measured: the outline and its parts — the station block
 *    (way/1107878404, 12 m; wings way/1107878407/8, 8 m), the platform
 *    canopy strip on the south (way/1107878403, 6 m) and its ends
 *    (way/1107878401/2/5/6), the Town Square side's central projection
 *    (way/1107878411, 8 m) and the two curved stair wings
 *    (way/1107878409/10, 3 m).
 *  - Photos: south front close up (SteamFan, CC BY 2.5), south across the
 *    Seven Seas Lagoon with a long lens (Diego Tirira, CC BY-SA 2.0) for
 *    the proportions, the south-west with the tunnels (Tom Arthur, CC BY-SA
 *    2.0), the Town Square side (Jackdude101, CC BY-SA 4.0).
 *
 * Estimated: every height. OSM's heights (8–12 m) look like they're taken
 * from the berm, not the plaza; the long-lens photo, scaled on the OSM
 * width, gives a berm of ~4.5 m, the cornice at ~11 m, the mansard at
 * ~16.5 m and the clock dome's top at ~25 m. The tower is centred on the
 * block, which OSM's parts suggest but do not show. The Town Square side's
 * glass-roofed veranda is reduced to a plain porch roof.
 */
import { Part, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const TAU = Math.PI * 2

const brick = new Part()
const cream = new Part()   // stone base, cornices, dormer frames
const slate = new Part()   // mansard roofs
const canopy = new Part()  // platform canopy roof
const iron = new Part()    // cresting, canopy posts
const glazing = new Part()

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

/**
 * A mansard: steep sides from the rectangle at z0, stepping in by `inset`
 * at z1, with a flat top. Bevelled corners come from the chamfer `c`.
 */
function mansard(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, inset: number, c = 0.3) {
  const ring = (d: number, z: number): V3[] => {
    const a = x0 + d, b = x1 - d, e = y0 + d, f = y1 - d
    return [[a + c, e, z], [b - c, e, z], [b, e + c, z], [b, f - c, z], [b - c, f, z], [a + c, f, z], [a, f - c, z], [a, e + c, z]]
  }
  const lo = ring(0, z0), hi = ring(inset, z1)
  for (let i = 0; i < 8; i++) { const j = (i + 1) % 8; quad(p, lo[i], lo[j], hi[j], hi[i]) }
  for (let i = 1; i < 7; i++) tri(p, hi[0], hi[i], hi[i + 1])
}

/** Iron cresting: a low dark band round a rectangle's top. */
function cresting(x0: number, x1: number, y0: number, y1: number, z: number, h = 0.7) {
  const t = 0.15
  prism(iron, [[x0, y0], [x1, y0], [x1, y1], [x0, y1]], z, z + h, { top: false })
  prism(iron, [[x0 + t, y1 - t], [x1 - t, y1 - t], [x1 - t, y0 + t], [x0 + t, y0 + t]].reverse() as XY[], z, z + h, { top: false })
}

/**
 * A dormer on a mansard face: a cream frame with an arched window, standing
 * on the roof slope. `face` 's' or 'n'; `y` is the face plane at its foot.
 */
function dormer(face: 's' | 'n', x: number, y: number, z0: number, w: number, h: number, depth: number) {
  const s = face === 's' ? -1 : 1
  const yf = y + s * 0.05, yb = y - s * depth
  const box: XY[] = [[x - w / 2, Math.min(yf, yb)], [x + w / 2, Math.min(yf, yb)], [x + w / 2, Math.max(yf, yb)], [x - w / 2, Math.max(yf, yb)]]
  prism(cream, box, z0, z0 + h)
  // A small pediment roof on top, slate.
  gable(slate, cream, x - w / 2 - 0.1, x + w / 2 + 0.1, Math.min(yf, yb), Math.max(yf, yb), z0 + h, z0 + h + w * 0.45, 'y')
  windowPanel(glazing, face, yf, x, w * 0.55, z0 + 0.35, z0 + h - 0.3, true)
}

// ---------------------------------------------------------------------------

const DECK = 4.5      // platform level, on the berm
const EAVE = 10.2     // top of the brick storey
const CORNICE = 11.6
const ROOF = 16.4     // main mansard
const PAV = 18.4      // pavilion mansards
const TOWER = 18.6    // tower brick
const DOME = 24.6     // clock dome top

const B = { x0: -23.7, x1: 22.7, y0: -4.6, y1: 3.2 } // the station block
const XC = (B.x0 + B.x1) / 2                          // its centre, the tower's axis

// The berm's stone base under everything, from the outline.
const OUTLINE: XY[] = [[28.7, -9.9], [-28.7, -9.8], [-28.7, -1.0], [-23.6, -1.1], [-23.7, 3.2], [-10.3, 3.2], [-10.3, 5.8], [-13.2, 7.0], [-15.1, 9.1], [-15.1, 10.5], [-14.3, 12.0], [-12.7, 12.7], [-7.0, 12.6], [-7.0, 13.6], [-3.5, 13.6], [-3.4, 16.1], [4.0, 16.1], [4.0, 13.6], [7.7, 13.6], [7.7, 12.5], [14.4, 12.5], [15.7, 11.9], [16.1, 10.7], [15.8, 8.6], [14.3, 6.8], [10.7, 5.4], [10.8, 3.1], [22.7, 3.1], [22.6, -1.1], [28.6, -1.1]].reverse() as XY[]
prism(cream, OUTLINE, 0, DECK)
// A balustrade along the deck's edges on the Town Square side (the stairs'
// tops) and the tunnel arches into the berm under the projection.
for (const x of [-0.6]) windowPanel(glazing, 'n', 16.1, x + 0.3, 3.2, 0, 3.6, true)
for (const x of [-11.5, 11.0]) windowPanel(glazing, 'n', x < 0 ? 12.65 : 12.5, x, 2.4, 0.2, 3.3, true)

// The brick storey and its cornice.
cbox(brick, B.x0, B.x1, B.y0, B.y1, DECK, EAVE, 0.3)
mansard(cream, B.x0 - 0.1, B.x1 + 0.1, B.y0 - 0.1, B.y1 + 0.1, EAVE, EAVE + 0.2, 0, 0.3)
mansard(cream, B.x0 - 0.1, B.x1 + 0.1, B.y0 - 0.1, B.y1 + 0.1, EAVE + 0.2, CORNICE, -0.5, 0.3)
// Tall platform-level windows, north and south, between the pavilions.
for (let k = 0; k < 9; k++) {
  const x = XC - 15 + k * 3.75
  if (Math.abs(x - XC) < 4) continue
  windowPanel(glazing, 's', B.y0, x, 1.5, DECK + 1.2, EAVE - 1.3, true)
  windowPanel(glazing, 'n', B.y1, x, 1.5, DECK + 1.2, EAVE - 1.3, true)
}

// The main mansard, the end pavilions, and their cresting.
mansard(slate, B.x0 + 0.2, B.x1 - 0.2, B.y0 - 0.3, B.y1 + 0.3, CORNICE, ROOF, 1.4)
cresting(B.x0 + 1.8, B.x1 - 1.8, B.y0 + 1.3, B.y1 - 1.3, ROOF)
for (const [p0, p1] of [[B.x0 - 0.3, B.x0 + 6.4], [B.x1 - 6.4, B.x1 + 0.3]]) {
  const y0 = B.y0 - 0.6, y1 = B.y1 + 0.6
  mansard(cream, p0, p1, y0, y1, EAVE - 0.4, CORNICE + 0.3, -0.3, 0.3)
  mansard(slate, p0, p1, y0, y1, CORNICE + 0.3, PAV, 1.3)
  cresting(p0 + 1.4, p1 - 1.4, y0 + 1.4, y1 - 1.4, PAV, 0.9)
  dormer('s', (p0 + p1) / 2, y0 + 0.3, CORNICE + 1.2, 1.9, 3.2, 1.2)
  dormer('n', (p0 + p1) / 2, y1 - 0.3, CORNICE + 1.2, 1.9, 3.2, 1.2)
}
// Dormers on the main roof, two each side of the tower, both faces.
for (const dx of [-12.4, -7.4, 7.4, 12.4]) {
  dormer('s', XC + dx, B.y0 + 0.1, CORNICE + 0.9, 1.5, 2.6, 1.0)
  dormer('n', XC + dx, B.y1 - 0.1, CORNICE + 0.9, 1.5, 2.6, 1.0)
}

// The clock tower: brick to above the roofs, a cream bracketed cornice, the
// curved mansard clock dome (two steps of slate), clock faces north and
// south, cresting and the flagpole.
{
  const w = 3.5, y0 = B.y0 - 0.5, y1 = B.y1 + 0.5
  cbox(brick, XC - w, XC + w, y0, y1, DECK, TOWER, 0.35)
  windowPanel(glazing, 's', y0, XC, 1.8, EAVE + 1.0, TOWER - 1.6, true)
  windowPanel(glazing, 'n', y1, XC, 1.8, EAVE + 1.0, TOWER - 1.6, true)
  mansard(cream, XC - w - 0.1, XC + w + 0.1, y0 - 0.1, y1 + 0.1, TOWER, TOWER + 0.9, -0.7, 0.35)
  const d0 = 0.6
  mansard(slate, XC - w + d0, XC + w - d0, y0 + d0, y1 - d0, TOWER + 0.9, TOWER + 3.6, 0.4, 0.4)
  mansard(slate, XC - w + d0 + 0.4, XC + w - d0 - 0.4, y0 + d0 + 0.4, y1 - d0 - 0.4, TOWER + 3.6, DOME - 0.9, 1.0, 0.4)
  mansard(cream, XC - 1.9, XC + 1.9, y0 + d0 + 1.2, y1 - d0 - 1.2, DOME - 0.9, DOME - 0.3, -0.25, 0.2)
  cresting(XC - 1.8, XC + 1.8, y0 + d0 + 1.3, y1 - d0 - 1.3, DOME - 0.3, 0.8)
  // Clock faces: cream discs on the dome's lower slope, north and south.
  for (const [yy, s] of [[y0 + d0 + 0.12, -1], [y1 - d0 - 0.12, 1]] as [number, number][]) {
    const cz = TOWER + 2.6, R = 1.15, n = 12
    for (let i = 0; i < n; i++) {
      const a = (i / n) * TAU, b = ((i + 1) / n) * TAU
      const P = (t: number): V3 => [XC + R * Math.cos(t), yy + s * 0.25, cz + R * Math.sin(t)]
      if (s < 0) tri(cream, [XC, yy + s * 0.25, cz], P(a), P(b))
      else tri(cream, [XC, yy + s * 0.25, cz], P(b), P(a))
    }
  }
  frustum(iron, XC, (y0 + y1) / 2, 0.12, DOME - 0.3, 0.06, DOME + 8, 4)
}

// The platform canopy along the south side, over the deck and both ends,
// on a row of iron posts.
{
  const y0 = -10.3, y1 = B.y0, z = 9.0, x0 = -29.1, x1 = 29.1
  // A shallow lean-to falling to the south, its eave edge thickened.
  quad(canopy, [x0, y0, z - 0.5], [x1, y0, z - 0.5], [x1, y1, z + 0.4], [x0, y1, z + 0.4])
  quad(canopy, [x1, y0, z - 1.0], [x1, y0, z - 0.5], [x0, y0, z - 0.5], [x0, y0, z - 1.0])
  quad(canopy, [x0, y0, z - 1.0], [x0, y1, z - 0.6], [x1, y1, z - 0.6], [x1, y0, z - 1.0])
  tri(canopy, [x0, y0, z - 1.0], [x0, y0, z - 0.5], [x0, y1, z + 0.4])
  tri(canopy, [x0, y0, z - 1.0], [x0, y1, z + 0.4], [x0, y1, z - 0.6])
  tri(canopy, [x1, y0, z - 0.5], [x1, y0, z - 1.0], [x1, y1, z + 0.4])
  tri(canopy, [x1, y1, z + 0.4], [x1, y0, z - 1.0], [x1, y1, z - 0.6])
  for (let k = 0; k <= 10; k++) {
    const x = x0 + 0.6 + k * (x1 - x0 - 1.2) / 10
    prism(iron, [[x - 0.2, y0 + 0.4], [x + 0.2, y0 + 0.4], [x + 0.2, y0 + 0.8], [x - 0.2, y0 + 0.8]], DECK, z - 1.0, { top: false })
  }
  // The deck's railing, a low cream parapet along the berm's south edge.
  prism(cream, [[x0 + 0.4, -9.9], [x1 - 0.4, -9.9], [x1 - 0.4, -9.6], [x0 + 0.4, -9.6]], DECK, DECK + 1.0)
}

// The Town Square side's porch roof over the central projection.
{
  const x0 = -5.9, x1 = 5.9, y0 = B.y1, y1 = 13.6
  hip(canopy, x0, x1, y0, y1, 9.6, 11.2)
  for (const [x, y] of [[x0 + 0.3, y1 - 0.3], [x1 - 0.3, y1 - 0.3]] as XY[])
    prism(iron, [[x - 0.2, y - 0.2], [x + 0.2, y - 0.2], [x + 0.2, y + 0.2], [x - 0.2, y + 0.2]], DECK, 9.6, { top: false })
}

// ---------------------------------------------------------------------------

// Red brick, sage slate and the red-brown canopy are the station's own
// colours, muted to the palette's range; cream stone is the palette's trim;
// the iron cresting stays dark, no darker than charcoal.
const parts = [
  { part: brick, material: finish('station-brick', 0xc98472) },
  { part: cream, material: PALETTE.trim },
  { part: slate, material: finish('station-slate', 0x9fb3aa) },
  { part: canopy, material: finish('station-canopy', 0xa76a5c) },
  { part: iron, material: finish('station-iron', 0x4a4f57) },
  { part: glazing, material: PALETTE.window },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Main Street, U.S.A. Station', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: DOME + 8,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-main-street-station.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
