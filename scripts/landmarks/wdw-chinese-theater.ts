/**
 * The Chinese Theater, Disney's Hollywood Studios (home of Mickey & Minnie's
 * Runaway Railway, formerly The Great Movie Ride) — procedural, CC0-1.0, no
 * textures.
 * bun scripts/landmarks/wdw-chinese-theater.ts
 *
 * Map frame: x east, y north, z up, metres, bearing 0, origin at the centroid
 * of the OSM outline way/294898804. That outline is the whole building: the
 * theater front and its forecourt walls at the north end, turned 45° to face
 * north-east down Hollywood Boulevard, and the square-set show building
 * behind. The front is built in its own frame (u across the facade towards
 * the south-east, v out of it towards the forecourt) and turned into place.
 *
 * Evidence
 * - OSM (measured): the outline way/294898804 with its forecourt cut-out and
 *   curved forecourt walls; way/607996087, the pagoda's portal (12 × 7 m);
 *   way/607972840, the round base of the forecourt's green-roofed pavilion.
 *   No heights are tagged.
 * - Published: a full-size replica of Grauman's Chinese Theatre built from
 *   its 1927 drawings (Wikipedia, "The Great Movie Ride"); the original's
 *   pagoda is 27 m tall, with a 9.1 m dragon mural between its two red
 *   pillars and a bronze roof (Wikipedia, "Grauman's Chinese Theatre").
 * - USGS NAIP orthophoto (public domain): the show building's flat white roof
 *   and its lower east strip.
 * - Photos (Wikimedia Commons):
 *     "Disney's Hollywood Studios, October 2015 (22121989876).jpg", Theme Park Tourist, CC BY 2.0 — whole front, head-on
 *     "The Great Movie Ride (42346719425).jpg", HarshLight, CC BY 2.0 — pagoda, head-on
 *     "Chinese Theatre (9410830645).jpg", Sam Howzit, CC BY 2.0 — pagoda from the left
 *     "Runaway Railway Chinese Theater.jpg", Jedi94, CC BY-SA 4.0 — the 2020 front
 *     "The Great Movie Ride and Chinese Theater at Walt Disney World.jpg", Jedi94, CC BY-SA 3.0 — the four obelisks
 *     "Chinese Theater (49560227308).jpg", Eden, Janine and Jim, CC BY 2.0 — portal
 *
 * Estimated: the forecourt walls (13 m), the wing fronts (15.5 m) and obelisks (23.5 m) are scaled
 * from the head-on photo against the 27 m pagoda; the show building's 16 m
 * is a guess, as no licensed photo shows it above the front. The pagoda's
 * eave (14.3 m) and ridge (24.2 m) are scaled from the same photo. The
 * pillars' ornament, the masks, the spiky dragon crests and the roof's small
 * upright finials are simplified to blocks and spikes.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const stone = new Part(), bronze = new Part(), red = new Part()
const gold = new Part(), dark = new Part(), win = new Part()

const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
type XY = [number, number]

function tri(p: Part, a: V3, b: V3, c: V3, n?: V3 | V3[]) {
  const f = cross(sub(b, a), sub(c, a))
  if (len(f) < 1e-9) return
  const N = n === undefined ? undefined : Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3]
  const ref = N ? unit([N[0][0] + N[1][0] + N[2][0], N[0][1] + N[1][1] + N[2][1], N[0][2] + N[1][2] + N[2][2]]) : f
  if (dot(f, ref) >= 0) p.tri(a, b, c, undefined, undefined, undefined, N)
  else p.tri(a, c, b, undefined, undefined, undefined, N && [N[0], N[2], N[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3 | V3[]) {
  const N = n === undefined ? undefined : Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, N && [N[0], N[1], N[2]])
  tri(p, a, c, d, N && [N[0], N[2], N[3]])
}

// ---------------------------------------------------------------------------
// The theater front's own frame. Everything for the front is drawn into
// parts of its own and turned into place at the end: u runs to bearing 135°,
// v to 45°, the origin is the middle of the pagoda's portal.

const O: XY = [-17.8, 69.5]
const C = Math.SQRT1_2
const place = (src: Part, dst: Part) => {
  // Part stores glTF (x, z_up, -y); turn the map-frame (u, v) into (x, y).
  for (let i = 0; i < src.pos.length; i += 3) {
    const u = src.pos[i], v = -src.pos[i + 2]
    const nu = src.nrm[i], nv = -src.nrm[i + 2]
    dst.pos.push(O[0] + C * (u + v), src.pos[i + 1], -(O[1] + C * (v - u)))
    dst.nrm.push(C * (nu + nv), src.nrm[i + 1], -(C * (nv - nu)))
  }
  for (const t of src.uv) dst.uv.push(t)
}
const F = { stone: new Part(), bronze: new Part(), red: new Part(), gold: new Part(), dark: new Part(), win: new Part() }

// ---------------------------------------------------------------------------
// Primitives.

type Rect = { x0: number; x1: number; y0: number; y1: number }
function box(p: Part, r: Rect, z0: number, z1: number, bottom = false) {
  const q = (z: number): V3[] => [[r.x0, r.y0, z], [r.x1, r.y0, z], [r.x1, r.y1, z], [r.x0, r.y1, z]]
  const a = q(z0), b = q(z1)
  const ns: V3[] = [[0, -1, 0], [1, 0, 0], [0, 1, 0], [-1, 0, 0]]
  for (let i = 0; i < 4; i++) quad(p, a[i], a[(i + 1) % 4], b[(i + 1) % 4], b[i], ns[i])
  quad(p, b[0], b[1], b[2], b[3], [0, 0, 1])
  if (bottom) quad(p, a[0], a[3], a[2], a[1], [0, 0, -1])
}
/** A block with a small rounded bevel round its top edge. */
function bevelBox(p: Part, r: Rect, z0: number, z1: number, bt = 0.35, top: Part = p) {
  box(p, r, z0, z1 - bt)
  const inner = { x0: r.x0 + bt, x1: r.x1 - bt, y0: r.y0 + bt, y1: r.y1 - bt }
  const lo: V3[] = [[r.x0, r.y0, z1 - bt], [r.x1, r.y0, z1 - bt], [r.x1, r.y1, z1 - bt], [r.x0, r.y1, z1 - bt]]
  const hi: V3[] = [[inner.x0, inner.y0, z1], [inner.x1, inner.y0, z1], [inner.x1, inner.y1, z1], [inner.x0, inner.y1, z1]]
  const out: V3[] = [[0, -1, 0], [1, 0, 0], [0, 1, 0], [-1, 0, 0]]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    quad(p, lo[i], lo[j], hi[j], hi[i], [out[i], out[i], [0, 0, 1], [0, 0, 1]])
  }
  quad(top, hi[0], hi[1], hi[2], hi[3], [0, 0, 1])
}
/** A tapered square shaft from (half0 at z0) to (half1 at z1), capped. */
function taper(p: Part, cx: number, cy: number, h0: number, h1: number, z0: number, z1: number, cap = true) {
  const q = (h: number, z: number): V3[] => [[cx - h, cy - h, z], [cx + h, cy - h, z], [cx + h, cy + h, z], [cx - h, cy + h, z]]
  const a = q(h0, z0), b = q(h1, z1)
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    quad(p, a[i], a[j], b[j], b[i], unit(cross(sub(a[j], a[i]), sub(b[i], a[i]))))
  }
  if (cap) quad(p, b[0], b[1], b[2], b[3], [0, 0, 1])
}
function pyramid(p: Part, cx: number, cy: number, h0: number, z0: number, h: number) {
  const q: V3[] = [[cx - h0, cy - h0, z0], [cx + h0, cy - h0, z0], [cx + h0, cy + h0, z0], [cx - h0, cy + h0, z0]]
  const top: V3 = [cx, cy, z0 + h]
  for (let i = 0; i < 4; i++) tri(p, q[i], q[(i + 1) % 4], top, unit(cross(sub(q[(i + 1) % 4], q[i]), sub(top, q[i]))))
}
/** A leaning spike: a four-sided blade from a base square to a point. */
function spike(p: Part, base: V3, half: number, tip: V3) {
  const q: V3[] = [[base[0] - half, base[1] - half, base[2]], [base[0] + half, base[1] - half, base[2]], [base[0] + half, base[1] + half, base[2]], [base[0] - half, base[1] + half, base[2]]]
  for (let i = 0; i < 4; i++) tri(p, q[i], q[(i + 1) % 4], tip, unit(cross(sub(q[(i + 1) % 4], q[i]), sub(tip, q[i]))))
}
/** A panel on the plane y = y (facing -y if `front`, else +y). */
function panelY(p: Part, y: number, x0: number, x1: number, z0: number, z1: number, facing: 1 | -1) {
  quad(p, [x0, y, z0], [x1, y, z0], [x1, y, z1], [x0, y, z1], [0, facing, 0])
}
function archY(p: Part, y: number, x0: number, x1: number, z0: number, spring: number, facing: 1 | -1) {
  panelY(p, y, x0, x1, z0, spring, facing)
  const r = (x1 - x0) / 2, cx = x0 + r, seg = 8
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * Math.PI, b = ((k + 1) / seg) * Math.PI
    tri(p, [cx, y, spring], [cx + r * Math.cos(a), y, spring + r * Math.sin(a)], [cx + r * Math.cos(b), y, spring + r * Math.sin(b)], [0, facing, 0])
  }
}

/** Ear-clipping triangulation of a simple polygon, counter-clockwise. */
function earcut(pts: XY[]): [number, number, number][] {
  const idx = pts.map((_, i) => i), out: [number, number, number][] = []
  const area = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => area(a, b, p) > 1e-9 && area(b, c, p) > 1e-9 && area(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let clipped = false
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i + idx.length - 1) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const a = pts[ia], b = pts[ib], c = pts[ic]
      if (area(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== ia && j !== ib && j !== ic && inside(pts[j], a, b, c))) continue
      out.push([ia, ib, ic]); idx.splice(i, 1); clipped = true; break
    }
    if (!clipped) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
/** A prism over an outline, walls flat-shaded, flat lid. */
function extrude(p: Part, ring: XY[], z0: number, z1: number, lidPart: Part = p) {
  let s = 0
  for (let i = 0; i < ring.length; i++) { const a = ring[i], b = ring[(i + 1) % ring.length]; s += a[0] * b[1] - b[0] * a[1] }
  const pts = s < 0 ? [...ring].reverse() : ring
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length]
    const n = unit([b[1] - a[1], a[0] - b[0], 0])
    quad(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], n)
  }
  for (const [i, j, k] of earcut(pts)) tri(lidPart, [pts[i][0], pts[i][1], z1], [pts[j][0], pts[j][1], z1], [pts[k][0], pts[k][1], z1], [0, 0, 1])
}

// ---------------------------------------------------------------------------
// The building outline (way/294898804), relative to the anchor.

const OUTLINE: XY[] = [[3.91, 64.31], [3.56, 62.48], [3.07, 61.43], [2.34, 60.19], [-0.75, 56.99], [-2.33, 55.69], [-4.31, 55.03], [-4.44, 60.03], [-6.58, 60.0], [-7.73, 60.0], [-13.9, 65.17], [-11.12, 67.97], [-19.46, 76.12], [-21.93, 73.63], [-27.09, 78.34], [-30.41, 75.39], [-31.3, 78.06], [-31.39, 80.72], [-30.94, 83.2], [-29.99, 85.54], [-26.53, 88.94], [-24.46, 89.92], [-23.59, 90.14], [-22.04, 90.56], [-19.99, 90.61], [-17.96, 90.28], [-16.44, 89.64], [-15.23, 88.71], [-13.52, 87.11], [-12.84, 87.84], [-12.17, 88.55], [-21.13, 97.27], [-25.81, 92.54], [-37.13, 81.08], [-38.96, 79.48], [-37.79, 78.35], [-30.91, 71.67], [-24.96, 65.72], [-24.73, 57.7], [-24.51, 37.46], [-35.9, 37.52], [-35.95, -16.63], [-35.96, -20.07], [-35.78, -42.89], [-35.77, -44.87], [-38.07, -44.82], [-37.88, -56.86], [-30.16, -56.89], [-25.9, -56.91], [-25.95, -54.56], [-23.46, -54.59], [-20.5, -54.54], [26.88, -54.49], [26.86, -48.94], [40.37, -48.88], [40.44, -42.57], [45.75, -42.3], [45.82, -27.75], [45.83, -25.58], [45.72, -4.47], [40.94, -4.46], [40.83, 8.74], [33.33, 8.78], [33.36, 31.99], [32.89, 31.99], [27.21, 32.32], [27.2, 34.78], [27.36, 42.09], [17.97, 42.16], [17.87, 37.71], [14.25, 37.76], [14.11, 52.66], [10.82, 52.68], [7.87, 52.54], [0.38, 52.39], [0.31, 55.68], [3.09, 58.41], [6.52, 61.4], [11.37, 65.96], [0.81, 75.21], [-0.41, 74.03], [1.7, 71.99], [3.03, 69.68], [3.78, 67.12]]

const WALL = 13   // forecourt walls and the low mass of the theater front
const SHOW = 16   // the show building's main box (estimated)

extrude(stone, OUTLINE, 0, WALL, stone)
// The show building: one tall plain box over the ride, its roof the pale
// membrane the orthophoto shows.
const roof = new Part()
bevelBox(stone, { x0: -38.2, x1: 22.4, y0: -54.6, y1: 37.4 }, 0, SHOW, 0.4, roof)

// ---------------------------------------------------------------------------
// The front, in its own frame.

// The portal block behind the pillars (OSM way/607996087), up to the eave.
box(F.stone, { x0: -5.8, x1: 5.8, y0: -3.3, y1: 3.6 }, 0, 14.0)
// The dragon mural between the pillars, and the grey drapery over it.
panelY(F.gold, 3.64, -1.9, 1.9, 3.4, 10.4, 1)
panelY(F.win, 3.64, -3.4, 3.4, 0, 3.2, 1)
{
  // The drapery: a shallow box with a scalloped hem, as three hanging swags.
  box(F.dark, { x0: -4.3, x1: 4.3, y0: 3.6, y1: 4.5 }, 10.6, 12.5)
  for (let k = 0; k < 3; k++) {
    const x0 = -4.3 + k * 2.867, x1 = x0 + 2.867
    tri(F.dark, [x0, 4.5, 10.6], [(x0 + x1) / 2, 4.2, 9.9], [x1, 4.5, 10.6], [0, 1, -0.4])
  }
}
// Red flared copings along the low walls either side of the pagoda.
for (const s of [-1, 1]) {
  const x0 = s < 0 ? -12.8 : 5.8, x1 = s < 0 ? -5.8 : 13.8
  quad(F.red, [x0, -0.3, 10.2], [x1, -0.3, 10.2], [x1, 1.6, 9.4], [x0, 1.6, 9.4], unit([0, 0.4, 1]))
  quad(F.red, [x0, 1.6, 9.4], [x1, 1.6, 9.4], [x1, 1.6, 9.0], [x0, 1.6, 9.0], [0, 1, 0])
}

// The two red pillars and what stands on them: a dark guardian mask, three
// tiers of gold relief panels stepping out, a pale cap, and the spiky crest.
for (const s of [-1, 1]) {
  const cx = s * 5.25, cy = 4.35, h = 0.85
  box(F.red, { x0: cx - h, x1: cx + h, y0: cy - h, y1: cy + h }, 0, 9.4)
  box(F.dark, { x0: cx - h - 0.15, x1: cx + h + 0.15, y0: cy - h - 0.15, y1: cy + h + 0.25 }, 9.4, 10.9)
  for (let t = 0; t < 3; t++) {
    const g = h + 0.12 + t * 0.12
    box(F.gold, { x0: cx - g, x1: cx + g, y0: cy - g, y1: cy + g }, 10.9 + t * 1.15, 11.9 + t * 1.15)
    box(F.red, { x0: cx - g + 0.05, x1: cx + g - 0.05, y0: cy - g + 0.05, y1: cy + g - 0.05 }, 11.9 + t * 1.15, 12.05 + t * 1.15)
  }
  bevelBox(F.stone, { x0: cx - 1.3, x1: cx + 1.3, y0: cy - 1.3, y1: cy + 1.3 }, 14.3, 15.9, 0.2)
  // The crest: a fan of dark blades leaning out from the pagoda.
  for (const [dx, dy, hgt] of [[0.7, 0, 6.6], [1.5, 0.2, 5.0], [0.1, 0.3, 5.6], [1.2, -0.4, 4.2]] as [number, number, number][])
    spike(F.dark, [cx + s * (dx - 0.8), cy + dy, 15.9], 0.34, [cx + s * (dx + 0.9), cy + dy * 1.6, 15.9 + hgt])
}

// The pagoda roof: a bronze hip roof flaring out at the eaves, its slopes
// curving from nearly flat at the eave to steep at the ridge, over a red band.
{
  const e = { x0: -6.3, x1: 6.3, y0: -4.4, y1: 5.2 }, r = { x0: -1.7, x1: 1.7, y0: 0.0, y1: 1.0 }
  const z0 = 14.3, z1 = 23.6
  // (inset, rise) down the slope: a short, nearly flat flare at the eave,
  // then a straight steep run to the ridge.
  const PROFILE: XY[] = [[0, 0], [0.13, 0.05], [0.3, 0.19], [0.54, 0.46], [0.77, 0.73], [1, 1]]
  const ring = ([k, h]: XY): V3[] => {
    const L = (a: number, b: number) => a + (b - a) * k
    const z = z0 + (z1 - z0) * h
    return [[L(e.x0, r.x0), L(e.y0, r.y0), z], [L(e.x1, r.x1), L(e.y0, r.y0), z], [L(e.x1, r.x1), L(e.y1, r.y1), z], [L(e.x0, r.x0), L(e.y1, r.y1), z]]
  }
  const n = PROFILE.length - 1
  const rings = PROFILE.map(ring)
  for (let k = 0; k < n; k++)
    for (let i = 0; i < 4; i++) {
      const j = (i + 1) % 4
      const a = rings[k][i], b = rings[k][j], c = rings[k + 1][j], d = rings[k + 1][i]
      quad(F.bronze, a, b, c, d, unit(cross(sub(b, a), sub(d, a))))
    }
  const top = rings[n]
  quad(F.bronze, top[0], top[1], top[2], top[3], [0, 0, 1])
  // The red band under the eave, and its dark soffit.
  const lo: V3[] = [[e.x0 + 0.7, e.y0 + 0.7, 12.5], [e.x1 - 0.7, e.y0 + 0.7, 12.5], [e.x1 - 0.7, e.y1 - 0.7, 12.5], [e.x0 + 0.7, e.y1 - 0.7, 12.5]]
  const hi = rings[0]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    quad(F.red, lo[i], lo[j], hi[j], hi[i], unit(cross(sub(lo[j], lo[i]), sub(hi[i], lo[i]))))
  }
  quad(F.dark, lo[0], lo[3], lo[2], lo[1], [0, 0, -1])
  // The two trident finials at the ends of the ridge.
  for (const s of [-1, 1]) {
    const x = s * 1.7, y = 0.5
    box(F.dark, { x0: x - 0.18, x1: x + 0.18, y0: y - 0.18, y1: y + 0.18 }, z1, z1 + 1.4)
    for (const dx of [-0.55, 0, 0.55]) spike(F.dark, [x + dx * 0.6, y, z1 + 1.3], 0.12, [x + dx, y, z1 + (dx ? 2.5 : 2.9)])
  }
}

// The forecourt's wing ends: arched entrances and window grids on the faces
// towards the plaza, and an obelisk at each corner, dark and tapering to a
// trident like the pagoda's.
for (const s of [-1, 1]) {
  const xi = s < 0 ? -9.5 : 9.1, xo = s < 0 ? -22.0 : 23.1, yf = s < 0 ? 17.35 : 17.65
  const a = Math.min(xi, xo), b = Math.max(xi, xo), m = (a + b) / 2
  // The wing's front is a tall false front over the curved forecourt wall.
  const WF = 15.5
  box(F.stone, { x0: a, x1: b, y0: yf - 2.0, y1: yf }, WALL - 0.5, WF)
  // A dark tiled hood over a niche, high on each wing front.
  box(F.dark, { x0: m - 1.6, x1: m + 1.6, y0: yf, y1: yf + 0.9 }, 11.6, 12.2)
  quad(F.dark, [m - 1.6, yf + 0.9, 12.2], [m + 1.6, yf + 0.9, 12.2], [m + 1.0, yf, 13.6], [m - 1.0, yf, 13.6], unit([0, 1, 0.65]))
  panelY(F.win, yf + 0.04, m - 0.9, m + 0.9, 10.2, 11.6, 1)
  archY(F.win, yf + 0.04, m - 2.2, m + 2.2, 0, 3.8, 1)
  box(F.red, { x0: m - 2.9, x1: m + 2.9, y0: yf, y1: yf + 0.3 }, 6.2, 7.6)   // the red-framed sign board
  for (const x of [a + 1.2, b - 3.2]) panelY(F.win, yf + 0.04, x, x + 2, 8.2, 10.2, 1)
  for (const x of [a + 1.3, b - 1.3]) {
    const y = yf - 1.4
    const y2 = yf - 1.0
    taper(F.dark, x, y2, 0.95, 0.8, WF, WF + 0.8)
    taper(F.dark, x, y2, 0.8, 0.28, WF + 0.8, 22.4, false)
    pyramid(F.red, x, y2, 0.28, 22.4, 0.5)
    for (const dx of [-0.35, 0, 0.35]) spike(F.dark, [x + dx * 0.5, y2, 22.8], 0.07, [x + dx, y2, 23.3 + (dx ? 0 : 0.4)])
  }
}
// The forecourt pavilion (way/607972840): a little open hall under a green
// tiled hip roof that flares at the eaves.
{
  const cx = 13.7, cy = 3.7, h = 1.7
  for (const [dx, dy] of [[-1, -1], [1, -1], [1, 1], [-1, 1]]) box(F.red, { x0: cx + dx * h - 0.15, x1: cx + dx * h + 0.15, y0: cy + dy * h - 0.15, y1: cy + dy * h + 0.15 }, 0, 3.2)
  box(F.dark, { x0: cx - h - 0.2, x1: cx + h + 0.2, y0: cy - h - 0.2, y1: cy + h + 0.2 }, 3.2, 3.6, true)
  const e = 2.9, r = 0.5, z0 = 3.4, z1 = 6.2
  const ring = (t: number): V3[] => {
    const k = 1 - Math.pow(1 - t, 0.6), w = e + (r - e) * k, z = z0 + (z1 - z0) * Math.pow(t, 1.3)
    return [[cx - w, cy - w, z], [cx + w, cy - w, z], [cx + w, cy + w, z], [cx - w, cy + w, z]]
  }
  const rings = [0, 0.34, 0.67, 1].map(ring)
  for (let k = 0; k < 3; k++)
    for (let i = 0; i < 4; i++) {
      const j = (i + 1) % 4, a = rings[k][i], b = rings[k][j], c = rings[k + 1][j], d = rings[k + 1][i]
      quad(F.bronze, a, b, c, d, unit(cross(sub(b, a), sub(d, a))))
    }
  pyramid(F.bronze, cx, cy, r, z1, 0.6)
}

// Turn the front into place.
place(F.stone, stone); place(F.bronze, bronze); place(F.red, red)
place(F.gold, gold); place(F.dark, dark); place(F.win, win)

// ---------------------------------------------------------------------------

// Cream render as the palette's stone; the pagoda's bronze roof as a muted
// green-grey patina; the pillars' lacquer red and the gold relief panels as
// finishes, since they are what the theater is; the obelisks, masks and
// crests a dark charcoal. The show building's roof membrane shares `roof`.
const parts = [
  { part: stone, material: PALETTE.stone },
  { part: bronze, material: finish('pagoda-bronze', 0x7a807a) },
  { part: red, material: finish('lacquer-red', 0xc4483e) },
  { part: gold, material: finish('relief-gold', 0xd7ad57) },
  { part: dark, material: finish('obelisk', 0x4b5469) },
  { part: win, material: PALETTE.window },
]
// The show building's roof is the stone lid: kept, with no seventh material.
for (const k of ['pos', 'nrm', 'uv'] as const) (stone as any)[k].push(...(roof as any)[k])
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Chinese Theater', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 27,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-chinese-theater.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
