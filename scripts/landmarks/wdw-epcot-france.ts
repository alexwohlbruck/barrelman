/**
 * EPCOT France pavilion: the Eiffel Tower and the boulevard — procedural,
 * CC0-1.0.
 * bun scripts/landmarks/wdw-epcot-france.ts
 *
 * Map frame: x across the pavilion (south-east), y along its boulevard
 * (north-east, towards the lagoon), z up, metres. Placed at bearing 47.5°, the
 * grid the pavilion's OSM outlines share (their long edges run 47–48° and
 * 137–138°). The anchor is the middle of the modelled block's extent.
 *
 * What it covers, from OSM (all building=yes, none with a height):
 * - way/305094683, the Les Chefs de France / Monsieur Paul building on the
 *   promenade and the boulevard's south-east side: ochre stone under a green
 *   copper mansard with dormers and tall chimneys, the pavilion's front.
 * - way/295447782, the boulevard's north-west side: brick and stone houses
 *   under slate mansards.
 * - way/305094845 (Souvenirs de France) with way/305094686, the strip OSM
 *   names "France Pavilion" at the boulevard's end, which is the Palais du
 *   Cinéma: a stone front with a pediment under a slate roof.
 * - way/305094846, the Impressions de France theatre behind it, and on its
 *   roof the tower (building:part way/311286680–684, nested squares from a
 *   6.65 m base to a 1.1 m top).
 * - way/305094685, Les Halles, and way/805286022 beside it.
 * The streets run between these as narrow gaps, so the block is drawn as a
 * few dozen rectangles in this frame: each OSM outline is covered by one to
 * five of them, a little over where a jog was smoothed.
 *
 * The tower: a 1:10 replica of the upper part of Gustave Eiffel's, standing
 * on the theatre's roof behind the Palais so it reads at full height from
 * the boulevard. It has no textures (Open Landmarks allows none), so the
 * lattice is drawn as its real members: four legs with the arches between
 * them, the first platform, four corner posts braced in X up the shaft, the
 * top platform and the lantern.
 *
 * Evidence and doubts:
 * - Measured: every footprint (OSM). The tower's base (6.65 m square) and
 *   platform widths follow OSM's nested squares.
 * - Published: the tower is a 1:10 replica showing only its upper part.
 * - The tower's height is the main doubt. The brief said about 31 m; the
 *   photos and OSM disagree. In two photos the shaft from the first platform
 *   to the tip is 4.2–4.3 times the platform's width, and OSM puts that width
 *   at 5.8–6.65 m, so the tower is about 25 m from its first platform up.
 *   Sight lines over the Palais from the boulevard and the Chefs building
 *   from the canal both put the tip near 38–40 m above the ground. The model
 *   puts the legs' feet on a 10.5 m roof, the first platform at 16.8 m, the
 *   top platform at 36.2 m and the tip at 41 m.
 * - Estimated from photos (Wikimedia Commons, credits in the report): the
 *   eave and mansard heights (two storeys plus mansard, 7.5–10.5 m to the
 *   cornice, 10.5–14.8 m to the roof), which building is brick and which
 *   stone, chimney positions.
 * - Invented: the bay rhythm of the windows (3.2 m, one panel per bay per
 *   storey) and the dormers' spacing; the backs of the buildings, which no
 *   licensed photo shows, are dressed like their fronts.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const stone = new Part(), brick = new Part(), slate = new Part()
const copper = new Part(), glazing = new Part(), iron = new Part()

// The modelled block's extent in the frame of the OSM survey (origin
// -81.5530, 28.3688, rotated 47.5°) is x -24.4..39.3, y -40.2..48.9; the
// anchor is its middle, so every point is shifted by (OX, OY).
const OX = 7.5, OY = 4.35
const P = (x: number, y: number, z: number): V3 => [x - OX, y - OY, z]

const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }

/** A box between corners in the survey frame, open at the bottom. */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number) {
  const c = [P(x0, y0, z0), P(x1, y0, z0), P(x1, y1, z0), P(x0, y1, z0)]
  const t = c.map(([x, y]) => [x, y, z1] as V3)
  p.loft([c, t])
  p.cap(t, true)
}

/** A rectangle's ring at height z, grown outward by d (negative to inset). */
const ring = (r: Rect, d: number, z: number): V3[] =>
  [P(r.x0 - d, r.y0 - d, z), P(r.x1 + d, r.y0 - d, z), P(r.x1 + d, r.y1 + d, z), P(r.x0 - d, r.y1 + d, z)]

type Rect = { x0: number; x1: number; y0: number; y1: number }
type Side = 'S' | 'E' | 'N' | 'W'
type Building = Rect & {
  wall: number            // cornice height
  top: number             // roof top
  walls: Part             // stone or brick
  roof: Part              // slate or copper
  roofKind?: 'mansard' | 'curved' | 'flat'
  dress: Side[]           // sides that face a street: windows, dormers
  chimneys?: [number, number][]  // positions in the survey frame
}

const BAY = 3.5

/** Spans of a side: its start, its direction along, and its outward normal. */
function sideFrame(b: Rect, s: Side) {
  switch (s) {
    case 'S': return { a: [b.x0, b.y0] as const, u: [1, 0] as const, n: [0, -1] as const, L: b.x1 - b.x0 }
    case 'E': return { a: [b.x1, b.y0] as const, u: [0, 1] as const, n: [1, 0] as const, L: b.y1 - b.y0 }
    case 'N': return { a: [b.x1, b.y1] as const, u: [-1, 0] as const, n: [0, 1] as const, L: b.x1 - b.x0 }
    case 'W': return { a: [b.x0, b.y1] as const, u: [0, -1] as const, n: [-1, 0] as const, L: b.y1 - b.y0 }
  }
}

/** Centres of the bays along a side, evenly spread, clear of the corners. */
function bays(L: number) {
  const n = Math.max(1, Math.floor((L - 1.2) / BAY))
  const pitch = (L - 1.2) / n
  return Array.from({ length: n }, (_, k) => 0.6 + pitch * (k + 0.5))
}

/** A flat panel on a side, `out` metres proud of the wall. */
function panel(p: Part, b: Rect, s: Side, at: number, half: number, z0: number, z1: number, out = 0.04) {
  const f = sideFrame(b, s)
  const pt = (t: number, z: number) => P(f.a[0] + f.u[0] * t + f.n[0] * out, f.a[1] + f.u[1] * t + f.n[1] * out, z)
  p.quad(pt(at - half, z0), pt(at + half, z0), pt(at + half, z1), pt(at - half, z1))
}

/**
 * A dormer standing on a mansard's lower slope: a stone box with a window in
 * its face and a small hipped cap in the roof material.
 */
function dormer(b: Building, s: Side, at: number) {
  const f = sideFrame(b, s)
  const hw = 0.6, front = 0.35, back = 1.9, z0 = b.wall + 0.25, z1 = b.wall + 1.85
  const pt = (t: number, d: number, z: number) => P(f.a[0] + f.u[0] * t - f.n[0] * d, f.a[1] + f.u[1] * t - f.n[1] * d, z)
  const F = [pt(at - hw, front, z0), pt(at + hw, front, z0), pt(at + hw, front, z1), pt(at - hw, front, z1)]
  stone.quad(F[0], F[1], F[2], F[3])
  stone.quad(pt(at + hw, front, z0), pt(at + hw, back, z0), pt(at + hw, back, z1), pt(at + hw, front, z1))
  stone.quad(pt(at - hw, back, z0), pt(at - hw, front, z0), pt(at - hw, front, z1), pt(at - hw, back, z1))
  // Cap: a gable running back into the roof.
  const ridge = z1 + 0.55
  b.roof.quad(pt(at - hw - 0.1, front - 0.1, z1), pt(at, front - 0.1, ridge), pt(at, back, ridge), pt(at - hw - 0.1, back, z1))
  b.roof.quad(pt(at, front - 0.1, ridge), pt(at + hw + 0.1, front - 0.1, z1), pt(at + hw + 0.1, back, z1), pt(at, back, ridge))
  stone.tri(pt(at - hw, front, z1), pt(at + hw, front, z1), pt(at, front, ridge - 0.08))
  // The window, just proud of the dormer's face.
  const W = [pt(at - 0.38, front - 0.04, z0 + 0.25), pt(at + 0.38, front - 0.04, z0 + 0.25), pt(at + 0.38, front - 0.04, z1 - 0.15), pt(at - 0.38, front - 0.04, z1 - 0.15)]
  glazing.quad(W[0], W[1], W[2], W[3])
}

/** A chimney stack on the roof: a plain brick or stone shaft. */
function chimney(p: Part, x: number, y: number, z0: number, z1: number, along: 'x' | 'y') {
  const hx = along === 'x' ? 1.1 : 0.45, hy = along === 'x' ? 0.45 : 1.1
  box(p, x - hx, x + hx, y - hy, y + hy, z0, z1)
}

function building(b: Building) {
  const W = b.walls
  // Walls up to the cornice band.
  const corniceLo = b.wall - 0.7
  W.loft([ring(b, 0, 0), ring(b, 0, corniceLo)])
  // Cornice: a projecting stone band, bevelled out underneath and in on top.
  stone.loft([ring(b, 0, corniceLo), ring(b, 0.3, corniceLo + 0.25), ring(b, 0.3, b.wall - 0.15), ring(b, 0.05, b.wall)])

  const mh = b.top - b.wall, kind = b.roofKind ?? 'mansard'
  if (kind === 'flat') {
    b.roof.cap(ring(b, 0.05, b.wall), true)
  } else if (kind === 'curved') {
    // The Chefs de France roof: a convex copper mansard, steep at the cornice
    // and rolling over to a flat top, in four facets.
    const prof: [number, number][] = [[0.05, 0], [0.22, 0.42], [0.62, 0.74], [1.3, 0.93], [2.3, 1]]
    b.roof.loft(prof.map(([d, t]) => ring(b, -d, b.wall + t * mh)))
    b.roof.cap(ring(b, -2.3, b.top), true)
  } else {
    // A slate mansard: the steep lower slope, then the shallow upper one.
    b.roof.loft([ring(b, 0.05, b.wall), ring(b, -0.9, b.wall + mh * 0.82), ring(b, -2.6, b.top)])
    b.roof.cap(ring(b, -2.6, b.top), true)
  }

  // Windows: one panel per bay per storey on every street side. The ground
  // floor carries shopfronts and doors, so its openings are taller.
  const rows: [number, number][] = [[0.7, 3.5]]
  for (let z = 4.3; z + 2.0 < corniceLo; z += 3.1) rows.push([z, Math.min(z + 2.2, corniceLo - 0.3)])
  for (const s of b.dress) {
    const { L } = sideFrame(b, s)
    for (const at of bays(L)) {
      for (const [z0, z1] of rows) panel(glazing, b, s, at, rows[0][0] === z0 ? 0.75 : 0.6, z0, z1)
      if (kind !== 'flat' && mh > 2.2) dormer(b, s, at)
    }
  }
  for (const [x, y] of b.chimneys ?? []) chimney(b.walls === brick ? brick : stone, x, y, b.top - 0.5, b.top + 2.4, b.x1 - b.x0 > b.y1 - b.y0 ? 'y' : 'x')
}

// ---------------------------------------------------------------------------
// The buildings, as rectangles in the survey frame (see the header).

const B: Building[] = [
  // Les Chefs de France: the front block on the promenade and the fountain.
  { x0: 11.3, x1: 32.2, y0: 25.4, y1: 48.9, wall: 9.5, top: 14.8, walls: stone, roof: copper, roofKind: 'curved', dress: ['N', 'W', 'E'], chimneys: [[15.5, 41.5], [22, 41.5], [28.5, 41.5], [15.5, 32], [28.5, 32]] },
  { x0: 7.2, x1: 11.3, y0: 28.0, y1: 37.7, wall: 8.5, top: 12.8, walls: stone, roof: copper, roofKind: 'curved', dress: ['W', 'N', 'S'] },
  { x0: 8.6, x1: 11.3, y0: 37.7, y1: 47.2, wall: 8.5, top: 12.8, walls: stone, roof: copper, roofKind: 'curved', dress: ['W'] },
  { x0: 32.2, x1: 39.1, y0: 35.3, y1: 43.8, wall: 8.5, top: 12.8, walls: stone, roof: copper, roofKind: 'curved', dress: ['N', 'E', 'S'] },
  // Its long wing down the boulevard's south-east side: brick under slate,
  // then a lower stone pavilion behind.
  { x0: 20.7, x1: 32.2, y0: -1.8, y1: 25.4, wall: 9.5, top: 13.2, walls: brick, roof: slate, dress: ['W', 'E'], chimneys: [[26.5, 3], [26.5, 12], [26.5, 21]] },
  { x0: 32.2, x1: 39.3, y0: 9.2, y1: 17.3, wall: 7.5, top: 10.6, walls: stone, roof: slate, dress: ['N', 'E', 'S'] },
  { x0: 17.5, x1: 20.7, y0: 15.7, y1: 25.4, wall: 8, top: 10.5, walls: stone, roof: slate, roofKind: 'flat', dress: ['W'] },
  // The boulevard's north-west side.
  { x0: -18.0, x1: 12.2, y0: 0.0, y1: 11.3, wall: 10.5, top: 14.3, walls: brick, roof: slate, dress: ['E', 'W', 'N'], chimneys: [[9.5, 2.5], [9.5, 9], [-2, 5.6], [-12, 5.6]] },
  { x0: 4.3, x1: 13.1, y0: -3.8, y1: 0.0, wall: 10, top: 13, walls: stone, roof: slate, roofKind: 'flat', dress: ['E'] },
  { x0: 4.9, x1: 13.5, y0: 11.3, y1: 18.0, wall: 10, top: 13.5, walls: stone, roof: slate, dress: ['E', 'N'] },
  { x0: -15.0, x1: -2.5, y0: 11.3, y1: 22.5, wall: 9, top: 12.5, walls: stone, roof: slate, dress: ['W', 'E', 'N'], chimneys: [[-8.2, 13.5]] },
  { x0: -13.8, x1: -6.3, y0: 22.5, y1: 31.1, wall: 7.5, top: 10.5, walls: brick, roof: slate, dress: ['W', 'E', 'N'] },
  // Souvenirs de France, either side of the Palais du Cinéma.
  { x0: 9.6, x1: 32.2, y0: -18.5, y1: -1.4, wall: 9, top: 12.5, walls: stone, roof: slate, dress: ['E', 'S'], chimneys: [[28.5, -10]] },
  // The Impressions de France theatre, dressed as houses on its street sides.
  { x0: -22.8, x1: 10.3, y0: -31.9, y1: -3.6, wall: 10, top: 13, walls: stone, roof: slate, dress: ['W', 'S', 'E'], chimneys: [[-18, -8], [-18, -22]] },
  { x0: -24.4, x1: 4.3, y0: -10.0, y1: 1.9, wall: 10, top: 13, walls: brick, roof: slate, dress: ['W', 'N'] },
  // Its stage house, the tower's base: a flat-roofed block, dressed like the
  // rest on the sides that face Remy's courtyard.
  { x0: -8.1, x1: 10.3, y0: -40.2, y1: -31.9, wall: 10.5, top: 10.5, walls: stone, roof: slate, roofKind: 'flat', dress: ['S', 'W'] },
  // Les Halles and the small building beside it.
  { x0: 16.8, x1: 33.2, y0: -38.9, y1: -17.6, wall: 7.5, top: 11, walls: stone, roof: slate, dress: ['E', 'W'], chimneys: [[20, -28], [29, -28]] },
  { x0: 10.2, x1: 16.9, y0: -39.0, y1: -36.8, wall: 5, top: 5, walls: stone, roof: slate, roofKind: 'flat', dress: [] },
]
for (const b of B) building(b)

// The Palais du Cinéma: the boulevard's end, a stone front a storey taller
// than its neighbours, with a pediment under a slate hip roof.
{
  const x0 = 12.8, x1 = 21.0, y0 = -9.0, y1 = -1.05, wall = 10.5, ridge = 15
  const b: Rect = { x0, x1, y0, y1 }
  stone.loft([ring(b, 0, 0), ring(b, 0, wall - 0.6)])
  stone.loft([ring(b, 0, wall - 0.6), ring(b, 0.3, wall - 0.35), ring(b, 0.3, wall - 0.1), ring(b, 0.05, wall)])
  // Hip roof.
  const xm = (x0 + x1) / 2, yr0 = y0 + 2.6, yr1 = y1 - 2.6
  slate.quad(P(x0, y0, wall), P(x1, y0, wall), P(x1 - 2.6, yr0, ridge), P(x0 + 2.6, yr0, ridge))
  slate.quad(P(x1, y1, wall), P(x0, y1, wall), P(x0 + 2.6, yr1, ridge), P(x1 - 2.6, yr1, ridge))
  slate.quad(P(x1, y0, wall), P(x1, y1, wall), P(x1 - 2.6, yr1, ridge), P(x1 - 2.6, yr0, ridge))
  slate.quad(P(x0, y1, wall), P(x0, y0, wall), P(x0 + 2.6, yr0, ridge), P(x0 + 2.6, yr1, ridge))
  slate.cap([P(x0 + 2.6, yr0, ridge), P(x1 - 2.6, yr0, ridge), P(x1 - 2.6, yr1, ridge), P(x0 + 2.6, yr1, ridge)], true)
  // Pediment: a stone gable on the front, set forward of the roof slope.
  const yf = y1 + 0.02, pz = wall + 2.3
  stone.tri(P(x1 - 0.3, yf, wall), P(x0 + 0.3, yf, wall), P(xm, yf, pz))
  stone.quad(P(xm, yf, pz), P(xm, yf - 1.2, pz), P(x1 - 0.3, yf - 1.2, wall), P(x1 - 0.3, yf, wall))
  stone.quad(P(x0 + 0.3, yf, wall), P(x0 + 0.3, yf - 1.2, wall), P(xm, yf - 1.2, pz), P(xm, yf, pz))
  // Its oculus dormer, a round window in a stone frame above the pediment.
  {
    const c = P(xm, yr1 + 0.4, pz + 1.1), r = 0.7
    const pts = Array.from({ length: 12 }, (_, k): V3 => [c[0] + r * Math.cos((k / 12) * 2 * Math.PI), c[1], c[2] + r * Math.sin((k / 12) * 2 * Math.PI)])
    box(stone, xm - 0.95, xm + 0.95, yr1 - 0.6, yr1 + 0.35, wall + 1.5, pz + 1.9)
    for (let k = 0; k < 12; k++) glazing.tri([c[0], c[1] + 0.02, c[2]], [pts[(k + 1) % 12][0], c[1] + 0.02, pts[(k + 1) % 12][2]], [pts[k][0], c[1] + 0.02, pts[k][2]])
  }
  // Tall windows over the door, in three bays between pilasters.
  for (const [at, hw, z0, z1] of [[xm, 0.9, 0.3, 3.8], [xm - 2.3, 0.55, 1.0, 3.6], [xm + 2.3, 0.55, 1.0, 3.6],
    [xm, 0.75, 4.6, 8.6], [xm - 2.3, 0.55, 4.6, 8.6], [xm + 2.3, 0.55, 4.6, 8.6]] as const)
    panel(glazing, b, 'N', x1 - at, hw, z0, z1)
  for (const at of [x0 + 0.9, xm - 1.25, xm + 1.25, x1 - 0.9]) box(stone, at - 0.3, at + 0.3, y1, y1 + 0.35, 0, wall - 0.6)
}

// ---------------------------------------------------------------------------
// The Eiffel Tower, on the stage house's roof. OSM's base square is centred
// at (5.45, -34.8) in the survey frame.

const TX = 5.45, TY = -34.8, BASE = 10.5
const TP = (x: number, y: number, z: number): V3 => P(TX + x, TY + y, z)

/** A square member from a to b, its section shrinking from s0 to s1. */
function member(p: Part, a: V3, b: V3, s0: number, s1: number) {
  const t = unit(sub(b, a))
  let side = cross(t, [0, 0, 1])
  if (len(side) < 1e-6) side = [1, 0, 0]
  side = unit(side)
  const up = unit(cross(side, t))
  const sec = (c: V3, s: number) => [[-1, -1], [1, -1], [1, 1], [-1, 1]].map(([i, j]): V3 =>
    [c[0] + (side[0] * i + up[0] * j) * s, c[1] + (side[1] * i + up[1] * j) * s, c[2] + (side[2] * i + up[2] * j) * s])
  const A = sec(a, s0), Bq = sec(b, s1)
  for (let k = 0; k < 4; k++) {
    const l = (k + 1) % 4
    p.quad(A[k], A[l], Bq[l], Bq[k])
  }
}

/** A square ring slab (a platform or a tie) around the shaft at height z. */
function squareSlab(p: Part, half: number, inner: number, z0: number, z1: number) {
  const sq = (h: number, z: number): V3[] => [TP(-h, -h, z), TP(h, -h, z), TP(h, h, z), TP(-h, h, z)]
  p.loft([sq(half, z0), sq(half, z1)])
  if (inner <= 0) { p.cap(sq(half, z1), true); p.cap(sq(half, z0), false); return }
  p.loft([sq(inner, z0).reverse(), sq(inner, z1).reverse()])
  const o1 = sq(half, z1), i1 = sq(inner, z1), o0 = sq(half, z0), i0 = sq(inner, z0)
  for (let k = 0; k < 4; k++) {
    const l = (k + 1) % 4
    p.quad(i1[k], o1[k], o1[l], i1[l])
    p.quad(i0[k], i0[l], o0[l], o0[k])
  }
}

const PLAT1 = 16.8, TOP = 36.2, TIP = 41
/** The shaft's corner half-width at height z: a concave taper, as on the real tower. */
const shaftHalf = (z: number) => 0.55 + 1.7 * Math.pow((TOP - z) / (TOP - PLAT1), 1.8)

// Legs: from OSM's base square in to the first platform, splayed.
for (const [sx, sy] of [[1, 1], [-1, 1], [-1, -1], [1, -1]]) {
  const pts: V3[] = [[2.95, 2.95, BASE], [2.62, 2.62, BASE + 2.4], [2.38, 2.38, BASE + 4.5], [2.22, 2.22, PLAT1]]
  for (let k = 0; k < pts.length - 1; k++) {
    const [x0, y0, z0] = pts[k], [x1, y1, z1] = pts[k + 1]
    member(iron, TP(sx * x0, sy * y0, z0), TP(sx * x1, sy * y1, z1), 0.42 - k * 0.05, 0.37 - k * 0.05)
  }
}
// The arches between the legs, one per face, springing low and peaking
// under the first platform — the tower's most recognisable line.
for (let face = 0; face < 4; face++) {
  const rot = (face * Math.PI) / 2
  const at = (s: number, z: number, d: number): V3 => TP(d * Math.cos(rot) - s * Math.sin(rot), d * Math.sin(rot) + s * Math.cos(rot), z)
  const N = 6, span = 2.3
  for (let k = 0; k < N; k++) {
    const a0 = Math.PI * (k / N), a1 = Math.PI * ((k + 1) / N)
    const s0 = -Math.cos(a0) * span, s1 = -Math.cos(a1) * span
    const z0 = BASE + 0.6 + Math.sin(a0) * (PLAT1 - 0.35 - BASE - 0.6), z1 = BASE + 0.6 + Math.sin(a1) * (PLAT1 - 0.35 - BASE - 0.6)
    const d0 = 2.6 - 0.3 * Math.sin(a0), d1 = 2.6 - 0.3 * Math.sin(a1)
    member(iron, at(s0, z0, d0), at(s1, z1, d1), 0.13, 0.13)
  }
}
// First platform: the widest solid band, OSM's second square.
squareSlab(iron, 2.8, 1.5, PLAT1 - 0.15, PLAT1 + 0.55)
// The shaft: corner posts, tie rings and X bracing on every face.
const TIES = [PLAT1 + 0.55, 19.6, 22.8, 26.3, 29.6, 32.9, TOP]
for (const [sx, sy] of [[1, 1], [-1, 1], [-1, -1], [1, -1]]) {
  for (let k = 0; k < TIES.length - 1; k++) {
    const za = TIES[k], zb = TIES[k + 1]
    member(iron, TP(sx * shaftHalf(za), sy * shaftHalf(za), za), TP(sx * shaftHalf(zb), sy * shaftHalf(zb), zb), 0.27 - k * 0.025, 0.25 - k * 0.025)
  }
}
for (let k = 1; k < TIES.length - 1; k++) {
  const z = TIES[k], h = shaftHalf(z)
  squareSlab(iron, h + 0.12, h - 0.12, z - 0.12, z + 0.12)
}
for (let face = 0; face < 4; face++) {
  const rot = (face * Math.PI) / 2
  const at = (s: number, z: number): V3 => {
    const h = shaftHalf(z), d = h, ss = s * h
    return TP(d * Math.cos(rot) - ss * Math.sin(rot), d * Math.sin(rot) + ss * Math.cos(rot), z)
  }
  for (let k = 0; k < TIES.length - 2; k++) {
    const za = TIES[k], zb = TIES[k + 1]
    member(iron, at(-1, za), at(1, zb), 0.09, 0.08)
    member(iron, at(1, za), at(-1, zb), 0.09, 0.08)
  }
}
// Top platform, lantern, dome and mast.
// The second platform, OSM's third square, a little bolder than the ties.
squareSlab(iron, shaftHalf(22.8) + 0.35, shaftHalf(22.8) - 0.25, 22.6, 23.1)
squareSlab(iron, 1.1, 0, TOP, TOP + 0.45)
squareSlab(iron, 0.55, 0, TOP + 0.45, TOP + 1.8)
{
  const z0 = TOP + 1.8, z1 = TOP + 2.6
  const sq = (h: number, z: number): V3[] => [TP(-h, -h, z), TP(h, -h, z), TP(h, h, z), TP(-h, h, z)]
  iron.loft([sq(0.6, z0), sq(0.36, z0 + 0.4), sq(0.08, z1)])
  member(iron, TP(0, 0, z1), TP(0, 0, TIP), 0.08, 0.04)
}

// ---------------------------------------------------------------------------
// The palette (landmarks/STYLE.md). The Chefs de France roof's green copper
// is the palette's own `copper`; the window panels its `window`; the tower
// its `metal`. Three finishes carry the rest of the pavilion's identity, read
// off the photos and kept in the palette's lightness: the warm ochre stone,
// the red-brown brick and the blue-grey slate of the mansards.
const parts = [
  { part: stone, material: finish('paris-stone', 0xe6d3b0) },
  { part: brick, material: finish('paris-brick', 0xb88471) },
  { part: slate, material: finish('paris-slate', 0x8e9bab) },
  { part: copper, material: PALETTE.copper },
  { part: glazing, material: PALETTE.window },
  { part: iron, material: PALETTE.metal },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('EPCOT France pavilion', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 47.5, osm: 'way/305094683, way/295447782, way/305094845, way/305094686, way/305094846, way/305094685', height: 41,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/wdw-epcot-france.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
