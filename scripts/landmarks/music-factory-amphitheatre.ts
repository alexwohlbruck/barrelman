/**
 * Skyla Credit Union Amphitheatre, AvidXchange Music Factory — procedural, CC0-1.0.
 * bun scripts/landmarks/music-factory-amphitheatre.ts
 *
 * The outdoor amphitheatre north of the mill (opened June 2009 as the Uptown
 * Amphitheatre, later Charlotte Metro Credit Union Amphitheatre, Skyla Credit
 * Union Amphitheatre today; 5,000 people). Its stage stands against the east
 * wall of Mill #2 (The Fillmore) and faces east-north-east over a fan of
 * reserved seats to the festival lawn.
 *
 * What stands up is the stage: a concrete-block deck, and over it a broad
 * shed roof on steel columns that tilts up towards the audience, its deep
 * dark fascia and underside full of rigging (Commons photos 2009 and 2018).
 * The stage is closed at the back by a grey wall, on stage left by a tall
 * glazed wing and on stage right by a grey one.
 * The seats are on graded ground and the lawn is grass, so both are the map's.
 *
 * OSM: the roof is way/1202433678 (building=roof, no height), traced loosely
 * with notches; the roof is drawn as that outline's box in the stage frame. The seating area way/957551010 is not a building. Heights are
 * from the photos: deck ~1.6 m, roof underside ~13 m over the deck at the
 * back and ~16 m at the front (a line array of ~4.5 m hangs well clear of the
 * deck in the 2018 photos), a 1.5 m fascia. Map frame: x east, y north,
 * z up; bearing 0.
 */
import { Part, type V3 } from './mesh'
import { PALETTE, finish } from './palette'
import { axes, centroid, clean, earcut, ground, lowest, quad, solid, tri, write, type Built, type XY } from './music-factory-mill'

/** way/1202433678. */
const ROOF: XY[] = [[-14.8, 96.3], [-6.3, 94.9], [-1.4, 98.0], [1.2, 92.7], [5.7, 92.6], [10.0, 85.0], [4.5, 81.9], [3.8, 80.4], [0.7, 73.9], [-6.9, 76.7], [-22.0, 79.1], [-15.4, 92.4]]

export function buildAmphitheatre(): Built {
  const top = new Part(), dark = new Part(), glass = new Part(), grey = top
  const O = clean(ROOF), ax = axes(73)            // f: towards the audience
  // The outline is traced loosely, with notches; the roof itself is a plain
  // rectangle, so it is drawn as the outline's box in the stage's own frame.
  const fs = O.map(ax.s), ts = O.map(ax.t)
  const F0 = Math.min(...fs) + 1, F1 = Math.max(...fs) - 1.5, T0 = Math.min(...ts) + 1.5, T1 = Math.max(...ts) - 1.5
  const box = (f0: number, f1: number, t0: number, t1: number) => clean([ax.at(f0, t0), ax.at(f1, t0), ax.at(f1, t1), ax.at(f0, t1)])
  const P = box(F0, F1, T0, T1)
  const stageGround = ground(ax.at((F0 + F1) / 2, (T0 + T1) / 2))
  const floor = stageGround + 1.6
  const under = (q: XY) => floor + 13 + 3 * (ax.s(q) - F0) / (F1 - F0)
  const FASCIA = 1.5

  // The roof: a slab over the outline, top in roof grey, fascia and soffit dark.
  for (const [i, j, k] of earcut(P)) {
    const T = (q: XY): V3 => [q[0], q[1], under(q) + FASCIA], U = (q: XY): V3 => [q[0], q[1], under(q)]
    tri(top, T(P[i]), T(P[j]), T(P[k]), [0, 0, 1])
    tri(dark, U(P[i]), U(P[k]), U(P[j]), [0, 0, -1])
  }
  for (let i = 0; i < P.length; i++) {
    const A = P[i], B = P[(i + 1) % P.length]
    quad(dark, [A[0], A[1], under(A)], [B[0], B[1], under(B)], [B[0], B[1], under(B) + FASCIA], [A[0], A[1], under(A) + FASCIA], [B[1] - A[1], A[0] - B[0], 0])
  }

  // The deck: the roof's outline less a strip at the front, so the roof
  // overhangs the stage edge.
  // Concrete-block sides and a black stage floor.
  solid(grey, box(F0 + 0.5, F1 - 4, T0 + 2, T1 - 2), stageGround - 2, floor, dark)

  // Back wall, just in front of the Fillmore's wall, and the two side wings
  // over the back half of the stage: stage left (north) glazed, stage right
  // dark.
  const fb = F0 + 2.5, fm = F0 + (F1 - F0) * 0.55
  solid(grey, box(F0 + 0.5, fb, T0 + 1, T1 - 1), stageGround - 1, (q) => under(q) + 0.05)
  solid(glass, box(fb, fm, T0 + 1, T0 + 2), floor, (q) => under(q) - 0.05)
  solid(grey, box(fb, fm, T1 - 2, T1 - 1), floor, (q) => under(q) - 0.05)

  // Steel columns at the front corners, each with a diagonal strut back up to
  // the roof (2009 photo), and two more at mid-depth.
  const column = (f: number, t: number, w = 0.35) => solid(dark, box(f - w, f + w, t - w, t + w), floor - 0.2, under(ax.at(f, t)) + 0.05)
  for (const t of [T0 + 1.6, T1 - 1.6]) {
    const fc = F1 - 3.5
    column(fc, t)
    // The strut: a square section from the column, up and back to the roof.
    const a = ax.at(fc, t), b = ax.at(fc - 6, t), za = floor + 6.5, zb = under(b)
    const w = 0.22, side: XY = [ax.r[0] * w, ax.r[1] * w]
    const S = (p: XY, z: number, sx: number, sz: number): V3 => [p[0] + side[0] * sx, p[1] + side[1] * sx, z + w * sz]
    const ring = (p: XY, z: number) => [S(p, z, -1, -1), S(p, z, 1, -1), S(p, z, 1, 1), S(p, z, -1, 1)]
    const r0 = ring(a, za), r1 = ring(b, zb)
    for (let i = 0; i < 4; i++) {
      const j = (i + 1) % 4, mx = (r0[i][0] + r0[j][0]) / 2 - (a[0]), my = (r0[i][1] + r0[j][1]) / 2 - a[1], mz = (r0[i][2] + r0[j][2]) / 2 - za
      quad(dark, r0[i], r0[j], r1[j], r1[i], [mx, my, mz])
    }
  }

  const anchor = centroid(O), base = lowest(O)
  return {
    id: 'music-factory-amphitheatre', name: 'Skyla Credit Union Amphitheatre', anchor, base,
    height: Math.max(...P.map((q) => under(q) + FASCIA)) - base,
    parts: [
      { part: top, material: PALETTE.roof },
      { part: dark, material: finish('stage-charcoal', 0x4a4f57) },
      { part: glass, material: PALETTE.glass },
    ],
  }
}

if (import.meta.main) await write(buildAmphitheatre())
