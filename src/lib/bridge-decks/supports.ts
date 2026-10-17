/**
 * Where a deck's supports may stand: never on a road, railway or path on the
 * ground below, nor on a lower deck. A pier that would is moved along the deck
 * to the nearest clear place, or left out.
 */
import { beside, box, covers, grow, inside, meet, metresPerUnit, sideAt, type Box, type Chain, type Point } from './profile'

/** Room a support keeps from the edge of what it stands beside, in metres. */
export const SUPPORT_ROOM = { road: 1, rail: 2.5, path: 0.5 } as const
/** Half the width of a way below when its tags do not say, in metres. */
const WAY_HALF = { road: 4, rail: 2, path: 1.5 } as const
/** Metres between the points a bent is tested at, across and along. */
const PROBE = 1
/** How far a bent reaches along the deck either side of its line: its cap, and the columns under it. */
const BENT_REACH = 1
/** The least distance a pier keeps from a deck's end, in metres. */
const END_ROOM = 3

/** A way on the ground under the decks, with half its width. */
export type Below = { kind: keyof typeof SUPPORT_ROOM; points: Point[]; half: number }

/** What supports keep clear of, each with the box within which it reaches: ways below, carriageways on the ground (outer ring, then holes), and the decks beneath. */
export type Obstacles = {
  ways: Array<Below & { reach: Box }>
  surfaces: Array<{ rings: Point[][]; reach: Box }>
  decks: Array<{ points: Point[]; half: number; reach: Box }>
}

/** Obstacles with their boxes worked out once. */
export function obstacles(ways: Below[], surfaces: Point[][][], decks: Array<{ points: Point[]; half: number }>): Obstacles {
  return {
    ways: ways.map(w => ({ ...w, reach: grow(box(w.points), w.half + SUPPORT_ROOM[w.kind]) })),
    surfaces: surfaces.map(rings => ({ rings, reach: grow(box(rings[0]), SUPPORT_ROOM.road) })),
    decks: decks.map(d => ({ ...d, reach: grow(box(d.points), d.half + SUPPORT_ROOM.road) })),
  }
}

/** Whether a point lies in a polygon: inside its outer ring and none of its holes. */
const inPolygon = (q: Point, rings: Point[][]) => inside(q, rings[0]) && !rings.slice(1).some(h => inside(q, h))

/** Half a way's width: its tagged width in metres, else its lanes, else its kind's usual. */
export function wayHalf(kind: Below['kind'], width: number | null, lanesTag: string | null = null): number {
  if (width !== null && width > 0) return width / 2
  const lanes = Number.parseInt(lanesTag ?? '', 10)
  if (kind === 'road' && Number.isFinite(lanes) && lanes > 0) return lanes * 1.75 + 0.5
  return WAY_HALF[kind]
}

/** Points across a deck at `at` metres along, edge to edge, on its line and BENT_REACH either side. */
export function bentPoints(chain: Pick<Chain, 'points' | 'edges' | 'sides'>, d: number[], at: number): Point[] {
  const n = chain.points.length
  let k = 1
  while (k < n - 1 && d[k] < at) k++
  const t = Math.max(0, Math.min(1, (at - d[k - 1]) / (d[k] - d[k - 1] || 1)))
  const [a, b] = [chain.points[k - 1], chain.points[k]]
  const scale = 1 / metresPerUnit(a[1])
  const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
  const [ux, uy] = [(b[0] - a[0]) / len, (b[1] - a[1]) / len]
  const c: Point = [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
  const [l, r] = [sideAt(chain, 0, k - 1, t), sideAt(chain, 1, k - 1, t)]
  const out: Point[] = []
  for (let along = -BENT_REACH; along <= BENT_REACH; along += BENT_REACH)
    for (let across = -r; across <= l + 1e-9; across += Math.min(PROBE, l + r || PROBE))
      out.push([c[0] + (ux * along - uy * across) * scale, c[1] + (uy * along + ux * across) * scale])
  return out
}

/** Whether any of these points stands on something a support must keep clear of. */
export function blocked(points: Point[], obstacles: Obstacles): boolean {
  if (!points.length) return false
  const own: Box = box(points)
  for (const w of obstacles.ways)
    if (meet(w.reach, own) && points.some(q => beside(w.points, q).distance < w.half + SUPPORT_ROOM[w.kind])) return true
  for (const s of obstacles.surfaces)
    if (meet(s.reach, own) && points.some(q => covers(s.reach, q) && inPolygon(q, s.rings))) return true
  for (const d of obstacles.decks)
    if (meet(d.reach, own) && points.some(q => beside(d.points, q).distance < d.half + SUPPORT_ROOM.road)) return true
  return false
}

/** The nearest place to `at` along a deck `total` metres long where `free` holds, within `reach` metres; null where none does. */
export function nearestFree(at: number, total: number, reach: number, free: (at: number) => boolean): number | null {
  for (let shift = 0; shift <= reach; shift += PROBE)
    for (const s of shift ? [at - shift, at + shift] : [at])
      if (s >= END_ROOM && s <= total - END_ROOM && free(s)) return s
  return null
}
