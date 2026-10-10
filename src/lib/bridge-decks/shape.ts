/**
 * Decks shaped to their hand-drawn `man_made=bridge` outlines.
 *
 * At every sample the deck reaches, on each side, to where a line square to
 * it leaves the outline: so a deck widens and narrows with the bridge drawn
 * around it rather than standing at its widest throughout. Decks that share an
 * outline (twin carriageways, a ramp leaving a main line) split the room
 * between them, and a deck that lands on the ground ends where its outline
 * does, at whatever skew the abutment was drawn.
 */
import { along, beside, box, covers, inside, MAX_REACH, meet, metresPerUnit, type Box, type Chain, type Point } from './profile'

export type Outline = { id: string; rings: Point[][] }

/**
 * Metres from the centreline to each edge at every sample, and how far each
 * side stops short of the deck's ends: [start left, start right, end left,
 * end right], negative where it runs on past them.
 */
export type Shape = { sides: [number[], number[]]; caps: [number, number, number, number] }

/** Farthest an end may move to meet its outline's end, in metres: a wide bridge crossing at a skew reaches far on one side. */
export const CAP_MAX = 20
/** How far inside its edge a corner is tested from, so one lying on the outline's side counts as in it. */
const CORNER_INSET = 0.5

/** A deck to shape: shaped only where `outline` names the outline it was found in, by `inOutline`. */
type Deck = Pick<Chain, 'points' | 'edges' | 'layer' | 'fit'> & { outline?: string | null }

/** Inside an outline's outer ring and none of its holes. */
export const within = (p: Point, rings: Point[][]) => inside(p, rings[0]) && !rings.slice(1).some(ring => inside(p, ring))

/** Whether a deck lies in an outline: at least half its points do. The one rule for naming a deck after an outline and shaping it to one. */
export const inOutline = (points: Point[], rings: Point[][]) => points.filter(p => within(p, rings)).length * 2 >= points.length

/** Unit tangent and left normal at a sample, in mercator directions. */
function frame(points: Point[], i: number): { t: Point; n: Point } {
  const [a, b] = [points[Math.max(0, i - 1)], points[Math.min(points.length - 1, i + 1)]]
  const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
  const t: Point = [(b[0] - a[0]) / len, (b[1] - a[1]) / len]
  return { t, n: [-t[1], t[0]] }
}

const neg = ([x, y]: Point): Point => [-x, -y]

/**
 * Where a ray first crosses one of the lines: metres along it, the unit
 * direction of the segment it crosses there, and that segment, by its first
 * point. Segments in `skip`, by their first point, are passed through.
 */
function cross(o: Point, dir: Point, lines: Point[][], skip?: Set<Point>): { metres: number; segment: Point; start: Point | null } {
  let best = Infinity
  let segment: Point = [0, 0]
  let start: Point | null = null
  for (const line of lines)
    for (let i = 1; i < line.length; i++) {
      const [a, b] = [line[i - 1], line[i]]
      if (skip?.has(a)) continue
      const s: Point = [b[0] - a[0], b[1] - a[1]]
      const den = dir[0] * s[1] - dir[1] * s[0]
      if (!den) continue
      const qx = a[0] - o[0]
      const qy = a[1] - o[1]
      const along = (qx * s[1] - qy * s[0]) / den
      const u = (qx * dir[1] - qy * dir[0]) / den
      if (along > 0 && u >= 0 && u <= 1 && along < best) [best, segment, start] = [along, s, a]
    }
  const len = Math.hypot(segment[0], segment[1]) || 1
  return { metres: best * metresPerUnit(o[1]), segment: [segment[0] / len, segment[1] / len], start }
}

/** Metres along a ray to the nearest place it crosses one of the lines; Infinity if it crosses none. */
export const rayHit = (o: Point, dir: Point, lines: Point[][], skip?: Set<Point>) => cross(o, dir, lines, skip).metres

/** Least cosine between a deck and a neighbour that shares its room: within about 35 degrees, not one crossing over or under. */
const ALONGSIDE = 0.82

const offset = (p: Point, dir: Point, metres: number): Point => {
  const u = metres / metresPerUnit(p[1])
  return [p[0] + dir[0] * u, p[1] + dir[1] * u]
}

/**
 * A neighbour's ends run on straight, so the room beside an end is still split
 * with it: each up to CAP_MAX, but stopping where it would cross this deck's
 * centreline, so it never pinches the far side of the deck it meets. An end
 * this deck carries (it rests on it, as a ramp leaving a main line or the next
 * deck of a road does) is not run on at all. Each run is a segment in the
 * neighbour's own direction, so the side of it a point lies on reads the same.
 */
function runOn(o: Point[], deck: Point[], reach: number): Point[][] {
  const n = o.length
  const out: Point[][] = []
  for (const end of [0, 1] as const) {
    const base = o[end ? n - 1 : 0]
    if (beside(deck, base).distance <= reach) continue
    const t = frame(o, end ? n - 1 : 0).t
    const dir = end ? t : neg(t)
    const metres = Math.min(CAP_MAX, rayHit(base, dir, [deck]))
    if (metres < CORNER_INSET) continue
    const tip = offset(base, dir, metres)
    out.push(end ? [base, tip] : [tip, base])
  }
  return out
}

/**
 * Single-sample spikes, where a square line catches a notch in the outline or
 * an end lying on its side, taken out: each value the median of it and its
 * neighbours, an end's of the three nearest.
 */
function despike(values: number[]): number[] {
  const median = (a: number, b: number, c: number) => Math.max(Math.min(a, b), Math.min(Math.max(a, b), c))
  const n = values.length
  if (n < 3) return values
  return values.map((v, i) => {
    const k = Math.min(Math.max(i, 1), n - 2)
    return median(values[k - 1], values[k], values[k + 1])
  })
}

/**
 * The side of an outline a deck's end crosses (or would, run on CAP_MAX past
 * it), by its first point: its abutment, which a square line near a skewed end
 * would otherwise leave through instead of the deck's side.
 */
function abutment(pts: Point[], end: 0 | 1, rings: Point[][]): Point | null {
  const i = end ? pts.length - 1 : 0
  const out = end ? frame(pts, i).t : neg(frame(pts, i).t)
  const hit = cross(pts[i], within(pts[i], rings) ? out : neg(out), rings)
  return hit.metres <= CAP_MAX ? hit.start : null
}

/**
 * Each deck's shape where it lies in an outline; null for one with no
 * `outline`, or none of whose samples lie in one. Decks are resampled, with
 * their edges at the least they may be (their carriageway's), and `grounded`
 * says which ends land on the ground: only those are moved to the outline's
 * end, so a ramp still meets the deck it joins.
 */
export function shapeDecks(decks: Deck[], outlines: Outline[], grounded: Array<[boolean, boolean]>): Array<Shape | null> {
  const bounds: Box[] = outlines.map(o => box(o.rings[0]))
  const boxes: Box[] = decks.map(d => box(d.points))
  // Decks meeting each outline, once, rather than every deck tested against every other.
  const meeting = bounds.map(b => boxes.flatMap((x, j) => (meet(x, b) ? [j] : [])))
  return decks.map((deck, k) => {
    if (!deck.outline) return null
    const pts = deck.points
    const n = pts.length
    const near = outlines.flatMap((_, j) => (meet(boxes[k], bounds[j]) ? [j] : []))
    if (!near.length) return null
    const home = pts.map(p => near.find(j => covers(bounds[j], p) && within(p, outlines[j].rings)) ?? -1)
    if (home.every(h => h < 0)) return null
    const d = along(pts)
    const fromEnd = (i: number, end: 0 | 1) => Math.abs(d[end ? n - 1 : 0] - d[i])
    // Each end's abutment, in the outline nearest that end.
    const homeAt = (end: 0 | 1) => (end ? [...home].reverse() : home).find(h => h >= 0)!
    const abutments = ([0, 1] as const).map(end => abutment(pts, end, outlines[homeAt(end)].rings))
    // Decks in the same outlines: the room between two running alongside is split between them.
    const reach = Math.max(1, ...(deck.fit ?? deck.edges)) + 1
    const others = [...new Set(near.flatMap(j => meeting[j]))]
      .filter(j => j !== k)
      .map(j => ({ edges: decks[j].edges, lines: [decks[j].points, ...runOn(decks[j].points, pts, reach)] }))
    const sides: [number[], number[]] = [new Array(n).fill(NaN), new Array(n).fill(NaN)]
    // Where the outline itself reaches at least as far as the carriageway: an end's corner there lies on its side.
    const reached: [boolean[], boolean[]] = [new Array(n).fill(false), new Array(n).fill(false)]
    for (let i = 0; i < n; i++) {
      if (home[i] < 0) continue
      const skip = new Set(([0, 1] as const).flatMap(end => (abutments[end] && fromEnd(i, end) <= CAP_MAX ? [abutments[end]!] : [])))
      const { t, n: left } = frame(pts, i)
      for (const side of [0, 1] as const) {
        const dir = side ? neg(left) : left
        const boundary = rayHit(pts[i], dir, outlines[home[i]].rings, skip)
        // On the outline's side, which a way often ends on, or out past a skewed end: no reading.
        if (boundary === Infinity) continue
        reached[side][i] = boundary >= deck.edges[side] - CORNER_INSET
        let edge = boundary
        for (const o of others) {
          const { metres: gap, segment, start } = cross(pts[i], dir, o.lines)
          if (!start || Math.abs(segment[0] * t[0] + segment[1] * t[1]) < ALONGSIDE) continue
          // The neighbour's side facing this deck, so two decks' edges meet exactly between them.
          const facing = segment[0] * (pts[i][1] - start[1]) - segment[1] * (pts[i][0] - start[0]) > 0 ? 0 : 1
          // One beyond the outline still splits the room if its own carriageway reaches back into it.
          if (gap - o.edges[facing] >= edge) continue
          edge = Math.min(edge, (gap + deck.edges[side] - o.edges[facing]) / 2)
        }
        sides[side][i] = Math.max(deck.edges[side], Math.min(edge, MAX_REACH))
      }
    }
    const read = sides[0].map((l, i) => !Number.isNaN(l) && !Number.isNaN(sides[1][i]))
    const first = read.indexOf(true)
    if (first < 0) return null
    const last = read.lastIndexOf(true)
    // An end running on past its outline keeps the outline's last width for as far as its cap may reach; a gap between outlines has the deck's own.
    const shaped = sides.map((s, side) => despike(s.map((v, i) => {
      if (!Number.isNaN(v)) return v
      const from = i < first ? first : i > last ? last : -1
      return from >= 0 && Math.abs(d[i] - d[from]) <= CAP_MAX ? s[from] : deck.edges[side]
    }))) as [number[], number[]]
    const caps: Shape['caps'] = [0, 0, 0, 0]
    for (const end of [0, 1] as const) {
      const nearEnd = home.flatMap((h, i) => (h >= 0 && fromEnd(i, end) <= CAP_MAX ? [i] : []))
      if (!grounded[k][end] || !nearEnd.length) continue
      for (const side of [0, 1] as const)
        if (nearEnd.some(i => reached[side][i]))
          caps[end * 2 + side] = capOf(pts, d, end, side, shaped[side][end ? n - 1 : 0], outlines[homeAt(end)].rings)
    }
    return { sides: shaped, caps }
  })
}

/**
 * How far one side of a grounded end moves to meet the outline's end: back
 * from a corner that lies outside it, on past one that lies inside it, along
 * the deck. Nowhere, if the outline does not end within CAP_MAX of it, and
 * never so far the two ends would cross.
 */
function capOf(pts: Point[], d: number[], end: 0 | 1, side: 0 | 1, edge: number, rings: Point[][]): number {
  const i = end ? pts.length - 1 : 0
  const { t, n: left } = frame(pts, i)
  const out = end ? t : neg(t)
  const corner = offset(pts[i], side ? neg(left) : left, Math.max(0, edge - CORNER_INSET))
  const cap = within(corner, rings) ? -rayHit(corner, out, rings) : rayHit(corner, neg(out), rings)
  return Math.abs(cap) <= CAP_MAX ? Math.min(cap, Math.max(0, d[d.length - 1] / 2 - 1)) : 0
}
