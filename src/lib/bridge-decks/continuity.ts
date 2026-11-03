/**
 * Where built decks fail to read as continuous roads: ends that stop in mid
 * air or step onto the deck they meet, widths that jump, sides that balloon
 * out past their lanes, decks stacked a hand's breadth apart, and stray
 * scraps. The generator's tests hold a study area to none of these.
 */
import type { Deck } from './build'
import { along, beside, box, covers, grow, heightAt, lngLat, meet, nearSegments, sideAt, type Box, type Point } from './profile'

export type Defect = {
  kind: 'dangling' | 'step' | 'floating' | 'width' | 'bulge' | 'overlap' | 'orphan'
  deck: string
  /** Longitude and latitude. */
  at: [number, number]
  /** Metres: the step, jump, excess width, or height between stacked decks. */
  size: number
  /** The deck it steps onto or lies over. */
  other?: string
}

/** Metres an end may lie off another deck's edge and still meet it, and the most it may step up or down onto it. */
const MEET = { reach: 0.5, step: 0.15 }
/** Most a grounded end may stand over the ground. */
const LANDED = 1.5
/** Most a side may change between samples, and stand past the deck's usual width, in metres. */
const WIDTH = { jump: 1.5, bulge: 3 }
/** Two decks stacked closer than this read as one with a ledge, in metres, over at least this length; and the most they may diverge to count as stacked, in degrees. */
const STACKED = { gap: 2.5, length: 6, angle: 30 }
/** Shortest a deck may be that meets nothing at either end. */
const SCRAP = 30

type Indexed = { deck: Deck; bounds: Box; d: number[] }

/** The deck surface under a point: how far it lies outside the deck's edge (negative inside) and the height there. */
function onto(o: Indexed, p: Point): { outside: number; z: number; segment: number } | null {
  const near = beside(o.deck.points, p, nearSegments(o.deck.points, p, 40))
  if (near.segment < 1) return null
  const side = sideAt({ edges: o.deck.edges, sides: o.deck.sides ?? undefined }, near.left ? 0 : 1, near.segment - 1, near.t)
  const s = o.d[near.segment - 1] + (o.d[near.segment] - o.d[near.segment - 1]) * near.t
  // Past an end the surface stops square, so only alongside counts.
  if (!near.alongside) return null
  return { outside: near.distance - side, z: heightAt(o.d, o.deck.heights, s), segment: near.segment }
}

const median = (values: number[]) => [...values].sort((a, b) => a - b)[values.length >> 1]

export function deckDefects(decks: Deck[]): Defect[] {
  const indexed: Indexed[] = decks.map(deck => ({ deck, bounds: grow(box(deck.points), Math.max(...deck.edges) + 2), d: along(deck.points) }))
  const out: Defect[] = []
  const report = (kind: Defect['kind'], deck: Deck, p: Point, size: number, other?: Deck) =>
    out.push({ kind, deck: deck.id, at: lngLat(p), size: Math.round(size * 100) / 100, ...(other ? { other: other.id } : {}) })
  for (const a of indexed) {
    const { deck } = a
    const n = deck.points.length
    if (n < 2) continue
    const meets: boolean[] = []
    for (const end of [0, 1] as const) {
      const i = end ? n - 1 : 0
      const p = deck.points[i]
      if (deck.grounded[end]) {
        meets.push(true)
        if (deck.heights[i] - deck.ground[i] > LANDED) report('floating', deck, p, deck.heights[i] - deck.ground[i])
        continue
      }
      let best: { outside: number; dz: number; on: Deck } | null = null
      for (const o of indexed) {
        if (o === a || !covers(o.bounds, p)) continue
        const at = onto(o, p)
        if (!at || at.outside > MEET.reach) continue
        const dz = Math.abs(at.z - deck.heights[i])
        if (!best || dz < best.dz) best = { outside: at.outside, dz, on: o.deck }
      }
      meets.push(!!best && best.dz <= MEET.step)
      if (!best) report('dangling', deck, p, 0)
      else if (best.dz > MEET.step) report('step', deck, p, best.dz, best.on)
    }
    if (!meets[0] && !meets[1] && deck.length < SCRAP) report('orphan', deck, deck.points[n >> 1], deck.length)
    if (deck.sides)
      for (const side of [0, 1] as const) {
        const s = deck.sides[side]
        const usual = median(s)
        let bulge = -1
        for (let i = 0; i < n; i++) {
          if (i && Math.abs(s[i] - s[i - 1]) > WIDTH.jump) report('width', deck, deck.points[i], Math.abs(s[i] - s[i - 1]))
          if (s[i] > usual + WIDTH.bulge && (bulge < 0 || s[i] > s[bulge])) bulge = i
          if (bulge >= 0 && (s[i] <= usual + WIDTH.bulge || i === n - 1)) {
            report('bulge', deck, deck.points[bulge], s[bulge] - usual)
            bulge = -1
          }
        }
      }
  }
  // Decks stacked a hand's breadth apart along a stretch: one shows through the other, or stands on it as a ledge.
  for (const [k, a] of indexed.entries())
    for (const b of indexed.slice(k + 1)) {
      if (!meet(a.bounds, b.bounds) || (a.deck.kind === 'rail') !== (b.deck.kind === 'rail')) continue
      let run = 0
      let worst = { gap: 0, at: a.deck.points[0] }
      const flush = () => {
        if (run >= STACKED.length) report('overlap', a.deck, worst.at, worst.gap, b.deck)
        run = 0
        worst = { gap: 0, at: a.deck.points[0] }
      }
      for (let i = 1; i < a.deck.points.length; i++) {
        const p = a.deck.points[i]
        const at = covers(b.bounds, p) ? onto(b, p) : null
        const gap = at ? Math.abs(at.z - a.deck.heights[i]) : 0
        const [u, v] = [a.deck.points[i - 1], p]
        const [q0, q1] = at ? [b.deck.points[at.segment - 1], b.deck.points[at.segment]] : [u, v]
        const cos = Math.abs(((v[0] - u[0]) * (q1[0] - q0[0]) + (v[1] - u[1]) * (q1[1] - q0[1])) / (Math.hypot(v[0] - u[0], v[1] - u[1]) * Math.hypot(q1[0] - q0[0], q1[1] - q0[1]) || 1))
        if (at && at.outside < -0.5 && gap > MEET.step && gap < STACKED.gap && cos > Math.cos((STACKED.angle * Math.PI) / 180)) {
          run += a.d[i] - a.d[i - 1]
          if (gap > worst.gap) worst = { gap, at: p }
        } else flush()
      }
      flush()
    }
  return out
}
