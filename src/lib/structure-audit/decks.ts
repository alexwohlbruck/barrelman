/**
 * Deck faults the generator's own continuity checks do not look for: a long
 * profile no driver could take, a grounded end that leaves a seam against the
 * terrain the road is draped on, a span sunk below the ground, a deck that
 * crosses another too close to clear it, and an end that rests on a deck at a
 * grade the deck under it does not share.
 */
import type { Deck } from '../bridge-decks/build'
import { along, beside, box, grow, heightAt, lngLat, meet, metresPerUnit, nearSegments, type Box, type Point } from '../bridge-decks/profile'
import { profileIssues, round, CHORD } from './profile'

export type DeckIssue = {
  kind: 'grade' | 'kink' | 'spike' | 'seam' | 'buried' | 'clash' | 'lurch'
  deck: string
  at: [number, number]
  size: number
  other?: string
}

/** Metres a grounded end may stand off the terrain before the joint shows. */
export const SEAM = 0.3
/** Metres past a grounded end where the draped road it lands on is read. */
export const SEAM_REACH = 10
/** Metres a deck may dip below the ground, and the run that counts as sunk rather than a touchdown. */
export const BURIED = { depth: 0.5, run: 12, clear: 24 }
/** Least vertical gap between crossing decks: 4.5 m clearance under a 0.8 m slab. Below `level` they cross at grade. */
export const CLEARANCE = { gap: 5.3, level: 0.3, angle: 30 }
/** Most an end's grade may differ from the deck it rests on, in percent. */
export const LURCH = 2

type Ground = (p: Point) => number

const pointAt = (points: Point[], d: number[], s: number): Point => {
  for (let i = 1; i < d.length; i++)
    if (s <= d[i]) {
      const t = (s - d[i - 1]) / (d[i] - d[i - 1] || 1)
      return [points[i - 1][0] + (points[i][0] - points[i - 1][0]) * t, points[i - 1][1] + (points[i][1] - points[i - 1][1]) * t]
    }
  return points[points.length - 1]
}

export function deckIssues(deck: Deck, ground: Ground): DeckIssue[] {
  const d = along(deck.points)
  const step = deck.length / Math.max(1, deck.points.length - 1)
  const out: DeckIssue[] = profileIssues(deck.heights, step, deck.kind).map(i => ({ kind: i.kind, deck: deck.id, at: lngLat(pointAt(deck.points, d, i.at)), size: i.size }))
  const n = deck.points.length
  for (const end of [0, 1] as const) {
    if (!deck.grounded[end]) continue
    const i = end ? n - 1 : 0
    const p = deck.points[i]
    // The last sample is the line's own end, often a hand's breadth from the one before: aim along the last full step.
    const q = pointAt(deck.points, d, end ? d[n - 1] - step : step)
    const reach = SEAM_REACH / (Math.hypot(p[0] - q[0], p[1] - q[1]) * metresPerUnit(p[1]) || 1)
    const beyond: Point = [p[0] + (p[0] - q[0]) * reach, p[1] + (p[1] - q[1]) * reach]
    // The road it lands on is draped from here on: off at the end but met just past it is a slope the terrain resolves, not a seam.
    const [off, past] = [deck.heights[i] - ground(p), deck.heights[i] - ground(beyond)]
    if (Math.abs(off) > SEAM && Math.abs(past) > SEAM) out.push({ kind: 'seam', deck: deck.id, at: lngLat(p), size: round(Math.abs(off) < Math.abs(past) ? off : past) })
  }
  let run = 0, deepest = 0, at = 0
  const flush = () => {
    if (run >= BURIED.run) out.push({ kind: 'buried', deck: deck.id, at: lngLat(deck.points[at]), size: round(deepest) })
    run = 0; deepest = 0
  }
  for (let i = 0; i < n; i++) {
    const clear = (deck.grounded[0] && d[i] < BURIED.clear) || (deck.grounded[1] && deck.length - d[i] < BURIED.clear)
    const depth = ground(deck.points[i]) - deck.heights[i]
    if (!clear && depth > BURIED.depth) {
      if (depth > deepest) { deepest = depth; at = i }
      run += step
    } else flush()
  }
  flush()
  return out
}

type Indexed = { deck: Deck; d: number[]; bounds: Box }

function crossing(a: Point, b: Point, c: Point, e: Point): [number, number] | null {
  const r = [b[0] - a[0], b[1] - a[1]], s = [e[0] - c[0], e[1] - c[1]]
  const den = r[0] * s[1] - r[1] * s[0]
  if (!den) return null
  const t = ((c[0] - a[0]) * s[1] - (c[1] - a[1]) * s[0]) / den
  const u = ((c[0] - a[0]) * r[1] - (c[1] - a[1]) * r[0]) / den
  return t >= 0 && t <= 1 && u >= 0 && u <= 1 ? [t, u] : null
}

const angle = (a: Point, b: Point, c: Point, e: Point) => {
  const cos = Math.abs(((b[0] - a[0]) * (e[0] - c[0]) + (b[1] - a[1]) * (e[1] - c[1])) / (Math.hypot(b[0] - a[0], b[1] - a[1]) * Math.hypot(e[0] - c[0], e[1] - c[1]) || 1))
  return (Math.acos(Math.min(1, cos)) * 180) / Math.PI
}

/** Decks whose lines cross in plan at a gap neither at grade nor clear of each other. */
export function clashes(decks: Deck[]): DeckIssue[] {
  const all: Indexed[] = decks.filter(k => k.kind !== 'path').map(deck => ({ deck, d: along(deck.points), bounds: box(deck.points) }))
  all.sort((p, q) => p.bounds[0] - q.bounds[0])
  const out: DeckIssue[] = []
  for (let x = 0; x < all.length; x++) {
    const A = all[x]
    for (let y = x + 1; y < all.length && all[y].bounds[0] <= A.bounds[2]; y++) {
      const B = all[y]
      if (!meet(A.bounds, B.bounds)) continue
      let worst: DeckIssue | null = null
      for (let i = 1; i < A.deck.points.length; i++)
        for (let j = 1; j < B.deck.points.length; j++) {
          const [a, b, c, e] = [A.deck.points[i - 1], A.deck.points[i], B.deck.points[j - 1], B.deck.points[j]]
          const hit = crossing(a, b, c, e)
          if (!hit || angle(a, b, c, e) < CLEARANCE.angle) continue
          const ha = heightAt(A.d, A.deck.heights, A.d[i - 1] + (A.d[i] - A.d[i - 1]) * hit[0])
          const hb = heightAt(B.d, B.deck.heights, B.d[j - 1] + (B.d[j] - B.d[j - 1]) * hit[1])
          const gap = Math.abs(ha - hb)
          if (gap > CLEARANCE.level && gap < CLEARANCE.gap && (!worst || gap < worst.size))
            worst = { kind: 'clash', deck: A.deck.id, other: B.deck.id, at: lngLat([a[0] + (b[0] - a[0]) * hit[0], a[1] + (b[1] - a[1]) * hit[0]]), size: round(gap) }
        }
      if (worst) out.push(worst)
    }
  }
  return out
}

/** Ends resting on another deck that arrive at a grade the deck under them does not share. */
export function lurches(decks: Deck[]): DeckIssue[] {
  const all: Indexed[] = decks.map(deck => ({ deck, d: along(deck.points), bounds: grow(box(deck.points), Math.max(...deck.edges) + 2) }))
  const out: DeckIssue[] = []
  for (const A of all) {
    const { deck } = A
    const n = deck.points.length
    const k = Math.max(1, Math.round(CHORD / (deck.length / Math.max(1, n - 1))))
    if (n <= k) continue
    for (const end of [0, 1] as const) {
      if (deck.grounded[end]) continue
      const [i, j] = end ? [n - 1, n - 1 - k] : [0, k]
      const p = deck.points[i]
      const outward = (deck.heights[i] - deck.heights[j]) / (Math.abs(A.d[i] - A.d[j]) || 1)
      const dir: Point = [p[0] - deck.points[j][0], p[1] - deck.points[j][1]]
      let best: { gap: number; other: Deck; grade: number } | null = null
      for (const B of all) {
        if (B === A || !(p[0] >= B.bounds[0] && p[0] <= B.bounds[2] && p[1] >= B.bounds[1] && p[1] <= B.bounds[3])) continue
        const near = beside(B.deck.points, p, nearSegments(B.deck.points, p, 30))
        if (!near.alongside || near.distance > Math.max(...B.deck.edges) + 0.5) continue
        const s = B.d[near.segment - 1] + (B.d[near.segment] - B.d[near.segment - 1]) * near.t
        const gap = Math.abs(heightAt(B.d, B.deck.heights, s) - deck.heights[i])
        if (gap > 1) continue
        const seg: Point = [B.deck.points[near.segment][0] - B.deck.points[near.segment - 1][0], B.deck.points[near.segment][1] - B.deck.points[near.segment - 1][1]]
        const cos = (dir[0] * seg[0] + dir[1] * seg[1]) / (Math.hypot(...dir) * Math.hypot(...seg) || 1)
        if (Math.abs(cos) < 0.85) continue
        const h = (t: number) => heightAt(B.d, B.deck.heights, Math.max(0, Math.min(B.deck.length, t)))
        const grade = ((h(s + CHORD / 2) - h(s - CHORD / 2)) / CHORD) * Math.sign(cos)
        if (!best || gap < best.gap) best = { gap, other: B.deck, grade }
      }
      if (best && Math.abs(outward - best.grade) * 100 > LURCH)
        out.push({ kind: 'lurch', deck: deck.id, other: best.other.id, at: lngLat(p), size: round(Math.abs(outward - best.grade) * 100) })
    }
  }
  return out
}
