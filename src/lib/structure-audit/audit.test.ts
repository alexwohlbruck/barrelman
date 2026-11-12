import { describe, expect, it } from 'bun:test'
import type { Deck } from '../bridge-decks/build'
import { mercator, type Point } from '../bridge-decks/profile'
import { clashes, deckIssues, lurches } from './decks'
import { profileIssues } from './profile'
import { cutIssues } from './tunnels'

const STEP = 6
/** A straight line east from (lng, lat), `n` samples STEP metres apart. */
const line = (lng: number, lat: number, n: number, north = false): Point[] =>
  Array.from({ length: n }, (_, i) => mercator(lng + (north ? 0 : (i * STEP) / 84_300), lat + (north ? (i * STEP) / 111_000 : 0)))

const deck = (id: string, points: Point[], heights: number[], grounded: [boolean, boolean] = [true, true]): Deck => ({
  id, bridge: id, ways: [], kind: 'road', layer: 1, edges: [5, 5], sides: null, caps: null, grounded,
  length: (points.length - 1) * STEP, points, heights, ground: heights.map(() => 0), piers: [], midpoint: points[0],
})

describe('profileIssues', () => {
  it('passes an arch held to the grade cap with long vertical curves', () => {
    const h = Array.from({ length: 41 }, (_, i) => 7 - ((i * STEP - 120) ** 2) / 4000)
    expect(profileIssues(h, STEP, 'road')).toEqual([])
  })

  it('flags a climb past a ramp grade, a sudden change of grade, and a lone bump', () => {
    const steep = Array.from({ length: 20 }, (_, i) => i * STEP * 0.15)
    expect(profileIssues(steep, STEP, 'road').some(i => i.kind === 'grade' && i.size > 14)).toBe(true)
    const kinked = Array.from({ length: 20 }, (_, i) => Math.max(0, (i - 10) * STEP * 0.08))
    expect(profileIssues(kinked, STEP, 'road').map(i => i.kind)).toContain('kink')
    const bump = Array.from({ length: 20 }, (_, i) => (i === 10 ? 1 : 0))
    expect(profileIssues(bump, STEP, 'road').map(i => i.kind)).toContain('spike')
  })
})

describe('deckIssues', () => {
  it('reports a grounded end left standing off the terrain, by how far', () => {
    const issues = deckIssues(deck('a', line(-74, 40.7, 20), Array.from({ length: 20 }, (_, i) => (i === 19 ? 2 : 0))), () => 0)
    expect(issues.filter(i => i.kind === 'seam').map(i => i.size)).toEqual([2])
  })

  it('reports a span sunk into the ground, away from its ends', () => {
    const issues = deckIssues(deck('a', line(-74, 40.7, 30), Array(30).fill(0)), () => 3)
    expect(issues.find(i => i.kind === 'buried')?.size).toBe(3)
  })
})

describe('between decks', () => {
  it('flags decks crossing too close to clear each other, but not one passing well over', () => {
    const under = deck('under', line(-74.0005, 40.7005, 20), Array(20).fill(10))
    const near = deck('near', line(-74, 40.7, 20, true), Array(20).fill(12))
    const clear = deck('clear', line(-74, 40.7, 20, true), Array(20).fill(17))
    expect(clashes([under, near]).map(c => c.size)).toEqual([2])
    expect(clashes([under, clear])).toEqual([])
  })

  it('flags a ramp arriving on a level deck at a steep grade', () => {
    const main = deck('main', line(-74, 40.7, 40), Array(40).fill(8))
    const ramp = deck('ramp', line(-74 - (19 * STEP) / 84_300, 40.7, 20), Array.from({ length: 20 }, (_, i) => 8 - (19 - i) * STEP * 0.06), [true, false])
    expect(lurches([main, ramp]).map(l => l.deck)).toEqual(['ramp'])
  })
})

describe('cutIssues', () => {
  const points = line(-74, 40.7, 30)
  const floor = Array(30).fill(0)
  const cut = { id: 't', kind: 'road' as const, points, at: 15, open: 5, floor, walls: [floor.map(() => 6), floor.map(() => 6)] as [number[], number[]], crown: 6.5, headroom: 5 }
  const hill = (top: (x: number) => number) => ([x]: Point) => (x > points[15][0] ? top(x) : 0)

  it('passes a portal set into a hill that covers its bore', () => {
    expect(cutIssues(cut, hill(() => 10))).toEqual([])
  })

  it('flags a bore the ground is too thin to cover', () => {
    expect(cutIssues(cut, hill(x => (x > points[22][0] ? 3 : 10))).map(i => [i.kind, i.size])).toEqual([['daylight', 3]])
  })
})
