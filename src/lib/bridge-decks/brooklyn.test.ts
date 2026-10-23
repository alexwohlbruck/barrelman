/**
 * The Brooklyn Bridge and the interchange at its Manhattan end, recorded from
 * OSM with its terrain: the suspended span, its anchorages and approaches, the
 * elevated promenade, and the ramps to the FDR Drive crossing under and over
 * one another.
 */
import { describe, expect, it } from 'bun:test'
import { buildDecks, type Deck } from './build'
import { along, beside, crossings, VERTICAL_CURVE, heightAt as profileAt, mercator, type Point } from './profile'
import { recorded } from './recorded'
import { deckDefects } from './continuity'

const { input, ground } = await recorded('brooklyn-bridge')
const decks = await buildDecks(input, ground)
const carrying = (way: number) => decks.find(d => d.ways.includes(way))!
const heightAt = (deck: Deck, p: Point) => {
  const near = beside(deck.points, p)
  const j = near.segment
  return deck.heights[j - 1] + (deck.heights[j] - deck.heights[j - 1]) * near.t
}
/** The river's surface on the terrain, which OSM's bridge height is measured from. */
const water = Math.min(...carrying(432550255).ground)
const towers = { manhattan: mercator(-73.9983344, 40.7072687), brooklyn: mercator(-73.9943541, 40.7041037) }
const midspan = mercator(-73.99634, 40.70570)
const roadways = [carrying(432550255), carrying(46177152)]
/** Most a deck's grade changes per metre, over 6 m each side of every metre at least `from` metres in from its ends. */
const bend = (deck: Deck, from = 6) => {
  const d = along(deck.points)
  const at = (s: number) => profileAt(d, deck.heights, s)
  let most = 0
  for (let s = from; s <= deck.length - from; s++) most = Math.max(most, Math.abs(at(s + 6) - 2 * at(s) + at(s - 6)) / 36)
  return most
}

describe('the Brooklyn Bridge', () => {
  it('carries its roadways at the height OSM records over the river, cambered to its towers', () => {
    for (const roadway of roadways) {
      expect(heightAt(roadway, midspan) - water).toBeCloseTo(41, 0)
      // The tower model's roadway band stands 36.5 m over the water.
      for (const tower of Object.values(towers)) expect(Math.abs(heightAt(roadway, tower) - water - 36.5)).toBeLessThan(1.5)
    }
  })

  it('hangs its suspended span from anchorage to anchorage on no piers', () => {
    const anchorages = [mercator(-74.00091, 40.70931), mercator(-73.99171, 40.70201)]
    for (const roadway of roadways) {
      const d = along(roadway.points)
      const [from, to] = anchorages.map(p => { const n = beside(roadway.points, p); return d[n.segment - 1] + (d[n.segment] - d[n.segment - 1]) * n.t }).sort((a, b) => a - b)
      expect(roadway.piers.filter(at => at > from + 20 && at < to - 20)).toEqual([])
      expect(roadway.piers.length).toBeGreaterThan(0)
    }
  })

  it('carries the promenade above its roadways between the towers', () => {
    const promenade = carrying(439842783)
    expect(heightAt(promenade, midspan)).toBeGreaterThan(heightAt(roadways[0], midspan) + 3)
  })

  it('keeps the promenade up between the roadways where it runs on over the anchorages', () => {
    for (const [lng, lat] of [[-74.00091, 40.70931], [-73.99171, 40.70201]]) {
      const anchorage = mercator(lng, lat)
      const promenade = decks.filter(d => d.kind === 'path').sort((a, b) => beside(a.points, anchorage).distance - beside(b.points, anchorage).distance)[0]
      expect(heightAt(promenade, anchorage) - water).toBeGreaterThan(20)
    }
  })

  it('runs the promenade as one walkway from the span down to its entrance at each end', () => {
    // Mapped as short footbridges over each street with untagged stretches between them.
    const brooklyn = carrying(439842769)
    expect(brooklyn).toBe(carrying(808611029))
    expect(brooklyn.grounded.filter(Boolean)).toHaveLength(1)
    const manhattan = carrying(439842783)
    expect(manhattan).toBe(carrying(808611022))
    expect(manhattan.grounded.filter(Boolean)).toHaveLength(1)
  })

  it('carries its roadways and promenade on vertical curves, with no bump where the camber meets the approaches or a neighbour', () => {
    for (const deck of [...roadways, carrying(439842783)]) expect(bend(deck)).toBeLessThan(0.005)
  })

  it('falls away from each tower to the ends of its roadways, never climbing back up', () => {
    for (const roadway of roadways) {
      const d = along(roadway.points)
      for (const tower of Object.values(towers)) {
        const near = beside(roadway.points, tower)
        const at = d[near.segment - 1] + (d[near.segment] - d[near.segment - 1]) * near.t
        const outward = at < roadway.length / 2 ? -1 : 1
        const kept = roadway.heights.filter((_, i) => outward * (d[i] - at) >= 0)
        const run = outward < 0 ? kept.reverse() : kept
        const climbs = run.slice(1).map((v, i) => v - run[i])
        expect(Math.max(...climbs)).toBeLessThan(0.02)
      }
    }
  })

  it('runs each roadway as one deck from its Manhattan approach to its Brooklyn one, without a dip', () => {
    for (const roadway of roadways) {
      expect(roadway.length).toBeGreaterThan(1600)
      const h = roadway.heights
      // Joints between a dozen ways may ripple by a few tenths of a metre, no more.
      for (let i = 1; i < h.length - 1; i++) expect(Math.min(Math.max(...h.slice(0, i + 1)), Math.max(...h.slice(i))) - h[i]).toBeLessThan(0.5)
    }
    // Where it crosses the Manhattan anchorage.
    expect(carrying(808611020)).toBe(carrying(122660460))
  })
})

describe('the interchange at its Manhattan end', () => {
  const near = (d: Deck) => d.points.some(([x, y]) => x > mercator(-74.004, 40.712)[0] && x < mercator(-73.996, 40.705)[0] && y > mercator(-74.004, 40.712)[1] && y < mercator(-73.996, 40.705)[1])

  it('carries each deck over the decks OSM puts below it, with room between', () => {
    const gaps: number[] = []
    for (const a of decks.filter(near))
      for (const b of decks.filter(near)) {
        if (a.layer <= b.layer) continue
        const d = along(a.points)
        for (const at of crossings(a.points, b.points, 2, d)) {
          const i = Math.min(a.points.length - 1, Math.round(at / 6))
          gaps.push(a.heights[i] - heightAt(b, a.points[i]))
        }
      }
    expect(gaps.length).toBeGreaterThan(10)
    expect(Math.min(...gaps)).toBeGreaterThan(5)
  })

  it('meets every deck an end rests on at its own height', () => {
    const steps: number[] = []
    for (const a of decks)
      for (const end of [0, 1]) {
        if (a.grounded[end]) continue
        const i = end ? a.points.length - 1 : 0
        for (const b of decks)
          if (b !== a && (b.kind === a.kind || b.layer === a.layer) && beside(b.points, a.points[i]).distance < 1) steps.push(Math.abs(heightAt(b, a.points[i]) - a.heights[i]))
      }
    expect(steps.length).toBeGreaterThan(20)
    expect(Math.max(...steps)).toBeLessThan(0.3)
  })

  it('eases every long deck into vertical curves away from its ends', () => {
    for (const deck of decks.filter(d => d.length >= 2 * VERTICAL_CURVE)) expect(bend(deck, VERTICAL_CURVE / 2)).toBeLessThan(0.004)
  })

  it('dips a ramp under one climbing from the street to cross it, rather than run them through each other', () => {
    const over = carrying(32922459), under = carrying(5672337)
    const d = along(over.points)
    const [at] = crossings(over.points, under.points, 2, d)
    const p = over.points[Math.round(at / 6)]
    expect(heightAt(over, p) - heightAt(under, p)).toBeGreaterThan(5)
  })
})

describe('the decks around the bridge, end to end', () => {
  const [w, sth, e, n] = [-74.005, 40.6995, -73.987, 40.713]
  const defects = deckDefects(decks).filter(({ at: [lng, lat] }) => lng >= w && lng <= e && lat >= sth && lat <= n)
  const of = (kind: string) => defects.filter(d => d.kind === kind)

  it('leaves no scrap of deck standing alone, meeting nothing at either end', () => {
    expect(of('orphan')).toEqual([])
  })

  it('keeps each deck to its own lanes and outline: no plates where it passes under another bridge or a walkway lands beside it', () => {
    expect(Math.max(0, ...of('bulge').map(d => d.size))).toBeLessThan(6)
    expect(Math.max(0, ...of('width').map(d => d.size))).toBeLessThan(5)
  })

  it('stops a deck in mid air only where OSM leaves it so', () => {
    // The Brooklyn-Queens Expressway's short bridges between embankment stretches, and a ramp into the Brooklyn anchorage.
    expect(of('dangling').length).toBeLessThanOrEqual(3)
  })

  it('carries a ramp merging into a main line at the main line\'s height along the merge', () => {
    expect(of('overlap').length).toBeLessThanOrEqual(4)
    expect(Math.max(0, ...of('overlap').map(d => d.size))).toBeLessThan(2)
  })
})
