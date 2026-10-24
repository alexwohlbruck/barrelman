import { describe, expect, it } from 'bun:test'
import { CAMBER, DECK_DEPTH, deckLevel, hungStretch, levelNeeds, type Support } from './elevation'
import { mercator, type Point } from './profile'

/** A square footprint `half` metres each way around a point east of the origin. */
const square = (east: number, half: number): Point[] => {
  const deg = (m: number) => m / 84_300
  return [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]].map(([x, y]) => mercator(-74 + deg(east + x * half), 40.7 + (y * half) / 111_000))
}
/** A point moved `north` metres. */
const lngLatOf = ([x, y]: Point, north: number): [number, number] => {
  const lng = x * 360 - 180
  const lat = (Math.atan(Math.sinh(Math.PI * (1 - 2 * y))) * 180) / Math.PI
  return [lng, lat + north / 111_000]
}
const tower = (east: number, height = 84): Support => ({ kind: 'pylon', ring: square(east, 8), height })
const anchorage = (east: number): Support => ({ kind: 'abutment', ring: square(east, 15), height: 12 })
/** A deck running east along the parallel every 6 m. */
const deck = (to: number): Point[] => Array.from({ length: to / 6 + 1 }, (_, i) => mercator(-74 + (i * 6) / 84_300, 40.7))

describe('a deck height from what OSM records on the bridge outline', () => {
  it('takes a charted clearance under the deck, plus the deck', () => {
    expect(deckLevel({ 'seamark:bridge:clearance_height': '40.8' }, [], {})).toMatchObject({ deck: 40.8 + DECK_DEPTH, source: 'seamark' })
    expect(deckLevel({ 'seamark:bridge:clearance_height': '65 ft' }, [], {}).deck).toBeCloseTo(65 * 0.3048 + DECK_DEPTH)
  })

  it('takes the height of a bridge that stays below its roadway as its deck', () => {
    expect(deckLevel({ height: '64' }, [], {})).toMatchObject({ deck: 64, source: 'height' })
  })

  it('takes the height of a suspension bridge as its deck only where it falls well short of its towers', () => {
    expect(deckLevel({ bridge: 'suspension', height: '41' }, [tower(0, 82.9)], {})).toMatchObject({ deck: 41, suspended: true })
    expect(deckLevel({ bridge: 'suspension', height: '82' }, [tower(0, 82.9)], {}).deck).toBeUndefined()
    expect(deckLevel({ bridge: 'suspension', height: '41' }, [], {}).deck).toBeUndefined()
  })

  it('falls back to a recorded deck height by Wikidata item, before any tag', () => {
    const overrides = { Q1: { deck: 30, note: 'test' } }
    expect(deckLevel({ wikidata: 'Q1', height: '12' }, [], overrides)).toMatchObject({ deck: 30, source: 'override' })
  })

  it('ignores layer and ele, which are not deck heights', () => {
    expect(deckLevel({ layer: '3', ele: '20' }, [], {}).deck).toBeUndefined()
  })
})

describe('a suspended span between its towers', () => {
  const towers = [tower(300), tower(786)]
  const level = { deck: 41, suspended: true, supports: [anchorage(0), ...towers, anchorage(1086)] }
  const points = deck(1086)
  const within = points.map(() => true)

  it('crests at the recorded height midway and falls by its camber to each tower', () => {
    const needs = levelNeeds(points, within, level, -1.6)
    const near = (at: number) => needs.reduce((a, b) => (Math.abs(b.at - at) < Math.abs(a.at - at) ? b : a))
    const span = 486
    expect(near(543).height).toBeCloseTo(41 - 1.6, 0)
    expect(near(300).height).toBeCloseTo(41 - 1.6 - span * CAMBER, 0)
    expect(needs.every(n => n.at >= 290 && n.at <= 796)).toBe(true)
  })

  it('hangs free of piers from anchorage to anchorage', () => {
    const [from, to] = hungStretch(points, within, level)!
    expect(from).toBeLessThan(20)
    expect(to).toBeGreaterThan(1066)
  })

  it('finds towers mapped as a leg either side of the roadway', () => {
    const leg = (east: number, north: number): Support => ({ kind: 'pylon', ring: square(east, 4).map(p => mercator(...lngLatOf(p, north))), height: 84 })
    const legs = [leg(300, 12), leg(300, -12), leg(786, 12), leg(786, -12)]
    expect(hungStretch(points, within, { ...level, supports: [anchorage(0), ...legs, anchorage(1086)] })).not.toBeNull()
  })

  it('stands on piers when it is not hung from towers', () => {
    expect(hungStretch(points, within, { ...level, suspended: false })).toBeNull()
  })
})
