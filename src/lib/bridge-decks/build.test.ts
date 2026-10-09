import { describe, expect, it } from 'bun:test'
import { buildDecks, CLEARANCE, numbered, STEP, type DeckInput } from './build'
import { Dem, demConfigured, sampleHeights, type Heights } from './dem'
import { along, mercator, type Point, type Way } from './profile'

const east = (m: number, north = 0): Point =>
  mercator(-80.83 + m / (111320 * Math.cos((35.22 * Math.PI) / 180)), 35.22 + north / 110574)
const flat = (metres: number): Heights => ({ size: 4, data: new Float32Array(16).fill(metres) })
const ground = (metres: number) => ({ async load() {}, at: () => metres })
const way = (id: number, from: number, to: number, kind: Way['kind'] = 'road'): Way =>
  ({ id, points: [east(from), east(to)], kind, layer: 1, width: 8 })
const key = (p: Point) => p.join(',')
const input = (over: Partial<DeckInput>): DeckInput =>
  ({ ways: [], onGround: new Set(), outlines: [], kerbs: [], crossed: [], water: [], wikidata: new Map(), ...over })

describe('buildDecks', () => {
  it('a bridge between two roads lands on both and clears the road under it', async () => {
    const [deck] = await buildDecks(input({
      ways: [way(7, 0, 150), way(5, 150, 300)],
      onGround: new Set([key(east(0)), key(east(300))]),
      crossed: [{ kind: 'road', points: [east(150, -30), east(150, 30)] }],
    }), ground(200))
    expect(deck.ways).toEqual([7, 5])
    expect(deck.grounded).toEqual([true, true])
    expect(along(deck.points)[1]).toBeCloseTo(STEP, 5)
    expect(deck.heights[0]).toBe(200)
    expect(deck.heights[25]).toBeGreaterThanOrEqual(200 + CLEARANCE.road - 0.5)
    expect(deck.piers.length).toBeGreaterThan(0)
  })

  it('a deck over another clears it, and a ramp rests on the deck it joins', async () => {
    const upper: Way = { id: 3, points: [east(100, -300), east(100, 300)], kind: 'road', layer: 2, width: 8 }
    const ramp: Way = { id: 9, points: [east(60, -80), east(60, -1)], kind: 'road', layer: 1, width: 6 }
    const decks = await buildDecks(input({
      ways: [way(1, 0, 200), upper, ramp],
      onGround: new Set([key(east(0)), key(east(200)), key(east(100, -300)), key(east(100, 300)), key(east(60, -80))]),
      crossed: [{ kind: 'rail', points: [east(30, -30), east(30, 30)] }],
    }), ground(200))
    const low = decks.find(d => d.ways.includes(1))!
    const high = decks.find(d => d.ways.includes(3))!
    const joining = decks.find(d => d.ways.includes(9))!
    const under = low.heights[Math.round(100 / STEP)]
    expect(high.heights[Math.round(300 / STEP)]).toBeGreaterThanOrEqual(under + CLEARANCE.deck - 0.5)
    expect(joining.grounded).toEqual([true, false])
    expect(joining.heights.at(-1)!).toBeGreaterThan(201)
  })

  it('an end rests on the deck it joins, not a lower one it lies over', async () => {
    // A layer-1 road runs north-south on the ground; a layer-2 deck crosses
    // over it, and a layer-2 ramp ends on that deck right above the road.
    const below: Way = { id: 1, points: [east(100, -300), east(100, 300)], kind: 'road', layer: 1, width: 8 }
    const over = (id: number, from: number, to: number): Way => ({ ...way(id, from, to), layer: 2 })
    const ramp: Way = { id: 4, points: [east(40, -80), east(100)], kind: 'road', layer: 2, width: 6 }
    const decks = await buildDecks(input({
      ways: [below, over(2, -100, 100), over(3, 100, 300), ramp],
      onGround: new Set([key(east(100, -300)), key(east(100, 300)), key(east(-100)), key(east(300)), key(east(40, -80))]),
    }), ground(200))
    const low = decks.find(d => d.ways.includes(1))!
    const high = decks.find(d => d.ways.includes(2))!
    const joining = decks.find(d => d.ways.includes(4))!
    const top = high.heights[Math.round(200 / STEP)]
    expect(low.heights[Math.round(300 / STEP)]).toBe(200)
    expect(top).toBeGreaterThanOrEqual(200 + CLEARANCE.deck - 0.5)
    expect(joining.grounded).toEqual([true, false])
    expect(joining.heights.at(-1)!).toBeCloseTo(top, 0)
  })

  it('names a deck by where it lies and a bridge by its outline or wikidata item', async () => {
    const outline = [[east(-5, 10), east(105, 10), east(105, -10), east(-5, -10)]]
    const [a] = await buildDecks(input({ ways: [way(1, 0, 100)], outlines: [{ id: 'way/77', rings: outline }] }), ground(0))
    const [b] = await buildDecks(input({ ways: [way(2, 0, 50), way(4, 50, 100)], outlines: [{ id: 'way/77', rings: outline }] }), ground(0))
    expect(a.id).toBe(b.id)
    expect(a.bridge).toBe('way/77')
    const [c] = await buildDecks(input({ ways: [way(1, 0, 100)], wikidata: new Map([[1, 'Q42']]) }), ground(0))
    expect(c.bridge).toBe('wikidata/Q42')
  })

  it('numbers decks that share a place the same way whatever order a cell builds them in', () => {
    const [a, b, c]: Point[] = [[0.3, 0.4], [0.3, 0.5], [0.2, 0.9]]
    expect(numbered(['x', 'x', 'y'], [a, b, c])).toEqual(['x', 'x/1', 'y'])
    expect(numbered(['y', 'x', 'x'], [c, b, a])).toEqual(['y', 'x/1', 'x'])
  })
})

describe('Dem', () => {
  it('falls back to a parent tile where the source has none', async () => {
    const asked: string[] = []
    const dem = new Dem(async (z, x, y) => {
      asked.push(`${z}/${x}/${y}`)
      return z === 15 ? null : flat(42)
    }, { zoom: 15, minZoom: 12, capacity: 8, concurrency: 2 })
    await dem.load([east(0), east(10)])
    expect(dem.at(east(0))).toBe(42)
    expect(asked.map(k => k.split('/')[0])).toEqual(['15', '14'])
  })

  it('keeps to its capacity as it loads, never dropping a tile the same load needs', async () => {
    let fetched = 0
    const dem = new Dem(async () => (fetched++, flat(7)), { zoom: 15, minZoom: 15, capacity: 2, concurrency: 2 })
    // Three tiles in one load: more than fit, but all are kept until it ends.
    const spread = [east(0), east(2000), east(4000)]
    await dem.load(spread)
    expect(fetched).toBe(3)
    expect(spread.map(p => dem.at(p))).toEqual([7, 7, 7])
    // The next load trims back to capacity, oldest first.
    await dem.load([east(6000)])
    expect(dem.size).toBe(2)
    await dem.load([east(4000)])
    expect(fetched).toBe(4)
    await dem.load([east(0)])
    expect(fetched).toBe(5)
  })

  it('counts a source configured only as a {z}/{x}/{y} template', () => {
    expect(demConfigured('https://tiles.mapterhorn.com/{z}/{x}/{y}.webp')).toBe(true)
    expect(demConfigured('off')).toBe(false)
  })

  it('samples pixel corners as MapLibre does', () => {
    const ramp = { size: 4, data: Float32Array.from({ length: 16 }, (_, i) => (i % 4) * 10) }
    expect(sampleHeights(ramp, 0.5, 0.5)).toBeCloseTo(20)
  })
})
