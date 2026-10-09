import { describe, expect, it } from 'bun:test'
import { buildDecks, STEP } from './build'
import { Dem, sampleHeights, type Heights } from './dem'
import { toEgm96 } from './geoid'
import { along, LAYER_CLEARANCE, mercator, type Point, type Way } from './profile'

const east = (m: number, north = 0): Point =>
  mercator(-80.83 + m / (111320 * Math.cos((35.22 * Math.PI) / 180)), 35.22 + north / 110574)
const flat = (metres: number): Heights => ({ size: 4, data: new Float32Array(16).fill(metres) })
const ground = (metres: number) => ({ async load() {}, at: () => metres })
const way = (id: number, from: number, to: number, kind: Way['kind'] = 'road'): Way =>
  ({ id, points: [east(from), east(to)], kind, layer: 1, width: 8 })
const key = (p: Point) => p.join(',')

describe('buildDecks', () => {
  it('a bridge between two roads lands on both and clears what is under it', async () => {
    const ways = [way(7, 0, 150), way(5, 150, 300)]
    const [deck] = await buildDecks(
      { ways, onGround: new Set([key(east(0)), key(east(300))]), outlines: [], kerbs: [], anchors: [] },
      ground(200),
      () => 0,
    )
    expect(deck.id).toBe('way/5')
    expect(deck.ways).toEqual([7, 5])
    expect(deck.grounded).toEqual([true, true])
    expect(along(deck.points)[1]).toBeCloseTo(STEP, 5)
    expect(deck.heights[0]).toBe(200)
    expect(Math.max(...deck.heights)).toBeCloseTo(200 + LAYER_CLEARANCE, 0)
  })

  it('a ramp rests on the deck it joins, and heights come out on EGM96', async () => {
    const ramp: Way = { id: 9, points: [east(60, -80), east(60, -1)], kind: 'road', layer: 1, width: 6 }
    const decks = await buildDecks(
      { ways: [way(1, 0, 120), ramp], onGround: new Set([key(east(0)), key(east(120)), key(east(60, -80))]), outlines: [], kerbs: [], anchors: [] },
      ground(200),
      () => -0.5,
    )
    const main = decks.find(d => d.id === 'way/1')!
    const joining = decks.find(d => d.id === 'way/9')!
    expect(joining.grounded).toEqual([true, false])
    expect(joining.heights.at(-1)!).toBeCloseTo(main.heights[Math.round(main.heights.length / 2)], 0)
    expect(main.ground[0]).toBe(199.5)
  })

  it('takes a mapped deck height where the bridge carries one', async () => {
    const [deck] = await buildDecks(
      { ways: [way(1, 0, 200)], onGround: new Set([key(east(0)), key(east(200))]), outlines: [], kerbs: [], anchors: [{ way: 1, point: east(100), ele: 215 }] },
      ground(200),
      () => 0,
    )
    expect(Math.max(...deck.heights)).toBeCloseTo(215, 0)
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

  it('samples pixel corners as MapLibre does', () => {
    const ramp = { size: 4, data: Float32Array.from({ length: 16 }, (_, i) => (i % 4) * 10) }
    expect(sampleHeights(ramp, 0.5, 0.5)).toBeCloseTo(20)
  })
})

describe('toEgm96', () => {
  it('moves NAVD88 in the contiguous US and EGM2008 elsewhere', () => {
    expect(toEgm96(-80.83, 35.22)).toBeCloseTo(-0.15, 1)
    expect(Math.abs(toEgm96(2.35, 48.85))).toBeLessThan(3)
    expect(toEgm96(2.35, 48.85)).not.toBe(0)
  })
})
