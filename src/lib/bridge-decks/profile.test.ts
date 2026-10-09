import { describe, expect, it } from 'bun:test'
import { along, chains, crossings, fitEdges, joinNeighbours, MAX_GRADE, mercator, resample, solve, type Chain, type Point, type Way } from './profile'

// Metres east of a point in Charlotte, as mercator.
const origin = mercator(-80.83, 35.22)
const east = (m: number, north = 0): Point => {
  const [lng, lat] = [-80.83 + m / (111320 * Math.cos((35.22 * Math.PI) / 180)), 35.22 + north / 110574]
  return mercator(lng, lat)
}
const way = (id: number, from: number, to: number, layer = 1, kind: Way['kind'] = 'road'): Way =>
  ({ id, points: [east(from), east(to)], kind, layer, width: 10 })

describe('chains', () => {
  it('joins ways that share an end, whichever way they run', () => {
    const [deck] = chains([way(2, 100, 50), way(1, 0, 50), way(3, 100, 150)])
    expect(deck.ways).toEqual([1, 2, 3])
    expect(along(deck.points).at(-1)).toBeCloseTo(150, 0)
  })

  it('carries on through the straightest pair where three ways meet, and never between layers', () => {
    const branch: Way = { ...way(3, 0, 0), points: [east(50), east(80, 40)] }
    const decks = chains([way(1, 0, 50), way(2, 50, 100), branch])
    expect(decks.map(d => d.ways)).toEqual([[1, 2], [3]])
    expect(chains([way(1, 0, 50), way(2, 50, 100, 2)])).toHaveLength(2)
  })
})

describe('resample', () => {
  it('a point every step metres, ending on the last', () => {
    const d = along(resample([east(0), east(23)], 5))
    expect(d.map(v => Math.round(v))).toEqual([0, 5, 10, 15, 20, 23])
  })
})

describe('fitEdges', () => {
  it('each side reaches the outline on that side', () => {
    const [deck] = chains([way(1, 0, 100)])
    const outline = [east(10, 7), east(90, 7), east(90, -4), east(10, -4)]
    const [fitted] = fitEdges([deck], outline)
    const [near, far] = [...fitted.edges].sort((a, b) => a - b)
    expect(near).toBeCloseTo(4, 0)
    expect(far).toBeCloseTo(7, 0)
  })
})

describe('solve', () => {
  const deck = { points: resample([east(0), east(300)], 5), layer: 1 }
  const flat = deck.points.map(() => 100)
  const d = along(deck.points)

  it('runs on the ground with nothing to clear, and lands on both ends', () => {
    expect(solve({ ...deck, grounded: [true, true] }, flat, [])).toEqual(flat)
  })

  it('rises over what it crosses no steeper than a road may', () => {
    const z = solve({ ...deck, grounded: [true, true] }, flat, [{ at: 150, height: 106 }])
    expect(z[0]).toBe(100)
    expect(z.at(-1)).toBe(100)
    expect(z[30]).toBeCloseTo(106, 5)
    for (let i = 1; i < z.length; i++) expect(Math.abs(z[i] - z[i - 1]) / (d[i] - d[i - 1])).toBeLessThanOrEqual(MAX_GRADE + 1e-9)
  })

  it('an end resting on another deck takes its height', () => {
    const z = solve({ ...deck, grounded: [true, false] }, flat, [], [null, 110])
    expect(z.at(-1)).toBe(110)
  })
})

describe('crossings', () => {
  it('where another line passes under, not where one meets an end', () => {
    const line = [east(0), east(100)]
    const at = crossings(line, [east(40, -20), east(40, 20)])
    expect(at).toHaveLength(1)
    expect(at[0]).toBeCloseTo(40, 0)
    expect(crossings(line, [east(0), east(0, 30)])).toEqual([])
  })
})

describe('joinNeighbours', () => {
  const deck = (north: number): Chain =>
    ({ ways: [north], points: resample([east(0, north), east(60, north)], 6), kind: 'road', layer: 1, edges: [5, 5] })

  it('lifts a deck to its twin alongside, but never off the ground where it lands', () => {
    const a = { chain: deck(0), z: deck(0).points.map(() => 100), grounded: [true, true] as [boolean, boolean] }
    const b = { chain: deck(10), z: deck(10).points.map(() => 101) }
    joinNeighbours([a, b])
    expect(a.z[0]).toBe(100)
    expect(a.z.at(-1)).toBe(100)
    expect(a.z.slice(1, -1).every(z => z === 101)).toBe(true)
    expect(b.z.every(z => z === 101)).toBe(true)

    const loose = { chain: deck(0), z: deck(0).points.map(() => 100) }
    joinNeighbours([loose, { chain: deck(10), z: deck(10).points.map(() => 101) }])
    expect(loose.z[0]).toBe(101)
  })

  it('leaves decks too far apart or too far above alone', () => {
    const a = { chain: deck(0), z: deck(0).points.map(() => 100) }
    joinNeighbours([a, { chain: deck(40), z: deck(40).points.map(() => 101) }, { chain: deck(10), z: deck(10).points.map(() => 108) }])
    expect(a.z.every(z => z === 100)).toBe(true)
  })
})
