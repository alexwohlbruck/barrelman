import { describe, expect, it } from 'bun:test'
import { along, anchor, chains, fitEdges, LAYER_CLEARANCE, MAX_GRADE, mercator, resample, solve, type Point, type Way } from './profile'

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

  it('stops at a node where three bridge ways meet, and between layers', () => {
    expect(chains([way(1, 0, 50), way(2, 50, 100), { ...way(3, 50, 0), points: [east(50), east(50, 40)] }])).toHaveLength(3)
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
  const deck = { points: resample([east(0), east(200)], 5), layer: 1 }
  const flat = deck.points.map(() => 100)

  it('lands on the ground at grounded ends and clears it between', () => {
    const z = solve({ ...deck, grounded: [true, true] }, flat)
    expect(z[0]).toBe(100)
    expect(z.at(-1)).toBe(100)
    expect(z[20]).toBeCloseTo(100 + LAYER_CLEARANCE, 3)
    const d = along(deck.points)
    for (let i = 1; i < z.length; i++) expect(Math.abs(z[i] - z[i - 1]) / (d[i] - d[i - 1])).toBeLessThanOrEqual(MAX_GRADE + 1e-9)
  })

  it('an end resting on another deck takes its height', () => {
    const z = solve({ ...deck, grounded: [true, false] }, flat, [null, 110])
    expect(z.at(-1)).toBe(110)
  })
})

describe('anchor', () => {
  it('pulls the profile through a mapped height and lets go at grounded ends', () => {
    const d = [0, 50, 100, 150, 200]
    const z = [100, 103, 106, 103, 100]
    const out = anchor(z, d, d.map(() => 100), [{ at: 100, ele: 110 }], [true, true])
    expect(out).toEqual([100, 105, 110, 105, 100])
  })
})
