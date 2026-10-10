import { describe, expect, it } from 'bun:test'
import {
  absorbPaths, along, arch, chains, crossings, fitEdges, joinNeighbours, MAX_GRADE, mercator, resample, smooth, solve, steady, widen,
  type Chain, type Point, type Way,
} from './profile'

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

  it('carries a road on through a junction rather than the ramp that leaves it straighter', () => {
    const road = (w: Way): Way => ({ ...w, road: 'motorway/FDR Drive' })
    const bend: Way = road({ ...way(2, 0, 0), points: [east(50), east(95, 25)] })
    const ramp: Way = { ...way(3, 50, 100), road: 'motorway_link/' }
    const decks = chains([road(way(1, 0, 50)), bend, ramp])
    expect(decks.map(d => d.ways)).toEqual([[1, 2], [3]])
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

  it('leaves the kerbs nearer a twin to the twin', () => {
    // Twin carriageways 8 m apart, 3.5 m kerbs either side of each.
    const twin = (id: number, north: number): Chain =>
      ({ ways: [id], points: [east(0, north), east(100, north)], kind: 'road', layer: 1, edges: [3.5, 3.5] })
    const [a, b] = [twin(1, 0), twin(2, 8)]
    const kerbs = [-3.5, 3.5, 4.5, 11.5].flatMap(north => [east(20, north), east(80, north)])
    const [fitted] = fitEdges([a], kerbs, [a, b])
    expect(Math.max(...fitted.edges)).toBeCloseTo(3.5, 1)
  })
})

describe('absorbPaths', () => {
  it('widens a deck only where a sidewalk runs beside it', () => {
    const road: Chain = { ways: [1], points: resample([east(0), east(600)], 6), kind: 'road', layer: 1, edges: [4, 4] }
    const sidewalk: Chain = { ways: [2], points: [east(0, 6), east(40, 6)], kind: 'path', layer: 1, edges: [1.5, 1.5] }
    const [deck] = absorbPaths([road, sidewalk])
    expect(deck.ways).toEqual([1, 2])
    const wide = widen(deck, 6)
    const left = wide.sides![0][0] > 4 ? wide.sides![0] : wide.sides![1]
    expect(left[3]).toBeCloseTo(7.5, 1)
    expect(left[50]).toBe(4)
    expect(Math.max(...wide.edges)).toBeCloseTo(7.5, 1)
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

  it('keeps an end it cannot reach at a road\'s grade on the straight line to it', () => {
    // A ramp 60 m long up to a deck 20 m above: steeper than MAX_GRADE, so straight.
    const ramp = { points: resample([east(0), east(60)], 6), layer: 1, grounded: [true, false] as [boolean, boolean] }
    const z = solve(ramp, ramp.points.map(() => 100), [], [null, 120])
    expect(z[5]).toBeCloseTo(110, 3)
  })
})

describe('a long viaduct', () => {
  // 1.2 km over flat ground read with lidar's noise, landing at both ends, over a street every 70 m.
  const viaduct = { points: resample([east(0), east(1200)], 6), layer: 1, grounded: [true, true] as [boolean, boolean] }
  const d = along(viaduct.points)
  const ground = viaduct.points.map((_, i) => 100 + 0.15 * Math.sin(i * 2.4))
  const streets = Array.from({ length: 13 }, (_, k) => ({ at: 180 + 70 * k, height: 106 }))
  const z = smooth(solve(viaduct, ground, streets), d, steady(ground, d))

  it('clears every street it crosses', () => {
    // Easing rounds the crest over the first and last at most 20 cm under it, inside the slab allowance CLEARANCE carries.
    for (const { at } of streets) expect(z[Math.round(at / 6)]).toBeGreaterThanOrEqual(106 - 0.2)
  })

  it('runs level between them rather than dipping toward the ground', () => {
    const [first, last] = [Math.round(180 / 6), Math.round(1020 / 6)]
    for (let i = first; i <= last; i++) expect(z[i]).toBeGreaterThanOrEqual(106 - 0.2)
  })

  it('has no waves: its grade changes no faster than a vertical curve allows', () => {
    // Second differences over a 6 m step; 0.05 m is a 720 m radius.
    for (let i = 1; i < z.length - 1; i++) expect(Math.abs(z[i + 1] - 2 * z[i] + z[i - 1])).toBeLessThan(0.05)
  })

  it('climbs from the ground no steeper than a road may', () => {
    for (let i = 1; i < z.length; i++) expect(Math.abs(z[i] - z[i - 1]) / (d[i] - d[i - 1])).toBeLessThanOrEqual(MAX_GRADE + 1e-6)
  })
})

describe('arch', () => {
  it('is the least concave line over every point, through both ends', () => {
    const d = [0, 10, 20, 30, 40]
    expect(arch(d, [0, 5, 1, 5, 0])).toEqual([0, 5, 5, 5, 0])
    expect(arch(d, [0, 1, 4, 1, 0])).toEqual([0, 2, 4, 2, 0])
  })
})

describe('steady', () => {
  it('reads the ground through a lone spike, and keeps a slope', () => {
    const d = [0, 6, 12, 18, 24, 30]
    expect(steady([1, 1, 1, 9, 1, 1], d)[3]).toBe(1)
    expect(steady([0, 1, 2, 3, 4, 5], d)).toEqual([0, 1, 2, 3, 4, 5])
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

  it('never lifts a deck to a ramp that rests on it and climbs away beside it', () => {
    const main = { chain: deck(0), z: deck(0).points.map(() => 100) }
    const ramp = { chain: deck(10), z: deck(10).points.map(() => 101), on: [{ deck: main, at: deck(10).points[0] }] }
    joinNeighbours([main, ramp])
    expect(main.z.every(z => z === 100)).toBe(true)
  })

  it('leaves decks too far apart or too far above alone', () => {
    const a = { chain: deck(0), z: deck(0).points.map(() => 100) }
    joinNeighbours([a, { chain: deck(40), z: deck(40).points.map(() => 101) }, { chain: deck(10), z: deck(10).points.map(() => 108) }])
    expect(a.z.every(z => z === 100)).toBe(true)
  })
})
