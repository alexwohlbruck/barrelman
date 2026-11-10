import { describe, expect, it } from 'bun:test'
import {
  cambered,
  fair,
  VERTICAL_CURVE,
  PINNED,
  absorbPaths, absorbTracks, along, beside, clearOfWalkways, closeVerges, detab, GORE_GAP, holdGores, landing, LANDING, arch, chains, crossings, fitEdges, joinNeighbours, MAX_GRADE, mercator, paveGores, pinned, resample, sideAt, smooth, solve, steady, widen,
  type Chain, type Pin, type Point, type Way,
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

  it('carries on through the straightest pair where three ways meet, and never between layers there', () => {
    const branch: Way = { ...way(3, 0, 0), points: [east(50), east(80, 40)] }
    const decks = chains([way(1, 0, 50), way(2, 50, 100), branch])
    expect(decks.map(d => d.ways)).toEqual([[1, 2], [3]])
    expect(chains([way(1, 0, 50), way(2, 50, 100, 2), branch]).map(d => d.ways)).toEqual([[1, 3], [2]])
  })

  it('carries one deck on where two ways meet alone and one has no layer', () => {
    const [deck] = chains([{ ...way(1, 0, 50), untagged: true }, way(2, 50, 100, 3)])
    expect(deck.ways).toEqual([1, 2])
    expect(deck.layer).toBe(3)
    expect(chains([way(1, 0, 50), way(2, 50, 100, 3)])).toHaveLength(2)
  })

  it('carries a road on through a junction rather than the ramp that leaves it straighter', () => {
    const road = (w: Way): Way => ({ ...w, road: 'motorway/FDR Drive' })
    const bend: Way = road({ ...way(2, 0, 0), points: [east(50), east(95, 25)] })
    const ramp: Way = { ...way(3, 50, 100), road: 'motorway_link/' }
    const decks = chains([road(way(1, 0, 50)), bend, ramp])
    expect(decks.map(d => d.ways)).toEqual([[1, 2], [3]])
  })

  it('breaks a tie between pairs the same way whatever order the ways come in', () => {
    // A crossroads: east-west and north-south run equally straight through it.
    const arm = (id: number, to: Point): Way => ({ ...way(id, 0, 0), points: [east(50), to] })
    const ways = [arm(1, east(100)), arm(2, east(0)), arm(3, east(50, 50)), arm(4, east(50, -50))]
    const members = (w: Way[]) => chains(w).map(d => [...d.ways].sort()).sort((a, b) => a[0] - b[0])
    expect(members([...ways].reverse())).toEqual(members(ways))
  })

  it('never folds a carriageway back onto the slip road it meets at a gore', () => {
    // Both bridge ways leave the gore heading west; the road beyond it is on the ground.
    const main: Way = { ...way(1, 0, 0), points: [east(60), east(0)], road: 'secondary/North Davidson Street' }
    const slip: Way = { ...way(2, 0, 0), points: [east(60), east(0, 12)], road: 'secondary_link/' }
    expect(chains([main, slip]).map(d => d.ways)).toEqual([[1], [2]])
  })

  it('carries the pins of its ways\' nodes, a node two ways share once', () => {
    const [chain] = chains([{ ...way(1, 0, 100), pins: [{ vertex: 1, height: 12 }] }, { ...way(2, 100, 200), pins: [{ vertex: 0, height: 12 }, { vertex: 1, height: 9 }] }])
    expect(chain.pins).toEqual([{ point: east(100), height: 12 }, { point: east(200), height: 9 }])
  })

  it('never chains two tracks parting at a turnout', () => {
    const main: Way = { ...way(1, 0, 0, 1, 'rail'), points: [east(60), east(0)] }
    const branch: Way = { ...way(2, 0, 0, 1, 'rail'), points: [east(60), east(0, 6)] }
    expect(chains([main, branch])).toHaveLength(2)
  })

  it('still chains a footbridge that doubles back at a landing', () => {
    const up: Way = { ...way(1, 0, 0, 1, 'path'), points: [east(0), east(40)], road: 'footway/' }
    const back: Way = { ...way(2, 0, 0, 1, 'path'), points: [east(40), east(0, 4)], road: 'steps/' }
    expect(chains([up, back]).map(d => d.ways)).toEqual([[1, 2]])
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

  it('keeps a walkway of its own that runs apart from the road, rather than widen the road out to it', () => {
    const road: Chain = { ways: [1], points: resample([east(0), east(600)], 6), kind: 'road', layer: 1, edges: [4, 4] }
    const walkway: Chain = { ways: [2], points: [east(0, 11), east(40, 11)], kind: 'path', layer: 1, edges: [1.5, 1.5] }
    expect(absorbPaths([road, walkway])).toHaveLength(2)
  })
})

describe('absorbTracks', () => {
  const road: Chain = { ways: [1], points: resample([east(0), east(80)], 6), kind: 'road', layer: 1, edges: [6, 6] }
  const track = (north: number, from = 0, to = 80, layer = 1): Chain =>
    ({ ways: [2], points: [east(from, north), east(to, north)], kind: 'rail', layer, edges: [2.5, 2.5] })

  it('folds a tram running in the carriageway into the road deck', () => {
    const decks = absorbTracks([road, track(2)])
    expect(decks).toHaveLength(1)
    expect(decks[0].ways).toEqual([1, 2])
    expect(decks[0].edges).toEqual([6, 6])
  })

  it('folds in a tram whose way runs a little past the road\'s', () => {
    expect(absorbTracks([road, track(2, -8, 88)])).toHaveLength(1)
  })

  it('keeps a track of its own beside the road, on another layer, or running far past it', () => {
    expect(absorbTracks([road, track(9)])).toHaveLength(2)
    expect(absorbTracks([road, track(2, 0, 80, 2)])).toHaveLength(2)
    expect(absorbTracks([road, track(2, 0, 120)])).toHaveLength(2)
  })
})

describe('landing', () => {
  const road = (f: (s: number) => number) => Array.from({ length: LANDING.reach / LANDING.step }, (_, j) => f((j + 1) * LANDING.step))

  it('finds the road beyond a hollow the terrain leaves at a bridge end', () => {
    // E 7th St over I-277: the way ends 7 m down in the hollow, the road runs on 12 m out.
    const land = landing(road(s => (s < 12 ? 198.5 + s * 0.5 : 205.3)), 198.5)
    expect(land?.height).toBeCloseTo(205.3, 1)
    expect(land?.run).toBeLessThanOrEqual(14)
  })

  it('never takes a road climbing steeper than a deck may for a hollow', () => {
    for (const grade of [0.07, 0.1, 0.15]) expect(landing(road(s => 200 + s * grade), 200)).toBeNull()
  })

  it('leaves an end alone where the road beyond carries on at its height, or a long way down', () => {
    expect(landing(road(s => 200 + s * 0.03), 200)).toBeNull()
    expect(landing(road(s => (s < 30 ? 195 : 205)), 195)).toBeNull()
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

/**
 * The San Diego–Coronado Bridge, as USGS lidar has its roadway: a straight
 * grade up from the Coronado end, one parabolic vertical curve over the
 * crest, a straight grade down to the San Diego end. The tangents and curve
 * are the ones that run through the ends and the three heights tagged on its
 * nodes: 4.69 % up to 1052 m, an 857 m curve cresting at 75.1 m, 5.02 % down.
 */
const CORONADO = { length: 2585, ends: [16, 30], up: 0.0469091, down: 0.0502002, from: 1051.872, curve: 857.198 }
const coronado = (s: number) => {
  const { ends, up, down, from, curve } = CORONADO
  const u = Math.min(Math.max(s, from), from + curve) - from
  const z = ends[0] + up * from + up * u - ((up + down) / (2 * curve)) * u * u
  return s < from ? ends[0] + up * s : s > from + curve ? z - down * (s - from - curve) : z
}
const CORONADO_PINS: Pin[] = [{ at: 1150, height: 69.4 }, { at: 1435, height: 75.0 }, { at: 1740, height: 70.8 }]

describe('a deck pinned by its nodes\' ele', () => {
  const deck = { points: resample([east(0), east(CORONADO.length)], 6), layer: 1, grounded: [true, true] as [boolean, boolean] }
  const d = along(deck.points)
  // The bay between its landfalls, which the solver clears by 4 m and no more.
  const bay = d.map(s => (s < 40 ? CORONADO.ends[0] : s > CORONADO.length - 40 ? CORONADO.ends[1] : 0))
  const water = d.flatMap((s, i) => (bay[i] === 0 ? [{ at: s, height: 4 }] : []))

  it('reproduces the San Diego–Coronado Bridge\'s grades and crest curve from three pins and its grounded ends', () => {
    const z = solve(deck, bay, water, [null, null], CORONADO_PINS)
    for (const [i, s] of d.entries()) expect(Math.abs(z[i] - coronado(s))).toBeLessThan(0.3)
    expect(z[0]).toBe(16)
    expect(z.at(-1)).toBe(30)
  })

  it('without them, it is drawn low over the bay', () => {
    const z = solve(deck, bay, water)
    expect(Math.max(...z)).toBeLessThan(40)
  })

  it('runs on a straight grade from each grounded end to a single pin', () => {
    const z = solve(deck, bay, water, [null, null], [{ at: 1435, height: 75 }])
    const line = (s: number) => (s <= 1435 ? 16 + ((75 - 16) * s) / 1435 : 75 + ((30 - 75) * (s - 1435)) / (CORONADO.length - 1435))
    for (const [i, s] of d.entries()) expect(z[i]).toBeCloseTo(line(s), 3)
  })

  it('takes a pin at an end as that end\'s height, over the ground there', () => {
    const z = solve(deck, bay, water, [null, null], [{ at: 0.5, height: 18 }, { at: 1435, height: 75 }])
    expect(z[0]).toBe(18)
  })

  it('clears what it crosses between a pin and an end, and holds the pin', () => {
    const z = solve(deck, bay, [...water, { at: 300, height: 32 }], [null, null], [{ at: 1435, height: 75 }])
    expect(z[50]).toBeGreaterThanOrEqual(32 - 1e-9)
    expect(z[0]).toBe(16)
    const i = d.findIndex(s => s >= 1435)
    expect(z[i]).toBeCloseTo(75 + ((30 - 75) * (d[i] - 1435)) / (CORONADO.length - 1435), 3)
  })

  it('is never cut to what MAX_GRADE reaches from its ends', () => {
    // 30 m up within 200 m of the end: steeper than a deck climbs by rule.
    const z = solve(deck, bay, water, [null, null], [{ at: 200, height: 46 }])
    expect(z[d.findIndex(s => s >= 200) - 1]).toBeGreaterThan(45)
  })
})

describe('pinned', () => {
  const d = Array.from({ length: 101 }, (_, i) => i * 10)

  it('runs straight through pins in a line', () => {
    const z = pinned(d, [{ at: 200, height: 12 }, { at: 500, height: 18 }, { at: 800, height: 24 }], [8, 28])
    for (const [i, s] of d.entries()) expect(z[i]).toBeCloseTo(8 + s / 50, 9)
  })

  it('carries an end that is held by nothing on at the grade it leaves its last pin, no steeper than a deck climbs', () => {
    const z = pinned(d, [{ at: 200, height: 12 }, { at: 500, height: 18 }], [null, null])
    expect(z[0]).toBeCloseTo(8, 9)
    expect(z.at(-1)).toBeCloseTo(28, 9)
    const steep = pinned(d, [{ at: 400, height: 40 }, { at: 500, height: 50 }], [null, null])
    expect(steep.at(-1)! - 50).toBeCloseTo(MAX_GRADE * 500, 9)
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

  it('never lifts a roadway to a walkway beside it, which may climb away over it', () => {
    const road = { chain: deck(0), z: deck(0).points.map(() => 100) }
    const walkway = { chain: { ...deck(7), kind: 'path' as const, edges: [1.5, 1.5] as [number, number] }, z: deck(7).points.map(() => 101) }
    joinNeighbours([road, walkway])
    expect(road.z.every(z => z === 100)).toBe(true)
  })

  it('joins twin tracks, but never a track to a road', () => {
    const rail = (north: number): Chain => ({ ...deck(north), kind: 'rail', edges: [2.5, 2.5] })
    const a = { chain: rail(0), z: rail(0).points.map(() => 100) }
    const b = { chain: rail(4), z: rail(4).points.map(() => 100.5) }
    joinNeighbours([a, b])
    expect(a.z.every(z => z === 100.5)).toBe(true)
    const road = { chain: { ...deck(7.5), edges: [2.5, 2.5] as [number, number] }, z: deck(7.5).points.map(() => 101) }
    joinNeighbours([b, road])
    expect(b.z.every(z => z === 100.5)).toBe(true)
  })

  it('leaves decks too far apart or too far above alone', () => {
    const a = { chain: deck(0), z: deck(0).points.map(() => 100) }
    joinNeighbours([a, { chain: deck(40), z: deck(40).points.map(() => 101) }, { chain: deck(10), z: deck(10).points.map(() => 108) }])
    expect(a.z.every(z => z === 100)).toBe(true)
  })
})

describe('paveGores', () => {
  // A main line running east, and a ramp leaving it at its start, bearing away 1 in 6.
  const main = (): Chain => ({ ways: [1], points: resample([east(0), east(90)], 6), kind: 'road', layer: 1, edges: [4, 4] })
  const ramp = (): Chain => ({ ways: [2], points: resample([east(0), east(90, 15)], 6), kind: 'road', layer: 1, edges: [4, 4] })
  const gapAt = (a: Chain, b: Chain, i: number) => {
    const near = beside(b.points, a.points[i])
    const facing = beside(a.points, b.points[near.segment]).left ? 0 : 1
    return near.distance - sideAt(a, facing, i) - sideAt(b, near.left ? 0 : 1, near.segment - 1, near.t)
  }

  it('closes the ground between a ramp and the deck it leaves, up to the gore nose', () => {
    const [m, r] = [main(), ramp()]
    const open = r.points.map((_, i) => gapAt(r, m, i))
    const M = { chain: m, z: m.points.map(() => 100) }
    paveGores([M, { chain: r, z: r.points.map(() => 100), on: [{ deck: M, at: r.points[0] }] }])
    r.points.forEach((_, i) => {
      if (open[i] > 0.5 && open[i] <= GORE_GAP) expect(Math.abs(gapAt(r, m, i))).toBeLessThan(0.6)
      if (open[i] > GORE_GAP + 2) expect(gapAt(r, m, i)).toBeCloseTo(open[i], 1)
    })
    expect(open.some(g => g > 0.5 && g <= GORE_GAP)).toBe(true)
  })

  it('leaves the ground between a ramp and a deck on two bridges of their own', () => {
    const [m, r] = [{ ...main(), outline: 'way/1' }, { ...ramp(), outline: 'way/2' }]
    const M = { chain: m, z: m.points.map(() => 100) }
    paveGores([M, { chain: r, z: r.points.map(() => 100), on: [{ deck: M, at: r.points[0] }] }])
    expect(r.sides ?? null).toBeNull()
  })

  it('leaves a ramp that has already climbed away from the deck it leaves', () => {
    const [m, r] = [main(), ramp()]
    const M = { chain: m, z: m.points.map(() => 100) }
    paveGores([M, { chain: r, z: r.points.map((_, i) => 100 + i), on: [{ deck: M, at: r.points[0] }] }])
    expect(r.sides ?? null).toBeNull()
    expect(m.sides ?? null).toBeNull()
  })
})

describe('holdGores', () => {
  const main = (): Chain => ({ ways: [1], points: resample([east(0), east(120)], 6), kind: 'road', layer: 1, edges: [4, 4] })
  const ramp = (): Chain => ({ ways: [2], points: resample([east(0), east(120, 40)], 6), kind: 'road', layer: 1, edges: [4, 4] })
  const pair = (grade: number) => {
    const [m, r] = [main(), ramp()]
    const d = along(r.points)
    const M = { chain: m, z: m.points.map(() => 100) }
    const R = { chain: r, z: d.map(x => 100 + x * grade), on: [{ deck: M, at: r.points[0] }] }
    return { M, R, d, own: R.z.slice() }
  }

  it('holds a climbing ramp level with the deck it leaves to the gore nose, then eases it back onto its own profile', () => {
    const { M, R, d, own } = pair(0.03)
    holdGores([M, R])
    expect(R.z[1]).toBeCloseTo(100, 1)
    expect(R.z[1]).toBeLessThan(own[1] - 0.1)
    for (let i = 1; i < d.length; i++) expect(R.z[i] - R.z[i - 1]).toBeLessThanOrEqual(MAX_GRADE * (d[i] - d[i - 1]) + 1e-6)
    expect(R.z.at(-1)).toBe(own.at(-1))
  })

  it('makes no hold a ramp could not climb back from, nor one leaving it short of what it must clear', () => {
    const steep = pair(0.1)
    holdGores([steep.M, steep.R])
    expect(steep.R.z).toEqual(steep.own)
    const cleared = pair(0.03)
    holdGores([cleared.M, Object.assign(cleared.R, { needs: [{ at: 12, height: 100.3 }] })])
    expect(cleared.R.z).toEqual(cleared.own)
  })
})

describe('paveGores between two ramps', () => {
  it('paves the gore between two ramps leaving one deck at the same place', () => {
    const trunk: Chain = { ways: [1], points: resample([east(-60), east(0)], 6), kind: 'road', layer: 1, edges: [4, 4] }
    const left: Chain = { ways: [2], points: resample([east(0), east(90, 14)], 6), kind: 'road', layer: 1, edges: [4, 4] }
    const right: Chain = { ways: [3], points: resample([east(0), east(90, -14)], 6), kind: 'road', layer: 1, edges: [4, 4] }
    const T = { chain: trunk, z: trunk.points.map(() => 100) }
    const L = { chain: left, z: left.points.map(() => 100), on: [{ deck: T, at: left.points[0] }] }
    const R = { chain: right, z: right.points.map(() => 100), on: [{ deck: T, at: right.points[0] }] }
    paveGores([T, L, R])
    expect(Math.max(...(left.sides ?? [[0], [0]]).flat())).toBeGreaterThan(4)
    expect(Math.max(...(right.sides ?? [[0], [0]]).flat())).toBeGreaterThan(4)
  })
})

describe('closeVerges', () => {
  const road: Chain = { ways: [1], points: resample([east(0), east(60)], 6), kind: 'road', layer: 1, edges: [4, 4] }
  // A sidewalk 1.5 m either side of its line, 7.5 m from the road's: a 2 m verge between.
  const walk = (): Chain => ({ ways: [2], points: resample([east(0, 7.5), east(60, 7.5)], 6), kind: 'path', layer: 1, edges: [1.5, 1.5] })

  it('widens a sidewalk deck across the verge to the road deck beside it on one bridge', () => {
    const w = walk()
    closeVerges([{ chain: road, z: road.points.map(() => 100) }, { chain: w, z: w.points.map(() => 100) }], () => true)
    expect(Math.abs(Math.max(...w.edges) - 3.5)).toBeLessThan(0.1)
  })

  it('leaves one on another bridge, or at another height', () => {
    const [a, b] = [walk(), walk()]
    closeVerges([{ chain: road, z: road.points.map(() => 100) }, { chain: a, z: a.points.map(() => 100) }], () => false)
    closeVerges([{ chain: road, z: road.points.map(() => 100) }, { chain: b, z: b.points.map(() => 103) }], () => true)
    expect(a.sides ?? null).toBeNull()
    expect(b.sides ?? null).toBeNull()
  })
})

describe('gores between ramps', () => {
  const main = (): Chain => ({ ways: [1], points: resample([east(0), east(120)], 6), kind: 'road', layer: 1, edges: [4, 4] })
  const off = (north: number, ways: number): Chain => ({ ways: [ways], points: resample([east(30), east(120, north)], 6), kind: 'road', layer: 1, edges: [4, 4] })

  it('never pairs ramps leaving a deck off its two sides', () => {
    const [m, l, r] = [main(), off(15, 2), off(-15, 3)]
    const M = { chain: m, z: m.points.map(() => 100) }
    const L = { chain: l, z: l.points.map(() => 100), on: [{ deck: M, at: l.points[0] }] }
    const alone = off(15, 2)
    paveGores([M, L, { chain: r, z: r.points.map(() => 100), on: [{ deck: M, at: r.points[0] }] }])
    const M2 = { chain: main(), z: m.points.map(() => 100) }
    paveGores([M2, { chain: alone, z: alone.points.map(() => 100), on: [{ deck: M2, at: alone.points[0] }] }])
    expect(l.sides ?? null).toEqual(alone.sides ?? null)
  })

  it('holds the ramps of a split together or not at all', () => {
    // Two ramps leaving the same side of one deck: the steep one cannot be held, so neither is.
    const m = main()
    const M = { chain: m, z: m.points.map(() => 100) }
    const [a, b] = [off(30, 2), off(24, 3)]
    const da = along(a.points), db = along(b.points)
    const A = { chain: a, z: da.map(x => 100 + x * 0.1), on: [{ deck: M, at: a.points[0] }] }
    const B = { chain: b, z: db.map(x => 100 + x * 0.02), on: [{ deck: M, at: b.points[0] }] }
    const [za, zb] = [A.z.slice(), B.z.slice()]
    holdGores([M, A, B])
    expect(A.z).toEqual(za)
    expect(B.z).toEqual(zb)
  })
})

describe('cambered', () => {
  const d = Array.from({ length: 41 }, (_, i) => i * 6)
  const z = d.map(() => 30)
  const camber = (from: number, to: number, height: number) => d.filter(x => x >= from && x <= to).map(at => ({ at, height, exact: true }))

  it('holds a suspended span to its camber and fades back to its own grade line beyond', () => {
    const out = cambered(z, d, camber(60, 180, 28))
    expect(out[d.indexOf(120)]).toBe(28)
    expect(out[0]).toBeCloseTo(30)
    expect(out[d.length - 1]).toBeCloseTo(30)
  })

  it('never holds a span below something else it must clear there', () => {
    const out = cambered(z, d, [...camber(60, 180, 28), { at: 120, height: 29 }])
    expect(out[d.indexOf(120)]).toBe(29)
  })
})

describe('fair', () => {
  const d = Array.from({ length: 101 }, (_, i) => i * 6)
  const crest = d.map(s => (s < 300 ? 0.04 * s : 12 - 0.04 * (s - 300)))
  const bend = (z: number[]) => Math.max(...z.slice(1, -1).map((v, k) => Math.abs(z[k + 2] - 2 * v + z[k]) / 36))

  it('eases a change of grade into a parabolic vertical curve VERTICAL_CURVE long', () => {
    expect(bend(fair(crest, d))).toBeCloseTo(0.08 / VERTICAL_CURVE, 4)
  })

  it('takes out a lone bump and leaves a parabola, as a camber, as it is', () => {
    const grade = d.map(s => 10 + 0.01 * s)
    const bumped = fair(grade.map((z, i) => z + (i === 50 ? 0.4 : 0)), d)
    expect(Math.max(...bumped.map((z, i) => Math.abs(z - grade[i])))).toBeLessThan(0.05)
    const camber = d.map(s => 41 - 4.85 * (s / 300 - 1) ** 2)
    expect(fair(camber, d).slice(25, 76).every((z, i) => Math.abs(z - camber[i + 25]) < 0.01)).toBe(true)
  })

  it('keeps over a floor it must clear and under a ceiling, and holds a pinned end', () => {
    // Level over what it crosses, as `solve` leaves it.
    const level = d.map(s => Math.min(12, 12.48 - 0.04 * Math.abs(s - 300)))
    const floor = d.map(s => (Math.abs(s - 300) <= 12 ? 12 : -Infinity))
    const raised = fair(level, d, { floor, weight: d.map((_, i) => (i ? 1 : PINNED)) })
    expect(Math.min(...raised.map((z, i) => z - floor[i]))).toBeGreaterThan(-0.01)
    expect(raised[0]).toBeCloseTo(level[0], 3)
    const sag = level.map(z => 12 - z)
    const ceiling = d.map(s => (Math.abs(s - 300) <= 12 ? 0 : Infinity))
    expect(Math.max(...fair(sag, d, { ceiling }).map((z, i) => z - ceiling[i]))).toBeLessThan(0.01)
  })
})

describe('detab', () => {
  it('puts a short tab back to the width around it, but keeps a longer widening and a flared end', () => {
    const side = [5, 5, 5, 5, 5, 5, 13, 13, 5, 5, 5, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 12]
    const out = detab(side)
    expect(out.slice(6, 8)).toEqual([5, 5])
    expect(out.slice(16, 26).every(v => v === 9)).toBe(true)
    expect(out.at(-1)).toBe(12)
  })
})

describe('clearOfWalkways', () => {
  type Deck = Pick<Chain, 'points' | 'kind' | 'layer' | 'edges' | 'sides'> & { lanes?: [number, number] }
  const road = (layer = 1): Deck => ({ points: resample([east(0), east(120)], 6), kind: 'road', layer, edges: [9, 9], lanes: [5, 5] })
  const walkway = (north: number, layer = 2): Deck => ({ points: resample([east(30, north), east(90, north)], 6), kind: 'path', layer, edges: [1.5, 1.5] })

  it('draws a roadway in short of a walkway alongside it, easing off either side, never inside its lanes', () => {
    const [deck] = clearOfWalkways([road(), walkway(-8)])
    const left = deck.sides![1][10] < 9 ? deck.sides![1] : deck.sides![0]
    expect(left[10]).toBeCloseTo(6.5, 0)
    expect(left[0]).toBe(9)
    expect(Math.min(...left)).toBeGreaterThanOrEqual(5)
    const [tight] = clearOfWalkways([road(), walkway(-4)])
    expect(Math.min(...tight.sides![0], ...tight.sides![1])).toBe(5)
  })

  it('leaves a roadway alone over a walkway on a lower layer', () => {
    const [deck] = clearOfWalkways([road(2), walkway(-8, 1)])
    expect(deck.sides).toBeUndefined()
  })
})
