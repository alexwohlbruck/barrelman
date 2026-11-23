import { describe, expect, it } from 'bun:test'
import { buildDecks, CLEARANCE, numbered, STEP, TRACK_DEPTH, type DeckInput } from './build'
import { Dem, demConfigured, sampleHeights, type Heights } from './dem'
import { DECK_DEPTH } from './elevation'
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

  it('carries a track higher over a street than a road deck, by its girders\' depth', async () => {
    const over = (kind: Way['kind']) => buildDecks(input({
      ways: [way(7, 0, 300, kind)],
      onGround: new Set([key(east(0)), key(east(300))]),
      crossed: [{ kind: 'road', points: [east(150, -30), east(150, 30)] }],
    }), ground(200)).then(([deck]) => deck.heights[25])
    expect(await over('rail')).toBeCloseTo((await over('road')) + TRACK_DEPTH, 0)
  })

  it('piers keep off the ways and carriageways below', async () => {
    const [deck] = await buildDecks(input({
      ways: [way(7, 0, 300)],
      onGround: new Set([key(east(0)), key(east(300))]),
      crossed: [{ kind: 'road', points: [east(135, -30), east(135, 30)], half: 4 }, { kind: 'rail', points: [east(75, -30), east(75, 30)] }],
      surfaces: [[[east(193, -20), east(197, -20), east(197, 20), east(193, 20), east(193, -20)]]],
    }), ground(200))
    expect(deck.piers.length).toBeGreaterThan(3)
    for (const at of deck.piers) {
      expect(Math.abs(at - 135)).toBeGreaterThan(4 + 1)
      expect(Math.abs(at - 75)).toBeGreaterThan(2 + 2.5)
      expect(at < 193 - 1 || at > 197 + 1).toBe(true)
    }
  })

  it('runs on over a hollow at its end only where the road beyond reaches no junction first', async () => {
    const hollow = { async load() {}, at: ([x]: Point) => (x > east(97)[0] && x < east(112)[0] ? 193 : 200) }
    const build = (cross: boolean) => buildDecks(input({
      ways: [way(1, 0, 100), { id: 2, points: [east(100), east(114)], kind: 'road', layer: 0, width: 8 }],
      onGround: new Set([key(east(0)), key(east(100))]),
      crossed: [
        { kind: 'road', points: [east(-60), east(0)] },
        { kind: 'road', points: [east(100), east(114)] },
        { kind: 'road', points: [east(114), east(160)] },
        ...(cross ? [{ kind: 'road' as const, points: [east(114, -40), east(114), east(114, 40)] }] : []),
      ],
    }), hollow)
    const runOn = (deck: { caps: number[] | null }) => -Math.min(...(deck.caps ?? [0, 0, 0, 0]).slice(2))
    const [open] = (await build(false)).filter(d => d.ways.includes(1))
    const [junction] = (await build(true)).filter(d => d.ways.includes(1))
    expect(runOn(open)).toBeGreaterThan(10)
    expect(runOn(junction)).toBeLessThan(1)
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

  it('a ramp merging alongside a main line takes its height along the merge; one meeting it square keeps its own grade', async () => {
    const main: Way = { id: 1, points: [east(0), east(400)], kind: 'road', layer: 1, width: 14 }
    // Runs in beside the main line's carriageway for its last 60 m, ending on it.
    const merging: Way = { id: 2, points: [east(60, -40), east(140, -4), east(200, -3)], kind: 'road', layer: 1, width: 4 }
    const square: Way = { id: 3, points: [east(300, -120), east(300, -1)], kind: 'road', layer: 1, width: 6 }
    const decks = await buildDecks(input({
      ways: [main, merging, square],
      onGround: new Set([key(east(0)), key(east(400)), key(east(60, -40)), key(east(300, -120))]),
      crossed: [{ kind: 'road', points: [east(200, -60), east(200, 60)] }],
    }), ground(200))
    const [m, r, t] = [1, 2, 3].map(w => decks.find(d => d.ways.includes(w))!)
    const zAt = (d: typeof m, p: Point) => { const k = d.points.reduce((b, q, i) => (Math.hypot(q[0] - p[0], q[1] - p[1]) < Math.hypot(d.points[b][0] - p[0], d.points[b][1] - p[1]) ? i : b), 0); return d.heights[k] }
    for (const x of [160, 180]) expect(Math.abs(zAt(r, east(x, -3.5)) - zAt(m, east(x)))).toBeLessThan(0.15)
    // Square to the main line the ramp is nowhere within it but its end, and climbs straight to it.
    const climb = t.heights.slice(1).map((z, i) => z - t.heights[i])
    expect(Math.max(...climb) - Math.min(...climb.slice(2, -2))).toBeLessThan(0.5)
  })

  it('leaves out a scrap of footbridge meeting nothing at either end, but keeps a short road bridge', async () => {
    const decks = await buildDecks(input({
      ways: [{ id: 1, points: [east(0), east(20)], kind: 'path', layer: 1, width: 3 }, { id: 2, points: [east(0, 50), east(20, 50)], kind: 'road', layer: 1, width: 8 }],
      raised: new Set([key(east(0)), key(east(20)), key(east(0, 50)), key(east(20, 50))]),
      onGround: new Set([key(east(0)), key(east(20)), key(east(0, 50)), key(east(20, 50))]),
      crossed: [{ kind: 'road', points: [east(10, -30), east(10, 80)] }],
    }), ground(200))
    expect(decks.map(d => d.kind)).toEqual(['road'])
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

  it('an end held on an embankment stays on it, not on a deck alongside', async () => {
    // The upper carriageway of a double-deck road crosses a street 4 m south of the lower one, each between embankments.
    const upper: Way = { id: 1, points: [east(0), east(20)], kind: 'road', layer: 2, width: 10 }
    const lower: Way = { id: 2, points: [east(0, 4), east(20, 4)], kind: 'road', layer: 1, width: 8 }
    const ends = [east(0), east(20), east(0, 4), east(20, 4)].map(key)
    const decks = await buildDecks(input({ ways: [upper, lower], onGround: new Set(ends), raised: new Set(ends) }), {
      async load() {},
      // The ground falls 1.5 m a metre north, so each carriageway's ends are on ground of their own.
      at: (p: Point) => 20 - 1.5 * Math.max(0, (east(0)[1] - p[1]) / (east(0)[1] - east(0, 1)[1])),
    })
    const [top, below] = [1, 2].map(w => decks.find(d => d.ways.includes(w))!)
    for (const d of [top, below]) expect([d.heights[0], d.heights.at(-1)!]).toEqual([d.ground[0], d.ground.at(-1)!])
  })

  it('starts an end held on an embankment at the road it lands on, past the hollow the terrain blurs its end into', async () => {
    // A short bridge over a street, its far end meeting an embankment whose wall the terrain blurs into a slope.
    const blurred = { async load() {}, at: ([x]: Point) => (x > east(15)[0] && x < east(25)[0] ? 12 : x > east(36)[0] && x < east(46)[0] ? 16 : 20) }
    const ends = [east(0), east(40)].map(key)
    const [deck] = (await buildDecks(input({
      ways: [way(1, 0, 40), { id: 2, points: [east(40), east(120)], kind: 'road', layer: 0, width: 8 }],
      onGround: new Set(ends),
      raised: new Set([key(east(40))]),
      crossed: [{ kind: 'road', points: [east(20, -30), east(20, 30)] }, { kind: 'road', points: [east(40), east(120)] }],
    }), blurred)).filter(d => d.ways.includes(1))
    expect(deck.heights.at(-1)!).toBeGreaterThanOrEqual(19.5)
  })

  it('holds a deck under a higher one to pins below the terrain, which takes the higher deck for ground', async () => {
    const upper: Way = { id: 1, points: [east(0), east(60)], kind: 'road', layer: 2, width: 12 }
    const lower: Way = { id: 2, points: [east(0, 3), east(30, 3), east(60, 3)], kind: 'road', layer: 1, width: 8, pins: [{ vertex: 0, height: 14 }, { vertex: 1, height: 13 }, { vertex: 2, height: 14 }] }
    const ends = [east(0), east(60), east(0, 3), east(60, 3)].map(key)
    const decks = await buildDecks(input({ ways: [upper, lower], onGround: new Set(ends) }), ground(20))
    const below = decks.find(d => d.ways.includes(2))!
    expect(below.heights[0]).toBeCloseTo(14, 1)
    expect(below.heights[Math.round(30 / STEP)]).toBeCloseTo(13, 1)
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
    expect(numbered(['x', 'x', 'y'], [a, b, c], [[1], [2], [3]])).toEqual(['x', 'x/1', 'y'])
    expect(numbered(['y', 'x', 'x'], [c, b, a], [[3], [2], [1]])).toEqual(['y', 'x/1', 'x'])
  })

  it('breaks a tie between decks at one midpoint by their ways, in any order', () => {
    const p: Point = [0.3, 0.4]
    expect(numbered(['x', 'x'], [p, p], [[9, 4], [5]])).toEqual(['x', 'x/1'])
    expect(numbered(['x', 'x'], [p, p], [[5], [4, 9]])).toEqual(['x/1', 'x'])
  })
})

describe('buildDecks on a lid over a sunken road', () => {
  // A street 0-300 m east on a lid 120 m wide, over a motorway running north in a trench 8 m down.
  // The terrain keeps the lid as ground: 200 m on it, 192 m in the trench beyond it.
  const lid = { id: 'way/9', rings: [[east(-10, 60), east(310, 60), east(310, -60), east(-10, -60), east(-10, 60)]] }
  const [band, lidEdge] = [east(160)[0] - east(150)[0], Math.abs(east(0, 60)[1] - east(0)[1])]
  const trench = { async load() {}, at: ([x, y]: Point) => (Math.abs(x - east(150)[0]) < band && Math.abs(y - east(0)[1]) > lidEdge ? 192 : 200) }
  const motorway = (covered?: boolean) => ({ kind: 'road' as const, points: [east(150, -200), east(150, 200)], ...(covered ? { covered } : {}) })
  const street = (crossed: DeckInput['crossed'], terrain = trench) => buildDecks(input({
    ways: [way(1, 0, 300)], outlines: [lid], onGround: new Set([key(east(0)), key(east(300))]), crossed,
  }), terrain).then(([deck]) => Math.max(...deck.heights))

  it('stays on the lid over a covered road under it, cleared from the road\'s own level beyond it', async () => {
    expect(await street([motorway(true)])).toBeLessThan(200.5)
    // A road not mapped as covered is cleared from the terrain under the deck, as an elevated line's streets are.
    expect(await street([motorway()])).toBeGreaterThan(200 + CLEARANCE.road - 0.5)
  })

  it('reads the road through the ways it is split into under the lid', async () => {
    const [a, b, c] = [[east(150, -200), east(150, -20)], [east(150, -20), east(150, 20)], [east(150, 20), east(150, 200)]]
    expect(await street([a, b, c].map(points => ({ kind: 'road' as const, points, covered: true })))).toBeLessThan(200.5)
  })

  it('reads a road climbing out from under the lid at its level there, between its two sides by distance', async () => {
    // The lid reaches 20 m south of the street and 100 m north; the road is in its trench to the south and at the street to the north.
    const edge = { id: 'way/9', rings: [[east(-10, 100), east(310, 100), east(310, -20), east(-10, -20), east(-10, 100)]] }
    const south = Math.abs(east(0, -20)[1] - east(0)[1])
    const ramp = { async load() {}, at: ([x, y]: Point) => (Math.abs(x - east(150)[0]) < band && y - east(0)[1] > south ? 192 : 200) }
    const [deck] = await buildDecks(input({
      ways: [way(1, 0, 300)], outlines: [edge], onGround: new Set([key(east(0)), key(east(300))]), crossed: [motorway(true)],
    }), ramp)
    expect(Math.max(...deck.heights)).toBeLessThan(200.5)
  })

  it('meets a way it shares a node with where they cross, rather than clearing it', async () => {
    const node = east(150)
    const footway = { kind: 'path' as const, points: [east(150, -40), node, east(150, 40)] }
    const crossing = (shared: boolean) => buildDecks(input({
      ways: [{ ...way(1, 0, 300), points: shared ? [east(0), node, east(300)] : [east(0), east(300)] }],
      onGround: new Set([key(east(0)), key(east(300))]), crossed: [footway],
    }), ground(200)).then(([deck]) => Math.max(...deck.heights))
    expect(await crossing(true)).toBeLessThan(200.5)
    expect(await crossing(false)).toBeGreaterThan(200 + CLEARANCE.path - 0.5)
  })
})

describe('buildDecks at a junction', () => {
  it('rests the two halves of a road ending at a junction on the road running through it', async () => {
    // Memorial runs straight through the junction; Capitol, bending a little there, is split into two decks ending at its node.
    const node = east(150)
    const decks = await buildDecks(input({
      ways: [{ ...way(5, 0, 150), road: 'primary/Memorial' }, { ...way(6, 150, 300), road: 'primary/Memorial' },
        { id: 2, points: [east(150, -120), node], kind: 'road', layer: 1, width: 8, road: 'secondary/Capitol' },
        { id: 3, points: [node, east(156, 120)], kind: 'road', layer: 1, width: 8, road: 'secondary/Capitol' }],
      onGround: new Set([key(east(0)), key(east(300)), key(east(150, -120)), key(east(150, 120))]),
      // A road under Capitol's south half, close to the junction: resting on the north half, the joint stood clear of it.
      crossed: [{ kind: 'road', points: [east(100, -30), east(200, -30)] }],
    }), ground(200))
    const through = decks.find(d => d.ways.includes(5))!
    for (const id of [2, 3]) {
      const half = decks.find(d => d.ways.includes(id))!
      const end = half.points.reduce((b, p, i) => (Math.hypot(p[0] - node[0], p[1] - node[1]) < Math.hypot(half.points[b][0] - node[0], half.points[b][1] - node[1]) ? i : b), 0)
      expect(Math.abs(half.heights[end] - through.heights[Math.round(150 / STEP)])).toBeLessThan(0.3)
    }
  })
})

describe('buildDecks in an outline', () => {
  it('shapes twin carriageways to their outline, side by side and at one height', async () => {
    const twin = (id: number, north: number): Way => ({ id, points: [east(0, north), east(120, north)], kind: 'road', layer: 1, width: 8 })
    const outline = { id: 'way/9', rings: [[east(-10, 14), east(130, 14), east(130, -14), east(-10, -14), east(-10, 14)]] }
    const decks = await buildDecks(input({
      ways: [twin(1, 7), twin(2, -7)],
      onGround: new Set([key(east(0, 7)), key(east(120, 7)), key(east(0, -7)), key(east(120, -7))]),
      outlines: [outline],
      crossed: [{ kind: 'road', points: [east(60, -40), east(60, 40)] }],
    }), ground(100))
    const [north, south] = [decks.find(d => d.ways.includes(1))!, decks.find(d => d.ways.includes(2))!]
    expect(north.bridge).toBe('way/9')
    // Each reaches 7 m out to the outline and 7 m in, to meet its twin.
    expect(north.sides![0][10]).toBeCloseTo(7, 0)
    expect(north.sides![1][10]).toBeCloseTo(7, 0)
    expect(north.edges).toEqual([Math.max(...north.sides![0]), Math.max(...north.sides![1])])
    // Both ends run on 10 m to the outline's.
    expect(north.caps!.every(c => Math.abs(c + 10) < 0.6)).toBe(true)
    expect(north.heights[10]).toBeCloseTo(south.heights[10], 1)
  })

  // A main line 0-120 m with a 10.5 m carriageway in an outline 24 m across,
  // drawn with points alongside it so the outline fits it.
  const wide = { id: 'way/9', rings: [[east(-10, 12), east(60, 12), east(130, 12), east(130, -12), east(60, -12), east(-10, -12), east(-10, 12)]] }
  const main: Way = { ...way(1, 0, 120), width: 10.5 }
  const landed = new Set([key(east(0)), key(east(120))])

  it('rests a ramp ending within its outline\'s reach of the deck it meets, not only its carriageway\'s', async () => {
    // Ends 8 m from the main line's centre: past its carriageway, inside its outline.
    const ramp: Way = { id: 2, points: [east(60, -60), east(60, -8)], kind: 'road', layer: 1, width: 6 }
    const decks = await buildDecks(input({ ways: [main, ramp], outlines: [wide], onGround: new Set([...landed, key(east(60, -60))]) }), ground(100))
    expect(decks.find(d => d.ways.includes(2))!.grounded).toEqual([true, false])
  })

  it('takes in a sidewalk within its outline\'s reach, not only its carriageway\'s', async () => {
    // 11.5 m out from a 6 m carriageway: past its reach of 3 + MAX_REACH / 2, inside the outline's 12 + MAX_REACH / 2.
    const narrow: Way = { ...way(1, 0, 120), width: 6 }
    const sidewalk: Way = { id: 3, points: [east(0, 11.5), east(120, 11.5)], kind: 'path', layer: 1, width: 3 }
    const decks = await buildDecks(input({ ways: [narrow, sidewalk], outlines: [wide], onGround: landed }), ground(100))
    expect(decks.map(d => d.ways)).toEqual([[1, 3]])
  })

  it('keeps the edges fitted to its outline where it cannot be shaped to it', async () => {
    // Most of its points lie in a sliver of outline that no 6 m sample falls in.
    const crossing: Way = { id: 4, points: [east(0), east(50), east(50.5), east(51), east(100)], kind: 'road', layer: 1, width: 8 }
    const sliver = { id: 'way/5', rings: [[east(49.8, 10), east(50.5, 10), east(51.2, 10), east(51.2, -10), east(50.5, -10), east(49.8, -10), east(49.8, 10)]] }
    const [deck] = await buildDecks(input({ ways: [crossing], outlines: [sliver], onGround: new Set([key(east(0)), key(east(100))]) }), ground(100))
    expect(deck.bridge).toBe('way/5')
    expect(deck.sides).toBeNull()
    expect(deck.edges[0]).toBeCloseTo(10, 0)
    expect(deck.edges[1]).toBeCloseTo(10, 0)
  })

  it('shapes only road decks: a railway in an outline keeps its own width and name', async () => {
    const rail: Way = { id: 6, points: [east(0), east(120)], kind: 'rail', layer: 1, width: 3 }
    const [deck] = await buildDecks(input({ ways: [rail], outlines: [wide], onGround: landed }), ground(100))
    expect(deck.sides).toBeNull()
    expect(deck.edges).toEqual([1.5, 1.5])
    expect(deck.bridge).not.toBe('way/9')
  })

  it('holds a track to the height of the bridge it crosses, though it runs on far beyond', async () => {
    const span = { id: 'way/8', rings: [[east(380, 10), east(720, 10), east(720, -10), east(380, -10), east(380, 10)]], deck: 30, source: 'height' as const }
    const rail = (id: number, at: number[]): Way => ({ id, points: at.map(m => east(m)), kind: 'rail', layer: 1, width: 3 })
    const tracks = [rail(5, [0, 100, 200, 300, 400]), rail(6, [400, 550, 700]), rail(7, [700, 800, 900, 1000, 1100, 1200])]
    const decks = await buildDecks(input({ ways: tracks, outlines: [span], onGround: new Set([key(east(0)), key(east(1200))]) }), ground(0))
    const bridge = decks.find(d => d.ways.includes(6))!
    expect(bridge.ways).toEqual([6])
    expect(Math.max(...bridge.heights)).toBeGreaterThan(29)
  })

  it('leaves a deck in no outline with its edges throughout', async () => {
    const [deck] = await buildDecks(input({ ways: [way(1, 0, 60)], onGround: new Set([key(east(0)), key(east(60))]) }), ground(100))
    expect(deck.sides).toBeNull()
    expect(deck.caps).toBeNull()
  })
})

describe('heights OSM gives a deck', () => {
  // The San Diego–Coronado Bridge as profile.test.ts has it, on its two ways
  // (split at the crest's node), landing at both ends on approaches that
  // carry its grades on, over the bay.
  const [up, down, from, curve] = [0.0469091, 0.0502002, 1051.872, 857.198]
  const truth = (s: number) => {
    const u = Math.min(Math.max(s, from), from + curve) - from
    const z = 16 + up * from + up * u - ((up + down) / (2 * curve)) * u * u
    return s < from ? 16 + up * s : s > from + curve ? z - down * (s - from - curve) : z
  }
  const along_ = (p: Point) => (p[0] - east(0)[0]) / (east(1)[0] - east(0)[0])
  const terrain = { async load() {}, at: (p: Point) => {
    const s = along_(p)
    return s < 0 ? 16 + up * s : s > 2585 ? 30 - down * (s - 2585) : s < 40 || s > 2545 ? truth(s) : 0
  } }
  const bay = [[east(40, -200), east(2545, -200), east(2545, 200), east(40, 200), east(40, -200)]]
  const ends = new Set([key(east(0)), key(east(2585))])

  it('holds the San Diego–Coronado Bridge to its tagged nodes on its true grades and crest, built whole', async () => {
    const decks = await buildDecks(input({
      ways: [
        { id: 1, points: [east(0), east(1150), east(1435)], kind: 'road', layer: 1, width: 10, pins: [{ vertex: 1, height: 69.4 }, { vertex: 2, height: 75 }] },
        { id: 2, points: [east(1435), east(1740), east(2585)], kind: 'road', layer: 1, width: 10, pins: [{ vertex: 0, height: 75 }, { vertex: 1, height: 70.8 }] },
      ],
      onGround: ends,
      water: bay,
    }), terrain)
    expect(decks).toHaveLength(1)
    const d = along(decks[0].points)
    for (const [i, s] of d.entries()) expect(Math.abs(decks[0].heights[i] - truth(s))).toBeLessThan(0.3)
  })

  it('holds twin carriageways side by side each to its own tagged nodes', async () => {
    const twin = (id: number, north: number, height: number): Way =>
      ({ id, points: [east(0, north), east(300, north), east(600, north)], kind: 'road', layer: 1, width: 8, pins: [{ vertex: 1, height }] })
    const decks = await buildDecks(input({
      ways: [twin(1, 0, 215), twin(2, 9, 216)],
      onGround: new Set([0, 9].flatMap(n => [key(east(0, n)), key(east(600, n))])),
    }), ground(200))
    const at = (w: number) => { const deck = decks.find(x => x.ways.includes(w))!; return deck.heights[Math.round(300 / STEP)] }
    expect(at(1)).toBeCloseTo(215, 1)
    expect(at(2)).toBeCloseTo(216, 1)
  })

  it('leaves out a tagged height below the ground under it', async () => {
    const [deck] = await buildDecks(input({ ways: [{ ...way(1, 0, 300), points: [east(0), east(150), east(300)], pins: [{ vertex: 1, height: 150 }] }], onGround: ends }), ground(200))
    expect(deck.heights[25]).toBe(200)
  })

  it('clears a seamark charting the clearance under the bridge', async () => {
    const [deck] = await buildDecks(input({
      ways: [way(1, 0, 1000)],
      onGround: new Set([key(east(0)), key(east(1000))]),
      seamarks: [{ point: east(500, 12), clearance: 20 }],
    }), ground(0))
    expect(deck.heights[Math.round(500 / STEP)]).toBeGreaterThanOrEqual(20 + DECK_DEPTH - 0.5)
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
