import { describe, expect, test } from 'bun:test'
import { getRouteLanes, stretches, type WayRecord } from './route-lanes.service'
import { cumulativeDistances, distanceAlong, headingChange, laneLine } from '../lib/lanes/lane-geometry'
import { lanePlan, turnFromAngle } from '../lib/lanes/lane-plan'
import { crossSection } from '../lib/lanes/cross-section'

const LAT = 40.75
const LON = -73.99
const M_LAT = 1 / 111_320
const M_LON = 1 / (111_320 * Math.cos((LAT * Math.PI) / 180))
/** [lon, lat] `east` and `north` metres from the origin. */
const at = (east: number, north: number): [number, number] => [LON + east * M_LON, LAT + north * M_LAT]
/** Metres east and north of the origin. */
const local = ([lon, lat]: [number, number]) => [(lon - LON) / M_LON, (lat - LAT) / M_LAT]

// East along one street for 200 m, then right, south down another for 200 m.
const route = [at(0, 0), at(100, 0), at(200, 0), at(200, -100), at(200, -200)]
const eastbound: WayRecord = {
  tags: { highway: 'primary', oneway: 'yes', lanes: '3', 'turn:lanes': 'left|through|through;right' },
  coordinates: [at(0, 0), at(200, 0)],
}
// Drawn north, so the route drives it against its nodes.
const avenue: WayRecord = { tags: { highway: 'secondary', lanes: '4' }, coordinates: [at(200, -200), at(200, 0)] }
const loadWays = async () => new Map([[1, eastbound], [2, avenue]])
const request = {
  coordinates: route,
  ways: [{ from: 0, to: 200, osmWayId: 1 }, { from: 200, to: 400, osmWayId: 2 }],
  maneuvers: [{ distance: 0 }, { distance: 200 }],
}

describe('lane plan', () => {
  test('names a turn from the heading change', () => {
    expect(turnFromAngle(90)).toBe('right')
    expect(turnFromAngle(-40)).toBe('slight_left')
    expect(turnFromAngle(5)).toBe('through')
    expect(Math.abs(headingChange(route, 200) - 90)).toBeLessThan(2)
  })

  test('marks the lanes that lead on and recommends the outermost for a turn', () => {
    const lanes = crossSection(eastbound.tags, { americas: true, side: 'right' }).forward
    const plan = lanePlan(lanes, 'right', 'right')!
    expect(plan.map(l => l.valid)).toEqual([false, false, true])
    expect(plan.map(l => l.recommended)).toEqual([false, false, true])
    expect(lanePlan(lanes, 'through', 'right')!.map(l => l.valid)).toEqual([false, true, true])
  })

  test('has no plan without arrows', () => {
    const lanes = crossSection({ highway: 'primary', oneway: 'yes', lanes: '3' }, { americas: true, side: 'right' }).forward
    expect(lanePlan(lanes, 'right', 'right')).toBeNull()
  })
})

describe('stretches', () => {
  test('joins a way split across edges and fills gaps with no way', () => {
    expect(stretches([{ from: 0, to: 50, osmWayId: 7 }, { from: 50, to: 80, osmWayId: 7 }, { from: 90, to: 100, osmWayId: 8 }], 100)).toEqual([
      { from: 0, to: 80, osmWayId: 7 },
      { from: 80, to: 90, osmWayId: null },
      { from: 90, to: 100, osmWayId: 8 },
    ])
  })
})

describe('laneLine', () => {
  test('a constant offset runs parallel to the route, on its right', () => {
    const straight = [at(0, 0), at(100, 0)]
    const line = laneLine(straight, [{ from: 0, to: 100, entry: 2, exit: 2, settleBefore: 0, laneWidth: 3 }], [])
    for (const p of line.coordinates) expect(local(p)[1]).toBeCloseTo(-2, 1)
    expect(line.measures).toEqual(cumulativeDistances(straight))
  })

  test('a lane change S-curves and is done by the settle point', () => {
    const straight = [at(0, 0), at(200, 0)]
    const line = laneLine(straight, [{ from: 0, to: 200, entry: 1.5, exit: 4.5, settleBefore: 20, laneWidth: 3 }], [])
    const north = line.coordinates.map(p => local(p)[1])
    expect(north[0]).toBeCloseTo(-1.5, 1)
    expect(north.at(-1)!).toBeCloseTo(-4.5, 1)
    const settled = line.measures.findIndex(m => m >= 180)
    expect(north[settled]).toBeCloseTo(-4.5, 1)
    // Monotonic: it moves one way only.
    for (let i = 1; i < north.length; i++) expect(north[i]).toBeLessThanOrEqual(north[i - 1] + 1e-6)
  })
})

describe('getRouteLanes', () => {
  test('plans the turn and draws the route in the lanes it takes', async () => {
    const res = await getRouteLanes(request, loadWays)
    expect(res.drivingSide).toBe('right')
    expect(res.lanePlan).toHaveLength(1)
    expect(res.lanePlan[0]).toMatchObject({ maneuver: 1, distance: 200, turn: 'right' })
    expect(res.lanePlan[0].lanes.map(l => l.recommended)).toEqual([false, false, true])

    const [first, second] = res.ways
    expect(first).toMatchObject({ direction: 'forward', entryLane: 3, exitLane: 3 })
    expect(second).toMatchObject({ direction: 'backward', entryLane: 2 })

    const geometry = res.laneGeometry!
    expect(geometry.coordinates.length).toBe(geometry.measures.length)
    for (let i = 1; i < geometry.measures.length; i++) expect(geometry.measures[i]).toBeGreaterThanOrEqual(geometry.measures[i - 1])
    // Kerbside lane of the 3-lane eastbound street: a lane right of its centre.
    const start = local(geometry.coordinates[0])
    expect(start[1]).toBeCloseTo(-3.4, 1)
    // Ends in the avenue's southbound kerb lane, west of its centreline.
    const end = local(geometry.coordinates.at(-1)!)
    expect(end[0]).toBeCloseTo(200 - 1.5 * 3.3, 1)
  })

  test('turns through the corner on an arc, cutting inside it', async () => {
    const res = await getRouteLanes(request, loadWays)
    const near = res.laneGeometry!.coordinates.map(local).filter(([e, n]) => e > 185 && n > -15)
    expect(near.length).toBeGreaterThan(3)
    // Nothing reaches the corner of the offset lines: the arc rounds it off.
    const corner = [200 - 1.5 * 3.3, -3.4]
    expect(Math.min(...near.map(([e, n]) => Math.hypot(e - corner[0], n - corner[1])))).toBeGreaterThan(0.5)
  })

  test('keeps to the centreline where a way is unknown', async () => {
    const res = await getRouteLanes({ ...request, ways: [{ from: 0, to: 400, osmWayId: 99 }] }, loadWays)
    expect(res.laneGeometry).toBeNull()
    expect(res.lanePlan).toEqual([])
  })

  test('drives on the left in a left-hand country', async () => {
    const res = await getRouteLanes({ ...request, country: 'GBR', maneuvers: [] }, loadWays)
    expect(res.drivingSide).toBe('left')
    // A one-way still starts in its kerb lane, now the left one.
    expect(local(res.laneGeometry!.coordinates[0])[1]).toBeCloseTo(3.4, 1)
  })

  test('projects onto a way to tell which way the route drives it', () => {
    expect(distanceAlong(avenue.coordinates, at(200, -50))).toBeCloseTo(150, 0)
  })
})

describe('getRouteLanes for bikes', () => {
  // 200 m east down a one-way street with a buffered bike lane on its right, then a separate cycleway.
  const street: WayRecord = {
    tags: { highway: 'secondary', oneway: 'yes', lanes: '2', 'cycleway:right': 'lane', 'cycleway:right:buffer': 'yes' },
    coordinates: [at(0, 0), at(200, 0)],
  }
  const path: WayRecord = { tags: { highway: 'cycleway' }, coordinates: [at(200, 0), at(400, 0)] }
  const plain: WayRecord = { tags: { highway: 'residential', oneway: 'yes', lanes: '1', 'parking:right': 'lane' }, coordinates: [at(0, 0), at(200, 0)] }
  const contraflow: WayRecord = {
    tags: { highway: 'residential', oneway: 'yes', 'oneway:bicycle': 'no', 'cycleway:left': 'opposite_lane' },
    coordinates: [at(200, 0), at(0, 0)],
  }
  const straight = [at(0, 0), at(200, 0), at(400, 0)]
  const ways = [{ from: 0, to: 200, osmWayId: 1 }, { from: 200, to: 400, osmWayId: 2 }]
  const lanes = (map: Record<number, WayRecord>) => async () => new Map(Object.entries(map).map(([k, v]) => [Number(k), v]))
  const north = (res: Awaited<ReturnType<typeof getRouteLanes>>, metres: number) => {
    const g = res.laneGeometry!
    const i = g.measures.findIndex(m => m >= metres)
    return local(g.coordinates[i])[1]
  }

  test('rides down the middle of the bike lane, then the cycleway on its own line', async () => {
    const res = await getRouteLanes({ coordinates: straight, ways, profile: 'bike' }, lanes({ 1: street, 2: path }))
    // A 9.1 m carriageway (two 3.3 m lanes, a 0.9 m buffer, a 1.6 m bike lane at the kerb), centred on the way.
    expect(north(res, 100)).toBeCloseTo(-(4.55 - 0.8), 1)
    expect(north(res, 300)).toBeCloseTo(0, 1)
    expect(res.lanePlan).toEqual([])
  })

  test('without a bike lane, keeps a metre inside the outer traffic lane, clear of parking', async () => {
    const res = await getRouteLanes({ coordinates: straight.slice(0, 2), ways: ways.slice(0, 1), profile: 'bike' }, lanes({ 1: plain }))
    // A 3 m lane beside 2.4 m of parking, centred together: the lane's kerb-side edge is 0.3 m right of the way.
    expect(north(res, 100)).toBeCloseTo(0.7, 1)
  })

  test('rides a contraflow lane against a one-way', async () => {
    const res = await getRouteLanes({ coordinates: straight.slice(0, 2), ways: ways.slice(0, 1), profile: 'bike' }, lanes({ 1: contraflow }))
    expect(res.ways[0].direction).toBe('backward')
    expect(north(res, 100)).toBeLessThan(-1)
  })

  test('keeps left where traffic does', async () => {
    const res = await getRouteLanes({ coordinates: straight, ways, profile: 'bike', drivingSide: 'left' }, lanes({ 1: { ...street, tags: { ...street.tags, 'cycleway:right': undefined, 'cycleway:left': 'lane' } }, 2: path }))
    expect(north(res, 100)).toBeGreaterThan(3)
  })
})
