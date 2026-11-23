import { describe, expect, test } from 'bun:test'
import { haversine, type LonLat } from '../route-hazards/route-path'
import { AREA_PATH_WAY_ID_BASE, areaPathWays, foldAreaManeuvers, roundPath } from './round-path'

const K = Math.cos((40.7 * Math.PI) / 180) * 111_320
const ll = ([x, y]: number[]) => [-74 + x / K, 40.7 + y / 111_320, 10]
const length = (c: number[][]) => c.slice(1).reduce((sum, p, i) => sum + haversine(c[i] as LonLat, p as LonLat), 0)

const plaza = { rings: [[[0, 0], [40, 0], [40, 40], [0, 40], [0, 0]].map(ll)] }
const AREA = AREA_PATH_WAY_ID_BASE + 7
const WALK = 1.4

function path() {
  const coordinates = [[-10, 35], [5, 35], [20, 20], [35, 35]].map(ll)
  const metres = [0, 1, 2].map(i => haversine(coordinates[i] as LonLat, coordinates[i + 1] as LonLat))
  const ms = metres.map(m => (m / WALK) * 1000)
  return {
    distance: metres[0] + metres[1] + metres[2],
    time: Math.round(ms[0] + ms[1] + ms[2]),
    points: { type: 'LineString', coordinates },
    snapped_waypoints: { type: 'LineString', coordinates: [coordinates[0], coordinates[3]] },
    details: {
      osm_way_id: [[0, 1, 42], [1, 2, AREA], [2, 3, AREA + 1]],
      average_speed: [[0, 3, WALK * 3.6]],
    },
    instructions: [
      { text: 'Continue', interval: [0, 2], distance: metres[0] + metres[1], time: Math.round(ms[0] + ms[1]) },
      { text: 'Turn left', interval: [2, 3], distance: metres[2], time: Math.round(ms[2]) },
      { text: 'Arrive', interval: [3, 3], distance: 0, time: 0 },
    ],
  }
}

describe('roundPath', () => {
  const spaces = new Map([[AREA, plaza], [AREA + 1, plaza]])

  test('finds the area-path ways', () => {
    expect(areaPathWays(path())).toEqual([AREA, AREA + 1])
  })

  test('the distance is the length of the curve, and the time walks it', () => {
    const before = path()
    const after = roundPath(before, spaces)
    const curve = length(after.points.coordinates)
    expect(after.distance).toBeCloseTo(curve, 6)
    expect(after.distance).toBeLessThan(before.distance)
    expect(after.time).toBeCloseTo((after.distance / WALK) * 1000, -1)
  })

  test('instructions and details point at the rounded points', () => {
    const after = roundPath(path(), spaces)
    const n = after.points.coordinates.length
    expect(after.instructions[0].interval[0]).toBe(0)
    expect(after.instructions[1].interval[1]).toBe(n - 1)
    expect(after.instructions[0].interval[1]).toBe(after.instructions[1].interval[0])
    expect(after.details.osm_way_id.map((d: number[]) => d[2])).toEqual([42, AREA, AREA + 1])
    expect(after.details.osm_way_id[2][1]).toBe(n - 1)
    const total = after.instructions.reduce((sum: number, i: any) => sum + i.distance, 0)
    expect(total).toBeCloseTo(after.distance, 6)
  })

  test('elevation carries along the curve', () => {
    const after = roundPath(path(), spaces)
    expect(after.points.coordinates.every((c: number[]) => c.length === 3 && Math.abs(c[2] - 10) < 1e-9)).toBe(true)
  })

  test('without its space a path is left as it came', () => {
    const before = path()
    expect(roundPath(before, new Map())).toBe(before)
  })
})

describe('foldAreaManeuvers', () => {
  const ways = [[0, 1, 42], [1, 2, AREA], [2, 3, AREA + 1], [3, 4, 43]]
  const step = (sign: number, from: number, to: number, distance: number) => ({ sign, interval: [from, to], distance, time: distance * 700, text: '' })

  test('crossing an area is one step, from the way in to the way out', () => {
    const path = { details: { osm_way_id: ways }, instructions: [step(0, 0, 1, 10), step(-7, 1, 2, 20), step(7, 2, 3, 30), step(2, 3, 4, 40), step(4, 4, 4, 0)] }
    const folded = foldAreaManeuvers(path).instructions
    expect(folded.map((i: any) => i.sign)).toEqual([0, -7, 2, 4])
    expect(folded[1]).toMatchObject({ interval: [1, 3], distance: 50, time: 35_000 })
  })

  test('the step across takes the area\'s name when the way in has none', () => {
    const path = { details: { osm_way_id: ways }, instructions: [step(0, 0, 1, 10), step(-7, 1, 2, 20), { ...step(7, 2, 3, 30), street_name: 'Foley Square' }, step(4, 4, 4, 0)] }
    expect(foldAreaManeuvers(path).instructions[1].street_name).toBe('Foley Square')
  })

  test('a waypoint inside an area stays a step', () => {
    const path = { details: { osm_way_id: ways }, instructions: [step(0, 0, 1, 10), step(-7, 1, 2, 20), step(5, 2, 3, 30), step(4, 4, 4, 0)] }
    expect(foldAreaManeuvers(path)).toBe(path)
  })
})
