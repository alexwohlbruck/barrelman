import { describe, expect, test } from 'bun:test'
import { axisAngle, settleRuns, streetZones, ROAD_RANK, type Road, type XY } from './zones'

const road = (name: string, points: XY[], cls = 'residential'): Road => ({ name, rank: ROAD_RANK[cls], points })

// A block corner at the origin: W 3rd St runs east–west along y = 0, Main St north–south along x = 0.
const third = road('W 3rd St', [[-200, 0], [200, 0]])
const main = road('Main St', [[0, -200], [0, 200]], 'primary')

describe('streetZones', () => {
  test('names a sidewalk after the street it runs beside, not the cross street at its end', () => {
    const sidewalk: XY[] = [[-100, -8], [-6, -8]]
    expect(streetZones(sidewalk, [third, main])).toEqual([{ from: 0, to: 1, name: 'W 3rd St' }])
  })

  test('splits a sidewalk that wraps a corner where it turns', () => {
    const sidewalk: XY[] = [[-100, -8], [-8, -8], [-8, -100]]
    const zones = streetZones(sidewalk, [third, main])
    expect(zones.map(z => z.name)).toEqual(['W 3rd St', 'Main St'])
    expect(zones[0].to).toBeCloseTo(0.5, 1)
    expect(zones[0].to).toBe(zones[1].from)
  })

  test('leaves a path with no street alongside unnamed', () => {
    expect(streetZones([[-100, -60], [100, -60]], [third])).toEqual([])
    expect(streetZones([[-30, -100], [-30, -10]], [third])).toEqual([])
  })

  test('prefers the bigger road when two run alongside about as close', () => {
    const avenue = road('Grand Ave', [[-200, -18], [200, -18]], 'primary')
    const alley = road('Back Alley', [[-200, 0], [200, 0]], 'service')
    expect(streetZones([[-100, -8], [100, -8]], [alley, avenue])).toEqual([{ from: 0, to: 1, name: 'Grand Ave' }])
  })

  test('the nearer road wins when it is clearly nearer', () => {
    const far = road('Grand Ave', [[-200, -24], [200, -24]], 'primary')
    const near = road('Elm St', [[-200, 0], [200, 0]])
    expect(streetZones([[-100, -4], [100, -4]], [far, near])).toEqual([{ from: 0, to: 1, name: 'Elm St' }])
  })
})

describe('settleRuns', () => {
  const names = (values: Array<string | null>) => settleRuns(values).map(r => [r.name, r.start, r.end])

  test('a change lasting a sample or two folds back in', () => {
    expect(names(['A', 'A', 'A', 'B', 'A', 'A', 'A'])).toEqual([['A', 0, 7]])
    expect(names(['A', 'A', 'A', 'B', 'B', 'C', 'C', 'C', 'C'])).toEqual([['A', 0, 3], ['C', 3, 9]])
  })

  test('a change that holds becomes a boundary', () => {
    expect(names(['A', 'A', 'A', null, null, null, 'B', 'B', 'B'])).toEqual([['A', 0, 3], [null, 3, 6], ['B', 6, 9]])
  })

  test('a way too short for a full run keeps its one street', () => {
    expect(names(['A', 'A'])).toEqual([['A', 0, 2]])
  })
})

test('axisAngle ignores which way each line points', () => {
  expect(axisAngle(0, 180)).toBe(0)
  expect(axisAngle(10, 350)).toBe(20)
  expect(axisAngle(0, 90)).toBe(90)
})
