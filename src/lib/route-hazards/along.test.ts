import { describe, expect, test } from 'bun:test'
import { crossings, runsAlong } from './along'
import { routePath, routePieces, type LonLat } from './route-path'

/** Metres east and north of a point in Manhattan, as [lon, lat]. */
const at = (east: number, north = 0): LonLat => [-73.99 + east / (111_320 * Math.cos(40.75 * Math.PI / 180)), 40.75 + north / 111_320]

const route = routePath([at(0), at(500)])

describe('runsAlong', () => {
  test('a way the route rides, a metre to the side', () => {
    const runs = runsAlong(route, [at(100, 1), at(300, 1)])
    expect(runs).toHaveLength(1)
    expect(runs[0].from).toBeCloseTo(100, -1)
    expect(runs[0].to).toBeCloseTo(300, -1)
  })

  test('not a way it only crosses, nor one running beside it across the street', () => {
    expect(runsAlong(route, [at(200, -50), at(200, 50)])).toEqual([])
    expect(runsAlong(route, [at(0, 12), at(500, 12)])).toEqual([])
  })
})

describe('crossings', () => {
  test('the angle of each crossing', () => {
    const shallow = crossings(route, [at(150, -10), at(250, 10)])
    expect(shallow).toHaveLength(1)
    expect(shallow[0].distance).toBeCloseTo(200, 0)
    expect(shallow[0].angle).toBeCloseTo(11.3, 0)
    expect(crossings(route, [at(300, -10), at(300, 10)])[0].angle).toBeCloseTo(90, 0)
  })
})

describe('routePieces', () => {
  test('cuts a long straight segment so each piece stays near its own stretch', () => {
    const pieces = routePieces(route, 150)
    expect(pieces).toHaveLength(4)
    expect(pieces.every(p => p.firstSegment === 0 && p.lastSegment === 0)).toBe(true)
    expect(pieces[3].coordinates.at(-1)).toEqual(at(500))
  })
})

describe('project', () => {
  test('a route passing a point twice sees it twice', () => {
    const loop = routePath([at(0), at(200), at(200, 20), at(0, 20)])
    expect(loop.project(at(100, 10), 15).map(p => Math.round(p.distance))).toEqual([100, 320])
  })
})
