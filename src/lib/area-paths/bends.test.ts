import { describe, expect, test } from 'bun:test'
import { roundBends, within, type Space } from './bends'

const K = Math.cos((40.7 * Math.PI) / 180) * 111_320
const ll = ([x, y]: number[]) => [-74 + x / K, 40.7 + y / 111_320]
const xy = ([lon, lat]: number[]) => [(lon + 74) * K, (lat - 40.7) * 111_320] as [number, number]
const space = (...rings: number[][][]): Space => ({ rings: rings.map(r => r.map(ll)) })
const roomOf = (s: Space) => ({ rings: s.rings.map(r => r.map(xy)) })
const square = [[0, 0], [40, 0], [40, 40], [0, 40], [0, 0]]

function allInside(s: Space, coordinates: number[][]) {
  const room = roomOf(s)
  return coordinates.every(c => within(room, xy(c)))
}

describe('roundBends', () => {
  test('curves an open bend and keeps its ends', () => {
    const plaza = space(square)
    const route = [[5, 35], [20, 20], [35, 35]].map(ll)
    const { coordinates, indexOf } = roundBends(route, () => plaza)
    expect(coordinates.length).toBeGreaterThan(3)
    expect(coordinates[0]).toBe(route[0])
    expect(coordinates[coordinates.length - 1]).toBe(route[2])
    expect(indexOf[0]).toBe(0)
    expect(indexOf[2]).toBe(coordinates.length - 1)
    expect(allInside(plaza, coordinates)).toBe(true)
  })

  test('the corner maps to the middle of its curve, which cuts inside it', () => {
    const plaza = space(square)
    const route = [[5, 35], [20, 20], [35, 35]].map(ll)
    const { coordinates, indexOf } = roundBends(route, () => plaza)
    const mid = xy(coordinates[indexOf[1]])
    expect(Math.abs(mid[0] - 20)).toBeLessThan(0.01)
    expect(mid[1]).toBeGreaterThan(20)
  })

  test('stays in a narrow corridor that turns', () => {
    const corridor = space([[0, 0], [30, 0], [30, 30], [27, 30], [27, 3], [0, 3], [0, 0]])
    const route = [[0, 1.5], [28.5, 1.5], [28.5, 30]].map(ll)
    const { coordinates } = roundBends(route, () => corridor)
    expect(coordinates.length).toBeGreaterThan(3)
    expect(allInside(corridor, coordinates)).toBe(true)
  })

  test('leaves a bend sharp when its inside is not open', () => {
    const notched = space([[0, 0], [20, 0], [20, 10], [10, 10], [10, 20], [0, 20], [0, 0]])
    const route = [[10, 20], [10, 10], [20, 10]].map(ll)
    expect(roundBends(route, () => notched).coordinates).toBe(route)
  })

  test('goes around a hole without entering it', () => {
    const hole = [[18, 18], [22, 18], [22, 22], [18, 22], [18, 18]]
    const plaza = space(square, hole)
    const route = [[2, 20], [17.5, 22.5], [22.5, 22.5], [38, 20]].map(ll)
    const { coordinates } = roundBends(route, () => plaza)
    expect(allInside(plaza, coordinates)).toBe(true)
  })

  test('a curve around a planter keeps a metre off it', () => {
    const big = [[0, 0], [60, 0], [60, 60], [0, 60], [0, 0]]
    const plaza = space(big, [[20, 20], [40, 20], [40, 40], [20, 40], [20, 20]])
    const route = [[55, 18.5], [18.5, 18.5], [2, 50]].map(ll)
    const { coordinates } = roundBends(route, () => plaza)
    expect(coordinates.length).toBeGreaterThan(3)
    const gap = Math.min(...coordinates.map(c => Math.hypot(...xy(c).map(v => Math.max(20 - v, 0, v - 40)))))
    expect(gap).toBeGreaterThan(0.99)
  })

  test('does not round where the lines cross different spaces', () => {
    const a = space(square), b = space(square)
    const route = [[5, 35], [20, 20], [35, 35]].map(ll)
    expect(roundBends(route, line => (line === 0 ? a : b)).coordinates).toBe(route)
  })

  test('settles a point that coordinate rounding left just outside onto the edge', () => {
    const plaza = space(square)
    const route = [[-0.05, 20], [20, 5], [39, 20]].map(ll)
    const { coordinates } = roundBends(route, () => plaza)
    expect(xy(coordinates[0])[0]).toBeCloseTo(0, 6)
    expect(allInside(plaza, coordinates)).toBe(true)
  })

  test('leaves a point well outside where it is', () => {
    const plaza = space(square)
    const route = [[-1, 20], [20, 5], [39, 20]].map(ll)
    expect(xy(roundBends(route, () => plaza).coordinates[0])[0]).toBeCloseTo(-1, 6)
  })

  test('keeps a fixed point, such as a waypoint, where it is', () => {
    const plaza = space(square)
    const route = [[5, 35], [20, 20], [35, 35]].map(ll)
    expect(roundBends(route, () => plaza, new Set([1])).coordinates).toBe(route)
  })
})
