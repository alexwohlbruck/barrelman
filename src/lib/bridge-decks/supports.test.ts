import { describe, expect, it } from 'bun:test'
import { mercator, type Point } from './profile'
import { bentPoints, blocked, nearestFree, obstacles, wayHalf } from './supports'

const east = (m: number, north = 0): Point =>
  mercator(-80.83 + m / (111320 * Math.cos((35.22 * Math.PI) / 180)), 35.22 + north / 110574)

describe('supports', () => {
  const chain = { points: [east(0), east(50), east(100)], edges: [6, 4] as [number, number] }
  const d = [0, 50, 100]

  it('tests a bent edge to edge across the deck', () => {
    const points = bentPoints(chain, d, 30)
    const norths = points.map(p => p[1])
    expect(Math.max(...norths) - Math.min(...norths)).toBeGreaterThan(0)
    const path = (north: number) => obstacles([{ kind: 'path', points: [east(25, north), east(35, north)], half: 1 }], [], [])
    expect(blocked(points, path(-5.5))).toBe(true)
    expect(blocked(points, path(-9))).toBe(false)
  })

  it('keeps off a carriageway but not out of its median', () => {
    const points = bentPoints(chain, d, 30)
    const ring = (w: number, s: number, e: number, n: number) => [east(w, s), east(e, s), east(e, n), east(w, n), east(w, s)]
    expect(blocked(points, obstacles([], [[ring(20, -20, 40, 20)]], []))).toBe(true)
    expect(blocked(points, obstacles([], [[ring(0, -20, 60, 20), ring(25, -15, 35, 15)]], []))).toBe(false)
  })

  it('finds the nearest clear place, never at the ends', () => {
    expect(nearestFree(30, 100, 10, at => Math.abs(at - 30) > 4)).toBe(25)
    expect(nearestFree(2, 100, 1, () => true)).toBe(3)
    expect(nearestFree(30, 100, 3, () => false)).toBeNull()
  })

  it('reads a way width from its tags', () => {
    expect(wayHalf('road', 12)).toBe(6)
    expect(wayHalf('road', null, '4')).toBe(7.5)
    expect(wayHalf('rail', null)).toBe(2)
  })
})
