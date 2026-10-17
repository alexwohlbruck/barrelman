import { describe, expect, it } from 'bun:test'
import { mercator, type Point } from './profile'
import { bentPoints, blocked, nearestFree, wayHalf } from './supports'

const east = (m: number, north = 0): Point =>
  mercator(-80.83 + m / (111320 * Math.cos((35.22 * Math.PI) / 180)), 35.22 + north / 110574)

describe('supports', () => {
  const chain = { points: [east(0), east(50), east(100)], edges: [6, 4] as [number, number] }
  const d = [0, 50, 100]

  it('tests a bent edge to edge across the deck', () => {
    const points = bentPoints(chain, d, 30)
    const norths = points.map(p => p[1])
    expect(Math.max(...norths) - Math.min(...norths)).toBeGreaterThan(0)
    expect(blocked(points, { ways: [{ kind: 'path', points: [east(25, -5.5), east(35, -5.5)], half: 1 }], surfaces: [], decks: [] })).toBe(true)
    expect(blocked(points, { ways: [{ kind: 'path', points: [east(25, -9), east(35, -9)], half: 1 }], surfaces: [], decks: [] })).toBe(false)
  })

  it('finds the nearest clear place, never at the ends', () => {
    expect(nearestFree(30, 100, 10, at => Math.abs(at - 30) > 4)).toBe(25)
    expect(nearestFree(2, 100, 1, () => true)).toBe(3)
    expect(nearestFree(30, 100, 3, () => false)).toBeNull()
  })

  it('reads a way width from its tags', () => {
    expect(wayHalf('road', { width: '12 m' })).toBe(6)
    expect(wayHalf('road', { lanes: '4' })).toBe(7.5)
    expect(wayHalf('rail', {})).toBe(2)
  })
})
