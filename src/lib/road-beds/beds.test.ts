import { describe, expect, it } from 'bun:test'
import { metresPerUnit, type Point } from '../bridge-mesh/decks'
import { BED, beds } from './beds'

const y0 = 0.376
const M = 1 / metresPerUnit(y0)
const road = (from: number, to: number): Point[] => [[0.2945 + from * M, y0], [0.2945 + to * M, y0]]
// Ground by metres along (x) and across (y) the road.
const ground = (f: (along: number, across: number) => number) => ([x, y]: Point) => f((x - 0.2945) / M, (y - y0) / M)

describe('road beds', () => {
  it('leaves a road on level ground alone', () => {
    expect(beds(road(0, 200), 8, ground(() => 10))).toEqual([])
  })

  it('holds a road on an embankment whose sides fall away at its height, past the stretch by its pad', () => {
    const embankment = ground((along, across) => (along > 80 && along < 120 && Math.abs(across) > 4 ? 6 : 10))
    const [bed, ...rest] = beds(road(0, 200), 8, embankment)
    expect(rest).toEqual([])
    const at = (p: Point) => (p[0] - 0.2945) / M
    expect(at(bed.points[0])).toBeCloseTo(80 - BED.pad, -1)
    expect(at(bed.points.at(-1)!)).toBeCloseTo(120 + BED.pad, -1)
    expect(bed.floor.every(h => h === 10)).toBe(true)
  })

  it('takes the centreline\'s height, smoothed, along a road climbing across a hillside', () => {
    const hillside = ground((along, across) => along * 0.05 + across * 0.3)
    const [bed] = beds(road(0, 100), 6, hillside)
    expect(bed.floor[0]).toBeCloseTo(0, 0)
    expect(bed.floor.at(-1)).toBeCloseTo(5, 0)
  })
})
