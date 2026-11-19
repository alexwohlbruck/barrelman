import { describe, expect, it } from 'bun:test'
import { metresPerUnit, type Point } from '../bridge-mesh/decks'
import { BED, beds, type BedRoad } from './beds'

const y0 = 0.376
const M = 1 / metresPerUnit(y0)
const x = (m: number) => 0.2945 + m * M
const along = (p: Point) => (p[0] - 0.2945) / M
// Ground by metres along (x) and across (y) the road.
const ground = (f: (along: number, across: number) => number) => ([px, py]: Point) => f((px - 0.2945) / M, (py - y0) / M)
const way = (id: number, from: number, to: number, over: Partial<BedRoad['ways'][number]> = {}): BedRoad['ways'][number] =>
  ({ id, points: [[x(from), y0], [x(to), y0]], half: 8, pins: [null, null], under: false, ...over })
const road = (...ways: BedRoad['ways']): BedRoad => ({ ways })

describe('road beds', () => {
  it('leaves a road on level ground alone', () => {
    expect(beds(road(way(1, 0, 200)), ground(() => 10))).toEqual([])
  })

  it('fills a road on an embankment whose sides fall away up to its height, past the stretch by its pad', () => {
    const embankment = ground((a, across) => (a > 80 && a < 120 && Math.abs(across) > 4 ? 6 : 10))
    const [bed, ...rest] = beds(road(way(1, 0, 200)), embankment)
    expect(rest).toEqual([])
    expect(along(bed.points[0])).toBeCloseTo(80 - BED.pad, -1)
    expect(along(bed.points.at(-1)!)).toBeCloseTo(120 + BED.pad, -1)
    expect(bed.floor.every(h => h === 10)).toBe(true)
    expect(bed.level).toBe(false)
  })

  it('takes the centreline\'s height, smoothed, along a road climbing across a hillside', () => {
    const [bed] = beds(road(way(1, 0, 100, { half: 6 })), ground((a, across) => a * 0.05 + across * 0.3))
    expect(bed.floor[0]).toBeCloseTo(0, 0)
    expect(bed.floor.at(-1)).toBeCloseTo(5, 0)
  })

  it('carries a covered stretch straight between the road either side, not down a hollow in the terrain under its cover', () => {
    const hollow = ground(a => (a > 100 && a < 160 ? 6 : 10 + a * 0.01))
    const found = beds(road(way(1, 0, 100), way(2, 100, 160, { under: true }), way(3, 160, 260)), hollow)
    const covered = found.find(b => b.way === 2)!
    expect(covered.level).toBe(true)
    for (const [k, p] of covered.points.entries()) expect(covered.floor[k]).toBeCloseTo(10 + along(p) * 0.01, 0)
    // Its beds on the ways either side meet it where the ways do.
    const before = found.find(b => b.way === 1)!
    expect(before.points.at(-1)).toEqual(covered.points[0])
    expect(before.floor.at(-1)).toBeCloseTo(covered.floor[0], 5)
  })

  it('lowers a road under a higher one to the heights OSM gives its nodes', () => {
    // The terrain takes the deck overhead for ground: 16 m, where the road runs at 12 m and 13 m.
    const stacked = ground(() => 16)
    const [bed] = beds(road(way(1, 0, 100, { under: true, pins: [12, 13] })), stacked)
    expect(bed.level).toBe(true)
    expect(bed.floor[0]).toBe(12)
    expect(bed.floor.at(-1)).toBe(13)
    expect(bed.floor[Math.round(bed.floor.length / 2)]).toBeCloseTo(12.5, 0)
  })
})
