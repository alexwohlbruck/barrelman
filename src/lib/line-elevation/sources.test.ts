import { describe, expect, test } from 'bun:test'
import { along, mercator, unprojected } from '../bridge-mesh/decks'
import type { Way } from '../tunnel-cuts/build'
import { PROFILES } from '../tunnel-cuts/tunnels'
import { deckSource, DEPTH, tunnelSources, TUNNEL_MARGIN } from './sources'
import { at } from './test-geometry'

const portals = [at(0, 0), at(600, 0), at(-100, 0), at(400, 0)]
const read = { minX: at(-2000, 0)[0], maxX: at(2000, 0)[0], minY: at(0, 2000)[1], maxY: at(0, -2000)[1] }
const tunnel = (id: number, from: number, to: number, extra: Partial<Way> = {}): Way =>
  ({ id, points: [at(from, 0), at(to, 0)], cls: 'rail', layer: -1, brunnel: 'tunnel', ...extra })
const heightAtMetre = (s: { points: any[]; heights: number[] }, m: number) => {
  const d = along(s.points)
  return s.heights[d.findIndex(v => Math.abs(v - m) < 0.5)]
}

describe('deckSource', () => {
  test('gives the height its surface is drawn at, at each vertex, sampled every step', () => {
    const coords = [0, 6, 12].map(m => unprojected(at(m, 0)))
    const source = deckSource({ ways: ['7', '8'], kind: 'rail', edges: [3, 4], heights: [10, 16, 12], ground: [10, 9, 12], step: 6, coords }, mercator)!
    expect(source.ways).toEqual([7, 8])
    expect(source.half).toBe(4)
    expect(source.heights.map(h => Math.round(h * 10) / 10)).toEqual([10, 16, 12])
    expect(source.relative).toBe(false)
  })

  test('refuses a row whose heights and ground disagree', () => {
    expect(deckSource({ ways: [1], kind: 'road', edges: [3, 3], heights: [1, 2], ground: [1], step: 6, coords: [[0, 0], [0, 1]] }, mercator)).toBeNull()
  })
})

describe('tunnelSources', () => {
  test('sinks a lone tunnel from both portals to its depth at its grade', () => {
    const [source] = tunnelSources([tunnel(1, 0, 600)], read, portals)
    const { grade } = PROFILES.rail
    expect(heightAtMetre(source, 0)).toBeCloseTo(0, 5)
    expect(heightAtMetre(source, 120)).toBeCloseTo(-grade * 120, 1)
    expect(heightAtMetre(source, 300)).toBeCloseTo(-DEPTH.rail, 5)
    expect(heightAtMetre(source, 600)).toBeCloseTo(0, 5)
    expect(source.ways).toEqual([1])
    expect(source.relative).toBe(true)
  })

  test('measures along ways joined end to end, so a joint is not a portal', () => {
    const [a, b] = tunnelSources([tunnel(1, 0, 100), tunnel(2, 100, 200)], read, portals)
    const { grade } = PROFILES.rail
    expect(a.heights.at(-1)).toBeCloseTo(-grade * 100, 1)
    expect(b.heights[0]).toBeCloseTo(-grade * 100, 1)
  })

  test('an end outside the box read is no portal', () => {
    const [source] = tunnelSources([tunnel(1, 0, 3000)], read, portals)
    expect(source.heights[0]).toBeCloseTo(0, 5)
    expect(source.heights.at(-1)).toBeCloseTo(-DEPTH.rail, 5)
  })

  test('an end with no solved portal near it, as at a station mapped without the tag, stays deep', () => {
    const [source] = tunnelSources([tunnel(1, 0, 1000)], read, [at(0, 0)])
    expect(source.heights[0]).toBeCloseTo(0, 5)
    expect(source.heights.at(-1)).toBeCloseTo(-DEPTH.rail, 5)
  })

  test('only joins ways of one kind, and ignores tunnels at grade', () => {
    const sources = tunnelSources([tunnel(1, 0, 100), tunnel(2, 100, 200, { cls: 'minor' }), tunnel(3, 300, 400, { layer: 0 })], read, [at(0, 0), at(100, 0)])
    expect(sources).toHaveLength(2)
    expect(sources[0].heights.at(-1)).toBeCloseTo(0, 5)
    expect(sources[1].kind).toBe('road')
  })

  test('lies at one depth whatever its layer, and is read far enough out to climb from it', () => {
    const [shallow, deep] = tunnelSources([tunnel(1, -1500, -1000), tunnel(2, 1000, 1500, { layer: -3 })], read, [])
    expect(Math.min(...shallow.heights)).toBeCloseTo(Math.min(...deep.heights), 5)
    expect(TUNNEL_MARGIN).toBeGreaterThanOrEqual(DEPTH.rail / PROFILES.rail.grade)
  })
})
