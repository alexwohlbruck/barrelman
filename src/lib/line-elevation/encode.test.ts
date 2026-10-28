import { describe, expect, test } from 'bun:test'
import { unprojected } from '../bridge-mesh/decks'
import { decodeElevation, encodeElevation, encodeRun, runsOf, thin, type RunPoint } from './encode'
import type { Sample } from './match'
import { at } from './test-geometry'

const samples = (heights: Array<number | null>): Sample[] => heights.map((height, i) => ({ p: at(i * 6, 0), d: i * 6, height }))

describe('runsOf', () => {
  test('pads each run with the ground point either side, where there is one', () => {
    const runs = runsOf(samples([null, null, 1, 2, null, 3]))
    expect(runs.map(r => r.map(p => p.height))).toEqual([[0, 1, 2, 0], [0, 3]])
    expect(runs[0][0].d).toBe(6)
  })
})

describe('thin', () => {
  const run = (heights: number[]): RunPoint[] => heights.map((height, i) => ({ p: at(i * 6, 0), d: i * 6, height }))

  test('drops points a straight climb already passes through', () => {
    expect(thin(run([0, 1, 2, 3, 4])).map(p => p.height)).toEqual([0, 4])
  })

  test('keeps the corners of a hump, and both ends', () => {
    expect(thin(run([0, 3, 6, 6, 6, 3, 0])).map(p => p.d)).toEqual([0, 12, 24, 36])
  })

  test('keeps a bump above the tolerance only', () => {
    expect(thin(run([0, 0.1, 0])).length).toBe(2)
    expect(thin(run([0, 0.5, 0])).length).toBe(3)
  })
})

describe('encodeRun', () => {
  test('writes the first point whole, then deltas with absolute heights', () => {
    const points = [at(0, 0), at(6, 0), at(12, 3)]
    const text = encodeRun(points.map((p, i) => ({ p, d: i * 6, height: [0, 12, -3.46][i] })))!
    const n = text.split(',').map(Number)
    const ints = points.map(p => unprojected(p).map(v => Math.round(v * 1e7)))
    expect(n.slice(0, 3)).toEqual([ints[0][0], ints[0][1], 0])
    expect(n.slice(3, 6)).toEqual([ints[1][0] - ints[0][0], ints[1][1] - ints[0][1], 120])
    expect(n.slice(6)).toEqual([ints[2][0] - ints[1][0], ints[2][1] - ints[1][1], -35])
  })

  test('a run that never leaves the ground is dropped', () => {
    expect(encodeRun([{ p: at(0, 0), d: 0, height: 0 }, { p: at(6, 0), d: 6, height: 0.02 }])).toBeNull()
  })
})

describe('encodeElevation', () => {
  test('round-trips through the decoder, run by run', () => {
    const text = encodeElevation([samples([null, 2, 4, 2, null, null, -5, -5]), samples([1, 1])])!
    const runs = decodeElevation(text)
    expect(runs).toHaveLength(3)
    expect(runs[0].map(p => p[2])).toEqual([0, 4, 0])
    expect(runs[1].map(p => p[2])).toEqual([0, -5, -5])
    const [lng, lat] = unprojected(at(6 * 2, 0))
    expect(runs[0][1][0]).toBeCloseTo(lng, 6)
    expect(runs[0][1][1]).toBeCloseTo(lat, 6)
  })

  test('is null for a line that rides nothing', () => {
    expect(encodeElevation([samples([null, null])])).toBeNull()
  })
})
