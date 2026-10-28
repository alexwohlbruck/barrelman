import { describe, expect, test } from 'bun:test'
import { unprojected } from '../bridge-mesh/decks'
import { decodeElevation, encodeElevation, encodeRun, runsOf, thin, type RunPoint } from './encode'
import type { Sample } from './match'
import { at } from './test-geometry'

const samples = (heights: Array<number | null>, relative = false, step = 6): Sample[] =>
  heights.map((height, i) => ({ p: at(i * step, 0), d: i * step, height, relative: height === null ? undefined : relative }))

describe('runsOf', () => {
  test('pads a run over the terrain with the ground point either side, where there is one', () => {
    const runs = runsOf(samples([null, null, -1, -2, null, null, null, null, null, null, null, null, null, null, -3], true))
    expect(runs.map(r => r.points.map(p => p.height))).toEqual([[0, -1, -2, 0], [0, -3]])
    expect(runs[0].points[0].d).toBe(6)
    expect(runs.every(r => r.relative)).toBe(true)
  })

  test('meets the ground at a deck\'s own ends, unpadded', () => {
    const [run] = runsOf(samples([null, 12, 14, null]))
    expect(run.relative).toBe(false)
    expect(run.points.map(p => p.height)).toEqual([12, 14])
  })

  test('carries a run across a short gap, but not a long one or into a tunnel', () => {
    expect(runsOf(samples([20, null, null, 22])).map(r => r.points.map(p => p.height))).toEqual([[20, 22]])
    expect(runsOf(samples([20, null, 22], false, 61))).toHaveLength(2)
    const mixed = [...samples([20, 21]), ...samples([-4, -4], true).map((s, i) => ({ ...s, p: at(12 + i * 6, 0), d: 12 + i * 6 }))]
    expect(runsOf(mixed).map(r => r.relative)).toEqual([false, true])
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

  test('keeps the points a curve needs for its path to stay on the line', () => {
    const curve = Array.from({ length: 21 }, (_, i) => ({ p: at(Math.sin(i / 20 * Math.PI / 2) * 200, (1 - Math.cos(i / 20 * Math.PI / 2)) * 200), d: i * 15.7, height: -9 }))
    const kept = thin(curve)
    expect(kept.length).toBeGreaterThan(4)
    expect(thin(curve.map((c, i) => ({ ...c, p: at(i * 15.7, 0) }))).length).toBe(2)
  })

  test('keeps a bump above the tolerance only', () => {
    expect(thin(run([0, 0.1, 0])).length).toBe(2)
    expect(thin(run([0, 0.5, 0])).length).toBe(3)
  })
})

describe('encodeRun', () => {
  test('writes the first point whole, then deltas with absolute heights', () => {
    const points = [at(0, 0), at(6, 0), at(12, 3)]
    const text = encodeRun({ relative: false, points: points.map((p, i) => ({ p, d: i * 6, height: [0, 12, -3.46][i] })) })!
    const n = text.split(',').map(Number)
    const ints = points.map(p => unprojected(p).map(v => Math.round(v * 1e7)))
    expect(n.slice(0, 3)).toEqual([ints[0][0], ints[0][1], 0])
    expect(n.slice(3, 6)).toEqual([ints[1][0] - ints[0][0], ints[1][1] - ints[0][1], 120])
    expect(n.slice(6)).toEqual([ints[2][0] - ints[1][0], ints[2][1] - ints[1][1], -35])
  })

  test('marks a run over the terrain, and drops one that never leaves it', () => {
    const flat = [{ p: at(0, 0), d: 0, height: 0 }, { p: at(6, 0), d: 6, height: 0.02 }]
    expect(encodeRun({ relative: true, points: flat })).toBeNull()
    expect(encodeRun({ relative: true, points: flat.map(p => ({ ...p, height: -9 })) })!.startsWith('~')).toBe(true)
    expect(encodeRun({ relative: false, points: flat })).not.toBeNull()
  })
})

describe('encodeElevation', () => {
  test('round-trips through the decoder, run by run', () => {
    const text = encodeElevation([samples([null, 2, 4, 2, null]), samples([null, -5, -5], true), samples([1, 1])])!
    const runs = decodeElevation(text)
    expect(runs).toHaveLength(3)
    expect(runs[0].map(p => p[2])).toEqual([2, 4, 2])
    expect(runs[1].map(p => p[2])).toEqual([0, -5, -5])
    expect(text.split(';')[1].startsWith('~')).toBe(true)
    const [lng, lat] = unprojected(at(6 * 2, 0))
    expect(runs[0][1][0]).toBeCloseTo(lng, 6)
    expect(runs[0][1][1]).toBeCloseTo(lat, 6)
  })

  test('is null for a line that rides nothing', () => {
    expect(encodeElevation([samples([null, null])])).toBeNull()
  })
})
