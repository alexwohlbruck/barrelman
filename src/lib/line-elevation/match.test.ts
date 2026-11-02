import { describe, expect, test } from 'bun:test'
import { sampleLine, SourceIndex } from './match'
import type { HeightSource } from './sources'
import { at } from './test-geometry'

const deck = (extra: Partial<HeightSource> = {}): HeightSource => ({
  points: [at(100, 0), at(200, 0)], heights: [0, 10], relative: false, lifts: [0, 10], half: 5, kind: 'rail', ways: [42], ...extra,
})

describe('sampleLine', () => {
  test('a line on its own way rides it, and is on the ground past it', () => {
    const samples = sampleLine([at(0, 0), at(300, 0)], new SourceIndex([deck()]), { way: 42 })
    const on = samples.filter(s => s.height !== null)
    expect(on[0].d).toBeGreaterThan(99.9)
    expect(on.at(-1)!.d).toBeLessThan(200.1)
    expect(samples.find(s => Math.abs(s.d - 150) < 3)!.height).toBeCloseTo(5, 0)
  })

  test('a way it is not made of is never ridden by id', () => {
    const samples = sampleLine([at(0, 0), at(300, 0)], new SourceIndex([deck()]), { way: 7 })
    expect(samples.every(s => s.height === null)).toBe(true)
  })

  test('a line alongside within the deck plus slack rides it, in either direction', () => {
    const index = new SourceIndex([deck({ lifts: [0, 0] })])
    for (const line of [[at(0, 8), at(300, 8)], [at(300, 8), at(0, 8)]]) {
      const samples = sampleLine(line, index, { kinds: ['rail'] })
      expect(samples.some(s => s.height !== null)).toBe(true)
    }
    expect(sampleLine([at(0, 10), at(300, 10)], index, { kinds: ['rail'] }).every(s => s.height === null)).toBe(true)
  })

  test('a line crossing a deck, or of another kind, does not ride it', () => {
    const index = new SourceIndex([deck()])
    expect(sampleLine([at(150, -50), at(150, 50)], index, { kinds: ['rail'] }).every(s => s.height === null)).toBe(true)
    expect(sampleLine([at(0, 0), at(300, 0)], index, { kinds: ['road'] }).every(s => s.height === null)).toBe(true)
  })

  test('keeps to the tunnel it arrives in where a viaduct runs over it', () => {
    const tunnel = deck({ points: [at(0, 0), at(600, 0)], heights: [-9, -9], lifts: [-9, -9], relative: true, ways: [1] })
    const viaduct = deck({ points: [at(200, 1), at(400, 1)], heights: [30, 30], lifts: [12, 12], ways: [2] })
    const samples = sampleLine([at(0, 0), at(600, 0)], new SourceIndex([tunnel, viaduct]), { kinds: ['rail'] })
    expect(samples.every(s => s.height === -9 && s.relative)).toBe(true)
  })

  test('does not climb onto a deck it only grazes', () => {
    const graze = deck({ points: [at(100, 0), at(118, 0)], heights: [20, 20], lifts: [8, 8] })
    expect(sampleLine([at(0, 0), at(300, 0)], new SourceIndex([graze]), { kinds: ['rail'] }).every(s => s.height === null)).toBe(true)
  })

  test('a line drawn along its ways rides their deck even where its drawing strays off it, and nothing else', () => {
    const own = deck({ ways: [9], heights: [20, 20], lifts: [10, 10] })
    const other = deck({ points: [at(100, 15), at(200, 15)], ways: [5], heights: [40, 40], lifts: [30, 30] })
    const samples = sampleLine([at(100, 15), at(200, 15)], new SourceIndex([own, other]), { ways: [9] })
    expect(samples.every(s => s.height === 20)).toBe(true)
  })
})

