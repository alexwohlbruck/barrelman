import { describe, expect, it } from 'bun:test'
import { mercator, resample, type Point } from './profile'
import { CAP_MAX, shapeDecks } from './shape'

// Metres east and north of a point in Charlotte. A deck running east has its
// left side to the south: left is the side `edgePoints` offsets toward.
const at = (east: number, north = 0): Point =>
  mercator(-80.83 + east / (111320 * Math.cos((35.22 * Math.PI) / 180)), 35.22 + north / 110574)
const deck = (from: number, to: number, north = 0, half = 4, layer = 1) =>
  ({ points: resample([at(from, north), at(to, north)], 6), edges: [half, half] as [number, number], layer })
const outline = (id: string, corners: Array<[number, number]>) => ({ id, rings: [[...corners, corners[0]].map(([e, n]) => at(e, n))] })

describe('shapeDecks', () => {
  it('follows an outline that widens along the deck', () => {
    // 6 m either side at the west end, 12 m at the east.
    const flare = outline('way/1', [[0, 6], [60, 12], [60, -12], [0, -6]])
    const [shape] = shapeDecks([deck(0, 60)], [flare], [[false, false]])
    const [left, right] = shape!.sides
    expect(left[1]).toBeCloseTo(6.6, 0)
    expect(left[5]).toBeCloseTo(9, 0)
    expect(right[8]).toBeCloseTo(10.8, 0)
    expect(left[0]).toBeLessThan(left[9])
  })

  it('never narrows a deck below its carriageway', () => {
    const tight = outline('way/1', [[0, 2], [60, 2], [60, -2], [0, -2]])
    const [shape] = shapeDecks([deck(0, 60, 0, 4)], [tight], [[false, false]])
    expect(Math.min(...shape!.sides[0], ...shape!.sides[1])).toBe(4)
  })

  it('leaves a deck in no outline alone', () => {
    const far = outline('way/1', [[0, 100], [60, 100], [60, 90], [0, 90]])
    expect(shapeDecks([deck(0, 60)], [far], [[false, false]])).toEqual([null])
  })

  it('splits an outline between twin carriageways, each reaching its own side', () => {
    // Centrelines 16 m apart in one outline 32 m across.
    const both = outline('way/1', [[0, 16], [60, 16], [60, -16], [0, -16]])
    const [north, south] = shapeDecks([deck(0, 60, 8), deck(0, 60, -8)], [both], [[false, false], [false, false]])
    const middle = 5
    // North deck: its right (north) side runs to the outline, its left meets the south deck's halfway.
    expect(north!.sides[1][middle]).toBeCloseTo(8, 0)
    expect(north!.sides[0][middle]).toBeCloseTo(8, 0)
    expect(south!.sides[1][middle] + north!.sides[0][middle]).toBeCloseTo(16, 0)
  })

  it('is not pinched by a deck crossing over it', () => {
    const wide = outline('way/1', [[0, 10], [60, 10], [60, -10], [0, -10]])
    const over = { points: resample([at(10, -40), at(50, 40)], 6), edges: [4, 4] as [number, number], layer: 2 }
    const [shape] = shapeDecks([deck(0, 60), over], [wide], [[false, false], [false, false]])
    expect(Math.min(...shape!.sides[0].slice(1, -1))).toBeCloseTo(10, 0)
  })

  it('moves a grounded end to the skewed end of its outline', () => {
    // The west end of the outline is skewed: its south corner 8 m short of the
    // deck's start, its north corner 8 m past it; the east end is square.
    const skew = outline('way/1', [[-8, 6], [60, 6], [60, -6], [8, -6]])
    const [shape] = shapeDecks([deck(0, 60)], [skew], [[true, true]])
    const [startLeft, startRight, endLeft, endRight] = shape!.caps
    // Left is south: trimmed back to the outline; right (north) runs on past
    // the start. Each corner is taken half a metre in from the edge, 5.5 m out.
    expect(startLeft).toBeCloseTo(8 - 0.5 * (16 / 12), 0)
    expect(startRight).toBeCloseTo(-(8 - 0.5 * (16 / 12)), 0)
    expect(Math.abs(endLeft)).toBeLessThan(0.1)
    expect(Math.abs(endRight)).toBeLessThan(0.1)
  })

  it('leaves an end resting on another deck, and one far from its outline end, where it is', () => {
    const skew = outline('way/1', [[-8, 6], [60, 6], [60, -6], [8, -6]])
    expect(shapeDecks([deck(0, 60)], [skew], [[false, false]])[0]!.caps).toEqual([0, 0, 0, 0])
    const long = outline('way/1', [[-CAP_MAX - 30, 6], [60, 6], [60, -6], [-CAP_MAX - 30, -6]])
    expect(shapeDecks([deck(0, 60)], [long], [[true, false]])[0]!.caps).toEqual([0, 0, 0, 0])
  })
})
