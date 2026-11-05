import { describe, test, expect } from 'bun:test'
import { cellRanges } from './large-area-search.service'

describe('cellRanges', () => {
  test("Midtown's typeahead box is two grid columns of two cells", () => {
    // 50 km either side of 40.76, -73.98 reaches longitudes -75 and -74 and
    // latitudes 40 and 41.
    expect(cellRanges(-73.98, 40.7649, 50_000 / 111320)).toEqual([
      [105 * 180 + 130, 105 * 180 + 131],
      [106 * 180 + 130, 106 * 180 + 131],
    ])
  })

  test('a box inside one cell is one cell', () => {
    expect(cellRanges(-80.5, 35.5, 0.1)).toEqual([[99 * 180 + 125, 99 * 180 + 125]])
  })

  test('a box spanning more than four columns gives up', () => {
    expect(cellRanges(-80.5, 35.5, 3)).toBeNull()
  })

  test('stays on the grid at the poles and the antimeridian', () => {
    for (const [lo, hi] of cellRanges(179.9, 89.9, 0.449)!) {
      expect(lo).toBeGreaterThanOrEqual(0)
      expect(hi).toBeLessThan(360 * 180)
    }
  })
})
