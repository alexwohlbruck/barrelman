import { describe, expect, it } from 'bun:test'
import { clip, contains, grid, toUnits } from './grid'

describe('grid', () => {
  it('puts every value in the cell whose box holds it, edges included', () => {
    for (const size of [250_000, 2_500_000, 200_000]) {
      const g = grid(size)
      for (let i = 0; i <= 36_000; i++) {
        // Every 0.01° from -180, each edge itself and a hair either side.
        for (const lng of [-180 + i / 100, -180 + i / 100 - 1e-9, -180 + i / 100 + 1e-9]) {
          const p: [number, number] = [toUnits(lng), toUnits(lng / 2)]
          if (!contains(g.box(g.at(p)), p)) throw new Error(`${lng} at ${size}: ${g.at(p)}`)
        }
      }
    }
    const p: [number, number] = [toUnits(-163.58), 0]
    expect(contains(grid(250_000).box(grid(250_000).at(p)), p)).toBe(true)
  })

  it('splits a box between neighbouring cells with nothing shared or lost', () => {
    const g = grid(250_000)
    const [a, b] = [g.box('0,0'), g.box('1,0')]
    expect(a[2]).toBe(b[0])
    expect(contains(a, [a[2], 0])).toBe(false)
    expect(contains(b, [a[2], 0])).toBe(true)
  })

  it('clips to where boxes overlap, and to nothing where they only touch', () => {
    expect(clip([0, 0, 10, 10], [5, -5, 20, 5])).toEqual([5, 0, 10, 5])
    expect(clip([0, 0, 10, 10], [10, 0, 20, 10])).toBeNull()
  })

  it('refuses a cell that is not a whole number of units', () => {
    expect(() => grid(0)).toThrow()
    expect(() => grid(2.5)).toThrow()
  })
})
