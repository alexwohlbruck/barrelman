import { describe, expect, test } from 'bun:test'
import { structuresOf, type OutlineRow } from './bridge-meshes'

const square = (x: number): number[][][][] => [[[[x, 0], [x + 0.001, 0], [x + 0.001, 0.001], [x, 0.001], [x, 0]]]]
const row = (landmark: string, bridge: boolean, x: number): OutlineRow => ({ landmark, bridge, coords: square(x), lng: 0, lat: 0, radius: 500 })

describe('structuresOf', () => {
  test("a bridge model's replaced supports count with its outline", () => {
    expect(structuresOf([row('bridge', true, 0), row('bridge', false, 0.002)])).toHaveLength(2)
  })

  test("a tower's model leaves the deck past it to the map", () => {
    expect(structuresOf([row('tower', false, 0)])).toHaveLength(0)
  })
})
