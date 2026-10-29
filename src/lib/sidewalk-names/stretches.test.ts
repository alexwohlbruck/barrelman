import { describe, expect, test } from 'bun:test'
import { nameSegments, streetStretches, type NameableSegment, type WayNames } from './stretches'
import type { XY } from './zones'

// About 100 m of sidewalk running east, turning a corner at its 60 m mark.
const lat = 40.74
const east = (m: number): XY => [-73.99 + m / (111_320 * Math.cos((lat * Math.PI) / 180)), lat]
const wrapping: WayNames = {
  coords: [east(0), east(100)],
  zones: [
    { from: 0, to: 0.6, name: 'W 3rd St', source: 'geometry' },
    { from: 0.6, to: 1, name: 'Main St', source: 'geometry' },
  ],
}

describe('streetStretches', () => {
  test('maps a way’s zones onto the route, whichever way the route walks it', () => {
    // 20 m on an unknown way, then the sidewalk from its far end back to its start.
    const coordinates = [east(120), east(100), east(50), east(0)]
    const distances = [0, 20, 70, 120]
    const stretches = streetStretches(coordinates, distances, [[0, 1, 1], [1, 3, 7]], new Map([[7, wrapping]]))
    expect(stretches).toEqual([
      { startDistance: 20, endDistance: 60, name: 'Main St', source: 'geometry' },
      { startDistance: 60, endDistance: 120, name: 'W 3rd St', source: 'geometry' },
    ])
  })

  test('joins one street carried on across consecutive ways', () => {
    const second: WayNames = { coords: [east(100), east(150)], zones: [{ from: 0, to: 1, name: 'Main St', source: 'geometry' }] }
    const coordinates = [east(70), east(100), east(150)]
    const stretches = streetStretches(coordinates, [0, 30, 80], [[0, 1, 7], [1, 2, 8]], new Map([[7, wrapping], [8, second]]))
    expect(stretches).toEqual([{ startDistance: 0, endDistance: 80, name: 'Main St', source: 'geometry' }])
  })
})

describe('nameSegments', () => {
  test('splits a segment where the street changes inside it', () => {
    const segments: Array<NameableSegment & { roadClass: string }> = [{ startDistance: 0, endDistance: 100, roadClass: 'footway' }]
    const named = nameSegments(segments, [
      { startDistance: 10, endDistance: 60, name: 'W 3rd St', source: 'geometry' },
      { startDistance: 60, endDistance: 100, name: 'Main St', source: 'tag' },
    ])
    expect(named).toEqual([
      { startDistance: 0, endDistance: 10, roadClass: 'footway' },
      { startDistance: 10, endDistance: 60, roadClass: 'footway', streetName: 'W 3rd St', streetNameSource: 'geometry' },
      { startDistance: 60, endDistance: 100, roadClass: 'footway', streetName: 'Main St', streetNameSource: 'tag' },
    ])
  })
})

test('a route crossing the seam of a closed sidewalk keeps both sides named', () => {
  const north = (m: number, [lon, l]: XY): XY => [lon, l + m / 111_320]
  const corner = east(0)
  const ring: WayNames = {
    coords: [corner, east(40), north(40, east(40)), north(40, corner), corner],
    zones: [
      { from: 0, to: 0.5, name: 'W 3rd St', source: 'geometry' },
      { from: 0.5, to: 1, name: 'Main St', source: 'geometry' },
    ],
  }
  const coordinates = [north(20, corner), corner, east(20)]
  const stretches = streetStretches(coordinates, [0, 20, 40], [[0, 2, 9]], new Map([[9, ring]]))
  expect(stretches).toEqual([
    { startDistance: 0, endDistance: 20, name: 'Main St', source: 'geometry' },
    { startDistance: 20, endDistance: 40, name: 'W 3rd St', source: 'geometry' },
  ])
})
