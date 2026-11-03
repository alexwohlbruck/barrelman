import { describe, expect, test } from 'bun:test'
import { crossingStretches, cyclewaySide, nodeMarks, routeWays, tagEdgeSegments, wayEdgeTags } from './edge-tags'
import type { CrossingSpan } from './crossing-names'

describe('wayEdgeTags', () => {
  test('names the passage a walkway runs through', () => {
    expect(wayEdgeTags({ highway: 'footway', tunnel: 'building_passage' }).passage).toBe('building')
    expect(wayEdgeTags({ highway: 'footway', covered: 'arcade' }).passage).toBe('arcade')
    expect(wayEdgeTags({ highway: 'corridor' }).passage).toBe('indoor')
    expect(wayEdgeTags({ highway: 'footway', indoor: 'corridor' }).passage).toBe('indoor')
    expect(wayEdgeTags({ highway: 'footway', covered: 'yes' }).passage).toBeUndefined()
  })

  test('keeps a level indoors or off the ground, not a sidewalk\'s level 0', () => {
    expect(wayEdgeTags({ highway: 'footway', level: '0' }).level).toBeUndefined()
    expect(wayEdgeTags({ highway: 'corridor', level: '0' }).level).toBe('0')
    expect(wayEdgeTags({ highway: 'footway', level: '-1' }).level).toBe('-1')
  })

  test('reads escalators and elevators', () => {
    expect(wayEdgeTags({ highway: 'steps', conveying: 'backward' })).toMatchObject({ conveying: true })
    expect(wayEdgeTags({ highway: 'steps', conveying: 'no' }).conveying).toBeUndefined()
    expect(wayEdgeTags({ highway: 'elevator' })).toMatchObject({ elevator: true })
  })

  test('reads islands, buttons and diagonals off crossing ways only', () => {
    expect(wayEdgeTags({ highway: 'footway', footway: 'crossing', 'crossing:island': 'yes', button_operated: 'yes' }))
      .toEqual({ crossingIsland: true, buttonOperated: true })
    expect(wayEdgeTags({ highway: 'footway', footway: 'crossing', 'crossing:scramble': 'diagonal' })).toEqual({ crossingDiagonal: true })
    expect(wayEdgeTags({ highway: 'footway', button_operated: 'yes' })).toEqual({})
  })
})

describe('cyclewaySide', () => {
  test('flips a way\'s sides for a rider going against its nodes', () => {
    const tags = { highway: 'residential', 'cycleway:right': 'lane' }
    expect(cyclewaySide(tags, true, 'right')).toBe('right')
    expect(cyclewaySide(tags, false, 'right')).toBe('left')
  })

  test('puts a one-way\'s plain lane on the side its traffic keeps to', () => {
    expect(cyclewaySide({ oneway: 'yes', cycleway: 'lane' }, true, 'right')).toBe('right')
    expect(cyclewaySide({ oneway: 'yes', cycleway: 'lane' }, true, 'left')).toBe('left')
    expect(cyclewaySide({ oneway: '-1', cycleway: 'lane' }, false, 'right')).toBe('right')
  })

  test('puts a contraflow lane against the traffic', () => {
    expect(cyclewaySide({ oneway: 'yes', cycleway: 'opposite_lane' }, false, 'right')).toBe('right')
    expect(cyclewaySide({ oneway: 'yes', cycleway: 'opposite_lane' }, true, 'right')).toBe('left')
  })

  test('a two-way street\'s plain lane is on both sides; sharrows and separate paths are none', () => {
    expect(cyclewaySide({ cycleway: 'lane' }, true, 'right')).toBe('both')
    expect(cyclewaySide({ 'cycleway:both': 'track' }, false, 'right')).toBe('both')
    expect(cyclewaySide({ cycleway: 'shared_lane' }, true, 'right')).toBeUndefined()
    expect(cyclewaySide({ 'cycleway:right': 'separate' }, true, 'right')).toBeUndefined()
  })
})

describe('crossingStretches', () => {
  const span = (startDistance: number, endDistance: number): CrossingSpan => ({ from: 0, to: 1, line: [], startDistance, endDistance })

  test('both halves of a crossing split by an island way cross to a refuge', () => {
    const spans = [span(10, 18), span(20, 28), span(60, 70)]
    const ways = [{ osmWayId: 1, startDistance: 18, endDistance: 20 }, { osmWayId: 2, startDistance: 20, endDistance: 60 }]
    const records = new Map([[1, { tags: { highway: 'footway', footway: 'traffic_island' } }], [2, { tags: { highway: 'footway' } }]])
    expect(crossingStretches(spans, [], ways, records).map(s => [s.startDistance, s.crossingIsland])).toEqual([[10, true], [20, true]])
  })

  test('a node past the crossing\'s end belongs to the next way', () => {
    const nodes = [{ distance: 14, tags: { 'crossing:island': 'yes' } }, { distance: 25, tags: { button_operated: 'yes' } }]
    expect(crossingStretches([span(10, 20)], nodes, [], new Map())).toEqual([{ startDistance: 10, endDistance: 20, crossingIsland: true }])
  })
})

describe('nodeMarks and tagEdgeSegments', () => {
  const segments = [{ startDistance: 0, endDistance: 40 }, { startDistance: 40, endDistance: 55 }, { startDistance: 55, endDistance: 90 }]

  test('an entrance starts the edge leaving it, and one walked past mid-edge is no entrance', () => {
    const marks = nodeMarks([
      { distance: 40.6, tags: { railway: 'subway_entrance', name: '34 St', ref: 'A' } },
      { distance: 70, tags: { railway: 'subway_entrance', name: 'Elsewhere' } },
    ])
    const tagged = tagEdgeSegments(segments, [], marks)
    expect(tagged.map(s => s.entrance)).toEqual([undefined, { name: '34 St', ref: 'A' }, undefined])
  })

  test('falls back to local_ref and marks elevators', () => {
    const marks = nodeMarks([{ distance: 55, tags: { public_transport: 'entrance', local_ref: '3' } }, { distance: 0, tags: { highway: 'elevator' } }])
    expect(marks).toEqual([{ distance: 55, entrance: { ref: '3' } }, { distance: 0, elevator: true }])
  })

  test('a stretch tags the segments whose middles it covers', () => {
    const tagged = tagEdgeSegments(segments, [{ startDistance: 40, endDistance: 90, passage: 'indoor' }], [])
    expect(tagged.map(s => s.passage)).toEqual([undefined, 'indoor', 'indoor'])
  })
})

describe('routeWays', () => {
  test('measures each way and keeps its country', () => {
    const details = { osm_way_id: [[0, 2, 7], [2, 3, 8]], country: [[0, 3, 'GBR']] }
    expect(routeWays([0, 10, 25, 40], details)).toEqual([
      { osmWayId: 7, startDistance: 0, endDistance: 25, country: 'GBR' },
      { osmWayId: 8, startDistance: 25, endDistance: 40, country: 'GBR' },
    ])
  })
})
