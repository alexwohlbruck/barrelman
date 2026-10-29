import { describe, expect, test } from 'bun:test'
import { crossingSpans, nameCrossings } from './crossing-names'
import type { LonLat } from '../route-hazards/route-path'

const at = (east: number, north = 0): LonLat => [-73.99 + east / (111_320 * Math.cos(40.75 * Math.PI / 180)), 40.75 + north / 111_320]
/** North 20 m across a road, along a sidewalk 100 m, then across another road 20 m. */
const coordinates = [at(0, 0), at(0, 20), at(0, 120), at(0, 140)]
const distances = [0, 20, 120, 140]

describe('crossingSpans', () => {
  test('takes road crossings, not railway crossings or plain ways', () => {
    const spans = crossingSpans(coordinates, distances, {
      crossing: [[0, 1, 'traffic_signals'], [1, 2, 'missing'], [2, 3, 'railway']],
    })
    expect(spans.map(s => [s.from, s.to, s.startDistance, s.endDistance])).toEqual([[0, 1, 0, 20]])
  })
})

describe('nameCrossings', () => {
  test('names each crossing for the road met nearest its middle', () => {
    const spans = crossingSpans(coordinates, distances, { crossing: [[0, 1, 'marked'], [2, 3, 'unmarked']] })
    const names = nameCrossings(spans, [
      { span: 0, name: 'West 34th Street', at: at(0, 10) },
      { span: 0, name: 'Herald Square', at: at(0, 19) },
      { span: 1, name: '7th Avenue', at: at(0, 131) },
    ])
    expect(names).toEqual([
      { startDistance: 0, endDistance: 20, name: 'West 34th Street' },
      { startDistance: 120, endDistance: 140, name: '7th Avenue' },
    ])
  })
})
