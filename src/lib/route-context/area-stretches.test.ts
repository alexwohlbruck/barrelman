import { describe, expect, test } from 'bun:test'
import { areaStretches } from './area-stretches'
import { routePath, routePieces, type LonLat } from '../route-hazards/route-path'

const at = (east: number, north = 0): LonLat => [-80.84 + east / (111_320 * Math.cos(35.2 * Math.PI / 180)), 35.2 + north / 111_320]
/** To the nearest 10 m: `at` and haversine measure a kilometre a metre apart. */
const tens = (m: number) => Math.round(m / 10) * 10
/** Eastbound, 4 km, in 1 km pieces. */
const pieces = routePieces(routePath([at(0), at(4000)]), 1000)

describe('areaStretches', () => {
  test('pieces an area covers join into one stretch, cut where the route leaves it', () => {
    const stretches = areaStretches(pieces, [
      { id: 'relation/1', name: 'North Carolina', code: 'US-NC', piece: 0, covers: true },
      { id: 'relation/1', name: 'North Carolina', code: 'US-NC', piece: 1, covers: false, parts: [[at(1000), at(1600)]] },
      { id: 'relation/2', name: 'South Carolina', code: 'US-SC', piece: 1, covers: false, parts: [[at(1600), at(2000)]] },
      { id: 'relation/2', name: 'South Carolina', code: 'US-SC', piece: 2, covers: true },
      { id: 'relation/2', name: 'South Carolina', code: 'US-SC', piece: 3, covers: true },
    ])
    expect(stretches.map(s => [s.code, tens(s.startDistance), tens(s.endDistance)])).toEqual([
      ['US-NC', 0, 1600],
      ['US-SC', 1600, 4000],
    ])
  })

  test('a park visited twice is two stretches; a corner clipped is none', () => {
    const stretches = areaStretches(pieces, [
      { id: 'way/1', name: 'Freedom Park', piece: 0, covers: false, parts: [[at(100), at(400)], [at(700), at(710)], [at(900), at(1000)]] },
      { id: 'way/1', name: 'Freedom Park', piece: 1, covers: false, parts: [[at(1000), at(1050)]] },
    ], { joinMeters: 15, minMeters: 20 })
    expect(stretches.map(s => [tens(s.startDistance), tens(s.endDistance)])).toEqual([[100, 400], [900, 1050]])
  })
})
