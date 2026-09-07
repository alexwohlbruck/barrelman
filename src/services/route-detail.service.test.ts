/**
 * Tests for telling a route's stops from the ones it only ever reached on a
 * reroute.
 *
 * The numbers are the measured MTA feed, because the whole question is where
 * the line falls in real data: the R's Second Av and West End strays are one
 * trip out of 735, while the 2's genuine late-night run to New Lots Av is nine
 * and the N's to Second Av is twelve.
 */

import { describe, test, expect } from 'bun:test'
import { servedStops } from './route-detail.service'

const stop = (stopId: string, trips: number) => ({ stopId, trips })
const ids = (stops: Array<{ stopId: string }>) => stops.map((s) => s.stopId)

describe('servedStops', () => {
  test('drops the stops a route reaches on a single trip', () => {
    const kept = servedStops([
      stop('R31', 735),
      stop('R16', 580),
      stop('Q03', 1),
      stop('B12', 1),
    ])
    expect(ids(kept)).toEqual(['R31', 'R16'])
  })

  test('keeps a real but infrequent branch', () => {
    // The 2 to New Lots Av: nine trips against a busiest stop of 848. Rare
    // enough to fall under any share worth using, and real service.
    const kept = servedStops([stop('248', 848), stop('257', 9)])
    expect(ids(kept)).toEqual(['248', '257'])
  })

  test('keeps every stop of a route that barely runs at all', () => {
    // Two trips a day is the whole timetable, not a rounding error in one.
    const kept = servedStops([stop('A', 2), stop('B', 2), stop('C', 2)])
    expect(ids(kept)).toEqual(['A', 'B', 'C'])
  })

  test('keeps a stop held up by its share alone', () => {
    // Below the absolute floor, but a fiftieth of the busiest stop.
    const kept = servedStops([stop('A', 100), stop('B', 2)])
    expect(ids(kept)).toEqual(['A', 'B'])
  })

  test('keeps everything when no stop has a count', () => {
    // A feed imported before patterns carried counts. Silence is not a verdict.
    const kept = servedStops([stop('A', 0), stop('B', 0), stop('C', 0)])
    expect(ids(kept)).toEqual(['A', 'B', 'C'])
  })

  test('drops a stop no pattern reaches once counts exist', () => {
    const kept = servedStops([stop('A', 500), stop('B', 0)])
    expect(ids(kept)).toEqual(['A'])
  })

  test('handles an empty route', () => {
    expect(servedStops([])).toEqual([])
  })
})
