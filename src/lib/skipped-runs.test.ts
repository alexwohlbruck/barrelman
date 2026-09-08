/**
 * The parade-day rule: a run the agency's skip alert disowns comes off the
 * board; the runs after the window stay, because their times say when the
 * station reopens.
 */

import { describe, test, expect } from 'bun:test'
import { dropSkippedRuns } from './skipped-runs'

const HOUR = 3_600_000
const iso = (ms: number) => new Date(ms).toISOString()
const run = (routeId: string, at: number) => ({
  route: { id: routeId },
  departureTime: iso(at),
})

// 2/3/4 skip station 238 from an hour ago until an hour from now.
const NOW = Date.now()
const skip = {
  effect: 'DETOUR',
  activePeriods: [{ start: iso(NOW - HOUR), end: iso(NOW + HOUR) }],
  informedEntities: [
    { routeId: '2', stopId: '238' },
    { routeId: '3', stopId: '238' },
    { routeId: '4', stopId: '238' },
  ],
}

describe('dropSkippedRuns', () => {
  test('drops runs inside the window, keeps the ones after it', () => {
    const out = dropSkippedRuns(
      [run('2', NOW + 5 * 60_000), run('4', NOW + 2 * HOUR)],
      [skip],
      ['238'],
    )
    expect(out.map((d) => d.route.id)).toEqual(['4'])
  })

  test('leaves other routes and other stations alone', () => {
    expect(dropSkippedRuns([run('5', NOW)], [skip], ['238'])).toHaveLength(1)
    expect(dropSkippedRuns([run('2', NOW)], [skip], ['999'])).toHaveLength(1)
  })

  test('matches through any id in the station family', () => {
    // The board's stop is the platform; the alert names the station.
    expect(dropSkippedRuns([run('3', NOW)], [skip], ['238N', '238'])).toHaveLength(0)
  })

  test('an alert with no window is in effect until lifted', () => {
    const openEnded = { ...skip, activePeriods: [] }
    expect(dropSkippedRuns([run('2', NOW + 9 * HOUR)], [openEnded], ['238'])).toHaveLength(0)
  })

  test('non-skip effects drop nothing', () => {
    const delay = { ...skip, effect: 'SIGNIFICANT_DELAYS' }
    expect(dropSkippedRuns([run('2', NOW)], [delay], ['238'])).toHaveLength(1)
  })

  test('keeps a run it cannot time', () => {
    expect(dropSkippedRuns([{ route: { id: '2' } }], [skip], ['238'])).toHaveLength(1)
  })
})
