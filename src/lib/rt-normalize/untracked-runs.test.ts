import { describe, expect, it } from 'bun:test'
import { untrackedRuns } from './untracked-runs'
import { serviceDayOrigin } from './static-index'

const TZ = 'America/New_York'
const now = serviceDayOrigin('20261112', TZ) + 22 * 3600
const live = { calls: new Map(), trips: new Set(['128700_4..S01R', '129200_4..S01R']), routes: new Set(['4']) }
const run = (tripId: string, live = false) => ({ tripId: `20261112_${tripId}`, routeKey: '5_4', live })

describe('untrackedRuns', () => {
  it('drops an under-way run the agency is silent about on a route it reports', () => {
    const runs = [run('21:17_5_127700_4..S01R'), run('21:27_5_128700_4..S01R', true), run('21:32_5_129200_4..S01R', true)]
    expect([...untrackedRuns(runs, live, TZ, now)]).toEqual(['20261112_21:17_5_127700_4..S01R'])
  })

  it('keeps runs that have not started, and routes the agency mostly does not report', () => {
    expect(untrackedRuns([run('21:58_5_131000_4..S01R'), run('21:27_5_128700_4..S01R', true)], live, TZ, now).size).toBe(0)
    expect(untrackedRuns([run('21:17_5_1_4..S'), run('21:20_5_2_4..S'), run('21:27_5_128700_4..S01R', true)], live, TZ, now).size).toBe(0)
  })

  it('keeps everything when the agency feed is unavailable', () => {
    expect(untrackedRuns([run('21:17_5_127700_4..S01R')], undefined, TZ, now).size).toBe(0)
  })
})
