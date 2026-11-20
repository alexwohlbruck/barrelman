import { describe, expect, it } from 'bun:test'
import { buildQueuePlacements, MAX_NAMED_STAGES, nextStages, type QueueRow } from './ops-job-store'
import { isExclusive } from './job-invocation'
import { getScript } from '../admin/scripts-manifest'

const row = (id: string, status: QueueRow['status']): QueueRow => ({
  id,
  script_id: `script-${id}`,
  script_name: `Script ${id}`,
  status,
})

describe('buildQueuePlacements', () => {
  it('chains each queued job to the one immediately ahead of it', () => {
    const p = buildQueuePlacements([row('a', 'running'), row('b', 'queued'), row('c', 'queued')])

    expect(p.get('a')).toBeUndefined() // a running job isn't waiting on anything
    expect(p.get('b')).toEqual({
      position: 1,
      waitingOn: { id: 'a', scriptId: 'script-a', scriptName: 'Script a', status: 'running' },
    })
    // Not the running job — c only starts once b has finished too.
    expect(p.get('c')).toEqual({
      position: 2,
      waitingOn: { id: 'b', scriptId: 'script-b', scriptName: 'Script b', status: 'queued' },
    })
  })

  it('leaves the head of an idle queue with no blocker', () => {
    expect(buildQueuePlacements([row('a', 'queued')]).get('a')).toEqual({ position: 1, waitingOn: undefined })
  })

  it('returns nothing when the worker is busy but nothing is waiting', () => {
    expect(buildQueuePlacements([row('a', 'running')]).size).toBe(0)
    expect(buildQueuePlacements([]).size).toBe(0)
  })
})

describe('isExclusive', () => {
  it('treats the nightly OSM diff as single-flight, so a schedule cannot overlap a slow run', () => {
    expect(isExclusive(getScript('osm-update')!)).toBe(true)
  })

  it('implies exclusivity from longRunning when it is not stated', () => {
    const implied = getScript('gtfs-shapes')!
    expect(implied.exclusive).toBeUndefined()
    expect(implied.longRunning).toBe(true)
    expect(isExclusive(implied)).toBe(true)
  })

  it('leaves short scripts free to queue behind each other', () => {
    expect(isExclusive(getScript('db-post-import')!)).toBe(false)
  })
})

describe('nextStages', () => {
  it('carries earlier stage names forward as later markers arrive', () => {
    const first = nextStages(null, { index: 1, total: 3, name: 'Download' })
    expect(nextStages(first, { index: 2, total: 3, name: 'Import' }).labels).toEqual(['Download', 'Import', ''])
  })

  it('keeps no names for a script that counts thousands of cells as stages', () => {
    const many = nextStages(null, { index: 5, total: 236893, name: 'cell -5333,1587' })
    expect(many).toEqual({ index: 5, total: 236893, labels: [] })
    expect(nextStages(null, { index: 1, total: MAX_NAMED_STAGES, name: 'a' }).labels).toHaveLength(MAX_NAMED_STAGES)
  })
})
