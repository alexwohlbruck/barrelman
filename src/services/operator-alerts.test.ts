/**
 * The decisions behind each operator alert: when a dependency, a scheduled job
 * or a disk crosses into (or out of) a state worth telling someone about.
 */
import { describe, test, expect } from 'bun:test'
import { createDependencyWatcher } from './health-alerts.service'
import { scheduledJobAlert, type EndedJob } from './job-alerts.service'
import { diskAlert, usedPercent } from '../worker/disk-alerts'
import type { DependencyHealth, DependencyKey } from './health.service'

function deps(overrides: Partial<Record<DependencyKey, DependencyHealth>> = {}) {
  const ok: DependencyHealth = { status: 'ok' }
  return {
    database: ok,
    graphhopper: ok,
    martin: ok,
    motis: ok,
    pelias: { status: 'unavailable', optional: true },
    tts: { status: 'unavailable', optional: true },
    ...overrides,
  } as Record<DependencyKey, DependencyHealth>
}

const motisDown = deps({ motis: { status: 'unavailable', message: 'HTTP 503' } })

describe('dependency watcher', () => {
  /** One cycle, with every returned alert delivered. */
  const cycle = (watcher: ReturnType<typeof createDependencyWatcher>, state: ReturnType<typeof deps>) => {
    const alerts = watcher.observe(state)
    for (const alert of alerts) watcher.delivered(alert)
    return alerts
  }

  test('defaults to three consecutive failed cycles, then alerts once', () => {
    const watcher = createDependencyWatcher()
    expect(cycle(watcher, motisDown)).toEqual([])
    expect(cycle(watcher, motisDown)).toEqual([])
    const [alert] = cycle(watcher, motisDown)
    expect(alert).toMatchObject({ key: 'dependency:motis', severity: 'critical' })
    expect(alert.message).toContain('HTTP 503')
    expect(cycle(watcher, motisDown)).toEqual([])
  })

  test('a single failed probe between good ones never alerts', () => {
    const watcher = createDependencyWatcher(2)
    cycle(watcher, motisDown)
    cycle(watcher, deps())
    expect(cycle(watcher, motisDown)).toEqual([])
  })

  test('sends one recovery after an alert, and none without one', () => {
    const watcher = createDependencyWatcher(2)
    cycle(watcher, motisDown)
    expect(cycle(watcher, deps())).toEqual([])

    cycle(watcher, motisDown)
    cycle(watcher, motisDown)
    const [recovery] = cycle(watcher, deps())
    expect(recovery).toMatchObject({ key: 'dependency:motis', severity: 'resolved' })
    expect(cycle(watcher, deps())).toEqual([])
  })

  test('an undelivered alert is offered again next cycle, and so is its recovery', () => {
    const watcher = createDependencyWatcher(1)
    expect(watcher.observe(motisDown)).toHaveLength(1)
    const [retry] = watcher.observe(motisDown)
    expect(retry).toMatchObject({ severity: 'critical' })
    watcher.delivered(retry)
    expect(watcher.observe(motisDown)).toEqual([])

    expect(watcher.observe(deps())).toHaveLength(1)
    expect(watcher.observe(deps())).toMatchObject([{ severity: 'resolved' }])
  })

  test('scrubs internal addresses out of the probe message', () => {
    const watcher = createDependencyWatcher(1)
    const [alert] = watcher.observe(deps({ graphhopper: { status: 'unavailable', message: 'connect ECONNREFUSED 172.19.0.4:8989' } }))
    expect(alert.message).not.toContain('172.19')
  })

  test('optional services never alert', () => {
    const watcher = createDependencyWatcher(1)
    for (let i = 0; i < 5; i++) expect(watcher.observe(deps())).toEqual([])
  })
})

describe('scheduled job alerts', () => {
  const job = (status: EndedJob['status'], extra: Partial<EndedJob> = {}): EndedJob => ({
    id: 'j1',
    scriptName: 'Update OSM',
    scheduleId: 's1',
    status,
    ...extra,
  })

  test('a failed run alerts, with the recorded reason', () => {
    const alert = scheduledJobAlert(job('failed', { error: 'worker heartbeat timeout' }), 'succeeded')
    expect(alert).toMatchObject({ key: 'schedule:s1', severity: 'critical' })
    expect(alert!.message).toContain('worker heartbeat timeout')
  })

  test('falls back to the exit code when no error text was recorded', () => {
    expect(scheduledJobAlert(job('failed', { exitCode: 3 }), null)!.message).toContain('exit code 3')
  })

  test('a success after a failure is a recovery', () => {
    expect(scheduledJobAlert(job('succeeded'), 'failed')).toMatchObject({ severity: 'resolved' })
  })

  test('a success after a success, or a first success, says nothing', () => {
    expect(scheduledJobAlert(job('succeeded'), 'succeeded')).toBeNull()
    expect(scheduledJobAlert(job('succeeded'), null)).toBeNull()
  })

  test('a canceled run says nothing', () => {
    expect(scheduledJobAlert(job('canceled'), 'failed')).toBeNull()
  })
})

describe('disk alerts', () => {
  test('computes usage the way df does: reserved blocks excluded, rounded up', () => {
    // 100 blocks, 20 free, of which 10 are reserved for root: 80 / 90 = 88.9%.
    expect(usedPercent({ blocks: 100, bfree: 20, bavail: 10 })).toBe(89)
    // 1002 / 1100 = 91.09%, which df shows as 92%.
    expect(usedPercent({ blocks: 1100, bfree: 98, bavail: 98 })).toBe(92)
  })

  test('warns at or above the threshold', () => {
    expect(diskAlert('/data', 90, 90, false)).toMatchObject({ key: 'disk:/data', severity: 'warning' })
    expect(diskAlert('/data', 95, 90, true)).toMatchObject({ severity: 'warning' })
  })

  test('recovers once when it drops back below', () => {
    expect(diskAlert('/data', 70, 90, true)).toMatchObject({ severity: 'resolved' })
    expect(diskAlert('/data', 70, 90, false)).toBeNull()
  })
})
