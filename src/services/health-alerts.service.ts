/**
 * Alerts when a required dependency goes down, and again when it comes back.
 *
 * A background check in the API runs the same probes `/health` does, once a
 * minute, and only when ALERT_WEBHOOK_URL is set. A dependency has to fail
 * ALERT_DEPENDENCY_CYCLES cycles in a row (default 3) before it alerts: a single failed probe is often a restart
 * (Watchtower, a rebuild job bouncing MOTIS) that is over before anyone could
 * act, and paging on each of those trains the operator to ignore the pager.
 *
 * Optional services (Pelias, TTS) never alert. An instance may legitimately not
 * run them, and `/health` already refuses to let them change its status.
 */
import { checkHealth, type DependencyHealth, type DependencyKey } from './health.service'
import { alertsEnabled, delivered, scrubForAlert, sendAlert, type Alert } from '../lib/alerts'
import { envNumber } from '../config/env'

const INTERVAL_MS = 60_000
/**
 * Consecutive failed cycles before a dependency alerts. Three by default, so a
 * GraphHopper cold start loading its graph does not page.
 */
const failCyclesFromEnv = () => Math.max(1, Math.round(envNumber('ALERT_DEPENDENCY_CYCLES', 3)))

const NAMES: Record<DependencyKey, string> = {
  database: 'Postgres',
  graphhopper: 'GraphHopper',
  martin: 'Martin',
  motis: 'MOTIS',
  pelias: 'Pelias',
  tts: 'Speech synthesis',
}

/**
 * Tracks each dependency across probe cycles and says what to send. Pure apart
 * from its own state, so the cycle rule is testable without timers.
 *
 * An alert counts as sent only once the caller reports it delivered. Until
 * then `observe()` returns it again on every cycle, so a webhook that was
 * briefly unreachable when MOTIS went down still hears about it.
 */
export function createDependencyWatcher(failCycles = failCyclesFromEnv()) {
  const failures = new Map<DependencyKey, number>()
  const alerted = new Set<DependencyKey>()

  return {
    observe(dependencies: Record<DependencyKey, DependencyHealth>): Alert[] {
      const alerts: Alert[] = []
      for (const [key, dep] of Object.entries(dependencies) as [DependencyKey, DependencyHealth][]) {
        if (dep.optional) continue
        const name = NAMES[key] ?? key

        if (dep.status === 'ok') {
          failures.delete(key)
          if (alerted.has(key)) {
            alerts.push({
              key: `dependency:${key}`,
              severity: 'resolved',
              title: `${name} recovered`,
              message: `${name} is answering health probes again.`,
            })
          }
          continue
        }

        const count = (failures.get(key) ?? 0) + 1
        failures.set(key, count)
        if (count >= failCycles && !alerted.has(key)) {
          alerts.push({
            key: `dependency:${key}`,
            severity: 'critical',
            title: `${name} is down`,
            message: `${name} has failed ${count} health probes in a row${dep.message ? `: ${scrubForAlert(dep.message)}` : '.'}`,
          })
        }
      }
      return alerts
    },

    /** Record that the operator has this alert. */
    delivered(alert: Alert): void {
      const key = alert.key.replace(/^dependency:/, '') as DependencyKey
      if (alert.severity === 'resolved') alerted.delete(key)
      else alerted.add(key)
    },
  }
}

/** See scheduler.service.ts: `bun --hot` keeps module-level timers alive across edits. */
const INTERVAL_KEY = Symbol.for('barrelman.health-alerts.interval')

export function startDependencyAlerts(): void {
  if (!alertsEnabled()) return
  const globals = globalThis as Record<symbol, unknown>
  if (globals[INTERVAL_KEY]) return

  const watcher = createDependencyWatcher()
  const run = async () => {
    const { dependencies } = await checkHealth()
    for (const alert of watcher.observe(dependencies)) {
      if (delivered(await sendAlert(alert))) watcher.delivered(alert)
    }
  }
  const tick = () => void run().catch((err) => console.error('[health-alerts] check failed', err))

  const timer = setInterval(tick, INTERVAL_MS)
  timer.unref?.()
  globals[INTERVAL_KEY] = timer
  tick()
}
