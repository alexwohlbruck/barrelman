/**
 * Alerts when a data volume fills up.
 *
 * Runs in the ops worker because that is the container that mounts every data
 * volume and the one whose jobs fill them: an OSM import, a basemap render or a
 * GTFS download that hits ENOSPC fails hours in, and usually takes Postgres
 * down with it when they share a disk. The check reads `statfs` on each mount,
 * the same numbers `df` prints, so no subprocess is needed.
 *
 * While a volume stays above ALERT_DISK_PERCENT the alert repeats every six
 * hours; once it drops back below, one recovery is sent.
 */
import { statfs, stat } from 'node:fs/promises'
import { envNumber, envRaw } from '../config/env'
import { alertsEnabled, sendAlert, type Alert } from '../lib/alerts'

const CHECK_MS = 10 * 60_000
const REPEAT_MS = 6 * 60 * 60_000

/** The mounts docker-compose.yml gives barrelman-ops. Missing ones are skipped. */
function candidatePaths(): string[] {
  const paths = [envRaw('OSM_DATA_DIR'), '/data', '/gtfs-data', '/portolan-data', '/gtfs-zips']
  return [...new Set(paths.filter((p): p is string => !!p))]
}

/** Used share of the filesystem holding `path`, computed as `df` does (reserved blocks excluded). */
export function usedPercent(fs: { blocks: number; bfree: number; bavail: number }): number {
  const used = fs.blocks - fs.bfree
  const usable = used + fs.bavail
  return usable > 0 ? Math.round((used / usable) * 100) : 0
}

export function diskAlert(path: string, percent: number, threshold: number, wasAbove: boolean): Alert | null {
  const key = `disk:${path}`
  if (percent >= threshold) {
    return {
      key,
      severity: 'warning',
      title: `Disk ${percent}% full: ${path}`,
      message: `The volume mounted at ${path} is ${percent}% full (alert threshold ${threshold}%). Imports and Postgres fail once it runs out.`,
    }
  }
  if (wasAbove) {
    return {
      key,
      severity: 'resolved',
      title: `Disk back under ${threshold}%: ${path}`,
      message: `The volume mounted at ${path} is now ${percent}% full.`,
    }
  }
  return null
}

/** Several mounts are often one filesystem; report each device once, under its first path. */
async function distinctMounts(): Promise<string[]> {
  const seen = new Set<number>()
  const out: string[] = []
  for (const path of candidatePaths()) {
    try {
      const { dev } = await stat(path)
      if (seen.has(dev)) continue
      seen.add(dev)
      out.push(path)
    } catch {
      /* not mounted in this container */
    }
  }
  return out
}

const INTERVAL_KEY = Symbol.for('barrelman.disk-alerts.interval')

export function startDiskAlerts(): void {
  if (!alertsEnabled()) return
  const globals = globalThis as Record<symbol, unknown>
  if (globals[INTERVAL_KEY]) return

  const threshold = envNumber('ALERT_DISK_PERCENT', 90)
  const above = new Set<string>()

  const run = async () => {
    for (const path of await distinctMounts()) {
      const percent = usedPercent(await statfs(path))
      const alert = diskAlert(path, percent, threshold, above.has(path))
      if (percent >= threshold) above.add(path)
      else above.delete(path)
      if (alert) await sendAlert(alert, alert.severity === 'warning' ? { dedupeMs: REPEAT_MS } : undefined)
    }
  }
  const tick = () => void run().catch((err) => console.error('[disk-alerts] check failed', err))

  const timer = setInterval(tick, CHECK_MS)
  timer.unref?.()
  globals[INTERVAL_KEY] = timer
  tick()
}
