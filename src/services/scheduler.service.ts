/**
 * The ticker that turns schedules into jobs.
 *
 * Polls `ops_schedules` for due rows and enqueues each through the same
 * `startJob()` the console's run button uses, tagged `trigger: 'schedule'`.
 * Everything downstream — the ops worker, log streaming, progress parsing,
 * cancel, the ETA history — is unchanged, which is the whole point: a nightly
 * import is now an ordinary job an operator can watch and cancel.
 *
 * Polling rather than one timer per schedule: schedules are edited at runtime,
 * the tick is a single indexed query, and a minute of slack on a nightly import
 * is immaterial. The claim is a compare-and-set in the DB (see claimSchedule),
 * so the poll interval is a lower bound on latency, never a source of
 * double-firing.
 */
import { JobConflictError, startJob } from './job-runner.service'
import {
  backfillNextRuns,
  claimSchedule,
  dueSchedules,
  ensureSchedulesSchema,
  recordFire,
  type Schedule,
} from './schedules.service'
import { envNumber } from '../config/env'

/** A minute of granularity is what cron itself offers; polling faster buys nothing. */
const TICK_MS = Math.max(10_000, envNumber('BARRELMAN_SCHEDULER_INTERVAL_MS', 30_000))

/**
 * Guarded on globalThis: `bun --hot` re-evaluates modules without tearing down
 * their timers, so an unguarded interval accumulates one copy per edit.
 */
const INTERVAL_KEY = Symbol.for('barrelman.scheduler.interval')

/** Fire one claimed schedule. Never throws — a bad schedule must not stop the tick. */
async function fire(schedule: Schedule): Promise<void> {
  try {
    const job = await startJob(schedule.scriptId, schedule.params, {
      trigger: 'schedule',
      scheduleId: schedule.id,
      scheduleName: schedule.scriptName,
    })
    await recordFire(schedule.id, { jobId: job.id })
    console.log(`[scheduler] ${schedule.scriptId} → job ${job.id}`)
  } catch (err) {
    // The common one: the previous run of an exclusive script is still going.
    // Skipping is correct — the data will be current when it finishes — but the
    // reason is recorded so a schedule that's permanently starved is visible in
    // the console rather than looking like it simply never ran.
    const reason =
      err instanceof JobConflictError
        ? 'Previous run still in progress — skipped'
        : err instanceof Error
          ? err.message
          : 'Failed to start'
    await recordFire(schedule.id, { skipReason: reason }).catch(() => {})
    console.warn(`[scheduler] ${schedule.scriptId} skipped: ${reason}`)
  }
}

async function tick(): Promise<void> {
  const due = await dueSchedules()
  for (const schedule of due) {
    // Whoever wins the compare-and-set owns this occurrence.
    if (await claimSchedule(schedule)) await fire(schedule)
  }
}

export async function startScheduler(): Promise<void> {
  const globals = globalThis as Record<symbol, unknown>
  if (globals[INTERVAL_KEY]) return

  await ensureSchedulesSchema()
  await backfillNextRuns()

  const run = () => void tick().catch((err) => console.error('[scheduler] tick failed', err))

  const timer = setInterval(run, TICK_MS)
  timer.unref?.()
  globals[INTERVAL_KEY] = timer

  // One pass at startup: a schedule whose time passed while the process was
  // down is due now, and claimSchedule rolls it forward to the next future
  // occurrence, so a long outage produces one catch-up run rather than a burst.
  run()
}

export function stopScheduler(): void {
  const globals = globalThis as Record<symbol, unknown>
  const timer = globals[INTERVAL_KEY] as ReturnType<typeof setInterval> | undefined
  if (timer) clearInterval(timer)
  globals[INTERVAL_KEY] = undefined
}
