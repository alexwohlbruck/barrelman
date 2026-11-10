/**
 * Alerts for scheduled jobs: one when a run fails, one when the next run on
 * the same schedule succeeds after a failure.
 *
 * Only `trigger: 'schedule'` runs alert. A job an operator started by hand is
 * one they are already watching in the console. Canceled runs are neither a
 * failure nor a recovery, so they are skipped both as the current run and when
 * looking back for the previous one.
 *
 * Called from `setStatus()` in ops-job-store, which every terminal transition
 * goes through: internal jobs in the API, process jobs in the ops worker, and
 * the heartbeat reaper. Both processes therefore need ALERT_WEBHOOK_URL.
 */
import { connection as sql } from '../db'
import { alertsEnabled, scrubForAlert, sendAlert, type Alert } from '../lib/alerts'
import type { JobStatus } from './job-invocation'

export interface EndedJob {
  id: string
  scriptName: string
  scheduleId: string
  status: JobStatus
  error?: string | null
  exitCode?: number | null
}

/** A repeat failure of the same schedule inside this window is not re-sent. */
const FAILURE_DEDUPE_MS = 60 * 60_000

/** What to send for a scheduled run that just ended, given how the previous one ended. */
export function scheduledJobAlert(job: EndedJob, previousStatus: JobStatus | null): Alert | null {
  const key = `schedule:${job.scheduleId}`

  if (job.status === 'failed') {
    const reason = job.error ? scrubForAlert(job.error) : (job.exitCode != null ? `exit code ${job.exitCode}` : 'no reason recorded')
    return {
      key,
      severity: 'critical',
      title: `Scheduled job failed: ${job.scriptName}`,
      message: `The scheduled run of ${job.scriptName} failed (${reason}). Job ${job.id}; its log is in the console.`,
    }
  }

  if (job.status === 'succeeded' && previousStatus === 'failed') {
    return {
      key,
      severity: 'resolved',
      title: `Scheduled job recovered: ${job.scriptName}`,
      message: `${job.scriptName} succeeded on its schedule after the previous run failed. Job ${job.id}.`,
    }
  }

  return null
}

/** Look the job up and alert if it warrants one. Never throws. */
export async function alertOnJobEnd(jobId: string, status: JobStatus): Promise<void> {
  if (!alertsEnabled() || (status !== 'failed' && status !== 'succeeded')) return
  try {
    const [row] = await sql`
      SELECT id, script_name, schedule_id, status, error, exit_code, ended_at
      FROM ops_jobs WHERE id = ${jobId} AND trigger = 'schedule' AND schedule_id IS NOT NULL`
    if (!row) return

    const [previous] = await sql`
      SELECT status FROM ops_jobs
      WHERE schedule_id = ${row.schedule_id} AND id <> ${jobId}
        AND status IN ('failed', 'succeeded') AND ended_at <= ${row.ended_at}
      ORDER BY ended_at DESC LIMIT 1`

    const alert = scheduledJobAlert(
      {
        id: row.id,
        scriptName: row.script_name,
        scheduleId: row.schedule_id,
        status: row.status,
        error: row.error,
        exitCode: row.exit_code,
      },
      (previous?.status as JobStatus | undefined) ?? null,
    )
    if (alert) await sendAlert(alert, alert.severity === 'critical' ? { dedupeMs: FAILURE_DEDUPE_MS } : undefined)
  } catch (err) {
    console.warn(`[alerts] could not evaluate job ${jobId}:`, err instanceof Error ? err.message : err)
  }
}
