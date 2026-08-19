/**
 * DB-backed schedules for manifest scripts.
 *
 * Data that goes stale — the OSM extract, GTFS feeds, GBFS systems — used to be
 * refreshed by a host crontab calling the shell scripts with `docker exec`.
 * That worked, but it bypassed the job store entirely: a nightly import left no
 * row in `ops_jobs`, so the console showed nothing and its only trace was a
 * logfile on the host. Schedules live here instead, and firing one goes through
 * the same `startJob()` an operator's click does, so a scheduled run is an
 * ordinary tracked job with logs, progress and cancel.
 *
 * Any script in the manifest can be scheduled; `SEEDS` below only pre-fills the
 * ones that refresh data, and pre-fills them *disabled* so upgrading an
 * existing instance never silently starts running imports at 3am.
 *
 * Timestamps are epoch-ms bigints, matching ops_jobs.
 */
import { connection as sql } from '../db'
import { randomUUID } from 'node:crypto'
import { getScript } from '../admin/scripts-manifest'
import { isValidCron, isValidTimeZone, nextRun } from '../lib/cron'
import { envString } from '../config/env'

export interface Schedule {
  id: string
  scriptId: string
  /** Denormalised for the console; the manifest stays the source of truth. */
  scriptName: string
  cron: string
  timezone: string
  params: Record<string, unknown>
  enabled: boolean
  createdAt: number
  updatedAt: number
  nextRunAt?: number
  lastRunAt?: number
  lastJobId?: string
  /** Why the previous fire didn't produce a job, if it didn't. */
  lastSkipReason?: string
}

export interface ScheduleInput {
  scriptId: string
  cron: string
  timezone?: string
  params?: Record<string, unknown>
  enabled?: boolean
}

export class ScheduleValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ScheduleValidationError'
  }
}

/** Default zone for new schedules. An operator writing "3am" usually means theirs. */
export const defaultTimeZone = (): string => {
  const configured = envString('BARRELMAN_SCHEDULE_TZ', 'UTC')
  return isValidTimeZone(configured) ? configured : 'UTC'
}

/**
 * Schedules created on a fresh install. Disabled — the console turns them on.
 * Staggered so a nightly window doesn't start three heavy imports at once; the
 * exclusive-job guard would reject the overlap anyway, but a skipped import is
 * worse than a spread-out one.
 */
const SEEDS: Array<Pick<ScheduleInput, 'scriptId' | 'cron'> & { params?: Record<string, unknown> }> = [
  // Incremental replication diff — cheap, safe to run nightly. Explicitly
  // pinned to replication so a seeded schedule can never trigger the
  // destructive full re-import.
  { scriptId: 'osm-update', cron: '0 3 * * *', params: { UPDATE_MODE: 'replication' } },
  // Agencies republish GTFS on no fixed cadence; this checks upstream shas and
  // only re-imports the regions that actually changed.
  { scriptId: 'gtfs-watch', cron: '0 4 * * *' },
  // GBFS systems churn slowly — weekly is plenty.
  { scriptId: 'gbfs-import', cron: '30 5 * * 0' },
]

let schemaReady: Promise<void> | null = null
export function ensureSchedulesSchema(): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS ops_schedules (
          id               uuid PRIMARY KEY,
          script_id        text NOT NULL,
          cron             text NOT NULL,
          timezone         text NOT NULL DEFAULT 'UTC',
          params           jsonb NOT NULL DEFAULT '{}'::jsonb,
          enabled          boolean NOT NULL DEFAULT false,
          created_at       bigint NOT NULL,
          updated_at       bigint NOT NULL,
          next_run_at      bigint,
          last_run_at      bigint,
          last_job_id      uuid,
          last_skip_reason text
        )`
      await sql`CREATE INDEX IF NOT EXISTS ops_schedules_due_idx ON ops_schedules (enabled, next_run_at)`
      await seedDefaults()
    })()
  }
  return schemaReady
}

/**
 * Insert the default schedules once, on a table that has never held any. Keyed
 * on emptiness rather than per-row existence so an operator who deletes a
 * seeded schedule doesn't get it back on the next restart.
 */
async function seedDefaults(): Promise<void> {
  const [{ count }] = await sql`SELECT count(*)::int AS count FROM ops_schedules`
  if (Number(count) > 0) return

  const tz = defaultTimeZone()
  const now = Date.now()
  for (const seed of SEEDS) {
    if (!getScript(seed.scriptId)) continue
    await sql`
      INSERT INTO ops_schedules (id, script_id, cron, timezone, params, enabled, created_at, updated_at, next_run_at)
      VALUES (${randomUUID()}, ${seed.scriptId}, ${seed.cron}, ${tz},
              ${JSON.stringify(seed.params ?? {})}::jsonb, false, ${now}, ${now}, null)`
  }
  console.log(`Schedules: seeded ${SEEDS.length} default schedules (disabled — enable them in the console)`)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function rowToSchedule(r: any): Schedule {
  return {
    id: r.id,
    scriptId: r.script_id,
    scriptName: getScript(r.script_id)?.name ?? r.script_id,
    cron: r.cron,
    timezone: r.timezone,
    params: r.params ?? {},
    enabled: r.enabled,
    createdAt: Number(r.created_at),
    updatedAt: Number(r.updated_at),
    nextRunAt: r.next_run_at != null ? Number(r.next_run_at) : undefined,
    lastRunAt: r.last_run_at != null ? Number(r.last_run_at) : undefined,
    lastJobId: r.last_job_id ?? undefined,
    lastSkipReason: r.last_skip_reason ?? undefined,
  }
}

/** Validate operator input and normalise it. Throws ScheduleValidationError. */
function validate(input: ScheduleInput): Required<Omit<ScheduleInput, 'params'>> & { params: Record<string, unknown> } {
  if (!getScript(input.scriptId)) {
    throw new ScheduleValidationError(`Unknown script: ${input.scriptId}`)
  }
  const cron = (input.cron ?? '').trim()
  if (!isValidCron(cron)) {
    throw new ScheduleValidationError(`Invalid cron expression: "${cron}"`)
  }
  const timezone = (input.timezone ?? defaultTimeZone()).trim()
  if (!isValidTimeZone(timezone)) {
    throw new ScheduleValidationError(`Unknown timezone: "${timezone}"`)
  }
  return { scriptId: input.scriptId, cron, timezone, enabled: input.enabled ?? false, params: input.params ?? {} }
}

/** Next fire time for a schedule, or null if enabled=false or nothing matches. */
function computeNextRun(cron: string, timezone: string, enabled: boolean, from = new Date()): number | null {
  if (!enabled) return null
  return nextRun(cron, from, timezone)?.getTime() ?? null
}

export async function listSchedules(): Promise<Schedule[]> {
  const rows = await sql`SELECT * FROM ops_schedules ORDER BY script_id ASC, created_at ASC`
  return rows.map(rowToSchedule)
}

export async function getSchedule(id: string): Promise<Schedule | undefined> {
  const [row] = await sql`SELECT * FROM ops_schedules WHERE id = ${id}`
  return row ? rowToSchedule(row) : undefined
}

export async function createSchedule(input: ScheduleInput): Promise<Schedule> {
  const v = validate(input)
  const now = Date.now()
  const [row] = await sql`
    INSERT INTO ops_schedules (id, script_id, cron, timezone, params, enabled, created_at, updated_at, next_run_at)
    VALUES (${randomUUID()}, ${v.scriptId}, ${v.cron}, ${v.timezone}, ${JSON.stringify(v.params)}::jsonb,
            ${v.enabled}, ${now}, ${now}, ${computeNextRun(v.cron, v.timezone, v.enabled)})
    RETURNING *`
  return rowToSchedule(row)
}

export async function updateSchedule(id: string, input: ScheduleInput): Promise<Schedule | undefined> {
  const v = validate(input)
  const [row] = await sql`
    UPDATE ops_schedules
    SET script_id = ${v.scriptId}, cron = ${v.cron}, timezone = ${v.timezone},
        params = ${JSON.stringify(v.params)}::jsonb, enabled = ${v.enabled},
        updated_at = ${Date.now()},
        next_run_at = ${computeNextRun(v.cron, v.timezone, v.enabled)},
        last_skip_reason = null
    WHERE id = ${id}
    RETURNING *`
  return row ? rowToSchedule(row) : undefined
}

/** Flip enabled without touching the expression. Recomputes the next fire time. */
export async function setScheduleEnabled(id: string, enabled: boolean): Promise<Schedule | undefined> {
  const existing = await getSchedule(id)
  if (!existing) return undefined
  const [row] = await sql`
    UPDATE ops_schedules
    SET enabled = ${enabled},
        updated_at = ${Date.now()},
        next_run_at = ${computeNextRun(existing.cron, existing.timezone, enabled)},
        last_skip_reason = null
    WHERE id = ${id}
    RETURNING *`
  return row ? rowToSchedule(row) : undefined
}

export async function deleteSchedule(id: string): Promise<boolean> {
  const rows = await sql`DELETE FROM ops_schedules WHERE id = ${id} RETURNING id`
  return rows.length > 0
}

/** Enabled schedules whose next fire time has arrived. */
export async function dueSchedules(now = Date.now()): Promise<Schedule[]> {
  const rows = await sql`
    SELECT * FROM ops_schedules
    WHERE enabled = true AND next_run_at IS NOT NULL AND next_run_at <= ${now}
    ORDER BY next_run_at ASC`
  return rows.map(rowToSchedule)
}

/**
 * Atomically take ownership of a due schedule by rolling its next_run_at
 * forward, guarded on the value we read. Only the caller that wins the compare
 * gets `true` and may enqueue — so two API processes (or a `bun --hot` re-eval
 * that left an old timer alive) can't double-fire the same occurrence.
 *
 * The new time is computed from *now*, not from the missed slot, so an instance
 * that was down for a week fires once on return rather than replaying every
 * occurrence it slept through.
 */
export async function claimSchedule(schedule: Schedule): Promise<boolean> {
  const next = computeNextRun(schedule.cron, schedule.timezone, true)
  const rows = await sql`
    UPDATE ops_schedules
    SET next_run_at = ${next}, last_run_at = ${Date.now()}, updated_at = ${Date.now()}
    WHERE id = ${schedule.id} AND next_run_at = ${schedule.nextRunAt ?? null}
    RETURNING id`
  return rows.length > 0
}

/** Record the outcome of a claimed fire: the job it produced, or why it produced none. */
export async function recordFire(id: string, result: { jobId?: string; skipReason?: string }): Promise<void> {
  await sql`
    UPDATE ops_schedules
    SET last_job_id = ${result.jobId ?? null}, last_skip_reason = ${result.skipReason ?? null}
    WHERE id = ${id}`
}

/**
 * Give every enabled schedule a next_run_at. Runs at boot: a schedule enabled
 * before a restart keeps its stored time, but one whose time is missing (seeded,
 * or migrated from an older row) would otherwise never become due.
 */
export async function backfillNextRuns(): Promise<void> {
  const rows = await sql`SELECT * FROM ops_schedules WHERE enabled = true AND next_run_at IS NULL`
  for (const r of rows) {
    const s = rowToSchedule(r)
    await sql`
      UPDATE ops_schedules SET next_run_at = ${computeNextRun(s.cron, s.timezone, true)}
      WHERE id = ${s.id} AND next_run_at IS NULL`
  }
}
