/**
 * DB-backed job store shared by the API and the ops worker.
 *
 * The API enqueues jobs; `kind:'internal'` jobs run in-process in the API, while
 * `kind:'process'` jobs are left `queued` for the privileged `barrelman-ops`
 * worker to claim and execute. All state (status, logs) lives in Postgres so the
 * console sees a single unified job list regardless of where a job runs.
 *
 * Timestamps are stored as epoch-ms bigints to match the in-memory Job shape the
 * console already consumes.
 */
import { connection as sql } from '../db'
import { randomUUID } from 'node:crypto'
import { getScript } from '../admin/scripts-manifest'
import { buildInvocation, type Job, type JobStatus, type LogLine, type LogStream } from './job-invocation'

const MAX_JOBS = 200
const MAX_LOG_LINES = 8000
/** A running job whose worker hasn't heartbeat in this long is presumed dead. */
const STALE_MS = 60_000

let schemaReady: Promise<void> | null = null
export function ensureOpsJobsSchema(): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS ops_jobs (
          id               uuid PRIMARY KEY,
          script_id        text NOT NULL,
          script_name      text NOT NULL,
          category         text NOT NULL,
          danger           text NOT NULL,
          exec_kind        text NOT NULL,
          status           text NOT NULL,
          params           jsonb NOT NULL DEFAULT '{}'::jsonb,
          display_command  text NOT NULL,
          exclusive        boolean NOT NULL DEFAULT false,
          advisory_key     integer,
          created_at       bigint NOT NULL,
          started_at       bigint,
          ended_at         bigint,
          exit_code        integer,
          error            text,
          log_count        integer NOT NULL DEFAULT 0,
          cancel_requested boolean NOT NULL DEFAULT false,
          worker_id        text,
          heartbeat_at     bigint
        )`
      await sql`CREATE INDEX IF NOT EXISTS ops_jobs_status_idx ON ops_jobs (status, created_at)`
      await sql`
        CREATE TABLE IF NOT EXISTS ops_job_logs (
          job_id  uuid NOT NULL REFERENCES ops_jobs(id) ON DELETE CASCADE,
          seq     integer NOT NULL,
          t       bigint NOT NULL,
          stream  text NOT NULL,
          text    text NOT NULL,
          PRIMARY KEY (job_id, seq)
        )`
    })()
  }
  return schemaReady
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function rowToJob(r: any): Job {
  return {
    id: r.id,
    scriptId: r.script_id,
    scriptName: r.script_name,
    category: r.category,
    danger: r.danger,
    execKind: r.exec_kind,
    status: r.status,
    params: r.params ?? {},
    displayCommand: r.display_command,
    createdAt: Number(r.created_at),
    startedAt: r.started_at != null ? Number(r.started_at) : undefined,
    endedAt: r.ended_at != null ? Number(r.ended_at) : undefined,
    durationMs: r.started_at != null && r.ended_at != null ? Number(r.ended_at) - Number(r.started_at) : undefined,
    exitCode: r.exit_code ?? undefined,
    error: r.error ?? undefined,
    logCount: r.log_count ?? 0,
  }
}

/** Create a job row. Process jobs start `queued`; internal jobs `running` (API runs them). */
export async function createJob(scriptId: string, params: Record<string, unknown>): Promise<Job> {
  const script = getScript(scriptId)
  if (!script) throw new Error(`Unknown script: ${scriptId}`)
  const exclusive = script.exclusive ?? script.longRunning
  const inv = buildInvocation(script, params)
  const now = Date.now()
  const id = randomUUID()
  const status: JobStatus = inv.kind === 'internal' ? 'running' : 'queued'
  const advisoryKey = exclusive ? (await import('./job-invocation')).advisoryKeyFor(scriptId) : null

  const [row] = await sql`
    INSERT INTO ops_jobs (id, script_id, script_name, category, danger, exec_kind, status,
                          params, display_command, exclusive, advisory_key, created_at,
                          started_at, log_count, cancel_requested)
    VALUES (${id}, ${script.id}, ${script.name}, ${script.category}, ${script.danger}, ${inv.kind},
            ${status}, ${sql.json(params as never)}, ${inv.display}, ${exclusive}, ${advisoryKey},
            ${now}, ${status === 'running' ? now : null}, 0, false)
    RETURNING *`
  await appendLogs(id, [
    { stream: 'system', text: `▶ ${script.name}` },
    { stream: 'system', text: `$ ${inv.display}` },
  ])
  await pruneHistory()
  return rowToJob(row)
}

/**
 * Atomically claim the oldest queued process job. Uses FOR UPDATE SKIP LOCKED so
 * multiple workers never grab the same row. Returns the claimed Job or null.
 */
export async function claimNextProcessJob(workerId: string): Promise<Job | null> {
  const now = Date.now()
  const rows = await sql`
    UPDATE ops_jobs SET status = 'running', worker_id = ${workerId}, started_at = ${now}, heartbeat_at = ${now}
    WHERE id = (
      SELECT id FROM ops_jobs
      WHERE status = 'queued' AND exec_kind = 'process'
      ORDER BY created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING *`
  return rows.length ? rowToJob(rows[0]) : null
}

export async function appendLogs(
  jobId: string,
  lines: Array<{ stream: LogStream; text: string }>,
): Promise<void> {
  if (!lines.length) return
  const now = Date.now()
  // Reserve a contiguous seq range by bumping log_count first.
  const [row] = await sql`
    UPDATE ops_jobs SET log_count = log_count + ${lines.length}
    WHERE id = ${jobId} RETURNING log_count`
  if (!row) return
  const endSeq = Number(row.log_count)
  const startSeq = endSeq - lines.length
  const values = lines.map((l, i) => ({
    job_id: jobId,
    seq: startSeq + i,
    t: now,
    stream: l.stream,
    text: l.text.length > 8192 ? l.text.slice(0, 8192) : l.text,
  }))
  await sql`INSERT INTO ops_job_logs ${sql(values, 'job_id', 'seq', 't', 'stream', 'text')}`
  // Trim to the last MAX_LOG_LINES so a runaway job can't grow unbounded.
  if (endSeq > MAX_LOG_LINES) {
    await sql`DELETE FROM ops_job_logs WHERE job_id = ${jobId} AND seq < ${endSeq - MAX_LOG_LINES}`
  }
}

export async function setStatus(
  jobId: string,
  status: JobStatus,
  exitCode?: number | null,
  error?: string,
): Promise<void> {
  const ended = status !== 'running' && status !== 'queued' ? Date.now() : null
  await sql`
    UPDATE ops_jobs
    SET status = ${status},
        ended_at = COALESCE(${ended}, ended_at),
        exit_code = ${exitCode ?? null},
        error = COALESCE(${error ?? null}, error)
    WHERE id = ${jobId}`
}

export async function heartbeat(jobId: string): Promise<void> {
  await sql`UPDATE ops_jobs SET heartbeat_at = ${Date.now()} WHERE id = ${jobId}`
}

export async function requestCancel(jobId: string): Promise<{ ok: boolean; message: string }> {
  const [row] = await sql`SELECT status FROM ops_jobs WHERE id = ${jobId}`
  if (!row) return { ok: false, message: 'Job not found' }
  if (row.status !== 'running' && row.status !== 'queued') {
    return { ok: false, message: `Job already ${row.status}` }
  }
  await sql`UPDATE ops_jobs SET cancel_requested = true WHERE id = ${jobId}`
  // A still-queued job can be canceled immediately (no worker has it yet).
  if (row.status === 'queued') {
    await appendLogs(jobId, [{ stream: 'system', text: '⨯ Canceled before start' }])
    await setStatus(jobId, 'canceled', null)
  }
  return { ok: true, message: 'Cancel requested' }
}

export async function isCancelRequested(jobId: string): Promise<boolean> {
  const [row] = await sql`SELECT cancel_requested FROM ops_jobs WHERE id = ${jobId}`
  return Boolean(row?.cancel_requested)
}

export async function getJob(id: string): Promise<{ job: Job; logs: LogLine[] } | undefined> {
  const [row] = await sql`SELECT * FROM ops_jobs WHERE id = ${id}`
  if (!row) return undefined
  const logs = await sql`SELECT seq, t, stream, text FROM ops_job_logs WHERE job_id = ${id} ORDER BY seq ASC`
  return { job: rowToJob(row), logs: logs.map((l: any) => ({ seq: Number(l.seq), t: Number(l.t), stream: l.stream, text: l.text })) }
}

export async function readLogsSince(id: string, afterSeq: number): Promise<LogLine[]> {
  const logs = await sql`
    SELECT seq, t, stream, text FROM ops_job_logs
    WHERE job_id = ${id} AND seq >= ${afterSeq} ORDER BY seq ASC`
  return logs.map((l: any) => ({ seq: Number(l.seq), t: Number(l.t), stream: l.stream, text: l.text }))
}

export async function listJobs(): Promise<Job[]> {
  const rows = await sql`SELECT * FROM ops_jobs ORDER BY created_at DESC LIMIT ${MAX_JOBS}`
  return rows.map(rowToJob)
}

/** Is there already a queued/running job for this script? (enqueue-time single-flight guard) */
export async function hasActiveJob(scriptId: string): Promise<boolean> {
  const [r] = await sql`SELECT 1 FROM ops_jobs WHERE script_id = ${scriptId} AND status IN ('queued','running') LIMIT 1`
  return Boolean(r)
}

export async function jobStats(): Promise<{ total: number; running: number; succeeded: number; failed: number; queued: number }> {
  const [r] = await sql`
    SELECT count(*)::int AS total,
      count(*) FILTER (WHERE status = 'running')::int   AS running,
      count(*) FILTER (WHERE status = 'queued')::int    AS queued,
      count(*) FILTER (WHERE status = 'succeeded')::int AS succeeded,
      count(*) FILTER (WHERE status = 'failed')::int    AS failed
    FROM ops_jobs`
  return { total: Number(r.total), running: Number(r.running), queued: Number(r.queued), succeeded: Number(r.succeeded), failed: Number(r.failed) }
}

/** Mark running jobs whose worker stopped heartbeating as failed (crash recovery). */
export async function reapStaleJobs(): Promise<void> {
  const cutoff = Date.now() - STALE_MS
  const rows = await sql`
    SELECT id FROM ops_jobs
    WHERE status = 'running' AND exec_kind = 'process'
      AND (heartbeat_at IS NULL OR heartbeat_at < ${cutoff})`
  for (const r of rows) {
    await appendLogs(r.id, [{ stream: 'system', text: '⨯ Worker stopped responding — job marked failed' }])
    await setStatus(r.id, 'failed', null, 'worker heartbeat timeout')
  }
}

async function pruneHistory(): Promise<void> {
  await sql`
    DELETE FROM ops_jobs WHERE id IN (
      SELECT id FROM ops_jobs
      WHERE status <> 'running' AND status <> 'queued'
      ORDER BY created_at DESC OFFSET ${MAX_JOBS}
    )`
}
