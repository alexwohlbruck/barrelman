/**
 * Job runner (API side).
 *
 * Manifest scripts become tracked jobs in Postgres (see ops-job-store). Two exec
 * kinds:
 *   - internal → run here, in the API process (these handlers need the API's DB
 *     client and run in-band SQL). Logs stream to the DB job store.
 *   - process  → left `queued`; the privileged `barrelman-ops` worker claims and
 *     executes them (it has docker + osm2pgsql/osmium/pelias tooling the lean API
 *     container lacks). See src/worker/index.ts.
 *
 * Job state (status, logs) lives entirely in the DB so the console renders a
 * single unified job list regardless of which process ran a job.
 */
import { getScript } from '../admin/scripts-manifest'
import { INTERNAL_HANDLERS } from './admin-internal-handlers'
import * as store from './ops-job-store'
import type { Job } from './job-invocation'

export type { Job } from './job-invocation'
export const { listJobs, getJob, jobStats, readLogsSince, ensureOpsJobsSchema } = store

export class JobConflictError extends Error {
  constructor(public readonly scriptId: string) {
    super(`A job for "${scriptId}" is already running`)
    this.name = 'JobConflictError'
  }
}

/** Enqueue a job for the given script id. Internal jobs start running immediately. */
export async function startJob(scriptId: string, params: Record<string, unknown> = {}): Promise<Job> {
  const script = getScript(scriptId)
  if (!script) throw new Error(`Unknown script: ${scriptId}`)

  const exclusive = script.exclusive ?? script.longRunning
  if (exclusive && (await store.hasActiveJob(scriptId))) {
    throw new JobConflictError(scriptId)
  }

  const job = await store.createJob(scriptId, params)

  if (job.execKind === 'internal' && script.exec.kind === 'internal') {
    // Run in-process; process jobs are picked up by the ops worker.
    void runInternal(job.id, script.exec.handler)
  }
  return job
}

async function runInternal(jobId: string, handler: string): Promise<void> {
  const fn = INTERNAL_HANDLERS[handler]
  if (!fn) {
    await store.appendLogs(jobId, [{ stream: 'stderr', text: `No internal handler registered for "${handler}"` }])
    await store.setStatus(jobId, 'failed', 1, `Unknown handler: ${handler}`)
    return
  }
  // Buffer log lines from the handler and flush periodically to avoid a DB
  // round-trip per line.
  const buf: Array<{ stream: 'stdout'; text: string }> = []
  const flush = async () => {
    if (!buf.length) return
    const batch = buf.splice(0, buf.length)
    await store.appendLogs(jobId, batch)
  }
  const timer = setInterval(() => void flush(), 500)
  try {
    await fn((text) => buf.push({ stream: 'stdout', text }))
    clearInterval(timer)
    await flush()
    await store.setStatus(jobId, 'succeeded', 0)
  } catch (err) {
    clearInterval(timer)
    await flush()
    const msg = err instanceof Error ? err.message : String(err)
    await store.appendLogs(jobId, [{ stream: 'stderr', text: msg }])
    await store.setStatus(jobId, 'failed', 1, msg)
  }
}

/** Request cancellation. A queued job is canceled immediately; a running process
 *  job is signalled via the DB flag and the worker sends SIGTERM. */
export async function cancelJob(id: string): Promise<{ ok: boolean; message: string }> {
  return store.requestCancel(id)
}
