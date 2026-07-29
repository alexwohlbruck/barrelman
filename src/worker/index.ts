/**
 * barrelman-ops worker.
 *
 * Executes queued `process` jobs from the DB job queue (see ops-job-store). Runs
 * in the privileged `barrelman-ops` container, which has the docker socket +
 * osm2pgsql/osmium/python/pelias tooling that the lean API container lacks. The
 * API only enqueues; this worker claims one job at a time, holds a Postgres
 * advisory lock for `exclusive` scripts (single-flight — the fix for the
 * overlapping-import failure mode), streams logs to the DB, heartbeats, and
 * honors the cancel flag.
 */
import { resolve } from 'node:path'
import { hostname } from 'node:os'
import { randomUUID } from 'node:crypto'
import postgres from 'postgres'
import { connection as sql, dbUrl } from '../db'
import { getScript } from '../admin/scripts-manifest'
import { buildInvocation, advisoryKeyFor, type Job } from '../services/job-invocation'
import * as store from '../services/ops-job-store'

const REPO_ROOT = resolve(import.meta.dir, '../..')
const WORKER_ID = `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`
const POLL_MS = 2000
const HEARTBEAT_MS = 10_000
const CANCEL_CHECK_MS = 3000
const LOG_FLUSH_MS = 500

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function pumpStream(stream: ReadableStream<Uint8Array>, onLine: (l: string) => void) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let buf = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buf += decoder.decode(value, { stream: true })
      let idx: number
      while ((idx = buf.indexOf('\n')) >= 0) {
        onLine(buf.slice(0, idx).replace(/\r$/, ''))
        buf = buf.slice(idx + 1)
      }
    }
  } finally {
    reader.releaseLock()
  }
  if (buf.length) onLine(buf.replace(/\r$/, ''))
}

async function runJob(job: Job): Promise<void> {
  const script = getScript(job.scriptId)
  if (!script || script.exec.kind !== 'process') {
    await store.setStatus(job.id, 'failed', 1, 'not a runnable process script')
    return
  }
  const exclusive = script.exclusive ?? script.longRunning
  const key = advisoryKeyFor(job.scriptId)

  // Session-level advisory lock needs its own dedicated connection (the shared
  // pool would unlock on a different socket). One connection, held for the job.
  let lockConn: ReturnType<typeof postgres> | null = null
  if (exclusive) {
    lockConn = postgres(dbUrl, { max: 1 })
    const [{ locked }] = await lockConn`SELECT pg_try_advisory_lock(${key}) AS locked`
    if (!locked) {
      await store.appendLogs(job.id, [{ stream: 'system', text: 'Another run of this script is in progress — requeued' }])
      await sql`UPDATE ops_jobs SET status='queued', worker_id=NULL, started_at=NULL WHERE id=${job.id}`
      await lockConn.end()
      return
    }
  }

  const inv = buildInvocation(script, job.params)
  if (inv.kind !== 'process') {
    await store.setStatus(job.id, 'failed', 1, 'invocation not process')
    if (lockConn) await lockConn.end()
    return
  }

  const buf: Array<{ stream: 'stdout' | 'stderr' | 'system'; text: string }> = []
  const flush = async () => {
    if (buf.length) await store.appendLogs(job.id, buf.splice(0, buf.length))
  }
  const flushTimer = setInterval(() => void flush(), LOG_FLUSH_MS)
  const hbTimer = setInterval(() => void store.heartbeat(job.id), HEARTBEAT_MS)
  let proc: ReturnType<typeof Bun.spawn> | undefined
  let canceled = false
  const cancelTimer = setInterval(() => {
    void (async () => {
      if (!canceled && (await store.isCancelRequested(job.id))) {
        canceled = true
        buf.push({ stream: 'system', text: '⨯ Cancel requested — sending SIGTERM' })
        try {
          proc?.kill()
        } catch {
          /* already gone */
        }
      }
    })()
  }, CANCEL_CHECK_MS)

  const cleanup = () => {
    clearInterval(flushTimer)
    clearInterval(hbTimer)
    clearInterval(cancelTimer)
  }

  try {
    proc = Bun.spawn([inv.command, ...inv.args], {
      cwd: REPO_ROOT,
      env: { ...process.env, ...inv.env },
      stdout: 'pipe',
      stderr: 'pipe',
    })
    const so = pumpStream(proc.stdout as ReadableStream<Uint8Array>, (l) => buf.push({ stream: 'stdout', text: l }))
    const se = pumpStream(proc.stderr as ReadableStream<Uint8Array>, (l) => buf.push({ stream: 'stderr', text: l }))
    const [, , code] = await Promise.all([so, se, proc.exited])
    cleanup()
    await flush()
    if (canceled) {
      await store.setStatus(job.id, 'canceled', code)
    } else if (code === 0) {
      await store.setStatus(job.id, 'succeeded', 0)
    } else {
      await store.appendLogs(job.id, [{ stream: 'system', text: `Process exited with code ${code}` }])
      await store.setStatus(job.id, 'failed', code)
    }
  } catch (err) {
    cleanup()
    await flush()
    const msg = err instanceof Error ? err.message : String(err)
    await store.appendLogs(job.id, [{ stream: 'stderr', text: `Failed to run: ${msg}` }])
    await store.setStatus(job.id, 'failed', 1, msg)
  } finally {
    if (lockConn) {
      try {
        await lockConn`SELECT pg_advisory_unlock(${key})`
      } catch {
        /* connection may be gone */
      }
      await lockConn.end()
    }
  }
}

async function main() {
  await store.ensureOpsJobsSchema()
  console.log(`[ops-worker] ${WORKER_ID} started; polling for process jobs from the DB queue`)
  for (;;) {
    let ranSomething = false
    try {
      await store.reapStaleJobs()
      const job = await store.claimNextProcessJob(WORKER_ID)
      if (job) {
        ranSomething = true
        console.log(`[ops-worker] running ${job.scriptId} (${job.id})`)
        await runJob(job)
        console.log(`[ops-worker] finished ${job.scriptId} (${job.id})`)
      }
    } catch (err) {
      console.error('[ops-worker] loop error:', err)
    }
    if (!ranSomething) await sleep(POLL_MS)
  }
}

main().catch((e) => {
  console.error('[ops-worker] fatal:', e)
  process.exit(1)
})
