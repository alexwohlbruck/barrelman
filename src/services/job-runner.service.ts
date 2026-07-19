/**
 * Job runner — executes manifest scripts as tracked, log-streaming jobs.
 *
 * Jobs run in the API's own environment (child processes for kind:'process',
 * in-process handlers for kind:'internal'). Each job keeps a capped ring buffer
 * of log lines and emits events so the admin console can stream live output via
 * Server-Sent Events. Job history is kept in memory (capped) — this is an
 * operator console, not an audit log.
 */
import { randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import { resolve } from 'node:path'
import { getScript, type ScriptDef, type DangerLevel, type ScriptCategory } from '../admin/scripts-manifest'
import { INTERNAL_HANDLERS } from './admin-internal-handlers'
import { getEtaMs, recordRun, parseProgress } from './job-history.service'

const REPO_ROOT = resolve(import.meta.dir, '../..')
const MAX_LOG_LINES = 8000
const MAX_JOBS = 200

export type JobStatus = 'running' | 'succeeded' | 'failed' | 'canceled'
export type LogStream = 'stdout' | 'stderr' | 'system'

export interface LogLine {
  seq: number
  t: number
  stream: LogStream
  text: string
}

export interface Job {
  id: string
  scriptId: string
  scriptName: string
  category: ScriptCategory
  danger: DangerLevel
  status: JobStatus
  params: Record<string, unknown>
  displayCommand: string
  startedAt: number
  endedAt?: number
  durationMs?: number
  exitCode?: number | null
  error?: string
  logCount: number
  /** Median successful runtime (ms) for this script, for ETA estimation. */
  etaMs?: number
  /** True progress fraction 0–1, parsed from the script's own log markers. */
  progress?: number
  /** Short label for the current progress marker, e.g. "3/8" or "42%". */
  progressLabel?: string
}

interface JobRuntime {
  job: Job
  logs: LogLine[]
  emitter: EventEmitter
  proc?: ReturnType<typeof Bun.spawn>
  seq: number
}

const runtimes = new Map<string, JobRuntime>()
/** Insertion order of job ids, newest last — used to cap history. */
const order: string[] = []

function pruneHistory() {
  while (order.length > MAX_JOBS) {
    const oldest = order.shift()
    if (oldest) {
      const rt = runtimes.get(oldest)
      // never evict a still-running job
      if (rt && rt.job.status === 'running') {
        order.push(oldest)
        break
      }
      runtimes.delete(oldest)
    }
  }
}

function addLog(rt: JobRuntime, stream: LogStream, text: string) {
  const line: LogLine = { seq: rt.seq++, t: Date.now(), stream, text }
  rt.logs.push(line)
  if (rt.logs.length > MAX_LOG_LINES) {
    rt.logs.splice(0, rt.logs.length - MAX_LOG_LINES)
  }
  rt.job.logCount = rt.seq
  rt.emitter.emit('log', line)

  // Derive a true progress fraction from the script's own markers ([N/M],
  // percentages, counts). Monotonic — only ever advances — so a small sub-step
  // count never drags a later stage backwards.
  if (rt.job.status === 'running' && stream !== 'system') {
    const p = parseProgress(text)
    if (p && p.fraction > (rt.job.progress ?? -1)) {
      rt.job.progress = p.fraction
      rt.job.progressLabel = p.label
      rt.emitter.emit('status', { ...rt.job })
    }
  }
}

function setStatus(rt: JobRuntime, status: JobStatus, exitCode?: number | null, error?: string) {
  rt.job.status = status
  if (status !== 'running') {
    rt.job.endedAt = Date.now()
    rt.job.durationMs = rt.job.endedAt - rt.job.startedAt
    rt.job.exitCode = exitCode ?? rt.job.exitCode
    if (error) rt.job.error = error
    if (status === 'succeeded') rt.job.progress = 1
    // Persist to history for future ETA estimates (best-effort, non-blocking).
    void recordRun({
      id: rt.job.id,
      scriptId: rt.job.scriptId,
      status,
      startedAt: rt.job.startedAt,
      endedAt: rt.job.endedAt,
      durationMs: rt.job.durationMs,
      exitCode: rt.job.exitCode,
    })
  }
  rt.emitter.emit('status', { ...rt.job })
}

/** Build the concrete invocation (argv + env) from a script + user params. */
function buildInvocation(script: ScriptDef, params: Record<string, unknown>) {
  const env: Record<string, string> = { ...(script.env ?? {}) }

  if (script.exec.kind === 'internal') {
    return { kind: 'internal' as const, handler: script.exec.handler, env, display: `internal: ${script.exec.handler}` }
  }

  const args = [...script.exec.args]
  const positional: string[] = []
  const secretValues = new Set<string>()

  for (const p of script.params ?? []) {
    let val = params[p.name]
    const isEmpty = val === undefined || val === null || val === ''
    if (isEmpty) {
      if (p.type === 'boolean') val = p.default ?? false
      else continue
    }

    if (p.secret && val) secretValues.add(String(val))

    if (p.apply === 'env') {
      if (p.type === 'boolean') {
        if (val === true || val === 'true') env[p.envVar ?? p.name] = '1'
      } else {
        env[p.envVar ?? p.name] = String(val)
      }
    } else if (p.apply === 'flag') {
      const flag = p.flag ?? `--${p.name}`
      if (p.type === 'boolean') {
        if (val === true || val === 'true') args.push(flag)
      } else {
        args.push(flag, String(val))
      }
    } else if (p.apply === 'positional') {
      if (typeof val === 'string' && val.trim()) positional.push(...val.trim().split(/\s+/))
    }
  }

  args.push(...positional)

  const display = [script.exec.command, ...args]
    .map((a) => (secretValues.has(a) ? '••••••' : a))
    .join(' ')

  return { kind: 'process' as const, command: script.exec.command, args, env, display }
}

async function pumpStream(
  stream: ReadableStream<Uint8Array>,
  onLine: (line: string) => void,
) {
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

export class JobConflictError extends Error {
  constructor(public readonly scriptId: string) {
    super(`A job for "${scriptId}" is already running`)
    this.name = 'JobConflictError'
  }
}

/** Start a job for the given script id. Returns the created Job. */
export function startJob(scriptId: string, params: Record<string, unknown> = {}): Job {
  const script = getScript(scriptId)
  if (!script) throw new Error(`Unknown script: ${scriptId}`)

  const exclusive = script.exclusive ?? script.longRunning
  if (exclusive) {
    for (const rt of runtimes.values()) {
      if (rt.job.scriptId === scriptId && rt.job.status === 'running') {
        throw new JobConflictError(scriptId)
      }
    }
  }

  const inv = buildInvocation(script, params)
  const id = randomUUID()
  const job: Job = {
    id,
    scriptId: script.id,
    scriptName: script.name,
    category: script.category,
    danger: script.danger,
    status: 'running',
    params,
    displayCommand: inv.display,
    startedAt: Date.now(),
    logCount: 0,
    etaMs: getEtaMs(script.id),
  }
  const rt: JobRuntime = { job, logs: [], emitter: new EventEmitter(), seq: 0 }
  rt.emitter.setMaxListeners(0)
  runtimes.set(id, rt)
  order.push(id)
  pruneHistory()

  addLog(rt, 'system', `▶ ${script.name}`)
  addLog(rt, 'system', `$ ${inv.display}`)

  if (inv.kind === 'internal') {
    runInternal(rt, inv.handler)
  } else {
    runProcess(rt, inv.command, inv.args, inv.env)
  }

  return job
}

function runInternal(rt: JobRuntime, handler: string) {
  const fn = INTERNAL_HANDLERS[handler]
  if (!fn) {
    addLog(rt, 'stderr', `No internal handler registered for "${handler}"`)
    setStatus(rt, 'failed', 1, `Unknown handler: ${handler}`)
    return
  }
  fn((text) => addLog(rt, 'stdout', text))
    .then(() => setStatus(rt, 'succeeded', 0))
    .catch((err) => {
      const msg = err instanceof Error ? err.message : String(err)
      addLog(rt, 'stderr', msg)
      setStatus(rt, 'failed', 1, msg)
    })
}

function runProcess(rt: JobRuntime, command: string, args: string[], env: Record<string, string>) {
  let proc: ReturnType<typeof Bun.spawn>
  try {
    proc = Bun.spawn([command, ...args], {
      cwd: REPO_ROOT,
      env: { ...process.env, ...env },
      stdout: 'pipe',
      stderr: 'pipe',
    })
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    addLog(rt, 'stderr', `Failed to spawn "${command}": ${msg}`)
    setStatus(rt, 'failed', 1, msg)
    return
  }
  rt.proc = proc

  const stdout = pumpStream(proc.stdout as ReadableStream<Uint8Array>, (l) => addLog(rt, 'stdout', l))
  const stderr = pumpStream(proc.stderr as ReadableStream<Uint8Array>, (l) => addLog(rt, 'stderr', l))

  Promise.all([stdout, stderr, proc.exited])
    .then(([, , code]) => {
      if (rt.job.status === 'canceled') return
      if (code === 0) {
        setStatus(rt, 'succeeded', 0)
      } else {
        addLog(rt, 'system', `Process exited with code ${code}`)
        setStatus(rt, 'failed', code)
      }
    })
    .catch((err) => {
      const msg = err instanceof Error ? err.message : String(err)
      addLog(rt, 'stderr', msg)
      setStatus(rt, 'failed', 1, msg)
    })
}

export function cancelJob(id: string): { ok: boolean; message: string } {
  const rt = runtimes.get(id)
  if (!rt) return { ok: false, message: 'Job not found' }
  if (rt.job.status !== 'running') return { ok: false, message: `Job already ${rt.job.status}` }
  if (!rt.proc) {
    return { ok: false, message: 'In-process jobs cannot be canceled once started' }
  }
  addLog(rt, 'system', '⨯ Cancel requested — sending SIGTERM')
  rt.proc.kill()
  setStatus(rt, 'canceled', null)
  return { ok: true, message: 'Job canceled' }
}

export function listJobs(): Job[] {
  return order
    .map((id) => runtimes.get(id)?.job)
    .filter((j): j is Job => Boolean(j))
    .reverse()
}

export function getJob(id: string): { job: Job; logs: LogLine[] } | undefined {
  const rt = runtimes.get(id)
  if (!rt) return undefined
  return { job: rt.job, logs: rt.logs }
}

/**
 * Subscribe to a job's live log/status stream. Returns an unsubscribe fn.
 * The callback fires with { type:'log', line } or { type:'status', job }.
 */
export function subscribeJob(
  id: string,
  cb: (event: { type: 'log'; line: LogLine } | { type: 'status'; job: Job }) => void,
): (() => void) | undefined {
  const rt = runtimes.get(id)
  if (!rt) return undefined
  const onLog = (line: LogLine) => cb({ type: 'log', line })
  const onStatus = (job: Job) => cb({ type: 'status', job })
  rt.emitter.on('log', onLog)
  rt.emitter.on('status', onStatus)
  return () => {
    rt.emitter.off('log', onLog)
    rt.emitter.off('status', onStatus)
  }
}

export function jobStats() {
  let running = 0
  let succeeded = 0
  let failed = 0
  for (const id of order) {
    const s = runtimes.get(id)?.job.status
    if (s === 'running') running++
    else if (s === 'succeeded') succeeded++
    else if (s === 'failed') failed++
  }
  return { total: order.length, running, succeeded, failed }
}
