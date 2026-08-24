/**
 * Shared job types + invocation builder.
 *
 * Both the API (which enqueues jobs and renders their display command) and the
 * ops worker (which actually spawns the process) need to turn a manifest script
 * + user params into a concrete argv/env. Keeping it here avoids duplicating the
 * param-application rules and keeps the display string identical on both sides.
 */
import { type ScriptDef, type DangerLevel, type ScriptCategory } from '../admin/scripts-manifest'

export type JobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'canceled'
export type LogStream = 'stdout' | 'stderr' | 'system'
export type ExecKind = 'internal' | 'process'
/** What put the job in the queue: an operator in the console, or a schedule. */
export type JobTrigger = 'manual' | 'schedule'

export interface LogLine {
  seq: number
  t: number
  stream: LogStream
  text: string
}

/**
 * Where a queued process job sits in line.
 *
 * The ops worker is a single serial loop, so "queued" always means "waiting
 * behind one specific job", never "waiting for a free slot". Naming the blocker
 * is the difference between a job that looks stuck and one that is visibly
 * third in line behind a nine-hour import.
 */
export interface QueuePlacement {
  /** 1-based position among queued process jobs. */
  position: number
  /** The job that has to finish before this one can start. */
  waitingOn?: { id: string; scriptId: string; scriptName: string; status: JobStatus }
}

export interface Job {
  id: string
  scriptId: string
  scriptName: string
  category: ScriptCategory
  danger: DangerLevel
  execKind: ExecKind
  status: JobStatus
  trigger: JobTrigger
  /** Set when trigger is 'schedule' — the schedule that enqueued this run. */
  scheduleId?: string
  params: Record<string, unknown>
  displayCommand: string
  createdAt: number
  startedAt?: number
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
  /**
   * Named stage breakdown, parsed from `[N/M] Stage name` log markers. `index`
   * is 1-based; `labels[i]` is the name of stage i (empty until first seen).
   */
  stages?: { total: number; index: number; labels: string[] }
  /** Set only while `status === 'queued'` — see QueuePlacement. */
  queue?: QueuePlacement
}

export type Invocation =
  | { kind: 'internal'; handler: string; env: Record<string, string>; display: string }
  | { kind: 'process'; command: string; args: string[]; env: Record<string, string>; display: string }

/** Build the concrete invocation (argv + env) from a script + user params. */
export function buildInvocation(script: ScriptDef, params: Record<string, unknown>): Invocation {
  const env: Record<string, string> = { ...(script.env ?? {}) }

  if (script.exec.kind === 'internal') {
    return { kind: 'internal', handler: script.exec.handler, env, display: `internal: ${script.exec.handler}` }
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
        const str = String(val)
        // A value starting with "-" (a bbox like "-109.06,36.99,...") is read as
        // the next option by node:util parseArgs, which import-gtfs.ts uses:
        //   TypeError: Option '--region' argument is ambiguous.
        // The "=" form is unambiguous. Only used when needed, since some
        // importers parse argv by index and expect the separated form.
        if (str.startsWith('-')) args.push(`${flag}=${str}`)
        else args.push(flag, str)
      }
    } else if (p.apply === 'positional') {
      if (typeof val === 'string' && val.trim()) positional.push(...val.trim().split(/\s+/))
    }
  }

  args.push(...positional)

  const display = [script.exec.command, ...args]
    .map((a) => (secretValues.has(a) ? '••••••' : a))
    .join(' ')

  return { kind: 'process', command: script.exec.command, args, env, display }
}

/**
 * Whether only one run of this script may be in flight at a time.
 *
 * An explicit `exclusive` wins; otherwise every long-running script is exclusive,
 * since those are the imports that corrupt each other when overlapped. Three
 * places enforce this (the enqueue guard, the worker's advisory lock, and the
 * stored `exclusive` column) and they must agree, so they all read it from here.
 */
export function isExclusive(script: ScriptDef): boolean {
  return script.exclusive ?? script.longRunning
}

/** Stable 31-bit advisory-lock key derived from a script id (for exclusive single-flight). */
export function advisoryKeyFor(scriptId: string): number {
  let h = 0
  for (let i = 0; i < scriptId.length; i++) {
    h = (h * 31 + scriptId.charCodeAt(i)) | 0
  }
  // keep it positive and within int4 range for pg_try_advisory_lock(int)
  return Math.abs(h) % 2147483647
}
