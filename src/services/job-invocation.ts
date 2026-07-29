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
  execKind: ExecKind
  status: JobStatus
  params: Record<string, unknown>
  displayCommand: string
  createdAt: number
  startedAt?: number
  endedAt?: number
  durationMs?: number
  exitCode?: number | null
  error?: string
  logCount: number
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

  return { kind: 'process', command: script.exec.command, args, env, display }
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
