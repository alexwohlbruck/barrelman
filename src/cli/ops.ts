/**
 * barrelman ops CLI — run and inspect maintenance jobs from a shell.
 *
 * This is the same job queue the admin console drives, reached a different way.
 * The console posts to `/admin/scripts/:id/run`, which is gated on an
 * admin-scoped account credential; that gate exists because the API listens on
 * a public interface in production and a full re-import is destructive. This CLI
 * skips HTTP entirely and enqueues straight into Postgres, so its trust boundary
 * is "can reach the database" — which, for anyone who can already read
 * DATABASE_URL or exec into the containers, is total control regardless. It adds
 * no new remotely-reachable surface, in the same way `docker` trusts its socket.
 *
 * Jobs enqueued here are indistinguishable from console ones: the ops worker
 * claims `process` jobs, logs stream to the same tables, and runs appear in the
 * console's Jobs view.
 *
 *   bun run src/cli/ops.ts scripts
 *   bun run src/cli/ops.ts run osm-full-import --REGIONS=north-carolina,nyc-metro
 *   bun run src/cli/ops.ts jobs
 *   bun run src/cli/ops.ts logs <job-id> --follow
 *   bun run src/cli/ops.ts cancel <job-id>
 */

import { connection as sql } from '../db'
import { SCRIPTS, CATEGORY_LABELS, getScript } from '../admin/scripts-manifest'
import { startJob, cancelJob, JobConflictError } from '../services/job-runner.service'
import * as store from '../services/ops-job-store'

const TERMINAL = new Set(['succeeded', 'failed', 'canceled'])
const POLL_MS = 1000

/** Colour only when attached to a terminal, so piped output stays clean. */
const tty = process.stdout.isTTY
const c = (code: string, s: string) => (tty ? `\x1b[${code}m${s}\x1b[0m` : s)
const dim = (s: string) => c('2', s)
const bold = (s: string) => c('1', s)
const red = (s: string) => c('31', s)
const green = (s: string) => c('32', s)
const yellow = (s: string) => c('33', s)

function statusColor(status: string): string {
  if (status === 'succeeded') return green(status)
  if (status === 'failed') return red(status)
  if (status === 'running') return yellow(status)
  return dim(status)
}

function usage(): void {
  console.log(`
${bold('barrelman ops')} — run maintenance jobs without going through the console

  ${bold('scripts')} [category]        list runnable scripts
  ${bold('run')} <script-id> [--K=V]   enqueue a script (see 'scripts' for params)
  ${bold('jobs')} [--limit N]          recent jobs, newest first
  ${bold('logs')} <job-id> [--follow]  print a job's log
  ${bold('cancel')} <job-id>           request cancellation

Options for ${bold('run')}:
  --follow, -f            stream logs until the job finishes (exit code mirrors it)
  --yes, -y               skip the confirmation on destructive scripts

Parameters are passed as --NAME=VALUE and validated against the manifest, so a
misspelled parameter is refused rather than silently ignored.
`)
}

/** Split argv into positionals and --flags, accepting `--k=v` and bare `--k`. */
function parseArgs(argv: string[]): { positional: string[]; flags: Record<string, string> } {
  const positional: string[] = []
  const flags: Record<string, string> = {}

  for (const arg of argv) {
    if (!arg.startsWith('-')) {
      positional.push(arg)
      continue
    }
    const body = arg.replace(/^--?/, '')
    const eq = body.indexOf('=')
    if (eq >= 0) flags[body.slice(0, eq)] = body.slice(eq + 1)
    else flags[body] = 'true'
  }

  return { positional, flags }
}

function cmdScripts(category?: string): void {
  const byCategory = new Map<string, typeof SCRIPTS>()
  for (const s of SCRIPTS) {
    if (category && s.category !== category) continue
    const list = byCategory.get(s.category) ?? []
    list.push(s)
    byCategory.set(s.category, list)
  }

  if (!byCategory.size) {
    const known = [...new Set(SCRIPTS.map(s => s.category))].join(', ')
    console.error(`No scripts in category "${category}". Known: ${known}`)
    process.exitCode = 1
    return
  }

  for (const [cat, list] of byCategory) {
    console.log(`\n${bold(CATEGORY_LABELS[cat as keyof typeof CATEGORY_LABELS] ?? cat)}`)
    for (const s of list) {
      const danger =
        s.danger === 'destructive' ? red(' [destructive]')
        : s.danger === 'caution' ? yellow(' [caution]')
        : ''
      console.log(`  ${s.id.padEnd(38)} ${s.name}${danger}`)
      for (const p of s.params ?? []) {
        const dflt = p.default !== undefined ? dim(` (default: ${p.default})`) : ''
        console.log(`      ${dim(`--${p.name}`)} ${dim(p.type)}${dflt}`)
      }
    }
  }
  console.log()
}

async function cmdRun(scriptId: string | undefined, flags: Record<string, string>): Promise<void> {
  if (!scriptId) {
    console.error('run: a script id is required (see: scripts)')
    process.exitCode = 1
    return
  }

  const script = getScript(scriptId)
  if (!script) {
    console.error(`Unknown script: ${scriptId} (see: scripts)`)
    process.exitCode = 1
    return
  }

  const follow = flags.follow === 'true' || flags.f === 'true'
  const assumeYes = flags.yes === 'true' || flags.y === 'true'

  // Anything not a known parameter is a typo. Silently dropping it is how you
  // end up running a destructive import with defaults you didn't intend.
  const reserved = new Set(['follow', 'f', 'yes', 'y'])
  const declared = new Map((script.params ?? []).map(p => [p.name, p]))
  const params: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(flags)) {
    if (reserved.has(key)) continue
    const param = declared.get(key)
    if (!param) {
      const known = [...declared.keys()].join(', ') || '(none)'
      console.error(`Unknown parameter "--${key}" for ${scriptId}. Accepts: ${known}`)
      process.exitCode = 1
      return
    }
    params[key] = param.type === 'boolean' ? value !== 'false' : value
  }

  if (script.danger === 'destructive' && !assumeYes) {
    console.error(
      `${red('Refusing to run a destructive script without --yes.')}\n` +
        `  ${script.name}: ${script.description}` +
        (script.notes ? `\n  ${dim(script.notes)}` : ''),
    )
    process.exitCode = 1
    return
  }

  let job
  try {
    job = await startJob(scriptId, params)
  } catch (err) {
    if (err instanceof JobConflictError) {
      console.error(`${err.message} — cancel it first, or wait for it to finish.`)
      process.exitCode = 1
      return
    }
    throw err
  }

  const where = job.execKind === 'internal' ? 'in this process' : 'on the ops worker'
  console.log(`${green('queued')} ${bold(scriptId)} → job ${job.id} (${where})`)
  if (Object.keys(params).length) console.log(dim(`  params: ${JSON.stringify(params)}`))

  // An internal job runs inside whoever enqueued it, so exiting now would kill
  // it mid-flight. Process jobs belong to the worker and outlive us.
  if (follow || job.execKind === 'internal') {
    if (!follow) console.log(dim('  running in-process — waiting for it to finish'))
    const status = await streamLogs(job.id, { quiet: !follow })
    process.exitCode = status === 'succeeded' ? 0 : 1
  } else {
    console.log(dim(`  follow with: logs ${job.id} --follow`))
  }
}

/** Print a job's logs, optionally until it reaches a terminal state. */
async function streamLogs(
  jobId: string,
  { follow = true, quiet = false }: { follow?: boolean; quiet?: boolean } = {},
): Promise<string> {
  let afterSeq = 0
  for (;;) {
    const lines = await store.readLogsSince(jobId, afterSeq)
    for (const line of lines) {
      afterSeq = Math.max(afterSeq, line.seq)
      if (quiet) continue
      const text = line.stream === 'stderr' ? red(line.text) : line.text
      console.log(text)
    }

    const found = await store.getJob(jobId)
    if (!found) {
      console.error(`Unknown job: ${jobId}`)
      return 'failed'
    }
    const { status, exitCode, error } = found.job
    if (TERMINAL.has(status)) {
      const suffix = exitCode !== null && exitCode !== undefined ? ` (exit ${exitCode})` : ''
      console.log(`\n${statusColor(status)}${suffix}${error ? `: ${error}` : ''}`)
      return status
    }
    if (!follow) return status

    await new Promise(r => setTimeout(r, POLL_MS))
  }
}

async function cmdJobs(limit: number): Promise<void> {
  const [jobs, stats] = await Promise.all([store.listJobs(), store.jobStats()])
  console.log(
    dim(
      `total ${stats.total}  queued ${stats.queued}  running ${stats.running}  ` +
        `succeeded ${stats.succeeded}  failed ${stats.failed}`,
    ),
  )
  if (!jobs.length) {
    console.log('\nNo jobs yet.')
    return
  }
  console.log()
  for (const job of jobs.slice(0, limit)) {
    const started = job.startedAt ? new Date(job.startedAt).toISOString().slice(11, 19) : '--:--:--'
    const took =
      job.startedAt && job.endedAt
        ? `${Math.round((job.endedAt - job.startedAt) / 1000)}s`
        : job.startedAt
          ? `${Math.round((Date.now() - job.startedAt) / 1000)}s…`
          : ''
    console.log(
      `${dim(started)}  ${statusColor(job.status.padEnd(9))} ${job.scriptId.padEnd(34)} ` +
        `${dim(took.padStart(7))}  ${dim(job.id)}`,
    )
  }
  console.log()
}

async function main(): Promise<void> {
  const { positional, flags } = parseArgs(process.argv.slice(2))
  const [command, arg] = positional

  switch (command) {
    case 'scripts':
      cmdScripts(arg)
      break
    case 'run':
      await cmdRun(arg, flags)
      break
    case 'jobs':
      await cmdJobs(Number(flags.limit ?? 20))
      break
    case 'logs': {
      if (!arg) {
        console.error('logs: a job id is required')
        process.exitCode = 1
        break
      }
      const follow = flags.follow === 'true' || flags.f === 'true'
      const status = await streamLogs(arg, { follow })
      if (TERMINAL.has(status) && status !== 'succeeded') process.exitCode = 1
      break
    }
    case 'cancel': {
      if (!arg) {
        console.error('cancel: a job id is required')
        process.exitCode = 1
        break
      }
      const { ok, message } = await cancelJob(arg)
      console.log(ok ? green(message) : yellow(message))
      if (!ok) process.exitCode = 1
      break
    }
    default:
      usage()
      if (command) process.exitCode = 1
  }
}

try {
  await main()
} finally {
  await sql.end({ timeout: 5 })
}
