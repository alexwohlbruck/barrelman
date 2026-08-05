/**
 * Periodic cleanup of expired account state.
 *
 * None of this is load-bearing — an expired session or a used-up one-time code
 * is already refused on its own merits — but without a sweep the tables grow
 * without bound, and the rate-limiter maps grow with every unique key seen.
 * Sessions in particular are credential material, so keeping dead ones around
 * is pure liability.
 */
import { connection as sql } from '../db'
import { pruneExpiredTokens } from './auth.service'
import { pruneSignupAttempts } from './accounts.service'
import { pruneRateLimiters } from '../lib/rate-limit'
import { pruneThrottleState } from './throttle.service'
import { expireSuspensions } from './moderation.service'
import { runAbuseDetection } from './abuse-detection.service'
import { envNumber } from '../config/env'

const SWEEP_INTERVAL_MS = envNumber('BARRELMAN_ACCOUNT_SWEEP_MS', 60 * 60_000)

export interface SweepResult {
  sessions: number
  suspensionsLifted: number
  flagged: number
  autoSuspended: number
}

export async function sweepAccounts(): Promise<SweepResult> {
  // Lucia validates expiry on read but never deletes on its own, so expired
  // rows accumulate for the lifetime of the instance.
  const expired = await sql`DELETE FROM accounts_sessions WHERE expires_at < now() RETURNING id`

  await pruneExpiredTokens()
  await pruneSignupAttempts()
  pruneRateLimiters()
  pruneThrottleState()

  // A timed suspension that never lifts is an indefinite one with extra steps.
  const suspensionsLifted = await expireSuspensions()

  // Detection is best-effort: a failure here must not stop the rest of the
  // sweep, which is doing the janitorial work that keeps tables bounded.
  let flagged = 0
  let autoSuspended = 0
  try {
    const result = await runAbuseDetection()
    flagged = result.flagged
    autoSuspended = result.suspended
  } catch (err) {
    console.error('[abuse] detection pass failed', err)
  }

  return { sessions: expired.length, suspensionsLifted, flagged, autoSuspended }
}

/**
 * Guarded on globalThis: `bun --hot` re-evaluates modules without tearing down
 * their timers, so an unguarded interval accumulates one copy per edit.
 */
const INTERVAL_KEY = Symbol.for('barrelman.accounts.sweepInterval')

export function startAccountSweep(): void {
  const globals = globalThis as Record<symbol, unknown>
  if (globals[INTERVAL_KEY]) return

  const run = () =>
    sweepAccounts()
      .then(({ sessions, suspensionsLifted, flagged, autoSuspended }) => {
        if (sessions > 0) console.log(`[accounts] swept ${sessions} expired session(s)`)
        if (suspensionsLifted > 0) console.log(`[moderation] lifted ${suspensionsLifted} expired suspension(s)`)
        if (flagged > 0 || autoSuspended > 0) {
          console.warn(`[abuse] ${flagged} new signal(s), ${autoSuspended} automatic hold(s)`)
        }
      })
      .catch((err) => console.error('[accounts] sweep failed', err))

  const timer = setInterval(run, SWEEP_INTERVAL_MS)
  timer.unref?.()
  globals[INTERVAL_KEY] = timer

  // One pass at startup clears whatever expired while the process was down.
  void run()
}

export function stopAccountSweep(): void {
  const globals = globalThis as Record<symbol, unknown>
  const timer = globals[INTERVAL_KEY] as ReturnType<typeof setInterval> | undefined
  if (timer) clearInterval(timer)
  globals[INTERVAL_KEY] = undefined
}
