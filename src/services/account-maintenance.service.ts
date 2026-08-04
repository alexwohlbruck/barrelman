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
import { pruneRateBuckets } from '../middleware/api-auth'

const SWEEP_INTERVAL_MS = Number(process.env.BARRELMAN_ACCOUNT_SWEEP_MS ?? 60 * 60_000)

export interface SweepResult {
  sessions: number
}

export async function sweepAccounts(): Promise<SweepResult> {
  // Lucia validates expiry on read but never deletes on its own, so expired
  // rows accumulate for the lifetime of the instance.
  const expired = await sql`DELETE FROM accounts_sessions WHERE expires_at < now() RETURNING id`

  await pruneExpiredTokens()
  await pruneSignupAttempts()
  pruneRateLimiters()
  pruneRateBuckets()

  return { sessions: expired.length }
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
      .then(({ sessions }) => {
        if (sessions > 0) console.log(`[accounts] swept ${sessions} expired session(s)`)
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
