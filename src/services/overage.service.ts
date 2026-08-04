/**
 * Reports metered overage to the billing provider.
 *
 * Runs periodically rather than per request. Overage is a slow-moving quantity —
 * an account has to burn a whole monthly allowance before any of it exists — and
 * batching keeps a provider outage away from the request path entirely.
 *
 * Idempotence is the important property, since this runs on a timer in a process
 * that can restart at any moment. Rather than reporting "what happened since
 * last time", it computes the account's total overage for the cycle, compares it
 * against what has already been reported, and ingests only the difference. A
 * crash mid-run therefore re-reports nothing; the next pass simply recomputes
 * the same delta.
 */
import { connection as sql } from '../db'
import { getPlan } from '../billing/plans'
import { billing } from '../config/billing.config'
import { reportOverage } from './billing.service'
import { currentCycleStart, flushUsage } from './usage.service'

const REPORT_INTERVAL_MS = Number(process.env.BARRELMAN_OVERAGE_REPORT_MS ?? 15 * 60_000)

interface OverageRow {
  user_id: string
  plan: string
  used: number
  purchased: number
  reported: number
}

/**
 * One pass. Returns how many accounts had overage reported, so the caller (and
 * the tests) can see whether it did anything.
 */
export async function reportPendingOverage(): Promise<{ accounts: number; credits: number }> {
  if (!billing.enabled || !billing.meterUsage) return { accounts: 0, credits: 0 }

  // Report against persisted counters only, so the numbers reconcile with what
  // the usage table would show if queried at the same moment.
  await flushUsage()

  const cycle = currentCycleStart()

  const rows = await sql<OverageRow[]>`
    SELECT
      u.id                                        AS user_id,
      u.plan                                      AS plan,
      COALESCE(usage.credits, 0)::int             AS used,
      COALESCE(ledger.credits, 0)::int            AS purchased,
      COALESCE(reports.reported_credits, 0)::int  AS reported
    FROM accounts_users u
    LEFT JOIN (
      SELECT user_id, SUM(credits) AS credits
      FROM accounts_usage
      WHERE day >= ${cycle}
      GROUP BY user_id
    ) usage ON usage.user_id = u.id
    LEFT JOIN (
      SELECT user_id, SUM(amount) AS credits
      FROM accounts_credit_ledger
      GROUP BY user_id
    ) ledger ON ledger.user_id = u.id
    LEFT JOIN accounts_overage_reports reports
      ON reports.user_id = u.id AND reports.cycle = ${cycle}
    WHERE u.plan <> 'free'
      AND u.suspended_at IS NULL
      AND COALESCE(usage.credits, 0) > 0`

  const pending: Array<{ userId: string; credits: number; total: number }> = []

  for (const row of rows) {
    const plan = getPlan(row.plan)
    if (!plan.overageAllowed) continue

    // Purchased credits are spent before overage begins: a customer who bought
    // a pack has already paid for those, and billing them again as metered
    // usage would charge twice for the same credits.
    const totalOverage = Math.max(0, row.used - plan.monthlyCredits - row.purchased)
    const delta = totalOverage - row.reported
    if (delta > 0) pending.push({ userId: row.user_id, credits: delta, total: totalOverage })
  }

  if (pending.length === 0) return { accounts: 0, credits: 0 }

  const ingested = await reportOverage(pending.map(({ userId, credits }) => ({ userId, credits })))
  if (ingested === 0) {
    // The provider refused the batch. Record nothing — the next pass recomputes
    // the same delta and tries again.
    return { accounts: 0, credits: 0 }
  }

  for (const entry of pending) {
    await sql`
      INSERT INTO accounts_overage_reports (user_id, cycle, reported_credits)
      VALUES (${entry.userId}, ${cycle}, ${entry.total})
      ON CONFLICT (user_id, cycle) DO UPDATE
        SET reported_credits = ${entry.total}, updated_at = now()`
  }

  const credits = pending.reduce((sum, entry) => sum + entry.credits, 0)
  console.log(`[billing] reported ${credits} overage credits for ${pending.length} account(s)`)
  return { accounts: pending.length, credits }
}

/**
 * Start the periodic reporter.
 *
 * Guarded on globalThis for the same reason as the usage flush: `bun --hot`
 * re-evaluates modules without tearing down their timers, so an unguarded
 * interval accumulates one copy per edit — and here that would mean several
 * concurrent passes racing on the same rows.
 */
const INTERVAL_KEY = Symbol.for('barrelman.overage.reportInterval')

export function startOverageReporting(): void {
  if (!billing.enabled || !billing.meterUsage) return

  const globals = globalThis as Record<symbol, unknown>
  if (globals[INTERVAL_KEY]) return

  const timer = setInterval(() => {
    reportPendingOverage().catch((err) => console.error('[billing] overage reporting failed', err))
  }, REPORT_INTERVAL_MS)
  timer.unref?.()

  globals[INTERVAL_KEY] = timer
}

export function stopOverageReporting(): void {
  const globals = globalThis as Record<symbol, unknown>
  const timer = globals[INTERVAL_KEY] as ReturnType<typeof setInterval> | undefined
  if (timer) clearInterval(timer)
  globals[INTERVAL_KEY] = undefined
}
