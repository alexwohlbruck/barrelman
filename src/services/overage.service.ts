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
import { envNumber } from '../config/env'

const REPORT_INTERVAL_MS = envNumber('BARRELMAN_OVERAGE_REPORT_MS', 15 * 60_000)

export interface OverageRow {
  user_id: string
  plan: string
  used: number
  purchased: number
  reported: number
}

export interface PendingOverage {
  userId: string
  /** Credits to ingest now — the delta since the last report. */
  credits: number
  /** Cycle-to-date total, stored as the new high-water mark. */
  total: number
}

/**
 * Work out what still needs reporting, as a pure function of the query rows.
 *
 * Reporting the *delta against a stored total* rather than "usage since last
 * time" is what makes a crashed pass harmless: the next one recomputes the same
 * figure. Extracted so that property is testable without a database or a
 * billing provider.
 */
export function computePendingOverage(rows: OverageRow[]): PendingOverage[] {
  const pending: PendingOverage[] = []

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

  return pending
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

  const pending = computePendingOverage(rows)

  if (pending.length === 0) return { accounts: 0, credits: 0 }

  const ingested = await reportOverage(pending.map(({ userId, credits }) => ({ userId, credits })))
  if (ingested === 0) {
    // The provider refused the batch. Record nothing — the next pass recomputes
    // the same delta and tries again.
    return { accounts: 0, credits: 0 }
  }

  // One statement, so the batch lands atomically. Row-by-row writes left a
  // window where the provider had accepted the whole batch but only some
  // accounts had their high-water mark recorded — a crash there re-reports the
  // rest on the next pass, billing those credits twice.
  try {
    await sql`
      INSERT INTO accounts_overage_reports ${sql(
        pending.map((entry) => ({
          user_id: entry.userId,
          cycle,
          reported_credits: entry.total,
        })),
        'user_id',
        'cycle',
        'reported_credits',
      )}
      ON CONFLICT (user_id, cycle) DO UPDATE
        SET reported_credits = EXCLUDED.reported_credits, updated_at = now()`
  } catch (err) {
    // Polar has the events but we failed to record that. Say so loudly: the
    // next pass will re-report the same delta, and someone has to reconcile.
    console.error(
      `[billing] ingested ${pending.length} overage event(s) but FAILED to record the high-water ` +
        'mark — the next pass will re-report them. Reconcile in Polar before it runs.',
      err,
    )
    throw err
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
