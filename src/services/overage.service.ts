/**
 * Reports metered overage to the billing provider.
 *
 * Runs periodically rather than per request. Overage is a slow-moving quantity —
 * an account has to burn a whole monthly allowance before any of it exists — and
 * batching keeps a provider outage away from the request path entirely.
 *
 * Idempotence is the important property, since this runs on a timer in a process
 * that can restart at any moment, possibly on several replicas. Rather than
 * reporting "what happened since last time", it computes the account's total
 * overage for the period, compares it against what has already been reported,
 * and ingests only the difference. Three things keep that exact:
 *
 *   - The total about to be sent is written down first (`pending_credits`), and
 *     each event carries an id derived from (account, period, total). A pass
 *     that fails or dies between sending and recording resends exactly that
 *     event on the next pass, and Polar drops the duplicate if it already had
 *     it. Computing a fresh delta instead would overlap whatever Polar holds.
 *   - A Postgres advisory lock lets one pass run at a time across replicas.
 *   - Each pass covers the period that just closed as well as the open one, so
 *     the usage between the last pass of a period and its end is still billed.
 */
import { connection as sql } from '../db'
import { getPlan, type Plan } from '../billing/plans'
import { billing } from '../config/billing.config'
import { addMonths, billingPeriod, calendarPeriod, previousPeriod } from '../billing/periods'
import { reportOverage } from './billing.service'
import { packUsageSince, settlePackUsage } from './credits.service'
import { creditsInRange, flushUsage, type DailyCredits } from './usage.service'
import { envNumber } from '../config/env'

const REPORT_INTERVAL_MS = envNumber('BARRELMAN_OVERAGE_REPORT_MS', 15 * 60_000)

/**
 * Advisory lock key, in the two-int form so it can never collide with the
 * single-key locks the job runner takes per script.
 */
const LOCK_CLASS = 0x6f766572 // "over"
const LOCK_ID = 1

export interface OverageRow {
  user_id: string
  plan: string
  /** Start of the billing period this row covers. */
  cycle: string
  used: number
  /** Pack credit available to the period: the ledger balance while it is open, what it took once closed. */
  purchased: number
  reported: number
}

export interface PendingOverage {
  userId: string
  cycle: string
  /** Credits to ingest now — the delta since the last report. */
  credits: number
  /** Period-to-date total, stored as the new high-water mark. */
  total: number
}

/** Event id for one report. The same account, period and total always produce the same id. */
export function overageEventId(userId: string, cycle: string, total: number): string {
  return `overage:${userId}:${cycle}:${total}`
}

/**
 * Work out what still needs reporting, as a pure function of the query rows.
 *
 * Reporting the *delta against a stored total* rather than "usage since last
 * time" is what makes a crashed pass harmless: the next one recomputes the same
 * figure. `plan` overrides each row's plan, for reporting what an account owed
 * on the plan it is leaving.
 */
export function computePendingOverage(rows: OverageRow[], plan?: Plan): PendingOverage[] {
  const pending: PendingOverage[] = []

  for (const row of rows) {
    const rowPlan = plan ?? getPlan(row.plan)
    if (!rowPlan.overageAllowed) continue

    // Purchased credits are spent before overage begins: a customer who bought
    // a pack has already paid for those, and billing them again as metered
    // usage would charge twice for the same credits.
    const total = Math.max(0, row.used - rowPlan.monthlyCredits - row.purchased)
    const delta = total - row.reported
    if (delta > 0) pending.push({ userId: row.user_id, cycle: row.cycle, credits: delta, total })
  }

  return pending
}

export interface AccountUsage {
  userId: string
  plan: string
  anchor: Date | string | null
  /** Daily credits since at least the start of the previous period. */
  days: DailyCredits[]
  /** Ledger balance now, after closed periods have been settled. */
  ledgerBalance: number
  /** Pack credit each closed period took, by period start. */
  packUsage: Record<string, number>
  /** Overage already reported, by period start. */
  reported: Record<string, number>
}

/**
 * One row for the open period and one for the period before it, per account.
 *
 * The closed period is included so its last stretch is billed: without it, the
 * usage between a period's final pass and its end was never reported, because
 * every pass after the boundary looked only at the new period.
 */
export function buildOverageRows(accounts: AccountUsage[], now: Date = new Date(), plan?: Plan): OverageRow[] {
  const rows: OverageRow[] = []

  for (const account of accounts) {
    const accountPlan = plan ?? getPlan(account.plan)
    const current = billingPeriod(accountPlan, account.anchor, now)
    const previous = previousPeriod(accountPlan, account.anchor, current)

    const previousUsed = creditsInRange(account.days, previous.start, previous.end)
    if (previousUsed > 0) {
      rows.push({
        user_id: account.userId,
        plan: account.plan,
        cycle: previous.start,
        used: previousUsed,
        purchased: account.packUsage[previous.start] ?? 0,
        reported: account.reported[previous.start] ?? 0,
      })
    }

    rows.push({
      user_id: account.userId,
      plan: account.plan,
      cycle: current.start,
      used: creditsInRange(account.days, current.start, current.end),
      purchased: account.ledgerBalance,
      reported: account.reported[current.start] ?? 0,
    })
  }

  return rows
}

/** Earliest day a previous period can start: two calendar months back covers any anchor. */
function windowStart(now: Date): string {
  return addMonths(calendarPeriod(now).start, -2)
}

/**
 * Gather usage for accounts that may owe overage. Suspended accounts are
 * included: a suspension stops new usage, not the bill for what was used.
 */
async function loadAccounts(now: Date, only?: { userId: string; plan?: Plan }): Promise<AccountUsage[]> {
  const from = windowStart(now)

  const users = await sql<Array<{ id: string; plan: string; anchor: Date | null }>>`
    SELECT u.id, u.plan, u.billing_period_start AS anchor
    FROM accounts_users u
    WHERE ${only ? sql`u.id = ${only.userId}` : sql`u.plan <> 'free'`}
      AND EXISTS (
        SELECT 1 FROM accounts_usage a
        WHERE a.user_id = u.id AND a.day >= ${from} AND a.credits > 0
      )`

  const eligible = users.filter((user) => (only?.plan ?? getPlan(user.plan)).overageAllowed)
  if (eligible.length === 0) return []

  // Settle closed periods first, so the pack balance and each closed period's
  // share of it are final before overage is computed against them.
  for (const user of eligible) await settlePackUsage(user.id, only?.plan ?? getPlan(user.plan), user.anchor)

  const ids = eligible.map((user) => user.id)
  const [days, ledger, reports, packUsage] = await Promise.all([
    sql<Array<{ user_id: string; day: string; credits: number }>>`
      SELECT user_id, day::text AS day, SUM(credits)::int AS credits
      FROM accounts_usage
      WHERE day >= ${from} AND user_id IN ${sql(ids)}
      GROUP BY user_id, day`,
    sql<Array<{ user_id: string; total: number }>>`
      SELECT user_id, SUM(amount)::int AS total
      FROM accounts_credit_ledger
      WHERE user_id IN ${sql(ids)}
      GROUP BY user_id`,
    sql<Array<{ user_id: string; cycle: string; reported: number }>>`
      SELECT user_id, cycle::text AS cycle, reported_credits AS reported
      FROM accounts_overage_reports
      WHERE cycle >= ${from} AND user_id IN ${sql(ids)}`,
    packUsageSince(ids, from),
  ])

  return eligible.map((user) => ({
    userId: user.id,
    plan: user.plan,
    anchor: user.anchor,
    days: days.filter((row) => row.user_id === user.id).map(({ day, credits }) => ({ day, credits })),
    ledgerBalance: ledger.find((row) => row.user_id === user.id)?.total ?? 0,
    packUsage: packUsage.get(user.id) ?? {},
    reported: Object.fromEntries(
      reports.filter((row) => row.user_id === user.id).map((row) => [row.cycle, row.reported]),
    ),
  }))
}

type Sql = typeof sql

/** Write down what is about to be sent. Never touches `reported_credits` on an existing row. */
async function stage(db: Sql, pending: PendingOverage[]): Promise<void> {
  await db`
    INSERT INTO accounts_overage_reports ${db(
      pending.map((entry) => ({
        user_id: entry.userId,
        cycle: entry.cycle,
        reported_credits: 0,
        pending_credits: entry.total,
      })),
      'user_id',
      'cycle',
      'reported_credits',
      'pending_credits',
    )}
    ON CONFLICT (user_id, cycle) DO UPDATE
      SET pending_credits = EXCLUDED.pending_credits, updated_at = now()`
}

/**
 * Send staged reports and, once Polar has them, promote them to reported.
 * Returns false when the send failed; the rows stay staged for the next pass.
 */
async function sendStaged(pending: PendingOverage[]): Promise<boolean> {
  const toSend = pending.filter((entry) => entry.credits > 0)
  if (toSend.length > 0) {
    const ingested = await reportOverage(
      toSend.map((entry) => ({
        userId: entry.userId,
        credits: entry.credits,
        externalId: overageEventId(entry.userId, entry.cycle, entry.total),
      })),
    )
    if (ingested === 0) return false
  }

  try {
    // One transaction, so the batch is recorded together.
    await sql.begin(async (rawTx) => {
      const tx = rawTx as unknown as Sql
      for (const entry of pending) {
        await tx`
          UPDATE accounts_overage_reports
          SET reported_credits = pending_credits, pending_credits = NULL, updated_at = now()
          WHERE user_id = ${entry.userId} AND cycle = ${entry.cycle} AND pending_credits = ${entry.total}`
      }
    })
  } catch (err) {
    // Polar has the events but the high-water mark was not recorded. The rows
    // are still staged, so the next pass resends the same event ids and Polar
    // drops them as duplicates.
    console.error(`[billing] ingested ${pending.length} overage event(s) but could not record them; will resend`, err)
  }
  return true
}

async function stagedReports(): Promise<PendingOverage[]> {
  const rows = await sql<Array<{ user_id: string; cycle: string; reported: number; pending: number }>>`
    SELECT user_id, cycle::text AS cycle, reported_credits AS reported, pending_credits AS pending
    FROM accounts_overage_reports
    WHERE pending_credits IS NOT NULL`
  return rows.map((row) => ({
    userId: row.user_id,
    cycle: row.cycle,
    credits: row.pending - row.reported,
    total: row.pending,
  }))
}

/**
 * Run `fn` holding the reporting lock, or return null when another process
 * holds it. A session lock needs one connection for both lock and unlock, so it
 * is taken on a reserved one; the work itself uses the pool.
 */
async function withReportingLock<T>(fn: () => Promise<T>, wait = false): Promise<T | null> {
  const conn = await sql.reserve()
  try {
    if (wait) {
      // Bounded, so a stuck pass cannot hang the caller; past it this throws.
      await conn`SET lock_timeout = '10s'`
      try {
        await conn`SELECT pg_advisory_lock(${LOCK_CLASS}::int, ${LOCK_ID}::int)`
      } finally {
        await conn`RESET lock_timeout`
      }
    } else {
      const [row] = await conn<Array<{ locked: boolean }>>`
        SELECT pg_try_advisory_lock(${LOCK_CLASS}::int, ${LOCK_ID}::int) AS locked`
      if (!row?.locked) return null
    }
    try {
      return await fn()
    } finally {
      await conn`SELECT pg_advisory_unlock(${LOCK_CLASS}::int, ${LOCK_ID}::int)`
    }
  } finally {
    conn.release()
  }
}

export interface ReportOptions {
  /**
   * Report one account only, and throw if the provider refuses. For the last
   * chance to bill an account, such as before deleting it. Waits for a pass in
   * progress rather than skipping.
   */
  userId?: string
  /** The clock, for tests. */
  now?: Date
}

/**
 * One pass. Returns how many reports were sent and their credits, so the caller
 * (and the tests) can see whether it did anything. Returns zeros without doing
 * anything when another replica's pass holds the lock.
 */
export async function reportPendingOverage(options: ReportOptions = {}): Promise<{ accounts: number; credits: number }> {
  const none = { accounts: 0, credits: 0 }
  if (!billing.enabled || !billing.meterUsage) return none
  const { userId, now = new Date() } = options
  const refused = () => {
    if (userId) throw new Error(`the billing provider refused the overage report for ${userId}`)
    return none
  }

  const result = await withReportingLock(async () => {
    // Report against persisted counters only, so the numbers reconcile with
    // what the usage table would show if queried at the same moment.
    await flushUsage()

    // Anything staged by an earlier pass, or by a plan change, goes first and
    // exactly as staged. New deltas are computed only once it is through, since
    // they are measured from the mark it moves.
    const staged = await stagedReports()
    if (staged.length > 0 && !(await sendStaged(staged))) return refused()

    const accounts = await loadAccounts(now, userId ? { userId } : undefined)
    const pending = computePendingOverage(buildOverageRows(accounts, now))
    if (pending.length > 0) {
      await stage(sql, pending)
      if (!(await sendStaged(pending))) return refused()
    }

    const sent = [...staged, ...pending].filter((entry) => entry.credits > 0)
    return { accounts: sent.length, credits: sent.reduce((sum, entry) => sum + entry.credits, 0) }
  }, Boolean(userId))

  if (!result) return none
  if (result.credits > 0) console.log(`[billing] reported ${result.credits} overage credits in ${result.accounts} report(s)`)
  return result
}

/**
 * Stage what an account owes on `plan` before it moves off it. The next pass
 * sends whatever is staged regardless of the account's plan by then, so
 * overage accrued before a downgrade to free is still billed.
 *
 * Waits for the reporting lock rather than skipping, because a pass running
 * concurrently could otherwise overwrite or promote the staged figure. The wait
 * is bounded; past it the change goes ahead and the remainder is logged as lost.
 */
export async function stageOverageForPlanChange(userId: string, plan: Plan, now: Date = new Date()): Promise<void> {
  if (!billing.enabled || !billing.meterUsage || !plan.overageAllowed) return

  await sql.begin(async (rawTx) => {
    const tx = rawTx as unknown as Sql
    await tx`SET LOCAL lock_timeout = '10s'`
    await tx`SELECT pg_advisory_xact_lock(${LOCK_CLASS}::int, ${LOCK_ID}::int)`
    await flushUsage()

    const pending = computePendingOverage(buildOverageRows(await loadAccounts(now, { userId, plan }), now, plan), plan)
    if (pending.length === 0) return

    // A report still staged from a failed pass carries its own event id, which
    // Polar may already hold. Overwriting its total would orphan that id, so
    // leave it; only the usage since that pass goes unbilled.
    const busy = await tx<Array<{ cycle: string }>>`
      SELECT cycle::text AS cycle FROM accounts_overage_reports
      WHERE user_id = ${userId} AND pending_credits IS NOT NULL`
    const blocked = new Set(busy.map((row) => row.cycle))
    const stageable = pending.filter((entry) => !blocked.has(entry.cycle))
    if (stageable.length < pending.length) {
      console.warn(`[billing] ${userId} left ${plan.id} with an unsent report pending; newer overage is not billed`)
    }
    if (stageable.length > 0) await stage(tx, stageable)
  })
}

/**
 * Start the periodic reporter.
 *
 * Guarded on globalThis for the same reason as the usage flush: `bun --hot`
 * re-evaluates modules without tearing down their timers, so an unguarded
 * interval accumulates one copy per edit.
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
