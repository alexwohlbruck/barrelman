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
 * and ingests only the difference. Four things keep that exact:
 *
 *   - The total about to be sent is written down first (`pending_credits`), and
 *     each event carries an id derived from (account, period, total). A pass
 *     that fails or dies between sending and recording resends exactly that
 *     event on the next pass, and Polar drops the duplicate if it already had
 *     it. Computing a fresh delta instead would overlap whatever Polar holds.
 *   - What was already reported is matched by the days it covers, not by the
 *     period's start, so a moved anchor cannot make a period look unbilled.
 *   - A Postgres advisory lock lets one pass run at a time across replicas.
 *   - Each pass covers the period that just closed as well as the open one, so
 *     the usage between the last pass of a period and its end is still billed.
 *
 * Only those two periods are looked at. If reporting is down for longer than a
 * whole period, the periods before the last closed one go unreported; that
 * errs toward the customer, and the reconciliation task's drift log shows it.
 */
import { connection as sql } from '../db'
import { getPlan, type Plan } from '../billing/plans'
import { billing } from '../config/billing.config'
import {
  addMonths,
  billingPeriod,
  calendarPeriod,
  overlapping,
  previousPeriod,
  type BillingPeriod,
  type Span,
} from '../billing/periods'
import { reportOverage } from './billing.service'
import { PACK_USAGE_PREFIX, settlePackUsage } from './credits.service'
import { creditsInRange, flushUsage, type DailyCredits } from './usage.service'
import { envNumber } from '../config/env'

const REPORT_INTERVAL_MS = envNumber('BARRELMAN_OVERAGE_REPORT_MS', 15 * 60_000)

/**
 * Advisory lock key, in the two-int form so it can never collide with the
 * single-key locks the job runner takes per script.
 */
const LOCK_CLASS = 0x6f766572 // "over"
const LOCK_ID = 1

type Sql = typeof sql

export interface OverageRow {
  user_id: string
  plan: string
  /** Start of the billing period this row covers; the key its reports are stored under. */
  cycle: string
  /** First day after the period. */
  cycle_end: string
  used: number
  /** Pack credit available to the period: the ledger balance while it is open, what it took once closed. */
  purchased: number
  /** Overage already reported (or staged) under this period's own key. */
  reported: number
  /**
   * Overage reported under other keys whose days overlap this period, left by
   * an anchor that has since moved. Already billed, so never billed again.
   */
  elsewhere?: number
}

export interface PendingOverage {
  userId: string
  cycle: string
  cycleEnd: string
  /** Credits to ingest now — the delta since the last report. */
  credits: number
  /** Running total under this period's key, stored as the new high-water mark. */
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
    // usage would charge twice for the same credits. A pack refunded after it
    // was spent leaves the ledger negative; that is a debt, not overage, so it
    // is clamped at zero rather than billed through the meter.
    const owed = Math.max(0, row.used - rowPlan.monthlyCredits - Math.max(0, row.purchased))
    const delta = owed - row.reported - (row.elsewhere ?? 0)
    if (delta > 0) {
      pending.push({ userId: row.user_id, cycle: row.cycle, cycleEnd: row.cycle_end, credits: delta, total: row.reported + delta })
    }
  }

  return pending
}

/** A stored report: its key, the days it covers, and what was reported (or staged) under it. */
export interface ReportSpan extends Span {
  cycle: string
}

export interface AccountUsage {
  userId: string
  plan: string
  anchor: Date | string | null
  /** Daily credits since at least the start of the previous period. */
  days: DailyCredits[]
  /** Ledger balance now, after closed periods have been settled. */
  ledgerBalance: number
  /** Pack usage settled into the ledger, by the days it covers. */
  packUsage: Span[]
  /** Overage reports, by the days they cover. */
  reports: ReportSpan[]
}

function rowFor(account: AccountUsage, period: BillingPeriod, purchased: number): OverageRow {
  const own = account.reports.find((report) => report.cycle === period.start)
  const others = account.reports.filter((report) => report.cycle !== period.start)
  return {
    user_id: account.userId,
    plan: account.plan,
    cycle: period.start,
    cycle_end: period.end,
    used: creditsInRange(account.days, period.start, period.end),
    purchased,
    reported: own?.amount ?? 0,
    elsewhere: overlapping(others, period),
  }
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

    const closed = rowFor(account, previous, overlapping(account.packUsage, previous))
    if (closed.used > 0) rows.push(closed)

    // Settled pack usage can overlap the open period too, when the anchor has
    // moved back over days already settled; those days were covered then.
    rows.push(rowFor(account, current, account.ledgerBalance + overlapping(account.packUsage, current)))
  }

  return rows
}

/** Earliest day a previous period can start: two calendar months back covers any anchor. */
function windowStart(now: Date): string {
  return addMonths(calendarPeriod(now).start, -2)
}

interface Candidate {
  id: string
  plan: string
  anchor: Date | null
}

/**
 * Accounts that may owe overage. Suspended accounts are included: a suspension
 * stops new usage, not the bill for what was used.
 */
async function candidates(q: Sql, now: Date, only?: { userId: string; plan?: Plan }): Promise<Candidate[]> {
  const users = await q<Candidate[]>`
    SELECT u.id, u.plan, u.billing_period_start AS anchor
    FROM accounts_users u
    WHERE ${only ? q`u.id = ${only.userId}` : q`u.plan <> 'free'`}
      AND EXISTS (
        SELECT 1 FROM accounts_usage a
        WHERE a.user_id = u.id AND a.day >= ${windowStart(now)} AND a.credits > 0
      )`
  return users.filter((user) => (only?.plan ?? getPlan(user.plan)).overageAllowed)
}

/** Gather usage, ledger and reports for `users`. Reads only; settle first. */
async function loadAccounts(q: Sql, now: Date, users: Candidate[]): Promise<AccountUsage[]> {
  if (users.length === 0) return []
  const from = windowStart(now)
  const ids = users.map((user) => user.id)

  const [days, ledger, reports, packUsage] = await Promise.all([
    q<Array<{ user_id: string; day: string; credits: number }>>`
      SELECT user_id, day::text AS day, SUM(credits)::int AS credits
      FROM accounts_usage
      WHERE day >= ${from} AND user_id IN ${q(ids)}
      GROUP BY user_id, day`,
    q<Array<{ user_id: string; total: number }>>`
      SELECT user_id, SUM(amount)::int AS total
      FROM accounts_credit_ledger
      WHERE user_id IN ${q(ids)}
      GROUP BY user_id`,
    // Staged totals count as reported: they are sent before anything new, so
    // computing past them would stage an overlapping increment under a new id.
    q<Array<{ user_id: string; cycle: string; end: string; amount: number }>>`
      SELECT user_id, cycle::text AS cycle,
             COALESCE(cycle_end, (cycle + interval '1 month')::date)::text AS end,
             GREATEST(reported_credits, COALESCE(pending_credits, 0))::int AS amount
      FROM accounts_overage_reports
      WHERE COALESCE(cycle_end, (cycle + interval '1 month')::date) > ${from} AND user_id IN ${q(ids)}`,
    q<Array<{ user_id: string; start: string; end: string; amount: number }>>`
      SELECT user_id, right(external_id, 10) AS start, (created_at AT TIME ZONE 'UTC')::date::text AS end,
             -amount AS amount
      FROM accounts_credit_ledger
      WHERE external_id LIKE ${`${PACK_USAGE_PREFIX}%`} AND created_at > ${from} AND user_id IN ${q(ids)}`,
  ])

  return users.map((user) => ({
    userId: user.id,
    plan: user.plan,
    anchor: user.anchor,
    days: days.filter((row) => row.user_id === user.id).map(({ day, credits }) => ({ day, credits })),
    ledgerBalance: ledger.find((row) => row.user_id === user.id)?.total ?? 0,
    packUsage: packUsage.filter((row) => row.user_id === user.id).map(({ start, end, amount }) => ({ start, end, amount })),
    reports: reports
      .filter((row) => row.user_id === user.id)
      .map(({ cycle, end, amount }) => ({ cycle, start: cycle, end, amount })),
  }))
}

/** Write down what is about to be sent. Never touches `reported_credits` on an existing row. */
async function stage(q: Sql, pending: PendingOverage[]): Promise<void> {
  await q`
    INSERT INTO accounts_overage_reports ${q(
      pending.map((entry) => ({
        user_id: entry.userId,
        cycle: entry.cycle,
        cycle_end: entry.cycleEnd,
        reported_credits: 0,
        pending_credits: entry.total,
      })),
      'user_id',
      'cycle',
      'cycle_end',
      'reported_credits',
      'pending_credits',
    )}
    ON CONFLICT (user_id, cycle) DO UPDATE
      SET pending_credits = EXCLUDED.pending_credits, cycle_end = EXCLUDED.cycle_end, updated_at = now()`
}

/**
 * Send staged reports and, once Polar has them, promote them to reported.
 * Returns false when either step failed; the rows stay staged, and the caller
 * must stop the pass, since anything computed now would be measured from marks
 * that did not move.
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
    return false
  }
  return true
}

async function stagedReports(): Promise<PendingOverage[]> {
  const rows = await sql<Array<{ user_id: string; cycle: string; end: string; reported: number; pending: number }>>`
    SELECT user_id, cycle::text AS cycle,
           COALESCE(cycle_end, (cycle + interval '1 month')::date)::text AS end,
           reported_credits AS reported, pending_credits AS pending
    FROM accounts_overage_reports
    WHERE pending_credits IS NOT NULL`
  return rows.map((row) => ({
    userId: row.user_id,
    cycle: row.cycle,
    cycleEnd: row.end,
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
    if (userId) throw new Error(`the overage report for ${userId} could not be sent or recorded`)
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

    // Settle closed periods first, so the pack balance and each closed
    // period's share of it are final before overage is computed against them.
    const users = await candidates(sql, now, userId ? { userId } : undefined)
    for (const user of users) await settlePackUsage(user.id, getPlan(user.plan), user.anchor)

    const pending = computePendingOverage(buildOverageRows(await loadAccounts(sql, now, users), now))
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
 * overage accrued before a downgrade to free is still billed. `setPlan` settles
 * the pack under the old plan before calling this.
 *
 * Waits for the reporting lock rather than skipping, because a pass running
 * concurrently could otherwise overwrite or promote the staged figure. The wait
 * is bounded; past it the change goes ahead and the remainder is logged as lost.
 * Every query runs on the transaction, so it holds one connection, not two.
 */
export async function stageOverageForPlanChange(userId: string, plan: Plan, now: Date = new Date()): Promise<void> {
  if (!billing.enabled || !billing.meterUsage || !plan.overageAllowed) return
  await flushUsage()

  await sql.begin(async (rawTx) => {
    const tx = rawTx as unknown as Sql
    await tx`SET LOCAL lock_timeout = '10s'`
    await tx`SELECT pg_advisory_xact_lock(${LOCK_CLASS}::int, ${LOCK_ID}::int)`

    const accounts = await loadAccounts(tx, now, await candidates(tx, now, { userId, plan }))
    const pending = computePendingOverage(buildOverageRows(accounts, now, plan), plan)
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
