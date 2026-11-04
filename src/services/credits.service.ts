/**
 * Credit balances and quota decisions.
 *
 * An account's spendable balance has two parts:
 *
 *   1. A monthly allowance from its plan, which resets at the start of each
 *      billing period and does not roll over. Free accounts' periods are
 *      calendar months; subscribed accounts' follow the subscription (see
 *      billing/periods.ts).
 *   2. Purchased and granted credits in the ledger, which do not expire but
 *      are used up.
 *
 * The allowance is spent first, so a prepaid pack survives an idle month rather
 * than being silently consumed by usage the plan already covered.
 *
 * How a pack gets used up: usage is counted per period while the ledger is
 * all-time, so subtracting only the open period's excess from the ledger total
 * would hand a pack back in full every time a period closed. So when a period
 * closes, the pack credit it consumed is written to the ledger as a negative
 * `usage` entry, dated at the period's end and keyed on (account, period) so it
 * is written once. The ledger total is then what is actually left, and the
 * live arithmetic below only has to account for the open period. Replaying
 * every past period on each read would give the same figure, but would put an
 * account's whole history on the hot path, and would need the plan it had in
 * each month, which nothing records.
 *
 * The check runs on every metered request, so the per-account figures are
 * cached briefly. The cache can let an account overshoot by at most the traffic
 * it lands inside one TTL — bounded, small, and preferable to a database round
 * trip per request. It is invalidated directly whenever credits are granted or
 * a plan changes.
 */
import { and, eq, isNull, ne, or, sql as dsql } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'
import { connection as sql, db } from '../db'
import { creditLedger, users, type CreditEntryKind } from '../schema/accounts'
import { generateId } from '../lib/crypto'
import { getPlan, type Plan } from '../billing/plans'
import {
  addDays,
  billingPeriod,
  calendarPeriod,
  dayStart,
  overlapping,
  periodsBetween,
  toDay,
  type BillingPeriod,
  type Span,
} from '../billing/periods'
import {
  creditsByDay,
  creditsInRange,
  creditsUsedThisCycle,
  flushUsage,
  pendingCredits,
  type DailyCredits,
} from './usage.service'
import { envNumber } from '../config/env'

const BALANCE_CACHE_TTL_MS = envNumber('BARRELMAN_BALANCE_CACHE_MS', 15_000)

interface CachedBalance {
  plan: Plan
  period: BillingPeriod
  /** Credits used this cycle as of the last database read. */
  usedAtFetch: number
  /** Ledger total (purchases, adjustments, settled pack usage) as of the last database read. */
  purchased: number
}

const balanceCache = new LRUCache<string, CachedBalance>({ max: 10_000, ttl: BALANCE_CACHE_TTL_MS })

export function invalidateBalance(userId: string): void {
  balanceCache.delete(userId)
}

export function clearBalanceCache(): void {
  balanceCache.clear()
}

async function loadBalance(userId: string): Promise<CachedBalance> {
  const cached = balanceCache.get(userId)
  if (cached) return cached

  const [userRow] = await db
    .select({ plan: users.plan, anchor: users.billingPeriodStart })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  const plan = getPlan(userRow?.plan)
  const anchor = userRow?.anchor ?? null
  const period = billingPeriod(plan, anchor)

  // Before reading the ledger, so a period that has just closed has taken its
  // share of the pack. Logged rather than thrown: a failed write must not
  // refuse requests, and the next cache miss tries again.
  await settlePackUsage(userId, plan, anchor, period).catch((err) =>
    console.error(`[credits] could not settle pack usage for ${userId}`, err),
  )

  const [[ledgerRow], used] = await Promise.all([
    db
      .select({ total: dsql<number>`COALESCE(SUM(${creditLedger.amount}), 0)::int` })
      .from(creditLedger)
      .where(eq(creditLedger.userId, userId)),
    creditsUsedThisCycle(userId, period.start),
  ])

  const fresh: CachedBalance = {
    plan,
    period,
    // `creditsUsedThisCycle` already folds in the unflushed buffer; subtract it
    // back out so the cached figure is the persisted one and the live buffer is
    // re-added on every read. Otherwise buffered usage would be counted twice
    // as soon as it flushed.
    usedAtFetch: used - pendingCredits(userId),
    purchased: ledgerRow?.total ?? 0,
  }

  balanceCache.set(userId, fresh)
  return fresh
}

export interface CreditBalance {
  plan: Plan
  /** Credits included with the plan this cycle. */
  monthlyCredits: number
  /** Credits consumed this cycle. */
  used: number
  /** Credits bought or granted that earlier cycles left unspent. They do not expire, and are used up. */
  purchased: number
  /** Remaining allowance before purchased credits are touched. */
  allowanceRemaining: number
  /** Everything still spendable: allowance plus purchased. */
  remaining: number
  /** Whether requests continue past zero, billed as overage. */
  overageAllowed: boolean
  /** Credits already served as overage this cycle. */
  overage: number
  /** Start of this cycle: the 1st of the month on free, the subscription's renewal day on a paid plan. */
  cycleStartedAt: string
  cycleResetsAt: string
}

/**
 * The balance arithmetic, as a pure function of (plan, used, purchased).
 *
 * Split out from `getBalance` so it can be tested without a database — this is
 * what decides whether a customer is served or refused, and it was previously
 * reachable only through a live Postgres connection.
 */
export function computeBalance(
  plan: Plan,
  used: number,
  purchased: number,
  period: BillingPeriod = calendarPeriod(new Date()),
): CreditBalance {
  const allowanceRemaining = Math.max(0, plan.monthlyCredits - used)
  const spentBeyondAllowance = Math.max(0, used - plan.monthlyCredits)

  // The monthly allowance is spent before purchased credits, so a prepaid pack
  // survives an idle month instead of being consumed by usage the plan covered.
  const purchasedRemaining = Math.max(0, purchased - spentBeyondAllowance)
  // A pack refunded after it was spent leaves the ledger negative. That debt is
  // not overage, so it is clamped here rather than added to the metered bill.
  const overage = plan.overageAllowed ? Math.max(0, spentBeyondAllowance - Math.max(0, purchased)) : 0

  return {
    plan,
    monthlyCredits: plan.monthlyCredits,
    used,
    purchased,
    allowanceRemaining,
    remaining: allowanceRemaining + purchasedRemaining,
    overageAllowed: plan.overageAllowed,
    overage,
    cycleStartedAt: dayStart(period.start).toISOString(),
    cycleResetsAt: dayStart(period.end).toISOString(),
  }
}

export async function getBalance(userId: string): Promise<CreditBalance> {
  const { plan, period, usedAtFetch, purchased } = await loadBalance(userId)
  return computeBalance(plan, usedAtFetch + pendingCredits(userId), purchased, period)
}

export type QuotaDecision =
  | { allowed: true; overage: boolean }
  | { allowed: false; reason: 'out-of-credits' | 'overage-cap-reached'; balance: CreditBalance }

/**
 * Decide whether an account may spend `credits` right now.
 *
 * Only reads state — the spend itself is recorded through the usage buffer, so
 * this is intentionally not a reservation. Two simultaneous requests can both
 * be approved against the last few credits; the overshoot is at most one
 * request's cost per concurrent caller, which is not worth a lock on the hot
 * path of an autocomplete endpoint.
 */
export function decideQuota(balance: CreditBalance, credits: number): QuotaDecision {
  if (balance.remaining >= credits) {
    return { allowed: true, overage: false }
  }

  if (balance.overageAllowed) {
    // Paid plans keep serving and bill the excess; stopping a production
    // integration dead at the allowance boundary would be worse than the bill.
    // But not without limit — a leaked key would otherwise accrue charges
    // indefinitely, and the burn-rate detector only runs on the hourly sweep.
    const cap = balance.plan.overageCapMultiple * balance.plan.monthlyCredits
    if (cap > 0 && balance.overage + credits > cap) {
      return { allowed: false, reason: 'overage-cap-reached', balance }
    }
    return { allowed: true, overage: true }
  }

  return { allowed: false, reason: 'out-of-credits', balance }
}

export async function checkQuota(userId: string, credits: number): Promise<QuotaDecision> {
  return decideQuota(await getBalance(userId), credits)
}

// ── Ledger ──────────────────────────────────────────────────────────────

export interface GrantOptions {
  userId: string
  amount: number
  kind: CreditEntryKind
  description?: string
  /**
   * Provider-side id (a Polar order). Unique, so a webhook Polar retries
   * cannot grant the same pack twice.
   */
  externalId?: string | null
}

/** Returns false when `externalId` has already been recorded. */
export async function grantCredits(options: GrantOptions): Promise<boolean> {
  const { userId, amount, kind, description = null, externalId = null } = options

  const inserted = await db
    .insert(creditLedger)
    .values({ id: generateId(), userId, amount, kind, description, externalId })
    .onConflictDoNothing({ target: creditLedger.externalId })
    .returning({ id: creditLedger.id })

  if (inserted.length === 0 && externalId) return false

  invalidateBalance(userId)
  return true
}

export interface LedgerEntry {
  id: string
  amount: number
  kind: CreditEntryKind
  description: string | null
  createdAt: Date
}

export async function listLedger(userId: string, limit = 50): Promise<LedgerEntry[]> {
  return db
    .select({
      id: creditLedger.id,
      amount: creditLedger.amount,
      kind: creditLedger.kind,
      description: creditLedger.description,
      createdAt: creditLedger.createdAt,
    })
    .from(creditLedger)
    .where(eq(creditLedger.userId, userId))
    .orderBy(dsql`${creditLedger.createdAt} DESC`)
    .limit(limit)
}

// ── Settling closed periods ─────────────────────────────────────────────

export const PACK_USAGE_PREFIX = 'pack-usage:'

/**
 * Ledger key for the pack credit one stretch of days consumed. Keyed on the
 * first day settled, which only moves forward, so two settlements can never
 * share a key.
 */
export function packUsageId(userId: string, from: string): string {
  return `${PACK_USAGE_PREFIX}${userId}:${from}`
}

/** The days a pack-usage ledger entry covers, as a span for `overlapping()`. */
export function packUsageSpan(externalId: string, createdAt: Date, amount: number): Span {
  return { start: externalId.slice(-10), end: toDay(createdAt), amount: -amount }
}

export interface SettlementLedgerEntry {
  amount: number
  createdAt: Date
  externalId: string | null
}

export interface PackUsage {
  /** The days settled: the period, less any days settled before it. */
  period: BillingPeriod
  /** Positive: how much pack credit those days consumed. */
  credits: number
}

/**
 * Work out what each closed period took from the pack, as a pure function.
 *
 * Periods are settled oldest first, and only days from `from` on count: every
 * day before it was judged by an earlier settlement, possibly under another
 * plan or anchor, and must not be judged again. A period clipped that way
 * keeps its whole allowance, which errs toward the customer.
 *
 * A period can spend only what the ledger held when it ended, so a pack bought
 * on the 2nd never pays for the month before it. Earlier settlements are dated
 * at their own period's end, so each counts against every later period and
 * never against its own.
 *
 * `reported` is overage already sent to the billing provider. That excess has
 * been paid for as overage, and taking it from the pack as well would charge
 * for the same credits twice.
 */
export function computePackUsage(input: {
  userId: string
  plan: Plan
  periods: BillingPeriod[]
  days: DailyCredits[]
  ledger: SettlementLedgerEntry[]
  from: string
  reported?: Span[]
}): PackUsage[] {
  const { userId, plan, days, from, reported = [] } = input
  const ledger = [...input.ledger]
  const settled = new Set(ledger.map((entry) => entry.externalId))
  const result: PackUsage[] = []

  for (const whole of [...input.periods].sort((a, b) => a.start.localeCompare(b.start))) {
    const period = { start: whole.start < from ? from : whole.start, end: whole.end }
    if (period.start >= period.end || settled.has(packUsageId(userId, period.start))) continue

    const used = creditsInRange(days, period.start, period.end)
    const excess = Math.max(0, used - plan.monthlyCredits - overlapping(reported, period))
    if (excess === 0) continue

    const endsAt = dayStart(period.end)
    const available = ledger.reduce((sum, entry) => (entry.createdAt < endsAt ? sum + entry.amount : sum), 0)
    const credits = Math.min(Math.max(0, available), excess)
    if (credits === 0) continue

    result.push({ period, credits })
    ledger.push({ amount: -credits, createdAt: endsAt, externalId: packUsageId(userId, period.start) })
  }

  return result
}

/**
 * The open period each account was last settled in, per process. The database
 * column is the record; this only keeps the read off every balance cache miss.
 */
const settledThrough = new LRUCache<string, string>({ max: 50_000 })

/**
 * Write the pack usage of every period that has closed since the account was
 * last settled, then move `pack_settled_through` to the open period's start.
 * Runs on balance reads, in the overage reporter and before a plan change,
 * whichever reaches a closed period first. Returns the credits written.
 *
 * The marker advances whether or not anything was written. Each closed day is
 * therefore judged once, against the plan current at the time, and a later
 * downgrade cannot reach back and drain a pack for months the old plan covered.
 */
export async function settlePackUsage(
  userId: string,
  plan: Plan,
  anchor: Date | string | null,
  current: BillingPeriod = billingPeriod(plan, anchor),
): Promise<number> {
  if (settledThrough.get(userId) === current.start) return 0

  const [account] = await db
    .select({ settledThrough: users.packSettledThrough })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  const stored = account?.settledThrough ?? null
  if (stored && stored >= current.start) {
    settledThrough.set(userId, current.start)
    return 0
  }

  const ledger = await db
    .select({ amount: creditLedger.amount, createdAt: creditLedger.createdAt, externalId: creditLedger.externalId })
    .from(creditLedger)
    .where(eq(creditLedger.userId, userId))

  // Never settled: start at the period of the first ledger entry, since before
  // it there was nothing to spend. No ledger at all: nothing to settle.
  const firstEntry = ledger.map((entry) => toDay(entry.createdAt)).sort()[0]
  const from = stored ?? (firstEntry ? billingPeriod(plan, anchor, firstEntry).start : null)
  const periods = from ? periodsBetween(plan, anchor, from, current.start) : []

  let written: PackUsage[] = []
  if (from && periods.length > 0) {
    // A few seconds of the closed period may still be in this process's buffer.
    await flushUsage()

    const [days, reportRows] = await Promise.all([
      creditsByDay(userId, from, current.start),
      sql<Array<{ start: string; end: string; amount: number }>>`
        SELECT cycle::text AS start,
               COALESCE(cycle_end, (cycle + interval '1 month')::date)::text AS end,
               GREATEST(reported_credits, COALESCE(pending_credits, 0))::int AS amount
        FROM accounts_overage_reports
        WHERE user_id = ${userId} AND COALESCE(cycle_end, (cycle + interval '1 month')::date) > ${from}`,
    ])

    written = computePackUsage({ userId, plan, periods, days, ledger, from, reported: [...reportRows] })

    for (const { period, credits } of written) {
      await db
        .insert(creditLedger)
        .values({
          id: generateId(),
          userId,
          amount: -credits,
          kind: 'usage',
          description: `Pack credit used ${period.start} to ${addDays(period.end, -1)}`,
          externalId: packUsageId(userId, period.start),
          createdAt: dayStart(period.end),
        })
        .onConflictDoNothing({ target: creditLedger.externalId })
    }
  }

  await db.update(users).set({ packSettledThrough: current.start }).where(eq(users.id, userId))
  if (written.length) invalidateBalance(userId)
  settledThrough.set(userId, current.start)
  return written.reduce((sum, entry) => sum + entry.credits, 0)
}

// ── Plans and periods ───────────────────────────────────────────────────

export interface SubscriptionPeriod {
  subscriptionId: string | null | undefined
  /** The subscription's `currentPeriodStart`. Never its period end. */
  periodStart: Date | string | null | undefined
}

/**
 * Record the subscription's period start as the account's anchor, once per
 * subscription. Later events for the same subscription leave it alone: Polar
 * steps the period by months from the same day, which the anchor already does,
 * and rewriting it from a clamped date (28 February for a subscription taken
 * on the 31st) would shift every later period. Only a new subscription, such
 * as a resubscription on another day, moves it.
 *
 * Returns whether the anchor changed.
 */
export async function recordBillingPeriod(userId: string, period: SubscriptionPeriod | null | undefined): Promise<boolean> {
  const { subscriptionId, periodStart } = period ?? {}
  if (!subscriptionId || !periodStart) return false
  const start = new Date(periodStart)
  if (Number.isNaN(start.getTime())) return false

  const updated = await db
    .update(users)
    .set({ billingPeriodStart: start, billingPeriodSubscriptionId: subscriptionId })
    .where(
      and(
        eq(users.id, userId),
        or(isNull(users.billingPeriodSubscriptionId), ne(users.billingPeriodSubscriptionId, subscriptionId)),
      ),
    )
    .returning({ id: users.id })
  if (updated.length === 0) return false

  invalidateBalance(userId)
  settledThrough.delete(userId)
  return true
}

/** Change an account's plan and drop every cached decision that depended on it. */
export async function setPlan(userId: string, planId: string): Promise<void> {
  const [row] = await db
    .select({ plan: users.plan, anchor: users.billingPeriodStart })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  const previous = row ? getPlan(row.plan) : null

  // Closed periods are settled against the plan they ran on while it is still
  // the current one; afterwards they would be judged by the new allowance.
  if (previous && previous.id !== planId) {
    await settlePackUsage(userId, previous, row?.anchor ?? null).catch((err) =>
      console.error(`[credits] could not settle pack usage for ${userId} before a plan change`, err),
    )
  }

  // Overage accrued on the old plan is still owed after the change, but the
  // reporter computes against the current plan and would never see it once the
  // account is on free. Stage it under the old plan first. Imported lazily
  // because overage.service imports billing.service, which imports this module.
  if (previous && previous.id !== planId && previous.overageAllowed) {
    const { stageOverageForPlanChange } = await import('./overage.service')
    await stageOverageForPlanChange(userId, previous).catch((err) =>
      console.error(`[billing] could not stage final overage for ${userId} leaving ${previous.id}`, err),
    )
  }

  await db.update(users).set({ plan: planId, updatedAt: new Date() }).where(eq(users.id, userId))
  invalidateBalance(userId)
  settledThrough.delete(userId)
}
