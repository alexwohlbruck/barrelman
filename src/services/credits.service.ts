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
import { and, eq, isNull, like, ne, or, sql as dsql } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'
import { accountsConnection as sql, accountsDb as db } from '../db'
import { creditLedger, users, type CreditEntryKind } from '../schema/accounts'
import { generateId } from '../lib/crypto'
import { getPlan, type Plan } from '../billing/plans'
import {
  addDays,
  billingPeriod,
  calendarPeriod,
  dayStart,
  periodsBetween,
  followsSubscription,
  segmentAllowance,
  segmentOf,
  toDay,
  type BillingPeriod,
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
import { publishCacheEvent, registerCacheHandler } from './cache-bus.service'

const BALANCE_CACHE_TTL_MS = envNumber('BARRELMAN_BALANCE_CACHE_MS', 15_000)

interface CachedBalance {
  plan: Plan
  period: BillingPeriod
  /** The open segment's included allowance, prorated after a plan change. */
  allowance: number
  /** Credits used this cycle as of the last database read. */
  usedAtFetch: number
  /** Ledger total (purchases, adjustments, settled pack usage) as of the last database read. */
  purchased: number
}

const balanceCache = new LRUCache<string, CachedBalance>({ max: 10_000, ttl: BALANCE_CACHE_TTL_MS })

/** Here and, through the cache bus, on every other replica. */
export function invalidateBalance(userId: string): void {
  balanceCache.delete(userId)
  publishCacheEvent({ kind: 'user', id: userId })
}

export function clearBalanceCache(): void {
  balanceCache.clear()
}

// A `user` event is anything that changes what an account may spend: a plan
// change, a grant, a suspension. Key events say nothing about balances.
registerCacheHandler('balance', {
  onEvent(event) {
    if (event.kind === 'user') balanceCache.delete(event.id)
  },
  onResync: () => balanceCache.clear(),
})

async function loadBalance(userId: string): Promise<CachedBalance> {
  const cached = balanceCache.get(userId)
  if (cached) return cached

  const [userRow] = await db
    .select({
      plan: users.plan,
      anchor: users.billingPeriodStart,
      meteredFrom: users.meteredFrom,
      newSubscription: users.meteredFromNewSubscription,
    })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  const plan = getPlan(userRow?.plan)
  const anchor = userRow?.anchor ?? null
  const period = billingPeriod(plan, anchor)
  // After a plan or subscription change this period, only the days since count
  // against the new plan's allowance: the earlier ones were judged at the change.
  const segment = segmentOf(period, userRow?.meteredFrom) ?? { start: period.end, end: period.end }
  const allowance = segmentAllowance(plan, period, segment, { fromPeriodStart: isNewSubscriptionSegment(segment, userRow) })

  // Before reading the ledger, so a period that has just closed has taken its
  // share of the pack. Logged rather than thrown: a failed write must not
  // refuse requests, and the next cache miss tries again.
  await settlePackUsage(userId, plan, anchor, period.start).catch((err) =>
    console.error(`[credits] could not settle pack usage for ${userId}`, err),
  )

  const [[ledgerRow], used] = await Promise.all([
    db
      .select({ total: dsql<number>`COALESCE(SUM(${creditLedger.amount}), 0)::int` })
      .from(creditLedger)
      .where(eq(creditLedger.userId, userId)),
    creditsUsedThisCycle(userId, segment.start),
  ])

  const fresh: CachedBalance = {
    plan,
    period,
    allowance,
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
  /** Credits included this cycle: the plan's allowance, prorated for the part of the cycle since a plan change. */
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
  allowance: number = plan.monthlyCredits,
): CreditBalance {
  const allowanceRemaining = Math.max(0, allowance - used)
  const spentBeyondAllowance = Math.max(0, used - allowance)

  // The monthly allowance is spent before purchased credits, so a prepaid pack
  // survives an idle month instead of being consumed by usage the plan covered.
  const purchasedRemaining = Math.max(0, purchased - spentBeyondAllowance)
  // A pack refunded after it was spent leaves the ledger negative. That debt is
  // not overage, so it is clamped here rather than added to the metered bill.
  const overage = plan.overageAllowed ? Math.max(0, spentBeyondAllowance - Math.max(0, purchased)) : 0

  return {
    plan,
    monthlyCredits: allowance,
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
  const { plan, period, allowance, usedAtFetch, purchased } = await loadBalance(userId)
  return computeBalance(plan, usedAtFetch + pendingCredits(userId), purchased, period, allowance)
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

/** The pool, or a transaction, for ledger writes that must share one. */
export type LedgerExecutor = typeof db | Parameters<Parameters<typeof db.transaction>[0]>[0]

/** Returns false when `externalId` has already been recorded. */
export async function grantCredits(options: GrantOptions, executor: LedgerExecutor = db): Promise<boolean> {
  const { userId, amount, kind, description = null, externalId = null } = options

  const inserted = await executor
    .insert(creditLedger)
    .values({ id: generateId(), userId, amount, kind, description, externalId })
    .onConflictDoNothing({ target: creditLedger.externalId })
    .returning({ id: creditLedger.id })

  if (inserted.length === 0 && externalId) return false

  invalidateBalance(userId)
  return true
}

/**
 * Sum of an account's ledger entries whose external id starts with `prefix`.
 * Lets a refund work out how much of an order it has already taken back.
 */
export async function ledgerTotalForExternalPrefix(
  userId: string,
  prefix: string,
  executor: LedgerExecutor = db,
): Promise<number> {
  const [row] = await executor
    .select({ total: dsql<number>`COALESCE(SUM(${creditLedger.amount}), 0)::int` })
    .from(creditLedger)
    .where(and(eq(creditLedger.userId, userId), like(creditLedger.externalId, `${escapeLike(prefix)}%`)))
  return row?.total ?? 0
}

function escapeLike(value: string): string {
  return value.replace(/[\\%_]/g, (c) => `\\${c}`)
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
 * share a key. That day is also the start of the overage segment the stretch
 * belongs to, which is how the reporter finds it.
 */
export function packUsageId(userId: string, from: string): string {
  return `${PACK_USAGE_PREFIX}${userId}:${from}`
}

export interface SettlementLedgerEntry {
  amount: number
  createdAt: Date
  externalId: string | null
}

export interface PackUsage {
  /** The days settled: the period, less any days settled before it or after `until`. */
  period: BillingPeriod
  /** Positive: how much pack credit those days consumed. */
  credits: number
}

/**
 * Work out what each stretch of days took from the pack, as a pure function.
 *
 * Periods are settled oldest first, and only days in `[from, until)` count:
 * every day before `from` was judged by an earlier settlement, possibly under
 * another plan or anchor, and must not be judged again. `until` is the open
 * period's start, or the day of a plan or subscription change, which settles
 * the open period's days so far under the plan they ran on. A clipped period
 * gets its plan's allowance prorated by the days it covers (`segmentAllowance`).
 *
 * A stretch can spend only what the ledger held when it ended, so a pack bought
 * on the 2nd never pays for the month before it. Earlier settlements are dated
 * at their own stretch's end, so each counts against every later one and never
 * against its own.
 *
 * `reported` is overage already sent to the billing provider, by segment start.
 * That excess has been paid for as overage, and taking it from the pack as
 * well would charge for the same credits twice.
 */
export function computePackUsage(input: {
  userId: string
  plan: Plan
  periods: BillingPeriod[]
  days: DailyCredits[]
  ledger: SettlementLedgerEntry[]
  from: string
  until?: string
  reported?: Record<string, number>
  /** Start of a segment opened by a new subscription, which keeps its allowance from the period's start. */
  fullFrom?: string | null
  /** The cut at `until` is a subscription ending, which keeps the allowance to the period's end. */
  untilKeepsAllowance?: boolean
}): PackUsage[] {
  const { userId, plan, days, from, until, reported = {}, fullFrom, untilKeepsAllowance = false } = input
  const ledger = [...input.ledger]
  const settled = new Set(ledger.map((entry) => entry.externalId))
  const result: PackUsage[] = []

  for (const whole of [...input.periods].sort((a, b) => a.start.localeCompare(b.start))) {
    const period = segmentOf(whole, from, until)
    if (!period || settled.has(packUsageId(userId, period.start))) continue

    const used = creditsInRange(days, period.start, period.end)
    const allowance = segmentAllowance(plan, whole, period, {
      fromPeriodStart: period.start === fullFrom,
      toPeriodEnd: untilKeepsAllowance && period.end === until,
    })
    const excess = Math.max(0, used - allowance - (reported[period.start] ?? 0))
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
 * The day each account was last settled through, per process. The database
 * column is the record; this only keeps the read off every balance cache miss.
 */
const settledThrough = new LRUCache<string, string>({ max: 50_000 })

/**
 * Write the pack usage of every day before `until` that has not been settled,
 * then move `pack_settled_through` to `until`. `until` defaults to the open
 * period's start. Runs on balance reads, in the overage reporter and at a plan
 * or subscription change, whichever comes first. Returns the credits written.
 *
 * The marker advances whether or not anything was written. Each day is
 * therefore judged once, against the plan current at the time, and a later
 * downgrade cannot reach back and drain a pack for days the old plan covered.
 */
export async function settlePackUsage(
  userId: string,
  plan: Plan,
  anchor: Date | string | null,
  until: string = billingPeriod(plan, anchor).start,
  untilKeepsAllowance = false,
): Promise<number> {
  if (settledThrough.get(userId) === until) return 0

  const [account] = await db
    .select({
      settledThrough: users.packSettledThrough,
      meteredFrom: users.meteredFrom,
      newSubscription: users.meteredFromNewSubscription,
    })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  const stored = account?.settledThrough ?? null
  if (stored && stored >= until) {
    settledThrough.set(userId, until)
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
  const periods = from ? periodsBetween(plan, anchor, from, until) : []

  let written: PackUsage[] = []
  if (from && periods.length > 0) {
    // A few seconds of the closed period may still be in this process's buffer.
    await flushUsage()

    const [days, reportRows] = await Promise.all([
      creditsByDay(userId, from, until),
      sql<Array<{ cycle: string; amount: number }>>`
        SELECT cycle::text AS cycle, GREATEST(reported_credits, COALESCE(pending_credits, 0))::int AS amount
        FROM accounts_overage_reports
        WHERE user_id = ${userId} AND cycle >= ${from}`,
    ])

    written = computePackUsage({
      userId,
      plan,
      periods,
      days,
      ledger,
      from,
      until,
      reported: Object.fromEntries(reportRows.map((row) => [row.cycle, row.amount])),
      fullFrom: account?.newSubscription ? account.meteredFrom : null,
      untilKeepsAllowance,
    })

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

  await db.update(users).set({ packSettledThrough: until }).where(eq(users.id, userId))
  if (written.length) invalidateBalance(userId)
  settledThrough.set(userId, until)
  return written.reduce((sum, entry) => sum + entry.credits, 0)
}

// ── Plans and periods ───────────────────────────────────────────────────

export interface SubscriptionPeriod {
  subscriptionId: string | null | undefined
  /** The subscription's `currentPeriodStart`. Never its period end. */
  periodStart: Date | string | null | undefined
}

/**
 * End the account's current metering segment today, under the plan and anchor
 * it ran on, before either changes.
 *
 * Settles the pack and stages the overage for the days so far, then sets
 * `metered_from` to today. From then on the new plan or anchor judges only the
 * days after the change: a downgrade mid-period does not re-judge what the
 * bigger plan already covered, and a new anchor never meets a report keyed on
 * the old one. Today itself goes to the new segment, so every day is metered
 * exactly once. After a plan change the new segment's allowance is prorated by
 * the days left in the period; after a new subscription it is whole.
 */
interface SegmentChange {
  /** A new subscription starts: the next segment keeps its allowance from the period's start. */
  newSubscription: boolean
  /** The subscription ends rather than changing plan: the closed segment keeps its allowance to the period's end. */
  subscriptionEnds: boolean
}

async function closeSegment(userId: string, plan: Plan, anchor: Date | null, change: SegmentChange): Promise<void> {
  const today = toDay(new Date())
  const [current] = await db
    .select({ meteredFrom: users.meteredFrom, newSubscription: users.meteredFromNewSubscription })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  // A checkout records the new subscription and then the plan, both today; the
  // second call must not take away the first one's whole allowance.
  const newSubscription = change.newSubscription || Boolean(current?.newSubscription && current.meteredFrom === today)

  await settlePackUsage(userId, plan, anchor, today, change.subscriptionEnds).catch((err) =>
    console.error(`[credits] could not settle pack usage for ${userId} before a change`, err),
  )

  // Overage accrued so far is still owed after the change, but the reporter
  // computes against the current plan and grid and would never see it again.
  // Imported lazily because overage.service imports billing.service, which
  // imports this module.
  if (plan.overageAllowed) {
    const { stageOverageBeforeChange } = await import('./overage.service')
    await stageOverageBeforeChange(userId, plan, today, change.subscriptionEnds).catch((err) =>
      console.error(`[billing] could not stage final overage for ${userId} on ${plan.id}`, err),
    )
  }

  await db.update(users).set({ meteredFrom: today, meteredFromNewSubscription: newSubscription }).where(eq(users.id, userId))
  invalidateBalance(userId)
  settledThrough.delete(userId)
}

/** Whether `segment` is the one a new subscription opened, which keeps a whole allowance. */
export function isNewSubscriptionSegment(
  segment: BillingPeriod,
  account: { meteredFrom: string | null; newSubscription: boolean } | null | undefined,
): boolean {
  return Boolean(account?.newSubscription && account.meteredFrom === segment.start)
}

async function currentAccount(userId: string) {
  const [row] = await db
    .select({ plan: users.plan, anchor: users.billingPeriodStart, subscriptionId: users.billingPeriodSubscriptionId })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1)
  return row ?? null
}

/**
 * Record the subscription's period start as the account's anchor, once per
 * subscription. Later events for the same subscription leave it alone: Polar
 * steps the period by months from the same day, which the anchor already does,
 * and rewriting it from a clamped date (28 February for a subscription taken
 * on the 31st) would shift every later period. Only a new subscription, such
 * as a resubscription on another day, moves it, and the segment so far is
 * closed under the old anchor first.
 *
 * Returns whether the anchor changed.
 */
export async function recordBillingPeriod(userId: string, period: SubscriptionPeriod | null | undefined): Promise<boolean> {
  const { subscriptionId, periodStart } = period ?? {}
  if (!subscriptionId || !periodStart) return false
  const start = new Date(periodStart)
  if (Number.isNaN(start.getTime())) return false

  const account = await currentAccount(userId)
  if (!account || account.subscriptionId === subscriptionId) return false

  await closeSegment(userId, getPlan(account.plan), account.anchor, { newSubscription: true, subscriptionEnds: true })

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
  const account = await currentAccount(userId)
  if (account && account.plan !== planId) {
    // Paid to paid is a plan change inside one subscription, prorated at the
    // cut. Anything else (to or from a plan with no subscription) is the
    // subscription ending or starting, which paid for the period it had.
    const from = getPlan(account.plan)
    const withinSubscription = followsSubscription(from) && followsSubscription(getPlan(planId))
    await closeSegment(userId, from, account.anchor, { newSubscription: false, subscriptionEnds: !withinSubscription })
  }

  await db.update(users).set({ plan: planId, updatedAt: new Date() }).where(eq(users.id, userId))
  invalidateBalance(userId)
  settledThrough.delete(userId)
}
