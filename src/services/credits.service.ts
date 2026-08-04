/**
 * Credit balances and quota decisions.
 *
 * An account's spendable balance has two parts:
 *
 *   1. A monthly allowance from its plan, which resets on the 1st (UTC) and
 *      does not roll over.
 *   2. Purchased credits in the ledger, which do not expire.
 *
 * The allowance is spent first, so a prepaid pack survives an idle month rather
 * than being silently consumed by usage the plan already covered.
 *
 * The check runs on every metered request, so the per-account figures are
 * cached briefly. The cache can let an account overshoot by at most the traffic
 * it lands inside one TTL — bounded, small, and preferable to a database round
 * trip per request. It is invalidated directly whenever credits are granted or
 * a plan changes.
 */
import { eq, sql as dsql } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'
import { db } from '../db'
import { creditLedger, users, type CreditEntryKind } from '../schema/accounts'
import { generateId } from '../lib/crypto'
import { getPlan, type Plan } from '../billing/plans'
import { creditsUsedThisCycle, pendingCredits } from './usage.service'

const BALANCE_CACHE_TTL_MS = Number(process.env.BARRELMAN_BALANCE_CACHE_MS ?? 15_000)

interface CachedBalance {
  plan: Plan
  /** Credits used this cycle as of the last database read. */
  usedAtFetch: number
  /** Ledger total (purchases and adjustments) as of the last database read. */
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

  const [[userRow], [ledgerRow], used] = await Promise.all([
    db.select({ plan: users.plan }).from(users).where(eq(users.id, userId)).limit(1),
    db
      .select({ total: dsql<number>`COALESCE(SUM(${creditLedger.amount}), 0)::int` })
      .from(creditLedger)
      .where(eq(creditLedger.userId, userId)),
    creditsUsedThisCycle(userId),
  ])

  const fresh: CachedBalance = {
    plan: getPlan(userRow?.plan),
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
  /** Non-expiring credits bought or granted. */
  purchased: number
  /** Remaining allowance before purchased credits are touched. */
  allowanceRemaining: number
  /** Everything still spendable: allowance plus purchased. */
  remaining: number
  /** Whether requests continue past zero, billed as overage. */
  overageAllowed: boolean
  /** Credits already served as overage this cycle. */
  overage: number
  cycleResetsAt: string
}

/**
 * The balance arithmetic, as a pure function of (plan, used, purchased).
 *
 * Split out from `getBalance` so it can be tested without a database — this is
 * what decides whether a customer is served or refused, and it was previously
 * reachable only through a live Postgres connection.
 */
export function computeBalance(plan: Plan, used: number, purchased: number): CreditBalance {
  const allowanceRemaining = Math.max(0, plan.monthlyCredits - used)
  const spentBeyondAllowance = Math.max(0, used - plan.monthlyCredits)

  // The monthly allowance is spent before purchased credits, so a prepaid pack
  // survives an idle month instead of being consumed by usage the plan covered.
  const purchasedRemaining = Math.max(0, purchased - spentBeyondAllowance)
  const overage = plan.overageAllowed ? Math.max(0, spentBeyondAllowance - purchased) : 0

  return {
    plan,
    monthlyCredits: plan.monthlyCredits,
    used,
    purchased,
    allowanceRemaining,
    remaining: allowanceRemaining + purchasedRemaining,
    overageAllowed: plan.overageAllowed,
    overage,
    cycleResetsAt: nextCycleStart(),
  }
}

export async function getBalance(userId: string): Promise<CreditBalance> {
  const { plan, usedAtFetch, purchased } = await loadBalance(userId)
  return computeBalance(plan, usedAtFetch + pendingCredits(userId), purchased)
}

function nextCycleStart(date: Date = new Date()): string {
  const year = date.getUTCFullYear()
  const month = date.getUTCMonth()
  return new Date(Date.UTC(month === 11 ? year + 1 : year, (month + 1) % 12, 1)).toISOString()
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

/** Change an account's plan and drop every cached decision that depended on it. */
export async function setPlan(userId: string, planId: string): Promise<void> {
  await db.update(users).set({ plan: planId, updatedAt: new Date() }).where(eq(users.id, userId))
  invalidateBalance(userId)
}
