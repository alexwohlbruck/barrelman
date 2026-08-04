/**
 * Automated abuse detection.
 *
 * Everything here raises a *signal* for a human to look at; nothing suspends an
 * account on its own except the burn-rate rule at its most extreme, and that
 * one is deliberately time-limited rather than permanent. Automated permanent
 * bans on heuristics this crude would cut off real customers having an unusual
 * week, and a false positive costs far more than a few hours of abuse.
 *
 * Runs on the periodic sweep rather than per request, because every rule here
 * is about a pattern over time.
 */
import { connection as sql } from '../db'
import { getPlan } from '../billing/plans'
import { recordAbuseSignal, suspendUser, SYSTEM_ACTOR } from './moderation.service'
import { currentCycleStart, utcDay } from './usage.service'

/**
 * Multiple of a plan's *monthly* allowance burned in a single day before the
 * account is flagged. A free account is entitled to spend its whole month in a
 * day; spending several months' worth is the signal.
 */
const DAILY_BURN_MULTIPLE = Number(process.env.BARRELMAN_BURN_RATE_MULTIPLE ?? 3)

/**
 * Multiple at which the account is auto-suspended rather than merely flagged.
 * Only reachable on plans that allow overage, since a plan without overage
 * stops itself at the allowance.
 */
const AUTO_SUSPEND_MULTIPLE = Number(process.env.BARRELMAN_BURN_RATE_SUSPEND_MULTIPLE ?? 25)

/** How long an automated suspension lasts before lifting itself. */
const AUTO_SUSPEND_HOURS = Number(process.env.BARRELMAN_AUTO_SUSPEND_HOURS ?? 6)

/** Accounts from one sign-up address before it looks like farming. */
const MULTI_ACCOUNT_THRESHOLD = Number(process.env.BARRELMAN_MULTI_ACCOUNT_THRESHOLD ?? 6)

export interface DetectionResult {
  flagged: number
  suspended: number
}

/** One detection pass. Safe to call concurrently — signals are deduplicated. */
export async function runAbuseDetection(): Promise<DetectionResult> {
  const [burn, multi, exhausted] = await Promise.all([
    detectBurnRate(),
    detectMultiAccountSignups(),
    detectQuotaExhaustion(),
  ])

  return {
    flagged: burn.flagged + multi + exhausted,
    suspended: burn.suspended,
  }
}

/**
 * Accounts consuming credits far faster than their plan anticipates.
 *
 * Compares today's spend against the plan's monthly allowance rather than
 * against the account's own history: a brand-new account has no history, and
 * that is exactly when a stolen card or a scraper shows up.
 */
async function detectBurnRate(): Promise<{ flagged: number; suspended: number }> {
  const rows = await sql<Array<{ user_id: string; plan: string; today: number }>>`
    SELECT user_id, u.plan, SUM(credits)::int AS today
    FROM accounts_usage a
    JOIN accounts_users u ON u.id = a.user_id
    WHERE a.day = ${utcDay()} AND u.suspended_at IS NULL
    GROUP BY user_id, u.plan
    HAVING SUM(credits) > 0`

  let flagged = 0
  let suspended = 0

  for (const row of rows) {
    const plan = getPlan(row.plan)
    const ratio = row.today / Math.max(1, plan.monthlyCredits)

    if (ratio >= AUTO_SUSPEND_MULTIPLE && plan.overageAllowed) {
      // Overage means this is running up a real bill with no ceiling. Pausing
      // for a few hours is recoverable for a legitimate customer and stops the
      // bleeding for everyone else; it lifts itself without anyone intervening.
      await suspendUser({
        userId: row.user_id,
        kind: 'automated-abuse',
        reason:
          `Automatic hold: ${row.today.toLocaleString()} credits in one day, ` +
          `${Math.round(ratio)}× the ${plan.name} monthly allowance. ` +
          'Contact support to lift this sooner.',
        actorId: SYSTEM_ACTOR,
        until: new Date(Date.now() + AUTO_SUSPEND_HOURS * 60 * 60_000),
        metadata: { creditsToday: row.today, ratio, plan: plan.id },
      })
      suspended += 1
      continue
    }

    if (ratio >= DAILY_BURN_MULTIPLE) {
      const signal = await recordAbuseSignal({
        userId: row.user_id,
        kind: 'burn-rate',
        severity: ratio >= DAILY_BURN_MULTIPLE * 3 ? 'high' : 'medium',
        detail: { creditsToday: row.today, ratio: Math.round(ratio * 10) / 10, plan: plan.id },
      })
      if (signal) flagged += 1
    }
  }

  return { flagged, suspended }
}

/**
 * Sign-up addresses behind an implausible number of accounts. Only ever a
 * signal: a university, an office or a mobile carrier NAT will trip this
 * legitimately, so suspending on it would be indefensible.
 */
async function detectMultiAccountSignups(): Promise<number> {
  const rows = await sql<Array<{ signup_ip_hash: string; accounts: number }>>`
    SELECT signup_ip_hash, count(*)::int AS accounts
    FROM accounts_users
    WHERE signup_ip_hash IS NOT NULL
      AND created_at > now() - interval '30 days'
    GROUP BY signup_ip_hash
    HAVING count(*) >= ${MULTI_ACCOUNT_THRESHOLD}`

  let flagged = 0
  for (const row of rows) {
    const signal = await recordAbuseSignal({
      kind: 'multi-account',
      severity: row.accounts >= MULTI_ACCOUNT_THRESHOLD * 3 ? 'high' : 'low',
      detail: { accounts: row.accounts },
      ipHash: row.signup_ip_hash,
      dedupeWindowMs: 24 * 60 * 60_000,
    })
    if (signal) flagged += 1
  }
  return flagged
}

/**
 * Accounts generating far more refusals than served requests — a client that
 * has been failing all day and nobody has noticed. Worth a nudge rather than a
 * sanction: it is usually someone's broken integration, not an attack.
 */
async function detectQuotaExhaustion(): Promise<number> {
  const rows = await sql<Array<{ user_id: string; rejected: number; served: number }>>`
    SELECT user_id, SUM(rejected)::int AS rejected, SUM(requests)::int AS served
    FROM accounts_usage
    WHERE day >= ${currentCycleStart()}
    GROUP BY user_id
    HAVING SUM(rejected) > 1000 AND SUM(rejected) > SUM(requests) * 2`

  let flagged = 0
  for (const row of rows) {
    const signal = await recordAbuseSignal({
      userId: row.user_id,
      kind: 'quota-exhausted',
      severity: 'low',
      detail: { rejected: row.rejected, served: row.served },
      dedupeWindowMs: 24 * 60 * 60_000,
    })
    if (signal) flagged += 1
  }
  return flagged
}
